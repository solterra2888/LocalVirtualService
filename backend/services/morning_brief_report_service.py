# -*- coding: utf-8 -*-
"""
Morning Brief Report Service — facts-report LLM agent 管线的报告登记服务

Morning Brief（事实/观点分离版晨报）：全局单例（无 user_id），每天一份。
生成体是外部 LLM agent 管线（tools/facts-report，40-60 分钟），由
task_scheduling(morning_brief) → celery generate_morning_brief 以 detached
子进程派发；本服务不生成内容，只负责：

  - 状态机：mark_generating / complete / fail（report_date upsert，
    report_date = 北京自然日 = 美股交易日 T + 1，与 close_brief 口径一致）
  - 孤儿回收：超过 4 小时仍 generating 的行重置 failed（管线被杀/重启后自愈）
  - 查询：latest / by_date / list / html_by_token（DB html_content 优先，
    OSS oss_object_name 兜底 —— 与 twitter_daily_report 同模式）

HTML 双存：html_content（DB，服务兜底）+ oss_object_name（OSS，键
reports/MorningBrief/report_YYYYMMDD.html 确定性命名，重跑覆盖同键幂等）。
"""

from __future__ import annotations

import os
import time
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4

from backend.database.db_manager import DatabaseManager
from backend.utils.database_config import get_database_config
from backend.utils.datetime_utils import utcnow, window_label_date
from backend.utils.logger import get_logger

logger = get_logger("morning_brief_report_service")

# report_date 口径与 close_brief / DMR 一致：北京自然日（生成日）
DEFAULT_REPORT_TIMEZONE = "Asia/Shanghai"

# 管线最长 ~160min（就绪门截 09:40 + run_pipeline 60min），留一倍余量
ORPHAN_THRESHOLD_MINUTES = 240

_COLS = ("id, report_date, status, file_path, oss_object_name, "
         "metadata, error_message, share_token, created_at, updated_at")

# ── 实时行情覆盖（报告打开时刷新 FT 表，/shared/{token}/quotes 调用）──────
# 数据链：EODHD 批量实时（一次最多 ~20 个代码）→ 闭市/缺行回退
# market_data_eod 最近两根收盘 → YAHOO: 前缀代码走 Yahoo chart（仅 DXY）。
# 注意：EODHD 实时对闭市代码返回 "NA"，且其 previousClose 是前一交易日值
# （实测 SSEC 闭市后 previousClose=前日收盘，而 EOD 表已有当日收盘），
# 所以降级必须查自己的 EOD 表，不能拿 previousClose 当最新价。
# 60s 进程级共享缓存：匿名分享链接可能有大量同时打开，无论多少观众，
# 上游每分钟最多 3 次调用（35 个 EODHD 代码 ÷ 每批 12 + 1 次 Yahoo）。
_LIVE_TTL_SEC = 60
_LIVE_BATCH = 12
_live_cache: Dict[Tuple[str, ...], Tuple[float, Dict[str, Any]]] = {}

# 涨跌榜按需 K 线(点击行才拉,600s 进程缓存挡住重复点击/多观众)
_KLINE_TTL_SEC = 600
_kline_cache: Dict[str, Tuple[float, Optional[Dict[str, Any]]]] = {}


class MorningBriefReportService:
    """Morning Brief 报告状态登记与查询（全局单例，每天一份）。"""

    def __init__(self):
        self.db_manager = DatabaseManager(get_database_config())

    # ── 状态机（供 celery 任务 / register_morning_brief.py 调用）─────────

    def mark_generating(self, report_date: date) -> int:
        """创建/重置当日行为 generating，返回行 id。"""
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                INSERT INTO morning_brief_reports (report_date, status)
                VALUES (%s, 'generating')
                ON CONFLICT (report_date) DO UPDATE
                    SET status = 'generating',
                        error_message = NULL,
                        updated_at = NOW()
                RETURNING id
                """,
                (report_date,),
            )
            conn.commit()
            return cursor.fetchone()[0]
        finally:
            cursor.close()

    def complete(
        self,
        report_date: date,
        *,
        file_path: str,
        oss_object_name: Optional[str],
        html_content: str,
        metadata: Dict[str, Any],
    ) -> Dict[str, Any]:
        """登记完成态。html 双存（DB + OSS 键名），每次换新 share_token。"""
        share_token = uuid4().hex
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            from psycopg2.extras import Json

            cursor.execute(
                """
                UPDATE morning_brief_reports
                SET status = 'completed',
                    file_path = %s,
                    oss_object_name = %s,
                    html_content = %s,
                    metadata = %s,
                    share_token = %s,
                    error_message = NULL,
                    updated_at = NOW()
                WHERE report_date = %s
                """,
                (
                    file_path,
                    oss_object_name,
                    html_content,
                    Json(metadata) if metadata is not None else None,
                    share_token,
                    report_date,
                ),
            )
            if cursor.rowcount == 0:  # mark_generating 未先行(手动补挂场景)
                cursor.execute(
                    """
                    INSERT INTO morning_brief_reports (
                        report_date, status, file_path, oss_object_name,
                        html_content, metadata, share_token
                    ) VALUES (%s, 'completed', %s, %s, %s, %s, %s)
                    """,
                    (
                        report_date, file_path, oss_object_name,
                        html_content,
                        Json(metadata) if metadata is not None else None,
                        share_token,
                    ),
                )
            conn.commit()
            logger.info(
                f"Morning Brief registered: date={report_date}, "
                f"oss={'yes' if oss_object_name else 'no'}, "
                f"html={len(html_content or '')} chars"
            )
            return {
                "success": True,
                "report_date": str(report_date),
                "share_token": share_token,
                "oss_object_name": oss_object_name,
            }
        finally:
            cursor.close()

    def fail(self, report_date: date, error_message: str) -> bool:
        """登记失败态（管线 rc!=0 或注册器异常时调用）。"""
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                INSERT INTO morning_brief_reports (report_date, status, error_message)
                VALUES (%s, 'failed', %s)
                ON CONFLICT (report_date) DO UPDATE
                    SET status = 'failed',
                        error_message = EXCLUDED.error_message,
                        updated_at = NOW()
                """,
                (report_date, error_message[:2000]),
            )
            conn.commit()
            return True
        finally:
            cursor.close()

    def reset_orphan_generating(
        self, threshold_minutes: int = ORPHAN_THRESHOLD_MINUTES
    ) -> int:
        """超时 generating 行重置 failed（管线被杀/宕机后下一次派发时自愈）。"""
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                UPDATE morning_brief_reports
                SET status = 'failed',
                    error_message = 'orphan generating reset (pipeline died?)',
                    updated_at = NOW()
                WHERE status = 'generating'
                  AND updated_at < NOW() - (%s * INTERVAL '1 minute')
                """,
                (threshold_minutes,),
            )
            conn.commit()
            if cursor.rowcount:
                logger.warning(
                    f"Morning Brief 孤儿回收: {cursor.rowcount} 行 "
                    f"generating>{threshold_minutes}min → failed"
                )
            return cursor.rowcount
        finally:
            cursor.close()

    # ── 查询（供 API 层调用；列表不含 html 正文）────────────────────────

    def get_report_by_date(self, report_date: date) -> Optional[Dict[str, Any]]:
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                f"SELECT {_COLS} FROM morning_brief_reports "
                "WHERE report_date = %s",
                (report_date,),
            )
            row = cursor.fetchone()
            return self._row_to_dict(row) if row else None
        finally:
            conn.close()

    def get_latest_report(self) -> Optional[Dict[str, Any]]:
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                f"SELECT {_COLS} FROM morning_brief_reports "
                "WHERE status = 'completed' "
                "ORDER BY report_date DESC LIMIT 1"
            )
            row = cursor.fetchone()
            return self._row_to_dict(row) if row else None
        finally:
            conn.close()

    def list_reports(self, limit: int = 20) -> List[Dict[str, Any]]:
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                f"SELECT {_COLS} FROM morning_brief_reports "
                "ORDER BY report_date DESC LIMIT %s",
                (limit,),
            )
            return [self._row_to_dict(r) for r in cursor.fetchall()]
        finally:
            conn.close()

    def get_user_reports(
        self, limit: int = 30, offset: int = 0
    ) -> Tuple[List[Dict[str, Any]], int]:
        """分页获取报告列表（Agent API 统一口径；全局单例,无 user_id）。"""
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT COUNT(*) FROM morning_brief_reports")
            total = cursor.fetchone()[0]
            cursor.execute(
                f"SELECT {_COLS} FROM morning_brief_reports "
                "ORDER BY report_date DESC LIMIT %s OFFSET %s",
                (limit, offset),
            )
            return [self._row_to_dict(r) for r in cursor.fetchall()], total
        finally:
            conn.close()

    def get_report_html_by_token(self, token: str) -> Optional[bytes]:
        """DB html_content 优先，OSS 兜底（twitter_daily 同模式）。"""
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT oss_object_name, html_content
                FROM morning_brief_reports
                WHERE share_token = %s AND status = 'completed'
                LIMIT 1
                """,
                (token,),
            )
            row = cursor.fetchone()
            if not row:
                return None
            obj_name, html = row
            if html:
                return html.encode("utf-8")
            if obj_name:
                from backend.services.oss_service import OSSService

                return OSSService().get_object_bytes(obj_name)
            return None
        finally:
            conn.close()

    # ── 实时行情覆盖（供 API /shared/{token}/quotes 调用）────────────────

    def get_live_quotes_by_token(self, token: str) -> Optional[Dict[str, Any]]:
        """share_token → 该报告 FT 表的实时行情。

        返回 None = token 无效或报告未带实时规格（旧报告，前端保留快照）。
        rows[]: m1d = 最新价 vs 该资产昨收（盘中即今日涨跌幅，收盘后即当日
        最终涨跌）；m1w/m1m/ytd 按生成时预埋的参考价（metadata.ft_live）
        重算；None = 无法刷新，前端保留快照值。unit='bp' 的利率行按价差
        ×100 出 bp（不用 EODHD change_p —— 那是收益率水平的百分比变化）。
        """
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                "SELECT metadata FROM morning_brief_reports "
                "WHERE share_token = %s AND status = 'completed' LIMIT 1",
                (token,),
            )
            row = cursor.fetchone()
        finally:
            cursor.close()
            conn.close()
        if not row:
            return None
        spec = ((row[0] or {}).get("ft_live")) or {}
        rows_spec: List[Dict[str, Any]] = [r for r in (spec.get("rows") or [])
                                           if r.get("c")]
        if not rows_spec:
            return None

        key = tuple(sorted({r["c"] for r in rows_spec}))
        now = time.monotonic()
        cached = _live_cache.get(key)
        if cached and now - cached[0] < _LIVE_TTL_SEC:
            return cached[1]

        quotes = self._fetch_live_quotes(key)
        payload = self._assemble_live_rows(spec, rows_spec, quotes)
        _live_cache[key] = (now, payload)
        return payload

    @staticmethod
    def _fetch_live_quotes(codes: Tuple[str, ...]) -> Dict[str, Dict[str, Any]]:
        """实时快照：code → {last, prev, ts?|d?, src}。

        优先级：EODHD 批量实时 → Yahoo chart（YAHOO: 前缀，仅 DXY）→
        market_data_eod 最近两根收盘（闭市代码降级；EOD 每日 4 次由
        market_data 任务刷新，含 EODHD 当日盘中行，通常比 08:00 快照新）。
        """
        import requests

        out: Dict[str, Dict[str, Any]] = {}
        eodhd = [c for c in codes if not c.startswith("YAHOO:")]
        api_token = os.getenv("EODHD_API_TOKEN")
        if eodhd and api_token:
            for i in range(0, len(eodhd), _LIVE_BATCH):
                chunk = eodhd[i:i + _LIVE_BATCH]
                try:
                    r = requests.get(
                        f"https://eodhd.com/api/real-time/{chunk[0]}",
                        params={"s": ",".join(chunk[1:]),
                                "api_token": api_token, "fmt": "json"},
                        timeout=8)
                    data = r.json()
                    for q in data if isinstance(data, list) else [data]:
                        if not isinstance(q, dict):
                            continue
                        close, prev = q.get("close"), q.get("previousClose")
                        if not isinstance(close, (int, float)):
                            continue  # "NA" = 闭市，走 EOD 降级
                        ts = q.get("timestamp")
                        out[q.get("code")] = {
                            "last": float(close),
                            "prev": float(prev)
                            if isinstance(prev, (int, float)) else None,
                            "ts": int(ts)
                            if isinstance(ts, (int, float)) else None,
                            "src": "live",
                        }
                except Exception as exc:  # noqa: BLE001 — 单批失败降级 EOD
                    logger.warning(f"MB live EODHD batch failed: {exc}")
        for c in [c for c in codes if c.startswith("YAHOO:")]:
            try:
                r = requests.get(
                    f"https://query1.finance.yahoo.com/v8/finance/chart/{c.split(':', 1)[1]}",
                    params={"range": "5d", "interval": "1d"},
                    headers={"User-Agent": "Mozilla/5.0"}, timeout=8)
                res = ((r.json().get("chart") or {}).get("result") or [None])[0]
                meta = (res or {}).get("meta") or {}
                q = (((res or {}).get("indicators") or {}).get("quote") or [{}])[0]
                closes = [x for x in (q.get("close") or []) if x is not None]
                last = meta.get("regularMarketPrice") or (
                    closes[-1] if closes else None)
                if last is None:
                    continue
                rmt = meta.get("regularMarketTime")
                out[c] = {
                    "last": float(last),
                    "prev": float(closes[-2]) if len(closes) >= 2 else None,
                    "ts": int(rmt) if isinstance(rmt, (int, float)) else None,
                    "src": "live",
                }
            except Exception as exc:  # noqa: BLE001 — Yahoo 失败该行保留快照
                logger.warning(f"MB live Yahoo {c} failed: {exc}")
        missing = [c for c in eodhd if c not in out]
        if missing:
            conn = DatabaseManager(get_database_config()).get_connection()
            cursor = conn.cursor()
            try:
                cursor.execute(
                    "SELECT code, date, close FROM market_data_eod "
                    "WHERE code = ANY(%s) AND close IS NOT NULL "
                    "AND date >= CURRENT_DATE - INTERVAL '20 days' "
                    "ORDER BY code, date DESC", (missing,))
                by: Dict[str, List[Tuple[date, float]]] = {}
                for code, d, close in cursor.fetchall():
                    by.setdefault(code, []).append((d, float(close)))
            finally:
                cursor.close()
                conn.close()
            for code, s in by.items():
                if len(s) >= 2:
                    out[code] = {"last": s[0][1], "prev": s[1][1],
                                 "d": s[0][0].isoformat(), "src": "eod"}
        return out

    @staticmethod
    def _assemble_live_rows(spec: Dict[str, Any],
                            rows_spec: List[Dict[str, Any]],
                            quotes: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        """按行的 unit/参考价把实时快照拼成前端可直接填数的 rows。"""
        out_rows: List[Dict[str, Any]] = []
        for r in rows_spec:
            q = quotes.get(r["c"])
            refs = r.get("r") or {}
            if not q:
                out_rows.append({"n": r.get("n"), "last": None, "m1d": None,
                                 "m1w": None, "m1m": None, "ytd": None})
                continue
            last, prev, unit = q["last"], q.get("prev"), (r.get("u") or "%")

            def chg(ref: Optional[float]) -> Optional[float]:
                if ref in (None, 0) or last is None:
                    return None
                return ((last / ref - 1) * 100 if unit != "bp"
                        else (last - ref) * 100)

            out_rows.append({
                "n": r.get("n"),
                "last": round(last, 6),
                "m1d": chg(prev),
                "m1w": chg(refs.get("w1")),
                "m1m": chg(refs.get("m1")),
                "ytd": chg(refs.get("ytd")),
            })
        srcs = {q.get("src") for q in quotes.values() if q}
        src = ("live" if srcs == {"live"}
               else "eod" if srcs == {"eod"} else "mixed") if srcs else "eod"
        ts_list = [q["ts"] for q in quotes.values() if q and q.get("ts")]
        d_list = [q["d"] for q in quotes.values() if q and q.get("d")]
        return {
            "t": spec.get("t"),
            "src": src,
            "ts": max(ts_list) if ts_list else None,  # 实时行最大成交时间(epoch s)
            "d": max(d_list) if d_list else None,     # 降级行最大数据日期(iso)
            "rows": out_rows,
        }

    def get_rank_kline_by_token(self, token: str, market: str,
                                idx: int) -> Optional[Dict[str, Any]]:
        """涨跌榜单只个股日 K(Yahoo,≤260 根 OHLC 升序),点击行时按需拉取。

        symbol 由 metadata.rank_kline 规格按 (market, 行序) 决定 —— 客户端
        只传索引不能传符号,不构成任意符号代理。返回 None = token 无效/规格
        缺失/该行无符号/上游失败,前端弹窗显示"走势数据暂不可用"。
        """
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                "SELECT metadata FROM morning_brief_reports "
                "WHERE share_token = %s AND status = 'completed' LIMIT 1",
                (token,),
            )
            row = cursor.fetchone()
        finally:
            cursor.close()
            conn.close()
        if not row:
            return None
        spec = ((row[0] or {}).get("rank_kline") or {}).get(market)
        if not isinstance(spec, list) or not (0 <= idx < len(spec)):
            return None
        sym = spec[idx]
        if not sym:
            return None
        now = time.monotonic()
        cached = _kline_cache.get(sym)
        if cached and now - cached[0] < _KLINE_TTL_SEC:
            return cached[1]
        data = self._yahoo_daily_ohlc(sym)
        _kline_cache[sym] = (now, data)
        return data

    @staticmethod
    def _yahoo_daily_ohlc(sym: str) -> Optional[Dict[str, Any]]:
        """Yahoo chart 1 年日线 → {h: [[iso,o,h,l,c]…]升序≤260, a: 最新日}。"""
        import requests

        try:
            r = requests.get(
                f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}",
                params={"range": "1y", "interval": "1d"},
                headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
            res = ((r.json().get("chart") or {}).get("result") or [None])[0]
            ts = (res or {}).get("timestamp") or []
            q = (((res or {}).get("indicators") or {}).get("quote") or [{}])[0]
            o, h = q.get("open") or [], q.get("high") or []
            lo, c = q.get("low") or [], q.get("close") or []
            bars: List[List[Any]] = []
            for i in range(min(len(ts), len(o), len(h), len(lo), len(c))):
                if None in (o[i], h[i], lo[i], c[i]):
                    continue
                # 服务器时区为北京:US/HK/CN 日线 bar 时间戳换算后恰为交易日历日
                d = datetime.fromtimestamp(ts[i]).date().isoformat()
                bars.append([d, round(float(o[i]), 4), round(float(h[i]), 4),
                             round(float(lo[i]), 4), round(float(c[i]), 4)])
            if not bars:
                return None
            return {"h": bars[-260:], "a": bars[-1][0]}
        except Exception as exc:  # noqa: BLE001
            logger.warning(f"MB kline Yahoo {sym} failed: {exc}")
            return None

    @staticmethod
    def get_share_url(oss_object_name: Optional[str],
                      expires: int = 86400) -> Optional[str]:
        """OSS 签名分享链接（24h）；未上传/失败返回 None。"""
        if not oss_object_name:
            return None
        try:
            from backend.services.oss_service import OSSService

            return OSSService().get_signed_url(oss_object_name, expires=expires)
        except Exception as exc:  # noqa: BLE001 — 链接失败不阻断元数据返回
            logger.warning(f"Morning Brief signed url 失败: {exc}")
            return None

    @staticmethod
    def _row_to_dict(row: tuple) -> Dict[str, Any]:
        (rid, report_date, status, file_path, oss_object_name,
         metadata, error_message, share_token, created_at, updated_at) = row
        return {
            "id": rid,
            "report_date": str(report_date),
            "status": status,
            "file_path": file_path,
            "oss_object_name": oss_object_name,
            "metadata": metadata or {},
            "error_message": error_message,
            "share_token": share_token,
            "created_at": created_at.isoformat() if created_at else None,
            "updated_at": updated_at.isoformat() if updated_at else None,
        }
