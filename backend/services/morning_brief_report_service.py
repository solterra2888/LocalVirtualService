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

import calendar
import hashlib
import json
import os
import re
import time
from datetime import date, datetime, timedelta, timezone
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

# 涨跌榜按需 K 线(点击行才拉,600s 进程缓存挡住重复点击/多观众)。键含截止日:
# 同一 symbol 在不同报告里的「当日」不同,不能共用一份缓存
_KLINE_TTL_SEC = 600
_kline_cache: Dict[Tuple[str, Optional[str]], Tuple[float, Optional[Dict[str, Any]]]] = {}


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
        """登记完成态。html 双存（DB + OSS 键名）。

        share_token 仅 INSERT 生成；同日重注册（ft_refresh 补数重发布等）保持
        原 token —— 换新会打断早间已分发的分享链接（2026-11-20 改）。"""
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
                    share_token = COALESCE(share_token, %s),
                    error_message = NULL,
                    updated_at = NOW()
                WHERE report_date = %s
                RETURNING share_token
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
            row = cursor.fetchone()
            if row:
                # 既有 token 沿用(同日重注册不打断已分发链接);NULL(mark_generating
                # 建行不带 token,当日首次完成)则取 COALESCE 填入的新值
                share_token = row[0]
            else:  # mark_generating 未先行(手动补挂场景)
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
        # 涨跌榜实时刷新规格(v2,2026-11-23):{市场: {t: 当日 D, rows: [{c, r}|None]}}
        # 行序与 rank_kline 一致;与 FT 共享一次行情拉取(批量,60s 进程缓存)。
        # v1 旧格式不识别 → 不下发 rank,该报告的榜单保持冻结
        meta = row[0] or {}
        rank_spec = self._rank_live_spec(meta)
        rank_codes = sorted({e["c"] for blk in rank_spec.values()
                             for e in blk["rows"] if e and e.get("c")})

        key = tuple(sorted({r["c"] for r in rows_spec} | set(rank_codes)))
        now = time.monotonic()
        cached = _live_cache.get(key)
        if cached and now - cached[0] < _LIVE_TTL_SEC:
            return cached[1]

        quotes = self._fetch_live_quotes(key)
        payload = self._assemble_live_rows(spec, rows_spec, quotes)
        if rank_spec:
            payload["rank"] = self._assemble_rank_live(
                rank_spec, quotes, self._rank_version(meta.get("rank_kline")))
        _live_cache[key] = (now, payload)
        return payload

    @staticmethod
    def _rank_live_spec(meta: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        """涨跌榜实时规格(v2):{市场: {t: 当日 D, rows: [{c, r}|None]}}。

        v1(顶层 "t",市场键直接是行列表,美股也在内)不识别 → 空 dict。v1 的
        两个问题(2026-11-23 实测 report_date=11-21 报告):美股闭市时拿 EODHD
        realtime 的 previousClose 当昨收,140 行里 72 行当日被改写、21 行刷成
        0.00%;D+1 的港股/A股行情覆盖了报告日的值。
        """
        raw = meta.get("rank_live") or {}
        if not isinstance(raw, dict) or raw.get("v") != 2:
            return {}
        out: Dict[str, Dict[str, Any]] = {}
        for mkt, blk in raw.items():
            if isinstance(blk, dict) and isinstance(blk.get("rows"), list):
                out[mkt] = blk
        return out

    @staticmethod
    def _rank_version(rank_kline: Any) -> Optional[str]:
        """榜单行序指纹:与 md2html_report 注入页面的 window.__RANKV__ 同算法
        (各市场 Yahoo 符号按行序)。报告重渲染后行序可能变化(收盘补数按新
        成交额重排),已打开的旧页面据此忽略实时补丁,不把数字贴到别的股票上。"""
        if not isinstance(rank_kline, dict) or not rank_kline:
            return None
        dig = json.dumps(rank_kline, sort_keys=True,
                         separators=(",", ":")).encode("utf-8")
        return hashlib.sha1(dig).hexdigest()[:12]

    @staticmethod
    def _quote_date(q: Dict[str, Any]) -> Optional[str]:
        """行情所属日期(ISO):实时行取成交时间戳的 UTC 日,降级行取入库日。"""
        ts = q.get("ts")
        if isinstance(ts, (int, float)) and ts > 0:
            return datetime.fromtimestamp(ts, tz=timezone.utc).date().isoformat()
        d = q.get("d")
        return str(d)[:10] if d else None

    @staticmethod
    def _assemble_rank_live(rank_spec: Dict[str, Dict[str, Any]],
                            quotes: Dict[str, Dict[str, Any]],
                            version: Optional[str] = None) -> Dict[str, Any]:
        """涨跌榜实时行:按行序输出 {市场: [{p,g,w,m,y,k}|None]},恒 '%' 口径。

        「当日」= 报告日 D(规格里的 t)。只接受行情日期 == D 的报价:生成前的
        上一场与 D+1 起的行情一律不下发(该行 None,前端保留冻结值)。昨收用
        规格预埋的 pv(D-1 收盘),不用 realtime 的 previousClose(闭市时不可靠)。
        5日/1月按预埋迷你序列以 D 为锚现算(与 FT 共用 _window_changes;
        fb=None → pv 恒作昨收)。k=1 表示冻结序列已含 D 日点(收盘补数后),
        前端替换走势图末点而非追加。
        """
        out: Dict[str, Any] = {}
        for mkt, blk in rank_spec.items():
            if not isinstance(blk, dict):
                continue
            target = str(blk.get("t") or "")[:10]
            rows_out: List[Optional[Dict[str, Any]]] = []
            for e in blk.get("rows") or []:
                if not target or not isinstance(e, dict):
                    rows_out.append(None)
                    continue
                q = quotes.get(e.get("c") or "")
                if not q or MorningBriefReportService._quote_date(q) != target:
                    rows_out.append(None)
                    continue
                refs = dict(e.get("r") or {})
                wc = MorningBriefReportService._window_changes(refs, q, "%", None)
                hist = refs.get("s")
                k = 1 if (isinstance(hist, list) and hist
                          and str(hist[-1][0])[:10] == target) else 0
                rows_out.append({"p": round(wc["last"], 4), "g": wc["m1d"],
                                 "w": wc["m1w"], "m": wc["m1m"], "y": wc["ytd"],
                                 "k": k})
            out[mkt] = rows_out
        if version:
            out["v"] = version
        return out

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
            # 2026-11-13:涨跌榜实时行并入后代码数 ~320(FT 35 + rank ~282),
            # chunk 12→40 —— 每次刷新约 8 次批量调用,60s 进程缓存内所有观众共享
            for i in range(0, len(eodhd), 40):
                chunk = eodhd[i:i + 40]
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
                        _o = q.get("open") if isinstance(q.get("open"), (int, float)) else None
                        _h = q.get("high") if isinstance(q.get("high"), (int, float)) else None
                        _l = q.get("low") if isinstance(q.get("low"), (int, float)) else None
                        out[q.get("code")] = {
                            "last": float(close),
                            "prev": float(prev)
                            if isinstance(prev, (int, float)) else None,
                            "o": _o, "h": _h, "l": _l,
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
                    # 1mo(非 5d):Yahoo 对部分沪市指数(000016/000300.SS)5d 图
                    # 偶发只回今日 1 根 bar,closes[-2] 取不到昨收
                    params={"range": "1mo", "interval": "1d"},
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
                # 当日 OHLC(日线末根;收盘后即终值,盘中即实时值)供 FT 弹窗补当日蜡烛
                qo = (q.get("open") or [])
                qh = (q.get("high") or [])
                ql = (q.get("low") or [])
                _o = qo[-1] if qo and isinstance(qo[-1], (int, float)) else None
                _h = qh[-1] if qh and isinstance(qh[-1], (int, float)) else None
                _l = ql[-1] if ql and isinstance(ql[-1], (int, float)) else None
                out[c] = {
                    "last": float(last),
                    "prev": float(closes[-2]) if len(closes) >= 2 else None,
                    "o": _o, "h": _h, "l": _l,
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
    def _window_changes(refs: Dict[str, Any], q: Dict[str, Any], unit: str,
                        fallback_anchor: Optional[date]) -> Dict[str, Any]:
        """按行情实际日期现算 5日/1月/YTD(锚点随最新收盘平移,对齐 Yahoo)。

        refs.s = 生成时预埋的迷你收盘序列(升序 [[iso, close]×≤45]);
        无序列的旧 spec 回退 refs.w1/m1 标量(T 锚)。unit='bp' 行按价差
        ×100 出 bp(不用百分比)。m1d 恒 = 最新价/昨收;refs.pv(=T 收盘)
        在锚定日 > T 时优先作昨收(见函数体内注释)。
        """
        last, prev = q["last"], q.get("prev")

        def chg(ref):
            if ref in (None, 0) or last is None:
                return None
            return (round((last / ref - 1) * 100, 2) if unit != "bp"
                    else round((last - ref) * 100, 2))

        anchor = fallback_anchor or date.today()
        ts = q.get("ts")
        if isinstance(ts, (int, float)) and ts > 0:
            anchor = datetime.fromtimestamp(ts, tz=timezone.utc).date()
        else:
            d = q.get("d")
            if d:
                try:
                    anchor = date.fromisoformat(str(d)[:10])
                except ValueError:
                    pass
        if fallback_anchor and anchor < fallback_anchor:
            anchor = fallback_anchor
        # pv 锚(2026-11-20):refs.pv = 冻结时的 T 收盘。报价锚定日已过 T 时
        # "当日"的法定昨收就是 T 收盘 —— EODHD realtime 的 previousClose
        # 早晨不滚动一天(加密/FX 实测会错算成两日涨跌),spec 带 pv 一律优先;
        # anchor == T 的 DB 降级行(最新收盘仍是 T)不用 pv,否则恒为 0%。
        pv = refs.get("pv")
        if pv not in (None, 0) and (not fallback_anchor or anchor > fallback_anchor):
            prev = pv

        w_ref = m_ref = prev_ref = None
        hist = refs.get("s")
        if isinstance(hist, list) and hist:
            try:
                y, m = (anchor.year, anchor.month - 1) if anchor.month > 1 \
                    else (anchor.year - 1, 12)
                mb = date(y, m, min(anchor.day, calendar.monthrange(y, m)[1]))
                bars = [(date.fromisoformat(str(x[0])[:10]), float(x[1]))
                        for x in hist if x and len(x) == 2]
                before = [b for b in bars if b[0] < anchor]
                if len(before) >= 5:
                    w_ref = before[-5][1]
                if before:
                    prev_ref = before[-1][1]     # 昨收兜底(行情源无 prev 时)
                m_ref = next((c for d, c in reversed(bars) if d <= mb), None)
            except (ValueError, TypeError, IndexError):
                pass
        if w_ref is None:
            w_ref = refs.get("w1")
        if m_ref is None:
            m_ref = refs.get("m1")
        return {"last": last, "m1d": chg(prev if prev is not None else prev_ref),
                "m1w": chg(w_ref), "m1m": chg(m_ref), "ytd": chg(refs.get("ytd")),
                "dt": anchor.isoformat()}

    def _assemble_live_rows(self, spec: Dict[str, Any],
                            rows_spec: List[Dict[str, Any]],
                            quotes: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        """按行的 unit/参考价把实时快照拼成前端可直接填数的 rows。"""
        snap_t = str(spec.get("t") or "")[:10]
        fb = (date.fromisoformat(snap_t)
              if re.match(r"\d{4}-\d{2}-\d{2}$", snap_t) else None)
        out_rows: List[Dict[str, Any]] = []
        for r in rows_spec:
            q = quotes.get(r["c"])
            if not q:
                out_rows.append({"n": r.get("n"), "last": None, "m1d": None,
                                 "m1w": None, "m1m": None, "ytd": None})
                continue
            wc = self._window_changes(r.get("r") or {}, q, r.get("u") or "%", fb)
            out_rows.append({"n": r.get("n"),
                             "o": q.get("o"), "h": q.get("h"), "l": q.get("l"),
                             **{k: (round(v, 6) if k == "last" else v)
                                for k, v in wc.items()}})
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
        meta = row[0] or {}
        spec = (meta.get("rank_kline") or {}).get(market)
        if not isinstance(spec, list) or not (0 <= idx < len(spec)):
            return None
        sym = spec[idx]
        if not sym:
            return None
        # 截止日(2026-11-23):K 线截至该市场的「当日」= 报告日 D(北京)—— 美股取
        # 美股交易日 T(北京 D 日凌晨收盘),港股/A股取 D。没有 rank_cut 的旧报告
        # 按美股交易日截止。此前不设截止、每次点击实时拉最新,旧报告里图比行
        # 多出后面几个交易日(行是上一天的、图是最新的一场)。
        cut = ((meta.get("rank_cut") or {}).get(market)
               or str(meta.get("session_date") or "")[:10] or None)
        now = time.monotonic()
        cached = _kline_cache.get((sym, cut))
        if cached and now - cached[0] < _KLINE_TTL_SEC:
            return cached[1]
        data = self._yahoo_daily_ohlc(sym, cut)
        _kline_cache[(sym, cut)] = (now, data)
        return data

    @staticmethod
    def _yahoo_daily_ohlc(sym: str,
                          until: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Yahoo chart 1 年日线 → {h: [[iso,o,h,l,c]…]升序≤260, a: 最新日}。

        until(ISO 日)非空时丢弃晚于它的 bar:弹窗走势与榜单行止于同一交易日。
        """
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
                bo, bh = float(o[i]), float(h[i])
                bl, bc = float(lo[i]), float(c[i])
                # 自洽校验(2026-11-18):Yahoo 当日未收盘 bar 的 open 会回填成
                # 数日前旧值(300308/300502 同日均拿到 11-13 开盘价),出现
                # open>high 的物理不可能蜡烛;坏 bar 整根丢弃,弹窗回退到
                # 最后一根有效 bar(历史收盘 bar 实测与交易所口径精确一致)
                if (min(bo, bh, bl, bc) <= 0
                        or bh < max(bo, bc) - 1e-6 or bl > min(bo, bc) + 1e-6):
                    logger.warning(
                        f"MB kline Yahoo {sym}: drop inconsistent bar "
                        f"ts={ts[i]} o={bo} h={bh} l={bl} c={bc}")
                    continue
                # 服务器时区为北京:US/HK/CN 日线 bar 时间戳换算后恰为交易日历日
                d = datetime.fromtimestamp(ts[i]).date().isoformat()
                if until and d > until:
                    continue
                bars.append([d, round(bo, 4), round(bh, 4),
                             round(bl, 4), round(bc, 4)])
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
