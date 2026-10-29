#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Stage 0 — 为 CNT 风格 Daily Market Brief 管线构建冻结数据快照。

从仓库现有数据源拉取当日全部素材,写入 reports/pipeline/{date}/snapshot.json:
  1. the_close_summaries          — Bloomberg The Close 八段式 PM Brief (LLM 已生成);
                                     去掉开头客套前言;Cross-Asset / 分歧 / 解决框架 /
                                     时间维度 / PM Takeaway 聚合为 positioning_material
  2. cnbc_closing_bell_summaries  — CNBC Closing Bell 摘要 (LLM 已生成),≤T 最新一份
  3. bloomberg_surveillance       — 按节目标题里的美东播出日期匹配 T
                                     (标题无日期时回退北京日 T+1 那一行)
  4. economic_events              — 近 3 天实际值 + 未来 7 天日历(读库,impact≥2,
                                     仅 US/CN/JP/DE/KR);保留 comparison(mom/yoy/水平);
                                     time 是 UTC,另写 time_et / date_et(美东,含夏令时)。
                                     构建前对 [T-2, T] 做一次 actual 回填:先
                                     format_event_data + calculate_event_impact
                                     (与 scheduler 同步链路一致),再 COALESCE upsert。
                                     这一步写生产库。--no-backfill 跳过
  5. earnings_calendar            — get_earnings_calendar_view:美股普通股、按
                                     (code, report_date) 去重。前一交易日盘后 + 当日
                                     ≥$100 亿(movers 催化剂);未来 5 天 ≥$500 亿
                                     (Forward Catalysts)。字段 code/name/session/
                                     eps_estimate/eps_actual/eps_surprise_pct
  6. market                       — 两块独立容错,一块失败不丢掉另一块:
                                     cross_asset(market_data_eod,≤T 两根日线,可回溯);
                                     finviz(整表刷新的当前状态,snapshot_date ≤ T+1,
                                     历史日期无快照)。S&P 500 全成分 + 板块均值 + 广度;
                                     Top Movers 限市值 ≥$20 亿(market_cap 单位:百万美元)
  7. macro_pillars                — MACROS 确定性底座(fetch_macro_pillars.py)
  8. prior_odds                   — 优先用前一交易日 ZQ 收盘自算的加息概率
                                     (next_fomc_odds.prior);没有时回退最近一期更早快照

转录附件写到同目录 transcripts/{the_close,cnbc_closing_bell,surveillance}.txt,
不进 JSON。每次先删旧文件(取数失败不得残留上一次的文本);远端断连重试 3 次;
超限时头 60% + 尾 40%(Surveillance 上限 22 万字符,避免砍掉 8:30 ET 后的讨论)。

设计约束: 快照一经生成本轮不可变 —— 后续章节 Agent 与核验 Agent 只看这一份
冻结数据,数字对账才可能成立。任何一个数据源失败只记录 errors,不阻塞整体
(章节 Agent 会在 prompt 约束下对缺失源降级处理)。

同日锚定: 行情/日历/曲线锚定交易日 T;The Close / Closing Bell 取 ≤T 最新一份
并披露 lag_days,不再静默混入跨交易日口径。

用法:
    python tools/report-pipeline/build_snapshot.py \
        [--date 2026-10-28] [--out PATH] [--no-backfill]

    不传 --date 时自动取 the_close_summaries 中最新 completed 的 report_date。
    Finviz 无法回溯,测历史日期时 market.finviz 会记入 errors,其余源照常。
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from backend.database.db_manager import DatabaseManager  # noqa: E402
from backend.utils.database_config import get_database_config  # noqa: E402
from backend.utils.logger import get_logger  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parent))
from fetch_macro_pillars import build_macro_pillars  # noqa: E402

logger = get_logger("report_pipeline_snapshot")

# 跨资产面板 — 复用 daily_market_report 的 Market Temperature 选型
CROSS_ASSET_CODES = {
    "GSPC.INDX": "S&P 500",
    "IXIC.INDX": "Nasdaq Composite",
    "NDX.INDX": "Nasdaq 100",
    "SOX.INDX": "PHLX Semiconductor",
    "RUT.INDX": "Russell 2000",
    "DJI.INDX": "Dow Jones",
    "VIX.INDX": "VIX",
    "US2Y.INDX": "US 2Y Yield",
    "US10Y.INDX": "US 10Y Yield",
    "US30Y.INDX": "US 30Y Yield",
    "GC1.COMMODITY": "Gold",
    "CL1.COMMODITY": "WTI Crude",
    "BZ1.COMMODITY": "Brent Crude",
    "HG1.COMMODITY": "Copper",
    "USDJPY.FOREX": "USD/JPY",
    "EURUSD.FOREX": "EUR/USD",
}

RECENT_EVENT_DAYS = 3
UPCOMING_EVENT_DAYS = 7
# 经济日历只收录五大经济体(用户口径):控制快照体积与 macro/核验耗时
ECON_COUNTRIES = ("US", "CN", "JP", "DE", "KR")
TOP_MOVERS_LIMIT = 30
TOP_MOVERS_MIN_CAP_M = 2000  # finviz market_cap 单位:百万美元 → $20 亿


def _conn() -> Any:
    db = DatabaseManager(get_database_config())
    return db.get_connection()


def _strip_preamble(md: Optional[str]) -> Optional[str]:
    """去掉摘要开头的 LLM 客套前言(如 "这是一份基于…转录生成的结构化 PM Brief…"):
    正文不以标题开头且 500 字内出现首个 Markdown 标题时,从该标题起算。"""
    if not md or md.lstrip().startswith("#"):
        return md
    idx = md.find("\n#")
    if 0 <= idx < 500:
        return md[idx + 1:]
    return md


def _latest_completed_summary(table: str, on_or_before: date,
                              report_date: date) -> Optional[Dict[str, Any]]:
    """取指定日期(含)之前最新一份 completed 摘要;找不到返回 None。

    同日锚定纪律: 摘要 report_date 落后报告日时计算 lag_days 随快照冻结,
    由口径块显式披露(如 "The Close 摘要✓2026-10-24(滞后2天)"),
    不再静默当作同日口径使用。
    """
    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT report_date, video_title, video_url, summary_content,
                       updated_at, video_id
                FROM {table}
                WHERE status = 'completed'
                  AND summary_content IS NOT NULL
                  AND video_id IS NOT NULL
                  AND report_date <= %(d)s
                ORDER BY report_date DESC
                LIMIT 1
                """,
                {"d": on_or_before},
            )
            row = cur.fetchone()
        if not row:
            return None
        return {
            "report_date": row[0].isoformat(),
            "lag_days": (report_date - row[0]).days,
            "video_title": row[1],
            "video_url": row[2],
            "summary_markdown": _strip_preamble(row[3]),
            "generated_at": row[4].isoformat() if row[4] else None,
            "video_id": row[5],
        }
    finally:
        conn.close()


# 对齐 insert_the_close_pm_brief_template.sql 真实八段标题
# (Cross-Asset / Key Disagreements / Resolution / Time Horizons / PM Takeaway)
# 以及中文别名;旧正则里的「嘉宾语录」在模板中不存在。
_POSITIONING_RE = re.compile(
    r"(^|\n)(#{1,4}\s*\d?\.?\s*(?:[^\n]*?(?:跨资产信号|核心分歧|分歧解决框架|"
    r"时间维度|投资组合启示|Cross[\-\s]?Asset|Key\s+Disagreement|"
    r"Resolution\s+Framework|Time\s+Horizon|PM\s+Takeaway|"
    r"Positioning[^\n]*))[^\n]*\n)"
    r"(.*?)(?=\n#{1,4}\s|\Z)",
    re.IGNORECASE | re.DOTALL,
)


def _extract_positioning(summary_markdown: str) -> Optional[str]:
    """从 The Close 八段式摘要抽取 positioning 章素材。

    The Close PM Brief 实际段落为: 1 Main Narrative / 2 Top Market Drivers /
    3 Cross-Asset Signals / 4 Key Disagreements / 5 Resolution Framework /
    6 Time Horizons / 7 (varies) / 8 PM Takeaway —— positioning 素材分散在
    3/4/5/6/8 段,这里按标题聚合抽出。
    """
    if not summary_markdown:
        return None
    chunks = []
    for m in _POSITIONING_RE.finditer(summary_markdown):
        heading = m.group(2).strip()
        body = (m.group(3) or "").strip()
        if body:
            chunks.append(f"{heading}\n{body}")
    return "\n\n".join(chunks) or None


def backfill_actuals(report_date: date) -> Dict[str, int]:
    """对 [T-2, T] 重拉 EODHD 经济事件并 COALESCE upsert,补齐事后回填的 actual。

    EODHD 的 actual 是事件发布后延迟回填的;库里 recent 窗口常见"全是预期/前值"。
    upsert 的 actual = COALESCE(EXCLUDED.actual, old) 只增不减,幂等安全;
    402 配额熔断由 safe_api_request 内部处理,失败只影响 actual 完整度。
    """
    from backend.services.economic_events_service import (
        EconomicEventsService, calculate_event_impact)
    from backend.services.market_data_service import MarketDataService

    lo = report_date - timedelta(days=2)
    econ = EconomicEventsService()
    try:
        raw = econ.get_economic_events_chunked(
            from_date=lo.isoformat(), to_date=report_date.isoformat(),
            chunk_days=3,
        ) or []
    except Exception as e:  # noqa: BLE001 — 回填失败不阻塞快照
        logger.warning("economic events backfill fetch failed: %s", e)
        return {"fetched": 0, "upserted": 0}
    if not raw:
        return {"fetched": 0, "upserted": 0}
    # 与 scheduler_service 同步链路一致: EODHD 原始报文字段是 type/estimate,
    # 必须先 format_event_data 映射成 event/forecast,并补 impact(upsert 时
    # impact=EXCLUDED.impact 会覆盖,缺了会把库里已有的 impact 清空)。
    fetched = []
    for ev in raw:
        f = econ.format_event_data(ev)
        if f.get("country") not in ECON_COUNTRIES or not f.get("event"):
            continue
        f["impact"] = calculate_event_impact(f["event"])
        fetched.append(f)
    if not fetched:
        return {"fetched": len(raw), "upserted": 0}
    mds = MarketDataService()
    stats = mds.batch_insert_economic_events(fetched)
    logger.info(f"economic events backfill {lo}~{report_date}: "
                f"fetched={len(fetched)} stats={stats}")
    return {"fetched": len(fetched), "upserted": stats.get("inserted", 0)
            + stats.get("updated", 0)}


def _cross_asset_snapshot(report_date: date) -> List[Dict[str, Any]]:
    """market_data_eod 每代码 ≤T 最新两根日线 → 1D 变化(历史日期重跑同样成立)。"""
    conn = _conn()
    out: List[Dict[str, Any]] = []
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT code, date, close
                FROM market_data_eod
                WHERE code = ANY(%(codes)s)
                  AND date <= %(t)s
                ORDER BY code, date DESC
                """,
                {"codes": list(CROSS_ASSET_CODES), "t": report_date},
            )
            bars: Dict[str, List] = {}
            for code, d, close in cur.fetchall():
                bars.setdefault(code, []).append((d, float(close) if close else None))
        for code, rows in bars.items():
            if not rows or rows[0][1] is None:
                continue
            latest_d, latest_c = rows[0]
            prev_c = rows[1][1] if len(rows) > 1 else None
            chg = None
            if prev_c:
                chg = round((latest_c - prev_c) / prev_c * 100, 2)
            out.append(
                {
                    "code": code,
                    "name": CROSS_ASSET_CODES[code],
                    "date": latest_d.isoformat(),
                    "close": latest_c,
                    "pct_change_1d": chg,
                }
            )
        return out
    finally:
        conn.close()


def _pct(raw: Any) -> Optional[float]:
    """finviz_screener 的百分比列是 VARCHAR('+1.23%'),宽容解析为 float。"""
    if raw is None:
        return None
    s = str(raw).strip().rstrip("%")
    if s in ("", "-", "--", "N/A"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _finviz_snapshot(report_date: date) -> Dict[str, Any]:
    """finviz_screener: 全量 S&P 500 成分 + 板块均值 + Top Movers + 广度。

    同日锚定: 该表为 TRUNCATE+整表刷新的"当前状态"表,snapshot_date 打的是
    同步时刻的 Beijing 日期 —— 管线于北京 T+1 早晨运行时,表内是美股 T 收盘
    数据、戳 T+1。因此锚定 snapshot_date ≤ T+1。
    """
    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT MAX(snapshot_date) FROM finviz_screener "
                "WHERE snapshot_date <= %(t)s",
                {"t": report_date + timedelta(days=1)},
            )
            snap_date = cur.fetchone()[0]
            if snap_date is None:
                raise RuntimeError(f"no finviz_screener snapshot on/before {report_date}+1d")
            cur.execute(
                """
                SELECT ticker, company, sector, industry, price, change_percent,
                       volume, market_cap,
                       performance_week, performance_month, performance_ytd
                FROM finviz_screener
                WHERE snapshot_date = %(sd)s
                  AND index_name ILIKE '%%S&P 500%%'
                  AND change_percent IS NOT NULL
                """,
                {"sd": snap_date},
            )
            constituents: List[Dict[str, Any]] = []
            by_sector: Dict[str, List[float]] = {}
            chgs_all: List[float] = []
            for row in cur.fetchall():
                t, c, s, ind, p, cp, v, mcap, pw, pm, py = row
                chg = _pct(cp)
                if chg is None:
                    continue
                chgs_all.append(chg)
                if s:
                    by_sector.setdefault(s, []).append(chg)
                constituents.append({
                    "ticker": t,
                    "company": c,
                    "sector": s,
                    "industry": ind,
                    "price": float(p) if p is not None else None,
                    "change_percent": chg,
                    "volume": v,
                    "market_cap": float(mcap) if mcap is not None else None,
                    "perf_week": _pct(pw),
                    "perf_month": _pct(pm),
                    "perf_ytd": _pct(py),
                })
            sectors = [
                {"sector": sec, "count": len(chgs),
                 "avg_change_pct": round(sum(chgs) / len(chgs), 2)}
                for sec, chgs in sorted(
                    by_sector.items(),
                    key=lambda kv: sum(kv[1]) / len(kv[1]),
                    reverse=True,
                )
            ]
            breadth = None
            if chgs_all:
                sorted_chgs = sorted(chgs_all)
                mid = len(sorted_chgs) // 2
                median = (sorted_chgs[mid] if len(sorted_chgs) % 2
                          else round((sorted_chgs[mid - 1] + sorted_chgs[mid]) / 2, 2))
                breadth = {
                    "universe": "S&P 500",
                    "count": len(chgs_all),
                    "advancers": sum(1 for x in chgs_all if x > 0),
                    "decliners": sum(1 for x in chgs_all if x < 0),
                    "unchanged": sum(1 for x in chgs_all if x == 0),
                    "median_change_pct": median,
                    "avg_change_pct": round(sum(chgs_all) / len(chgs_all), 2),
                }
            # Top movers: 全 screener 按绝对涨跌幅,但限市值 ≥ $20 亿(market_cap 单位为百万美元):
            # 不设门槛时榜单被微盘/壳公司(+145% 的 ULBI、AIOK 之类)占满,无报道价值。
            cur.execute(
                """
                SELECT ticker, company, sector, price, change_percent, volume
                FROM finviz_screener
                WHERE snapshot_date = %(sd)s
                  AND change_percent IS NOT NULL
                  AND market_cap >= %(mc)s
                """,
                {"sd": snap_date, "mc": TOP_MOVERS_MIN_CAP_M},
            )
            all_movers = []
            for t, c, s, p, cp, v in cur.fetchall():
                chg = _pct(cp)
                if chg is not None:
                    all_movers.append((abs(chg), t, c, s, p, chg, v))
            all_movers.sort(reverse=True)
            top = []
            seen = set()
            for _, t, c, s, p, chg, v in all_movers:
                if t in seen:
                    continue
                seen.add(t)
                top.append({
                    "ticker": t,
                    "company": c,
                    "sector": s,
                    "price": float(p) if p is not None else None,
                    "change_percent": chg,
                    "volume": v,
                })
                if len(top) >= TOP_MOVERS_LIMIT:
                    break
        return {
            "snapshot_date": snap_date.isoformat() if snap_date else None,
            "sp500_constituents": constituents,
            "sp500_sector_avg": sectors,
            "breadth": breadth,
            "top_movers": top,
        }
    finally:
        conn.close()


def _surveillance_summary(report_date: date) -> Optional[Dict[str, Any]]:
    """Bloomberg Surveillance AM Brief。

    表内 report_date 是北京自然日(与 DMR 口径一致),美股交易日 T 对应的
    摘要通常落在 T 或 T+1 —— 取 [T, T+1] 内最新一份 completed。
    """
    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT report_date, video_title, video_url, summary_content,
                       updated_at, video_id
                FROM bloomberg_surveillance_summaries
                WHERE status = 'completed'
                  AND summary_content IS NOT NULL
                  AND report_date BETWEEN %(lo)s AND %(hi)s
                ORDER BY report_date DESC
                """,
                {"lo": report_date, "hi": report_date + timedelta(days=2)},
            )
            rows = cur.fetchall()
        # 以节目标题里的美东播出日期(如 "Bloomberg Surveillance 10/27/2026")为准匹配 T,
        # 而不是只看北京 report_date:T+1 的摘要尚未生成时,按日期倒序取会静默
        # 拿到前一天的节目。标题无日期时才回退到 T+1(北京)那一行。
        row = None
        for r in rows:
            m = re.search(r"(\d{1,2})/(\d{1,2})/(\d{4})", r[1] or "")
            if m and date(int(m.group(3)), int(m.group(1)), int(m.group(2))) == report_date:
                row = r
                break
        if row is None:
            undated = [r for r in rows
                       if not re.search(r"\d{1,2}/\d{1,2}/\d{4}", r[1] or "")]
            row = next((r for r in undated
                        if r[0] == report_date + timedelta(days=1)), None)
        if not row:
            return None
        return {
            "report_date": row[0].isoformat(),
            "beijing_date": row[0].isoformat(),
            "lag_days": 0,
            "video_title": row[1],
            "video_url": row[2],
            "summary_markdown": _strip_preamble(row[3]),
            "generated_at": row[4].isoformat() if row[4] else None,
            "video_id": row[5],
        }
    finally:
        conn.close()


def _earnings_calendar(report_date: date) -> Dict[str, Any]:
    """当日已公布 + 未来 10 日安排(喂 Deep Dives / Movers / Forward Catalysts)。"""
    from backend.services.earnings_calendar_service import EarningsCalendarService

    svc = EarningsCalendarService()

    def _view(start: date, end: date, min_cap: float, limit: int) -> List[Dict[str, Any]]:
        """走服务的展示查询:同 (code, report_date) 只取最新抓取行、仅美股普通股、
        市值(USD)过滤。远端库偶发断连,重试 2 次。"""
        last: Optional[Exception] = None
        for _ in range(3):
            try:
                res = svc.get_earnings_calendar_view(
                    start.isoformat(), end.isoformat(), scope="us",
                    min_market_cap=min_cap, limit=limit)
                return res.get("items") or []
            except Exception as e:  # noqa: BLE001
                last = e
                time.sleep(1.5)
        raise RuntimeError(f"earnings view failed after retries: {last}")

    def _slim(rows: List[Dict[str, Any]], with_result: bool) -> List[Dict[str, Any]]:
        out = []
        for r in rows:
            item = {
                "code": r.get("code"),
                "name": r.get("company_name"),
                "report_date": r.get("report_date"),
                "session": _session(r.get("before_after_market")),
                "eps_estimate": r.get("eps_estimate"),
            }
            if with_result:
                item["eps_actual"] = r.get("eps_actual")
                item["eps_surprise_pct"] = r.get("eps_surprise_percent")
            out.append(item)
        return out

    prev = report_date - timedelta(days=1)
    while prev.weekday() >= 5:  # 周一 → 上周五,而不是周日
        prev -= timedelta(days=1)
    # T-1 盘后 + T 全天:Movers 催化剂(T-1 盘后公布的在 T 开盘反应);T 盘后的在 T+1 才反应,
    # 仅作 Deep Dives / Forward 上下文。
    recent_rows = _slim(_view(prev, report_date, EARNINGS_TODAY_MIN_CAP, 150), True)
    prior_after_close = [r for r in recent_rows
                         if r["report_date"] == prev.isoformat() and r["session"] == "after"]
    today = [r for r in recent_rows if r["report_date"] == report_date.isoformat()]
    upcoming = _slim(
        _view(report_date + timedelta(days=1),
              report_date + timedelta(days=EARNINGS_UPCOMING_DAYS),
              EARNINGS_UPCOMING_MIN_CAP, 80), False)
    return {
        "filter": {
            "scope": "us common stock",
            "today_min_cap_usd": EARNINGS_TODAY_MIN_CAP,
            "upcoming_min_cap_usd": EARNINGS_UPCOMING_MIN_CAP,
            "upcoming_days": EARNINGS_UPCOMING_DAYS,
            "note": "large caps only; absent ticker = below threshold or not US common stock",
        },
        "prior_after_close": prior_after_close,
        "today": today,
        "upcoming": upcoming,
    }


# 财报日历只取重点:当日/前一日盘后看 ≥$100 亿,未来 5 天只看 ≥$500 亿(Forward Catalysts 够用)
EARNINGS_TODAY_MIN_CAP = 10e9
EARNINGS_UPCOMING_MIN_CAP = 50e9
EARNINGS_UPCOMING_DAYS = 5


def _session(raw: Optional[str]) -> Optional[str]:
    s = (raw or "").lower()
    if s.startswith("before"):
        return "before"
    if s.startswith("after"):
        return "after"
    return None


# 单个转录的 attach 上限(字符)。Surveillance 是 3 小时节目、8:30 ET 数据发布后的
# 讨论在中后段,旧的 80k 头部截断恰好砍掉最需要的部分 → 放宽;仍超限时头尾各留一段,
# 不再只留开头。
TRANSCRIPT_CAPS = {"the_close": 120_000, "cnbc_closing_bell": 100_000,
                   "surveillance": 220_000}
TRANSCRIPT_DEFAULT_CAP = 100_000


def _fetch_transcript_by_video_id(video_id: Optional[str],
                                  retries: int = 3) -> Optional[str]:
    """按 video_id 取全文;远端库偶发断连,每次尝试新开连接,最多重试 retries 次。"""
    if not video_id:
        return None
    last: Optional[Exception] = None
    for attempt in range(retries):
        conn = None
        try:
            conn = _conn()
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COALESCE(transcript_cleaned, transcript)
                    FROM subscription_content
                    WHERE content_id = %(vid)s
                      AND (transcript IS NOT NULL OR transcript_cleaned IS NOT NULL)
                    LIMIT 1
                    """,
                    {"vid": video_id},
                )
                row = cur.fetchone()
            return row[0] if row and row[0] else None
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(2 * (attempt + 1))
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:  # noqa: BLE001
                    pass
    raise RuntimeError(f"transcript fetch failed after {retries} tries: {last}")


def _cap_transcript(text: str, cap: int) -> str:
    """超限时保留头 60% + 尾 40%(开场要闻与收尾结论都留),中间以标记替代。"""
    if len(text) <= cap:
        return text
    head, tail = int(cap * 0.6), cap - int(cap * 0.6)
    cut = len(text) - head - tail
    return (text[:head] + f"\n\n[... {cut} chars omitted from the middle for "
            f"attach size ...]\n\n" + text[-tail:])


def write_transcript_attachments(ws: Path, sources: Dict[str, Any],
                                 errors: Dict[str, str]) -> None:
    """按 show 写出 transcripts/{key}.txt,供编排器按 agent --attach。

    纪律:每次先删除同名旧文件 —— 取数失败时旧运行遗留的文件会被 agent 静默
    当作当日转录挂载(曾发生:surveillance 取数失败,16:47 的旧文件仍在)。
    """
    tdir = ws / "transcripts"
    tdir.mkdir(parents=True, exist_ok=True)
    for src_key in ("the_close", "cnbc_closing_bell", "surveillance"):
        path = tdir / f"{src_key}.txt"
        if path.exists():
            path.unlink()
        src = sources.get(src_key) or {}
        vid = src.get("video_id")
        try:
            text = _fetch_transcript_by_video_id(vid)
            if text:
                full_len = len(text)
                text = _cap_transcript(
                    text, TRANSCRIPT_CAPS.get(src_key, TRANSCRIPT_DEFAULT_CAP))
                path.write_text(text, encoding="utf-8")
                src["transcript"] = {"file": f"transcripts/{src_key}.txt",
                                     "full_chars": full_len,
                                     "attached_chars": len(text),
                                     "truncated": len(text) < full_len}
            else:
                errors[f"transcript.{src_key}"] = f"no transcript for video_id={vid}"
        except Exception as e:  # noqa: BLE001
            errors[f"transcript.{src_key}"] = str(e)[:300]


def _to_et(date_str: Optional[str], time_str: Optional[str]) -> Dict[str, str]:
    """EODHD 日历的 date/time 是 UTC —— 换算美东(含夏令时),供 prompt 直接引用。

    例: 2026-10-27 12:30:00 UTC → time_et=08:30(EDT)。00:00:00 视为"无具体时点"不换算。
    返回 {time_et, date_et(仅当与 UTC 日期不同)}。
    """
    if not date_str or not time_str:
        return {}
    date_str, time_str = str(date_str)[:10], str(time_str)  # DB 返回 date/time 对象
    if time_str.startswith("00:00"):
        return {}
    try:
        from zoneinfo import ZoneInfo
        dt = datetime.fromisoformat(f"{date_str} {time_str[:8]}").replace(
            tzinfo=timezone.utc).astimezone(ZoneInfo("America/New_York"))
    except Exception:  # noqa: BLE001
        return {}
    out = {"time_et": dt.strftime("%H:%M")}
    if dt.date().isoformat() != date_str:
        out["date_et"] = dt.date().isoformat()
    return out


def _economic_events(report_date: date) -> Dict[str, Any]:
    """经济日历 — 读同步好的 economic_events 表(事件名/预期值/重要性/类别齐全)。

    窗口锚定 report_date(而非 utcnow),并且必须走 get_economic_events_from_db,
    不能用 get_recent/upcoming_events —— 后者直连 EODHD 活 API,有两个坑:
      1) 原始报文的事件名/预期值字段叫 type/estimate,与本函数的白名单
         (event/forecast)不符会被静默丢弃,impact 原始报文根本不返回
         (是同步时从事件名推导后入库的);
      2) 活 API 按日期倒序、单发 limit=100,宽窗口会被截到最新端
         (见 get_economic_events_chunked 的说明),曾把 upcoming 前 8 天整段
         截成"日历空档"。

    收录口径(用户约定,控制快照体量与管线耗时):
      recent   [report_date-2, report_date]    impact>=2,五国(US/CN/JP/DE/KR)
      upcoming [report_date+1, report_date+7]  同上
    """
    from backend.services.economic_events_service import EconomicEventsService

    svc = EconomicEventsService()
    countries = ",".join(ECON_COUNTRIES)

    def _fetch(lo: date, hi: date) -> List[Dict[str, Any]]:
        return svc.get_economic_events_from_db(
            from_date=lo.isoformat(), to_date=hi.isoformat(),
            country=countries, impacts=[2, 3], order="asc", limit=1000,
        ) or []

    recent = _fetch(
        report_date - timedelta(days=RECENT_EVENT_DAYS - 1), report_date,
    )
    upcoming = _fetch(
        report_date + timedelta(days=1),
        report_date + timedelta(days=UPCOMING_EVENT_DAYS),
    )

    # comparison: ''/None=指数或绝对水平 / 'mom' / 'yoy' —— 同一事件的多口径行靠它区分
    # (如 Core PCE Price Index 的 m/m 与 y/y),缺了会被误判为"重复行"
    keep = ("date", "time", "country", "event", "period", "comparison", "actual",
            "forecast", "previous", "impact", "category")

    def _clean(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        cleaned: List[Dict[str, Any]] = []
        seen = set()
        for e in events:
            if not isinstance(e, dict):
                continue
            row = {k: e[k] for k in keep if e.get(k) not in (None, "")}
            if not row:
                continue
            row.update(_to_et(row.get("date"), row.get("time")))
            sig = json.dumps(row, sort_keys=True, default=str)
            if sig in seen:  # 完全相同的重复行
                continue
            seen.add(sig)
            cleaned.append(row)
        return cleaned

    return {
        "filter": f"近3天+未来{UPCOMING_EVENT_DAYS}天 impact≥2 "
                  f"({'/'.join(ECON_COUNTRIES)})",
        "recent": _clean(recent),
        "upcoming": _clean(upcoming),
    }


def resolve_report_date(conn_date: Optional[str]) -> date:
    """默认锚定最新一份 The Close 摘要的交易日(快照与节目摘要同日)。"""
    if conn_date:
        return date.fromisoformat(conn_date)
    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT MAX(report_date) FROM the_close_summaries
                WHERE status = 'completed' AND summary_content IS NOT NULL
                  AND video_id IS NOT NULL
                """
            )
            row = cur.fetchone()
        if row and row[0]:
            return row[0]
        raise SystemExit(
            "the_close_summaries 中没有 completed 摘要 —— 请先运行 The Close 摘要任务, "
            "或用 --date 显式指定日期。"
        )
    finally:
        conn.close()


def _prior_odds(report_date: date) -> Optional[Dict[str, Any]]:
    """最近一期更早快照的加息概率 —— 供 "(prior: ~x%)" 对比口径(CNT 惯例)。"""
    base = REPO_ROOT / "backend" / "reports" / "pipeline"
    best: Optional[Path] = None
    best_date = ""
    if not base.exists():
        return None
    for f in base.glob("*/snapshot.json"):
        try:
            d = json.loads(f.read_text(encoding="utf-8")).get("report_date", "")
        except (json.JSONDecodeError, OSError):
            continue
        if d and d < report_date.isoformat() and d > best_date:
            best, best_date = f, d
    if not best:
        return None
    try:
        prev = json.loads(best.read_text(encoding="utf-8"))
        # macro_pillars 存在 sources 下(本管线写入口径),兼容旧顶层键
        pillars = (prev.get("sources") or {}).get("macro_pillars") or prev.get(
            "macro_pillars") or {}
    except (json.JSONDecodeError, OSError):
        return None
    odds = pillars.get("next_fomc_odds") or {}
    if not odds:
        return None
    return {"as_of_report_date": best_date, **odds}


def build_snapshot(report_date: date, do_backfill: bool = True,
                   out_dir: Optional[Path] = None) -> Dict[str, Any]:
    errors: Dict[str, str] = {}
    sources: Dict[str, Any] = {}
    notes: Dict[str, Any] = {}

    for key, table in (
        ("the_close", "the_close_summaries"),
        ("cnbc_closing_bell", "cnbc_closing_bell_summaries"),
    ):
        try:
            sources[key] = _latest_completed_summary(table, report_date, report_date)
            if sources[key] is None:
                errors[key] = f"no completed summary on/before {report_date}"
            elif key == "the_close":
                pos = _extract_positioning(sources[key]["summary_markdown"])
                sources[key]["positioning_material"] = pos
                if pos is None:
                    notes["positioning_material"] = (
                        "The Close 摘要中未找到 Cross-Asset / Disagreements / "
                        "Resolution / Time Horizons / PM Takeaway 段落"
                    )
        except Exception as e:  # noqa: BLE001 — 单源失败不阻塞
            sources[key] = None
            errors[key] = str(e)

    try:
        sources["surveillance"] = _surveillance_summary(report_date)
        if sources["surveillance"] is None:
            errors["surveillance"] = (
                f"no completed Surveillance summary on {report_date}~+1d"
            )
    except Exception as e:  # noqa: BLE001
        sources["surveillance"] = None
        errors["surveillance"] = str(e)

    if do_backfill:
        try:
            notes["backfill_actuals"] = backfill_actuals(report_date)
        except Exception as e:  # noqa: BLE001
            notes["backfill_actuals"] = {"error": str(e)}

    try:
        sources["economic_events"] = _economic_events(report_date)
    except Exception as e:  # noqa: BLE001
        sources["economic_events"] = None
        errors["economic_events"] = str(e)

    try:
        sources["earnings_calendar"] = _earnings_calendar(report_date)
    except Exception as e:  # noqa: BLE001
        sources["earnings_calendar"] = None
        errors["earnings_calendar"] = str(e)

    # cross_asset(market_data_eod,可回溯历史)与 finviz(整表刷新的"当前状态",
    # 历史日期无快照)必须独立容错 —— finviz 失败不能连带丢掉跨资产行情。
    market: Dict[str, Any] = {}
    try:
        market["cross_asset"] = _cross_asset_snapshot(report_date)
    except Exception as e:  # noqa: BLE001
        errors["market.cross_asset"] = str(e)[:300]
    try:
        market.update(_finviz_snapshot(report_date))
    except Exception as e:  # noqa: BLE001
        errors["market.finviz"] = str(e)[:300]
    sources["market"] = market or None

    # MACROS 确定性数据底座(六子块 + release_series + 拍卖补字段)
    try:
        fred_key = os.environ.get("FRED_API_KEY", "")
        if not fred_key:
            env_path = REPO_ROOT / ".env"
            if env_path.exists():
                m = re.search(r"^FRED_API_KEY=(.+)$",
                              env_path.read_text(encoding="utf-8"), re.MULTILINE)
                if m:
                    fred_key = m.group(1).strip()
        pillars, pillar_errors = build_macro_pillars(
            report_date, fred_key, sources.get("economic_events"))
        sources["macro_pillars"] = pillars or None
        errors.update({f"macro_pillars.{k}": v for k, v in pillar_errors.items()})
    except Exception as e:  # noqa: BLE001
        sources["macro_pillars"] = None
        errors["macro_pillars"] = str(e)

    # prior 加息概率: 优先用 macro_pillars 直接按前一交易日 ZQ 日线算出的值
    # (不依赖历史快照存在);缺失时回退最近一期更早快照。
    try:
        nxt = ((sources.get("macro_pillars") or {}).get("next_fomc_odds") or {})
        p = nxt.get("prior")
        if p:
            sources["prior_odds"] = {
                "as_of_report_date": p.get("as_of"),
                "meeting": nxt.get("meeting"),
                "contract_month": nxt.get("contract_month"),
                "odds_pct": p.get("odds_pct"),
                "cum_bp_priced": p.get("cum_bp_priced"),
                "source": "ZQ prior trading-day close",
            }
        else:
            prior = _prior_odds(report_date)
            if prior:
                sources["prior_odds"] = prior
    except Exception as e:  # noqa: BLE001
        notes["prior_odds"] = str(e)

    snapshot = {
        "report_date": report_date.isoformat(),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "sources": sources,
        "errors": errors,
        "notes": notes,
    }

    # 转录附件写到快照同目录(不塞进 snapshot.json,避免每个 agent 都付费)
    if out_dir is not None:
        try:
            write_transcript_attachments(out_dir, sources, errors)
        except Exception as e:  # noqa: BLE001
            errors["transcripts"] = str(e)[:300]
            snapshot["errors"] = errors

    return snapshot


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", help="报告日期 YYYY-MM-DD;缺省取最新 The Close 摘要日期")
    parser.add_argument("--out", help="输出路径;缺省 backend/reports/pipeline/{date}/snapshot.json")
    parser.add_argument("--no-backfill", action="store_true",
                        help="跳过 EODHD actual 回填(调试用;省 API 配额)")
    args = parser.parse_args()

    report_date = resolve_report_date(args.date)
    out_path = Path(args.out) if args.out else (
        REPO_ROOT / "backend" / "reports" / "pipeline" / report_date.isoformat() / "snapshot.json"
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)

    snapshot = build_snapshot(
        report_date, do_backfill=not args.no_backfill, out_dir=out_path.parent)
    # default=str: DB 返回的 Decimal/时间等类型兜底转字符串,快照写入永不因类型崩溃
    out_path.write_text(
        json.dumps(snapshot, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )

    loaded = [k for k, v in snapshot["sources"].items() if v]
    print(f"snapshot written: {out_path}")
    print(f"  report_date={snapshot['report_date']}  sources_loaded={loaded}")
    if snapshot["errors"]:
        print(f"  errors={json.dumps(snapshot['errors'], ensure_ascii=False)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
