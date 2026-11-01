#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Stage 0 — 为 CNT 风格 Daily Market Brief 管线构建冻结数据快照。

从仓库现有数据源拉取当日全部素材,写入 reports/pipeline/{date}/snapshot.json:
  1-2. the_close / cnbc_closing_bell   — 节目转录直查(v5,不经摘要表):
        The Close(YouTube,Bloomberg Television 频道)按 T 晚间发布窗口 +
        开场白 "THIS IS THE CLOSE" 自识别;Closing Bell(播客)按标题
        MM/DD/YY 日期后缀取主段 + Overtime 两行。Layer 1 确定性清洗后内联为
        sources.the_close.clean_transcript 与 sources.cnbc_closing_bell.
        clean_transcript / clean_transcript_ot。不取 Surveillance,不读摘要。
        转录行自身 published_at 即美东场次日(同 UTC 晚),无 T+1 戳错位问题。
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

设计约束: 快照一经生成本轮不可变 —— 后续章节 Agent 与核验 Agent 只看这一份
冻结数据,数字对账才可能成立。任何一个数据源失败只记录 errors,不阻塞整体
(章节 Agent 会在 prompt 约束下对缺失源降级处理)。

同日锚定: 行情/日历/曲线/节目转录均锚定交易日 T(转录按节目当晚 UTC 发布
时间匹配,不存在摘要表的北京 T+1 戳问题)。

用法:
    python tools/report-pipeline/build_snapshot.py \
        [--date 2026-10-28] [--out PATH] [--no-backfill]

    不传 --date 时取美东最近一个已收盘的交易日(≥18:30 ET 视为当日已收盘)。
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


# ── 新闻事件层(v6):L2 快讯 + L3 通讯社全文 + L4 财报电话会 ─────────────

# 快讯账号白名单(subscription_content.twitter 已订阅且活跃;与 CNT 的
# squawk/独家层对应:@DeItaone 头条、LiveSquawk 财报三元组、独家并购线等)
NEWS_FLASH_AUTHORS = (
    "*Walter Bloomberg", "LiveSquawk", "Wall St Engine",
    "The Kobeissi Letter", "The Information", "Nick Timiraos",
    "RenMac: Renaissance Macro Research", "Earnings Whispers",
    "(((The Daily Shot)))",
)
# 通讯社全文(news 频道,订阅活跃且为全文;CNBC/Globenewswire 抓取断流中,
# Reuters/FT/WSJ 未订 —— 恢复/补订后加入本元组即可)
NEWS_WIRE_AUTHORS = ("Yahoo Finance", "Seeking Alpha")
_NEWS_TICKER_RE = re.compile(r"\$([A-Z]{1,5})\b")
_FLASH_MAX_ITEMS = 300      # 超出时优先保留带 $TICKER 的条目
_FLASH_TEXT_CAP = 300
_WIRE_MAX_ITEMS = 50
_WIRE_MIN_CHARS = 800       # 导语/标题级短条目不进 wire(全文层)
_WIRE_TEXT_CAP = 1200
_CALLS_MAX_ITEMS = 6        # SA 电话会节选(Deep Dives 的 Management said 素材)
_CALLS_TEXT_CAP = 8000


def _news_snapshot(report_date: date,
                   earnings_calendar: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """T 日新闻事件层,冻结为 sources.news(独立容错,不影响其余源)。

    flash  twitter 白名单账号,窗口 = T 00:00–24:00 ET;RT/同文去重;
           提取 $TICKER;每条截 _FLASH_TEXT_CAP。
    wire   news 频道全文源,窗口 [T-1 20:00 ET, T+1 00:00 ET) 覆盖隔夜报道;
           ≥_WIRE_MIN_CHARS 的截 _WIRE_TEXT_CAP。
    calls  wire 中的 earnings call transcript(标题特征),只保留财报日历
           (当日 + 前一交易日盘后)命中的 ticker,截 _CALLS_TEXT_CAP。
    """
    from zoneinfo import ZoneInfo
    et = ZoneInfo("America/New_York")
    t0 = datetime(report_date.year, report_date.month, report_date.day,
                  tzinfo=et).astimezone(timezone.utc)
    t1 = t0 + timedelta(days=1)
    w0 = t0 - timedelta(hours=4)  # T-1 20:00 ET

    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT author_name, published_at, coalesce(content, '')
                FROM subscription_content
                WHERE channel_type = 'twitter'
                  AND author_name = ANY(%(a)s)
                  AND published_at >= %(t0)s AND published_at < %(t1)s
                ORDER BY published_at
                """,
                {"a": list(NEWS_FLASH_AUTHORS), "t0": t0, "t1": t1},
            )
            frows = cur.fetchall()
            cur.execute(
                """
                SELECT author_name, title, published_at, coalesce(content, '')
                FROM subscription_content
                WHERE channel_type = 'news'
                  AND author_name = ANY(%(a)s)
                  AND published_at >= %(w0)s AND published_at < %(t1)s
                ORDER BY published_at DESC
                """,
                {"a": list(NEWS_WIRE_AUTHORS), "w0": w0, "t1": t1},
            )
            wrows = cur.fetchall()
    finally:
        conn.close()

    def _et_fields(ts: Any) -> Dict[str, str]:
        dt = ts.astimezone(et) if ts else None
        if not dt:
            return {}
        out = {"time_et": dt.strftime("%H:%M")}
        if dt.date().isoformat() != report_date.isoformat():
            out["date_et"] = dt.date().isoformat()
        return out

    # flash:同文去重(RT/转发),截断 + ticker 提取
    flash: List[Dict[str, Any]] = []
    seen_text: set = set()
    for author, ts, text in frows:
        body = re.sub(r"\s+", " ", (text or "")).strip()
        if not body or body.startswith("RT @"):
            continue
        key = body[:160].upper()
        if key in seen_text:
            continue
        seen_text.add(key)
        flash.append({
            "author": author,
            **_et_fields(ts),
            "text": body[:_FLASH_TEXT_CAP],
            "tickers": sorted(set(_NEWS_TICKER_RE.findall(text or "")))[:8],
        })
    if len(flash) > _FLASH_MAX_ITEMS:
        with_tkr = [f for f in flash if f["tickers"]]
        without = [f for f in flash if not f["tickers"]]
        keep, room = with_tkr[:_FLASH_MAX_ITEMS], _FLASH_MAX_ITEMS - min(
            len(with_tkr), _FLASH_MAX_ITEMS)
        keep += without[:max(room, 0)]
        keep.sort(key=lambda f: (f.get("date_et") or "", f.get("time_et") or ""))
        flash = keep

    # wire:全文层过滤/截断;SA 的管理层长文(电话会纪要/指引)单列为 calls
    ec_codes = set()
    for bucket in ("today", "prior_after_close"):
        for r in ((earnings_calendar or {}).get(bucket) or []):
            if r.get("code"):
                ec_codes.add(str(r["code"]).upper())
    wire: List[Dict[str, Any]] = []
    calls: List[Dict[str, Any]] = []
    for author, title, ts, text in wrows:
        body = re.sub(r"\s+", " ", (text or "")).strip()
        if len(body) < _WIRE_MIN_CHARS:
            continue
        # SA 的电话会纪要长文:正文开头 "Earnings Call Insights: <Company> (TKR)
        # ... MANAGEMENT VIEW",带逐字管理层引语;ticker 是 (XXX) 或 $XXX 格式
        head3 = body[:300]
        is_call = ("earnings call insights" in head3.lower()
                   or "transcript" in (title or "").lower()
                   or (author == "Seeking Alpha" and len(body) >= 8000))
        if is_call:
            tkrs = (set(_NEWS_TICKER_RE.findall(title or ""))
                    | set(_NEWS_TICKER_RE.findall(head3))
                    | {m for m in re.findall(r"\(([A-Z]{2,5})\)", head3)})
            matched = sorted(tkrs & ec_codes) if ec_codes else sorted(tkrs)
            if matched and len(calls) < _CALLS_MAX_ITEMS:
                calls.append({
                    "author": author, "title": (title or "")[:200],
                    "tickers": matched[:4],
                    **_et_fields(ts),
                    "text": body[:_CALLS_TEXT_CAP],
                })
            continue
        if len(wire) >= _WIRE_MAX_ITEMS:
            continue
        wire.append({
            "author": author, "title": (title or "")[:200],
            **_et_fields(ts),
            "text": body[:_WIRE_TEXT_CAP],
        })

    out = {
        "filter": (f"flash:twitter {len(NEWS_FLASH_AUTHORS)}账号 T日ET / "
                   f"wire:{'+'.join(NEWS_WIRE_AUTHORS)} [T-1 20:00,T 24:00)ET "
                   f"≥{_WIRE_MIN_CHARS}c"),
        "flash": flash,
        "wire": wire,
        "calls": calls,
    }
    if not flash:
        out["error_flash"] = "no flash items in window"
    if not wire:
        out["error_wire"] = "no wire articles in window"
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
    """默认锚定美东最近一个已收盘的交易日(≤18:30 ET 视为当日收盘线节目已出)。"""
    if conn_date:
        return date.fromisoformat(conn_date)
    from zoneinfo import ZoneInfo
    et_now = datetime.now(ZoneInfo("America/New_York"))
    d = et_now.date()
    if et_now.hour < 18 or (et_now.hour == 18 and et_now.minute < 30):
        d -= timedelta(days=1)
    while d.weekday() >= 5:  # 周六日无收盘线节目
        d -= timedelta(days=1)
    return d


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

    # 节目源(the_close / cnbc_closing_bell)不再读摘要表:main() 写出快照后由
    # transcript_prep.prep_workspace 按 T 直查 subscription_content 转录行
    # (The Close youtube 自识别 + Closing Bell 主段/Overtime 双行)清洗内联。

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

    # 新闻事件层(v6):L2 快讯 + L3 通讯社全文 + L4 电话会(calls 需要财报日历)
    try:
        news = _news_snapshot(report_date, sources.get("earnings_calendar"))
        sources["news"] = news or None
        for k in ("flash", "wire"):
            if news.get(f"error_{k}"):
                errors[f"news.{k}"] = news[f"error_{k}"]
    except Exception as e:  # noqa: BLE001
        sources["news"] = None
        errors["news"] = str(e)[:300]

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

    # 节目转录直查 + Layer 1 清洗(不经摘要表):The Close youtube 自识别 +
    # Closing Bell 主段/Overtime 双行,内联为 sources.{show}.clean_transcript
    # (CB 另有 clean_transcript_ot)。转录缺失记 errors,该节目按缺源降级。
    import transcript_prep  # 同目录;仅 CLI 构建路径需要,故局部导入
    try:
        transcript_prep.prep_workspace(out_path.parent)
        snapshot = json.loads(out_path.read_text(encoding="utf-8"))
    except Exception as e:  # noqa: BLE001 — 预处理失败不阻塞快照产出
        print(f"  [transcripts] prep skipped: {e}")

    loaded = [k for k, v in snapshot["sources"].items() if v]
    print(f"snapshot written: {out_path}")
    print(f"  report_date={snapshot['report_date']}  sources_loaded={loaded}")
    if snapshot["errors"]:
        print(f"  errors={json.dumps(snapshot['errors'], ensure_ascii=False)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
