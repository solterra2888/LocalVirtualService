#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Stage 0 — 为 GuanHe 风格 Daily Market Brief 管线构建冻结数据快照。

从仓库现有数据源拉取当日全部素材,写入 reports/facts-report/{date}/snapshot.json:
  1-2. the_close / cnbc_closing_bell   — 节目转录直查(v5,不经摘要表):
        The Close(YouTube,Bloomberg Television 频道)按 T 晚间发布窗口 +
        开场白 "THIS IS THE CLOSE" 自识别;Closing Bell(播客)按标题
        MM/DD/YY 日期后缀取主段 + Overtime 两行。Layer 1 确定性清洗后内联为
        sources.the_close.clean_transcript 与 sources.cnbc_closing_bell.
        clean_transcript / clean_transcript_ot。不取 Surveillance,不读摘要。
        转录行自身 published_at 即美东场次日(同 UTC 晚),无 T+1 戳错位问题。
  4. economic_events              — 近 3 天实际值 + 未来 7 天日历(读库,impact=3,
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
  9. macro_dashboard(v8)          — 四经济体(US/EU/CN/JP)× 指标族白名单的最新
                                     actual(窗口 [T-75,T],绕过 impact=3 过滤:
                                     ECB/BoJ/LPR 利率行 impact 只有 1-2)
 10. fomc(v8)                     — fomc_decisions 最近 4 场(投票/异议)+
                                     fomc_probabilities 未来会议定价路径 +
                                     最近两份 FOMC 声明全文(措辞对比用)
 11. news_macro(v8.1)              — 宏观地缘素材层(源池 = Admin
                                     user_id=1 的 enabled 订阅,中英混合):
                                     digest(标题模式晨报,北京 T+1 晨窗口)/
                                     research(中英关键词宏观解读;推特源仅收
                                     ≥250 字符长解读)/ geo(中英地缘关键词,
                                     含 news 频道的 CCTV 联播快讯)。周五 T 的
                                     digest 空窗属已知边界(README)

设计约束: 快照一经生成本轮不可变 —— 后续章节 Agent 与核验 Agent 只看这一份
冻结数据,数字对账才可能成立。任何一个数据源失败只记录 errors,不阻塞整体
(章节 Agent 会在 prompt 约束下对缺失源降级处理)。

同日锚定: 行情/日历/曲线/节目转录均锚定交易日 T(转录按节目当晚 UTC 发布
时间匹配,不存在摘要表的北京 T+1 戳问题)。宏观地缘素材层例外:digest 锚定
北京 T+1 早晨(覆盖美股 T 夜盘的隔夜要闻),research/geo 锚定 [T, T+1 06:00)
UTC,published_at 一律按 UTC 存储比较。

用法:
    python tools/facts-report/build_snapshot.py \
        [--date 2026-10-28] [--out PATH] [--no-backfill]

    不传 --date 时取美东最近一个已收盘的交易日(≥18:30 ET 视为当日已收盘)。
    Finviz 无法回溯,测历史日期时 market.finviz 会记入 errors,其余源照常。
"""

from __future__ import annotations

import argparse
import json
import os
import re
import requests
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

# 2026-11-04:Python 3.13 subprocess 默认走 vfork;多线程/特定堆状态下
# vfork 子进程偶发卡死在 exec 前(本机三次复现:子进程显示父进程的 exe/
# cmdline、futex/poll 空等、py-spy 无法附着)。禁用 vfork 走传统
# fork+exec,每次调用多 ~1ms,换来确定性。详见当天交付记录。
try:
    subprocess._USE_VFORK = False  # type: ignore[attr-defined]
except AttributeError:  # 未来版本若移除该开关则忽略
    pass

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from backend.database.db_manager import DatabaseManager  # noqa: E402
from backend.utils.database_config import get_database_config  # noqa: E402
from backend.utils.logger import get_logger  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parent))
from fetch_macro_pillars import build_macro_pillars  # noqa: E402
from prompts import annotate_event_units, assign_item_ids, flag_reported_items  # noqa: E402

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
    "SI1.COMMODITY": "Silver",
    "USDJPY.FOREX": "USD/JPY",
    "EURUSD.FOREX": "EUR/USD",
}

RECENT_EVENT_DAYS = 3
UPCOMING_EVENT_DAYS = 7
# 经济日历只收录主要经济体(用户口径):控制快照体积与 macro/核验耗时。
# v8 加 EU 聚合(新 Macros 四经济体表需要欧元区 GDP/HICP/存款利率)。
ECON_COUNTRIES = ("US", "CN", "JP", "DE", "KR", "EU")

# ── 宏观速览底座(v8):四经济体 × 指标族白名单 ──────────────────────────
# 新 Macros(07 宏观速览)的确定性数据口径。事件名以 EODHD 日历为准;每族
# 取窗口 [T-75, T] 内最新有 actual 的行(按列表顺序优先,前 = 更重要)。
# 不走 EconomicEventsService 的 impact=3 过滤 —— ECB/BoJ/LPR 等利率行
# impact 只有 1-2,该口径会全部漏掉。
MACRO_DASHBOARD_COUNTRIES = ("US", "EU", "CN", "JP")
MACRO_DASHBOARD_WINDOW_DAYS = 75
MACRO_DASHBOARD_FAMILIES: Dict[str, Dict[str, List[str]]] = {
    "US": {
        "policy_rate": [r"^Fed Interest Rate Decision$"],
        "growth": [r"^GDP Growth Rate$", r"^GDP Growth Annualized$"],
        "inflation": [r"^Inflation Rate$", r"^Core Inflation Rate$",
                      r"^Core PCE Price Index$", r"^PPI$"],
        "pmi": [r"^S&P Global (Manufacturing|Services|Composite) PMI$",
                r"^ISM Manufacturing PMI$"],
        "labor": [r"^Non Farm Payrolls$", r"^Unemployment Rate$",
                  r"^Average Hourly Earnings$"],
    },
    "EU": {
        "policy_rate": [r"^Deposit Facility Rate$", r"^Interest Rate Decision$"],
        "growth": [r"^GDP Growth Rate$"],
        "inflation": [r"^Harmonised Inflation Rate$", r"^Core Inflation Rate$",
                      r"^Inflation Rate$"],
        "pmi": [r"^S&P Global (Manufacturing|Services|Composite) PMI$"],
        "labor": [r"^Unemployment Rate$"],
    },
    "CN": {
        "policy_rate": [r"^Loan Prime Rate 1Y$", r"^Prime Rate$"],
        "growth": [r"^GDP Growth Rate$"],
        "inflation": [r"^Inflation Rate$", r"^Producer Price",
                      r"^Producer Prices"],
        "pmi": [r"^NBS Manufacturing PMI$", r"^NBS Non Manufacturing PMI$",
                r"^S&P Global Composite PMI$"],
        "labor": [r"^Unemployment Rate$"],
    },
    "JP": {
        "policy_rate": [r"^BoJ Interest Rate Decision$"],
        "growth": [r"^GDP Growth Rate$", r"^GDP Growth Annualized$"],
        "inflation": [r"^Inflation Rate$", r"^Core Inflation Rate$",
                      r"^Tokyo Core CPI$", r"^Tokyo CPI$"],
        "pmi": [r"^S&P Global (Manufacturing|Services|Composite) PMI$"],
        "labor": [r"^Unemployment Rate$"],
    },
}
# 每族保留行数上限(policy_rate 单行;growth/inflation 留多口径供渲染端取舍
# —— inflation 取 4 是为了让 Core 行排进 US 的 [CPI yoy, CPI level, CPI mom, Core])
_MACRO_FAMILY_CAP = {"policy_rate": 1, "growth": 2, "inflation": 4,
                     "pmi": 3, "labor": 2}
# 同一事件多口径行时 yoy 优先(通胀/增长头条惯例),水平次之,mom 最后
_MACRO_CMP_RANK = {"yoy": 0, None: 1, "": 1, "index": 1, "qoq": 1, "mom": 2}

# ── 宏观地缘素材层(v8.1):Admin(user_id=1)订阅驱动,中英双语 ──────────
# 订阅纪律:素材源一律取自 subscription_list 中 Admin(user_id=1)且 enabled
# 的订阅(与筛选窗口一起在 SQL 里 JOIN),不再硬编码作者白名单 —— Admin
# 补订/退订即自动生效。Admin 订阅本身中英混合:中文(深蓝Alpha/调研鹅纪要/
# 社长说/Dots/CCTV)与英文(Bloomberg TV/Reuters 视频摘要、Geopolitical
# Dispatch、Kobeissi/Steno/Anna Wong 等宏观号),筛选模式因此中英双语。
# published_at 按 UTC 存储。digest 覆盖北京 T+1 早晨(隔夜要闻/利率早报
# 08:51-09:22 发布);research/geo 从 T 00:00 UTC(北京 T 早 8 点)起收。
# 周五 T 的隔夜要闻要下周一早晨才发 —— 周六构建时 digest 层常为空,属已知
# 边界(README),由 geo/research + 英文 wire 兜底。
ADMIN_USER_ID = 1
MACRO_DIGEST_TITLE_RE = re.compile(r"隔夜全球要闻|利率早报|复盘笔记|盘前播报|每日要闻")
# research 双通道:星球源按标题识别券商宏观日报(正文关键词在个股调研纪要
# 里也常出现,误召回高);推特/youtube/substack 按正文关键词 + 长度门槛
MACRO_RESEARCH_TITLE_RE = re.compile(
    r"宏观|隔夜市场|隔夜全球|海外每日|流动性|经济数据|策略周报|利率策略")
MACRO_RESEARCH_BODY_RE = re.compile(
    r"宏观|美联储|Fed\b|FOMC|加息|降息|央行|利率|国债|收益率|CPI|PCE|PMI|GDP"
    r"|非农|失业率|LPR|流动性|衰退|通胀|inflation|recession|rate hike|rate cut"
    r"|central bank|treasury yield|payrolls?", re.I)
MACRO_GEO_RE = re.compile(
    r"伊朗|霍尔木兹|俄乌|乌克兰|俄罗斯|加沙|以色列|哈马斯|真主党|胡塞|也门"
    r"|中东|停火|制裁|关税|选举|OPEC|原油库存|油田|输油管道"
    r"|Iran|Hormuz|Persian Gulf|Ukraine|Russia|Putin|Zelensky|Gaza|Israel"
    r"|Netanyahu|Hezbollah|Houthi|Yemen|Lebanon|ceasefire|sanction|tariff"
    r"|missile|strait", re.I)
# geo 需 ≥2 个不同关键词命中:Yahoo 财报文正文尾部常带单个地缘词(导航/
# 广告/市场概况),单词命中不进层;CCTV 联播快讯等多主题电讯可过
MACRO_GEO_MIN_TERMS = 2
_HTML_TAG_RE = re.compile(r"<[^>]+>")
# 推特层只收「解读形」内容(≥250 字符):*Walter/LiveSquawk 式短快讯属于
# 事实层(news.flash 已服务 01-06 节);zsxq/news/youtube/substack ≥60 即可
MACRO_TWITTER_MIN_CHARS = 250
_CN_DIGEST_MAX, _CN_DIGEST_CAP = 4, 6000
_CN_RESEARCH_MAX, _CN_RESEARCH_CAP = 8, 2500
_CN_GEO_MAX, _CN_GEO_CAP = 40, 1200
TOP_MOVERS_LIMIT = 30
TOP_MOVERS_MIN_CAP_M = 2000  # finviz market_cap 单位:百万美元 → $20 亿
# 盘中残片防护:finviz_screener 每个 snapshot_date 只留最新一份,美股盘中
# (北京时间 22:30 后)重刷会把当日行覆盖为盘中数据(量能只有开市一小段,
# 2026-11-05 23:00 实测 AAPL 仅 1100 万股/全天约 5000 万)。S&P 500 全天
# 成交量约 3-4B 股;低于 2B 判为不完整 —— 宁可缺源降级,不能用错数据。
FINVIZ_MIN_DAY_VOLUME = 2e9


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
            # 盘中残片防护(见 FINVIZ_MIN_DAY_VOLUME 注释):总量不足即拒绝,
            # 让外层 errors 记录 market.finviz 并降级,不用盘中错数据。
            total_vol = sum(float(r["volume"] or 0) for r in constituents)
            if constituents and total_vol < FINVIZ_MIN_DAY_VOLUME:
                raise RuntimeError(
                    f"finviz snapshot {snap_date} looks intraday/incomplete "
                    f"(S&P500 total volume {total_vol / 1e6:.0f}M shares < "
                    f"{FINVIZ_MIN_DAY_VOLUME / 1e6:.0f}M)")
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


# ── 成交额榜(v8.6):美股/港股/A股成交额前列个股,冻结进快照供渲染 ────────
# 美股从 finviz_screener 成分股近似(价格×成交量;S&P 500 内近似 —— 日成交
# 额 ≥$10 亿的个股几乎都在指数内);港股/A股走 EODHD 批量端点 eod-bulk-
# last-day,按报告日 T 显式取数(不受构建时刻影响),用 exchange-symbol-list
# 的 Type=Common Stock 过滤指数/ETF/窝轮,前导零(0700/700)与 -OL 变体去重。
# 港股后缀 .HK、沪 .SHG、深 .SHE 是 EODHD 内部代码,展示层再转 .HK/.SH/.SZ。
VOL_LEADERS_US_MIN = 1e9   # 美股:成交额 ≥ 10 亿美元
VOL_LEADERS_HK_MIN = 1e9   # 港股:成交额 ≥ 10 亿港元
VOL_LEADERS_CN_MIN = 2e9   # A股:成交额 ≥ 20 亿元人民币
VOL_LEADERS_ROWS = 8


def _eodhd_api(path: str, params: Dict[str, str]) -> Any:
    tok = os.environ.get("EODHD_API_TOKEN", "")
    if not tok:
        env_path = REPO_ROOT / ".env"
        if env_path.exists():
            m = re.search(r"^EODHD_API_TOKEN=(.+)$",
                          env_path.read_text(encoding="utf-8"), re.MULTILINE)
            if m:
                tok = m.group(1).strip()
    if not tok:
        raise RuntimeError("EODHD_API_TOKEN not configured")
    resp = requests.get(f"https://eodhd.com/api/{path}",
                        params={**params, "api_token": tok, "fmt": "json"},
                        timeout=90)
    resp.raise_for_status()
    return resp.json()


def _bulk_turnover_rows(exchange: str, day: date) -> Dict[str, Dict[str, Any]]:
    """EODHD bulk 指定日收盘 {code: {close, volume, turnover}}(休市日为空)。"""
    rows = _eodhd_api(f"eod-bulk-last-day/{exchange}",
                      {"date": day.isoformat()})
    out: Dict[str, Dict[str, Any]] = {}
    if not isinstance(rows, list):
        return out
    for r in rows:
        code = str(r.get("code") or "").strip()
        if not code:
            continue
        try:
            close_, vol = float(r.get("close") or 0), float(r.get("volume") or 0)
        except (TypeError, ValueError):
            continue
        if close_ > 0 and vol > 0:
            out[code] = {"close": close_, "volume": vol,
                         "turnover": close_ * vol}
    return out


def _prev_trading_days(day: date, tries: int = 3) -> List[date]:
    """往前跳过周末的候选交易日(每个市场最多回看 tries 个,覆盖周一/节假日)。"""
    out: List[date] = []
    d = day - timedelta(days=1)
    while len(out) < tries:
        if d.weekday() < 5:
            out.append(d)
        d -= timedelta(days=1)
    return out


def _volume_leaders_us(market: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows = []
    for r in market.get("sp500_constituents") or []:
        p, v = r.get("price"), r.get("volume")
        if not p or not v:
            continue
        turnover = float(p) * float(v)
        if turnover < VOL_LEADERS_US_MIN:
            continue
        rows.append({
            "code": r.get("ticker", ""), "exchange": "US",
            "name": r.get("company", ""), "price": float(p),
            "change_percent": r.get("change_percent"),
            "turnover": round(turnover, 2),
        })
    rows.sort(key=lambda x: x["turnover"], reverse=True)
    return rows[:VOL_LEADERS_ROWS]


def _resolve_cn_names(rows: List[Dict[str, Any]]) -> None:
    """EODHD 只有英文名;用腾讯行情接口批量补中文名(A股/港股),失败保留英文。

    只取名字不改数字 —— 价格/成交额仍以 EODHD 冻结数据为准。腾讯一条
    请求可混带沪深港代码(sz300308,sh688825,hk00700),GBK 编码,免密钥。
    """
    if not rows:
        return

    def _tx_code(r: Dict[str, Any]) -> str:
        code, ex = r["code"], r["exchange"]
        if ex == "HK":
            return "hk" + code.zfill(5)
        return ("sh" if ex == "SHG" else "sz") + code

    try:
        q = ",".join(_tx_code(r) for r in rows)
        resp = requests.get(f"http://qt.gtimg.cn/q={q}", timeout=10)
        txt = resp.content.decode("gbk", "replace")
        names: Dict[str, str] = {}
        for line in txt.strip().split(";"):
            line = line.strip()
            if "=" not in line:
                continue
            var, payload = line.split("=", 1)
            key = var[2:] if var.startswith("v_") else var
            fields = payload.strip('"').split("~")
            if len(fields) > 1 and fields[1]:
                names[key.strip()] = fields[1]
        for r in rows:
            n = names.get(_tx_code(r))
            if n:
                r["name_cn"] = n
    except Exception:  # noqa: BLE001
        pass  # 名称解析失败不阻塞榜单,保留英文名


def _volume_leaders_eodhd(report_date: date) -> Dict[str, Any]:
    """港股/A股成交额榜。美股榜独立计算(见 _volume_leaders_us),此函数可整体失败。"""
    exchanges = ("HK", "SHG", "SHE")
    listings: Dict[str, Dict[str, str]] = {}
    for ex in exchanges:
        rows = _eodhd_api(f"exchange-symbol-list/{ex}", {})
        listings[ex] = {
            str(x.get("Code")): str(x.get("Name") or "")
            for x in rows if x.get("Type") == "Common Stock"
        }
    day_rows = {ex: _bulk_turnover_rows(ex, report_date) for ex in exchanges}
    prev_closes: Dict[str, Dict[str, float]] = {}
    for ex in exchanges:
        if not day_rows[ex]:
            continue  # 该市场 T 日休市(如 A 股黄金周),无需涨跌幅
        for cand in _prev_trading_days(report_date):
            prev = _bulk_turnover_rows(ex, cand)
            if prev:
                prev_closes[ex] = {c: r["close"] for c, r in prev.items()}
                break

    def _pick(ex: str) -> Dict[str, Dict[str, Any]]:
        """去重(0700/700 取标准零填充码、丢弃 -OL 衍生行)+ Type 过滤。"""
        best: Dict[str, Dict[str, Any]] = {}
        for code, r in day_rows[ex].items():
            if code not in listings[ex]:
                continue  # 指数/ETF/窝轮/未知代码
            stem, _, suffix = code.partition("-")
            if suffix:
                continue
            key = stem.lstrip("0").upper() or stem
            cur = best.get(key)
            if cur is None or len(code) > len(cur["code"]):
                best[key] = {"code": code, "exchange": ex, **r}
        return best

    def _rows(exs: tuple, min_turnover: float) -> List[Dict[str, Any]]:
        merged: Dict[str, Dict[str, Any]] = {}
        for ex in exs:
            for key, r in _pick(ex).items():
                merged.setdefault(key, r)
        out = []
        for r in merged.values():
            if r["turnover"] < min_turnover:
                continue
            pc = (prev_closes.get(r["exchange"]) or {}).get(r["code"])
            chg = round((r["close"] / pc - 1) * 100, 2) if pc else None
            out.append({
                "code": r["code"], "exchange": r["exchange"],
                "name": listings[r["exchange"]].get(r["code"], ""),
                "price": r["close"], "change_percent": chg,
                "turnover": round(r["turnover"], 2),
            })
        out.sort(key=lambda x: x["turnover"], reverse=True)
        return out[:VOL_LEADERS_ROWS]

    hk_rows = _rows(("HK",), VOL_LEADERS_HK_MIN)
    cn_rows = _rows(("SHG", "SHE"), VOL_LEADERS_CN_MIN)
    for rows in (hk_rows, cn_rows):
        _resolve_cn_names(rows)
    return {
        "hk": {
            "currency": "HKD", "min_turnover": VOL_LEADERS_HK_MIN,
            "as_of": report_date.isoformat(), "rows": hk_rows,
            "note": None if hk_rows else "港股当日休市或无成交额达标个股。",
        },
        "cn": {
            "currency": "CNY", "min_turnover": VOL_LEADERS_CN_MIN,
            "as_of": report_date.isoformat(), "rows": cn_rows,
            "note": None if cn_rows else "A股当日休市或无成交额达标个股。",
        },
    }


def _earnings_calendar(report_date: date) -> Dict[str, Any]:
    """当日已公布 + 未来 10 日安排(喂 个股深挖 / 今日涨跌榜 / Forward Catalysts)。"""
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
    # 仅作 个股深挖 / Forward 上下文。
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


# ── 新闻事件层(v7):Admin 订阅驱动 + L/E 分级,无白名单 ────────────────
# 快讯/通讯社一律取 Admin(user_id=1)enabled 订阅的信息源(与 news_macro
# 素材层同一 JOIN);素材质量由入库打标的 L/E 双轴分级控制 ——
# L4(噪音)整层排除,溢出按 L1>L2>L3/未评级 排序。
# 分级标准见 tools/facts-report/INFO_GRADING_STANDARD.md。
_NEWS_TICKER_RE = re.compile(r"\$([A-Z]{1,5})\b")
_FLASH_MAX_ITEMS = 300      # 超出时按 L1>L2>L3/未评级 + 带$TICKER优先 保留
_FLASH_TEXT_CAP = 300
_WIRE_MAX_ITEMS = 50        # 超出时按 L1>L2>L3/未评级 + 时序新者优先
_WIRE_MIN_CHARS = 800       # 导语/标题级短条目不进 wire(全文层)
_WIRE_TEXT_CAP = 1200
_CALLS_MAX_ITEMS = 6        # SA 电话会节选(个股深挖 的 Management said 素材)
_CALLS_TEXT_CAP = 8000
# L 轴排序权重:小者先保留;L4 已在 SQL 排除,未评级(NULL,存量/滞后)与 L3 同档
_L_RANK = {"L1": 0, "L2": 1, "L3": 2, None: 2, "": 2}


def _news_snapshot(report_date: date,
                   earnings_calendar: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """T 日新闻事件层,冻结为 sources.news(独立容错,不影响其余源)。

    v7:信源一律取 Admin(user_id=1)enabled 订阅(无白名单);入库打标的
    L/E 分级参与选材 —— L4(噪音)SQL 排除;flash/wire 溢出时按
    L1>L2>L3/未评级 排序保留(INFO_GRADING_STANDARD.md),条目携带
    info_level/evidence_level 供下游 E 级表述规则使用。

    flash  Admin 订阅 twitter,窗口 = T 00:00–24:00 ET;RT/同文去重;
           提取 $TICKER;每条截 _FLASH_TEXT_CAP。
    wire   Admin 订阅 news 全文源,窗口 [T-1 20:00 ET, T+1 00:00 ET) 覆盖
           隔夜报道;≥_WIRE_MIN_CHARS 的截 _WIRE_TEXT_CAP;超出 _WIRE_MAX_ITEMS
           按重要度+时序保留(替代旧版"先到先得"截断)。
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
            # Admin 订阅驱动(与 news_macro 素材层同一 JOIN 键);L4 排除,
            # 未评级(NULL,存量/打标滞后)放行 —— 由溢出排序的末档兜住
            cur.execute(
                """
                SELECT c.author_name, c.published_at,
                       coalesce(c.content, ''),
                       t.info_level, t.evidence_level, c.url
                FROM subscription_content c
                JOIN subscription_list l
                  ON l.user_id = %(uid)s AND l.enabled = true
                 AND c.channel_type = l.channel_type
                 AND (c.channel_id = l.channel_identifier::text
                      OR c.author_identifier = l.channel_identifier::text
                      OR c.feed_url = l.channel_url
                      OR c.author_name = l.channel_name)
                LEFT JOIN subscription_content_tags t
                  ON t.subscription_content_id = c.id
                WHERE c.channel_type = 'twitter'
                  AND (t.info_level IS NULL OR t.info_level <> 'L4')
                  AND c.published_at >= %(t0)s AND c.published_at < %(t1)s
                ORDER BY c.published_at
                """,
                {"uid": ADMIN_USER_ID, "t0": t0, "t1": t1},
            )
            frows = cur.fetchall()
            cur.execute(
                """
                SELECT c.author_name, c.title, c.published_at,
                       coalesce(c.content, ''),
                       t.info_level, t.evidence_level, c.url
                FROM subscription_content c
                JOIN subscription_list l
                  ON l.user_id = %(uid)s AND l.enabled = true
                 AND c.channel_type = l.channel_type
                 AND (c.channel_id = l.channel_identifier::text
                      OR c.author_identifier = l.channel_identifier::text
                      OR c.feed_url = l.channel_url
                      OR c.author_name = l.channel_name)
                LEFT JOIN subscription_content_tags t
                  ON t.subscription_content_id = c.id
                WHERE c.channel_type = 'news'
                  AND (t.info_level IS NULL OR t.info_level <> 'L4')
                  AND c.published_at >= %(w0)s AND c.published_at < %(t1)s
                ORDER BY c.published_at DESC
                """,
                {"uid": ADMIN_USER_ID, "w0": w0, "t1": t1},
            )
            wrows = cur.fetchall()
    finally:
        conn.close()

    def _et_fields(ts: Any) -> Dict[str, str]:
        if not ts:
            return {}
        # published_at 是无时区的 UTC;naive 直接 astimezone 会按服务器本地
        # 时区(Asia/Shanghai)解释,time_et 整体早 8 小时
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        dt = ts.astimezone(et)
        out = {"time_et": dt.strftime("%H:%M")}
        if dt.date().isoformat() != report_date.isoformat():
            out["date_et"] = dt.date().isoformat()
        return out

    # flash:同文去重(RT/转发),截断 + ticker 提取;L/E 随条目冻结
    flash: List[Dict[str, Any]] = []
    seen_text: set = set()
    for author, ts, text, lv, ev, url in frows:
        body = re.sub(r"\s+", " ", (text or "")).strip()
        if not body or body.startswith("RT @"):
            continue
        key = body[:160].upper()
        if key in seen_text:
            continue
        seen_text.add(key)
        flash.append({
            "author": author, "url": url,
            **_et_fields(ts),
            "text": body[:_FLASH_TEXT_CAP],
            "tickers": sorted(set(_NEWS_TICKER_RE.findall(text or "")))[:8],
            "info_level": lv,
            "evidence_level": ev,
        })
    if len(flash) > _FLASH_MAX_ITEMS:
        # 溢出按重要度保留:L1>L2>L3/未评级,同档带 $TICKER 优先,再按时间
        flash.sort(key=lambda f: (_L_RANK.get(f.get("info_level"), 2),
                                  0 if f["tickers"] else 1,
                                  f.get("date_et") or "", f.get("time_et") or ""))
        flash = flash[:_FLASH_MAX_ITEMS]

    # wire:全文层过滤/截断;SA 的管理层长文(电话会纪要/指引)单列为 calls。
    # 合格条目先全量按 L 轴重要度稳定排序(组内保持时序倒序),再截
    # _WIRE_MAX_ITEMS —— 替代旧版按时间"先到先得"(实测窗口合格条目可达
    # 千级,旧法会把发得早的重要报道挤掉)
    ec_codes = set()
    for bucket in ("today", "prior_after_close"):
        for r in ((earnings_calendar or {}).get(bucket) or []):
            if r.get("code"):
                ec_codes.add(str(r["code"]).upper())
    qualified: List[tuple] = []
    calls: List[Dict[str, Any]] = []
    for author, title, ts, text, lv, ev, url in wrows:
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
                    "author": author, "title": (title or "")[:200], "url": url,
                    "tickers": matched[:4],
                    # P28:查询本就取了评级,组装时别丢(wire 条目一直带)
                    "info_level": lv, "evidence_level": ev,
                    **_et_fields(ts),
                    "text": body[:_CALLS_TEXT_CAP],
                })
            continue
        qualified.append((author, title, ts, body, lv, ev, url))
    qualified.sort(key=lambda r: _L_RANK.get(r[4], 2))   # 稳定:同档时序不变
    wire = [{
        "author": author, "title": (title or "")[:200], "url": url,
        **_et_fields(ts),
        "text": body[:_WIRE_TEXT_CAP],
        "info_level": lv,
        "evidence_level": ev,
    } for author, title, ts, body, lv, ev, url in qualified[:_WIRE_MAX_ITEMS]]

    out = {
        "filter": (f"flash:Admin订阅twitter {len({a for a, *_ in frows})}源"
                   f" T日ET(L4排除) / "
                   f"wire:Admin订阅news [T-1 20:00,T 24:00)ET "
                   f"≥{_WIRE_MIN_CHARS}c top{_WIRE_MAX_ITEMS}按L级"),
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
      recent   [report_date-2, report_date]    impact=3,五国(US/CN/JP/DE/KR)
      upcoming [report_date+1, report_date+7]  同上
    """
    from backend.services.economic_events_service import EconomicEventsService

    svc = EconomicEventsService()
    countries = ",".join(ECON_COUNTRIES)

    def _fetch(lo: date, hi: date) -> List[Dict[str, Any]]:
        return svc.get_economic_events_from_db(
            from_date=lo.isoformat(), to_date=hi.isoformat(),
            country=countries, impacts=[3], order="asc", limit=1000,
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

    def _event_key(e: Dict[str, Any]) -> tuple:
        # 无 period 的事件(讲话/会议)同日可能多场,带上 time 区分
        period = e.get("period") or ""
        return (str(e.get("date"))[:10], e.get("country"), e.get("event"),
                e.get("comparison") or "", period,
                "" if period else str(e.get("time") or ""))

    def _fresher(e: Dict[str, Any]) -> tuple:
        return (e.get("actual") not in (None, ""), str(e.get("updated_at") or ""),
                str(e.get("time") or ""))

    def _clean(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        # 唯一约束含 time:DST 切换或 EODHD 改时刻后同一事件另插新行,旧行
        # actual 永远为空、预期值也可能过期(11-02 ISM 14:00 空行 / 15:00 实值)。
        # 同一事件只留一行:有 actual 优先,其次最近更新的行。
        best: Dict[tuple, Dict[str, Any]] = {}
        for e in events:
            if not isinstance(e, dict):
                continue
            k = _event_key(e)
            if k not in best or _fresher(e) > _fresher(best[k]):
                best[k] = e
        cleaned: List[Dict[str, Any]] = []
        seen = set()
        for e in events:
            if not isinstance(e, dict) or best.get(_event_key(e)) is not e:
                continue
            row = {k: e[k] for k in keep if e.get(k) not in (None, "")}
            if isinstance(row.get("actual"), str):
                # P21-fix:actual 列是 VARCHAR,同步值为字符串;可解析则统一为
                # 数值(与 flash 回填路径的 float 一致,整数值转 int 保显示干净)
                v = _num(row["actual"].replace("%", ""))
                if v is not None:
                    row["actual"] = int(v) if v == int(v) else v
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
        "filter": f"近3天+未来{UPCOMING_EVENT_DAYS}天 impact=3 "
                  f"({'/'.join(ECON_COUNTRIES)})",
        "recent": _clean(recent),
        "upcoming": _clean(upcoming),
    }


# EODHD 的 actual 回填可能晚到 T+1 下午(2026-11-03 JOLTS 发布后 ~25h 才写库),
# 晨间构建时 T 日一线数据常缺 actual;快讯里的标准打印格式
# ("Job Openings (Sep): 7.52M (Est. 7.10M, Prior 7.19M)")可以兜底。
_PRINT_EST_RE = re.compile(r"\b(?:est|exp|expected|consensus|f'?cast)\b\.?", re.I)
_PRINT_NUM_RE = re.compile(
    r"(?<![\w.])([+\-−]?\d[\d,]*(?:\.\d+)?)\s*(%|k|m|b|million|billion|thousand)?(?![\w])",
    re.I)
_PRINT_CMP_RE = {"yoy": re.compile(r"y/y|\byoy\b|year", re.I),
                 "mom": re.compile(r"m/m|\bmom\b|month", re.I)}
_ANY_CMP_RE = re.compile(r"y/y|m/m|\byoy\b|\bmom\b", re.I)
# 只认带本国标识的快讯;美国用大小写敏感的 US/U.S.(避免 "for us" 误中)
_PRINT_COUNTRY_RE = {
    "US": re.compile(r"\bU\.S\.|\bUS\b|🇺🇸|[Uu]nited [Ss]tates"),
    "EU": re.compile(r"euro\s*(?:zone|area)|\bEZ\b|🇪🇺", re.I),
    "DE": re.compile(r"german|🇩🇪", re.I),
    "CN": re.compile(r"china|chinese|🇨🇳", re.I),
    "JP": re.compile(r"japan|🇯🇵", re.I),
    "KR": re.compile(r"korea|🇰🇷", re.I),
}
_PRINT_FOREIGN_RE = re.compile(
    r"\b(?:spain|spanish|france|french|ital(?:y|ian)|u\.?k\.?|britain|british|"
    r"canad(?:a|ian)|australia|swed(?:en|ish)|swiss|switzerland|india|brazil|"
    r"mexic(?:o|an)|norw(?:ay|egian)|ireland|irish|netherlands|dutch)\b"
    r"|🇪🇸|🇫🇷|🇮🇹|🇬🇧|🇨🇦|🇦🇺|🇸🇪|🇨🇭|🇮🇳|🇧🇷|🇲🇽", re.I)
# 一条快讯常并列多个指标("Services PMI: 56.1 (est …) - Composite PMI: 55.9 (est …)"),
# 按分隔符切段,只在命中事件名的那一段里取数
_PRINT_SEGMENT_RE = re.compile(r"\s[-–—|•]\s|\n")
# 快讯须在日历发布时刻之后的这个窗口内(分钟)
_PRINT_WINDOW_MIN = (-5, 90)
_EVENT_GENERIC = {"rate", "index", "change", "final", "prel", "flash", "s&p",
                  "global", "the", "and"}
_MONTH_ABBR = ("jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep",
               "oct", "nov", "dec")


def _num(s: str) -> Optional[float]:
    try:
        return float(s.replace(",", "").replace("−", "-"))
    except ValueError:
        return None


def _country_ok(text: str, country: Optional[str]) -> bool:
    own = _PRINT_COUNTRY_RE.get(country or "")
    if own is None or not own.search(text):
        return False
    others = [rx for c, rx in _PRINT_COUNTRY_RE.items() if c != country]
    return not (_PRINT_FOREIGN_RE.search(text) or any(rx.search(text) for rx in others))


def _parse_segment(seg: str, row: Dict[str, Any], tokens: List[str]) -> Optional[float]:
    compact = re.sub(r"[^a-z0-9]", "", seg.lower())
    if not all(t in compact for t in tokens):
        return None
    est = _PRINT_EST_RE.search(seg)
    if not est:
        return None
    before = [m for m in _PRINT_NUM_RE.finditer(seg[:est.start()])
              if not re.fullmatch(r"20\d\d", m.group(1))]
    if not before:
        return None
    actual = _num(before[-1].group(1))
    if actual is None:
        return None
    # 量级守卫:与日历的预期/前值同一量级(单位换算不一致的打印直接放弃)
    refs = [v for v in (_num(str(row.get("forecast") or "")),
                        _num(str(row.get("previous") or ""))) if v is not None]
    if refs and not any(abs(actual - r) <= 1.0
                        or (r and 0.5 <= actual / r <= 2.0) for r in refs):
        return None
    return actual


def _parse_print(text: str, row: Dict[str, Any]) -> Optional[float]:
    """从一条快讯里解析该日历行的实际值;任一守卫不过即返回 None。"""
    tokens = [t for t in re.findall(r"[a-z0-9&]+", (row.get("event") or "").lower())
              if len(t) >= 3 and t not in _EVENT_GENERIC]
    if not tokens:
        return None
    cmp_ = (row.get("comparison") or "").lower()
    if cmp_ in _PRINT_CMP_RE:
        if not _PRINT_CMP_RE[cmp_].search(text):
            return None
    elif _ANY_CMP_RE.search(text):
        return None
    period = (row.get("period") or "").lower()
    if period[:3] in _MONTH_ABBR and period[:3] not in text.lower():
        return None
    if not _country_ok(text, row.get("country")):
        return None
    for seg in _PRINT_SEGMENT_RE.split(text):
        val = _parse_segment(seg, row, tokens)
        if val is not None:
            return val
    return None


def _in_release_window(f: Dict[str, Any], row: Dict[str, Any],
                       report_date: date) -> bool:
    """快讯美东时刻须落在日历发布时刻之后的窗口内;日历无具体时点时不设限。"""
    rel = _to_et(row.get("date"), row.get("time"))
    if not rel.get("time_et") or not f.get("time_et"):
        return True
    rel_day = rel.get("date_et") or str(row.get("date"))[:10]
    f_day = f.get("date_et") or report_date.isoformat()
    try:
        r_dt = datetime.fromisoformat(f"{rel_day} {rel['time_et']}")
        f_dt = datetime.fromisoformat(f"{f_day} {f['time_et']}")
    except ValueError:
        return True
    minutes = (f_dt - r_dt).total_seconds() / 60
    return _PRINT_WINDOW_MIN[0] <= minutes <= _PRINT_WINDOW_MIN[1]


def fill_actuals_from_flash(sources: Dict[str, Any], report_date: date) -> int:
    """T 日缺 actual 的日历行用发布窗口内最早一条匹配快讯的打印值补齐(标 actual_source)。"""
    econ = sources.get("economic_events") or {}
    flash = sorted(((sources.get("news") or {}).get("flash") or []),
                   key=lambda f: (f.get("date_et") or report_date.isoformat(),
                                  f.get("time_et") or ""))
    filled = 0
    for row in econ.get("recent") or []:
        if (str(row.get("date"))[:10] != report_date.isoformat()
                or row.get("actual") not in (None, "")):
            continue
        for f in flash:
            if not _in_release_window(f, row, report_date):
                continue
            val = _parse_print(f.get("text") or "", row)
            if val is not None:
                row["actual"] = val
                row["actual_source"] = f"news.flash[id={f.get('id')}]"
                filled += 1
                break
    return filled


def _pick_macro_family(rows: List[Dict[str, Any]], patterns: List[str],
                       cap: int) -> List[Dict[str, Any]]:
    """按白名单正则顺序从 actual 行(date 降序)选指标族行,同 (事件,口径) 取最新。"""
    kept: List[Dict[str, Any]] = []
    seen: set = set()
    for idx, pat in enumerate(patterns):
        rx = re.compile(pat)
        for r in rows:
            if not rx.match(r.get("event") or ""):
                continue
            key = (r.get("event"), r.get("comparison") or "")
            if key in seen:
                continue
            seen.add(key)
            r = dict(r)
            r["_pat"] = idx
            kept.append(r)
    kept.sort(key=lambda r: (r.pop("_pat"),
                             _MACRO_CMP_RANK.get(r.get("comparison"), 1)))
    return kept[:cap]


def _macro_dashboard_snapshot(report_date: date) -> Dict[str, Any]:
    """宏观速览底座 — 四大经济体 × 指标族的最新已发布值(07 节表格数据源)。

    与 sources.economic_events(impact=3、近 3 天)口径互补:这里按国家 ×
    指标族白名单直查 economic_events,窗口 [T-75, T],取每族最新有 actual
    的行 —— 月度/季度指标(如日本 GDP、中国 CPI)常早于 3 天窗口,必须
    拉宽;且 ECB/BoJ/LPR 利率行 impact 只有 1-2,不能走 impact=3 过滤。
    """
    lo = (report_date - timedelta(days=MACRO_DASHBOARD_WINDOW_DAYS)).isoformat()
    hi = report_date.isoformat()
    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT country, date, event, comparison, period,
                       actual, forecast, previous
                FROM economic_events
                WHERE country = ANY(%(c)s) AND date >= %(lo)s AND date <= %(hi)s
                  AND actual IS NOT NULL AND actual <> ''
                ORDER BY date DESC
                """,
                {"c": list(MACRO_DASHBOARD_COUNTRIES), "lo": lo, "hi": hi},
            )
            cols = [d.name for d in cur.description]
            by_country: Dict[str, List[Dict[str, Any]]] = {
                c: [] for c in MACRO_DASHBOARD_COUNTRIES}
            for row in cur.fetchall():
                d = dict(zip(cols, row))
                d["date"] = str(d["date"])[:10]
                by_country.setdefault(d["country"], []).append(d)
    finally:
        conn.close()

    countries: Dict[str, Any] = {}
    missing: List[str] = []
    for c, fam_pats in MACRO_DASHBOARD_FAMILIES.items():
        rows = by_country.get(c) or []
        block: Dict[str, Any] = {}
        for fam, patterns in fam_pats.items():
            picked = _pick_macro_family(rows, patterns, _MACRO_FAMILY_CAP[fam])
            if picked:
                block[fam] = picked
            else:
                missing.append(f"{c}.{fam}")
        if block:
            countries[c] = block
    return {
        "filter": (f"四经济体({'/'.join(MACRO_DASHBOARD_COUNTRIES)}) 指标族白名单 "
                   f"[T-{MACRO_DASHBOARD_WINDOW_DAYS}, T] 最新 actual"),
        "countries": countries,
        "missing": missing,
    }


def _fomc_snapshot(report_date: date) -> Dict[str, Any]:
    """FOMC 决议 / 市场定价路径 / 声明文本(10 节美联储观察的数据源)。

    decisions  fomc_decisions 最近 4 场,含投票比与异议理由
               (「10:2 维持、Cook/Hammack 主张加息」式陈述由此而来)
    path       fomc_probabilities asof≤T 最新一日的全部未来会议。
               口径注意:下一场会议的概率一律引 macro_pillars.next_fomc_odds
               (会后整月合约);本表是会议月合约,其余会议按「合约定价」表述
    statement / statement_prev  最近两份声明全文(截 5000 字符,供措辞对比)
    """
    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT decision_date, rate_low_bp, rate_high_bp, change_bp,
                       vote_for, vote_against, dissenters
                FROM fomc_decisions
                WHERE central_bank = 'FED'
                ORDER BY decision_date DESC LIMIT 4
                """)
            cols = [d.name for d in cur.description]
            decisions = []
            for row in cur.fetchall():
                d = dict(zip(cols, row))
                d["decision_date"] = str(d["decision_date"])[:10]
                for k in ("rate_low_bp", "rate_high_bp", "change_bp",
                          "vote_for", "vote_against"):
                    if d.get(k) is not None:
                        d[k] = float(d[k])
                decisions.append(d)

            cur.execute(
                """
                SELECT meeting_date, p_hike, p_hold, p_cut, implied_rate,
                       futures_contract, asof_date
                FROM fomc_probabilities
                WHERE central_bank = 'FED' AND asof_date <= %(t)s
                  AND meeting_date >= %(t)s
                  AND asof_date = (
                      SELECT max(asof_date) FROM fomc_probabilities
                      WHERE central_bank = 'FED' AND asof_date <= %(t)s)
                ORDER BY meeting_date
                """, {"t": report_date})
            cols = [d.name for d in cur.description]
            meetings = []
            for row in cur.fetchall():
                m = dict(zip(cols, row))
                m["meeting_date"] = str(m["meeting_date"])[:10]
                m["asof_date"] = str(m["asof_date"])[:10]
                for k in ("p_hike", "p_hold", "p_cut", "implied_rate"):
                    if m.get(k) is not None:
                        m[k] = round(float(m[k]), 4)
                meetings.append(m)

            cur.execute(
                """
                SELECT meeting_date, word_count, raw_content, source_url
                FROM fomc_documents
                WHERE central_bank = 'FED' AND doc_type = 'statement'
                  AND meeting_date <= %(t)s AND raw_content IS NOT NULL
                ORDER BY meeting_date DESC LIMIT 2
                """, {"t": report_date})
            stmts = [
                {"meeting_date": str(md)[:10], "word_count": int(wc or 0),
                 "text": (raw or "")[:5000], "source_url": su}
                for md, wc, raw, su in cur.fetchall()
            ]
    finally:
        conn.close()
    return {
        "decisions": decisions,
        "path": {"asof": meetings[0]["asof_date"] if meetings else None,
                 "meetings": meetings},
        "statement": stmts[0] if stmts else None,
        "statement_prev": stmts[1] if len(stmts) > 1 else None,
    }


# ── 素材 triage(v8.2):Admin 订阅轻扫 + 分块 LLM 分拣 ────────────────
# 选材语义化:不再靠关键字正则圈素材,而是把候选条目的「AI 分析轻量字段」
# (title + tags.summary + concepts + countries + L/E 分级)交给一次批量
# LLM 调用分拣;只有被选中且标记需全文的条目才回库取全文(两级漏斗,
# 每日新增输入 ≈ 0.3M tokens,详见 README §triage)。
# 分类失败/关闭(--no-triage)→ 回退 v8.1 关键字正则路径,管线不中断。
DEFAULT_ZCODE_BIN = "/root/.zcode/server/bin/zcode"
TRIAGE_TYPES = ("zhishixingqiu", "substack", "youtube", "twitter", "news")
_TRIAGE_CHUNK = 500          # 每次分类调用的条目数(控制单次上下文)
_TRIAGE_WORKERS = 4          # 分块并行度
_TRIAGE_TIMEOUT_SEC = 600
# 低于此长度的条目不进 triage:twitter 短快讯归 news.flash 事实层,
# news 短导语归 news.wire;长解读/联播快讯才值得语义分拣
_TRIAGE_MIN_CHARS = {"twitter": 180, "news": 200}

_TRIAGE_PROMPT = """你是财经晨报的素材分拣器。下面是 Admin 订阅源的候选清单(轻量字段,无全文)。
对每条判断是否属于以下三层之一,只输出入选条目:

- geo      地缘政治与国家间博弈:战争与冲突、制裁与关税、选举与权力更替、
           能源供应与运输通道、大国博弈/外交谈判、国家支持的网络攻击等安全事件。
           t = 中文主题标签(开放集):美伊局势/俄乌冲突/中东局势/贸易与制裁/
           选举与权力更替/大国博弈 等既有主题沿用,新事件自拟简短名(如 朝鲜半岛/
           台海/印巴)。同类合并:选举类统一归「选举与权力更替」,网络攻击类统一
           归「网络攻击」,不要按国家/事件拆出平行主题。
           注意:美联储/欧央行/中国央行等央行的政策、官员表态与利率内容
           不是 geo(若是观点解读归 research,纯快讯不选)。
- research 宏观解读:券商或经济学家对经济数据、央行政策与利率、流动性、
           汇率、市场策略的观点/解读文章(含央行官员讲话的解读)
- digest   每日晨报整篇:隔夜要闻汇总/利率早报/复盘笔记类多主题简报
           (通常出自知识星球,发布于北京时间次日早晨,覆盖多条新闻)

规则:
1. 只输出入选条目,一行一条 JSON,未入选的不输出,不要任何其他文字:
   {"i":<id>,"p":"geo|research|digest","t":"主题(仅geo必填,其余空串)","x":<1-3>,"d":<0或1>}
2. 个股财报、个股观点、单一公司产品动态、社交闲聊 → 不输出
3. x 重要度:3=事件级(可能改变市场逻辑) 2=有实质影响 1=情报价值
4. d=1 表示摘要不足以支撑写作、需要读全文(数据密集的解读/多事件晨报);快讯类 d=0
5. 一条内容可多层(既是地缘又有宏观解读)时选最主要的一层;多条同主题各自独立输出

候选清单(格式 id|类型|作者|标题|AI摘要|概念|国家|分级|北京时间):
"""


def _admin_macro_scan(report_date: date) -> List[Dict[str, Any]]:
    """Admin 订阅 × [T 00:00, T+1 06:00) UTC 轻扫(不取全文)。

    返回行:{id, published_at, channel_type, author, title, clen,
            summary, concepts, countries, info_level, evidence_level}
    L4(噪音)在 SQL 排除;未评级(NULL)放行由分拣器判断。
    """
    w0 = datetime(report_date.year, report_date.month, report_date.day)
    w1 = w0 + timedelta(hours=30)
    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT c.id, c.published_at, c.channel_type, c.author_name,
                       c.title, char_length(coalesce(c.content, '')) AS clen,
                       t.summary, t.concepts, t.countries,
                       t.info_level, t.evidence_level
                FROM subscription_content c
                JOIN subscription_list l
                  ON l.user_id = %(uid)s AND l.enabled = true
                 AND c.channel_type = l.channel_type
                 AND (c.channel_id = l.channel_identifier::text
                      OR c.author_identifier = l.channel_identifier::text
                      OR c.feed_url = l.channel_url
                      OR c.author_name = l.channel_name)
                LEFT JOIN subscription_content_tags t
                  ON t.subscription_content_id = c.id
                WHERE c.channel_type = ANY(%(types)s)
                  AND (t.info_level IS NULL OR t.info_level <> 'L4')
                  AND c.published_at >= %(lo)s AND c.published_at < %(hi)s
                ORDER BY c.published_at DESC
                """,
                {"uid": ADMIN_USER_ID, "types": list(TRIAGE_TYPES),
                 "lo": w0, "hi": w1},
            )
            cols = [d.name for d in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]
    finally:
        conn.close()


def _triage_line(row: Dict[str, Any]) -> Optional[str]:
    """候选行 → 分拣器输入行;低于门槛的渠道/长度返回 None(不进分拣)。"""
    min_c = _TRIAGE_MIN_CHARS.get(row.get("channel_type"), 0)
    if (row.get("clen") or 0) < min_c:
        return None
    ts = row.get("published_at")
    try:
        t_cn = (ts + timedelta(hours=8)).strftime("%m-%d %H:%M")
    except TypeError:
        t_cn = ""
    concepts = ",".join(str(x) for x in (row.get("concepts") or [])[:5])
    countries = ",".join(str(x) for x in (row.get("countries") or [])[:4])
    lv = f"{row.get('info_level') or '?'}/{row.get('evidence_level') or '?'}"
    return "|".join(str(x)[:80] for x in (
        row["id"], row.get("channel_type"), row.get("author"),
        (row.get("title") or "").strip(), (row.get("summary") or "").strip(),
        concepts, countries, lv, t_cn))


def _parse_zcode_json(stdout: str) -> Dict[str, Any]:
    """zcode --json 输出单个 JSON 对象;容错截取首个 { 到末个 }。

    与 run_pipeline._parse_zcode_json 同实现(Stage 0 不能反向 import
    编排器,保持两份小函数各自独立)。
    """
    stdout = stdout.strip()
    try:
        return json.loads(stdout)
    except json.JSONDecodeError:
        pass
    start, end = stdout.find("{"), stdout.rfind("}")
    if start >= 0 and end > start:
        return json.loads(stdout[start:end + 1])
    raise RuntimeError(f"cannot parse zcode output: {stdout[:300]!r}")


def _triage_chunk(zcode_bin: str, lines: List[str]) -> List[Dict[str, Any]]:
    """对一块候选行跑一次无头 zcode 分类,返回该块的入选条目列表。

    单块失败重试 1 次(20s 退避):并行分块的间歇性 provider 错误
    ("Turn execution failed")曾两次让整轮 triage 静默回退正则——
    分块级重试把这类抖动消化掉,不再向上抛。
    """
    prompt = (_TRIAGE_PROMPT + "\n".join(lines)
              + "\n\n只输出 JSONL,一行一条,不要包裹在其他文字里。")
    cmd = [zcode_bin, "-p", prompt, "--json", "--mode", "yolo",
           "--surface", "terminal"]
    last_err: Optional[Exception] = None
    for attempt in (1, 2):
        try:
            proc = subprocess.run(cmd, capture_output=True, text=True,
                                  timeout=_TRIAGE_TIMEOUT_SEC)
            if proc.returncode != 0:
                raise RuntimeError(
                    f"triage zcode exited {proc.returncode}: "
                    f"{proc.stderr[-300:]}")
            out = _parse_zcode_json(proc.stdout)
            break
        except Exception as e:  # noqa: BLE001
            last_err = e
            if attempt == 1:
                print(f"[triage] chunk failed ({e}); retry in 20s",
                      flush=True)
                time.sleep(20)
    else:
        raise last_err  # type: ignore[misc]
    text = (out.get("response") or "").strip()
    verdicts: List[Dict[str, Any]] = []
    for ln in text.splitlines():
        ln = ln.strip().removeprefix("```json").removeprefix("```")
        if not ln.startswith("{"):
            continue
        try:
            v = json.loads(ln)
        except json.JSONDecodeError:
            continue
        if (isinstance(v.get("i"), int) and v.get("p") in
                ("geo", "research", "digest")):
            verdicts.append({
                "id": v["i"], "pool": v["p"],
                "theme": str(v.get("t") or "")[:24],
                "importance": max(1, min(3, int(v.get("x") or 1))),
                "drill": bool(v.get("d")),
            })
    return verdicts


def run_triage(zcode_bin: str, report_date: date,
               limit: Optional[int] = None) -> Dict[str, int]:
    """轻扫 → 分块并行分拣。返回统计并写中间文件(由调用方读回)。

    结果缓存到 REPO_ROOT/backend/reports/facts-report/{date}/triage.json,
    供 _news_macro_snapshot 组池与人工巡检;失败向上抛(调用方回退正则)。
    """
    t_start = time.time()
    rows = _admin_macro_scan(report_date)
    lines = [ln for ln in (_triage_line(r) for r in rows) if ln]
    if limit:
        lines = lines[:limit]
    chunks = [lines[i:i + _TRIAGE_CHUNK]
              for i in range(0, len(lines), _TRIAGE_CHUNK)]
    verdicts: List[Dict[str, Any]] = []
    # 2026-11-04:线程池并行分块在本机出现 fork 前死锁(子进程 futex_wait,
    # 见 1473967 案例)。TRIAGE_WORKERS=1 时完全绕开 executor,主线程串行
    # fork —— 慢(~2 分钟/块)但稳。默认仍并行,问题复现时环境变量降级。
    workers = int(os.environ.get("TRIAGE_WORKERS", str(_TRIAGE_WORKERS)))
    if workers <= 1 or len(chunks) <= 1:
        for i, ch in enumerate(chunks):
            t0 = time.time()
            verdicts.extend(_triage_chunk(zcode_bin, ch))
            print(f"[triage] chunk {i + 1}/{len(chunks)} done in "
                  f"{time.time() - t0:.0f}s", flush=True)
    else:
        with ThreadPoolExecutor(max_workers=min(workers, len(chunks))) as pool:
            futs = {pool.submit(_triage_chunk, zcode_bin, ch): i
                    for i, ch in enumerate(chunks)}
            for fut in as_completed(futs):
                verdicts.extend(fut.result())
    seen: set = set()
    dedup: List[Dict[str, Any]] = []
    for v in verdicts:            # 同 id 多块/多层输出以先到为准
        if v["id"] not in seen:
            seen.add(v["id"])
            dedup.append(v)
    out_dir = REPO_ROOT / "backend" / "reports" / "facts-report" / report_date.isoformat()
    out_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "report_date": report_date.isoformat(),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "candidates": len(lines),
        "selected": len(dedup),
        "chunks": len(chunks),
        "elapsed_sec": round(time.time() - t_start, 1),
        "verdicts": dedup,
    }
    (out_dir / "triage.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8")
    stats = {
        "candidates": len(lines),
        "selected": len(dedup),
        "geo": sum(1 for v in dedup if v["pool"] == "geo"),
        "research": sum(1 for v in dedup if v["pool"] == "research"),
        "digest": sum(1 for v in dedup if v["pool"] == "digest"),
        "elapsed_sec": payload["elapsed_sec"],
    }
    print(f"[triage] {stats}", flush=True)
    return stats


def load_triage(report_date: date) -> Optional[Dict[str, Any]]:
    """读取本日 triage.json(快照重跑时复用,不重复花 tokens)。"""
    p = (REPO_ROOT / "backend" / "reports" / "facts-report"
         / report_date.isoformat() / "triage.json")
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _news_macro_snapshot(report_date: date) -> Dict[str, Any]:
    """宏观地缘素材层(v8.2):优先走 triage(LLM 语义分拣)组池。

    triage.json 存在且有判定 → _news_macro_triage_pools(开放集主题);
    否则(未跑分拣/结果为空/组池异常)→ _news_macro_regex(v8.1 关键字
    正则路径,行为与既往一致),保证快照永远可建。
    """
    triage = load_triage(report_date)
    if triage and triage.get("verdicts"):
        try:
            return _news_macro_triage_pools(report_date, triage)
        except Exception as e:  # noqa: BLE001
            print(f"[news_macro] triage pools failed ({e}); "
                  f"fallback to regex path", flush=True)
    return _news_macro_regex(report_date)


def _news_macro_triage_pools(report_date: date,
                             triage: Dict[str, Any]) -> Dict[str, Any]:
    """按 triage 判定组三层池:选定条目回库取全文(d 系数不再二次过滤,
    分拣器已按'摘要是否够写'判定;此处仅执行窗口/长度/容量规则)。"""
    w0 = datetime(report_date.year, report_date.month, report_date.day)
    digest_lo, digest_hi = (w0 + timedelta(days=1),
                            w0 + timedelta(hours=30))
    ids = [v["id"] for v in triage["verdicts"]]
    verdicts = {v["id"]: v for v in triage["verdicts"]}
    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT c.id, c.published_at, c.channel_type, c.author_name,
                       c.title, c.content, c.url,
                       t.info_level, t.evidence_level
                FROM subscription_content c
                LEFT JOIN subscription_content_tags t
                  ON t.subscription_content_id = c.id
                WHERE c.id = ANY(%(ids)s)
                """,
                {"ids": ids},
            )
            cols = [d.name for d in cur.description]
            rows = {r[0]: dict(zip(cols, r)) for r in cur.fetchall()}
    finally:
        conn.close()

    def _item(rid: int, cap: int) -> Optional[Dict[str, Any]]:
        row, v = rows.get(rid), verdicts[rid]
        if not row:
            return None
        text = _HTML_TAG_RE.sub(" ", (row.get("content") or "")).strip()
        ts = row.get("published_at")
        try:
            time_cn = (ts + timedelta(hours=8)).strftime("%m-%d %H:%M")
        except TypeError:
            time_cn = None
        return {
            "published_utc": str(ts)[:16] if ts else None,
            "time_cn": time_cn,
            "channel_type": row.get("channel_type"),
            "author": row.get("author_name"),
            "title": (row.get("title") or "").strip()[:120],
            "url": row.get("url"),
            "text": text[:cap],
            "theme": v.get("theme") or "",
            "importance": v.get("importance"),
            "evidence_level": row.get("evidence_level"),
        }

    def _fill(pool: str, cap: int, max_items: int,
              pred=None) -> List[Dict[str, Any]]:
        picked = [v for v in triage["verdicts"] if v["pool"] == pool]
        picked.sort(key=lambda v: (-v["importance"],
                                   str(rows.get(v["id"], {}).get("published_at"))))
        out, seen = [], set()
        for v in picked:
            it = _item(v["id"], cap)
            if not it or (pred and not pred(it, rows.get(v["id"]))):
                continue
            key = re.sub(r"\s+", "", it["text"])[:80]
            if key in seen:
                continue
            seen.add(key)
            out.append(it)
            if len(out) >= max_items:
                break
        return out

    digest = _fill("digest", _CN_DIGEST_CAP, _CN_DIGEST_MAX,
                   pred=lambda it, row: (row["published_at"] >= digest_lo
                                         and row["published_at"] < digest_hi))
    research = _fill("research", _CN_RESEARCH_CAP, _CN_RESEARCH_MAX,
                     pred=lambda it, row: (
                         row["channel_type"] != "news"
                         and (row["channel_type"] != "twitter"
                              or len(row["content"] or "")
                              >= MACRO_TWITTER_MIN_CHARS)))
    geo = _fill("geo", _CN_GEO_CAP, _CN_GEO_MAX)

    n_sources = len({r.get("author") for r in digest + research + geo})
    out: Dict[str, Any] = {
        "filter": (f"Admin(user_id={ADMIN_USER_ID})订阅 × triage(LLM分拣 "
                   f"{triage.get('candidates')}候选→选中{triage.get('selected')}) "
                   f"/ 命中源{n_sources}个;主题开放集,条目带 theme/evidence_level"),
        "digest": digest,
        "research": research,
        "geo": geo,
        "triage": {k: triage.get(k) for k in
                   ("candidates", "selected", "chunks", "elapsed_sec",
                    "generated_at")},
    }
    if not digest:
        out["error_digest"] = "no digest items selected"
        if report_date.weekday() == 4:
            out["note"] = ("Friday T: 隔夜全球要闻下周一晨才发,digest 层空属"
                           "已知边界,由 geo/research + 英文 wire 兜底")
    if not research:
        out["error_research"] = "no research items selected"
    if not geo:
        out["error_geo"] = "no geo items selected"
    return out


def _news_macro_regex(report_date: date) -> Dict[str, Any]:
    """宏观地缘素材层 — Admin 订阅驱动,digest / research / geo 三子层。

    源池 = subscription_list 中 Admin(user_id=ADMIN_USER_ID)且 enabled 的全部
    订阅(zsxq/twitter/news/substack/youtube 的文本内容;podcast 排除 —— 节目
    转录属 01-06 节的素材边界),中英混合:
    digest   订阅源的每日晨报整篇(标题模式:隔夜要闻/利率早报/复盘笔记/
             盘前播报),窗口 = 北京 T+1 早 8-14 点
    research 宏观解读(中英关键词:Fed/FOMC/CPI/PMI/GDP/加息/宏观/inflation/
             rate hike…),窗口 [T 00:00, T+1 06:00) UTC;推特源要求
             ≥250 字符的解读形内容,短快讯留给 news.flash 事实层
    geo      地缘关键词快讯(中英:伊朗/霍尔木兹/俄乌/加沙/Iran/Hormuz/
             Ukraine/Gaza/tariff…),同窗,跨源同文去重封顶
    """
    w0 = datetime(report_date.year, report_date.month, report_date.day)
    w_research = (w0, w0 + timedelta(hours=30))            # [T 00:00, T+1 06:00) UTC
    w_digest = (w0 + timedelta(days=1), w0 + timedelta(hours=30))
    conn = _conn()
    try:
        with conn.cursor() as cur:
            def _fetch(window: tuple, types: tuple) -> List[Dict[str, Any]]:
                # Admin 订阅驱动:JOIN subscription_list(user_id=1, enabled),
                # 键覆盖 channel_id / author_identifier / feed_url / channel_url /
                # author_name(channel_identifier 与 author_name 任一命中即可)。
                # 打标表与 triage 候选查询同款关联(P28 修复:兜底路径原本不
                # JOIN,降级时条目丢 evidence_level、且无 L4 噪音排除;LEFT JOIN
                # 容 NULL —— 打标滞后/缺行时行为与旧兜底一致,保证快照永远可建)
                cur.execute(
                    """
                    SELECT c.published_at, c.channel_type, c.author_name,
                           c.title, c.content, c.url,
                           t.info_level, t.evidence_level
                    FROM subscription_content c
                    JOIN subscription_list l
                      ON l.user_id = %(uid)s AND l.enabled = true
                     AND c.channel_type = l.channel_type
                     AND (c.channel_id = l.channel_identifier::text
                          OR c.author_identifier = l.channel_identifier::text
                          OR c.feed_url = l.channel_url
                          OR c.author_name = l.channel_name)
                    LEFT JOIN subscription_content_tags t
                      ON t.subscription_content_id = c.id
                    WHERE c.channel_type = ANY(%(types)s)
                      AND (t.info_level IS NULL OR t.info_level <> 'L4')
                      AND c.published_at >= %(lo)s AND c.published_at < %(hi)s
                    ORDER BY c.published_at DESC
                    """,
                    {"uid": ADMIN_USER_ID, "types": list(types),
                     "lo": window[0], "hi": window[1]},
                )
                cols = [d.name for d in cur.description]
                return [dict(zip(cols, r)) for r in cur.fetchall()]

            # digest:任意文本源(标题模式,实际命中=星球晨报)
            digest_rows = _fetch(w_digest, ("zhishixingqiu", "twitter", "news",
                                            "substack", "youtube"))
            # research:解读形内容 —— 星球/substack/youtube 全收,推特只要
            # ≥250 字符的长解读;news 全文类型不进(与 news.wire 层重复)
            research_rows = _fetch(w_research, ("zhishixingqiu", "substack",
                                                "youtube", "twitter"))
            # geo:全部文本源(含 news —— CCTV 国际联播快讯只在此层可得)
            geo_rows = _fetch(w_research, ("zhishixingqiu", "twitter", "news",
                                           "substack", "youtube"))
    finally:
        conn.close()

    def _norm(row: Dict[str, Any]) -> Dict[str, Any]:
        ts = row.get("published_at")
        try:
            time_cn = (ts + timedelta(hours=8)).strftime("%m-%d %H:%M")
        except TypeError:
            time_cn = None
        text = _HTML_TAG_RE.sub(" ", (row.get("content") or "")).strip()
        return {
            "published_utc": str(ts)[:16] if ts else None,
            "time_cn": time_cn,
            "channel_type": row.get("channel_type"),
            "author": row.get("author_name"),
            "title": (row.get("title") or "").strip()[:120],
            "url": row.get("url"),
            # P28:与 triage 路径条目对齐,降级不丢证据分级(未评级为 None)
            "evidence_level": row.get("evidence_level"),
            "text": text,
        }

    def _select(rows: List[Dict[str, Any]], title_re, cap: int,
                max_items: int, content_re=None,
                geo_min_terms: int = 0) -> List[Dict[str, Any]]:
        out, seen = [], set()
        for r in rows:
            min_chars = (MACRO_TWITTER_MIN_CHARS
                         if r.get("channel_type") == "twitter" else 60)
            if len(r["text"]) < min_chars:
                continue
            if title_re is not None and not title_re.search(r["title"]):
                continue
            if content_re is not None:
                blob = f"{r['title']} {r['text']}"
                if geo_min_terms:
                    # 不同关键词命中数门槛(同词多次出现算 1)
                    if len(set(m.group(0).lower()
                               for m in content_re.finditer(blob))) < geo_min_terms:
                        continue
                elif not content_re.search(blob):
                    continue
            key = re.sub(r"\s+", "", r["text"])[:80]
            if key in seen:
                continue
            seen.add(key)
            r["text"] = r["text"][:cap]
            out.append(r)
            if len(out) >= max_items:
                break
        return out

    digest = _select([_norm(r) for r in digest_rows], MACRO_DIGEST_TITLE_RE,
                     _CN_DIGEST_CAP, _CN_DIGEST_MAX)
    # research 三遍:星球源按标题(券商宏观日报)优先,推特/youtube/substack
    # 按正文关键词(推特另设 ≥250 字符门槛),其余名额由后者补位
    norm_research = [_norm(r) for r in research_rows]
    research_core = _select(
        norm_research, MACRO_RESEARCH_TITLE_RE, _CN_RESEARCH_CAP,
        _CN_RESEARCH_MAX)
    research_kw = _select(
        [r for r in norm_research
         if r.get("channel_type") != "zhishixingqiu"],
        None, _CN_RESEARCH_CAP, _CN_RESEARCH_MAX,
        content_re=MACRO_RESEARCH_BODY_RE)
    seen_r = {re.sub(r"\s+", "", r["text"])[:80] for r in research_core}
    research = research_core + [
        r for r in research_kw
        if re.sub(r"\s+", "", r["text"])[:80] not in seen_r
    ][:_CN_RESEARCH_MAX - len(research_core)]
    geo_all = [_norm(x) for x in geo_rows]
    # 标题命中的地缘条目优先占名额(真地缘新闻标题必含主题词;财报文/
    # 个股文的 incidental 正文命中排后);sort 稳定,组内保持时间倒序
    geo_all.sort(key=lambda r: 0 if MACRO_GEO_RE.search(r["title"]) else 1)
    geo = _select(geo_all, None, _CN_GEO_CAP, _CN_GEO_MAX,
                  content_re=MACRO_GEO_RE, geo_min_terms=MACRO_GEO_MIN_TERMS)

    n_sources = len({r.get("author") for r in digest + research + geo})
    out: Dict[str, Any] = {
        "filter": (f"Admin(user_id={ADMIN_USER_ID})订阅 中英素材 / "
                   f"digest:标题模式 北京T+1晨 / research:标题+关键词 "
                   f"[T,T+1 06:00)UTC / geo:≥{MACRO_GEO_MIN_TERMS}个不同关键词"
                   f"(命中源{n_sources}个)"),
        "digest": digest,
        "research": research,
        "geo": geo,
    }
    if not digest:
        out["error_digest"] = "no digest items in window"
        if report_date.weekday() == 4:
            out["note"] = ("Friday T: 隔夜全球要闻下周一晨才发,digest 层空属"
                           "已知边界,由 geo/research + 英文 wire 兜底")
    if not research:
        out["error_research"] = "no research items in window"
    if not geo:
        out["error_geo"] = "no geo items in window"
    return out


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
    """最近一期更早快照的加息概率 —— 供 "(prior: ~x%)" 对比口径(GuanHe 惯例)。

    本项目历史优先;自有历史不足时回退只读扫描稳定版管线的历史快照
    (只读不写,不影响稳定版),直到本项目积累起更早快照。
    """
    bases = [REPO_ROOT / "backend" / "reports" / "facts-report",
             # 稳定版管线的产物目录名沿用旧名,仅作只读历史回退
             REPO_ROOT / "backend" / "reports" / "morning-brief"]
    best: Optional[Path] = None
    best_date = ""
    for base in bases:
        if not base.exists():
            continue
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

    # 宏观速览底座 + FOMC 表(v8,新 Macros):四经济体指标表 / 决议·定价·声明
    try:
        sources["macro_dashboard"] = _macro_dashboard_snapshot(report_date)
    except Exception as e:  # noqa: BLE001
        sources["macro_dashboard"] = None
        errors["macro_dashboard"] = str(e)[:300]
    try:
        sources["fomc"] = _fomc_snapshot(report_date)
    except Exception as e:  # noqa: BLE001
        sources["fomc"] = None
        errors["fomc"] = str(e)[:300]

    # 宏观地缘素材层(v8.1,新 Macros):Admin 订阅驱动,digest/research/geo
    try:
        news_macro = _news_macro_snapshot(report_date)
        sources["news_macro"] = news_macro or None
        for k in ("digest", "research", "geo"):
            if news_macro.get(f"error_{k}"):
                errors[f"news_macro.{k}"] = news_macro[f"error_{k}"]
        if isinstance(news_macro.get("note"), str):
            notes["news_macro.digest"] = news_macro["note"]
    except Exception as e:  # noqa: BLE001
        sources["news_macro"] = None
        errors["news_macro"] = str(e)[:300]

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
    # 成交额榜(v8.6):美股(finviz 本地计算)与 港股/A股(EODHD bulk)
    # 独立容错,任一失败不拖垮其余两榜。
    volume_leaders: Dict[str, Any] = {}
    try:
        us_rows = _volume_leaders_us(market)
        volume_leaders["us"] = {
            "currency": "USD", "min_turnover": VOL_LEADERS_US_MIN,
            "as_of": report_date.isoformat(), "rows": us_rows,
            "note": None if us_rows else "美股当日无成交额达标个股。",
        }
    except Exception as e:  # noqa: BLE001
        errors["market.volume_leaders.us"] = str(e)[:300]
    try:
        volume_leaders.update(_volume_leaders_eodhd(report_date))
    except Exception as e:  # noqa: BLE001
        errors["market.volume_leaders.hk_cn"] = str(e)[:300]
    if volume_leaders:
        market["volume_leaders"] = volume_leaders
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

    assign_item_ids(sources)
    try:
        filled = fill_actuals_from_flash(sources, report_date)
        if filled:
            notes["actuals_from_flash"] = filled
    except Exception as e:  # noqa: BLE001
        notes["actuals_from_flash"] = str(e)[:300]
    annotate_event_units(sources)
    flag_reported_items(sources)

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
    parser.add_argument("--out", help="输出路径;缺省 backend/reports/facts-report/{date}/snapshot.json")
    parser.add_argument("--no-backfill", action="store_true",
                        help="跳过 EODHD actual 回填(调试用;省 API 配额)")
    parser.add_argument("--no-triage", action="store_true",
                        help="跳过 LLM 素材分拣,news_macro 走 v8.1 关键字正则路径")
    parser.add_argument("--force-triage", action="store_true",
                        help="忽略已存在的 triage.json,强制重跑分拣(默认复用,不重复花 tokens)")
    parser.add_argument("--triage-limit", type=int, default=None,
                        help="只分拣前 N 条候选(冒烟测试用)")
    parser.add_argument("--zcode-bin", default=os.environ.get(
        "ZCODE_BIN", DEFAULT_ZCODE_BIN))
    args = parser.parse_args()

    report_date = resolve_report_date(args.date)
    out_path = Path(args.out) if args.out else (
        REPO_ROOT / "backend" / "reports" / "facts-report" / report_date.isoformat() / "snapshot.json"
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # 素材 triage(v8.2):先分拣写 triage.json,再建快照(组池时读取)。
    # 失败不阻塞 —— _news_macro_snapshot 自动回退正则路径。
    if not args.no_triage:
        if not args.force_triage and load_triage(report_date):
            print(f"[triage] reuse existing triage.json for "
                  f"{report_date} (--force-triage 重跑)", flush=True)
        else:
            try:
                run_triage(args.zcode_bin, report_date,
                           limit=args.triage_limit)
            except Exception as e:  # noqa: BLE001
                print(f"[triage] FAILED ({e}); news_macro falls back to "
                      f"regex", flush=True)

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
