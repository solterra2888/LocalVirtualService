#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Stage 0 — 为 CNT 风格 Daily Market Brief 管线构建冻结数据快照。

从仓库现有数据源拉取当日全部素材,写入 reports/pipeline/{date}/snapshot.json:
  1. the_close_summaries          — Bloomberg The Close 八段式 PM Brief (LLM 已生成)
                                     + 第 4 段 Positioning & Flow 单独抽出(positioning 章素材)
  2. cnbc_closing_bell_summaries  — CNBC Closing Bell 六段式摘要 (LLM 已生成)
  3. economic_events              — 近 3 天实际值 + 未来 7 天日历 (economic_events 表,
                                     读库不走 EODHD 活 API;impact≥2,仅 US/CN/JP/DE/KR);
                                     构建前对 [T-2, T] 做一次 actual 回填(EODHD 事后补 actual,
                                     COALESCE upsert 只增不减)
  4. market_data_eod              — 跨资产最新两根日线 → 1D 涨跌幅
  5. finviz_screener              — S&P 500 板块均值 + 当日 Top Movers(锚定 snapshot_date ≤ T)
  6. macro_pillars                — MACROS 章确定性数据底座(fetch_macro_pillars.py):
                                     par_curve / fred_ranks / fred_extras / fed_odds(自算加息概率)/
                                     fomc_calendar / auction_events / nowcasts(可选)
  7. prior_odds                   — 最近一期更早快照的加息概率,供 "(prior: ~x%)" 对比口径

设计约束: 快照一经生成本轮不可变 —— 后续章节 Agent 与核验 Agent 只看这一份
冻结数据,数字对账才可能成立。任何一个数据源失败只记录 errors,不阻塞整体
(章节 Agent 会在 prompt 约束下对缺失源降级处理)。

同日锚定: 行情/日历/曲线锚定交易日 T;节目摘要取 ≤T 最新一份并披露 lag_days,
不再静默混入跨交易日口径。

用法:
    /root/miniconda3/envs/guanhe/bin/python tools/report-pipeline/build_snapshot.py \
        [--date 2026-10-24] [--out PATH] [--no-backfill]

    不传 --date 时自动取 the_close_summaries 中最新 completed 的 report_date。
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
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


def _conn() -> Any:
    db = DatabaseManager(get_database_config())
    return db.get_connection()


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
                SELECT report_date, video_title, video_url, summary_content, updated_at
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
            "summary_markdown": row[3],
            "generated_at": row[4].isoformat() if row[4] else None,
        }
    finally:
        conn.close()


_POSITIONING_RE = re.compile(
    r"(^|\n)(#{1,4}\s*\d?\.?\s*(?:[^\n]*?(?:跨资产信号|核心分歧|分歧解决框架|"
    r"关键嘉宾语录|投资组合启示|Positioning[^\n]*))[^\n]*\n)"
    r"(.*?)(?=\n#{1,4}\s|\Z)",
    re.IGNORECASE | re.DOTALL,
)


def _extract_positioning(summary_markdown: str) -> Optional[str]:
    """从 The Close 八段式摘要抽取 positioning 章素材。

    The Close PM Brief 实际段落为: 1市场主线/2驱动/3跨资产信号/4核心分歧/
    5分歧解决框架/6嘉宾语录/7盘后与明日/8投资组合启示 —— 没有独立的
    Positioning 段(那是 Surveillance 模板),positioning 素材分散在
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
    from backend.services.economic_events_service import EconomicEventsService
    from backend.services.market_data_service import MarketDataService

    lo = report_date - timedelta(days=2)
    econ = EconomicEventsService()
    try:
        fetched = econ.get_economic_events_chunked(
            from_date=lo.isoformat(), to_date=report_date.isoformat(),
            chunk_days=3,
        ) or []
    except Exception as e:  # noqa: BLE001 — 回填失败不阻塞快照
        logger.warning("economic events backfill fetch failed: %s", e)
        return {"fetched": 0, "upserted": 0}
    if not fetched:
        return {"fetched": 0, "upserted": 0}
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
    """finviz_screener: S&P 500 板块日涨幅均值 + 当日 Top Movers。

    同日锚定: 该表为 TRUNCATE+整表刷新的"当前状态"表,snapshot_date 打的是
    同步时刻的 Beijing 日期 —— 管线于北京 T+1 早晨运行时,表内是美股 T 收盘
    数据、戳 T+1(2026-10-26 一轮 finviz✓2026-10-27 即此口径,数据本身是
    10-26 的收盘涨跌)。因此锚定 snapshot_date ≤ T+1,并把 as-of 日期随快照
    冻结供口径块披露;超出 T+1(如补跑历史日期)则视为不可得,避免混入
    更晚交易日的涨跌。change_percent 为 VARCHAR,聚合在 Python 侧完成。
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
                SELECT ticker, company, sector, price, change_percent, volume
                FROM finviz_screener
                WHERE snapshot_date = %(sd)s
                  AND index_name ILIKE '%%S&P 500%%'
                  AND change_percent IS NOT NULL
                """,
                {"sd": snap_date},
            )
            rows = cur.fetchall()
            by_sector: Dict[str, List[float]] = {}
            for t, c, s, p, cp, v in rows:
                chg = _pct(cp)
                if chg is None:
                    continue
                if s:
                    by_sector.setdefault(s, []).append(chg)
            sectors = [
                {"sector": sec, "count": len(chgs),
                 "avg_change_pct": round(sum(chgs) / len(chgs), 2)}
                for sec, chgs in sorted(
                    by_sector.items(),
                    key=lambda kv: sum(kv[1]) / len(kv[1]),
                    reverse=True,
                )
            ]
            cur.execute(
                """
                SELECT ticker, company, sector, price, change_percent, volume
                FROM finviz_screener
                WHERE snapshot_date = %(sd)s
                  AND change_percent IS NOT NULL
                """,
                {"sd": snap_date},
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
            "sp500_sector_avg": sectors,
            "top_movers": top,
        }
    finally:
        conn.close()


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

    keep = ("date", "time", "country", "event", "period", "actual",
            "forecast", "previous", "impact", "category")

    def _clean(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        cleaned = []
        for e in events:
            if isinstance(e, dict):
                row = {k: e[k] for k in keep if e.get(k) not in (None, "")}
                if row:
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
        pillars = json.loads(best.read_text(encoding="utf-8")).get("macro_pillars", {})
    except (json.JSONDecodeError, OSError):
        return None
    odds = pillars.get("next_fomc_odds") or {}
    if not odds:
        return None
    return {"as_of_report_date": best_date, **odds}


def build_snapshot(report_date: date, do_backfill: bool = True) -> Dict[str, Any]:
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
                        "The Close 摘要中未找到 'Positioning & Flow' 段落标题"
                    )
        except Exception as e:  # noqa: BLE001 — 单源失败不阻塞
            sources[key] = None
            errors[key] = str(e)

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
        sources["market"] = {
            "cross_asset": _cross_asset_snapshot(report_date),
            **_finviz_snapshot(report_date),
        }
    except Exception as e:  # noqa: BLE001
        sources["market"] = None
        errors["market"] = str(e)

    # MACROS 确定性数据底座(六子块,单块失败只记 errors)
    try:
        fred_key = os.environ.get("FRED_API_KEY", "")
        if not fred_key:
            # .env 在仓库根,读库路径之外兜底
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

    try:
        prior = _prior_odds(report_date)
        if prior:
            sources["prior_odds"] = prior
    except Exception as e:  # noqa: BLE001
        notes["prior_odds"] = str(e)

    return {
        "report_date": report_date.isoformat(),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "sources": sources,
        "errors": errors,
        "notes": notes,
    }


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

    snapshot = build_snapshot(report_date, do_backfill=not args.no_backfill)
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
