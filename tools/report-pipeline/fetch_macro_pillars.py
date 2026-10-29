#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""macro_pillars — MACROS 章节的确定性数据底座(全部免费源,不经 LLM)。

对照 CNT 报告已证实的数据管线(作者自述 + 报告内归因)实现的子块:

  pillar            source                                          用途
  ─────────────────────────────────────────────────────────────────────────
  par_curve         home.treasury.gov 每日面值收益率曲线 CSV        收盘水平 + 日变动 bp
  fred_ranks        FRED DGS2/5/10/20/30 全历史(近1万条)           "highest since YYYY-MM" 断言
  fred_extras       FRED T10YIE/DFII10/HY-OAS/IG-OAS/PMMS/柴油/DFF  盈亏平衡/实际利率/利差/EFFR
  fed_odds          Yahoo 30天联邦基金期货(ZQ*)合约价 vs EFFR 自算  加息概率(CNT 的自算方法)
  next_fomc_odds    下次会议对应的会后整月合约                      含 prior:前一交易日同一合约
  fomc_calendar     federalreserve.gov FOMC 日历页                  下次/未来会议日期
  auction_events    economic_events 中的拍卖 + FiscalData 补字段    今日结果 + 未来拍卖;
                    (只取 Note/Bond;期限按 "7year" 归一;上一场       规模、上次中标、日期
                    必须有 high_yield;"null" 字符串当空值)
  release_series    FRED 月度序列(HOUST 等用环比百分比,不用绝对差)  Release 图数据底座
  nowcasts          网页抓取,失败则回退日历里的 Atlanta Fed GDPNow  CPI/PCE/GDP 前瞻(可选)

设计约束:
  - 每个子块独立 try/except;失败只写入 errors,绝不阻塞快照;
  - 所有请求锚定 report_date(≤T 过滤),历史日期重跑结果可复现;
  - 只用 stdlib + requests,无 LLM、无 API key(除 FRED,读环境变量/参数)。
"""

from __future__ import annotations

import csv
import io
import re
import time
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import requests

UA = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 guanhe-report-pipeline"}
REQUEST_TIMEOUT = 25

TREASURY_CURVE_URL = (
    "https://home.treasury.gov/resource-center/data-chart-center/interest-rates/"
    "daily-treasury-rates.csv/{year}/all?type=daily_treasury_yield_curve"
    "&field_tdr_date_value={year}&page&_format=csv"
)
FRED_API = "https://api.stlouisfed.org/fred/series/observations"
YAHOO_CHART = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
FOMC_CALENDAR_URL = "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm"

# par curve CSV 列名 → 标准期限名
CURVE_TENORS = {
    "2 Yr": "2y", "5 Yr": "5y", "7 Yr": "7y", "10 Yr": "10y",
    "20 Yr": "20y", "30 Yr": "30y",
}
RANK_SERIES = {"DGS2": "2y", "DGS5": "5y", "DGS10": "10y", "DGS20": "20y", "DGS30": "30y"}
EXTRA_SERIES = {
    "T10YIE": "10y breakeven",
    "DFII10": "10y real yield",
    "BAMLH0A0HYM2": "HY OAS",
    "BAMLC0A0CM": "IG OAS",
    "MORTGAGE30US": "30y mortgage",
    "GASDESW": "diesel ($/gal)",
    # DFF(EFFR)不在 extras —— 单独作为 effr 块先取,fed_odds 只依赖它
}

# ZQ(30 天联邦基金期货)月代码:1=F 2=G 3=H 4=J 5=K 6=M 7=N 8=Q 9=U 10=V 11=X 12=Z
ZQ_MONTH_CODES = {1: "F", 2: "G", 3: "H", 4: "J", 5: "K", 6: "M",
                  7: "N", 8: "Q", 9: "U", 10: "V", 11: "X", 12: "Z"}
HIKE_BP = 25.0  # 单次加息幅度假设(bp),与 CNT 的自算口径一致
PILLAR_TIMEOUT_SEC = 200  # 单块看门狗:Stage 0 总耗时有界


def _get(url: str, params: Optional[Dict[str, Any]] = None,
         retries: int = 2) -> requests.Response:
    last_err: Optional[Exception] = None
    for attempt in range(retries + 1):
        try:
            r = requests.get(url, params=params, headers=UA, timeout=REQUEST_TIMEOUT)
            if r.status_code == 200:
                return r
            if r.status_code == 429:  # 限流:FRED 等一个窗口重试一次,其余短退避
                if "fred" in url and attempt == 0:
                    time.sleep(65)
                    continue
                raise RuntimeError("HTTP 429 rate limited")
            last_err = RuntimeError(f"HTTP {r.status_code}")
        except Exception as e:  # noqa: BLE001 — 网络抖动重试
            last_err = e
        time.sleep(1.5)
    raise last_err or RuntimeError("request failed")


# FRED 全局节流 + 熔断: 免费档按分钟限流,首调用连续 429 即禁用本轮全部
# FRED 序列(快照降级为无 fred_ranks/fred_extras/fed_odds,只记 errors),
# 避免 12 个序列 × 每个几十秒退避把 Stage 0 拖死。
_FRED_LAST_CALL = 0.0
_FRED_MIN_INTERVAL_SEC = 3.0
_FRED_429_STREAK = 0
_FRED_DISABLED = False


def _fred_throttle() -> None:
    global _FRED_LAST_CALL, _FRED_429_STREAK, _FRED_DISABLED
    if _FRED_DISABLED:
        raise RuntimeError("FRED circuit open (persistent 429) — skipped")
    wait = _FRED_MIN_INTERVAL_SEC - (time.time() - _FRED_LAST_CALL)
    if wait > 0:
        time.sleep(wait)
    _FRED_LAST_CALL = time.time()


def _fred_mark_429() -> None:
    global _FRED_429_STREAK, _FRED_DISABLED
    _FRED_429_STREAK += 1
    if _FRED_429_STREAK >= 2:
        _FRED_DISABLED = True


def _fred_mark_ok() -> None:
    global _FRED_429_STREAK
    _FRED_429_STREAK = 0


# ── par curve ──────────────────────────────────────────────────────────

def fetch_par_curve(report_date: date) -> Dict[str, Any]:
    """Treasury 每日面值曲线:取 ≤T 的最新两行 → 各期限水平 + 日变动 bp。"""
    r = _get(TREASURY_CURVE_URL.format(year=report_date.year))
    rows = list(csv.DictReader(io.StringIO(r.text)))
    parsed: List[Tuple[date, Dict[str, float]]] = []
    for row in rows:
        try:
            d = datetime.strptime(row["Date"], "%m/%d/%Y").date()
        except (KeyError, ValueError):
            continue
        if d > report_date:
            continue
        parsed.append((d, row))
    parsed.sort(key=lambda x: x[0])
    if not parsed:
        raise RuntimeError("no par curve rows on/before report_date")
    d_today, row_today = parsed[-1]
    d_prev, row_prev = parsed[-2] if len(parsed) > 1 else (None, None)
    levels, changes = {}, {}
    for col, tenor in CURVE_TENORS.items():
        v = row_today.get(col)
        if v in (None, "", "N/A"):
            continue
        levels[tenor] = float(v)
        if row_prev and row_prev.get(col) not in (None, "", "N/A"):
            changes[tenor] = round((float(v) - float(row_prev[col])) * 100, 1)  # pct → bp
    return {"as_of": d_today.isoformat(),
            "prev_date": d_prev.isoformat() if d_prev else None,
            "levels_pct": levels, "change_bp": changes}


# ── FRED ───────────────────────────────────────────────────────────────

def _fred_observations(api_key: str, series: str, end: date,
                       max_obs: Optional[int] = None) -> List[Tuple[date, float]]:
    """FRED 序列观察值(降序,≤end)。

    max_obs=None 取全历史(按 count 翻页,用于 ranks);否则只取最新 max_obs 条。
    注意不能用"页返回数 < 页大小"作为唯一终止条件 —— 请求多少条 API 就恰好
    返回多少条,该条件永假(v1 的死循环根因:limit=5 无限翻页打爆限流)。
    """
    out: List[Tuple[date, float]] = []
    offset = 0
    page = min(max_obs, 10000) if max_obs else 10000
    while True:
        params = {
            "series_id": series, "api_key": api_key, "file_type": "json",
            "sort_order": "desc", "limit": page, "offset": offset,
            "observation_end": end.isoformat(),
        }
        _fred_throttle()
        try:
            resp = _get(FRED_API, params=params)
        except RuntimeError as e:
            if "429" in str(e):
                _fred_mark_429()
            raise
        _fred_mark_ok()
        data = resp.json()
        obs = data.get("observations", [])
        for o in obs:
            v = o.get("value")
            if v in (None, ".", ""):
                continue
            try:
                out.append((date.fromisoformat(o["date"]), float(v)))
            except ValueError:
                continue
        total = data.get("count")
        collected = offset + len(obs)
        if total is not None and collected >= int(total):
            break
        if max_obs is not None and len(out) >= max_obs:
            break
        if len(obs) < page:
            break
        offset = collected
    return out  # 降序(最新在前)


def fetch_fred_ranks(api_key: str, report_date: date,
                     par_levels: Dict[str, float]) -> Dict[str, Any]:
    """对每个关键期限预计算 "highest since" 断言 —— LLM 只能引用,禁止自算。

    优先用 par curve 的当日水平(CNT 口径),FRED 序列仅用于历史比较;
    par curve 缺该期限时回退 FRED 最新值。
    """
    out: Dict[str, Any] = {}
    for series, tenor in RANK_SERIES.items():
        obs = _fred_observations(api_key, series, report_date)
        if not obs:
            continue
        current = par_levels.get(tenor, obs[0][1])
        as_of = obs[0][0]
        # 断言语义: "highest since D" 要求 D 之后至今(不含当日)没有任何更高值。
        # 昨日更高时宣称 "highest since 昨天" 是废话 —— 只有当创下高点的
        # 连续区间足够长(≥20 个观测,约一个月交易日)才生成 claim,
        # 否则只给 prior_peak 事实字段,不生成可引用断言。
        history = [(d, v) for d, v in obs if d < as_of]
        prior_higher = next(((d, v) for d, v in history if v > current), None)
        entry: Dict[str, Any] = {
            "series": series, "value": current, "as_of": as_of.isoformat(),
        }
        if prior_higher is None:
            entry["highest_since"] = None
            entry["obs_at_new_high"] = len(history)
            entry["claim"] = (f"highest in FRED {series} history back to "
                              f"{obs[-1][0].isoformat()}")
        else:
            peak_d, peak_v = prior_higher
            run = sum(1 for d, _ in history if d > peak_d)
            entry["prior_peak"] = peak_v
            entry["prior_peak_date"] = peak_d.isoformat()
            entry["obs_since_peak"] = run
            if run >= 20:
                entry["highest_since"] = peak_d.isoformat()
                entry["claim"] = f"highest since {peak_d.isoformat()}"
            else:
                entry["highest_since"] = None
                entry["claim"] = None
        out[tenor] = entry
    return out


def fetch_fred_extras(api_key: str, report_date: date) -> Dict[str, Any]:
    """辅助序列:最新值 + 前一观测 + 变动(EFFR 同时喂 fed_odds)。"""
    out: Dict[str, Any] = {}
    for series, label in EXTRA_SERIES.items():
        obs = _fred_observations(api_key, series, report_date, max_obs=5)
        if not obs:
            continue
        latest_d, latest_v = obs[0]
        prev_v = obs[1][1] if len(obs) > 1 else None
        out[label] = {
            "series": series, "value": latest_v, "as_of": latest_d.isoformat(),
            "prev": prev_v,
            "change": round(latest_v - prev_v, 3) if prev_v is not None else None,
        }
    return out


# ── fed funds odds(自算,CNT 方法)──────────────────────────────────────

def _zq_symbol(y: int, m: int) -> str:
    return f"ZQ{ZQ_MONTH_CODES[m]}{str(y)[2:]}.CBT"


def _yahoo_close_on(symbol: str, report_date: date) -> Optional[Tuple[date, float]]:
    """取 ≤T 的最后一根日线收盘;空则回退 meta.regularMarketPrice(限当日)。"""
    p2 = int(datetime.combine(report_date + timedelta(days=1),
                              datetime.min.time()).timestamp())
    p1 = p2 - 21 * 86400
    data = _get(YAHOO_CHART.format(symbol=symbol),
                params={"period1": p1, "period2": p2, "interval": "1d"}).json()
    result = (data.get("chart") or {}).get("result") or []
    if not result:
        return None
    res = result[0]
    ts = res.get("timestamp") or []
    closes = (res.get("indicators", {}).get("quote") or [{}])[0].get("close") or []
    best: Optional[Tuple[date, float]] = None
    for t, c in zip(ts, closes):
        if c is None:
            continue
        d = datetime.fromtimestamp(t).date()
        if d <= report_date and (best is None or d > best[0]):
            best = (d, round(float(c), 3))
    if best is None:
        meta = res.get("meta") or {}
        price, rmt = meta.get("regularMarketPrice"), meta.get("regularMarketTime")
        if price is not None and rmt and datetime.fromtimestamp(rmt).date() <= report_date:
            best = (datetime.fromtimestamp(rmt).date(), round(float(price), 3))
    return best


def _prev_weekday(d: date) -> date:
    d -= timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d


def fetch_prior_contract_odds(report_date: date, effr: Optional[float],
                              month_key: str) -> Optional[Dict[str, Any]]:
    """前一交易日同一合约的加息概率 —— 直接用 Yahoo 历史日线算,
    不依赖"上一期快照是否存在"(CNT 每期都有 prior 对比)。"""
    if effr is None:
        return None
    cy, cm = (int(x) for x in month_key.split("-"))
    prev_d = _prev_weekday(report_date)
    got = _yahoo_close_on(_zq_symbol(cy, cm), prev_d)
    if got is None:
        return None
    d, price = got
    implied = round(100 - price, 3)
    return {
        "as_of": d.isoformat(), "price": price,
        "odds_pct": round(max(0.0, (implied - effr) / (HIKE_BP / 100)) * 100, 1),
        "cum_bp_priced": round((implied - effr) * 100, 1),
    }


def fetch_fed_odds(report_date: date, effr: Optional[float]) -> Dict[str, Any]:
    """ZQ 合约价 → 隐含均值利率 → 相对 EFFR 的 +25bp 概率。

    odds = (100 - price - effr) / 0.25(CNT 已被证实的自算口径,用会后整月合约)。
    14 个月度合约并行抓取(Yahoo 单发可达 10-20s,串行会拖死 Stage 0)。
    """
    from concurrent.futures import ThreadPoolExecutor

    if effr is None:
        raise RuntimeError("EFFR unavailable from FRED (DFF)")

    def _fetch_contract(i: int) -> Optional[Dict[str, Any]]:
        y, m = report_date.year, report_date.month
        total = (y * 12 + (m - 1)) + i
        cy, cm = total // 12, total % 12 + 1
        got = _yahoo_close_on(_zq_symbol(cy, cm), report_date)
        if got is None:
            return None
        d, price = got
        implied = round(100 - price, 3)
        odds = round(max(0.0, (implied - effr) / (HIKE_BP / 100)) * 100, 1)
        return {
            "month": f"{cy}-{cm:02d}", "symbol": _zq_symbol(cy, cm),
            "price": price, "close_date": d.isoformat(),
            "implied_avg_rate": implied,
            "odds_of_25bp_hike_pct": odds,
            "cum_bp_priced": round((implied - effr) * 100, 1),
        }

    with ThreadPoolExecutor(max_workers=7) as pool:
        results = list(pool.map(_fetch_contract, range(1, 15)))
    contracts = [c for c in results if c]
    if not contracts:
        raise RuntimeError("no ZQ contract quotes from Yahoo")
    contracts.sort(key=lambda c: c["month"])
    return {"effr": effr, "hike_size_bp": HIKE_BP, "contracts": contracts}


# ── FOMC calendar ──────────────────────────────────────────────────────

_MONTH_NUM = {name: i + 1 for i, name in enumerate(
    ["January", "February", "March", "April", "May", "June", "July",
     "August", "September", "October", "November", "December"])}
_FOMC_SECTION_RE = re.compile(r'(\d{4}) FOMC Meetings</a>')
_FOMC_BLOCK_RE = re.compile(
    r'fomc-meeting__month[^>]*>\s*<strong>([A-Za-z]+)</strong>\s*</div>\s*'
    r'<div class="[^"]*fomc-meeting__date[^>]*>\s*(\d{1,2})(?:\s*[-–]\s*(\d{1,2}))?\*?\s*<',
    re.DOTALL)


def fetch_fomc_calendar(report_date: date) -> List[Dict[str, Any]]:
    """解析 FOMC 日历页:年份在 'NNNN FOMC Meetings' 面板标题,会议在
    fomc-meeting__month/__date 成对 div(不带年份、可能带 * 规划标记)。
    返回 end ≥ T 的会议列表。"""
    html = _get(FOMC_CALENDAR_URL).text
    meetings: List[Dict[str, Any]] = []
    sections = list(_FOMC_SECTION_RE.finditer(html))
    for idx, sec in enumerate(sections):
        year = int(sec.group(1))
        seg_end = sections[idx + 1].start() if idx + 1 < len(sections) else len(html)
        segment = html[sec.start():seg_end]
        for month_name, d1, d2 in _FOMC_BLOCK_RE.findall(segment):
            m = _MONTH_NUM.get(month_name)
            if not m:
                continue
            try:
                start = date(year, m, int(d1))
                end = date(year, m, int(d2 or d1))
            except ValueError:
                continue
            meetings.append({"start": start.isoformat(), "end": end.isoformat(),
                             "text": f"{month_name} {d1}-{d2 or d1}, {year}"})
    upcoming = [x for x in meetings if x["end"] >= report_date.isoformat()]
    upcoming.sort(key=lambda x: x["start"])
    if not upcoming:
        raise RuntimeError("no upcoming FOMC meetings parsed")
    return upcoming


# ── nowcasts(尽力而为)────────────────────────────────────────────────

def fetch_nowcasts() -> Optional[Dict[str, str]]:
    """Cleveland Fed 通胀 nowcast / Atlanta GDPNow —— 页面重度 JS,常失败;
    失败只影响 Forward Catalysts 的 nowcast 行,属可选增强(单次尝试、短超时,
    绝不让可选源拖慢 Stage 0)。"""
    out: Dict[str, str] = {}
    try:
        html = requests.get("https://www.atlantafed.org/cqer/research/gdpnow",
                            headers=UA, timeout=10).text
        m = re.search(r"(?:latest model estimate|GDPNow estimate)[^<]{0,20}"
                      r"([+-]?\d+\.\d+)\s*(?:percent|%)", html, re.IGNORECASE)
        if m:
            out["gdpnow_qoq_ann"] = m.group(1)
    except Exception:  # noqa: BLE001 — 可选源
        pass
    try:
        html = requests.get(
            "https://www.clevelandfed.org/indicators-and-data/inflation-nowcasting",
            headers=UA, timeout=10).text
        for label, key in (("CPI", "cpi_mom"), ("PCE", "pce_mom"), ("Core PCE", "core_pce_mom")):
            m = re.search(rf"{label}[^<]{{0,120}}?([+-]?\d+\.\d+)\s*(?:percent|%)", html)
            if m:
                out[key] = m.group(1)
    except Exception:  # noqa: BLE001 — 可选源
        pass
    return out or None


# ── 聚合入口 ───────────────────────────────────────────────────────────

def build_macro_pillars(report_date: date, fred_api_key: str,
                        economic_events: Optional[Dict[str, Any]] = None
                        ) -> Tuple[Dict[str, Any], Dict[str, str]]:
    """六子块聚合:互不依赖的块并行,每块看门狗超时;单块失败 → errors,不阻塞。

    依赖分批: batch1(并行) par_curve / fred_extras / fomc_calendar / nowcasts
              batch2(依赖 batch1) fred_ranks(需 par_curve 水平)/ fed_odds(需 EFFR)
              batch3(依赖 batch2) next_fomc_odds;auction_events 纯内存即时算。
    网络慢源(Yahoo 逐合约、FRED 翻页)被 PILLAR_TIMEOUT_SEC 硬性兜底,
    Stage 0 总耗时有界。
    """
    from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutTimeout

    pillars: Dict[str, Any] = {}
    errors: Dict[str, str] = {}

    def _run(key: str, fn):
        t0 = time.time()
        try:
            pillars[key] = fn()
            print(f"[macro_pillars] {key} ok in {time.time() - t0:.1f}s",
                  flush=True)
        except Exception as e:  # noqa: BLE001
            errors[key] = str(e)[:300]
            print(f"[macro_pillars] {key} FAILED in {time.time() - t0:.1f}s: "
                  f"{errors[key]}", flush=True)

    def _wait(pool: "ThreadPoolExecutor", futures: Dict[str, Any]) -> None:
        """逐块收结果,看门狗超时只记 errors;shutdown(wait=False) 不被慢线程拖死。"""
        for key, fut in futures.items():
            t0 = time.time()
            try:
                result = fut.result(timeout=PILLAR_TIMEOUT_SEC)
                if key == "nowcasts" and not result:
                    errors[key] = "nowcast pages not parseable (optional)"
                    print(f"[macro_pillars] {key} empty (optional)", flush=True)
                    continue
                pillars[key] = result
                print(f"[macro_pillars] {key} ok in {time.time() - t0:.1f}s",
                      flush=True)
            except FutTimeout:
                errors[key] = f"timeout after {PILLAR_TIMEOUT_SEC}s"
                print(f"[macro_pillars] {key} TIMEOUT", flush=True)
            except Exception as e:  # noqa: BLE001
                errors[key] = str(e)[:300]
                print(f"[macro_pillars] {key} FAILED: {errors[key]}", flush=True)

    # EFFR(DFF)单独快速取 —— fed_odds 只依赖它,不等整个 fred_extras
    def _fetch_effr() -> Optional[float]:
        obs = _fred_observations(fred_api_key, "DFF", report_date, max_obs=5)
        return obs[0][1] if obs else None

    pool = ThreadPoolExecutor(max_workers=8)
    try:
        futures = {
            "par_curve": pool.submit(fetch_par_curve, report_date),
            "fred_extras": pool.submit(fetch_fred_extras, fred_api_key, report_date),
            "fomc_calendar": pool.submit(fetch_fomc_calendar, report_date),
            "nowcasts": pool.submit(fetch_nowcasts),
            "effr": pool.submit(_fetch_effr),
            "release_series": pool.submit(
                fetch_release_series, fred_api_key, report_date),
        }
        # auction_events 纯内存 + 可选 FiscalData 补字段
        _run("auction_events", lambda: _auction_events(economic_events))
        _wait(pool, futures)

        levels = (pillars.get("par_curve") or {}).get("levels_pct", {})
        _wait(pool, {
            "fred_ranks": pool.submit(fetch_fred_ranks, fred_api_key,
                                      report_date, levels),
            "fed_odds": pool.submit(fetch_fed_odds, report_date,
                                    pillars.get("effr")),
        })
    finally:
        # 不 join 慢线程:看门狗已判超时的块不再阻塞 Stage 0 收尾
        pool.shutdown(wait=False, cancel_futures=True)

    def _next_odds():
        meetings = pillars.get("fomc_calendar") or []
        contracts = (pillars.get("fed_odds") or {}).get("contracts") or []
        if not (meetings and contracts):
            raise RuntimeError("fomc_calendar or fed_odds missing")
        nxt = date.fromisoformat(meetings[0]["end"])
        total = nxt.year * 12 + (nxt.month - 1) + 1
        my, mm = total // 12, total % 12 + 1
        month_key = f"{my}-{mm:02d}"
        c = next((x for x in contracts if x["month"] == month_key), None)
        if not c:
            raise RuntimeError(f"no contract for {month_key}")
        out = {
            "meeting": meetings[0]["text"], "contract_month": month_key,
            "odds_pct": c["odds_of_25bp_hike_pct"],
            "cum_bp_priced": c["cum_bp_priced"],
        }
        try:  # prior 对比口径:best-effort,失败不影响当日数
            prior = fetch_prior_contract_odds(
                report_date, pillars.get("effr"), month_key)
            if prior:
                out["prior"] = prior
        except Exception as e:  # noqa: BLE001
            out["prior_error"] = str(e)[:120]
        return out
    _run("next_fomc_odds", _next_odds)

    # nowcasts 网页抓取失败 → 回退经济日历里的 Atlanta Fed GDPNow 行
    if not pillars.get("nowcasts"):
        fb = _nowcasts_from_calendar(economic_events)
        if fb:
            pillars["nowcasts"] = fb
            errors.pop("nowcasts", None)

    return pillars, errors


def _nowcasts_from_calendar(economic_events: Optional[Dict[str, Any]]
                            ) -> Optional[Dict[str, str]]:
    """经济日历(EODHD)含 'Atlanta Fed GDPNow' 行:取最新一条有 actual 的。"""
    if not economic_events:
        return None
    rows = [e for k in ("recent", "upcoming") for e in economic_events.get(k, [])
            if e.get("country") == "US" and "gdpnow" in (e.get("event") or "").lower()
            and e.get("actual") not in (None, "")]
    if not rows:
        return None
    rows.sort(key=lambda e: (e.get("date") or "", e.get("time") or ""))
    last = rows[-1]
    return {"gdpnow_qoq_ann": str(last["actual"]), "gdpnow_as_of": last.get("date"),
            "source": "economic_events (Atlanta Fed GDPNow)"}


def _auction_events(economic_events: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """从快照的经济日历(US)中抽出国债拍卖事件:recent(含 actual=stop)与 upcoming。

    尽量用 Treasury FiscalData 补 size / prior stop(失败则保留日历字段)。
    """
    if not economic_events:
        return {"today": [], "upcoming": []}
    rx = re.compile(r"(note|bond|bill)\s+auction|treasury auction", re.IGNORECASE)
    today, upcoming = [], []
    for e in economic_events.get("recent", []):
        if e.get("country") == "US" and rx.search(e.get("event", "")):
            today.append(dict(e))
    for e in economic_events.get("upcoming", []):
        if e.get("country") == "US" and rx.search(e.get("event", "")):
            upcoming.append(dict(e))
    try:
        _enrich_auctions_from_fiscaldata(today + upcoming)
    except Exception as e:  # noqa: BLE001
        # enrichment is best-effort
        for row in today + upcoming:
            row.setdefault("enrichment_error", str(e)[:120])
    return {"today": today, "upcoming": upcoming}


# CNT Forward Catalysts 拍卖行常用字段:规模 + 上次中标
FISCALDATA_AUCTIONS_URL = (
    "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/"
    "v1/accounting/od/auctions_query"
)


def _enrich_auctions_from_fiscaldata(rows: List[Dict[str, Any]]) -> None:
    """为拍卖事件补 offering_amt / prior auction high_yield(若 API 可用)。"""
    if not rows:
        return
    # 只取 Note/Bond(排除同期限的 TIPS/FRN/Bill 串味),一次拉近一批结果
    params = {
        "fields": "auction_date,security_type,security_term,high_yield,"
                  "offering_amt,allocation_pctage",
        "filter": "security_type:in:(Note,Bond)",
        "sort": "-auction_date",
        "page[size]": 150,
    }
    try:
        data = _get(FISCALDATA_AUCTIONS_URL, params=params).json()
    except Exception:
        return
    def _clean(v: Any) -> Any:
        # FiscalData 用字符串 "null" 表示空值
        return None if v in (None, "", "null") else v

    records = []
    for r in (data.get("data") or []):
        if (r.get("security_type") or "") not in ("Note", "Bond"):
            continue
        records.append({k: _clean(v) for k, v in r.items()})
    if not records:
        return

    def _term_key(s: str) -> str:
        # "7-Year" / "7 Year" / "7-year" → "7year"(连字符与空白都剥掉,两侧同口径)
        return re.sub(r"[\s\-]+", "", (s or "").lower())

    for row in rows:
        name = (row.get("event") or "").lower()
        m = re.search(r"(\d+)[\s\-]*year", name)
        if not m:
            continue
        tenor_key = f"{m.group(1)}year"
        row_date = row.get("date")  # YYYY-MM-DD(UTC 日期)
        same_tenor = [r for r in records
                      if _term_key(r.get("security_term", "")) == tenor_key]
        # 本场(同日已入库则有 offering_amt)与"上一场"(严格早于本场日期的最新一场)
        own = next((r for r in same_tenor if r.get("auction_date") == row_date), None)
        # 上一场必须已有中标收益率(无 high_yield 的是 TIPS 增发/未结算记录,不能当对比口径)
        prior = next((r for r in same_tenor
                      if row_date and (r.get("auction_date") or "") < row_date
                      and r.get("high_yield") is not None), None)
        if not (own or prior):
            continue
        fd: Dict[str, Any] = {}
        if own:
            fd["offering_amt"] = own.get("offering_amt")
            fd["high_yield"] = own.get("high_yield")
        if prior:
            fd["prior_auction_date"] = prior.get("auction_date")
            fd["prior_high_yield"] = prior.get("high_yield")
            if "offering_amt" not in fd:
                fd["prior_offering_amt"] = prior.get("offering_amt")
        row["fiscaldata"] = fd


# 美国一线发布 → FRED series(供 Release 图与 since 断言)
RELEASE_FRED_SERIES = {
    "nonfarm": ("PAYEMS", "level", "Nonfarm Payrolls"),
    "cpi": ("CPIAUCSL", "mom_pct", "CPI"),
    "core_cpi": ("CPILFESL", "mom_pct", "Core CPI"),
    "ppi": ("PPIFIS", "mom_pct", "PPI Final Demand"),
    "retail": ("RSAFS", "mom_pct", "Retail Sales"),
    "durable": ("DGORDER", "mom_pct", "Durable Goods Orders"),
    "housing_starts": ("HOUST", "mom_pct", "Housing Starts"),
    "jolts": ("JTSJOL", "level", "JOLTS Job Openings"),
}


def fetch_release_series(api_key: str, report_date: date) -> Dict[str, Any]:
    """为一线发布拉取近 3 年月度序列 + 近 10 年同比(CNT Release 图数据底座)。"""
    out: Dict[str, Any] = {}
    start_3y = report_date - timedelta(days=365 * 3 + 30)
    for key, (series, mode, label) in RELEASE_FRED_SERIES.items():
        try:
            obs = _fred_observations(api_key, series, report_date)
            if not obs:
                continue
            # obs is newest-first
            recent = [(d, v) for d, v in obs if d >= start_3y]
            recent_asc = list(reversed(recent))
            mom = []
            for i, (d, v) in enumerate(recent_asc):
                if mode == "level":
                    if i == 0:
                        continue
                    prev = recent_asc[i - 1][1]
                    mom.append({"date": d.isoformat(), "value": round(v - prev, 3)})
                else:
                    if i == 0:
                        continue
                    prev = recent_asc[i - 1][1]
                    if prev:
                        mom.append({
                            "date": d.isoformat(),
                            "value": round((v / prev - 1) * 100, 3),
                        })
            # YoY over ~10y
            yoy = []
            by_date = {d: v for d, v in obs}
            for d, v in reversed(obs):
                if d < report_date - timedelta(days=365 * 10):
                    break
                prior = by_date.get(date(d.year - 1, d.month, min(d.day, 28)))
                # fallback: find closest prior year obs
                if prior is None:
                    candidates = [v2 for d2, v2 in obs
                                  if d2.year == d.year - 1 and d2.month == d.month]
                    prior = candidates[0] if candidates else None
                if prior:
                    yoy.append({
                        "date": d.isoformat(),
                        "value": round((v / prior - 1) * 100, 3),
                    })
            yoy = list(reversed(yoy))
            latest = mom[-1] if mom else None
            out[key] = {
                "series": series,
                "label": label,
                "mode": mode,
                "latest": latest,
                "mom_3y": mom[-36:],
                "yoy_10y": yoy[-120:],
            }
        except Exception as e:  # noqa: BLE001
            out[key] = {"series": series, "label": label, "error": str(e)[:200]}
    return out
