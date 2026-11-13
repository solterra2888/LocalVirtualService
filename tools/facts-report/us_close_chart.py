#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""GuanHe-style "US close" overview chart (FT-market-table look) from guanhetech DB.

Replicates the first chart of GuanHe Trading's daily report (大盘总览 section):
a cream-background table grouped into EQUITIES / RATES / CREDIT / COMMODITIES /
FX / VOLATILITY / S&P 500 SECTORS. Each row shows Name (TICKER), Last, a
zero-centred 1-day bar, and 1D/1W/1M/YTD columns. Rows whose |1D| passes the
section threshold (0.75% / 8bp / 8%) are bold; bars past the per-section scale
get an arrowhead; the scale itself is printed at the bar-zone edges of each
group header.

Methodology (reverse-engineered from GuanHe 2026-10-30 chart, verified numerically):
  - Last & 1D quotes      -> finviz_screener snapshot MAX(snapshot_date) <= T+1
                             (ETF/sector rows match GuanHe value-for-value)
  - 5D / 1M windows       -> close 5 trading days back / one calendar month back
                             (Yahoo Finance window conventions, 2026-11-13)
  - ETF/sector W/M/YTD    -> total return from market_data_eod adjusted_close
                             (matches GuanHe incl. dividends: GLD -6.6/USO +15.9)
  - indices/yields/FX/VIX -> market_data_eod close history (EODHD); yields and
                             yields quoted in bp
  - DXY                   -> EODHD DXY.INDX EOD (直接 API),Yahoo 回退;MOVE 无源(页脚注明)
  - CHINA / ASIA & EUROPE -> cash-index EODHD history (market_data_eod)

Usage:
    python tools/US-market-report/us_close_chart.py --workspace backend/reports/pipeline/2026-10-30
    python tools/US-market-report/us_close_chart.py --date 2026-10-30 --out /tmp/us_close.png
"""

from __future__ import annotations

import argparse
import calendar
import json
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import os
import re
import tempfile

os.environ.setdefault("MPLCONFIGDIR", tempfile.mkdtemp(prefix="mplconfig-"))

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.patches import FancyBboxPatch, Polygon, Rectangle  # noqa: E402
import requests  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from backend.database.db_manager import DatabaseManager  # noqa: E402
from backend.utils.database_config import get_database_config  # noqa: E402

# ---------------------------------------------------------------- canvas spec
W, H_BASE, DPI = 1440, 2393, 100  # pixel canvas == data coords at dpi=100
BG, INK = "#fbf7f0", "#16130f"
GRAY, GRAY_LT = "#6b6863", "#8a857c"
GREEN, RED = "#0d7757", "#b63a26"
BAND, RULE, ZERO_LINE = "#f3eee2", "#d8d2c5", "#cfc9bb"
ROW_RULE = "#e9e5de"  # hairline separator at each row bottom (GuanHe style)

M_L, M_R = 64, 1376
NAME_X, LAST_R = 64, 560
BAR_L, BAR_R = 596, 908
BAR_CX = (BAR_L + BAR_R) / 2
C1D, C1W, C1M, CYTD = 1005, 1100, 1195, 1350

TITLE_Y, SUB_Y, COLHDR_Y = 66, 104, 146
HEAD_H, SECTION_GAP, FOOTER_Y = 44, 10, 40
ROW_H_MIN, ROW_H_MAX, BAR_H = 46, 60, 20

FINVIZ_ETFS = ("USO", "GLD", "SLV", "CPER")
ETF_DB_CODE = {"USO": "USO.US", "GLD": "GLD.US",
               "SLV": "SLV.US", "CPER": "CPER.US"}
ETF_YAHOO: Dict[str, str] = {}  # EEM/FXI rows removed (superseded by CHINA group)
SECTOR_ETFS = ("XLB", "XLC", "XLE", "XLF", "XLI", "XLK", "XLP", "XLRE", "XLU", "XLV", "XLY")
SECTOR_DB_CODE = {tk: tk + ".US" for tk in SECTOR_ETFS}
SECTOR_NAMES = {
    "XLE": "Energy", "XLF": "Financial", "XLB": "Basic Materials",
    "XLRE": "Real Estate", "XLU": "Utilities", "XLP": "Consumer Defensive",
    "XLV": "Healthcare", "XLY": "Consumer Cyclical",
    "XLC": "Communication Services", "XLI": "Industrials",
    "XLK": "Technology",
}
UA = {"User-Agent": "Mozilla/5.0 guanhe-report-pipeline-charts"}
CJK_FONT = ["WenQuanYi Zen Hei", "DejaVu Sans"]  # 中文主字体,数学负号等缺字回落 DejaVu
FOOTER_NOTES: List[str] = []  # filled by build_sections (which source was live)


def _minus(s: str) -> str:
    return s.replace("-", "\u2212")


def _f_pct1(v: Optional[float]) -> str:
    return "\u2013" if v is None else _minus(f"{v:+.1f}%")


def _f_pct2(v: Optional[float]) -> str:
    return "\u2013" if v is None else _minus(f"{v:+.2f}%")


def _f_bp(v: Optional[float]) -> str:
    if v is None:
        return "\u2013"
    r = round(v)
    sign = "+" if (r > 0 or (r == 0 and v >= 0)) else "\u2212"
    return f"{sign}{abs(r)}bp"


# ------------------------------------------------------------------ change math
def _ref_trading(s: List[Tuple[date, float]], n: int) -> Optional[float]:
    """n 个交易日前的收盘(降序序列,最新为第 0 根)。Yahoo Finance 5D 口径。"""
    return s[n][1] if len(s) > n else None


def _month_back(d: date) -> date:
    """d 减一个自然月(末日溢出取当月最后一日),如 3-31 → 2-28。"""
    y, m = (d.year, d.month - 1) if d.month > 1 else (d.year - 1, 12)
    return date(y, m, min(d.day, calendar.monthrange(y, m)[1]))


def _ref_month(s: List[Tuple[date, float]]) -> Optional[float]:
    """一个自然月前(含)的最近收盘。Yahoo Finance 1M 口径
    (2026-11-13 定案:T-30 与减自然月在 31 天月份差一天,MU 该日 -3.7%
    造成 3pp 可见差异;对齐 Yahoo)。"""
    target = _month_back(s[0][0])
    return next((c for d, c in s if d <= target), None)


def _changes(s: List[Tuple[date, float]], pct: bool = True) -> Dict[str, Optional[float]]:
    """1D (prev close) / 5D (5 交易日) / 1M (减一个自然月) / YTD (上年末收盘)。

    窗口口径对齐 Yahoo Finance 页面(2026-11-13 用户决策);
    YTD 本就是行业标准(上年末),与 Yahoo 一致无需改。
    """
    if len(s) < 2:
        return {"m1d": None, "m1w": None, "m1m": None, "ytd": None}

    def chg(ref: Optional[float]) -> Optional[float]:
        if ref in (None, 0):
            return None
        return (s[0][1] / ref - 1) * 100 if pct else (s[0][1] - ref) * 100

    ytd_ref = _ytd_ref(s)
    return {"m1d": chg(s[1][1]), "m1w": chg(_ref_trading(s, 5)),
            "m1m": chg(_ref_month(s)), "ytd": chg(ytd_ref)}


def _ytd_ref(s: List[Tuple[date, float]]) -> Optional[float]:
    """上年最后一个收盘(YTD 参考价)。"""
    return next((c for d, c in s if d.year < s[0][0].year), None)


def _refs(s: Optional[List[Tuple[date, float]]]) -> Optional[Dict[str, Optional[float]]]:
    """实时覆盖用的周期参考价(5日/1月/YTD)+ 迷你收盘序列,与 _changes 同窗口口径。

    s = 最近 45 根收盘(升序):服务端按**行情实际日期**现算 5日/1月窗口 ——
    港/A 股收盘早于查看时刻,实时锚点比快照 T 日新,窗口须随锚点平移;
    w1/m1/ytd 标量作旧 spec 兼容回退。序列不足两根时返回 None。
    """
    if not s or len(s) < 2:
        return None
    return {"w1": _ref_trading(s, 5), "m1": _ref_month(s), "ytd": _ytd_ref(s),
            "s": [[d.isoformat(), round(c, 4)] for d, c in s[:45][::-1]]}


# 实时行情代码覆盖(2026-11-13):EODHD realtime 对这些代码闭市恒 NA(沪市系
# 指数/日经/富时/罗素),降级 market_data_eod 会拿到 14:00 盘中入库行而非收盘
# 终值(上证实测 3642.63 vs 终值 3634.2)→ 改走 Yahoo chart(收盘后即终值;
# 与 ingest 侧 YAHOO_CHART_CODES 的经验一致)
_LIVE_YAHOO = {
    "SSEC.INDX": "YAHOO:000001.SS",
    "SSE50.INDX": "YAHOO:000016.SS",
    "CSI300.INDX": "YAHOO:000300.SS",
    "CSI500.INDX": "YAHOO:000905.SS",
    "CSI1000.INDX": "YAHOO:000852.SS",
    "N225.INDX": "YAHOO:^N225",
    "FTSE.INDX": "YAHOO:^FTSE",
    "RUT.INDX": "YAHOO:^RUT",
}


# ------------------------------------------------------------------- DB layer
def _series(conn: Any, code: str, t: date, column: str = "close",
            limit: int = 340) -> List[Tuple[date, float]]:
    """(date, value) desc for code, up to t. column: close | adjusted_close."""
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT date, {column} FROM market_data_eod WHERE code=%s AND date<=%s "
            f"AND {column} IS NOT NULL ORDER BY date DESC LIMIT %s", (code, t, limit))
        return [(r[0], float(r[1])) for r in cur.fetchall()]


def _series_ohlc(conn: Any, code: str, t: date,
                 limit: int = 260) -> Optional[List[Tuple[date, float, float, float, float]]]:
    """(date, open, high, low, close) 降序,最新在前;供网页蜡烛图。
    OHLC 任一为空的行跳过;整段缺失返回 None(调用方回退收盘折线)。"""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT date, open, high, low, close FROM market_data_eod "
            "WHERE code=%s AND date<=%s AND close IS NOT NULL "
            "AND open IS NOT NULL AND high IS NOT NULL AND low IS NOT NULL "
            "ORDER BY date DESC LIMIT %s", (code, t, limit))
        rows = cur.fetchall()
    if not rows:
        return None
    return [(r[0], float(r[1]), float(r[2]), float(r[3]), float(r[4])) for r in rows]


def _yahoo_ohlc(symbol: str, t: date,
                days: int = 340) -> Optional[List[Tuple[date, float, float, float, float]]]:
    """Yahoo chart API 的 OHLC(DXY 的回退源,EODHD 优先见 _eodhd_eod);失败返回 None。"""
    p2 = int(datetime.combine(t + timedelta(days=1), datetime.min.time()).timestamp())
    try:
        r = requests.get(
            f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}",
            params={"period1": p2 - days * 86400, "period2": p2, "interval": "1d"},
            headers=UA, timeout=15)
        res = ((r.json().get("chart") or {}).get("result") or [None])[0]
        ts = (res or {}).get("timestamp") or []
        q = ((res or {}).get("indicators") or {}).get("quote") or [{}]
        o, h = q[0].get("open") or [], q[0].get("high") or []
        l, c = q[0].get("low") or [], q[0].get("close") or []
        out = []
        for i in range(min(len(ts), len(o), len(h), len(l), len(c))):
            d = datetime.fromtimestamp(ts[i]).date()
            if d > t or o[i] is None or h[i] is None or l[i] is None or c[i] is None:
                continue
            out.append((d, float(o[i]), float(h[i]), float(l[i]), float(c[i])))
        out.sort(key=lambda x: x[0], reverse=True)
        return out[:260] or None
    except Exception:  # noqa: BLE001
        return None


def _finviz_rows(conn: Any, t: date) -> Tuple[Dict[str, Dict[str, Any]], date]:
    """ETF rows from the newest finviz_screener snapshot frozen at/before T+1.

    finviz_screener 是滚动表(仅保留最新一份快照)。历史工作区重渲(快照比
    T+1 新)或当日快照尚未刷新(比 T+1 旧)时,回退到库中最新快照 —— ETF 行
    本就是"快照时点状态",接受此日期偏移,避免整表渲染失败。
    """
    with conn.cursor() as cur:
        cur.execute("SELECT MAX(snapshot_date) FROM finviz_screener WHERE snapshot_date<=%s",
                    (t + timedelta(days=1),))
        snap = cur.fetchone()[0]
        if snap is None:
            cur.execute("SELECT MAX(snapshot_date) FROM finviz_screener")
            snap = cur.fetchone()[0]
            if snap is None:
                raise RuntimeError("no finviz_screener snapshot on/before T+1")
            print(f"[us_close] finviz snapshot {snap} outside T+1 window — relaxed fallback")
        wanted = FINVIZ_ETFS + SECTOR_ETFS
        cur.execute(
            "SELECT ticker, price, change_percent, performance_week, "
            "performance_month, performance_ytd FROM finviz_screener "
            "WHERE snapshot_date=%s AND ticker = ANY(%s)", (snap, list(wanted)))
        out: Dict[str, Dict[str, Any]] = {}
        for tk, price, chg, pw, pm, py in cur.fetchall():
            def _num(x: Any) -> Optional[float]:
                if x is None:
                    return None
                try:
                    return float(str(x).replace("%", "").replace("+", ""))
                except ValueError:
                    return None
            out[tk] = {"price": float(price), "m1d": _num(chg),
                       "m1w": _num(pw), "m1m": _num(pm), "ytd": _num(py)}
        missing = [tk for tk in wanted if tk not in out]
        if missing:
            print(f"[us_close] finviz snapshot {snap} missing tickers: {missing}")
        return out, snap


def _fred_yields(t: date) -> Optional[Dict[str, List[Tuple[date, float]]]]:
    """Treasury par yields (DGS2/10/30) from FRED public CSV; None on failure.

    Fetched per-series with cosd=t-400d (fast). May end before T
    (publication lags a few hours after the ET close) — the caller splices
    T from the Treasury par curve or falls back to EODHD.
    """
    out: Dict[str, List[Tuple[date, float]]] = {}
    cosd = (t - timedelta(days=400)).isoformat()
    for sid in ("DGS2", "DGS10", "DGS30"):
        s: List[Tuple[date, float]] = []
        for attempt in (1, 2):
            try:
                r = requests.get("https://fred.stlouisfed.org/graph/fredgraph.csv",
                                 params={"id": sid, "cosd": cosd},
                                 headers={"User-Agent": "curl/8.5.0"},  # FRED CDN throttles python-requests browser UAs
                                 timeout=20)
                for ln in r.text.splitlines()[1:]:
                    d_str, _, v = ln.partition(",")
                    if not v or v == ".":
                        continue
                    d = date.fromisoformat(d_str)
                    if d <= t:
                        s.append((d, float(v)))
                break
            except Exception as e:  # noqa: BLE001
                if attempt == 2:
                    print(f"[us_close] FRED {sid} failed: {e}")
        if not s:
            return None
        s.sort(key=lambda x: x[0], reverse=True)
        out[sid] = s
    return out


def _yahoo_series(symbol: str, t: date, adjusted: bool = True,
                  days: int = 340) -> Optional[List[Tuple[date, float]]]:
    p2 = int(datetime.combine(t + timedelta(days=1), datetime.min.time()).timestamp())
    try:
        r = requests.get(
            f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}",
            params={"period1": p2 - days * 86400, "period2": p2, "interval": "1d"},
            headers=UA, timeout=15)
        res = ((r.json().get("chart") or {}).get("result") or [None])[0]
        ts = (res or {}).get("timestamp") or []
        ind = (res or {}).get("indicators") or {}
        key = "adjclose"
        vals = (ind.get(key) or [{}])[0].get(key) if adjusted else None
        if not vals:
            vals = ((ind.get("quote") or [{}])[0].get("close")) or []
        s = sorted(((datetime.fromtimestamp(ts[i]).date(), float(vals[i]))
                    for i in range(min(len(ts), len(vals)))
                    if vals[i] is not None and datetime.fromtimestamp(ts[i]).date() <= t),
                   key=lambda x: x[0], reverse=True)
        return s or None
    except Exception:  # noqa: BLE001
        return None


def _eodhd_token() -> str:
    """EODHD token:env 优先,回退解析仓库 .env(与 build_snapshot._eodhd_api 同模式)。"""
    tok = os.environ.get("EODHD_API_TOKEN", "")
    if not tok:
        env_path = ROOT / ".env"
        if env_path.exists():
            m = re.search(r"^EODHD_API_TOKEN=(.+)$",
                          env_path.read_text(encoding="utf-8"), re.MULTILINE)
            if m:
                tok = m.group(1).strip()
    return tok


def _eodhd_eod(code: str, t: date,
               days: int = 400) -> Tuple[Optional[List[Tuple[date, float]]],
                                         Optional[List[Tuple[date, float, float, float, float]]]]:
    """EODHD EOD 端点日线,一次调用同时产出收盘序列与 OHLC(均降序,≤t)。

    DXY 用:EODHD 有 DXY.INDX 且当日收盘出数早于 Yahoo 的 DX-Y.NYB 终值
    (2026-11-13 实测:渲染时刻 21:00 ET EODHD 已有当日值、Yahoo 尚未出);
    需要 ~340 天历史算 1W/1M/YTD,故直连 API 而不依赖 market_data_eod 的
    增量入库。任何异常返回 (None, None),由调用方回退 Yahoo。
    """
    tok = _eodhd_token()
    if not tok:
        return None, None
    try:
        r = requests.get(
            f"https://eodhd.com/api/eod/{code}",
            params={"from": (t - timedelta(days=days)).isoformat(),
                    "to": t.isoformat(), "api_token": tok, "fmt": "json"},
            timeout=90)
        r.raise_for_status()
        rows = r.json()
        if not isinstance(rows, list) or not rows:
            return None, None
        series: List[Tuple[date, float]] = []
        ohlc: List[Tuple[date, float, float, float, float]] = []
        for row in sorted(rows, key=lambda x: x.get("date", ""), reverse=True):
            d = date.fromisoformat(row["date"])
            if d > t or row.get("close") is None:
                continue
            series.append((d, float(row["close"])))
            if all(row.get(k) is not None for k in ("open", "high", "low")):
                ohlc.append((d, float(row["open"]), float(row["high"]),
                             float(row["low"]), float(row["close"])))
        return (series or None), (ohlc or None)
    except Exception:  # noqa: BLE001
        return None, None


# --------------------------------------------------------------- row assembly
def _row(name: str, ticker: str, last_str: str, unit: str,
         ch: Dict[str, Optional[float]], last: Optional[float] = None,
         as_of: Optional[date] = None, src: Optional[str] = None,
         hist: Optional[List[Tuple[date, float]]] = None,
         code: Optional[str] = None,
         refs: Optional[Dict[str, Optional[float]]] = None) -> Dict[str, Any]:
    """as_of = 该行收盘值的实际日期(新鲜度台账,md2html 据此提示过期);
    src='finviz' 的行按快照日期宽松判定(滚动快照常比 T 早/晚一天,属正常)。
    hist = 日线序列(降序,最新在前) → 存为升序最多 260 根。两种元素:
    (date, close)          → [iso, c]         折线(如 FRED 利率,无 OHLC)
    (date, o, h, l, close) → [iso, o, h, l, c] 蜡烛(DMR 风格,market_data_eod 有 OHLC)
    供网页版走势 sparkline(近3月)与点击放大交互图(1M/3M/6M/1Y + MA)使用。
    code = 实时行情代码(EODHD realtime,如 GSPC.INDX;DXY 用 YAHOO:DX-Y.NYB
    前缀走 Yahoo),register 存入元数据,报告打开时 /shared/{token}/quotes
    据此就地刷新收盘与涨跌;refs = 周期参考价(见 _refs)。"""
    h = [[d.isoformat()] + [round(float(x), 6) for x in rest]
         for row in list(hist or [])[:260][::-1]
         for d, *rest in [row]]
    return {"name": name, "ticker": ticker, "last": last, "last_str": last_str,
            "unit": unit, "as_of": as_of.isoformat() if as_of else None,
            "src": src, "hist": h,
            "code": _LIVE_YAHOO.get(code, code), "refs": refs, **ch}


_PAR_TENOR = {"DGS2": "2y", "DGS10": "10y", "DGS30": "30y"}


def _splice_par(fred: Optional[Dict[str, List[Tuple[date, float]]]], t: date,
                par_curve: Optional[Dict[str, Any]]) -> Optional[str]:
    """FRED 缺 T 时用快照的财政部 par curve 补 T 日值(与正文同源),历史仍走 FRED。

    原地修改 fred;返回脚注来源说明,无法拼接时返回 None。
    """
    if not fred or not par_curve or par_curve.get("as_of") != t.isoformat():
        return None
    levels = par_curve.get("levels_pct") or {}
    if any(levels.get(_PAR_TENOR[sid]) is None for sid in fred):
        return None
    for sid, s in fred.items():
        if s[0][0] != t:
            s.insert(0, (t, float(levels[_PAR_TENOR[sid]])))
    return "美债收益率当日值来自美国财政部收益率曲线,历史来自 FRED"


def build_sections(conn: Any, t: date,
                   par_curve: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    FOOTER_NOTES.clear()
    fv, snap = _finviz_rows(conn, t)
    print(f"[us_close] finviz snapshot {snap} (frozen at/after {t} close)")

    def idx_row(name: str, ticker: str, code: str) -> Dict[str, Any]:
        s = _series(conn, code, t)
        ohlc = _series_ohlc(conn, code, t)
        return _row(name, ticker, _minus(f"{s[0][1]:,.2f}") if s else "\u2013", "%",
                    _changes(s), s[0][1] if s else None,
                    as_of=s[0][0] if s else None, hist=ohlc or s,
                    code=code, refs=_refs(s))

    def etf_row(name: str, ticker: str, perf_source: str = "tr") -> Optional[Dict[str, Any]]:
        """Last/1D from finviz quote; 1W/1M/YTD via perf_source:
        "tr"     total return from adjusted closes — DB first, Yahoo best-effort,
                 finviz perf columns as last resort (GuanHe credit/commodities style)
        "finviz" finviz performance columns verbatim (GuanHe sector-ETF style)
        """
        db_code = ETF_DB_CODE.get(ticker) or SECTOR_DB_CODE.get(ticker)
        s_adj = _series(conn, db_code, t, column="adjusted_close") if db_code else None
        if not s_adj and ticker in ETF_YAHOO:
            s_adj = _yahoo_series(ETF_YAHOO[ticker], t)
        ch = _changes(s_adj) if s_adj else {"m1d": None, "m1w": None, "m1m": None, "ytd": None}
        q = fv.get(ticker)
        last = q["price"] if q else (s_adj[0][1] if s_adj else None)
        if last is None:
            return None
        if q and q["m1d"] is not None:
            ch["m1d"] = q["m1d"]
        elif s_adj and len(s_adj) > 1 and ch["m1d"] is None:
            ch["m1d"] = (s_adj[0][1] / s_adj[1][1] - 1) * 100
        if perf_source == "finviz" and q:
            for src, dst in (("m1w", "m1w"), ("m1m", "m1m"), ("ytd", "ytd")):
                if q.get(src) is not None:
                    ch[dst] = q[src]
        ohlc = _series_ohlc(conn, db_code, t) if db_code else None
        return _row(name, ticker, _minus(f"{last:.2f}"), "%", ch, last,
                    as_of=snap, src="finviz", hist=ohlc or s_adj,
                    code=db_code, refs=_refs(s_adj))

    # ---- EQUITIES (US indices from EODHD history; 英文表述,FT 原版风格)
    eq = [idx_row("S&P 500", "SPX", "GSPC.INDX"),
          idx_row("Nasdaq 100", "NDX", "NDX.INDX"),
          idx_row("Dow Jones", "DJIA", "DJI.INDX"),
          idx_row("Russell 2000", "RUT", "RUT.INDX"),
          idx_row("PHLX Semiconductor", "SOX", "SOX.INDX")]

    # ---- CHINA (cash benchmarks first, then CFFEX index futures shown via
    # their underlying cash index — same convention as the Reports module's
    # Market Temperature cards)
    china = [idx_row("上证综指", "000001.SH", "SSEC.INDX"),
             idx_row("深证成指", "399001.SZ", "399001.SHE"),
             idx_row("恒生指数", "HSI", "HSI.INDX"),
             idx_row("创业板指", "399006.SZ", "399006.SHE"),
             idx_row("上证50", "IH", "SSE50.INDX"),
             idx_row("沪深300", "IF", "CSI300.INDX"),
             idx_row("中证500", "IC", "CSI500.INDX"),
             idx_row("中证1000", "IM", "CSI1000.INDX")]

    # ---- ASIA & EUROPE (major benchmark indices; 英文表述,中国组除外)
    intl = [idx_row("Nikkei 225", "N225", "N225.INDX"),
            idx_row("KOSPI", "KOSPI", "KS11.INDX"),
            idx_row("FTSE 100", "FTSE", "FTSE.INDX"),
            idx_row("DAX", "DAX", "GDAXI.INDX")]

    # ---- RATES (Treasury par yields: FRED, T spliced from the snapshot's
    # Treasury curve when FRED lags; EODHD only when neither covers T)
    fred = _fred_yields(t)
    note = "美债收益率来自 FRED"
    if fred and any(s[0][0] != t for s in fred.values()):
        note = _splice_par(fred, t, par_curve)
        if note is None:
            fred = None
    if fred:
        print(f"[us_close] rates: {note}")
        FOOTER_NOTES.append(note)
        rates = []
        series = {"US 2Y Treasury": ("2Y", "US2Y.INDX", fred["DGS2"]),
                  "US 10Y Treasury": ("10Y", "US10Y.INDX", fred["DGS10"]),
                  "US 30Y Treasury": ("30Y", "US30Y.INDX", fred["DGS30"])}
        for nm, (tk, rcode, s) in series.items():
            rates.append(_row(nm, tk, _minus(f"{s[0][1]:.2f}%"), "bp",
                              _changes(s, pct=False), s[0][1], as_of=s[0][0],
                              hist=s, code=rcode, refs=_refs(s)))
    else:
        print("[us_close] rates: FRED and Treasury curve lack T — falling back to EODHD yields")
        FOOTER_NOTES.append("美债收益率来自 EODHD(FRED 与财政部曲线均未覆盖当日)")

        def rate_row(name: str, ticker: str, code: str) -> Dict[str, Any]:
            s = _series(conn, code, t)
            ohlc = _series_ohlc(conn, code, t)
            return _row(name, ticker, _minus(f"{s[0][1]:.2f}%") if s else "\u2013", "bp",
                        _changes(s, pct=False), s[0][1] if s else None,
                        as_of=s[0][0] if s else None, hist=ohlc or s,
                        code=code, refs=_refs(s))

        rates = [rate_row("US 2Y Treasury", "2Y", "US2Y.INDX"),
                 rate_row("US 10Y Treasury", "10Y", "US10Y.INDX"),
                 rate_row("US 30Y Treasury", "30Y", "US30Y.INDX")]

    comm = [r for r in (etf_row("Crude Oil", "USO"), etf_row("Gold", "GLD"),
                        etf_row("Silver", "SLV"), etf_row("Copper", "CPER")) if r]

    # ---- FX (DXY: EODHD DXY.INDX 优先,Yahoo 回退; EUR/USD & USD/JPY from EODHD)
    fx: List[Dict[str, Any]] = []
    dxy, dohlc = _eodhd_eod("DXY.INDX", t)
    if dxy:
        print("[us_close] DXY via EODHD DXY.INDX")
    else:
        # EODHD 失败(配额/网络)才回退 Yahoo;注意 Yahoo 的 DX-Y.NYB 当日
        # 终值发布偏晚(美东晚间),渲染早于出数时会落入 stale 台账(横幅+告警)
        dxy = _yahoo_series("DX-Y.NYB", t, adjusted=False)
        dohlc = _yahoo_ohlc("DX-Y.NYB", t)
        print("[us_close] DXY EODHD unavailable — Yahoo fallback")
    if dxy:
        # code 保持 YAHOO 前缀:实时刷新(/shared quotes)仍走 Yahoo —— EODHD
        # realtime 对 DXY 闭市返回 NA,Yahoo 盘中有实时价
        fx.append(_row("US dollar", "DXY", _minus(f"{dxy[0][1]:.2f}"), "%",
                       _changes(dxy), dxy[0][1], as_of=dxy[0][0], hist=dohlc or dxy,
                       code="YAHOO:DX-Y.NYB", refs=_refs(dxy)))
    else:
        print("[us_close] DXY unavailable via both EODHD and Yahoo — row skipped")

    def fx_row(name: str, ticker: str, code: str) -> Dict[str, Any]:
        s = _series(conn, code, t)
        ohlc = _series_ohlc(conn, code, t)
        return _row(name, ticker, _minus(f"{s[0][1]:.2f}") if s else "\u2013", "%",
                    _changes(s), s[0][1] if s else None,
                    as_of=s[0][0] if s else None, hist=ohlc or s,
                    code=code, refs=_refs(s))

    fx += [fx_row("EUR/USD", "EURUSD", "EURUSD.FOREX"),
           fx_row("USD/JPY", "USDJPY", "USDJPY.FOREX")]

    # ---- VOLATILITY (VIX from EODHD; MOVE not available in our stack)
    sv = _series(conn, "VIX.INDX", t)
    vohlc = _series_ohlc(conn, "VIX.INDX", t)
    vol = [_row("VIX Volatility Index", "", _minus(f"{sv[0][1]:.2f}") if sv else "\u2013", "%",
                _changes(sv), sv[0][1] if sv else None,
                as_of=sv[0][0] if sv else None, hist=vohlc or sv,
                code="VIX.INDX", refs=_refs(sv))]

    # ---- S&P 500 SECTORS (finviz quotes + finviz perf columns, sorted by 1D desc)
    sectors = sorted((r for r in (etf_row(SECTOR_NAMES[tk], tk, perf_source="finviz")
                                  for tk in SECTOR_ETFS) if r),
                     key=lambda r: -(r["m1d"] or 0))

    sections = [
        {"title": "美国", "scale": 5.0, "scale_lbl": "\u22125% / +5%", "unit": "%", "rows": eq},
        {"title": "中国", "scale": 5.0, "scale_lbl": "\u22125% / +5%", "unit": "%", "rows": china},
        {"title": "亚洲与欧洲", "scale": 5.0, "scale_lbl": "\u22125% / +5%", "unit": "%", "rows": intl},
        {"title": "利率", "scale": 10.0, "scale_lbl": "\u221210bp / +10bp", "unit": "bp", "rows": rates},
        {"title": "商品", "scale": 5.0, "scale_lbl": "\u22125% / +5%", "unit": "%", "rows": comm},
        {"title": "外汇", "scale": 5.0, "scale_lbl": "\u22125% / +5%", "unit": "%", "rows": fx},
        {"title": "波动率", "scale": 15.0, "scale_lbl": "\u221215% / +15%", "unit": "%", "rows": vol},
        {"title": "标普500板块", "scale": 5.0, "scale_lbl": "\u22125% / +5%", "unit": "%", "rows": sectors},
    ]
    return [s for s in sections if s["rows"]]


# ------------------------------------------------------------------ rendering
def render(sections: List[Dict[str, Any]], t: date, out: Path) -> Path:
    n_rows, n_sec = sum(len(s["rows"]) for s in sections), len(sections)
    # canvas grows with content: H is the CNT-look base height, but never
    # lets row_h fall below ROW_H_MIN (9 sections / 38 rows overflowed the
    # fixed 2393px canvas and clipped the last S&P sector rows + footer)
    H = max(H_BASE,
            80 + COLHDR_Y + 26 + n_sec * (HEAD_H + SECTION_GAP)
            + ROW_H_MIN * n_rows)
    row_h = int(max(ROW_H_MIN, min(ROW_H_MAX,
                  (H - 80 - COLHDR_Y - 26 - n_sec * (HEAD_H + SECTION_GAP)) / n_rows)))

    fig = plt.figure(figsize=(W / DPI, H / DPI), dpi=DPI)
    fig.patch.set_facecolor(BG)
    ax = fig.add_axes([0, 0, 1, 1])
    ax.set_xlim(0, W)
    ax.set_ylim(0, H)
    ax.axis("off")
    ax.set_facecolor(BG)

    def Y(top_down: float) -> float:
        return H - top_down

    def txt(x: float, y_td: float, s: str, size: float, color: str = INK,
            ha: str = "left", weight: str = "normal", style: str = "normal") -> Any:
        return ax.text(x, Y(y_td), s, fontsize=size, color=color, ha=ha, va="center",
                       fontweight=weight, fontstyle=style, family=CJK_FONT)

    txt(NAME_X, TITLE_Y,
        f"Markets · {t:%A, %B %d, %Y}", 19, INK, weight="bold")
    txt(LAST_R, COLHDR_Y, "最新", 9.5, GRAY, ha="right")
    txt(BAR_CX, COLHDR_Y, "单日变动", 9.5, GRAY, ha="center")
    for cx, lab in ((C1D, "当日"), (C1W, "5日"), (C1M, "1月"), (CYTD, "年初至今")):
        txt(cx, COLHDR_Y, lab, 9.5, GRAY, ha="right")

    name_texts = []  # (text_obj, ticker, y) for 2nd-pass ticker placement
    y = COLHDR_Y + 26
    for sec in sections:
        unit, scale = sec["unit"], sec["scale"]
        cy = y + HEAD_H / 2
        txt(NAME_X, cy, sec["title"], 11, GRAY, weight="bold")
        txt(BAR_L - 6, cy, sec["scale_lbl"].split(" / ")[0], 8, GRAY_LT, ha="right")
        txt(BAR_R + 6, cy, sec["scale_lbl"].split(" / ")[1], 8, GRAY_LT)
        ax.plot([M_L, M_R], [Y(cy + HEAD_H / 2 - 7)] * 2, color=RULE, lw=0.9)

        row_top = y + HEAD_H
        n = len(sec["rows"])
        sec_bot = row_top + n * row_h
        ax.plot([BAR_CX, BAR_CX], [Y(row_top + 5), Y(sec_bot - 5)],
                color=ZERO_LINE, lw=1.1, zorder=2)
        for i, r in enumerate(sec["rows"]):
            ry = row_top + i * row_h + row_h / 2
            if i:  # hairline separator above every row except the first
                ax.plot([M_L, M_R], [Y(ry - row_h / 2)] * 2, color=ROW_RULE, lw=0.7, zorder=1)
            thr = 8 if unit == "bp" else (8.0 if sec["title"] == "VOLATILITY" else 0.75)
            bold = abs(r["m1d"] or 0) >= thr
            name_texts.append((txt(NAME_X, ry, r["name"], 12, INK,
                                   weight="bold" if bold else "normal"), r["ticker"], ry))
            txt(LAST_R, ry, r["last_str"], 11.5, INK, ha="right")
            fmt_1d = _f_pct2 if unit == "%" else _f_bp
            fmt_rest = _f_pct1 if unit == "%" else _f_bp
            for cx, v, k in zip((C1D, C1W, C1M, CYTD), (r["m1d"], r["m1w"], r["m1m"], r["ytd"]), range(4)):
                color = GREEN if (v or 0) > 0 else RED if (v or 0) < 0 else GRAY
                txt(cx, ry, (fmt_1d if k == 0 else fmt_rest)(v), 11 if k else 11.5,
                    color, ha="right", weight="bold" if (k == 0 and bold) else "normal")
            v = r["m1d"] or 0
            half = (BAR_R - BAR_L) / 2
            length = min(abs(v) / scale, 1.0) * half
            if length > 0.5:
                x0 = BAR_CX if v >= 0 else BAR_CX - length
                color = GREEN if v >= 0 else RED
                if length < 12:  # tiny move -> plain thin bar (rounded box would degenerate)
                    ax.add_patch(Rectangle((x0, Y(ry + BAR_H / 2)), max(length, 2.5), BAR_H,
                                           facecolor=color, edgecolor="none", zorder=3))
                else:
                    ax.add_patch(FancyBboxPatch(
                        (x0, Y(ry + BAR_H / 2)), length, BAR_H,
                        boxstyle=f"round,pad=0,rounding_size={min(4, length / 6)}",
                        facecolor=color, edgecolor="none", zorder=3))
                if abs(v) > scale:  # past scale -> arrowhead
                    tip, sgn = (BAR_R, 1) if v > 0 else (BAR_L, -1)
                    ax.add_patch(Polygon(
                        [(tip, Y(ry + 11)), (tip, Y(ry - 11)), (tip + sgn * 13, Y(ry))],
                        closed=True, facecolor=color, edgecolor="none", zorder=3))
        y = sec_bot + SECTION_GAP

    fig.canvas.draw()
    renderer = fig.canvas.get_renderer()
    for t_obj, ticker, ry in name_texts:
        if ticker:
            txt(t_obj.get_window_extent(renderer).x1 / DPI + 9, ry, ticker, 9, GRAY_LT)

    rates_note = f" ({'; '.join(FOOTER_NOTES)})" if FOOTER_NOTES else ""
    ax.plot([M_L, M_R], [Y(H - 70)] * 2, color=RULE, lw=0.9)
    txt(NAME_X, H - 52,
        f"来源: Finviz(延迟行情) · EODHD(指数/外汇/VIX/ETF 复权价){rates_note}"
        " · DXY: Yahoo Finance.",
        8, GRAY_LT, style="italic")
    txt(NAME_X, H - 32,
        f"MOVE index unavailable. Last updated {t:%A, %B %d, %Y} \u00b7 "
        "GuanHe Daily Report.",
        8, GRAY_LT, style="italic")

    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, dpi=DPI, facecolor=BG)
    plt.close(fig)
    return out


def _dump_ft_json(ws: Path, t: date, sections: List[Dict[str, Any]]) -> Path:
    """FT 表格数据落盘 {ws}/final/charts/ft_sections.json,供 md2html_report
    生成网页版跨资产表 —— 与 01_markets.png 同源同批、随管线每日刷新。
    (md2html 曾读一次性脚本捕获的静态副本,数据日期会滞后,已改为优先工作区。)

    同时输出新鲜度台账 stale:严格组(指数/收益率/外汇/波动率,EODHD/FRED/Yahoo
    官方日收盘)要求 as_of == t;finviz 组(src='finviz')只要求快照日期 >= t
    (滚动快照本来就取"最新时点",常有 ±1 天北京日期偏移)。stale 非空时由
    md2html 在表上方渲染提醒横幅、register 记入元数据并推 Lark 告警。
    """
    strict = [{"name": r["name"], "as_of": r.get("as_of")}
              for s in sections for r in s["rows"]
              if r.get("src") != "finviz" and r.get("as_of") != t.isoformat()]
    fv = [{"name": r["name"], "as_of": r.get("as_of")}
          for s in sections for r in s["rows"]
          if r.get("src") == "finviz" and (r.get("as_of") or "") < t.isoformat()]
    stale = strict + fv
    p = ws / "final" / "charts" / "ft_sections.json"
    p.write_text(json.dumps(
        {"t": t.isoformat(), "sections": sections, "stale": stale,
         "generated_at": datetime.now().isoformat(timespec="seconds")},
        ensure_ascii=False), encoding="utf-8")
    if stale:
        print(f"[us_close] ⚠ STALE data rows (t={t}): "
              f"{[(s['name'], s['as_of']) for s in stale]}")
    return p


def render_for_workspace(ws: Path, conn: Any = None) -> Optional[Path]:
    """Pipeline entry: FT-style chart -> {ws}/final/charts/01_markets.png.

    Owns its own DB connection when none is passed. Raises on failure —
    the caller (render_charts.render_all) decides the fallback.
    """
    snap = json.loads((ws / "snapshot.json").read_text(encoding="utf-8"))
    t = date.fromisoformat(snap["report_date"])
    par = ((snap.get("sources") or {}).get("macro_pillars") or {}).get("par_curve")
    out = ws / "final" / "charts" / "01_markets.png"
    own = conn is None
    if own:
        conn = DatabaseManager(get_database_config()).get_connection()
    try:
        sections = build_sections(conn, t, par)
    finally:
        if own:
            conn.close()
    n = sum(len(s["rows"]) for s in sections)
    if n < 15:
        raise RuntimeError(f"only {n} rows assembled")
    _dump_ft_json(ws, t, sections)   # 先落盘数据再画 PNG:PNG 渲染失败不丢表格数据
    return render(sections, t, out)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--workspace")
    ap.add_argument("--date")
    ap.add_argument("--out")
    args = ap.parse_args()

    t: Optional[date] = None
    par = None
    if args.workspace:
        snap = json.loads((Path(args.workspace) / "snapshot.json").read_text())
        t = date.fromisoformat(snap["report_date"])
        par = ((snap.get("sources") or {}).get("macro_pillars") or {}).get("par_curve")
    if args.date:
        t = date.fromisoformat(args.date)
    if t is None:
        print("need --date or --workspace", file=sys.stderr)
        return 1

    out = Path(args.out) if args.out else (
        Path(args.workspace) / "final" / "charts" / "01_markets.png"
        if args.workspace else Path(f"us_close_{t}.png"))

    conn = DatabaseManager(get_database_config()).get_connection()
    try:
        sections = build_sections(conn, t, par)
    finally:
        conn.close()
    n = sum(len(s["rows"]) for s in sections)
    if n < 15:
        print(f"[us_close] only {n} rows assembled — aborting", file=sys.stderr)
        return 1
    if args.workspace:
        _dump_ft_json(Path(args.workspace), t, sections)
    render(sections, t, out)
    print(f"[us_close] wrote {out} ({n} rows, {len(sections)} sections)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
