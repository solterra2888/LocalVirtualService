#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""GuanHe-style "US close" overview chart (FT-market-table look) from guanhetech DB.

Replicates the first chart of GuanHe Trading's daily report (Overall Market section):
a cream-background table grouped into EQUITIES / RATES / CREDIT / COMMODITIES /
FX / VOLATILITY / S&P 500 SECTORS. Each row shows Name (TICKER), Last, a
zero-centred 1-day bar, and 1D/1W/1M/YTD columns. Rows whose |1D| passes the
section threshold (0.75% / 8bp / 8%) are bold; bars past the per-section scale
get an arrowhead; the scale itself is printed at the bar-zone edges of each
group header.

Methodology (reverse-engineered from GuanHe 2026-10-30 chart, verified numerically):
  - Last & 1D quotes      -> finviz_screener snapshot MAX(snapshot_date) <= T+1
                             (ETF/sector rows match GuanHe value-for-value)
  - 1W / 1M windows       -> close on the last trading day <= T-7d / <= T-30d
                             (calendar windows; 21-trading-days was ruled out)
  - ETF/sector W/M/YTD    -> total return from market_data_eod adjusted_close
                             (matches GuanHe incl. dividends: GLD -6.6/USO +15.9)
  - indices/yields/FX/VIX -> market_data_eod close history (EODHD); yields and
                             2s10s quoted in bp; 2s10s spread computed inline
  - DXY                   -> Yahoo best-effort; MOVE unavailable (footnote)
  - EEM/FXI (not in DB)   -> Yahoo adjusted best-effort, finviz perf fallback

Usage:
    python tools/US-market-report/us_close_chart.py --workspace backend/reports/pipeline/2026-10-30
    python tools/US-market-report/us_close_chart.py --date 2026-10-30 --out /tmp/us_close.png
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import os
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
W, H, DPI = 1440, 2393, 100  # pixel canvas == data coords at dpi=100
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

FINVIZ_ETFS = ("HYG", "LQD", "USO", "GLD", "SLV", "CPER", "EEM", "FXI")
ETF_DB_CODE = {"HYG": "HYG.US", "LQD": "LQD.US", "USO": "USO.US", "GLD": "GLD.US",
               "SLV": "SLV.US", "CPER": "CPER.US"}
ETF_YAHOO = {"EEM": "EEM", "FXI": "FXI"}
SECTOR_ETFS = ("XLB", "XLC", "XLE", "XLF", "XLI", "XLK", "XLP", "XLRE", "XLU", "XLV", "XLY")
SECTOR_DB_CODE = {tk: tk + ".US" for tk in SECTOR_ETFS}
SECTOR_NAMES = {
    "XLE": "Energy", "XLF": "Financials", "XLB": "Materials", "XLRE": "Real estate",
    "XLU": "Utilities", "XLP": "Staples", "XLV": "Health care",
    "XLY": "Discretionary", "XLC": "Comm services", "XLI": "Industrials",
    "XLK": "Technology",
}
UA = {"User-Agent": "Mozilla/5.0 guanhe-us-market-report-charts"}
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
def _ref_cal(s: List[Tuple[date, float]], days: int) -> Optional[float]:
    """close on the last trading day <= T-days (calendar window)."""
    target = s[0][0] - timedelta(days=days)
    for d, c in s:
        if d <= target:
            return c
    return None


def _changes(s: List[Tuple[date, float]], pct: bool = True) -> Dict[str, Optional[float]]:
    """1D (prev close) / 1W (7cal) / 1M (30cal) / YTD (prior-year close)."""
    if len(s) < 2:
        return {"m1d": None, "m1w": None, "m1m": None, "ytd": None}

    def chg(ref: Optional[float]) -> Optional[float]:
        if ref in (None, 0):
            return None
        return (s[0][1] / ref - 1) * 100 if pct else (s[0][1] - ref) * 100

    ytd_ref = next((c for d, c in s if d.year < s[0][0].year), None)
    return {"m1d": chg(s[1][1]), "m1w": chg(_ref_cal(s, 7)),
            "m1m": chg(_ref_cal(s, 30)), "ytd": chg(ytd_ref)}


# ------------------------------------------------------------------- DB layer
def _series(conn: Any, code: str, t: date, column: str = "close",
            limit: int = 340) -> List[Tuple[date, float]]:
    """(date, value) desc for code, up to t. column: close | adjusted_close."""
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT date, {column} FROM market_data_eod WHERE code=%s AND date<=%s "
            f"AND {column} IS NOT NULL ORDER BY date DESC LIMIT %s", (code, t, limit))
        return [(r[0], float(r[1])) for r in cur.fetchall()]


def _finviz_rows(conn: Any, t: date) -> Tuple[Dict[str, Dict[str, Any]], date]:
    """ETF rows from the newest finviz_screener snapshot frozen at/before T+1."""
    with conn.cursor() as cur:
        cur.execute("SELECT MAX(snapshot_date) FROM finviz_screener WHERE snapshot_date<=%s",
                    (t + timedelta(days=1),))
        snap = cur.fetchone()[0]
        if snap is None:
            raise RuntimeError("no finviz_screener snapshot on/before T+1")
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

    Fetched per-series with cosd=t-400d (fast). Must cover T itself
    (publication lags a few hours after the ET close) — else the caller
    falls back to EODHD yields.
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
        if s[0][0] != t:
            return None
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


# --------------------------------------------------------------- row assembly
def _row(name: str, ticker: str, last_str: str, unit: str,
         ch: Dict[str, Optional[float]], last: Optional[float] = None) -> Dict[str, Any]:
    return {"name": name, "ticker": ticker, "last": last, "last_str": last_str,
            "unit": unit, **ch}


def build_sections(conn: Any, t: date) -> List[Dict[str, Any]]:
    FOOTER_NOTES.clear()
    fv, snap = _finviz_rows(conn, t)
    print(f"[us_close] finviz snapshot {snap} (frozen at/after {t} close)")

    def idx_row(name: str, ticker: str, code: str) -> Dict[str, Any]:
        s = _series(conn, code, t)
        return _row(name, ticker, _minus(f"{s[0][1]:,.0f}") if s else "\u2013", "%",
                    _changes(s), s[0][1] if s else None)

    def etf_row(name: str, ticker: str, perf_source: str = "tr") -> Optional[Dict[str, Any]]:
        """Last/1D from finviz quote; 1W/1M/YTD via perf_source:
        "tr"     total return from adjusted closes — DB first, Yahoo for EEM/FXI,
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
        return _row(name, ticker, _minus(f"{last:.2f}"), "%", ch, last)

    # ---- EQUITIES (indices from EODHD history; EM/China ETFs as above)
    eq = [idx_row("S&P 500", "SPX", "GSPC.INDX"),
          idx_row("Nasdaq 100", "NDX", "NDX.INDX"),
          idx_row("Dow", "DJIA", "DJI.INDX"),
          idx_row("Russell 2000", "R2K", "RUT.INDX"),
          idx_row("Semis", "SOX", "SOX.INDX")]
    for nm, tk in (("EM equity", "EEM"), ("China", "FXI")):
        r = etf_row(nm, tk)
        if r:
            eq.append(r)

    # ---- RATES (Treasury par yields via FRED when T is published; EODHD fallback)
    fred = _fred_yields(t)
    if fred:
        print("[us_close] rates: FRED Treasury par yields (DGS2/10/30)")
        FOOTER_NOTES.append("US Treasury par yields via FRED")
        rates = []
        series = {"US 2Y": fred["DGS2"], "US 10Y": fred["DGS10"], "US 30Y": fred["DGS30"]}
        for nm, s in series.items():
            rates.append(_row(nm, "", _minus(f"{s[0][1]:.2f}%"), "bp",
                              _changes(s, pct=False), s[0][1]))
        s2, s10 = series["US 2Y"], series["US 10Y"]
        m10 = {d: c for d, c in s10}
        spread = sorted(((d, m10[d] - c) for d, c in s2 if d in m10),
                        key=lambda x: x[0], reverse=True)
        rates.append(_row("2s10s", "", _minus(f"{spread[0][1] * 100:+.0f}bp"), "bp",
                          _changes(spread, pct=False), spread[0][1] * 100))
    else:
        print("[us_close] rates: FRED lacks T — falling back to EODHD yields")
        FOOTER_NOTES.append("Treasury yields: EODHD (FRED not yet published for T)")

        def rate_row(name: str, code: str) -> Dict[str, Any]:
            s = _series(conn, code, t)
            return _row(name, "", _minus(f"{s[0][1]:.2f}%") if s else "\u2013", "bp",
                        _changes(s, pct=False), s[0][1] if s else None)

        s2, s10 = _series(conn, "US2Y.INDX", t), _series(conn, "US10Y.INDX", t)
        rates = [rate_row("US 2Y", "US2Y.INDX"), rate_row("US 10Y", "US10Y.INDX"),
                 rate_row("US 30Y", "US30Y.INDX")]
        if s2 and s10:
            m10 = {d: c for d, c in s10}
            spread = sorted(((d, m10[d] - c) for d, c in s2 if d in m10),
                            key=lambda x: x[0], reverse=True)
            rates.append(_row("2s10s", "", _minus(f"{spread[0][1] * 100:+.0f}bp"), "bp",
                              _changes(spread, pct=False), spread[0][1] * 100))

    # ---- CREDIT / COMMODITIES / SECTORS (finviz quotes + total return)
    credit = [r for r in (etf_row("High yield", "HYG"),
                          etf_row("Investment grade", "LQD")) if r]
    comm = [r for r in (etf_row("Crude oil", "USO"), etf_row("Gold", "GLD"),
                        etf_row("Silver", "SLV"), etf_row("Copper", "CPER")) if r]

    # ---- FX (DXY via Yahoo best-effort; EUR/USD & USD/JPY from EODHD)
    fx: List[Dict[str, Any]] = []
    dxy = _yahoo_series("DX-Y.NYB", t, adjusted=False)
    if dxy:
        fx.append(_row("US dollar", "DXY", _minus(f"{dxy[0][1]:.2f}"), "%",
                       _changes(dxy), dxy[0][1]))
    else:
        print("[us_close] DXY unavailable via Yahoo — row skipped")

    def fx_row(name: str, ticker: str, code: str) -> Dict[str, Any]:
        s = _series(conn, code, t)
        dp = 4 if code.startswith("EUR") else 2
        return _row(name, ticker, _minus(f"{s[0][1]:.{dp}f}") if s else "\u2013", "%",
                    _changes(s), s[0][1] if s else None)

    fx += [fx_row("EUR/USD", "", "EURUSD.FOREX"), fx_row("USD/JPY", "", "USDJPY.FOREX")]

    # ---- VOLATILITY (VIX from EODHD; MOVE not available in our stack)
    sv = _series(conn, "VIX.INDX", t)
    vol = [_row("VIX", "", _minus(f"{sv[0][1]:.2f}") if sv else "\u2013", "%",
                _changes(sv), sv[0][1] if sv else None)]

    # ---- S&P 500 SECTORS (finviz quotes + finviz perf columns, sorted by 1D desc)
    sectors = sorted((r for r in (etf_row(SECTOR_NAMES[tk], tk, perf_source="finviz")
                                  for tk in SECTOR_ETFS) if r),
                     key=lambda r: -(r["m1d"] or 0))

    sections = [
        {"title": "EQUITIES", "scale": 5.0, "scale_lbl": "\u22125% / +5%", "unit": "%", "rows": eq},
        {"title": "RATES", "scale": 10.0, "scale_lbl": "\u221210bp / +10bp", "unit": "bp", "rows": rates},
        {"title": "CREDIT", "scale": 5.0, "scale_lbl": "\u22125% / +5%", "unit": "%", "rows": credit},
        {"title": "COMMODITIES", "scale": 5.0, "scale_lbl": "\u22125% / +5%", "unit": "%", "rows": comm},
        {"title": "FX", "scale": 5.0, "scale_lbl": "\u22125% / +5%", "unit": "%", "rows": fx},
        {"title": "VOLATILITY", "scale": 15.0, "scale_lbl": "\u221215% / +15%", "unit": "%", "rows": vol},
        {"title": "S&P 500 SECTORS", "scale": 5.0, "scale_lbl": "\u22125% / +5%", "unit": "%", "rows": sectors},
    ]
    return [s for s in sections if s["rows"]]


# ------------------------------------------------------------------ rendering
def render(sections: List[Dict[str, Any]], t: date, out: Path) -> Path:
    n_rows, n_sec = sum(len(s["rows"]) for s in sections), len(sections)
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
                       fontweight=weight, fontstyle=style, family="DejaVu Sans")

    txt(NAME_X, TITLE_Y, f"US close \u00b7 {t:%A, %B %d, %Y}", 19, INK, weight="bold")
    txt(NAME_X, SUB_Y,
        "Bold = 1-day move of 0.75% or more (8bp for rates, 8% for vol). "
        "Each group's bar scale is in its header; an arrowhead marks a bar past it.",
        9, GRAY)
    txt(LAST_R, COLHDR_Y, "Last", 8.5, GRAY, ha="right")
    txt(BAR_CX, COLHDR_Y, "1-day move", 8.5, GRAY, ha="center")
    for cx, lab in ((C1D, "1D"), (C1W, "1W"), (C1M, "1M"), (CYTD, "YTD")):
        txt(cx, COLHDR_Y, lab, 8.5, GRAY, ha="right")

    name_texts = []  # (text_obj, ticker, y) for 2nd-pass ticker placement
    y = COLHDR_Y + 26
    for sec in sections:
        unit, scale = sec["unit"], sec["scale"]
        cy = y + HEAD_H / 2
        txt(NAME_X, cy, sec["title"], 10, GRAY, weight="bold")
        txt(BAR_L - 6, cy, sec["scale_lbl"].split(" / ")[0], 7.5, GRAY_LT, ha="right")
        txt(BAR_R + 6, cy, sec["scale_lbl"].split(" / ")[1], 7.5, GRAY_LT)
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
            name_texts.append((txt(NAME_X, ry, r["name"], 11, INK,
                                   weight="bold" if bold else "normal"), r["ticker"], ry))
            txt(LAST_R, ry, r["last_str"], 10.5, INK, ha="right")
            fmt_1d = _f_pct2 if unit == "%" else _f_bp
            fmt_rest = _f_pct1 if unit == "%" else _f_bp
            for cx, v, k in zip((C1D, C1W, C1M, CYTD), (r["m1d"], r["m1w"], r["m1m"], r["ytd"]), range(4)):
                color = GREEN if (v or 0) > 0 else RED if (v or 0) < 0 else GRAY
                txt(cx, ry, (fmt_1d if k == 0 else fmt_rest)(v), 10 if k else 10.5,
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
            txt(t_obj.get_window_extent(renderer).x1 / DPI + 9, ry, ticker, 8, GRAY_LT)

    rates_note = f" ({'; '.join(FOOTER_NOTES)})" if FOOTER_NOTES else ""
    ax.plot([M_L, M_R], [Y(H - 70)] * 2, color=RULE, lw=0.9)
    txt(NAME_X, H - 52,
        "Sources: Finviz screener (quotes, delayed) \u00b7 EODHD (indices, FX, VIX, "
        f"adjusted ETF closes){rates_note} \u00b7 DXY: Yahoo Finance.",
        8, GRAY_LT, style="italic")
    txt(NAME_X, H - 32,
        f"MOVE index unavailable. Last updated {t:%A, %B %d, %Y} \u00b7 "
        "guanhetech US-market-report.",
        8, GRAY_LT, style="italic")

    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, dpi=DPI, facecolor=BG)
    plt.close(fig)
    return out


def render_for_workspace(ws: Path, conn: Any = None) -> Optional[Path]:
    """Pipeline entry: FT-style chart -> {ws}/final/charts/01_us_close.png.

    Owns its own DB connection when none is passed. Raises on failure —
    the caller (render_charts.render_all) decides the fallback.
    """
    t = date.fromisoformat(
        json.loads((ws / "snapshot.json").read_text(encoding="utf-8"))["report_date"])
    out = ws / "final" / "charts" / "01_us_close.png"
    own = conn is None
    if own:
        conn = DatabaseManager(get_database_config()).get_connection()
    try:
        sections = build_sections(conn, t)
    finally:
        if own:
            conn.close()
    n = sum(len(s["rows"]) for s in sections)
    if n < 15:
        raise RuntimeError(f"only {n} rows assembled")
    return render(sections, t, out)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--workspace")
    ap.add_argument("--date")
    ap.add_argument("--out")
    args = ap.parse_args()

    t: Optional[date] = None
    if args.date:
        t = date.fromisoformat(args.date)
    elif args.workspace:
        t = date.fromisoformat(json.loads(
            (Path(args.workspace) / "snapshot.json").read_text())["report_date"])
    if t is None:
        print("need --date or --workspace", file=sys.stderr)
        return 1

    out = Path(args.out) if args.out else (
        Path(args.workspace) / "final" / "charts" / "01_us_close.png"
        if args.workspace else Path(f"us_close_{t}.png"))

    conn = DatabaseManager(get_database_config()).get_connection()
    try:
        sections = build_sections(conn, t)
    finally:
        conn.close()
    n = sum(len(s["rows"]) for s in sections)
    if n < 15:
        print(f"[us_close] only {n} rows assembled — aborting", file=sys.stderr)
        return 1
    render(sections, t, out)
    print(f"[us_close] wrote {out} ({n} rows, {len(sections)} sections)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
