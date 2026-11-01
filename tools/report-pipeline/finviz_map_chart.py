#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Finviz-style market heatmap (sector > industry > stock treemap).

Replicates the heatmap CNT embeds under "Equity Market", matching the live
elite.finviz.com/map (filters: map · s&p500 · 1-day performance). Spec was
reverse-engineered from the user's screenshots and verified cell-by-cell:

  universe   S&P 500 members of the finviz_screener export, minus the
             'Shell Companies' taxonomy and recent IPOs the live map omits
  structure  sector > industry > stock; areas proportional to market cap;
             biggest block always at the TOP-LEFT (layout runs in math space,
             mirrored once at draw time — see _mir)
  columns    sectors pack into four fixed columns (SECTOR_GROUPS), column
             width ~ group cap; sectors stack cap-desc inside their column;
             industries and stocks pack cap-desc within their block
  headers    sector strips = bare UPPERCASE name (no pct); industry strips =
             tiny UPPERCASE label, truncated mid-word when narrow
  labels     ticker + pct per stock cell; font cap by cell height
             (STOCK_CAP), actual size computed per cell so BOTH lines fit
             (measured width + two-line height check); regular weight
  palette    finviz's compressed 1-day scale — bright red/green from ±3%
             (sampled off CNT's published heatmap)

Why self-rendered instead of screenshotting elite.finviz.com/map: every
finviz HTML/asset path sits behind an interactive Cloudflare Turnstile
challenge on this server's datacenter IP (verified: plain/headed/stealth
playwright and curl_cffi all blocked), while the finviz_screener export our
backend refreshes daily carries the exact same values the live map shows.

Historical dates: finviz_screener keeps only the latest full refresh, so for
T older than the snapshot the structure comes from that snapshot and the
1-day changes are backfilled from Yahoo daily closes (JSON-cached per date;
aborts if >20% of tickers are missing).

Usage:
    python tools/report-pipeline/finviz_map_chart.py --workspace backend/reports/pipeline/2026-10-30
    python tools/report-pipeline/finviz_map_chart.py --date 2026-10-30 --out /tmp/map.png
"""

from __future__ import annotations

import argparse
import json
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import os
import tempfile

os.environ.setdefault("MPLCONFIGDIR", tempfile.mkdtemp(prefix="mplconfig-"))

import requests  # noqa: E402  (Yahoo backfill for historical dates)

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.patches import Rectangle  # noqa: E402
import squarify  # noqa: E402
from PIL import ImageFont  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from backend.database.db_manager import DatabaseManager  # noqa: E402
from backend.utils.database_config import get_database_config  # noqa: E402

# ---------------------------------------------------------------- canvas spec
W, H, DPI = 1683, 899, 100          # same canvas as CNT's published heatmap
BG, HEADER_BG, INK, WHITE = "#262931", "#262931", "#262931", "#ffffff"
GAP = 1.5                           # px gutter between cells (thin dark seams)
SECTOR_HEADER_H = 18                # px, sector title strip
INDUSTRY_STRIP_H = 12               # px, industry label strip

# 1-day % -> fill color (bright endpoints at ±3%: GE -3.59 -> #f63538,
# LLY +3.19 -> #30cc5a on the reference heatmap)
_DOWN = [(-3, "#f63538"), (-2, "#ce3d41"), (-1.5, "#bf4045"),
         (-1, "#9d434b"), (-0.5, "#824450"), (-0.1, "#784551")]
_UP = [(3, "#30cc5a"), (2, "#2faa51"), (1.5, "#2fa450"),
       (1, "#30974f"), (0.5, "#32844e"), (0.1, "#38694f")]
FLAT = "#3d5451"                    # |chg| <= 0.1%

# ---------------------------------------------------------------- layout spec
# Live-map sector columns (reverse-engineered off the reference screenshot;
# NOT pure cap-desc — that would push Consumer Defensive into column 2).
# Column width ~ group cap; sectors stack cap-desc inside their column.
# Unknown sectors land in the last column.
SECTOR_GROUPS = [
    ["Technology"],
    ["Communication Services", "Financial", "Consumer Cyclical"],
    ["Healthcare", "Consumer Defensive", "Energy"],
    ["Industrials", "Utilities", "Real Estate", "Basic Materials"],
]
# In the screener export but absent from the live map view (recent IPOs the
# map has not added yet — SpaceX on 2026-10-30). Revisit as the map evolves.
MAP_EXCLUDE = {"SPCX"}

# -------------------------------------------------------------- label spec
# Ticker+pct per stock cell, regular weight. Font CAP by cell height
# (finviz caps its biggest labels ~42px cap height on a 1456px canvas ->
# ~29pt here); within the cap the size is computed PER CELL so that BOTH
# lines fit: width via measured metrics (_fit_pt), height via two-line math.
STOCK_CAP = ((289, 29), (150, 20), (87, 14), (36, 10), (0, 8))
LABEL_MIN_CW, LABEL_MIN_CH = 20, 27  # below this even the 8pt floor can't fit
SECTOR_HEADER_PT, SECTOR_HEADER_PT_MIN = 11, 7
INDUSTRY_PT, INDUSTRY_PT_THIN = 5.5, 4.5

# ---------------------------------------------------------------- font stack
# Liberation Sans ≈ the map's Helvetica look; DejaVu Sans is matplotlib's
# always-available fallback. Widths are measured with the SAME font file the
# text renders in.
if Path("/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf").exists():
    FONT_FAMILY = "Liberation Sans"
    _FONT_TTF = "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf"
else:
    FONT_FAMILY = "DejaVu Sans"
    _FONT_TTF = str(Path(matplotlib.get_data_path()) / "fonts" / "ttf" / "DejaVuSans.ttf")


def _text_px(s: str, fs: float) -> float:
    """Rendered width of s (FONT_FAMILY) at fontsize fs, in canvas px."""
    return ImageFont.truetype(_FONT_TTF, max(1, round(fs * DPI / 72))).getlength(s)


def _fit_label(s: str, fs: float, max_w: float, min_chars: int = 5) -> str:
    """Longest prefix of s measuring <= max_w (finviz clips mid-glyph; we clip
    at char boundary). "" when even min_chars don't fit."""
    if max_w <= 0 or _text_px(s, fs) <= max_w:
        return s
    lo, hi = 0, len(s)
    while lo < hi:
        mid = (lo + hi + 1) // 2
        if _text_px(s[:mid], fs) <= max_w:
            lo = mid
        else:
            hi = mid - 1
    return s[:lo] if lo >= min_chars else ""


def _fit_pt(text: str, cw: float, cap_pt: float,
            floor_pt: float = 5.0) -> Optional[float]:
    """Largest pt <= cap_pt whose measured width fits the cell; None if the
    floor doesn't fit."""
    pt = cap_pt
    while pt > floor_pt and _text_px(text, pt) > cw - 6:
        pt -= 0.5
    return pt if _text_px(text, pt) <= cw - 6 else None


# ------------------------------------------------------------------- helpers
def _layout(sizes: List[float], x: float, y: float,
            w: float, h: float) -> List[Dict[str, float]]:
    """Squarified rects inside one block, in MATH orientation (y-up). The tree
    is built in math space and mirrored once at draw time (_mir), so the
    first-placed (largest) block reads from the TOP like finviz."""
    norm = squarify.normalize_sizes(sizes, w, h)
    return [dict(r) for r in squarify.squarify(norm, x, y, w, h)]


def _mir(y: float, h: float) -> float:
    """math-y -> draw-y (vertical mirror about the canvas midline)."""
    return H - y - h


def fill_color(pct: Optional[float]) -> str:
    p = pct or 0.0
    if p <= -0.1:
        for thr, c in _DOWN:
            if p <= thr:
                return c
        return _DOWN[-1][1]
    if p >= 0.1:
        for thr, c in _UP:
            if p >= thr:
                return c
        return _UP[-1][1]
    return FLAT


def _text_color(bg_hex: str) -> str:
    r, g, b = (int(bg_hex[i:i + 2], 16) for i in (1, 3, 5))
    return WHITE if (0.299 * r + 0.587 * g + 0.114 * b) < 140 else INK


# ---------------------------------------------------------------- data access
def _yahoo_pct_change(ticker: str, t: date) -> Optional[float]:
    """1-day % change of ticker on trading day t, from Yahoo daily closes."""
    p2 = int(datetime.combine(t + timedelta(days=1), datetime.min.time()).timestamp())
    try:
        r = requests.get(
            f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}",
            params={"period1": p2 - 12 * 86400, "period2": p2, "interval": "1d"},
            headers={"User-Agent": "Mozilla/5.0 guanhe-report-pipeline-charts"},
            timeout=10)
        res = ((r.json().get("chart") or {}).get("result") or [None])[0]
        ts = (res or {}).get("timestamp") or []
        closes = (((res or {}).get("indicators") or {}).get("quote") or [{}])[0].get("close") or []
        pairs = [(datetime.fromtimestamp(ts[i]).date(), float(closes[i]))
                 for i in range(min(len(ts), len(closes))) if closes[i] is not None]
        pairs = [(d, c) for d, c in pairs if d <= t]
        if len(pairs) >= 2 and pairs[-1][0] == t and pairs[-2][1]:
            return (pairs[-1][1] / pairs[-2][1] - 1) * 100
    except Exception:  # noqa: BLE001
        return None
    return None


def _backfill_changes(rows: List[Dict[str, Any]], t: date) -> List[Dict[str, Any]]:
    """1-day changes from Yahoo (12-way parallel, JSON-cached per date so
    repeated historical renders don't re-fetch ~500 tickers). Drops rows
    whose change could not be fetched; aborts if >20% are missing."""
    cache_dir = Path(__file__).resolve().parent / "logs"
    cache_dir.mkdir(exist_ok=True)
    cache_file = cache_dir / f"yahoo_backfill_{t.isoformat()}.json"
    cache: Dict[str, Optional[float]] = {}
    if cache_file.exists():
        cache = json.loads(cache_file.read_text())
        print(f"[finviz_map] Yahoo backfill cache hit: {cache_file.name}")
    todo = [r for r in rows if r["ticker"] not in cache]
    if todo:
        with ThreadPoolExecutor(max_workers=12) as pool:
            got = dict(zip((r["ticker"] for r in todo),
                           pool.map(lambda r: _yahoo_pct_change(r["ticker"], t), todo)))
        cache.update(got)
        cache_file.write_text(json.dumps(cache))
        print(f"[finviz_map] Yahoo backfill fetched {len(todo)} tickers")
    missing = [r["ticker"] for r in rows if cache.get(r["ticker"]) is None]
    if len(missing) > len(rows) * 0.2:
        raise RuntimeError(f"Yahoo backfill too incomplete "
                           f"({len(missing)}/{len(rows)} missing)")
    if missing:
        print(f"[finviz_map] dropped {len(missing)} tickers without history: "
              f"{missing[:10]}")
    return [dict(r, chg=cache[r["ticker"]]) for r in rows
            if cache.get(r["ticker"]) is not None]


def fetch_stocks(conn: Any, t: date) -> Tuple[List[Dict[str, Any]], date]:
    """Stock rows for session date T from the finviz_screener snapshot.

    Universe = S&P 500 members (live-map sidebar: map · s&p500), minus the
    'Shell Companies' taxonomy and MAP_EXCLUDE — counts verified against the
    live map (Semiconductors 14 / Software - Infrastructure 17 / Consumer
    Electronics 1 / Internet Content & Information 4).

    The table keeps only the latest full refresh. When it does not cover T's
    session (snapshot < T or > T+1), structure comes from that snapshot and
    the 1-day changes are backfilled from Yahoo.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT MAX(snapshot_date) FROM finviz_screener WHERE snapshot_date<=%s",
                    (t + timedelta(days=1),))
        snap = cur.fetchone()[0]
        if snap is None:  # no snapshot covering T — fall back to the latest
            cur.execute("SELECT MAX(snapshot_date) FROM finviz_screener")
            snap = cur.fetchone()[0]
        if snap is None:
            raise RuntimeError("finviz_screener is empty")
        cur.execute(
            "SELECT ticker, sector, industry, market_cap, change_percent "
            "FROM finviz_screener "
            "WHERE snapshot_date=%s AND market_cap IS NOT NULL AND sector IS NOT NULL "
            "AND index_name LIKE '%%S&P 500%%' "
            # the live map doesn't render this taxonomy (e.g. VYLR carries an
            # S&P 500 tag in the export but has no strip on the map)
            "AND industry <> 'Shell Companies'",
            (snap,))
        rows = []
        for tk, sec, ind, cap, chg in cur.fetchall():
            if tk in MAP_EXCLUDE:
                continue
            try:
                chg_f = None if chg is None else float(str(chg).replace("%", "").replace("+", ""))
            except ValueError:
                chg_f = None
            rows.append({"ticker": tk, "sector": sec, "industry": ind,
                         "cap": float(cap), "chg": chg_f})
    if not (t <= snap <= t + timedelta(days=1)):
        print(f"[finviz_map] snapshot {snap} does not cover {t} — structure "
              f"from {snap}, 1-day changes backfilled from Yahoo")
        rows = _backfill_changes(rows, t)
    return rows, snap


# ---------------------------------------------------------------- drawing
def _stock_cells(ax: Any, stocks: List[Dict[str, Any]],
                 x: float, y: float, w: float, h: float) -> None:
    """Paint stock cells (and labels where they fit) inside one industry block."""
    if not stocks or w < 4 or h < 4:
        return
    for s, r in zip(stocks, _layout([s["cap"] for s in stocks], x, y, w, h)):
        cx, cy, cw, ch = r["x"], r["y"], r["dx"], r["dy"]
        # every cell is painted (only labels have a size threshold) —
        # skipping sub-pixel cells would leave dark holes in the treemap
        color = fill_color(s["chg"])
        ax.add_patch(Rectangle((cx + GAP / 2, _mir(cy + GAP / 2, ch - GAP)),
                               max(cw - GAP, 0.5), max(ch - GAP, 0.5),
                               facecolor=color, edgecolor="none"))
        if cw < LABEL_MIN_CW or ch < LABEL_MIN_CH:
            continue
        cap_pt = next((cap for min_h, cap in STOCK_CAP if ch >= min_h), 8)
        tc = _text_color(color)
        tk, pct_s = s["ticker"], ("\u2013" if s["chg"] is None
                                  else f"{s['chg']:+.2f}%")
        t_pt = _fit_pt(tk, cw, cap_pt)
        if t_pt is None:
            continue  # ticker can't fit at any readable size
        p_pt = _fit_pt(pct_s, cw, max(5.0, t_pt * 0.62), 4.5)
        cym = _mir(cy, ch) + ch / 2
        if p_pt is not None and (t_pt + p_pt) * 1.39 * 1.15 + 6 <= ch:
            # two lines: ticker above middle, pct below, symmetric
            g = (t_pt * 1.39 + p_pt * 1.39) * 0.31
            ax.text(cx + cw / 2, cym + g, tk, fontsize=t_pt, color=tc,
                    ha="center", va="center", family=FONT_FAMILY)
            ax.text(cx + cw / 2, cym - g, pct_s, fontsize=p_pt, color=tc,
                    ha="center", va="center", family=FONT_FAMILY)
        elif t_pt * 1.6 <= ch:
            ax.text(cx + cw / 2, cym, tk, fontsize=t_pt, color=tc,
                    ha="center", va="center", family=FONT_FAMILY)


def build_render(out: Path, t: date, rows: List[Dict[str, Any]]) -> Path:
    """Group rows into the live-map column layout and draw the heatmap."""
    # sector -> industry -> stocks; cap-desc within every block
    by_sec: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}
    for r in rows:
        by_sec.setdefault(r["sector"], {}).setdefault(
            r.get("industry") or "\u2014", []).append(r)
    sectors: List[Dict[str, Any]] = []
    for sec, inds in by_sec.items():
        ind_list = [{"name": ind, "stocks": sorted(st, key=lambda s: -s["cap"]),
                     "cap": sum(s["cap"] for s in st)}
                    for ind, st in inds.items()]
        ind_list.sort(key=lambda i: -i["cap"])
        sectors.append({"name": sec, "industries": ind_list,
                        "cap": sum(i["cap"] for i in ind_list)})
    sectors.sort(key=lambda s: -s["cap"])

    fig = plt.figure(figsize=(W / DPI, H / DPI), dpi=DPI)
    ax = fig.add_axes([0, 0, 1, 1])
    ax.set_xlim(0, W)
    ax.set_ylim(0, H)
    ax.axis("off")

    # level 1 — fixed live-map columns; column width ~ group cap
    total_cap = sum(s["cap"] for s in sectors) or 1.0
    by_name = {s["name"]: s for s in sectors}
    groups = [[by_name.pop(n) for n in g if n in by_name] for g in SECTOR_GROUPS]
    if by_name:  # sectors outside the observed map land in the last column
        groups[-1].extend(sorted(by_name.values(), key=lambda s: -s["cap"]))
    groups = [g for g in groups if g]

    x0 = 0.0
    for g in groups:
        gw = W * sum(s["cap"] for s in g) / total_cap
        for sec, r in zip(g, _layout([s["cap"] for s in g], x0, 0, gw, H)):
            x, y, w, h = r["x"], r["y"], r["dx"], r["dy"]
            # the mirror swaps top/bottom: the header sits on the block's
            # math-BOTTOM edge to land at its visual top
            hdr_h = min(SECTOR_HEADER_H, h * 0.5)
            ax.add_patch(Rectangle((x, _mir(y, hdr_h)), w, hdr_h,
                                   facecolor=HEADER_BG, edgecolor="none"))
            # live-map header = bare UPPERCASE sector name; shrink to the
            # column width first, then truncate whatever still overflows
            name = sec["name"].upper()
            fs = SECTOR_HEADER_PT
            while fs > SECTOR_HEADER_PT_MIN and _text_px(name, fs) > w - 12:
                fs -= 1
            name = _fit_label(name, fs, w - 12)
            if name:
                ax.text(x + 6, _mir(y, hdr_h) + hdr_h / 2, name, fontsize=fs,
                        color=WHITE, va="center", ha="left", family=FONT_FAMILY)

            # level 2 — industry sub-blocks inside the sector (math region
            # starts above the header's mirrored footprint)
            inner_h = h - hdr_h
            if inner_h <= 6:
                continue
            for ind, ir in zip(sec["industries"],
                               _layout([i["cap"] for i in sec["industries"]],
                                       x, y + hdr_h, w, inner_h)):
                ix, iy, iw, ih = ir["x"], ir["y"], ir["dx"], ir["dy"]
                if iw < 4 or ih < 4:
                    continue
                # thin industry blocks still get a (thinner) labelled strip —
                # the live map labels every industry, clipping when narrow
                if ih >= 14:
                    strip_h = float(INDUSTRY_STRIP_H)
                elif ih >= 7:
                    strip_h = max(7.0, ih * 0.6)
                else:
                    strip_h = 0.0
                if strip_h:
                    ax.add_patch(Rectangle((ix, _mir(iy, strip_h)), iw, strip_h,
                                           facecolor=HEADER_BG, edgecolor="none"))
                    if iw >= 24:
                        fs_i = INDUSTRY_PT if strip_h >= 10 else INDUSTRY_PT_THIN
                        lbl = _fit_label(ind["name"].upper(), fs_i, iw - 8)
                        if lbl:
                            ax.text(ix + 4, _mir(iy, strip_h) + strip_h / 2, lbl,
                                    fontsize=fs_i, color=WHITE, va="center",
                                    ha="left", family=FONT_FAMILY)
                _stock_cells(ax, ind["stocks"], ix, iy + strip_h, iw, ih - strip_h)
        x0 += gw

    # CNT-style corner overlay (their published map carries "1D" + date)
    ax.text(10, 14, f"1D \u00b7 {t:%A, %B %d, %Y}", fontsize=8, color=WHITE,
            family=FONT_FAMILY, zorder=5)

    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, dpi=DPI, facecolor=HEADER_BG)
    plt.close(fig)
    return out


# --------------------------------------------------------------- entry points
def _session_date(ws: Path) -> date:
    return date.fromisoformat(
        json.loads((ws / "snapshot.json").read_text(encoding="utf-8"))["report_date"])


def render_for_workspace(ws: Path, conn: Any = None) -> Optional[Path]:
    """Pipeline entry -> {ws}/final/charts/02_finviz_map.png."""
    t = _session_date(ws)
    out = ws / "final" / "charts" / "02_finviz_map.png"
    own = conn is None
    if own:
        conn = DatabaseManager(get_database_config()).get_connection()
    try:
        rows, snap = fetch_stocks(conn, t)
    finally:
        if own:
            conn.close()
    if len(rows) < 50:
        raise RuntimeError(f"only {len(rows)} finviz rows in snapshot {snap}")
    print(f"[finviz_map] snapshot {snap}: {len(rows)} S&P 500 stocks")
    return build_render(out, t, rows)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--workspace", help="pipeline workspace (reads snapshot.json)")
    ap.add_argument("--date", help="session date YYYY-MM-DD (standalone mode)")
    ap.add_argument("--out", help="output png (default: workspace charts dir)")
    args = ap.parse_args()

    if args.date:
        t = date.fromisoformat(args.date)
    elif args.workspace:
        t = _session_date(Path(args.workspace))
    else:
        print("need --date or --workspace", file=sys.stderr)
        return 1
    out = Path(args.out) if args.out else (
        Path(args.workspace) / "final" / "charts" / "02_finviz_map.png"
        if args.workspace else Path(f"finviz_map_{t}.png"))

    conn = DatabaseManager(get_database_config()).get_connection()
    try:
        rows, snap = fetch_stocks(conn, t)
    finally:
        conn.close()
    if len(rows) < 50:
        print(f"only {len(rows)} rows — aborting", file=sys.stderr)
        return 1
    print(f"[finviz_map] wrote {build_render(out, t, rows)} "
          f"({len(rows)} stocks, snapshot {snap})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
