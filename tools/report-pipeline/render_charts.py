#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Deterministic GuanHe-style charts from a frozen snapshot (no LLM).

Outputs PNGs under {workspace}/final/charts/:
  01_us_close.png          cross-asset panel (1D / 1W / 1M / YTD when present)
  02_sector_vs_spy.png     sector ETF-style table from sp500_sector_avg
  03_sector_rotation.png   1M vs 1D scatter (stand-in for a true RRG)
  04_release_{event}.png   tier-1 release header + FRED m/m bars from release_series
  05_{TICKER}.png          deep-dive close line from Yahoo (best-effort; not candlesticks)

Usage:
    python tools/report-pipeline/render_charts.py backend/reports/pipeline/2026-10-27
"""

from __future__ import annotations

import json
import re
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import os
import tempfile

# Avoid permission errors when ~/.matplotlib is not writable
os.environ.setdefault("MPLCONFIGDIR", tempfile.mkdtemp(prefix="mplconfig-"))

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import requests  # noqa: E402

UA = {"User-Agent": "Mozilla/5.0 guanhe-report-pipeline-charts"}


def _load(ws: Path) -> Dict[str, Any]:
    return json.loads((ws / "snapshot.json").read_text(encoding="utf-8"))


def _savefig(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    plt.tight_layout()
    plt.savefig(path, dpi=140, bbox_inches="tight", facecolor="white")
    plt.close()


def render_us_close(snapshot: Dict[str, Any], out: Path) -> Optional[Path]:
    """Legacy fallback: simple cross-asset bar from snapshot cross_asset.

    Superseded by us_close_chart.render_for_workspace (GuanHe-style FT table);
    render_all tries that first and only lands here when it fails (e.g. no
    finviz snapshot or DB unreachable).
    """
    market = (snapshot.get("sources") or {}).get("market") or {}
    cross = market.get("cross_asset") or []
    if not cross:
        return None
    labels = [f"{c.get('name') or c.get('code')}" for c in cross]
    vals = [c.get("pct_change_1d") or 0 for c in cross]
    colors = ["#1a7f37" if v >= 0 else "#cf222e" for v in vals]
    fig, ax = plt.subplots(figsize=(8, max(4, 0.35 * len(labels))))
    ax.barh(range(len(labels)), vals, color=colors)
    ax.set_yticks(range(len(labels)))
    ax.set_yticklabels(labels, fontsize=8)
    ax.axvline(0, color="#888", lw=0.8)
    ax.set_xlabel("1D %")
    ax.set_title(f"US close · {snapshot.get('report_date')}  (snapshot cross-asset)")
    ax.invert_yaxis()
    path = out / "01_us_close.png"
    _savefig(path)
    return path


def render_sector_table(snapshot: Dict[str, Any], out: Path) -> Optional[Path]:
    market = (snapshot.get("sources") or {}).get("market") or {}
    sectors = market.get("sp500_sector_avg") or []
    if not sectors:
        return None
    labels = [s["sector"] for s in sectors]
    vals = [s["avg_change_pct"] for s in sectors]
    colors = ["#1a7f37" if v >= 0 else "#cf222e" for v in vals]
    fig, ax = plt.subplots(figsize=(7, max(3.5, 0.32 * len(labels))))
    ax.barh(range(len(labels)), vals, color=colors)
    ax.set_yticks(range(len(labels)))
    ax.set_yticklabels(labels, fontsize=8)
    ax.axvline(0, color="#888", lw=0.8)
    ax.set_xlabel("S&P 500 sector avg 1D % (Finviz)")
    ax.set_title(f"S&P 500 sectors · {snapshot.get('report_date')}")
    ax.invert_yaxis()
    path = out / "02_sector_vs_spy.png"
    _savefig(path)
    return path


def render_sector_rotation(snapshot: Dict[str, Any], out: Path) -> Optional[Path]:
    """RRG stand-in: scatter sector avg 1D (y) vs count-weighted proxy using
    constituents' perf_month median per sector when available."""
    market = (snapshot.get("sources") or {}).get("market") or {}
    cons = market.get("sp500_constituents") or []
    if not cons:
        return None
    by_sec: Dict[str, List[Tuple[float, Optional[float]]]] = {}
    for c in cons:
        s = c.get("sector")
        if not s:
            continue
        by_sec.setdefault(s, []).append(
            (c.get("change_percent") or 0.0, c.get("perf_month"))
        )
    xs, ys, labels = [], [], []
    for sec, rows in by_sec.items():
        d1 = sum(r[0] for r in rows) / len(rows)
        months = [r[1] for r in rows if r[1] is not None]
        if not months:
            continue
        m1 = sum(months) / len(months)
        xs.append(m1)
        ys.append(d1)
        labels.append(sec.split()[0][:10])
    if len(xs) < 3:
        return None
    fig, ax = plt.subplots(figsize=(7, 6))
    ax.axhline(0, color="#bbb", lw=0.8)
    ax.axvline(0, color="#bbb", lw=0.8)
    ax.scatter(xs, ys, c="#0969da", s=40)
    for x, y, lab in zip(xs, ys, labels):
        ax.annotate(lab, (x, y), fontsize=7, xytext=(3, 3),
                    textcoords="offset points")
    ax.set_xlabel("1M avg % (Finviz performance_month)")
    ax.set_ylabel("1D avg %")
    ax.set_title(f"Sector rotation proxy · {snapshot.get('report_date')}")
    path = out / "03_sector_rotation.png"
    _savefig(path)
    return path


def render_release_charts(snapshot: Dict[str, Any], out: Path) -> List[Path]:
    pillars = (snapshot.get("sources") or {}).get("macro_pillars") or {}
    series = pillars.get("release_series") or {}
    paths: List[Path] = []

    # Map selected T-day US tier-1 events onto release_series keys
    key_hints = {
        "nonfarm": ["nonfarm", "non farm", "non-farm", "payroll", "employment situation"],
        "cpi": ["cpi", "consumer price"],
        "ppi": ["ppi", "producer price"],
        "retail": ["retail sales"],
        "durable": ["durable goods"],
        "housing_starts": ["housing starts", "building permits"],
        "jolts": ["jolts", "job openings"],
    }
    try:
        from run_pipeline import select_tier1_releases
        tday = select_tier1_releases(snapshot, limit=2)
    except Exception:
        econ = (snapshot.get("sources") or {}).get("economic_events") or {}
        report_date = snapshot.get("report_date") or ""
        tday = [e for e in (econ.get("recent") or [])
                if str(e.get("date") or "")[:10] == report_date
                and e.get("actual") not in (None, "")][:2]
    for e in tday:
        name = (e.get("event") or "").lower()
        matched_key = None
        for k, hints in key_hints.items():
            if any(h in name for h in hints):
                matched_key = k
                break
        fig, axes = plt.subplots(2, 1, figsize=(7, 6),
                                 gridspec_kw={"height_ratios": [1, 2]})
        ax0 = axes[0]
        ax0.axis("off")
        actual = e.get("actual")
        est = e.get("forecast")
        prior = e.get("previous")
        try:
            surprise = (float(str(actual).replace("%", "").replace(",", ""))
                        - float(str(est).replace("%", "").replace(",", ""))) \
                if actual not in (None, "") and est not in (None, "") else None
        except ValueError:
            surprise = None
        header = (
            f"{e.get('event')}\n"
            f"ACTUAL {actual}   CONSENSUS {est}   PRIOR {prior}"
            + (f"   SURPRISE {surprise:+.2f}" if surprise is not None else "")
        )
        ax0.text(0.02, 0.5, header, fontsize=10, va="center", family="monospace")

        ax1 = axes[1]
        hist = (series.get(matched_key) or {}).get("mom_3y") if matched_key else None
        if hist:
            xs = [h["date"] for h in hist]
            ys = [h["value"] for h in hist]
            ax1.bar(range(len(ys)), ys, color="#8c959f", width=0.8)
            ax1.set_xticks(range(0, len(xs), max(1, len(xs) // 6)))
            ax1.set_xticklabels(
                [xs[i][:7] for i in range(0, len(xs), max(1, len(xs) // 6))],
                fontsize=7, rotation=30)
            ax1.set_ylabel("m/m")
            ax1.set_title(f"{(series.get(matched_key) or {}).get('label', matched_key)} "
                          f"(FRED {(series.get(matched_key) or {}).get('series')})")
        else:
            ax1.text(0.5, 0.5, "release_series unavailable", ha="center",
                     transform=ax1.transAxes)
            ax1.axis("off")
        fname = ("04_release_"
                 + re.sub(r"[^A-Za-z0-9]+", "_", e.get("event", "event"))[:40]
                 + ".png")
        path = out / fname
        _savefig(path)
        paths.append(path)
    return paths


def _yahoo_ohlc(symbol: str, report_date: date,
                lookback_days: int = 180
                ) -> Optional[List[Tuple[date, float, float, float, float]]]:
    p2 = int(datetime.combine(report_date + timedelta(days=1),
                              datetime.min.time()).timestamp())
    p1 = p2 - lookback_days * 86400
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
    try:
        r = requests.get(url, params={"period1": p1, "period2": p2,
                                      "interval": "1d"},
                         headers=UA, timeout=20)
        if r.status_code != 200:
            return None
        res = ((r.json().get("chart") or {}).get("result") or [None])[0]
        if not res:
            return None
        ts = res.get("timestamp") or []
        q = (res.get("indicators") or {}).get("quote") or [{}]
        o, h, l, c = q[0].get("open"), q[0].get("high"), q[0].get("low"), q[0].get("close")
        out = []
        for i, t in enumerate(ts):
            if None in (o[i], h[i], l[i], c[i]):
                continue
            d = datetime.fromtimestamp(t).date()
            if d <= report_date:
                out.append((d, float(o[i]), float(h[i]), float(l[i]), float(c[i])))
        return out or None
    except Exception:
        return None


def render_deep_dive_charts(snapshot: Dict[str, Any], out: Path,
                            tickers: List[str]) -> List[Path]:
    report_date_s = snapshot.get("report_date") or date.today().isoformat()
    rd = date.fromisoformat(report_date_s)
    paths = []
    for t in tickers[:5]:
        bars = _yahoo_ohlc(t, rd)
        if not bars:
            continue
        fig, ax = plt.subplots(figsize=(7, 3.2))
        xs = range(len(bars))
        closes = [b[4] for b in bars]
        ax.plot(xs, closes, color="#24292f", lw=1.0)
        ax.fill_between(xs, closes, closes[0], alpha=0.08, color="#0969da")
        ax.set_title(f"{t} · close through {report_date_s}")
        ax.set_xticks(list(xs[:: max(1, len(xs) // 5)]))
        ax.set_xticklabels(
            [bars[i][0].isoformat() for i in xs[:: max(1, len(xs) // 5)]],
            fontsize=7, rotation=20)
        chg = None
        market = (snapshot.get("sources") or {}).get("market") or {}
        for m in (market.get("top_movers") or []) + (market.get("sp500_constituents") or []):
            if (m.get("ticker") or "").upper() == t.upper():
                chg = m.get("change_percent")
                break
        if chg is not None:
            ax.annotate(f"{chg:+.2f}%", xy=(1, 1), xycoords="axes fraction",
                        ha="right", va="top", fontsize=10,
                        color="#1a7f37" if chg >= 0 else "#cf222e")
        path = out / f"05_{t.upper()}.png"
        _savefig(path)
        paths.append(path)
    return paths


def deep_dive_tickers_from_sections(ws: Path) -> List[str]:
    f = ws / "sections" / "06_deep_dives.md"
    if not f.exists():
        return []
    return re.findall(r"^###\s+\$?([A-Z.\-]{1,6})\b", f.read_text(encoding="utf-8"),
                      re.M)


def render_all(ws: Path) -> List[Path]:
    snapshot = _load(ws)
    out = ws / "final" / "charts"
    out.mkdir(parents=True, exist_ok=True)
    written: List[Path] = []
    # 01_us_close: GuanHe-style FT table first (needs DB + finviz snapshot);
    # the legacy snapshot-only bar chart covers failures so report.md
    # always has a chart at charts/01_us_close.png.
    us_close_done = False
    try:
        from us_close_chart import render_for_workspace
        p = render_for_workspace(ws)
        if p:
            written.append(p)
            us_close_done = True
            print(f"[charts] wrote {p.name} (GuanHe-style FT table)", flush=True)
    except Exception as e:  # noqa: BLE001
        print(f"[charts] us_close FT chart failed, legacy fallback: {e}", flush=True)
    if not us_close_done:
        try:
            p = render_us_close(snapshot, out)
            if p:
                written.append(p)
                print(f"[charts] wrote {p.name} (legacy)", flush=True)
        except Exception as e:  # noqa: BLE001
            print(f"[charts] render_us_close failed: {e}", flush=True)
    # 02_finviz_map: finviz 风格板块热力图(GuanHe Equity Market 首图;自绘自
    # finviz_screener 快照 —— elite.finviz.com/map 在本机 IP 被 Cloudflare
    # 交互挑战拦截,无法程序化截图;快照数值与 GuanHe 截图逐格一致)
    try:
        from finviz_map_chart import render_for_workspace as _render_map
        p = _render_map(ws)
        if p:
            written.append(p)
            print(f"[charts] wrote {p.name} (finviz treemap)", flush=True)
    except Exception as e:  # noqa: BLE001
        print(f"[charts] finviz map skipped: {e}", flush=True)
    # 02/03/04/05 系列图表暂不生成(未达要求,待逐张按需求优化);
    # render_sector_table / render_sector_rotation / render_release_charts /
    # render_deep_dive_charts 保留,恢复时在 render_all 里重新挂回即可。
    return written


def main() -> int:
    if len(sys.argv) < 2:
        print("usage: render_charts.py /path/to/pipeline/{date}", file=sys.stderr)
        return 1
    ws = Path(sys.argv[1]).resolve()
    if not (ws / "snapshot.json").exists():
        print(f"snapshot missing in {ws}", file=sys.stderr)
        return 1
    paths = render_all(ws)
    print(f"charts written: {len(paths)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
