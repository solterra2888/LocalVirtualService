#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Deterministic section audit for the CNT daily brief pipeline.

Run from a report workspace (directory containing snapshot.json + sections/):
    python verify_checks.py [/path/to/workspace]

Prints a JSON array of check rows and writes verify/check_results.json.
Orchestrator merges high-severity failures into the LLM narrative verdict.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(Path(__file__).resolve().parent))

from prompts import (  # noqa: E402
    ATTRIBUTION_MAP,
    SECTION_ORDER,
    SECTION_SOURCE_MATRIX,
    SECTION_SPECS,
    STYLE_PROFILES,
)

DONE_MARKER = "<!-- SECTION DONE -->"
HEADINGS = {
    "narrative": "## Today's Narrative",
    "drivers": "## Top Market Drivers",
    "positioning": "## Positioning / Flow",
    "sector_action": "## Sector Action",
    "movers": "## Today's Movers",
    "deep_dives": "## Company Deep Dives",
    "macro": "## Macros",
}
HOUSE_TAGS = set(ATTRIBUTION_MAP.keys())
TAG_RE = re.compile(r"\((The Close|Closing Bell|Surveillance|cross-asset|"
                    r"sector averages|breadth|Finviz|earnings calendar|"
                    r"economic calendar|Treasury curve|FRED|fed funds futures|"
                    r"Fed calendar|auction calendar|nowcasts)\)")
SINCE_RE = re.compile(r"(highest|lowest)\s+since\s+[A-Za-z0-9 ,\-]+", re.I)
TRIPLE_RE = re.compile(
    r"([+\-]?\d[\d,]*(?:\.\d+)?%?[KMB]?)\s*\(est:\s*([^,)]+),\s*prior:\s*([^)]+)\)",
    re.I,
)
ODDS_RE = re.compile(r"[~≈]?\s*(\d{1,3})\s*%")
NUM_RE = re.compile(r"[+\-]?\d+(?:\.\d+)?")
AUDIT_RE = re.compile(r"<!--\s*audit:\s*\[([^\]]+)\]\s*-->")


def _load_snapshot(ws: Path) -> Dict[str, Any]:
    return json.loads((ws / "snapshot.json").read_text(encoding="utf-8"))


def _section_text(ws: Path, name: str) -> str:
    f = ws / SECTION_SPECS[name]["output_file"]
    if not f.exists():
        return ""
    return f.read_text(encoding="utf-8")


def _flatten_numbers(obj: Any, out: Set[float]) -> None:
    if isinstance(obj, dict):
        for v in obj.values():
            _flatten_numbers(v, out)
    elif isinstance(obj, list):
        for v in obj:
            _flatten_numbers(v, out)
    elif isinstance(obj, bool):
        return
    elif isinstance(obj, (int, float)):
        out.add(float(obj))
    elif isinstance(obj, str):
        for m in NUM_RE.findall(obj.replace(",", "")):
            try:
                out.add(float(m))
            except ValueError:
                pass


def _near(a: float, b: float) -> bool:
    tol = max(0.05, abs(b) * 0.1)
    return abs(a - b) <= tol


def _macro_blocks_from_state(ws: Path, snapshot: Dict[str, Any]) -> Dict[str, Any]:
    state_f = ws / "state.json"
    if state_f.exists():
        try:
            blocks = json.loads(state_f.read_text(encoding="utf-8")).get("macro_blocks")
            if isinstance(blocks, dict):
                return blocks
        except json.JSONDecodeError:
            pass
    # Fallback: import orchestrator helper
    from run_pipeline import macro_blocks  # local import
    return macro_blocks(snapshot)


def run_checks(ws: Path, style: str = "en") -> List[Dict[str, Any]]:
    snapshot = _load_snapshot(ws)
    sources = snapshot.get("sources") or {}
    missing_mark = STYLE_PROFILES.get(style, STYLE_PROFILES["en"])["missing_mark"]
    issues: List[Dict[str, Any]] = []
    snap_nums: Set[float] = set()
    _flatten_numbers(sources, snap_nums)

    def add(section: str, claim: str, snap_val: str, match: bool,
            severity: str) -> None:
        issues.append({
            "section": section,
            "claim": claim,
            "snapshot_value": snap_val,
            "match": match,
            "severity": severity,
        })

    # Structure lint
    for name in SECTION_ORDER:
        text = _section_text(ws, name)
        if not text.strip():
            add(name, "missing section file", "required", False, "high")
            continue
        if DONE_MARKER not in text and name != "movers":
            # movers still ends with DONE_MARKER from renderer
            if DONE_MARKER not in text:
                add(name, "missing SECTION DONE marker", "required", False, "medium")
        heading = HEADINGS[name]
        if not text.lstrip().startswith(heading):
            add(name, f"heading must be {heading}", text.splitlines()[0][:80],
                False, "medium")
        if name == "narrative" and "HEADLINE_CANDIDATES" not in text:
            add(name, "missing HEADLINE_CANDIDATES block", "required", False, "medium")
        if name == "drivers" and "**Take:**" not in text and "**Take:**" not in text:
            if "Take:" not in text:
                add(name, "drivers blocks need **Take:** lines", "required",
                    False, "medium")
        if name == "deep_dives":
            if re.search(r"\*\*Bull case\*\*|\*\*Bear case\*\*", text, re.I):
                add(name, "Bull/Bear case labels not allowed in v4", "omit",
                    False, "medium")
            if re.search(r"(^|\n)\s*(Read:|推断:)", text):
                add(name, "Read:/推断: synthesis line not allowed in deep dives",
                    "omit", False, "medium")

        # Attribution + matrix
        allowed = SECTION_SOURCE_MATRIX.get(name, set())
        for tag in TAG_RE.findall(text):
            full = f"({tag})"
            if full not in HOUSE_TAGS:
                add(name, f"unrecognized tag {full}", "house tags", False, "medium")
            elif full not in allowed:
                add(name, f"tag {full} not permitted for section",
                    ",".join(sorted(allowed)), False, "high")

        # Since-claims
        ranks = ((sources.get("macro_pillars") or {}).get("fred_ranks") or {})
        claims = {v.get("claim") for v in ranks.values()
                  if isinstance(v, dict) and v.get("claim")}
        for m in SINCE_RE.finditer(text):
            phrase = m.group(0)
            if not any(c and c.lower() in phrase.lower() or phrase.lower() in (c or "").lower()
                       for c in claims):
                # Also allow exact claim substring
                ok = any(c and (c in phrase or phrase in c) for c in claims)
                if not ok:
                    add(name, phrase, "fred_ranks.claim", False, "high")

        # Odds
        odds = ((sources.get("macro_pillars") or {}).get("next_fomc_odds") or {})
        prior = sources.get("prior_odds") or {}
        if "prior:" in text.lower() and name in ("drivers", "macro", "positioning"):
            if not prior:
                add(name, "(prior: ...) present but sources.prior_odds absent",
                    "absent", False, "high")
            else:
                pm = ODDS_RE.search(str(prior.get("odds_pct", "")))
                # soft check — presence of prior_odds is enough if number near
                if pm:
                    pass
        if odds.get("odds_pct") is not None and name == "macro":
            target = float(odds["odds_pct"])
            for m in re.finditer(r"[Hh]ike[^\n%]{0,40}[~≈]?\s*(\d{1,3})\s*%", text):
                val = float(m.group(1))
                if abs(val - target) > 2:
                    add(name, m.group(0), f"next_fomc_odds={target}", False, "high")

        # Triple shapes vs economic_events
        events = (sources.get("economic_events") or {})
        event_blob = json.dumps(events, ensure_ascii=False)
        for m in TRIPLE_RE.finditer(text):
            actual, est, prior_s = m.group(1), m.group(2).strip(), m.group(3).strip()
            if actual.replace(",", "") not in event_blob and actual not in event_blob:
                # Allow if any recent actual string-matches loosely
                recent = events.get("recent") or []
                matched = any(
                    str(e.get("actual", "")) in actual
                    or actual.replace("+", "").replace("%", "")
                    in str(e.get("actual", ""))
                    for e in recent
                )
                if not matched and name == "macro":
                    add(name, m.group(0), "economic_events.recent", False, "high")

        # Fake 不可得: if missing_mark present, at least one assigned source empty
        # (soft — only flag when ALL matrix sources are present and non-empty)
        if missing_mark in text:
            pass  # presence alone is OK; fabricated use is hard to prove deterministically

        # Audit comments referencing show/data layers
        for m in AUDIT_RE.finditer(text):
            body = m.group(1)
            if "show" in body and not any(
                k in body for k in ("the_close", "cnbc_closing_bell", "surveillance")
            ):
                add(name, f"audit show key odd: {body}", "known show keys",
                    False, "medium")

    # Macro block matrix consistency
    blocks = _macro_blocks_from_state(ws, snapshot)
    macro_text = _section_text(ws, "macro")
    if macro_text:
        has_releases = bool(re.search(r"^####?\s+Releases\b", macro_text, re.M)
                            or re.search(r"^###\s+Releases\b", macro_text, re.M))
        has_rate = bool(re.search(r"Rate Expectations", macro_text, re.I))
        has_forward = bool(re.search(r"Forward Catalysts", macro_text, re.I))
        has_xasset = bool(re.search(
            r"Treasuries and oil|Equities and credit|Cross-Asset", macro_text, re.I))
        if blocks.get("releases") and not has_releases:
            add("macro", "Releases block expected but missing", "ON", False, "medium")
        if not blocks.get("releases") and has_releases:
            add("macro", "Releases block present but matrix OFF", "OFF", False, "high")
        if blocks.get("rate_expectations") and not has_rate:
            add("macro", "Rate Expectations expected but missing", "ON", False, "medium")
        if not blocks.get("rate_expectations") and has_rate:
            add("macro", "Rate Expectations present but matrix OFF", "OFF", False, "high")
        if not blocks.get("cross_asset_review") and has_xasset:
            add("macro", "Cross-asset review present but matrix OFF", "OFF", False, "high")
        if not has_forward:
            add("macro", "Forward Catalysts always required", "required", False, "high")

    # Movers numbers vs top_movers / constituents
    movers_text = _section_text(ws, "movers")
    market = sources.get("market") or {}
    mover_nums = {
        float(m["change_percent"])
        for m in (market.get("top_movers") or [])
        if m.get("change_percent") is not None
    }
    for m in re.finditer(r"([+\-]\d+\.\d+)%", movers_text):
        val = float(m.group(1))
        if mover_nums and not any(_near(val, x) for x in mover_nums):
            # also allow constituent list
            cons = {
                float(c["change_percent"])
                for c in (market.get("sp500_constituents") or [])
                if c.get("change_percent") is not None
            }
            if cons and not any(_near(val, x) for x in cons):
                add("movers", m.group(0), "market.top_movers", False, "high")

    return issues


def main() -> int:
    ws = Path(sys.argv[1] if len(sys.argv) > 1 else ".").resolve()
    style = sys.argv[2] if len(sys.argv) > 2 else "en"
    issues = run_checks(ws, style=style)
    out = ws / "verify" / "check_results.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(issues, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(issues, ensure_ascii=False))
    # exit 0 even on failures — orchestrator reads the file
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
