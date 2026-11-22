#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""素材质量评估(只读,不调 LLM、不连库):快照选材 → 编辑计划 → 成稿引用。

    python tools/facts-report/eval_data_quality.py 2026-11-18 2026-11-20
    python tools/facts-report/eval_data_quality.py /path/to/ws_or_snapshot.json ...
    python tools/facts-report/eval_data_quality.py --json 2026-11-20

参数是日期(按 run_pipeline.workspace 解析,受 FACTS_REPORT_ROOT 影响)、工作区
目录或单个 snapshot JSON(可只含 news / news_macro)。新旧快照同一把尺子:旧快照
没有 tier 字段时按 source_quality 现算。

指标与目标(2026-11 数据质量评估方案):
  wire   第 1 层占比 / B·R L1 召回 / 时段覆盖 / SEO·公告·过期稿条数 / 字数
  flash  残留近似重复簇 / 转播号占比 / 字数
  geo    聚合站垃圾条数 / 第 1-2 层占比
  plan   故事数 / 带 evidence 的故事 / evidence 含第 1 层 / 规范数值取自新闻的条数
  report 成稿引用的条目(按池、按层级),第 1 层被引条数
  cost   切片字数(snapshot.{agent}.json)与 token(state.json)
"""

from __future__ import annotations

import json
import re
import sys
from collections import Counter
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent))

import source_quality as sq  # noqa: E402

_ID_POOL = {"f": ("news", "flash"), "w": ("news", "wire"), "c": ("news", "calls"),
            "d": ("news_macro", "digest"), "r": ("news_macro", "research"),
            "g": ("news_macro", "geo")}
_AUDIT_RE = re.compile(r"<!--\s*audit:(.*?)-->", re.S)
_AUDIT_GROUP_RE = re.compile(r"\[(news[^\]]*)\]")
_ITEM_ID_RE = re.compile(r"\b([fwcdrg])(\d+)\b")


def _load(p: Path) -> Any:
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _resolve(arg: str) -> tuple:
    """→ (label, workspace or None, snapshot dict)."""
    p = Path(arg)
    if p.suffix == ".json" and p.exists():
        return arg, None, _load(p) or {}
    if p.is_dir():
        return p.name, p, _load(p / "snapshot.json") or {}
    from run_pipeline import workspace
    ws = workspace(arg)
    return arg, ws, _load(ws / "snapshot.json") or {}


def _tier(item: Dict[str, Any], channel: str) -> int:
    if isinstance(item.get("tier"), int):
        return item["tier"]
    t = sq.source_tier(item.get("author"), item.get("channel_type") or channel)
    if channel == "twitter":
        return sq.effective_tier(t, sq.credited_origin(item.get("text") or ""))
    if t != 1:
        return sq.effective_tier(t, sq.syndicated_origin(item.get("text") or ""))
    return t


def _report_date(snap: Dict[str, Any], label: str) -> Optional[date]:
    d = snap.get("report_date") or label
    m = re.search(r"\d{4}-\d{2}-\d{2}", str(d))
    return date.fromisoformat(m.group(0)) if m else None


def snapshot_metrics(src: Dict[str, Any], t: Optional[date]) -> Dict[str, Any]:
    news = src.get("news") or {}
    wire = news.get("wire") or []
    flash = news.get("flash") or []
    geo = (src.get("news_macro") or {}).get("geo") or []
    research = (src.get("news_macro") or {}).get("research") or []
    out: Dict[str, Any] = {}

    wt = [_tier(w, "news") for w in wire]
    non_t1 = [w for w, tr in zip(wire, wt) if tr != 1]
    kinds = Counter(sq.classify_article(w.get("title") or "", w.get("text") or "")
                    for w in non_t1)
    stale = sum(1 for w in non_t1 if t and sq.stale_session_date(w.get("text") or "", t))
    buckets = Counter()
    for w in wire:
        tm = w.get("time_et") or "12:00"
        if w.get("date_et") and t and w["date_et"] < t.isoformat():
            buckets["pre"] += 1
        elif w.get("date_et") and t and w["date_et"] > t.isoformat():
            buckets["post"] += 1
        else:
            buckets["pre" if tm < "09:30" else "session" if tm < "16:00" else "post"] += 1
    sel = (news.get("selection") or {}).get("wire") or {}
    out["wire"] = {
        "n": len(wire),
        "t1": sum(1 for x in wt if x == 1),
        "t1_share": round(sum(1 for x in wt if x == 1) / len(wire), 2) if wire else 0,
        "t1_l1": sum(1 for w, x in zip(wire, wt) if x == 1 and w.get("info_level") == "L1"),
        "t1_l1_available": sel.get("t1_l1_available"),
        "by_tier": dict(sorted(Counter(wt).items())),
        "junk": sum(v for k, v in kinds.items() if k != "news"),
        "junk_kinds": {k: v for k, v in kinds.items() if k != "news"},
        "stale": stale,
        "buckets": dict(buckets),
        "chars": sum(len(w.get("text") or "") for w in wire),
        "reported": sum(1 for w in wire if w.get("reported")),
    }

    items = []
    for f in flash:
        items.append({"text": f.get("text") or "",
                      "_ts": _flash_ts(f, t)})
    clusters = [c for c in sq.cluster_flash(items) if len(c) > 1] if items else []
    ft = [_tier(f, "twitter") for f in flash]
    out["flash"] = {
        "n": len(flash),
        "l1": sum(1 for f in flash if f.get("info_level") == "L1"),
        "dup_clusters_left": len(clusters),
        "dup_items_left": sum(len(c) - 1 for c in clusters),
        "max_cluster": max((len(c) for c in clusters), default=1),
        "relay_share": round(sum(1 for x in ft if x >= 3) / len(flash), 2) if flash else 0,
        "with_origin": sum(1 for f in flash if f.get("origin")
                           or sq.credited_origin(f.get("text") or "")),
        "chars": sum(len(f.get("text") or "") for f in flash),
    }

    gt = [_tier(g, g.get("channel_type") or "news") for g in geo]
    geo_junk = sum(1 for g, tr in zip(geo, gt) if g.get("channel_type") == "news" and tr >= 3
                   and sq.classify_article(g.get("title") or "", g.get("text") or "") != "news")
    out["geo"] = {"n": len(geo), "by_tier": dict(sorted(Counter(gt).items())),
                  "aggregator_junk": geo_junk,
                  "chars": sum(len(g.get("text") or "") for g in geo)}
    out["research"] = {"n": len(research),
                       "truncated_titles": sum(1 for r in research
                                               if str(r.get("title") or "").endswith(("...", "…")))}
    return out


def _flash_ts(f: Dict[str, Any], t: Optional[date]):
    from datetime import datetime
    d = f.get("date_et") or (t.isoformat() if t else "2000-01-01")
    try:
        return datetime.fromisoformat(f"{d}T{f.get('time_et') or '00:00'}:00").replace(
            tzinfo=sq.ET)
    except ValueError:
        return datetime(2000, 1, 1, tzinfo=sq.ET)


def _tier_of_id(src: Dict[str, Any], iid: str) -> Optional[int]:
    head, sub = _ID_POOL[iid[0]]
    for it in (src.get(head) or {}).get(sub) or []:
        if it.get("id") == iid:
            channel = "twitter" if sub == "flash" else it.get("channel_type") or "news"
            return _tier(it, channel)
    return None


def plan_metrics(ws: Path, src: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    plan = _load(ws / "plan.json")
    if not isinstance(plan, dict):
        return None
    stories = [s for s in plan.get("stories") or [] if isinstance(s, dict)]
    ev_t1 = 0
    with_ev = 0
    for s in stories:
        ev = [str(e) for e in s.get("evidence") or []]
        if ev:
            with_ev += 1
        if any(_ITEM_ID_RE.fullmatch(e) and _tier_of_id(src, e) == 1 for e in ev):
            ev_t1 += 1
    canon = [c for c in plan.get("canonical") or [] if isinstance(c, dict)]
    return {"stories": len(stories), "with_evidence": with_ev,
            "evidence_has_t1": ev_t1,
            "canonical": len(canon),
            "canonical_from_news": sum(1 for c in canon
                                       if str(c.get("path") or "").startswith("news"))}


def report_metrics(ws: Path, src: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    secs = sorted((ws / "sections").glob("*.md")) if (ws / "sections").is_dir() else []
    if not secs:
        return None
    cited: Dict[str, set] = {}
    for f in secs:
        ids = set()
        for m in _AUDIT_RE.finditer(f.read_text(encoding="utf-8")):
            for g in _AUDIT_GROUP_RE.finditer(m.group(1)):
                ids.update(a + b for a, b in _ITEM_ID_RE.findall(g.group(1)))
        cited[f.stem] = ids
    all_ids = set().union(*cited.values())
    by_pool = Counter(".".join(_ID_POOL[i[0]]) for i in all_ids)
    by_tier = Counter(_tier_of_id(src, i) for i in all_ids)
    return {"cited_ids": len(all_ids), "by_pool": dict(by_pool),
            "by_tier": {str(k): v for k, v in sorted(by_tier.items(), key=lambda kv: str(kv[0]))},
            "t1_cited": by_tier.get(1, 0),
            "t1_cited_by_section": {k: sum(1 for i in v if _tier_of_id(src, i) == 1)
                                    for k, v in cited.items()
                                    if any(_tier_of_id(src, i) == 1 for i in v)}}


def cost_metrics(ws: Path) -> Dict[str, Any]:
    slices = {p.stem.split(".", 1)[1]: p.stat().st_size
              for p in ws.glob("snapshot.*.json")}
    state = _load(ws / "state.json") or {}
    tot = state.get("usage_total") or {}
    return {"slice_bytes": slices,
            "input_tokens": tot.get("inputTokens"),
            "output_tokens": tot.get("outputTokens")}


def evaluate(arg: str) -> Dict[str, Any]:
    label, ws, snap = _resolve(arg)
    src = snap.get("sources") if "sources" in snap else snap
    t = _report_date(snap, label)
    out: Dict[str, Any] = {"label": label, "snapshot": snapshot_metrics(src or {}, t)}
    if ws is not None:
        out["plan"] = plan_metrics(ws, src or {})
        out["report"] = report_metrics(ws, src or {})
        out["cost"] = cost_metrics(ws)
    return out


def _md(rows: List[Dict[str, Any]]) -> str:
    lines = ["| 期 | wire n / 第1层 / B·R L1(可得) | 时段 | 垃圾 / 过期 | wire 字数 "
             "| flash n / 残留重复 / 最大簇 / 转播占比 | geo 垃圾 / 第1-2层 "
             "| 计划 evidence含第1层 / 规范取自新闻 | 成稿引用 / 第1层 |",
             "|---|---|---|---|---|---|---|---|---|"]
    for r in rows:
        w = r["snapshot"]["wire"]
        f = r["snapshot"]["flash"]
        g = r["snapshot"]["geo"]
        p = r.get("plan") or {}
        rp = r.get("report") or {}
        top12 = sum(v for k, v in g["by_tier"].items() if int(k) <= 2)
        lines.append(
            f"| {r['label']} | {w['n']} / {w['t1']} / {w['t1_l1']}"
            f"({w['t1_l1_available'] if w['t1_l1_available'] is not None else '?'}) "
            f"| {len(w['buckets'])} | {w['junk']} / {w['stale']} | {w['chars']:,} "
            f"| {f['n']} / {f['dup_items_left']} / {f['max_cluster']} / {f['relay_share']} "
            f"| {g['aggregator_junk']} / {top12}/{g['n']} "
            f"| {p.get('evidence_has_t1', '-')}/{p.get('stories', '-')} / "
            f"{p.get('canonical_from_news', '-')} "
            f"| {rp.get('cited_ids', '-')} / {rp.get('t1_cited', '-')} |")
    return "\n".join(lines)


def main() -> int:
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    if not args:
        print(__doc__)
        return 1
    rows = [evaluate(a) for a in args]
    if "--json" in sys.argv:
        print(json.dumps(rows, ensure_ascii=False, indent=2))
    else:
        print(_md(rows))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
