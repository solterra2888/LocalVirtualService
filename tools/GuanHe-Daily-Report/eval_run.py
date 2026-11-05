#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""一期报告的运行指标汇总(只读,不调 LLM、不连库)。

    python tools/GuanHe-Daily-Report/eval_run.py 2026-11-05 [> eval.md]

输出 Markdown:发布状态、token 分阶段占比、编辑计划、章节模式、台账写法、
核验残留、跨节重复数字、快照回填与时间戳抽查。真跑后把输出整段贴回即可。
"""

from __future__ import annotations

import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from prompts import SECTION_ORDER, SECTION_SPECS  # noqa: E402
from run_pipeline import workspace  # noqa: E402
from verify_checks import claims_path  # noqa: E402

_NUM_RE = re.compile(r"\d[\d,]*(?:\.\d+)?\s*(?:%|bp|B|bn|M|亿|万亿)", re.I)


def _load(p: Path, default=None):
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return default


def _fmt_tokens(u: dict) -> str:
    return (f"in {u.get('inputTokens', 0):,} / cache {u.get('cacheReadTokens', 0):,} "
            f"/ out {u.get('outputTokens', 0):,}")


def main() -> int:
    if len(sys.argv) < 2:
        print(__doc__)
        return 1
    ws = workspace(sys.argv[1])
    state = _load(ws / "state.json", {}) or {}
    snap = _load(ws / "snapshot.json", {}) or {}
    out = [f"# eval_run — {sys.argv[1]}", ""]

    # 1. 发布与核验
    verdict = _load(ws / "verify" / "verdict.json", {}) or {}
    out += ["## 1. 发布与核验",
            f"- published: {state.get('published')}  "
            f"NEEDS_HUMAN_REVIEW: {(ws / 'NEEDS_HUMAN_REVIEW.md').exists()}",
            f"- verify attempts: {state.get('verify', {}).get('attempts')}  "
            f"final verdict: {verdict.get('verdict')}  "
            f"deterministic high (last): "
            f"{state.get('verify', {}).get('deterministic_high_fails')}",
            "- revise timings: " + json.dumps(
                {k: v for k, v in (state.get("timings") or {}).items()
                 if k.startswith("revise:")}, ensure_ascii=False),
            ""]
    issues = verdict.get("issues") or []
    if issues:
        sev = Counter((i.get("severity"), bool(i.get("advisory")),
                       i.get("source") or "llm") for i in issues)
        out.append("- verdict issues (severity, advisory, source): "
                   + ", ".join(f"{k}×{v}" for k, v in sev.items()))
        out.append("")

    # 2. token 分阶段
    tot = state.get("usage_total") or {}
    phases = state.get("usage_by_phase") or {}
    out += ["## 2. Tokens", f"- total: {_fmt_tokens(tot)}"]
    t_in = tot.get("inputTokens") or 0
    for ph in ("plan", "write", "verify", "revise"):
        u = phases.get(ph)
        if u:
            share = f" ({u.get('inputTokens', 0) / t_in:.0%} of input)" if t_in else ""
            out.append(f"- {ph}: {_fmt_tokens(u)}{share}")
    if not phases:
        out.append("- usage_by_phase 缺失(旧版 state 或全部复用)")
    timings = state.get("timings") or {}
    out += ["- wall: total " + str(timings.get("total")) + "s; plan "
            + str(timings.get("plan")) + "s; slowest sections: " + ", ".join(
                f"{k}={max(v)}s" for k, v in sorted(
                    ((k, v) for k, v in timings.items() if k.startswith("section:")),
                    key=lambda kv: -max(kv[1]))[:4]), ""]

    # 3. 编辑计划
    plan = _load(ws / "plan.json")
    out += ["## 3. 编辑计划", f"- state.plan: {state.get('plan')}"]
    if isinstance(plan, dict):
        for s in plan.get("stories") or []:
            out.append(f"  - {s.get('id')} [{s.get('owner')}] {s.get('title')} "
                       f"(mention: {','.join(s.get('may_mention') or []) or '-'})")
        out.append(f"- canonical rows written: {len(plan.get('canonical') or [])}")
    else:
        out.append("- plan.json 不存在或不可解析")
    out.append("")

    # 4. 章节模式与体量
    out += ["## 4. 章节", "| 节 | mode | 字符 | 台账条数 | 条目引用 | 整桶引用 | entity+metric |",
            "|---|---|---|---|---|---|---|"]
    by_value = defaultdict(set)
    totals = Counter()
    for n in SECTION_ORDER:
        f = ws / SECTION_SPECS[n]["output_file"]
        text = f.read_text(encoding="utf-8") if f.exists() else ""
        rec = (state.get("sections") or {}).get(n, {})
        claims = _load(claims_path(ws, n), []) or []
        claims = [c for c in claims if isinstance(c, dict)]
        item = sum(1 for c in claims if re.search(r"^news(_macro)?\.\w+\[", str(c.get("path"))))
        bare = sum(1 for c in claims if re.fullmatch(
            r"news(_macro)?(\.\w+)?", str(c.get("path") or "").strip()))
        em = sum(1 for c in claims if c.get("entity") and c.get("metric"))
        totals.update(claims=len(claims), item=item, bare=bare, em=em)
        mode = rec.get("mode") or SECTION_SPECS[n].get("mode") or "llm"
        out.append(f"| {n} | {mode}{' (' + rec['note'] + ')' if rec.get('note') else ''} "
                   f"| {len(text)} | {len(claims)} | {item} | {bare} | {em} |")
        if rec.get("mode") != "deterministic" and SECTION_SPECS[n].get("mode") != "deterministic":
            body = re.sub(r"<!--.*?-->", "", text, flags=re.S)
            for m in _NUM_RE.finditer(body):
                v = re.sub(r"[\s,]", "", m.group(0)).lower()
                if not re.fullmatch(r"\d(\.\d)?%", v):  # 个位数百分比太常见,不计
                    by_value[v].add(n)
    out += ["", f"- 台账合计 {totals['claims']} 条:条目引用 {totals['item']}、"
            f"整桶引用 {totals['bare']}、带 entity+metric {totals['em']}", ""]

    # 5. 确定性核验残留
    rows = _load(ws / "verify" / "check_results.json", []) or []
    out += ["## 5. 确定性核验残留(最后一轮)",
            "- " + ", ".join(f"{k}×{v}" for k, v in Counter(
                r.get("severity") for r in rows).items()) if rows else "- 无"]
    for r in rows:
        if r.get("severity") == "high" or "cross-section" in str(r.get("snapshot_value")):
            out.append(f"  - [{r.get('severity')}] ({r.get('section')}) "
                       f"{str(r.get('claim'))[:140]} | {str(r.get('snapshot_value'))[:80]}")
    out.append("")

    # 6. 跨节重复数字(正文)
    dup = sorted(((v, s) for v, s in by_value.items() if len(s) >= 3),
                 key=lambda x: -len(x[1]))
    out += ["## 6. 出现在 ≥3 个 LLM 节的数字(重复叙述的粗指标)",
            f"- 共 {len(dup)} 个"]
    out += [f"  - {v}: {', '.join(sorted(s, key=SECTION_ORDER.index))}" for v, s in dup[:15]]
    out.append("")

    # 7. 快照抽查
    notes = snap.get("notes") or {}
    src = snap.get("sources") or {}
    flash = (src.get("news") or {}).get("flash") or []
    hours = Counter((x.get("time_et") or "??")[:2] for x in flash)
    out += ["## 7. 快照",
            f"- actuals_from_flash: {json.dumps(notes.get('actuals_from_flash'), ensure_ascii=False)}",
            f"- flash 条目 {len(flash)},带 id {sum(1 for x in flash if x.get('id'))}",
            "- flash time_et 小时分布: " + ", ".join(
                f"{h}:{c}" for h, c in sorted(hours.items())),
            f"- macro_blocks: {json.dumps({k: (list(v) if isinstance(v, dict) else v) for k, v in (state.get('macro_blocks') or {}).items() if k != 'focal_events'}, ensure_ascii=False)[:600]}",
            f"- snapshot errors: {json.dumps(snap.get('errors'), ensure_ascii=False)[:400]}",
            ""]
    print("\n".join(out))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
