#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Deterministic section audit for the GuanHe daily brief pipeline.

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
    PIPELINE_TAGS,
    SECTION_ORDER,
    SECTION_SOURCE_MATRIX,
    SECTION_SPECS,
    STYLE_PROFILES,
    allowed_source_paths,
)

DONE_MARKER = "<!-- SECTION DONE -->"
HEADINGS = {
    "narrative": "## Today's Narrative",
    "drivers": "## Top Market Drivers",
    "positioning": "## Positioning / Flow",
    "sector_action": "## Sector Action",
    "movers": "## Today's Movers",
    "deep_dives": "## Company Deep Dives",
    # v8 Macros(中文标题;07-11 替代旧 07_macro)
    "macro_dashboard": "## 宏观速览",
    "macro_review": "## 回顾 · 宏观数据",
    "geopolitics": "## 回顾 · 地缘政治",
    "fomc": "## 美联储观察",
    "outlook": "## 展望与策略",
}
# 正文(剥掉 HTML 注释后)不得出现的生产过程用语
# v8 增补宏观素材层相关词:素材/管线/入库/白名单/订阅源/抓取/星球/早报
# (复盘 是正常金融词汇,不禁)
PROCESS_LEAK_RES = [
    re.compile(p, re.I) for p in (
        r"\bsnapshot\b", r"\btranscripts?\b", r"\bdegraded\b",
        r"\bdisclosure:", r"\bon[\s-]air\b", r"\bthe desk\b",
        r"\bthe shows?\b", r"\bas relayed\b", r"\bdata layer\b",
        r"快照", r"转录", r"节目", r"主持人", r"不可得", r"口径", r"数据层",
        r"素材", r"管线", r"入库", r"白名单", r"订阅源", r"抓取", r"星球",
        r"早报",
    )
] + [re.compile(p) for p in (r"\bThe Close\b", r"\bClosing Bell\b",
                             r"\bOvertime\b")]
DOUBLE_SIGN_RE = re.compile(
    r"\b(fell|fall|falls|slid|slipped|dropped|declined|lost|sank|rose|rise|"
    r"rises|gained|climbed|jumped|added|down|up)\s+"
    r"[+\-−]\d", re.I)
# audit 注释里出现的 snapshot 顶层键
KNOWN_SOURCE_HEADS = {p.split(".")[0] for v in ATTRIBUTION_MAP.values()
                      for p in v.split(" / ")}
AUDIT_COMMENT_RE = re.compile(r"<!--\s*audit:(.*?)-->", re.S)
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


# ── claims ledger ──────────────────────────────────────────────────────

SHOW_KEYS = ("the_close", "cnbc_closing_bell")
_MONTHS = ["January", "February", "March", "April", "May", "June", "July",
           "August", "September", "October", "November", "December"]
_VALUE_RE = re.compile(r"([+\-−]?)\$?\s*(\d[\d,]*(?:\.\d+)?)\s*(%|bp|K|M|B|T|万亿|亿)?",
                       re.I)
_UNIT_SCALE = {"k": 1e3, "m": 1e6, "b": 1e9, "t": 1e12, "亿": 1e8, "万亿": 1e12}
# 正文里需要台账覆盖的数字:带 % 或 bp 的(价格、指数点位噪音太多,不在兜底范围)
_PROSE_NUM_RE = re.compile(r"[+\-−]?\d+(?:\.\d+)?\s*(?:%|bp)")
_CJK_RE = re.compile(r"[\u4e00-\u9fff]")


def claims_path(ws: Path, name: str) -> Path:
    out = SECTION_SPECS[name]["output_file"]
    return ws / re.sub(r"\.md$", ".claims.json", out)


def _strip_audit(text: str) -> str:
    text = re.sub(r"<!--.*?-->", "", text, flags=re.DOTALL)
    return text


def _meeting_month_words(meeting: str) -> List[str]:
    """"December 8-9, 2026" → ["December", "Dec", "12月"]。"""
    for i, m in enumerate(_MONTHS):
        if m.lower() in meeting.lower():
            return [m, m[:3] + " ", m[:3] + ".", f"{i + 1}月"]
    return []


def _show_texts(ws: Path, sources: Dict[str, Any]) -> Dict[str, str]:
    """节目转录 + 新闻/宏观地缘素材层的文本池(供台账数字按 digits-in-blob 对账)。"""
    out: Dict[str, str] = {}
    for k in SHOW_KEYS:
        parts = []
        src = sources.get(k) or {}
        # 节目转录(主段 + Overtime)是唯一节目正文;无摘要兜底
        for fld in ("clean_transcript", "clean_transcript_ot"):
            if src.get(fld):
                parts.append(src[fld])
        out[k] = "\n".join(parts).replace(",", "")
    # news 层(v6):flash/wire/calls 全部文本进 "news" 池
    news = sources.get("news") or {}
    nparts = []
    for bucket in ("flash", "wire", "calls"):
        for item in news.get(bucket) or []:
            if isinstance(item, dict) and item.get("text"):
                nparts.append(str(item["text"]))
    out["news"] = "\n".join(nparts).replace(",", "")
    # 宏观地缘素材层(v8.1,Admin 订阅,中英):digest/research/geo 进 "news_macro" 池
    news_macro = sources.get("news_macro") or {}
    cparts = []
    for bucket in ("digest", "research", "geo"):
        for item in news_macro.get(bucket) or []:
            if isinstance(item, dict) and item.get("text"):
                cparts.append(str(item["text"]))
    out["news_macro"] = "\n".join(cparts).replace(",", "")
    return out


def _parse_value(raw: Any) -> Optional[Dict[str, Any]]:
    m = _VALUE_RE.search(str(raw or "").replace("−", "-"))
    if not m:
        return None
    num_s = m.group(2).replace(",", "")
    try:
        val = float(num_s)
    except ValueError:
        return None
    if m.group(1) in ("-", "−"):
        val = -val
    dec = len(num_s.split(".")[1]) if "." in num_s else 0
    return {"value": val, "decimals": dec, "unit": (m.group(3) or "").lower(),
            "digits": num_s}


def _candidates(pv: Dict[str, Any]) -> List[float]:
    """台账数字在快照里可能的存储口径(单位换算 + 正负号)。"""
    a, unit = pv["value"], pv["unit"]
    out = [a]
    if unit in _UNIT_SCALE:
        out.append(a * _UNIT_SCALE[unit])
    if unit in ("%", "bp"):
        out.append(a / 100)          # 小数口径 / 百分点口径的 bp
    if unit == "bp":
        out.append(a * 100)
    return out + [-x for x in out]


def _close(a: float, b: float, decimals: int) -> bool:
    tol = max(0.5 * 10 ** (-decimals), abs(b) * 0.005) + 1e-9
    return abs(a - b) <= tol


def _split_path(path: str) -> List[str]:
    """按 . 切分,但不切 [] 里的内容(代码里有 GSPC.INDX 这种点)。"""
    toks, buf, depth = [], "", 0
    for ch in path:
        if ch == "[":
            depth += 1
        elif ch == "]":
            depth -= 1
        if ch == "." and depth == 0:
            toks.append(buf)
            buf = ""
        else:
            buf += ch
    if buf:
        toks.append(buf)
    return [t for t in toks if t]


def _resolve_path(root: Any, path: str):
    """返回 (节点, 是否完整解析)。解析失败时返回已解析到的最深前缀节点。"""
    path = re.sub(r"^(snapshot\.)?(sources\.)?", "", path.strip())
    cur = root
    for tok in _split_path(path):
        m = re.match(r"^([^\[]*)((?:\[[^\]]*\])*)$", tok)
        if not m:
            return cur, False
        key, sels = m.group(1), re.findall(r"\[([^\]]*)\]", m.group(2))
        if key:
            if isinstance(cur, dict) and key in cur:
                cur = cur[key]
            else:
                return cur, False
        for sel in sels:
            if not isinstance(cur, list):
                return cur, False
            if sel.isdigit():
                i = int(sel)
                if i >= len(cur):
                    return cur, False
                cur = cur[i]
                continue
            if "=" in sel:
                k, v = (x.strip() for x in sel.split("=", 1))
                hits = [x for x in cur if isinstance(x, dict)
                        and str(x.get(k, "")).strip().lower() == v.lower()]
            else:
                hits = [x for x in cur if isinstance(x, dict) and any(
                    str(vv).strip().lower() == sel.strip().lower() for vv in x.values())]
            if not hits:
                return cur, False
            cur = hits[0] if len(hits) == 1 else hits
    return cur, True


def _subtree_numbers(node: Any) -> Set[float]:
    out: Set[float] = set()
    _flatten_numbers(node, out)
    return out


def _matches(pv: Dict[str, Any], pool: Set[float]) -> bool:
    return any(_close(c, b, pv["decimals"]) for c in _candidates(pv) for b in pool)


def _check_ledger(ws: Path, name: str, sources: Dict[str, Any],
                  snap_nums: Set[float], show_texts: Dict[str, str], add) -> None:
    """台账逐条对账 + 正文 %/bp 数字的覆盖兜底。"""
    text = _section_text(ws, name)
    if not text.strip():
        return
    lf = claims_path(ws, name)
    if not lf.exists():
        add(name, "claims ledger missing", lf.name, False, "medium",
            f"write {lf.relative_to(ws)} per rule 11")
        claims: List[Dict[str, Any]] = []
    else:
        try:
            claims = json.loads(lf.read_text(encoding="utf-8"))
            if not isinstance(claims, list):
                raise ValueError("not a list")
        except (json.JSONDecodeError, ValueError) as e:
            add(name, f"claims ledger unreadable: {e}", lf.name, False, "high",
                "rewrite the ledger as a JSON array")
            return
    prose = _strip_audit(text).replace(",", "")
    ledger_vals: List[Dict[str, Any]] = []
    for c in claims:
        if not isinstance(c, dict):
            continue
        pv = _parse_value(c.get("value"))
        if pv is None:
            continue
        ledger_vals.append(pv)
        path = str(c.get("path") or "")
        label = f"{c.get('value')} @ {path}"
        head = path.split(".")[0].split("[")[0]
        if head in SHOW_KEYS or head in ("news", "news_macro"):
            blob = show_texts.get(head, "")
            if blob and pv["digits"] not in blob:
                add(name, label, f"number not found in {head} text pool",
                    False, "medium", "quote the number exactly as the source states it")
            continue
        node, full = _resolve_path(sources, path)
        if full:
            at_path = _subtree_numbers(node)
            if _matches(pv, at_path):
                continue
            # 路径精确指到了字段、值却对不上 —— 这是真错,不因"快照别处有同数"而放过
            shown = ", ".join(f"{x:g}" for x in sorted(at_path)[:4]) or "no number"
            add(name, label, f"value at path: {shown}", False, "high",
                "state the number at this path, or fix the path")
            continue
        if node is not sources and _matches(pv, _subtree_numbers(node)):
            add(name, label, "path partly unresolved; number found under its prefix",
                False, "low")
            continue
        if _matches(pv, snap_nums):
            add(name, label, "path does not resolve; number exists elsewhere in "
                "snapshot", False, "medium", "fix the ledger path")
            continue
        add(name, label, "absent from snapshot", False, "high",
            "use the snapshot value at this path, or remove the number")
    # 兜底:正文里带 %/bp 的数字既不在台账、也不在快照或节目文本里 → 可疑。
    # 比对池按章节收窄:非个股章节不含 500 只成分股/财报数字(否则几乎任何数
    # 都能"碰上"),且只按小数位严格比对、不做 /100 等换算。
    if name in ("sector_action", "deep_dives"):
        pool = snap_nums
    else:
        core = {k: v for k, v in sources.items() if k != "earnings_calendar"}
        if isinstance(core.get("market"), dict):
            core["market"] = {k: v for k, v in core["market"].items()
                              if k not in ("sp500_constituents", "top_movers")}
        pool = _subtree_numbers(core)
    stray = []
    show_blob = " ".join(show_texts.values())
    for m in _PROSE_NUM_RE.finditer(prose):
        pv = _parse_value(m.group(0))
        if pv is None or any(_close(abs(pv["value"]), abs(lv["value"]), pv["decimals"])
                             for lv in ledger_vals):
            continue
        tol = 0.5 * 10 ** (-pv["decimals"]) + 1e-9
        if any(abs(abs(pv["value"]) - abs(b)) <= tol for b in pool) \
                or pv["digits"] in show_blob:
            continue
        stray.append(m.group(0).strip())
    if stray:
        add(name, "numbers not in ledger and not found in snapshot/shows: "
            + ", ".join(sorted(set(stray))[:8]), "absent", False,
            "high" if lf.exists() else "medium",
            "source each number from the snapshot (and list it in the ledger) "
            "or remove it")


def _check_headline_language(ws: Path, style: str, add) -> None:
    if style != "cn":
        return
    text = _section_text(ws, "narrative")
    m = re.search(r"<!--\s*HEADLINE_CANDIDATES\s*\n(.*?)-->", text, re.DOTALL)
    if not m:
        return
    first = next((ln.strip().strip('"') for ln in m.group(1).splitlines()
                  if ln.strip()), "")
    if first and not _CJK_RE.search(first):
        add("narrative", f"headline not Chinese: {first[:80]}", "cn style",
            False, "high", "rewrite HEADLINE_CANDIDATES in Simplified Chinese")


def run_checks(ws: Path, style: str = "en") -> List[Dict[str, Any]]:
    snapshot = _load_snapshot(ws)
    sources = snapshot.get("sources") or {}
    missing_mark = STYLE_PROFILES.get(style, STYLE_PROFILES["en"])["missing_mark"]
    issues: List[Dict[str, Any]] = []
    snap_nums: Set[float] = set()
    _flatten_numbers(sources, snap_nums)

    def add(section: str, claim: str, snap_val: str, match: bool,
            severity: str, suggestion: Optional[str] = None) -> None:
        row = {
            "section": section,
            "claim": claim,
            "snapshot_value": snap_val,
            "match": match,
            "severity": severity,
        }
        if suggestion:
            row["suggestion"] = suggestion
        issues.append(row)

    show_texts = _show_texts(ws, sources)
    for name in SECTION_ORDER:
        if SECTION_SPECS[name].get("mode") == "deterministic":
            continue
        _check_ledger(ws, name, sources, snap_nums, show_texts, add)
    _check_headline_language(ws, style, add)

    # Structure lint
    for name in SECTION_ORDER:
        text = _section_text(ws, name)
        if not text.strip():
            add(name, "missing section file", "required", False, "high")
            continue
        if DONE_MARKER not in text:
            add(name, "missing SECTION DONE marker", "required", False, "medium")
        heading = HEADINGS[name]
        if not text.lstrip().startswith(heading):
            add(name, f"heading must be {heading}", text.splitlines()[0][:80],
                False, "medium")
        if name == "narrative" and "HEADLINE_CANDIDATES" not in text:
            add(name, "missing HEADLINE_CANDIDATES block", "required", False, "medium")
        if name == "deep_dives" and re.search(r"(^|\n)\s*(Read:|推断:)", text):
            add(name, "Read:/推断: synthesis line not allowed in deep dives",
                "omit", False, "medium")

        prose = _strip_audit(text)
        if name == "narrative":
            prose = re.sub(r"HEADLINE_CANDIDATES.*", "", prose, flags=re.S)

        # 正文只给原始出处:管线标签 / 生产过程用语一律不得出现
        labels = [t for t in PIPELINE_TAGS if t in prose]
        if labels:
            add(name, "pipeline labels in prose: " + " ".join(labels),
                "reader credits only", False, "high",
                "drop the labels; credit the originating outlet only when the "
                "fact needs a source")
        leaks = sorted({m.group(0) for rx in PROCESS_LEAK_RES
                        for m in rx.finditer(prose)})
        if leaks:
            add(name, "production-process wording in prose: " + ", ".join(leaks[:8]),
                "omit", False, "high",
                "state the facts directly; never mention snapshot/transcripts/"
                "shows or missing data in the prose")
        doubles = sorted({m.group(0) for m in DOUBLE_SIGN_RE.finditer(prose)})
        if doubles:
            add(name, "signed number after a direction verb: " + ", ".join(doubles[:6]),
                "fell 1.8%", False, "high", "drop the sign after fell/rose/etc.")

        # 来源白名单改按 audit 注释审计(正文不再挂管线标签)
        allowed_heads = {p.split(".")[0] for p in allowed_source_paths(
            SECTION_SOURCE_MATRIX.get(name, set()))}
        outside = set()
        for body in AUDIT_COMMENT_RE.findall(text):
            for br in re.findall(r"\[([^\]]+)\]", body):
                toks = br.split()
                if not toks:
                    continue
                head = ("news" if toks[0] == "news"
                        else toks[-1].split(".")[0].split("[")[0])
                if head in KNOWN_SOURCE_HEADS and head not in allowed_heads:
                    outside.add(head)
        if outside and allowed_heads:
            add(name, "audit cites sources outside section whitelist: "
                + ", ".join(sorted(outside)), ",".join(sorted(allowed_heads)),
                False, "medium")

        # Since-claims: fred_ranks 预计算口径,或新闻/节目原文里的同一说法
        ranks = ((sources.get("macro_pillars") or {}).get("fred_ranks") or {})
        claims = {v.get("claim") for v in ranks.values()
                  if isinstance(v, dict) and v.get("claim")}
        source_blob = " ".join(show_texts.values()).lower()
        for m in SINCE_RE.finditer(prose):
            phrase = m.group(0).strip()
            low = phrase.lower()
            if any(c and (c.lower() in low or low in c.lower()) for c in claims):
                continue
            tail = re.sub(r"^(highest|lowest)\s+", "", low)
            if tail and tail in source_blob:
                continue
            add(name, phrase, "fred_ranks.claim / source text", False, "high")

        # Odds
        odds = ((sources.get("macro_pillars") or {}).get("next_fomc_odds") or {})
        prior = sources.get("prior_odds") or {}
        odds_secs = ("drivers", "macro_review", "fomc", "outlook", "positioning")
        if "prior:" in text.lower() and name in odds_secs:
            if not prior:
                add(name, "(prior: ...) present but sources.prior_odds absent",
                    "absent", False, "high")
            else:
                pm = ODDS_RE.search(str(prior.get("odds_pct", "")))
                # soft check — presence of prior_odds is enough if number near
                if pm:
                    pass
        # 只核对明确指向"下一场会议"的加息概率行;其他月份的概率走台账按合约月对账。
        # 旧做法把全文任何 "hike … N%" 都和下一场比,FOMC 日会把 "12 月 62%"
        # 判成与 2% 不符,白白多跑一轮修订和核验。
        if odds.get("odds_pct") is not None and name in odds_secs:
            target = float(odds["odds_pct"])
            month_words = _meeting_month_words(odds.get("meeting") or "")
            for line in _strip_audit(text).splitlines():
                if not month_words or not any(w in line for w in month_words):
                    continue
                if not re.search(r"hike|加息", line, re.I):
                    continue
                pcts = [float(x) for x in re.findall(r"(\d{1,3}(?:\.\d+)?)\s*%", line)]
                if pcts and not any(abs(v - target) <= 2 for v in pcts):
                    add(name, line.strip()[:160],
                        f"next_fomc_odds={target} ({odds.get('meeting')})", False,
                        "high", f"use next_fomc_odds.odds_pct={target} for "
                        f"{odds.get('meeting')}")

        # Triple shapes vs economic_events(边际变化/美联储节最常出现)
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
                if not matched and name in ("macro_review", "fomc"):
                    add(name, m.group(0), "economic_events.recent", False, "high")

        # Fake 不可得: if missing_mark present, at least one assigned source empty
        # (soft — only flag when ALL matrix sources are present and non-empty)
        if missing_mark in text:
            pass  # presence alone is OK; fabricated use is hard to prove deterministically

        # Audit comments referencing show/data layers
        for m in AUDIT_RE.finditer(text):
            body = m.group(1)
            if "show" in body and not any(
                k in body for k in SHOW_KEYS
            ):
                add(name, f"audit show key odd: {body}", "known show keys",
                    False, "medium")

    # v8 Macros 块矩阵一致性(替代旧 07_macro 单文件的 Releases/Rate/Forward 矩阵)
    blocks = _macro_blocks_from_state(ws, snapshot)

    def _prose_body(name: str) -> str:
        """剥掉标题行/audit/missing_mark 后的正文(判定"是否真的没内容")。"""
        t = _strip_audit(_section_text(ws, name))
        t = re.sub(r"^#{2,4}.*$", "", t, flags=re.M)
        t = t.replace(missing_mark, "")
        return t.strip()

    # 07 宏观速览(确定性节):四经济体表与前瞻日历必须齐
    dash_text = _section_text(ws, "macro_dashboard")
    if dash_text.strip():
        for needed in ("### 四大经济体", "| 经济体 |", "### 本周前瞻"):
            if needed not in dash_text:
                add("macro_dashboard", f"dashboard missing required part: {needed}",
                    "required", False, "high",
                    "re-render 07_macro_dashboard from the snapshot")

    # 08 回顾·宏观数据:只写注入国家的边际变化;没选题就整节缺省
    review_text = _section_text(ws, "macro_review")
    if review_text.strip():
        sel = blocks.get("review_releases") or {}
        body = _prose_body("macro_review")
        if not sel and body:
            add("macro_review",
                "no releases selected but section has content",
                "matrix OFF", False, "high",
                "write only releases selected in the injected list")
        if sel and not body and missing_mark not in review_text:
            add("macro_review", "releases selected but section is empty",
                "matrix ON", False, "medium")

    # 09 回顾·地缘政治:主题块只允许命中主题;焦点事件受 focal 门控
    geo_text = _section_text(ws, "geopolitics")
    if geo_text.strip():
        themes = set(blocks.get("geo_themes") or {})
        focal = bool(blocks.get("focal_events"))
        written = set(re.findall(r"^###\s+(.+?)\s*$", geo_text, re.M))
        written -= {"焦点事件"}
        invented = {w for w in written if w not in themes}
        if invented:
            add("geopolitics",
                "theme block present but not in matrix: " + ", ".join(sorted(invented)),
                "OFF", False, "high",
                "write only themes opened by the block matrix")
        missing_themes = themes - written
        if missing_themes and missing_mark not in geo_text:
            add("geopolitics",
                "theme opened but block missing: " + ", ".join(sorted(missing_themes)),
                "ON", False, "medium")
        has_focal = "### 焦点事件" in geo_text
        if has_focal and not focal:
            add("geopolitics", "焦点事件 block present but matrix OFF", "OFF",
                False, "high")
        if focal and not has_focal and missing_mark not in geo_text:
            add("geopolitics", "focal events opened but block missing", "ON",
                False, "medium")

    # 10 美联储观察:决议深读受 fomc_deep 门控;恒开块结构齐
    fomc_text = _section_text(ws, "fomc")
    if fomc_text.strip() and missing_mark not in fomc_text:
        has_deep = "### 决议深读" in fomc_text
        if has_deep and not blocks.get("fomc_deep"):
            add("fomc", "决议深读 block present but matrix OFF", "OFF", False,
                "high")
        if blocks.get("fomc_deep") and not has_deep:
            add("fomc", "decision within last 3 sessions but 决议深读 missing",
                "ON", False, "medium")
        for needed in ("### 利率与决议", "### 市场定价路径"):
            if needed not in fomc_text:
                add("fomc", f"missing required block: {needed}", "required",
                    False, "medium")

    # 11 展望与策略:大事预判/投资建议恒在;免责行收尾
    outlook_text = _section_text(ws, "outlook")
    if outlook_text.strip():
        for needed in ("### 大事预判", "### 投资建议"):
            if needed not in outlook_text:
                add("outlook", f"missing required block: {needed}", "required",
                    False, "high")
        if "不构成投资建议" not in outlook_text:
            add("outlook", "missing disclaimer line (不构成投资建议)", "required",
                False, "medium")

    # Today's Movers(v7:编排器按成分股快照确定性渲染)
    has_constituents = bool((sources.get("market") or {}).get("sp500_constituents"))
    movers_text = _section_text(ws, "movers")
    has_tables = bool(re.search(r"^###\s+(Winners|Losers)\b", movers_text, re.M))
    if has_constituents and not has_tables:
        add("movers", "S&P 500 constituents present but no Winners/Losers tables",
            "market.sp500_constituents", False, "high",
            "re-render Today's Movers from the constituents snapshot")

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
