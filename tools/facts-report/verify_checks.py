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
from provenance import citation_issues, item_index  # noqa: E402

DONE_MARKER = "<!-- SECTION DONE -->"
HEADINGS = {
    # v8.6 起 01-06 节标题与 07-11 一样统一为中文
    "narrative": "## 今日要闻",
    "drivers": "## 核心驱动",
    "positioning": "## 持仓与资金流",
    "sector_action": "## 板块动态",
    "movers": "## 今日涨跌榜",
    "deep_dives": "## 个股深挖",
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


def _state_sections(ws: Path) -> Dict[str, Any]:
    try:
        secs = json.loads((ws / "state.json").read_text(encoding="utf-8")).get("sections")
    except (OSError, json.JSONDecodeError):
        return {}
    return secs if isinstance(secs, dict) else {}


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
_VALUE_RE = re.compile(r"([+\-−]?)\$?\s*(\d[\d,]*(?:\.\d+)?)\s*(%|bp|K|M|B|T|万亿|亿|万)?",
                       re.I)
_UNIT_SCALE = {"k": 1e3, "m": 1e6, "b": 1e9, "t": 1e12, "万": 1e4, "亿": 1e8,
               "万亿": 1e12}
_WORD_SCALE = {"thousand": 1e3, "million": 1e6, "billion": 1e9, "trillion": 1e12,
               "mn": 1e6, "bn": 1e9}
_WORD_NUM_RE = re.compile(r"(\d+(?:\.\d+)?)\s*(thousand|million|billion|trillion|mn|bn)\b",
                          re.I)
# 文本素材层:台账按条目引用(news.flash[id=f12]),数字只在被引条目里找
_TEXT_POOLS = {"news": ("flash", "wire", "calls"),
               "news_macro": ("digest", "research", "geo")}
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
                # 多条件 [author=Reuters,time_cn=11-03 16:24]:逐条都要命中
                conds = [tuple(x.strip() for x in part.split("=", 1))
                         for part in re.split(r",\s*(?=[\w]+=)", sel)]
                hits = [x for x in cur if isinstance(x, dict) and all(
                    str(x.get(k, "")).strip().lower() == v.lower() for k, v in conds)]
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


def _with_tol(num_s: str, scale: float = 1.0) -> tuple:
    dec = len(num_s.split(".")[1]) if "." in num_s else 0
    return (float(num_s) * scale, 0.5 * 10 ** (-dec) * scale)


def _text_numbers(text: str) -> List[tuple]:
    """文本里的数字 [(值, 容差)],含单位换算(7.8M→7.8e6、752万→7.52e6、
    25 billion→2.5e10);容差按原文写出的小数位。"""
    t = str(text or "").replace(",", "").replace("−", "-")
    out: List[tuple] = []
    for m in _VALUE_RE.finditer(t):
        num_s, unit = m.group(2), (m.group(3) or "").lower()
        out.append(_with_tol(num_s))
        if unit in _UNIT_SCALE:
            out.append(_with_tol(num_s, _UNIT_SCALE[unit]))
    for m in _WORD_NUM_RE.finditer(t):
        out.append(_with_tol(m.group(1), _WORD_SCALE[m.group(2).lower()]))
    return out


def _item_blob(node: Any) -> str:
    items = node if isinstance(node, list) else [node]
    return "\n".join(f"{it.get('title') or ''} {it.get('text') or ''}"
                     for it in items if isinstance(it, dict))


def _pool_index(sources: Dict[str, Any]) -> Dict[str, List[tuple]]:
    """{head: 按数值排序的 [(abs 值, 容差, 条目 id)]},给"这个数在哪几条里"做二分查找。"""
    out: Dict[str, List[tuple]] = {}
    for head, buckets in _TEXT_POOLS.items():
        rows = []
        for bucket in buckets:
            for i, it in enumerate((sources.get(head) or {}).get(bucket) or []):
                if not isinstance(it, dict):
                    continue
                iid = it.get("id") or f"{bucket}[{i}]"
                rows.extend((v, tol, iid) for v, tol in _text_numbers(_item_blob(it)))
        out[head] = sorted((abs(v), t, i) for v, t, i in rows)
    return out


def _text_candidates(pv: Dict[str, Any]) -> List[tuple]:
    """台账数字在文本里的写法:原值或单位换算后的值(带各自容差)。不做
    % ↔ 小数口径换算 —— 那是结构化字段的存法,放进文本池会让 91.37% 碰上
    随便哪条里的 0.91。"""
    num_s = pv["digits"]
    out = [_with_tol(num_s)]
    if pv["unit"] in _UNIT_SCALE:
        out.append(_with_tol(num_s, _UNIT_SCALE[pv["unit"]]))
    return out


def _text_hit(c: tuple, b: tuple) -> bool:
    # 只按台账写出的精度放宽:原文 91% 撑不起台账的 91.37%
    return abs(c[0] - abs(b[0])) <= c[1] + 1e-9


def _matches_text(pv: Dict[str, Any], nums: List[tuple]) -> bool:
    return any(_text_hit(c, b) for c in _text_candidates(pv) for b in nums)


def _found_in(pv: Dict[str, Any], index: List[tuple], limit: int = 4) -> List[str]:
    from bisect import bisect_left, bisect_right
    ids: List[str] = []
    for c in _text_candidates(pv):
        win = c[1] + 1e-9
        lo = bisect_left(index, (c[0] - win,))
        hi = bisect_right(index, (c[0] + win, float("inf")))
        for v, tol, iid in index[lo:hi]:
            if _text_hit(c, (v, tol)) and iid not in ids:
                ids.append(iid)
    return ids[:limit]


def _check_value(ctx: Dict[str, Any], path: str, pv: Dict[str, Any]) -> tuple:
    """单条数字对账。返回 (severity|None, 说明, 建议, verified)。
    verified=True 表示数字在路径/被引条目里精确核到,可作跨节一致性的参照。"""
    sources = ctx["sources"]
    head = path.split(".")[0].split("[")[0]
    if head in SHOW_KEYS:
        blob = ctx["show_texts"].get(head, "")
        if blob and pv["digits"] not in blob:
            return ("medium", f"number not found in {head} text",
                    "quote the number exactly as the source states it", False)
        return (None, "", "", False)
    if head in _TEXT_POOLS:
        node, full = _resolve_path(sources, path)
        where = _found_in(pv, ctx["pool_index"].get(head, []))
        hint = f"; it appears in {', '.join(where)}" if where else ""
        if "[" in path and full:
            # P25(2026-11-05):台账路径带叶子字段(如 .text)时 node 是字符串,
            # _item_blob 只认条目 dict,曾返回空 blob 把真实命中的引用误报成
            # high(修订轮无法收敛)。字符串直接当原文用。
            blob = node if isinstance(node, str) else _item_blob(node)
            if _matches_text(pv, _text_numbers(blob)):
                return (None, "", "", True)
            return ("high", f"cited item does not state this number{hint}",
                    "cite the item that states the number, or fix the number", False)
        if where:
            if "[" in path:
                return ("medium", f"item selector does not resolve{hint}",
                        f"cite by item id, e.g. {head}.<bucket>[id={where[0]}]", False)
            return ("bare", "", "", False)
        if _matches(pv, ctx["snap_nums"]):
            return ("medium", f"number not in {head} text; exists elsewhere in snapshot",
                    "fix the ledger path", False)
        return ("high", f"absent from {head} text and snapshot",
                "use a number the cited item states, or remove it", False)
    node, full = _resolve_path(sources, path)
    if full:
        at_path = _subtree_numbers(node)
        if _matches(pv, at_path):
            return (None, "", "", True)
        # 路径精确指到了字段、值却对不上 —— 这是真错,不因"快照别处有同数"而放过
        shown = ", ".join(f"{x:g}" for x in sorted(at_path)[:4]) or "no number"
        return ("high", f"value at path: {shown}",
                "state the number at this path, or fix the path", False)
    if node is not sources and _matches(pv, _subtree_numbers(node)):
        return ("low", "path partly unresolved; number found under its prefix", "", False)
    if _matches(pv, ctx["snap_nums"]):
        return ("medium", "path does not resolve; number exists elsewhere in snapshot",
                "fix the ledger path", False)
    return ("high", "absent from snapshot",
            "use the snapshot value at this path, or remove the number", False)


def _check_ledger(ws: Path, name: str, ctx: Dict[str, Any], add,
                  collect: List[Dict[str, Any]]) -> None:
    """台账逐条对账 + 正文 %/bp 数字的覆盖兜底;带 entity/metric 的条目收进
    collect 供跨节一致性核验。"""
    sources, snap_nums, show_texts = ctx["sources"], ctx["snap_nums"], ctx["show_texts"]
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
    bare: List[str] = []
    for c in claims:
        if not isinstance(c, dict):
            continue
        pv = _parse_value(c.get("value"))
        if pv is None:
            continue
        ledger_vals.append(pv)
        path = str(c.get("path") or "")
        label = f"{c.get('value')} @ {path}"
        sev, detail, suggestion, verified = _check_value(ctx, path, pv)
        if sev == "bare":
            bare.append(str(c.get("value")))
        elif sev:
            add(name, label, detail, False, sev, suggestion or None)
        if c.get("entity") and c.get("metric"):
            collect.append({"section": name, "entity": c["entity"],
                            "metric": c["metric"], "value": c.get("value"),
                            "pv": pv, "verified": verified})
    if bare:
        add(name, f"{len(bare)} ledger entries cite a whole text pool instead of an "
            f"item: " + ", ".join(bare[:6]), "item id", False, "medium",
            "cite items by id, e.g. news.flash[id=f12] / news_macro.geo[id=g3]")
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


# ── 跨节一致性(台账 entity/metric)─────────────────────────────────────
# 行情类口径必须全篇一致(high,路由到偏离的节);财报类允许口径差异(medium)
_CONSIST_HIGH = {"move", "ah_move", "close", "level", "yield", "odds"}
_CONSIST_MEDIUM_PREFIX = ("revenue", "eps", "guide_")
_METRIC_ALIAS = {"pct_change": "move", "change_pct": "move", "pct_move": "move",
                 "change_percent": "move", "after_hours_move": "ah_move",
                 "ah_change": "ah_move", "price": "close", "odds_pct": "odds",
                 "hike_odds": "odds", "premarket_move": "pm_move",
                 "pre_market_move": "pm_move", "intraday_move": "pm_move"}


def _norm_metric(m: Any) -> str:
    m = re.sub(r"[\s\-]+", "_", str(m or "").strip().lower())
    return _METRIC_ALIAS.get(m, m)


def _metric_severity(m: str) -> Optional[str]:
    if m in _CONSIST_HIGH:
        return "high"
    if m.startswith(_CONSIST_MEDIUM_PREFIX):
        return "medium"
    return None


def _norm_entity(e: Any) -> str:
    e = str(e or "").strip().lower().lstrip("$")
    return re.sub(r"\s+", "", re.sub(r"\.us$", "", e))


def _norm_num(pv: Dict[str, Any]) -> float:
    v, unit = abs(pv["value"]), pv["unit"]
    if unit in _UNIT_SCALE:
        return v * _UNIT_SCALE[unit]
    return v / 100 if unit == "bp" else v


def _same_value(a: Dict[str, Any], b: Dict[str, Any]) -> bool:
    return _close(_norm_num(a), _norm_num(b), min(a["decimals"], b["decimals"]))


def _make_ctx(ws: Optional[Path], sources: Dict[str, Any]) -> Dict[str, Any]:
    snap_nums: Set[float] = set()
    _flatten_numbers(sources, snap_nums)
    return {"sources": sources, "snap_nums": snap_nums,
            "show_texts": _show_texts(ws, sources) if ws else {},
            "pool_index": _pool_index(sources)}


def verified_canonicals(rows: Any, sources: Dict[str, Any],
                        ctx: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """编辑计划里的规范数值,只留能在快照里按路径核到的(计划本身也是 LLM
    产物);同一 entity/metric 只留第一条。任务书注入与核验共用这份清单。"""
    ctx = ctx or _make_ctx(None, sources)
    out, seen = [], set()
    for r in rows or []:
        if not isinstance(r, dict):
            continue
        pv = _parse_value(r.get("value"))
        metric = _norm_metric(r.get("metric"))
        key = (_norm_entity(r.get("entity")), metric)
        if pv is None or not key[0] or not _metric_severity(metric) or key in seen:
            continue
        if _check_value(ctx, str(r.get("path") or ""), pv)[3]:
            seen.add(key)
            out.append(r)
    return out


def _plan_canonicals(ws: Path, ctx: Dict[str, Any]) -> Dict[tuple, Dict[str, Any]]:
    f = ws / "plan.json"
    if not f.exists():
        return {}
    try:
        rows = json.loads(f.read_text(encoding="utf-8")).get("canonical") or []
    except (json.JSONDecodeError, AttributeError):
        return {}
    return {(_norm_entity(r["entity"]), _norm_metric(r["metric"])):
            {"value": r.get("value"), "pv": _parse_value(r.get("value"))}
            for r in verified_canonicals(rows, ctx["sources"], ctx)}


# 指数/商品的常见写法 → cross_asset 代码前缀
_ENTITY_ALIAS = {"spx": "gspc", "s&p500": "gspc", "sp500": "gspc", "s&p": "gspc",
                 "djia": "dji", "dow": "dji", "dowjones": "dji", "nasdaq": "ixic",
                 "comp": "ixic", "nasdaq100": "ndx", "r2k": "rut", "russell2000": "rut",
                 "wti": "cl1", "brent": "bz1", "gold": "gc1", "silver": "si1",
                 "copper": "hg1"}
_UST_ENTITY_RE = re.compile(r"^(?:us)?(\d+)y(?:\.indx)?$")
_UST_CODE_RE = re.compile(r"^us\d+y\.indx$")


def _anchor_entity(e: str) -> str:
    """规范化后的 entity → 锚点键(美债统一成 2y/10y/30y,指数别名转代码前缀)。"""
    m = _UST_ENTITY_RE.match(e)
    if m:
        return f"{m.group(1)}y"
    return _ENTITY_ALIAS.get(e, e)


def _anchor(value: str, src: str, strict: bool = False) -> Optional[Dict[str, Any]]:
    pv = _parse_value(value)
    return {"value": value, "pv": pv, "src": src, "strict": strict} if pv else None


def data_anchors(sources: Dict[str, Any]) -> Dict[tuple, Dict[str, Any]]:
    """当日收盘口径的数据锚点:(锚点键, metric) → 参照值。

    move = 常规交易时段收盘对收盘;快讯里的盘前/盘中报价不是 move(Tesla
    盘前 5.6% 写成 move 时收盘实为 -3.34%)。美债只认财政部 par curve。
    """
    out: Dict[tuple, Dict[str, Any]] = {}
    mkt = sources.get("market") or {}
    for r in mkt.get("sp500_constituents") or []:
        t = _norm_entity(r.get("ticker"))
        if not t:
            continue
        src = f"market.sp500_constituents[ticker={r.get('ticker')}]"
        if isinstance(r.get("change_percent"), (int, float)):
            out[(t, "move")] = _anchor(f"{r['change_percent']}%", src + ".change_percent")
        if isinstance(r.get("price"), (int, float)):
            out[(t, "close")] = _anchor(str(r["price"]), src + ".price")
    for r in mkt.get("cross_asset") or []:
        code = _norm_entity(r.get("code"))
        if not code or _UST_CODE_RE.match(code):
            continue
        src = f"market.cross_asset[code={r.get('code')}]"
        for key in {code, code.split(".")[0]}:
            if isinstance(r.get("pct_change_1d"), (int, float)):
                out[(key, "move")] = _anchor(f"{r['pct_change_1d']}%",
                                             src + ".pct_change_1d")
            if isinstance(r.get("close"), (int, float)):
                for metric in ("close", "level"):
                    out[(key, metric)] = _anchor(str(r["close"]), src + ".close")
    curve = (sources.get("macro_pillars") or {}).get("par_curve") or {}
    for tenor, lv in (curve.get("levels_pct") or {}).items():
        if isinstance(lv, (int, float)):
            src = f"macro_pillars.par_curve.levels_pct.{tenor}"
            for metric in ("yield", "level", "close"):
                out[(tenor, metric)] = _anchor(f"{lv:.2f}%", src, strict=True)
        bp = (curve.get("change_bp") or {}).get(tenor)
        if isinstance(bp, (int, float)):
            out[(tenor, "move")] = _anchor(f"{bp:g}bp",
                                           f"macro_pillars.par_curve.change_bp.{tenor}",
                                           strict=True)
    return {k: v for k, v in out.items() if v}


def _matches_ref(pv: Dict[str, Any], ref: Dict[str, Any]) -> bool:
    if ref.get("strict"):
        dec = min(pv["decimals"], ref["pv"]["decimals"])
        return abs(_norm_num(pv) - _norm_num(ref["pv"])) <= (
            0.5 * 10 ** (-dec) / (100 if pv["unit"] == "bp" else 1)) + 1e-9
    return _same_value(pv, ref["pv"])


def _check_consistency(collect: List[Dict[str, Any]],
                       canon: Dict[tuple, Dict[str, Any]], add,
                       anchors: Optional[Dict[tuple, Dict[str, Any]]] = None) -> None:
    """同一实体同一指标跨节数值必须一致。参照优先级:当日收盘数据锚点 >
    编辑计划规范值 > 台账里已按路径核实的值 > 多数节;有锚点或规范值时单节
    也核;找不到唯一参照只记 medium,不路由。"""
    anchors = anchors or {}
    groups: Dict[tuple, List[Dict[str, Any]]] = {}
    for r in collect:
        metric = _norm_metric(r["metric"])
        if _metric_severity(metric):
            groups.setdefault((_norm_entity(r["entity"]), metric), []).append(r)
    order = {n: i for i, n in enumerate(SECTION_ORDER)}
    for key, rows in groups.items():
        sev = _metric_severity(key[1])
        label = f"{rows[0]['entity']} {key[1]}"
        ref, ref_src = None, ""
        anchor = anchors.get((_anchor_entity(key[0]), key[1]))
        if anchor:
            ref, ref_src = anchor, f"session data, {anchor['src']}"
        elif key in canon:
            ref, ref_src = canon[key], "editorial plan"
        else:
            if len({r["section"] for r in rows}) < 2:
                continue
            verified = [r for r in rows if r["verified"]]
            if verified and all(_same_value(v["pv"], verified[0]["pv"]) for v in verified):
                ref, ref_src = verified[0], f"{verified[0]['section']}, verified at path"
            elif not verified:
                clusters: List[List[Dict[str, Any]]] = []
                for r in rows:
                    for cl in clusters:
                        if _same_value(r["pv"], cl[0]["pv"]):
                            cl.append(r)
                            break
                    else:
                        clusters.append([r])
                if len(clusters) == 1:
                    continue
                size = sorted((len({r["section"] for r in cl}) for cl in clusters),
                              reverse=True)
                if size[0] > size[1]:
                    best = max(clusters, key=lambda cl: len({r["section"] for r in cl}))
                    ref, ref_src = best[0], "majority of sections"
        if ref is None:
            vals = sorted({(r["section"], str(r["value"])) for r in rows},
                          key=lambda x: order.get(x[0], 99))
            if len({v for _, v in vals}) > 1:
                add(vals[0][0], f"{label} differs across sections: "
                    + "; ".join(f"{v} ({s})" for s, v in vals), "cross-section",
                    False, "medium", "pick one source number and use it everywhere")
            continue
        flagged: Set[str] = set()
        for r in rows:
            if r["section"] in flagged or _matches_ref(r["pv"], ref):
                continue
            flagged.add(r["section"])
            fix = (f"use {ref['value']} — the same fact must read identically "
                   "across the brief")
            if anchor and key[1] == "move":
                fix = (f"use {ref['value']} (close-to-close), or if this is a "
                       "premarket/intraday quote say so (盘前/盘中) and tag the "
                       "ledger entry metric pm_move")
            add(r["section"], f"{label} {r['value']} disagrees with {ref['value']} "
                f"({ref_src})", "cross-section", False, sev, fix)


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


# 事实行里的观点/因果/渲染措辞(prompts 规则 12)。「预计」不列入:公司自己的
# 指引是当事方表态,属于事实。
_VIEW_WORDS = (r"认为|预期将|料将|或将|有望|看好|看空|看多|目标价|建议|担忧|"
               r"归因|源于|"
               r"\b(?:believes?|argues?|argued|price target|bullish|bearish)\b")
_CAUSAL_WORDS = (r"引发|导致|触发|拖累|提振|意味着|表明|凸显|标志着|受益|利好|利空|点燃|"
                 r"恐慌|罕见|史诗|暴跌|暴涨|重挫|狂飙|惨烈|崩塌|"
                 r"\b(?:sparks?|sparked|fears?|amid|signals?|suggests?|likely)\b")
NARRATIVE_VIEW_RE = re.compile(_VIEW_WORDS + "|" + _CAUSAL_WORDS, re.I)
FACT_LINE_VIEW_RE = re.compile(_VIEW_WORDS, re.I)
FACT_LINE_SECTIONS = ("drivers", "sector_action", "macro_review", "geopolitics")
VIEW_LINE_RE = re.compile(r"\*\*(?:观点|推断|Views|Read)\s*[:：]")


def _fact_line_views(name: str, prose: str) -> List[tuple]:
    """→ [(severity, [命中词…])]:今日要闻 整节 high;其余事实节只查事实行,medium。"""
    if name == "narrative":
        rx, sev = NARRATIVE_VIEW_RE, "high"
    elif name in FACT_LINE_SECTIONS:
        rx, sev = FACT_LINE_VIEW_RE, "medium"
    else:
        return []
    hits: List[str] = []
    for line in prose.splitlines():
        if not line.lstrip().startswith("- ") or VIEW_LINE_RE.search(line):
            continue
        hits += [m.group(0) for m in rx.finditer(line)]
    return [(sev, sorted(set(hits))[:8])] if hits else []


def run_checks(ws: Path, style: str = "en") -> List[Dict[str, Any]]:
    snapshot = _load_snapshot(ws)
    sources = snapshot.get("sources") or {}
    missing_mark = STYLE_PROFILES.get(style, STYLE_PROFILES["en"])["missing_mark"]
    issues: List[Dict[str, Any]] = []

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

    ctx = _make_ctx(ws, sources)
    show_texts = ctx["show_texts"]
    items = item_index(sources)
    state_secs = _state_sections(ws)
    collect: List[Dict[str, Any]] = []
    for name in SECTION_ORDER:
        if (SECTION_SPECS[name].get("mode") == "deterministic"
                or state_secs.get(name, {}).get("mode") == "deterministic"):
            continue
        _check_ledger(ws, name, ctx, add, collect)
    _check_consistency(collect, _plan_canonicals(ws, ctx), add,
                       data_anchors(ctx["sources"]))
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
            # 2026-11-06 实测:任务书脚手架被照抄成正文标题(## 第 4 节 — 板块动态),
            # medium 不门控直接带病发布 —— 升为 high,让修订轮当场改掉
            add(name, f"heading must be {heading}", text.splitlines()[0][:80],
                False, "high")
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
        for sev, words in _fact_line_views(name, prose):
            add(name, "view/causal wording in fact lines: " + ", ".join(words),
                "facts only", False, sev,
                "keep fact bullets to what happened; move named views to the "
                "block's **观点：** line, drop causal links and adjectives"
                if name != "narrative" else
                "今日要闻 states facts only: drop the view, forecast, causal "
                "link or adjective")
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
        # 条目 id 会渲染成读者可见的来源标记:不存在的 id 必须改掉
        for claim, sev in citation_issues(text, items):
            add(name, claim, "news/news_macro item ids", False, sev,
                "cite the exact item ids (e.g. [news flash f12 f40], "
                "[news_macro.geo g3]) whose text carries the bullet's facts")

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

    # 11 展望与策略:大事预判/策略应对恒在;免责行收尾
    # (小节不叫「投资建议」—— 那是投顾业务口径;免责行反而必须含该词)
    outlook_text = _section_text(ws, "outlook")
    if outlook_text.strip():
        for needed in ("### 大事预判", "### 策略应对"):
            if needed not in outlook_text:
                add("outlook", f"missing required block: {needed}", "required",
                    False, "high")
        if "不构成投资建议" not in outlook_text:
            add("outlook", "missing disclaimer line (不构成投资建议)", "required",
                False, "medium")

    # 今日涨跌榜(v8.6:美股/港股/A股成交额榜,编排器确定性渲染)——
    # 快照里有榜单的市场,正文必须出现对应小节与榜首代码
    vl = ((sources.get("market") or {}).get("volume_leaders")) or {}
    movers_text = _section_text(ws, "movers")
    for key, label in (("us", "美股"), ("hk", "港股"), ("cn", "A股")):
        rows = (vl.get(key) or {}).get("rows") or []
        if not rows:
            continue
        top_code = str(rows[0].get("code", ""))
        if f"### {label}" not in movers_text or (
                top_code and top_code not in movers_text):
            add("movers", f"volume leaders ({label}) present in data but "
                f"missing from 今日涨跌榜", f"market.volume_leaders.{key}",
                False, "high", "re-render 今日涨跌榜 from the volume leaders")

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
