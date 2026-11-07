#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""正文溯源:把各节 audit 注释反查成快照条目,渲染为要点末尾标记 + 节末来源列表。

写作 agent 只负责在 audit 注释里引用条目 id(f# 快讯 / w# 通讯社 / c# 电话会 /
d# 宏观日报 / r# 研报 / g# 地缘);L/E 等级、作者、时间全部由这里按快照查出,
不让模型在正文里写等级。分级定义见 INFO_GRADING_STANDARD.md。

    〔f78 f82 · L1/E2〕               快讯/素材条目(等级取自入库打标)
    〔f41 · L1/E2 · 传闻〕            条目带 reported 标记(匿名信源/据报道)
    〔Bloomberg The Close〕           节目转录 —— 无条目级等级
    〔跨资产收盘行情〕                 结构化数据 —— 无 L/E

一条要点引用多个条目时:L 取最高一级;E 不合成,逐级并列(E2,E3),
避免把单一信源的传闻和可靠报道洗成同一个等级。
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple

AUDIT_COMMENT_RE = re.compile(r"[ \t]*<!--\s*audit:(.*?)-->", re.S)
_BRACKET_RE = re.compile(r"\[([^\]]+)\]")
ITEM_ID_RE = re.compile(r"\b(?:id=)?([fwcdrg]\d+)\b")

NEWS_BUCKETS = (("news", ("flash", "wire", "calls")),
                ("news_macro", ("digest", "research", "geo")))

SHOW_LABELS = {
    "the_close": "Bloomberg The Close",
    "cnbc_closing_bell": "CNBC Closing Bell",
    "cnbc_closing_bell_ot": "CNBC Closing Bell Overtime",
}

# 最长前缀匹配;标签面向读者,不用管线内部名
DATA_LABELS = (
    ("market.cross_asset", "跨资产收盘行情"),
    ("market.sp500_constituents", "标普500成分股收盘"),
    ("market.sp500_sector_avg", "标普500板块涨跌"),
    ("market.top_movers", "异动股收盘"),
    ("market.breadth", "标普500涨跌家数"),
    ("market.volume_leaders", "成交额榜"),
    ("market", "收盘行情"),
    ("macro_pillars.par_curve", "美国财政部收益率曲线"),
    ("macro_pillars.next_fomc_odds", "联邦基金期货"),
    ("macro_pillars.fed_odds", "联邦基金期货"),
    ("macro_pillars.fred_ranks", "FRED 历史序列"),
    ("macro_pillars.fred_extras", "FRED 历史序列"),
    ("macro_pillars.fomc_calendar", "美联储议息日程"),
    ("macro_pillars.auction_events", "美债拍卖日程"),
    ("macro_pillars", "宏观数据"),
    ("prior_odds", "联邦基金期货"),
    ("fomc.decisions", "FOMC 决议"),
    ("fomc.statement", "FOMC 声明"),
    ("fomc.path", "联邦基金期货"),
    ("fomc", "FOMC 决议"),
    ("economic_events", "经济日历"),
    ("earnings_calendar", "财报日历"),
    ("macro_dashboard", "四经济体宏观数据"),
)

_L_RANK = {"L1": 1, "L2": 2, "L3": 3, "L4": 4}
INLINE_MAX_IDS = 3
LIST_TEXT_CHARS = 70


def item_index(sources: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """id → 条目(附 _bucket),覆盖新闻层与宏观素材层。"""
    idx: Dict[str, Dict[str, Any]] = {}
    for top, buckets in NEWS_BUCKETS:
        layer = sources.get(top) or {}
        for b in buckets:
            for it in layer.get(b) or []:
                if isinstance(it, dict) and it.get("id"):
                    idx[str(it["id"])] = {**it, "_bucket": f"{top}.{b}"}
    return idx


def _data_label(path: str) -> Optional[str]:
    p = path.split("[")[0]
    for prefix, label in DATA_LABELS:
        if p == prefix or p.startswith(prefix + "."):
            return label
    return None


def parse_audit(body: str) -> Dict[str, List[str]]:
    """audit 注释体 → {ids, shows, data, bare_news}。

    兼容三种写法:[news flash id=f1] / [news flash f1 f2] / [news_macro.geo g1];
    bare_news 记录没写条目 id 的整桶引用(如 [news flash])。
    """
    out: Dict[str, List[str]] = {"ids": [], "shows": [], "data": [], "bare_news": []}

    def _add(key: str, v: str) -> None:
        if v and v not in out[key]:
            out[key].append(v)

    for br in _BRACKET_RE.findall(body or ""):
        toks = br.split()
        if not toks:
            continue
        head = toks[0]
        if head.startswith("news"):
            ids = ITEM_ID_RE.findall(" ".join(toks[1:]))
            for i in ids:
                _add("ids", i)
            if not ids:
                _add("bare_news", br.strip())
        elif head == "show":
            for t in toks[1:]:
                _add("shows", t)
        else:
            paths = toks[1:] if head == "data" else toks
            for t in paths:
                label = _data_label(t)
                if label:
                    _add("data", label)
    return out


def _grade(it: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    lv = it.get("info_level")
    ev = it.get("evidence_level")
    return (lv if lv in _L_RANK else None,
            ev if isinstance(ev, str) and ev.startswith("E") else None)


def _is_reported(it: Dict[str, Any]) -> bool:
    return it.get("reported") is True or str(it.get("reported")).lower() == "true"


def grade_label(items: List[Dict[str, Any]]) -> str:
    """多条目的等级标签:L 取最高,E 并列;都没有则空串。"""
    ls = [g[0] for g in map(_grade, items) if g[0]]
    es = sorted({g[1] for g in map(_grade, items) if g[1]})
    lv = min(ls, key=_L_RANK.get) if ls else None
    ev = ",".join(es) if es else None
    if lv and ev:
        return f"{lv}/{ev}"
    return lv or ev or ""


def inline_tag(refs: Dict[str, List[str]], idx: Dict[str, Dict[str, Any]]) -> str:
    parts: List[str] = []
    ids = [i for i in refs["ids"] if i in idx]
    if ids:
        shown = " ".join(ids[:INLINE_MAX_IDS])
        if len(ids) > INLINE_MAX_IDS:
            shown += f" 等{len(ids)}条"
        items = [idx[i] for i in ids]
        seg = [shown]
        g = grade_label(items)
        if g:
            seg.append(g)
        if any(_is_reported(it) for it in items):
            seg.append("传闻")
        parts.append(" · ".join(seg))
    parts += [SHOW_LABELS.get(s, s) for s in refs["shows"]]
    parts += refs["data"]
    return f"〔{'｜'.join(parts)}〕" if parts else ""


def _clip(text: Any, n: int = LIST_TEXT_CHARS) -> str:
    t = re.sub(r"https?://\S+", "", str(text or ""))
    t = re.sub(r"[*_`#>\[\]|]", "", t)
    t = re.sub(r"\s+", " ", t).strip()
    t = re.sub(r"^\d{1,2}:\d{2}\s+", "", t)
    return t if len(t) <= n else t[:n].rstrip() + "…"


def _when(it: Dict[str, Any], report_date: str) -> str:
    if it.get("time_et"):
        day = str(it.get("date_et") or report_date)[5:10]
        return f"{day} {it['time_et']} ET".strip()
    if it.get("time_cn"):
        return f"北京 {it['time_cn']}"
    return ""


def source_line(iid: str, it: Dict[str, Any], report_date: str) -> str:
    head = [f"**{iid}**"]
    g = grade_label([it])
    if g:
        head.append(g)
    if _is_reported(it):
        head.append("传闻")
    meta = [m for m in (str(it.get("author") or "").lstrip("*").strip(),
                        _when(it, report_date)) if m]
    title = str(it.get("title") or "").strip()
    # 星球等源的标题是正文前若干字的截断("13:01 俄罗斯称对乌克兰两...")
    if not title or title.endswith(("...", "…")):
        title = it.get("text") or title
    body = _clip(title)
    line = "- " + " · ".join(head + meta)
    return f"{line}:{body}" if body else line


def _attach(lines: List[str]) -> List[Tuple[int, str]]:
    """每条 audit 注释 → 它所属的正文行号(同行注释挂本行;独占一行挂上一条正文行)。"""
    out: List[Tuple[int, str]] = []
    last = -1
    for i, line in enumerate(lines):
        found = AUDIT_COMMENT_RE.findall(line)
        rest = AUDIT_COMMENT_RE.sub("", line).strip()
        if rest and not rest.startswith("<!--"):
            last = i
        for body in found:
            out.append((i if rest else last, body))
    return out


def render_section(text: str, sources: Dict[str, Any], report_date: str,
                   idx: Optional[Dict[str, Dict[str, Any]]] = None
                   ) -> Tuple[str, List[str]]:
    """返回 (带要点标记、已剥注释的正文, 节末来源列表行)。"""
    idx = item_index(sources) if idx is None else idx
    lines = text.split("\n")
    per_line: Dict[int, Dict[str, List[str]]] = {}
    order_ids: List[str] = []
    shows: List[str] = []
    data: List[str] = []
    for ln, body in _attach(lines):
        refs = parse_audit(body)
        merged = per_line.setdefault(ln, {"ids": [], "shows": [], "data": [],
                                          "bare_news": []})
        for k, vs in refs.items():
            for v in vs:
                if v not in merged[k]:
                    merged[k].append(v)
        order_ids += [i for i in refs["ids"] if i in idx and i not in order_ids]
        shows += [s for s in refs["shows"] if s not in shows]
        data += [d for d in refs["data"] if d not in data]

    out_lines: List[str] = []
    for i, line in enumerate(lines):
        clean = AUDIT_COMMENT_RE.sub("", line).rstrip()
        tag = inline_tag(per_line[i], idx) if i in per_line and i >= 0 else ""
        if tag and clean.strip():
            clean = f"{clean} {tag}"
        out_lines.append(clean)

    src: List[str] = []
    if order_ids or shows or data:
        src = ["**本节来源**", ""]
        src += [source_line(i, idx[i], report_date) for i in order_ids]
        if shows:
            src.append("- 节目:" + "、".join(SHOW_LABELS.get(s, s) for s in shows))
        if data:
            src.append("- 数据:" + "、".join(data))
    return "\n".join(out_lines), src


def citation_issues(text: str, idx: Dict[str, Dict[str, Any]]) -> List[Tuple[str, str]]:
    """引用核验:(说明, severity)。快照里不存在的条目 id → high;整桶引用 → medium。"""
    unknown: List[str] = []
    bare: List[str] = []
    for body in AUDIT_COMMENT_RE.findall(text):
        refs = parse_audit(body)
        unknown += [i for i in refs["ids"] if i not in idx and i not in unknown]
        bare += [b for b in refs["bare_news"] if b not in bare]
    out: List[Tuple[str, str]] = []
    if unknown:
        out.append(("audit cites item ids not in snapshot: " + ", ".join(unknown[:10]),
                    "high"))
    if bare:
        out.append(("audit cites a whole news bucket without item ids: "
                    + "; ".join(bare[:5]), "medium"))
    return out
