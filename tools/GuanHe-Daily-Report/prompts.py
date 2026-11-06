# -*- coding: utf-8 -*-
"""章节任务书与核验任务书(xiao 式晨报,v8 Macros)。

v8 对齐 xiao 式晨报参考稿(设计出处;参考文件已删,风格固化于各节任务书),
Macros 推倒重来为「回顾/当下/展望」五文件:
  07 宏观速览   确定性渲染(编排器,零 LLM):四经济体指标表 + 商品行情 + 前瞻日历
  08 回顾·宏观数据  边际变化:多国数据意外(美欧中日),券商宏观解读挂名
  09 回顾·地缘政治 + 焦点事件  主题块按快照命中量开块(美伊/俄乌/中东/贸易)
  10 美联储观察 利率与决议(投票/异议)+ 声明措辞对比 + 市场定价路径 + 官员表态
  11 展望与策略 第二段综合:大事预判 / 主线跟踪 / 策略应对(直接给操作口径)
v8 新信源: macro_dashboard(指标表底座)/ fomc(决议·定价·声明)/
news_macro(宏观地缘素材层:digest 晨间日报 · research 宏观解读 · geo 地缘快讯;
源池 = Admin user_id=1 的订阅,中英混合)。
新 Macros 完全不读节目转录;节目素材只属于 01-06。

写作按信源分组两段并行:第一段 overall_market(narrative/drivers/positioning)、
company_desk(sector_action/deep_dives)、movers 与 macro_dashboard(确定性)、
macro_review、geopolitics、fomc 六组;第二段 outlook 读第一段成品做综合。
"""

import re
from typing import Any, Dict, List, Optional

# ── 风格档案 ───────────────────────────────────────────────────────────

STYLE_PROFILES: Dict[str, Dict[str, Any]] = {
    "en": {
        "body_language": "English (Simplified style of a professional financial newsletter)",
        "missing_mark": "Nothing notable today.",
        "quality_review_language": "Write verdict.json summary/issues in English.",
    },
    "cn": {
        "body_language": "简体中文 (keep tickers, company/person names "
                         "and numbers in original form)",
        "missing_mark": "今日无重点。",
        "headline_rule": "HEADLINE_CANDIDATES must be written in Simplified "
                         "Chinese (两句式中文标题; tickers/names may stay in "
                         "original form).",
        "quality_review_language": "Review the Chinese text; write verdict.json "
                                   "summary/issues in Chinese (JSON keys and enum "
                                   "values stay English).",
    },
}

# 内部信源标签 → snapshot 路径。v7 起标签不再出现在正文(正文只给原始出处),
# 仅用于章节信源白名单与核验时按 audit 注释做来源审计。
ATTRIBUTION_MAP = {
    "(The Close)": "the_close.clean_transcript",
    "(Closing Bell)": "cnbc_closing_bell.clean_transcript / "
                      "cnbc_closing_bell.clean_transcript_ot",
    "(cross-asset)": "market.cross_asset",
    "(sector averages)": "market.sp500_sector_avg",
    "(breadth)": "market.breadth",
    "(Finviz)": "market.sp500_constituents / market.top_movers",
    "(earnings calendar)": "earnings_calendar",
    "(economic calendar)": "economic_events",
    "(Treasury curve)": "macro_pillars.par_curve",
    "(FRED)": "macro_pillars.fred_ranks / macro_pillars.fred_extras / "
              "macro_pillars.release_series",
    "(fed funds futures)": "macro_pillars.fed_odds / macro_pillars.next_fomc_odds "
                           "/ prior_odds",
    "(Fed calendar)": "macro_pillars.fomc_calendar",
    "(auction calendar)": "macro_pillars.auction_events",
    "(nowcasts)": "macro_pillars.nowcasts",
    "(news flash)": "news.flash",
    "(newswire)": "news.wire / news.calls",
    # v8 Macros 新信源(内部标签,同其余标签一样禁入正文)
    "(macro dashboard)": "macro_dashboard",
    "(FOMC decisions)": "fomc.decisions / fomc.statement / fomc.path",
    "(macro daily)": "news_macro.digest / news_macro.research",
    "(macro geo)": "news_macro.geo",
}

# 文本素材条目的稳定 id:台账按条目引用(news.flash[id=f12]),核验只在该条目
# 内找数字 —— 整池子串匹配下改错的数字有 2/3 照样能"通过"
ITEM_ID_PREFIX = {
    ("news", "flash"): "f", ("news", "wire"): "w", ("news", "calls"): "c",
    ("news_macro", "digest"): "d", ("news_macro", "research"): "r",
    ("news_macro", "geo"): "g",
}


def assign_item_ids(sources: Dict[str, Any]) -> int:
    """给文本素材条目编号(已有 id 的不动,幂等)。返回新编号条数。"""
    added = 0
    for (head, sub), prefix in ITEM_ID_PREFIX.items():
        items = (sources.get(head) or {}).get(sub)
        if not isinstance(items, list):
            continue
        for i, item in enumerate(items):
            if isinstance(item, dict) and not item.get("id"):
                items[i] = {"id": f"{prefix}{i + 1}", **item}
                added += 1
    return added


# 日历数值的单位(EODHD 只给裸数:"预期 65" 是 6.5 万还是 65%?);先按口径再按事件名
_UNIT_RULES = (
    (re.compile(r"payroll|jobless claims|claimant|employment change|\badp\b", re.I), "K"),
    (re.compile(r"job openings|jolts|crude oil .*change|gasoline .*change", re.I), "M"),
    (re.compile(r"balance of trade|current account|exports|imports|budget", re.I), "B"),
    (re.compile(r"rate|yield|auction|gdp growth|earnings", re.I), "%"),
)
_INDEX_LEVEL_RE = re.compile(r"inflation|cpi|pce|ppi|price index", re.I)


def event_unit(row: Dict[str, Any]) -> str:
    if (row.get("comparison") or "").lower() in ("yoy", "mom", "qoq"):
        return "%"
    ev = row.get("event") or ""
    if re.search(r"foreign exchange reserves|fx reserves", ev, re.I):
        try:
            return "T" if abs(float(row.get("previous") or row.get("forecast"))) < 100 else "B"
        except (TypeError, ValueError):
            return ""
    if re.search(r"pmi|sentiment|confidence|index|rig count|\bism\b", ev, re.I):
        return ""
    if _INDEX_LEVEL_RE.search(ev):
        # 无口径的通胀行是指数水平(CPI s.a 336.94),小数值才是百分比
        try:
            return "%" if abs(float(row.get("previous") or row.get("forecast"))) < 30 else ""
        except (TypeError, ValueError):
            return ""
    for rx, unit in _UNIT_RULES:
        if rx.search(ev):
            return unit
    return ""


def annotate_event_units(sources: Dict[str, Any]) -> int:
    """给 economic_events 各行补 unit 字段(已有的不动,幂等)。返回新标注行数。"""
    econ = sources.get("economic_events") or {}
    n = 0
    for bucket in ("recent", "upcoming"):
        for row in econ.get(bucket) or []:
            if isinstance(row, dict) and "unit" not in row:
                row["unit"] = event_unit(row)
                n += 1
    return n


# 匿名信源/传闻措辞:上游 evidence_level 常把这类条目标成 E2(2026-11-04 的
# "US Diplomatic Sources: Trump May Decide…" 即是),按措辞另打 reported 标记
_REPORTED_RE = re.compile(
    r"\bsources?\b(?!\s+of\b)|reportedly|people (?:familiar|close|briefed)|"
    r"according to (?:a |the )?(?:report|people|person|officials? who)|"
    r"\bsaid to be\b|\bmulls?\b|rumou?rs?|unconfirmed|"
    r"据报道|据悉|知情人士|消息人士|消息称|传闻|传言|未经证实|据传", re.I)
_REPORTED_POOLS = (("news", "flash"), ("news", "wire"), ("news_macro", "geo"),
                   ("news_macro", "digest"), ("news_macro", "research"))


def flag_reported_items(sources: Dict[str, Any]) -> int:
    """给文本条目加 reported 布尔标记(已有该键的不动,幂等)。返回新标注条数。"""
    n = 0
    for head, sub in _REPORTED_POOLS:
        for item in (sources.get(head) or {}).get(sub) or []:
            if not isinstance(item, dict) or "reported" in item:
                continue
            blob = " ".join(str(item.get(k) or "") for k in ("title", "text"))
            item["reported"] = bool(_REPORTED_RE.search(blob))
            n += 1
    return n


def fmt_event_value(v: Any, unit: str) -> str:
    if v in (None, ""):
        return "–"
    return f"{v}{unit}" if unit else str(v)


# 正文禁用的管线标签(读者看到的出处只能是原始发布方/机构)
PIPELINE_TAGS = tuple(k for k in ATTRIBUTION_MAP if k != "(FRED)" and k != "(Finviz)")

# 章节 × 信源矩阵(v8: 节目转录为 01-06 素材; Macros 五节用
# macro_dashboard/fomc/news_macro + 结构化层,完全不读节目转录)
SECTION_SOURCE_MATRIX = {
    "narrative": {"(The Close)", "(cross-asset)", "(breadth)",
                  "(news flash)"},
    "drivers": {"(The Close)", "(cross-asset)", "(Treasury curve)",
                "(fed funds futures)", "(FRED)", "(breadth)",
                "(news flash)", "(newswire)"},
    "positioning": {"(The Close)", "(cross-asset)", "(breadth)",
                    "(Treasury curve)", "(fed funds futures)", "(Finviz)",
                    "(news flash)", "(newswire)"},
    "sector_action": {"(Closing Bell)", "(sector averages)", "(Finviz)",
                      "(breadth)", "(earnings calendar)",
                      "(news flash)"},
    "movers": {"(Finviz)", "(earnings calendar)"},
    # v8.3 第二部分瘦身(2026-11-04,claims 实测:The Close/wire/calls 四节两天
    # 仅贡献 ~8% 事实,却占 ~56% 文本素材):04/06 文本源收窄为 CB 双转录 +
    # flash;结构化数字层(finviz/日历/跨资产)不动
    "deep_dives": {"(Closing Bell)", "(Finviz)",
                   "(earnings calendar)", "(news flash)"},
    # ── v8 Macros(08-11 为 LLM 节;07 确定性渲染,不写 audit 注释) ──
    "macro_dashboard": {"(macro dashboard)", "(economic calendar)",
                        "(cross-asset)", "(Fed calendar)"},
    "macro_review": {"(economic calendar)", "(FRED)", "(cross-asset)",
                     "(Treasury curve)", "(fed funds futures)", "(macro daily)",
                     "(news flash)", "(newswire)"},
    "geopolitics": {"(macro geo)", "(macro daily)", "(news flash)", "(newswire)",
                    "(cross-asset)"},
    "fomc": {"(FOMC decisions)", "(fed funds futures)", "(economic calendar)",
             "(Treasury curve)", "(FRED)", "(news flash)", "(newswire)"},
    "outlook": {"(fed funds futures)", "(FOMC decisions)",
                "(economic calendar)", "(cross-asset)", "(Treasury curve)", "(macro daily)",
                "(macro geo)", "(news flash)", "(newswire)"},
}


def _style_rules(style: str) -> str:
    p = STYLE_PROFILES[style]
    headline = f"\n   {p['headline_rule']}" if p.get("headline_rule") else ""
    voice = ("\n   中文正文同样不得出现:快照、转录、节目、主持人、不可得、口径、数据层。"
             if style == "cn" else "")
    return f"""\
4. Output language: {p['body_language']}. Keep section headings exactly as each
   task specifies (house style: 全部章节标题为中文,以各任务书给定的标题为准).
   Keep tickers ($MU, CCI), company/person names and all numbers in their
   original form.{headline}{voice}
"""


def allowed_source_paths(allowed_tags: Optional[set]) -> list:
    """归因白名单(内部标签) → 允许读取的 snapshot 路径。"""
    paths: list = []
    for tag in sorted(allowed_tags or ()):
        for p in ATTRIBUTION_MAP.get(tag, "").split(" / "):
            p = p.strip()
            if p and p not in paths:
                paths.append(p)
    return paths


def agent_snapshot_filter(agent: str) -> Dict[str, set]:
    """agent → 允许的快照源块(head → 子键集合;空集 = 整块保留)。

    v8.3:SOURCE RESTRICTION 从"仅 prompt 约定"升级为"物理切片"——
    run_pipeline 按此过滤 sources 后写 snapshot.{agent}.json 挂给该
    agent,白名单外的素材(如第二部分的 The Close / wire / calls)不再
    进入其上下文。核验器(verifier)始终用全量快照,不受此影响。
    """
    out: Dict[str, set] = {}
    for n in agent_section_names(agent):
        for p in allowed_source_paths(SECTION_SOURCE_MATRIX.get(n, set())):
            parts = p.split(".")
            head = parts[0]
            if len(parts) > 1:
                out.setdefault(head, set()).add(parts[1])
            else:
                out.setdefault(head, set())
    return out


def _common_rules(style: str, allowed_tags: Optional[set] = None) -> str:
    p = STYLE_PROFILES[style]
    src_line = ""
    if allowed_tags is not None:
        paths = ", ".join(allowed_source_paths(allowed_tags)) or "(none)"
        src_line = f"""
   SOURCE RESTRICTION for THIS section: read facts and numbers ONLY from these
   snapshot paths: {paths}. If they lack a fact you want, leave it out — do
   not borrow from any other part of the snapshot.
"""
    return f"""\
You are one section-writer inside a scheduled report pipeline. You are writing ONE
section of a "GuanHe 每日晨报" for professional investors. Write like a senior
markets analyst briefing a trading desk: every sentence states a market fact or a
judgment, numbers are clean and few, and the reader never sees how the brief was
produced. Model the voice on this reference excerpt (same house):

  **Intel's worst day in 50 years exposed the memory squeeze.**
  - Intel ($INTC) fell 28.6%, its worst day in more than 50 years, after guiding
    far below forecasts because memory costs have priced buyers out of PCs (CNBC).
  - Sandisk ($SNDK) fell too after partner Kioxia signaled the NAND price surge
    may be ending, and the chip index closed below its 200-day average.
  - Avoid PC, phone and chip suppliers exposed to memory costs until Apple
    (Monday) and Qualcomm (Wednesday) show how far the squeeze reaches.

HARD RULES (violating any of these fails pipeline verification):
1. The ONLY data source is the attached snapshot.json (a frozen data snapshot).
   Do NOT invent numbers. Do NOT fetch anything from the internet.
   SHOW CONTENT: the shows' cleaned transcripts are the content of record —
   sources.the_close.clean_transcript (">>" speaker turns; Bloomberg's The Close)
   and sources.cnbc_closing_bell.clean_transcript + clean_transcript_ot
   ([mm:ss] paragraphs; CNBC Closing Bell MAIN hour and its OVERTIME hour,
   which covers the after-close earnings block). There are no show digests.
   Quote guests with the speaker attribution spoken on air (name + title/firm
   when stated; do not guess names or affiliations). If a transcript field is
   absent from the snapshot, that show's material is unavailable — do not
   paraphrase from memory.
   NEWS LAYER: sources.news.flash[] are same-day squawk/headline tweets
   ({{id, time_et, author, text, tickers, info_level, evidence_level, reported}}) and
   sources.news.wire[] are full-text articles, both drawn from Admin's
   subscribed sources and graded L1–L4 (importance) / E1–E3 (evidence);
   sources.news.calls[] are
   earnings-call insight excerpts carrying VERBATIM management quotes.
   When the same number exists in the structured data layer (market,
   earnings_calendar, economic_events, macro_pillars), use that value
   instead of the show/news text.
   MACROS LAYER (Sections 07–11 only; those sections never read show
   transcripts): sources.macro_dashboard.countries is a curated per-economy
   table of the latest PUBLISHED values (policy_rate / growth / inflation /
   pmi / labor, each row {{event, comparison, period, date, actual,
   forecast, previous}}); sources.fomc carries decisions[] (rate range bp,
   vote split, dissenters with their stated reasons), path.meetings[]
   (futures pricing for future meetings) and statement / statement_prev
   (full statement texts for language comparison); sources.news_macro has
   digest[] (human-curated daily briefs), research[] (macro interpretation
   — broker notes name the ORIGINATING broker in the body; English macro
   voices included) and geo[] (geopolitical items in Chinese OR English),
   each item {{id, time_cn (Beijing), author, title, text, theme, importance,
   evidence_level, reported}} (theme/importance come from semantic triage). The material is
   bilingual: quote/paraphrase Chinese items in Chinese prose, English
   items in Chinese prose with the original English term kept where it is
   the house term (FOMC, CPI, Hormuz).
   EVIDENCE LEVELS: news and macro material items may carry evidence_level.
   E3 = single-source / unverified (KOL views, rumors): any fact drawn from
   an E3 item MUST read as unverified (「据…报道」「未经证实」style) and may
   never be stated as established fact. E1 = primary official disclosure;
   E2 = reliable professional coverage — state directly with the outlet
   credited.
   REPORTED CLAIMS: an item flagged `reported: true` (anonymous sources,
   "reportedly", 消息人士, 传闻 — whatever its evidence_level) is a claim,
   not a fact: write it as 「据…报道」「…消息人士称」, never as established
   fact. A headline, an inference (推断), a base case or advice must rest on
   an observed market move or a confirmed event; a reported claim may only
   appear as the risk / flip condition ("若…属实/落地").
2. Every number you state must exist in snapshot.json. If absent, leave the
   number (or the sentence) out.
3. Write the section to the exact output file specified below, as Markdown,
   plus its claims ledger (rule 11). Do not create any other files.
{_style_rules(style)}
5. MISSING DATA: if a fact or number is absent from your permitted sources,
   simply do not write it — NEVER substitute a proxy metric, a stale value, a
   similar-but-different instrument, or a number from outside your SOURCE
   RESTRICTION. Never mention the absence in the prose. Only if the WHOLE
   section has no material, write the heading plus the single line
   {p['missing_mark']} and stop.{src_line}
6. VOICE — the reader must never see the production process. In the prose
   (outside audit comments) NEVER write: "snapshot", "transcript", "data
   layer", "not covered", "degraded", "disclosure", "the desk", "on air", "the
   show(s)", "as relayed", "anchor", or the show names "The Close" / "Closing
   Bell" / "Overtime". State facts directly as market facts. Hosts and
   anchors are not sources: turn what they report into plain statements;
   credit only guests/analysts/executives, by name and firm.
7. READER ATTRIBUTION (sparing, like a newspaper):
   - At most ONE parenthetical credit per bullet, at its end, naming the
     ORIGINATING outlet or institution: (Reuters), (FT), (Bloomberg), (CNBC),
     (WSJ), (Yahoo Finance), (Seeking Alpha), (BofA), (Morgan Stanley), etc.
   - Credit only facts a reader would want sourced: scoops, reports, survey/
     flow data, analyst calls. NO credit for prices, % moves, yields, index
     levels, economic-calendar prints, futures-implied odds, or reported EPS.
   - Material from Bloomberg's The Close → (Bloomberg) when a credit is
     needed; from CNBC Closing Bell → (CNBC). News flash items: credit the
     outlet the item itself names (e.g. "...- FT", "TO RTRS" → (Reuters),
     "per Bloomberg"); the posting account is a relay, NOT the source —
     "*Walter Bloomberg" is not Bloomberg, LiveSquawk is not an outlet. If the
     item names no outlet, give no credit.
   - Never use pipeline labels as credits: (The Close), (Closing Bell),
     (cross-asset), (sector averages), (breadth), (earnings calendar),
     (economic calendar), (Treasury curve), (fed funds futures), (Fed
     calendar), (auction calendar), (nowcasts), (news flash), (newswire),
     (macro dashboard), (FOMC decisions), (macro daily), (macro geo).
   - Judgment is welcome in the analyst's voice (the action bullet of a
     block, a "so what" clause), but never dress an inference up as a
     reported fact.
8. AUDIT TRAIL (hidden layer, mandatory — this is where traceability lives):
   append an HTML comment at the END OF THE SAME LINE as each factual bullet
   carrying the precise snapshot key(s), e.g.
   `<!-- audit: [show the_close] -->`,
   `<!-- audit: [data market.cross_asset] [news flash f12 f40] -->`,
   `<!-- audit: [news_macro.geo g3] [news wire w2] -->`.
   News and macro material are cited BY ITEM ID — never a bare bucket like
   [news flash]; cite the items whose text actually carries the bullet's
   facts, not every related item. The publisher turns these ids into the
   reader-visible source tag (outlet, time, importance/evidence grade), so
   an id that does not exist fails the section. Never write grades (L1/E2)
   or item ids in the prose yourself.
9. NUMBER STYLE:
   - Price/index/stock changes: one decimal ("fell 28.6%", "rose 2.2%"). After
     a direction verb (fell/rose/gained/lost/slid) write NO sign — never
     "fell -1.8%". Use a sign only in tables or verb-less lists.
   - Yield changes in bp ("the 2-year yield fell 5bp"); timestamps in ET.
   - Money: $25bn, $1.5tn, $300mn, $6.3bn — never "25 billion dollars".
   - Give a reference frame only when it adds information: results/guidance
     vs estimates ("$0.25 against $0.43 expected"), a record or multi-year
     extreme, a level crossed. Do not attach est/prior to every number.
   - ONE number per fact. If the data close and a spoken figure differ, use
     the data close and drop the spoken one — never "closed -10.6%, described
     as off nearly 13%".
   - The same fact must read identically everywhere in the brief (e.g. a
     "worst day since ..." framing, a market-cap loss).
   - "highest/lowest since <date>" / "first time since" / "worst day in N
     years" claims are ONLY allowed when the exact framing appears in
     macro_pillars.fred_ranks.*.claim or verbatim in a show/news item — never
     compute one yourself.
   - Fed odds: "hike odds at the <next meeting>" = macro_pillars.next_fomc_odds
     (computed from the post-meeting month contract). fed_odds.contracts[] are
     MONTHLY AVERAGE contracts — never call a contract month's number "<month>
     meeting odds"; phrase it as "the <month> contract prices ~Nbp".
   - No vague quantifiers ("sharply", "significantly") without a number.
   - Quotes: verbatim from a show transcript or news item, with the speaker's
     name and firm; never construct a quote. Keep quotes short (one clause).
10. End the file with a line containing only: <!-- SECTION DONE -->
11. CLAIMS LEDGER (machine-checked; this replaces a separate verification pass):
   next to each section file write `<same name>.claims.json` (e.g.
   sections/02_drivers.md → sections/02_drivers.claims.json) — a JSON array with
   ONE entry per number you stated in that file:
   {{"value": "62%", "path": "<snapshot path>", "text": "<≤12-word fragment>",
    "entity": "<optional>", "metric": "<optional>"}}
   - `path` is a dot path into snapshot.json `sources` (omit the `sources.`
     prefix). Select list items with [field=value], e.g.
     macro_pillars.fed_odds.contracts[month=2026-12].odds_of_25bp_hike_pct,
     market.cross_asset[code=GSPC.INDX].pct_change_1d,
     market.sp500_constituents[ticker=INTC].change_percent,
     economic_events.recent[event=Fed Interest Rate Decision].actual,
     macro_pillars.par_curve.change_bp.10y
   - Numbers from a show: path = the_close.clean_transcript,
     cnbc_closing_bell.clean_transcript or cnbc_closing_bell.clean_transcript_ot.
     Numbers from a news item: cite THAT item by its id, e.g.
     news.flash[id=f12], news.wire[id=w3], news.calls[id=c2],
     news_macro.digest[id=d1], news_macro.geo[id=g7]. The number must appear
     in that one item ("$25bn" is fine when it says "25 billion"; "752万" is
     fine when it says "7.52M"); a number the item does not state fails the
     section even if another item has it. Never cite a whole bucket
     (news.flash) — the checker cannot tie it to a story.
   - `entity` + `metric`: fill both for every market move, level, yield,
     odds or reported figure, so the brief can be checked for one number
     per fact across sections. entity = ticker for stocks/ETFs (NVDA),
     cross_asset code for indices/assets (GSPC.INDX), US2Y/US10Y for
     Treasury yields, the meeting month for Fed odds (2026-12). metric = one
     of move (THIS session's regular-session % change, close vs prior close),
     ah_move (after-hours % change), pm_move (premarket or intraday % change —
     a news.flash quote timestamped before the close is pm_move, never move),
     close, level, yield, odds, revenue, eps, guide_revenue, guide_eps.
     The checker compares every move against the session's close-to-close
     data; a multi-day or year-to-date change gets no metric. Skip both for
     anything else.
   - US Treasury yields come ONLY from macro_pillars.par_curve (levels_pct /
     change_bp, the official Treasury curve), written with two decimals
     (4.90%); the curve is the single yield source for the whole brief.
   - `value` exactly as written in the prose. Rounding a data value to one
     decimal is fine (the checker tolerates it).
   A script checks every entry against the snapshot; an entry whose number is
   not at its path fails the section. Write the ledger AFTER the prose so it
   matches the final text; update it whenever you revise.
"""


# ── 章节任务书 ─────────────────────────────────────────────────────────

_POSITIONING_BODY = """\
Write "## 持仓与资金流" with up to three items, in this order; drop an
item entirely when there is no real material for it. Material: the The Close
guest-strategist segments (allocators, fixed-income/credit views, technicians,
disagreements, time horizons) and same-day flow/positioning items in
news.flash / news.wire (prime-brokerage exposure, short interest, fund flows,
sell-side levels). Structured numbers (breadth, curve, odds, Finviz closes)
may back a point.

Item format — a bold label + bold one-clause headline, ONE sentence of
evidence, then 1–2 sub-bullets each with a bold short lead-in:

- **Primary — Funds cut risk at a record pace.** Hedge-fund gross exposure is
  7.8% below its peak, the biggest cut since 2016 (Morgan Stanley).
  - **The shorts sit in the Nasdaq 100.** <one fact>.
  - **Dip buyers kept adding.** <one fact>.

- **Primary** — the dominant positioning theme (rates, equity beta, duration,
  rotation, de-grossing).
- **Secondary** — a second flow thread (credit, gold, FX, sector rotation,
  buybacks, ETF flows, a sell-side level that broke or held — give the level
  and the close).
- **Guest positioning** — the one or two most decision-relevant strategist
  calls: "Firm (Name): the call" with its level/target, one sentence each.
Each item ≤ 70 words. Do not repeat a driver block's analysis here.
"""

OVERALL_MARKET_TASK = """\
**任务一(一次分析,三个文件)—— 今日叙事 + 核心驱动 + 持仓与资金流**

Read The Close transcript and the trading-day news ONCE, decide the day's 3–5
real stories, then export THREE files. They complement, never repeat: the
narrative states the thesis, the drivers prove it, positioning shows how money
is reacting.

### File A — sections/01_narrative.md  (THESIS layer)

Write "## 今日叙事" with EXACTLY 2–3 bullets. Each bullet is ONE plain
sentence (no bold, no credits, no numbers) stating a conclusion about the day —
what moved and why it matters — e.g.:
- The AI selloff spread from spenders to suppliers and lenders: memory costs
  are crushing PC demand, and the build-out increasingly runs on debt.
- Money rotated rather than fled: defensives rose and fewer stocks fell than in
  past routs, but funds are cutting risk without a washout.
Still add an audit comment per bullet.

THEN append the headline candidates comment block (the report's editorial
title — two short clauses naming the day's two biggest stories, e.g.
"Intel's worst day in 50 years deepens the tech rout. Defensive stocks hold
firm."):

<!-- HEADLINE_CANDIDATES
"First clause. Second clause."
"Alternate headline clause. Other clause."
-->

2-3 candidates, single quoted lines, no credits inside. The FIRST candidate is
the published headline. Any superlative in it must obey the "since" rule.

### File B — sections/02_drivers.md  (EVIDENCE layer)

Write "## 核心驱动" with 3–5 blocks, ordered by market impact. A
block may come from The Close (desk segments, guest interviews, data reads) or
from market-moving trading-day news in news.flash / news.wire that the show
missed (M&A, geopolitics, central-bank or government headlines, credit
events). Each block:
- A bold one-clause headline that states the conclusion
  (e.g. **The build-out is moving onto borrowed money.**).
- 1–3 fact bullets. Pack related facts into one sentence; each bullet ≤ 45
  words; numbers per rule 9; at most one credit per bullet.
- Fact bullets carry facts only (what happened, who said what). The causal
  read and the call go in the closing bullet — never mix judgment into a
  fact bullet.
- A closing bullet that starts with **推断：** (en style: **Read:**), one
  sentence of ≤ 70 Chinese characters / 35 words, in three parts: what the
  facts imply (the mechanism, as a judgment); a concrete trade-level
  positioning move; and the scenario that would change it — 「若<event /
  date / level>，则<how the call changes>」. E.g. 「**推断：**存储成本挤压正
  从 PC 蔓延到手机；回避存储成本敞口大的整机与芯片供应商，若 Apple(周一)
  与 Qualcomm(周三)毛利率守住，则挤压有限、可回补。」 The inference must
  follow from the block's facts — a reported claim may only be the 若
  condition. Keep it specific to this driver; the portfolio-level stance and
  sector allocation belong to 第 11 节(展望与策略).
Do not restate the same stock move in two blocks.

### File C — sections/03_positioning.md

""" + _POSITIONING_BODY + """
ALL THREE files end with the <!-- SECTION DONE --> marker on their own last line.
Each file also gets its own claims ledger (rule 11).
"""

NARRATIVE_TASK = None
DRIVERS_TASK = None

POSITIONING_TASK = "**第 3 节任务 —— 持仓与资金流**(输出文件首行必须是 \"## 持仓与资金流\")\n\n" + _POSITIONING_BODY

SECTOR_ACTION_TASK = """\
**第 4 节任务 —— 板块动态**(输出文件首行必须是 "## 板块动态",不要照抄本任务标题)

Write "## 板块动态" as 5–7 thematic blocks (NO sector scorecard / league
table, no sector-average lines). Choose themes by MARKET IMPACT: the biggest
same-day moves among large caps (market.sp500_constituents / top_movers) and
the catalysts behind them. The Closing Bell transcripts (main hour
clean_transcript + Overtime clean_transcript_ot) supply catalysts, color and
named guest/analyst views; news.flash supplies catalysts the show missed
(reports, downgrades, guidance and deal headlines). A big mover the show
skipped is folded into the theme it belongs to — never a separate "not
covered" block.
Each block:
- A bold one-clause headline stating what happened to the group
  (e.g. **Memory sold off even with prices high.**).
- 1–3 bullets. Bundle peers into one sentence with company names and one-
  decimal moves ("AMD fell 10.9%, Qualcomm 11.1%, Marvell 10.0% and Arm
  10.0%"), then the catalyst in one clause. A named guest/analyst view may
  close a bullet ("Mizuho's Jared Holz called inflows 'unusually strong'").
- Results only where they explain the move: beat/miss vs estimate, guidance,
  dividend, deal terms.
Every company named must appear in sp500_constituents, top_movers,
earnings_calendar, Closing Bell or news items. Use company names in prose
(tickers only where a name is ambiguous). The index/tape recap is NOT part of
this section.
"""

DEEP_DIVES_TASK = """\
**第 6 节任务 —— 个股深挖**(输出文件首行必须是 "## 个股深挖",不要照抄本任务标题)

Write "## 个股深挖" with the 3–5 most market-relevant single-name
stories of the day, ranked by: size of the move × market cap, an earnings or
guidance event, an analyst-rating wave, and how much the story drove peers.
How long the show talked about a name is only a tie-break. A deep dive may be a
non-listed company when its news moved the market (e.g. a private company's
IPO, rating or financing). Every subject must also appear in 板块动态.

Sources: the Closing Bell transcripts (main hour + Overtime),
earnings_calendar, market.sp500_constituents (price, change), news.flash
(rating and target changes, guidance and results headlines, deal terms).

Format per subject:

### INTC — Intel
(use "### Name" alone for a non-listed company)

- **What happened:** 1–2 sentences — the move with the closing price if
  available, and the catalyst.
- 3–5 detail bullets, each "Label: fact" with a short label
  (Guide / Results / One-off / Mix / Financing / Value …), e.g.
  "- Guide: fourth-quarter revenue of $15bn–$16bn and adjusted EPS of $0.25,
  against about $17bn and $0.43 expected (CNBC)." Put guidance-vs-consensus
  first when it caused the move.
- **Management said:** ONE short verbatim executive quote (name + title) from
  the Closing Bell transcripts. Omit the line if none exists.
- **Analyst reaction:** bullets in the form "Firm (Rating): target $X from
  $Y; one-clause reason." taken from news.flash items or named analysts in the
  transcripts. Up to 5, most influential first. Omit if none exists.
- **Bear case:** optional, only when named analysts/strategists argue the
  other side; same bullet form.

Never fabricate estimates, guidance, ratings, targets or quotes.
"""

COMPANY_DESK_TASK = """\
**任务二(一次读入,两个文件)—— 板块动态 + 个股深挖**(每个输出文件首行必须是任务规定的 H2 标题,不要照抄本任务标题)

Read the Closing Bell transcripts (main hour + Overtime) ONCE — while
reading, collect every executive quote (name + title + exact words) and
analyst view you might use, because you may NOT re-read the transcripts
afterwards. Then read the large-cap moves (finviz), the earnings calendar and
news.flash, and export TWO files. 个股深挖 subjects MUST
also appear in 板块动态. (今日涨跌榜 is rendered by the pipeline
from market data — do not write it.)

### File A — sections/04_sector_action.md

""" + SECTOR_ACTION_TASK + """
### File B — sections/06_deep_dives.md

""" + DEEP_DIVES_TASK + """
BOTH files end with the <!-- SECTION DONE --> marker on their own last line.
Each file also gets its own claims ledger (rule 11).
"""


def _macro_review_task(blocks: Dict[str, Any]) -> str:
    """08 回顾·宏观数据任务书(块注入:review_releases = 各国选中事件)。"""
    sel = blocks.get("review_releases") or {}
    lines = []
    for c, rows in sel.items():
        def _fmt(e: Dict[str, Any]) -> str:
            cmp_ = f" [{e['comparison']}]" if e.get("comparison") else ""
            u = e["unit"] if "unit" in e else event_unit(e)
            return (f"{e.get('event', '?')}{cmp_} {e.get('period') or ''} "
                    f"(actual={fmt_event_value(e.get('actual'), u)}, "
                    f"est={fmt_event_value(e.get('forecast'), u)}, "
                    f"prior={fmt_event_value(e.get('previous'), u)}, {e.get('date')})")
        lines.append(f"  - {c}: " + "; ".join(_fmt(e) for e in rows))
    selected = "\n".join(lines) if lines else "  (none)"

    return f"""\
## Section 8 — 回顾 · 宏观数据

Write "## 回顾 · 宏观数据", then "### 边际变化" with 3–6 bullets (≤45 words
each), Chinese prose, ranked by market impact — not by country. ONLY the
releases selected below; a country not listed must NOT get its own bullet
(its levels already sit in the Macro Dashboard table).

Selected releases (published with actual in [T-2, T]):
{selected}

Content rules:
- Each bullet: what printed, vs estimate/prior when both exist — house style
  「9月CPI同比 3.7%(预期 3.6%,前值 3.4%)」— then ONE clause of why it
  matters (direction of surprise, trend break, revision). Same event name with
  different comparison (mom/yoy/水平) is a different measure; never mix them.
- Do NOT restate a number that is already a cell of the Macro Dashboard table
  (Section 07 prints levels; this section is the CHANGE story).
- Interpretation from macro research material (sources.news_macro.research)
  may follow a fact, credited to the ORIGINATING institution named in the
  item (华创宏观/华泰宏观/申万宏源… for broker notes; Kobeissi/Anna Wong/
  Steno 等 for English macro voices — use name as posted). At most one
  credit per bullet. The posting community is a relay, not a source.
- PMI/景气 lines may bundle two economies in one bullet when they tell one
  story (e.g. 欧元区 10 月 PMI 全面回落而美国加速).
- EIA 原油库存、现货金银等无结构化口径的数字:只在新闻/研报素材里有原文
  时引用(出处+数字入台账),没有就整句不写 —— 禁止用替代指标拼数。
- If nothing was selected: heading + missing_mark line, then stop."""


def _geopolitics_task(blocks: Dict[str, Any]) -> str:
    """09 回顾·地缘政治 + 焦点事件任务书(块注入:geo_themes / focal_events)。"""
    themes = blocks.get("geo_themes") or {}
    theme_lines = []
    for name, info in themes.items():
        tops = " | ".join(str(t) for t in (info.get("top") or [])[:3])
        theme_lines.append(f"  - {name} (hits={info.get('hits')}): {tops}")
    themes_txt = "\n".join(theme_lines) if theme_lines else "  (none)"
    focal = blocks.get("focal_events") or []
    focal_txt = "\n".join(
        f"  - {f.get('name')} (hits={f.get('hits')})" for f in focal
    ) or "  (none)"

    return f"""\
## Section 9 — 回顾 · 地缘政治 + 焦点事件

Write "## 回顾 · 地缘政治". For EACH theme listed as open below, write an H3
subsection with EXACTLY that theme name (e.g. "### 美伊局势"), 2–4 bullets.
A theme NOT listed must NOT appear as a subsection.

Open themes today (from the macro geo layer + newswire; hits = snapshot
matches):
{themes_txt}

Then write "### 焦点事件" ONLY IF focal events are listed below — one-off
political events of the current moment (elections, summits, cabinet changes,
market-moving political shocks), 1–3 bullets each with what happened and the
immediate market read. Omit the whole block when none is listed.

Focal events today:
{focal_txt}

Content rules:
- Every bullet states facts with dates and numbers taken from the material
  (news_macro.geo / news_macro.digest / news.flash / news.wire). Material is
  bilingual: English items are written into Chinese prose, keeping the house
  English terms (FOMC, CPI, Hormuz) as-is. A bullet may close with ONE clause
  of implication (oil, rates, risk appetite) — speculation must read as
  speculation (「若…则…」), never as reported fact.
- Credit the ORIGINATING outlet or broker the item itself names ((财通证券)
  for a 利率早报 item, (CCTV) for a 联播快讯 item, (Geopolitical Dispatch),
  (Reuters) inside a wire item). Relaying accounts/communities are NOT sources.
- Cross-check against the structured layer when both exist (oil/gold moves
  from market.cross_asset) — the structured value wins; the material's number
  is dropped.
- Evergreen background (rules, seat counts, history) at most ONE short clause,
  and only when that event is active today.
- If no theme AND no focal event is open: heading + missing_mark line, stop."""


def _fomc_task(blocks: Dict[str, Any]) -> str:
    """10 美联储观察任务书(块注入:fomc_deep = 决议在近 3 个交易日内)。"""
    if blocks.get("fomc_deep"):
        deep_txt = """\
### 决议深读 (ON — decision within the last 3 sessions)
Write "### 决议深读" FIRST, before 利率与决议: the decision, the vote split and
each dissenter by name with their stated reason in one quoted clause
(sources.fomc.decisions[0].dissenters); then the statement language changes
that ACTUALLY differ between fomc.statement and fomc.statement_prev — quote
both sides briefly (e.g. "solid"→"strong"); list only real differences, never
invent one, and skip this half when the texts are materially identical."""
    else:
        deep_txt = """\
### 决议深读 (OFF)
No FOMC decision in the last 3 sessions — do NOT write a 决议深读 block."""

    return f"""\
## Section 10 — 美联储观察

Write "## 美联储观察" with these H3 blocks, in this order:
{deep_txt}

### 利率与决议
Current target range and the LATEST decision in 1–2 sentences: rate range,
change, vote split, dissenters by name — from sources.fomc.decisions[0] ONLY
(never economic_events for the Fed). One clause on the move since the previous
decision when change_bp ≠ 0. Keep it tight; the narrative lives in 决议深读.

### 市场定价路径
Futures-implied pricing as of fomc.path.asof, 2–4 bullets:
- The NEXT meeting's odds from macro_pillars.next_fomc_odds (odds_pct), house
  style 「12月会议加息概率 ~74%」(with 「(前值 ~xx%)」 only when
  sources.prior_odds exists; never invent a prior).
- Meetings BEYOND the next: phrase as contract pricing from fomc.path /
  fed_odds.contracts (「3月合约定价累计 ~2 次加息」style, cum bp ÷ 25) — a
  monthly-average contract is NEVER "某场会议的概率".
- Curve/rate anchors (par_curve change_bp) may back a bullet in half a
  sentence.

### 官员表态
1–3 bullets of Fed-speak from news.flash / news.wire (name + stance + the
occasion). Omit the whole block when nothing real exists today.

If sources.fomc is absent from the snapshot: heading + missing_mark, stop."""


OUTLOOK_TASK = """\
## Section 11 — 展望与策略 (second stage — synthesis over finished sections)

FIRST read these section files in your workspace (already written and
verified; treat their facts as established — reuse as-is, do not re-derive):
sections/01_narrative.md, sections/02_drivers.md, sections/04_sector_action.md,
sections/07_macro_dashboard.md, sections/08_macro_review.md,
sections/09_geopolitics.md, sections/10_fomc.md.
The snapshot remains your ONLY source for any NUMBER you state. Do not read
transcripts/ or any other file.

Write "## 展望与策略" with:

### 大事预判
Up to 4 forward items. Each: a bold date or horizon, the event, the base case,
and what would flip it. The base case rests on confirmed facts and pricing;
a reported claim (`reported: true` items, 消息人士/据报道) may only be a flip
condition, written as such. ONLY events that have NOT happened yet — dates from
economic_events.upcoming, macro_pillars.fomc_calendar, or events the news
layers name with a date. FOMC path judgment lives here. 2–4 bullets,
≤60 words each.

### 主线跟踪 (omit when no live cross-market theme deserves it)
ONE ongoing market theme drawn from the market sections (an AI-capex cycle, a
rotation, a de-grossing): where it stands, the bull/bear hinge. Never restate
single-stock moves already written in 板块动态 / 个股深挖.

### 策略应对
Direct, actionable house positioning at PORTFOLIO level, 3–5 bullets: overall
stance (加仓/观望/减仓), structure (e.g. 哑铃:高股息防守 + 主线进攻), sectors
to overweight and to avoid, and the macro/Fed condition that would change
the stance. The action bullets in sections/02_drivers.md are trade-level
calls on single drivers — never restate or paraphrase them here; if you
agree with one, fold it into a sector stance instead of repeating it.
Judgment IS expected in this block — but every NUMBER still comes from the
snapshot and enters the claims ledger; judgments must read as judgments
(「建议」「预计」), never as reported facts. Do NOT use the heading 投资建议
or present the block as personalized advice. Close the block with exactly:
(以上为本报观点,不构成投资建议。)

Cross-section consistency: a fact already stated in Sections 01–10 must read
IDENTICALLY here — same value, same framing, no new framing of old facts.
"""


SECTION_SPECS: Dict[str, Dict[str, Any]] = {
    "narrative": {
        "output_file": "sections/01_narrative.md",
        "agent": "overall_market",
        "task": NARRATIVE_TASK,
    },
    "drivers": {
        "output_file": "sections/02_drivers.md",
        "agent": "overall_market",
        "task": DRIVERS_TASK,
    },
    "positioning": {
        "output_file": "sections/03_positioning.md",
        "agent": "overall_market",
        "task": POSITIONING_TASK,
    },
    "sector_action": {
        "output_file": "sections/04_sector_action.md",
        "agent": "company_desk",
        "task": SECTOR_ACTION_TASK,
    },
    "movers": {
        "output_file": "sections/05_movers.md",
        "agent": "movers",
        "mode": "deterministic",
        "task": None,
    },
    "deep_dives": {
        "output_file": "sections/06_deep_dives.md",
        "agent": "company_desk",
        "task": DEEP_DIVES_TASK,
    },
    # ── v8 Macros:回顾/当下/展望五文件(替代旧 07_macro 单文件) ──
    "macro_dashboard": {
        "output_file": "sections/07_macro_dashboard.md",
        "agent": "macro_dashboard",
        "mode": "deterministic",
        "task": None,
    },
    "macro_review": {
        "output_file": "sections/08_macro_review.md",
        "agent": "macro_review",
        "task": None,
    },
    "geopolitics": {
        "output_file": "sections/09_geopolitics.md",
        "agent": "geopolitics",
        "task": None,
    },
    "fomc": {
        "output_file": "sections/10_fomc.md",
        "agent": "fomc",
        "task": None,
    },
    "outlook": {
        "output_file": "sections/11_outlook.md",
        "agent": "outlook",
        "task": None,
    },
}

# v8.6:今日涨跌榜(美股/港股/A股成交额榜)上移到板块动态之前 —— 紧跟
# 美股全景图下方的版式位;章节文件名(05_movers.md)保持不变,避免破坏
# 断点续跑的文件契约。
SECTION_ORDER = ["narrative", "drivers", "positioning", "movers",
                 "sector_action", "deep_dives", "macro_dashboard",
                 "macro_review", "geopolitics", "fomc", "outlook"]

# 两段编排:第一段互不依赖、并行;第二段(outlook)读第一段成品做综合。
AGENT_STAGES = [
    ["overall_market", "company_desk", "movers", "macro_dashboard",
     "macro_review", "geopolitics", "fomc"],
    ["outlook"],
]
# 扁平清单(兼容单层遍历的旧调用方;顺序 = 第一段全部 + 第二段)
AGENT_ORDER = [a for stage in AGENT_STAGES for a in stage]

# 各 LLM agent 的任务书构造器(blocks 由编排器确定性判定后注入);
# deterministic 节(movers/macro_dashboard)无任务书,由编排器渲染。
AGENT_TASK_BUILDERS = {
    "overall_market": lambda style, blocks: OVERALL_MARKET_TASK,
    "company_desk": lambda style, blocks: COMPANY_DESK_TASK,
    "macro_review": lambda style, blocks: _macro_review_task(blocks or {}),
    "geopolitics": lambda style, blocks: _geopolitics_task(blocks or {}),
    "fomc": lambda style, blocks: _fomc_task(blocks or {}),
    "outlook": lambda style, blocks: OUTLOOK_TASK,
}

_FAKE_SECTIONS = {
    "narrative": (
        "## 今日叙事\n"
        "- Placeholder narrative bullet (fake mode). "
        "<!-- audit: [show the_close] -->\n"
        "<!-- HEADLINE_CANDIDATES\n"
        '"Placeholder headline clause. Other clause."\n'
        "-->\n"
    ),
    "drivers": (
        "## 核心驱动\n"
        "**Placeholder driver.**\n"
        "- Index moved {spx_move} "
        "(fake mode value — deterministic check must flag this). "
        "<!-- audit: [show the_close] -->\n"
        "- Placeholder action (fake mode).\n"
    ),
    "positioning": (
        "## 持仓与资金流\n"
        "- **Primary — Placeholder.** Placeholder (fake mode). "
        "<!-- audit: [show the_close] -->\n"
    ),
    "sector_action": (
        "## 板块动态\n"
        "**Placeholder theme.**\n"
        "- Placeholder sector action (fake mode). "
        "<!-- audit: [show cnbc_closing_bell] -->\n"
    ),
    "deep_dives": (
        "## 个股深挖\n"
        "### FAKE — Placeholder Corp\n"
        "- **What happened:** placeholder (fake mode). "
        "<!-- audit: [show cnbc_closing_bell] -->\n"
    ),
    # v8 Macros 五节(fake 模式占位;中文标题与真实任务书一致)
    "macro_dashboard": (
        "## 宏观速览\n"
        "### 四大经济体\n"
        "| 经济体 | 增长 | 通胀 | 政策利率 |\n"
        "|---|---|---|---|\n"
        "| 美国 | fake | fake | fake |\n"
        "### 商品与贵金属\n"
        "| 品种 | 收盘 | 1D |\n"
        "|---|---|---|\n"
        "| Brent | 0 | 0% |\n"
        "### 本周前瞻\n"
        "- Fake catalyst (fake mode).\n"
    ),
    "macro_review": (
        "## 回顾 · 宏观数据\n"
        "### 边际变化\n"
        "- Placeholder macro change (fake mode). "
        "<!-- audit: [data economic_events.recent] -->\n"
    ),
    "geopolitics": (
        "## 回顾 · 地缘政治\n"
        "### 美伊局势\n"
        "- Placeholder geo fact (fake mode). "
        "<!-- audit: [news flash] -->\n"
    ),
    "fomc": (
        "## 美联储观察\n"
        "### 利率与决议\n"
        "- Placeholder fomc fact (fake mode). "
        "<!-- audit: [data fomc.decisions] -->\n"
    ),
    "outlook": (
        "## 展望与策略\n"
        "### 大事预判\n"
        "- Placeholder outlook (fake mode). "
        "<!-- audit: [data economic_events.upcoming] -->\n"
        "### 策略应对\n"
        "- Placeholder strategy response (fake mode).\n"
    ),
}


def fake_section_text(name: str, corrected: bool = False,
                      spx_move: str = "+9.99%") -> str:
    text = _FAKE_SECTIONS[name].replace("{spx_move}", spx_move)
    if corrected and name == "drivers":
        return (
            "## 核心驱动\n"
            "**Placeholder driver.**\n"
            "- Placeholder fact (fake mode corrected). "
            "<!-- audit: [show the_close] -->\n"
            "- Placeholder action (fake mode).\n"
        )
    return text


def build_section_prompt(spec: dict, style: str = "en",
                         blocks: Optional[Dict[str, Any]] = None) -> str:
    if spec.get("mode") == "deterministic":
        raise ValueError("deterministic sections have no prompt")
    task = spec["task"]
    name = next(n for n, s in SECTION_SPECS.items() if s is spec)
    if task is None:
        if name not in AGENT_TASK_BUILDERS:
            raise ValueError("section has no standalone task — use build_agent_prompt")
        task = AGENT_TASK_BUILDERS[name](style, blocks)
    allowed = SECTION_SOURCE_MATRIX.get(name)
    p = STYLE_PROFILES[style]
    task = task.replace("{missing_mark}", p["missing_mark"])
    return (f"{_common_rules(style, allowed_tags=allowed)}\n{task}\n\n"
            f"Output file: {spec['output_file']}\n")


def agent_section_names(agent: str) -> list:
    """该 agent 负责的章节文件(按 SECTION_ORDER 顺序)。"""
    return [n for n in SECTION_ORDER if SECTION_SPECS[n].get("agent", n) == agent]


# ── 编辑计划(写作前一次规划:故事归属 + 规范数值) ─────────────────────

PLAN_OWNER_ROLES = {
    "drivers": "market-wide drivers: index moves, macro data as traded, "
               "policy/government headlines, large M&A, credit events",
    "sector_action": "sector and industry moves; single-stock moves grouped by theme",
    "deep_dives": "1–3 companies with earnings, guidance or a corporate event "
                  "worth a full read",
    "macro_review": "the economic releases selected for today (see digest)",
    "geopolitics": "the geopolitical themes opened for today (see digest)",
    "fomc": "Fed decisions, Fed speakers, rate-path pricing",
    "outlook": "events that have not happened yet (calendar, earnings ahead)",
}


def _sec_label(name: str) -> str:
    return (f"{SECTION_ORDER.index(name) + 1:02d} {name}"
            if name in SECTION_ORDER else name)


def build_plan_prompt(owners: List[str], digest: str = "") -> str:
    """单发版编辑计划 prompt(v8.5-fix):digest 全文内联,输出走 stdout JSON,
    不挂附件、不带工作区 —— agent 无文件可读、无工具需求,游荡在结构上不可能
    (2026-11-05 事故:游荡 11 次请求/1.74M 输入 + CLI 挂死,见 TEST P23)。"""
    roles = "\n".join(f"- {n}: {PLAN_OWNER_ROLES[n]}" for n in owners
                      if n in PLAN_OWNER_ROLES)
    return (
        "You are the editor of the GuanHe Daily Report. Before the section "
        "writers start, decide what the day's stories are and which section "
        "owns each one, so the brief tells each story once and every number "
        "reads the same everywhere.\n"
        "\n"
        "Below (after the marker) is the complete digest of today's data and "
        "news with item ids — everything you need. This is a quick editorial "
        "call, not research: do NOT use any tools, do NOT read or write any "
        "files; answer from the digest alone, in one reply.\n"
        "\n"
        "Sections that can own a story today:\n"
        f"{roles}\n"
        "(Section 01 narrative summarises the lead stories' conclusions and "
        "owns none; 03 positioning covers flows only.)\n"
        "\n"
        "Reply with ONE JSON object of this shape (and nothing else — no "
        "prose, no code fences):\n"
        '{"stories": [{"id": "s1", "title": "<one line>", '
        '"owner": "<section key>", "may_mention": ["<section key>"], '
        '"angle": "<what the owner argues, ≤20 words>"}], '
        '"canonical": [{"entity": "NVDA", "metric": "move", "value": "2.1%", '
        '"path": "market.sp500_constituents[ticker=NVDA].change_percent"}]}\n'
        "Rules:\n"
        "- 4–7 stories, ordered by market impact; exactly one owner each, "
        "chosen from the list above.\n"
        "- Items marked [reported] are anonymous-source claims or rumours: "
        "one may be part of a story, but never the story's sole basis or its "
        "canonical number.\n"
        "- may_mention: other sections that may reference the story in one "
        "clause (e.g. outlook for what comes next); leave it empty when none "
        "should.\n"
        "- canonical: at most 12 entries — the key number of the lead "
        "stories (stock moves, index closes/moves, yields, Fed odds, "
        "reported revenue/EPS), ONE number per entry exactly as the brief "
        'should state it ("2.1%", not "53356.12 (+3.03%)"; a close and its '
        "move are two entries), with its source — a snapshot path for data, "
        "or the news item (news.flash[id=f21]). Only numbers shown in the "
        "digest. Treasury yields: path "
        "macro_pillars.par_curve.levels_pct.<tenor>, two decimals (4.90%). "
        "entity = ticker / cross_asset code / US2Y, US10Y / meeting month "
        "(2026-12); metric = move, ah_move, pm_move, close, level, yield, "
        "odds, revenue, eps, guide_revenue, guide_eps.\n"
        "- Facts and assignments only — no evidence lists, no advice, no "
        "prose.\n"
        "\n"
        "===== plan_input digest =====\n"
        f"{digest}\n"
        "===== end digest ====="
    )


def plan_brief(plan: Optional[Dict[str, Any]], names: List[str]) -> str:
    """把编辑计划裁成某个 agent 能用的那一段:自己负责的、可一句带过的、别人的。"""
    if not plan or not plan.get("stories"):
        return ""
    own, mention, other = [], [], []
    for s in plan["stories"]:
        tag = f'{s.get("id")} "{s.get("title")}"'
        owner = s.get("owner")
        if owner in names:
            ev = ", ".join(str(e) for e in (s.get("evidence") or [])[:6])
            own.append(f"- {tag} → {_sec_label(owner)}; angle: {s.get('angle') or '-'}"
                       + (f" [evidence: {ev}]" if ev else ""))
        elif "narrative" in names or set(s.get("may_mention") or []) & set(names):
            mention.append(f"- {tag} (owned by {_sec_label(owner)})")
        else:
            other.append(f"{tag} ({_sec_label(owner)})")
    lines = ["EDITORIAL PLAN (fixed before writing — follow it):"]
    if own:
        lines += ["Stories you OWN — cover them in full:", *own]
    if mention:
        lines += ["Stories you may reference in ONE clause (owned elsewhere — no "
                  "repeated numbers or detail):", *mention]
    if other:
        lines.append("Owned by other sections — do not write them: " + "; ".join(other))
    lines.append("A story outside the plan is fine only if it clearly belongs to "
                 "your section.")
    canon = [c for c in plan.get("canonical") or [] if isinstance(c, dict)]
    if canon:
        lines.append("Canonical numbers — when you state these facts, use exactly "
                     "these values (the checker compares every section to them):")
        lines += [f"- {c.get('entity')} {c.get('metric')}: {c.get('value')} "
                  f"({c.get('path')})" for c in canon]
    return "\n".join(lines) + "\n"


def build_agent_prompt(agent: str, style: str = "en",
                       blocks: Optional[Dict[str, Any]] = None,
                       plan: Optional[Dict[str, Any]] = None) -> str:
    """按信源组构建任务书。归因白名单 = 组内各文件矩阵的并集。不挂转录。

    outlook(第二段)例外:任务书指示先读第一段成品文件,快照仍是数字唯一来源。
    """
    names = agent_section_names(agent)
    if agent in AGENT_TASK_BUILDERS:
        task = AGENT_TASK_BUILDERS[agent](style, blocks)
    else:
        spec = SECTION_SPECS[names[0]]
        if spec.get("mode") == "deterministic":
            raise ValueError("deterministic sections have no prompt")
        task = spec["task"]
        if task is None:
            raise ValueError(f"agent {agent} has no task builder")
    files = " and ".join(SECTION_SPECS[n]["output_file"] for n in names)
    out_line = (f"Output files: exactly {files} — write each listed file in "
                "this one run (and its .claims.json ledger).")
    allowed = set()
    for n in names:
        allowed |= SECTION_SOURCE_MATRIX.get(n, set())
    p = STYLE_PROFILES[style]
    task = task.replace("{missing_mark}", p["missing_mark"])
    brief = plan_brief(plan, names)
    return (f"{_common_rules(style, allowed_tags=allowed or None)}\n{task}\n"
            + (f"\n{brief}" if brief else "")
            + f"\n{out_line}\n")


# ── 核验任务书(叙事质量;数字对账由编排器跑 verify_checks.py) ──────────

def build_verifier_prompt(style: str = "en") -> str:
    p = STYLE_PROFILES[style]
    return f"""\
You are the narrative-quality verification agent of a scheduled report pipeline.
A deterministic audit script (verify/check_results.json) has ALREADY been run by
the orchestrator against snapshot.json. Read that file first.

Your job is ONLY Layer 2 — narrative quality review
({p['quality_review_language']}):
- Contradictions between sections or between a section and the data.
- The same fact stated differently in different places (a "worst day since"
  framing, a market-cap loss, a % move measured from different bases).
- Unsupported causal claims presented as facts.
- House-style violations that matter: vague quantifiers without numbers,
  constructed quotes, yield moves stated in % instead of bp, the same stock
  move repeated across blocks, production-process wording in the prose
  ("snapshot", "transcript", "on air", "the desk", show names).
- Macros must not invent Releases / Rate Expectations blocks that the
  deterministic audit already flagged as structural violations.
- Missing sections, empty files, missing <!-- SECTION DONE --> markers.
High-severity narrative problems (fabricated facts, wrong direction of a move)
fail the verdict.

Merge: if verify/check_results.json contains any high-severity mismatch
(match=false), the overall verdict MUST be "fail" and those issues must appear
in issues[]. You may add additional narrative issues.

OUTPUT CONTRACT — write verify/verdict.json (this file is what the orchestrator
reads; your chat reply is ignored):
{{
  "verdict": "pass" | "fail",
  "summary": "one-paragraph audit summary",
  "issues": [
    {{"section": "narrative|drivers|positioning|sector_action|movers|deep_dives|macro_dashboard|macro_review|geopolitics|fomc|outlook",
     "claim": "what the section says",
     "snapshot_value": "what the snapshot says (or 'absent')",
     "severity": "high|medium|low",
     "suggestion": "concrete fix"}}
  ],
  "quality_notes": ["..."]
}}
Rules: an empty issues array with verdict "pass" is valid. Do not modify
sections/, and do NOT touch state.json — the orchestrator owns it. Do NOT
rewrite verify/check.py.

Your task now: read verify/check_results.json and sections/*.md, then write
verify/verdict.json.

Output file: verify/verdict.json
"""


def build_light_verifier_prompt(style: str = "en") -> str:
    """轻量叙事抽查:数字已由脚本按台账对过,这里只看跨节一致性与文风。

    不挂快照、不挂转录 —— 上下文只有七节正文,目标是一分钟内完成。
    """
    p = STYLE_PROFILES[style]
    return f"""\
You are a quick editorial reviewer in a scheduled report pipeline. Every number
in sections/*.md has ALREADY been checked against the data by a script (see
verify/check_results.json — all high-severity rows passed). Do NOT re-check
numbers and do NOT open snapshot.json or transcripts/.

Read sections/*.md only ({p['quality_review_language']}) and flag ONLY:
- Contradictions between sections (same asset/company moving in opposite
  directions, a story called the day's driver in one section and absent or
  reversed in another).
- The same fact stated differently in different places: a superlative
  ("worst day ever" vs "since 1974" vs "in four decades"), a market-cap or
  dollar figure, or a % move quoted with two different values.
- Speculation stated as reported fact, or an anonymous-source / reported
  claim (消息人士、据报道、传闻) written as confirmed fact — high when the
  claim is stated as fact; medium when a 推断, base case or piece of advice
  rests on such a claim alone.
- In 02_drivers.md: a block whose fact bullets carry the judgment, or whose
  closing 推断 bullet lacks the 「若…则…」 condition (medium).
- Quotes without a named speaker and affiliation.
- Vague quantifiers ("sharply", "大幅") with no number attached.
- Bloat: the same stock move restated in several blocks of one section.
Severity: high = contradiction, wrong direction, or the same fact stated with
two different values; everything else medium/low.
Verdict is "fail" only if there is at least one high issue. Be brief.

Write verify/verdict.json:
{{"verdict": "pass" | "fail", "summary": "...",
  "issues": [{{"section": "...", "claim": "...", "snapshot_value": "n/a",
              "severity": "high|medium|low", "suggestion": "..."}}],
  "quality_notes": ["..."]}}
Do not modify any other file.

Output file: verify/verdict.json
"""
