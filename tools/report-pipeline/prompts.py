# -*- coding: utf-8 -*-
"""章节任务书与核验任务书(CNT 风格 Daily Market Brief,v3)。

v3 核心变化 —— 严格的"章节 × 信源"映射(对照用户验证过的 CNT 对应关系):

    第一部分 OVERALL MARKET(narrative/drivers/positioning)
        ⟵ 只有 The Close 摘要(节目自带数字;缺失即「不可得」)
    第二部分 EQUITY MARKET(sector_action/movers/deep_dives)
        ⟵ 只有 Closing Bell 摘要 + Finviz/板块结构化数据(数字锚)
    第三部分 MACROS(macro)
        ⟵ 只有经济日历 + macro_pillars(曲线/FRED/自算概率/拍卖/Fed日历)

v2 的问题:每节都可引用全部信源 → 内容跨节重复(同一事实在 4 个章节出现)、
任务书臃肿、且偏离"节目→报告段落"的对应假设。v3 由 prompt 约束映射、
由核验器按 SECTION_SOURCE_MATRIX 执法(越界归因 = high)。

其余 v2 机制保留: STYLE_PROFILE(en/cn)、审计注释化、三元组/since-claims/
概率断言、MACROS 块矩阵、CNT 句式惯例。
"""

from typing import Any, Dict, Optional

# ── 风格档案 ───────────────────────────────────────────────────────────

STYLE_PROFILES: Dict[str, Dict[str, Any]] = {
    "en": {
        "body_language": "English (Simplified style of a professional financial newsletter)",
        "missing_mark": "(not in snapshot)",
        "read_prefix": "Read:",
        "quality_review_language": "Write verdict.json summary/issues in English.",
    },
    "cn": {
        "body_language": "简体中文 (keep tickers, company/person names, show names "
                         "and numbers in original form)",
        "missing_mark": "「不可得」",
        "read_prefix": "推断:",
        "quality_review_language": "Review the Chinese text; write verdict.json "
                                   "summary/issues in Chinese (JSON keys and enum "
                                   "values stay English).",
    },
}

# 括号归因词 → snapshot 源(核验据此做归因映射审计)
ATTRIBUTION_MAP = {
    "(The Close)": "the_close.summary_markdown",
    "(Closing Bell)": "cnbc_closing_bell.summary_markdown",
    "(cross-asset)": "market.cross_asset",
    "(sector averages)": "market.sp500_sector_avg",
    "(Finviz)": "market.top_movers",
    "(economic calendar)": "economic_events",
    "(Treasury curve)": "macro_pillars.par_curve",
    "(FRED)": "macro_pillars.fred_ranks / fred_extras",
    "(fed funds futures)": "macro_pillars.fed_odds",
    "(Fed calendar)": "macro_pillars.fomc_calendar",
    "(auction calendar)": "macro_pillars.auction_events",
    "(nowcasts)": "macro_pillars.nowcasts",
}

# 严格"章节 × 信源"矩阵:每节允许出现的括号归因标签(核验执法依据)
SECTION_SOURCE_MATRIX = {
    "narrative": {"(The Close)"},
    "drivers": {"(The Close)"},
    "positioning": {"(The Close)"},
    "sector_action": {"(Closing Bell)", "(sector averages)", "(Finviz)"},
    "movers": {"(Finviz)"},          # 确定性渲染,核验只对数字
    "deep_dives": {"(Closing Bell)", "(Finviz)"},
    "macro": {"(economic calendar)", "(Treasury curve)", "(FRED)",
              "(fed funds futures)", "(Fed calendar)", "(auction calendar)",
              "(nowcasts)"},
}


def _style_rules(style: str) -> str:
    p = STYLE_PROFILES[style]
    return f"""\
4. Output language: {p['body_language']}. Keep section headings in English exactly
   as specified (house style). Keep tickers ($MU, CCI), company/person names, show
   names, and all numbers in their original form.
"""


def _common_rules(style: str, allowed_tags: Optional[set] = None) -> str:
    p = STYLE_PROFILES[style]
    attrib_list = "\n   ".join(f"{k} → {v}" for k, v in ATTRIBUTION_MAP.items())
    tag_line = ""
    if allowed_tags is not None:
        tags = ", ".join(sorted(allowed_tags)) or "(none)"
        tag_line = f"""
   SOURCE RESTRICTION for THIS section: the ONLY permitted attribution tags are
   {tags}. Content, quotes, numbers and companies come ONLY from the snapshot
   sources those tags map to; other sources in the snapshot are OFF-LIMITS for
   this section (verification fails on any violation). If your assigned source
   lacks a number or fact you want to state, mark it {p['missing_mark']} — do not
   borrow from another source.
"""
    return f"""\
You are one section-writer inside a scheduled report pipeline. You are writing ONE
section of a "Daily market brief" in the style of a professional financial newsletter
(think: concise, data-anchored, every claim traceable — house style mirrors the CNT
daily brief: dense bullets, bold lead-ins, parenthetical source attributions).

HARD RULES (violating any of these fails pipeline verification):
1. The ONLY data source is the attached snapshot.json (a frozen data snapshot).
   Do NOT invent numbers. Do NOT fetch anything from the internet.
2. Every number you state must exist in snapshot.json. If the snapshot lacks a
   number you would like to state, either omit the sentence or mark the gap
   explicitly as {p['missing_mark']}.
3. Write the section to the exact output file specified below, as Markdown.
   Do not create any other files.
{_style_rules(style)}
5. When your assigned source (see SOURCE RESTRICTION) is missing from the
   snapshot, degrade gracefully: write from what IS available; if nothing is
   available, write the heading plus a single {p['missing_mark']} line and stop.
6. 不可得红线: if a data point you would need is absent from your assigned
   source, write {p['missing_mark']} explicitly at the point of need — NEVER
   substitute a proxy metric, a stale value, a similar-but-different instrument,
   or a number from a source outside your section's SOURCE RESTRICTION.
7. ATTRIBUTION (house style, in the PROSE itself): end each factual sentence or
   bullet with one or two parenthetical source tags, chosen ONLY from this list:
   {attrib_list}
   {tag_line}
   Interpretation is allowed on the designated Take line unlabeled; a standalone
   causal/interpretive conclusion resting on a single show tag must be prefixed
   "{p['read_prefix']}".
8. AUDIT TRAIL (hidden layer, mandatory): append an HTML comment at the end of
   each factual bullet carrying the precise snapshot key, e.g.
   `<!-- audit: [show the_close] -->` or `<!-- audit: [data market.cross_asset] -->`
   (both keys, space-separated, when a bullet mixes layers). These comments are
   stripped at publish time into an audit sidecar — they are for the verifier.
9. NUMBER STYLE (house conventions):
   - Every number carries a reference frame: actual vs est vs prior, level vs
     change, or a historical comparison. Preferred shapes:
     `+29K (est: +90K, prior: +162K)`, `5.30% (prior: 4.834%)`, `-17bp to 4.96%`.
   - Yield changes in bp; price/index changes in %; timestamps in ET.
   - "highest/lowest since <date>" claims are ONLY allowed by quoting the
     precomputed `macro_pillars.fred_ranks.*.claim` field — never compute a
     "since" assertion yourself.
   - No vague quantifiers ("sharply", "significantly") without a number.
   - Quotes need name + affiliation, verbatim from your assigned source; never
     construct a quote that is not in the snapshot.
10. End the file with a line containing only: <!-- SECTION DONE -->
"""


# ── 章节任务书 ─────────────────────────────────────────────────────────
#
# 执行分组: narrative + drivers 由同一个 agent 一次分析、两个文件分别导出
# ("overall_market" 组)。理由: 两节读同一份 The Close 摘要,拆成两个 agent
# 互相看不到对方输出,"速览不带数字/深挖带数字"的分工只能靠各自自觉,主题与
# 数字必然重复;合并后同一上下文里天然知道哪些数字已放进 drivers,narrative
# 才可能真正做成无重复的速览层。文件契约不变(01/02 两个文件)、核验矩阵不变。

OVERALL_MARKET_TASK = """\
## Part 1 — Today's Narrative + Top Market Drivers (one analysis, two files)

You analyze The Close digest ONCE and export TWO files. The two parts
complement, never repeat: numbers and depth live in the drivers file; the
narrative file is the scan layer — a market-wrap chyron, not a mini-drivers.

### File A — sections/01_narrative.md  (SCAN layer)

Write "## Today's Narrative" with EXACTLY 3-4 bullets. Rules:
- Each bullet is ONE short sentence: bold lead-in naming ONE dominant story
  from The Close, plus at most ONE number (an index move or the single most
  salient figure — and prefer numbers you did NOT use in the drivers file).
- No detailed figures, unit economics, multi-number lists, or quotes here —
  analytical depth belongs to Top Market Drivers. A narrative bullet that
  reads like a drivers bullet is a failure.
- Order by importance; anything The Close does not quantify is omitted or
  marked {missing_mark}.

THEN append the headline candidates comment block (the report's editorial
title — house style: two-clause headline naming the day's two biggest stories,
e.g. "Chip stocks rally again before the Fed and Big Tech results. Boeing
sinks."):

<!-- HEADLINE_CANDIDATES
"First clause. Second clause."
"Alternate headline clause. Other clause."
-->

2-3 candidates, single quoted lines, two short clauses, no source tags inside.
The FIRST candidate is used as the published headline. Derive them from the
combined picture of BOTH files.

### File B — sections/02_drivers.md  (ANALYSIS layer)

Write "## Top Market Drivers" with up to 5 driver blocks derived ONLY from
The Close (its 市场主线/市场驱动因素/核心分歧 segments). This is where the
day's stories get mechanism, transmission logic, full number reference
frames, and quotes. Each block:
- A bold lead-in headline (one clause, e.g. **A soft print could not hold the long end.**)
- 2-5 factual bullets with numbers as stated in The Close, each tagged
  (The Close) and carrying an audit comment.
- A final one-line takeaway, exactly: **Take:** <one sentence> — the actionable
  reading of the block (unlabeled interpretation is allowed ONLY on this line;
  everywhere else interpretation needs the read prefix).
Do not exceed 5 blocks; pick the ones The Close actually supports.

BOTH files end with the <!-- SECTION DONE --> marker on their own last line.
"""

NARRATIVE_TASK = None   # 并入 OVERALL_MARKET_TASK(由 overall_market 组执行)
DRIVERS_TASK = None     # 同上

POSITIONING_TASK = """\
## Section 3 — Positioning / Flow

Write "## Positioning / Flow" with three labeled items, in this order (skip any
whose material is genuinely absent, marking it {missing_mark} in one short line
instead). Material comes ONLY from the_close.positioning_material — the bundle
of the digest's 跨资产信号/核心分歧/分歧解决框架/关键嘉宾语录/投资组合启示
segments:
- **Primary positioning:** the dominant positioning theme of the day (rates,
  equity beta, duration) with the digest's prescribed response.
- **Secondary positioning:** a second flow/positioning thread (credit, gold,
  FX, sector rotation, buybacks, ETF flows).
- **Guest positioning:** the most decision-relevant strategist/guest call —
  name + firm + the call, verbatim where possible.
Rules: every claim tagged (The Close); numbers only as stated in the digest.
"""

SECTOR_ACTION_TASK = """\
## Section 4 — Sector Action

Write "## Sector Action" as:
1. **Sector scorecard** — from market.sp500_sector_avg: best and worst sectors
   with average moves; name 2-3 standout tickers per side from market.top_movers
   with their numbers.
2. **Thematic blocks** — 2-4 blocks synthesized ONLY from the Closing Bell
   digest (an earnings story, a policy story, a disruption story), each with a
   bold lead-in, 2-4 bullets tagged (Closing Bell).
Every ticker mentioned must come from top_movers or the Closing Bell digest.
Interpretive sentences need the read prefix unless they are the block's
one-line takeaway. The overall index/tape recap is NOT part of this section
(it lives in Today's Narrative).
"""

# movers 章节由编排器确定性渲染(sections/05_movers.md),无 LLM 任务书。

DEEP_DIVES_TASK = """\
## Section 6 — Company Deep Dives

Write "## Company Deep Dives". Pick the 3-5 companies most prominently discussed
in the Closing Bell digest (tie-break: appearance in market.top_movers). For
each company write a subsection headed by its ticker:

### $TICKER — Company Name

- **What happened:** the event/coverage as told by Closing Bell, with the price
  move if in top_movers.
- **Key numbers:** only numbers present in the Closing Bell digest or
  top_movers (with attribution tags and audit comments); if none, write
  {missing_mark}.
- **Management said:** a verbatim quote from an executive, ONLY if a direct
  quote exists in the Closing Bell digest (name + title + the quote). If no
  direct quote exists, OMIT this line entirely — do not paraphrase into a quote
  and do not write the label with nothing under it.
- **Analyst reaction:** analyst/strategist reactions ONLY if present in the
  Closing Bell digest (name + firm + view). Same omission rule as above.
- **Bull case / Bear case:** each side must cite its basis in the digest;
  omit the missing side rather than inventing it.
- One synthesis line prefixed with the read prefix.

STRICT: do not add companies not discussed by Closing Bell; do not fabricate
estimates, EPS, guidance numbers, or quotes; do not pull material from The Close.
"""


def _macro_task(blocks: Dict[str, Any]) -> str:
    """MACROS 任务书按块矩阵动态拼装 —— 内容源只有结构化数据(经济日历 +
    macro_pillars),不复刻 CNT "不是每期一致" 的伸缩,且不含节目内容。

    blocks 由编排器从快照确定性判定:
      releases   当日 recent 有含 actual 的事件 → 写 Releases 块
      auction    拍卖事件存在(今日有结果或未来有拍卖)→ 写拍卖块
      fomc       FOMC 在 recent/upcoming 窗口内 → 写 Fed 块
      rate_expectations  恒 True(fed_odds 可用)→ Rate Expectations 块
      forward    恒 True → Forward Catalysts 块
    """
    parts = ["""\
## Section 7 — Macros

Write "## Macros" with the blocks below, in the order given. A block listed as
OFF must NOT appear in your output at all (its absence is correct house style:
the Macros section flexes with the calendar). This section uses ONLY structured
data (economic calendar + macro pillars) — never the show digests."""]

    if blocks.get("releases"):
        parts.append("""\

### Releases (ON)
From economic_events.recent, for each major release that HAS an actual value,
one block:
- **Headline line:** event name — actual vs forecast vs previous, house style
  `+29K (est: +90K, prior: +162K)`. Add revision/subread detail ONLY if the
  calendar carries it.
- At most 1-2 bullets of factual context from the calendar fields (subreads,
  related series). Data fields with no actual value are marked {missing_mark},
  never filled with substitutes.""")
    else:
        parts.append("""\

### Releases (OFF)
No recent event in the snapshot carries an actual value — do NOT write a
Releases block at all today.""")

    if blocks.get("auction"):
        parts.append("""\

### Treasury Auctions (ON)
From macro_pillars.auction_events (economic calendar rows):
- For TODAY's auctions with results: the security, size if present, the
  actual (stop yield) vs previous — house style `5.033% (prior: 4.393%)`.
- For UPCOMING auctions within the forward window: date, security, the level
  context (e.g. where the tenor closed today, from macro_pillars.par_curve) —
  house style "the 20-year closed at 5.47%, 5bp above September's 5.420% stop"
  only when both numbers exist in the snapshot.""")
    if blocks.get("fomc"):
        parts.append("""\

### Fed block (ON)
Cover the FOMC-relevant material the structured sources support: decision/
dots/minutes items in economic_events, the next meeting from
macro_pillars.fomc_calendar, and the rate-expectations headline below. If the
Fed event is in `recent` (with actual), lead with it; if it is upcoming, frame
it as forward catalyst context only.""")
    if blocks.get("rate_expectations"):
        parts.append("""\

### Rate Expectations (ON — always present when fed_odds is in the snapshot)
Headline from macro_pillars.next_fomc_odds: odds of a hike at the next
meeting, with the prior comparison ONLY if sources.prior_odds exists —
house style `Hike Odds ~20% (prior: ~24%)`. Add 1-3 supporting bullets from
macro_pillars.fed_odds.contracts (cum bp priced through following meetings)
and curve moves (par_curve change_bp). Attribution tag: (fed funds futures).
Do NOT invent a prior if sources.prior_odds is absent.""")
    parts.append("""\

### Forward Catalysts (ON — always present)
From economic_events.upcoming plus macro_pillars.fomc_calendar and
macro_pillars.auction_events.upcoming: a date-grouped list covering the next
~7 days. Each line: date — event — time if present — forecast vs previous
when both exist — one short relevance note ONLY for the biggest items.
US impact-3 events first; compress impact-2 lines; keep non-US to the
notable few. Attribution: (economic calendar) / (Fed calendar) /
(auction calendar). Add nowcast lines ONLY from macro_pillars.nowcasts.""")
    return "\n".join(parts)


SECTION_SPECS: Dict[str, Dict[str, Any]] = {
    "narrative": {
        "output_file": "sections/01_narrative.md",
        "agent": "overall_market",   # 与 drivers 同一 agent 一次产出(防重复)
        "task": NARRATIVE_TASK,
    },
    "drivers": {
        "output_file": "sections/02_drivers.md",
        "agent": "overall_market",
        "task": DRIVERS_TASK,
    },
    "positioning": {
        "output_file": "sections/03_positioning.md",
        "task": POSITIONING_TASK,
    },
    "sector_action": {
        "output_file": "sections/04_sector_action.md",
        "task": SECTOR_ACTION_TASK,
    },
    "movers": {
        "output_file": "sections/05_movers.md",
        "mode": "deterministic",  # 编排器渲染,无 LLM
    },
    "deep_dives": {
        "output_file": "sections/06_deep_dives.md",
        "task": DEEP_DIVES_TASK,
    },
    "macro": {
        "output_file": "sections/07_macro.md",
        "task": None,  # 由 _macro_task(blocks) 动态生成
    },
}

SECTION_ORDER = ["narrative", "drivers", "positioning", "sector_action",
                 "movers", "deep_dives", "macro"]

# agent 执行顺序(narrative+drivers 合并为 overall_market 一次调用)
AGENT_ORDER = ["overall_market", "positioning", "sector_action", "movers",
               "deep_dives", "macro"]

# fake 模式占位文案(结构上必须过 lint:narrative 带 HEADLINE_CANDIDATES)
_FAKE_SECTIONS = {
    "narrative": (
        "## Today's Narrative\n"
        "- Placeholder narrative bullet (fake mode). "
        "<!-- audit: [show the_close] -->\n"
        "<!-- HEADLINE_CANDIDATES\n"
        '"Placeholder headline clause. Other clause."\n'
        "-->\n"
    ),
    "drivers": (
        "## Top Market Drivers\n"
        "- **Placeholder driver.** Placeholder fact as told by The Close "
        "(fake mode value). <!-- audit: [show the_close] -->\n"
        "- **Take:** placeholder take (fake mode).\n"
    ),
    "positioning": (
        "## Positioning / Flow\n"
        "- **Primary positioning:** placeholder (fake mode). "
        "<!-- audit: [show the_close] -->\n"
    ),
    "sector_action": (
        "## Sector Action\n"
        "- Placeholder sector action (fake mode). "
        "<!-- audit: [show cnbc_closing_bell] -->\n"
    ),
    "deep_dives": (
        "## Company Deep Dives\n"
        "### $FAKE — Placeholder Corp\n"
        "- **What happened:** placeholder (fake mode). "
        "<!-- audit: [show cnbc_closing_bell] -->\n"
    ),
    "macro": (
        "## Macros\n"
        "### Rate Expectations\n"
        "- Placeholder rate expectations (fake mode). "
        "<!-- audit: [data macro_pillars.fed_odds] -->\n"
        "### Forward Catalysts\n"
        "- Placeholder catalyst (fake mode). "
        "<!-- audit: [data economic_events.upcoming] -->\n"
    ),
}


def fake_section_text(name: str, corrected: bool = False,
                      spx_move: str = "+9.99%") -> str:
    return _FAKE_SECTIONS[name]


def build_section_prompt(spec: dict, style: str = "en",
                         blocks: Optional[Dict[str, Any]] = None) -> str:
    if spec.get("mode") == "deterministic":
        raise ValueError("deterministic sections have no prompt")
    task = spec["task"]
    if task is None:  # 组执行章节(narrative/drivers)或 macro 动态任务书
        raise ValueError("section has no standalone task — use build_agent_prompt")
    if spec is SECTION_SPECS["macro"]:
        task = _macro_task(blocks or {})
    name = next(n for n, s in SECTION_SPECS.items() if s is spec)
    allowed = SECTION_SOURCE_MATRIX.get(name)
    p = STYLE_PROFILES[style]
    task = task.replace("{missing_mark}", p["missing_mark"])
    return (f"{_common_rules(style, allowed_tags=allowed)}\n{task}\n\n"
            f"Output file: {spec['output_file']}\n")


def agent_section_names(agent: str) -> list:
    """该 agent 负责的章节文件(按 SECTION_ORDER 顺序)。"""
    return [n for n in SECTION_ORDER if SECTION_SPECS[n].get("agent", n) == agent]


def build_agent_prompt(agent: str, style: str = "en",
                       blocks: Optional[Dict[str, Any]] = None) -> str:
    """按 agent 组构建任务书。overall_market = narrative+drivers 一次分析两文件;
    其余 agent 单章节。归因白名单 = 组内各文件矩阵的并集。"""
    names = agent_section_names(agent)
    if agent == "overall_market":
        task = OVERALL_MARKET_TASK
        out_line = ("Output files: exactly " + " and ".join(
            SECTION_SPECS[n]["output_file"] for n in names) +
            " — write BOTH files in this one run.")
    else:
        spec = SECTION_SPECS[names[0]]
        if spec.get("mode") == "deterministic":
            raise ValueError("deterministic sections have no prompt")
        task = spec["task"]
        if spec is SECTION_SPECS["macro"]:
            task = _macro_task(blocks or {})
        out_line = f"Output file: {spec['output_file']}"
    allowed = set()
    for n in names:
        allowed |= SECTION_SOURCE_MATRIX.get(n, set())
    p = STYLE_PROFILES[style]
    task = task.replace("{missing_mark}", p["missing_mark"])
    return (f"{_common_rules(style, allowed_tags=allowed or None)}\n{task}\n\n"
            f"{out_line}\n")


# ── 核验任务书 ─────────────────────────────────────────────────────────

def build_verifier_prompt(style: str = "en") -> str:
    p = STYLE_PROFILES[style]
    attrib_list = "\n   ".join(f"{k} → {v}" for k, v in ATTRIBUTION_MAP.items())
    matrix_lines = "\n   ".join(
        f"{sec}: {', '.join(sorted(tags)) or '(none)'}"
        for sec, tags in SECTION_SOURCE_MATRIX.items())
    return f"""\
You are the verification agent of a scheduled report pipeline. You have Bash.
Your job has two layers:

LAYER 1 — Deterministic audit (primary). Read every file in sections/*.md and
snapshot.json, then write verify/check.py and RUN it (python3 verify/check.py).
check.py must implement ALL of the following and print a JSON array:
[{{"section","claim","snapshot_value","match": true|false,"severity"}}]

1. NUMBER AUDIT — extract numeric claims (percentages, prices, levels, counts,
   bp) and check each against snapshot.json. Tolerance: 0.05 absolute or 0.1
   relative, whichever is larger. Untraceable hard numbers = high.
2. TRIPLE AUDIT — every "actual (est: X, prior: Y)" shape must match the
   economic_events / macro_pillars fields exactly. Mismatched triple = high.
3. SINCE-CLAIM AUDIT — every "highest/lowest since <date>" in the text must
   equal a macro_pillars.fred_ranks.*.claim (or be derived verbatim from it).
   A since-claim not backed by fred_ranks = high.
4. ODDS AUDIT — hike-odds percentages must match macro_pillars.next_fomc_odds
   / fed_odds within ±2 percentage points; a "(prior: ...)" must match
   sources.prior_odds if present, and must NOT exist if prior_odds is absent
   (fabricated prior = high).
5. ATTRIBUTION + SOURCE-MATRIX AUDIT — every parenthetical source tag in the
   prose must be one of the house tags AND must be permitted for that section
   by the source matrix (violation = high — this is the核心纪律: each section
   draws only from its assigned source):
   {matrix_lines}
   Additionally a tag must point at a NON-EMPTY snapshot source:
   {attrib_list}
   A tag pointing at an empty/missing source = high. An unrecognized tag = medium.
6. AUDIT-COMMENT AUDIT — `<!-- audit: [data ...] -->` must reference keys that
   exist in the snapshot's structured parts AND be permitted for that section
   by the matrix; `<!-- audit: [show ...] -->` must reference the_close /
   cnbc_closing_bell as allowed by the matrix. Wrong layer or wrong section
   = high. Factual bullets missing an audit comment = medium.
7. 不可得 AUDIT — every {p['missing_mark']} must correspond to a real gap in
   that section's assigned source; a fake one used to dodge available assigned-
   source data = high. A section writing about a source flagged in
   snapshot.errors (when that source is in its matrix) without the mark = medium.
8. STRUCTURE LINT — (a) each section file starts with its exact heading
   (## Today's Narrative / ## Top Market Drivers / ## Positioning / Flow /
   ## Sector Action / ## Company Deep Dives / ## Macros); (b) narrative file
   contains a <!-- HEADLINE_CANDIDATES ... --> block with >=1 quoted line;
   (c) every drivers block ends with a **Take:** line; (d) deep dives use the
   template where present fields are exactly What happened / Key numbers /
   Management said / Analyst reaction / Bull case / Bear case —
   "Management said"/"Analyst reaction" may be absent entirely but must never
   be present with paraphrase-as-quote; (e) sections/05_movers.md was rendered
   deterministically — only check its numbers against market.top_movers;
   (f) the Macros file must contain exactly the blocks the snapshot supports:
   Releases only if some recent event has an actual, auctions only if
   macro_pillars.auction_events has entries, Rate Expectations only if
   macro_pillars.fed_odds exists, Forward Catalysts always. Violations = medium
   (structural) or high (missing Forward Catalysts).
9. CAUSAL DISCIPLINE — an unlabeled causal conclusion resting on a single show
   tag with no second anchor = medium.

LAYER 2 — Narrative quality review ({p['quality_review_language']}):
- Contradictions between sections or between a section and the data.
- Unsupported causal claims presented as facts.
- House-style violations that matter: vague quantifiers without numbers,
  constructed quotes, yield moves stated in % instead of bp.
- Missing sections, empty files, missing <!-- SECTION DONE --> markers.
high-severity narrative problems (fabricated facts, wrong direction of a move)
fail the verdict.

OUTPUT CONTRACT — write verify/verdict.json (this file is what the orchestrator
reads; your chat reply is ignored):
{{
  "verdict": "pass" | "fail",
  "summary": "one-paragraph audit summary",
  "issues": [
    {{"section": "narrative|drivers|positioning|sector_action|movers|deep_dives|macro",
     "claim": "what the section says",
     "snapshot_value": "what the snapshot says (or 'absent')",
     "severity": "high|medium|low",
     "suggestion": "concrete fix"}}
  ],
  "quality_notes": ["..."]
}}
Rules: an empty issues array with verdict "pass" is valid. Do not modify
sections/, and do NOT touch state.json — the orchestrator owns it. Keep
check.py on disk for the audit trail.

Your task now: verify sections/*.md in this working directory against
snapshot.json, then write verify/verdict.json.

Output file: verify/verdict.json
"""
