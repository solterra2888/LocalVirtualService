# -*- coding: utf-8 -*-
"""章节任务书与核验任务书(CNT 风格 Daily Market Brief,v4)。

v4 对齐 CNT 现行版式(2026-10-12 之后):
  - Drivers/Positioning 允许结构化数字锚(曲线/fed_odds/跨资产)
  - Sector Action = 主题块(无板块记分牌)
  - Deep Dives = What happened / Key numbers / Management said / Analyst reaction
  - Macros: Releases 仅美国一线白名单; Rate Expectations/跨资产仅周五或极端行情;
    Forward Catalysts 恒在; 拍卖并入 Forward Catalysts
  - Macros 反应引语只引 The Close 转录(不用 Surveillance)
  - 数字对账由仓库内 verify_checks.py 执行; ZCode 核验只做叙事质量审查

写作按信源分三组并行:overall_market(The Close 转录: narrative/drivers/
positioning)、company_desk(Closing Bell 主段+Overtime 转录: sector_action/
movers/deep_dives)、macro(日历 + macro_pillars)。
Today's Movers 不走 Finviz 全市场榜,由 company_desk 从 Closing Bell 转录的
节目点名抽出(Winners / Losers / After-Hours),走台账核验。
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
        "headline_rule": "HEADLINE_CANDIDATES must be written in Simplified "
                         "Chinese (两句式中文标题; tickers/names may stay in "
                         "original form).",
        "quality_review_language": "Review the Chinese text; write verdict.json "
                                   "summary/issues in Chinese (JSON keys and enum "
                                   "values stay English).",
    },
}

# 括号归因词 → snapshot 源(核验据此做归因映射审计)
ATTRIBUTION_MAP = {
    "(The Close)": "the_close.clean_transcript",
    "(Closing Bell)": "cnbc_closing_bell.clean_transcript / clean_transcript_ot",
    "(cross-asset)": "market.cross_asset",
    "(sector averages)": "market.sp500_sector_avg",
    "(breadth)": "market.breadth",
    "(Finviz)": "market.sp500_constituents / market.top_movers",
    "(earnings calendar)": "earnings_calendar",
    "(economic calendar)": "economic_events",
    "(Treasury curve)": "macro_pillars.par_curve",
    "(FRED)": "macro_pillars.fred_ranks / fred_extras / release_series",
    "(fed funds futures)": "macro_pillars.fed_odds",
    "(Fed calendar)": "macro_pillars.fomc_calendar",
    "(auction calendar)": "macro_pillars.auction_events",
    "(nowcasts)": "macro_pillars.nowcasts",
}

# 章节 × 信源矩阵(v4: Drivers/Positioning 可带数字锚; macro 可引节目反应)
SECTION_SOURCE_MATRIX = {
    "narrative": {"(The Close)", "(cross-asset)"},
    "drivers": {"(The Close)", "(cross-asset)", "(Treasury curve)",
                "(fed funds futures)", "(FRED)", "(breadth)"},
    "positioning": {"(The Close)", "(cross-asset)", "(breadth)",
                    "(Treasury curve)", "(fed funds futures)", "(Finviz)"},
    "sector_action": {"(Closing Bell)", "(sector averages)", "(Finviz)",
                      "(breadth)", "(earnings calendar)"},
    "movers": {"(Closing Bell)", "(Finviz)"},
    "deep_dives": {"(Closing Bell)", "(Finviz)", "(earnings calendar)"},
    "macro": {"(economic calendar)", "(Treasury curve)", "(FRED)",
              "(fed funds futures)", "(Fed calendar)", "(auction calendar)",
              "(nowcasts)", "(The Close)",
              "(earnings calendar)"},
}


def _style_rules(style: str) -> str:
    p = STYLE_PROFILES[style]
    headline = f"\n   {p['headline_rule']}" if p.get("headline_rule") else ""
    return f"""\
4. Output language: {p['body_language']}. Keep section headings in English exactly
   as specified (house style). Keep tickers ($MU, CCI), company/person names, show
   names, and all numbers in their original form.{headline}
"""


def _common_rules(style: str, allowed_tags: Optional[set] = None) -> str:
    p = STYLE_PROFILES[style]
    attrib_list = "\n   ".join(f"{k} → {v}" for k, v in ATTRIBUTION_MAP.items())
    tag_line = ""
    if allowed_tags is not None:
        tags = ", ".join(sorted(allowed_tags)) or "(none)"
        tag_line = f"""
   SOURCE RESTRICTION for THIS section: the ONLY permitted attribution tags are
   {tags}. Narrative/quotes come from show transcripts when tagged with a show
   tag; numbers may be anchored to structured sources listed above. If your
   assigned source lacks a number or fact you want to state, mark it {p['missing_mark']}
   — do not borrow from a source outside this whitelist.
"""
    return f"""\
You are one section-writer inside a scheduled report pipeline. You are writing ONE
section of a "Daily market brief" in the style of a professional financial newsletter
(think: concise, data-anchored, every claim traceable — house style mirrors the CNT
daily brief: dense bullets, bold lead-ins, parenthetical source attributions).

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
2. Every number you state must exist in snapshot.json. If absent, omit the
   sentence or mark {p['missing_mark']}.
3. Write the section to the exact output file specified below, as Markdown,
   plus its claims ledger (rule 11). Do not create any other files.
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
   - Fed odds: "hike odds at the <next meeting>" = macro_pillars.next_fomc_odds
     (computed from the post-meeting month contract). fed_odds.contracts[] are
     MONTHLY AVERAGE contracts — never call a contract month's number "<month>
     meeting odds"; phrase it as "the <month> contract prices ~Nbp".
   - No vague quantifiers ("sharply", "significantly") without a number.
   - Quotes need name + affiliation, verbatim from your assigned show
     transcript; never construct a quote that is not in the snapshot.
10. End the file with a line containing only: <!-- SECTION DONE -->
11. CLAIMS LEDGER (machine-checked; this replaces a separate verification pass):
   next to each section file write `<same name>.claims.json` (e.g.
   sections/02_drivers.md → sections/02_drivers.claims.json) — a JSON array with
   ONE entry per number you stated in that file:
   {{"value": "62%", "path": "<snapshot path>", "text": "<≤12-word fragment>"}}
   - `path` is a dot path into snapshot.json `sources` (omit the `sources.`
     prefix). Select list items with [field=value], e.g.
     macro_pillars.fed_odds.contracts[month=2026-12].odds_of_25bp_hike_pct,
     market.cross_asset[code=GSPC.INDX].pct_change_1d,
     economic_events.recent[event=Fed Interest Rate Decision].actual,
     macro_pillars.par_curve.change_bp.10y
   - Numbers quoted from a show: path = the_close.clean_transcript,
     cnbc_closing_bell.clean_transcript or cnbc_closing_bell.clean_transcript_ot
     (spoken numbers; value exactly as spoken, e.g. "48" not "47.6%").
   - `value` exactly as written in the prose (keep %, bp, K, $, sign).
   A script checks every entry against the snapshot; an entry whose number is
   not at its path fails the section. Write the ledger AFTER the prose so it
   matches the final text; update it whenever you revise.
"""


# ── 章节任务书 ─────────────────────────────────────────────────────────

OVERALL_MARKET_TASK = """\
## Part 1 — Today's Narrative + Top Market Drivers (one analysis, two files)

You analyze The Close transcript ONCE and export TWO files. The two parts
complement, never repeat: numbers and depth live in the drivers file; the
narrative file is the scan layer — a market-wrap chyron, not a mini-drivers.

### File A — sections/01_narrative.md  (SCAN layer)

Write "## Today's Narrative" with EXACTLY 3-4 bullets. Rules:
- Each bullet is ONE short sentence: bold lead-in naming ONE dominant story
  from The Close, plus at most ONE number (an index move or the single most
  salient figure — and prefer numbers you did NOT use in the drivers file).
- Index / cross-asset levels may be tagged (cross-asset) when taken from
  market.cross_asset.
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

Write "## Top Market Drivers" with up to 5 driver blocks derived from The Close
(its market-wrap desk segments, guest interviews, and data reads), with
structured numbers as anchors when the show states a level or move that also
appears in the snapshot. Each block:
- A bold lead-in headline (one clause, e.g. **A soft print could not hold the long end.**)
- 2-5 factual bullets with numbers as stated on The Close and/or matching
  snapshot fields (par_curve change_bp, next_fomc_odds, cross_asset, breadth),
  each tagged with the appropriate attribution and carrying an audit comment.
- A final one-line takeaway, exactly: **Take:** <one sentence> — the actionable
  reading of the block (unlabeled interpretation is allowed ONLY on this line;
  everywhere else interpretation needs the read prefix).
Do not exceed 5 blocks; pick the ones The Close actually supports.

### File C — sections/03_positioning.md

Write "## Positioning / Flow" with three labeled items, in this order (skip any
whose material is genuinely absent, marking it {missing_mark} in one short line
instead). Mine the positioning material directly from the The Close transcript
— the guest-strategist segments (asset allocators, fixed-income and credit
views, cross-asset reads, disagreements and time horizons) plus the hosts'
positioning discussion. Structured numbers (breadth, curve, fed_odds, Finviz)
may anchor Primary/Secondary when they corroborate the show:
- **Primary positioning:** the dominant positioning theme of the day (rates,
  equity beta, duration, rotation) with the prescribed response.
- **Secondary positioning:** a second flow/positioning thread (credit, gold,
  FX, sector rotation, buybacks, ETF flows).
- **Guest positioning:** the most decision-relevant strategist/guest call —
  name + firm + the call, verbatim where possible.
Rules: show claims tagged (The Close); structured numbers use their own tags.
Do not repeat driver-block analysis here — this is the positioning/flow cut.

ALL THREE files end with the <!-- SECTION DONE --> marker on their own last line.
Each file also gets its own claims ledger (rule 11).
"""

NARRATIVE_TASK = None
DRIVERS_TASK = None

POSITIONING_TASK = """\
## Section 3 — Positioning / Flow

Write "## Positioning / Flow" with three labeled items, in this order (skip any
whose material is genuinely absent, marking it {missing_mark} in one short line
instead). Mine the positioning material directly from the The Close transcript
— the guest-strategist segments (asset allocators, fixed-income and credit
views, cross-asset reads, disagreements and time horizons) plus the hosts'
positioning discussion. Structured numbers (breadth, curve, fed_odds, Finviz)
may anchor Primary/Secondary when they corroborate the show:
- **Primary positioning:** the dominant positioning theme of the day (rates,
  equity beta, duration, rotation) with the prescribed response.
- **Secondary positioning:** a second flow/positioning thread (credit, gold,
  FX, sector rotation, buybacks, ETF flows).
- **Guest positioning:** the most decision-relevant strategist/guest call —
  name + firm + the call, verbatim where possible.
Rules: show claims tagged (The Close); structured numbers use their own tags.
"""

SECTOR_ACTION_TASK = """\
## Section 4 — Sector Action

Write "## Sector Action" as 4–7 thematic blocks (NO sector scorecard / league
table). Synthesize from the Closing Bell transcripts (MAIN hour clean_transcript
+ OVERTIME hour clean_transcript_ot — after-close earnings coverage lives in
Overtime); use market.sp500_constituents / top_movers / breadth /
earnings_calendar only as number anchors for named tickers.
Each block:
- A bold lead-in naming the theme (e.g. **Funding, not demand, sank AI hardware.**)
- 2–4 bullets with company moves and catalysts, tagged (Closing Bell) and/or
  (Finviz)/(earnings calendar) for numbers.
- Optional one-line takeaway unlabeled.
Every ticker mentioned must appear in Closing Bell, sp500_constituents, or
top_movers. The overall index/tape recap is NOT part of this section
(it lives in Today's Narrative).
"""

DEEP_DIVES_TASK = """\
## Section 6 — Company Deep Dives

Write "## Company Deep Dives". Pick the 3-5 companies most prominently discussed
across the Closing Bell transcripts (main + Overtime; tie-break: appearance in
market.top_movers / sp500_constituents / earnings_calendar.today /
earnings_calendar.prior_after_close; the earnings calendar is pre-filtered to US
large caps, fields: code, session, eps_actual, eps_estimate, eps_surprise_pct).
For each company write a subsection headed by its ticker:

### $TICKER — Company Name

- **What happened:** the event/coverage as told by Closing Bell, with the price
  move if in the snapshot.
- **Key numbers:** only numbers present in the Closing Bell transcripts,
  earnings_calendar, or Finviz fields (with attribution tags and audit
  comments); if none, write {missing_mark}.
- **Management said:** a verbatim quote from an executive, ONLY if a direct
  quote exists in the Closing Bell transcripts (name + title + the quote; the
  Overtime hour carries most after-close management comments). If none,
  OMIT this line entirely — do not paraphrase into a quote.
- **Analyst reaction:** analyst/strategist reactions ONLY if present in the
  Closing Bell transcripts (name + firm + view). Same omission rule.

Do NOT write Bull case / Bear case labels. Do NOT write a Read: synthesis line.
STRICT: do not add companies not discussed by Closing Bell; do not fabricate
estimates, EPS, guidance numbers, or quotes; do not pull material from The Close.
"""

MOVERS_TASK = """\
## Section 5 — Today's Movers

Write "## Today's Movers" as the show's named-movers tables, built from the
Closing Bell transcripts (main hour + Overtime after-hours block) — NOT a
Finviz ranked tape. Exactly three Markdown tables, in this order:

### Winners
### Losers
### After-Hours Movers

Columns for Winners/Losers: `| Ticker | Company | Change | Catalyst |`;
for After-Hours: `| Ticker | Company | Move | Catalyst |`.

- Include ONLY companies the shows actually named as notable movers, with the
  catalyst as stated on air (earnings beat/miss, guidance, deal, downgrade…).
  2–8 rows per table; a table with no names in the transcripts gets a single
  {missing_mark} line instead of an empty table.
- Change/Move: prefer the show's spoken number tagged (Closing Bell); a Finviz
  close change may anchor it tagged (Finviz). If neither quantifies it, write
  "未提及" / "not stated" in the cell — never invent a number.
- After-Hours rows come from the Overtime hour; note the move as "AH -x%" style.
- One intro line above the tables noting these are show-named movers
  (not a full-market ranking).
Every number goes into the claims ledger (rule 11).
"""

COMPANY_DESK_TASK = """\
## Part 2 — Sector Action + Today's Movers + Company Deep Dives
(one Closing Bell pass, three files)

You analyze the Closing Bell transcripts ONCE (main hour + Overtime) and
export THREE files. Deep Dives companies MUST be a subset of the names that
appear in Sector Action — pick the 3–5 most prominently discussed, tie-break
by market.top_movers / sp500_constituents / earnings_calendar. Movers rows
must also be names the shows actually covered.

### File A — sections/04_sector_action.md

""" + SECTOR_ACTION_TASK + """
### File B — sections/05_movers.md

""" + MOVERS_TASK + """
### File C — sections/06_deep_dives.md

""" + DEEP_DIVES_TASK + """
ALL THREE files end with the <!-- SECTION DONE --> marker on their own last
line. Each file also gets its own claims ledger (rule 11).
"""


def _macro_task(blocks: Dict[str, Any]) -> str:
    """MACROS 任务书按块矩阵动态拼装(CNT 现行版式 2026-10-12+)。

    blocks 由编排器从快照确定性判定:
      releases            T 日美国一线白名单事件(最多 2 个)
      rate_expectations   周五或极端行情
      cross_asset_review  同 rate_expectations
      forward             恒 True
      selected_releases   选中的事件列表(供任务书点名)
    """
    parts = ["""\
## Section 7 — Macros

Write "## Macros" with the blocks below, in the order given. A block listed as
OFF must NOT appear in your output at all (its absence is correct house style:
the Macros section flexes with the calendar).

Numbers and calendar facts come from economic_events + macro_pillars +
earnings_calendar. Calendar `time`/`date` are UTC — ALWAYS quote times from the
`time_et` field (US Eastern; use `date_et` when present), never the raw `time`.
Rows with the same event name but a different `comparison` (mom / yoy / level)
are different measures — do not mix their actual/est/prior. Sell-side / guest
reaction quotes for Releases and Rate Expectations may come from The Close
transcript only — tag them (The Close). Do not cite any other show."""]

    selected = blocks.get("selected_releases") or []
    if blocks.get("releases") and selected:
        def _fmt(e: Dict[str, Any]) -> str:
            cmp_ = f" [{e['comparison']}]" if e.get("comparison") else ""
            when = f" @ {e['time_et']} ET" if e.get("time_et") else ""
            s = (f"{e.get('event', '?')}{cmp_}{when} (actual={e.get('actual')}, "
                 f"est={e.get('forecast')}, prior={e.get('previous')})")
            rel = e.get("related") or []
            if rel:
                s += " + related rows: " + ", ".join(
                    f"{r.get('event')}[{r.get('comparison') or 'level'}]"
                    f"={r.get('actual')}" for r in rel)
            return s
        names = "; ".join(_fmt(e) for e in selected)
        parts.append(f"""\

### Releases (ON)
Write "#### Releases" then one "#### <Event Name>" subsection for EACH of these
selected events ONLY (max 2; do not invent others): {names}

For each event:
- **Headline line:** house style `**+29K** (est: +90K, prior: +162K → +133K rev)`
  using economic_events fields (and prior revision only if present in snapshot).
- 4–6 detail bullets: subreads / components / revisions / "highest since" ONLY
  when backed by macro_pillars.fred_ranks.*.claim or release_series. Tag
  (economic calendar) / (FRED).
- 2–4 **Sell-Side Reaction** bullets: named economist + firm + view, ONLY if
  present in The Close transcript (tag (The Close)). If no quote exists, omit the
  reaction bullets rather than inventing them.""")
    else:
        parts.append("""\

### Releases (OFF)
No T-day US tier-1 release with an actual is in the snapshot — do NOT write a
Releases block at all today.""")

    if blocks.get("rate_expectations"):
        parts.append("""\

### Rate Expectations (ON — Friday or extreme move day)
Write "#### Rate Expectations · <next FOMC>" with:
- Headline from macro_pillars.next_fomc_odds: odds of a hike at the next
  meeting, with the prior comparison ONLY if sources.prior_odds exists —
  house style `Hike Odds ~20% (prior: ~24%)`.
- 1–4 supporting bullets from fed_odds.contracts (cum bp) and par_curve
  change_bp. Attribution: (fed funds futures) / (Treasury curve).
- Optional guest reaction bullets from The Close transcript only.
Do NOT invent a prior if sources.prior_odds is absent.""")
    else:
        parts.append("""\

### Rate Expectations (OFF)
Not Friday and no extreme odds/yield move — do NOT write a Rate Expectations
block. Fold any needed odds into Forward Catalysts' FOMC line instead.""")

    if blocks.get("cross_asset_review"):
        parts.append("""\

### Treasuries / Cross-Asset review (ON — Friday or extreme move day)
Write one or two short subsections (e.g. "#### Treasuries and oil" /
"#### Equities and credit") covering curve moves, oil if in cross_asset,
breadth, HY/IG OAS from fred_extras, using only snapshot numbers. Guest
quotes optional from The Close transcript only.""")
    else:
        parts.append("""\

### Treasuries / Cross-Asset review (OFF)
Do NOT write a Treasuries or Cross-Asset review subsection today.""")

    parts.append("""\

### Forward Catalysts (ON — always present)
Write "#### Forward Catalysts" as a date-grouped list covering the next ~7–10
days from economic_events.upcoming, macro_pillars.fomc_calendar,
macro_pillars.auction_events.upcoming, and earnings_calendar.upcoming.
Each line: date — event — time if present — forecast vs previous when both
exist. For Treasury auctions include size / prior stop / today's tenor close
when present in auction_events or par_curve. For earnings include ticker and
before/after market when present. US impact-3 first; compress impact-2;
notable non-US / central-bank / geopolitical items only when in the snapshot.
Attribution: (economic calendar) / (Fed calendar) / (auction calendar) /
(earnings calendar). Add nowcast lines ONLY from macro_pillars.nowcasts.
If next_fomc_odds exists, include a FOMC line with odds and prior when available.""")
    return "\n".join(parts)


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
        "agent": "company_desk",
        "task": MOVERS_TASK,
    },
    "deep_dives": {
        "output_file": "sections/06_deep_dives.md",
        "agent": "company_desk",
        "task": DEEP_DIVES_TASK,
    },
    "macro": {
        "output_file": "sections/07_macro.md",
        "task": None,
    },
}

SECTION_ORDER = ["narrative", "drivers", "positioning", "sector_action",
                 "movers", "deep_dives", "macro"]

AGENT_ORDER = ["overall_market", "company_desk", "movers", "macro"]

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
        "- **Placeholder driver.** Index {spx_move} as told by The Close "
        "(fake mode value — deterministic check must flag this). "
        "<!-- audit: [show the_close] -->\n"
        "- **Take:** placeholder take (fake mode).\n"
    ),
    "positioning": (
        "## Positioning / Flow\n"
        "- **Primary positioning:** placeholder (fake mode). "
        "<!-- audit: [show the_close] -->\n"
    ),
    "sector_action": (
        "## Sector Action\n"
        "- **Placeholder theme.** Placeholder sector action (fake mode). "
        "<!-- audit: [show cnbc_closing_bell] -->\n"
    ),
    "movers": (
        "## Today's Movers\n"
        "Named movers as covered by Closing Bell (fake mode). "
        "<!-- audit: [show cnbc_closing_bell] -->\n"
        "### Winners\n"
        "| Ticker | Company | Change | Catalyst |\n"
        "| --- | --- | --- | --- |\n"
        "| $FAKE | Placeholder Corp | not stated | placeholder beat "
        "<!-- audit: [show cnbc_closing_bell] -->\n"
    ),
    "deep_dives": (
        "## Company Deep Dives\n"
        "### $FAKE — Placeholder Corp\n"
        "- **What happened:** placeholder (fake mode). "
        "<!-- audit: [show cnbc_closing_bell] -->\n"
        "- **Key numbers:** (not in snapshot).\n"
    ),
    "macro": (
        "## Macros\n"
        "### Forward Catalysts\n"
        "- Placeholder catalyst (fake mode). "
        "<!-- audit: [data economic_events.upcoming] -->\n"
    ),
}


def fake_section_text(name: str, corrected: bool = False,
                      spx_move: str = "+9.99%") -> str:
    text = _FAKE_SECTIONS[name].replace("{spx_move}", spx_move)
    if corrected and name == "drivers":
        return (
            "## Top Market Drivers\n"
            "- **Placeholder driver.** Placeholder fact aligned with snapshot "
            "(fake mode corrected). <!-- audit: [show the_close] -->\n"
            "- **Take:** placeholder take (fake mode).\n"
        )
    return text


def build_section_prompt(spec: dict, style: str = "en",
                         blocks: Optional[Dict[str, Any]] = None) -> str:
    if spec.get("mode") == "deterministic":
        raise ValueError("deterministic sections have no prompt")
    task = spec["task"]
    if task is None:
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
    """按信源组构建任务书。归因白名单 = 组内各文件矩阵的并集。不挂转录。"""
    names = agent_section_names(agent)
    if agent == "overall_market":
        task = OVERALL_MARKET_TASK
    elif agent == "company_desk":
        task = COMPANY_DESK_TASK
    else:
        spec = SECTION_SPECS[names[0]]
        if spec.get("mode") == "deterministic":
            raise ValueError("deterministic sections have no prompt")
        task = spec["task"]
        if spec is SECTION_SPECS["macro"]:
            task = _macro_task(blocks or {})
    files = " and ".join(SECTION_SPECS[n]["output_file"] for n in names)
    out_line = (f"Output files: exactly {files} — write each listed file in "
                "this one run (and its .claims.json ledger).")
    allowed = set()
    for n in names:
        allowed |= SECTION_SOURCE_MATRIX.get(n, set())
    p = STYLE_PROFILES[style]
    task = task.replace("{missing_mark}", p["missing_mark"])
    return (f"{_common_rules(style, allowed_tags=allowed or None)}\n{task}\n"
            f"\n{out_line}\n")


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
- Unsupported causal claims presented as facts.
- House-style violations that matter: vague quantifiers without numbers,
  constructed quotes, yield moves stated in % instead of bp.
- Deep Dives must NOT contain Bull case / Bear case labels or a Read: line.
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
    {{"section": "narrative|drivers|positioning|sector_action|movers|deep_dives|macro",
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
- Causal claims stated as fact without a source tag or the read prefix
  "{p['read_prefix']}".
- Quotes without a named speaker and affiliation.
- Vague quantifiers ("sharply", "大幅") with no number attached.
Severity: high = contradiction or wrong direction; everything else medium/low.
Verdict is "fail" only if there is at least one high issue. Be brief.

Write verify/verdict.json:
{{"verdict": "pass" | "fail", "summary": "...",
  "issues": [{{"section": "...", "claim": "...", "snapshot_value": "n/a",
              "severity": "high|medium|low", "suggestion": "..."}}],
  "quality_notes": ["..."]}}
Do not modify any other file.

Output file: verify/verdict.json
"""
