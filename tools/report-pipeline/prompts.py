# -*- coding: utf-8 -*-
"""章节任务书与核验任务书(GuanHe 风格 Daily Market Brief,v7)。

v7 对齐 CNT 参考稿的文风(2026-11 评估):
  - 正文只给原始出处(Reuters/FT/CNBC/券商…),管线标签不进正文;
    可追溯性全部落在 audit 注释 + claims 台账
  - 正文不得暴露生产过程(snapshot/transcript/on air/节目名/不可得)
  - 数字: 一位小数、动词后不带符号、$25bn 写法、同一事实一个数
  - Narrative = 2–3 句纯论点; Drivers 每块 ≤3 条 + 一句行动建议
  - Sector Action / Deep Dives 按市场重要性选题,节目只是素材之一
  - Deep Dives = What happened / 标签式要点 / Management said /
    Analyst reaction(券商·评级·目标价) / 可选 Bear case
  - Macros: Releases 仅美国一线白名单; Rate Expectations/跨资产仅极端行情;
    Forward Catalysts 恒在、只收未发生事件
  - 数字对账由 verify_checks.py 执行; ZCode 核验只做叙事质量审查

写作按信源分三组并行:overall_market(narrative/drivers/positioning)、
company_desk(sector_action/deep_dives)、macro(日历 + macro_pillars)。
Today's Movers 由编排器从 S&P 500 成分股快照确定性渲染,不经 LLM。
"""

from typing import Any, Dict, Optional

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
}

# 正文禁用的管线标签(读者看到的出处只能是原始发布方/机构)
PIPELINE_TAGS = tuple(k for k in ATTRIBUTION_MAP if k != "(FRED)" and k != "(Finviz)")

# 章节 × 信源矩阵(v7: 节目转录为叙事素材; news flash/wire 补事件选题与
# 分析师评级; drivers 双引擎 = 节目块 + 新闻事件块; deep_dives 按市场重要性选题)
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
                      "(news flash)", "(newswire)"},
    "movers": {"(Finviz)", "(earnings calendar)"},
    "deep_dives": {"(Closing Bell)", "(The Close)", "(Finviz)",
                   "(earnings calendar)", "(news flash)", "(newswire)"},
    "macro": {"(economic calendar)", "(Treasury curve)", "(FRED)", "(cross-asset)",
              "(fed funds futures)", "(Fed calendar)", "(auction calendar)",
              "(nowcasts)", "(The Close)",
              "(earnings calendar)", "(news flash)", "(newswire)"},
}


def _style_rules(style: str) -> str:
    p = STYLE_PROFILES[style]
    headline = f"\n   {p['headline_rule']}" if p.get("headline_rule") else ""
    voice = ("\n   中文正文同样不得出现:快照、转录、节目、主持人、不可得、口径、数据层。"
             if style == "cn" else "")
    return f"""\
4. Output language: {p['body_language']}. Keep section headings in English exactly
   as specified (house style). Keep tickers ($MU, CCI), company/person names
   and all numbers in their original form.{headline}{voice}
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
section of a "Daily market brief" for professional investors. Write like a senior
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
   ({{time_et, author, text, tickers}}); sources.news.wire[] are full-text
   articles (Yahoo Finance / Seeking Alpha); sources.news.calls[] are
   earnings-call insight excerpts carrying VERBATIM management quotes.
   When the same number exists in the structured data layer (market,
   earnings_calendar, economic_events, macro_pillars), use that value
   instead of the show/news text.
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
     calendar), (auction calendar), (nowcasts), (news flash), (newswire).
   - Judgment is welcome in the analyst's voice (the action bullet of a
     block, a "so what" clause), but never dress an inference up as a
     reported fact.
8. AUDIT TRAIL (hidden layer, mandatory — this is where traceability lives):
   append an HTML comment at the end of each factual bullet carrying the
   precise snapshot key(s), e.g. `<!-- audit: [show the_close] -->`,
   `<!-- audit: [data market.cross_asset] [news flash] -->`. Stripped at
   publish time into an audit sidecar; the verifier reads them.
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
   {{"value": "62%", "path": "<snapshot path>", "text": "<≤12-word fragment>"}}
   - `path` is a dot path into snapshot.json `sources` (omit the `sources.`
     prefix). Select list items with [field=value], e.g.
     macro_pillars.fed_odds.contracts[month=2026-12].odds_of_25bp_hike_pct,
     market.cross_asset[code=GSPC.INDX].pct_change_1d,
     market.sp500_constituents[ticker=INTC].change_percent,
     economic_events.recent[event=Fed Interest Rate Decision].actual,
     macro_pillars.par_curve.change_bp.10y
   - Numbers from a show: path = the_close.clean_transcript,
     cnbc_closing_bell.clean_transcript or cnbc_closing_bell.clean_transcript_ot;
     from news: path = news.flash, news.wire or news.calls. The digits must
     appear in that text ("$25bn" is fine when the text says "25 billion").
   - `value` exactly as written in the prose. Rounding a data value to one
     decimal is fine (the checker tolerates it).
   A script checks every entry against the snapshot; an entry whose number is
   not at its path fails the section. Write the ledger AFTER the prose so it
   matches the final text; update it whenever you revise.
"""


# ── 章节任务书 ─────────────────────────────────────────────────────────

_POSITIONING_BODY = """\
Write "## Positioning / Flow" with up to three items, in this order; drop an
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
## Part 1 — Today's Narrative + Top Market Drivers + Positioning
(one analysis, three files)

Read The Close transcript and the trading-day news ONCE, decide the day's 3–5
real stories, then export THREE files. They complement, never repeat: the
narrative states the thesis, the drivers prove it, positioning shows how money
is reacting.

### File A — sections/01_narrative.md  (THESIS layer)

Write "## Today's Narrative" with EXACTLY 2–3 bullets. Each bullet is ONE plain
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

Write "## Top Market Drivers" with 3–5 blocks, ordered by market impact. A
block may come from The Close (desk segments, guest interviews, data reads) or
from market-moving trading-day news in news.flash / news.wire that the show
missed (M&A, geopolitics, central-bank or government headlines, credit
events). Each block:
- A bold one-clause headline that states the conclusion
  (e.g. **The build-out is moving onto borrowed money.**).
- 1–3 fact bullets. Pack related facts into one sentence; each bullet ≤ 45
  words; numbers per rule 9; at most one credit per bullet.
- A final action bullet (no label): one sentence, ≤ 30 words, naming a
  concrete positioning move AND what would change it — an event, a date, a
  level (e.g. "Avoid PC and chip suppliers exposed to memory costs until Apple
  (Monday) and Qualcomm (Wednesday) show how far the squeeze reaches.").
Do not restate the same stock move in two blocks.

### File C — sections/03_positioning.md

""" + _POSITIONING_BODY + """
ALL THREE files end with the <!-- SECTION DONE --> marker on their own last line.
Each file also gets its own claims ledger (rule 11).
"""

NARRATIVE_TASK = None
DRIVERS_TASK = None

POSITIONING_TASK = "## Section 3 — Positioning / Flow\n\n" + _POSITIONING_BODY

SECTOR_ACTION_TASK = """\
## Section 4 — Sector Action

Write "## Sector Action" as 5–7 thematic blocks (NO sector scorecard / league
table, no sector-average lines). Choose themes by MARKET IMPACT: the biggest
same-day moves among large caps (market.sp500_constituents / top_movers) and
the catalysts behind them. The Closing Bell transcripts (main hour
clean_transcript + Overtime clean_transcript_ot) supply catalysts, color and
named guest/analyst views; news.flash / news.wire supply catalysts the show
missed (reports, downgrades, guidance, deals). A big mover the show skipped is
folded into the theme it belongs to — never a separate "not covered" block.
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
## Section 6 — Company Deep Dives

Write "## Company Deep Dives" with the 3–5 most market-relevant single-name
stories of the day, ranked by: size of the move × market cap, an earnings or
guidance event, an analyst-rating wave, and how much the story drove peers.
How long the show talked about a name is only a tie-break. A deep dive may be a
non-listed company when its news moved the market (e.g. a private company's
IPO, rating or financing). Every subject must also appear in Sector Action.

Sources: Closing Bell / The Close transcripts, earnings_calendar,
market.sp500_constituents (price, change), news.flash (rating changes, target
changes, guidance headlines, reports), news.wire and news.calls (results
detail, verbatim management quotes).

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
  a transcript, news.calls or news.wire. Omit the line if none exists.
- **Analyst reaction:** bullets in the form "Firm (Rating): target $X from
  $Y; one-clause reason." taken from news items or named analysts in the
  transcripts. Up to 5, most influential first. Omit if none exists.
- **Bear case:** optional, only when named analysts/strategists argue the
  other side; same bullet form.

Never fabricate estimates, guidance, ratings, targets or quotes.
"""

COMPANY_DESK_TASK = """\
## Part 2 — Sector Action + Company Deep Dives (one pass, two files)

Read the Closing Bell transcripts (main hour + Overtime), the large-cap moves
and the trading-day news ONCE, then export TWO files. Deep Dives subjects MUST
also appear in Sector Action. (Today's Movers is rendered by the pipeline
from market data — do not write it.)

### File A — sections/04_sector_action.md

""" + SECTOR_ACTION_TASK + """
### File B — sections/06_deep_dives.md

""" + DEEP_DIVES_TASK + """
BOTH files end with the <!-- SECTION DONE --> marker on their own last line.
Each file also gets its own claims ledger (rule 11).
"""


def _macro_task(blocks: Dict[str, Any]) -> str:
    """MACROS 任务书按块矩阵动态拼装(GuanHe 现行版式 2026-10-12+)。

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
transcript only. Central-bank / government / policy headlines may come from
news.flash / news.wire. Rates, curve moves and odds already discussed in Top
Market Drivers are not repeated here."""]

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
- 3–5 detail bullets: subreads / components / revisions / "highest since" ONLY
  when backed by macro_pillars.fred_ranks.*.claim or release_series.
- 1–3 **Sell-Side Reaction** bullets: named economist + firm + view, ONLY if
  present in The Close transcript. If no quote exists, omit them.""")
    else:
        parts.append("""\

### Releases (OFF)
No T-day US tier-1 release with an actual is in the snapshot — do NOT write a
Releases block at all today.""")

    if blocks.get("rate_expectations"):
        parts.append("""\

### Rate Expectations (ON — extreme odds/yield move today)
Write "#### Rate Expectations · <next FOMC>" with:
- Headline from macro_pillars.next_fomc_odds: odds of a hike at the next
  meeting, with the prior comparison ONLY if sources.prior_odds exists —
  house style `Hike Odds ~20% (prior: ~24%)`.
- 1–3 supporting bullets from fed_odds.contracts (cum bp) and par_curve
  change_bp.
- Optional guest reaction bullet from The Close transcript only.
Do NOT invent a prior if sources.prior_odds is absent.""")
    else:
        parts.append("""\

### Rate Expectations (OFF)
No extreme odds/yield move — do NOT write a Rate Expectations block. Put the
next-meeting hike odds in half a sentence on the Forward Catalysts line of the
next key data print or FOMC date (e.g. "December is ~74% priced for a hike").""")

    if blocks.get("cross_asset_review"):
        parts.append("""\

### Treasuries / Cross-Asset review (ON — extreme move day)
Write one short subsection (e.g. "#### Treasuries and oil") covering curve
moves, oil if in cross_asset, and HY/IG OAS from fred_extras — only what Top
Market Drivers did not already say. Guest quotes optional from The Close
transcript only.""")
    else:
        parts.append("""\

### Treasuries / Cross-Asset review (OFF)
Do NOT write a Treasuries or Cross-Asset review subsection today.""")

    parts.append("""\

### Forward Catalysts (ON — always present)
Write "#### Forward Catalysts", then one bold date line per day
("**Mon Nov 2**", weekend days grouped as "**Sat Oct 31 – Sun Nov 1**"), each
followed by bullets. Cover the next ~7 days. Format and content:
- Data: "ISM manufacturing (Oct), 10:00 ET — 54.7 est vs 54.5." Give est vs
  previous when both exist; add key sub-components (prices paid, new orders)
  only when listed in economic_events. US impact-3 first, then US impact-2.
- Only events that have NOT happened yet: skip any row that already carries an
  actual. Non-US data is skipped except central-bank decisions.
- Fed: scheduled speakers and FOMC dates (macro_pillars.fomc_calendar), with
  the hike odds in half a sentence where relevant.
- Treasury: auctions/refunding from macro_pillars.auction_events (size, prior
  stop, today's close of that tenor from par_curve). If there is none, write
  nothing about auctions.
- Policy / geopolitics / company events scheduled in the coming days that
  news.flash / news.wire name explicitly (elections, OPEC+ meetings, deadlines,
  hearings, promised disclosures) — one line each.
- Earnings: one line per session — "$MAR before the open; $AAPL, $PLTR and
  $VRTX after the close." Large caps only. When the company (or a direct
  peer) moved ≥5% today, add today's move: "$AMD (−10.9% Friday)".
- Nowcasts ONLY from macro_pillars.nowcasts.""")
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
        "agent": "movers",
        "mode": "deterministic",
        "task": None,
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
        "**Placeholder driver.**\n"
        "- Index moved {spx_move} "
        "(fake mode value — deterministic check must flag this). "
        "<!-- audit: [show the_close] -->\n"
        "- Placeholder action (fake mode).\n"
    ),
    "positioning": (
        "## Positioning / Flow\n"
        "- **Primary — Placeholder.** Placeholder (fake mode). "
        "<!-- audit: [show the_close] -->\n"
    ),
    "sector_action": (
        "## Sector Action\n"
        "**Placeholder theme.**\n"
        "- Placeholder sector action (fake mode). "
        "<!-- audit: [show cnbc_closing_bell] -->\n"
    ),
    "deep_dives": (
        "## Company Deep Dives\n"
        "### FAKE — Placeholder Corp\n"
        "- **What happened:** placeholder (fake mode). "
        "<!-- audit: [show cnbc_closing_bell] -->\n"
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
- The same fact stated differently in different places: a superlative
  ("worst day ever" vs "since 1974" vs "in four decades"), a market-cap or
  dollar figure, or a % move quoted with two different values.
- Speculation stated as reported fact.
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
