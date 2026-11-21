# -*- coding: utf-8 -*-
"""Stable external JSON schema for Agent / MCP (versioned under /api/v1/agent).

v0.2.0 breaking changes:
- /search removed (merged into /daily with optional q)
- twitter_daily_* / youtube_daily_* removed (use /reports?kind=twitter|youtube)
- New: /daily/batch, /daily/multi, /subscriptions, /sources
- New filter params: tickers (multi-value), tags, author
- since accepts any N[hdw] pattern (e.g. 14d, 2w, 48h)
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field

AgentSource = Literal[
    # Feed sources
    "twitter",
    "youtube",
    "news",
    "zhishixingqiu",
    "substack",
    "weibo",
    # Upload sources — generic group
    "web",
    "audio",
    "text",
    "podcast",
    "image",
    # Upload document formats (was uniformly "pdf" in v0.2; now specific)
    "pdf",    # uploaded PDF
    "word",   # uploaded Word / .doc / .docx
    "excel",  # uploaded Excel / CSV spreadsheet
]

ReportKind = Literal[
    "twitter_daily",
    "youtube_daily",
    "market_daily",
    "market_weekly",
    "theme",
    "the_close",
    "closing_bell",
    "bloomberg_surveillance",
]


class AgentDailyItem(BaseModel):
    id: str
    title: str
    summary: str = Field("", description="AI summary or snippet, trimmed for agents")
    url: Optional[str] = None
    source: AgentSource = Field(
        ...,
        description=(
            "Source type. For uploaded files: 'pdf', 'word', 'excel', 'image', 'audio', 'text', 'podcast', 'web'. "
            "For feeds: 'twitter', 'youtube', 'news', 'zhishixingqiu', 'substack', 'weibo'. "
            "All uploaded document-type files (PDF/Word/Excel/CSV/image) now return their specific format "
            "rather than the legacy 'pdf' catch-all."
        ),
    )
    author: Optional[str] = None
    published_at: datetime
    tickers: List[str] = Field(default_factory=list)
    tags: List[str] = Field(default_factory=list, description="themes / concepts")
    people: List[str] = Field(
        default_factory=list,
        description=(
            "Key persons mentioned (name only). Original-form names from content "
            "(e.g. 'Trump', not '特朗普')."
        ),
    )
    countries: List[str] = Field(
        default_factory=list,
        description="Countries mentioned, full English names (e.g. 'United States').",
    )
    matched_themes: List[Dict[str, Any]] = Field(
        default_factory=list,
        description=(
            "Matched investment themes [{theme_id, theme_name, category, match_mode}]"
        ),
    )


# ---------------------------------------------------------------------------
# Diagnostics (returned when total == 0 on a search/feed query with q set)
# ---------------------------------------------------------------------------

class AgentSearchDiagnostics(BaseModel):
    parsed_q_groups: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="Parsed q groups: [{op: 'and'|'or', terms: [...]}]",
    )
    terms_in_corpus: Dict[str, int] = Field(
        default_factory=dict,
        description="For each query term, how many rows contain it (sampled, max 8 terms, 200 ms budget).",
    )
    since_used: Optional[str] = Field(
        None, description="ISO timestamp of the date_from bound actually applied."
    )
    sources_used: List[str] = Field(
        default_factory=list,
        description="source_subtypes that were queried (after preset expansion).",
    )
    hint: Optional[str] = Field(
        None,
        description="Human-readable hint: which term is over-restrictive or which source has no items.",
    )


# ---------------------------------------------------------------------------
# Feed / search list response
# ---------------------------------------------------------------------------

class AgentDailyFeedResponse(BaseModel):
    items: List[AgentDailyItem]
    next_cursor: Optional[str] = None
    has_more: bool = False
    total: int = 0
    diagnostics: Optional[AgentSearchDiagnostics] = Field(
        None,
        description="Populated only when total == 0 and a q filter was active. "
        "Use to diagnose over-constrained queries.",
    )


# ---------------------------------------------------------------------------
# Item detail
# ---------------------------------------------------------------------------

class AgentDailyItemDetail(AgentDailyItem):
    body_excerpt: Optional[str] = Field(
        None,
        description=(
            "[Deprecated, kept for backward compatibility] Short, lightly-stripped excerpt "
            "of the main body (~4000 chars). Prefer `content` / `transcript` below."
        ),
    )
    content: Optional[str] = Field(
        None,
        description=(
            "Main body for the item, suitable for LLM consumption. "
            "Mapping by source: "
            "upload (pdf/word/web/text/image/audio/youtube/podcast) → "
            "file_processing_details.content (Markdown / OCR / web parse / notes); "
            "twitter / news → subscription_content.content; "
            "youtube (feed) → subscription_content.content (the YouTube description). "
            "Truncated when very long (see `content_truncated`)."
        ),
    )
    content_truncated: bool = Field(
        False,
        description="True when `content` was truncated; open `url` for the full original.",
    )
    transcript: Optional[str] = Field(
        None,
        description=(
            "Plain-text transcript / spoken-word body. "
            "youtube_feed → subscription_content.transcript_cleaned (fallback transcript). "
            "Upload youtube / audio / podcast → flattened text from "
            "file_processing_details.asr_with_diarization. "
            "Null for items that don't have a transcript (e.g. tweets, PDFs)."
        ),
    )
    transcript_truncated: bool = Field(
        False,
        description="True when `transcript` was truncated; open `url` for the full original.",
    )
    people_detail: List[Dict[str, Any]] = Field(
        default_factory=list,
        description=(
            "Detail-level people tags: [{name, name_en, role, affiliation?}]. "
            "List-level `people` on the parent item remains name-only strings."
        ),
    )


# ---------------------------------------------------------------------------
# Batch detail response
# ---------------------------------------------------------------------------

class AgentDailyBatchResponse(BaseModel):
    items: List[AgentDailyItemDetail]
    errors: Dict[str, str] = Field(
        default_factory=dict,
        description="Map of id → error message for any ids that failed to resolve.",
    )


# ---------------------------------------------------------------------------
# Multi-group search response
# ---------------------------------------------------------------------------

class AgentMultiGroupStats(BaseModel):
    group_index: int
    total: int
    has_more: bool


class AgentDailyMultiResponse(BaseModel):
    items: List[AgentDailyItem] = Field(
        description="Merged, deduped results sorted by published_at DESC."
    )
    total_merged: int = Field(description="Count of items after dedup.")
    per_group: List[AgentMultiGroupStats] = Field(
        description="Per-group raw hit counts before dedup (useful for diagnosing which group contributed)."
    )
    dedup_by: str = Field(description="'url' or 'id'")


# ---------------------------------------------------------------------------
# Introspection — subscriptions
# ---------------------------------------------------------------------------

class AgentSubscriptionChannel(BaseModel):
    channel_type: str
    channel_identifier: str
    channel_title: Optional[str] = None
    item_count_30d: int = 0


class AgentSubscriptionListResponse(BaseModel):
    channels: List[AgentSubscriptionChannel]
    total: int
    by_source: Dict[str, int] = Field(
        default_factory=dict,
        description="channel_type → count of enabled channels.",
    )


# ---------------------------------------------------------------------------
# Introspection — sources
# ---------------------------------------------------------------------------

class AgentSourceInfo(BaseModel):
    source: str
    in_preset: bool = Field(description="Whether this source is in the user's current Daily preset.")
    item_count_30d: int = Field(description="Items in the last 30 days for this user.")


class AgentSourcesInfoResponse(BaseModel):
    all_sources: List[AgentSourceInfo]
    preset_sources: List[str] = Field(description="Sources currently in the user's preset.")


# ---------------------------------------------------------------------------
# Reports
# ---------------------------------------------------------------------------

class AgentReportSummary(BaseModel):
    report_date: str
    status: Optional[str] = None
    title: Optional[str] = None
    share_url: Optional[str] = Field(None, description="Public no-auth URL to the rendered report")


class AgentReportListResponse(BaseModel):
    reports: List[AgentReportSummary]
    total: int


# Backward-compatible aliases — the pre-v0.5 names leaked "Twitter" although
# these models have carried every report kind since /reports?kind= landed.
AgentTwitterReportSummary = AgentReportSummary
AgentTwitterReportListResponse = AgentReportListResponse


class AgentReportBodyResponse(BaseModel):
    report_date: str
    kind: Optional[str] = Field(
        None,
        description=(
            "Canonical report kind: 'twitter_daily', 'youtube_daily', 'market_daily', "
            "'market_weekly', 'theme', 'the_close', 'closing_bell', or "
            "'bloomberg_surveillance'. Request accepts legacy aliases "
            "(twitter/youtube/market) which normalize to the canonical form."
        ),
    )
    status: Optional[str] = None
    content_markdown: Optional[str] = Field(
        None,
        description="Always None as of v0.5.0 — all reports are delivered via share_url. Field retained for backward compatibility.",
    )
    share_url: Optional[str] = Field(None, description="Public no-auth URL to the rendered report")
    metadata: Optional[dict] = None


# ---------------------------------------------------------------------------
# Form 13F (SEC quarterly institutional holdings — global read-only data)
# ---------------------------------------------------------------------------

class AgentForm13fPreset(BaseModel):
    filer_id: int = Field(
        ..., description="Preset filer id — pass to /form13f/filers/{filer_id}."
    )
    label: str = Field(..., description="Display name, e.g. 'Berkshire Hathaway Inc'")
    cik: Optional[str] = Field(None, description="10-digit SEC CIK")
    latest_quarter: Optional[str] = Field(None, description="Latest snapshotted quarter, e.g. '2026Q3'")
    latest_total_value: Optional[int] = Field(None, description="Portfolio value in USD")


class AgentForm13fManifestResponse(BaseModel):
    quarters: List[str] = Field(
        ..., description="Snapshotted quarters ascending, e.g. ['2024Q1', ..., '2026Q3']"
    )
    presets: List[AgentForm13fPreset] = Field(
        ..., description="The ~20 tracked institutions (label → filer_id map)."
    )


class AgentForm13fHolding(BaseModel):
    issuer: str
    title: Optional[str] = Field(None, description="Share class ('COM', 'CL A', ...)")
    cusip: str
    putcall: str = Field("", description="'' stock/ETF long | 'Call' | 'Put'")
    value: int = Field(..., description="Position value in USD")
    shares: Optional[int] = None
    pct: float = Field(..., description="% of portfolio (value / quarter total)")
    prev_shares: Optional[int] = Field(
        None,
        description="Shares held in the previous quarter (full holdings, not just "
                    "prior top20). null = no prior-quarter position (new).",
    )


class AgentForm13fTopChange(BaseModel):
    issuer: str
    cusip: str
    putcall: str = ""
    kind: str = Field(
        ..., description="'new' | 'increase' | 'decrease' | 'exited'"
    )
    value: int = Field(..., description="Current-quarter value in USD")
    pct: float = Field(..., description="Current % of stock+ETF-long portfolio")
    dpct: float = Field(..., description="Change in % of portfolio vs previous quarter")
    spct: Optional[float] = Field(
        None, description="Share count change %. null for new positions."
    )


class AgentForm13fQuarterSnapshot(BaseModel):
    quarter: str
    total_value: Optional[int] = Field(None, description="Total portfolio value in USD")
    position_count: Optional[int] = None
    sector_alloc: Dict[str, float] = Field(
        default_factory=dict,
        description="Sector/asset allocation % {'Technology': 30.29} — GICS 11 "
                    "sectors + 9 ETF asset classes + Other.",
    )
    top20_holdings: List[AgentForm13fHolding] = Field(
        default_factory=list,
        description="20 largest positions by value (includes option legs).",
    )
    top5_buys: List[AgentForm13fTopChange] = Field(
        default_factory=list,
        description="Top 5 position increases/new entries by weight gain. Long "
                    "stocks & ETFs only (no Put/Call, bonds, or convertibles); "
                    "denominator = stock+ETF long total.",
    )
    top5_sells: List[AgentForm13fTopChange] = Field(
        default_factory=list,
        description="Top 5 position decreases/exits by weight loss. Same scope as top5_buys.",
    )


class AgentForm13fFilerDetailResponse(BaseModel):
    filer_id: int
    name: str
    cik: Optional[str] = None
    quarters: List[AgentForm13fQuarterSnapshot] = Field(
        default_factory=list,
        description="Snapshots ascending by quarter. 13F filings are due within "
                    "45 days after quarter end — data is inherently lagged.",
    )


# ---------------------------------------------------------------------------
# Request bodies (POST endpoints)
# ---------------------------------------------------------------------------

class AgentDailyBatchRequest(BaseModel):
    ids: List[str] = Field(
        description="List of item ids (e.g. 'feed_88247', 'upload_123'). Max 10.",
        max_length=10,
    )


class AgentMultiSearchGroup(BaseModel):
    q: Optional[str] = None
    since: Optional[str] = None
    sources: Optional[str] = None
    tags: Optional[str] = None
    tickers: Optional[str] = None
    author: Optional[str] = None
    people: Optional[str] = None
    countries: Optional[str] = None
    limit: int = Field(50, ge=1, le=100)


class AgentMultiSearchRequest(BaseModel):
    groups: List[AgentMultiSearchGroup] = Field(
        description="1–5 search groups to run in parallel then merge.", max_length=5
    )
    dedup_by: Literal["url", "id"] = "url"
    limit_per_group: int = Field(50, ge=1, le=100)
