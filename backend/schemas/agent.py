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

ReportKind = Literal["twitter", "youtube"]


class AgentDailyItem(BaseModel):
    id: str
    title: str
    summary: str = Field("", description="AI summary or snippet, trimmed for agents")
    url: Optional[str] = None
    source: AgentSource = Field(
        ...,
        description=(
            "Source type. For uploaded files: 'pdf', 'word', 'excel', 'image', 'audio', 'text', 'podcast', 'web'. "
            "For feeds: 'twitter', 'youtube', 'news', 'zhishixingqiu'. "
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

class AgentTwitterReportSummary(BaseModel):
    report_date: str
    status: Optional[str] = None
    title: Optional[str] = None


class AgentTwitterReportListResponse(BaseModel):
    reports: List[AgentTwitterReportSummary]
    total: int


class AgentReportBodyResponse(BaseModel):
    report_date: str
    kind: Optional[str] = Field(None, description="'twitter' or 'youtube'")
    status: Optional[str] = None
    content_markdown: Optional[str] = Field(None, description="Full markdown body when completed")
    metadata: Optional[dict] = None


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
