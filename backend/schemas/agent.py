# -*- coding: utf-8 -*-
"""Stable external JSON schema for Agent / MCP (versioned under /api/v1/agent)."""

from __future__ import annotations

from datetime import datetime
from typing import List, Literal, Optional

from pydantic import BaseModel, Field

AgentSource = Literal[
    "twitter",
    "youtube",
    "web",
    "pdf",
    "image",
    "audio",
    "text",
    "podcast",
    "news",
]


class AgentDailyItem(BaseModel):
    id: str
    title: str
    summary: str = Field("", description="AI summary or snippet, trimmed for agents")
    url: Optional[str] = None
    source: AgentSource
    author: Optional[str] = None
    published_at: datetime
    tickers: List[str] = Field(default_factory=list)
    tags: List[str] = Field(default_factory=list, description="themes / concepts")


class AgentDailyFeedResponse(BaseModel):
    items: List[AgentDailyItem]
    next_cursor: Optional[str] = None
    has_more: bool = False
    total: int = 0


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


class AgentTwitterReportSummary(BaseModel):
    report_date: str
    status: Optional[str] = None
    title: Optional[str] = None


class AgentTwitterReportListResponse(BaseModel):
    reports: List[AgentTwitterReportSummary]
    total: int


class AgentReportBodyResponse(BaseModel):
    report_date: str
    status: Optional[str] = None
    content_markdown: Optional[str] = Field(None, description="Full markdown body when completed")
    metadata: Optional[dict] = None
