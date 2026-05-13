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
        description="Short excerpt of raw body when available (agents should prefer summary + url)",
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
