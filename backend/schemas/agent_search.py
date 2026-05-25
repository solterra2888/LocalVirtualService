# -*- coding: utf-8 -*-
"""Agent / MCP v2 search schemas.

Distinguishes itself from v1 (`backend/schemas/agent.py`):
  - v1 returns AgentDailyItem (one row per article / file).
  - v2 returns AgentSearchHit (one row per chunk) for semantic / hybrid search.

Field convention:
  - content_id: parent identifier (file_processing_details.id OR subscription_content.id).
              Use with daily_items_batch (v1) for full content retrieval.
  - chunk_id:  document_chunks.id; use with get_chunks_by_locator(chunk_id_range=...).
"""

from __future__ import annotations

from datetime import datetime
from typing import List, Literal, Optional

from pydantic import BaseModel, Field

# ───────────────────────── source types ────────────────────────────────
AgentSearchSourceType = Literal[
    # Upload
    "upload",
    "upload_text",
    "upload_web",
    "upload_youtube",
    "upload_audio",
    "upload_podcast",
    "upload_image",
    # Feed
    "feed_twitter",
    "feed_youtube",
    "feed_news",
    "feed_rss",
]


# ───────────────────────── semantic_search ─────────────────────────────
class SemanticSearchRequest(BaseModel):
    query: str = Field(..., min_length=1, max_length=2000, description="Natural-language query.")
    top_k: int = Field(10, ge=1, le=50, description="Number of chunk results to return.")
    source_types: Optional[List[str]] = Field(
        None,
        description=(
            "Filter to specific source_types (e.g. ['upload','feed_youtube']). "
            "Omit for all sources the user can access."
        ),
    )
    file_ids: Optional[List[int]] = Field(
        None,
        description="Scope to specific file_processing_details.id values (upload files only).",
    )
    content_ids: Optional[List[int]] = Field(
        None,
        description="Scope to specific subscription_content.id values (feed entries only).",
    )
    since: Optional[str] = Field(
        None,
        description="Time window: 24h | 14d | 2w | ISO 8601. Filters by chunk.created_at.",
    )
    lang: Optional[str] = Field(
        None,
        description="Filter by detected chunk language: 'zh' | 'en'. Omit for all.",
    )
    min_score: Optional[float] = Field(
        None,
        ge=0.0,
        le=1.0,
        description="Discard hits below this cosine similarity score (0-1). Default: no floor.",
    )


class SearchHit(BaseModel):
    """One chunk-level hit. Use content_id + chunk_id to fetch full context."""

    content_id: int = Field(..., description="Parent id (file_processing_details OR subscription_content).")
    chunk_id: int = Field(..., description="document_chunks.id")
    source_type: str
    title: Optional[str] = None
    chapter: Optional[str] = None
    page: Optional[int] = None
    chunk_text: str = Field(..., description="The matched chunk text (~600 chars).")
    score: float = Field(..., description="Cosine similarity score (1.0 = identical).")
    found_via: List[str] = Field(
        default_factory=lambda: ["vector"],
        description="Which retrieval branch produced this hit: vector | keyword | both",
    )


class SearchDiagnostics(BaseModel):
    embed_ms: int = 0
    query_ms: int = 0
    fetch_ms: int = 0
    candidates: int = 0
    returned: int = 0
    notes: Optional[str] = None


class SemanticSearchResponse(BaseModel):
    results: List[SearchHit] = Field(default_factory=list)
    diagnostics: SearchDiagnostics = Field(default_factory=SearchDiagnostics)


# ───────────────────────── hybrid_search ───────────────────────────────
class HybridSearchRequest(SemanticSearchRequest):
    keyword_weight: float = Field(
        0.5,
        ge=0.0,
        le=1.0,
        description=(
            "RRF fusion blend: 0.0 = pure vector, 1.0 = pure keyword. "
            "Default 0.5 weighs both equally."
        ),
    )


class HybridSearchResponse(SemanticSearchResponse):
    pass


# ───────────────────────── chunks_by_locator ───────────────────────────
class ChunksByLocatorRequest(BaseModel):
    file_id: Optional[int] = Field(None, description="Look-up by upload file_id.")
    content_id: Optional[int] = Field(None, description="Look-up by feed subscription_content.id.")
    chapter: Optional[str] = Field(
        None,
        description="Exact chapter / heading path (e.g. '5.2' or 'Chapter 3 / Value Investing').",
    )
    chapter_prefix: Optional[str] = Field(
        None,
        description="Match chapter starting with this prefix (e.g. 'Chapter 5').",
    )
    page_range: Optional[List[int]] = Field(
        None,
        description="[start, end] inclusive page range (when source has pages).",
        min_length=2,
        max_length=2,
    )
    chunk_idx_range: Optional[List[int]] = Field(
        None,
        description="[start, end] inclusive chunk_idx range.",
        min_length=2,
        max_length=2,
    )
    max_chars: int = Field(
        16000,
        ge=200,
        le=64000,
        description="Cap total returned characters to avoid context overflow.",
    )


class LocatorChunk(BaseModel):
    chunk_id: int
    chunk_idx: int
    chapter: Optional[str] = None
    page: Optional[int] = None
    chunk_text: str


class ChunksByLocatorResponse(BaseModel):
    content_id: int
    source_type: str
    title: Optional[str] = None
    total_chars: int = 0
    truncated: bool = False
    chunks: List[LocatorChunk] = Field(default_factory=list)
