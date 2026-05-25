# -*- coding: utf-8 -*-
"""
Agent HTTP API v2 — semantic / hybrid search and chunk locator.

Mounts under /api/v2/agent/ side-by-side with /api/v1/agent (which stays
unchanged). External Agents (Claude Code / Cursor / Hermes / OpenClaw) can
mix v1 and v2 tools freely.

Tool intent:
  POST /api/v2/agent/semantic-search       → concept retrieval over chunks
  POST /api/v2/agent/hybrid-search         → keyword + semantic, RRF-fused
  POST /api/v2/agent/chunks-by-locator     → exact-locator chunk fetch

The contract: every search response returns *chunk-level* hits with
`content_id` — clients should call /api/v1/agent/daily/batch (or
chunks-by-locator) to retrieve full content. See
``docs/instructions/AGENT_API_V2_SEARCH.md`` for the full tool selection
decision tree.
"""

from __future__ import annotations

from fastapi import APIRouter, Body, Depends, Response

from backend.auth.pat_auth import optional_pat_rate_headers, require_pat_scopes
from backend.schemas.agent_search import (
    ChunksByLocatorRequest,
    ChunksByLocatorResponse,
    HybridSearchRequest,
    HybridSearchResponse,
    SemanticSearchRequest,
    SemanticSearchResponse,
)
from backend.schemas.user import TokenData
from backend.services.vector_search_service import VectorSearchService

router = APIRouter(prefix="/api/v2/agent", tags=["Agent API v2"])


def _svc() -> VectorSearchService:
    return VectorSearchService()


def _rl(response: Response, user: TokenData) -> None:
    for k, v in optional_pat_rate_headers(user).items():
        response.headers[k] = v


# ---------------------------------------------------------------------------
# semantic_search
# ---------------------------------------------------------------------------
@router.post("/semantic-search", response_model=SemanticSearchResponse)
def semantic_search(
    response: Response,
    body: SemanticSearchRequest = Body(...),
    user: TokenData = Depends(require_pat_scopes("daily:read")),
):
    """Concept retrieval over chunks (OSS Vector Bucket).

    Use this when keywords cannot capture the user's intent — e.g.
    "supply chain risks", "market sentiment about AI", "value investing
    methodology". Returns ranked chunks with `content_id`; follow up with
    /api/v1/agent/daily/batch or /api/v2/agent/chunks-by-locator to read
    full content.
    """
    data = _svc().semantic_search(user, body)
    _rl(response, user)
    return data


# ---------------------------------------------------------------------------
# hybrid_search
# ---------------------------------------------------------------------------
@router.post("/hybrid-search", response_model=HybridSearchResponse)
def hybrid_search(
    response: Response,
    body: HybridSearchRequest = Body(...),
    user: TokenData = Depends(require_pat_scopes("daily:read")),
):
    """Combined keyword (Daily v1 ILIKE / pg_trgm) + semantic, RRF-fused.

    Use when the query mixes specific terms (tickers, names) with concepts
    (e.g. "What did Powell say about rate cuts?"). Prefer this over running
    daily_feed + semantic_search separately — RRF fusion already balances
    the two.
    """
    data = _svc().hybrid_search(user, body)
    _rl(response, user)
    return data


# ---------------------------------------------------------------------------
# chunks_by_locator
# ---------------------------------------------------------------------------
@router.post("/chunks-by-locator", response_model=ChunksByLocatorResponse)
def chunks_by_locator(
    response: Response,
    body: ChunksByLocatorRequest = Body(...),
    user: TokenData = Depends(require_pat_scopes("daily:read")),
):
    """Pull contiguous chunks by chapter / page / chunk_idx range.

    Use when you already know *where* in a document the relevant content
    lives (typically after semantic_search surfaced a chunk in chapter 6.1
    and you want the full section). For whole-item content of feed
    articles / tweets / short uploads, prefer /api/v1/agent/daily/batch.
    """
    data = _svc().get_chunks_by_locator(user, body)
    _rl(response, user)
    return data
