# -*- coding: utf-8 -*-
"""
Agent HTTP API v1 (readonly) — v0.2.0

Breaking changes from v0.1:
  - GET /search removed → use GET /daily?q=...
  - GET /reports/twitter-daily/* and /reports/youtube-daily/* removed
    → use GET /reports?kind=twitter|youtube  etc.
  - Query param `ticker` (singular) → `tickers` (comma-separated multi-value)
  - New params: tags (comma-sep), author (string), dedup (bool, default true)
  - New endpoints: POST /daily/batch, POST /daily/multi, GET /subscriptions, GET /sources
  - Old paths return 410 Gone with upgrade guidance.

Auth: Bearer JWT (web) or Bearer PAT (ght_...).
"""

from __future__ import annotations

from typing import List, Optional

from fastapi import APIRouter, Body, Depends, HTTPException, Query, Response
from fastapi.responses import JSONResponse

from backend.auth.pat_auth import optional_pat_rate_headers, require_pat_scopes
from backend.schemas.agent import (
    AgentDailyBatchRequest,
    AgentDailyBatchResponse,
    AgentDailyFeedResponse,
    AgentDailyItemDetail,
    AgentDailyMultiResponse,
    AgentMultiSearchRequest,
    AgentReportBodyResponse,
    AgentSourcesInfoResponse,
    AgentSubscriptionListResponse,
    AgentTwitterReportListResponse,
)
from backend.schemas.user import TokenData
from backend.services.agent_service import AgentService

router = APIRouter(prefix="/api/v1/agent", tags=["Agent API v1"])


def _svc() -> AgentService:
    return AgentService()


def _rl(response: Response, user: TokenData) -> None:
    for k, v in optional_pat_rate_headers(user).items():
        response.headers[k] = v


# ---------------------------------------------------------------------------
# Daily feed / search (merged)
# ---------------------------------------------------------------------------

@router.get("/daily", response_model=AgentDailyFeedResponse)
def agent_daily_feed(
    response: Response,
    since: Optional[str] = Query(
        None,
        description="Time window: 24h | 14d | 2w | ISO 8601. Default 7d. Raises 400 on invalid values.",
    ),
    q: Optional[str] = Query(
        None,
        max_length=200,
        description=(
            "Keyword filter. Syntax: space=AND, OR (uppercase)=any match, \"phrase\"=exact. "
            "Example: q=NVDA OR 英伟达. "
            "DO NOT pass a full sentence — extract 2-5 keywords joined by OR."
        ),
    ),
    sources: Optional[str] = Query(
        None,
        description=(
            "Comma-separated source types to include. "
            "Values: twitter, youtube, youtube_feed, news, pdf, image, audio, text, podcast. "
            "Omit to use your web-app preset."
        ),
    ),
    tickers: Optional[str] = Query(
        None,
        description=(
            "Comma-separated ticker symbols with exchange suffix. "
            "Example: NVDA.US,AAPL.US,9988.HK. "
            "Bare codes like 'NVDA' will NOT match — suffix is required. "
            "If unsure of suffix, use q= to search, then read the tickers field in the response."
        ),
    ),
    tags: Optional[str] = Query(
        None,
        description=(
            "Comma-separated concept/theme tags. "
            "Matches items tagged with ANY of the supplied tags (OR logic). "
            "Use list_sources or prior response tags fields to discover valid values."
        ),
    ),
    author: Optional[str] = Query(
        None,
        description=(
            "Substring filter on author_name / author_identifier. "
            "Example: author=Bloomberg, author=@elonmusk. "
            "Only applies to feed items (Twitter, YouTube, news); upload items have no author."
        ),
    ),
    limit: int = Query(20, ge=1, le=100),
    cursor: Optional[str] = Query(None, description="Pagination cursor from previous response next_cursor."),
    dedup: bool = Query(
        True,
        description=(
            "When true (default), collapse duplicate URLs from the result set "
            "(e.g. same article indexed from multiple feed sources). "
            "Set false for debugging."
        ),
    ),
    user: TokenData = Depends(require_pat_scopes("daily:read")),
):
    data = _svc().get_daily_feed(
        user,
        since=since,
        q=q,
        sources=sources,
        tickers=tickers,
        tags=tags,
        author=author,
        limit=limit,
        cursor=cursor,
        dedup=dedup,
    )
    _rl(response, user)
    return data


@router.get("/daily/{item_id}", response_model=AgentDailyItemDetail)
def agent_daily_item(
    response: Response,
    item_id: str,
    user: TokenData = Depends(require_pat_scopes("daily:read")),
):
    data = _svc().get_daily_item(user, item_id)
    _rl(response, user)
    return data


# ---------------------------------------------------------------------------
# Batch detail
# ---------------------------------------------------------------------------

@router.post("/daily/batch", response_model=AgentDailyBatchResponse)
def agent_daily_batch(
    response: Response,
    body: AgentDailyBatchRequest = Body(...),
    user: TokenData = Depends(require_pat_scopes("daily:read")),
):
    """Fetch full detail for up to 10 items in a single request.

    Prefer this over N serial daily_item calls when drilling down on multiple results.
    """
    data = _svc().get_daily_items_batch(user, body.ids)
    _rl(response, user)
    return data


# ---------------------------------------------------------------------------
# Multi-group search
# ---------------------------------------------------------------------------

@router.post("/daily/multi", response_model=AgentDailyMultiResponse)
def agent_daily_multi(
    response: Response,
    body: AgentMultiSearchRequest = Body(...),
    user: TokenData = Depends(require_pat_scopes("daily:read")),
):
    """Run 1–5 search groups in parallel, merge results, and deduplicate server-side.

    Use instead of separate daily_feed calls when covering multiple topic angles.
    Each group supports all daily_feed parameters (q, since, sources, tickers, tags, author).
    per_group in the response shows how many items each group contributed before dedup.
    """
    data = _svc().multi_search(user, body)
    _rl(response, user)
    return data


# ---------------------------------------------------------------------------
# Introspection
# ---------------------------------------------------------------------------

@router.get("/subscriptions", response_model=AgentSubscriptionListResponse)
def agent_list_subscriptions(
    response: Response,
    user: TokenData = Depends(require_pat_scopes("daily:read")),
):
    """List all enabled feed subscriptions with 30-day item counts.

    Use to disambiguate 0-result searches: check whether the relevant channel
    is subscribed and enabled before assuming the topic has no coverage.
    """
    data = _svc().list_subscriptions(user)
    _rl(response, user)
    return data


@router.get("/sources", response_model=AgentSourcesInfoResponse)
def agent_list_sources(
    response: Response,
    user: TokenData = Depends(require_pat_scopes("daily:read")),
):
    """List all available source types with preset membership and 30-day item counts.

    Use to understand which sources are active in the user's Daily preset
    before filtering with sources= parameter.
    """
    data = _svc().list_sources(user)
    _rl(response, user)
    return data


# ---------------------------------------------------------------------------
# Reports (consolidated — kind=twitter|youtube)
# ---------------------------------------------------------------------------

@router.get("/reports", response_model=AgentTwitterReportListResponse)
def agent_report_list(
    response: Response,
    kind: str = Query(..., description="Report type: 'twitter' or 'youtube'"),
    limit: int = Query(30, ge=1, le=100),
    offset: int = Query(0, ge=0),
    user: TokenData = Depends(require_pat_scopes("reports:read")),
):
    data = _svc().report_list(user, kind=kind, limit=limit, offset=offset)
    _rl(response, user)
    return data


@router.get("/reports/latest", response_model=Optional[AgentReportBodyResponse])
def agent_report_latest(
    response: Response,
    kind: str = Query(..., description="Report type: 'twitter' or 'youtube'"),
    user: TokenData = Depends(require_pat_scopes("reports:read")),
):
    data = _svc().report_latest(user, kind=kind)
    _rl(response, user)
    return data


@router.get("/reports/{report_date}", response_model=AgentReportBodyResponse)
def agent_report_by_date(
    response: Response,
    report_date: str,
    kind: str = Query(..., description="Report type: 'twitter' or 'youtube'"),
    user: TokenData = Depends(require_pat_scopes("reports:read")),
):
    data = _svc().report_by_date(user, kind=kind, report_date=report_date)
    if data is None:
        raise HTTPException(status_code=404, detail="No report for this date")
    _rl(response, user)
    return data


# ---------------------------------------------------------------------------
# 410 Gone stubs for removed v0.1 paths
# ---------------------------------------------------------------------------

_GONE_MSG = (
    "This endpoint was removed in guanhe-mcp v0.2. "
    "Upgrade to guanhe-mcp >= 0.2.0 and update your MCP configuration. "
    "See: https://www.guanhecapital.hk/agent/skill/GUANHE_DAILY.md"
)


@router.get("/search")
def _gone_search():
    """Removed: use GET /daily?q=... instead."""
    return JSONResponse(
        status_code=410,
        content={"detail": _GONE_MSG, "replacement": "GET /api/v1/agent/daily?q=..."},
    )


@router.get("/reports/twitter-daily/latest")
@router.get("/reports/twitter-daily/{report_date}")
@router.get("/reports/twitter-daily")
def _gone_twitter_reports():
    """Removed: use GET /reports?kind=twitter instead."""
    return JSONResponse(
        status_code=410,
        content={
            "detail": _GONE_MSG,
            "replacement": "GET /api/v1/agent/reports?kind=twitter",
        },
    )


@router.get("/reports/youtube-daily/latest")
@router.get("/reports/youtube-daily/{report_date}")
@router.get("/reports/youtube-daily")
def _gone_youtube_reports():
    """Removed: use GET /reports?kind=youtube instead."""
    return JSONResponse(
        status_code=410,
        content={
            "detail": _GONE_MSG,
            "replacement": "GET /api/v1/agent/reports?kind=youtube",
        },
    )
