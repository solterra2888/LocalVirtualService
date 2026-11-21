# -*- coding: utf-8 -*-
"""
Agent HTTP API v1 (readonly) — current as of server v0.6.0.

Breaking changes from v0.1 (v0.2.0):
  - GET /search removed → use GET /daily?q=...
  - GET /reports/twitter-daily/* and /reports/youtube-daily/* removed
    → use GET /reports?kind=twitter|youtube  etc.
  - Query param `ticker` (singular) → `tickers` (comma-separated multi-value)
  - New params: tags (comma-sep), author (string), dedup (bool, default true)
  - New endpoints: POST /daily/batch, POST /daily/multi, GET /subscriptions, GET /sources
  - Legacy v0.1 paths (/search, /reports/twitter-daily/*, /reports/youtube-daily/*)
    removed entirely in v0.5.0 (previously 410 Gone) — use the replacements above.

v0.6.0 additions (no breaking changes):
  - GET /form13f/manifest, GET /form13f/filers/{filer_id} — SEC quarterly
    institutional holdings (global read-only; PAT scope form13f:read).

Auth: Bearer JWT (web) or Bearer PAT (ght_...).
"""

from __future__ import annotations

from typing import List, Optional

from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request, Response

from backend.auth.pat_auth import optional_pat_rate_headers, require_pat_scopes
from backend.schemas.agent import (
    AgentDailyBatchRequest,
    AgentDailyBatchResponse,
    AgentDailyFeedResponse,
    AgentDailyItemDetail,
    AgentDailyMultiResponse,
    AgentForm13fFilerDetailResponse,
    AgentForm13fManifestResponse,
    AgentMultiSearchRequest,
    AgentReportBodyResponse,
    AgentReportListResponse,
    AgentSourcesInfoResponse,
    AgentSubscriptionListResponse,
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
            "Values: twitter, youtube, youtube_feed, news, zhishixingqiu, substack, weibo, "
            "pdf, image, audio, text, podcast. "
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
    people: Optional[str] = Query(
        None,
        description=(
            "Comma-separated person names (OR logic — match ANY). "
            "Matches the structured 'name' field in people tags, case-insensitive. "
            "Use original-form names from content (e.g. people=Trump,Musk — NOT '特朗普'). "
            "Far more precise than q= text search for person-centric queries."
        ),
    ),
    countries: Optional[str] = Query(
        None,
        description=(
            "Comma-separated country full English names (OR logic). "
            "Example: countries=United States,China. "
            "NOT ISO codes — use 'United States' not 'US'."
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
        people=people,
        countries=countries,
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
# Reports (consolidated — kind= twitter_daily | youtube_daily | market_daily |
# market_weekly | theme | the_close | closing_bell | bloomberg_surveillance)
# ---------------------------------------------------------------------------

_REPORT_KIND_QUERY = (
    "Report type (canonical): 'twitter_daily', 'youtube_daily', 'market_daily', "
    "'market_weekly', 'theme', 'the_close', 'closing_bell', 'bloomberg_surveillance'. "
    "Legacy aliases twitter/youtube/market still accepted."
)


@router.get("/reports", response_model=AgentReportListResponse)
def agent_report_list(
    request: Request,
    response: Response,
    kind: str = Query(..., description=_REPORT_KIND_QUERY),
    limit: int = Query(30, ge=1, le=100),
    offset: int = Query(0, ge=0),
    user: TokenData = Depends(require_pat_scopes("reports:read")),
):
    data = _svc().report_list(
        user, kind=kind, limit=limit, offset=offset, request_base=str(request.base_url),
    )
    _rl(response, user)
    return data


@router.get("/reports/latest", response_model=Optional[AgentReportBodyResponse])
def agent_report_latest(
    request: Request,
    response: Response,
    kind: str = Query(..., description=_REPORT_KIND_QUERY),
    user: TokenData = Depends(require_pat_scopes("reports:read")),
):
    data = _svc().report_latest(user, kind=kind, request_base=str(request.base_url))
    _rl(response, user)
    return data


@router.get("/reports/{report_date}", response_model=AgentReportBodyResponse)
def agent_report_by_date(
    request: Request,
    response: Response,
    report_date: str,
    kind: str = Query(..., description=_REPORT_KIND_QUERY),
    user: TokenData = Depends(require_pat_scopes("reports:read")),
):
    data = _svc().report_by_date(
        user, kind=kind, report_date=report_date, request_base=str(request.base_url),
    )
    if data is None:
        raise HTTPException(status_code=404, detail="No report for this date")
    _rl(response, user)
    return data


# ---------------------------------------------------------------------------
# Form 13F (SEC quarterly institutional holdings — global read-only data,
# PAT scope: form13f:read)
# ---------------------------------------------------------------------------

@router.get("/form13f/manifest", response_model=AgentForm13fManifestResponse)
def agent_form13f_manifest(
    response: Response,
    user: TokenData = Depends(require_pat_scopes("form13f:read")),
):
    """Quarters with snapshots + the ~20 tracked institutions (label → filer_id).

    Call this FIRST to resolve an institution name (e.g. 'Berkshire Hathaway')
    to its filer_id, then call /form13f/filers/{filer_id}.
    """
    data = _svc().form13f_manifest()
    _rl(response, user)
    return data


@router.get("/form13f/filers/{filer_id}", response_model=AgentForm13fFilerDetailResponse)
def agent_form13f_filer_detail(
    response: Response,
    filer_id: int,
    quarter: Optional[str] = Query(
        None,
        description=(
            "Exact quarter label, e.g. '2026Q3'. Omit for ALL quarters "
            "(trend analysis, larger payload); pass to get a single quarter."
        ),
    ),
    user: TokenData = Depends(require_pat_scopes("form13f:read")),
):
    """One institution's quarterly snapshots: sector allocation, top20 holdings
    (with prev_shares for QoQ deltas), and top5 buys/sells.

    NOTE: 13F filings are due within 45 days after quarter end — the latest
    quarter lags ~1-2 months behind "today".
    """
    data = _svc().form13f_filer_detail(filer_id, quarter=quarter)
    if data is None:
        detail = f" or no snapshot for quarter {quarter}" if quarter else ""
        raise HTTPException(
            status_code=404, detail=f"Filer id={filer_id} not found{detail}")
    _rl(response, user)
    return data
