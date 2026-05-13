# -*- coding: utf-8 -*-
"""
Agent HTTP API v1 (readonly): Daily feed, search, Twitter/YouTube daily reports.

Auth: Bearer JWT (web) or Bearer PAT (ght_...).
"""

from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Response

from backend.auth.pat_auth import optional_pat_rate_headers, require_pat_scopes
from backend.schemas.agent import (
    AgentDailyFeedResponse,
    AgentDailyItemDetail,
    AgentReportBodyResponse,
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


@router.get("/daily", response_model=AgentDailyFeedResponse)
def agent_daily_feed(
    response: Response,
    since: Optional[str] = Query(None, description="24h | 7d | 30d | ISO8601; default 7d"),
    q: Optional[str] = Query(None, max_length=200),
    sources: Optional[str] = Query(None, description="Comma-separated source_subtypes; overrides preset"),
    ticker: Optional[str] = Query(None),
    limit: int = Query(20, ge=1, le=100),
    cursor: Optional[str] = Query(None),
    user: TokenData = Depends(require_pat_scopes("daily:read")),
):
    data = _svc().get_daily_feed(
        user, since=since, q=q, sources=sources, ticker=ticker, limit=limit, cursor=cursor
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


@router.get("/search", response_model=AgentDailyFeedResponse)
def agent_search(
    response: Response,
    q: str = Query(..., min_length=1, max_length=200),
    since: Optional[str] = Query(None),
    sources: Optional[str] = Query(None),
    ticker: Optional[str] = Query(None),
    limit: int = Query(20, ge=1, le=100),
    cursor: Optional[str] = Query(None),
    user: TokenData = Depends(require_pat_scopes("daily:read")),
):
    data = _svc().search(
        user, q=q, since=since, sources=sources, ticker=ticker, limit=limit, cursor=cursor
    )
    _rl(response, user)
    return data


@router.get("/reports/twitter-daily", response_model=AgentTwitterReportListResponse)
def agent_twitter_list(
    response: Response,
    limit: int = Query(30, ge=1, le=100),
    offset: int = Query(0, ge=0),
    user: TokenData = Depends(require_pat_scopes("reports:read")),
):
    data = _svc().twitter_list(user, limit=limit, offset=offset)
    _rl(response, user)
    return data


@router.get("/reports/twitter-daily/latest", response_model=Optional[AgentReportBodyResponse])
def agent_twitter_latest(
    response: Response,
    user: TokenData = Depends(require_pat_scopes("reports:read")),
):
    data = _svc().twitter_latest(user)
    _rl(response, user)
    return data


@router.get("/reports/twitter-daily/{report_date}", response_model=AgentReportBodyResponse)
def agent_twitter_by_date(
    response: Response,
    report_date: str,
    user: TokenData = Depends(require_pat_scopes("reports:read")),
):
    data = _svc().twitter_by_date(user, report_date)
    _rl(response, user)
    if data is None:
        raise HTTPException(status_code=404, detail="No report for this date")
    return data


@router.get("/reports/youtube-daily", response_model=AgentTwitterReportListResponse)
def agent_youtube_list(
    response: Response,
    limit: int = Query(30, ge=1, le=100),
    offset: int = Query(0, ge=0),
    user: TokenData = Depends(require_pat_scopes("reports:read")),
):
    data = _svc().youtube_list(user, limit=limit, offset=offset)
    _rl(response, user)
    return data


@router.get("/reports/youtube-daily/latest", response_model=Optional[AgentReportBodyResponse])
def agent_youtube_latest(
    response: Response,
    user: TokenData = Depends(require_pat_scopes("reports:read")),
):
    data = _svc().youtube_latest(user)
    _rl(response, user)
    return data


@router.get("/reports/youtube-daily/{report_date}", response_model=AgentReportBodyResponse)
def agent_youtube_by_date(
    response: Response,
    report_date: str,
    user: TokenData = Depends(require_pat_scopes("reports:read")),
):
    data = _svc().youtube_by_date(user, report_date)
    _rl(response, user)
    if data is None:
        raise HTTPException(status_code=404, detail="No report for this date")
    return data
