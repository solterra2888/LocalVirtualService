# -*- coding: utf-8 -*-
"""
Market Weekly Report API Routes

GET  /market-weekly-report/list      — 返回当前用户的报告历史列表（含 OSS 签名 URL）
GET  /market-weekly-report/latest    — 返回最新一份已完成报告
POST /market-weekly-report/generate  — 手动触发生成（异步 Celery 任务）
GET  /market-weekly-report/shared/{token} — 公开分享链接（HTML，无需认证）
"""

from fastapi import APIRouter, HTTPException, Depends, Query
from fastapi.responses import HTMLResponse
from typing import Optional
from pydantic import BaseModel

from backend.services.market_weekly_service import MarketWeeklyService
from backend.utils.logger import get_logger
from backend.auth.jwt_utils import get_current_user
from backend.schemas.user import TokenData

logger = get_logger("market_weekly_report_api")

router = APIRouter(tags=["Market Weekly Report"])


class GenerateRequest(BaseModel):
    week_end: Optional[str] = None  # ISO date string 'YYYY-MM-DD', defaults to latest Friday


# ──────────────────────────────────────────────────────────────
# GET /market-weekly-report/list
# ──────────────────────────────────────────────────────────────

@router.get("/market-weekly-report/list")
async def list_market_weekly_reports(
    limit: int = Query(20, ge=1, le=100, description="Max number of reports to return"),
    current_user: TokenData = Depends(get_current_user),
):
    """
    返回当前用户的 Market Weekly Report 历史列表，最新在前。
    每条记录包含一个 24h 有效的 OSS 签名内联 URL（适合 iframe 嵌入）。
    """
    try:
        svc = MarketWeeklyService()
        reports = svc.list_reports(user_id=current_user.username, limit=limit)
        return {"success": True, "data": reports, "total": len(reports)}
    except Exception as exc:
        logger.error(f"[list_market_weekly_reports] Error for user {current_user.username}: {exc}")
        raise HTTPException(status_code=500, detail=f"Failed to list reports: {exc}")


# ──────────────────────────────────────────────────────────────
# GET /market-weekly-report/latest
# ──────────────────────────────────────────────────────────────

@router.get("/market-weekly-report/latest")
async def get_latest_market_weekly_report(
    current_user: TokenData = Depends(get_current_user),
):
    """
    返回当前用户最新一份已完成的 Market Weekly Report（含签名内联 URL）。
    若无报告则返回 404。
    """
    try:
        svc = MarketWeeklyService()
        report = svc.get_latest_report(user_id=current_user.username)
        if not report:
            raise HTTPException(
                status_code=404,
                detail="No completed Market Weekly Report found. "
                       "Tag themes with 'Market Weekly' and trigger generation.",
            )
        return {"success": True, "data": report}
    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"[get_latest_market_weekly_report] Error for user {current_user.username}: {exc}")
        raise HTTPException(status_code=500, detail=f"Failed to get latest report: {exc}")


# ──────────────────────────────────────────────────────────────
# POST /market-weekly-report/generate
# ──────────────────────────────────────────────────────────────

@router.post("/market-weekly-report/generate")
async def generate_market_weekly_report(
    request: GenerateRequest = GenerateRequest(),
    current_user: TokenData = Depends(get_current_user),
):
    """
    手动触发为当前用户生成 Market Weekly Report。

    - 如不传 week_end，默认使用最新周五。
    - 任务通过 Celery 异步执行；接口立即返回任务 ID。
    """
    try:
        from backend.tasks.market_weekly_report_tasks import generate_market_weekly_report_task

        task = generate_market_weekly_report_task.apply_async(
            kwargs={
                "user_id": current_user.username,
                "week_end": request.week_end,
            }
        )

        logger.info(
            f"[generate_market_weekly_report] Queued for user={current_user.username}, "
            f"week_end={request.week_end or 'latest'}, task_id={task.id}"
        )

        return {
            "success": True,
            "message": "Market Weekly Report generation queued.",
            "task_id": task.id,
            "user_id": current_user.username,
            "week_end": request.week_end or "latest",
        }
    except Exception as exc:
        logger.error(f"[generate_market_weekly_report] Error for user {current_user.username}: {exc}")
        raise HTTPException(status_code=500, detail=f"Failed to queue report generation: {exc}")


# ──────────────────────────────────────────────────────────────
# GET /market-weekly-report/shared/{token}  — public, no auth required
# ──────────────────────────────────────────────────────────────

@router.get("/market-weekly-report/shared/{token}", response_class=HTMLResponse)
async def get_shared_market_weekly_report(token: str):
    """
    Public endpoint — no authentication required.

    Returns the raw HTML of the completed Market Weekly Report identified by
    its share_token, streaming it directly from the private OSS bucket
    so it renders inline in any browser.

    Returns 404 if the token is unknown or the report is not yet completed.
    """
    try:
        svc = MarketWeeklyService()
        html_bytes = svc.get_report_html_by_token(token)
        if html_bytes is None:
            raise HTTPException(
                status_code=404,
                detail="Shared report not found or not yet completed.",
            )
        return HTMLResponse(
            content=html_bytes.decode("utf-8"),
            status_code=200,
            headers={"Cache-Control": "public, max-age=3600"},
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"[get_shared_market_weekly_report] Error for token={token[:8]}…: {exc}")
        raise HTTPException(status_code=500, detail="Failed to retrieve shared report.")