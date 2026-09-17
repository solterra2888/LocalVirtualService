# -*- coding: utf-8 -*-
"""
Bloomberg Surveillance Summary API

Bloomberg "Surveillance" 节目每日 AM Brief 摘要的查询和生成接口。
"""

from datetime import date as _date

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from backend.auth.jwt_utils import get_current_user
from backend.schemas.user import TokenData
from backend.services.bloomberg_surveillance_service import BloombergSurveillanceService
from backend.utils.logger import get_logger

logger = get_logger("bloomberg_surveillance_api")

router = APIRouter(prefix="/bloomberg-surveillance", tags=["Bloomberg Surveillance"])


class GenerateSummaryRequest(BaseModel):
    report_date: str | None = Field(
        None, description="ISO date string (e.g. 2026-06-11). Defaults to today."
    )
    window_end: str | None = Field(
        None, description="ISO datetime string (UTC). Defaults to now."
    )
    window_hours: int = Field(
        24, description="Look-back hours for content window.", ge=1, le=72
    )
    force: bool = Field(
        False, description="Force regeneration even if summary exists."
    )


@router.get("/latest")
async def get_latest_summary(current_user: TokenData = Depends(get_current_user)):
    """获取最新的 Surveillance AM Brief 摘要。"""
    svc = BloombergSurveillanceService()
    summary = svc.get_latest_summary()
    if not summary:
        raise HTTPException(status_code=404, detail="No completed summary found")
    return summary


@router.get("/list")
async def list_summaries(
    limit: int = 20,
    current_user: TokenData = Depends(get_current_user),
):
    """获取摘要历史列表。"""
    svc = BloombergSurveillanceService()
    return svc.list_summaries(limit=limit)


@router.get("/{report_date}")
async def get_summary_by_date(
    report_date: str,
    current_user: TokenData = Depends(get_current_user),
):
    """按日期获取 Surveillance AM Brief 摘要。"""
    try:
        as_of = _date.fromisoformat(report_date)
    except ValueError:
        raise HTTPException(
            status_code=400, detail=f"Invalid date format: {report_date}"
        )
    svc = BloombergSurveillanceService()
    summary = svc.get_summary_by_date(as_of)
    if not summary:
        raise HTTPException(
            status_code=404, detail=f"No summary found for {report_date}"
        )
    return summary


@router.post("/generate")
async def generate_summary(
    request: GenerateSummaryRequest = GenerateSummaryRequest(),
    current_user: TokenData = Depends(get_current_user),
):
    """触发 Surveillance AM Brief 生成（异步 Celery 任务，报告为后置钩子）。"""
    from backend.tasks.bloomberg_surveillance_tasks import (
        generate_bloomberg_surveillance_task,
    )

    task = generate_bloomberg_surveillance_task.delay(
        report_date=request.report_date,
        window_end=request.window_end,
        window_hours=request.window_hours,
        force=request.force,
    )
    return {
        "success": True,
        "message": "Bloomberg Surveillance summary generation queued",
        "celery_task_id": task.id,
    }
