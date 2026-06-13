# -*- coding: utf-8 -*-
"""
知识星球每日摘要 API

按用户过滤的报告查询和生成接口（对标 Twitter Daily Report）。
"""

from datetime import date as _date

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from backend.auth.jwt_utils import get_current_user
from backend.schemas.user import TokenData
from backend.services.zhishixingqiu_summary_service import ZhishixingqiuSummaryService
from backend.utils.logger import get_logger

logger = get_logger("zhishixingqiu_summary_api")

router = APIRouter(prefix="/zhishixingqiu-summary", tags=["Zhishixingqiu Summary"])


class GenerateReportRequest(BaseModel):
    report_date: str | None = Field(
        None, description="ISO date string (e.g. 2026-06-11). Defaults to today."
    )
    window_end: str | None = Field(
        None, description="ISO datetime string (UTC). Defaults to Beijing calendar day."
    )
    window_hours: int = Field(
        24, description="Look-back hours for content window.", ge=1, le=72
    )
    force: bool = Field(
        False, description="Force regeneration even if report exists."
    )


@router.get("/latest")
async def get_latest_report(current_user: TokenData = Depends(get_current_user)):
    """获取当前用户最新的知识星球每日报告。"""
    svc = ZhishixingqiuSummaryService()
    report = svc.get_latest_report(current_user.user_id)
    if not report:
        return {"success": True, "data": None, "message": "暂无报告"}
    return {"success": True, "data": report}


@router.get("/list")
async def list_reports(
    limit: int = Query(default=20, le=100),
    offset: int = Query(default=0, ge=0),
    current_user: TokenData = Depends(get_current_user),
):
    """获取当前用户的知识星球报告历史列表。"""
    svc = ZhishixingqiuSummaryService()
    reports, total = svc.get_user_reports(current_user.user_id, limit, offset)
    return {
        "success": True,
        "data": {
            "reports": reports,
            "total": total,
            "limit": limit,
            "offset": offset,
        },
    }


@router.get("/{report_date}")
async def get_report_by_date(
    report_date: str,
    current_user: TokenData = Depends(get_current_user),
):
    """按日期获取当前用户的知识星球每日报告。"""
    try:
        as_of = _date.fromisoformat(report_date)
    except ValueError:
        raise HTTPException(
            status_code=400, detail=f"Invalid date format: {report_date}"
        )
    svc = ZhishixingqiuSummaryService()
    report = svc.get_report_by_date(current_user.user_id, as_of)
    if not report:
        return {"success": True, "data": None, "message": f"No report found for {report_date}"}
    return {"success": True, "data": report}


@router.post("/generate")
async def generate_report(
    request: GenerateReportRequest = GenerateReportRequest(),
    current_user: TokenData = Depends(get_current_user),
):
    """触发知识星球每日报告生成（异步 Celery 任务，当前用户）。"""
    from backend.tasks.zhishixingqiu_summary_tasks import (
        generate_zhishixingqiu_user_report_task,
    )

    report_date = request.report_date or str(_date.today())
    task = generate_zhishixingqiu_user_report_task.delay(
        user_id=current_user.user_id,
        report_date=report_date,
        window_end=request.window_end,
        window_hours=request.window_hours,
        force=request.force,
    )
    logger.info(
        f"用户 {current_user.username} 触发知识星球报告生成: "
        f"date={report_date}, force={request.force}, task_id={task.id}"
    )
    return {
        "success": True,
        "message": "Zhishixingqiu report generation queued",
        "data": {
            "celery_task_id": task.id,
            "report_date": report_date,
        },
    }
