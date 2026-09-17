# -*- coding: utf-8 -*-
"""
Close Brief Report API

The Close Report（Bloomberg "The Close" PM Brief 独立报告）的查询和生成接口。
生成入口复用 The Close Summary 任务（报告渲染是其后置钩子），无独立调度器。
"""

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

from backend.auth.jwt_utils import get_current_user
from backend.schemas.user import TokenData
from backend.services.close_brief_report_service import CloseBriefReportService
from backend.services.the_close_summary_service import (
    PLACEHOLDER_REASONS,
    TheCloseSummaryService,
)
from backend.utils.datetime_utils import utcnow, window_label_date
from backend.utils.logger import get_logger

logger = get_logger("close_brief_report_api")

router = APIRouter(prefix="/close-brief-report", tags=["Close Brief Report"])


class GenerateReportRequest(BaseModel):
    report_date: str | None = Field(
        None, description="ISO date string (e.g. 2026-06-11). Defaults to today."
    )
    window_end: str | None = Field(
        None,
        description="ISO datetime (UTC) for the rolling content window. Defaults to now.",
    )
    window_hours: int = Field(
        24, description="Look-back hours for content window.", ge=1, le=72
    )
    force: bool = Field(
        False,
        description="Force re-summarize the episode before rendering the report.",
    )


@router.get("/latest")
async def get_latest_report(current_user: TokenData = Depends(get_current_user)):
    """获取最新的 The Close Report。"""
    svc = CloseBriefReportService()
    report = svc.get_latest_report()
    if not report:
        raise HTTPException(status_code=404, detail="No completed report found")
    return report


@router.get("/status")
async def get_report_status(current_user: TokenData = Depends(get_current_user)):
    """当天摘要/报告状态 + 最新已完成报告（供前端区分旧报告与本次生成结果）。"""
    today = window_label_date(utcnow(), "Asia/Shanghai")
    brief_svc = CloseBriefReportService()
    summary = TheCloseSummaryService().get_summary_by_date(today)
    today_summary = None
    if summary:
        meta = summary.get("metadata") or {}
        reason = meta.get("reason") if isinstance(meta, dict) else None
        today_summary = {
            "report_date": summary["report_date"],
            "status": summary["status"],
            "video_title": summary.get("video_title"),
            "placeholder_reason": reason if reason in PLACEHOLDER_REASONS else None,
            "updated_at": summary.get("updated_at"),
        }
    return {
        "as_of": str(today),
        "latest_report": brief_svc.get_latest_report(),
        "today_summary": today_summary,
        "today_report": brief_svc.get_report_by_date(today),
    }


@router.get("/list")
async def list_reports(
    limit: int = 20,
    current_user: TokenData = Depends(get_current_user),
):
    """获取报告历史列表。"""
    svc = CloseBriefReportService()
    return svc.list_reports(limit=limit)


@router.get("/shared/{token}", response_class=HTMLResponse)
async def get_shared_report(token: str):
    """通过 share_token 公共访问报告 HTML（无需认证）。"""
    svc = CloseBriefReportService()
    html = svc.get_report_html_by_token(token)
    if not html:
        raise HTTPException(status_code=404, detail="Report not found")
    return HTMLResponse(content=html)


@router.post("/generate")
async def generate_report(
    request: GenerateReportRequest = GenerateReportRequest(),
    current_user: TokenData = Depends(get_current_user),
):
    """触发 The Close Report 生成（异步 Celery 任务）。

    报告内容来源于 The Close Summary，因此这里转发摘要任务：
    - 摘要缺失/占位 → 重新生成摘要；
    - 摘要已存在 → 任务内幂等跳过摘要，后置钩子按新鲜度补渲染报告。
    """
    from backend.tasks.the_close_summary_tasks import generate_the_close_summary_task

    task = generate_the_close_summary_task.delay(
        report_date=request.report_date,
        window_end=request.window_end or utcnow().isoformat(),
        window_hours=request.window_hours,
        force=request.force,
    )
    return {
        "success": True,
        "message": "Report generation queued",
        "celery_task_id": task.id,
    }
