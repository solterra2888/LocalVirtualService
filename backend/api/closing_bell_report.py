# -*- coding: utf-8 -*-
"""
Closing Bell Report API

Closing Bell Report（CNBC "Closing Bell" PM Brief 独立报告）的查询和生成接口。
生成入口复用 CNBC Closing Bell Summary 任务（报告渲染是其后置钩子），
无独立调度器。
"""

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field
from psycopg2 import errors as pg_errors

from backend.auth.jwt_utils import get_current_user
from backend.schemas.user import TokenData
from backend.services.closing_bell_report_service import ClosingBellReportService
from backend.utils.logger import get_logger

logger = get_logger("closing_bell_report_api")

router = APIRouter(prefix="/closing-bell-report", tags=["Closing Bell Report"])


class GenerateReportRequest(BaseModel):
    report_date: str | None = Field(
        None, description="ISO date string (e.g. 2026-06-11). Defaults to today."
    )
    force: bool = Field(
        False,
        description="Force re-summarize the episode before rendering the report.",
    )


@router.get("/latest")
async def get_latest_report(current_user: TokenData = Depends(get_current_user)):
    """获取最新的 Closing Bell Report。"""
    svc = ClosingBellReportService()
    try:
        report = svc.get_latest_report()
    except pg_errors.UndefinedTable:
        report = None
    if not report:
        raise HTTPException(status_code=404, detail="No completed report found")
    return report


@router.get("/list")
async def list_reports(
    limit: int = 20,
    current_user: TokenData = Depends(get_current_user),
):
    """获取报告历史列表。"""
    svc = ClosingBellReportService()
    return svc.list_reports(limit=limit)


@router.get("/shared/{token}", response_class=HTMLResponse)
async def get_shared_report(token: str):
    """通过 share_token 公共访问报告 HTML（无需认证）。"""
    svc = ClosingBellReportService()
    html = svc.get_report_html_by_token(token)
    if not html:
        raise HTTPException(status_code=404, detail="Report not found")
    return HTMLResponse(content=html)


@router.post("/generate")
async def generate_report(
    request: GenerateReportRequest = GenerateReportRequest(),
    current_user: TokenData = Depends(get_current_user),
):
    """触发 Closing Bell Report 生成（异步 Celery 任务）。

    报告内容来源于 CNBC Closing Bell Summary，因此这里转发摘要任务：
    - 摘要缺失/占位 → 重新生成摘要；
    - 摘要已存在 → 任务内幂等跳过摘要，后置钩子按新鲜度补渲染报告。
    """
    from backend.tasks.cnbc_closing_bell_tasks import generate_cnbc_closing_bell_task

    task = generate_cnbc_closing_bell_task.delay(
        report_date=request.report_date,
        force=request.force,
    )
    return {
        "success": True,
        "message": "Report generation queued",
        "celery_task_id": task.id,
    }
