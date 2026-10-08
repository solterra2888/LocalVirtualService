# -*- coding: utf-8 -*-
"""
Daily Market Report API
"""

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

from backend.auth.jwt_utils import get_current_user
from backend.schemas.user import TokenData
from backend.services.daily_market_report_service import DailyMarketReportService
from backend.utils.logger import get_logger

logger = get_logger("daily_market_report_api")

router = APIRouter(prefix="/daily-market-report", tags=["Daily Market Report"])


class GenerateReportRequest(BaseModel):
    report_date: str | None = Field(
        None, description="ISO date string (e.g. 2026-06-11). Defaults to today."
    )


@router.get("/latest")
async def get_latest_report(current_user: TokenData = Depends(get_current_user)):
    """获取最新的 Daily Market Report。"""
    svc = DailyMarketReportService()
    report = svc.get_latest_report(user_id=str(current_user.user_id))
    if not report:
        raise HTTPException(status_code=404, detail="No completed report found")
    return report


@router.get("/list")
async def list_reports(
    limit: int = 20,
    current_user: TokenData = Depends(get_current_user),
):
    """获取报告历史列表。"""
    svc = DailyMarketReportService()
    return svc.list_reports(user_id=str(current_user.user_id), limit=limit)


@router.post("/generate")
async def generate_report(
    request: GenerateReportRequest = GenerateReportRequest(),
    current_user: TokenData = Depends(get_current_user),
):
    """触发 Daily Market Report 生成（异步 Celery 任务）。"""
    from backend.tasks.daily_market_report_tasks import generate_daily_market_report_task

    user_id = str(current_user.user_id)
    task = generate_daily_market_report_task.delay(
        user_id=user_id,
        report_date=request.report_date,
    )
    return {
        "success": True,
        "message": "Report generation queued",
        "celery_task_id": task.id,
        "user_id": user_id,
    }


@router.get("/shared/{token}/temperature")
async def get_shared_temperature(token: str):
    """打开报告时刷新温度卡片。未完成的外汇 K 线来自实时价，不写入日线表。"""
    svc = DailyMarketReportService()
    cards = svc.get_temperature_cards_by_token(token)
    if cards is None:
        raise HTTPException(status_code=404, detail="Report not found")
    return {"cards": cards}


@router.get("/shared/{token}", response_class=HTMLResponse)
async def get_shared_report(token: str):
    """通过 share_token 公共访问报告 HTML（无需认证）。"""
    svc = DailyMarketReportService()
    html = svc.get_report_html_by_token(token)
    if not html:
        raise HTTPException(status_code=404, detail="Report not found")
    return HTMLResponse(content=html)
