# -*- coding: utf-8 -*-
"""
Twitter Daily Report API

端点:
  GET   /reports/twitter-daily              — 报告列表（分页）
  GET   /reports/twitter-daily/latest       — 最新已完成报告
  GET   /reports/twitter-daily/{date}       — 指定日期报告
  POST  /reports/twitter-daily/generate     — 手动触发生成（异步 Celery）
"""

from datetime import date, datetime, timedelta
from typing import Optional

from fastapi import APIRouter, HTTPException, Depends, Query
from pydantic import BaseModel, Field

from backend.auth.jwt_utils import get_current_user
from backend.schemas.user import TokenData
from backend.utils.datetime_utils import yesterday_in_tz
from backend.utils.logger import get_logger

logger = get_logger("twitter_daily_reports_api")

router = APIRouter(prefix="/reports/twitter-daily", tags=["Twitter Daily Report"])


# ── Helpers ────────────────────────────────────────────────────────────────

def _get_service():
    from backend.services.twitter_daily_report_service import TwitterDailyReportService
    return TwitterDailyReportService()


def _parse_date(date_str: Optional[str], tz: Optional[str] = None) -> date:
    """解析 report_date 字符串; 缺省时按用户时区取"昨天".

    服务器部署在 HK (UTC+8); 若用 `date.today()` 取 server local, 美西用户深夜时段
    会得到错位 1 天的报告. 现在显式接受 `tz` (IANA 名), 缺省回退 UTC.
    """
    if not date_str:
        return yesterday_in_tz(tz)
    try:
        return datetime.strptime(date_str, '%Y-%m-%d').date()
    except ValueError:
        raise HTTPException(status_code=400, detail="日期格式错误，请使用 YYYY-MM-DD")


# ── Endpoints ──────────────────────────────────────────────────────────────

@router.get("", summary="获取报告列表")
async def list_reports(
    limit: int = Query(default=30, le=100, description="每页条数"),
    offset: int = Query(default=0, ge=0, description="偏移量"),
    current_user: TokenData = Depends(get_current_user),
):
    """获取当前用户的 Twitter Daily Report 列表（分页，按日期倒序）。"""
    try:
        service = _get_service()
        reports, total = service.get_user_reports(current_user.user_id, limit, offset)
        return {
            "success": True,
            "data": {"reports": reports, "total": total, "limit": limit, "offset": offset},
        }
    except Exception as e:
        logger.error(f"获取报告列表失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/latest", summary="获取最新报告")
async def get_latest_report(
    current_user: TokenData = Depends(get_current_user),
):
    """获取当前用户最新的已完成报告。"""
    try:
        service = _get_service()
        report = service.get_latest_report(current_user.user_id)
        if not report:
            return {"success": True, "data": None, "message": "暂无报告"}
        return {"success": True, "data": report}
    except Exception as e:
        logger.error(f"获取最新报告失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


class GenerateReportRequest(BaseModel):
    report_date: Optional[str] = Field(None, description="日期 YYYY-MM-DD，默认按 tz 算昨天")
    tz: Optional[str] = Field(None, description="IANA 时区名（如 'America/Los_Angeles'），用于计算默认日期；不传则用 UTC")
    force: bool = Field(False, description="强制重新生成（覆盖已有报告）")


@router.post("/generate", summary="手动触发报告生成")
async def generate_report(
    body: GenerateReportRequest,
    current_user: TokenData = Depends(get_current_user),
):
    """触发 Celery 异步任务为当前用户生成 Twitter Daily Report。"""
    try:
        parsed_date = _parse_date(body.report_date, body.tz)
        from backend.tasks.report_tasks import generate_twitter_daily_report_task
        task = generate_twitter_daily_report_task.delay(
            user_id=current_user.user_id,
            report_date=str(parsed_date),
            force=body.force,
        )
        logger.info(
            f"用户 {current_user.username} 触发 Twitter Daily Report 生成: "
            f"date={parsed_date}, force={body.force}, task_id={task.id}"
        )
        return {
            "success": True,
            "data": {"task_id": task.id, "report_date": str(parsed_date)},
            "message": "报告生成任务已提交",
        }
    except Exception as e:
        logger.error(f"触发报告生成失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{report_date}", summary="获取指定日期报告")
async def get_report_by_date(
    report_date: str,
    current_user: TokenData = Depends(get_current_user),
):
    """获取指定日期的 Twitter Daily Report（含完整内容）。"""
    try:
        parsed_date = _parse_date(report_date)
        service = _get_service()
        report = service.get_report_by_date(current_user.user_id, parsed_date)
        if not report:
            return {"success": True, "data": None, "message": "该日期无报告"}
        return {"success": True, "data": report}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取报告失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
