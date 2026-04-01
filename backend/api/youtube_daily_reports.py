# -*- coding: utf-8 -*-
"""
YouTube Daily Report API

端点:
  GET  /reports/youtube-daily              — 报告列表（分页）
  GET  /reports/youtube-daily/latest       — 最新已完成报告
  GET  /reports/youtube-daily/{date}       — 指定日期报告

报告由 Scheduler 定时自动生成，不提供手动触发接口。
"""

from datetime import date, datetime, timedelta
from typing import Optional

from fastapi import APIRouter, HTTPException, Depends, Query

from backend.auth.jwt_utils import get_current_user
from backend.schemas.user import TokenData
from backend.utils.logger import get_logger

logger = get_logger("youtube_daily_reports_api")

router = APIRouter(prefix="/reports/youtube-daily", tags=["YouTube Daily Report"])


# ── Helpers ────────────────────────────────────────────────────────────────

def _get_service():
    from backend.services.youtube_daily_report_service import YouTubeDailyReportService
    return YouTubeDailyReportService()


def _parse_date(date_str: Optional[str]) -> date:
    if not date_str:
        return date.today() - timedelta(days=1)
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
    """获取当前用户的 YouTube Daily Report 列表（分页，按日期倒序）。"""
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


@router.get("/{report_date}", summary="获取指定日期报告")
async def get_report_by_date(
    report_date: str,
    current_user: TokenData = Depends(get_current_user),
):
    """获取指定日期的 YouTube Daily Report（含完整内容）。"""
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
