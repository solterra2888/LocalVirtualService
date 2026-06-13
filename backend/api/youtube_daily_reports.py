# -*- coding: utf-8 -*-
"""
YouTube Daily Report API

端点:
  GET   /reports/youtube-daily              — 报告列表（分页）
  GET   /reports/youtube-daily/latest       — 最新已完成报告
  POST  /reports/youtube-daily/generate     — 手动触发生成（异步 Celery）
  GET   /reports/youtube-daily/{date}       — 指定日期报告
  GET   /reports/youtube-daily/shared/{token} — 公开分享报告（无需认证）
"""

from datetime import date, datetime
from typing import Optional

from fastapi import APIRouter, HTTPException, Depends, Query
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

from backend.auth.jwt_utils import get_current_user
from backend.schemas.user import TokenData
from backend.utils.datetime_utils import yesterday_in_tz, utcnow, window_label_date
from backend.utils.logger import get_logger

logger = get_logger("youtube_daily_reports_api")

router = APIRouter(prefix="/reports/youtube-daily", tags=["YouTube Daily Report"])


# ── Helpers ────────────────────────────────────────────────────────────────

def _get_service():
    from backend.services.youtube_daily_report_service import YouTubeDailyReportService
    return YouTubeDailyReportService()


def _parse_date(date_str: Optional[str], tz: Optional[str] = None) -> date:
    """解析 report_date; 缺省时按用户时区算"昨天" (避免依赖 server local TZ).

    详见 `backend/utils/datetime_utils.py::yesterday_in_tz`.
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


class GenerateReportRequest(BaseModel):
    report_date: Optional[str] = Field(
        None,
        description="日期 YYYY-MM-DD；不传则按 tz 取当前时刻并生成过去 24h 滚动窗口报告",
    )
    tz: Optional[str] = Field(None, description="IANA 时区名（如 'America/Los_Angeles'），用于计算默认日期；不传则用 UTC")
    force: bool = Field(False, description="强制重新生成（覆盖已有报告）")


@router.post("/generate", summary="手动触发报告生成")
async def generate_report(
    body: GenerateReportRequest,
    current_user: TokenData = Depends(get_current_user),
):
    """触发 Celery 异步任务为当前用户生成 YouTube Daily Report。"""
    try:
        window_end_iso = None
        if body.report_date:
            parsed_date = _parse_date(body.report_date, body.tz)
        else:
            window_end = utcnow()
            parsed_date = window_label_date(window_end, body.tz)
            window_end_iso = window_end.isoformat()

        from backend.tasks.report_tasks import generate_youtube_daily_report_task
        task = generate_youtube_daily_report_task.delay(
            user_id=current_user.user_id,
            report_date=str(parsed_date),
            force=body.force,
            window_end=window_end_iso,
        )
        logger.info(
            f"用户 {current_user.username} 触发 YouTube Daily Report 生成: "
            f"date={parsed_date}, force={body.force}, window_end={window_end_iso}, task_id={task.id}"
        )
        return {
            "success": True,
            "data": {
                "task_id": task.id,
                "report_date": str(parsed_date),
                "window_end": window_end_iso,
            },
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


# ── Public Share Endpoint (no authentication required) ──────────────────────

@router.get("/shared/{token}", response_class=HTMLResponse, summary="公开分享报告")
async def get_shared_youtube_report(token: str):
    """
    通过 share_token 公开访问 HTML 报告（无需认证）。

    用于 iframe 嵌入或直接分享链接。
    """
    try:
        service = _get_service()
        html_bytes = service.get_report_html_by_token(token)
        if not html_bytes:
            raise HTTPException(status_code=404, detail="报告不存在或未生成")
        return HTMLResponse(
            content=html_bytes.decode("utf-8"),
            headers={
                # 禁止缓存，确保用户每次都能看到最新内容
                "Cache-Control": "no-cache, no-store, must-revalidate",
                "Pragma": "no-cache",
                "Expires": "0",
                "Content-Type": "text/html; charset=utf-8",
            },
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取分享报告失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
