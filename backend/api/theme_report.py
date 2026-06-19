# -*- coding: utf-8 -*-
"""
Theme Report API Routes

GET  /theme-report/list      — 返回当前用户的 Theme Report 历史列表（含 OSS 签名 URL）
GET  /theme-report/latest    — 返回最新一份已完成报告
POST /theme-report/generate  — 手动触发生成（异步 Celery 任务）
"""

from fastapi import APIRouter, HTTPException, Depends, Query
from fastapi.responses import HTMLResponse, Response
from typing import Optional
from pydantic import BaseModel

from backend.services.theme_report_service import ThemeReportService
from backend.utils.logger import get_logger
from backend.auth.jwt_utils import get_current_user
from backend.schemas.user import TokenData

logger = get_logger("theme_report_api")

router = APIRouter(tags=["Theme Report"])


class GenerateRequest(BaseModel):
    report_date: Optional[str] = None  # ISO date string 'YYYY-MM-DD', defaults to today


class BackfillRequest(BaseModel):
    days_back: int = 400  # calendar days of EOD history to fetch (>= 365 for 1Y returns)


# ──────────────────────────────────────────────────────────────
# GET /theme-report/list
# ──────────────────────────────────────────────────────────────

@router.get("/theme-report/list")
async def list_theme_reports(
    limit: int = Query(20, ge=1, le=100, description="Max number of reports to return"),
    current_user: TokenData = Depends(get_current_user),
):
    """
    返回当前用户的 Theme Report 历史列表，最新在前。
    每条记录包含一个 24h 有效的 OSS 签名内联 URL（适合 iframe 嵌入）。
    """
    try:
        svc = ThemeReportService()
        reports = svc.list_reports(user_id=current_user.username, limit=limit)
        return {"success": True, "data": reports, "total": len(reports)}
    except Exception as exc:
        logger.error(f"[list_theme_reports] Error for user {current_user.username}: {exc}")
        raise HTTPException(status_code=500, detail=f"Failed to list reports: {exc}")


# ──────────────────────────────────────────────────────────────
# GET /theme-report/latest
# ──────────────────────────────────────────────────────────────

@router.get("/theme-report/latest")
async def get_latest_theme_report(
    current_user: TokenData = Depends(get_current_user),
):
    """
    返回当前用户最新一份已完成的 Theme Report（含签名内联 URL）。
    若无报告则返回 404。
    """
    try:
        svc = ThemeReportService()
        report = svc.get_latest_report(user_id=current_user.username)
        if not report:
            raise HTTPException(
                status_code=404,
                detail="No completed Theme Report found. "
                       "Tag themes with 'Theme Report' and trigger generation.",
            )
        return {"success": True, "data": report}
    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"[get_latest_theme_report] Error for user {current_user.username}: {exc}")
        raise HTTPException(status_code=500, detail=f"Failed to get latest report: {exc}")


# ──────────────────────────────────────────────────────────────
# POST /theme-report/generate
# ──────────────────────────────────────────────────────────────

@router.post("/theme-report/generate")
async def generate_theme_report(
    request: GenerateRequest = GenerateRequest(),
    current_user: TokenData = Depends(get_current_user),
):
    """
    手动触发为当前用户生成 Theme Report。

    - 如不传 report_date，默认使用今天的日期。
    - 任务通过 Celery 异步执行；接口立即返回任务 ID。
    - 若需同步生成（测试/回填），可直接调用服务层。
    """
    try:
        from backend.tasks.theme_report_tasks import generate_all_theme_reports_task

        task = generate_all_theme_reports_task.apply_async(
            kwargs={
                "user_id": current_user.username,
                "report_date": request.report_date,
            }
        )

        logger.info(
            f"[generate_theme_report] Queued for user={current_user.username}, "
            f"date={request.report_date or 'today'}, task_id={task.id}"
        )

        return {
            "success": True,
            "message": "Theme Report generation queued.",
            "task_id": task.id,
            "user_id": current_user.username,
            "report_date": request.report_date or "today",
        }
    except Exception as exc:
        logger.error(f"[generate_theme_report] Error for user {current_user.username}: {exc}")
        raise HTTPException(status_code=500, detail=f"Failed to queue report generation: {exc}")


# ──────────────────────────────────────────────────────────────
# GET /theme-report/shared/{token}  — public, no auth required
# ──────────────────────────────────────────────────────────────

@router.get("/theme-report/shared/{token}", response_class=HTMLResponse)
async def get_shared_theme_report(token: str):
    """
    Public endpoint — no authentication required.

    Returns the raw HTML of the completed Theme Report identified by
    its share_token, streaming it directly from the private OSS bucket
    so it renders inline in any browser.

    Returns 404 if the token is unknown or the report is not yet completed.
    """
    try:
        svc = ThemeReportService()
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
        logger.error(f"[get_shared_theme_report] Error for token={token[:8]}…: {exc}")
        raise HTTPException(status_code=500, detail="Failed to retrieve shared report.")


# ──────────────────────────────────────────────────────────────
# GET /theme-report/shared/{token}/csv  — public, no auth required
# ──────────────────────────────────────────────────────────────

@router.get("/theme-report/shared/{token}/csv")
async def get_shared_theme_report_csv(token: str):
    """
    Public endpoint — no authentication required.

    Returns the constituent-performance CSV (Download spreadsheet) for the
    completed Theme Report identified by its share_token. Mirrors the layout
    of the cntlabs reference export, with an extra `Categories` column:
        ticker, theme, Categories, 1D, 1W, 1M, 3M, 1Y

    Returns 404 if the token is unknown, the report is not completed, or its
    csv_content has not been populated yet (regenerate the report to fill it).
    """
    try:
        svc = ThemeReportService()
        csv_bytes = svc.get_report_csv_by_token(token)
        if csv_bytes is None:
            raise HTTPException(
                status_code=404,
                detail="Shared report CSV not found. Regenerate the report to populate it.",
            )
        return Response(
            content=csv_bytes,
            media_type="text/csv; charset=utf-8",
            headers={
                "Content-Disposition": 'attachment; filename="theme_constituent_performance.csv"',
                "Cache-Control": "public, max-age=3600",
            },
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"[get_shared_theme_report_csv] Error for token={token[:8]}…: {exc}")
        raise HTTPException(status_code=500, detail="Failed to retrieve shared report CSV.")


# ──────────────────────────────────────────────────────────────
# POST /theme-report/backfill
# ──────────────────────────────────────────────────────────────

@router.post("/theme-report/backfill")
async def backfill_eod_history(
    request: BackfillRequest = BackfillRequest(),
    current_user: TokenData = Depends(get_current_user),
):
    """
    触发 EOD 历史数据回填，确保 3M / 1Y 收益率有足够数据。

    - 调用已有的 fetch_eodhd_eod_stock_prices_task（days_back=400）。
    - 建议在首次生成 Theme Report 前执行一次。
    - 管理员或普通用户均可调用；任务在后台异步执行。
    """
    try:
        from backend.tasks.theme_report_tasks import backfill_theme_report_eod_history_task

        task = backfill_theme_report_eod_history_task.apply_async(
            kwargs={"days_back": request.days_back}
        )

        logger.info(
            f"[backfill_eod_history] Queued by user={current_user.username}, "
            f"days_back={request.days_back}, task_id={task.id}"
        )

        return {
            "success": True,
            "message": f"EOD history backfill queued (days_back={request.days_back}).",
            "task_id": task.id,
            "days_back": request.days_back,
        }
    except Exception as exc:
        logger.error(f"[backfill_eod_history] Error: {exc}")
        raise HTTPException(status_code=500, detail=f"Failed to queue backfill: {exc}")
