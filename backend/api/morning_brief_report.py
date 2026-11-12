# -*- coding: utf-8 -*-
"""
Morning Brief Report API（facts-report 晨报，纯只读）

生成体是 40-60 分钟的 LLM agent 管线，极重 —— 本报告**不设任何手动触发
端点**（无 POST /generate、无调度管理接口）。唯一生成入口是后端
task_scheduling(type=morning_brief) 定时调度；补跑走服务器 CLI。

读取: latest / status / list / shared/{token}。
"""

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import HTMLResponse
from psycopg2 import errors as pg_errors

from backend.auth.jwt_utils import get_current_user
from backend.schemas.user import TokenData
from backend.services.morning_brief_report_service import (
    MorningBriefReportService,
)
from backend.utils.datetime_utils import utcnow, window_label_date
from backend.utils.logger import get_logger

logger = get_logger("morning_brief_report_api")

router = APIRouter(prefix="/morning-brief-report", tags=["Morning Brief Report"])


@router.get("/latest")
async def get_latest_report(current_user: TokenData = Depends(get_current_user)):
    """获取最新已完成报告（含 OSS 签名分享链接）。"""
    svc = MorningBriefReportService()
    try:
        report = svc.get_latest_report()
    except pg_errors.UndefinedTable:
        report = None
    if not report:
        raise HTTPException(status_code=404, detail="No completed report found")
    report["share_url"] = svc.get_share_url(report.get("oss_object_name"))
    return report


@router.get("/status")
async def get_report_status(current_user: TokenData = Depends(get_current_user)):
    """今日报告状态 + 最新已完成报告（供前端展示生成中/失败态）。"""
    today = window_label_date(utcnow(), "Asia/Shanghai")
    svc = MorningBriefReportService()
    try:
        today_report = svc.get_report_by_date(today)
        latest = svc.get_latest_report()
    except pg_errors.UndefinedTable:
        today_report, latest = None, None
    return {
        "as_of": str(today),
        "today_report": today_report,
        "latest_report": latest,
    }


@router.get("/list")
async def list_reports(
    limit: int = 20,
    current_user: TokenData = Depends(get_current_user),
):
    """报告历史列表（元数据，不含正文）。"""
    svc = MorningBriefReportService()
    try:
        return svc.list_reports(limit=limit)
    except pg_errors.UndefinedTable:
        return []


@router.get("/shared/{token}", response_class=HTMLResponse)
async def get_shared_report(token: str):
    """通过 share_token 公共访问报告 HTML（无需认证；DB 优先、OSS 兜底）。"""
    svc = MorningBriefReportService()
    html = svc.get_report_html_by_token(token)
    if not html:
        raise HTTPException(status_code=404, detail="Report not found")
    # token 每次重新发布即轮换(complete 换新),同一 token 的 URL 内容不可变,
    # 允许浏览器缓存(报告 ~5MB,重复打开 iframe 免再拉取,消除加载闪烁)
    return HTMLResponse(content=html, headers={
        "Cache-Control": "public, max-age=3600",
    })
