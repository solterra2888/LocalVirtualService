# -*- coding: utf-8 -*-
"""
Morning Brief Report API（facts-report 晨报，纯只读）

生成体是 40-60 分钟的 LLM agent 管线，极重 —— 本报告**不设任何手动触发
端点**（无 POST /generate、无调度管理接口）。唯一生成入口是后端
task_scheduling(type=morning_brief) 定时调度；补跑走服务器 CLI。

读取: latest / status / list / shared/{token}。
"""

import hashlib

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from fastapi.responses import HTMLResponse, JSONResponse
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
async def get_shared_report(request: Request, token: str):
    """通过 share_token 公共访问报告 HTML（无需认证；DB 优先、OSS 兜底）。"""
    svc = MorningBriefReportService()
    html = svc.get_report_html_by_token(token)
    if not html:
        raise HTTPException(status_code=404, detail="Report not found")
    # ETag 协商缓存(2026-11-21):share_token 已稳定化(complete 不再轮换,为
    # ft_refresh 10:30 补数重发布不打断已分发链接),同 token 的 URL 内容**会变**
    # —— 旧的 max-age=3600 会让用户在重发布后 1 小时内仍看到旧横幅(6.6MB 大页
    # 不能直接 no-cache 全量重拉)。ETag 内容指纹:变了 200 新页,没变 304 秒回。
    etag = hashlib.sha1(html if isinstance(html, bytes)
                        else html.encode("utf-8")).hexdigest()
    if request.headers.get("if-none-match") == etag:
        return Response(status_code=304, headers={
            "Cache-Control": "no-cache", "ETag": etag})
    return HTMLResponse(content=html, headers={
        "Cache-Control": "no-cache", "ETag": etag,
    })


@router.get("/shared/{token}/quotes")
async def get_shared_quotes(token: str):
    """报告打开时刷新 FT 表行情（实时覆盖；无需认证，token 即门禁）。

    报告内嵌 JS 对近两天的报告调用此端点就地刷新收盘/涨跌（与 DMR
    /shared/{token}/temperature 同模式）。60s 服务端共享缓存挡住匿名
    分享链接的并发；失败返回 404/5xx，前端静默保留快照。
    """
    svc = MorningBriefReportService()
    data = svc.get_live_quotes_by_token(token)
    if data is None:
        raise HTTPException(status_code=404, detail="Report or live spec not found")
    return JSONResponse(content=data, headers={"Cache-Control": "no-store"})


@router.get("/shared/{token}/kline")
async def get_shared_kline(token: str, m: str, i: int):
    """今日涨跌榜点击行 → 单只个股日 K（Yahoo OHLC ≤260 根，按需拉取）。

    m∈{us,hk,cn} + 行序 i，symbol 由服务端 metadata.rank_kline 规格决定
    （客户端不能任意传符号）。600s 进程缓存；失败 404，前端弹窗提示。
    """
    if m not in ("us", "hk", "cn") or i < 0:
        raise HTTPException(status_code=404, detail="Bad market or index")
    svc = MorningBriefReportService()
    data = svc.get_rank_kline_by_token(token, m, i)
    if data is None:
        raise HTTPException(status_code=404, detail="Kline not available")
    return JSONResponse(content=data, headers={"Cache-Control": "no-store"})
