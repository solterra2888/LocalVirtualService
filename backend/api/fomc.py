#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FOMC API - 央行 Dashboard 接口

提供美联储 FOMC 利率决议、声明/纪要 AI 概括、Fed updates 流的查询与手动刷新
"""

import json
from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Depends, Query, status
from pydantic import BaseModel, Field

from backend.auth.jwt_utils import get_current_user
from backend.utils.logger import info, error
from backend.utils.service_registry import get_service_registry
from backend.services.fed_dashboard_service import FedDashboardService

router = APIRouter()


# === 服务获取辅助函数 ===

def get_fed_dashboard_service() -> FedDashboardService:
    """获取央行 Dashboard 服务实例（单例）"""
    registry = get_service_registry()
    return registry.get_or_create_service(
        "fed_dashboard_service", FedDashboardService
    )


# === Schemas ===

class Dissenter(BaseModel):
    name: str
    direction: str = Field(..., description="hawkish | dovish | unknown")


class DecisionCard(BaseModel):
    meeting_date: Optional[str] = None
    decision_date: Optional[str] = None
    rate_low_bp: Optional[int] = None
    rate_high_bp: Optional[int] = None
    decision_direction: Optional[str] = None
    change_bp: Optional[int] = None
    vote_for: Optional[int] = None
    vote_against: Optional[int] = None
    dissenters: List[Dissenter] = []
    source_url: Optional[str] = None
    previous: Optional[Dict[str, Any]] = Field(
        None, description="上次决议（对照用）")
    eodhd_cross_check: Optional[Dict[str, Any]] = Field(
        None, description="EODHD economic_events 交叉验证值")


class SummaryInfo(BaseModel):
    document_id: Optional[int] = None
    doc_type: Optional[str] = None
    meeting_date: Optional[str] = None
    title: Optional[str] = None
    source_url: Optional[str] = None
    status: Optional[str] = None
    template_id: Optional[str] = None
    model: Optional[str] = None
    summary_content: Optional[str] = None
    prev_meeting_date: Optional[str] = None
    generated_at: Optional[str] = None


class UpdateItem(BaseModel):
    id: int
    title: str
    link: str
    category: Optional[str] = None
    is_monetary_policy: bool = False
    pub_date: Optional[str] = None


class DashboardResponse(BaseModel):
    decision: Optional[DecisionCard] = None
    statement_summary: Optional[SummaryInfo] = None
    minutes_summary: Optional[SummaryInfo] = None
    recent_meetings: List[Dict[str, Any]] = []
    updates: List[UpdateItem] = []
    updates_total: int = 0
    probabilities: Optional[Dict[str, Any]] = Field(
        None, description="加息/降息概率（ZQ 期货自算）: next_meetings 最新快照"
                          " + expectations_vs_actual 预期差对照 + method_note")


class RefreshRequest(BaseModel):
    type: str = Field(
        "poll",
        description="poll=轮询RSS+发现 / fetch=抓取单文档 / summarize=概括单文档 / "
                    "backfill=回填2026 / retry=重试failed / "
                    "probabilities=概率快照 / probabilities_backfill=回填历史概率",
    )
    document_id: Optional[int] = None
    force: bool = False


class RefreshResponse(BaseModel):
    task_id: Optional[str] = None
    dispatched: str = Field(..., description="派发的 Celery 任务名")


# === API Endpoints ===

@router.get("/dashboard", response_model=DashboardResponse,
            summary="获取央行 Dashboard 聚合数据")
async def get_dashboard(current_user=Depends(get_current_user)):
    """
    央行 Dashboard（V1 仅 Fed）聚合端点：

    - **decision**: 最近一次 FOMC 利率决议（区间/票型/异议/上次对照/EODHD 交叉验证）
    - **statement_summary / minutes_summary**: 最近声明与纪要的 AI 概括
    - **recent_meetings**: 近期会议列表（决议+文档状态）
    - **updates**: Fed updates 流（默认前 20 条）
    - **probabilities**: 未来会议概率快照 + 历史预期差对照（ZQ 期货自算）
    """
    try:
        info(f"获取央行 Dashboard: user={current_user.username}", source="fomc_api")
        svc = get_fed_dashboard_service()
        return svc.get_dashboard()
    except Exception as e:
        error(f"获取央行 Dashboard 失败: {str(e)}", source="fomc_api")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取央行 Dashboard 失败: {str(e)}",
        )


@router.get("/documents/{document_id}",
            summary="获取文档原文与概括（原文对照用）")
async def get_document(
    document_id: int,
    current_user=Depends(get_current_user),
):
    """按 ID 取 FOMC 文档：原文 + 元数据 + 关联概括（AI 概括卡片展开对照用）"""
    try:
        svc = get_fed_dashboard_service()
        doc = svc.get_document_detail(document_id)
        if not doc:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"文档不存在: {document_id}",
            )
        return doc
    except HTTPException:
        raise
    except Exception as e:
        error(f"获取文档失败: {str(e)}", source="fomc_api")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取文档失败: {str(e)}",
        )


@router.get("/updates", summary="获取 Fed updates 流（分页）")
async def get_updates(
    page: int = Query(1, ge=1, description="页码"),
    page_size: int = Query(20, ge=1, le=100, description="每页数量"),
    monetary_only: bool = Query(False, description="仅货币政策类"),
    current_user=Depends(get_current_user),
):
    """Fed press_all.xml updates 流分页查询"""
    try:
        svc = get_fed_dashboard_service()
        return svc.get_updates(page=page, page_size=page_size,
                               monetary_only=monetary_only)
    except Exception as e:
        error(f"获取 updates 失败: {str(e)}", source="fomc_api")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取 updates 失败: {str(e)}",
        )


@router.get("/probabilities", summary="单会议概率完整趋势（折线图数据源）")
async def get_probability_trend(
    meeting_date: str = Query(..., description="会议日期（会议末日 YYYY-MM-DD）"),
    days: int = Query(90, ge=7, le=365, description="趋势窗口天数"),
    current_user=Depends(get_current_user),
):
    """指定会议的概率时序（每日快照），含 p_cut/p_hold/p_hike/implied_rate"""
    try:
        svc = get_fed_dashboard_service()
        try:
            datetime.strptime(meeting_date, "%Y-%m-%d")
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="meeting_date 格式须为 YYYY-MM-DD",
            )
        return svc.get_probability_trend(meeting_date, days=days)
    except HTTPException:
        raise
    except Exception as e:
        error(f"获取概率趋势失败: {str(e)}", source="fomc_api")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取概率趋势失败: {str(e)}",
        )


@router.post("/refresh", response_model=RefreshResponse,
             summary="手动刷新（派发 Celery 任务）")
async def refresh(
    req: RefreshRequest,
    current_user=Depends(get_current_user),
):
    """
    手动触发后台任务（立即返回 task_id，前端可轮询状态）：

    - **poll**: 轮询 RSS + 发现新文档（常用）
    - **fetch**: 抓取指定 document_id
    - **summarize**: 对指定 document_id 生成/重生成概括（force 可强制）
    - **backfill**: 回填 2026 年五次会议
    - **retry**: 重试 failed/pending 文档
    - **probabilities**: 概率快照（ZQ 期货 + EFFR，忽略当日节流强制执行）
    - **probabilities_backfill**: 回填历史概率（ZQ=F 连续合约）
    """
    try:
        from backend.celery_app import app as celery_app
        task_map = {
            "poll": ("fomc_poll_updates", {}),
            "fetch": ("fomc_fetch_document",
                      {"doc_id": req.document_id}),
            "summarize": ("fomc_summarize",
                          {"doc_id": req.document_id, "force": req.force}),
            "backfill": ("fomc_backfill_2026", {}),
            "retry": ("fomc_retry_pending", {}),
            "probabilities": ("fomc_compute_probabilities", {"force": True}),
            "probabilities_backfill": ("fomc_backfill_probabilities", {}),
        }
        if req.type not in task_map:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"未知刷新类型: {req.type}，支持: {list(task_map)}",
            )
        if req.type in ("fetch", "summarize") and not req.document_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"type={req.type} 需要 document_id",
            )
        name, kwargs = task_map[req.type]
        result = celery_app.send_task(name, kwargs=kwargs)
        info(f"央行 Dashboard 刷新派发: {name} kwargs={kwargs} "
             f"task_id={result.id} user={current_user.username}",
             source="fomc_api")
        return RefreshResponse(task_id=result.id, dispatched=name)
    except HTTPException:
        raise
    except Exception as e:
        error(f"派发刷新失败: {str(e)}", source="fomc_api")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"派发刷新失败: {str(e)}",
        )
