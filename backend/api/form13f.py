#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Form 13F API — SEC 季度持仓接口

提供机构搜索/详情（行业分布、top20 持仓、top5 买卖）与预设机构同步
"""

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Depends, Query, status
from pydantic import BaseModel, Field

from backend.auth.jwt_utils import get_current_user
from backend.utils.logger import info, error
from backend.utils.service_registry import get_service_registry
from backend.services.form13f_service import Form13fService

router = APIRouter()


# === 服务获取辅助函数 ===

def get_form13f_service() -> Form13fService:
    """获取 Form 13F 服务实例（单例）"""
    registry = get_service_registry()
    return registry.get_or_create_service(
        "form13f_service", Form13fService
    )


# === Schemas ===

class PresetItem(BaseModel):
    id: Optional[int] = None
    label: str
    latest_quarter: Optional[str] = None
    latest_total_value: Optional[int] = None


class FilerSummary(BaseModel):
    id: int
    name: str
    latest_quarter: Optional[str] = None
    latest_total_value: Optional[int] = None


class ManifestResponse(BaseModel):
    quarters: List[str] = Field(..., description="已有快照的季度标签，如 ['2025Q2', ...]")
    presets: List[PresetItem] = Field(..., description="预设机构（快捷选择）")
    top_filers: List[FilerSummary] = Field(
        ..., description="规模 TopN 机构摘要（全量搜索走 /filers）")


class FilerSearchItem(FilerSummary):
    cik: Optional[str] = None
    is_preset: bool = False


class FilerSearchResponse(BaseModel):
    items: List[FilerSearchItem]
    total: int
    page: int
    page_size: int


class QuarterSnapshot(BaseModel):
    quarter: str
    total_value: Optional[int] = None
    position_count: Optional[int] = None
    sector_alloc: Dict[str, float] = Field(
        default_factory=dict,
        description="行业/资产分布百分比 {'Technology': 30.29}，含 ETF 9 类资产")
    top20_holdings: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="前 20 持仓 [{issuer,title,cusip,putcall,value,shares,pct}]")
    top5_buys: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="对上一有效季度市值增加最大的 5 笔（含 new）")
    top5_sells: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="市值减少最大的 5 笔（含 exited）")


class FilerDetailResponse(BaseModel):
    id: int
    name: str
    cik: Optional[str] = None
    is_preset: bool = False
    quarters: List[QuarterSnapshot] = []


class RefreshRequest(BaseModel):
    type: str = Field(
        "poll",
        description="poll=同步20家预设机构13F-HR / reaggregate=重算快照 / "
                    "backfill|retry|import=同 poll")
    dataset_id: Optional[int] = None
    quarter: Optional[str] = None
    force: bool = False


class RefreshResponse(BaseModel):
    task_id: Optional[str] = None
    dispatched: str = Field(..., description="派发的 Celery 任务名")


# === API Endpoints ===

@router.get("/manifest", response_model=ManifestResponse,
            summary="获取 13F 仪表盘清单（季度/预设机构/TopN 摘要）")
async def get_manifest(
    top_n: int = Query(200, ge=1, le=1000, description="TopN 机构数"),
    current_user=Depends(get_current_user),
):
    """仪表盘初始数据：季度列表 + 预设机构 + 规模 TopN 机构（不全量下发）"""
    try:
        svc = get_form13f_service()
        return ManifestResponse(**svc.get_manifest(top_n=top_n))
    except Exception as e:
        error(f"获取 13F manifest 失败: {str(e)}", source="form13f_api")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取 13F manifest 失败: {str(e)}",
        )


@router.get("/filers", response_model=FilerSearchResponse,
            summary="机构搜索（分页）")
async def search_filers(
    q: str = Query(..., min_length=1, max_length=100, description="机构名关键词"),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    current_user=Depends(get_current_user),
):
    """按机构名 ILIKE 搜索，按组合规模降序分页"""
    try:
        svc = get_form13f_service()
        return FilerSearchResponse(**svc.search_filers(q, page, page_size))
    except Exception as e:
        error(f"搜索 13F 机构失败: {str(e)}", source="form13f_api")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"搜索 13F 机构失败: {str(e)}",
        )


@router.get("/filers/{filer_id}", response_model=FilerDetailResponse,
            summary="机构详情（全季度快照）")
async def get_filer_detail(
    filer_id: int,
    current_user=Depends(get_current_user),
):
    """单个机构全部季度快照：行业分布 / top20 持仓 / top5 买卖"""
    try:
        svc = get_form13f_service()
        result = svc.get_filer_detail(filer_id)
        if not result:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"机构 id={filer_id} 不存在",
            )
        return FilerDetailResponse(**result)
    except HTTPException:
        raise
    except Exception as e:
        error(f"获取 13F 机构详情失败: {str(e)}", source="form13f_api")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取 13F 机构详情失败: {str(e)}",
        )


@router.post("/refresh", response_model=RefreshResponse,
             summary="手动刷新（派发 Celery 任务）")
async def refresh(
    req: RefreshRequest,
    current_user=Depends(get_current_user),
):
    """
    手动触发后台任务（立即返回 task_id）：

    - **poll**: 按 20 家预设 CIK 拉 13F-HR（含 amendment 折叠与快照）
    - **reaggregate**: 重算快照（可带 quarter；不带则全部季度）
    - **backfill / retry / import**: 与 poll 相同
    """
    try:
        from backend.celery_app import app as celery_app
        task_map = {
            "poll": ("form13f_poll", {}),
            "import": ("form13f_poll", {}),
            "reaggregate": ("form13f_aggregate", {"quarter": req.quarter}),
            "backfill": ("form13f_poll", {}),
            "retry": ("form13f_poll", {}),
        }
        if req.type not in task_map:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"未知刷新类型: {req.type}，支持: {list(task_map)}",
            )
        name, kwargs = task_map[req.type]
        result = celery_app.send_task(name, kwargs=kwargs)
        info(f"Form 13F 刷新派发: {name} kwargs={kwargs} "
             f"task_id={result.id} user={current_user.username}",
             source="form13f_api")
        return RefreshResponse(task_id=result.id, dispatched=name)
    except HTTPException:
        raise
    except Exception as e:
        error(f"派发 13F 刷新失败: {str(e)}", source="form13f_api")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"派发 13F 刷新失败: {str(e)}",
        )
