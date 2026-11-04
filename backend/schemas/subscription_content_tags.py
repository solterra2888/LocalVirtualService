# -*- coding: utf-8 -*-
"""
Feeds 内容标签 (subscription_content_tags) 相关 Pydantic Schema

对标 backend/schemas/file_tags.py, 字段对齐 + 额外 summary 字段.
详见 docs/instructions/FEEDS_AUTO_TAGGING_PLAN.md
"""
from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from backend.utils.datetime_utils import UtcDateTime


class SubscriptionContentTagsUpdate(BaseModel):
    """手动编辑 subscription_content_tags 的请求体 (所有字段可选)."""

    sector: Optional[str] = Field(None, description="行业分类 (中文)")
    sector_en: Optional[str] = Field(None, description="行业分类 (英文, GICS)")
    industry: Optional[str] = Field(None, description="子行业分类 (中文)")
    industry_en: Optional[str] = Field(None, description="子行业分类 (英文, GICS)")
    tickers: Optional[List[str]] = Field(None, description="股票代码列表")
    concepts: Optional[List[str]] = Field(None, description="投资概念列表")
    people: Optional[List[Dict[str, Any]]] = Field(
        None, description="关键人物 [{name, name_en, role, affiliation}]"
    )
    countries: Optional[List[str]] = Field(
        None, description="相关国家 ISO 3166-1 alpha-2 代码"
    )
    confidence: Optional[float] = Field(None, ge=0, le=1, description="置信度 (0-1)")
    reasoning: Optional[str] = Field(None, description="推理说明")
    summary: Optional[str] = Field(None, description="50-200 字短摘要")
    info_level: Optional[str] = Field(
        None,
        pattern=r"^(L1|L2|L3|L4)$",
        description="候选重要度 L1-L4 (对内容所聚焦标的/行业的投资判断冲击潜力)",
    )
    evidence_level: Optional[str] = Field(
        None,
        pattern=r"^(E1|E2|E3)$",
        description="证据等级 E1-E3 (来源属性与可独立核验性)",
    )
    additional_tags: Optional[Dict[str, Any]] = Field(None, description="扩展标签 (merge 写入)")


class SubscriptionContentTagsResponse(BaseModel):
    """单条 subscription_content_tags 响应体."""

    id: int
    subscription_content_id: int
    sector: str
    sector_en: Optional[str] = None
    industry: str
    industry_en: Optional[str] = None
    tickers: List[str] = Field(default_factory=list)
    concepts: List[str] = Field(default_factory=list)
    people: List[Dict[str, Any]] = Field(default_factory=list)
    countries: List[str] = Field(default_factory=list)
    tag_schema_version: Optional[str] = Field(
        default="feeds_v1", description="打标 schema: feeds_v1 | tweet_signal_v1"
    )
    additional_tags: Dict[str, Any] = Field(default_factory=dict)
    confidence: Optional[float] = None
    reasoning: Optional[str] = None
    summary: Optional[str] = None
    info_level: Optional[str] = Field(
        default=None, description="候选重要度 L1-L4; NULL = 未评级 (存量/旧模板产物)"
    )
    evidence_level: Optional[str] = Field(
        default=None, description="证据等级 E1-E3; NULL = 未评级 (存量/旧模板产物)"
    )
    channel_type: str
    tagged_by: Optional[int] = None
    tagged_at: Optional[UtcDateTime] = None
    created_at: Optional[UtcDateTime] = None
    updated_at: Optional[UtcDateTime] = None

    class Config:
        from_attributes = True


class RetagFeedRequest(BaseModel):
    """重新打标单条 feed content."""

    force: bool = Field(default=True, description="是否强制重跑 (默认 True: 手动触发即重跑)")
    additional_context: Optional[str] = Field(default="", description="额外上下文")


class BatchRetagFeedRequest(BaseModel):
    """批量重新打标."""

    content_ids: List[int] = Field(..., description="subscription_content.id 列表")
    force: bool = Field(default=True, description="是否强制重跑")


class FeedTaggingTaskStatus(BaseModel):
    """派发 tag_feed_content_task 后返回的任务状态载体."""

    task_id: str
    status: str = "pending"
    message: Optional[str] = None
    content_id: Optional[int] = None
