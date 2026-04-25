# -*- coding: utf-8 -*-
"""
Daily 聚合视图 schema.

Daily 模块把两条数据链路（Upload Files + Feeds 订阅）合并成同一个统一的
按时间排序的信息流卡片视图（参考 AI Notes Daily 的 Summaries 页面）。

数据源:
    - file_processing_details + file_tags          (Upload 链路)
    - subscription_content + subscription_content_tags  (Feeds 链路)

详见: docs/instructions/UPLOAD_FILES_AND_LLM_ANALYSIS_FLOW.md
      docs/instructions/FEEDS_MODULE.md
"""

from datetime import datetime

from backend.utils.datetime_utils import UtcDateTime
from typing import List, Literal, Optional

from pydantic import BaseModel, Field


SourceType = Literal["upload", "feed"]

# Upload 子类型对应 file_processing_details.file_type
# Feed 子类型对应 subscription_content.channel_type
SourceSubtype = Literal[
    # Upload
    "document",  # PDF / Word / Excel / CSV
    "image",
    "audio",
    "youtube",  # Upload YouTube Link
    "web",  # Upload Web Link
    "text",  # My Notes / Thoughts
    # Feed
    "twitter",
    "news",
    "youtube_feed",  # 区分 Upload youtube 与 Feed youtube
]


class DailyNavigate(BaseModel):
    """卡片点击跳转目标信息（前端依此构造路由）."""

    type: Literal["execution", "feed_article", "external_url"] = Field(
        ..., description="跳转类型: execution=Sources 详情, feed_article=Feeds 详情, external_url=外链"
    )
    execution_id: Optional[int] = Field(None, description="processing_stats.id (Upload)")
    subscription_id: Optional[int] = Field(None, description="subscription_list.id (Feed)")
    content_id: Optional[int] = Field(None, description="subscription_content.id (Feed)")
    external_url: Optional[str] = Field(None, description="原始链接（Twitter / YouTube 等）")


class DailyFeedItem(BaseModel):
    """聚合后的单条信息流卡片."""

    id: str = Field(..., description="复合 ID: upload_<id> 或 feed_<id>")
    source: SourceType = Field(..., description="来源大类")
    source_subtype: SourceSubtype = Field(..., description="来源子类型")

    title: str = Field(..., description="标题")
    summary: Optional[str] = Field(None, description="LLM 生成摘要 (50-200 字)")
    snippet: Optional[str] = Field(None, description="无摘要时回退展示的内容片段")

    author_name: Optional[str] = Field(None, description="作者展示名 (Feed 用)")
    author_identifier: Optional[str] = Field(None, description="作者标识 (Feed 用)")

    sector: Optional[str] = Field(None, description="GICS 一级行业 (中文)")
    industry: Optional[str] = Field(None, description="GICS 子行业 (中文)")
    tickers: List[str] = Field(default_factory=list, description="股票代码列表")
    concepts: List[str] = Field(default_factory=list, description="投资主题/概念")
    confidence: Optional[float] = Field(None, description="LLM 置信度 0-1")

    published_at: UtcDateTime = Field(..., description="发布/创建时间 (UTC), 序列化时带 +00:00")
    reading_time_min: Optional[int] = Field(None, description="预估阅读时长（分钟）")

    navigate: DailyNavigate = Field(..., description="点击跳转目标")


class DailyFeedResponse(BaseModel):
    """分页响应."""

    items: List[DailyFeedItem] = Field(..., description="当前页卡片列表，按 published_at 倒序")
    total: int = Field(..., description="过滤条件下的总条数")
    has_more: bool = Field(..., description="是否还有下一页")
    limit: int = Field(..., description="当前页大小")
    offset: int = Field(..., description="当前页偏移")


class DailySourceCount(BaseModel):
    """单个来源类型的统计."""

    source_subtype: SourceSubtype
    count: int


class DailyStatsResponse(BaseModel):
    """Daily 页面顶部统计信息."""

    total: int = Field(..., description="过滤条件下的总条数")
    by_source: List[DailySourceCount] = Field(..., description="按来源子类型分布")


# ─── Preset (per-user) ─────────────────────────────────────────────
#
# 每个用户可在 Daily 页面通过齿轮入口配置一份 preset, 限制信息流只展示
# 选中的 source_subtype。语义约定：
#   - source_subtypes = []  → 未设置 / 等同于"展示全部"
#   - source_subtypes = [..] → 仅展示这些子类型 (chip 行也只显示这些)
#
# 持久化表: user_daily_preferences
#   见 backend/database/migrations/create_user_daily_preferences.sql

class DailyPresetResponse(BaseModel):
    """读取当前用户的 Daily preset 设置。"""

    source_subtypes: List[SourceSubtype] = Field(
        default_factory=list,
        description="勾选的 source_subtype 列表；空数组表示未设置 (展示全部)",
    )


class DailyPresetUpdate(BaseModel):
    """更新当前用户的 Daily preset 设置。"""

    source_subtypes: List[SourceSubtype] = Field(
        default_factory=list,
        description="勾选的 source_subtype 列表；传空数组表示清除 preset (展示全部)",
    )
