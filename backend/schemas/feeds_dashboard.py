# -*- coding: utf-8 -*-
"""
Feeds Dashboard (按主题聚合的重要信息视图) 请求/响应模型.

后端: backend/api/feeds_dashboard.py (prefix=/api/feeds-dashboard)
读取层: feed_topics + feed_topic_daily_items + subscription_content(_tags)
前端: web-frontend/src/pages/FeedsDashboardPage.tsx + services/feedsDashboard.ts
"""

from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, Field

from backend.schemas.daily import DailyNavigate


class FeedsDashboardItem(BaseModel):
    """主题下的单条内容卡片 (DailyPreviewDrawer 兼容复合 ID)."""

    id: str = Field(..., description="复合 ID: feed_<subscription_content_id> (Drawer 拉详情复用 /api/daily/items)")
    source: str = Field("feed", description="来源大类 (Dashboard 仅 Feed 链路)")
    source_subtype: str = Field(..., description="来源子类型 (与 Daily 口径一致: twitter/news/youtube_feed/...)")
    channel_type: str = Field(..., description="DB 原始 channel_type")
    title: str = Field(..., description="标题")
    summary: Optional[str] = Field(None, description="LLM 单句摘要 (subscription_content_tags.summary)")
    author_name: Optional[str] = Field(None, description="作者展示名")
    author_identifier: Optional[str] = Field(None, description="作者标识")
    author_avatar: Optional[str] = Field(None, description="作者头像 URL (已解析)")
    tickers: List[str] = Field(default_factory=list, description="关联 ticker")
    importance: str = Field(..., description="high/medium/low")
    score: float = Field(..., description="排序数值分 (0-1)")
    published_at: datetime = Field(..., description="发布时间 (aware UTC)")
    url: Optional[str] = Field(None, description="原文外链")
    in_user_scope: bool = Field(
        True,
        description="是否在当前用户订阅范围内; server 视图下 false 的条目点击走原文外链 (抽屉详情有用户鉴权)",
    )
    navigate: Optional[DailyNavigate] = Field(None, description="跳转目标 (Open in Feeds / 外链)")


class FeedsDashboardTopic(BaseModel):
    """主题分组 (含该用户当日可见条目统计 + top 条目)."""

    topic_id: int
    topic_key: str
    name_cn: str
    name_en: Optional[str] = None
    category: Optional[str] = None
    needs_review: bool = Field(False, description="auto 新建主题待管理员确认")
    item_count: int = Field(..., description="该用户当日可见条目数 (min_importance 过滤后)")
    high_count: int = Field(..., description="high 条目数")
    medium_count: int = Field(..., description="medium 条目数")
    top_tickers: List[str] = Field(default_factory=list, description="主题内出现频次最高的 ticker")
    items: List[FeedsDashboardItem] = Field(default_factory=list, description="按 score 倒序的 top 条目")
    has_more: bool = Field(False, description="是否还有更多条目 (item_count > len(items))")


class FeedsDashboardResponse(BaseModel):
    """GET /api/feeds-dashboard 主响应."""

    date: str = Field(..., description="报告日期标签 (北京自然日, YYYY-MM-DD)")
    tz: str = Field("Asia/Shanghai", description="report_date 标签时区")
    min_importance: Optional[str] = Field(None, description="当前 importance 过滤")
    topics: List[FeedsDashboardTopic] = Field(default_factory=list)
    total_items: int = Field(0, description="所有主题条目数合计 (去重前按主题计数)")
    unique_content_count: int = Field(0, description="去重后的内容条数")
    aggregated_at: Optional[datetime] = Field(None, description="当日快照最近一次聚合时间 (无=None, 提示需先生成)")


class FeedsDashboardTopicItemsResponse(BaseModel):
    """GET /topics/{topic_id}/items 分页响应."""

    topic_id: int
    topic_name: str
    date: str
    items: List[FeedsDashboardItem]
    total: int
    has_more: bool
    limit: int
    offset: int


class FeedsDashboardGenerateRequest(BaseModel):
    """POST /generate 手动触发聚合 (admin)."""

    date: Optional[str] = Field(None, description="指定日期 YYYY-MM-DD (北京自然日); 缺省=最近 days 天")
    days: Optional[int] = Field(None, description="date 为空时聚合最近 N 天 (默认 2, 上限 30)")
    force: bool = Field(False, description="P1 Phase A 预留: 跳过摘要缓存强制重生成")


class FeedsDashboardGenerateResponse(BaseModel):
    success: bool
    celery_task_id: Optional[str] = None
    message: str


class FeedsDashboardWord(BaseModel):
    """词云单个词 (concepts 词频 / ticker 词频 / watchlist 符号)."""

    word: str = Field(..., description="词文本 (折并后为主题名 / 原始散词 / ticker / watchlist symbol)")
    count: int = Field(..., description="日期窗口内该用户可见内容中的出现条数 (watchlist 可为 0)")
    type: str = Field("concept", description="concept | ticker | watchlist")
    topic_id: Optional[int] = Field(
        None,
        description="折并到的主题 id (点击词云走 topic 过滤, 命中该主题全部变体); 散词/ticker 为 None",
    )


class FeedsDashboardWordCloudResponse(BaseModel):
    """GET /wordcloud 词频响应."""

    date: str
    words: List[FeedsDashboardWord] = Field(default_factory=list)
    watchlist: List[FeedsDashboardWord] = Field(
        default_factory=list,
        description="用户 watchlist 符号 (target_tracking_config) + 当日命中条数; 0 条也返回",
    )
    unique_content_count: int = Field(0, description="参与词频统计的内容条数 (去重)")


class FeedsDashboardItemsResponse(BaseModel):
    """GET /items 扁平条目列表响应 (词过滤 / 全部)."""

    date: str
    word: Optional[str] = None
    word_type: Optional[str] = None
    items: List[FeedsDashboardItem] = Field(default_factory=list)
    total: int = 0
    has_more: bool = False
    limit: int = 20
    offset: int = 0
