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
from typing import Any, Dict, List, Literal, Optional

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
    "podcast",  # Apple Podcast Episode (Upload)
    # Feed
    "twitter",
    "news",
    "youtube_feed",  # 区分 Upload youtube 与 Feed youtube
]


class DailyShareRecipient(BaseModel):
    """Upload 文件分享给的用户 (仅 owner 视角 Daily 卡片用)."""

    user_id: int = Field(..., description="被分享用户 id")
    username: str = Field(..., description="被分享用户名")
    avatar_url: Optional[str] = Field(
        None, description="头像可访问 URL (后端已 resolve_avatar_url)"
    )


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
    raw_file_type: Optional[str] = Field(
        None,
        description=(
            "Upload 文件的 DB 原始 file_type（如 pdf / word / excel / csv）。"
            "仅 source='upload' 时有值；Feed 条目为 None。"
            "Agent 层用于细化 source 显示，前端无需关注此字段。"
        ),
    )

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

    # Upload 被分享者视角: shared_by_* 非空, shared_to 为空。
    # Upload owner 且曾分享: shared_to 非空, shared_by_* 为空。
    # Feed 侧 shared_by_* 为空且 shared_to 为空。
    shared_by_username: Optional[str] = Field(
        None,
        description="文件分享者用户名; 仅 source=upload 且该文件由他人共享给当前用户时非 null",
    )
    shared_by_user_id: Optional[int] = Field(
        None,
        description="文件分享者 user_id; 含义同 shared_by_username",
    )
    shared_by_avatar_url: Optional[str] = Field(
        None,
        description="文件分享者头像 URL (OSS 签名 URL 或外链); 无头像或非共享卡片时为 null",
    )
    shared_to: List[DailyShareRecipient] = Field(
        default_factory=list,
        description=(
            "当前用户为文件 owner 且曾分享给他人的收件人列表; "
            "与 Sources 左侧 share_info.shared_to 同形。被分享者视角为空列表。"
        ),
    )


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


# ─── Item Detail (drawer 预览用) ──────────────────────────────────
#
# 卡片点击后, 前端不再跳出到 Sources/Feeds, 而是在 Daily 内打开右侧抽屉预览.
# 抽屉里上半部分摘要/标签/作者/时间/跳转目标都来自 DailyFeedItem (列表已有);
# 下半部分 "原文区" 需要按 source_subtype 差异化呈现:
#   - upload  → file_processing_details.content + processing_stats.final_report
#               (image/audio/youtube/pdf 二进制资源 + asr 仍走已有
#                /api/files/original/{file_id}, 由前端按需懒加载)
#   - feed    → 与 SubscriptionContent 列表行同形 (FeedArticleBody 复用)
#               (youtube_feed transcript 仍走已有
#                /api/subscriptions/contents/{id}/transcript, 由前端按需懒加载)
#
# 详见 docs/instructions/DAILY_MODULE.md §1 (改造后的卡片点击导航).


class DailyItemDetailUpload(BaseModel):
    """Upload 卡片点击后, 抽屉原文区所需的额外字段."""

    file_id: int = Field(..., description="file_processing_details.id")
    file_type_raw: Optional[str] = Field(
        None,
        description=(
            "file_processing_details.file_type 原始值 (pdf/word/excel/csv/"
            "image/audio/youtube/web/text), 用于前端区分 PDF / 图片 / 音频等"
        ),
    )
    display_name: Optional[str] = Field(None, description="展示名 (优先 display_name, 兜底 filename)")
    filename: Optional[str] = Field(None, description="原始 filename")
    original_file_path: Optional[str] = Field(
        None,
        description=(
            "原始路径 (本地路径 / OSS object key / 外部 URL 兜底)。"
            "前端拼接预览资源时一般用 file_url, 这里仅作元信息展示."
        ),
    )
    content: Optional[str] = Field(
        None,
        description="file_processing_details.content (Markdown 正文 / OCR 文本 / 网页正文 / 笔记)",
    )
    final_report: Optional[str] = Field(
        None,
        description="processing_stats.final_report (执行级 AI 分析报告; 多文件任务为整组报告)",
    )
    tags: Optional[Dict[str, Any]] = Field(
        None,
        description=(
            "AI 标签轻量结构 (file_tags LEFT JOIN): "
            "sector, sector_en, industry, industry_en, tickers[], concepts[], "
            "people[], countries[], tag_schema_version, summary, confidence"
        ),
    )


class DailyItemDetailFeed(BaseModel):
    """Feed 卡片点击后, 抽屉原文区所需的额外字段.

    形态与 SubscriptionContent 列表行对齐, 方便前端复用 FeedArticleBody / FeedTweetContent.
    Transcript (YouTube) 不在此返回, 走 GET /api/subscriptions/contents/{id}/transcript.
    """

    content_db_id: int = Field(..., description="subscription_content.id")
    channel_type: str = Field(..., description="twitter / youtube / news / rss")
    content_id: Optional[str] = Field(
        None, description="平台原生 ID (Twitter tweet_id / YouTube video_id / news guid)"
    )
    title: Optional[str] = None
    content: Optional[str] = Field(
        None,
        description="正文 (Twitter 推文文本 / YouTube 描述 / News 文章正文)",
    )
    author_name: Optional[str] = None
    author_identifier: Optional[str] = None
    url: Optional[str] = Field(None, description="原始外链 (View on Twitter / Read original)")
    media_urls: Optional[List[str]] = None
    metadata: Optional[Dict[str, Any]] = Field(
        None,
        description=(
            "平台特定 JSONB: Twitter (like_count/...) / YouTube (view_count/...) / "
            "News (symbols/sentiment/...)"
        ),
    )
    categories: Optional[List[str]] = Field(None, description="News 类别 / RSS tags")

    # Twitter 特定
    is_retweet: Optional[bool] = None
    retweeted_author: Optional[str] = None
    retweeted_content: Optional[str] = None
    retweeted_url: Optional[str] = None
    is_quote: Optional[bool] = None
    quoted_author: Optional[str] = None
    quoted_content: Optional[str] = None
    quoted_url: Optional[str] = None
    is_reply: Optional[bool] = None
    in_reply_to_id: Optional[str] = None
    in_reply_to_username: Optional[str] = None
    in_reply_to_user_id: Optional[str] = None
    heat_level: Optional[int] = None

    # YouTube
    has_transcript: bool = Field(
        False, description="是否有可用 transcript / transcript_data, 前端按需懒加载正文"
    )

    # AI 标签 lite (与 subscription_content_tags LEFT JOIN 同形, 未打标 = None)
    tags: Optional[Dict[str, Any]] = Field(
        None,
        description=(
            "AI 标签轻量结构: {sector, sector_en, industry, industry_en, "
            "tickers[], concepts[], people[], countries[], tag_schema_version, "
            "summary, confidence, tagged_at}"
        ),
    )

    # PR-D1: per-user 个性化摘要 (subscription_content_user_summaries)
    # 抽屉前端 "AI 深度分析" 段消费; None = 该用户对本条 feed 还没生成个性化.
    personalized_summary: Optional[str] = Field(
        None,
        description=(
            "subscription_content_user_summaries.summary, per-user 个性化摘要正文; "
            "由 personalize_feed_summary_task 写入 (用户开启自动 fan-out 后随 tag 完成自动生成)"
        ),
    )
    personalized_template_id: Optional[int] = Field(
        None,
        description="生成时使用的 llm_prompt_templates.id (feed_summary / youtube_summary 模块)",
    )
    personalized_at: Optional[str] = Field(
        None,
        description="ISO 8601 时间戳 (subscription_content_user_summaries.tagged_at)",
    )


class DailyItemDetail(DailyFeedItem):
    """卡片完整详情 (列表 DailyFeedItem 字段 + 来源专属原文区字段).

    upload / feed 二选一非空 (与 source 字段一致):
      - source == 'upload' → upload 必非空, feed 为 None
      - source == 'feed'   → feed   必非空, upload 为 None
    """

    upload: Optional[DailyItemDetailUpload] = None
    feed: Optional[DailyItemDetailFeed] = None


# ─── Calendar (日期导航栏) ─────────────────────────────────────────
#
# 新增 GET /api/daily/calendar，为前端左侧日期导航栏提供按自然日聚合的统计数据。
# 按用户时区切桶，包含最近 N 天的每日总数与各 source_subtype 分布。
# 空白日（0 条）由后端补齐，使前端可感知"今天没有新内容"。
#
# 详见: docs/instructions/DAILY_MODULE.md §4 (API 表格)


class DailyCalendarDaySource(BaseModel):
    """某天某类来源的统计."""

    source_subtype: SourceSubtype
    count: int


class DailyCalendarDay(BaseModel):
    """日历中单天的统计信息."""

    date: str = Field(..., description="YYYY-MM-DD 格式 (按用户时区切桶)")
    total: int = Field(..., description="当天总条数（0 = 空白日）")
    by_source: List[DailyCalendarDaySource] = Field(
        default_factory=list,
        description="各 source_subtype 分布，按 count 降序，0 条类型省略",
    )


class DailyCalendarResponse(BaseModel):
    """GET /api/daily/calendar 响应."""

    tz: str = Field(..., description="实际使用的时区 (IANA, 非法时回退 UTC)")
    days: int = Field(..., description="实际返回的天数")
    items: List[DailyCalendarDay] = Field(
        ...,
        description="按日期倒序排列的日历数据（含 0 条空白日），长度 == days",
    )
    max_total: int = Field(
        ..., description="窗口内单日最大条数，用于前端活跃度等级归一化"
    )
    total_in_window: int = Field(..., description="窗口内总条数")
