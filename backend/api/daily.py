# -*- coding: utf-8 -*-
"""
Daily 聚合视图 API.

把两条数据链路（Upload Files + Feeds 订阅）合并成同一条按时间排序的卡片信息流，
作为登录后的默认落地页 (/daily)。设计思路参考 AI Notes Daily 的 Summaries 页面，
同时扩展支持本系统的 9 类来源（详见 schemas/daily.py 的 SourceSubtype）。

数据源:
    Upload: file_processing_details + processing_stats + file_tags
    Feed:   subscription_content + subscription_content_tags

关键 SQL: 两侧字段对齐后 UNION ALL, 按 published_at 倒序分页.

挂载方式 (main.py):
    app.include_router(daily.router, prefix="/api/daily", tags=["Daily 聚合视图"])
"""

import json
import math
import re
import shlex
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from backend.auth.jwt_utils import get_current_user
from backend.database.db_manager import DatabaseManager
from backend.utils.avatar import resolve_avatar_url as _resolve_avatar
from backend.schemas.daily import (
    DailyCalendarDay,
    DailyCalendarDaySource,
    DailyCalendarResponse,
    DailyFeedItem,
    DailyFeedResponse,
    DailyItemDetail,
    DailyItemDetailFeed,
    DailyItemDetailUpload,
    DailyNavigate,
    DailyPresetResponse,
    DailyPresetUpdate,
    DailyShareRecipient,
    DailySourceCount,
    DailyStatsResponse,
)
from backend.schemas.user import TokenData
from backend.utils.database_config import get_database_config
from backend.utils.logger import get_logger

logger = get_logger("daily_api")

router = APIRouter()

# ─── Constants ─────────────────────────────────────────────────────

# 每分钟阅读字符数（中英文混合粗略估算，与 AI Notes Daily 的 "X min read" 对齐）
WORDS_PER_MINUTE = 400

# Upload 链路 file_type 的标准化映射
# file_processing_details.file_type 实际可能值:
#   pdf/word/excel/csv/image/audio/youtube/web/text/podcast
# 前端 source_subtype 把 pdf/word/excel/csv 统一归并为 'document'，image 单独保留
_UPLOAD_FILE_TYPE_MAP = {
    "pdf": "document",
    "word": "document",
    "excel": "document",
    "csv": "document",
    "document": "document",  # 兜底兼容旧数据
    "image": "image",
    "audio": "audio",
    "youtube": "youtube",
    "web": "web",
    "text": "text",
    "podcast": "podcast",  # Apple Podcast Episode
}

# 反向映射：前端 source_subtype → DB file_type 集合（用于 WHERE IN 过滤）
_UPLOAD_SUBTYPE_TO_DB_TYPES: Dict[str, List[str]] = {
    "document": ["pdf", "word", "excel", "csv", "document"],
    "image": ["image"],
    "audio": ["audio"],
    "youtube": ["youtube"],
    "web": ["web"],
    "text": ["text"],
    "podcast": ["podcast"],
}

# Feed 链路 channel_type 的标准化映射
# subscription_content.channel_type 可能为：twitter/youtube/news/rss
# 注意：Feed 的 youtube 与 Upload 的 youtube 重名，前端徽章共用 'YT'，
# 但内部 source_subtype 必须区分，否则点击跳转目标会错。
_FEED_CHANNEL_TYPE_MAP = {
    "twitter": "twitter",
    "youtube": "youtube_feed",
    "news": "news",
    "rss": "news",  # rss 暂归并到 news 同类展示
}

# 所有合法 source_subtype
ALL_SOURCE_SUBTYPES = [
    "document",
    "image",
    "audio",
    "youtube",
    "web",
    "text",
    "podcast",
    "twitter",
    "news",
    "youtube_feed",
]

# podcast 上线前 Daily 已有的 6 类 Upload 子类型（旧「全选 Upload」preset 不含 podcast）
_LEGACY_UPLOAD_SUBTYPES_NO_PODCAST = frozenset(
    {"document", "image", "audio", "youtube", "web", "text"}
)


def _expand_preset_for_legacy_full_upload(subtypes: List[str]) -> List[str]:
    """若 preset 已包含上述 6 类 Upload 但未含 podcast，自动并入 podcast。

    避免新增 source_subtype 后，旧 preset 仍按旧列表过滤，导致 Daily 永远不展示 Podcast。
    在首个 ``text`` 之后插入 ``podcast``；若无 ``text``（脏数据）则追加到末尾。
    其余 subtype 顺序保持不变（含 Feeds 各项）。
    """
    if not subtypes or "podcast" in subtypes:
        return subtypes
    if not _LEGACY_UPLOAD_SUBTYPES_NO_PODCAST.issubset(frozenset(subtypes)):
        return subtypes
    out: List[str] = []
    inserted = False
    for s in subtypes:
        out.append(s)
        if s == "text" and not inserted:
            out.append("podcast")
            inserted = True
    if not inserted:
        out.append("podcast")
    return out


def get_db() -> DatabaseManager:
    return DatabaseManager(get_database_config())


# ─── Helpers ───────────────────────────────────────────────────────


def _truncate(text: Optional[str], max_chars: int = 220) -> Optional[str]:
    """截断长文本作为 snippet 回退展示。"""
    if not text:
        return None
    cleaned = re.sub(r"\s+", " ", text).strip()
    if len(cleaned) <= max_chars:
        return cleaned
    return cleaned[: max_chars - 1].rstrip() + "…"


def _reading_time_min(content_length: Optional[int]) -> Optional[int]:
    """根据字符数估算阅读时长（分钟），最少 1 分钟。"""
    if not content_length or content_length <= 0:
        return None
    return max(1, math.ceil(content_length / WORDS_PER_MINUTE))


def _ensure_list(value: Any) -> List[str]:
    """JSONB 字段读出来可能是 None / list / 字符串，统一成 list."""
    if value is None:
        return []
    if isinstance(value, list):
        return [str(x) for x in value if x]
    if isinstance(value, str):
        return [value] if value else []
    return []


def _normalize_subtypes(raw: Optional[List[str]]) -> Optional[List[str]]:
    """前端传入的 source_subtypes 过滤校验。"""
    if not raw:
        return None
    cleaned = [s for s in raw if s in ALL_SOURCE_SUBTYPES]
    return cleaned or None


# ─── SQL 构造 ──────────────────────────────────────────────────────
#
# 两侧字段必须对齐到同一 SELECT 列序，方便 UNION ALL：
#   1  source         text
#   2  raw_subtype    text  (file_type 或 channel_type 原始值)
#   3  item_id        int   (file_processing_details.id 或 subscription_content.id)
#   4  processing_stats_id  int / null
#   5  subscription_id      int / null   (后续从 channel_identifier 反查)
#   6  channel_identifier   text / null  (Feed 用)
#   7  title          text
#   8  content_length int
#   9  summary        text
#  10  reasoning      text  (Upload 兜底)
#  11  content_snippet text
#  12  author_name    text / null
#  13  author_identifier text / null
#  14  external_url   text / null
#  15  sector         text / null
#  16  industry       text / null
#  17  ticker         jsonb / null
#  18  concepts       jsonb / null
#  19  confidence     double precision / null
#  20  published_at   timestamptz
#  21  shared_by_username      text / null  (Upload 共享：分享者用户名; 无共享或 Feed 侧 = null)
#  22  shared_by_user_id       int / null   (Upload 共享：分享者 user_id)
#  23  shared_by_avatar_url_raw text / null  (Upload 共享：分享者头像原始值, Python 层再经 resolve_avatar_url 签名)
#  24  shared_to_recipients_raw json / null   (Upload owner：分享给的用户 JSON 数组; 非 owner = null)

_UPLOAD_SQL = """
    SELECT
        'upload'::text                                AS source,
        lower(fpd.file_type::text)                    AS raw_subtype,
        fpd.id                                        AS item_id,
        fpd.processing_stats_id                       AS processing_stats_id,
        NULL::integer                                 AS subscription_id,
        NULL::text                                    AS channel_identifier,
        COALESCE(NULLIF(fpd.display_name, ''),
                 NULLIF(fpd.filename, ''),
                 'Untitled')                          AS title,
        COALESCE(fpd.content_length, 0)               AS content_length,
        ft.summary                                    AS summary,
        ft.reasoning                                  AS reasoning,
        LEFT(COALESCE(NULLIF(trim(fpd.content), ''),
                      NULLIF(trim(COALESCE(fpd.display_name, fpd.filename, '')), ''),
                      ''),
             400)          AS content_snippet,
        NULL::text                                    AS author_name,
        NULL::text                                    AS author_identifier,
        CASE
            WHEN lower(fpd.file_type::text) IN ('youtube', 'web')
                 AND fpd.original_file_path LIKE 'http%%'
                THEN fpd.original_file_path
            ELSE NULL
        END                                           AS external_url,
        ft.sector                                     AS sector,
        ft.industry                                   AS industry,
        ft.ticker                                     AS ticker,
        ft.concepts                                   AS concepts,
        ft.confidence                                 AS confidence,
        fpd.created_at                                AS published_at,
        (SELECT u.username
         FROM file_shares fs
         JOIN users u ON u.id = fs.shared_by_user_id
         WHERE fs.file_id = fpd.id
           AND fs.shared_to_user_id = %(user_id)s
         ORDER BY fs.created_at DESC LIMIT 1)         AS shared_by_username,
        (SELECT fs.shared_by_user_id
         FROM file_shares fs
         WHERE fs.file_id = fpd.id
           AND fs.shared_to_user_id = %(user_id)s
         ORDER BY fs.created_at DESC LIMIT 1)         AS shared_by_user_id,
        (SELECT u.avatar_url
         FROM file_shares fs
         JOIN users u ON u.id = fs.shared_by_user_id
         WHERE fs.file_id = fpd.id
           AND fs.shared_to_user_id = %(user_id)s
         ORDER BY fs.created_at DESC LIMIT 1)         AS shared_by_avatar_url_raw,
        CASE
          WHEN ps."user" = %(username)s THEN
            COALESCE(
              (
                SELECT json_agg(
                  json_build_object(
                    'user_id', u_to.id,
                    'username', u_to.username,
                    'avatar_url', u_to.avatar_url
                  )
                  ORDER BY fs_to.created_at
                )
                FROM file_shares fs_to
                JOIN users u_to ON u_to.id = fs_to.shared_to_user_id
                WHERE fs_to.file_id = fpd.id
              ),
              '[]'::json
            )
          ELSE NULL
        END                                           AS shared_to_recipients_raw
    FROM file_processing_details fpd
    JOIN processing_stats ps ON fpd.processing_stats_id = ps.id
    LEFT JOIN file_tags ft ON ft.file_id = fpd.id
    WHERE (
        ps."user" = %(username)s
        OR EXISTS (
            SELECT 1 FROM file_shares fs
            WHERE fs.file_id = fpd.id
              AND fs.shared_to_user_id = %(user_id)s
        )
    )
      AND fpd.status = 'completed'
      AND (
          COALESCE(fpd.content, '') <> ''
          OR (
              lower(fpd.file_type::text) IN ('audio', 'youtube')
              AND fpd.asr_with_diarization IS NOT NULL
          )
          OR lower(fpd.file_type::text) = 'podcast'
      )
"""

_FEED_SQL = """
    SELECT
        'feed'::text                                  AS source,
        sc.channel_type                               AS raw_subtype,
        sc.id                                         AS item_id,
        NULL::integer                                 AS processing_stats_id,
        NULL::integer                                 AS subscription_id,
        sc.author_identifier                          AS channel_identifier,
        COALESCE(
            NULLIF(sc.title, ''),
            NULLIF(sc.author_name, ''),
            NULLIF(sc.author_identifier, ''),
            'Untitled'
        )                                             AS title,
        COALESCE(LENGTH(sc.content), 0)
            + COALESCE(LENGTH(sc.transcript), 0)      AS content_length,
        sct.summary                                   AS summary,
        NULL::text                                    AS reasoning,
        LEFT(COALESCE(sc.content, sc.transcript, ''), 400) AS content_snippet,
        sc.author_name                                AS author_name,
        sc.author_identifier                          AS author_identifier,
        sc.url                                        AS external_url,
        sct.sector                                    AS sector,
        sct.industry                                  AS industry,
        sct.ticker                                    AS ticker,
        sct.concepts                                  AS concepts,
        sct.confidence                                AS confidence,
        sc.published_at                               AS published_at,
        NULL::text                                    AS shared_by_username,
        NULL::integer                                 AS shared_by_user_id,
        NULL::text                                    AS shared_by_avatar_url_raw,
        NULL::json                                    AS shared_to_recipients_raw
    FROM subscription_content sc
    LEFT JOIN subscription_content_tags sct
        ON sct.subscription_content_id = sc.id
    WHERE %(user_id)s = ANY(sc.user_ids)
      AND sc.published_at IS NOT NULL
      AND EXISTS (
          SELECT 1 FROM subscription_list sl
          WHERE sl.user_id  = %(user_id)s
            AND sl.channel_type       = sc.channel_type
            AND sl.channel_identifier = sc.author_identifier
            AND sl.enabled = true
      )
"""


def _parse_q(q: str) -> List[Dict[str, Any]]:
    """将用户输入的 q 字符串解析为 token 组列表，用于构造多词 ILIKE 查询。

    支持的语法：
      - 空格分隔 = AND（每个词都必须命中）
      - OR（大写）分隔 = OR（任一词命中即可）
      - "双引号短语" = 作为一个整体匹配（短语内部的空格不拆分）
      - 混合：Trump OR "北京 会晤" 先按 OR 分组，每组内再按空格 AND

    返回结构：[{"op": "or"|"and", "terms": [str, ...]}]
    - 含 OR 关键字：整体为一个 OR 组
    - 不含 OR：整体为一个 AND 组
    """
    try:
        tokens = shlex.split(q)
    except ValueError:
        tokens = q.split()

    tokens = [t for t in tokens if t]
    if not tokens:
        return []

    has_or = any(t.upper() == "OR" for t in tokens)
    if has_or:
        terms = [t for t in tokens if t.upper() != "OR"]
        return [{"op": "or", "terms": terms}] if terms else []
    else:
        return [{"op": "and", "terms": tokens}]


def _build_filter_clauses(
    *,
    date_from: Optional[datetime],
    date_to: Optional[datetime],
    source_subtypes: Optional[List[str]],
    q: Optional[str],
    tickers: Optional[List[str]],
    tags: Optional[List[str]] = None,
    author: Optional[str] = None,
) -> tuple[str, str, Dict[str, Any]]:
    """构造 Upload / Feed 两侧的额外 WHERE 子句与参数。

    Returns:
        (upload_extra, feed_extra, params)
    """
    upload_parts: List[str] = []
    feed_parts: List[str] = []
    params: Dict[str, Any] = {}

    if date_from:
        params["date_from"] = date_from
        upload_parts.append("AND fpd.created_at >= %(date_from)s")
        feed_parts.append("AND sc.published_at >= %(date_from)s")
    if date_to:
        params["date_to"] = date_to
        upload_parts.append("AND fpd.created_at <= %(date_to)s")
        feed_parts.append("AND sc.published_at <= %(date_to)s")

    # source_subtypes 过滤：upload 一侧映射到 file_type, feed 一侧映射到 channel_type.
    upload_subtypes_pg: List[str] = []
    feed_subtypes_pg: List[str] = []
    if source_subtypes:
        for st in source_subtypes:
            if st in _UPLOAD_SUBTYPE_TO_DB_TYPES:
                upload_subtypes_pg.extend(_UPLOAD_SUBTYPE_TO_DB_TYPES[st])
            elif st == "twitter":
                feed_subtypes_pg.append("twitter")
            elif st == "youtube_feed":
                feed_subtypes_pg.append("youtube")
            elif st == "news":
                feed_subtypes_pg.extend(["news", "rss"])
        # 任一边没匹配到对应类型 → 用恒假条件让该边返回 0 行（避免 IN ()
        if not upload_subtypes_pg:
            upload_parts.append("AND FALSE")
        else:
            params["upload_subtypes"] = tuple(upload_subtypes_pg)
            upload_parts.append(
                "AND lower(fpd.file_type::text) IN %(upload_subtypes)s"
            )
        if not feed_subtypes_pg:
            feed_parts.append("AND FALSE")
        else:
            params["feed_subtypes"] = tuple(feed_subtypes_pg)
            feed_parts.append("AND sc.channel_type IN %(feed_subtypes)s")

    if q:
        groups = _parse_q(q)
        if groups:
            upload_group_clauses: List[str] = []
            feed_group_clauses: List[str] = []
            for gi, group in enumerate(groups):
                sub_u: List[str] = []
                sub_f: List[str] = []
                for ti, term in enumerate(group["terms"]):
                    key = f"q_{gi}_{ti}"
                    params[key] = f"%{term}%"
                    sub_u.append(
                        f"(COALESCE(fpd.display_name, '') ILIKE %({key})s"
                        f" OR COALESCE(fpd.filename, '') ILIKE %({key})s"
                        f" OR COALESCE(ft.summary, '') ILIKE %({key})s"
                        f" OR COALESCE(fpd.content, '') ILIKE %({key})s)"
                    )
                    sub_f.append(
                        f"(COALESCE(sc.title, '') ILIKE %({key})s"
                        f" OR COALESCE(sc.author_name, '') ILIKE %({key})s"
                        f" OR COALESCE(sct.summary, '') ILIKE %({key})s"
                        f" OR COALESCE(sc.content, '') ILIKE %({key})s)"
                    )
                joiner = " OR " if group["op"] == "or" else " AND "
                upload_group_clauses.append("(" + joiner.join(sub_u) + ")")
                feed_group_clauses.append("(" + joiner.join(sub_f) + ")")
            upload_parts.append("AND (" + " AND ".join(upload_group_clauses) + ")")
            feed_parts.append("AND (" + " AND ".join(feed_group_clauses) + ")")

    if tickers:
        # ticker 字段为 JSONB 数组（如 ["NVDA.US", "TSLA.US"]）
        # 用 ?| 操作符判断"任一存在"
        params["ticker_arr"] = tickers
        upload_parts.append("AND ft.ticker ?| %(ticker_arr)s")
        feed_parts.append("AND sct.ticker ?| %(ticker_arr)s")

    if tags:
        params["tag_arr"] = tags
        upload_parts.append("AND ft.concepts ?| %(tag_arr)s")
        feed_parts.append("AND sct.concepts ?| %(tag_arr)s")

    if author:
        params["author_like"] = f"%{author}%"
        # Upload items have no author; exclude them when author filter is set
        upload_parts.append("AND FALSE")
        feed_parts.append(
            "AND (COALESCE(sc.author_name, '') ILIKE %(author_like)s"
            " OR COALESCE(sc.author_identifier, '') ILIKE %(author_like)s)"
        )

    return " ".join(upload_parts), " ".join(feed_parts), params


def _resolve_subscription_ids(
    cursor, user_id: int, channel_identifiers: List[str]
) -> Dict[tuple, int]:
    """为 Feed 行批量反查 subscription_list.id (用于卡片点击跳转)。

    返回: {(channel_type, channel_identifier): subscription_id}
    """
    if not channel_identifiers:
        return {}
    cursor.execute(
        """
        SELECT id, channel_type, channel_identifier
        FROM subscription_list
        WHERE user_id = %s
          AND channel_identifier = ANY(%s)
        """,
        (user_id, channel_identifiers),
    )
    out: Dict[tuple, int] = {}
    for sid, ch_type, ch_id in cursor.fetchall():
        out[(ch_type, ch_id)] = sid
    return out


def _parse_shared_to_recipients_json(raw: Any) -> List[DailyShareRecipient]:
    """将 SQL json_agg 结果解析为 DailyShareRecipient 列表 (头像 URL 经 resolve)."""
    if raw is None:
        return []
    data: Any = raw
    if isinstance(raw, (bytes, memoryview)):
        try:
            data = json.loads(raw.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            return []
    elif isinstance(raw, str):
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            return []
    if not isinstance(data, list):
        return []
    out: List[DailyShareRecipient] = []
    for o in data:
        if not isinstance(o, dict):
            continue
        uid = o.get("user_id")
        if uid is None:
            continue
        uname = o.get("username")
        out.append(
            DailyShareRecipient(
                user_id=int(uid),
                username=str(uname) if uname is not None else "",
                avatar_url=_resolve_avatar(o.get("avatar_url")),
            )
        )
    return out


def _row_to_item(
    row: tuple,
    subscription_id_map: Dict[tuple, int],
) -> DailyFeedItem:
    """SQL 行 → DailyFeedItem。列序见上面 SQL 注释。"""
    (
        source,
        raw_subtype,
        item_id,
        processing_stats_id,
        _subscription_id_unused,
        channel_identifier,
        title,
        content_length,
        summary,
        reasoning,
        content_snippet,
        author_name,
        author_identifier,
        external_url,
        sector,
        industry,
        ticker_raw,
        concepts_raw,
        confidence,
        published_at,
        shared_by_username,
        shared_by_user_id,
        shared_by_avatar_url_raw,
        shared_to_recipients_raw,
    ) = row

    if source == "upload":
        subtype = _UPLOAD_FILE_TYPE_MAP.get(raw_subtype or "document", "document")
        composite_id = f"upload_{item_id}"
        navigate = DailyNavigate(
            type="execution",
            execution_id=processing_stats_id,
        )
    else:
        subtype = _FEED_CHANNEL_TYPE_MAP.get(raw_subtype or "news", "news")
        composite_id = f"feed_{item_id}"
        sub_id = subscription_id_map.get((raw_subtype, channel_identifier))
        if sub_id is not None:
            navigate = DailyNavigate(
                type="feed_article",
                subscription_id=sub_id,
                content_id=item_id,
                external_url=external_url,
            )
        else:
            # 用户被订阅源含进 user_ids[] 但订阅记录已删除（极少见），降级到外链
            navigate = DailyNavigate(
                type="external_url" if external_url else "feed_article",
                content_id=item_id,
                external_url=external_url,
            )

    summary_text = summary or None
    snippet_text = None
    if not summary_text:
        # 兜底用 reasoning（Upload 旧数据）或内容前 220 字
        snippet_text = _truncate(reasoning) or _truncate(content_snippet)

    # 全系统契约: DB 中 naive timestamp 一律视为 UTC (db_manager 已在连接上 SET TIME ZONE 'UTC').
    # 此处把 naive → aware UTC, Pydantic 会序列化为 ISO 8601 (...Z), 前端再按用户时区渲染.
    from backend.utils.datetime_utils import ensure_utc
    pub = ensure_utc(published_at)

    return DailyFeedItem(
        id=composite_id,
        source=source,
        source_subtype=subtype,
        title=title or "Untitled",
        summary=summary_text,
        snippet=snippet_text,
        author_name=author_name,
        author_identifier=author_identifier,
        sector=sector,
        industry=industry,
        tickers=_ensure_list(ticker_raw),
        concepts=_ensure_list(concepts_raw),
        confidence=float(confidence) if confidence is not None else None,
        published_at=pub,
        reading_time_min=_reading_time_min(content_length),
        navigate=navigate,
        shared_by_username=shared_by_username or None,
        shared_by_user_id=int(shared_by_user_id) if shared_by_user_id is not None else None,
        shared_by_avatar_url=_resolve_avatar(shared_by_avatar_url_raw),
        shared_to=_parse_shared_to_recipients_json(shared_to_recipients_raw)
        if source == "upload"
        else [],
    )


# ─── Endpoints ─────────────────────────────────────────────────────


@router.get("/feed", response_model=DailyFeedResponse)
def list_daily_feed(
    date_from: Optional[datetime] = Query(None, description="起始时间 (ISO 8601)"),
    date_to: Optional[datetime] = Query(None, description="结束时间 (ISO 8601)"),
    source_subtypes: Optional[List[str]] = Query(
        None,
        description=(
            "按来源子类型过滤（多选）: document, image, audio, youtube, web, text, podcast, "
            "twitter, news, youtube_feed"
        ),
    ),
    q: Optional[str] = Query(None, max_length=200, description="标题/摘要/正文模糊搜索"),
    tickers: Optional[List[str]] = Query(
        None, description='Ticker 多选过滤，如 ["NVDA.US"]'
    ),
    limit: int = Query(30, ge=1, le=100, description="每页条数"),
    offset: int = Query(0, ge=0, description="偏移量"),
    db: DatabaseManager = Depends(get_db),
    current_user: TokenData = Depends(get_current_user),
):
    """
    Daily 聚合信息流：合并 Upload Files + Feeds 订阅，按发布时间倒序。

    响应单条结构见 `DailyFeedItem`，前端按 `published_at` 的日期分组渲染（参考
    AI Notes Daily 的 Summaries 视图）。
    """
    if current_user.user_id is None or not current_user.username:
        raise HTTPException(status_code=401, detail="未授权")

    source_subtypes = _normalize_subtypes(source_subtypes)
    upload_extra, feed_extra, extra_params = _build_filter_clauses(
        date_from=date_from,
        date_to=date_to,
        source_subtypes=source_subtypes,
        q=q,
        tickers=tickers,
    )

    base_params: Dict[str, Any] = {
        "username": current_user.username,
        "user_id": current_user.user_id,
    }
    base_params.update(extra_params)

    union_sql = f"""
        WITH upload_items AS (
            {_UPLOAD_SQL}
            {upload_extra}
        ),
        feed_items AS (
            {_FEED_SQL}
            {feed_extra}
        ),
        merged AS (
            SELECT * FROM upload_items
            UNION ALL
            SELECT * FROM feed_items
        )
        SELECT * FROM merged
        ORDER BY published_at DESC NULLS LAST
        LIMIT %(limit)s OFFSET %(offset)s
    """

    count_sql = f"""
        WITH upload_items AS (
            {_UPLOAD_SQL}
            {upload_extra}
        ),
        feed_items AS (
            {_FEED_SQL}
            {feed_extra}
        )
        SELECT
            (SELECT COUNT(*) FROM upload_items)
          + (SELECT COUNT(*) FROM feed_items) AS total
    """

    conn = db.get_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="数据库连接失败")
    try:
        cursor = conn.cursor()

        # 1) 拉总数
        cursor.execute(count_sql, base_params)
        total = int(cursor.fetchone()[0] or 0)

        # 2) 拉当前页
        page_params = dict(base_params, limit=limit, offset=offset)
        cursor.execute(union_sql, page_params)
        rows = cursor.fetchall()

        # 3) 为 Feed 行批量反查 subscription_list.id
        feed_channel_identifiers: List[str] = list({
            r[5] for r in rows if r[0] == "feed" and r[5]
        })
        sub_id_map = _resolve_subscription_ids(
            cursor, current_user.user_id, feed_channel_identifiers
        )

        items = [_row_to_item(row, sub_id_map) for row in rows]

        return DailyFeedResponse(
            items=items,
            total=total,
            has_more=(offset + len(items)) < total,
            limit=limit,
            offset=offset,
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"[daily.list_daily_feed] 查询失败: {exc}")
        raise HTTPException(status_code=500, detail=f"查询失败: {exc}")
    finally:
        conn.close()


@router.get("/stats", response_model=DailyStatsResponse)
def get_daily_stats(
    date_from: Optional[datetime] = Query(None, description="起始时间 (ISO 8601)"),
    date_to: Optional[datetime] = Query(None, description="结束时间 (ISO 8601)"),
    q: Optional[str] = Query(None, max_length=200),
    tickers: Optional[List[str]] = Query(None),
    db: DatabaseManager = Depends(get_db),
    current_user: TokenData = Depends(get_current_user),
):
    """
    返回当前过滤条件下，每个来源子类型的卡片数（顶部过滤器/源徽章 chip 用）。
    """
    if current_user.user_id is None or not current_user.username:
        raise HTTPException(status_code=401, detail="未授权")

    upload_extra, feed_extra, extra_params = _build_filter_clauses(
        date_from=date_from,
        date_to=date_to,
        source_subtypes=None,  # stats 不按 subtype 过滤
        q=q,
        tickers=tickers,
    )

    base_params: Dict[str, Any] = {
        "username": current_user.username,
        "user_id": current_user.user_id,
    }
    base_params.update(extra_params)

    sql = f"""
        WITH upload_items AS (
            {_UPLOAD_SQL}
            {upload_extra}
        ),
        feed_items AS (
            {_FEED_SQL}
            {feed_extra}
        ),
        merged AS (
            SELECT 'upload'::text AS source, raw_subtype FROM upload_items
            UNION ALL
            SELECT 'feed'::text   AS source, raw_subtype FROM feed_items
        )
        SELECT source, raw_subtype, COUNT(*) AS cnt
        FROM merged
        GROUP BY source, raw_subtype
    """

    conn = db.get_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="数据库连接失败")
    try:
        cursor = conn.cursor()
        cursor.execute(sql, base_params)
        rows = cursor.fetchall()

        by_source_count: Dict[str, int] = {}
        total = 0
        for source, raw_subtype, cnt in rows:
            cnt = int(cnt or 0)
            total += cnt
            if source == "upload":
                key = _UPLOAD_FILE_TYPE_MAP.get(raw_subtype or "document", "document")
            else:
                key = _FEED_CHANNEL_TYPE_MAP.get(raw_subtype or "news", "news")
            by_source_count[key] = by_source_count.get(key, 0) + cnt

        return DailyStatsResponse(
            total=total,
            by_source=[
                DailySourceCount(source_subtype=k, count=v)  # type: ignore[arg-type]
                for k, v in sorted(by_source_count.items(), key=lambda x: -x[1])
            ],
        )
    except Exception as exc:
        logger.error(f"[daily.get_daily_stats] 统计失败: {exc}")
        raise HTTPException(status_code=500, detail=f"统计失败: {exc}")
    finally:
        conn.close()


# ─── Calendar (日期导航栏) ──────────────────────────────────────────
#
# 为前端左侧日期导航栏提供按自然日聚合的统计数据。
# 复用 _UPLOAD_SQL / _FEED_SQL + _build_filter_clauses 逻辑，
# 按用户时区 (tz) 把 published_at 切到自然日，再补齐 0 条空白日。
# 详见 docs/instructions/DAILY_MODULE.md §4 (API 表格)


@router.get("/calendar", response_model=DailyCalendarResponse)
def list_daily_calendar(
    tz: str = Query("UTC", description="用户时区 (IANA)，如 Asia/Hong_Kong"),
    days: int = Query(30, ge=1, le=180, description="从今天往回看的天数"),
    source_subtypes: Optional[List[str]] = Query(None),
    q: Optional[str] = Query(None, max_length=200),
    tickers: Optional[List[str]] = Query(None),
    db: DatabaseManager = Depends(get_db),
    current_user: TokenData = Depends(get_current_user),
):
    """
    日历聚合：返回最近 N 天每天的卡片总数与 source_subtype 分布。

    前端日期导航栏（DailyDateSidebar）消费本接口，展示活跃度色块、迷你彩条等统计。
    - 按用户时区切自然日（不依赖服务器本地时区）。
    - 含 0 条空白日，让用户感知某天无新内容。
    - 结果随 source_subtypes / q / tickers 联动，与主信息流过滤口径一致。
    """
    if current_user.user_id is None or not current_user.username:
        raise HTTPException(status_code=401, detail="未授权")

    from backend.utils.datetime_utils import parse_user_tz, today_in_tz
    from datetime import date, datetime as _dt, timedelta, timezone as _tz

    # 解析时区，非法 → 回退 UTC
    user_tz = parse_user_tz(tz)
    actual_tz_str = str(user_tz)

    # 计算窗口的 UTC 起点（用户时区的 window_start 日 00:00:00 转 UTC-aware）
    today_date: date = today_in_tz(actual_tz_str)
    window_start_date: date = today_date - timedelta(days=days - 1)
    window_start_local = _dt.combine(window_start_date, _dt.min.time(), tzinfo=user_tz)
    window_start_utc = window_start_local.astimezone(_tz.utc)

    source_subtypes = _normalize_subtypes(source_subtypes)
    upload_extra, feed_extra, extra_params = _build_filter_clauses(
        date_from=None,
        date_to=None,
        source_subtypes=source_subtypes,
        q=q,
        tickers=tickers,
    )

    base_params: Dict[str, Any] = {
        "username": current_user.username,
        "user_id": current_user.user_id,
        "window_start_utc": window_start_utc,
        "cal_tz": actual_tz_str,
    }
    base_params.update(extra_params)

    # SQL：按用户时区切自然日分组统计
    sql = f"""
        WITH upload_items AS (
            {_UPLOAD_SQL}
            {upload_extra}
        ),
        feed_items AS (
            {_FEED_SQL}
            {feed_extra}
        ),
        merged AS (
            SELECT 'upload'::text AS source, raw_subtype, published_at FROM upload_items
            UNION ALL
            SELECT 'feed'::text   AS source, raw_subtype, published_at FROM feed_items
        ),
        by_day AS (
            SELECT
                -- 契约: uploaded_at / published_at 列为 naive UTC (连接 TimeZone=UTC).
                -- 若写成 (published_at AT TIME ZONE cal_tz)::date，会把存盘钟面误当成 cal_tz
                -- 本地时刻 → 与前端 dateKeyInTz(dayjs.utc(...).tz(cal_tz))) 差 ~8h，
                -- 「今天」桶内 Twitter/YouTube 会落到「昨天」桶。
                (published_at::timestamptz AT TIME ZONE %(cal_tz)s)::date AS d,
                source,
                raw_subtype,
                COUNT(*) AS cnt
            FROM merged
            WHERE published_at >= %(window_start_utc)s
            GROUP BY 1, 2, 3
        )
        SELECT d, source, raw_subtype, cnt FROM by_day
        ORDER BY d DESC
    """

    conn = db.get_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="数据库连接失败")
    try:
        cursor = conn.cursor()
        cursor.execute(sql, base_params)
        rows = cursor.fetchall()

        # 聚合：{date_str: {source_subtype: count}}
        from collections import defaultdict  # noqa: PLC0415
        day_map: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
        for d, source, raw_subtype, cnt in rows:
            cnt = int(cnt or 0)
            if source == "upload":
                key = _UPLOAD_FILE_TYPE_MAP.get(raw_subtype or "document", "document")
            else:
                key = _FEED_CHANNEL_TYPE_MAP.get(raw_subtype or "news", "news")
            day_map[str(d)][key] += cnt

        # 补齐 0 条空日，形成连续 N 天列表（倒序：today → window_start）
        result_items: List[DailyCalendarDay] = []
        total_in_window = 0
        max_total = 0
        for i in range(days):
            target = today_date - timedelta(days=i)
            date_str = target.isoformat()
            subtype_counts = day_map.get(date_str, {})
            day_total = sum(subtype_counts.values())
            total_in_window += day_total
            if day_total > max_total:
                max_total = day_total
            by_source = [
                DailyCalendarDaySource(source_subtype=k, count=v)  # type: ignore[arg-type]
                for k, v in sorted(subtype_counts.items(), key=lambda x: -x[1])
            ]
            result_items.append(
                DailyCalendarDay(date=date_str, total=day_total, by_source=by_source)
            )

        return DailyCalendarResponse(
            tz=actual_tz_str,
            days=days,
            items=result_items,
            max_total=max_total,
            total_in_window=total_in_window,
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"[daily.list_daily_calendar] 统计失败: {exc}")
        raise HTTPException(status_code=500, detail=f"统计失败: {exc}")
    finally:
        conn.close()


# ─── Preset (per-user) ─────────────────────────────────────────────
#
# 表: user_daily_preferences (user_id PK, source_subtypes TEXT[])
# 语义: source_subtypes 为空数组 = 未设置 / 展示全部。
# 详见 backend/database/migrations/create_user_daily_preferences.sql


@router.get("/preset", response_model=DailyPresetResponse)
def get_daily_preset(
    db: DatabaseManager = Depends(get_db),
    current_user: TokenData = Depends(get_current_user),
):
    """读取当前用户的 Daily preset (无记录返回空数组)。"""
    if current_user.user_id is None:
        raise HTTPException(status_code=401, detail="未授权")

    conn = db.get_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="数据库连接失败")
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT source_subtypes FROM user_daily_preferences WHERE user_id = %s",
            (current_user.user_id,),
        )
        row = cursor.fetchone()
        raw = list(row[0]) if row and row[0] else []
        # 防御性过滤：剔除非法值（schema 升级或脏数据兜底）
        cleaned = [s for s in raw if s in ALL_SOURCE_SUBTYPES]
        expanded = _expand_preset_for_legacy_full_upload(cleaned)
        return DailyPresetResponse(source_subtypes=expanded)  # type: ignore[arg-type]
    except Exception as exc:
        logger.error(f"[daily.get_daily_preset] 读取失败: {exc}")
        raise HTTPException(status_code=500, detail=f"读取失败: {exc}")
    finally:
        conn.close()


@router.put("/preset", response_model=DailyPresetResponse)
def update_daily_preset(
    payload: DailyPresetUpdate,
    db: DatabaseManager = Depends(get_db),
    current_user: TokenData = Depends(get_current_user),
):
    """UPSERT 当前用户的 Daily preset。

    - 传空数组 → 清空 preset (等同未设置 / 展示全部)
    - Pydantic 已校验只接受合法 SourceSubtype 字面量
    - 内部仍二次去重 + 校验，避免脏写
    """
    if current_user.user_id is None:
        raise HTTPException(status_code=401, detail="未授权")

    # 去重 + 二次校验 + 保留稳定顺序
    seen: set = set()
    cleaned: List[str] = []
    for s in payload.source_subtypes:
        if s in ALL_SOURCE_SUBTYPES and s not in seen:
            seen.add(s)
            cleaned.append(s)

    conn = db.get_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="数据库连接失败")
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO user_daily_preferences (user_id, source_subtypes, updated_at)
            VALUES (%s, %s, NOW())
            ON CONFLICT (user_id) DO UPDATE
                SET source_subtypes = EXCLUDED.source_subtypes,
                    updated_at = NOW()
            RETURNING source_subtypes
            """,
            (current_user.user_id, cleaned),
        )
        row = cursor.fetchone()
        conn.commit()
        saved = list(row[0]) if row and row[0] else []
        return DailyPresetResponse(source_subtypes=saved)  # type: ignore[arg-type]
    except Exception as exc:
        conn.rollback()
        logger.error(f"[daily.update_daily_preset] 保存失败: {exc}")
        raise HTTPException(status_code=500, detail=f"保存失败: {exc}")
    finally:
        conn.close()


# ─── 单卡片详情 (Drawer 预览用) ────────────────────────────────────
#
# 卡片点击 → 前端抽屉预览, 而不是跳出到 /files / /feeds (旧行为). 详情数据
# 包含: 列表已有的 DailyFeedItem 字段 + 来源专属原文区字段:
#   upload: file_processing_details.content + processing_stats.final_report
#   feed:   subscription_content + subscription_content_tags 的列表行同形
#
# 二进制资源 (image/audio/pdf 的 file_url + audio/youtube 的 asr) 不在此返回,
# 前端按需调 /api/files/original/{file_id} (已存在, 包含 OSS / 签名 URL 逻辑).
# YouTube transcript 同样懒加载 /api/subscriptions/contents/{id}/transcript.
#
# 详见 docs/instructions/DAILY_MODULE.md §1 (改造后的卡片点击导航).


_ITEM_ID_RE = re.compile(r"^(upload|feed)_(\d+)$")


def _parse_item_id(item_id: str) -> tuple[str, int]:
    """复合 ID 'upload_123' / 'feed_4567' → (source, int)。

    非法格式抛 400 而不是 422 (路径参数, 业务级校验).
    """
    m = _ITEM_ID_RE.match(item_id or "")
    if not m:
        raise HTTPException(status_code=400, detail=f"非法的 item id: {item_id}")
    return m.group(1), int(m.group(2))


def _fetch_upload_detail(
    cursor, file_id: int, current_user: TokenData
) -> DailyItemDetail:
    """根据 file_processing_details.id 装配 Upload 卡片完整详情.

    鉴权: 当前用户是文件 owner (processing_stats."user") 或
          被通过 file_shares 主动分享到该文件 (shared_to_user_id)。
    与 _UPLOAD_SQL 及 /api/files/details 列表接口口径保持一致。
    """
    cursor.execute(
        """
        SELECT
            fpd.id,
            fpd.file_type,
            fpd.filename,
            fpd.display_name,
            fpd.original_file_path,
            fpd.content,
            COALESCE(fpd.content_length, 0)         AS content_length,
            fpd.created_at,
            fpd.processing_stats_id,
            ps.final_report,
            ft.summary,
            ft.reasoning,
            ft.sector,
            ft.industry,
            ft.ticker,
            ft.concepts,
            ft.confidence,
            ps."user"                               AS owner_username
        FROM file_processing_details fpd
        JOIN processing_stats ps ON fpd.processing_stats_id = ps.id
        LEFT JOIN file_tags ft ON ft.file_id = fpd.id
        WHERE fpd.id = %s
          AND (
              ps."user" = %s
              OR EXISTS (
                  SELECT 1 FROM file_shares fs
                  WHERE fs.file_id = fpd.id
                    AND fs.shared_to_user_id = %s
              )
          )
        """,
        (file_id, current_user.username, current_user.user_id),
    )
    row = cursor.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Upload 文件不存在、已删除或无访问权限")

    (
        item_id,
        file_type,
        filename,
        display_name,
        original_file_path,
        content,
        content_length,
        created_at,
        processing_stats_id,
        final_report,
        summary,
        reasoning,
        sector,
        industry,
        ticker_raw,
        concepts_raw,
        confidence,
        owner_username,
    ) = row

    if owner_username == current_user.username:
        cursor.execute(
            """
            SELECT COALESCE(
                json_agg(
                    json_build_object(
                        'user_id', u_to.id,
                        'username', u_to.username,
                        'avatar_url', u_to.avatar_url
                    )
                    ORDER BY fs_to.created_at
                ),
                '[]'::json
            )
            FROM file_shares fs_to
            JOIN users u_to ON u_to.id = fs_to.shared_to_user_id
            WHERE fs_to.file_id = %s
            """,
            (item_id,),
        )
        r_share = cursor.fetchone()
        shared_to_list = _parse_shared_to_recipients_json(r_share[0] if r_share else None)
        shared_by_username = None
        shared_by_user_id = None
        shared_by_avatar_url = None
    else:
        cursor.execute(
            """
            SELECT u.username, fs.shared_by_user_id, u.avatar_url
            FROM file_shares fs
            JOIN users u ON u.id = fs.shared_by_user_id
            WHERE fs.file_id = %s AND fs.shared_to_user_id = %s
            ORDER BY fs.created_at DESC
            LIMIT 1
            """,
            (item_id, current_user.user_id),
        )
        r_in = cursor.fetchone()
        if r_in:
            shared_by_username, shared_by_user_id, av_raw = r_in
            shared_by_avatar_url = _resolve_avatar(av_raw)
        else:
            shared_by_username = None
            shared_by_user_id = None
            shared_by_avatar_url = None
        shared_to_list = []

    subtype = _UPLOAD_FILE_TYPE_MAP.get(file_type or "document", "document")
    summary_text = summary or None
    snippet_text = None
    if not summary_text:
        snippet_text = _truncate(reasoning) or _truncate(content)

    title = display_name or filename or "Untitled"

    from backend.utils.datetime_utils import ensure_utc
    pub = ensure_utc(created_at)

    navigate = DailyNavigate(
        type="execution",
        execution_id=processing_stats_id,
    )

    upload_payload = DailyItemDetailUpload(
        file_id=item_id,
        file_type_raw=file_type,
        display_name=display_name,
        filename=filename,
        original_file_path=original_file_path,
        content=content,
        final_report=final_report,
    )

    return DailyItemDetail(
        id=f"upload_{item_id}",
        source="upload",
        source_subtype=subtype,  # type: ignore[arg-type]
        title=title,
        summary=summary_text,
        snippet=snippet_text,
        author_name=None,
        author_identifier=None,
        sector=sector,
        industry=industry,
        tickers=_ensure_list(ticker_raw),
        concepts=_ensure_list(concepts_raw),
        confidence=float(confidence) if confidence is not None else None,
        published_at=pub,
        reading_time_min=_reading_time_min(content_length),
        navigate=navigate,
        shared_by_username=shared_by_username,
        shared_by_user_id=int(shared_by_user_id) if shared_by_user_id is not None else None,
        shared_by_avatar_url=shared_by_avatar_url,
        shared_to=shared_to_list,
        upload=upload_payload,
        feed=None,
    )


def _fetch_feed_detail(
    cursor, content_db_id: int, current_user: TokenData
) -> DailyItemDetail:
    """根据 subscription_content.id 装配 Feed 卡片完整详情.

    鉴权: user_id = ANY(sc.user_ids) (与列表口径 + Feeds 模块一致).
    单查询 LEFT JOIN subscription_content_tags 取 AI 打标轻量结构;
    LEFT JOIN subscription_content_user_summaries (PR-D1) 取 per-user 个性化摘要,
    抽屉前端 "AI 深度分析" 段消费.
    跳转目标 subscription_id 用 (channel_type, channel_identifier) 反查 subscription_list.
    """
    cursor.execute(
        """
        SELECT
            sc.id, sc.channel_type, sc.content_id,
            sc.title, sc.content,
            sc.author_name, sc.author_identifier,
            sc.url, sc.media_urls, sc.published_at,
            sc.engagement_count, sc.view_count,
            sc.metadata, sc.heat_level,
            sc.is_retweet, sc.retweeted_author, sc.retweeted_content, sc.retweeted_url,
            sc.is_quote,   sc.quoted_author,    sc.quoted_content,    sc.quoted_url,
            sc.is_reply,   sc.in_reply_to_id,   sc.in_reply_to_username, sc.in_reply_to_user_id,
            (sc.transcript IS NOT NULL AND sc.transcript <> '') AS has_transcript,
            COALESCE(LENGTH(sc.content), 0) + COALESCE(LENGTH(sc.transcript), 0) AS content_length,
            sct.sector, sct.sector_en, sct.industry, sct.industry_en,
            sct.ticker, sct.concepts, sct.summary, sct.confidence, sct.tagged_at,
            scus.summary       AS personalized_summary,
            scus.template_id   AS personalized_template_id,
            scus.tagged_at     AS personalized_at
        FROM subscription_content sc
        LEFT JOIN subscription_content_tags sct
            ON sct.subscription_content_id = sc.id
        LEFT JOIN subscription_content_user_summaries scus
            ON scus.subscription_content_id = sc.id
           AND scus.user_id = %s
        WHERE sc.id = %s
          AND %s = ANY(sc.user_ids)
          AND EXISTS (
              SELECT 1 FROM subscription_list sl
              WHERE sl.user_id          = %s
                AND sl.channel_type       = sc.channel_type
                AND sl.channel_identifier = sc.author_identifier
                AND sl.enabled = true
          )
        """,
        (current_user.user_id, content_db_id, current_user.user_id, current_user.user_id),
    )
    row = cursor.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Feed 内容不存在或无权访问")

    (
        item_id, channel_type, content_id_raw,
        title, content,
        author_name, author_identifier,
        url, media_urls, published_at,
        _engagement_count, _view_count,
        metadata, heat_level,
        is_retweet, retweeted_author, retweeted_content, retweeted_url,
        is_quote, quoted_author, quoted_content, quoted_url,
        is_reply, in_reply_to_id, in_reply_to_username, in_reply_to_user_id,
        has_transcript,
        content_length,
        sector, sector_en, industry, industry_en,
        ticker_raw, concepts_raw, summary, confidence, tagged_at,
        personalized_summary, personalized_template_id, personalized_at,
    ) = row

    subtype = _FEED_CHANNEL_TYPE_MAP.get(channel_type or "news", "news")
    summary_text = summary or None
    snippet_text = None
    if not summary_text:
        snippet_text = _truncate(content)

    # 反查 subscription_id 用于跳转 (与列表 _resolve_subscription_ids 同口径)
    sub_id_map = _resolve_subscription_ids(
        cursor, current_user.user_id, [author_identifier] if author_identifier else []
    )
    sub_id = sub_id_map.get((channel_type, author_identifier))
    if sub_id is not None:
        navigate = DailyNavigate(
            type="feed_article",
            subscription_id=sub_id,
            content_id=item_id,
            external_url=url,
        )
    else:
        navigate = DailyNavigate(
            type="external_url" if url else "feed_article",
            content_id=item_id,
            external_url=url,
        )

    from backend.utils.datetime_utils import ensure_utc
    pub = ensure_utc(published_at)

    tickers = _ensure_list(ticker_raw)
    concepts = _ensure_list(concepts_raw)

    tags_payload = None
    if sector is not None:
        tags_payload = {
            "sector": sector,
            "sector_en": sector_en or "",
            "industry": industry,
            "industry_en": industry_en or "",
            "tickers": tickers,
            "concepts": concepts,
            "summary": summary,
            "confidence": float(confidence) if confidence is not None else None,
            "tagged_at": tagged_at.isoformat() if tagged_at else None,
        }

    # News categories 在 metadata 之外, 这里当前 schema 不单列, FeedArticleBody
    # 通过 selectedContent.categories 读, 我们尽量从 metadata.categories 兜底.
    categories: Optional[List[str]] = None
    if isinstance(metadata, dict):
        cats = metadata.get("categories")
        if isinstance(cats, list):
            categories = [str(c) for c in cats if c]

    feed_payload = DailyItemDetailFeed(
        content_db_id=item_id,
        channel_type=channel_type,
        content_id=content_id_raw,
        title=title,
        content=content,
        author_name=author_name,
        author_identifier=author_identifier,
        url=url,
        media_urls=list(media_urls) if media_urls else None,
        metadata=metadata if isinstance(metadata, dict) else None,
        categories=categories,
        is_retweet=is_retweet,
        retweeted_author=retweeted_author,
        retweeted_content=retweeted_content,
        retweeted_url=retweeted_url,
        is_quote=is_quote,
        quoted_author=quoted_author,
        quoted_content=quoted_content,
        quoted_url=quoted_url,
        is_reply=is_reply,
        in_reply_to_id=in_reply_to_id,
        in_reply_to_username=in_reply_to_username,
        in_reply_to_user_id=in_reply_to_user_id,
        heat_level=heat_level,
        has_transcript=bool(has_transcript),
        tags=tags_payload,
        personalized_summary=personalized_summary or None,
        personalized_template_id=personalized_template_id,
        personalized_at=personalized_at.isoformat() if personalized_at else None,
    )

    composed_title = title or author_name or author_identifier or "Untitled"

    return DailyItemDetail(
        id=f"feed_{item_id}",
        source="feed",
        source_subtype=subtype,  # type: ignore[arg-type]
        title=composed_title,
        summary=summary_text,
        snippet=snippet_text,
        author_name=author_name,
        author_identifier=author_identifier,
        sector=sector,
        industry=industry,
        tickers=tickers,
        concepts=concepts,
        confidence=float(confidence) if confidence is not None else None,
        published_at=pub,
        reading_time_min=_reading_time_min(content_length),
        navigate=navigate,
        upload=None,
        feed=feed_payload,
    )


@router.get("/items/{item_id}", response_model=DailyItemDetail)
def get_daily_item_detail(
    item_id: str,
    db: DatabaseManager = Depends(get_db),
    current_user: TokenData = Depends(get_current_user),
):
    """单条卡片完整详情 (Drawer 预览用).

    item_id 形如 'upload_<file_id>' 或 'feed_<content_id>', 与列表
    DailyFeedItem.id 完全一致. 内部按前缀分发到 file_processing_details
    或 subscription_content 单条查询, 鉴权口径与列表 endpoint 一致.

    返回结构: DailyFeedItem 列表字段 + 来源专属 upload/feed 子结构.
    """
    if current_user.user_id is None or not current_user.username:
        raise HTTPException(status_code=401, detail="未授权")

    source, numeric_id = _parse_item_id(item_id)

    conn = db.get_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="数据库连接失败")
    try:
        cursor = conn.cursor()
        if source == "upload":
            return _fetch_upload_detail(cursor, numeric_id, current_user)
        else:
            return _fetch_feed_detail(cursor, numeric_id, current_user)
    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"[daily.get_daily_item_detail] 查询失败 ({item_id}): {exc}")
        raise HTTPException(status_code=500, detail=f"查询失败: {exc}")
    finally:
        conn.close()
