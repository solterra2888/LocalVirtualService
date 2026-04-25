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

import math
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from backend.auth.jwt_utils import get_current_user
from backend.database.db_manager import DatabaseManager
from backend.schemas.daily import (
    DailyFeedItem,
    DailyFeedResponse,
    DailyNavigate,
    DailyPresetResponse,
    DailyPresetUpdate,
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
# file_processing_details.file_type 实际可能值: pdf/word/excel/csv/image/audio/youtube/web/text
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
}

# 反向映射：前端 source_subtype → DB file_type 集合（用于 WHERE IN 过滤）
_UPLOAD_SUBTYPE_TO_DB_TYPES: Dict[str, List[str]] = {
    "document": ["pdf", "word", "excel", "csv", "document"],
    "image": ["image"],
    "audio": ["audio"],
    "youtube": ["youtube"],
    "web": ["web"],
    "text": ["text"],
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
    "twitter",
    "news",
    "youtube_feed",
]


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

_UPLOAD_SQL = """
    SELECT
        'upload'::text                                AS source,
        fpd.file_type                                 AS raw_subtype,
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
        LEFT(COALESCE(fpd.content, ''), 400)          AS content_snippet,
        NULL::text                                    AS author_name,
        NULL::text                                    AS author_identifier,
        CASE
            WHEN fpd.file_type IN ('youtube', 'web')
                 AND fpd.original_file_path LIKE 'http%%'
                THEN fpd.original_file_path
            ELSE NULL
        END                                           AS external_url,
        ft.sector                                     AS sector,
        ft.industry                                   AS industry,
        ft.ticker                                     AS ticker,
        ft.concepts                                   AS concepts,
        ft.confidence                                 AS confidence,
        fpd.created_at                                AS published_at
    FROM file_processing_details fpd
    JOIN processing_stats ps ON fpd.processing_stats_id = ps.id
    LEFT JOIN file_tags ft ON ft.file_id = fpd.id
    WHERE ps."user" = %(username)s
      AND fpd.status = 'completed'
      AND COALESCE(fpd.content, '') <> ''
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
        sc.published_at                               AS published_at
    FROM subscription_content sc
    LEFT JOIN subscription_content_tags sct
        ON sct.subscription_content_id = sc.id
    WHERE %(user_id)s = ANY(sc.user_ids)
      AND sc.published_at IS NOT NULL
"""


def _build_filter_clauses(
    *,
    date_from: Optional[datetime],
    date_to: Optional[datetime],
    source_subtypes: Optional[List[str]],
    q: Optional[str],
    tickers: Optional[List[str]],
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
            upload_parts.append("AND fpd.file_type IN %(upload_subtypes)s")
        if not feed_subtypes_pg:
            feed_parts.append("AND FALSE")
        else:
            params["feed_subtypes"] = tuple(feed_subtypes_pg)
            feed_parts.append("AND sc.channel_type IN %(feed_subtypes)s")

    if q:
        params["q_like"] = f"%{q}%"
        upload_parts.append(
            """AND (
                COALESCE(fpd.display_name, '') ILIKE %(q_like)s
                OR COALESCE(fpd.filename, '') ILIKE %(q_like)s
                OR COALESCE(ft.summary, '') ILIKE %(q_like)s
                OR COALESCE(fpd.content, '') ILIKE %(q_like)s
            )"""
        )
        feed_parts.append(
            """AND (
                COALESCE(sc.title, '') ILIKE %(q_like)s
                OR COALESCE(sc.author_name, '') ILIKE %(q_like)s
                OR COALESCE(sct.summary, '') ILIKE %(q_like)s
                OR COALESCE(sc.content, '') ILIKE %(q_like)s
            )"""
        )

    if tickers:
        # ticker 字段为 JSONB 数组（如 ["NVDA.US", "TSLA.US"]）
        # 用 ?| 操作符判断"任一存在"
        params["ticker_arr"] = tickers
        upload_parts.append("AND ft.ticker ?| %(ticker_arr)s")
        feed_parts.append("AND sct.ticker ?| %(ticker_arr)s")

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
    )


# ─── Endpoints ─────────────────────────────────────────────────────


@router.get("/feed", response_model=DailyFeedResponse)
def list_daily_feed(
    date_from: Optional[datetime] = Query(None, description="起始时间 (ISO 8601)"),
    date_to: Optional[datetime] = Query(None, description="结束时间 (ISO 8601)"),
    source_subtypes: Optional[List[str]] = Query(
        None,
        description=(
            "按来源子类型过滤（多选）: document, audio, youtube, web, text, "
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
        return DailyPresetResponse(source_subtypes=cleaned)  # type: ignore[arg-type]
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
