# -*- coding: utf-8 -*-
"""
Feeds Dashboard API — 按主题聚合的重要信息视图 (/feeds-dashboard).

解决「用户看不完每天订阅信息」: 把 auto-tag 产物按「主题 × 重要性」重组,
点击条目可看原文 (前端复用 DailyPreviewDrawer, 深链 Open in Feeds).

数据链路 (读路径零 LLM):
    feed_topics ⨝ feed_topic_daily_items (聚合器写入, 见 feed_topic_aggregation_service)
    ⨝ subscription_content
    scope=mine (默认): user_ids ∩ enabled 订阅可见性过滤 (与 Daily 同口径)
    scope=server:     全站内容池 (快照本就按全局池聚合, 仅读时少拼过滤;
                      条目带 in_user_scope 标记, false 的走原文外链)
    server 模式受 FEEDS_DASHBOARD_SERVER_SCOPE_ENABLED 环境开关控制 (默认开, 紧急回滚用)

挂载方式 (main.py):
    app.include_router(feeds_dashboard.router, prefix="/api/feeds-dashboard",
                       tags=["Feeds Dashboard"])

写入路径 (admin 手动 / task_scheduling feeds_topic_summary 每 2h):
    POST /generate → Celery aggregate_feed_topics(_single)_task
"""

from collections import Counter
from datetime import date as date_type
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, Depends, HTTPException, Query

from backend.api.daily import (
    _FEED_CHANNEL_TYPE_MAP,
    _ensure_list,
    _fetch_feed_detail,
    _resolve_subscription_ids,
)
from backend.api.dashboard import get_smart_ticker_formats
from backend.auth.jwt_utils import get_current_admin_user, get_current_user
from backend.database.db_manager import DatabaseManager
from backend.schemas.feeds_dashboard import (
    FeedsDashboardGenerateRequest,
    FeedsDashboardGenerateResponse,
    FeedsDashboardItem,
    FeedsDashboardItemsResponse,
    FeedsDashboardResponse,
    FeedsDashboardTopic,
    FeedsDashboardTopicItemsResponse,
    FeedsDashboardWord,
    FeedsDashboardWordCloudResponse,
)
from backend.schemas.user import TokenData
from backend.utils.avatar import resolve_avatar_url as _resolve_avatar
from backend.utils.datetime_utils import ensure_utc, today_in_tz, utcnow
from backend.utils.database_config import get_database_config
from backend.utils.logger import get_logger

logger = get_logger("feeds_dashboard_api")

router = APIRouter()

# report_date 标签时区 (与聚合器/摘要类任务统一: 北京自然日)
DASHBOARD_TZ = "Asia/Shanghai"

# 单次读取的安全上限 (用户当日可见条目; 正常量级为数百)
_MAX_USER_ITEMS = 4000

# importance 过滤 → SQL IN 集合
_IMPORTANCE_SETS = {
    "high": ("high",),
    "medium": ("high", "medium"),
}

# 用户可见性表达式 (mine 模式的 WHERE 片段; server 模式整体不拼入)
# 注意: 聚合快照 feed_topic_daily_items 本身是全站的 (按全局内容池写入),
# 用户过滤只发生在读取层 — 因此 server 视图 = 读时少拼此片段, 零额外聚合成本。
_USER_VISIBILITY_SQL = """
      AND %(user_id)s = ANY(sc.user_ids)
      AND EXISTS (
          SELECT 1 FROM subscription_list sl
          WHERE sl.user_id  = %(user_id)s
            AND sl.channel_type       = sc.channel_type
            AND sl.channel_identifier = sc.author_identifier
            AND sl.enabled = true
      )
"""

# 单条内容是否在当前用户订阅范围内 (server 模式下条目可能来自他人订阅;
# false 时前端点击走原文外链而非 Daily 抽屉 — 抽屉详情接口有用户鉴权会 404)
_IN_USER_SCOPE_EXPR = """
    ( %(user_id)s = ANY(sc.user_ids)
      AND EXISTS (
          SELECT 1 FROM subscription_list sl
          WHERE sl.user_id  = %(user_id)s
            AND sl.channel_type       = sc.channel_type
            AND sl.channel_identifier = sc.author_identifier
            AND sl.enabled = true ) )
"""


def _scope_or_400(scope: Optional[str]) -> str:
    """校验并归一 scope (mine 默认 | server). server 受环境开关控制 (紧急回滚)."""
    normalized = (scope or "mine").strip().lower()
    if normalized not in ("mine", "server"):
        raise HTTPException(status_code=400, detail=f"非法 scope: {scope} (期望 mine/server)")
    if normalized == "server":
        import os
        if os.getenv("FEEDS_DASHBOARD_SERVER_SCOPE_ENABLED", "true").lower() != "true":
            raise HTTPException(status_code=403, detail="全站视图已被管理员关闭")
    return normalized


# 条目行 SELECT 列 (与 _row_to_dashboard_item 的解包顺序严格对应)
def _build_item_sql(scope: str) -> str:
    visibility = _USER_VISIBILITY_SQL if scope == "mine" else ""
    return f"""
        SELECT
            i.topic_id,
            i.importance,
            i.score,
            sc.id                        AS content_id,
            sc.channel_type,
            COALESCE(
                NULLIF(sc.title, ''),
                NULLIF(sc.author_name, ''),
                NULLIF(sc.author_identifier, ''),
                'Untitled'
            )                            AS title,
            sct.summary,
            sc.author_name,
            sc.author_identifier,
            COALESCE(
                NULLIF(TRIM(sc.author_avatar), ''),
                NULLIF(TRIM(sp.avatar), ''),
                NULLIF(TRIM(sfa.avatar), '')
            )                            AS author_avatar,
            sct.ticker,
            sc.url,
            sc.published_at,
            ({_IN_USER_SCOPE_EXPR})      AS in_user_scope
        FROM feed_topic_daily_items i
        JOIN feed_topics ft
            ON ft.id = i.topic_id AND ft.is_enabled = true
        JOIN subscription_content sc
            ON sc.id = i.subscription_content_id
        LEFT JOIN subscription_content_tags sct
            ON sct.subscription_content_id = sc.id
        LEFT JOIN substack_publications sp
            ON sc.channel_type = 'substack'
           AND sp.publication_url = sc.author_identifier
        LEFT JOIN substack_followed_authors sfa
            ON sc.channel_type = 'substack'
           AND sfa.publication_url = sc.author_identifier
        WHERE i.report_date = ANY(%(dates)s){visibility}
          AND sc.published_at IS NOT NULL
    """

# 主题元信息 (名称等; 词典是共享的, 单独小查询)
_TOPIC_META_SQL = """
    SELECT id, topic_key, name_cn, name_en, category, needs_review
    FROM feed_topics
    WHERE is_enabled = true
"""


def get_db() -> DatabaseManager:
    return DatabaseManager(get_database_config())


def _parse_date(date_str: Optional[str]) -> date_type:
    """解析 YYYY-MM-DD; 缺省 = 今天 (北京自然日标签, 与聚合口径一致)."""
    if not date_str:
        return today_in_tz(DASHBOARD_TZ)
    try:
        return date_type.fromisoformat(date_str)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"非法日期: {date_str} (期望 YYYY-MM-DD)")


def _parse_dates(date_str: Optional[str], days: Optional[int]) -> List[date_type]:
    """解析报告日期窗口 (北京自然日标签列表, 升序).

    - 指定 date → 单日 [date] (向后兼容)
    - 否则按 days (默认 1, 即当天) 取最近 N 天: [today-N+1, ..., today]
    """
    if date_str:
        return [_parse_date(date_str)]
    from datetime import timedelta
    n = max(1, min(int(days or 1), 30))
    today = today_in_tz(DASHBOARD_TZ)
    return [today - timedelta(days=offset) for offset in range(n - 1, -1, -1)]


def _importance_tuple(min_importance: Optional[str]) -> Optional[Tuple[str, ...]]:
    """min_importance → SQL IN 元组; None/low = 不过滤."""
    if not min_importance or min_importance == "low":
        return None
    if min_importance not in _IMPORTANCE_SETS:
        raise HTTPException(
            status_code=400,
            detail=f"非法 min_importance: {min_importance} (期望 high/medium/low)",
        )
    return _IMPORTANCE_SETS[min_importance]


def _fetch_user_items(
    cursor,
    *,
    user_id: int,
    report_dates: List[date_type],
    importance_set: Optional[Tuple[str, ...]],
    channels: Optional[List[str]],
    topic_id: Optional[int] = None,
    limit: Optional[int] = None,
    offset: int = 0,
    count_only: bool = False,
    scope: str = "mine",
) -> List[Tuple[Any, ...]]:
    """拉日期窗口内条目行 (可再按主题/importance/渠道过滤).

    scope='mine' 按用户可见性过滤; 'server' 全站内容池 (快照本身全站, 只是少拼过滤).
    """
    params: Dict[str, Any] = {"dates": report_dates, "user_id": user_id}
    extra = ""
    if importance_set:
        params["importance_set"] = importance_set
        extra += " AND i.importance IN %(importance_set)s"
    if channels:
        params["channels"] = tuple(channels)
        extra += " AND sc.channel_type = ANY(%(channels)s)"
    if topic_id is not None:
        params["topic_id"] = topic_id
        extra += " AND i.topic_id = %(topic_id)s"

    order_limit = ""
    if limit is not None:
        order_limit = " ORDER BY sc.published_at DESC, i.score DESC LIMIT %(limit)s OFFSET %(offset)s"
        params["limit"] = limit
        params["offset"] = offset
    elif not count_only:
        order_limit = f" LIMIT {_MAX_USER_ITEMS}"

    cursor.execute(_build_item_sql(scope) + extra + order_limit, params)
    return cursor.fetchall()


def _row_to_dashboard_item(
    row: Tuple[Any, ...],
    sub_id_map: Dict[Tuple[str, str], int],
) -> FeedsDashboardItem:
    """SQL 行 → FeedsDashboardItem (列序见 _build_item_sql)."""
    (
        _topic_id,
        importance,
        score,
        content_id,
        channel_type,
        title,
        summary,
        author_name,
        author_identifier,
        author_avatar_raw,
        ticker_raw,
        url,
        published_at,
        in_user_scope,
    ) = row

    subtype = _FEED_CHANNEL_TYPE_MAP.get(channel_type or "news", "news")
    sub_id = sub_id_map.get((channel_type, author_identifier))
    if sub_id is not None:
        navigate = {
            "type": "feed_article",
            "subscription_id": sub_id,
            "content_id": content_id,
            "external_url": url,
        }
    else:
        navigate = {
            "type": "external_url" if url else "feed_article",
            "content_id": content_id,
            "external_url": url,
        }

    return FeedsDashboardItem(
        id=f"feed_{content_id}",
        source="feed",
        source_subtype=subtype,
        channel_type=channel_type or "",
        title=title or "Untitled",
        summary=summary or None,
        author_name=author_name,
        author_identifier=author_identifier,
        author_avatar=_resolve_avatar(author_avatar_raw),
        tickers=_ensure_list(ticker_raw),
        importance=importance,
        score=float(score) if score is not None else 0.0,
        published_at=ensure_utc(published_at) or utcnow(),
        url=url,
        in_user_scope=bool(in_user_scope) if in_user_scope is not None else True,
        navigate=navigate,  # type: ignore[arg-type]
    )


# ─── Endpoints ─────────────────────────────────────────────


@router.get("/", response_model=FeedsDashboardResponse)
def get_feeds_dashboard(
    date: Optional[str] = Query(None, description="YYYY-MM-DD (北京自然日); 与 days 二选一"),
    days: Optional[int] = Query(
        None, ge=1, le=30, description="最近 N 天 (1=当天 默认; 3=近3天; 7=近一周)"
    ),
    min_importance: Optional[str] = Query(
        None, description="high=仅高, medium=中+高, low/缺省=全部"
    ),
    channels: Optional[List[str]] = Query(
        None, description="渠道过滤 (channel_type 多选: twitter/youtube/news/...)"
    ),
    items_per_topic: int = Query(5, ge=1, le=20, description="每主题返回条目数"),
    scope: Optional[str] = Query(
        None, description="mine=我的订阅 (默认) | server=全站内容池"
    ),
    db: DatabaseManager = Depends(get_db),
    current_user: TokenData = Depends(get_current_user),
):
    """主题分组视图: 每主题含统计 + top 条目 (按 score 倒序)."""
    if current_user.user_id is None:
        raise HTTPException(status_code=401, detail="未授权")

    report_dates = _parse_dates(date, days)
    importance_set = _importance_tuple(min_importance)
    channels_clean = [c for c in (channels or []) if c] or None
    scope_ok = _scope_or_400(scope)

    conn = db.get_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="数据库连接失败")
    try:
        cursor = conn.cursor()

        rows = _fetch_user_items(
            cursor,
            user_id=current_user.user_id,
            report_dates=report_dates,
            importance_set=importance_set,
            channels=channels_clean,
            scope=scope_ok,
        )

        # 主题元信息 + 当日聚合时间戳
        cursor.execute(_TOPIC_META_SQL)
        topic_meta = {
            tid: {"topic_key": key, "name_cn": name_cn, "name_en": name_en,
                  "category": category, "needs_review": needs_review}
            for tid, key, name_cn, name_en, category, needs_review in cursor.fetchall()
        }
        cursor.execute(
            "SELECT MAX(created_at) FROM feed_topic_daily_items WHERE report_date = ANY(%s)",
            (report_dates,),
        )
        aggregated_raw = cursor.fetchone()[0]

        # 反查 subscription_id (Drawer navigate 用)
        channel_identifiers = list({(r[4], r[8]) for r in rows if r[8]})
        sub_id_map = _resolve_subscription_ids(
            cursor, current_user.user_id, [ident for _, ident in channel_identifiers]
        )

        # 按主题分组
        by_topic: Dict[int, List[Tuple[Any, ...]]] = {}
        for row in rows:
            by_topic.setdefault(row[0], []).append(row)

        topics: List[FeedsDashboardTopic] = []
        total_items = 0
        unique_content_ids: set = set()
        for topic_id, topic_rows in by_topic.items():
            meta = topic_meta.get(topic_id)
            if meta is None:
                continue  # 词典刚被禁用, 跳过
            items = [_row_to_dashboard_item(r, sub_id_map) for r in topic_rows]
            # 列表展示序: 时间降序优先 (阅读预期), score 仅作同刻并列裁决; 重要性由徽标表达
            items.sort(key=lambda x: (x.published_at, x.score), reverse=True)
            for item in items:
                unique_content_ids.add(item.id)

            ticker_counter: Counter = Counter()
            for item in items:
                ticker_counter.update(item.tickers)

            total_items += len(items)
            topics.append(
                FeedsDashboardTopic(
                    topic_id=topic_id,
                    topic_key=meta["topic_key"],
                    name_cn=meta["name_cn"],
                    name_en=meta["name_en"],
                    category=meta["category"],
                    needs_review=bool(meta["needs_review"]),
                    item_count=len(items),
                    high_count=sum(1 for x in items if x.importance == "high"),
                    medium_count=sum(1 for x in items if x.importance == "medium"),
                    top_tickers=[t for t, _ in ticker_counter.most_common(6)],
                    items=items[:items_per_topic],
                    has_more=len(items) > items_per_topic,
                )
            )

        # 主题排序: high 条数 → 条目数 → 分数和 (热度优先)
        topics.sort(key=lambda t: (-t.high_count, -t.item_count, -sum(i.score for i in t.items)))

        date_label = (
            str(report_dates[-1])
            if len(report_dates) == 1
            else f"{report_dates[0]}~{report_dates[-1]}"
        )
        return FeedsDashboardResponse(
            date=date_label,
            tz=DASHBOARD_TZ,
            min_importance=min_importance,
            topics=topics,
            total_items=total_items,
            unique_content_count=len(unique_content_ids),
            aggregated_at=ensure_utc(aggregated_raw),
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"[feeds_dashboard.get] 查询失败: {exc}")
        raise HTTPException(status_code=500, detail=f"查询失败: {exc}")
    finally:
        conn.close()


@router.get("/wordcloud", response_model=FeedsDashboardWordCloudResponse)
def get_feeds_dashboard_wordcloud(
    date: Optional[str] = Query(None, description="YYYY-MM-DD; 与 days 二选一"),
    days: Optional[int] = Query(None, ge=1, le=30, description="最近 N 天 (1=当天 默认)"),
    min_importance: Optional[str] = Query(None),
    channels: Optional[List[str]] = Query(None),
    concept_limit: int = Query(60, ge=1, le=200, description="concepts 词数上限"),
    ticker_limit: int = Query(15, ge=1, le=50, description="ticker 词数上限"),
    scope: Optional[str] = Query(None, description="mine=我的订阅 (默认) | server=全站内容池"),
    db: DatabaseManager = Depends(get_db),
    current_user: TokenData = Depends(get_current_user),
):
    """词云词频: 日期窗口内 (mine=用户可见 / server=全站) 内容的 concepts / ticker 出现条数 (按 content 去重).

    点击词 → 前端调 /items?word=... 过滤. 注意 feed_topic_daily_items 里
    一条内容可命中多主题, 必须 DISTINCT content 去重后再统计词频.
    watchlist 区始终是当前用户自己的符号, 但命中数按所选 scope 的内容池统计.
    """
    if current_user.user_id is None:
        raise HTTPException(status_code=401, detail="未授权")

    report_dates = _parse_dates(date, days)
    importance_set = _importance_tuple(min_importance)
    channels_clean = [c for c in (channels or []) if c] or None
    scope_ok = _scope_or_400(scope)

    conn = db.get_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="数据库连接失败")
    try:
        cursor = conn.cursor()

        # 日期窗口内容池 (mine=用户可见, 与 _build_item_sql 同口径; server=全站),
        # DISTINCT 到 content 粒度; 一条内容只属于其发布日, 跨天不会重复计入
        params: Dict[str, Any] = {"dates": report_dates, "user_id": current_user.user_id}
        extra = ""
        if importance_set:
            params["importance_set"] = importance_set
            extra += " AND i.importance IN %(importance_set)s"
        if channels_clean:
            params["channels"] = tuple(channels_clean)
            extra += " AND sc.channel_type = ANY(%(channels)s)"

        visibility = _USER_VISIBILITY_SQL if scope_ok == "mine" else ""
        visible_sql = f"""
            SELECT DISTINCT sc.id, sct.concepts, sct.ticker
            FROM feed_topic_daily_items i
            JOIN feed_topics ft
                ON ft.id = i.topic_id AND ft.is_enabled = true
            JOIN subscription_content sc
                ON sc.id = i.subscription_content_id
            LEFT JOIN subscription_content_tags sct
                ON sct.subscription_content_id = sc.id
            WHERE i.report_date = ANY(%(dates)s){visibility}
              AND sc.published_at IS NOT NULL{extra}
        """
        cursor.execute(f"SELECT COUNT(*) FROM ({visible_sql}) v", params)
        unique_content_count = int(cursor.fetchone()[0] or 0)

        # 概念词按主题折并 (与聚合归一化同口径): 「地缘政治风险/能源地缘政治/…」→「地缘政治」。
        # 计数单位 = 内容条数 (per-content 折并, 同一条内容命中多变体只计 1);
        # 未命中词典的散词保留原词原计数 (多为尚未成主题的新词)。
        from backend.services.feed_topic_aggregation_service import FeedTopicAggregationService

        matcher, topic_names = FeedTopicAggregationService().build_topic_matcher()

        concept_words: List[FeedsDashboardWord] = []
        if matcher is not None:
            from collections import defaultdict as _defaultdict

            topic_content_counts: Dict[int, int] = _defaultdict(int)
            raw_word_counts: Dict[str, int] = _defaultdict(int)
            cursor.execute(
                f"SELECT v.concepts FROM ({visible_sql}) v WHERE v.concepts IS NOT NULL",
                params,
            )
            for (concepts_raw,) in cursor.fetchall():
                matched_topics = set()
                for concept in _ensure_list(concepts_raw):
                    if not concept or not concept.strip():
                        continue
                    topic_id = matcher(concept)
                    if topic_id is not None:
                        matched_topics.add(topic_id)
                    else:
                        raw_word_counts[concept.strip()] += 1
                for topic_id in matched_topics:
                    topic_content_counts[topic_id] += 1

            folded = [
                FeedsDashboardWord(
                    word=topic_names.get(tid, f"topic_{tid}"),
                    count=cnt,
                    type="concept",
                    topic_id=tid,
                )
                for tid, cnt in topic_content_counts.items()
            ]
            leftovers = [
                FeedsDashboardWord(word=w, count=cnt, type="concept", topic_id=None)
                for w, cnt in raw_word_counts.items()
            ]
            folded.sort(key=lambda x: (-x.count, x.word))
            leftovers.sort(key=lambda x: (-x.count, x.word))
            concept_words = folded[:concept_limit] or []
            remaining_slots = max(0, concept_limit - len(concept_words))
            concept_words = concept_words + leftovers[:remaining_slots]
        else:
            # 词典不可用 → 降级为原始词频 (旧行为)
            cursor.execute(
                f"""
                SELECT c.word, COUNT(*)::int AS cnt
                FROM ({visible_sql}) v
                CROSS JOIN LATERAL jsonb_array_elements_text(v.concepts) AS c(word)
                WHERE btrim(c.word) <> ''
                GROUP BY c.word
                ORDER BY cnt DESC, c.word
                LIMIT %(concept_limit)s
                """,
                dict(params, concept_limit=concept_limit),
            )
            concept_words = [
                FeedsDashboardWord(word=w, count=int(cnt), type="concept")
                for w, cnt in cursor.fetchall()
            ]

        words: List[FeedsDashboardWord] = list(concept_words)
        cursor.execute(
            f"""
            SELECT t.word, COUNT(*)::int AS cnt
            FROM ({visible_sql}) v
            CROSS JOIN LATERAL jsonb_array_elements_text(v.ticker) AS t(word)
            WHERE btrim(t.word) <> ''
            GROUP BY t.word
            ORDER BY cnt DESC, t.word
            LIMIT %(ticker_limit)s
            """,
            dict(params, ticker_limit=ticker_limit),
        )
        words.extend(
            FeedsDashboardWord(word=w, count=int(cnt), type="ticker")
            for w, cnt in cursor.fetchall()
        )

        # 用户 watchlist 符号 + 当日命中条数 (0 条也返回 — 「关注的票今天有没有动静」)
        # symbol 可能无交易所后缀 (NVDA) 而标签是 NVDA.US, 用 smart formats 变体匹配;
        # 与 /api/dashboard/watchlist 同表同排序 (target_tracking_config).
        watchlist_words: List[FeedsDashboardWord] = []
        try:
            cursor.execute(
                """
                SELECT ttc.symbol
                FROM target_tracking_config ttc
                WHERE ttc.user_id = %s AND ttc.enabled = true
                ORDER BY COALESCE(ttc.sort_order, 999999), ttc.priority DESC, ttc.added_at DESC
                """,
                (current_user.user_id,),
            )
            watchlist_symbols = [row[0] for row in cursor.fetchall() if row[0]]

            if watchlist_symbols:
                cursor.execute(
                    f"SELECT v.ticker FROM ({visible_sql}) v WHERE v.ticker IS NOT NULL",
                    params,
                )
                pool_arrays = [_ensure_list(row[0]) for row in cursor.fetchall()]
                for symbol in watchlist_symbols:
                    formats = set(get_smart_ticker_formats(symbol))
                    count = sum(1 for arr in pool_arrays if formats.intersection(arr))
                    watchlist_words.append(
                        FeedsDashboardWord(word=symbol, count=count, type="watchlist")
                    )
        except Exception as wl_exc:
            # watchlist 缺表/异常不应影响词云主功能
            logger.warning(f"[feeds_dashboard.wordcloud] watchlist 读取失败 (跳过): {wl_exc}")

        date_label = (
            str(report_dates[-1])
            if len(report_dates) == 1
            else f"{report_dates[0]}~{report_dates[-1]}"
        )
        return FeedsDashboardWordCloudResponse(
            date=date_label,
            words=words,
            watchlist=watchlist_words,
            unique_content_count=unique_content_count,
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"[feeds_dashboard.wordcloud] 查询失败: {exc}")
        raise HTTPException(status_code=500, detail=f"查询失败: {exc}")
    finally:
        conn.close()


@router.get("/items", response_model=FeedsDashboardItemsResponse)
def get_feeds_dashboard_items(
    date: Optional[str] = Query(None, description="YYYY-MM-DD; 与 days 二选一"),
    days: Optional[int] = Query(None, ge=1, le=30, description="最近 N 天 (1=当天 默认)"),
    min_importance: Optional[str] = Query(None),
    channels: Optional[List[str]] = Query(None),
    word: Optional[str] = Query(None, max_length=100, description="词过滤 (点击词云散词)"),
    word_type: Optional[str] = Query(None, description="concept | ticker; word 非空时必填"),
    topic_id: Optional[int] = Query(
        None, description="主题过滤 (点击词云折并词); 与 word 互斥"
    ),
    limit: int = Query(30, ge=1, le=100),
    offset: int = Query(0, ge=0),
    scope: Optional[str] = Query(None, description="mine=我的订阅 (默认) | server=全站内容池"),
    db: DatabaseManager = Depends(get_db),
    current_user: TokenData = Depends(get_current_user),
):
    """扁平条目列表 (词云点击过滤 / 不带词=窗口内全部), 按 score 倒序分页.

    一条内容可命中多主题: 用 LATERAL 取其最高 score/importance, 保证按 content 去重.
    topic_id 过滤时 LATERAL 限定在该主题内取最高分.
    server 模式下条目可能来自他人订阅 (in_user_scope=false → 前端走原文外链).
    """
    if current_user.user_id is None:
        raise HTTPException(status_code=401, detail="未授权")

    report_dates = _parse_dates(date, days)
    importance_set = _importance_tuple(min_importance)
    channels_clean = [c for c in (channels or []) if c] or None
    scope_ok = _scope_or_400(scope)

    if word and word_type not in ("concept", "ticker"):
        raise HTTPException(status_code=400, detail="word 非空时 word_type 必须为 concept 或 ticker")
    if word and topic_id:
        raise HTTPException(status_code=400, detail="word 与 topic_id 互斥")

    conn = db.get_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="数据库连接失败")
    try:
        cursor = conn.cursor()

        params: Dict[str, Any] = {"dates": report_dates, "user_id": current_user.user_id}
        lateral_extra = ""
        if importance_set:
            params["importance_set"] = importance_set
            lateral_extra += " AND i.importance IN %(importance_set)s"
        if topic_id is not None:
            params["topic_id"] = topic_id
            lateral_extra += " AND i.topic_id = %(topic_id)s"

        where_extra = ""
        if channels_clean:
            params["channels"] = tuple(channels_clean)
            where_extra += " AND sc.channel_type = ANY(%(channels)s)"
        if word:
            if word_type == "concept":
                # JSONB ? : 数组包含该字符串元素; NULL 字段返回 NULL (视为未命中)
                params["word"] = word
                where_extra += " AND sct.concepts ? %(word)s"
            else:
                # ticker: watchlist symbol 可能无后缀 (NVDA) 而标签是 NVDA.US,
                # 复用 dashboard 的 smart formats 变体 + JSONB ?| any-of 匹配
                params["word_formats"] = get_smart_ticker_formats(word)
                where_extra += " AND sct.ticker ?| %(word_formats)s"

        # 列序与 _row_to_dashboard_item 解包严格对齐 (首列占位 topic_id)
        visibility = _USER_VISIBILITY_SQL if scope_ok == "mine" else ""
        base_sql = f"""
            SELECT
                NULL::int                          AS topic_id,
                best.importance,
                best.score,
                sc.id                              AS content_id,
                sc.channel_type,
                COALESCE(
                    NULLIF(sc.title, ''),
                    NULLIF(sc.author_name, ''),
                    NULLIF(sc.author_identifier, ''),
                    'Untitled'
                )                                  AS title,
                sct.summary,
                sc.author_name,
                sc.author_identifier,
                COALESCE(
                    NULLIF(TRIM(sc.author_avatar), ''),
                    NULLIF(TRIM(sp.avatar), ''),
                    NULLIF(TRIM(sfa.avatar), '')
                )                                  AS author_avatar,
                sct.ticker,
                sc.url,
                sc.published_at,
                ({_IN_USER_SCOPE_EXPR})            AS in_user_scope
            FROM subscription_content sc
            LEFT JOIN subscription_content_tags sct
                ON sct.subscription_content_id = sc.id
            LEFT JOIN substack_publications sp
                ON sc.channel_type = 'substack'
               AND sp.publication_url = sc.author_identifier
            LEFT JOIN substack_followed_authors sfa
                ON sc.channel_type = 'substack'
               AND sfa.publication_url = sc.author_identifier
            LEFT JOIN LATERAL (
                SELECT i.importance, i.score
                FROM feed_topic_daily_items i
                JOIN feed_topics ft
                    ON ft.id = i.topic_id AND ft.is_enabled = true
                WHERE i.subscription_content_id = sc.id
                  AND i.report_date = ANY(%(dates)s){lateral_extra}
                ORDER BY i.score DESC
                LIMIT 1
            ) best ON true
            WHERE sc.published_at IS NOT NULL
              AND best.importance IS NOT NULL{visibility}{where_extra}
        """

        cursor.execute(f"SELECT COUNT(*) FROM ({base_sql}) q", params)
        total = int(cursor.fetchone()[0] or 0)

        cursor.execute(
            base_sql + " ORDER BY sc.published_at DESC, best.score DESC LIMIT %(limit)s OFFSET %(offset)s",
            dict(params, limit=limit, offset=offset),
        )
        rows = cursor.fetchall()

        channel_identifiers = list({(r[4], r[8]) for r in rows if r[8]})
        sub_id_map = _resolve_subscription_ids(
            cursor, current_user.user_id, [ident for _, ident in channel_identifiers]
        )
        items = [_row_to_dashboard_item(r, sub_id_map) for r in rows]

        return FeedsDashboardItemsResponse(
            date=(
                str(report_dates[-1])
                if len(report_dates) == 1
                else f"{report_dates[0]}~{report_dates[-1]}"
            ),
            word=word,
            word_type=word_type if word else None,
            items=items,
            total=total,
            has_more=(offset + len(items)) < total,
            limit=limit,
            offset=offset,
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"[feeds_dashboard.items] 查询失败 (word={word}): {exc}")
        raise HTTPException(status_code=500, detail=f"查询失败: {exc}")
    finally:
        conn.close()


@router.get("/items/{content_db_id}/detail")
def get_feeds_dashboard_item_detail(
    content_db_id: int,
    current_user: TokenData = Depends(get_current_user),
    db: DatabaseManager = Depends(get_db),
):
    """全站视图条目详情 (Drawer 预览用, 复用 Daily 的装配逻辑但跳过用户范围检查).

    场景: server 模式下来自他人订阅的条目 (in_user_scope=false), Daily 的
    /api/daily/items/{id} 会因用户鉴权 404; 本端点供抽屉就地预览正文/摘要/字幕。
    门控: 与 server 模式共用 FEEDS_DASHBOARD_SERVER_SCOPE_ENABLED 环境开关。
    响应结构与 DailyItemDetail 完全一致 (前端抽屉零适配)。
    """
    if current_user.user_id is None:
        raise HTTPException(status_code=401, detail="未授权")
    _scope_or_400("server")  # 复用环境开关校验 (关闭时 403)

    conn = db.get_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="数据库连接失败")
    try:
        cursor = conn.cursor()
        return _fetch_feed_detail(cursor, content_db_id, current_user, enforce_scope=False)
    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"[feeds_dashboard.item_detail] 查询失败 (id={content_db_id}): {exc}")
        raise HTTPException(status_code=500, detail=f"查询失败: {exc}")
    finally:
        conn.close()


@router.get("/topics/{topic_id}/items", response_model=FeedsDashboardTopicItemsResponse)
def get_topic_items(
    topic_id: int,
    date: Optional[str] = Query(None, description="YYYY-MM-DD; 与 days 二选一"),
    days: Optional[int] = Query(None, ge=1, le=30, description="最近 N 天 (1=当天 默认)"),
    min_importance: Optional[str] = Query(None),
    channels: Optional[List[str]] = Query(None),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    scope: Optional[str] = Query(None, description="mine=我的订阅 (默认) | server=全站内容池"),
    db: DatabaseManager = Depends(get_db),
    current_user: TokenData = Depends(get_current_user),
):
    """主题内条目分页 (点击「查看全部」时调用)."""
    if current_user.user_id is None:
        raise HTTPException(status_code=401, detail="未授权")

    report_dates = _parse_dates(date, days)
    importance_set = _importance_tuple(min_importance)
    channels_clean = [c for c in (channels or []) if c] or None
    scope_ok = _scope_or_400(scope)

    conn = db.get_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="数据库连接失败")
    try:
        cursor = conn.cursor()

        # 总数 (同过滤条件)
        all_rows = _fetch_user_items(
            cursor,
            user_id=current_user.user_id,
            report_dates=report_dates,
            importance_set=importance_set,
            channels=channels_clean,
            topic_id=topic_id,
            count_only=True,
            scope=scope_ok,
        )
        total = len(all_rows)

        page_rows = _fetch_user_items(
            cursor,
            user_id=current_user.user_id,
            report_dates=report_dates,
            importance_set=importance_set,
            channels=channels_clean,
            topic_id=topic_id,
            limit=limit,
            offset=offset,
            scope=scope_ok,
        )

        channel_identifiers = list({(r[4], r[8]) for r in page_rows if r[8]})
        sub_id_map = _resolve_subscription_ids(
            cursor, current_user.user_id, [ident for _, ident in channel_identifiers]
        )
        items = [_row_to_dashboard_item(r, sub_id_map) for r in page_rows]

        cursor.execute(
            "SELECT name_cn FROM feed_topics WHERE id = %s AND is_enabled = true",
            (topic_id,),
        )
        meta_row = cursor.fetchone()
        topic_name = meta_row[0] if meta_row else "未知主题"

        return FeedsDashboardTopicItemsResponse(
            topic_id=topic_id,
            topic_name=topic_name,
            date=(
                str(report_dates[-1])
                if len(report_dates) == 1
                else f"{report_dates[0]}~{report_dates[-1]}"
            ),
            items=items,
            total=total,
            has_more=(offset + len(items)) < total,
            limit=limit,
            offset=offset,
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"[feeds_dashboard.topic_items] 查询失败 (topic={topic_id}): {exc}")
        raise HTTPException(status_code=500, detail=f"查询失败: {exc}")
    finally:
        conn.close()


@router.post("/generate", response_model=FeedsDashboardGenerateResponse)
def generate_feeds_dashboard(
    payload: FeedsDashboardGenerateRequest,
    current_user: TokenData = Depends(get_current_admin_user),
):
    """手动触发主题聚合 (admin). 指定 date 单日重建, 否则聚合最近 days 天."""
    try:
        from backend.tasks.feed_topic_tasks import (
            aggregate_feed_topics_single_task,
            aggregate_feed_topics_task,
        )

        if payload.date:
            # 校验日期格式 (fail fast)
            _parse_date(payload.date)
            task = aggregate_feed_topics_single_task.delay(report_date=payload.date)
            message = f"✅ 已提交单日聚合任务 ({payload.date}), task_id={task.id}"
        else:
            days = max(1, min(int(payload.days or 2), 30))
            task = aggregate_feed_topics_task.delay(days=days, force=payload.force)
            message = f"✅ 已提交最近 {days} 天聚合任务, task_id={task.id}"

        logger.info("Feeds Dashboard 手动聚合已提交", celery_task_id=task.id)
        return FeedsDashboardGenerateResponse(
            success=True, celery_task_id=task.id, message=message
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"[feeds_dashboard.generate] 分发失败: {exc}")
        raise HTTPException(status_code=500, detail=f"分发失败: {exc}")
