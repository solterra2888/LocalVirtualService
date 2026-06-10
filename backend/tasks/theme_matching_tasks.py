# -*- coding: utf-8 -*-
"""
内容主题匹配 Celery Tasks

在内容打标完成后，自动将内容关联到 Investment Themes：
  - match_content_themes_ticker: 基于 ticker 重叠（纯 SQL，零 LLM）
  - match_content_themes_vector: 基于向量语义（一次 Qwen embedding 调用）
  - backfill_content_theme_matches: 批量回填历史内容

触发方式:
  链式: tag_file_content_task / tag_feed_content_task / map_news_tags_task
        → match_content_themes_ticker_task
        → [可选] match_content_themes_vector_task
"""

import asyncio
import os
from typing import Any, Dict, Optional

from celery import current_task

from backend.celery_app import app
from backend.utils.database_config import get_database_config
from backend.utils.logger import get_logger

logger = get_logger("theme_matching_tasks")

# ─── 环境变量 ────────────────────────────────────────────────
_TICKER_ENABLED_ENV = "THEME_TICKER_MATCHING_ENABLED"
_VECTOR_ENABLED_ENV = "THEME_VECTOR_MATCHING_ENABLED"


def _is_ticker_enabled() -> bool:
    return os.getenv(_TICKER_ENABLED_ENV, "true").lower() == "true"


def _is_vector_enabled() -> bool:
    return os.getenv(_VECTOR_ENABLED_ENV, "false").lower() == "true"


# ─── Phase 1: Ticker 匹配 ──────────────────────────────────


@app.task(
    bind=True,
    name="match_content_themes_ticker",
    queue="file_processing",
    autoretry_for=(Exception,),
    retry_kwargs={"max_retries": 2, "countdown": 15},
    retry_backoff=True,
)
def match_content_themes_ticker_task(
    self, content_type: str, content_id: int
) -> Dict[str, Any]:
    """
    Phase 1: 基于 ticker 重叠的主题匹配（纯 SQL，零 LLM）。

    匹配完成后，若 THEME_VECTOR_MATCHING_ENABLED=true，自动链式派发向量匹配。

    Args:
        content_type: 'file' | 'feed'
        content_id: file_processing_details.id 或 subscription_content.id
    """
    if not _is_ticker_enabled():
        logger.debug("Ticker 匹配已禁用 (THEME_TICKER_MATCHING_ENABLED=false)")
        return {"status": "skipped", "reason": "disabled"}

    logger.info(
        f"Ticker 匹配开始: content_type={content_type} content_id={content_id}"
    )

    from backend.services.content_theme_matching_service import (
        ContentThemeMatchingService,
    )

    service = ContentThemeMatchingService()
    result = service.match_by_tickers(content_type, content_id)

    # Phase 2 链式触发
    if (
        _is_vector_enabled()
        and result.get("status") == "completed"
        and result.get("matches_count", 0) >= 0  # 即使 0 matches 也做向量匹配
    ):
        try:
            match_content_themes_vector_task.apply_async(
                args=(content_type, content_id),
                countdown=2,
            )
            logger.info(
                f"已链式派发 vector 匹配: content_type={content_type} content_id={content_id}"
            )
        except Exception as chain_err:
            logger.error(f"派发 vector 匹配失败: {chain_err}")

    return result


# ─── Phase 2: Vector 匹配 ──────────────────────────────────


@app.task(
    bind=True,
    name="match_content_themes_vector",
    queue="ai_processing",
    autoretry_for=(Exception,),
    retry_kwargs={"max_retries": 2, "countdown": 30},
    retry_backoff=True,
)
def match_content_themes_vector_task(
    self, content_type: str, content_id: int
) -> Dict[str, Any]:
    """
    Phase 2: 基于向量语义的主题匹配（一次 Qwen embedding 调用）。

    Args:
        content_type: 'file' | 'feed'
        content_id: 内容 ID
    """
    if not _is_vector_enabled():
        return {"status": "skipped", "reason": "disabled"}

    logger.info(
        f"Vector 匹配开始: content_type={content_type} content_id={content_id}"
    )

    from backend.services.content_theme_matching_service import (
        ContentThemeMatchingService,
    )

    service = ContentThemeMatchingService()
    loop = asyncio.new_event_loop()
    try:
        result = loop.run_until_complete(
            service.match_by_vector(content_type, content_id)
        )
        return result
    finally:
        loop.close()


# ─── 批量回填 ────────────────────────────────────────────────


@app.task(
    bind=True,
    name="backfill_content_theme_matches",
    queue="ai_processing",
    autoretry_for=(Exception,),
    retry_kwargs={"max_retries": 1, "countdown": 60},
)
def backfill_content_theme_matches_task(
    self,
    content_type: Optional[str] = None,
    force: bool = False,
    hours: int = 48,
) -> Dict[str, Any]:
    """
    批量回填: 为过去 N 小时内有 tickers 的已标记内容生成主题匹配。

    Args:
        content_type: 'file' | 'feed' | None (两者都回填)
        force: 是否重新匹配已有记录
        hours: 回填时间窗口（小时），默认 48
    """
    from datetime import datetime, timezone, timedelta

    since = datetime.now(timezone.utc) - timedelta(hours=hours)

    logger.info(
        f"回填开始: content_type={content_type or 'all'} force={force} "
        f"since={since.isoformat()} (past {hours}h)"
    )

    from backend.database.db_manager import DatabaseManager

    db = DatabaseManager(get_database_config())
    conn = db.get_connection()
    if not conn:
        return {"status": "failed", "reason": "db_connection_failed"}

    total_dispatched = 0

    try:
        cursor = conn.cursor()

        # File 侧: 向量匹配需要 summary 或 content 文本，不依赖 ticker
        if content_type in (None, "file"):
            if force:
                cursor.execute(
                    """
                    SELECT ft.file_id FROM file_tags ft
                    JOIN file_processing_details fpd ON fpd.id = ft.file_id
                    WHERE (ft.summary IS NOT NULL AND LENGTH(ft.summary) > 10
                           OR fpd.content IS NOT NULL AND LENGTH(fpd.content) > 50)
                      AND fpd.created_at >= %s
                    """,
                    (since,),
                )
            else:
                cursor.execute(
                    """
                    SELECT ft.file_id FROM file_tags ft
                    JOIN file_processing_details fpd ON fpd.id = ft.file_id
                    WHERE (ft.summary IS NOT NULL AND LENGTH(ft.summary) > 10
                           OR fpd.content IS NOT NULL AND LENGTH(fpd.content) > 50)
                      AND fpd.created_at >= %s
                      AND NOT EXISTS (
                        SELECT 1 FROM content_theme_matches ctm
                        WHERE ctm.content_type='file' AND ctm.content_id=ft.file_id
                      )
                    """,
                    (since,),
                )
            file_ids = [row[0] for row in cursor.fetchall()]
            for idx, fid in enumerate(file_ids):
                match_content_themes_vector_task.apply_async(
                    args=("file", fid),
                    countdown=idx * 0.5,
                )
            total_dispatched += len(file_ids)
            logger.info(f"File 回填: {len(file_ids)} 条待处理 (past {hours}h, vector)")

        # Feed 侧: 向量匹配需要 title+summary 文本，不依赖 ticker
        if content_type in (None, "feed"):
            if force:
                cursor.execute(
                    """
                    SELECT sc.id
                    FROM subscription_content sc
                    LEFT JOIN subscription_content_tags sct ON sct.subscription_content_id = sc.id
                    WHERE (sct.summary IS NOT NULL AND LENGTH(sct.summary) > 10
                           OR sc.content IS NOT NULL AND LENGTH(sc.content) > 50)
                      AND sc.published_at >= %s
                    """,
                    (since,),
                )
            else:
                cursor.execute(
                    """
                    SELECT sc.id
                    FROM subscription_content sc
                    LEFT JOIN subscription_content_tags sct ON sct.subscription_content_id = sc.id
                    WHERE (sct.summary IS NOT NULL AND LENGTH(sct.summary) > 10
                           OR sc.content IS NOT NULL AND LENGTH(sc.content) > 50)
                      AND sc.published_at >= %s
                      AND NOT EXISTS (
                        SELECT 1 FROM content_theme_matches ctm
                        WHERE ctm.content_type='feed'
                          AND ctm.content_id=sc.id
                      )
                    """,
                    (since,),
                )
            feed_ids = [row[0] for row in cursor.fetchall()]
            for idx, fid in enumerate(feed_ids):
                match_content_themes_vector_task.apply_async(
                    args=("feed", fid),
                    countdown=idx * 0.5,
                )
            total_dispatched += len(feed_ids)
            logger.info(f"Feed 回填: {len(feed_ids)} 条待处理 (past {hours}h, vector)")

        conn.commit()
        return {
            "status": "dispatched",
            "total_dispatched": total_dispatched,
            "hours": hours,
            "since": since.isoformat(),
        }

    except Exception as e:
        conn.rollback()
        logger.error(f"回填失败: {e}")
        return {"status": "failed", "reason": str(e)}
    finally:
        conn.close()
