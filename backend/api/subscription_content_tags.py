# -*- coding: utf-8 -*-
"""
Feeds (subscription_content) 自动打标 API

对标 backend/api/file_tags.py, 绑定到 subscription_content + subscription_content_tags.

挂载方式 (main.py):
    app.include_router(
        subscription_content_tags.router,
        prefix="/api/subscriptions",
        tags=["订阅内容标签"],
    )

所以所有路径前缀为 /api/subscriptions/contents/...

设计文档: docs/instructions/FEEDS_AUTO_TAGGING_PLAN.md §7
"""
import json
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from backend.auth.jwt_utils import get_current_user
from backend.database.db_manager import DatabaseManager
from backend.schemas.subscription_content_tags import (
    BatchRetagFeedRequest,
    FeedTaggingTaskStatus,
    RetagFeedRequest,
    SubscriptionContentTagsResponse,
    SubscriptionContentTagsUpdate,
)
from backend.schemas.user import TokenData
from backend.utils.database_config import get_database_config
from backend.utils.logger import get_logger

logger = get_logger("subscription_content_tags_api")

router = APIRouter()


def get_db() -> DatabaseManager:
    return DatabaseManager(get_database_config())


# ─── Helpers ──────────────────────────────────────────────────────


def _row_to_tags_dict(row) -> Dict[str, Any]:
    """将 subscription_content_tags 一行转成 dict 响应体."""
    tickers = row[6] if isinstance(row[6], list) else (row[6] or [])
    concepts = row[7] if isinstance(row[7], list) else (row[7] or [])
    additional_tags = row[8] if row[8] else {}
    people = row[17] if isinstance(row[17], list) else (row[17] or [])
    countries = row[18] if isinstance(row[18], list) else (row[18] or [])
    return {
        "id": row[0],
        "subscription_content_id": row[1],
        "sector": row[2],
        "sector_en": row[3] or "",
        "industry": row[4],
        "industry_en": row[5] or "",
        "tickers": tickers,
        "concepts": concepts,
        "additional_tags": additional_tags,
        "confidence": row[9],
        "reasoning": row[10],
        "summary": row[11],
        "channel_type": row[12],
        "tagged_by": row[13],
        "tagged_at": row[14],
        "created_at": row[15],
        "updated_at": row[16],
        "people": people,
        "countries": countries,
        "tag_schema_version": row[19] or "feeds_v1",
    }


_TAGS_SELECT = """
    SELECT id, subscription_content_id, sector, sector_en,
           industry, industry_en, ticker, concepts,
           additional_tags, confidence, reasoning, summary,
           channel_type, tagged_by, tagged_at,
           created_at, updated_at,
           people, countries, tag_schema_version
    FROM subscription_content_tags
"""


def _assert_content_exists_and_accessible(
    cursor, content_id: int, user_id: int
) -> Dict[str, Any]:
    """校验 subscription_content 行存在, 且当前用户在 user_ids[] 内."""
    cursor.execute(
        """
        SELECT id, channel_type, title, author_name, user_ids
        FROM subscription_content
        WHERE id = %s
        """,
        (content_id,),
    )
    row = cursor.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="内容不存在")
    user_ids = row[4] or []
    if user_id not in user_ids:
        raise HTTPException(status_code=403, detail="无权访问该内容")
    return {
        "id": row[0],
        "channel_type": row[1],
        "title": row[2],
        "author_name": row[3],
    }


# ─── Routes ────────────────────────────────────────────────────────


@router.get(
    "/contents/tags/distinct-tickers",
    summary="获取 Feeds 标签中所有去重 ticker (前端自动补全)",
)
def get_distinct_feed_tickers(
    search: Optional[str] = Query(None, description="按 ticker 代码模糊搜索"),
    channel_type: Optional[str] = Query(
        None, description="可选, 按渠道过滤 (twitter/youtube)"
    ),
    db: DatabaseManager = Depends(get_db),
    _current_user: TokenData = Depends(get_current_user),
):
    conn = db.get_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="数据库连接失败")
    try:
        cursor = conn.cursor()
        sql = """
            SELECT t.ticker_val, COUNT(*) AS content_count
            FROM subscription_content_tags sct,
                 jsonb_array_elements_text(sct.ticker) AS t(ticker_val)
            WHERE sct.ticker IS NOT NULL
        """
        params: List[Any] = []
        if channel_type:
            sql += " AND sct.channel_type = %s"
            params.append(channel_type)
        sql += """
            GROUP BY t.ticker_val
            ORDER BY content_count DESC, t.ticker_val
        """
        cursor.execute(sql, params)
        results = []
        search_upper = search.strip().upper() if search else None
        for row in cursor.fetchall():
            ticker_val = row[0]
            if search_upper and search_upper not in ticker_val.upper():
                continue
            results.append({"ticker": ticker_val, "content_count": row[1]})
        return results
    finally:
        conn.close()


@router.get(
    "/contents/tags/distinct-concepts",
    summary="获取 Feeds 标签中所有去重 concept (前端自动补全)",
)
def get_distinct_feed_concepts(
    search: Optional[str] = Query(None, description="按 concept 中文/英文模糊搜索"),
    channel_type: Optional[str] = Query(None, description="可选, 按渠道过滤"),
    db: DatabaseManager = Depends(get_db),
    _current_user: TokenData = Depends(get_current_user),
):
    conn = db.get_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="数据库连接失败")
    try:
        cursor = conn.cursor()
        sql = """
            SELECT c.concept_val, COUNT(*) AS content_count
            FROM subscription_content_tags sct,
                 jsonb_array_elements_text(sct.concepts) AS c(concept_val)
            WHERE sct.concepts IS NOT NULL AND sct.concepts <> '[]'::jsonb
        """
        params: List[Any] = []
        if channel_type:
            sql += " AND sct.channel_type = %s"
            params.append(channel_type)
        sql += """
            GROUP BY c.concept_val
            ORDER BY content_count DESC, c.concept_val
        """
        cursor.execute(sql, params)
        results = []
        needle = search.strip() if search else None
        for row in cursor.fetchall():
            concept_val = row[0]
            if needle and needle not in concept_val:
                continue
            results.append({"concept": concept_val, "content_count": row[1]})
        return results
    finally:
        conn.close()


@router.get(
    "/contents/{content_id}/tags",
    response_model=SubscriptionContentTagsResponse,
    summary="获取单条 feed content 的 AI 标签",
)
def get_content_tags(
    content_id: int,
    db: DatabaseManager = Depends(get_db),
    current_user: TokenData = Depends(get_current_user),
):
    conn = db.get_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="数据库连接失败")
    try:
        cursor = conn.cursor()
        # 权限校验 (user_ids[] 内)
        _assert_content_exists_and_accessible(cursor, content_id, current_user.user_id)

        cursor.execute(
            _TAGS_SELECT + " WHERE subscription_content_id = %s",
            (content_id,),
        )
        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="标签不存在")
        return _row_to_tags_dict(row)
    finally:
        conn.close()


@router.post(
    "/contents/{content_id}/retag",
    response_model=FeedTaggingTaskStatus,
    summary="异步重新为单条 feed content 打标 + 生成摘要",
)
def retag_content(
    content_id: int,
    request: Optional[RetagFeedRequest] = None,
    db: DatabaseManager = Depends(get_db),
    current_user: TokenData = Depends(get_current_user),
):
    from backend.tasks.feed_tagging_tasks import tag_feed_content_task

    req = request or RetagFeedRequest()
    conn = db.get_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="数据库连接失败")
    try:
        cursor = conn.cursor()
        content_row = _assert_content_exists_and_accessible(
            cursor, content_id, current_user.user_id
        )
    finally:
        conn.close()

    try:
        task = tag_feed_content_task.apply_async(
            args=(content_id, current_user.username, req.additional_context or "", req.force),
            countdown=1,
        )
        logger.info(
            f"已派发 feed 重新打标: content_id={content_id}, task_id={task.id}, "
            f"force={req.force}, user={current_user.username}"
        )
        return {
            "task_id": task.id,
            "status": "pending",
            "message": f"已派发打标任务: {content_row.get('title') or content_row.get('author_name') or content_id}",
            "content_id": content_id,
        }
    except Exception as e:
        logger.error(f"派发 feed 打标任务失败: {e}")
        raise HTTPException(status_code=500, detail=f"派发打标任务失败: {e}")


@router.put(
    "/contents/{content_id}/tags",
    response_model=SubscriptionContentTagsResponse,
    summary="手动编辑 feed content 的标签",
)
def update_content_tags(
    content_id: int,
    updates: SubscriptionContentTagsUpdate,
    db: DatabaseManager = Depends(get_db),
    current_user: TokenData = Depends(get_current_user),
):
    from backend.services.feed_content_tagging_service import (
        FeedContentTaggingService,
    )

    conn = db.get_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="数据库连接失败")
    try:
        cursor = conn.cursor()
        _assert_content_exists_and_accessible(cursor, content_id, current_user.user_id)
        cursor.execute(
            "SELECT id FROM subscription_content_tags WHERE subscription_content_id = %s",
            (content_id,),
        )
        if not cursor.fetchone():
            raise HTTPException(status_code=404, detail="标签不存在")
    finally:
        conn.close()

    update_dict: Dict[str, Any] = {}
    for key in (
        "sector",
        "sector_en",
        "industry",
        "industry_en",
        "tickers",
        "concepts",
        "confidence",
        "reasoning",
        "summary",
        "additional_tags",
    ):
        val = getattr(updates, key)
        if val is not None:
            update_dict[key] = val

    if not update_dict:
        raise HTTPException(status_code=400, detail="没有需要更新的字段")

    try:
        service = FeedContentTaggingService(db_manager=db)
        service.update_tags(
            content_id=content_id,
            updates=update_dict,
            tagged_by=current_user.user_id,
        )
        return get_content_tags(content_id, db, current_user)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"更新 feed 标签失败: {e}")
        raise HTTPException(status_code=500, detail=f"更新 feed 标签失败: {e}")


@router.delete(
    "/contents/{content_id}/tags",
    summary="删除 feed content 的标签 (用户可随后 retag 重新生成)",
)
def delete_content_tags(
    content_id: int,
    db: DatabaseManager = Depends(get_db),
    current_user: TokenData = Depends(get_current_user),
):
    from backend.services.feed_content_tagging_service import (
        FeedContentTaggingService,
    )

    conn = db.get_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="数据库连接失败")
    try:
        cursor = conn.cursor()
        _assert_content_exists_and_accessible(cursor, content_id, current_user.user_id)
    finally:
        conn.close()

    try:
        service = FeedContentTaggingService(db_manager=db)
        result = service.delete_tags(content_id=content_id)
        if result.get("deleted"):
            return {"success": True, "message": "标签已删除"}
        raise HTTPException(status_code=404, detail="标签不存在")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"删除 feed 标签失败: {e}")
        raise HTTPException(status_code=500, detail=f"删除 feed 标签失败: {e}")


@router.post(
    "/contents/batch-retag",
    summary="批量派发 feed content 打标任务",
)
def batch_retag_contents(
    request: BatchRetagFeedRequest,
    current_user: TokenData = Depends(get_current_user),
):
    from backend.tasks.feed_tagging_tasks import batch_tag_feed_content_task

    if not request.content_ids:
        raise HTTPException(status_code=400, detail="content_ids 不能为空")
    if len(request.content_ids) > 500:
        raise HTTPException(status_code=400, detail="单次最多 500 条")

    try:
        task = batch_tag_feed_content_task.apply_async(
            args=(request.content_ids, current_user.username, request.force),
            countdown=1,
        )
        logger.info(
            f"已派发批量 feed 打标: count={len(request.content_ids)}, "
            f"task_id={task.id}, user={current_user.username}"
        )
        return {
            "task_id": task.id,
            "status": "pending",
            "total": len(request.content_ids),
            "message": f"已派发批量打标任务: {len(request.content_ids)} 条",
        }
    except Exception as e:
        logger.error(f"派发批量 feed 打标失败: {e}")
        raise HTTPException(status_code=500, detail=f"派发批量打标失败: {e}")


# ==========================================================================
# Per-User Personalized Summaries (PR-7)
#
# 与 subscription_content_tags 全局共享层并存:
#   subscription_content_tags             → 客观分类 + baseline summary (全局)
#   subscription_content_user_summaries   → per-user 个性化 summary 覆盖
# ==========================================================================

from pydantic import BaseModel, Field  # noqa: E402  (放在末尾避免顶部 import 复杂化)


class PersonalizeRequest(BaseModel):
    """POST /contents/{id}/personalize 请求体."""
    template_id: Optional[int] = Field(
        default=None,
        description="运行时指定的 feed_summary 模板 ID; 未传则走 user_template_preferences[feed_summary]",
    )
    template_variables: Optional[Dict[str, Any]] = Field(
        default=None,
        description="模板运行时变量 (合并到 LLM custom_variables)",
    )
    force: bool = Field(
        default=False,
        description="True 时即使已有同模板个性化也强制重生成",
    )


class PersonalizedSummaryResponse(BaseModel):
    """GET /contents/{id}/personalized-summary 响应体."""
    id: int
    subscription_content_id: int
    user_id: int
    template_id: Optional[int]
    summary: str
    reasoning: Optional[str]
    custom_concepts: List[Any]
    ai_metadata: Dict[str, Any]
    tagged_at: Optional[str]
    updated_at: Optional[str]


@router.post(
    "/contents/{content_id}/personalize",
    summary="(PR-7) 为当前用户生成个性化 feed summary (per-user 覆盖)",
)
def personalize_content_summary(
    content_id: int,
    request: Optional[PersonalizeRequest] = None,
    db: DatabaseManager = Depends(get_db),
    current_user: TokenData = Depends(get_current_user),
):
    """
    异步生成个性化 summary. 与全局 retag 不同: 每用户独立, 不影响其他用户视图.

    模板解析优先级 (LLMTemplateResolver):
        runtime template_id (本请求体)  >
        user_template_preferences[feed_summary]  >
        系统默认 (无 → 抛 ResolveError, 即必须先在 Settings 设偏好)
    """
    from backend.tasks.feed_summary_tasks import personalize_feed_summary_task

    req = request or PersonalizeRequest()

    # 校验内容存在 + 用户有权访问
    conn = db.get_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="数据库连接失败")
    try:
        cursor = conn.cursor()
        content_row = _assert_content_exists_and_accessible(
            cursor, content_id, current_user.user_id
        )
    finally:
        conn.close()

    try:
        task = personalize_feed_summary_task.apply_async(
            args=(content_id, current_user.user_id),
            kwargs={
                "template_id": req.template_id,
                "template_variables": req.template_variables,
                "force": req.force,
            },
            countdown=1,
        )
        logger.info(
            f"已派发 feed summary 个性化: content_id={content_id} "
            f"user_id={current_user.user_id} template_id={req.template_id} "
            f"task_id={task.id}"
        )
        return {
            "task_id": task.id,
            "status": "pending",
            "content_id": content_id,
            "user_id": current_user.user_id,
            "title": content_row.get("title") or content_row.get("author_name"),
            "message": "个性化 summary 任务已派发, 请轮询 task_id 或刷新内容列表查看结果",
        }
    except Exception as e:
        logger.error(f"派发 feed summary 个性化任务失败: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"派发个性化任务失败: {e}",
        )


@router.get(
    "/contents/{content_id}/personalized-summary",
    summary="(PR-7) 获取当前用户对该 feed 的个性化 summary (404 表示无个性化)",
)
def get_personalized_summary(
    content_id: int,
    db: DatabaseManager = Depends(get_db),
    current_user: TokenData = Depends(get_current_user),
):
    """读取 subscription_content_user_summaries; 不存在返回 404."""
    conn = db.get_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="数据库连接失败")
    try:
        cursor = conn.cursor()
        _assert_content_exists_and_accessible(cursor, content_id, current_user.user_id)
        cursor.execute(
            """
            SELECT id, subscription_content_id, user_id, template_id,
                   summary, reasoning, custom_concepts, ai_metadata,
                   tagged_at, updated_at
            FROM subscription_content_user_summaries
            WHERE subscription_content_id = %s AND user_id = %s
            """,
            (content_id, current_user.user_id),
        )
        row = cursor.fetchone()
        if not row:
            raise HTTPException(
                status_code=404,
                detail="No personalized summary; falling back to global summary",
            )
        return {
            "id": row[0],
            "subscription_content_id": row[1],
            "user_id": row[2],
            "template_id": row[3],
            "summary": row[4],
            "reasoning": row[5],
            "custom_concepts": row[6] if isinstance(row[6], list) else (row[6] or []),
            "ai_metadata": row[7] if row[7] else {},
            "tagged_at": row[8].isoformat() if row[8] else None,
            "updated_at": row[9].isoformat() if row[9] else None,
        }
    finally:
        conn.close()


@router.delete(
    "/contents/{content_id}/personalized-summary",
    summary="(PR-7) 取消当前用户对该 feed 的个性化 (回退到全局 summary)",
)
def delete_personalized_summary(
    content_id: int,
    db: DatabaseManager = Depends(get_db),
    current_user: TokenData = Depends(get_current_user),
):
    from backend.services.feed_summary_personalization_service import (
        FeedSummaryPersonalizationService,
    )

    conn = db.get_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="数据库连接失败")
    try:
        cursor = conn.cursor()
        _assert_content_exists_and_accessible(cursor, content_id, current_user.user_id)
    finally:
        conn.close()

    service = FeedSummaryPersonalizationService(db_manager=db)
    deleted = service.delete_for_user(content_id, current_user.user_id)
    if deleted:
        logger.info(
            f"已删除 feed summary 个性化: content_id={content_id} "
            f"user_id={current_user.user_id}"
        )
        return {"success": True, "deleted": True}
    return {"success": True, "deleted": False, "message": "无个性化 summary 可删"}
