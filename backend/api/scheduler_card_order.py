# -*- coding: utf-8 -*-
"""
Scheduler 卡片排序偏好 API — 每用户独立持久化 SchedulerPage 的卡片拖拽顺序。

注意：与 scheduler_proxy.py（把请求转发给 8001 调度引擎）不同，这里直连主库，
因为卡片排序是纯 UI 偏好（前端硬编码的静态目录顺序），与调度引擎无关。
架构与 subscriptions.py 的 reorder 端点一致。
"""

from fastapi import APIRouter, HTTPException, Depends
from typing import List, Literal
from pydantic import BaseModel

from backend.auth.jwt_utils import get_current_user
from backend.schemas.user import TokenData
from backend.database.db_manager import DatabaseManager
from backend.utils.database_config import get_database_config
from backend.utils.logger import get_logger

logger = get_logger("scheduler_card_order")

router = APIRouter()


def get_db_manager():
    """获取数据库管理器实例"""
    db_params = get_database_config()
    return DatabaseManager(db_params)


class ReorderCardItem(BaseModel):
    """单个卡片的排序更新项"""
    source_id: str
    sort_order: int


class ReorderCardsRequest(BaseModel):
    """批量重排卡片请求（按 category 维度提交单列全量顺序）"""
    category: Literal["data_fetch", "report_generation"]
    items: List[ReorderCardItem]


@router.get("/card-order", summary="获取当前用户的 Scheduler 卡片排序")
async def get_card_order(current_user: TokenData = Depends(get_current_user)):
    """
    返回当前用户的卡片排序偏好。

    懒加载：用户从未拖拽过时各 category 返回空数组，前端用静态目录默认顺序兜底。
    """
    user_id = current_user.user_id
    db_manager = get_db_manager()
    conn = None
    try:
        conn = db_manager.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT category, source_id FROM scheduler_card_order "
            "WHERE user_id = %s ORDER BY category, sort_order",
            (user_id,)
        )
        result = {"data_fetch": [], "report_generation": []}
        for category, source_id in cursor.fetchall():
            result.setdefault(category, []).append(source_id)
        cursor.close()
        return {"success": True, "data": result}
    except Exception as e:
        logger.error(f"Failed to get card order for user {user_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get card order: {e}")
    finally:
        if conn:
            conn.close()


@router.put("/reorder", summary="更新当前用户某列的卡片排序")
async def reorder_cards(
    request: ReorderCardsRequest,
    current_user: TokenData = Depends(get_current_user)
):
    """
    批量更新当前用户某 category 的卡片顺序（拖拽排序持久化）。

    请求体: {"category": "data_fetch"|"report_generation",
             "items": [{"source_id": str, "sort_order": int}, ...]}

    策略：DELETE 该用户该 category 的旧记录 + 循环 INSERT（事务内），
    天然清理掉前端静态目录里已删除的 slug，无需额外维护白名单。
    """
    if not request.items:
        raise HTTPException(status_code=400, detail="Items must be a non-empty list")
    if any(item.sort_order < 1 for item in request.items):
        raise HTTPException(status_code=400, detail="sort_order must be >= 1")

    user_id = current_user.user_id
    db_manager = get_db_manager()
    conn = None
    try:
        conn = db_manager.get_connection()
        cursor = conn.cursor()

        # 清空该用户该 category 的旧顺序（同时清理掉已从静态目录里删除的 slug）
        cursor.execute(
            "DELETE FROM scheduler_card_order WHERE user_id = %s AND category = %s",
            (user_id, request.category)
        )
        for item in request.items:
            cursor.execute(
                "INSERT INTO scheduler_card_order (user_id, source_id, category, sort_order) "
                "VALUES (%s, %s, %s, %s)",
                (user_id, item.source_id, request.category, item.sort_order)
            )

        conn.commit()
        cursor.close()
        logger.info(f"Reordered {len(request.items)} cards ({request.category}) for user {user_id}")
        return {"success": True, "updated": len(request.items)}
    except HTTPException:
        if conn:
            conn.rollback()
        raise
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Failed to reorder cards for user {user_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to reorder cards: {e}")
    finally:
        if conn:
            conn.close()
