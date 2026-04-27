# -*- coding: utf-8 -*-
"""
用户标识解析工具

背景:
    系统中 user 标识存在双轨制 (历史决策 D1.C 仅新表用 INT FK):
      - 旧表 (processing_stats / file_folders 等) 用 username (TEXT)
      - 新表 (user_template_preferences / file_shares 等) 用 user_id (INT FK)
      - LLM usage_log / 其他可观测组件统一用 user_id (INT)

    Celery task 通常只拿到 username (从 API 上下文透传), 但下游 LLM 调用
    需要 user_id 才能正确归属到 llm_prompt_usage_logs.user_id. 本工具提供
    username → user_id 的轻量级查询.

设计原则:
    - 失败安静: 找不到 / DB 故障时返回 None, 不抛错 (LLM 流程不应被用户解析中断)
    - 进程内 LRU 缓存: 避免每次 LLM 调用都跑一次 SELECT (Celery worker 进程内复用)
"""
from functools import lru_cache
from typing import Optional

from backend.database.db_manager import DatabaseManager
from backend.utils.database_config import get_database_config
from backend.utils.logger import get_logger

logger = get_logger("user_resolver")


@lru_cache(maxsize=512)
def get_user_id_by_username(username: Optional[str]) -> Optional[int]:
    """
    根据 username 查 user_id; 找不到或异常时返回 None.

    Args:
        username: 用户名 (允许为 None / 空字符串, 直接返回 None)

    Returns:
        user_id (INT) 或 None
    """
    if not username:
        return None

    try:
        db = DatabaseManager(get_database_config())
        conn = db.get_connection()
        if not conn:
            return None
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT id FROM users WHERE username = %s LIMIT 1",
                (username,),
            )
            row = cur.fetchone()
            return int(row[0]) if row else None
        finally:
            conn.close()
    except Exception as e:
        logger.warning(f"get_user_id_by_username failed: username={username}, error={e}")
        return None


def clear_cache():
    """清空 LRU 缓存 (单测 / 用户名变更后调用)."""
    get_user_id_by_username.cache_clear()
