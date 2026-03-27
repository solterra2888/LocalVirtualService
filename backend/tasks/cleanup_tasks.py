# -*- coding: utf-8 -*-
"""
数据清理相关 Celery 任务
定时清理过期的 LLM usage logs，防止表无限膨胀
"""

from backend.celery_app import app
from backend.utils.database_config import get_database_config
from backend.database.db_manager import DatabaseManager
from backend.utils.logger import get_logger

logger = get_logger("cleanup_tasks")

USAGE_LOG_RETAIN_DAYS = 7


@app.task(bind=True, name='cleanup_llm_usage_logs')
def cleanup_llm_usage_logs_task(self, retain_days: int = USAGE_LOG_RETAIN_DAYS):
    """
    清理超过 retain_days 天的 llm_prompt_usage_logs 记录。

    默认保留最近 7 天的数据。通过 Celery Beat 每天凌晨执行一次，
    也可手动触发：cleanup_llm_usage_logs_task.delay(retain_days=7)
    """
    try:
        db_config = get_database_config()
        db_manager = DatabaseManager(db_config)
        conn = db_manager.get_connection()

        if not conn:
            logger.error("cleanup_llm_usage_logs: 无法获取数据库连接")
            return {"success": False, "message": "数据库连接失败"}

        cursor = conn.cursor()

        cursor.execute(
            "SELECT COUNT(*) FROM llm_prompt_usage_logs "
            "WHERE created_at < CURRENT_TIMESTAMP - INTERVAL %s",
            (f"{retain_days} days",),
        )
        count = cursor.fetchone()[0]

        if count == 0:
            cursor.close()
            conn.close()
            logger.info(f"cleanup_llm_usage_logs: 没有超过 {retain_days} 天的记录需要清理")
            return {"success": True, "deleted": 0, "retain_days": retain_days}

        cursor.execute(
            "DELETE FROM llm_prompt_usage_logs "
            "WHERE created_at < CURRENT_TIMESTAMP - INTERVAL %s",
            (f"{retain_days} days",),
        )
        conn.commit()
        cursor.close()
        conn.close()

        logger.info(f"cleanup_llm_usage_logs: 已清理 {count} 条超过 {retain_days} 天的日志")
        return {"success": True, "deleted": count, "retain_days": retain_days}

    except Exception as e:
        logger.error(f"cleanup_llm_usage_logs 失败: {str(e)}")
        if conn:
            try:
                conn.rollback()
                conn.close()
            except Exception:
                pass
        return {"success": False, "message": str(e)}
