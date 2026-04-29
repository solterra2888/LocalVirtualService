# -*- coding: utf-8 -*-
"""
User Feed Summary Settings Service (PR-N1)

管理 user_feed_summary_settings 表的 CRUD:
    - 查询用户对各渠道的开关 / 模板 / 长度门槛配置
    - upsert (PUT 接口语义)
    - fan-out 任务用的批量查询 (find_eligible_users)

调用方:
    - backend/api/feed_summary_settings.py        (HTTP API)
    - backend/tasks/feed_summary_tasks.py         (fan_out_personalize_summary_task)

设计要点:
    - 不允许同时改 channel_type / user_id (主键稳定); 只能更新 enabled/template_id/min_length
    - find_eligible_users 用 partial index (channel_type, user_id WHERE enabled=true) 命中
    - 校验 template_id 是否存在 + 属于对应 module (feed_summary / youtube_summary)
      由 API 层处理 (这里只做 DB 操作, 不强加业务规则).
"""
from typing import Any, Dict, List, Optional

from backend.database.db_manager import DatabaseManager
from backend.utils.logger import get_logger

logger = get_logger("user_feed_summary_settings_service")

SUPPORTED_CHANNELS = ("twitter", "youtube")


class UserFeedSummarySettingsService:
    """user_feed_summary_settings CRUD"""

    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager

    # ──────────────────────────────────────────────────────────────────
    # 查询
    # ──────────────────────────────────────────────────────────────────

    def list_for_user(self, user_id: int) -> List[Dict[str, Any]]:
        """
        返回用户对所有渠道的设置 (未配置过的渠道返回默认 disabled 行).

        一定返回 len == len(SUPPORTED_CHANNELS) 的列表, 让前端 UI 可以
        直接渲染两个 section 而不需要担心 "用户从未访问过设置页" 的空状态.
        """
        conn = self.db_manager.get_connection()
        if not conn:
            raise ValueError("无法连接到数据库")
        try:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT id, channel_type, enabled, template_id, min_length,
                       created_at, updated_at
                FROM user_feed_summary_settings
                WHERE user_id = %s
                """,
                (user_id,),
            )
            rows = cur.fetchall() or []
            existing = {
                row[1]: {
                    "id": row[0],
                    "channel_type": row[1],
                    "enabled": row[2],
                    "template_id": row[3],
                    "min_length": row[4],
                    "created_at": row[5].isoformat() if row[5] else None,
                    "updated_at": row[6].isoformat() if row[6] else None,
                }
                for row in rows
            }

            result = []
            for ch in SUPPORTED_CHANNELS:
                if ch in existing:
                    result.append(existing[ch])
                else:
                    result.append(
                        {
                            "id": None,
                            "channel_type": ch,
                            "enabled": False,
                            "template_id": None,
                            "min_length": None,
                            "created_at": None,
                            "updated_at": None,
                        }
                    )
            return result
        finally:
            conn.close()

    def get(
        self, user_id: int, channel_type: str
    ) -> Optional[Dict[str, Any]]:
        """单条查询 (channel_type 不存在时返回 None)."""
        if channel_type not in SUPPORTED_CHANNELS:
            raise ValueError(
                f"channel_type 必须为 {SUPPORTED_CHANNELS} 之一, got {channel_type!r}"
            )
        conn = self.db_manager.get_connection()
        if not conn:
            raise ValueError("无法连接到数据库")
        try:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT id, channel_type, enabled, template_id, min_length,
                       created_at, updated_at
                FROM user_feed_summary_settings
                WHERE user_id = %s AND channel_type = %s
                """,
                (user_id, channel_type),
            )
            row = cur.fetchone()
            if not row:
                return None
            return {
                "id": row[0],
                "channel_type": row[1],
                "enabled": row[2],
                "template_id": row[3],
                "min_length": row[4],
                "created_at": row[5].isoformat() if row[5] else None,
                "updated_at": row[6].isoformat() if row[6] else None,
            }
        finally:
            conn.close()

    # ──────────────────────────────────────────────────────────────────
    # 更新 (UPSERT)
    # ──────────────────────────────────────────────────────────────────

    def upsert(
        self,
        user_id: int,
        channel_type: str,
        enabled: bool,
        template_id: Optional[int] = None,
        min_length: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        创建或更新 (user_id, channel_type) 行.

        Returns:
            完整字段 dict (与 get() 一致)
        """
        if channel_type not in SUPPORTED_CHANNELS:
            raise ValueError(
                f"channel_type 必须为 {SUPPORTED_CHANNELS} 之一, got {channel_type!r}"
            )
        if min_length is not None and min_length < 0:
            raise ValueError("min_length 必须为非负整数")

        conn = self.db_manager.get_connection()
        if not conn:
            raise ValueError("无法连接到数据库")
        try:
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO user_feed_summary_settings (
                    user_id, channel_type, enabled, template_id, min_length
                ) VALUES (
                    %s, %s, %s, %s, %s
                )
                ON CONFLICT (user_id, channel_type) DO UPDATE SET
                    enabled     = EXCLUDED.enabled,
                    template_id = EXCLUDED.template_id,
                    min_length  = EXCLUDED.min_length,
                    updated_at  = CURRENT_TIMESTAMP
                RETURNING id, channel_type, enabled, template_id, min_length,
                          created_at, updated_at
                """,
                (user_id, channel_type, enabled, template_id, min_length),
            )
            row = cur.fetchone()
            conn.commit()
            logger.info(
                f"UFSS upsert: user_id={user_id} channel={channel_type} "
                f"enabled={enabled} template_id={template_id} "
                f"min_length={min_length}"
            )
            return {
                "id": row[0],
                "channel_type": row[1],
                "enabled": row[2],
                "template_id": row[3],
                "min_length": row[4],
                "created_at": row[5].isoformat() if row[5] else None,
                "updated_at": row[6].isoformat() if row[6] else None,
            }
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    # ──────────────────────────────────────────────────────────────────
    # fan-out 用: 找出某渠道下所有 enabled=true 的用户
    # ──────────────────────────────────────────────────────────────────

    def find_eligible_users(
        self,
        channel_type: str,
        user_id_pool: Optional[List[int]] = None,
    ) -> List[Dict[str, Any]]:
        """
        返回某渠道下所有 enabled=true 的 (user_id, template_id, min_length).

        Args:
            channel_type: 'twitter' | 'youtube'
            user_id_pool: 限定用户范围 (通常是 subscription_content.user_ids).
                          为 None 时返回该渠道全平台开启用户 (一般只在测试用).

        Returns:
            [{user_id, template_id, min_length}, ...]
        """
        if channel_type not in SUPPORTED_CHANNELS:
            raise ValueError(
                f"channel_type 必须为 {SUPPORTED_CHANNELS} 之一, got {channel_type!r}"
            )
        conn = self.db_manager.get_connection()
        if not conn:
            raise ValueError("无法连接到数据库")
        try:
            cur = conn.cursor()
            if user_id_pool:
                cur.execute(
                    """
                    SELECT user_id, template_id, min_length
                    FROM   user_feed_summary_settings
                    WHERE  channel_type = %s
                      AND  enabled = TRUE
                      AND  user_id = ANY(%s)
                    """,
                    (channel_type, list(user_id_pool)),
                )
            else:
                cur.execute(
                    """
                    SELECT user_id, template_id, min_length
                    FROM   user_feed_summary_settings
                    WHERE  channel_type = %s
                      AND  enabled = TRUE
                    """,
                    (channel_type,),
                )
            rows = cur.fetchall() or []
            return [
                {"user_id": r[0], "template_id": r[1], "min_length": r[2]}
                for r in rows
            ]
        finally:
            conn.close()
