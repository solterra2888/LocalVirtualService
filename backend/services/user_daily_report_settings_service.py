# -*- coding: utf-8 -*-
"""
用户 Daily Report 自动生成设置服务

管理 user_daily_report_auto_settings 表，提供用户级别的 Twitter / YouTube
Daily Report 自动生成开关。关闭后系统定时任务不再为该用户自动生成报告，
但用户仍可在页面手动触发生成。
"""

from typing import Dict, Any

from backend.database.db_manager import DatabaseManager
from backend.utils.database_config import get_database_config
from backend.utils.logger import get_logger

logger = get_logger("user_daily_report_settings_service")


class UserDailyReportSettingsService:
    """管理每个用户的 Daily Report 自动生成开关。"""

    def __init__(self):
        db_params = get_database_config()
        self.db_manager = DatabaseManager(db_params)

    def get_settings(self, user_id: int) -> Dict[str, Any]:
        """
        获取用户的 Daily Report 自动生成设置。
        若该用户尚无记录，返回默认值（两个开关均为 True）。
        """
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT twitter_auto_enabled, youtube_auto_enabled
                FROM user_daily_report_auto_settings
                WHERE user_id = %s
                """,
                (user_id,),
            )
            row = cursor.fetchone()
            if row:
                return {
                    "twitter_auto_enabled": row[0],
                    "youtube_auto_enabled": row[1],
                }
            return {
                "twitter_auto_enabled": False,
                "youtube_auto_enabled": False,
            }
        finally:
            cursor.close()

    def update_settings(
        self,
        user_id: int,
        twitter_auto_enabled: bool | None = None,
        youtube_auto_enabled: bool | None = None,
    ) -> Dict[str, Any]:
        """
        更新用户的 Daily Report 自动生成设置（UPSERT）。
        仅传入需要修改的字段；另一字段保持原值。

        Returns:
            更新后的完整设置 dict。
        """
        current = self.get_settings(user_id)

        new_twitter = twitter_auto_enabled if twitter_auto_enabled is not None else current["twitter_auto_enabled"]
        new_youtube = youtube_auto_enabled if youtube_auto_enabled is not None else current["youtube_auto_enabled"]

        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                INSERT INTO user_daily_report_auto_settings
                    (user_id, twitter_auto_enabled, youtube_auto_enabled)
                VALUES (%s, %s, %s)
                ON CONFLICT (user_id) DO UPDATE
                    SET twitter_auto_enabled = EXCLUDED.twitter_auto_enabled,
                        youtube_auto_enabled = EXCLUDED.youtube_auto_enabled,
                        updated_at           = CURRENT_TIMESTAMP
                """,
                (user_id, new_twitter, new_youtube),
            )
            conn.commit()
            logger.info(
                "Daily Report 自动生成设置已更新",
                user_id=user_id,
                twitter_auto_enabled=new_twitter,
                youtube_auto_enabled=new_youtube,
            )
            return {
                "twitter_auto_enabled": new_twitter,
                "youtube_auto_enabled": new_youtube,
            }
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()

    def is_twitter_auto_enabled(self, user_id: int) -> bool:
        """快速查询单个用户的 Twitter 自动生成开关。"""
        return self.get_settings(user_id)["twitter_auto_enabled"]

    def is_youtube_auto_enabled(self, user_id: int) -> bool:
        """快速查询单个用户的 YouTube 自动生成开关。"""
        return self.get_settings(user_id)["youtube_auto_enabled"]
