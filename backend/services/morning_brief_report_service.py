# -*- coding: utf-8 -*-
"""
Morning Brief Report Service — facts-report LLM agent 管线的报告登记服务

Morning Brief（事实/观点分离版晨报）：全局单例（无 user_id），每天一份。
生成体是外部 LLM agent 管线（tools/facts-report，40-60 分钟），由
task_scheduling(morning_brief) → celery generate_morning_brief 以 detached
子进程派发；本服务不生成内容，只负责：

  - 状态机：mark_generating / complete / fail（report_date upsert，
    report_date = 北京自然日 = 美股交易日 T + 1，与 close_brief 口径一致）
  - 孤儿回收：超过 4 小时仍 generating 的行重置 failed（管线被杀/重启后自愈）
  - 查询：latest / by_date / list / html_by_token（DB html_content 优先，
    OSS oss_object_name 兜底 —— 与 twitter_daily_report 同模式）

HTML 双存：html_content（DB，服务兜底）+ oss_object_name（OSS，键
reports/MorningBrief/report_YYYYMMDD.html 确定性命名，重跑覆盖同键幂等）。
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4

from backend.database.db_manager import DatabaseManager
from backend.utils.database_config import get_database_config
from backend.utils.datetime_utils import utcnow, window_label_date
from backend.utils.logger import get_logger

logger = get_logger("morning_brief_report_service")

# report_date 口径与 close_brief / DMR 一致：北京自然日（生成日）
DEFAULT_REPORT_TIMEZONE = "Asia/Shanghai"

# 管线最长 ~160min（就绪门截 09:40 + run_pipeline 60min），留一倍余量
ORPHAN_THRESHOLD_MINUTES = 240

_COLS = ("id, report_date, status, file_path, oss_object_name, "
         "metadata, error_message, share_token, created_at, updated_at")


class MorningBriefReportService:
    """Morning Brief 报告状态登记与查询（全局单例，每天一份）。"""

    def __init__(self):
        self.db_manager = DatabaseManager(get_database_config())

    # ── 状态机（供 celery 任务 / register_morning_brief.py 调用）─────────

    def mark_generating(self, report_date: date) -> int:
        """创建/重置当日行为 generating，返回行 id。"""
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                INSERT INTO morning_brief_reports (report_date, status)
                VALUES (%s, 'generating')
                ON CONFLICT (report_date) DO UPDATE
                    SET status = 'generating',
                        error_message = NULL,
                        updated_at = NOW()
                RETURNING id
                """,
                (report_date,),
            )
            conn.commit()
            return cursor.fetchone()[0]
        finally:
            cursor.close()

    def complete(
        self,
        report_date: date,
        *,
        file_path: str,
        oss_object_name: Optional[str],
        html_content: str,
        metadata: Dict[str, Any],
    ) -> Dict[str, Any]:
        """登记完成态。html 双存（DB + OSS 键名），每次换新 share_token。"""
        share_token = uuid4().hex
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            from psycopg2.extras import Json

            cursor.execute(
                """
                UPDATE morning_brief_reports
                SET status = 'completed',
                    file_path = %s,
                    oss_object_name = %s,
                    html_content = %s,
                    metadata = %s,
                    share_token = %s,
                    error_message = NULL,
                    updated_at = NOW()
                WHERE report_date = %s
                """,
                (
                    file_path,
                    oss_object_name,
                    html_content,
                    Json(metadata) if metadata is not None else None,
                    share_token,
                    report_date,
                ),
            )
            if cursor.rowcount == 0:  # mark_generating 未先行(手动补挂场景)
                cursor.execute(
                    """
                    INSERT INTO morning_brief_reports (
                        report_date, status, file_path, oss_object_name,
                        html_content, metadata, share_token
                    ) VALUES (%s, 'completed', %s, %s, %s, %s, %s)
                    """,
                    (
                        report_date, file_path, oss_object_name,
                        html_content,
                        Json(metadata) if metadata is not None else None,
                        share_token,
                    ),
                )
            conn.commit()
            logger.info(
                f"Morning Brief registered: date={report_date}, "
                f"oss={'yes' if oss_object_name else 'no'}, "
                f"html={len(html_content or '')} chars"
            )
            return {
                "success": True,
                "report_date": str(report_date),
                "share_token": share_token,
                "oss_object_name": oss_object_name,
            }
        finally:
            cursor.close()

    def fail(self, report_date: date, error_message: str) -> bool:
        """登记失败态（管线 rc!=0 或注册器异常时调用）。"""
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                INSERT INTO morning_brief_reports (report_date, status, error_message)
                VALUES (%s, 'failed', %s)
                ON CONFLICT (report_date) DO UPDATE
                    SET status = 'failed',
                        error_message = EXCLUDED.error_message,
                        updated_at = NOW()
                """,
                (report_date, error_message[:2000]),
            )
            conn.commit()
            return True
        finally:
            cursor.close()

    def reset_orphan_generating(
        self, threshold_minutes: int = ORPHAN_THRESHOLD_MINUTES
    ) -> int:
        """超时 generating 行重置 failed（管线被杀/宕机后下一次派发时自愈）。"""
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                UPDATE morning_brief_reports
                SET status = 'failed',
                    error_message = 'orphan generating reset (pipeline died?)',
                    updated_at = NOW()
                WHERE status = 'generating'
                  AND updated_at < NOW() - (%s * INTERVAL '1 minute')
                """,
                (threshold_minutes,),
            )
            conn.commit()
            if cursor.rowcount:
                logger.warning(
                    f"Morning Brief 孤儿回收: {cursor.rowcount} 行 "
                    f"generating>{threshold_minutes}min → failed"
                )
            return cursor.rowcount
        finally:
            cursor.close()

    # ── 查询（供 API 层调用；列表不含 html 正文）────────────────────────

    def get_report_by_date(self, report_date: date) -> Optional[Dict[str, Any]]:
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                f"SELECT {_COLS} FROM morning_brief_reports "
                "WHERE report_date = %s",
                (report_date,),
            )
            row = cursor.fetchone()
            return self._row_to_dict(row) if row else None
        finally:
            conn.close()

    def get_latest_report(self) -> Optional[Dict[str, Any]]:
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                f"SELECT {_COLS} FROM morning_brief_reports "
                "WHERE status = 'completed' "
                "ORDER BY report_date DESC LIMIT 1"
            )
            row = cursor.fetchone()
            return self._row_to_dict(row) if row else None
        finally:
            conn.close()

    def list_reports(self, limit: int = 20) -> List[Dict[str, Any]]:
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                f"SELECT {_COLS} FROM morning_brief_reports "
                "ORDER BY report_date DESC LIMIT %s",
                (limit,),
            )
            return [self._row_to_dict(r) for r in cursor.fetchall()]
        finally:
            conn.close()

    def get_user_reports(
        self, limit: int = 30, offset: int = 0
    ) -> Tuple[List[Dict[str, Any]], int]:
        """分页获取报告列表（Agent API 统一口径；全局单例,无 user_id）。"""
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT COUNT(*) FROM morning_brief_reports")
            total = cursor.fetchone()[0]
            cursor.execute(
                f"SELECT {_COLS} FROM morning_brief_reports "
                "ORDER BY report_date DESC LIMIT %s OFFSET %s",
                (limit, offset),
            )
            return [self._row_to_dict(r) for r in cursor.fetchall()], total
        finally:
            conn.close()

    def get_report_html_by_token(self, token: str) -> Optional[bytes]:
        """DB html_content 优先，OSS 兜底（twitter_daily 同模式）。"""
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT oss_object_name, html_content
                FROM morning_brief_reports
                WHERE share_token = %s AND status = 'completed'
                LIMIT 1
                """,
                (token,),
            )
            row = cursor.fetchone()
            if not row:
                return None
            obj_name, html = row
            if html:
                return html.encode("utf-8")
            if obj_name:
                from backend.services.oss_service import OSSService

                return OSSService().get_object_bytes(obj_name)
            return None
        finally:
            conn.close()

    @staticmethod
    def get_share_url(oss_object_name: Optional[str],
                      expires: int = 86400) -> Optional[str]:
        """OSS 签名分享链接（24h）；未上传/失败返回 None。"""
        if not oss_object_name:
            return None
        try:
            from backend.services.oss_service import OSSService

            return OSSService().get_signed_url(oss_object_name, expires=expires)
        except Exception as exc:  # noqa: BLE001 — 链接失败不阻断元数据返回
            logger.warning(f"Morning Brief signed url 失败: {exc}")
            return None

    @staticmethod
    def _row_to_dict(row: tuple) -> Dict[str, Any]:
        (rid, report_date, status, file_path, oss_object_name,
         metadata, error_message, share_token, created_at, updated_at) = row
        return {
            "id": rid,
            "report_date": str(report_date),
            "status": status,
            "file_path": file_path,
            "oss_object_name": oss_object_name,
            "metadata": metadata or {},
            "error_message": error_message,
            "share_token": share_token,
            "created_at": created_at.isoformat() if created_at else None,
            "updated_at": updated_at.isoformat() if updated_at else None,
        }
