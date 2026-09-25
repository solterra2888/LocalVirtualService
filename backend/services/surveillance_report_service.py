# -*- coding: utf-8 -*-
"""
Surveillance Report Service

Bloomberg Surveillance Report：Bloomberg "Surveillance" 节目每日 AM Brief 的
独立报告（处理方式同 The Close / Closing Bell Report）。

- 内容来源：bloomberg_surveillance_summaries（report_date 唯一，全局单例），
  本服务不调 LLM，只做 Markdown → HTML 渲染 + 模板组装。
- 生成时机：由 generate_bloomberg_surveillance_task 后置钩子驱动（摘要生成/
  跳过后），无独立调度器；摘要 force 重跑会通过 updated_at 新鲜度判断自动
  带动重渲染。
- 占位摘要（no_video_found / no_transcript）不渲染报告，避免产出空报告。
- 幂等：surveillance_reports 以 report_date 唯一键，重复生成覆盖 + 换 share_token。
"""

import json
import os
import time
from datetime import date
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4
from zoneinfo import ZoneInfo

from backend.database.db_manager import DatabaseManager
from backend.utils.database_config import get_database_config
from backend.utils.datetime_utils import utcnow, window_label_date
from backend.utils.logger import get_logger

logger = get_logger("surveillance_report_service")

# report_date 口径与 bloomberg_surveillance_summaries / Daily Market Report 一致：北京自然日
DEFAULT_REPORT_TIMEZONE = "Asia/Shanghai"

# 占位摘要原因（与 bloomberg_surveillance_service.PLACEHOLDER_REASONS 对齐）
PLACEHOLDER_REASONS = {
    "no_video_found", "no_transcript", "episode_not_yet_published",
}

# 报告输出目录
REPORTS_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "reports", "surveillance"
)

_SUMMARY_QUERY = """
    SELECT id, report_date, video_id, video_title, video_url,
           transcript_word_count, summary_content, metadata, updated_at
    FROM bloomberg_surveillance_summaries
    WHERE report_date = %s AND status = 'completed'
"""


class SurveillanceReportService:
    """Bloomberg Surveillance Report 生成服务（全局单例，每天一份）。"""

    def __init__(self):
        self.db_manager = DatabaseManager(get_database_config())
        self.template_dir = os.path.join(
            os.path.dirname(__file__), "..", "templates"
        )

    # ──────────────────────────────────────────
    # 主入口
    # ──────────────────────────────────────────

    def generate(
        self, report_date: Optional[date] = None, force: bool = False
    ) -> Dict[str, Any]:
        """
        渲染并保存指定日期的 Bloomberg Surveillance Report。

        Args:
            report_date: 报告日期，默认北京自然日（与摘要口径一致）。
            force: 是否强制重新渲染（忽略新鲜度判断）。

        Returns:
            dict with keys: success, report_date, skipped, reason,
                            file_path, share_token, metadata
        """
        generated_at_utc = utcnow()
        if report_date is None:
            report_date = window_label_date(generated_at_utc, DEFAULT_REPORT_TIMEZONE)

        conn = self.db_manager.get_connection()
        cursor = conn.cursor()

        try:
            start_ts = time.time()

            # 1. 查询当日已完成的 Surveillance 摘要
            cursor.execute(_SUMMARY_QUERY, (report_date,))
            row = cursor.fetchone()
            if not row:
                return {
                    "success": True,
                    "report_date": str(report_date),
                    "skipped": True,
                    "reason": "summary_not_ready",
                }
            summary = self._summary_row_to_dict(row)

            # 2. 占位摘要不渲染（无节目 / 无转录），避免产出空报告
            if summary["placeholder"]:
                logger.info(
                    f"Surveillance Report skipped: placeholder summary "
                    f"({summary['placeholder_reason']}) for {report_date}"
                )
                return {
                    "success": True,
                    "report_date": str(report_date),
                    "skipped": True,
                    "reason": summary["placeholder_reason"],
                }

            # 3. 新鲜度判断：报告不存在 / 未完成 / 落后于摘要时才渲染
            existing = self._get_existing_report(cursor, report_date)
            if not force and existing and self._is_fresh(existing, summary):
                return {
                    "success": True,
                    "report_date": str(report_date),
                    "skipped": True,
                    "reason": "report_up_to_date",
                }

            # 4. 标记 generating（供前端展示状态）
            cursor.execute(
                """
                INSERT INTO surveillance_reports (report_date, status, updated_at)
                VALUES (%s, 'generating', NOW())
                ON CONFLICT (report_date)
                DO UPDATE SET status = 'generating', error_message = NULL,
                              updated_at = NOW()
                RETURNING id
                """,
                (report_date,),
            )
            report_id = cursor.fetchone()[0]
            conn.commit()

            # 5. 渲染
            summary_html = self._render_markdown_to_html(summary["summary_content"])
            if not summary_html:
                raise ValueError("摘要 Markdown 渲染结果为空")

            gen_at_ny = generated_at_utc.astimezone(ZoneInfo("America/New_York"))
            gen_at_bj = generated_at_utc.astimezone(ZoneInfo("Asia/Shanghai"))

            html = self._render_html(
                report_date=report_date,
                generated_at_utc=generated_at_utc.strftime("%Y-%m-%d %H:%M UTC"),
                generated_at_new_york=gen_at_ny.strftime("%b %d, %Y %H:%M ET"),
                generated_at_beijing=gen_at_bj.strftime("%b %d, %Y %H:%M Beijing"),
                video_title=summary["video_title"],
                video_url=summary["video_url"],
                transcript_word_count=summary["transcript_word_count"],
                video_count=summary["video_count"],
                summary_html=summary_html,
            )

            # 6. 落盘
            file_path = self._save_to_local_file(html, report_date)

            # 7. 更新记录
            elapsed = round(time.time() - start_ts, 1)
            metadata = {
                "generation_time_seconds": elapsed,
                "video_id": summary["video_id"],
                "video_title": summary["video_title"],
                "transcript_word_count": summary["transcript_word_count"],
                "video_count": summary["video_count"],
                "summary_updated_at": summary["updated_at"].isoformat(),
                "generated_at_utc": generated_at_utc.isoformat(),
            }
            share_token = uuid4().hex

            cursor.execute(
                """
                UPDATE surveillance_reports
                SET status = 'completed',
                    file_path = %s,
                    html_content = %s,
                    metadata = %s,
                    share_token = %s,
                    updated_at = NOW()
                WHERE id = %s
                """,
                (file_path, html, json.dumps(metadata), share_token, report_id),
            )
            conn.commit()

            logger.info(
                f"Surveillance Report generated for {report_date}: "
                f"video={summary['video_id']}, elapsed={elapsed}s"
            )
            return {
                "success": True,
                "report_date": str(report_date),
                "skipped": False,
                "file_path": file_path,
                "share_token": share_token,
                "metadata": metadata,
            }

        except Exception as exc:
            conn.rollback()
            cursor.execute(
                """
                UPDATE surveillance_reports
                SET status = 'failed', error_message = %s, updated_at = NOW()
                WHERE report_date = %s
                """,
                (str(exc), report_date),
            )
            conn.commit()
            logger.error(f"Surveillance Report generation failed: {exc}", exc_info=True)
            return {
                "success": False,
                "report_date": str(report_date),
                "error": str(exc),
            }
        finally:
            conn.close()

    # ──────────────────────────────────────────
    # 内部辅助
    # ──────────────────────────────────────────

    @staticmethod
    def _summary_row_to_dict(row: tuple) -> Dict[str, Any]:
        """bloomberg_surveillance_summaries 查询行 → 字典（含占位判断）。"""
        metadata = row[7] or {}
        if isinstance(metadata, str):
            try:
                metadata = json.loads(metadata)
            except ValueError:
                metadata = {}
        placeholder_reason = metadata.get("reason") if isinstance(metadata, dict) else None
        return {
            "id": row[0],
            "report_date": row[1],
            "video_id": row[2],
            "video_title": row[3],
            "video_url": row[4],
            "transcript_word_count": row[5],
            "summary_content": row[6],
            "metadata": metadata,
            "updated_at": row[8],
            "placeholder": placeholder_reason in PLACEHOLDER_REASONS,
            "placeholder_reason": placeholder_reason,
            # 同日多段合并转录：metadata.transcript_segment_count（缺失时回退 1）
            "video_count": (
                metadata.get("transcript_segment_count")
                if isinstance(metadata, dict)
                else None
            ) or 1,
        }

    @staticmethod
    def _get_existing_report(cursor, report_date: date) -> Optional[Dict[str, Any]]:
        cursor.execute(
            """
            SELECT id, status, updated_at FROM surveillance_reports
            WHERE report_date = %s
            """,
            (report_date,),
        )
        row = cursor.fetchone()
        if not row:
            return None
        return {"id": row[0], "status": row[1], "updated_at": row[2]}

    @staticmethod
    def _is_fresh(existing: Dict[str, Any], summary: Dict[str, Any]) -> bool:
        """报告已完成且不落后于摘要（摘要 force 重跑后自动带动重渲染）。"""
        if existing["status"] != "completed":
            return False
        if not existing["updated_at"] or not summary["updated_at"]:
            return False
        return existing["updated_at"] >= summary["updated_at"]

    def _render_html(
        self,
        report_date: date,
        generated_at_utc: str,
        generated_at_new_york: str,
        generated_at_beijing: str,
        video_title: Optional[str],
        video_url: Optional[str],
        transcript_word_count: Optional[int],
        video_count: int,
        summary_html: str,
    ) -> str:
        """通过 Jinja2 渲染自包含 HTML。"""
        from jinja2 import Environment, FileSystemLoader, select_autoescape
        from markupsafe import Markup

        env = Environment(
            loader=FileSystemLoader(os.path.abspath(self.template_dir)),
            autoescape=select_autoescape(["html"]),
        )

        template = env.get_template("surveillance_report.html.j2")
        return template.render(
            report_date=report_date.strftime("%B %d, %Y"),
            generated_at_utc=generated_at_utc,
            generated_at_new_york=generated_at_new_york,
            generated_at_beijing=generated_at_beijing,
            video_title=video_title,
            video_url=video_url,
            transcript_word_count=transcript_word_count,
            video_count=video_count,
            summary_html=Markup(summary_html),
        )

    @staticmethod
    def _render_markdown_to_html(md_text: str) -> str:
        """将 Markdown 转换为 HTML（与 Daily Market Report 同口径，含表格插件）。"""
        if not md_text:
            return ""
        import mistune
        import mistune.plugins.table as table_plugin

        renderer = mistune.HTMLRenderer()
        markdown_parser = mistune.Markdown(renderer, plugins=[table_plugin.table])
        return markdown_parser(md_text)

    @staticmethod
    def _save_to_local_file(html: str, report_date: date) -> str:
        """保存 HTML 到 reports/surveillance/YYYYMMDD.html。"""
        os.makedirs(REPORTS_DIR, exist_ok=True)
        filename = f"{report_date.strftime('%Y%m%d')}.html"
        file_path = os.path.join(REPORTS_DIR, filename)
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(html)
        logger.info(f"Surveillance Report saved to {file_path}")
        return file_path

    # ──────────────────────────────────────────
    # 查询接口
    # ──────────────────────────────────────────

    def get_latest_report(self) -> Optional[Dict[str, Any]]:
        """返回最新的已完成报告（不含正文）。"""
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT id, report_date, file_path, metadata, status,
                       error_message, created_at, share_token
                FROM surveillance_reports
                WHERE status = 'completed'
                ORDER BY report_date DESC
                LIMIT 1
                """
            )
            row = cursor.fetchone()
            return self._row_to_dict(row) if row else None
        finally:
            conn.close()

    def get_report_by_date(self, report_date: date) -> Optional[Dict[str, Any]]:
        """按日期返回报告元数据（不含正文）。"""
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT id, report_date, file_path, metadata, status,
                       error_message, created_at, share_token
                FROM surveillance_reports
                WHERE report_date = %s
                """,
                (report_date,),
            )
            row = cursor.fetchone()
            return self._row_to_dict(row) if row else None
        finally:
            conn.close()

    def get_user_reports(
        self, limit: int = 30, offset: int = 0
    ) -> Tuple[List[Dict[str, Any]], int]:
        """分页获取报告列表（供 Agent API 使用）。全局单例，无 user_id。"""
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT COUNT(*) FROM surveillance_reports")
            total = cursor.fetchone()[0]
            cursor.execute(
                """
                SELECT id, report_date, file_path, metadata, status,
                       error_message, created_at, share_token
                FROM surveillance_reports
                ORDER BY report_date DESC
                LIMIT %s OFFSET %s
                """,
                (limit, offset),
            )
            return [self._row_to_dict(row) for row in cursor.fetchall()], total
        finally:
            conn.close()

    def list_reports(self, limit: int = 20) -> List[Dict[str, Any]]:
        """返回最近的报告列表（不含正文）。"""
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT id, report_date, file_path, metadata, status,
                       error_message, created_at, share_token
                FROM surveillance_reports
                ORDER BY report_date DESC
                LIMIT %s
                """,
                (limit,),
            )
            return [self._row_to_dict(row) for row in cursor.fetchall()]
        finally:
            conn.close()

    def get_report_html_by_token(self, token: str) -> Optional[bytes]:
        """通过 share_token 获取报告 HTML。"""
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT html_content FROM surveillance_reports
                WHERE share_token = %s AND status = 'completed'
                """,
                (token,),
            )
            row = cursor.fetchone()
            if not row or not row[0]:
                return None
            return row[0].encode("utf-8")
        finally:
            conn.close()

    @staticmethod
    def _row_to_dict(row: tuple) -> Dict[str, Any]:
        rid, report_date, file_path, metadata, status, error_message, created_at, share_token = row
        return {
            "id": rid,
            "report_date": str(report_date),
            "file_path": file_path,
            "metadata": metadata or {},
            "status": status,
            "error_message": error_message,
            "created_at": created_at.isoformat() if created_at else None,
            "share_token": share_token,
        }
