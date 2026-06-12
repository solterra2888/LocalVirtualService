# -*- coding: utf-8 -*-
"""
The Close Summary 服务 — Bloomberg "The Close" 每日 PM Brief 摘要

独立管线，从 subscription_content 中筛选 Bloomberg Television 频道的
"The Close" 节目转录，通过 LLM 生成八段式 PM Brief 摘要。

与 YouTubeAuthorSummaryService 的区别：
- 无用户维度（全局报告）
- 专用的八段式 PM Brief 模板（非通用频道摘要）
- 仅处理 Bloomberg The Close 单一节目
"""

import time
from datetime import date, datetime
from typing import Any, Dict, List, Optional

from backend.database.db_manager import DatabaseManager
from backend.utils.database_config import get_database_config
from backend.utils.datetime_utils import ensure_utc, resolve_report_content_window
from backend.utils.logger import get_logger

logger = get_logger("the_close_summary_service")

# Bloomberg Television YouTube channel_id
BLOOMBERG_TV_CHANNEL_ID = "UCIALMKvObZNtJ6AmdCLP7Lg"
# 经 _TRANSCRIPTA_CANONICAL 标准化后的 show_name
THE_CLOSE_SHOW_NAME = "Bloomberg The Close"
# LLM 模板（max_tokens / provider / model 由 llm_prompt_templates 配置，经 ConfigResolver 解析）
DEFAULT_TEMPLATE = "the_close_pm_brief"
# 转录文本截断阈值（字符数，约 ~20,000 英文单词的 4 倍估算）
MAX_TRANSCRIPT_CHARS = 80000
# 标题匹配：Bloomberg TV 频道内 "| The Close" / "The Close M/D/..." 均为该节目；
# 已限定 author_identifier，不会误匹配其他频道。
THE_CLOSE_TITLE_PATTERN = "%The Close%"


def is_bloomberg_the_close_video(
    title: Optional[str],
    show_name: Optional[str] = None,
) -> bool:
    """判断是否为 Bloomberg Television The Close 节目视频。"""
    if show_name == THE_CLOSE_SHOW_NAME:
        return True
    if not title:
        return False
    return "The Close" in title


class TheCloseSummaryService:
    """Bloomberg The Close 每日 PM Brief 摘要生成服务"""

    def __init__(self):
        db_params = get_database_config()
        self.db_manager = DatabaseManager(db_params)

    # ── Public API ─────────────────────────────────────────────────────────

    async def generate(
        self,
        report_date: date,
        window_end: Optional[datetime] = None,
        window_hours: int = 24,
        force: bool = False,
    ) -> Dict[str, Any]:
        """
        为指定日期生成 The Close PM Brief 摘要。

        Args:
            report_date: 报告日期（美东交易日）
            window_end: 内容窗口结束时间（UTC），默认当前时刻
            window_hours: 回溯小时数，默认 24
            force: 是否强制重新生成

        Returns:
            {'success': bool, 'summary_id': int, 'message': str, ...}
        """
        start_time = time.time()
        window_start, resolved_end = resolve_report_content_window(
            report_date, window_end, window_hours
        )
        logger.info(
            f"开始生成 The Close 摘要: date={report_date}, "
            f"window=[{window_start.isoformat()}, {resolved_end.isoformat()})"
        )

        # 幂等检查：已存在且非强制时跳过
        if not force:
            existing = self._get_existing_summary(report_date)
            if existing and existing["status"] == "completed":
                logger.info(f"The Close 摘要已存在, 跳过: {report_date}")
                return {
                    "success": True,
                    "summary_id": existing["id"],
                    "message": "摘要已存在",
                    "skipped": True,
                }

        # 创建/更新摘要记录
        summary_id = self._upsert_summary_status(report_date, "generating")

        try:
            # 查询 The Close 视频
            videos = self._fetch_the_close_videos(window_start, resolved_end)
            logger.info(f"找到 {len(videos)} 个 The Close 视频")

            if not videos:
                self._update_summary(
                    summary_id,
                    status="completed",
                    summary_content="当日无 The Close 节目（可能为非交易日或节目尚未上传）。",
                    metadata={"reason": "no_video_found"},
                )
                elapsed = round(time.time() - start_time, 2)
                return {
                    "success": True,
                    "summary_id": summary_id,
                    "message": "当日无 The Close 节目",
                    "video_count": 0,
                    "skipped": False,
                }

            # 取最新视频（同日可能有多段，取 published_at 最晚的）
            video = videos[-1]
            transcript, word_count = self._prepare_transcript(video)

            if not transcript:
                self._update_summary(
                    summary_id,
                    status="completed",
                    video_id=video.get("content_id"),
                    video_title=video.get("title"),
                    video_url=video.get("url"),
                    summary_content="当日 The Close 节目无可用转录文本。",
                    metadata={"reason": "no_transcript", "video_id": video.get("content_id")},
                )
                return {
                    "success": True,
                    "summary_id": summary_id,
                    "message": "无可用转录",
                    "video_count": len(videos),
                    "skipped": False,
                }

            # LLM 生成摘要
            video_title = video.get("title") or "The Close"
            summary_text = await self._call_llm(transcript, video_title, report_date)

            elapsed = round(time.time() - start_time, 2)
            metadata = {
                "generation_time_seconds": elapsed,
                "transcript_word_count": word_count,
                "transcript_char_count": len(transcript),
                "video_id": video.get("content_id"),
                "video_count": len(videos),
                "template_code": DEFAULT_TEMPLATE,
                "window_start": ensure_utc(window_start).isoformat(),
                "window_end": ensure_utc(resolved_end).isoformat(),
            }

            self._update_summary(
                summary_id,
                status="completed",
                video_id=video.get("content_id"),
                video_title=video_title,
                video_url=video.get("url"),
                transcript_word_count=word_count,
                summary_content=summary_text,
                metadata=metadata,
            )

            logger.info(
                f"The Close 摘要完成: {len(videos)} 个视频, "
                f"transcript {word_count} words, "
                f"summary {len(summary_text)} chars, "
                f"耗时 {elapsed}s"
            )

            return {
                "success": True,
                "summary_id": summary_id,
                "message": f"摘要生成成功（{word_count} words transcript, {elapsed}s）",
                "video_count": len(videos),
                "transcript_word_count": word_count,
                "generation_time_seconds": elapsed,
                "skipped": False,
            }

        except Exception as e:
            error_msg = f"The Close 摘要生成失败: {str(e)}"
            logger.error(error_msg, exception=str(e))
            self._update_summary(summary_id, status="failed", error_message=error_msg)
            return {
                "success": False,
                "summary_id": summary_id,
                "message": error_msg,
                "skipped": False,
            }

    # ── Data fetching ──────────────────────────────────────────────────────

    def _fetch_the_close_videos(
        self, window_start: datetime, window_end: datetime
    ) -> List[Dict[str, Any]]:
        """查询 subscription_content 中 Bloomberg The Close 的视频。

        匹配策略（限定 Bloomberg Television 频道）：
        1. metadata.show_name = 'Bloomberg The Close'（入库时由 YouTubeAdapter 解析）
        2. title ILIKE '%The Close%'（兼容 show_name 缺失的历史/未回填记录）
        """
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT id, content_id, title, url, published_at,
                       transcript, transcript_cleaned, metadata
                FROM subscription_content
                WHERE channel_type = 'youtube'
                  AND author_identifier = %s
                  AND published_at >= %s AND published_at < %s
                  AND (
                    metadata->>'show_name' = %s
                    OR title ILIKE %s
                  )
                ORDER BY published_at ASC
                """,
                (
                    BLOOMBERG_TV_CHANNEL_ID,
                    window_start,
                    window_end,
                    THE_CLOSE_SHOW_NAME,
                    THE_CLOSE_TITLE_PATTERN,
                ),
            )
            columns = [
                "id", "content_id", "title", "url", "published_at",
                "transcript", "transcript_cleaned", "metadata",
            ]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
        finally:
            cursor.close()

    # ── Transcript preparation ─────────────────────────────────────────────

    def _prepare_transcript(self, video: Dict[str, Any]) -> tuple:
        """
        从视频记录中准备转录文本。

        Returns:
            (transcript_text, word_count) — 文本可能为空字符串
        """
        # 优先 transcript_cleaned，fallback transcript
        cleaned = (video.get("transcript_cleaned") or "").strip()
        raw = (video.get("transcript") or "").strip()
        transcript = cleaned or raw

        if not transcript:
            return "", 0

        # 估算英文字数（按空格分词）
        word_count = len(transcript.split())

        # 超长截断
        if len(transcript) > MAX_TRANSCRIPT_CHARS:
            logger.warning(
                f"转录过长 ({len(transcript)} chars), "
                f"截断至 {MAX_TRANSCRIPT_CHARS} chars"
            )
            transcript = transcript[:MAX_TRANSCRIPT_CHARS] + "\n\n...(转录过长，已截断)"

        return transcript, word_count

    # ── LLM call ──────────────────────────────────────────────────────────

    async def _call_llm(
        self, transcript: str, video_title: str, report_date: date
    ) -> str:
        """通过 DB 模板调用 LLM 生成 PM Brief。"""
        from backend.core.llm_framework import llm_service

        variables = {
            "report_date": str(report_date),
            "video_title": video_title,
            "transcript": transcript,
        }

        var_preview = {
            k: (f"{str(v)[:200]}..." if len(str(v)) > 200 else str(v))
            for k, v in variables.items()
        }
        logger.info(
            f"[LLM 调用] 模板={DEFAULT_TEMPLATE} (max_tokens 由模板配置), "
            f"变量预览={var_preview}"
        )

        llm_start = time.time()
        response = await llm_service.chat_completion_with_template(
            template_code=DEFAULT_TEMPLATE,
            variables=variables,
        )
        llm_elapsed = round(time.time() - llm_start, 2)

        content_preview = response.content[:300] if response.content else "(empty)"
        logger.info(
            f"[LLM 响应] 模板={DEFAULT_TEMPLATE}, 耗时={llm_elapsed}s, "
            f"响应长度={len(response.content)}字符, 预览: {content_preview}"
        )
        return response.content

    # ── Summary persistence ────────────────────────────────────────────────

    def _get_existing_summary(self, report_date: date) -> Optional[Dict]:
        """查询指定日期的已有摘要。"""
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT id, status, summary_content, video_id, updated_at
                FROM the_close_summaries
                WHERE report_date = %s
                """,
                (report_date,),
            )
            row = cursor.fetchone()
            if row:
                return {
                    "id": row[0],
                    "status": row[1],
                    "summary_content": row[2],
                    "video_id": row[3],
                    "updated_at": row[4],
                }
            return None
        finally:
            cursor.close()

    def _upsert_summary_status(self, report_date: date, status: str) -> int:
        """创建/更新摘要记录状态，返回 summary id。"""
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                INSERT INTO the_close_summaries (report_date, status)
                VALUES (%s, %s)
                ON CONFLICT (report_date) DO UPDATE
                    SET status = EXCLUDED.status,
                        error_message = NULL,
                        updated_at = CURRENT_TIMESTAMP
                RETURNING id
                """,
                (report_date, status),
            )
            conn.commit()
            return cursor.fetchone()[0]
        finally:
            cursor.close()

    def _update_summary(
        self,
        summary_id: int,
        *,
        status: str,
        video_id: Optional[str] = None,
        video_title: Optional[str] = None,
        video_url: Optional[str] = None,
        transcript_word_count: Optional[int] = None,
        summary_content: Optional[str] = None,
        metadata: Optional[Dict] = None,
        error_message: Optional[str] = None,
    ):
        """更新摘要记录。"""
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            from psycopg2.extras import Json

            cursor.execute(
                """
                UPDATE the_close_summaries
                SET status = %s,
                    video_id = COALESCE(%s, video_id),
                    video_title = COALESCE(%s, video_title),
                    video_url = COALESCE(%s, video_url),
                    transcript_word_count = COALESCE(%s, transcript_word_count),
                    summary_content = COALESCE(%s, summary_content),
                    metadata = COALESCE(%s, metadata),
                    error_message = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
                """,
                (
                    status,
                    video_id,
                    video_title,
                    video_url,
                    transcript_word_count,
                    summary_content,
                    Json(metadata) if metadata is not None else None,
                    error_message,
                    summary_id,
                ),
            )
            conn.commit()
        finally:
            cursor.close()

    # ── Query helpers (供 API 层调用) ──────────────────────────────────────

    def get_latest_summary(self) -> Optional[Dict[str, Any]]:
        """返回最新的已完成摘要。"""
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT id, report_date, video_id, video_title, video_url,
                       transcript_word_count, status, summary_content,
                       metadata, error_message, created_at, updated_at
                FROM the_close_summaries
                WHERE status = 'completed'
                ORDER BY report_date DESC
                LIMIT 1
                """
            )
            row = cursor.fetchone()
            return self._row_to_dict(row) if row else None
        finally:
            conn.close()

    def get_summary_by_date(self, report_date: date) -> Optional[Dict[str, Any]]:
        """按日期获取摘要。"""
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT id, report_date, video_id, video_title, video_url,
                       transcript_word_count, status, summary_content,
                       metadata, error_message, created_at, updated_at
                FROM the_close_summaries
                WHERE report_date = %s
                """,
                (report_date,),
            )
            row = cursor.fetchone()
            return self._row_to_dict(row) if row else None
        finally:
            conn.close()

    def list_summaries(self, limit: int = 20) -> List[Dict[str, Any]]:
        """列出最近的摘要记录。"""
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT id, report_date, video_id, video_title, video_url,
                       transcript_word_count, status, summary_content,
                       metadata, error_message, created_at, updated_at
                FROM the_close_summaries
                ORDER BY report_date DESC
                LIMIT %s
                """,
                (limit,),
            )
            return [self._row_to_dict(row) for row in cursor.fetchall()]
        finally:
            conn.close()

    @staticmethod
    def _row_to_dict(row: tuple) -> Dict[str, Any]:
        """将数据库行转换为字典。"""
        return {
            "id": row[0],
            "report_date": str(row[1]),
            "video_id": row[2],
            "video_title": row[3],
            "video_url": row[4],
            "transcript_word_count": row[5],
            "status": row[6],
            "summary_content": row[7],
            "metadata": row[8] or {},
            "error_message": row[9],
            "created_at": row[10].isoformat() if row[10] else None,
            "updated_at": row[11].isoformat() if row[11] else None,
        }
