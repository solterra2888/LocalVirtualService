# -*- coding: utf-8 -*-
"""
Bloomberg Surveillance Summary 服务 — Bloomberg "Surveillance" 每日 AM Brief 摘要

独立管线，从 subscription_content 中筛选 Bloomberg Television 频道的
"Surveillance" 节目转录，通过 LLM 生成八段式开盘前 AM Brief 摘要。

与 TheCloseSummaryService / CNBCClosingBellService 的区别：
- 无用户维度（全局报告）
- 早间节目（美股开盘前播出），摘要为开盘前 AM Brief（非收盘复盘）
- 节目约 2 小时，转录较长（MAX_TRANSCRIPT_CHARS 放宽至 120,000）
- 同日可能有多段上传，按 published_at 升序合并转录（同 Closing Bell）
"""

import asyncio
import time
from datetime import date, datetime
from typing import Any, Dict, List, Optional

from backend.database.db_manager import DatabaseManager
from backend.utils.database_config import get_database_config
from backend.utils.datetime_utils import ensure_utc, resolve_report_content_window
from backend.utils.logger import get_logger
from backend.services.summary_common import SINGLE_LLM_TIMEOUT_SEC, SINGLE_LLM_RETRY

logger = get_logger("bloomberg_surveillance_service")

# Bloomberg Television YouTube channel_id
BLOOMBERG_TV_CHANNEL_ID = "UCIALMKvObZNtJ6AmdCLP7Lg"
# 经 youtube_adapter standalone_shows 提取的 show_name（_TRANSCRIPTA_CANONICAL 无改名映射）
SURVEILLANCE_SHOW_NAME = "Bloomberg Surveillance"
# LLM 模板（max_tokens / provider / model 由 llm_prompt_templates 配置，经 ConfigResolver 解析）
DEFAULT_TEMPLATE = "bloomberg_surveillance_am_brief"
# 转录文本截断阈值（字符数）：早间节目约 2h，~25,000-30,000 英文单词
MAX_TRANSCRIPT_CHARS = 120000
# 标题/show_name 匹配模式（Bloomberg TV 频道内，无其他节目名包含该串）
SURVEILLANCE_TITLE_PATTERN = "%Bloomberg Surveillance%"


# 占位摘要原因（无节目 / 无转录）——不作为幂等终态，允许后续重生成
PLACEHOLDER_REASONS = {"no_video_found", "no_transcript"}


def _is_placeholder_summary(existing: Dict[str, Any]) -> bool:
    """判断已有 completed 记录是否为占位摘要（无内容），占位则允许重生成。"""
    meta = existing.get("metadata") or {}
    if isinstance(meta, dict):
        return meta.get("reason") in PLACEHOLDER_REASONS
    return False


def is_bloomberg_surveillance_video(
    title: Optional[str],
    show_name: Optional[str] = None,
) -> bool:
    """判断是否为 Bloomberg Television Surveillance 节目视频。

    Surveillance 标题格式稳定（"Bloomberg Surveillance - 09/16/26" /
    前缀 standalone 格式），无需 The Close 那样的口播兜底。
    """
    if show_name == SURVEILLANCE_SHOW_NAME:
        return True
    return bool(title) and "bloomberg surveillance" in title.lower()


class BloombergSurveillanceService:
    """Bloomberg Surveillance 每日 AM Brief 摘要生成服务"""

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
        为指定日期生成 Surveillance AM Brief 摘要。

        Args:
            report_date: 报告日期（北京自然日，与 DMR 口径一致）
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
            f"开始生成 Surveillance 摘要: date={report_date}, "
            f"window=[{window_start.isoformat()}, {resolved_end.isoformat()})"
        )

        # 幂等检查：已存在且非强制时跳过。
        # 占位摘要（no_video_found / no_transcript）不视为终态，
        # 允许后续调度在内容就绪后重新生成，避免被空占位永久阻断。
        if not force:
            existing = self._get_existing_summary(report_date)
            if (
                existing
                and existing["status"] == "completed"
                and not _is_placeholder_summary(existing)
            ):
                logger.info(f"Surveillance 摘要已存在, 跳过: {report_date}")
                return {
                    "success": True,
                    "summary_id": existing["id"],
                    "message": "摘要已存在",
                    "skipped": True,
                }

        # 先筛选视频，避免空窗期把 generating 占住后立刻写成空占位。
        videos = self._fetch_surveillance_videos(window_start, resolved_end)
        logger.info(f"找到 {len(videos)} 个 Surveillance 视频")

        summary_id = self._upsert_summary_status(report_date, "generating")

        try:
            if not videos:
                self._update_summary(
                    summary_id,
                    status="completed",
                    summary_content="当日无 Surveillance 节目（可能为非交易日或节目尚未上传）。",
                    metadata={"reason": "no_video_found"},
                )
                return {
                    "success": True,
                    "summary_id": summary_id,
                    "message": "当日无 Surveillance 节目",
                    "video_count": 0,
                    "skipped": False,
                }

            # 同日可能有多段上传（完整版 + 分段），按 published_at 合并转录
            transcript, word_count, used_videos, transcript_truncated = (
                self._prepare_merged_transcript(videos)
            )
            primary = used_videos[-1] if used_videos else videos[-1]

            if not transcript:
                self._update_summary(
                    summary_id,
                    status="completed",
                    video_id=primary.get("content_id"),
                    video_title=primary.get("title"),
                    video_url=primary.get("url"),
                    summary_content="当日 Surveillance 节目无可用转录文本。",
                    metadata={
                        "reason": "no_transcript",
                        "video_id": primary.get("content_id"),
                        "video_count": len(videos),
                    },
                )
                return {
                    "success": True,
                    "summary_id": summary_id,
                    "message": "无可用转录",
                    "video_count": len(videos),
                    "skipped": False,
                }

            # LLM 生成摘要（带超时与重试）
            video_title = self._format_video_title_for_llm(used_videos)
            summary_text = await self._call_llm_with_retry(transcript, video_title, report_date)

            elapsed = round(time.time() - start_time, 2)
            video_refs = [
                {
                    "content_id": v.get("content_id"),
                    "title": v.get("title"),
                    "url": v.get("url"),
                    "published_at": (
                        ensure_utc(v["published_at"]).isoformat()
                        if v.get("published_at")
                        else None
                    ),
                }
                for v in used_videos
            ]
            metadata = {
                "generation_time_seconds": elapsed,
                "transcript_word_count": word_count,
                "transcript_char_count": len(transcript),
                "transcript_truncated": transcript_truncated,
                "video_id": primary.get("content_id"),
                "video_ids": [v.get("content_id") for v in used_videos],
                "videos": video_refs,
                "video_count": len(videos),
                "transcript_segment_count": len(used_videos),
                "template_code": DEFAULT_TEMPLATE,
                "window_start": ensure_utc(window_start).isoformat(),
                "window_end": ensure_utc(resolved_end).isoformat(),
            }

            self._update_summary(
                summary_id,
                status="completed",
                video_id=primary.get("content_id"),
                video_title=video_title,
                video_url=primary.get("url"),
                transcript_word_count=word_count,
                summary_content=summary_text,
                metadata=metadata,
            )

            logger.info(
                f"Surveillance 摘要完成: {len(videos)} 个视频, "
                f"{len(used_videos)} 段转录, "
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
            error_msg = f"Surveillance 摘要生成失败: {str(e)}"
            logger.error(error_msg, exception=str(e))
            self._update_summary(summary_id, status="failed", error_message=error_msg)
            return {
                "success": False,
                "summary_id": summary_id,
                "message": error_msg,
                "skipped": False,
            }

    # ── Data fetching ──────────────────────────────────────────────────────

    def _fetch_surveillance_videos(
        self, window_start: datetime, window_end: datetime
    ) -> List[Dict[str, Any]]:
        """查询 subscription_content 中 Bloomberg Surveillance 的视频。

        匹配策略（限定 Bloomberg Television 频道）：
        1. metadata.show_name = 'Bloomberg Surveillance'
        2. 标题含 "Bloomberg Surveillance"（standalone 前缀格式，标题稳定）
        The Close 匹配已显式排除 surveillance 标记，两边互不误伤。
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
                    SURVEILLANCE_SHOW_NAME,
                    SURVEILLANCE_TITLE_PATTERN,
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

    @staticmethod
    def _extract_video_transcript(video: Dict[str, Any]) -> str:
        """优先 transcript_cleaned，fallback transcript。"""
        cleaned = (video.get("transcript_cleaned") or "").strip()
        raw = (video.get("transcript") or "").strip()
        return cleaned or raw

    def _prepare_merged_transcript(
        self, videos: List[Dict[str, Any]]
    ) -> tuple:
        """
        合并同日多段 Surveillance 转录（按 published_at 升序）。

        Returns:
            (merged_transcript, total_word_count, videos_with_transcript, truncated)
        """
        segments: List[str] = []
        used_videos: List[Dict[str, Any]] = []
        total_words = 0

        videos_with_text = [
            v for v in videos if self._extract_video_transcript(v)
        ]
        segment_total = len(videos_with_text)

        for idx, video in enumerate(videos_with_text, start=1):
            text = self._extract_video_transcript(video)
            title = (video.get("title") or "Bloomberg Surveillance").strip()
            published = video.get("published_at")
            published_label = (
                ensure_utc(published).strftime("%Y-%m-%d %H:%M UTC")
                if published
                else "unknown time"
            )
            header = (
                f"=== Segment {idx}/{segment_total}: {title} "
                f"({published_label}) ==="
            )
            segments.append(f"{header}\n\n{text}")
            used_videos.append(video)
            total_words += len(text.split())

        if not segments:
            return "", 0, [], False

        merged = "\n\n---\n\n".join(segments)

        truncated = False
        if len(merged) > MAX_TRANSCRIPT_CHARS:
            logger.warning(
                f"合并转录过长 ({len(merged)} chars), "
                f"截断至 {MAX_TRANSCRIPT_CHARS} chars "
                f"({segment_total} segments)"
            )
            merged = merged[:MAX_TRANSCRIPT_CHARS] + "\n\n...(转录过长，已截断)"
            truncated = True

        return merged, total_words, used_videos, truncated

    @staticmethod
    def _format_video_title_for_llm(videos: List[Dict[str, Any]]) -> str:
        """为多段转录生成 LLM 可见的节目标题。"""
        if not videos:
            return "Bloomberg Surveillance"
        if len(videos) == 1:
            return videos[0].get("title") or "Bloomberg Surveillance"
        titles = [v.get("title") or "Bloomberg Surveillance" for v in videos]
        return f"Bloomberg Surveillance — {len(videos)} segments:\n" + "\n".join(
            f"{i}. {t}" for i, t in enumerate(titles, start=1)
        )

    # ── LLM call ──────────────────────────────────────────────────────────

    async def _call_llm_with_retry(
        self, transcript: str, video_title: str, report_date: date
    ) -> str:
        """带超时与重试的 LLM 调用入口。"""
        last_exc: BaseException | None = None
        for attempt in range(SINGLE_LLM_RETRY + 1):
            try:
                return await asyncio.wait_for(
                    self._call_llm(transcript, video_title, report_date),
                    timeout=SINGLE_LLM_TIMEOUT_SEC,
                )
            except asyncio.TimeoutError as exc:
                last_exc = exc
                if attempt < SINGLE_LLM_RETRY:
                    logger.warning(
                        f"[Surveillance] LLM 超时 (attempt {attempt+1}/{SINGLE_LLM_RETRY+1}), retrying"
                    )
            except Exception as exc:
                last_exc = exc
                if attempt < SINGLE_LLM_RETRY:
                    logger.warning(
                        f"[Surveillance] LLM 错误 (attempt {attempt+1}): {exc}, retrying"
                    )
        raise last_exc  # type: ignore[misc]

    async def _call_llm(
        self, transcript: str, video_title: str, report_date: date
    ) -> str:
        """通过 DB 模板调用 LLM 生成 AM Brief。"""
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
            timeout=SINGLE_LLM_TIMEOUT_SEC,
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
                SELECT id, status, summary_content, video_id, updated_at, metadata
                FROM bloomberg_surveillance_summaries
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
                    "metadata": row[5],
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
                INSERT INTO bloomberg_surveillance_summaries (report_date, status)
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
                UPDATE bloomberg_surveillance_summaries
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
                FROM bloomberg_surveillance_summaries
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
                FROM bloomberg_surveillance_summaries
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
                FROM bloomberg_surveillance_summaries
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
