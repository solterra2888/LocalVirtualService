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

import asyncio
import re
import time
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

from backend.database.db_manager import DatabaseManager
from backend.utils.database_config import get_database_config
from backend.utils.datetime_utils import ensure_utc, resolve_report_content_window
from backend.utils.logger import get_logger
from backend.services.summary_common import SINGLE_LLM_TIMEOUT_SEC, SINGLE_LLM_RETRY

logger = get_logger("the_close_summary_service")

# Bloomberg Television YouTube channel_id
BLOOMBERG_TV_CHANNEL_ID = "UCIALMKvObZNtJ6AmdCLP7Lg"
# 经 _TRANSCRIPTA_CANONICAL 标准化后的 show_name
THE_CLOSE_SHOW_NAME = "Bloomberg The Close"
# LLM 模板（max_tokens / provider / model 由 llm_prompt_templates 配置，经 ConfigResolver 解析）
DEFAULT_TEMPLATE = "the_close_pm_brief"
# 转录文本截断阈值（字符数，约 ~20,000 英文单词的 4 倍估算）
MAX_TRANSCRIPT_CHARS = 80000
# 标题匹配：Bloomberg TV 频道内 "| The Close" / 独立 "The Close M/D/..."。
# 2026-09 起正片常不再带该后缀，需靠转录开场口播兜底（见 _has_the_close_bumper）。
THE_CLOSE_TITLE_PATTERN = "%The Close%"
# 只扫转录开场（含冷开场）。正片口播有时在广告之后约 8–10KB。
BUMPER_SCAN_CHARS = 16000
# 经验入库点（北京）。此前不把「尚未上传」写成终态空节目。
TYPICAL_PUBLISH_HOUR_BJ = 22
DEFAULT_REPORT_TIMEZONE = "Asia/Shanghai"

# 开场口播：THIS IS "THE CLOSE." / Welcome to (a special edition of) The Close
# 不用裸匹配 "the close"，以免 into/after the close、the closest 误伤。
_BUMPER_RE = re.compile(
    r"(?:this\s+is\s+[\"'“”]?the\s+close(?![a-z])"
    r"|welcome\s+to\s+(?:a\s+special\s+edition\s+of\s+)?"
    r"(?:bloomberg(?:['’]s)?\s+)?[\"'“”]?the\s+close(?![a-z]))",
    re.IGNORECASE,
)
_TITLE_SHOW_RE = re.compile(
    r"(?:\|\s*The Close\b|^\s*The Close\b)",
    re.IGNORECASE,
)
# 标题已标明其他 Bloomberg 栏目时，即使转录里提到 The Close 也不选用
_OTHER_SHOW_MARKERS = (
    "bloomberg surveillance",
    "bloomberg businessweek",
    "bloomberg money",
    "bloomberg deals",
    "bloomberg this weekend",
    "fed special",
    "| etf iq",
    "| the pulse",
    "| the opening trade",
    "| real yield",
    "| balance of power",
    "| daybreak europe",
    "| the asia trade",
    "| the china show",
)


# 占位摘要原因（无节目 / 无转录 / 尚未到点）——不作为幂等终态，允许后续重生成
PLACEHOLDER_REASONS = {"no_video_found", "no_transcript", "episode_not_yet_published"}


def _is_placeholder_summary(existing: Dict[str, Any]) -> bool:
    """判断已有 completed 记录是否为占位摘要（无内容），占位则允许重生成。"""
    meta = existing.get("metadata") or {}
    if isinstance(meta, dict):
        return meta.get("reason") in PLACEHOLDER_REASONS
    return False


def _title_looks_like_the_close(title: Optional[str]) -> bool:
    """标题带节目后缀或独立节目名（不含 'Too Close' 这类词）。"""
    if not title:
        return False
    return bool(_TITLE_SHOW_RE.search(title))


def _title_is_other_show(title: Optional[str]) -> bool:
    """标题已是其他 Bloomberg 栏目。"""
    if not title or _title_looks_like_the_close(title):
        return False
    lowered = title.lower()
    return any(marker in lowered for marker in _OTHER_SHOW_MARKERS)


def _has_the_close_bumper(transcript: Optional[str]) -> bool:
    """转录开场是否出现 The Close 口播。"""
    if not transcript:
        return False
    head = transcript[:BUMPER_SCAN_CHARS]
    return bool(_BUMPER_RE.search(head))


def _bumper_index(transcript: Optional[str]) -> int:
    """口播在开场扫描区内的位置；未命中返回 -1。"""
    if not transcript:
        return -1
    match = _BUMPER_RE.search(transcript[:BUMPER_SCAN_CHARS])
    return match.start() if match else -1


def should_defer_no_video_placeholder(
    report_date: date,
    now: Optional[datetime] = None,
    typical_hour_bj: int = TYPICAL_PUBLISH_HOUR_BJ,
) -> bool:
    """交易日、且仍早于经验入库点时，用 episode_not_yet_published 而不是 no_video_found。"""
    from backend.services.summary_common import (
        should_defer_no_video_placeholder as _defer,
    )

    return _defer(
        report_date,
        now=now,
        typical_hour_bj=typical_hour_bj,
        timezone_name=DEFAULT_REPORT_TIMEZONE,
    )


def is_bloomberg_the_close_video(
    title: Optional[str],
    show_name: Optional[str] = None,
    transcript: Optional[str] = None,
) -> bool:
    """判断是否为 Bloomberg Television The Close 节目视频。"""
    if show_name == THE_CLOSE_SHOW_NAME:
        return True
    if _title_looks_like_the_close(title):
        return True
    if _title_is_other_show(title):
        return False
    return _has_the_close_bumper(transcript)


def _select_primary_the_close_video(
    videos: List[Dict[str, Any]],
) -> Tuple[Optional[Dict[str, Any]], List[Dict[str, Any]]]:
    """从候选中选出正片：排除其他栏目，优先较晚发布、转录更长。"""
    ranked: List[Tuple[datetime, int, int, Dict[str, Any]]] = []
    for video in videos:
        title = video.get("title")
        metadata = video.get("metadata") or {}
        if isinstance(metadata, str):
            metadata = {}
        show_name = metadata.get("show_name") if isinstance(metadata, dict) else None
        transcript = (video.get("transcript_cleaned") or video.get("transcript") or "")
        if not is_bloomberg_the_close_video(title, show_name=show_name, transcript=transcript):
            continue
        published_at = video.get("published_at") or datetime.min
        bumper_pos = _bumper_index(transcript)
        ranked.append((published_at, len(transcript), -bumper_pos, video))

    if not ranked:
        return None, []
    ranked.sort(key=lambda item: (item[0], item[1], item[2]), reverse=True)
    selected = [item[3] for item in ranked]
    return selected[0], selected


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
                logger.info(f"The Close 摘要已存在, 跳过: {report_date}")
                return {
                    "success": True,
                    "summary_id": existing["id"],
                    "message": "摘要已存在",
                    "skipped": True,
                }

        # 先筛选视频，避免空窗期把 generating 占住后立刻写成空占位。
        candidates = self._fetch_the_close_videos(window_start, resolved_end)
        video, videos = _select_primary_the_close_video(candidates)
        logger.info(
            f"找到 {len(videos)} 个 The Close 视频 "
            f"(candidates={len(candidates)}, "
            f"primary={video.get('content_id') if video else None})"
        )

        summary_id = self._upsert_summary_status(report_date, "generating")

        try:
            if not video:
                reason = (
                    "episode_not_yet_published"
                    if should_defer_no_video_placeholder(report_date)
                    else "no_video_found"
                )
                message = (
                    "The Close 尚未入库（通常北京时间 22:00 前后上传）"
                    if reason == "episode_not_yet_published"
                    else "当日无 The Close 节目"
                )
                self._update_summary(
                    summary_id,
                    status="completed",
                    summary_content=message + "。",
                    metadata={"reason": reason},
                )
                return {
                    "success": True,
                    "summary_id": summary_id,
                    "message": message,
                    "video_count": 0,
                    "skipped": True,
                    "reason": reason,
                }

            match_via = self._match_via(video)
            self._persist_show_name(video)
            transcript, word_count, transcript_truncated = self._prepare_transcript(video)

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
                    "skipped": True,
                    "reason": "no_transcript",
                }

            # LLM 生成摘要（带超时与重试）
            video_title = video.get("title") or "The Close"
            summary_text = await self._call_llm_with_retry(transcript, video_title, report_date)

            elapsed = round(time.time() - start_time, 2)
            metadata = {
                "generation_time_seconds": elapsed,
                "transcript_word_count": word_count,
                "transcript_char_count": len(transcript),
                "transcript_truncated": transcript_truncated,
                "video_id": video.get("content_id"),
                "video_count": len(videos),
                "match_via": match_via,
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
        1. metadata.show_name = 'Bloomberg The Close'
        2. 标题含 "| The Close" / 独立 "The Close …"
        3. 转录开场口播 THIS IS "THE CLOSE" / Welcome to The Close
           （2026-09 起 YouTube 标题不再带节目后缀）
        SQL 只拉候选，最终以 is_bloomberg_the_close_video 过滤误伤。
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
                    OR LEFT(COALESCE(transcript_cleaned, transcript, ''), %s)
                       ~* 'this is[[:space:]]+.{0,3}the close'
                    OR LEFT(COALESCE(transcript_cleaned, transcript, ''), %s)
                       ~* 'welcome to[[:space:]].{0,40}the close'
                  )
                ORDER BY published_at ASC
                """,
                (
                    BLOOMBERG_TV_CHANNEL_ID,
                    window_start,
                    window_end,
                    THE_CLOSE_SHOW_NAME,
                    THE_CLOSE_TITLE_PATTERN,
                    BUMPER_SCAN_CHARS,
                    BUMPER_SCAN_CHARS,
                ),
            )
            columns = [
                "id", "content_id", "title", "url", "published_at",
                "transcript", "transcript_cleaned", "metadata",
            ]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
        finally:
            cursor.close()

    @staticmethod
    def _match_via(video: Dict[str, Any]) -> str:
        metadata = video.get("metadata") or {}
        show_name = metadata.get("show_name") if isinstance(metadata, dict) else None
        if show_name == THE_CLOSE_SHOW_NAME:
            return "show_name"
        if _title_looks_like_the_close(video.get("title")):
            return "title"
        return "transcript_bumper"

    def _persist_show_name(self, video: Dict[str, Any]) -> None:
        """口播命中后回写 show_name，避免下次 RSS 刷新丢掉识别结果。"""
        content_id = video.get("content_id")
        if not content_id:
            return
        metadata = video.get("metadata") or {}
        if isinstance(metadata, dict) and metadata.get("show_name") == THE_CLOSE_SHOW_NAME:
            return
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                UPDATE subscription_content
                SET metadata = jsonb_set(
                    COALESCE(metadata, '{}'::jsonb),
                    '{show_name}',
                    to_jsonb(%s::text)
                )
                WHERE channel_type = 'youtube'
                  AND content_id = %s
                  AND COALESCE(metadata->>'show_name', '') = ''
                """,
                (THE_CLOSE_SHOW_NAME, content_id),
            )
            conn.commit()
            if cursor.rowcount:
                logger.info(
                    f"已回写 show_name={THE_CLOSE_SHOW_NAME} content_id={content_id}"
                )
                if isinstance(metadata, dict):
                    metadata["show_name"] = THE_CLOSE_SHOW_NAME
                    video["metadata"] = metadata
        except Exception as exc:
            conn.rollback()
            logger.warning(f"回写 show_name 失败: {exc}")
        finally:
            cursor.close()
            conn.close()

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
            return "", 0, False

        # 估算英文字数（按空格分词）
        word_count = len(transcript.split())

        # 超长截断
        truncated = False
        if len(transcript) > MAX_TRANSCRIPT_CHARS:
            logger.warning(
                f"转录过长 ({len(transcript)} chars), "
                f"截断至 {MAX_TRANSCRIPT_CHARS} chars"
            )
            transcript = transcript[:MAX_TRANSCRIPT_CHARS] + "\n\n...(转录过长，已截断)"
            truncated = True

        return transcript, word_count, truncated

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
                        f"[The Close] LLM 超时 (attempt {attempt+1}/{SINGLE_LLM_RETRY+1}), retrying"
                    )
            except Exception as exc:
                last_exc = exc
                if attempt < SINGLE_LLM_RETRY:
                    logger.warning(
                        f"[The Close] LLM 错误 (attempt {attempt+1}): {exc}, retrying"
                    )
        raise last_exc  # type: ignore[misc]

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
