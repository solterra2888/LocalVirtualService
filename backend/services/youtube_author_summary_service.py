# -*- coding: utf-8 -*-
"""
YouTube Author Daily Summary 服务 — 全局频道摘要预生成

将 Phase 1 按 YouTube 频道摘要提升为全局共享：
同一频道 + 同一天只调用一次 LLM，结果存入 youtube_author_daily_summaries 表，
所有订阅该频道的用户在生成 Daily Report 时直接复用。
"""

import asyncio
import time
from datetime import date, datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple

from backend.database.db_manager import DatabaseManager
from backend.utils.database_config import get_database_config
from backend.utils.logger import get_logger
from backend.core.llm_framework import llm_service
from backend.utils.datetime_utils import ensure_utc, resolve_report_content_window

logger = get_logger("youtube_author_summary_service")

DEFAULT_PHASE1_TEMPLATE = "youtube_daily_phase1_default"
MAX_VIDEOS_PER_CHANNEL_CHUNK = 10
MAX_TOKENS_OUTPUT_PHASE1 = 3000

# 批量生成时的并发与超时控制
# 最多 N 个频道同时处理，防止因某一频道 LLM 超时阻塞整个批次
BATCH_CONCURRENT_CHANNELS = 4
# 单频道摘要的最长等待时间（秒），超出后直接标 failed 并继续下一个
PER_CHANNEL_TIMEOUT_SEC = 180


class YouTubeAuthorSummaryService:
    """全局 YouTube 频道每日摘要生成服务"""

    def __init__(self):
        db_params = get_database_config()
        self.db_manager = DatabaseManager(db_params)

    def _get_template_updated_at(self, template_code: str) -> Optional[datetime]:
        """获取模板的最近更新时间，用于检测模板变更后自动重新生成。"""
        try:
            from backend.services.llm_prompt_template_service import LLMPromptTemplateService
            svc = LLMPromptTemplateService()
            tpl = svc.get_template_by_code(template_code)
            if tpl and tpl.get('updated_at'):
                return datetime.fromisoformat(tpl['updated_at'])
        except Exception as e:
            logger.warning(f"获取模板更新时间失败: {template_code}, {e}")
        return None

    def _get_template_model_info(self, template_code: str) -> Dict[str, Optional[str]]:
        """获取模板当前配置的 provider/model 信息。"""
        try:
            from backend.services.llm_prompt_template_service import LLMPromptTemplateService
            svc = LLMPromptTemplateService()
            tpl = svc.get_template_by_code(template_code)
            if tpl:
                return {
                    'recommended_provider': tpl.get('recommended_provider'),
                    'recommended_model': tpl.get('recommended_model'),
                }
        except Exception:
            pass
        return {'recommended_provider': None, 'recommended_model': None}

    # ── Public API ─────────────────────────────────────────────────────────

    async def generate_all_author_summaries(
        self,
        summary_date: date,
        force: bool = False,
        window_end: Optional[datetime] = None,
        window_hours: int = 24,
    ) -> Dict[str, Any]:
        """
        为所有启用的 YouTube 订阅频道批量生成当日摘要。

        Returns:
            {'success': bool, 'message': str, 'results': [...]}
        """
        start_time = time.time()
        authors = self._get_distinct_enabled_authors()
        if not authors:
            return {'success': True, 'message': '没有启用的 YouTube 订阅频道', 'results': []}

        window_start, resolved_end = resolve_report_content_window(
            summary_date, window_end, window_hours
        )
        logger.info(
            f"开始批量生成频道摘要: {len(authors)} 个频道, date={summary_date}, "
            f"window=[{window_start.isoformat()}, {resolved_end.isoformat()})"
        )

        total = len(authors)
        semaphore = asyncio.Semaphore(BATCH_CONCURRENT_CHANNELS)

        async def _process_one(idx: int, author_id: str, author_name: Optional[str]):
            async with semaphore:
                author_start = time.time()
                try:
                    result = await asyncio.wait_for(
                        self.generate_author_summary(
                            author_id,
                            author_name,
                            summary_date,
                            force=force,
                            window_end=window_end,
                            window_hours=window_hours,
                        ),
                        timeout=PER_CHANNEL_TIMEOUT_SEC,
                    )
                except asyncio.TimeoutError:
                    author_elapsed = round(time.time() - author_start, 2)
                    logger.warning(
                        f"[{idx}/{total}] {author_name or author_id} — TIMEOUT "
                        f"after {author_elapsed}s，标记 failed 并继续"
                    )
                    try:
                        existing = self._get_existing_summary(author_id, summary_date)
                        if existing:
                            self._update_summary(
                                existing['id'],
                                status='failed',
                                error_message=f"单频道超时（>{PER_CHANNEL_TIMEOUT_SEC}s）",
                            )
                    except Exception:
                        pass
                    return idx, author_id, {
                        'success': False,
                        'message': f'per-channel timeout (>{PER_CHANNEL_TIMEOUT_SEC}s)',
                        'skipped': False,
                    }, author_elapsed

                author_elapsed = round(time.time() - author_start, 2)
                return idx, author_id, result, author_elapsed

        raw_results = await asyncio.gather(
            *[_process_one(idx, aid, aname) for idx, (aid, aname) in enumerate(authors, 1)]
        )

        results = []
        succeeded = 0
        skipped = 0
        for idx, author_id, result, author_elapsed in raw_results:
            results.append({'author': author_id, **result})
            if result.get('skipped'):
                skipped += 1
                logger.info(
                    f"[{idx}/{total}] {author_id} — skipped (already completed), {author_elapsed}s"
                )
            elif result.get('success'):
                succeeded += 1
                video_count = result.get('video_count', 0)
                logger.info(
                    f"[{idx}/{total}] {author_id} — OK, {video_count} videos, {author_elapsed}s"
                )
            else:
                logger.warning(
                    f"[{idx}/{total}] {author_id} — FAILED: {result.get('message', '')}, {author_elapsed}s"
                )

        elapsed = round(time.time() - start_time, 2)
        logger.info(f"批量频道摘要完成: {succeeded} 成功, {skipped} 跳过, "
                    f"{len(authors) - succeeded - skipped} 失败, 耗时 {elapsed}s")

        return {
            'success': True,
            'message': f'{len(authors)} 个频道: {succeeded} 成功, {skipped} 跳过, 耗时 {elapsed}s',
            'total': len(authors),
            'succeeded': succeeded,
            'skipped': skipped,
            'generation_time_seconds': elapsed,
            'results': results,
        }

    async def generate_author_summary(
        self,
        author_identifier: str,
        author_name: Optional[str],
        summary_date: date,
        force: bool = False,
        window_end: Optional[datetime] = None,
        window_hours: int = 24,
    ) -> Dict[str, Any]:
        """
        为单个 YouTube 频道生成指定日期的摘要。

        Returns:
            {'success': bool, 'summary_id': int | None, 'message': str, ...}
        """
        start_time = time.time()
        display_name = author_name or author_identifier
        window_start, resolved_end = resolve_report_content_window(
            summary_date, window_end, window_hours
        )
        logger.info(
            f"开始生成频道摘要: {display_name} ({author_identifier}), date={summary_date}, "
            f"window=[{window_start.isoformat()}, {resolved_end.isoformat()})"
        )

        if not force:
            existing = self._get_existing_summary(author_identifier, summary_date)
            if existing and existing['status'] == 'completed':
                tpl_updated = self._get_template_updated_at(DEFAULT_PHASE1_TEMPLATE)
                summary_updated = existing.get('updated_at')
                from datetime import timedelta
                if tpl_updated and summary_updated and tpl_updated > (summary_updated + timedelta(hours=1)):
                    logger.info(
                        f"模板已更新 (模板={tpl_updated}, 摘要={summary_updated}), "
                        f"自动重新生成: {display_name}"
                    )
                else:
                    logger.info(f"频道摘要已存在, 跳过: {display_name}")
                    return {
                        'success': True,
                        'summary_id': existing['id'],
                        'message': '摘要已存在',
                        'skipped': True,
                    }

        summary_id = self._upsert_summary_status(
            author_identifier, display_name, summary_date, 'generating'
        )

        try:
            videos = self._fetch_author_videos(
                author_identifier, window_start, resolved_end
            )
            logger.info(f"{display_name}: 拉取到 {len(videos)} 个视频")

            if not videos:
                self._update_summary(
                    summary_id,
                    status='completed',
                    content='当天无视频。',
                    video_count=0,
                    ref_map={},
                    top_videos=[],
                )
                return {
                    'success': True,
                    'summary_id': summary_id,
                    'message': '当天无视频',
                    'video_count': 0,
                    'skipped': False,
                }

            ref_map = self._prepare_author_refs(videos)
            summary_text = await self._run_phase1(
                videos, ref_map, author_identifier, display_name, summary_date
            )
            top_videos = self._get_author_top_videos(videos, ref_map, top_n=5)

            videos_with_transcript = sum(1 for v in videos if v.get('_has_transcript'))
            elapsed = round(time.time() - start_time, 2)
            tpl_model_info = self._get_template_model_info(DEFAULT_PHASE1_TEMPLATE)
            metadata = {
                'generation_time_seconds': elapsed,
                'chunk_count': max(1, (len(videos) - 1) // MAX_VIDEOS_PER_CHANNEL_CHUNK + 1),
                'videos_with_transcript': videos_with_transcript,
                'videos_without_transcript': len(videos) - videos_with_transcript,
                'template_code': DEFAULT_PHASE1_TEMPLATE,
                'recommended_provider': tpl_model_info.get('recommended_provider'),
                'recommended_model': tpl_model_info.get('recommended_model'),
                'window_start': ensure_utc(window_start).isoformat(),
                'window_end': ensure_utc(resolved_end).isoformat(),
            }

            self._update_summary(
                summary_id,
                status='completed',
                content=summary_text,
                video_count=len(videos),
                ref_map=ref_map,
                top_videos=top_videos,
                metadata=metadata,
            )

            logger.info(f"{display_name} 摘要完成: {len(videos)} 个视频 "
                        f"(transcript: {videos_with_transcript}), "
                        f"{len(summary_text)} 字符, 耗时 {elapsed}s")

            return {
                'success': True,
                'summary_id': summary_id,
                'message': f'摘要生成成功（{len(videos)} 个视频，{elapsed}s）',
                'video_count': len(videos),
                'generation_time_seconds': elapsed,
                'skipped': False,
            }

        except Exception as e:
            error_msg = f"频道摘要生成失败: {str(e)}"
            logger.error(error_msg, author=author_identifier, exception=str(e))
            self._update_summary(summary_id, status='failed', error_message=error_msg)
            return {'success': False, 'summary_id': summary_id, 'message': error_msg, 'skipped': False}

    # ── Data fetching ──────────────────────────────────────────────────────

    def _get_distinct_enabled_authors(self) -> List[Tuple[str, Optional[str]]]:
        """查询 subscription_list 中所有启用的 YouTube 频道（去重）。"""
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT DISTINCT channel_identifier, MAX(channel_name) as channel_name
                FROM subscription_list
                WHERE channel_type = 'youtube'
                  AND enabled = true
                GROUP BY channel_identifier
                ORDER BY channel_identifier
            """)
            return [(row[0], row[1]) for row in cursor.fetchall()]
        finally:
            cursor.close()

    def _fetch_author_videos(
        self,
        author_identifier: str,
        window_start: datetime,
        window_end: datetime,
    ) -> List[Dict[str, Any]]:
        """查询指定频道在时间窗口内的所有视频（不按用户过滤）。"""
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            start_dt = window_start
            end_dt = window_end
            cursor.execute("""
                SELECT id, content_id, title, content, author_name, author_identifier,
                       url, media_urls, published_at, engagement_count, view_count,
                       metadata, transcript, duration_seconds, heat_level
                FROM subscription_content
                WHERE channel_type = 'youtube'
                  AND author_identifier = %s
                  AND published_at >= %s AND published_at < %s
                ORDER BY published_at ASC
            """, (author_identifier, start_dt, end_dt))
            columns = [
                'id', 'content_id', 'title', 'content', 'author_name',
                'author_identifier', 'url', 'media_urls', 'published_at',
                'engagement_count', 'view_count', 'metadata',
                'transcript', 'duration_seconds', 'heat_level',
            ]
            videos = [dict(zip(columns, row)) for row in cursor.fetchall()]
            for v in videos:
                self._prepare_video_content(v)
            return videos
        finally:
            cursor.close()

    # ── Video content preparation ──────────────────────────────────────────

    def _prepare_video_content(self, video: Dict[str, Any]) -> None:
        """
        为单个视频准备 LLM 输入内容。
        优先使用 transcript；不可用时 fallback 到 title + description。
        在 video dict 上就地增加 _llm_content 和 _has_transcript 字段。
        """
        transcript = (video.get('transcript') or '').strip()
        if transcript:
            video['_has_transcript'] = True
            video['_llm_content'] = transcript
        else:
            video['_has_transcript'] = False
            title = (video.get('title') or '').strip()
            description = (video.get('content') or '').strip()
            parts = []
            if title:
                parts.append(f"标题: {title}")
            if description:
                parts.append(f"描述: {description}")
            video['_llm_content'] = '\n'.join(parts) if parts else '（无内容）'

    def _prepare_author_refs(self, videos: List[Dict[str, Any]]) -> Dict[str, Dict]:
        """为每个视频分配内部键 V-001…V-n。"""
        ref_map: Dict[str, Dict] = {}
        for idx, v in enumerate(videos, start=1):
            ref_id = f"V-{idx:03d}"
            v['ref_id'] = ref_id
            ref_map[ref_id] = {
                'url': v.get('url') or '',
                'title': (v.get('title') or '')[:200],
                'content_preview': (v.get('content') or '')[:150],
                'author': v.get('author_identifier') or '',
                'author_name': v.get('author_name') or v.get('author_identifier') or '',
                'time': v['published_at'].strftime('%H:%M') if v.get('published_at') else '',
                'views': v.get('view_count') or 0,
                'engagement': v.get('engagement_count') or 0,
                'has_transcript': v.get('_has_transcript', False),
            }
        return ref_map

    def _format_author_videos(self, videos: List[Dict], ref_map: Dict) -> str:
        """将一个频道的视频格式化为 LLM 输入文本。"""
        lines = []
        for v in videos:
            time_str = v['published_at'].strftime('%H:%M') if v.get('published_at') else ''
            title = (v.get('title') or '').strip()
            views = v.get('view_count', 0)
            engagement = v.get('engagement_count', 0)
            has_transcript = v.get('_has_transcript', False)
            llm_content = v.get('_llm_content', '')

            source_tag = "字幕" if has_transcript else "标题+描述"
            url = v.get('url', '')

            # Truncate very long transcripts to ~4000 chars per video
            if len(llm_content) > 4000:
                llm_content = llm_content[:4000] + "\n...(字幕过长，已截断)"

            entry = (
                f"[{time_str}] 「{title}」\n"
                f"观看:{views} | 互动:{engagement} | 来源:{source_tag}\n"
                f"URL: {url}\n"
                f"内容:\n{llm_content}"
            )
            lines.append(entry)
        return '\n\n---\n\n'.join(lines)

    def _get_author_top_videos(
        self, videos: List[Dict], ref_map: Dict, top_n: int = 5
    ) -> List[Dict]:
        """返回该频道互动/观看最高的视频列表。"""
        sorted_videos = sorted(
            videos,
            key=lambda v: (v.get('view_count') or 0) + (v.get('engagement_count') or 0),
            reverse=True,
        )
        result = []
        for v in sorted_videos[:top_n]:
            result.append({
                'ref_id': v.get('ref_id', ''),
                'author': v.get('author_identifier', ''),
                'author_name': v.get('author_name', ''),
                'views': v.get('view_count', 0),
                'engagement': v.get('engagement_count', 0),
                'title': (v.get('title') or '')[:120],
                'url': v.get('url', ''),
                'has_transcript': v.get('_has_transcript', False),
            })
        return result

    # ── Phase 1 LLM ──────────────────────────────────────────────────────

    async def _run_phase1(
        self,
        videos: List[Dict],
        ref_map: Dict,
        author_identifier: str,
        author_display_name: str,
        summary_date: date,
    ) -> str:
        """执行 Phase 1 LLM 调用，支持多 chunk 切分。"""
        chunks = [
            videos[i:i + MAX_VIDEOS_PER_CHANNEL_CHUNK]
            for i in range(0, len(videos), MAX_VIDEOS_PER_CHANNEL_CHUNK)
        ]

        logger.info(f"[Phase 1] {author_display_name}: "
                    f"{len(videos)} 个视频, {len(chunks)} 个 chunk")

        chunk_summaries = []
        for chunk_idx, chunk in enumerate(chunks):
            videos_text = self._format_author_videos(chunk, ref_map)

            variables = {
                'author_name': author_display_name,
                'report_date': str(summary_date),
                'video_count': str(len(chunk)),
                'videos_text': videos_text,
                'report_language': 'zh',
                'custom_instructions': '',
            }

            logger.info(f"[Phase 1] {author_display_name} chunk {chunk_idx+1}/{len(chunks)}: "
                        f"{len(chunk)} 个视频, videos_text={len(videos_text)} 字符")

            summary = await self._call_llm(
                template_code=DEFAULT_PHASE1_TEMPLATE,
                variables=variables,
                max_tokens=MAX_TOKENS_OUTPUT_PHASE1,
            )
            chunk_summaries.append(summary)

        return '\n\n'.join(chunk_summaries)

    async def _call_llm(
        self, template_code: str, variables: Dict[str, Any], max_tokens: int = 3000
    ) -> str:
        """通过 DB 模板调用 LLM。"""
        var_preview = {k: (f"{str(v)[:200]}..." if len(str(v)) > 200 else str(v))
                       for k, v in variables.items()}
        logger.info(f"[LLM 调用] 模板={template_code}, max_tokens={max_tokens}, 变量预览={var_preview}")

        llm_start = time.time()
        response = await llm_service.chat_completion_with_template(
            template_code=template_code,
            variables=variables,
            max_tokens=max_tokens,
        )
        llm_elapsed = round(time.time() - llm_start, 2)

        content_preview = response.content[:300] if response.content else "(empty)"
        logger.info(f"[LLM 响应] 模板={template_code}, 耗时={llm_elapsed}s, "
                    f"响应长度={len(response.content)}字符, 预览: {content_preview}")
        return response.content

    # ── Summary persistence ───────────────────────────────────────────────

    def _get_existing_summary(self, author_identifier: str, summary_date: date) -> Optional[Dict]:
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT id, status, summary_content, ref_map, top_videos, video_count, updated_at
                FROM youtube_author_daily_summaries
                WHERE author_identifier = %s AND summary_date = %s
            """, (author_identifier, summary_date))
            row = cursor.fetchone()
            if row:
                return {
                    'id': row[0], 'status': row[1], 'summary_content': row[2],
                    'ref_map': row[3], 'top_videos': row[4], 'video_count': row[5],
                    'updated_at': row[6],
                }
            return None
        finally:
            cursor.close()

    def _upsert_summary_status(
        self, author_identifier: str, author_name: str, summary_date: date, status: str
    ) -> int:
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("""
                INSERT INTO youtube_author_daily_summaries
                    (author_identifier, author_name, summary_date, status)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (author_identifier, summary_date) DO UPDATE
                    SET status = EXCLUDED.status,
                        author_name = EXCLUDED.author_name,
                        error_message = NULL,
                        updated_at = CURRENT_TIMESTAMP
                RETURNING id
            """, (author_identifier, author_name, summary_date, status))
            conn.commit()
            return cursor.fetchone()[0]
        finally:
            cursor.close()

    def _update_summary(
        self, summary_id: int, *,
        status: str,
        content: Optional[str] = None,
        video_count: Optional[int] = None,
        ref_map: Optional[Dict] = None,
        top_videos: Optional[List] = None,
        metadata: Optional[Dict] = None,
        error_message: Optional[str] = None,
    ):
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            from psycopg2.extras import Json
            cursor.execute("""
                UPDATE youtube_author_daily_summaries
                SET status = %s,
                    summary_content = COALESCE(%s, summary_content),
                    video_count = COALESCE(%s, video_count),
                    ref_map = COALESCE(%s, ref_map),
                    top_videos = COALESCE(%s, top_videos),
                    metadata = COALESCE(%s, metadata),
                    error_message = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
            """, (
                status,
                content,
                video_count,
                Json(ref_map) if ref_map is not None else None,
                Json(top_videos) if top_videos is not None else None,
                Json(metadata) if metadata is not None else None,
                error_message,
                summary_id,
            ))
            conn.commit()
        finally:
            cursor.close()

    # ── Query helpers ─────────────────────────────────────────────────────

    def get_author_summaries_for_date(
        self, author_identifiers: List[str], summary_date: date
    ) -> Dict[str, Dict]:
        """批量获取指定频道列表在某天的已完成摘要。"""
        if not author_identifiers:
            return {}
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT author_identifier, author_name, summary_content,
                       ref_map, top_videos, video_count
                FROM youtube_author_daily_summaries
                WHERE author_identifier = ANY(%s)
                  AND summary_date = %s
                  AND status IN ('completed', 'failed')
            """, (author_identifiers, summary_date))
            result = {}
            for row in cursor.fetchall():
                result[row[0]] = {
                    'author_identifier': row[0],
                    'author_name': row[1],
                    'summary_content': row[2],
                    'ref_map': row[3] or {},
                    'top_videos': row[4] or [],
                    'video_count': row[5] or 0,
                }
            return result
        finally:
            cursor.close()
