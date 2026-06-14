# -*- coding: utf-8 -*-
"""
Twitter Author Daily Summary 服务 — 全局作者摘要预生成

将原本 per-user 的 Phase 1（按作者摘要）提升为全局共享：
同一作者 + 同一天只调用一次 LLM，结果存入 twitter_author_daily_summaries 表，
所有订阅该作者的用户在生成 Daily Report 时直接复用。
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
from backend.services.summary_common import (
    BATCH_CONCURRENT,
    PER_ITEM_TIMEOUT_SEC,
    CHUNK_CONCURRENT,
    CHUNK_LLM_TIMEOUT_SEC,
    CHUNK_LLM_RETRY,
    ORPHAN_THRESHOLD_MINUTES,
    SUMMARY_LLM_READ_TIMEOUT_SEC,
    orphan_cutoff_dt,
)

logger = get_logger("twitter_author_summary_service")

DEFAULT_PHASE1_TEMPLATE = "twitter_daily_phase1_default"
MAX_TWEETS_PER_AUTHOR_CHUNK = 30
MAX_TOKENS_OUTPUT_PHASE1 = 2000

# 批量生成时的并发与超时控制（常量来自 summary_common）
BATCH_CONCURRENT_AUTHORS = BATCH_CONCURRENT
PER_AUTHOR_TIMEOUT_SEC = PER_ITEM_TIMEOUT_SEC  # 600s


class TwitterAuthorSummaryService:
    """全局 Twitter 作者每日摘要生成服务"""

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
        为所有启用的 Twitter 订阅作者批量生成当日摘要。

        Returns:
            {'success': bool, 'message': str, 'results': [...]}
        """
        start_time = time.time()
        self._reset_orphan_generating_records(summary_date)
        authors = self._get_distinct_enabled_authors()
        if not authors:
            return {'success': True, 'message': '没有启用的 Twitter 订阅作者', 'results': []}

        window_start, resolved_end = resolve_report_content_window(
            summary_date, window_end, window_hours
        )
        logger.info(
            f"开始批量生成作者摘要: {len(authors)} 位作者, date={summary_date}, "
            f"window=[{window_start.isoformat()}, {resolved_end.isoformat()})"
        )

        total = len(authors)
        semaphore = asyncio.Semaphore(BATCH_CONCURRENT_AUTHORS)

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
                        timeout=PER_AUTHOR_TIMEOUT_SEC,
                    )
                except asyncio.TimeoutError:
                    author_elapsed = round(time.time() - author_start, 2)
                    logger.warning(
                        f"[{idx}/{total}] @{author_id} — TIMEOUT "
                        f"after {author_elapsed}s，标记 failed 并继续"
                    )
                    try:
                        existing = self._get_existing_summary(author_id, summary_date)
                        if existing:
                            self._update_summary(
                                existing['id'],
                                status='failed',
                                error_message=f"单作者超时（>{PER_AUTHOR_TIMEOUT_SEC}s）",
                            )
                    except Exception:
                        pass
                    return idx, author_id, {
                        'success': False,
                        'message': f'per-author timeout (>{PER_AUTHOR_TIMEOUT_SEC}s)',
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
        degraded = 0
        for idx, author_id, result, author_elapsed in raw_results:
            results.append({'author': author_id, **result})
            if result.get('skipped'):
                skipped += 1
                logger.info(
                    f"[{idx}/{total}] @{author_id} — skipped (already completed), {author_elapsed}s"
                )
            elif result.get('success'):
                succeeded += 1
                tweet_count = result.get('tweet_count', 0)
                dc = result.get('degraded_chunks', 0)
                if dc > 0:
                    degraded += 1
                logger.info(
                    f"[{idx}/{total}] @{author_id} — OK, {tweet_count} tweets, "
                    f"degraded_chunks={dc}, {author_elapsed}s"
                )
            else:
                logger.warning(
                    f"[{idx}/{total}] @{author_id} — FAILED: {result.get('message', '')}, {author_elapsed}s"
                )

        failed_count = len(authors) - succeeded - skipped
        elapsed = round(time.time() - start_time, 2)
        logger.info(
            f"批量作者摘要完成: succeeded={succeeded}, skipped={skipped}, "
            f"failed={failed_count}, degraded={degraded}, 耗时={elapsed}s"
        )

        return {
            'success': True,
            'message': (
                f'{len(authors)} 位作者: {succeeded} 成功 ({degraded} 降级), '
                f'{skipped} 跳过, {failed_count} 失败, 耗时 {elapsed}s'
            ),
            'total': len(authors),
            'succeeded': succeeded,
            'skipped': skipped,
            'failed': failed_count,
            'degraded': degraded,
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
        为单个作者生成指定日期的摘要。

        Returns:
            {'success': bool, 'summary_id': int | None, 'message': str, ...}
        """
        start_time = time.time()
        window_start, resolved_end = resolve_report_content_window(
            summary_date, window_end, window_hours
        )
        logger.info(
            f"开始生成作者摘要: @{author_identifier}, date={summary_date}, "
            f"window=[{window_start.isoformat()}, {resolved_end.isoformat()})"
        )

        if not force:
            existing = self._get_existing_summary(author_identifier, summary_date)
            if existing and existing['status'] == 'completed':
                tpl_updated = self._get_template_updated_at(DEFAULT_PHASE1_TEMPLATE)
                summary_updated = existing.get('updated_at')
                # Only re-generate if template was updated more than 1 hour before the summary
                # (avoids false positives caused by service restarts refreshing template updated_at)
                from datetime import timedelta
                if tpl_updated and summary_updated and tpl_updated > (summary_updated + timedelta(hours=1)):
                    logger.info(
                        f"模板已更新 (模板={tpl_updated}, 摘要={summary_updated}), "
                        f"自动重新生成: @{author_identifier}"
                    )
                else:
                    logger.info(f"作者摘要已存在, 跳过: @{author_identifier}")
                    return {
                        'success': True,
                        'summary_id': existing['id'],
                        'message': '摘要已存在',
                        'skipped': True,
                    }

        summary_id = self._upsert_summary_status(
            author_identifier, author_name or author_identifier, summary_date, 'generating'
        )

        try:
            tweets = self._fetch_author_tweets(
                author_identifier, window_start, resolved_end
            )
            logger.info(f"@{author_identifier}: 拉取到 {len(tweets)} 条推文")

            if not tweets:
                self._update_summary(
                    summary_id,
                    status='completed',
                    content='当天无推文。',
                    tweet_count=0,
                    ref_map={},
                    top_tweets=[],
                )
                return {
                    'success': True,
                    'summary_id': summary_id,
                    'message': '当天无推文',
                    'tweet_count': 0,
                    'skipped': False,
                }

            ref_map = self._prepare_author_refs(tweets)
            summary_text, degraded_count = await self._run_phase1(
                tweets, ref_map, author_identifier,
                author_name or author_identifier, summary_date
            )
            top_tweets = self._get_author_top_tweets(tweets, ref_map, top_n=5)

            elapsed = round(time.time() - start_time, 2)
            chunk_count = max(1, (len(tweets) - 1) // MAX_TWEETS_PER_AUTHOR_CHUNK + 1)
            tpl_model_info = self._get_template_model_info(DEFAULT_PHASE1_TEMPLATE)
            metadata = {
                'generation_time_seconds': elapsed,
                'chunk_count': chunk_count,
                'degraded_chunks': degraded_count,
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
                tweet_count=len(tweets),
                ref_map=ref_map,
                top_tweets=top_tweets,
                metadata=metadata,
            )

            logger.info(f"@{author_identifier} 摘要完成: {len(tweets)} 条推文, "
                        f"{len(summary_text)} 字符, 耗时 {elapsed}s")

            return {
                'success': True,
                'summary_id': summary_id,
                'message': f'摘要生成成功（{len(tweets)} 条推文，{elapsed}s）',
                'tweet_count': len(tweets),
                'generation_time_seconds': elapsed,
                'degraded_chunks': degraded_count,
                'skipped': False,
            }

        except Exception as e:
            error_msg = f"作者摘要生成失败: {str(e)}"
            logger.error(error_msg, author=author_identifier, exception=str(e))
            self._update_summary(summary_id, status='failed', error_message=error_msg)
            return {'success': False, 'summary_id': summary_id, 'message': error_msg, 'skipped': False}

    # ── Data fetching ──────────────────────────────────────────────────────

    def _get_distinct_enabled_authors(self) -> List[Tuple[str, Optional[str]]]:
        """
        查询 subscription_list 中所有启用的 twitter 作者（去重）。

        Returns:
            [(channel_identifier, channel_name), ...]
        """
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT DISTINCT channel_identifier, MAX(channel_name) as channel_name
                FROM subscription_list
                WHERE channel_type = 'twitter'
                  AND enabled = true
                GROUP BY channel_identifier
                ORDER BY channel_identifier
            """)
            return [(row[0], row[1]) for row in cursor.fetchall()]
        finally:
            cursor.close()

    def _fetch_author_tweets(
        self,
        author_identifier: str,
        window_start: datetime,
        window_end: datetime,
    ) -> List[Dict[str, Any]]:
        """查询指定作者在时间窗口内的所有推文（不按用户过滤）。"""
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            start_dt = window_start
            end_dt = window_end
            cursor.execute("""
                SELECT id, content_id, title, content, author_name, author_identifier,
                       url, media_urls, published_at, engagement_count, view_count,
                       metadata, is_retweet, retweeted_author, retweeted_content,
                       retweeted_url, is_quote, quoted_author, quoted_content,
                       quoted_url, is_reply, in_reply_to_username, heat_level
                FROM subscription_content
                WHERE channel_type = 'twitter'
                  AND author_identifier = %s
                  AND published_at >= %s AND published_at < %s
                ORDER BY published_at ASC
            """, (author_identifier, start_dt, end_dt))
            columns = [
                'id', 'content_id', 'title', 'content', 'author_name',
                'author_identifier', 'url', 'media_urls', 'published_at',
                'engagement_count', 'view_count', 'metadata',
                'is_retweet', 'retweeted_author', 'retweeted_content',
                'retweeted_url', 'is_quote', 'quoted_author', 'quoted_content',
                'quoted_url', 'is_reply', 'in_reply_to_username', 'heat_level',
            ]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
        finally:
            cursor.close()

    # ── Tweet preparation ──────────────────────────────────────────────────

    def _prepare_author_refs(self, tweets: List[Dict[str, Any]]) -> Dict[str, Dict]:
        """
        为每条推文分配内部键 T-001…T-n（仅用于 ref_map / top_tweets / 阶段 B 元数据）。
        LLM 输入里不展示 T-xxx；摘要若按「每条要点附原文 URL」输出，正文可不含 [T-xxx]。
        """
        ref_map: Dict[str, Dict] = {}
        for idx, tw in enumerate(tweets, start=1):
            ref_id = f"T-{idx:03d}"
            tw['ref_id'] = ref_id
            ref_map[ref_id] = {
                'url': tw.get('url') or '',
                'content': (tw.get('content') or '')[:200],
                'author': tw.get('author_identifier') or '',
                'author_name': tw.get('author_name') or tw.get('author_identifier') or '',
                'time': tw['published_at'].strftime('%H:%M') if tw.get('published_at') else '',
                'engagement': tw.get('engagement_count') or 0,
            }
        return ref_map

    def _format_author_tweets(self, author_tweets: List[Dict], ref_map: Dict) -> str:
        """将一个作者的推文格式化为 LLM 输入文本（时间 + 正文 + URL，不暴露内部 T-xxx 编号）。"""
        lines = []
        for tw in author_tweets:
            time_str = tw['published_at'].strftime('%H:%M') if tw.get('published_at') else ''
            content = (tw.get('content') or '').strip()
            url = tw.get('url', '')
            engagement = tw.get('engagement_count', 0)

            entry = f"({time_str}) 互动:{engagement}\n\"{content}\"\nURL: {url}"

            if tw.get('is_retweet') and tw.get('retweeted_content'):
                entry += f"\n  ↻ 转推 @{tw.get('retweeted_author','')}: \"{tw['retweeted_content'][:100]}\""
            if tw.get('is_quote') and tw.get('quoted_content'):
                entry += f"\n  ❝ 引用 @{tw.get('quoted_author','')}: \"{tw['quoted_content'][:100]}\""
            if tw.get('is_reply') and tw.get('in_reply_to_username'):
                entry += f"\n  ↩ 回复 @{tw['in_reply_to_username']}"

            lines.append(entry)
        return '\n\n'.join(lines)

    def _get_author_top_tweets(
        self, tweets: List[Dict], ref_map: Dict, top_n: int = 5
    ) -> List[Dict]:
        """返回该作者互动最高的推文列表（结构化数据，供组装时使用）。"""
        sorted_tweets = sorted(tweets, key=lambda t: t.get('engagement_count') or 0, reverse=True)
        result = []
        for tw in sorted_tweets[:top_n]:
            result.append({
                'ref_id': tw.get('ref_id', ''),
                'author': tw.get('author_identifier', ''),
                'author_name': tw.get('author_name', ''),
                'engagement': tw.get('engagement_count', 0),
                'content_preview': (tw.get('content') or '')[:120].replace('\n', ' '),
                'url': tw.get('url', ''),
            })
        return result

    # ── Phase 1 LLM ──────────────────────────────────────────────────────

    async def _run_phase1(
        self,
        tweets: List[Dict],
        ref_map: Dict,
        author_identifier: str,
        author_display_name: str,
        summary_date: date,
    ) -> Tuple[str, int]:
        """执行 Phase 1 LLM 调用，支持多 chunk 有界并发 + per-chunk 超时重试。

        Returns:
            (summary_text, degraded_chunk_count)
        """
        chunks = [
            tweets[i:i + MAX_TWEETS_PER_AUTHOR_CHUNK]
            for i in range(0, len(tweets), MAX_TWEETS_PER_AUTHOR_CHUNK)
        ]
        logger.info(
            f"[Phase 1] @{author_identifier}: "
            f"{len(tweets)} 条推文, {len(chunks)} 个 chunk, "
            f"并发={CHUNK_CONCURRENT}, chunk超时={CHUNK_LLM_TIMEOUT_SEC}s"
        )

        semaphore = asyncio.Semaphore(CHUNK_CONCURRENT)

        async def _run_one_chunk(chunk_idx: int, chunk: list) -> Tuple[int, str, bool]:
            async with semaphore:
                tweets_text = self._format_author_tweets(chunk, ref_map)
                variables = {
                    'author_name': author_display_name,
                    'report_date': str(summary_date),
                    'tweet_count': str(len(chunk)),
                    'tweets_text': tweets_text,
                    'report_language': 'zh',
                    'custom_instructions': '',
                }
                logger.info(
                    f"[Phase 1] @{author_identifier} chunk {chunk_idx+1}/{len(chunks)}: "
                    f"{len(chunk)} 条推文, input={len(tweets_text)} 字符"
                )
                for attempt in range(CHUNK_LLM_RETRY + 1):
                    try:
                        result = await asyncio.wait_for(
                            self._call_llm(
                                template_code=DEFAULT_PHASE1_TEMPLATE,
                                variables=variables,
                                max_tokens=MAX_TOKENS_OUTPUT_PHASE1,
                            ),
                            timeout=CHUNK_LLM_TIMEOUT_SEC,
                        )
                        return chunk_idx, result, False
                    except asyncio.TimeoutError:
                        if attempt < CHUNK_LLM_RETRY:
                            logger.warning(
                                f"[Phase 1] @{author_identifier} chunk {chunk_idx+1} "
                                f"超时 (attempt {attempt+1}/{CHUNK_LLM_RETRY+1}), retrying"
                            )
                            continue
                        logger.warning(
                            f"[Phase 1] @{author_identifier} chunk {chunk_idx+1} 全部超时, 降级"
                        )
                        return chunk_idx, self._build_degraded_chunk_summary(chunk, author_display_name), True
                    except Exception as exc:
                        if attempt < CHUNK_LLM_RETRY:
                            logger.warning(
                                f"[Phase 1] @{author_identifier} chunk {chunk_idx+1} "
                                f"错误 (attempt {attempt+1}): {exc}, retrying"
                            )
                            continue
                        logger.warning(
                            f"[Phase 1] @{author_identifier} chunk {chunk_idx+1} "
                            f"全部失败: {exc}, 降级"
                        )
                        return chunk_idx, self._build_degraded_chunk_summary(chunk, author_display_name), True
            return chunk_idx, "", True

        raw_results = await asyncio.gather(
            *[_run_one_chunk(i, chunk) for i, chunk in enumerate(chunks)]
        )
        raw_results = sorted(raw_results, key=lambda x: x[0])

        chunk_summaries = [r[1] for r in raw_results]
        degraded_count = sum(1 for r in raw_results if r[2])
        if degraded_count:
            logger.warning(
                f"[Phase 1] @{author_identifier}: {degraded_count}/{len(chunks)} 个 chunk 降级"
            )

        return '\n\n'.join(chunk_summaries), degraded_count

    def _build_degraded_chunk_summary(self, tweets: List[Dict], author_display_name: str) -> str:
        """LLM chunk 全部失败时的 title-only 降级摘要。"""
        lines = ["[摘要降级：LLM 生成超时，仅列出推文]"]
        for tw in tweets:
            content = (tw.get('content') or '').strip()[:100]
            url = tw.get('url', '')
            time_str = tw['published_at'].strftime('%H:%M') if tw.get('published_at') else ''
            lines.append(f"- [{time_str}] {content}  {url}")
        return '\n'.join(lines)

    async def _call_llm(
        self, template_code: str, variables: Dict[str, Any], max_tokens: int = 2000
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
            timeout=SUMMARY_LLM_READ_TIMEOUT_SEC,
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
                SELECT id, status, summary_content, ref_map, top_tweets, tweet_count, updated_at
                FROM twitter_author_daily_summaries
                WHERE author_identifier = %s AND summary_date = %s
            """, (author_identifier, summary_date))
            row = cursor.fetchone()
            if row:
                return {
                    'id': row[0], 'status': row[1], 'summary_content': row[2],
                    'ref_map': row[3], 'top_tweets': row[4], 'tweet_count': row[5],
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
                INSERT INTO twitter_author_daily_summaries
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
        tweet_count: Optional[int] = None,
        ref_map: Optional[Dict] = None,
        top_tweets: Optional[List] = None,
        metadata: Optional[Dict] = None,
        error_message: Optional[str] = None,
    ):
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            from psycopg2.extras import Json
            cursor.execute("""
                UPDATE twitter_author_daily_summaries
                SET status = %s,
                    summary_content = COALESCE(%s, summary_content),
                    tweet_count = COALESCE(%s, tweet_count),
                    ref_map = COALESCE(%s, ref_map),
                    top_tweets = COALESCE(%s, top_tweets),
                    metadata = COALESCE(%s, metadata),
                    error_message = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
            """, (
                status,
                content,
                tweet_count,
                Json(ref_map) if ref_map is not None else None,
                Json(top_tweets) if top_tweets is not None else None,
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
        """批量获取指定作者列表在某天的已完成（completed）摘要。
        不包含 failed 记录，使其进入 Phase B fallback 重试。"""
        if not author_identifiers:
            return {}
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT author_identifier, author_name, summary_content,
                       ref_map, top_tweets, tweet_count
                FROM twitter_author_daily_summaries
                WHERE author_identifier = ANY(%s)
                  AND summary_date = %s
                  AND status = 'completed'
            """, (author_identifiers, summary_date))
            result = {}
            for row in cursor.fetchall():
                result[row[0]] = {
                    'author_identifier': row[0],
                    'author_name': row[1],
                    'summary_content': row[2],
                    'ref_map': row[3] or {},
                    'top_tweets': row[4] or [],
                    'tweet_count': row[5] or 0,
                }
            return result
        finally:
            cursor.close()

    def get_failed_author_ids_for_date(
        self, author_identifiers: List[str], summary_date: date
    ) -> List[str]:
        """返回指定作者列表中当天 Phase A 状态为 failed 的 author_identifier 列表。"""
        if not author_identifiers:
            return []
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT author_identifier
                FROM twitter_author_daily_summaries
                WHERE author_identifier = ANY(%s)
                  AND summary_date = %s
                  AND status = 'failed'
            """, (author_identifiers, summary_date))
            return [row[0] for row in cursor.fetchall()]
        finally:
            cursor.close()

    def _reset_orphan_generating_records(self, target_date: date) -> int:
        """将当天超过 ORPHAN_THRESHOLD_MINUTES 仍处于 'generating' 的记录重置为 'failed'。"""
        cutoff = orphan_cutoff_dt()
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("""
                UPDATE twitter_author_daily_summaries
                SET status = 'failed',
                    error_message = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE summary_date = %s
                  AND status = 'generating'
                  AND updated_at < %s
            """, (
                f"orphan reset: stuck generating > {ORPHAN_THRESHOLD_MINUTES}min",
                target_date,
                cutoff,
            ))
            count = cursor.rowcount
            conn.commit()
            if count > 0:
                logger.warning(
                    f"Orphan reaper: {count} 条 'generating' 记录已重置为 'failed' "
                    f"(date={target_date}, cutoff={cutoff.isoformat()})"
                )
            return count
        finally:
            cursor.close()
