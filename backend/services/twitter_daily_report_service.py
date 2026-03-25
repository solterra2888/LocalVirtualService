# -*- coding: utf-8 -*-
"""
Twitter Daily Report 服务 — AI Agent 定期为每个用户生成 Twitter 数据总结报告

两阶段 LLM 策略：
  Phase 1: 按作者分组，逐作者生成摘要（保留推文引用编号 + URL）
  Phase 2: 汇总所有作者摘要，生成结构化 Daily Report（Markdown）

使用系统默认 prompt 模板：twitter_daily_phase1_default / twitter_daily_phase2_default
"""

import asyncio
import time
import concurrent.futures
from datetime import date, datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple

from backend.database.db_manager import DatabaseManager
from backend.utils.database_config import get_database_config
from backend.utils.logger import get_logger
from backend.core.llm_framework import llm_service

logger = get_logger("twitter_daily_report_service")

DEFAULT_PHASE1_TEMPLATE = "twitter_daily_phase1_default"
DEFAULT_PHASE2_TEMPLATE = "twitter_daily_phase2_default"

MAX_TWEETS_PER_AUTHOR_CHUNK = 30
MAX_TOKENS_OUTPUT_PHASE1 = 2000
MAX_TOKENS_OUTPUT_PHASE2 = 4000


class TwitterDailyReportService:
    """Twitter Daily Report 生成服务"""

    def __init__(self):
        db_params = get_database_config()
        self.db_manager = DatabaseManager(db_params)

    # ── Public API ─────────────────────────────────────────────────────────

    async def generate_report(
        self,
        user_id: int,
        report_date: date,
        force: bool = False,
    ) -> Dict[str, Any]:
        """
        为指定用户生成指定日期的 Twitter Daily Report。

        Returns:
            {'success': bool, 'report_id': int | None, 'message': str, ...}
        """
        start_time = time.time()
        logger.info("开始生成 Twitter Daily Report",
                     user_id=user_id, report_date=str(report_date))

        # 如果已存在且非强制，跳过
        if not force:
            existing = self._get_existing_report(user_id, report_date)
            if existing and existing.get('status') == 'completed':
                logger.info("报告已存在，跳过生成", user_id=user_id, report_date=str(report_date))
                return {
                    'success': True,
                    'report_id': existing['id'],
                    'message': '报告已存在',
                    'skipped': True,
                }

        # 创建 / 更新为 generating 状态
        report_id = self._upsert_report_status(user_id, report_date, 'generating')

        try:
            # 1) 拉取该用户当天的推文
            tweets = self._fetch_user_tweets(user_id, report_date)
            logger.info(f"[Step 1/7] 推文拉取完成: {len(tweets)} 条",
                        user_id=user_id, report_date=str(report_date))
            if not tweets:
                self._update_report(report_id, status='completed',
                                    content='当天无推文数据。', metadata={'tweet_count': 0})
                return {
                    'success': True,
                    'report_id': report_id,
                    'message': '当天无推文数据',
                    'tweet_count': 0,
                }

            # 2) 分配全局引用编号 + 按作者分组
            ref_map, grouped = self._prepare_tweets(tweets)
            authors_summary = {author: len(tws) for author, tws in grouped.items()}
            logger.info(f"[Step 2/6] 推文分组完成: {len(grouped)} 位作者, "
                        f"引用编号 {len(ref_map)} 个, 分布: {authors_summary}",
                        user_id=user_id)

            # 3) Phase 1 — per-author summaries
            logger.info(f"[Step 3/6] Phase 1 开始 — 逐作者生成摘要 ({len(grouped)} 位作者)...",
                        user_id=user_id)
            phase1_start = time.time()
            author_summaries = await self._phase1_author_summaries(
                grouped, ref_map, report_date
            )
            phase1_elapsed = round(time.time() - phase1_start, 2)
            for author, summary in author_summaries.items():
                logger.info(f"[Phase 1] @{author} 摘要完成: {len(summary)} 字符")
            logger.info(f"[Step 3/6] Phase 1 完成: {len(author_summaries)} 位作者, 耗时 {phase1_elapsed}s",
                        user_id=user_id)

            # 4) Phase 2 — synthesize final report
            top_tweets = self._get_top_tweets(tweets, ref_map, top_n=5)
            logger.info(f"[Step 4/6] Phase 2 开始 — 综合分析报告生成...",
                        user_id=user_id)
            phase2_start = time.time()
            report_content, phase2_usage = await self._phase2_synthesize(
                author_summaries, ref_map, top_tweets,
                report_date, len(tweets)
            )
            phase2_elapsed = round(time.time() - phase2_start, 2)
            logger.info(f"[Step 4/6] Phase 2 完成: 报告 {len(report_content)} 字符, 耗时 {phase2_elapsed}s",
                        user_id=user_id)

            # 5) 构建 metadata
            elapsed = round(time.time() - start_time, 2)
            metadata = {
                'tweet_count': len(tweets),
                'author_count': len(grouped),
                'top_authors': [a for a, _ in sorted(
                    grouped.items(), key=lambda x: len(x[1]), reverse=True
                )[:5]],
                'generation_time_seconds': elapsed,
                'tweet_references': [
                    {'ref_id': rid, 'url': info['url'], 'author': info['author']}
                    for rid, info in ref_map.items()
                ],
            }
            logger.info(f"[Step 5/6] Metadata 已构建", user_id=user_id)

            # 6) 保存
            self._update_report(report_id, status='completed',
                                content=report_content, metadata=metadata)

            logger.info(f"[Step 6/6] Twitter Daily Report 生成完成: "
                        f"{len(tweets)} 条推文, {len(grouped)} 位作者, "
                        f"Phase1={phase1_elapsed}s, Phase2={phase2_elapsed}s, 总计={elapsed}s",
                        user_id=user_id, report_date=str(report_date))

            return {
                'success': True,
                'report_id': report_id,
                'message': f'报告生成成功（{len(tweets)} 条推文，{len(grouped)} 位作者，{elapsed}s）',
                'tweet_count': len(tweets),
                'author_count': len(grouped),
                'generation_time_seconds': elapsed,
            }

        except Exception as e:
            error_msg = f"报告生成失败: {str(e)}"
            logger.error(error_msg, user_id=user_id, exception=str(e))
            self._update_report(report_id, status='failed', error_message=error_msg)
            return {'success': False, 'report_id': report_id, 'message': error_msg}

    async def generate_all_user_reports(self, report_date: date) -> Dict[str, Any]:
        """遍历所有启用了 Twitter 订阅的用户，逐一生成报告。"""
        user_ids = self._get_enabled_twitter_user_ids()
        if not user_ids:
            return {'success': True, 'message': '没有启用 Twitter 订阅的用户', 'reports': []}

        results = []
        for uid in user_ids:
            result = await self.generate_report(uid, report_date)
            results.append({'user_id': uid, **result})

        succeeded = sum(1 for r in results if r.get('success'))
        return {
            'success': True,
            'message': f'共 {len(user_ids)} 个用户，成功 {succeeded} 个',
            'reports': results,
        }

    # ── Data fetching ──────────────────────────────────────────────────────

    def _fetch_user_tweets(self, user_id: int, report_date: date) -> List[Dict[str, Any]]:
        """查询该用户在指定日期内可见的所有 Twitter 内容。"""
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            start_dt = datetime.combine(report_date, datetime.min.time())
            end_dt = start_dt + timedelta(days=1)
            cursor.execute("""
                SELECT id, content_id, title, content, author_name, author_identifier,
                       url, media_urls, published_at, engagement_count, view_count,
                       metadata, is_retweet, retweeted_author, retweeted_content,
                       retweeted_url, is_quote, quoted_author, quoted_content,
                       quoted_url, is_reply, in_reply_to_username, heat_level
                FROM subscription_content
                WHERE channel_type = 'twitter'
                  AND published_at >= %s AND published_at < %s
                  AND %s = ANY(user_ids)
                ORDER BY published_at ASC
            """, (start_dt, end_dt, user_id))
            columns = [
                'id', 'content_id', 'title', 'content', 'author_name',
                'author_identifier', 'url', 'media_urls', 'published_at',
                'engagement_count', 'view_count', 'metadata',
                'is_retweet', 'retweeted_author', 'retweeted_content',
                'retweeted_url', 'is_quote', 'quoted_author', 'quoted_content',
                'quoted_url', 'is_reply', 'in_reply_to_username', 'heat_level',
            ]
            rows = cursor.fetchall()
            return [dict(zip(columns, row)) for row in rows]
        finally:
            cursor.close()

    def _get_enabled_twitter_user_ids(self) -> List[int]:
        """获取所有有活跃 Twitter 订阅的用户 ID。"""
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT DISTINCT user_id
                FROM subscription_list
                WHERE channel_type = 'twitter'
                  AND enabled = true
                ORDER BY user_id
            """)
            return [row[0] for row in cursor.fetchall()]
        finally:
            cursor.close()

    # ── Tweet preparation ──────────────────────────────────────────────────

    def _prepare_tweets(
        self, tweets: List[Dict[str, Any]]
    ) -> Tuple[Dict[str, Dict], Dict[str, List[Dict]]]:
        """
        为每条推文分配全局引用编号，按作者分组。

        Returns:
            (ref_map, grouped)
            ref_map: { 'T-001': { url, content, author, time, engagement, ... } }
            grouped: { author_identifier: [tweet_dicts_with_ref_id] }
        """
        ref_map: Dict[str, Dict] = {}
        grouped: Dict[str, List[Dict]] = {}
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
            author = tw.get('author_identifier') or 'unknown'
            grouped.setdefault(author, []).append(tw)
        return ref_map, grouped

    def _get_top_tweets(
        self, tweets: List[Dict], ref_map: Dict, top_n: int = 5
    ) -> str:
        """返回互动最高的推文的格式化文本，供 Phase 2 使用。"""
        sorted_tweets = sorted(tweets, key=lambda t: t.get('engagement_count') or 0, reverse=True)
        lines = []
        for tw in sorted_tweets[:top_n]:
            ref_id = tw.get('ref_id', '')
            author = tw.get('author_identifier', '')
            content_preview = (tw.get('content') or '')[:120].replace('\n', ' ')
            engagement = tw.get('engagement_count', 0)
            url = tw.get('url', '')
            lines.append(f"[{ref_id}] @{author} | 互动:{engagement} | \"{content_preview}\" | {url}")
        return '\n'.join(lines)

    # ── Phase 1: Per-author summaries ──────────────────────────────────────

    def _format_author_tweets(
        self, author_tweets: List[Dict], ref_map: Dict
    ) -> str:
        """将一个作者的推文格式化为 LLM 输入文本。"""
        lines = []
        for tw in author_tweets:
            ref_id = tw.get('ref_id', '')
            time_str = tw['published_at'].strftime('%H:%M') if tw.get('published_at') else ''
            content = (tw.get('content') or '').strip()
            url = tw.get('url', '')
            engagement = tw.get('engagement_count', 0)

            entry = f"[{ref_id}] ({time_str}) 互动:{engagement}\n\"{content}\"\nURL: {url}"

            if tw.get('is_retweet') and tw.get('retweeted_content'):
                entry += f"\n  ↻ 转推 @{tw.get('retweeted_author','')}: \"{tw['retweeted_content'][:100]}\""
            if tw.get('is_quote') and tw.get('quoted_content'):
                entry += f"\n  ❝ 引用 @{tw.get('quoted_author','')}: \"{tw['quoted_content'][:100]}\""
            if tw.get('is_reply') and tw.get('in_reply_to_username'):
                entry += f"\n  ↩ 回复 @{tw['in_reply_to_username']}"

            lines.append(entry)
        return '\n\n'.join(lines)

    async def _phase1_author_summaries(
        self,
        grouped: Dict[str, List[Dict]],
        ref_map: Dict,
        report_date: date,
    ) -> Dict[str, str]:
        """Phase 1: 逐作者生成摘要。返回 {author: summary_text}。"""
        summaries: Dict[str, str] = {}

        author_idx = 0
        total_authors = len(grouped)
        for author, author_tweets in grouped.items():
            author_idx += 1
            chunks = [
                author_tweets[i:i + MAX_TWEETS_PER_AUTHOR_CHUNK]
                for i in range(0, len(author_tweets), MAX_TWEETS_PER_AUTHOR_CHUNK)
            ]

            logger.info(f"[Phase 1] ({author_idx}/{total_authors}) @{author}: "
                        f"{len(author_tweets)} 条推文, {len(chunks)} 个 chunk")

            chunk_summaries = []
            for chunk_idx, chunk in enumerate(chunks):
                tweets_text = self._format_author_tweets(chunk, ref_map)
                author_display = chunk[0].get('author_name') or author

                variables = {
                    'author_name': author_display,
                    'report_date': str(report_date),
                    'tweet_count': str(len(chunk)),
                    'tweets_text': tweets_text,
                    'report_language': 'zh',
                    'custom_instructions': '',
                }

                logger.info(f"[Phase 1] @{author} chunk {chunk_idx+1}/{len(chunks)}: "
                            f"{len(chunk)} 条推文, tweets_text={len(tweets_text)} 字符")

                summary = await self._call_llm(
                    template_code=DEFAULT_PHASE1_TEMPLATE,
                    variables=variables,
                    max_tokens=MAX_TOKENS_OUTPUT_PHASE1,
                )
                chunk_summaries.append(summary)

            summaries[author] = '\n\n'.join(chunk_summaries)

        return summaries

    # ── Phase 2: Synthesis ─────────────────────────────────────────────────

    async def _phase2_synthesize(
        self,
        author_summaries: Dict[str, str],
        ref_map: Dict,
        top_tweets: str,
        report_date: date,
        tweet_count: int,
    ) -> Tuple[str, Dict]:
        """Phase 2: 合成最终报告。返回 (report_markdown, usage_info)。"""
        all_summaries = '\n\n---\n\n'.join(
            f"{summary}" for author, summary in author_summaries.items()
        )

        variables = {
            'report_date': str(report_date),
            'author_count': str(len(author_summaries)),
            'tweet_count': str(tweet_count),
            'author_summaries': all_summaries,
            'top_tweets': top_tweets,
            'report_language': 'zh',
            'focus_topics_instruction': '',
            'custom_instructions': '',
        }

        logger.info(f"[Phase 2] 输入: {len(author_summaries)} 位作者摘要 "
                    f"({len(all_summaries)} 字符), top_tweets={len(top_tweets)} 字符, "
                    f"模板={DEFAULT_PHASE2_TEMPLATE}")

        report_content = await self._call_llm(
            template_code=DEFAULT_PHASE2_TEMPLATE,
            variables=variables,
            max_tokens=MAX_TOKENS_OUTPUT_PHASE2,
        )

        logger.info(f"[Phase 2] 最终报告: {len(report_content)} 字符")
        return report_content, {}

    # ── LLM call helper ───────────────────────────────────────────────────

    async def _call_llm(
        self,
        template_code: str,
        variables: Dict[str, Any],
        max_tokens: int = 2000,
    ) -> str:
        """通过 DB 模板调用 LLM。模板必须已在 llm_prompt_templates 中配置。"""
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

    # ── Report persistence ─────────────────────────────────────────────────

    def _get_existing_report(self, user_id: int, report_date: date) -> Optional[Dict]:
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT id, status, report_content, metadata
                FROM twitter_daily_reports
                WHERE user_id = %s AND report_date = %s
            """, (user_id, report_date))
            row = cursor.fetchone()
            if row:
                return {'id': row[0], 'status': row[1], 'content': row[2], 'metadata': row[3]}
            return None
        finally:
            cursor.close()

    def _upsert_report_status(self, user_id: int, report_date: date, status: str) -> int:
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("""
                INSERT INTO twitter_daily_reports (user_id, report_date, report_content, status)
                VALUES (%s, %s, '', %s)
                ON CONFLICT (user_id, report_date) DO UPDATE
                    SET status = EXCLUDED.status,
                        error_message = NULL,
                        updated_at = CURRENT_TIMESTAMP
                RETURNING id
            """, (user_id, report_date, status))
            conn.commit()
            return cursor.fetchone()[0]
        finally:
            cursor.close()

    def _update_report(
        self, report_id: int, *,
        status: str,
        content: Optional[str] = None,
        metadata: Optional[Dict] = None,
        error_message: Optional[str] = None,
    ):
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            from psycopg2.extras import Json
            cursor.execute("""
                UPDATE twitter_daily_reports
                SET status = %s,
                    report_content = COALESCE(%s, report_content),
                    metadata = COALESCE(%s, metadata),
                    error_message = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
            """, (status, content, Json(metadata) if metadata else None,
                  error_message, report_id))
            conn.commit()
        finally:
            cursor.close()

    # ── Query helpers (for API layer) ──────────────────────────────────────

    def get_user_reports(
        self, user_id: int, limit: int = 30, offset: int = 0
    ) -> Tuple[List[Dict], int]:
        """分页获取用户报告列表。"""
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT COUNT(*) FROM twitter_daily_reports WHERE user_id = %s
            """, (user_id,))
            total = cursor.fetchone()[0]

            cursor.execute("""
                SELECT id, report_date, status, error_message,
                       metadata, created_at, updated_at
                FROM twitter_daily_reports
                WHERE user_id = %s
                ORDER BY report_date DESC
                LIMIT %s OFFSET %s
            """, (user_id, limit, offset))
            columns = ['id', 'report_date', 'status', 'error_message',
                        'metadata', 'created_at', 'updated_at']
            reports = [dict(zip(columns, row)) for row in cursor.fetchall()]
            for r in reports:
                if r.get('report_date'):
                    r['report_date'] = str(r['report_date'])
                for ts_field in ('created_at', 'updated_at'):
                    if r.get(ts_field):
                        r[ts_field] = r[ts_field].isoformat()
            return reports, total
        finally:
            cursor.close()

    def get_report_by_date(self, user_id: int, report_date: date) -> Optional[Dict]:
        """获取指定日期的完整报告。"""
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT id, report_date, report_content, status,
                       error_message, metadata, created_at, updated_at
                FROM twitter_daily_reports
                WHERE user_id = %s AND report_date = %s
            """, (user_id, report_date))
            row = cursor.fetchone()
            if not row:
                return None
            columns = ['id', 'report_date', 'report_content', 'status',
                        'error_message', 'metadata', 'created_at', 'updated_at']
            report = dict(zip(columns, row))
            if report.get('report_date'):
                report['report_date'] = str(report['report_date'])
            for ts_field in ('created_at', 'updated_at'):
                if report.get(ts_field):
                    report[ts_field] = report[ts_field].isoformat()
            return report
        finally:
            cursor.close()

    def get_latest_report(self, user_id: int) -> Optional[Dict]:
        """获取用户最新的已完成报告。"""
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT id, report_date, report_content, status,
                       error_message, metadata, created_at, updated_at
                FROM twitter_daily_reports
                WHERE user_id = %s AND status = 'completed'
                ORDER BY report_date DESC
                LIMIT 1
            """, (user_id,))
            row = cursor.fetchone()
            if not row:
                return None
            columns = ['id', 'report_date', 'report_content', 'status',
                        'error_message', 'metadata', 'created_at', 'updated_at']
            report = dict(zip(columns, row))
            if report.get('report_date'):
                report['report_date'] = str(report['report_date'])
            for ts_field in ('created_at', 'updated_at'):
                if report.get(ts_field):
                    report[ts_field] = report[ts_field].isoformat()
            return report
        finally:
            cursor.close()

