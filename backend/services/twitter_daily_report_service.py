# -*- coding: utf-8 -*-
"""
Twitter Daily Report 服务 — AI Agent 定期为每个用户生成 Twitter 数据总结报告

优化后的两阶段解耦架构：
  阶段 A（全局）: TwitterAuthorSummaryService 按作者预生成摘要，同一作者同一天只调 LLM 一次
  阶段 B（本服务）: 读取预计算的作者摘要 → 全局引用重编号 → Phase 2 综合报告

如果预计算摘要缺失，会自动 fallback 到即时生成并缓存。
"""

import re
import time
from datetime import date, datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple

from backend.database.db_manager import DatabaseManager
from backend.utils.database_config import get_database_config
from backend.utils.logger import get_logger
from backend.core.llm_framework import llm_service

logger = get_logger("twitter_daily_report_service")

DEFAULT_PHASE2_TEMPLATE = "twitter_daily_phase2_default"
MAX_TOKENS_OUTPUT_PHASE2 = 4000

_REF_PATTERN = re.compile(r'\[T-(\d{3,})\]')


class TwitterDailyReportService:
    """Twitter Daily Report 生成服务（阶段 B：组装用户报告）"""

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

    # ── Public API ─────────────────────────────────────────────────────────

    async def generate_report(
        self,
        user_id: int,
        report_date: date,
        force: bool = False,
    ) -> Dict[str, Any]:
        """
        为指定用户生成指定日期的 Twitter Daily Report。
        优先从 twitter_author_daily_summaries 读取预计算摘要（阶段 A），
        缺失时自动 fallback 到即时生成。

        Returns:
            {'success': bool, 'report_id': int | None, 'message': str, ...}
        """
        start_time = time.time()
        logger.info("开始生成 Twitter Daily Report",
                     user_id=user_id, report_date=str(report_date))

        # 解析用户 Phase 2 模板偏好
        from backend.services.template_preference_service import TemplatePreferenceService
        pref_service = TemplatePreferenceService()
        phase2_template = pref_service.resolve_template_code(user_id, 'twitter_phase2')
        logger.info(f"Phase2 模板解析完成: {phase2_template}", user_id=user_id)

        if not force:
            existing = self._get_existing_report(user_id, report_date)
            if existing and existing.get('status') == 'completed':
                tpl_updated = self._get_template_updated_at(phase2_template)
                report_updated = existing.get('updated_at')
                if tpl_updated and report_updated:
                    if isinstance(report_updated, str):
                        report_updated = datetime.fromisoformat(report_updated)
                    if tpl_updated > report_updated:
                        logger.info(
                            f"Phase2 模板已更新 (模板={tpl_updated}, 报告={report_updated}), "
                            f"自动重新生成",
                            user_id=user_id, report_date=str(report_date)
                        )
                    else:
                        logger.info("报告已存在，跳过生成", user_id=user_id, report_date=str(report_date))
                        return {
                            'success': True,
                            'report_id': existing['id'],
                            'message': '报告已存在',
                            'skipped': True,
                        }
                else:
                    logger.info("报告已存在，跳过生成", user_id=user_id, report_date=str(report_date))
                    return {
                        'success': True,
                        'report_id': existing['id'],
                        'message': '报告已存在',
                        'skipped': True,
                    }

        report_id = self._upsert_report_status(user_id, report_date, 'generating')

        try:
            # 1) 获取用户订阅的 Twitter 作者列表
            user_authors = self._get_user_subscribed_authors(user_id)
            logger.info(f"[Step 1/6] 用户订阅作者: {len(user_authors)} 位",
                        user_id=user_id)
            if not user_authors:
                self._update_report(report_id, status='completed',
                                    content='当前没有启用的 Twitter 订阅。',
                                    metadata={'tweet_count': 0, 'author_count': 0})
                return {
                    'success': True,
                    'report_id': report_id,
                    'message': '没有启用的 Twitter 订阅',
                    'tweet_count': 0,
                }

            # 2) 从 twitter_author_daily_summaries 批量读取预计算摘要
            from backend.services.twitter_author_summary_service import TwitterAuthorSummaryService
            author_summary_service = TwitterAuthorSummaryService()

            author_ids = [a[0] for a in user_authors]
            cached_summaries = author_summary_service.get_author_summaries_for_date(
                author_ids, report_date
            )
            logger.info(f"[Step 2/6] 缓存摘要命中: {len(cached_summaries)}/{len(author_ids)}",
                        user_id=user_id)

            # 3) 补生成缺失的摘要（fallback）
            missing_authors = [
                (aid, aname) for aid, aname in user_authors
                if aid not in cached_summaries
            ]
            if missing_authors:
                logger.info(f"[Step 3/6] Fallback: 需即时生成 {len(missing_authors)} 位作者摘要",
                            user_id=user_id)
                for aid, aname in missing_authors:
                    result = await author_summary_service.generate_author_summary(
                        aid, aname, report_date
                    )
                    if result.get('success'):
                        fresh = author_summary_service.get_author_summaries_for_date(
                            [aid], report_date
                        )
                        cached_summaries.update(fresh)
            else:
                logger.info(f"[Step 3/6] 所有作者摘要均已缓存，无需 fallback",
                            user_id=user_id)

            # 过滤掉无推文数据的作者
            active_summaries = {
                k: v for k, v in cached_summaries.items()
                if v.get('tweet_count', 0) > 0
            }

            if not active_summaries:
                self._update_report(report_id, status='completed',
                                    content='当天无推文数据。',
                                    metadata={'tweet_count': 0, 'author_count': 0})
                return {
                    'success': True,
                    'report_id': report_id,
                    'message': '当天无推文数据',
                    'tweet_count': 0,
                }

            # 4) 全局引用重编号 + 组装
            renumbered_summaries, global_ref_map, author_display_map = self._renumber_and_assemble(
                active_summaries
            )
            total_tweet_count = sum(s.get('tweet_count', 0) for s in active_summaries.values())
            top_tweets_text = self._merge_top_tweets(active_summaries, global_ref_map)
            logger.info(f"[Step 4/6] 组装完成: {len(renumbered_summaries)} 位作者, "
                        f"{total_tweet_count} 条推文, {len(global_ref_map)} 引用",
                        user_id=user_id)

            # 5) Phase 2 — 综合报告
            logger.info(f"[Step 5/6] Phase 2 开始 — 综合分析报告生成...",
                        user_id=user_id)
            phase2_start = time.time()
            report_content, _ = await self._phase2_synthesize(
                renumbered_summaries, global_ref_map, top_tweets_text,
                report_date, total_tweet_count,
                template_code=phase2_template,
            )
            report_content = self._append_author_summaries_appendix(
                report_content, renumbered_summaries, author_display_map
            )
            phase2_elapsed = round(time.time() - phase2_start, 2)
            logger.info(f"[Step 5/6] Phase 2 完成: 报告（含附录）{len(report_content)} 字符, "
                        f"耗时 {phase2_elapsed}s", user_id=user_id)

            # 6) 保存
            elapsed = round(time.time() - start_time, 2)
            metadata = {
                'tweet_count': total_tweet_count,
                'author_count': len(renumbered_summaries),
                'top_authors': sorted(
                    active_summaries.keys(),
                    key=lambda a: active_summaries[a].get('tweet_count', 0),
                    reverse=True
                )[:5],
                'generation_time_seconds': elapsed,
                'tweet_references': [
                    {'ref_id': rid, 'url': info['url'], 'author': info['author']}
                    for rid, info in global_ref_map.items()
                ],
                'used_cached_summaries': len(cached_summaries) - len(missing_authors),
                'fallback_generated': len(missing_authors),
            }
            self._update_report(report_id, status='completed',
                                content=report_content, metadata=metadata)

            logger.info(f"[Step 6/6] Twitter Daily Report 生成完成: "
                        f"{total_tweet_count} 条推文, {len(renumbered_summaries)} 位作者, "
                        f"Phase2={phase2_elapsed}s, 总计={elapsed}s",
                        user_id=user_id, report_date=str(report_date))

            return {
                'success': True,
                'report_id': report_id,
                'message': f'报告生成成功（{total_tweet_count} 条推文，'
                           f'{len(renumbered_summaries)} 位作者，{elapsed}s）',
                'tweet_count': total_tweet_count,
                'author_count': len(renumbered_summaries),
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

    def _get_user_subscribed_authors(self, user_id: int) -> List[Tuple[str, Optional[str]]]:
        """获取用户订阅的所有启用 Twitter 作者 [(channel_identifier, channel_name)]。"""
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT channel_identifier, channel_name
                FROM subscription_list
                WHERE user_id = %s
                  AND channel_type = 'twitter'
                  AND enabled = true
                ORDER BY channel_identifier
            """, (user_id,))
            return [(row[0], row[1]) for row in cursor.fetchall()]
        finally:
            cursor.close()

    def _get_enabled_twitter_user_ids(self) -> List[int]:
        """获取所有有活跃 Twitter 订阅且未关闭自动生成的用户 ID。"""
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT DISTINCT sl.user_id
                FROM subscription_list sl
                LEFT JOIN user_daily_report_auto_settings udras
                    ON udras.user_id = sl.user_id
                WHERE sl.channel_type = 'twitter'
                  AND sl.enabled = true
                  AND COALESCE(udras.twitter_auto_enabled, false) = true
                ORDER BY sl.user_id
            """)
            return [row[0] for row in cursor.fetchall()]
        finally:
            cursor.close()

    # ── Ref renumbering & assembly ─────────────────────────────────────────

    def _renumber_and_assemble(
        self, author_summaries: Dict[str, Dict]
    ) -> Tuple[Dict[str, str], Dict[str, Dict], Dict[str, str]]:
        """
        合并各作者的 ref_map 为全局 T-001…T-m；若摘要正文含 [T-xxx] 则替换为全局编号。
        以 URL 为要点的 Phase1 输出通常不含 [T-xxx]，此时仅更新元数据用的 global_ref_map。

        Args:
            author_summaries: {author_id: {summary_content, ref_map, ...}}

        Returns:
            (renumbered_text_dict, global_ref_map, author_display_map)
            renumbered_text_dict: {author_id: renumbered_summary_text}
            global_ref_map: {T-001: {url, content, author, ...}} (globally unique)
            author_display_map: {author_id: display_name}
        """
        global_ref_map: Dict[str, Dict] = {}
        renumbered: Dict[str, str] = {}
        author_display_map: Dict[str, str] = {}
        offset = 0

        for author_id, data in author_summaries.items():
            local_ref_map = data.get('ref_map') or {}
            summary_text = data.get('summary_content') or ''
            author_display_map[author_id] = data.get('author_name') or author_id

            if not local_ref_map:
                renumbered[author_id] = summary_text
                continue

            rename_map = {}
            for local_idx, (local_ref, info) in enumerate(local_ref_map.items(), start=1):
                global_ref = f"T-{offset + local_idx:03d}"
                rename_map[local_ref] = global_ref
                global_ref_map[global_ref] = info

            def _replace_ref(match):
                old_ref = match.group(0).strip('[]')
                return f"[{rename_map.get(old_ref, old_ref)}]"

            renumbered_text = _REF_PATTERN.sub(_replace_ref, summary_text)
            renumbered[author_id] = renumbered_text
            offset += len(local_ref_map)

        return renumbered, global_ref_map, author_display_map

    def _merge_top_tweets(
        self, author_summaries: Dict[str, Dict], global_ref_map: Dict
    ) -> str:
        """
        合并各作者的 top_tweets，按互动量排序取 Top 5，
        用全局 ref_map 中的 ref_id 输出格式化文本。
        """
        all_top = []
        for author_id, data in author_summaries.items():
            local_ref_map = data.get('ref_map') or {}
            for tw in (data.get('top_tweets') or []):
                local_ref = tw.get('ref_id', '')
                global_ref = None
                for gref, ginfo in global_ref_map.items():
                    if ginfo.get('url') == tw.get('url') and ginfo.get('url'):
                        global_ref = gref
                        break
                all_top.append({
                    **tw,
                    'global_ref': global_ref or local_ref,
                })

        all_top.sort(key=lambda t: t.get('engagement', 0), reverse=True)
        lines = []
        for tw in all_top[:5]:
            ref = tw.get('global_ref', '')
            author = tw.get('author', '')
            engagement = tw.get('engagement', 0)
            preview = tw.get('content_preview', '')
            url = tw.get('url', '')
            lines.append(f"[{ref}] @{author} | 互动:{engagement} | \"{preview}\" | {url}")
        return '\n'.join(lines)

    # ── Phase 2: Synthesis ─────────────────────────────────────────────────

    async def _phase2_synthesize(
        self,
        author_summaries: Dict[str, str],
        ref_map: Dict,
        top_tweets: str,
        report_date: date,
        tweet_count: int,
        template_code: str = DEFAULT_PHASE2_TEMPLATE,
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
                    f"模板={template_code}")

        report_content = await self._call_llm(
            template_code=template_code,
            variables=variables,
            max_tokens=MAX_TOKENS_OUTPUT_PHASE2,
        )

        logger.info(f"[Phase 2] 最终报告: {len(report_content)} 字符")
        return report_content, {}

    def _append_author_summaries_appendix(
        self,
        report_body: str,
        author_summaries: Dict[str, str],
        author_display_map: Dict[str, str],
    ) -> str:
        """
        在 Phase 2 主文下方追加附录：按作者顺序编号 S1…SN，对应各作者的 Phase 1 摘要全文。
        """
        if not author_summaries:
            return report_body
        parts: List[str] = [report_body.rstrip(), "\n\n---\n\n"]
        n = len(author_summaries)
        parts.append(f"## 附录：各作者摘要（S1–S{n}）\n\n")
        for idx, author in enumerate(author_summaries.keys(), start=1):
            display = author_display_map.get(author, author)
            summary = (author_summaries.get(author) or "").strip()
            parts.append(f"### S{idx} — @{display}\n\n")
            parts.append(summary)
            parts.append("\n\n")
        return "".join(parts).rstrip() + "\n"

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
                SELECT id, status, report_content, metadata, updated_at
                FROM twitter_daily_reports
                WHERE user_id = %s AND report_date = %s
            """, (user_id, report_date))
            row = cursor.fetchone()
            if row:
                return {'id': row[0], 'status': row[1], 'content': row[2], 'metadata': row[3], 'updated_at': row[4]}
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
