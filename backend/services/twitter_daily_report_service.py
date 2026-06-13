# -*- coding: utf-8 -*-
"""
Twitter Daily Report 服务 — AI Agent 定期为每个用户生成 Twitter 数据总结报告

两阶段解耦架构：
  阶段 A（全局）: TwitterAuthorSummaryService 按作者预生成摘要，同一作者同一天只调 LLM 一次
  阶段 B（本服务）: 读取预计算的作者摘要 → 全局引用重编号 → 机械拼接最终报告（不调 LLM）

如果预计算摘要缺失，会自动 fallback 到即时生成并缓存。
"""

import re
import time
from datetime import date, datetime, timedelta, timezone
from typing import Dict, Any, List, Optional, Tuple
from zoneinfo import ZoneInfo

from backend.database.db_manager import DatabaseManager
from backend.utils.database_config import get_database_config
from backend.utils.logger import get_logger
from backend.utils.datetime_utils import ensure_utc, resolve_report_content_window

logger = get_logger("twitter_daily_report_service")

_REF_PATTERN = re.compile(r'\[T-(\d{3,})\]')


def _build_generation_time_display(generated_at_utc: datetime) -> Dict[str, str]:
    """Format report generation instant in NY and Beijing time."""
    if generated_at_utc.tzinfo is None:
        generated_at_utc = generated_at_utc.replace(tzinfo=timezone.utc)

    def _fmt_in_zone(dt: datetime, tz_name: str, place_label: str) -> str:
        local = dt.astimezone(ZoneInfo(tz_name))
        return f"{local.strftime('%b %d, %Y %H:%M')} ({place_label})"

    return {
        "generated_at_new_york": _fmt_in_zone(
            generated_at_utc, "America/New_York", "New York"
        ),
        "generated_at_beijing": _fmt_in_zone(
            generated_at_utc, "Asia/Shanghai", "Beijing"
        ),
    }


class TwitterDailyReportService:
    """Twitter Daily Report 生成服务（阶段 B：机械拼接用户报告，不调 LLM）"""

    def __init__(self):
        db_params = get_database_config()
        self.db_manager = DatabaseManager(db_params)

    # ── Public API ─────────────────────────────────────────────────────────

    async def generate_report(
        self,
        user_id: int,
        report_date: date,
        force: bool = False,
        window_end: Optional[datetime] = None,
        window_hours: int = 24,
    ) -> Dict[str, Any]:
        """
        为指定用户生成指定日期的 Twitter Daily Report。
        优先从 twitter_author_daily_summaries 读取预计算摘要（阶段 A），
        缺失时自动 fallback 到即时生成。
        Phase 2 机械拼接各作者摘要，不调 LLM。

        Returns:
            {'success': bool, 'report_id': int | None, 'message': str, ...}
        """
        start_time = time.time()
        window_start, resolved_end = resolve_report_content_window(
            report_date, window_end, window_hours
        )
        window_metadata = {
            'window_start': ensure_utc(window_start).isoformat(),
            'window_end': ensure_utc(resolved_end).isoformat(),
        }
        logger.info(
            "开始生成 Twitter Daily Report",
            user_id=user_id,
            report_date=str(report_date),
            window_start=window_metadata['window_start'],
            window_end=window_metadata['window_end'],
        )

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

        report_id = self._upsert_report_status(user_id, report_date, 'generating')

        try:
            # 1) 获取用户订阅的 Twitter 作者列表
            user_authors = self._get_user_subscribed_authors(user_id)
            logger.info(f"[Step 1/5] 用户订阅作者: {len(user_authors)} 位",
                        user_id=user_id)
            if not user_authors:
                self._update_report(report_id, status='completed',
                                    content='当前没有启用的 Twitter 订阅。',
                                    metadata={'tweet_count': 0, 'author_count': 0, **window_metadata})
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
            logger.info(f"[Step 2/5] 缓存摘要命中: {len(cached_summaries)}/{len(author_ids)}",
                        user_id=user_id)

            # 3) 补生成缺失的摘要（fallback）
            missing_authors = [
                (aid, aname) for aid, aname in user_authors
                if aid not in cached_summaries
            ]
            if missing_authors:
                logger.info(f"[Step 3/5] Fallback: 需即时生成 {len(missing_authors)} 位作者摘要",
                            user_id=user_id)
                for aid, aname in missing_authors:
                    result = await author_summary_service.generate_author_summary(
                        aid, aname, report_date,
                        window_end=window_end,
                        window_hours=window_hours,
                    )
                    if result.get('success'):
                        fresh = author_summary_service.get_author_summaries_for_date(
                            [aid], report_date
                        )
                        cached_summaries.update(fresh)
            else:
                logger.info(f"[Step 3/5] 所有作者摘要均已缓存，无需 fallback",
                            user_id=user_id)

            # 过滤掉无推文数据的作者
            active_summaries = {
                k: v for k, v in cached_summaries.items()
                if v.get('tweet_count', 0) > 0
            }

            if not active_summaries:
                self._update_report(report_id, status='completed',
                                    content='当天无推文数据。',
                                    metadata={'tweet_count': 0, 'author_count': 0, **window_metadata})
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
            logger.info(f"[Step 4/5] 组装完成: {len(renumbered_summaries)} 位作者, "
                        f"{total_tweet_count} 条推文, {len(global_ref_map)} 引用",
                        user_id=user_id)

            # 5) Phase 2 — 机械拼接（不调 LLM）
            logger.info(f"[Step 5/5] Phase 2: 机械拼接 {len(renumbered_summaries)} 位作者摘要",
                        user_id=user_id)
            report_content = self._assemble_final_report(
                renumbered_summaries, active_summaries, author_display_map
            )

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
                'phase2_mode': 'mechanical_concat',
                **window_metadata,
            }
            self._update_report(report_id, status='completed',
                                content=report_content, metadata=metadata)

            # 7) HTML 渲染 + OSS 上传 + share_token
            html_result = self.render_and_upload_html(
                report_id=report_id,
                report_content=report_content,
                metadata=metadata,
                user_id=user_id,
                report_date=report_date,
            )
            if html_result.get('success'):
                share_token = html_result.get('share_token')
            else:
                share_token = None
                logger.warning(f"HTML 渲染/上传失败，share_token 未生成: {html_result.get('error')}")

            logger.info(f"Twitter Daily Report 生成完成: "
                        f"{total_tweet_count} 条推文, {len(renumbered_summaries)} 位作者, "
                        f"phase2=mechanical_concat, 总计={elapsed}s",
                        user_id=user_id, report_date=str(report_date))

            return {
                'success': True,
                'report_id': report_id,
                'share_token': share_token,
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

    # ── Ref renumbering & assembly ─────────────────────────────────────────

    def _renumber_and_assemble(
        self, author_summaries: Dict[str, Dict]
    ) -> Tuple[Dict[str, str], Dict[str, Dict], Dict[str, str]]:
        """
        合并各作者的 ref_map 为全局 T-001…T-m；若摘要正文含 [T-xxx] 则替换为全局编号。
        以 URL 为要点的 Phase1 输出通常不含 [T-xxx]，此时仅更新元数据用的 global_ref_map。

        Returns:
            (renumbered_text_dict, global_ref_map, author_display_map)
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

    # ── Phase 2: Mechanical assembly ──────────────────────────────────────

    def _assemble_final_report(
        self,
        renumbered_summaries: Dict[str, str],
        active_summaries: Dict[str, Dict],
        author_display_map: Dict[str, str],
    ) -> str:
        """
        Phase 2: 机械拼接各作者 Phase 1 摘要（不调 LLM）。

        按推文数量降序排序作者，每个作者单独一节。
        """
        total_tweet_count = sum(s.get('tweet_count', 0) for s in active_summaries.values())
        n_authors = len(renumbered_summaries)

        sorted_authors = sorted(
            renumbered_summaries.keys(),
            key=lambda aid: active_summaries.get(aid, {}).get('tweet_count', 0),
            reverse=True,
        )

        sections = []
        for author_id in sorted_authors:
            display_name = author_display_map.get(author_id) or author_id
            content = (renumbered_summaries.get(author_id) or '').strip()
            # 去除正文里重复的"作者标题行"(每个 Phase 1 chunk 都会输出
            # '### @<作者名> 摘要'),只保留下方 '## @{display_name}' 总标题，
            # 避免 @WalterBloomberg 这类作者名在拼接后一节内出现多次。
            author_keys = [
                display_name,
                author_id,
                active_summaries.get(author_id, {}).get('author_name'),
            ]
            content = self._strip_redundant_author_headings(content, author_keys)
            sections.append(f"## @{display_name}\n\n{content}")

        header = (
            f"# Twitter 每日摘要\n\n"
            f"共 {n_authors} 位作者 · {total_tweet_count} 条推文\n\n---\n\n"
        )
        return header + "\n\n---\n\n".join(sections)

    @staticmethod
    def _strip_redundant_author_headings(content: str, author_keys: List[str]) -> str:
        """
        删除摘要正文里重复的"作者标题行"。

        Phase 1 按推文 chunk 调用 LLM，每个 chunk 的输出都会带一行
        '### @<作者名> 摘要'；多个 chunk 拼接后该标题重复多次，再叠加阶段 B
        的 '## @<作者名>' 总标题，会让同一作者名在一节内出现 4+ 次。

        本方法仅删除"行首的 Markdown 标题行 / 纯作者名行"，保留正文要点与
        推文 URL（https://x.com/<作者名>/status/... 不受影响）。
        """
        if not content or not author_keys:
            return content or ''
        keys = sorted(
            {k.strip() for k in author_keys if k and k.strip()},
            key=len, reverse=True,
        )
        if not keys:
            return content
        name_alt = '|'.join(re.escape(k) for k in keys)
        heading_re = re.compile(
            rf'^(?:#{{1,6}}\s+@?(?:{name_alt})(?:\s.*)?$|@?(?:{name_alt})\s*$)',
            re.IGNORECASE,
        )
        kept = [
            ln for ln in content.split('\n')
            if not heading_re.match(ln.strip())
        ]
        return re.sub(r'\n{3,}', '\n\n', '\n'.join(kept)).strip()

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
                       error_message, metadata, created_at, updated_at,
                       oss_object_name, html_content, share_token
                FROM twitter_daily_reports
                WHERE user_id = %s AND report_date = %s
            """, (user_id, report_date))
            row = cursor.fetchone()
            if not row:
                return None
            columns = ['id', 'report_date', 'report_content', 'status',
                        'error_message', 'metadata', 'created_at', 'updated_at',
                        'oss_object_name', 'html_content', 'share_token']
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
                       error_message, metadata, created_at, updated_at,
                       oss_object_name, html_content, share_token
                FROM twitter_daily_reports
                WHERE user_id = %s AND status = 'completed'
                ORDER BY report_date DESC
                LIMIT 1
            """, (user_id,))
            row = cursor.fetchone()
            if not row:
                return None
            columns = ['id', 'report_date', 'report_content', 'status',
                        'error_message', 'metadata', 'created_at', 'updated_at',
                        'oss_object_name', 'html_content', 'share_token']
            report = dict(zip(columns, row))
            if report.get('report_date'):
                report['report_date'] = str(report['report_date'])
            for ts_field in ('created_at', 'updated_at'):
                if report.get(ts_field):
                    report[ts_field] = report[ts_field].isoformat()

            if report.get('updated_at'):
                try:
                    gen_time = datetime.fromisoformat(report['updated_at'])
                    time_display = _build_generation_time_display(gen_time)
                    report['generated_at_new_york'] = time_display['generated_at_new_york']
                    report['generated_at_beijing'] = time_display['generated_at_beijing']
                except Exception as e:
                    logger.warning(f"Failed to format generation time: {e}")

            return report
        finally:
            cursor.close()

    # ── HTML Rendering & OSS Upload ───────────────────────────────────────

    def _markdown_to_html(self, markdown_content: str) -> str:
        """
        使用 mistune 库将 Markdown 转换为 HTML。
        """
        import html

        if not markdown_content:
            return ''

        # 预处理：删除首行主标题（避免与 Header 日期重复）
        lines = markdown_content.split('\n')
        if lines and lines[0].startswith('# ') and ('Twitter' in lines[0] or 'Daily Report' in lines[0]):
            lines = lines[1:]
            if lines and lines[0].strip() == '':
                lines = lines[1:]
        markdown_content = '\n'.join(lines)

        try:
            import mistune
            import mistune.plugins.table as table_plugin

            renderer = mistune.HTMLRenderer()
            markdown_parser = mistune.Markdown(renderer, plugins=[table_plugin.table])
            rendered = markdown_parser(markdown_content)
            return self._ensure_links_open_in_new_tab(rendered)
        except ModuleNotFoundError:
            logger.warning("mistune 未安装，使用降级 HTML 渲染")
            escaped = html.escape(markdown_content)
            escaped = re.sub(
                r'\[([^\]]+)\]\((https?://[^\s)]+)\)',
                r'<a href="\2">\1</a>',
                escaped
            )
            escaped = re.sub(
                r'(?<!href=")(https?://[^\s<]+)',
                r'<a href="\1">\1</a>',
                escaped
            )
            paragraphs = [f"<p>{p.replace(chr(10), '<br/>')}</p>" for p in escaped.split("\n\n") if p.strip()]
            fallback_html = "".join(paragraphs) if paragraphs else "<p></p>"
            return self._ensure_links_open_in_new_tab(fallback_html)

    def _ensure_links_open_in_new_tab(self, html_content: str) -> str:
        """确保所有链接默认新开标签页并带安全 rel。"""
        if not html_content:
            return html_content

        def _patch_anchor(match: re.Match) -> str:
            attrs = match.group(1)
            if re.search(r'\btarget\s*=', attrs, flags=re.IGNORECASE):
                attrs = re.sub(r'target\s*=\s*["\'][^"\']*["\']', 'target="_blank"', attrs, flags=re.IGNORECASE)
            else:
                attrs = f'{attrs} target="_blank"'
            if not re.search(r'\brel\s*=', attrs, flags=re.IGNORECASE):
                attrs = f'{attrs} rel="noopener noreferrer"'
            return f'<a {attrs}>'

        return re.sub(r'<a\s+([^>]*?)>', _patch_anchor, html_content, flags=re.IGNORECASE)

    def _inject_tweet_links(self, content: str, tweet_refs: List[Dict]) -> str:
        """将 [T-xxx] 引用替换为可点击的链接。"""
        import re
        if not content or not tweet_refs:
            return content

        ref_map = {}
        for ref in tweet_refs:
            ref_id = ref.get('ref_id', '')
            url = ref.get('url', '')
            if ref_id and url:
                ref_map[ref_id] = url

        pattern = r'\[T-(\d+)\]'

        def replace_ref(match):
            ref_num = match.group(1)
            ref_key = f"T-{ref_num}"
            if ref_key in ref_map:
                url = ref_map[ref_key]
                return f'<a href="{url}" target="_blank" class="tweet-ref">[T-{ref_num}]</a>'
            return match.group(0)

        return re.sub(pattern, replace_ref, content)

    def _render_html(self, report_content: str, metadata: Dict) -> str:
        """使用 Jinja2 模板渲染 HTML。"""
        from jinja2 import Environment, FileSystemLoader, select_autoescape
        import os

        template_dir = os.path.join(os.path.dirname(__file__), "..", "templates")
        env = Environment(
            loader=FileSystemLoader(os.path.abspath(template_dir)),
            autoescape=select_autoescape(["html"]),
        )
        template = env.get_template("twitter_report_simple.html.j2")

        report_date = metadata.get('report_date', '')
        if isinstance(report_date, date):
            report_date = report_date.strftime("%B %d, %Y")
        elif isinstance(report_date, str):
            try:
                report_date = datetime.fromisoformat(report_date).strftime("%B %d, %Y")
            except:
                pass

        gen_time_utc = datetime.now(timezone.utc)
        time_display = _build_generation_time_display(gen_time_utc)

        tweet_refs = metadata.get('tweet_references', [])
        report_content = self._inject_tweet_links(report_content, tweet_refs)

        content_html = self._markdown_to_html(report_content)

        html = template.render(
            report_date=report_date,
            author_count=metadata.get('author_count', 0),
            tweet_count=metadata.get('tweet_count', 0),
            content_html=content_html,
            generated_at=datetime.now().strftime("%Y-%m-%d %H:%M"),
            generated_at_new_york=time_display['generated_at_new_york'],
            generated_at_beijing=time_display['generated_at_beijing'],
        )
        return html

    def render_and_upload_html(
        self,
        report_id: int,
        report_content: str,
        metadata: Dict,
        user_id: int,
        report_date: date,
    ) -> Dict[str, Any]:
        """
        渲染 HTML 报告并上传到 OSS，生成 share_token。

        Returns:
            {'success': bool, 'oss_object_name': str, 'share_token': str, 'html_content': str}
        """
        from uuid import uuid4
        from backend.services.oss_service import OSSService

        try:
            metadata['report_date'] = report_date

            html_content = self._render_html(report_content, metadata)

            share_token = uuid4().hex

            date_str = report_date.strftime("%Y%m%d")
            oss_object_name = f"reports/Twitter/{user_id}/report_{date_str}.html"
            conn = self.db_manager.get_connection()
            cursor = conn.cursor()
            try:
                cursor.execute("""
                    UPDATE twitter_daily_reports
                    SET html_content = %s,
                        share_token = %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                """, (html_content, share_token, report_id))
                conn.commit()
            finally:
                cursor.close()

            oss_uploaded = False
            try:
                oss_service = OSSService()
                upload_result = oss_service.upload_bytes(
                    object_name=oss_object_name,
                    data=html_content.encode("utf-8"),
                    content_type="text/html; charset=utf-8",
                )
                if upload_result.get('success'):
                    oss_uploaded = True
                    conn = self.db_manager.get_connection()
                    cursor = conn.cursor()
                    try:
                        cursor.execute("""
                            UPDATE twitter_daily_reports
                            SET oss_object_name = %s,
                                updated_at = CURRENT_TIMESTAMP
                            WHERE id = %s
                        """, (oss_object_name, report_id))
                        conn.commit()
                    finally:
                        cursor.close()
                else:
                    logger.warning(f"OSS 上传失败（已保留数据库 HTML 版本）: {upload_result.get('error')}")
            except Exception as upload_exc:
                logger.warning(f"OSS 上传异常（已保留数据库 HTML 版本）: {str(upload_exc)}")

            logger.info(
                f"HTML 报告渲染完成: report_id={report_id}, "
                f"oss_uploaded={oss_uploaded}, share_token={share_token[:8]}…"
            )

            return {
                'success': True,
                'oss_object_name': oss_object_name if oss_uploaded else None,
                'oss_uploaded': oss_uploaded,
                'share_token': share_token,
                'html_content': html_content,
            }

        except Exception as e:
            logger.error(f"HTML 渲染/上传失败: {str(e)}")
            return {'success': False, 'error': str(e)}

    def get_report_html_by_token(self, token: str) -> Optional[bytes]:
        """
        通过 share_token 获取 HTML 报告内容。

        优先从数据库 html_content 字段读取，fallback 到 OSS。
        """
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT oss_object_name, html_content
                FROM twitter_daily_reports
                WHERE share_token = %s AND status = 'completed'
                LIMIT 1
            """, (token,))
            row = cursor.fetchone()
            if not row:
                logger.warning(f"[get_report_html_by_token] token not found: {token[:8]}…")
                return None
            obj_name, html_content = row

            if html_content:
                return html_content.encode("utf-8")

            if obj_name:
                from backend.services.oss_service import OSSService
                oss_service = OSSService()
                logger.info(f"[get_report_html_by_token] 从 OSS 获取: {obj_name}")
                html_bytes = oss_service.get_object_bytes(obj_name)
                if html_bytes is None:
                    logger.error(f"[get_report_html_by_token] OSS 获取失败: {obj_name}")
                return html_bytes

            logger.error(f"[get_report_html_by_token] 无 html_content 和 oss_object_name: {token[:8]}…")
            return None
        finally:
            cursor.close()
