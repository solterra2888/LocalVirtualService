# -*- coding: utf-8 -*-
"""
YouTube Daily Report 服务 — 定期为每个用户生成 YouTube 数据总结报告

两阶段解耦架构：
  阶段 A（全局）: YouTubeAuthorSummaryService 按频道预生成摘要，同一频道同一天只调 LLM 一次
  阶段 B（本服务）: 读取预计算的频道摘要 → 全局引用重编号 → 机械拼接最终报告（不调 LLM）

如果预计算摘要缺失，会自动 fallback 到即时生成并缓存。
"""

import re
import asyncio
import time
from datetime import date, datetime, timezone
from typing import Dict, Any, List, Optional, Tuple
from zoneinfo import ZoneInfo

from backend.database.db_manager import DatabaseManager
from backend.utils.database_config import get_database_config
from backend.utils.logger import get_logger
from backend.utils.datetime_utils import ensure_utc, resolve_report_content_window
from backend.services.summary_common import PER_ITEM_TIMEOUT_SEC

logger = get_logger("youtube_daily_report_service")

_REF_PATTERN = re.compile(r'\[V-(\d{3,})\]')


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


class YouTubeDailyReportService:
    """YouTube Daily Report 生成服务（阶段 B：机械拼接用户报告，不调 LLM）"""

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
        为指定用户生成指定日期的 YouTube Daily Report。
        优先从 youtube_author_daily_summaries 读取预计算摘要（阶段 A），
        缺失时自动 fallback 到即时生成。
        Phase 2 机械拼接各频道摘要，不调 LLM。

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
            "开始生成 YouTube Daily Report",
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
            # 1) 获取用户订阅的 YouTube 频道列表
            user_authors = self._get_user_subscribed_authors(user_id)
            logger.info(f"[Step 1/5] 用户订阅频道: {len(user_authors)} 个",
                        user_id=user_id)
            if not user_authors:
                self._update_report(report_id, status='completed',
                                    content='当前没有启用的 YouTube 订阅。',
                                    metadata={'video_count': 0, 'channel_count': 0, **window_metadata})
                return {
                    'success': True,
                    'report_id': report_id,
                    'message': '没有启用的 YouTube 订阅',
                    'video_count': 0,
                }

            # 2) 从 youtube_author_daily_summaries 批量读取预计算摘要
            from backend.services.youtube_author_summary_service import YouTubeAuthorSummaryService
            author_summary_service = YouTubeAuthorSummaryService()

            author_ids = [a[0] for a in user_authors]
            cached_summaries = author_summary_service.get_author_summaries_for_date(
                author_ids, report_date
            )
            logger.info(f"[Step 2/5] 缓存摘要命中: {len(cached_summaries)}/{len(author_ids)}",
                        user_id=user_id)

            # 3) 补生成缺失的摘要（fallback）
            if force:
                # force 时重新生成所有订阅频道摘要（含 failed / generating / 已完成）
                missing_authors = list(user_authors)
            else:
                missing_authors = [
                    (aid, aname) for aid, aname in user_authors
                    if aid not in cached_summaries
                ]
            if missing_authors:
                logger.info(f"[Step 3/5] Fallback: 需即时生成 {len(missing_authors)} 个频道摘要",
                            user_id=user_id, force=force)
                for aid, aname in missing_authors:
                    try:
                        result = await asyncio.wait_for(
                            author_summary_service.generate_author_summary(
                                aid, aname, report_date,
                                force=force,
                                window_end=window_end,
                                window_hours=window_hours,
                            ),
                            timeout=PER_ITEM_TIMEOUT_SEC,
                        )
                    except asyncio.TimeoutError:
                        logger.warning(
                            f"[Step 3/5] Fallback 频道超时 (>{PER_ITEM_TIMEOUT_SEC}s): "
                            f"{aname or aid}",
                            user_id=user_id,
                        )
                        continue
                    if result.get('success'):
                        fresh = author_summary_service.get_author_summaries_for_date(
                            [aid], report_date
                        )
                        cached_summaries.update(fresh)
            else:
                logger.info(f"[Step 3/5] 所有频道摘要均已缓存，无需 fallback",
                            user_id=user_id)

            # 过滤掉无视频数据的频道
            active_summaries = {
                k: v for k, v in cached_summaries.items()
                if v.get('video_count', 0) > 0
            }

            if not active_summaries:
                # 区分"真无内容"与"上游 Phase A 失败"
                author_ids = [a[0] for a in user_authors]
                failed_channels = author_summary_service.get_failed_author_ids_for_date(
                    author_ids, report_date
                )
                is_partial = len(failed_channels) > 0
                if is_partial:
                    content_msg = (
                        f"当天有 {len(failed_channels)} 个频道摘要生成失败，无有效视频数据可组装报告。\n"
                        f"失败频道: {', '.join(failed_channels)}"
                    )
                    logger.warning(
                        f"[YouTube Report] 无有效摘要，{len(failed_channels)} 个频道失败",
                        user_id=user_id, failed_channels=failed_channels
                    )
                else:
                    content_msg = '当天无视频数据。'
                self._update_report(
                    report_id,
                    status='completed',
                    content=content_msg,
                    metadata={
                        'video_count': 0,
                        'channel_count': 0,
                        'is_partial': is_partial,
                        'failed_channels': failed_channels,
                        **window_metadata,
                    },
                )
                return {
                    'success': True,
                    'report_id': report_id,
                    'message': content_msg,
                    'video_count': 0,
                    'is_partial': is_partial,
                }

            # 4) 全局引用重编号 + 组装
            renumbered_summaries, global_ref_map, author_display_map = self._renumber_and_assemble(
                active_summaries
            )
            total_video_count = sum(s.get('video_count', 0) for s in active_summaries.values())
            logger.info(f"[Step 4/5] 组装完成: {len(renumbered_summaries)} 个频道, "
                        f"{total_video_count} 个视频, {len(global_ref_map)} 引用",
                        user_id=user_id)

            # 5) Phase 2 — 机械拼接（不调 LLM）
            logger.info(f"[Step 5/5] Phase 2: 机械拼接 {len(renumbered_summaries)} 个频道摘要",
                        user_id=user_id)
            report_content = self._assemble_final_report(
                renumbered_summaries, active_summaries, author_display_map
            )

            # 6) 保存
            elapsed = round(time.time() - start_time, 2)
            metadata = {
                'video_count': total_video_count,
                'channel_count': len(renumbered_summaries),
                'top_channels': sorted(
                    active_summaries.keys(),
                    key=lambda a: active_summaries[a].get('video_count', 0),
                    reverse=True
                )[:5],
                'generation_time_seconds': elapsed,
                'video_references': [
                    {'ref_id': rid, 'url': info['url'], 'author': info.get('author', '')}
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

            logger.info(f"YouTube Daily Report 生成完成: "
                        f"{total_video_count} 个视频, {len(renumbered_summaries)} 个频道, "
                        f"phase2=mechanical_concat, 总计={elapsed}s",
                        user_id=user_id, report_date=str(report_date))

            return {
                'success': True,
                'report_id': report_id,
                'share_token': share_token,
                'message': f'报告生成成功（{total_video_count} 个视频，'
                           f'{len(renumbered_summaries)} 个频道，{elapsed}s）',
                'video_count': total_video_count,
                'channel_count': len(renumbered_summaries),
                'generation_time_seconds': elapsed,
            }

        except Exception as e:
            error_msg = f"报告生成失败: {str(e)}"
            logger.error(error_msg, user_id=user_id, exception=str(e))
            self._update_report(report_id, status='failed', error_message=error_msg)
            return {'success': False, 'report_id': report_id, 'message': error_msg}

    async def generate_all_user_reports(self, report_date: date) -> Dict[str, Any]:
        """遍历所有启用了 YouTube 订阅的用户，逐一生成报告。"""
        user_ids = self._get_enabled_youtube_user_ids()
        if not user_ids:
            return {'success': True, 'message': '没有启用 YouTube 订阅的用户', 'reports': []}

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
        """获取用户订阅的所有启用 YouTube 频道 [(channel_identifier, channel_name)]。"""
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT channel_identifier, channel_name
                FROM subscription_list
                WHERE user_id = %s
                  AND channel_type = 'youtube'
                  AND enabled = true
                ORDER BY channel_identifier
            """, (user_id,))
            return [(row[0], row[1]) for row in cursor.fetchall()]
        finally:
            cursor.close()

    def _get_enabled_youtube_user_ids(self) -> List[int]:
        """获取所有有活跃 YouTube 订阅的用户 ID。"""
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT DISTINCT user_id
                FROM subscription_list
                WHERE channel_type = 'youtube'
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
        合并各频道的 ref_map 为全局 V-001…V-m；若摘要正文含 [V-xxx] 则替换为全局编号。

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
                global_ref = f"V-{offset + local_idx:03d}"
                rename_map[local_ref] = global_ref
                global_ref_map[global_ref] = info

            def _replace_ref(match, _rename=rename_map):
                old_ref = match.group(0).strip('[]')
                return f"[{_rename.get(old_ref, old_ref)}]"

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
        Phase 2: 机械拼接各频道 Phase 1 摘要（不调 LLM）。

        按视频数量降序排序频道，每个频道单独一节。
        """
        total_video_count = sum(s.get('video_count', 0) for s in active_summaries.values())
        n_channels = len(renumbered_summaries)

        sorted_channels = sorted(
            renumbered_summaries.keys(),
            key=lambda aid: active_summaries.get(aid, {}).get('video_count', 0),
            reverse=True,
        )

        sections = []
        for author_id in sorted_channels:
            display_name = author_display_map.get(author_id) or author_id
            content = (renumbered_summaries.get(author_id) or '').strip()
            sections.append(f"## {display_name}\n\n{content}")

        header = (
            f"# YouTube 每日摘要\n\n"
            f"共 {n_channels} 个频道 · {total_video_count} 个视频\n\n---\n\n"
        )
        return header + "\n\n---\n\n".join(sections)

    # ── Report persistence ─────────────────────────────────────────────────

    def _get_existing_report(self, user_id: int, report_date: date) -> Optional[Dict]:
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT id, status, report_content, metadata, updated_at
                FROM youtube_daily_reports
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
                INSERT INTO youtube_daily_reports (user_id, report_date, report_content, status)
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
                UPDATE youtube_daily_reports
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
                SELECT COUNT(*) FROM youtube_daily_reports WHERE user_id = %s
            """, (user_id,))
            total = cursor.fetchone()[0]

            cursor.execute("""
                SELECT id, report_date, status, error_message,
                       metadata, created_at, updated_at, share_token
                FROM youtube_daily_reports
                WHERE user_id = %s
                ORDER BY report_date DESC
                LIMIT %s OFFSET %s
            """, (user_id, limit, offset))
            columns = ['id', 'report_date', 'status', 'error_message',
                        'metadata', 'created_at', 'updated_at', 'share_token']
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
                FROM youtube_daily_reports
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
        """获取用户最新的已完成报告，附带 is_empty / is_partial 派生字段。"""
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT id, report_date, report_content, status,
                       error_message, metadata, created_at, updated_at,
                       oss_object_name, html_content, share_token
                FROM youtube_daily_reports
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

            meta = report.get('metadata') or {}
            report['is_empty'] = (meta.get('video_count', -1) == 0)
            report['is_partial'] = bool(meta.get('is_partial', False))

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
        if lines and lines[0].startswith('# ') and ('YouTube' in lines[0] or 'Daily Report' in lines[0]):
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

    def _inject_video_links(self, content: str, video_refs: List[Dict]) -> str:
        """将 [V-xxx] 引用替换为可点击的链接。"""
        import re
        if not content or not video_refs:
            return content

        ref_map = {}
        for ref in video_refs:
            ref_id = ref.get('ref_id', '')
            url = ref.get('url', '')
            if ref_id and url:
                ref_map[ref_id] = url

        pattern = r'\[V-(\d+)\]'

        def replace_ref(match):
            ref_num = match.group(1)
            ref_key = f"V-{ref_num}"
            if ref_key in ref_map:
                url = ref_map[ref_key]
                return f'<a href="{url}" target="_blank" class="video-ref">[V-{ref_num}]</a>'
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
        template = env.get_template("youtube_report_simple.html.j2")

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

        video_refs = metadata.get('video_references', [])
        report_content = self._inject_video_links(report_content, video_refs)

        content_html = self._markdown_to_html(report_content)

        html = template.render(
            report_date=report_date,
            channel_count=metadata.get('channel_count', 0),
            video_count=metadata.get('video_count', 0),
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
            oss_object_name = f"reports/YouTube/{user_id}/report_{date_str}.html"
            conn = self.db_manager.get_connection()
            cursor = conn.cursor()
            try:
                cursor.execute("""
                    UPDATE youtube_daily_reports
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
                            UPDATE youtube_daily_reports
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
                FROM youtube_daily_reports
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
