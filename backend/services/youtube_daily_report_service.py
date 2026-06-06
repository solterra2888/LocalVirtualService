# -*- coding: utf-8 -*-
"""
YouTube Daily Report 服务 — 定期为每个用户生成 YouTube 数据总结报告

两阶段解耦架构：
  阶段 A（全局）: YouTubeAuthorSummaryService 按频道预生成摘要，同一频道同一天只调 LLM 一次
  阶段 B（本服务）: 读取预计算的频道摘要 → 全局引用重编号 → Phase 2 综合报告

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
from backend.core.llm_framework import llm_service
from backend.utils.datetime_utils import ensure_utc, resolve_report_content_window

logger = get_logger("youtube_daily_report_service")

DEFAULT_PHASE2_TEMPLATE = "youtube_daily_phase2_default"
MAX_TOKENS_OUTPUT_PHASE2 = 4000

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
    """YouTube Daily Report 生成服务（阶段 B：组装用户报告）"""

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
        window_end: Optional[datetime] = None,
        window_hours: int = 24,
    ) -> Dict[str, Any]:
        """
        为指定用户生成指定日期的 YouTube Daily Report。
        优先从 youtube_author_daily_summaries 读取预计算摘要（阶段 A），
        缺失时自动 fallback 到即时生成。
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

        # 解析用户 Phase 2 模板偏好
        from backend.services.template_preference_service import TemplatePreferenceService
        pref_service = TemplatePreferenceService()
        phase2_template = pref_service.resolve_template_code(user_id, 'youtube_phase2')
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
            # 1) 获取用户订阅的 YouTube 频道列表
            user_authors = self._get_user_subscribed_authors(user_id)
            logger.info(f"[Step 1/6] 用户订阅频道: {len(user_authors)} 个",
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
            logger.info(f"[Step 2/6] 缓存摘要命中: {len(cached_summaries)}/{len(author_ids)}",
                        user_id=user_id)

            # 3) 补生成缺失的摘要（fallback）
            missing_authors = [
                (aid, aname) for aid, aname in user_authors
                if aid not in cached_summaries
            ]
            if missing_authors:
                logger.info(f"[Step 3/6] Fallback: 需即时生成 {len(missing_authors)} 个频道摘要",
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
                logger.info(f"[Step 3/6] 所有频道摘要均已缓存，无需 fallback",
                            user_id=user_id)

            # 过滤掉无视频数据的频道
            active_summaries = {
                k: v for k, v in cached_summaries.items()
                if v.get('video_count', 0) > 0
            }

            if not active_summaries:
                self._update_report(report_id, status='completed',
                                    content='当天无视频数据。',
                                    metadata={'video_count': 0, 'channel_count': 0, **window_metadata})
                return {
                    'success': True,
                    'report_id': report_id,
                    'message': '当天无视频数据',
                    'video_count': 0,
                }

            # 4) 全局引用重编号 + 组装
            renumbered_summaries, global_ref_map, author_display_map = self._renumber_and_assemble(
                active_summaries
            )
            total_video_count = sum(s.get('video_count', 0) for s in active_summaries.values())
            top_videos_text = self._merge_top_videos(active_summaries, global_ref_map)
            logger.info(f"[Step 4/6] 组装完成: {len(renumbered_summaries)} 个频道, "
                        f"{total_video_count} 个视频, {len(global_ref_map)} 引用",
                        user_id=user_id)

            # 5) Phase 2 — 综合报告
            logger.info(f"[Step 5/6] Phase 2 开始 — 综合分析报告生成...",
                        user_id=user_id)
            phase2_start = time.time()
            report_content, _ = await self._phase2_synthesize(
                renumbered_summaries, global_ref_map, top_videos_text,
                report_date, total_video_count,
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
                'video_count': total_video_count,
                'channel_count': len(renumbered_summaries),
                'top_channels': sorted(
                    active_summaries.keys(),
                    key=lambda a: active_summaries[a].get('video_count', 0),
                    reverse=True
                )[:5],
                'generation_time_seconds': elapsed,
                'video_references': [
                    {'ref_id': rid, 'url': info['url'], 'author': info['author']}
                    for rid, info in global_ref_map.items()
                ],
                'used_cached_summaries': len(cached_summaries) - len(missing_authors),
                'fallback_generated': len(missing_authors),
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

            logger.info(f"[Step 6/6] YouTube Daily Report 生成完成: "
                        f"{total_video_count} 个视频, {len(renumbered_summaries)} 个频道, "
                        f"Phase2={phase2_elapsed}s, 总计={elapsed}s",
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
        """获取所有有活跃 YouTube 订阅且未关闭自动生成的用户 ID。"""
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT DISTINCT sl.user_id
                FROM subscription_list sl
                LEFT JOIN user_daily_report_auto_settings udras
                    ON udras.user_id = sl.user_id
                WHERE sl.channel_type = 'youtube'
                  AND sl.enabled = true
                  AND COALESCE(udras.youtube_auto_enabled, false) = true
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

    def _merge_top_videos(
        self, author_summaries: Dict[str, Dict], global_ref_map: Dict
    ) -> str:
        """合并各频道的 top_videos，按综合排序取 Top 5。"""
        all_top = []
        for author_id, data in author_summaries.items():
            for v in (data.get('top_videos') or []):
                local_ref = v.get('ref_id', '')
                global_ref = None
                for gref, ginfo in global_ref_map.items():
                    if ginfo.get('url') == v.get('url') and ginfo.get('url'):
                        global_ref = gref
                        break
                all_top.append({
                    **v,
                    'global_ref': global_ref or local_ref,
                })

        all_top.sort(
            key=lambda t: (t.get('views', 0) + t.get('engagement', 0)),
            reverse=True,
        )
        lines = []
        for v in all_top[:5]:
            ref = v.get('global_ref', '')
            author = v.get('author_name') or v.get('author', '')
            views = v.get('views', 0)
            engagement = v.get('engagement', 0)
            title = v.get('title', '')
            url = v.get('url', '')
            lines.append(f"[{ref}] {author} | 观看:{views} 互动:{engagement} | 「{title}」 | {url}")
        return '\n'.join(lines)

    # ── Phase 2: Synthesis ─────────────────────────────────────────────────

    async def _phase2_synthesize(
        self,
        author_summaries: Dict[str, str],
        ref_map: Dict,
        top_videos: str,
        report_date: date,
        video_count: int,
        template_code: str = DEFAULT_PHASE2_TEMPLATE,
    ) -> Tuple[str, Dict]:
        """Phase 2: 合成最终报告。返回 (report_markdown, usage_info)。"""
        all_summaries = '\n\n---\n\n'.join(
            f"{summary}" for author, summary in author_summaries.items()
        )

        variables = {
            'report_date': str(report_date),
            'channel_count': str(len(author_summaries)),
            'video_count': str(video_count),
            'channel_summaries': all_summaries,
            'top_videos': top_videos,
            'report_language': 'zh',
            'focus_topics_instruction': '',
            'custom_instructions': '',
        }

        logger.info(f"[Phase 2] 输入: {len(author_summaries)} 个频道摘要 "
                    f"({len(all_summaries)} 字符), top_videos={len(top_videos)} 字符, "
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
        """在 Phase 2 主文下方追加附录：按频道顺序编号 S1…SN。"""
        if not author_summaries:
            return report_body
        parts: List[str] = [report_body.rstrip(), "\n\n---\n\n"]
        n = len(author_summaries)
        parts.append(f"## 附录：各频道摘要（S1–S{n}）\n\n")
        for idx, author in enumerate(author_summaries.keys(), start=1):
            display = author_display_map.get(author, author)
            summary = (author_summaries.get(author) or "").strip()
            parts.append(f"### S{idx} — {display}\n\n")
            parts.append(summary)
            parts.append("\n\n")
        return "".join(parts).rstrip() + "\n"

    # ── LLM call helper ───────────────────────────────────────────────────

    async def _call_llm(
        self,
        template_code: str,
        variables: Dict[str, Any],
        max_tokens: int = 4000,
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
                       metadata, created_at, updated_at
                FROM youtube_daily_reports
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
        """获取用户最新的已完成报告。"""
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

            # Add generation time display (New York and Beijing)
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
        相比手写正则更稳定，支持完整 Markdown 语法。
        """
        import mistune
        import mistune.plugins.table as table_plugin

        if not markdown_content:
            return ''

        # 预处理：删除首行主标题（避免与 Header 日期重复）
        lines = markdown_content.split('\n')
        if lines and lines[0].startswith('# ') and ('YouTube' in lines[0] or 'Daily Report' in lines[0]):
            lines = lines[1:]  # 删除第一行标题
            # 同时删除标题后的空行（如果有）
            if lines and lines[0].strip() == '':
                lines = lines[1:]
        markdown_content = '\n'.join(lines)

        # 创建 mistune 解析器，启用表格插件
        renderer = mistune.HTMLRenderer()
        markdown_parser = mistune.Markdown(renderer, plugins=[table_plugin.table])

        html = markdown_parser(markdown_content)
        return html

    def _inject_video_links(self, content: str, video_refs: List[Dict]) -> str:
        """将 [V-xxx] 引用替换为可点击的链接。"""
        import re
        if not content or not video_refs:
            return content

        # 构建 ref_id -> url 映射
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
        """使用 Jinja2 模板渲染 HTML。简化版：直接渲染 Markdown 内容，不做结构解析。"""
        from jinja2 import Environment, FileSystemLoader, select_autoescape
        import os

        template_dir = os.path.join(os.path.dirname(__file__), "..", "templates")
        env = Environment(
            loader=FileSystemLoader(os.path.abspath(template_dir)),
            autoescape=select_autoescape(["html"]),
        )
        # 使用简化模板
        template = env.get_template("youtube_report_simple.html.j2")

        report_date = metadata.get('report_date', '')
        if isinstance(report_date, date):
            report_date = report_date.strftime("%B %d, %Y")
        elif isinstance(report_date, str):
            try:
                report_date = datetime.fromisoformat(report_date).strftime("%B %d, %Y")
            except:
                pass

        # 计算生成时间（不同时区）
        gen_time_utc = datetime.now(timezone.utc)
        time_display = _build_generation_time_display(gen_time_utc)

        # 1. 先替换引用链接 [V-xxx]（在 mistune 渲染之前）
        video_refs = metadata.get('video_references', [])
        report_content = self._inject_video_links(report_content, video_refs)

        # 2. 然后 Markdown 转 HTML（使用 mistune）
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
        简化版：直接渲染 Markdown，不做复杂解析。

        Returns:
            {'success': bool, 'oss_object_name': str, 'share_token': str, 'html_content': str}
        """
        from uuid import uuid4
        from backend.services.oss_service import OSSService

        try:
            # 确保 report_date 在 metadata 中
            metadata['report_date'] = report_date

            # 1. 渲染 HTML（直接 Markdown → HTML）
            html_content = self._render_html(report_content, metadata)

            # 2. 上传到 OSS
            oss_service = OSSService()
            date_str = report_date.strftime("%Y%m%d")
            oss_object_name = f"reports/YouTube/{user_id}/report_{date_str}.html"

            upload_result = oss_service.upload_bytes(
                object_name=oss_object_name,
                data=html_content.encode("utf-8"),
                content_type="text/html; charset=utf-8",
            )

            if not upload_result.get('success'):
                logger.error(f"OSS 上传失败: {upload_result.get('error')}")
                return {'success': False, 'error': upload_result.get('error')}

            # 3. 生成 share_token
            share_token = uuid4().hex

            # 4. 更新数据库
            conn = self.db_manager.get_connection()
            cursor = conn.cursor()
            try:
                cursor.execute("""
                    UPDATE youtube_daily_reports
                    SET oss_object_name = %s,
                        html_content = %s,
                        share_token = %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                """, (oss_object_name, html_content, share_token, report_id))
                conn.commit()
            finally:
                cursor.close()

            logger.info(f"HTML 报告渲染并上传成功: report_id={report_id}, "
                        f"oss_path={oss_object_name}, share_token={share_token[:8]}…")

            return {
                'success': True,
                'oss_object_name': oss_object_name,
                'share_token': share_token,
                'html_content': html_content,
            }

        except Exception as e:
            logger.error(f"HTML 渲染/上传失败: {str(e)}")
            return {'success': False, 'error': str(e)}
            parsed_data = self._parse_markdown_structure(report_content, metadata)

            # Ensure report_date is in metadata
            metadata['report_date'] = report_date

            # 2. 渲染 HTML
            html_content = self._render_html(parsed_data, metadata)

            # 3. 上传到 OSS
            oss_service = OSSService()
            date_str = report_date.strftime("%Y%m%d")
            oss_object_name = f"reports/YouTube/{user_id}/report_{date_str}.html"

            upload_result = oss_service.upload_bytes(
                object_name=oss_object_name,
                data=html_content.encode("utf-8"),
                content_type="text/html; charset=utf-8",
            )

            if not upload_result.get('success'):
                logger.error(f"OSS 上传失败: {upload_result.get('error')}")
                return {'success': False, 'error': upload_result.get('error')}

            # 4. 生成 share_token
            share_token = uuid4().hex

            # 5. 更新数据库
            conn = self.db_manager.get_connection()
            cursor = conn.cursor()
            try:
                cursor.execute("""
                    UPDATE youtube_daily_reports
                    SET oss_object_name = %s,
                        html_content = %s,
                        share_token = %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                """, (oss_object_name, html_content, share_token, report_id))
                conn.commit()
            finally:
                cursor.close()

            logger.info(f"HTML 报告渲染并上传成功: report_id={report_id}, "
                        f"oss_path={oss_object_name}, share_token={share_token[:8]}…")

            return {
                'success': True,
                'oss_object_name': oss_object_name,
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

            # Primary: 直接从数据库读取（无需 OSS 调用）
            if html_content:
                return html_content.encode("utf-8")

            # Fallback: 从 OSS 获取
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
