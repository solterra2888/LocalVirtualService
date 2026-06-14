# -*- coding: utf-8 -*-
"""
知识星球每日摘要服务 — 两阶段生成（Phase 1 全局，Phase 2 按用户）

Phase 1: 按星球(group = author_identifier) 各调一次 LLM，结果缓存于
         zhishixingqiu_group_daily_summaries（全局共享）。
Phase 2: 按用户机械拼接其订阅星球的 Phase 1 摘要，写入
         zhishixingqiu_user_reports（不调 LLM）。

与 Twitter Daily Report 的对应关系:
- Phase 1 ≈ TwitterAuthorSummaryService（全局按星球，无 user_id）
- Phase 2 ≈ 按用户组装（Twitter 会再调 LLM，本服务仅机械拼接）
"""

import asyncio
import json
import time
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from backend.database.db_manager import DatabaseManager
from backend.utils.database_config import get_database_config
from backend.utils.datetime_utils import (
    day_window_utc,
    ensure_utc,
    rolling_window_utc,
)
from backend.utils.logger import get_logger
from backend.services.summary_common import (
    BATCH_CONCURRENT,
    PER_ITEM_TIMEOUT_SEC,
    CHUNK_LLM_TIMEOUT_SEC,
    CHUNK_LLM_RETRY,
    ORPHAN_THRESHOLD_MINUTES,
    SUMMARY_LLM_READ_TIMEOUT_SEC,
    orphan_cutoff_dt,
)

logger = get_logger("zhishixingqiu_summary_service")

DEFAULT_PHASE1_TEMPLATE = "zhishixingqiu_group_phase1"
# deprecated: 旧单次 LLM 聚合模板，Phase 2 改为机械拼接后不再使用
DEPRECATED_DAILY_BRIEF_TEMPLATE = "zhishixingqiu_daily_brief"
DEFAULT_TIMEZONE = "Asia/Shanghai"
MAX_INPUT_CHARS = 80000
TOPIC_CONTENT_PREVIEW_CHARS = 2000
MAX_TOKENS_OUTPUT_PHASE1 = 2000

# 批量生成时的并发与超时控制（常量来自 summary_common）
BATCH_CONCURRENT_GROUPS = BATCH_CONCURRENT
PER_GROUP_TIMEOUT_SEC = PER_ITEM_TIMEOUT_SEC  # 600s


def resolve_zhishixingqiu_content_window(
    report_date: date,
    window_end: Optional[datetime] = None,
    window_hours: int = 24,
) -> Tuple[datetime, datetime]:
    """解析知识星球内容抓取窗口.

    - 若提供 window_end：返回 [window_end - window_hours, window_end)（aware UTC）
    - 否则：report_date 在北京时区下的自然日 [00:00, +1day)（aware UTC）
    """
    if window_end is not None:
        return rolling_window_utc(window_end, window_hours)
    return day_window_utc(report_date, DEFAULT_TIMEZONE)


class ZhishixingqiuSummaryService:
    """知识星球每日摘要生成服务（Phase 1 全局 + Phase 2 按用户）"""

    def __init__(self):
        db_params = get_database_config()
        self.db_manager = DatabaseManager(db_params)

    # ── Public API ─────────────────────────────────────────────────────────

    async def generate_all_group_summaries(
        self,
        report_date: date,
        window_end: Optional[datetime] = None,
        window_hours: int = 24,
        force: bool = False,
    ) -> Dict[str, Any]:
        """
        Phase 1 批量：为当日有话题的所有星球生成摘要（全局共享缓存）。

        Returns:
            {'success': bool, 'message': str, 'total': int, 'succeeded': int, ...}
        """
        start_time = time.time()
        self._reset_orphan_generating_records(report_date)
        window_start, resolved_end = resolve_zhishixingqiu_content_window(
            report_date, window_end, window_hours
        )
        logger.info(
            f"开始批量生成星球摘要: date={report_date}, "
            f"window=[{window_start.isoformat()}, {resolved_end.isoformat()})"
        )

        topics = self._fetch_topics(window_start, resolved_end)
        if not topics:
            return {
                "success": True,
                "message": "当日无知识星球更新",
                "total": 0,
                "succeeded": 0,
                "skipped": 0,
                "topic_count": 0,
                "group_count": 0,
                "group_ids": [],
            }

        topics_by_group, group_count = self._group_topics(topics)
        logger.info(f"Phase 1: 开始为 {group_count} 个星球生成摘要")

        sorted_groups = sorted(
            topics_by_group.items(),
            key=lambda x: x[1][0].get("author_name") or x[0],
        )
        semaphore = asyncio.Semaphore(BATCH_CONCURRENT_GROUPS)

        async def _process_one(idx: int, group_id: str, group_topics: list):
            group_name = group_topics[0].get("author_name") or group_id
            async with semaphore:
                try:
                    result = await asyncio.wait_for(
                        self.generate_group_summary(
                            group_id=group_id,
                            group_name=group_name,
                            topics=group_topics,
                            summary_date=report_date,
                            window_start=window_start,
                            window_end=resolved_end,
                            force=force,
                        ),
                        timeout=PER_GROUP_TIMEOUT_SEC,
                    )
                except asyncio.TimeoutError:
                    logger.warning(
                        f"[{idx}/{group_count}] {group_name} — TIMEOUT "
                        f"after {PER_GROUP_TIMEOUT_SEC}s，标记 failed 并继续"
                    )
                    try:
                        existing = self._get_existing_group_summary(group_id, report_date)
                        if existing:
                            self._update_group_summary(
                                existing["id"],
                                status="failed",
                                error_message=f"单星球超时（>{PER_GROUP_TIMEOUT_SEC}s）",
                            )
                    except Exception:
                        pass
                    result = {
                        "success": False,
                        "message": f"per-group timeout (>{PER_GROUP_TIMEOUT_SEC}s)",
                        "skipped": False,
                    }

            logger.info(
                f"[{idx}/{group_count}] {group_name}: "
                f"{'skipped' if result.get('skipped') else 'OK' if result.get('success') else 'FAILED'}"
            )
            return idx, group_id, result

        raw_results = await asyncio.gather(
            *[_process_one(idx, gid, gtopics) for idx, (gid, gtopics) in enumerate(sorted_groups, 1)]
        )

        succeeded = 0
        skipped = 0
        results = []
        for idx, group_id, result in raw_results:
            results.append({"group_id": group_id, **result})
            if result.get("skipped"):
                skipped += 1
            elif result.get("success"):
                succeeded += 1

        elapsed = round(time.time() - start_time, 2)
        return {
            "success": True,
            "message": (
                f"{group_count} 个星球: {succeeded} 成功, {skipped} 跳过, 耗时 {elapsed}s"
            ),
            "total": group_count,
            "succeeded": succeeded,
            "skipped": skipped,
            "topic_count": len(topics),
            "group_count": group_count,
            "group_ids": list(topics_by_group.keys()),
            "generation_time_seconds": elapsed,
            "results": results,
        }

    async def generate_user_report(
        self,
        user_id: int,
        report_date: date,
        window_end: Optional[datetime] = None,
        window_hours: int = 24,
        force: bool = False,
    ) -> Dict[str, Any]:
        """
        Phase 2 按用户：机械拼接该用户订阅星球的 Phase 1 摘要。

        Returns:
            {'success': bool, 'report_id': int | None, 'message': str, ...}
        """
        start_time = time.time()
        window_start, resolved_end = resolve_zhishixingqiu_content_window(
            report_date, window_end, window_hours
        )
        window_metadata = {
            "window_start": ensure_utc(window_start).isoformat(),
            "window_end": ensure_utc(resolved_end).isoformat(),
            "timezone": DEFAULT_TIMEZONE,
        }
        logger.info(
            f"开始生成知识星球用户报告: user_id={user_id}, date={report_date}, "
            f"window=[{window_metadata['window_start']}, {window_metadata['window_end']})"
        )

        if not force:
            existing = self._get_existing_user_report(user_id, report_date)
            if existing and existing.get("status") == "completed":
                logger.info(
                    f"用户报告已存在, 跳过: user_id={user_id}, date={report_date}"
                )
                return {
                    "success": True,
                    "report_id": existing["id"],
                    "message": "报告已存在",
                    "skipped": True,
                }

        report_id = self._upsert_user_report_status(user_id, report_date, "generating")

        try:
            user_groups = self._get_user_subscribed_groups(user_id)
            logger.info(
                f"[Step 1] 用户订阅星球: {len(user_groups)} 个", user_id=user_id
            )
            if not user_groups:
                self._update_user_report(
                    report_id,
                    status="completed",
                    content="当前没有启用的知识星球订阅。",
                    topic_count=0,
                    group_count=0,
                    metadata={"reason": "no_subscriptions", **window_metadata},
                )
                return {
                    "success": True,
                    "report_id": report_id,
                    "message": "没有启用的知识星球订阅",
                    "topic_count": 0,
                    "group_count": 0,
                }

            group_ids = [g[0] for g in user_groups]
            group_name_map = {gid: gname or gid for gid, gname in user_groups}

            cached = self.get_group_summaries_for_date(group_ids, report_date)
            logger.info(
                f"[Step 2] 缓存摘要命中: {len(cached)}/{len(group_ids)}",
                user_id=user_id,
            )

            missing_groups = [
                (gid, group_name_map[gid])
                for gid in group_ids
                if gid not in cached
            ]
            fallback_generated = 0
            if missing_groups:
                logger.info(
                    f"[Step 3] Fallback: 需即时生成 {len(missing_groups)} 个星球摘要",
                    user_id=user_id,
                )
                topics_by_group, _ = self._group_topics(
                    self._fetch_topics_for_groups(
                        [g[0] for g in missing_groups], window_start, resolved_end
                    )
                )
                for group_id, group_name in missing_groups:
                    group_topics = topics_by_group.get(group_id, [])
                    result = await self.generate_group_summary(
                        group_id=group_id,
                        group_name=group_name,
                        topics=group_topics,
                        summary_date=report_date,
                        window_start=window_start,
                        window_end=resolved_end,
                        force=force,
                    )
                    if result.get("success"):
                        fallback_generated += 1
                        fresh = self.get_group_summaries_for_date([group_id], report_date)
                        cached.update(fresh)
            else:
                logger.info("[Step 3] 所有星球摘要均已缓存，无需 fallback", user_id=user_id)

            active_summaries = {
                gid: data
                for gid, data in cached.items()
                if gid in group_ids
                and data.get("topic_count", 0) > 0
                and (data.get("summary_content") or "").strip()
            }

            if not active_summaries:
                # 区分"真无内容"与"上游 Phase A 失败"
                failed_groups = self.get_failed_group_ids_for_date(group_ids, report_date)
                is_partial = len(failed_groups) > 0
                if is_partial:
                    content_msg = (
                        f"当天有 {len(failed_groups)} 个星球摘要生成失败，无有效话题数据可组装报告。\n"
                        f"失败星球: {', '.join(failed_groups)}"
                    )
                    logger.warning(
                        f"[ZhiXingQiu Report] 无有效摘要，{len(failed_groups)} 个星球失败",
                        user_id=user_id, failed_groups=failed_groups,
                    )
                else:
                    content_msg = "当天订阅的知识星球无可用摘要。"
                self._update_user_report(
                    report_id,
                    status="completed",
                    content=content_msg,
                    topic_count=0,
                    group_count=0,
                    metadata={
                        "reason": "no_active_summaries",
                        "is_partial": is_partial,
                        "failed_groups": failed_groups,
                        "subscribed_group_count": len(group_ids),
                        **window_metadata,
                    },
                )
                return {
                    "success": True,
                    "report_id": report_id,
                    "message": content_msg,
                    "topic_count": 0,
                    "group_count": 0,
                    "is_partial": is_partial,
                }

            logger.info(
                f"[Step 4] Phase 2: 机械拼接 {len(active_summaries)} 个星球摘要",
                user_id=user_id,
            )
            report_content = self._assemble_final_summary(active_summaries)

            total_topic_count = sum(
                data.get("topic_count", 0) for data in active_summaries.values()
            )
            group_stats = {
                gid: data.get("topic_count", 0) for gid, data in active_summaries.items()
            }
            elapsed = round(time.time() - start_time, 2)
            metadata = {
                "generation_time_seconds": elapsed,
                "topic_count": total_topic_count,
                "group_count": len(active_summaries),
                "subscribed_group_count": len(group_ids),
                "group_topic_counts": group_stats,
                "phase2_mode": "mechanical_concat",
                "used_cached_summaries": len(cached) - fallback_generated,
                "fallback_generated": fallback_generated,
                **window_metadata,
            }

            self._update_user_report(
                report_id,
                status="completed",
                content=report_content,
                topic_count=total_topic_count,
                group_count=len(active_summaries),
                metadata=metadata,
            )

            logger.info(
                f"知识星球用户报告完成: user_id={user_id}, "
                f"{len(active_summaries)} 星球, {total_topic_count} 话题, 耗时 {elapsed}s"
            )

            return {
                "success": True,
                "report_id": report_id,
                "message": (
                    f"报告生成成功（{len(active_summaries)} 星球, "
                    f"{total_topic_count} 话题, {elapsed}s）"
                ),
                "topic_count": total_topic_count,
                "group_count": len(active_summaries),
                "generation_time_seconds": elapsed,
                "skipped": False,
            }

        except Exception as e:
            error_msg = f"知识星球用户报告生成失败: {str(e)}"
            logger.error(error_msg, user_id=user_id, exception=str(e))
            self._update_user_report(
                report_id, status="failed", error_message=error_msg
            )
            return {
                "success": False,
                "report_id": report_id,
                "message": error_msg,
                "skipped": False,
            }

    async def generate_group_summary(
        self,
        group_id: str,
        group_name: str,
        topics: List[Dict[str, Any]],
        summary_date: date,
        window_start: datetime,
        window_end: datetime,
        force: bool = False,
    ) -> Dict[str, Any]:
        """
        Phase 1: 为单个星球生成当日话题摘要。

        Returns:
            {'success': bool, 'summary_id': int | None, 'message': str, ...}
        """
        start_time = time.time()
        logger.info(
            f"[Phase 1] 星球 {group_name} ({group_id}): "
            f"{len(topics)} 条话题, date={summary_date}"
        )

        if not force:
            existing = self._get_existing_group_summary(group_id, summary_date)
            if existing and existing["status"] == "completed":
                tpl_updated = self._get_template_updated_at(DEFAULT_PHASE1_TEMPLATE)
                summary_updated = existing.get("updated_at")
                if tpl_updated and summary_updated:
                    summary_updated = ensure_utc(summary_updated)
                    tpl_updated = ensure_utc(tpl_updated)
                    if tpl_updated > (summary_updated + timedelta(hours=1)):
                        logger.info(
                            f"Phase1 模板已更新, 自动重新生成: {group_name}"
                        )
                    else:
                        logger.info(f"星球摘要已存在, 跳过: {group_name}")
                        return {
                            "success": True,
                            "summary_id": existing["id"],
                            "message": "摘要已存在",
                            "topic_count": existing.get("topic_count", 0),
                            "skipped": True,
                        }
                else:
                    logger.info(f"星球摘要已存在, 跳过: {group_name}")
                    return {
                        "success": True,
                        "summary_id": existing["id"],
                        "message": "摘要已存在",
                        "topic_count": existing.get("topic_count", 0),
                        "skipped": True,
                    }

        summary_id = self._upsert_group_summary_status(
            group_id, group_name, summary_date, "generating"
        )

        try:
            if not topics:
                self._update_group_summary(
                    summary_id,
                    status="completed",
                    content="当天无话题。",
                    topic_count=0,
                )
                return {
                    "success": True,
                    "summary_id": summary_id,
                    "message": "当天无话题",
                    "topic_count": 0,
                    "skipped": False,
                }

            topics_text, truncated = self._format_group_topics(topics)
            if not topics_text.strip():
                self._update_group_summary(
                    summary_id,
                    status="completed",
                    content="当天话题无可用正文。",
                    topic_count=len(topics),
                )
                return {
                    "success": True,
                    "summary_id": summary_id,
                    "message": "无可用正文",
                    "topic_count": len(topics),
                    "skipped": False,
                }

            summary_text, is_degraded = await self._run_phase1(
                topics_text=topics_text,
                group_name=group_name,
                report_date=summary_date,
                topic_count=len(topics),
            )

            elapsed = round(time.time() - start_time, 2)
            metadata = {
                "generation_time_seconds": elapsed,
                "template_code": DEFAULT_PHASE1_TEMPLATE,
                "input_char_count": len(topics_text),
                "input_truncated": truncated,
                "degraded": is_degraded,
                "window_start": ensure_utc(window_start).isoformat(),
                "window_end": ensure_utc(window_end).isoformat(),
            }

            self._update_group_summary(
                summary_id,
                status="completed",
                content=summary_text,
                topic_count=len(topics),
                metadata=metadata,
            )

            logger.info(
                f"[Phase 1] {group_name} 完成: {len(topics)} 话题, "
                f"{len(summary_text)} chars, 耗时 {elapsed}s"
            )

            return {
                "success": True,
                "summary_id": summary_id,
                "message": f"摘要生成成功（{len(topics)} 话题，{elapsed}s）",
                "topic_count": len(topics),
                "generation_time_seconds": elapsed,
                "skipped": False,
            }

        except Exception as e:
            error_msg = f"星球摘要生成失败: {str(e)}"
            logger.error(error_msg, group_id=group_id, exception=str(e))
            self._update_group_summary(
                summary_id, status="failed", error_message=error_msg
            )
            return {
                "success": False,
                "summary_id": summary_id,
                "message": error_msg,
                "skipped": False,
            }

    # ── Data fetching ──────────────────────────────────────────────────────

    def _fetch_topics(
        self, window_start: datetime, window_end: datetime
    ) -> List[Dict[str, Any]]:
        """查询 subscription_content 中当日知识星球话题。"""
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT author_identifier, author_name, content_id, title, content,
                       url, published_at, metadata
                FROM subscription_content
                WHERE channel_type = 'zhishixingqiu'
                  AND published_at >= %s AND published_at < %s
                  AND content IS NOT NULL AND length(trim(content)) > 0
                ORDER BY author_identifier, published_at ASC
                """,
                (window_start, window_end),
            )
            columns = [
                "author_identifier",
                "author_name",
                "content_id",
                "title",
                "content",
                "url",
                "published_at",
                "metadata",
            ]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
        finally:
            cursor.close()

    def _fetch_topics_for_groups(
        self,
        group_ids: List[str],
        window_start: datetime,
        window_end: datetime,
    ) -> List[Dict[str, Any]]:
        """查询指定星球列表在窗口内的知识星球话题。"""
        if not group_ids:
            return []
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT author_identifier, author_name, content_id, title, content,
                       url, published_at, metadata
                FROM subscription_content
                WHERE channel_type = 'zhishixingqiu'
                  AND author_identifier = ANY(%s)
                  AND published_at >= %s AND published_at < %s
                  AND content IS NOT NULL AND length(trim(content)) > 0
                ORDER BY author_identifier, published_at ASC
                """,
                (group_ids, window_start, window_end),
            )
            columns = [
                "author_identifier",
                "author_name",
                "content_id",
                "title",
                "content",
                "url",
                "published_at",
                "metadata",
            ]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
        finally:
            cursor.close()

    def _get_user_subscribed_groups(
        self, user_id: int
    ) -> List[Tuple[str, Optional[str]]]:
        """获取用户订阅的所有启用知识星球 [(channel_identifier, channel_name)]。"""
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT channel_identifier, channel_name
                FROM subscription_list
                WHERE user_id = %s
                  AND channel_type = 'zhishixingqiu'
                  AND enabled = true
                ORDER BY channel_identifier
                """,
                (user_id,),
            )
            return [(row[0], row[1]) for row in cursor.fetchall()]
        finally:
            cursor.close()

    def _get_enabled_zhishixingqiu_user_ids(self) -> List[int]:
        """获取所有有启用知识星球订阅的用户 ID。"""
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT DISTINCT user_id
                FROM subscription_list
                WHERE channel_type = 'zhishixingqiu'
                  AND enabled = true
                ORDER BY user_id
                """
            )
            return [row[0] for row in cursor.fetchall()]
        finally:
            cursor.close()

    def _group_topics(
        self, topics: List[Dict[str, Any]]
    ) -> Tuple[Dict[str, List[Dict[str, Any]]], int]:
        """按星球 group_id 分组。"""
        grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for topic in topics:
            group_id = topic.get("author_identifier") or "unknown"
            grouped[group_id].append(topic)
        return dict(grouped), len(grouped)

    @staticmethod
    def _parse_metadata(metadata: Any) -> Dict[str, Any]:
        if isinstance(metadata, dict):
            return metadata
        if isinstance(metadata, str):
            try:
                return json.loads(metadata)
            except Exception:
                return {}
        return {}

    def _format_group_topics(
        self, topics: List[Dict[str, Any]]
    ) -> Tuple[str, bool]:
        """将单个星球的话题格式化为 Phase 1 LLM 输入文本。"""
        lines: List[str] = []
        truncated = False

        for item in topics:
            title = (item.get("title") or "").strip()
            content = (item.get("content") or "").strip()
            if not content:
                continue

            metadata = self._parse_metadata(item.get("metadata"))
            post_author = metadata.get("post_author") or "未知"
            published = item.get("published_at")
            time_str = ""
            if published:
                time_str = ensure_utc(published).strftime("%Y-%m-%d %H:%M UTC")

            preview = content
            if len(preview) > TOPIC_CONTENT_PREVIEW_CHARS:
                preview = preview[:TOPIC_CONTENT_PREVIEW_CHARS] + "...(已截断)"

            header = "- "
            if title:
                header += f"【{title}】"
            header += (
                f"（{post_author}，{time_str}）" if time_str else f"（{post_author}）"
            )
            url = item.get("url") or ""
            lines.append(header)
            lines.append(f"  {preview}")
            if url:
                lines.append(f"  URL: {url}")

        full_text = "\n".join(lines)
        if len(full_text) > MAX_INPUT_CHARS:
            logger.warning(
                f"星球话题文本过长 ({len(full_text)} chars), "
                f"截断至 {MAX_INPUT_CHARS} chars"
            )
            full_text = full_text[:MAX_INPUT_CHARS] + "\n\n...(话题过多，已截断)"
            truncated = True

        return full_text, truncated

    def _assemble_final_summary(
        self, group_summaries: Dict[str, Dict[str, Any]]
    ) -> str:
        """
        Phase 2: 机械拼接各星球 Phase 1 摘要（不调 LLM）。

        Args:
            group_summaries: {group_id: {group_name, summary_content, topic_count, ...}}
        """
        sections: List[str] = []
        sorted_items = sorted(
            group_summaries.items(),
            key=lambda x: x[1].get("group_name") or x[0],
        )
        for group_id, data in sorted_items:
            group_name = data.get("group_name") or group_id
            content = (data.get("summary_content") or "").strip()
            topic_count = data.get("topic_count", 0)
            sections.append(f"## {group_name}\n\n{content}")

        header = (
            f"# 知识星球每日摘要\n\n"
            f"共 {len(sorted_items)} 个星球\n\n---\n\n"
        )
        return header + "\n\n---\n\n".join(sections)

    # ── Phase 1 LLM ──────────────────────────────────────────────────────

    async def _run_phase1(
        self,
        topics_text: str,
        group_name: str,
        report_date: date,
        topic_count: int,
    ) -> Tuple[str, bool]:
        """执行 Phase 1 LLM 调用，带超时与重试。

        Returns:
            (summary_text, degraded) — degraded=True 表示 LLM 失败，返回降级摘要。
        """
        variables = {
            "group_name": group_name,
            "report_date": str(report_date),
            "topic_count": str(topic_count),
            "topics_text": topics_text,
        }
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
                return result, False
            except asyncio.TimeoutError:
                if attempt < CHUNK_LLM_RETRY:
                    logger.warning(
                        f"[Phase 1] {group_name} LLM 超时 "
                        f"(attempt {attempt+1}/{CHUNK_LLM_RETRY+1}), retrying"
                    )
                    continue
                logger.warning(f"[Phase 1] {group_name} 全部超时, 降级为话题标题列表")
            except Exception as exc:
                if attempt < CHUNK_LLM_RETRY:
                    logger.warning(
                        f"[Phase 1] {group_name} 错误 (attempt {attempt+1}): {exc}, retrying"
                    )
                    continue
                logger.warning(f"[Phase 1] {group_name} 全部失败: {exc}, 降级")

        return f"[摘要降级：LLM 生成超时，共 {topic_count} 条话题]", True

    async def _call_llm(
        self,
        template_code: str,
        variables: Dict[str, Any],
        max_tokens: int = 2000,
    ) -> str:
        """通过 DB 模板调用 LLM。"""
        from backend.core.llm_framework import llm_service

        var_preview = {
            k: (f"{str(v)[:200]}..." if len(str(v)) > 200 else str(v))
            for k, v in variables.items()
        }
        logger.info(
            f"[LLM 调用] 模板={template_code}, max_tokens={max_tokens}, "
            f"变量预览={var_preview}"
        )

        llm_start = time.time()
        response = await llm_service.chat_completion_with_template(
            template_code=template_code,
            variables=variables,
            max_tokens=max_tokens,
            timeout=SUMMARY_LLM_READ_TIMEOUT_SEC,
        )
        llm_elapsed = round(time.time() - llm_start, 2)

        content_preview = response.content[:300] if response.content else "(empty)"
        logger.info(
            f"[LLM 响应] 模板={template_code}, 耗时={llm_elapsed}s, "
            f"响应长度={len(response.content)}字符, 预览: {content_preview}"
        )
        return response.content

    def _get_template_updated_at(self, template_code: str) -> Optional[datetime]:
        try:
            from backend.services.llm_prompt_template_service import (
                LLMPromptTemplateService,
            )

            svc = LLMPromptTemplateService()
            tpl = svc.get_template_by_code(template_code)
            if tpl and tpl.get("updated_at"):
                return ensure_utc(datetime.fromisoformat(tpl["updated_at"]))
        except Exception as e:
            logger.warning(f"获取模板更新时间失败: {template_code}, {e}")
        return None

    # ── Group summary persistence (Phase 1) ──────────────────────────────

    def _get_existing_group_summary(
        self, group_id: str, summary_date: date
    ) -> Optional[Dict]:
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT id, status, summary_content, topic_count, updated_at
                FROM zhishixingqiu_group_daily_summaries
                WHERE group_id = %s AND summary_date = %s
                """,
                (group_id, summary_date),
            )
            row = cursor.fetchone()
            if row:
                return {
                    "id": row[0],
                    "status": row[1],
                    "summary_content": row[2],
                    "topic_count": row[3],
                    "updated_at": row[4],
                }
            return None
        finally:
            cursor.close()

    def _upsert_group_summary_status(
        self,
        group_id: str,
        group_name: str,
        summary_date: date,
        status: str,
    ) -> int:
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                INSERT INTO zhishixingqiu_group_daily_summaries
                    (group_id, group_name, summary_date, status)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (group_id, summary_date) DO UPDATE
                    SET status = EXCLUDED.status,
                        group_name = EXCLUDED.group_name,
                        error_message = NULL,
                        updated_at = CURRENT_TIMESTAMP
                RETURNING id
                """,
                (group_id, group_name, summary_date, status),
            )
            conn.commit()
            return cursor.fetchone()[0]
        finally:
            cursor.close()

    def _update_group_summary(
        self,
        summary_id: int,
        *,
        status: str,
        content: Optional[str] = None,
        topic_count: Optional[int] = None,
        metadata: Optional[Dict] = None,
        error_message: Optional[str] = None,
    ):
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            from psycopg2.extras import Json

            cursor.execute(
                """
                UPDATE zhishixingqiu_group_daily_summaries
                SET status = %s,
                    summary_content = COALESCE(%s, summary_content),
                    topic_count = COALESCE(%s, topic_count),
                    metadata = COALESCE(%s, metadata),
                    error_message = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
                """,
                (
                    status,
                    content,
                    topic_count,
                    Json(metadata) if metadata is not None else None,
                    error_message,
                    summary_id,
                ),
            )
            conn.commit()
        finally:
            cursor.close()

    def get_group_summaries_for_date(
        self, group_ids: List[str], summary_date: date
    ) -> Dict[str, Dict[str, Any]]:
        """批量获取指定星球列表在某天的已完成（completed）摘要。"""
        if not group_ids:
            return {}
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT group_id, group_name, summary_content, topic_count
                FROM zhishixingqiu_group_daily_summaries
                WHERE group_id = ANY(%s)
                  AND summary_date = %s
                  AND status = 'completed'
                """,
                (group_ids, summary_date),
            )
            result = {}
            for row in cursor.fetchall():
                result[row[0]] = {
                    "group_id": row[0],
                    "group_name": row[1],
                    "summary_content": row[2],
                    "topic_count": row[3] or 0,
                }
            return result
        finally:
            cursor.close()

    def get_failed_group_ids_for_date(
        self, group_ids: List[str], summary_date: date
    ) -> List[str]:
        """返回指定星球列表中当天 Phase A 状态为 failed 的 group_id 列表。"""
        if not group_ids:
            return []
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT group_id
                FROM zhishixingqiu_group_daily_summaries
                WHERE group_id = ANY(%s)
                  AND summary_date = %s
                  AND status = 'failed'
                """,
                (group_ids, summary_date),
            )
            return [row[0] for row in cursor.fetchall()]
        finally:
            cursor.close()

    def _reset_orphan_generating_records(self, target_date: date) -> int:
        """将当天超过 ORPHAN_THRESHOLD_MINUTES 仍处于 'generating' 的记录重置为 'failed'。"""
        cutoff = orphan_cutoff_dt()
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                UPDATE zhishixingqiu_group_daily_summaries
                SET status = 'failed',
                    error_message = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE summary_date = %s
                  AND status = 'generating'
                  AND updated_at < %s
                """,
                (
                    f"orphan reset: stuck generating > {ORPHAN_THRESHOLD_MINUTES}min",
                    target_date,
                    cutoff,
                ),
            )
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

    # ── User report persistence (Phase 2 output) ───────────────────────────

    def _get_existing_user_report(
        self, user_id: int, report_date: date
    ) -> Optional[Dict]:
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT id, status, report_content, topic_count, group_count,
                       metadata, updated_at
                FROM zhishixingqiu_user_reports
                WHERE user_id = %s AND report_date = %s
                """,
                (user_id, report_date),
            )
            row = cursor.fetchone()
            if row:
                return {
                    "id": row[0],
                    "status": row[1],
                    "report_content": row[2],
                    "topic_count": row[3],
                    "group_count": row[4],
                    "metadata": row[5],
                    "updated_at": row[6],
                }
            return None
        finally:
            cursor.close()

    def _upsert_user_report_status(
        self, user_id: int, report_date: date, status: str
    ) -> int:
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                INSERT INTO zhishixingqiu_user_reports
                    (user_id, report_date, report_content, status)
                VALUES (%s, %s, '', %s)
                ON CONFLICT (user_id, report_date) DO UPDATE
                    SET status = EXCLUDED.status,
                        error_message = NULL,
                        updated_at = CURRENT_TIMESTAMP
                RETURNING id
                """,
                (user_id, report_date, status),
            )
            conn.commit()
            return cursor.fetchone()[0]
        finally:
            cursor.close()

    def _update_user_report(
        self,
        report_id: int,
        *,
        status: str,
        content: Optional[str] = None,
        topic_count: Optional[int] = None,
        group_count: Optional[int] = None,
        metadata: Optional[Dict] = None,
        error_message: Optional[str] = None,
    ):
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            from psycopg2.extras import Json

            cursor.execute(
                """
                UPDATE zhishixingqiu_user_reports
                SET status = %s,
                    report_content = COALESCE(%s, report_content),
                    topic_count = COALESCE(%s, topic_count),
                    group_count = COALESCE(%s, group_count),
                    metadata = COALESCE(%s, metadata),
                    error_message = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
                """,
                (
                    status,
                    content,
                    topic_count,
                    group_count,
                    Json(metadata) if metadata is not None else None,
                    error_message,
                    report_id,
                ),
            )
            conn.commit()
        finally:
            cursor.close()

    # ── Query helpers (供 API 层调用) ──────────────────────────────────────

    def get_user_reports(
        self, user_id: int, limit: int = 30, offset: int = 0
    ) -> Tuple[List[Dict[str, Any]], int]:
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT COUNT(*) FROM zhishixingqiu_user_reports WHERE user_id = %s
                """,
                (user_id,),
            )
            total = cursor.fetchone()[0]

            cursor.execute(
                """
                SELECT id, report_date, topic_count, group_count, status,
                       report_content, metadata, error_message,
                       created_at, updated_at
                FROM zhishixingqiu_user_reports
                WHERE user_id = %s
                ORDER BY report_date DESC
                LIMIT %s OFFSET %s
                """,
                (user_id, limit, offset),
            )
            reports = [self._row_to_dict(row) for row in cursor.fetchall()]
            return reports, total
        finally:
            cursor.close()

    def get_latest_report(self, user_id: int) -> Optional[Dict[str, Any]]:
        """获取用户最新已完成报告，附带 is_empty / is_partial 派生字段。"""
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT id, report_date, topic_count, group_count, status,
                       report_content, metadata, error_message,
                       created_at, updated_at
                FROM zhishixingqiu_user_reports
                WHERE user_id = %s AND status = 'completed'
                ORDER BY report_date DESC
                LIMIT 1
                """,
                (user_id,),
            )
            row = cursor.fetchone()
            if not row:
                return None
            report = self._row_to_dict(row)
            meta = report.get("metadata") or {}
            report["is_empty"] = (report.get("topic_count") or 0) == 0
            report["is_partial"] = bool(meta.get("is_partial", False))
            return report
        finally:
            cursor.close()

    def get_report_by_date(
        self, user_id: int, report_date: date
    ) -> Optional[Dict[str, Any]]:
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT id, report_date, topic_count, group_count, status,
                       report_content, metadata, error_message,
                       created_at, updated_at
                FROM zhishixingqiu_user_reports
                WHERE user_id = %s AND report_date = %s
                """,
                (user_id, report_date),
            )
            row = cursor.fetchone()
            return self._row_to_dict(row) if row else None
        finally:
            cursor.close()

    @staticmethod
    def _row_to_dict(row: tuple) -> Dict[str, Any]:
        return {
            "id": row[0],
            "report_date": str(row[1]),
            "topic_count": row[2],
            "group_count": row[3],
            "status": row[4],
            "report_content": row[5],
            "metadata": row[6] or {},
            "error_message": row[7],
            "created_at": row[8].isoformat() if row[8] else None,
            "updated_at": row[9].isoformat() if row[9] else None,
        }
