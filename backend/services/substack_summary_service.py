# -*- coding: utf-8 -*-
"""
Substack 每日摘要服务 — 两阶段生成（Phase 1 全局，Phase 2 按用户）

Phase 1: 按 publication(author_identifier) 各调一次 LLM，输入 = 该刊当日 文章(post) + 动态(note)，
         结果缓存于 substack_publication_daily_summaries（全局共享）。
Phase 2: 按用户机械拼接其订阅 publication 的 Phase 1 摘要，写入 substack_user_reports（不调 LLM）。

与 zhishixingqiu_summary_service 同构；差异：
  - 内容来自 subscription_content WHERE channel_type='substack'，按 metadata.content_kind 分 post/note
  - 模板 substack_publication_phase1，变量含 post_count / note_count
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
    CHUNK_LLM_RETRY,
    CHUNK_LLM_TIMEOUT_SEC,
    ORPHAN_THRESHOLD_MINUTES,
    PER_ITEM_TIMEOUT_SEC,
    SUMMARY_LLM_READ_TIMEOUT_SEC,
    orphan_cutoff_dt,
)

logger = get_logger("substack_summary_service")

DEFAULT_PHASE1_TEMPLATE = "substack_publication_phase1"
DEFAULT_TIMEZONE = "Asia/Shanghai"
MAX_INPUT_CHARS = 80000
POST_CONTENT_PREVIEW_CHARS = 2000
NOTE_CONTENT_PREVIEW_CHARS = 600
MAX_TOKENS_OUTPUT_PHASE1 = 2000

BATCH_CONCURRENT_PUBS = BATCH_CONCURRENT
PER_PUB_TIMEOUT_SEC = PER_ITEM_TIMEOUT_SEC  # 600s


def resolve_substack_content_window(
    report_date: date,
    window_end: Optional[datetime] = None,
    window_hours: int = 24,
) -> Tuple[datetime, datetime]:
    if window_end is not None:
        return rolling_window_utc(window_end, window_hours)
    return day_window_utc(report_date, DEFAULT_TIMEZONE)


class SubstackSummaryService:
    """Substack 每日摘要生成服务（Phase 1 全局 + Phase 2 按用户）"""

    def __init__(self):
        db_params = get_database_config()
        self.db_manager = DatabaseManager(db_params)

    # ── Public API ─────────────────────────────────────────────────────────

    async def generate_all_publication_summaries(
        self,
        report_date: date,
        window_end: Optional[datetime] = None,
        window_hours: int = 24,
        force: bool = False,
    ) -> Dict[str, Any]:
        """Phase 1 批量：为当日有内容的所有 publication 生成摘要。"""
        start_time = time.time()
        self._reset_orphan_generating_records(report_date)
        window_start, resolved_end = resolve_substack_content_window(
            report_date, window_end, window_hours
        )
        logger.info(
            f"开始批量生成 Substack publication 摘要: date={report_date}, "
            f"window=[{window_start.isoformat()}, {resolved_end.isoformat()})"
        )

        items = self._fetch_content(window_start, resolved_end)
        if not items:
            return {
                "success": True,
                "message": "当日无 Substack 更新",
                "total": 0, "succeeded": 0, "skipped": 0,
                "post_count": 0, "note_count": 0, "publication_count": 0,
                "publication_urls": [],
            }

        grouped, pub_count, total_posts, total_notes = self._group_content(items)
        logger.info(f"Phase 1: 开始为 {pub_count} 个 publication 生成摘要")

        sorted_pubs = sorted(
            grouped.items(),
            key=lambda x: x[1][0].get("author_name") or x[0],
        )
        semaphore = asyncio.Semaphore(BATCH_CONCURRENT_PUBS)

        async def _process_one(idx: int, pub_url: str, pub_items: list):
            pub_name = pub_items[0].get("author_name") or pub_url
            async with semaphore:
                try:
                    result = await asyncio.wait_for(
                        self.generate_publication_summary(
                            publication_url=pub_url,
                            publication_name=pub_name,
                            items=pub_items,
                            summary_date=report_date,
                            window_start=window_start,
                            window_end=resolved_end,
                            force=force,
                        ),
                        timeout=PER_PUB_TIMEOUT_SEC,
                    )
                except asyncio.TimeoutError:
                    logger.warning(
                        f"[{idx}/{pub_count}] {pub_name} — TIMEOUT "
                        f"after {PER_PUB_TIMEOUT_SEC}s，标记 failed 并继续"
                    )
                    try:
                        existing = self._get_existing_publication_summary(pub_url, report_date)
                        if existing:
                            self._update_publication_summary(
                                existing["id"], status="failed",
                                error_message=f"单 publication 超时（>{PER_PUB_TIMEOUT_SEC}s）",
                            )
                    except Exception:
                        pass
                    result = {
                        "success": False,
                        "message": f"per-pub timeout (>{PER_PUB_TIMEOUT_SEC}s)",
                        "skipped": False,
                    }
            logger.info(
                f"[{idx}/{pub_count}] {pub_name}: "
                f"{'skipped' if result.get('skipped') else 'OK' if result.get('success') else 'FAILED'}"
            )
            return idx, pub_url, result

        raw_results = await asyncio.gather(
            *[_process_one(idx, purl, pitems) for idx, (purl, pitems) in enumerate(sorted_pubs, 1)]
        )

        succeeded = 0
        skipped = 0
        results = []
        for idx, pub_url, result in raw_results:
            results.append({"publication_url": pub_url, **result})
            if result.get("skipped"):
                skipped += 1
            elif result.get("success"):
                succeeded += 1

        elapsed = round(time.time() - start_time, 2)
        return {
            "success": True,
            "message": f"{pub_count} 个 publication: {succeeded} 成功, {skipped} 跳过, 耗时 {elapsed}s",
            "total": pub_count,
            "succeeded": succeeded,
            "skipped": skipped,
            "post_count": total_posts,
            "note_count": total_notes,
            "publication_count": pub_count,
            "publication_urls": list(grouped.keys()),
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
        """Phase 2 按用户：机械拼接其订阅 publication 的 Phase 1 摘要。"""
        start_time = time.time()
        window_start, resolved_end = resolve_substack_content_window(
            report_date, window_end, window_hours
        )
        window_metadata = {
            "window_start": ensure_utc(window_start).isoformat(),
            "window_end": ensure_utc(resolved_end).isoformat(),
            "timezone": DEFAULT_TIMEZONE,
        }
        logger.info(
            f"开始生成 Substack 用户报告: user_id={user_id}, date={report_date}"
        )

        if not force:
            existing = self._get_existing_user_report(user_id, report_date)
            if existing and existing.get("status") == "completed":
                if not self._is_user_report_stale(existing, user_id, report_date):
                    return {
                        "success": True,
                        "report_id": existing["id"],
                        "message": "报告已存在",
                        "skipped": True,
                    }

        report_id = self._upsert_user_report_status(user_id, report_date, "generating")

        try:
            user_pubs = self._get_user_subscribed_publications(user_id)
            if not user_pubs:
                self._update_user_report(
                    report_id, status="completed",
                    content="当前没有启用的 Substack 订阅。",
                    post_count=0, note_count=0, publication_count=0,
                    metadata={"reason": "no_subscriptions", **window_metadata},
                )
                return {
                    "success": True, "report_id": report_id,
                    "message": "没有启用的 Substack 订阅",
                    "post_count": 0, "note_count": 0, "publication_count": 0,
                }

            pub_ids = [g[0] for g in user_pubs]
            pub_name_map = {purl: pname or purl for purl, pname in user_pubs}

            cached = self.get_publication_summaries_for_date(pub_ids, report_date)

            if force:
                missing = [(purl, pub_name_map[purl]) for purl in pub_ids]
            else:
                missing = [(purl, pub_name_map[purl]) for purl in pub_ids if purl not in cached]
            fallback_generated = 0
            if missing:
                items_by_pub, _, _, _ = self._group_content(
                    self._fetch_content_for_publications([m[0] for m in missing], window_start, resolved_end)
                )
                for pub_url, pub_name in missing:
                    pub_items = items_by_pub.get(pub_url, [])
                    result = await self.generate_publication_summary(
                        publication_url=pub_url, publication_name=pub_name,
                        items=pub_items, summary_date=report_date,
                        window_start=window_start, window_end=resolved_end, force=force,
                    )
                    if result.get("success"):
                        fallback_generated += 1
                        cached.update(self.get_publication_summaries_for_date([pub_url], report_date))

            active = {
                purl: data
                for purl, data in cached.items()
                if purl in pub_ids
                and ((data.get("post_count", 0) + data.get("note_count", 0)) > 0)
                and (data.get("summary_content") or "").strip()
            }

            if not active:
                failed_pubs = self.get_failed_publication_urls_for_date(pub_ids, report_date)
                is_partial = len(failed_pubs) > 0
                content_msg = (
                    f"当天有 {len(failed_pubs)} 个 publication 摘要生成失败，无有效内容可组装报告。\n"
                    f"失败: {', '.join(failed_pubs)}"
                    if is_partial else "当天订阅的 Substack 无可用摘要。"
                )
                self._update_user_report(
                    report_id, status="completed", content=content_msg,
                    post_count=0, note_count=0, publication_count=0,
                    metadata={
                        "reason": "no_active_summaries", "is_partial": is_partial,
                        "failed_publications": failed_pubs,
                        "subscribed_publication_count": len(pub_ids), **window_metadata,
                    },
                )
                return {
                    "success": True, "report_id": report_id, "message": content_msg,
                    "post_count": 0, "note_count": 0, "publication_count": 0,
                    "is_partial": is_partial,
                }

            report_content = self._assemble_final_summary(active)
            total_posts = sum(d.get("post_count", 0) for d in active.values())
            total_notes = sum(d.get("note_count", 0) for d in active.values())
            pub_stats = {
                purl: {"posts": d.get("post_count", 0), "notes": d.get("note_count", 0)}
                for purl, d in active.items()
            }
            elapsed = round(time.time() - start_time, 2)
            metadata = {
                "generation_time_seconds": elapsed,
                "post_count": total_posts,
                "note_count": total_notes,
                "publication_count": len(active),
                "subscribed_publication_count": len(pub_ids),
                "publication_post_counts": {k: v["posts"] for k, v in pub_stats.items()},
                "publication_note_counts": {k: v["notes"] for k, v in pub_stats.items()},
                "phase2_mode": "mechanical_concat",
                "used_cached_summaries": len(cached) - fallback_generated,
                "fallback_generated": fallback_generated,
                **window_metadata,
            }

            self._update_user_report(
                report_id, status="completed", content=report_content,
                post_count=total_posts, note_count=total_notes,
                publication_count=len(active), metadata=metadata,
            )
            return {
                "success": True, "report_id": report_id,
                "message": f"报告生成成功（{len(active)} publication, {total_posts} 文章, {total_notes} 动态, {elapsed}s）",
                "post_count": total_posts, "note_count": total_notes,
                "publication_count": len(active),
                "generation_time_seconds": elapsed, "skipped": False,
            }

        except Exception as e:
            error_msg = f"Substack 用户报告生成失败: {str(e)}"
            logger.error(error_msg, user_id=user_id, exception=str(e))
            self._update_user_report(report_id, status="failed", error_message=error_msg)
            return {"success": False, "report_id": report_id, "message": error_msg, "skipped": False}

    async def generate_publication_summary(
        self,
        publication_url: str,
        publication_name: str,
        items: List[Dict[str, Any]],
        summary_date: date,
        window_start: datetime,
        window_end: datetime,
        force: bool = False,
    ) -> Dict[str, Any]:
        """Phase 1: 为单个 publication 生成当日摘要（文章 + 动态）。"""
        start_time = time.time()
        posts = [it for it in items if self._content_kind(it) == "post"]
        notes = [it for it in items if self._content_kind(it) == "note"]
        logger.info(
            f"[Phase 1] publication {publication_name} ({publication_url}): "
            f"{len(posts)} 文章 + {len(notes)} 动态, date={summary_date}"
        )

        if not force:
            existing = self._get_existing_publication_summary(publication_url, summary_date)
            if existing and existing["status"] == "completed":
                logger.info(f"publication 摘要已存在, 跳过: {publication_name}")
                return {
                    "success": True, "summary_id": existing["id"], "message": "摘要已存在",
                    "post_count": existing.get("post_count", 0),
                    "note_count": existing.get("note_count", 0), "skipped": True,
                }

        summary_id = self._upsert_publication_summary_status(
            publication_url, publication_name, summary_date, "generating"
        )

        try:
            if not items:
                self._update_publication_summary(
                    summary_id, status="completed", content="当天无内容。",
                    post_count=0, note_count=0,
                )
                return {"success": True, "summary_id": summary_id, "message": "当天无内容",
                        "post_count": 0, "note_count": 0, "skipped": False}

            text, truncated = self._format_publication_content(posts, notes)
            if not text.strip():
                self._update_publication_summary(
                    summary_id, status="completed", content="当天内容无可用正文。",
                    post_count=len(posts), note_count=len(notes),
                )
                return {"success": True, "summary_id": summary_id, "message": "无可用正文",
                        "post_count": len(posts), "note_count": len(notes), "skipped": False}

            summary_text, is_degraded = await self._run_phase1(
                posts_text=text, publication_name=publication_name,
                report_date=summary_date, post_count=len(posts), note_count=len(notes),
            )

            elapsed = round(time.time() - start_time, 2)
            metadata = {
                "generation_time_seconds": elapsed,
                "template_code": DEFAULT_PHASE1_TEMPLATE,
                "input_char_count": len(text),
                "input_truncated": truncated,
                "degraded": is_degraded,
                "window_start": ensure_utc(window_start).isoformat(),
                "window_end": ensure_utc(window_end).isoformat(),
            }
            self._update_publication_summary(
                summary_id, status="completed", content=summary_text,
                post_count=len(posts), note_count=len(notes), metadata=metadata,
            )
            logger.info(
                f"[Phase 1] {publication_name} 完成: {len(posts)} 文章 + {len(notes)} 动态, "
                f"{len(summary_text)} chars, {elapsed}s"
            )
            return {
                "success": True, "summary_id": summary_id,
                "message": f"摘要生成成功（{len(posts)} 文章 + {len(notes)} 动态，{elapsed}s）",
                "post_count": len(posts), "note_count": len(notes),
                "generation_time_seconds": elapsed, "skipped": False,
            }

        except Exception as e:
            error_msg = f"publication 摘要生成失败: {str(e)}"
            logger.error(error_msg, publication_url=publication_url, exception=str(e))
            self._update_publication_summary(summary_id, status="failed", error_message=error_msg)
            return {"success": False, "summary_id": summary_id, "message": error_msg, "skipped": False}

    # ── Data fetching ──────────────────────────────────────────────────────

    def _fetch_content(self, window_start: datetime, window_end: datetime) -> List[Dict[str, Any]]:
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT author_identifier, author_name, content_id, title, content,
                       url, published_at, metadata
                FROM subscription_content
                WHERE channel_type = 'substack'
                  AND published_at >= %s AND published_at < %s
                  AND content IS NOT NULL AND length(trim(content)) > 0
                ORDER BY author_identifier, published_at ASC
                """,
                (window_start, window_end),
            )
            columns = ["author_identifier", "author_name", "content_id", "title",
                       "content", "url", "published_at", "metadata"]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
        finally:
            cursor.close()

    def _fetch_content_for_publications(
        self, pub_urls: List[str], window_start: datetime, window_end: datetime
    ) -> List[Dict[str, Any]]:
        if not pub_urls:
            return []
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT author_identifier, author_name, content_id, title, content,
                       url, published_at, metadata
                FROM subscription_content
                WHERE channel_type = 'substack'
                  AND author_identifier = ANY(%s)
                  AND published_at >= %s AND published_at < %s
                  AND content IS NOT NULL AND length(trim(content)) > 0
                ORDER BY author_identifier, published_at ASC
                """,
                (pub_urls, window_start, window_end),
            )
            columns = ["author_identifier", "author_name", "content_id", "title",
                       "content", "url", "published_at", "metadata"]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
        finally:
            cursor.close()

    def _get_user_subscribed_publications(
        self, user_id: int
    ) -> List[Tuple[str, Optional[str]]]:
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT channel_identifier, channel_name
                FROM subscription_list
                WHERE user_id = %s AND channel_type = 'substack' AND enabled = true
                ORDER BY channel_identifier
                """,
                (user_id,),
            )
            return [(row[0], row[1]) for row in cursor.fetchall()]
        finally:
            cursor.close()

    def _get_enabled_substack_user_ids(self) -> List[int]:
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT DISTINCT user_id FROM subscription_list
                WHERE channel_type = 'substack' AND enabled = true
                ORDER BY user_id
                """
            )
            return [row[0] for row in cursor.fetchall()]
        finally:
            cursor.close()

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

    @staticmethod
    def _content_kind(item: Dict[str, Any]) -> str:
        return SubstackSummaryService._parse_metadata(item.get("metadata")).get("content_kind", "post")

    def _group_content(
        self, items: List[Dict[str, Any]]
    ) -> Tuple[Dict[str, List[Dict[str, Any]]], int, int, int]:
        grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        total_posts = 0
        total_notes = 0
        for it in items:
            pub_url = it.get("author_identifier") or "unknown"
            grouped[pub_url].append(it)
            if self._content_kind(it) == "note":
                total_notes += 1
            else:
                total_posts += 1
        return dict(grouped), len(grouped), total_posts, total_notes

    def _format_publication_content(
        self, posts: List[Dict[str, Any]], notes: List[Dict[str, Any]]
    ) -> Tuple[str, bool]:
        lines: List[str] = []
        truncated = False

        if posts:
            lines.append(f"=== 文章（{len(posts)} 篇）===")
            for item in posts:
                title = (item.get("title") or "").strip()
                content = (item.get("content") or "").strip()
                if not content:
                    continue
                preview = content[:POST_CONTENT_PREVIEW_CHARS]
                if len(content) > POST_CONTENT_PREVIEW_CHARS:
                    preview += "...(已截断)"
                url = item.get("url") or ""
                lines.append(f"- 【{title}】" if title else "- ")
                lines.append(f"  {preview}")
                if url:
                    lines.append(f"  URL: {url}")

        if notes:
            lines.append(f"\n=== 动态（{len(notes)} 条）===")
            for item in notes:
                content = (item.get("content") or "").strip()
                if not content:
                    continue
                meta = self._parse_metadata(item.get("metadata"))
                handle = meta.get("author_handle") or ""
                preview = content[:NOTE_CONTENT_PREVIEW_CHARS]
                if len(content) > NOTE_CONTENT_PREVIEW_CHARS:
                    preview += "...(已截断)"
                lines.append(f"- @{handle}: {preview}" if handle else f"- {preview}")

        full_text = "\n".join(lines)
        if len(full_text) > MAX_INPUT_CHARS:
            logger.warning(f"publication 文本过长 ({len(full_text)} chars), 截断")
            full_text = full_text[:MAX_INPUT_CHARS] + "\n\n...(内容过多，已截断)"
            truncated = True
        return full_text, truncated

    def _assemble_final_summary(
        self, pub_summaries: Dict[str, Dict[str, Any]]
    ) -> str:
        sections: List[str] = []
        sorted_items = sorted(
            pub_summaries.items(),
            key=lambda x: x[1].get("publication_name") or x[0],
        )
        for pub_url, data in sorted_items:
            pub_name = data.get("publication_name") or pub_url
            content = (data.get("summary_content") or "").strip()
            sections.append(f"## {pub_name}\n\n{content}")

        header = f"# Substack 每日摘要\n\n共 {len(sorted_items)} 个 publication\n\n---\n\n"
        return header + "\n\n---\n\n".join(sections)

    # ── Phase 1 LLM ──────────────────────────────────────────────────────

    async def _run_phase1(
        self, posts_text: str, publication_name: str, report_date: date,
        post_count: int, note_count: int,
    ) -> Tuple[str, bool]:
        variables = {
            "publication_name": publication_name,
            "report_date": str(report_date),
            "post_count": str(post_count),
            "note_count": str(note_count),
            "posts_text": posts_text,
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
                    logger.warning(f"[Phase 1] {publication_name} LLM 超时, retry")
                    continue
                logger.warning(f"[Phase 1] {publication_name} 全部超时, 降级")
            except Exception as exc:
                if attempt < CHUNK_LLM_RETRY:
                    logger.warning(f"[Phase 1] {publication_name} 错误: {exc}, retry")
                    continue
                logger.warning(f"[Phase 1] {publication_name} 全部失败: {exc}, 降级")

        return f"[摘要降级：LLM 生成超时，共 {post_count} 文章 + {note_count} 动态]", True

    async def _call_llm(
        self, template_code: str, variables: Dict[str, Any], max_tokens: int = 2000
    ) -> str:
        from backend.core.llm_framework import llm_service

        var_preview = {
            k: (f"{str(v)[:200]}..." if len(str(v)) > 200 else str(v))
            for k, v in variables.items()
        }
        logger.info(f"[LLM 调用] 模板={template_code}, 变量预览={var_preview}")

        response = await llm_service.chat_completion_with_template(
            template_code=template_code, variables=variables,
            max_tokens=max_tokens, timeout=SUMMARY_LLM_READ_TIMEOUT_SEC,
        )
        return response.content

    def _get_template_updated_at(self, template_code: str) -> Optional[datetime]:
        try:
            from backend.services.llm_prompt_template_service import LLMPromptTemplateService
            svc = LLMPromptTemplateService()
            tpl = svc.get_template_by_code(template_code)
            if tpl and tpl.get("updated_at"):
                return ensure_utc(datetime.fromisoformat(tpl["updated_at"]))
        except Exception as e:
            logger.warning(f"获取模板更新时间失败: {template_code}, {e}")
        return None

    # ── Publication summary persistence (Phase 1) ────────────────────────

    def _get_existing_publication_summary(
        self, publication_url: str, summary_date: date
    ) -> Optional[Dict]:
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT id, status, summary_content, post_count, note_count, updated_at
                FROM substack_publication_daily_summaries
                WHERE publication_url = %s AND summary_date = %s
                """,
                (publication_url, summary_date),
            )
            row = cursor.fetchone()
            if row:
                return {"id": row[0], "status": row[1], "summary_content": row[2],
                        "post_count": row[3], "note_count": row[4], "updated_at": row[5]}
            return None
        finally:
            cursor.close()

    def _upsert_publication_summary_status(
        self, publication_url: str, publication_name: str, summary_date: date, status: str
    ) -> int:
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                INSERT INTO substack_publication_daily_summaries
                    (publication_url, publication_name, summary_date, status)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (publication_url, summary_date) DO UPDATE
                    SET status = EXCLUDED.status,
                        publication_name = EXCLUDED.publication_name,
                        error_message = NULL,
                        updated_at = CURRENT_TIMESTAMP
                RETURNING id
                """,
                (publication_url, publication_name, summary_date, status),
            )
            conn.commit()
            return cursor.fetchone()[0]
        finally:
            cursor.close()

    def _update_publication_summary(
        self, summary_id: int, *, status: str, content: Optional[str] = None,
        post_count: Optional[int] = None, note_count: Optional[int] = None,
        metadata: Optional[Dict] = None, error_message: Optional[str] = None,
    ):
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            from psycopg2.extras import Json
            cursor.execute(
                """
                UPDATE substack_publication_daily_summaries
                SET status = %s,
                    summary_content = COALESCE(%s, summary_content),
                    post_count = COALESCE(%s, post_count),
                    note_count = COALESCE(%s, note_count),
                    metadata = COALESCE(%s, metadata),
                    error_message = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
                """,
                (status, content, post_count, note_count,
                 Json(metadata) if metadata is not None else None, error_message, summary_id),
            )
            conn.commit()
        finally:
            cursor.close()

    def get_publication_summaries_for_date(
        self, pub_urls: List[str], summary_date: date
    ) -> Dict[str, Dict[str, Any]]:
        if not pub_urls:
            return {}
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT publication_url, publication_name, summary_content, post_count, note_count
                FROM substack_publication_daily_summaries
                WHERE publication_url = ANY(%s) AND summary_date = %s AND status = 'completed'
                """,
                (pub_urls, summary_date),
            )
            result = {}
            for row in cursor.fetchall():
                result[row[0]] = {
                    "publication_url": row[0], "publication_name": row[1],
                    "summary_content": row[2], "post_count": row[3] or 0, "note_count": row[4] or 0,
                }
            return result
        finally:
            cursor.close()

    def get_failed_publication_urls_for_date(
        self, pub_urls: List[str], summary_date: date
    ) -> List[str]:
        if not pub_urls:
            return []
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT publication_url FROM substack_publication_daily_summaries
                WHERE publication_url = ANY(%s) AND summary_date = %s AND status = 'failed'
                """,
                (pub_urls, summary_date),
            )
            return [row[0] for row in cursor.fetchall()]
        finally:
            cursor.close()

    def _reset_orphan_generating_records(self, target_date: date) -> int:
        cutoff = orphan_cutoff_dt()
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                UPDATE substack_publication_daily_summaries
                SET status = 'failed', error_message = %s, updated_at = CURRENT_TIMESTAMP
                WHERE summary_date = %s AND status = 'generating' AND updated_at < %s
                """,
                (f"orphan reset: stuck generating > {ORPHAN_THRESHOLD_MINUTES}min", target_date, cutoff),
            )
            count = cursor.rowcount
            conn.commit()
            return count
        finally:
            cursor.close()

    # ── User report persistence (Phase 2 output) ─────────────────────────

    def _is_user_report_stale(
        self, existing: Dict[str, Any], user_id: int, report_date: date
    ) -> bool:
        pub_urls = [g[0] for g in self._get_user_subscribed_publications(user_id)]
        if not pub_urls:
            return False
        max_updated = self._get_max_publication_summary_updated_at(pub_urls, report_date)
        if max_updated is None:
            return False
        report_updated = existing.get("updated_at")
        if report_updated is None:
            return True
        return ensure_utc(max_updated) > ensure_utc(report_updated)

    def _get_max_publication_summary_updated_at(
        self, pub_urls: List[str], summary_date: date
    ) -> Optional[datetime]:
        if not pub_urls:
            return None
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT MAX(updated_at) FROM substack_publication_daily_summaries
                WHERE publication_url = ANY(%s) AND summary_date = %s AND status = 'completed'
                """,
                (pub_urls, summary_date),
            )
            row = cursor.fetchone()
            return row[0] if row else None
        finally:
            cursor.close()

    def _get_existing_user_report(self, user_id: int, report_date: date) -> Optional[Dict]:
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT id, status, report_content, post_count, note_count,
                       publication_count, metadata, updated_at
                FROM substack_user_reports
                WHERE user_id = %s AND report_date = %s
                """,
                (user_id, report_date),
            )
            row = cursor.fetchone()
            if row:
                return {"id": row[0], "status": row[1], "report_content": row[2],
                        "post_count": row[3], "note_count": row[4], "publication_count": row[5],
                        "metadata": row[6], "updated_at": row[7]}
            return None
        finally:
            cursor.close()

    def _upsert_user_report_status(self, user_id: int, report_date: date, status: str) -> int:
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                INSERT INTO substack_user_reports (user_id, report_date, report_content, status)
                VALUES (%s, %s, '', %s)
                ON CONFLICT (user_id, report_date) DO UPDATE
                    SET status = EXCLUDED.status, error_message = NULL, updated_at = CURRENT_TIMESTAMP
                RETURNING id
                """,
                (user_id, report_date, status),
            )
            conn.commit()
            return cursor.fetchone()[0]
        finally:
            cursor.close()

    def _update_user_report(
        self, report_id: int, *, status: str, content: Optional[str] = None,
        post_count: Optional[int] = None, note_count: Optional[int] = None,
        publication_count: Optional[int] = None, metadata: Optional[Dict] = None,
        error_message: Optional[str] = None,
    ):
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            from psycopg2.extras import Json
            cursor.execute(
                """
                UPDATE substack_user_reports
                SET status = %s,
                    report_content = COALESCE(%s, report_content),
                    post_count = COALESCE(%s, post_count),
                    note_count = COALESCE(%s, note_count),
                    publication_count = COALESCE(%s, publication_count),
                    metadata = COALESCE(%s, metadata),
                    error_message = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
                """,
                (status, content, post_count, note_count, publication_count,
                 Json(metadata) if metadata is not None else None, error_message, report_id),
            )
            conn.commit()
        finally:
            cursor.close()

    # ── Query helpers (供 API 层调用) ──────────────────────────────────────

    def get_user_reports(self, user_id: int, limit: int = 30, offset: int = 0) -> Tuple[List[Dict[str, Any]], int]:
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT COUNT(*) FROM substack_user_reports WHERE user_id = %s", (user_id,))
            total = cursor.fetchone()[0]
            cursor.execute(
                """
                SELECT id, report_date, post_count, note_count, publication_count, status,
                       report_content, metadata, error_message, created_at, updated_at
                FROM substack_user_reports
                WHERE user_id = %s
                ORDER BY report_date DESC
                LIMIT %s OFFSET %s
                """,
                (user_id, limit, offset),
            )
            return [self._row_to_dict(row) for row in cursor.fetchall()], total
        finally:
            cursor.close()

    def get_latest_report(self, user_id: int) -> Optional[Dict[str, Any]]:
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT id, report_date, post_count, note_count, publication_count, status,
                       report_content, metadata, error_message, created_at, updated_at
                FROM substack_user_reports
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
            report["is_empty"] = ((report.get("post_count") or 0) + (report.get("note_count") or 0)) == 0
            report["is_partial"] = bool(meta.get("is_partial", False))
            return report
        finally:
            cursor.close()

    def get_report_by_date(self, user_id: int, report_date: date) -> Optional[Dict[str, Any]]:
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT id, report_date, post_count, note_count, publication_count, status,
                       report_content, metadata, error_message, created_at, updated_at
                FROM substack_user_reports
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
            "post_count": row[2],
            "note_count": row[3],
            "publication_count": row[4],
            "status": row[5],
            "report_content": row[6],
            "metadata": row[7] or {},
            "error_message": row[8],
            "created_at": row[9].isoformat() if row[9] else None,
            "updated_at": row[10].isoformat() if row[10] else None,
        }
