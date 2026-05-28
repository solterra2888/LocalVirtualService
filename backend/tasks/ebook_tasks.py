# -*- coding: utf-8 -*-
"""
Ebook 章节级 Map-Reduce 摘要任务。

当 upload_and_process_document_task 检测到电子书（content 超过 EBOOK_MIN_CHARS），
会在 Vector RAG 向量化完成后（延迟 EBOOK_MAP_REDUCE_DELAY 秒）触发此任务。

流程：
  Map:    按 chapter 分组 document_chunks → 每章调一次 LLM（qwen-turbo）生成 chapter_summary
  Reduce: 把所有 chapter_summary 拼接后再调一次 LLM → 生成全书概览 final_report
  写回:   processing_stats.final_report（前端 AI Analysis 面板可展示）

成本估算（以《The Nvidia Way》246 页 / 1110 chunks / 46 章为例）：
  Map    ~0.08 RMB  （46 章 × qwen-turbo）
  Reduce ~0.005 RMB
  合计   ~0.085 RMB（比一次性全量塞入 qwen3-max 便宜约 6x，且无上下文溢出）
"""

from __future__ import annotations

import asyncio
import os
from typing import Dict, Any, List, Optional, Tuple

from backend.celery_app import app
from backend.utils.database_config import get_database_config
from backend.utils.logger import get_logger

logger = get_logger("ebook_tasks")

# ── 环境变量默认值 ────────────────────────────────────────────────────────────
def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        return default


# 单次 map 调用最多喂多少字符（避免 qwen-turbo 128K token 爆）
_MAP_MAX_CHARS = _env_int("EBOOK_MAP_MAX_CHARS", 8000)
# reduce 阶段拼接 chapter summaries 的最大字符数（截断整体 reduce 输入）
_REDUCE_MAX_CHARS = _env_int("EBOOK_REDUCE_MAX_CHARS", 32000)
# Map 阶段使用的模板 code（系统模板，不需要用户选）
_MAP_TEMPLATE_CODE = os.getenv("EBOOK_MAP_TEMPLATE_CODE", "ebook_chapter_summary_v1")
# Reduce 阶段使用的模板 code
_REDUCE_TEMPLATE_CODE = os.getenv("EBOOK_REDUCE_TEMPLATE_CODE", "ebook_book_overview_v1")


# ─────────────────────────────────────────────────────────────────────────────
@app.task(
    bind=True,
    name="ebook_map_reduce",
    queue="ai_processing",
    acks_late=True,
    max_retries=1,
    default_retry_delay=120,
    soft_time_limit=3600,
    time_limit=3900,
)
def ebook_map_reduce_task(
    self,
    file_id: int,
    execution_id: Optional[int],
    username: Optional[str],
) -> Dict[str, Any]:
    """
    对已完成向量化的 ebook 执行章节级 Map-Reduce 摘要。

    Args:
        file_id:      file_processing_details.id
        execution_id: processing_stats.id（用于写回 final_report；可为 None）
        username:     用户名（用于 LLM usage log）

    Returns:
        {"success": bool, "file_id": int, "chapters": int, "final_report": str, ...}
    """
    logger.info(f"[ebook_map_reduce] start: file_id={file_id} execution_id={execution_id}")

    if execution_id:
        _set_rc_agent_status(execution_id, "analyzing")

    try:
        result = asyncio.new_event_loop().run_until_complete(
            _run_map_reduce(file_id=file_id, execution_id=execution_id, username=username)
        )
        logger.info(
            f"[ebook_map_reduce] done: file_id={file_id} "
            f"chapters={result.get('chapters')} success={result.get('success')}"
        )
        return result
    except Exception as exc:
        logger.error(f"[ebook_map_reduce] failed: file_id={file_id} err={exc}", exc_info=True)
        if self.request.retries < self.max_retries:
            raise self.retry(exc=exc)
        return {"success": False, "file_id": file_id, "error": str(exc)}


# ─────────────────────────────────────────────────────────────────────────────
async def _run_map_reduce(
    file_id: int,
    execution_id: Optional[int],
    username: Optional[str],
) -> Dict[str, Any]:
    """核心 async 逻辑，由 Celery task 包装调用。"""
    from backend.database.db_manager import DatabaseManager
    from backend.services.local_llm_analysis_service import LocalLLMAnalysisService

    db = DatabaseManager(get_database_config())

    # 1. 查询 user_id（用于 LLM usage log）
    user_id = _resolve_user_id(db, username)

    # 2. 读取 document_chunks（按 chapter 分组，仅取 file_id 对应的行）
    chapter_chunks = _load_chunks_by_chapter(db, file_id)
    if not chapter_chunks:
        logger.warning(
            f"[ebook_map_reduce] file_id={file_id} 无 document_chunks，"
            f"可能向量化尚未完成或已跳过；任务提前退出。"
        )
        return {"success": False, "file_id": file_id, "error": "no chunks found"}

    logger.info(
        f"[ebook_map_reduce] file_id={file_id}: "
        f"{len(chapter_chunks)} 章，共 {sum(len(v) for v in chapter_chunks.values())} chunks"
    )

    # 3. Map 阶段：每章生成摘要
    chapter_summaries: List[Tuple[str, str]] = []  # [(chapter_title, summary)]
    map_svc = LocalLLMAnalysisService(template_code=_MAP_TEMPLATE_CODE)
    total_map_tokens = 0

    for chapter_title, chunks in chapter_chunks.items():
        chapter_text = _join_chunks(chunks, max_chars=_MAP_MAX_CHARS)
        try:
            map_result = await map_svc.analyze_file_content(
                file_content=chapter_text,
                file_name=chapter_title,
                custom_variables={"chapter_title": chapter_title},
                user_id=user_id,
            )
            summary = map_result.get("report_content", "")
            total_map_tokens += map_result.get("total_tokens", 0)
            chapter_summaries.append((chapter_title, summary))
            logger.info(
                f"[ebook_map_reduce] map: chapter='{chapter_title[:40]}' "
                f"input={len(chapter_text)} chars → summary={len(summary)} chars"
            )
        except Exception as exc:
            logger.warning(
                f"[ebook_map_reduce] map failed for chapter='{chapter_title}': {exc}"
            )
            chapter_summaries.append((chapter_title, f"[摘要生成失败: {exc}]"))

    # 4. Reduce 阶段：合并章节摘要 → 全书概览
    reduce_input = _build_reduce_input(chapter_summaries, max_chars=_REDUCE_MAX_CHARS)
    reduce_svc = LocalLLMAnalysisService(template_code=_REDUCE_TEMPLATE_CODE)
    final_report = ""
    reduce_tokens = 0

    try:
        reduce_result = await reduce_svc.analyze_file_content(
            file_content=reduce_input,
            file_name=f"ebook_overview_file_{file_id}",
            user_id=user_id,
        )
        final_report = reduce_result.get("report_content", "")
        reduce_tokens = reduce_result.get("total_tokens", 0)
        logger.info(
            f"[ebook_map_reduce] reduce done: "
            f"input={len(reduce_input)} chars → report={len(final_report)} chars"
        )
    except Exception as exc:
        logger.error(f"[ebook_map_reduce] reduce failed: {exc}", exc_info=True)
        # Reduce 失败则把章节摘要直接拼接作为 fallback
        final_report = reduce_input

    # 5. 写回 processing_stats.final_report
    if execution_id and final_report:
        _write_final_report(db, execution_id, final_report, total_map_tokens + reduce_tokens)

    return {
        "success": True,
        "file_id": file_id,
        "chapters": len(chapter_summaries),
        "map_tokens": total_map_tokens,
        "reduce_tokens": reduce_tokens,
        "final_report_len": len(final_report),
    }


# ─────────────────────────────────────────────────────────────────────────────
# 辅助函数
# ─────────────────────────────────────────────────────────────────────────────

def _resolve_user_id(db, username: Optional[str]) -> Optional[int]:
    if not username:
        return None
    try:
        with db.get_connection_context() as conn:
            cur = conn.cursor()
            cur.execute("SELECT id FROM users WHERE username = %s LIMIT 1", (username,))
            row = cur.fetchone()
            return row[0] if row else None
    except Exception:
        return None


def _load_chunks_by_chapter(db, file_id: int) -> Dict[str, List[str]]:
    """读取 document_chunks，按 chapter 分组，保持 chunk_idx 顺序。"""
    from collections import defaultdict
    chapters: Dict[str, List[str]] = defaultdict(list)
    try:
        with db.get_connection_context() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT COALESCE(chapter, '(未分章节)'), chunk_text
                FROM document_chunks
                WHERE file_id = %s
                ORDER BY chunk_idx ASC
                """,
                (file_id,),
            )
            for chapter_title, chunk_text in cur.fetchall():
                chapters[chapter_title].append(chunk_text or "")
    except Exception as exc:
        logger.error(f"_load_chunks_by_chapter failed file_id={file_id}: {exc}")
    return dict(chapters)


def _join_chunks(chunks: List[str], max_chars: int) -> str:
    """将 chunk 列表拼接，超过 max_chars 截断。"""
    text = "\n\n".join(chunks)
    if len(text) > max_chars:
        text = text[:max_chars]
    return text


def _build_reduce_input(
    chapter_summaries: List[Tuple[str, str]],
    max_chars: int,
) -> str:
    """将章节摘要列表格式化为 reduce 阶段的输入文本。"""
    parts = []
    for title, summary in chapter_summaries:
        parts.append(f"## {title}\n\n{summary}")
    text = "\n\n---\n\n".join(parts)
    if len(text) > max_chars:
        text = text[:max_chars] + "\n\n[...（章节摘要已截断至 reduce 上限）]"
    return text


def _set_rc_agent_status(execution_id: int, status: str) -> None:
    """更新 processing_stats.rc_agent_status（供前端 NavBar / AI 面板感知进度）。"""
    from backend.database.db_manager import DatabaseManager

    try:
        db = DatabaseManager(get_database_config())
        with db.get_connection_context() as conn:
            cur = conn.cursor()
            cur.execute(
                "UPDATE processing_stats SET rc_agent_status = %s WHERE id = %s",
                (status, execution_id),
            )
    except Exception as exc:
        logger.warning(f"_set_rc_agent_status failed execution_id={execution_id}: {exc}")


def _write_final_report(
    db,
    execution_id: int,
    final_report: str,
    total_tokens: int,
) -> None:
    """写回 processing_stats.final_report 并更新 rc_agent_status。"""
    try:
        with db.get_connection_context() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                UPDATE processing_stats
                SET final_report     = %s,
                    rc_agent_status  = 'success',
                    total_tokens     = COALESCE(total_tokens, 0) + %s,
                    llm_model        = COALESCE(llm_model, 'ebook_map_reduce')
                WHERE id = %s
                """,
                (final_report, total_tokens, execution_id),
            )
        logger.info(
            f"[ebook_map_reduce] written final_report to execution_id={execution_id} "
            f"tokens={total_tokens}"
        )
    except Exception as exc:
        logger.error(f"_write_final_report failed execution_id={execution_id}: {exc}")
