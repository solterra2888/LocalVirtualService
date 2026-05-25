# -*- coding: utf-8 -*-
"""
自动向量化触发器：5 条判定规则 + enqueue。

判定规则（任一不满足即跳过；幂等通过 content_hash 比对）：
  ① 全局开关       AUTO_VECTORIZE_ENABLED
  ② 用户/文件开关  upload_params.vectorize_enabled / user_pref（暂未启用时按默认）
  ③ 文件类型白名单 source_type ∈ {pdf, word, text, web, youtube, audio, podcast,
                                  feed_twitter, feed_youtube, feed_news, feed_rss}
  ④ 内容长度门槛   MIN ≤ len(content) ≤ MAX；≥ FORCE_LONG 强制
  ⑤ 幂等去重       (vector_status='done' AND content_hash 未变) → 跳过

跳过场景会把 vector_status / vector_skip_reason 写回 parent 表，方便前端展示原因。
真正执行向量化时投递到 Celery `ai_processing` 队列。
"""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Optional, Sequence

from backend.utils.logger import error as log_error
from backend.utils.logger import info as log_info

# 可向量化的文件类型（与 file_processing_details.file_type / subscription_content.channel_type 对齐）
_FILE_VECTORIZABLE = {
    "pdf",
    "word",
    "document",
    "text",
    "web",
    "youtube",
    "audio",
    "podcast",
}
_FEED_VECTORIZABLE = {
    "twitter",
    "youtube",
    "news",
    "rss",
}


def _env_bool(name: str, default: bool = True) -> bool:
    return os.getenv(name, "true" if default else "false").lower() == "true"


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        return default


def _sha256(text: str) -> str:
    return hashlib.sha256((text or "").encode("utf-8", errors="replace")).hexdigest()


# ─────────────────────── public trigger entries ──────────────────────
def trigger_auto_vectorize_file(
    file_id: int,
    *,
    upload_params: Optional[Dict[str, Any]] = None,
    delay: Optional[int] = None,
) -> None:
    """文件上传/解析完成后调用。"""
    if not _env_bool("AUTO_VECTORIZE_ENABLED", True):
        log_info("auto_vectorize disabled by env, skip file_id=%s" % file_id, source="auto_vectorize")
        return

    # 用户上传弹窗里关闭了向量化（默认 True）
    if upload_params:
        enabled = upload_params.get("vectorize_enabled")
        if enabled is False:
            _mark_skipped("file", file_id, "disabled")
            return

    decision = _decide_for_file(file_id)
    if decision is None:
        return  # 已经写过 skipped 状态或 parent 不存在
    if decision.skip:
        _mark_skipped("file", file_id, decision.skip_reason or "unknown")
        return

    _enqueue_file(file_id, delay=delay)


def trigger_auto_vectorize_feed(
    content_id: int,
    *,
    delay: Optional[int] = None,
) -> None:
    """订阅条目入库后调用。"""
    if not _env_bool("AUTO_VECTORIZE_ENABLED", True):
        return

    decision = _decide_for_feed(content_id)
    if decision is None:
        return
    if decision.skip:
        _mark_skipped("feed", content_id, decision.skip_reason or "unknown")
        return

    _enqueue_feed(content_id, delay=delay)


# ─────────────────────────── decisions ───────────────────────────────
@dataclass
class _Decision:
    skip: bool
    skip_reason: Optional[str] = None


def _decide_for_file(file_id: int) -> Optional[_Decision]:
    """读 file_processing_details 决定该不该向量化。"""
    parent = _load_file_meta(file_id)
    if parent is None:
        return None

    file_type = (parent.get("file_type") or "").lower()
    if file_type not in _FILE_VECTORIZABLE:
        return _Decision(True, "non_vectorizable_type")

    content = parent.get("content") or ""
    n = len(content)
    return _decide_by_length(n, parent.get("vector_status"), parent.get("content_hash"), content)


def _decide_for_feed(content_id: int) -> Optional[_Decision]:
    parent = _load_feed_meta(content_id)
    if parent is None:
        return None

    ch = (parent.get("channel_type") or "").lower()
    if ch not in _FEED_VECTORIZABLE:
        return _Decision(True, "non_vectorizable_type")

    content = parent.get("content") or ""
    n = len(content)
    return _decide_by_length(n, parent.get("vector_status"), parent.get("content_hash"), content)


def _decide_by_length(
    n: int,
    vector_status: Optional[str],
    old_hash: Optional[str],
    content: str,
) -> _Decision:
    min_len = _env_int("AUTO_VECTORIZE_MIN_CONTENT_LENGTH", 3000)
    max_len = _env_int("AUTO_VECTORIZE_MAX_CONTENT_LENGTH", 10_000_000)
    force_long = _env_int("AUTO_VECTORIZE_FORCE_LONG_THRESHOLD", 32000)

    if n < min_len:
        return _Decision(True, "too_short")
    if n > max_len:
        return _Decision(True, "too_large")

    # 幂等：完成且 hash 未变 → 跳过
    if vector_status == "done" and old_hash and old_hash == _sha256(content):
        return _Decision(True, "already_up_to_date")

    # n >= force_long 也走向量化（force=True）；这里不区分 force 标志，统一进入 enqueue
    # 中等长度暂时不依赖 user_pref（schema 还没有该字段；后续 Phase 8 加入即可）
    return _Decision(False)


# ─────────────────────────── PG helpers ──────────────────────────────
def _load_file_meta(file_id: int) -> Optional[Dict[str, Any]]:
    from backend.database.db_manager import DatabaseManager
    from backend.utils.database_config import get_database_config

    db = DatabaseManager(get_database_config())
    try:
        with db.get_connection_context() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT id, file_type, content, vector_status, content_hash
                FROM file_processing_details WHERE id = %s
                """,
                (file_id,),
            )
            row = cur.fetchone()
            if not row:
                return None
            return {
                "id": row[0],
                "file_type": row[1],
                "content": row[2],
                "vector_status": row[3],
                "content_hash": row[4],
            }
    except Exception as exc:  # noqa: BLE001
        log_error(
            f"trigger_auto_vectorize_file load_meta failed: file_id={file_id} err={exc}",
            source="auto_vectorize",
        )
        return None


def _load_feed_meta(content_id: int) -> Optional[Dict[str, Any]]:
    from backend.database.db_manager import DatabaseManager
    from backend.utils.database_config import get_database_config

    db = DatabaseManager(get_database_config())
    try:
        with db.get_connection_context() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT id, channel_type, content, vector_status, content_hash
                FROM subscription_content WHERE id = %s
                """,
                (content_id,),
            )
            row = cur.fetchone()
            if not row:
                return None
            return {
                "id": row[0],
                "channel_type": row[1],
                "content": row[2],
                "vector_status": row[3],
                "content_hash": row[4],
            }
    except Exception as exc:  # noqa: BLE001
        log_error(
            f"trigger_auto_vectorize_feed load_meta failed: content_id={content_id} err={exc}",
            source="auto_vectorize",
        )
        return None


def _mark_skipped(target: str, target_id: int, reason: str) -> None:
    from backend.database.db_manager import DatabaseManager
    from backend.utils.database_config import get_database_config

    table = (
        "file_processing_details" if target == "file" else "subscription_content"
    )
    db = DatabaseManager(get_database_config())
    try:
        with db.get_connection_context() as conn:
            cur = conn.cursor()
            cur.execute(
                f"""
                UPDATE {table}
                SET vector_status = 'skipped',
                    vector_skip_reason = %s,
                    vector_error = NULL
                WHERE id = %s
                """,
                (reason, target_id),
            )
        log_info(
            f"auto_vectorize skip: target={target} id={target_id} reason={reason}",
            source="auto_vectorize",
        )
    except Exception as exc:  # noqa: BLE001
        log_error(
            f"mark_skipped failed: target={target} id={target_id} err={exc}",
            source="auto_vectorize",
        )


# ─────────────────────────── enqueue ─────────────────────────────────
def _default_delay() -> int:
    return _env_int("AUTO_VECTORIZE_DELAY", 2)


def _enqueue_file(file_id: int, *, delay: Optional[int] = None) -> None:
    try:
        from backend.tasks.vectorize_tasks import vectorize_file_content_task

        # 立即把状态置 pending，避免重复触发
        _mark_pending("file", file_id)
        vectorize_file_content_task.apply_async(
            args=(file_id,),
            countdown=delay if delay is not None else _default_delay(),
        )
        log_info(
            f"auto_vectorize enqueued: file_id={file_id}", source="auto_vectorize"
        )
    except Exception as exc:  # noqa: BLE001
        log_error(
            f"auto_vectorize enqueue failed: file_id={file_id} err={exc}",
            source="auto_vectorize",
        )


def _enqueue_feed(content_id: int, *, delay: Optional[int] = None) -> None:
    try:
        from backend.tasks.vectorize_tasks import vectorize_feed_content_task

        _mark_pending("feed", content_id)
        vectorize_feed_content_task.apply_async(
            args=(content_id,),
            countdown=delay if delay is not None else _default_delay(),
        )
        log_info(
            f"auto_vectorize enqueued: content_id={content_id}", source="auto_vectorize"
        )
    except Exception as exc:  # noqa: BLE001
        log_error(
            f"auto_vectorize enqueue failed: content_id={content_id} err={exc}",
            source="auto_vectorize",
        )


def _mark_pending(target: str, target_id: int) -> None:
    from backend.database.db_manager import DatabaseManager
    from backend.utils.database_config import get_database_config

    table = (
        "file_processing_details" if target == "file" else "subscription_content"
    )
    db = DatabaseManager(get_database_config())
    try:
        with db.get_connection_context() as conn:
            cur = conn.cursor()
            cur.execute(
                f"""
                UPDATE {table}
                SET vector_status = 'pending',
                    vector_skip_reason = NULL,
                    vector_error = NULL
                WHERE id = %s
                """,
                (target_id,),
            )
    except Exception as exc:  # noqa: BLE001
        log_error(
            f"mark_pending failed: target={target} id={target_id} err={exc}",
            source="auto_vectorize",
        )


# ─────────────────────────── batch helpers ──────────────────────────
def trigger_auto_vectorize_files(
    file_ids: Sequence[int],
    *,
    upload_params: Optional[Dict[str, Any]] = None,
    delay: Optional[int] = None,
) -> None:
    for fid in file_ids:
        try:
            trigger_auto_vectorize_file(int(fid), upload_params=upload_params, delay=delay)
        except Exception as exc:  # noqa: BLE001
            log_error(
                f"trigger_auto_vectorize_files item failed: file_id={fid} err={exc}",
                source="auto_vectorize",
            )


def trigger_auto_vectorize_feeds(
    content_ids: Sequence[int],
    *,
    delay: Optional[int] = None,
) -> None:
    for cid in content_ids:
        try:
            trigger_auto_vectorize_feed(int(cid), delay=delay)
        except Exception as exc:  # noqa: BLE001
            log_error(
                f"trigger_auto_vectorize_feeds item failed: content_id={cid} err={exc}",
                source="auto_vectorize",
            )
