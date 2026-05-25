# -*- coding: utf-8 -*-
"""
Vector Bucket 向量化 Celery 任务。

队列：ai_processing（与 theme / tagging 任务复用）

任务列表：
- vectorize_file_content_task(file_id)            → upload 路径
- vectorize_feed_content_task(content_id)         → feed 路径
- delete_file_vectors_task(file_id)               → 文件删除时级联清向量
- delete_feed_vectors_task(content_id)            → Feed 退订/删除时级联清向量
- reconcile_vector_bucket_task()                  → 每日 reconcile：清孤儿、补失败、报警

约定：
- 任务即使失败也不抛出（已写 vector_status='failed'），避免 Celery 自动 retry 风暴
  对 Qwen / Vector Bucket 形成放大写。
- 显式 retry 仅用于"瞬时网络错误"场景；当前 MVP 不启用 retry。
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from backend.celery_app import app
from backend.services.document_vectorize_service import (
    DocumentVectorizeService,
    VectorizeOutcome,
)
from backend.services.vector_bucket_client import (
    VectorBucketError,
    get_default_client,
)
from backend.utils.database_config import get_database_config
from backend.utils.logger import get_logger

logger = get_logger("vectorize_tasks")


def _service() -> DocumentVectorizeService:
    return DocumentVectorizeService(db_config=get_database_config())


# ───────────────────────── vectorize ─────────────────────────────────
@app.task(
    bind=True,
    name="vectorize_file_content",
    queue="ai_processing",
    acks_late=True,
    max_retries=0,
)
def vectorize_file_content_task(self, file_id: int) -> Dict[str, Any]:
    """向量化一条 file_processing_details 记录。"""
    logger.info(f"vectorize_file_content_task start: file_id={file_id}")
    outcome = _service().vectorize_file_content(int(file_id))
    return _outcome_to_dict(outcome)


@app.task(
    bind=True,
    name="vectorize_feed_content",
    queue="ai_processing",
    acks_late=True,
    max_retries=0,
)
def vectorize_feed_content_task(self, content_id: int) -> Dict[str, Any]:
    """向量化一条 subscription_content 记录。"""
    logger.info(f"vectorize_feed_content_task start: content_id={content_id}")
    outcome = _service().vectorize_feed_content(int(content_id))
    return _outcome_to_dict(outcome)


# ───────────────────────── delete cascade ────────────────────────────
@app.task(
    bind=True,
    name="delete_file_vectors",
    queue="ai_processing",
    acks_late=True,
    max_retries=3,
    default_retry_delay=30,
)
def delete_file_vectors_task(self, file_id: int) -> Dict[str, Any]:
    """删除 file_id 关联的 document_chunks + Vector Bucket keys。"""
    return _delete_vectors(target="file", target_id=int(file_id))


@app.task(
    bind=True,
    name="delete_feed_vectors",
    queue="ai_processing",
    acks_late=True,
    max_retries=3,
    default_retry_delay=30,
)
def delete_feed_vectors_task(self, content_id: int) -> Dict[str, Any]:
    """删除 content_id 关联的 document_chunks + Vector Bucket keys。"""
    return _delete_vectors(target="feed", target_id=int(content_id))


@app.task(
    bind=True,
    name="delete_vectors_by_keys",
    queue="ai_processing",
    acks_late=True,
    max_retries=3,
    default_retry_delay=30,
)
def delete_vectors_by_keys_task(self, chunk_ids: List[int]) -> Dict[str, Any]:
    """仅从 Vector Bucket 按 key 删除. 适用于 processing_stats CASCADE 删除已清空 PG 后,
    需要异步把 OSS Vector Bucket 上的向量也清干净的场景."""
    if not chunk_ids:
        return {"success": True, "deleted": 0}
    try:
        client = get_default_client()
        deleted = client.delete_vectors([str(i) for i in chunk_ids])
    except VectorBucketError as exc:
        logger.warning(f"delete_vectors_by_keys failed (chunks={len(chunk_ids)}): {exc}")
        return {"success": False, "deleted": 0, "error": str(exc)}
    logger.info(f"delete_vectors_by_keys: deleted={deleted}")
    return {"success": True, "deleted": deleted}


def _delete_vectors(target: str, target_id: int) -> Dict[str, Any]:
    from backend.database.db_manager import DatabaseManager

    db = DatabaseManager(get_database_config())
    col = "file_id" if target == "file" else "content_id"
    with db.get_connection_context() as conn:
        cur = conn.cursor()
        cur.execute(
            f"SELECT id FROM document_chunks WHERE {col} = %s",
            (target_id,),
        )
        chunk_ids = [int(r[0]) for r in cur.fetchall()]
        if chunk_ids:
            cur.execute(
                f"DELETE FROM document_chunks WHERE {col} = %s",
                (target_id,),
            )
    deleted_from_vb = 0
    if chunk_ids:
        try:
            client = get_default_client()
            deleted_from_vb = client.delete_vectors([str(i) for i in chunk_ids])
        except VectorBucketError as exc:
            logger.warning(
                f"delete_vectors VectorBucket call failed (PG already cleaned): "
                f"target={target} id={target_id} err={exc}"
            )
    logger.info(
        f"delete_vectors done: target={target} id={target_id} "
        f"pg_rows={len(chunk_ids)} vb_keys={deleted_from_vb}"
    )
    return {
        "success": True,
        "target": target,
        "target_id": target_id,
        "pg_chunks_deleted": len(chunk_ids),
        "vector_bucket_keys_deleted": deleted_from_vb,
    }


# ───────────────────────── reconcile ─────────────────────────────────
@app.task(
    bind=True,
    name="reconcile_vector_bucket",
    queue="ai_processing",
    acks_late=True,
    max_retries=0,
)
def reconcile_vector_bucket_task(self, dry_run: bool = False) -> Dict[str, Any]:
    """
    日常清理孤儿 chunks。

    Step 1: PG 层先清掉 parent 已删除的孤儿 document_chunks（ON DELETE CASCADE 通常已处理，
            但显式扫一次以兜底）。
    Step 2: 把这些孤儿 chunk_ids 也从 Vector Bucket 删除。
    Step 3: 检测 vector_status='done' 但 PG 实际 chunk 数为 0 的异常行，重置为 NULL。
    """
    from backend.database.db_manager import DatabaseManager

    db = DatabaseManager(get_database_config())
    orphans: List[int] = []
    fixed_zero_count = 0

    with db.get_connection_context() as conn:
        cur = conn.cursor()
        # Step 1+2: 找孤儿（理论上 ON DELETE CASCADE 已处理；扫描是兜底）
        # 这里把"parent 已不存在"的 chunk 找出来
        cur.execute(
            """
            SELECT dc.id FROM document_chunks dc
            LEFT JOIN file_processing_details f ON dc.file_id = f.id
            LEFT JOIN subscription_content sc   ON dc.content_id = sc.id
            WHERE (dc.file_id    IS NOT NULL AND f.id  IS NULL)
               OR (dc.content_id IS NOT NULL AND sc.id IS NULL)
            LIMIT 5000
            """
        )
        orphans = [int(r[0]) for r in cur.fetchall()]

        if orphans and not dry_run:
            cur.execute(
                "DELETE FROM document_chunks WHERE id = ANY(%s)",
                (orphans,),
            )

        # Step 3: vector_status='done' 但 PG 中 chunk 数为 0 → 重置（说明历史脏数据）
        if not dry_run:
            cur.execute(
                """
                UPDATE file_processing_details fpd
                SET vector_status = NULL,
                    vector_chunk_count = 0,
                    vector_indexed_at = NULL,
                    vector_error = 'reconcile: no chunks found'
                WHERE fpd.vector_status = 'done'
                  AND NOT EXISTS (
                      SELECT 1 FROM document_chunks dc WHERE dc.file_id = fpd.id
                  )
                """
            )
            fixed_zero_count += cur.rowcount or 0
            cur.execute(
                """
                UPDATE subscription_content sc
                SET vector_status = NULL,
                    vector_chunk_count = 0,
                    vector_indexed_at = NULL,
                    vector_error = 'reconcile: no chunks found'
                WHERE sc.vector_status = 'done'
                  AND NOT EXISTS (
                      SELECT 1 FROM document_chunks dc WHERE dc.content_id = sc.id
                  )
                """
            )
            fixed_zero_count += cur.rowcount or 0

    deleted_from_vb = 0
    if orphans and not dry_run:
        try:
            client = get_default_client()
            deleted_from_vb = client.delete_vectors([str(i) for i in orphans])
        except VectorBucketError as exc:
            logger.warning(f"reconcile vector delete failed: {exc}")

    logger.info(
        f"reconcile_vector_bucket: orphans={len(orphans)} vb_deleted={deleted_from_vb} "
        f"fixed_zero={fixed_zero_count} dry_run={dry_run}"
    )
    return {
        "success": True,
        "orphans_found": len(orphans),
        "vector_bucket_keys_deleted": deleted_from_vb,
        "rows_reset_to_null": fixed_zero_count,
        "dry_run": dry_run,
    }


# ───────────────────────── helpers ───────────────────────────────────
def _outcome_to_dict(outcome: VectorizeOutcome) -> Dict[str, Any]:
    return {
        "success": outcome.success,
        "target": outcome.target,
        "target_id": outcome.target_id,
        "chunk_count": outcome.chunk_count,
        "skipped": outcome.skipped,
        "skip_reason": outcome.skip_reason,
        "error": outcome.error,
        "duration_ms": outcome.duration_ms,
    }
