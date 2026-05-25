# -*- coding: utf-8 -*-
"""
文档/Feed 向量化编排服务。

职责：
1. 从 PG 读取 parent 内容（file_processing_details 或 subscription_content）
2. 比对 content_hash 做幂等判定
3. 调用 ChunkingService 切分
4. 写 document_chunks（PG 真相层）
5. 批量调用 Qwen embedding → PutVectors 到 OSS Vector Bucket
6. 更新 parent 行的 vector_status / vector_chunk_count / content_hash

不在本服务里实现的：
- 触发判定（5 条规则）：放 vectorize_triggers.py
- 删除/级联：放 vectorize_tasks.delete_*_vectors_task

调用入口：
- vectorize_file_content(file_id) — 上传文件
- vectorize_feed_content(content_id) — 订阅条目
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from backend.core.llm_framework import llm_service
from backend.database.db_manager import DatabaseManager
from backend.services.chunking_service import ChunkDraft, chunk_text, sha256_of
from backend.services.vector_bucket_client import (
    VectorBucketClient,
    VectorBucketError,
    VectorRecord,
    get_default_client,
)
from backend.utils.database_config import get_database_config
from backend.utils.datetime_utils import utcnow
from backend.utils.logger import get_logger

logger = get_logger("document_vectorize_service")


EMBED_MODEL = "text-embedding-v3"
EMBED_DIM = 1024
EMBED_BATCH = 10  # Qwen text-embedding API 单批 25 上限，留 buffer 取 10


@dataclass
class VectorizeOutcome:
    """单次向量化结果（用于上层任务记录日志/状态）。"""

    success: bool
    target: str  # 'file' | 'feed'
    target_id: int
    chunk_count: int = 0
    skipped: bool = False
    skip_reason: Optional[str] = None
    error: Optional[str] = None
    duration_ms: int = 0


class DocumentVectorizeService:
    """文档/Feed 向量化主服务。"""

    PARENT_FILE = "file"
    PARENT_FEED = "feed"

    def __init__(
        self,
        db_config: Optional[Dict[str, Any]] = None,
        vector_client: Optional[VectorBucketClient] = None,
    ) -> None:
        self.db = DatabaseManager(db_config or get_database_config())
        self.vector_client = vector_client or get_default_client()

    # ────────────────────── public entrypoints ─────────────────────────
    def vectorize_file_content(self, file_id: int) -> VectorizeOutcome:
        """处理 file_processing_details 的一条记录。"""
        return self._vectorize(self.PARENT_FILE, file_id)

    def vectorize_feed_content(self, content_id: int) -> VectorizeOutcome:
        """处理 subscription_content 的一条记录。"""
        return self._vectorize(self.PARENT_FEED, content_id)

    # ────────────────────── orchestrator ───────────────────────────────
    def _vectorize(self, target: str, target_id: int) -> VectorizeOutcome:
        t0 = time.time()
        try:
            parent = self._load_parent(target, target_id)
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Failed to load parent {target}/{target_id}: {exc}")
            return VectorizeOutcome(False, target, target_id, error=str(exc))

        if parent is None:
            return VectorizeOutcome(
                False, target, target_id, error="parent_not_found"
            )

        content: str = parent["content"] or ""
        file_type: str = parent["file_type"]
        owner_username: Optional[str] = parent.get("owner_username")

        new_hash = sha256_of(content)
        old_hash = parent.get("content_hash")
        old_status = parent.get("vector_status")

        if old_status == "done" and old_hash == new_hash:
            logger.info(f"Vectorize skip: already up-to-date target={target} id={target_id}")
            return VectorizeOutcome(
                True,
                target,
                target_id,
                chunk_count=int(parent.get("vector_chunk_count") or 0),
                skipped=True,
                skip_reason="already_up_to_date",
                duration_ms=int((time.time() - t0) * 1000),
            )

        # 以下场景需先清理旧 chunks：hash 变更 或 上次失败/中断后重试
        should_clean = (old_hash and old_hash != new_hash) or (
            old_status in ("failed", "running")
        )
        if should_clean:
            try:
                self._delete_old_chunks(target, target_id, reason="content_hash_changed")
            except Exception as exc:  # noqa: BLE001
                logger.warning(f"Failed to drop stale chunks for {target}/{target_id}: {exc}")

        # 切分
        drafts = chunk_text(content, file_type)
        if not drafts:
            self._mark_status(
                target,
                target_id,
                status="skipped",
                skip_reason="empty_after_chunking",
                content_hash=new_hash,
            )
            return VectorizeOutcome(
                True,
                target,
                target_id,
                chunk_count=0,
                skipped=True,
                skip_reason="empty_after_chunking",
                duration_ms=int((time.time() - t0) * 1000),
            )

        # 状态置 running
        self._mark_status(target, target_id, status="running", content_hash=new_hash)

        try:
            chunk_ids = self._insert_chunks(
                target=target,
                target_id=target_id,
                owner_username=owner_username,
                drafts=drafts,
                content_hash=new_hash,
                source_type=self._derive_source_type(target, parent),
            )
            self._embed_and_upload(
                chunk_ids=chunk_ids,
                drafts=drafts,
                target=target,
                target_id=target_id,
                owner_username=owner_username,
                source_type=self._derive_source_type(target, parent),
                lang=self._guess_lang(content),
                created_ts=int(parent.get("created_at_epoch") or int(time.time())),
            )
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Vectorize failed: target={target} id={target_id} err={exc}")
            self._mark_status(
                target,
                target_id,
                status="failed",
                error=str(exc)[:1000],
                content_hash=new_hash,
            )
            return VectorizeOutcome(
                False,
                target,
                target_id,
                error=str(exc),
                duration_ms=int((time.time() - t0) * 1000),
            )

        chunk_count = len(drafts)
        self._mark_status(
            target,
            target_id,
            status="done",
            chunk_count=chunk_count,
            content_hash=new_hash,
            indexed_at=utcnow(),
        )

        return VectorizeOutcome(
            True,
            target,
            target_id,
            chunk_count=chunk_count,
            duration_ms=int((time.time() - t0) * 1000),
        )

    # ────────────────────── parent loaders ────────────────────────────
    def _load_parent(self, target: str, target_id: int) -> Optional[Dict[str, Any]]:
        if target == self.PARENT_FILE:
            return self._load_file_parent(target_id)
        if target == self.PARENT_FEED:
            return self._load_feed_parent(target_id)
        raise ValueError(f"unknown target: {target}")

    def _load_file_parent(self, file_id: int) -> Optional[Dict[str, Any]]:
        sql = """
        SELECT fpd.id, fpd.file_type, fpd.content, fpd.content_hash,
               fpd.vector_status, fpd.vector_chunk_count,
               ps."user" AS owner_username,
               EXTRACT(EPOCH FROM fpd.created_at)::BIGINT AS created_at_epoch
        FROM file_processing_details fpd
        LEFT JOIN processing_stats ps ON ps.id = fpd.processing_stats_id
        WHERE fpd.id = %s
        """
        with self.db.get_connection_context() as conn:
            cur = conn.cursor()
            cur.execute(sql, (file_id,))
            row = cur.fetchone()
            if not row:
                return None
            return {
                "id": row[0],
                "file_type": row[1],
                "content": row[2] or "",
                "content_hash": row[3],
                "vector_status": row[4],
                "vector_chunk_count": row[5] or 0,
                "owner_username": row[6],
                "created_at_epoch": row[7],
            }

    def _load_feed_parent(self, content_id: int) -> Optional[Dict[str, Any]]:
        sql = """
        SELECT sc.id, sc.channel_type, sc.content, sc.content_hash,
               sc.vector_status, sc.vector_chunk_count,
               EXTRACT(EPOCH FROM sc.created_at)::BIGINT AS created_at_epoch
        FROM subscription_content sc
        WHERE sc.id = %s
        """
        with self.db.get_connection_context() as conn:
            cur = conn.cursor()
            cur.execute(sql, (content_id,))
            row = cur.fetchone()
            if not row:
                return None
            return {
                "id": row[0],
                "file_type": row[1],  # 复用 file_type 字段名，存的是 channel_type
                "content": row[2] or "",
                "content_hash": row[3],
                "vector_status": row[4],
                "vector_chunk_count": row[5] or 0,
                "owner_username": None,
                "created_at_epoch": row[6],
            }

    # ────────────────────── PG writes ─────────────────────────────────
    def _insert_chunks(
        self,
        target: str,
        target_id: int,
        owner_username: Optional[str],
        drafts: Sequence[ChunkDraft],
        content_hash: str,
        source_type: str,
    ) -> List[int]:
        """批量 INSERT document_chunks；返回每条对应的 id 列表（保持顺序）。"""
        file_id = target_id if target == self.PARENT_FILE else None
        content_id = target_id if target == self.PARENT_FEED else None
        rows: List[Tuple[Any, ...]] = []
        for d in drafts:
            rows.append(
                (
                    source_type,
                    file_id,
                    content_id,
                    owner_username if target == self.PARENT_FILE else None,
                    d.chunk_idx,
                    d.chapter,
                    d.page,
                    d.char_start,
                    d.char_end,
                    d.text,
                    d.token_count,
                    content_hash,
                )
            )

        sql = """
        INSERT INTO document_chunks
            (source_type, file_id, content_id, owner_username,
             chunk_idx, chapter, page, char_start, char_end,
             chunk_text, token_count, content_hash)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        RETURNING id
        """
        ids: List[int] = []
        with self.db.get_connection_context() as conn:
            cur = conn.cursor()
            for row in rows:
                cur.execute(sql, row)
                ids.append(int(cur.fetchone()[0]))
        return ids

    def _mark_chunks_written(self, chunk_ids: Sequence[int]) -> None:
        if not chunk_ids:
            return
        with self.db.get_connection_context() as conn:
            cur = conn.cursor()
            cur.execute(
                "UPDATE document_chunks SET vector_written = TRUE WHERE id = ANY(%s)",
                (list(chunk_ids),),
            )

    def _delete_old_chunks(self, target: str, target_id: int, reason: str) -> None:
        """删除旧 chunks 行（并触发删除 Vector Bucket 中对应 keys）。"""
        col = "file_id" if target == self.PARENT_FILE else "content_id"
        with self.db.get_connection_context() as conn:
            cur = conn.cursor()
            cur.execute(
                f"SELECT id FROM document_chunks WHERE {col} = %s",
                (target_id,),
            )
            old_ids = [int(r[0]) for r in cur.fetchall()]
            if old_ids:
                cur.execute(
                    f"DELETE FROM document_chunks WHERE {col} = %s",
                    (target_id,),
                )
        if old_ids and self.vector_client is not None:
            try:
                self.vector_client.delete_vectors([str(i) for i in old_ids])
                logger.info(
                    f"Dropped {len(old_ids)} stale chunks for {target}/{target_id} ({reason})"
                )
            except VectorBucketError as exc:
                logger.warning(
                    f"Vector delete failed for stale chunks {target}/{target_id}: {exc}",
                )

    def _mark_status(
        self,
        target: str,
        target_id: int,
        *,
        status: str,
        skip_reason: Optional[str] = None,
        error: Optional[str] = None,
        chunk_count: Optional[int] = None,
        content_hash: Optional[str] = None,
        indexed_at: Optional[datetime] = None,
    ) -> None:
        table = (
            "file_processing_details"
            if target == self.PARENT_FILE
            else "subscription_content"
        )

        sets: List[str] = ["vector_status = %s"]
        params: List[Any] = [status]
        if skip_reason is not None:
            sets.append("vector_skip_reason = %s")
            params.append(skip_reason)
        elif status not in ("skipped",):
            sets.append("vector_skip_reason = NULL")
        if error is not None:
            sets.append("vector_error = %s")
            params.append(error)
        elif status not in ("failed",):
            sets.append("vector_error = NULL")
        if chunk_count is not None:
            sets.append("vector_chunk_count = %s")
            params.append(chunk_count)
        if content_hash is not None:
            sets.append("content_hash = %s")
            params.append(content_hash)
        if indexed_at is not None:
            sets.append("vector_indexed_at = %s")
            params.append(indexed_at)

        sql = f"UPDATE {table} SET {', '.join(sets)} WHERE id = %s"
        params.append(target_id)
        with self.db.get_connection_context() as conn:
            cur = conn.cursor()
            cur.execute(sql, tuple(params))

    # ────────────────────── embedding + vector upload ─────────────────
    def _embed_and_upload(
        self,
        chunk_ids: Sequence[int],
        drafts: Sequence[ChunkDraft],
        target: str,
        target_id: int,
        owner_username: Optional[str],
        source_type: str,
        lang: str,
        created_ts: int,
    ) -> None:
        if not chunk_ids:
            return
        if self.vector_client is None or not self.vector_client.available:
            raise VectorBucketError("vector client unavailable")

        provider = llm_service.providers.get("qwen") if llm_service else None
        if provider is None:
            raise RuntimeError("Qwen provider not configured")

        owner_id = owner_username or ("__feed__")
        parent_content_id = str(target_id)

        async def _run() -> None:
            for batch_start in range(0, len(chunk_ids), EMBED_BATCH):
                batch_ids = chunk_ids[batch_start : batch_start + EMBED_BATCH]
                batch_drafts = drafts[batch_start : batch_start + EMBED_BATCH]
                texts = [d.text for d in batch_drafts]
                vectors = await provider.generate_embeddings(
                    texts=texts,
                    model=EMBED_MODEL,
                    text_type="document",
                )
                if len(vectors) != len(batch_ids):
                    raise RuntimeError(
                        f"Embedding count mismatch: got {len(vectors)}, expected {len(batch_ids)}"
                    )
                records: List[VectorRecord] = []
                for cid, d, vec in zip(batch_ids, batch_drafts, vectors):
                    if len(vec) != EMBED_DIM:
                        raise RuntimeError(
                            f"Embedding dim mismatch: got {len(vec)}, expected {EMBED_DIM}"
                        )
                    records.append(
                        VectorRecord(
                            key=str(cid),
                            vector=vec,
                            metadata=self._build_metadata(
                                content_id=parent_content_id,
                                chunk_id=cid,
                                source_type=source_type,
                                owner_id=owner_id,
                                chapter=d.chapter,
                                page=d.page,
                                lang=lang,
                                created_ts=created_ts,
                            ),
                        )
                    )
                self.vector_client.put_vectors(records)
                self._mark_chunks_written(batch_ids)

        # 在同步 Celery worker 里跑 async
        loop = asyncio.new_event_loop()
        try:
            asyncio.set_event_loop(loop)
            loop.run_until_complete(_run())
        finally:
            loop.close()

    # ────────────────────── helpers ───────────────────────────────────
    @staticmethod
    def _derive_source_type(target: str, parent: Dict[str, Any]) -> str:
        if target == DocumentVectorizeService.PARENT_FILE:
            ft = (parent.get("file_type") or "").lower()
            if ft in {"pdf", "word", "excel", "csv", "document"}:
                return "upload"
            if ft == "youtube":
                return "upload_youtube"
            if ft in {"audio", "podcast"}:
                return f"upload_{ft}"
            if ft == "web":
                return "upload_web"
            if ft == "text":
                return "upload_text"
            if ft == "image":
                return "upload_image"
            return "upload"
        # feed
        ct = (parent.get("file_type") or "").lower()  # channel_type
        if ct in {"twitter", "youtube", "news", "rss"}:
            return f"feed_{ct}"
        return "feed"

    @staticmethod
    def _guess_lang(content: str) -> str:
        # 极简启发：CJK 占比 > 20% 视为 zh，其余 en。够用即可，后期可换 langid。
        if not content:
            return "und"
        sample = content[:2000]
        cjk = sum(1 for ch in sample if "\u4e00" <= ch <= "\u9fff")
        if cjk > 0 and cjk / max(1, len(sample)) > 0.2:
            return "zh"
        return "en"

    @staticmethod
    def _build_metadata(
        *,
        content_id: str,
        chunk_id: int,
        source_type: str,
        owner_id: str,
        chapter: Optional[str],
        page: Optional[int],
        lang: str,
        created_ts: int,
    ) -> Dict[str, str]:
        # 注意：Vector Bucket metadata 仅支持 String；过滤字段保持简短。
        # 每条 vector metadata <= 40KB，单字段 <= 2KB（filterable）。
        md: Dict[str, str] = {
            "source_type": source_type,        # filterable
            "owner_id": owner_id,              # filterable (upload owner / __feed__)
            "content_id": content_id,          # filterable (反查权威表)
            "chunk_id": str(chunk_id),         # non-filterable
            "lang": lang,                      # filterable
            "created_ts": str(int(created_ts)),  # filterable
        }
        if chapter:
            md["chapter"] = chapter[:200]
        if page is not None:
            md["page"] = str(int(page))
        return md
