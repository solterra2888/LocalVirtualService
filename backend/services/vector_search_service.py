# -*- coding: utf-8 -*-
"""
Vector-backed search service for the v2 Agent API.

Three public entrypoints — each takes a TokenData (auth) and a Request DTO
(see ``backend/schemas/agent_search.py``):

  * semantic_search:        embed query → QueryVectors → PG join chunk_text + auth.
  * hybrid_search:          semantic + keyword (Daily v1 ILIKE/pg_trgm) RRF fusion.
  * get_chunks_by_locator:  pure PG lookup by chapter / page / chunk_idx range.

Auth model:
  - Vector layer pre-filter via metadata `owner_id IN [username, '__feed__']`.
  - PG join post-filter enforces real ownership:
      upload chunks → owner_username = current_user.username
      feed chunks   → current_user.user_id = ANY(subscription_content.user_ids)

Embedding generation reuses ``QwenProvider.generate_embeddings`` (model
text-embedding-v3, 1024 dim). All synchronous PG operations go through
``DatabaseManager.get_connection_context`` for safe rollback / connection lease.
"""

from __future__ import annotations

import asyncio
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from fastapi import HTTPException

from backend.core.llm_framework import llm_service
from backend.database.db_manager import DatabaseManager
from backend.schemas.agent_search import (
    ChunksByLocatorRequest,
    ChunksByLocatorResponse,
    HybridSearchRequest,
    HybridSearchResponse,
    LocatorChunk,
    SearchDiagnostics,
    SearchHit,
    SemanticSearchRequest,
    SemanticSearchResponse,
)
from backend.schemas.user import TokenData
from backend.services.vector_bucket_client import (
    VectorBucketError,
    VectorHit,
    get_default_client,
)
from backend.utils.database_config import get_database_config
from backend.utils.logger import get_logger

logger = get_logger("vector_search_service")

EMBED_MODEL = "text-embedding-v3"
EMBED_DIM = 1024
DEFAULT_OVERSAMPLE = 2  # request 2x topK from vector store, then PG-filter & cap


# ───────────────────────────── service ──────────────────────────────────
class VectorSearchService:
    """Read service powering /api/agent/v2/* endpoints + MCP semantic tools."""

    def __init__(self, db_config: Optional[Dict[str, Any]] = None) -> None:
        self.db = DatabaseManager(db_config or get_database_config())
        self.vector_client = get_default_client()

    # =================== semantic_search ===================
    def semantic_search(self, user: TokenData, req: SemanticSearchRequest) -> SemanticSearchResponse:
        diag = SearchDiagnostics()
        if self.vector_client is None or not self.vector_client.available:
            diag.notes = "vector_bucket_unavailable"
            return SemanticSearchResponse(results=[], diagnostics=diag)

        t0 = time.time()
        try:
            q_vec = self._embed_query(req.query)
        except Exception as exc:  # noqa: BLE001
            logger.error(f"embed_query failed: {exc}")
            diag.notes = f"embed_failed:{exc}"
            return SemanticSearchResponse(results=[], diagnostics=diag)
        diag.embed_ms = int((time.time() - t0) * 1000)

        accessible_owners = self._resolve_accessible_owners(user.user_id or -1)
        filter_expr = self._build_vector_filter(user, req, accessible_owners)

        t1 = time.time()
        try:
            hits = self.vector_client.query_vectors(
                query_vector=q_vec,
                top_k=min(50, max(req.top_k * DEFAULT_OVERSAMPLE, req.top_k)),
                filter_expr=filter_expr,
                return_metadata=True,
                return_distance=True,
            )
        except VectorBucketError as exc:
            logger.error(f"QueryVectors failed: {exc}")
            diag.notes = f"query_failed:{exc}"
            return SemanticSearchResponse(results=[], diagnostics=diag)
        diag.query_ms = int((time.time() - t1) * 1000)
        diag.candidates = len(hits)

        if not hits:
            return SemanticSearchResponse(results=[], diagnostics=diag)

        t2 = time.time()
        rows = self._fetch_authorized_chunks(user, [int(h.key) for h in hits if h.key.isdigit()])
        diag.fetch_ms = int((time.time() - t2) * 1000)

        results = self._assemble_results(
            hits=hits,
            rows_by_id=rows,
            top_k=req.top_k,
            min_score=req.min_score,
        )
        diag.returned = len(results)
        return SemanticSearchResponse(results=results, diagnostics=diag)

    # =================== hybrid_search ===================
    def hybrid_search(self, user: TokenData, req: HybridSearchRequest) -> HybridSearchResponse:
        """Hybrid = semantic ⊕ keyword (Daily v1) fused via Reciprocal Rank Fusion."""
        diag = SearchDiagnostics()
        # Run semantic
        sem_resp = self.semantic_search(user, req)
        diag.embed_ms = sem_resp.diagnostics.embed_ms
        diag.query_ms = sem_resp.diagnostics.query_ms
        diag.fetch_ms = sem_resp.diagnostics.fetch_ms
        diag.candidates = sem_resp.diagnostics.candidates

        # Run keyword via existing Daily v1 service (returns content-level items)
        kw_items = self._keyword_search(user, req)

        # RRF fusion
        # Semantic ranks: by content_id (best chunk per content), already in score-desc order
        semantic_ranking: Dict[int, int] = {}
        chunk_by_content: Dict[int, SearchHit] = {}
        for idx, hit in enumerate(sem_resp.results):
            if hit.content_id not in semantic_ranking:
                semantic_ranking[hit.content_id] = idx
                chunk_by_content[hit.content_id] = hit

        keyword_ranking: Dict[int, int] = {}
        for idx, cid in enumerate(kw_items):
            if cid not in keyword_ranking:
                keyword_ranking[cid] = idx

        all_cids = set(semantic_ranking) | set(keyword_ranking)
        rrf_k = 60
        weight_kw = req.keyword_weight
        weight_sem = 1.0 - weight_kw
        rrf_scores: List[Tuple[float, int]] = []
        for cid in all_cids:
            score = 0.0
            if cid in semantic_ranking:
                score += weight_sem / (rrf_k + semantic_ranking[cid] + 1)
            if cid in keyword_ranking:
                score += weight_kw / (rrf_k + keyword_ranking[cid] + 1)
            rrf_scores.append((score, cid))
        rrf_scores.sort(key=lambda t: t[0], reverse=True)

        # Materialise hits
        fused: List[SearchHit] = []
        for _, cid in rrf_scores[: req.top_k]:
            sem_hit = chunk_by_content.get(cid)
            sources: List[str] = []
            if cid in semantic_ranking:
                sources.append("vector")
            if cid in keyword_ranking:
                sources.append("keyword")
            if sem_hit:
                fused.append(
                    SearchHit(
                        content_id=sem_hit.content_id,
                        chunk_id=sem_hit.chunk_id,
                        source_type=sem_hit.source_type,
                        title=sem_hit.title,
                        chapter=sem_hit.chapter,
                        page=sem_hit.page,
                        chunk_text=sem_hit.chunk_text,
                        score=sem_hit.score,
                        found_via=sources,
                    )
                )
            else:
                # keyword-only hit: pull first chunk (best-effort) or just title
                stub = self._build_keyword_only_hit(user, cid)
                if stub:
                    stub.found_via = sources
                    fused.append(stub)

        diag.returned = len(fused)
        return HybridSearchResponse(results=fused, diagnostics=diag)

    # =================== get_chunks_by_locator ===================
    def get_chunks_by_locator(
        self, user: TokenData, req: ChunksByLocatorRequest
    ) -> ChunksByLocatorResponse:
        if not req.file_id and not req.content_id:
            raise HTTPException(400, "Provide either file_id or content_id.")
        if req.file_id and req.content_id:
            raise HTTPException(400, "Provide only one of file_id / content_id.")

        if req.file_id:
            return self._locator_for_file(user, req)
        return self._locator_for_feed(user, req)

    # =================== helpers ===================
    def _embed_query(self, query: str) -> List[float]:
        provider = llm_service.providers.get("qwen") if llm_service else None
        if provider is None:
            raise RuntimeError("Qwen provider not configured")

        async def _embed() -> List[float]:
            vectors = await provider.generate_embeddings(
                texts=[query],
                model=EMBED_MODEL,
                text_type="query",
            )
            if not vectors:
                raise RuntimeError("Empty embedding result")
            vec = vectors[0]
            if len(vec) != EMBED_DIM:
                raise RuntimeError(f"Embedding dim mismatch: {len(vec)} != {EMBED_DIM}")
            return vec

        loop = asyncio.new_event_loop()
        try:
            asyncio.set_event_loop(loop)
            return loop.run_until_complete(_embed())
        finally:
            loop.close()

    def _build_vector_filter(
        self,
        user: TokenData,
        req: SemanticSearchRequest,
        accessible_owners: Sequence[str],
    ) -> Dict[str, Any]:
        """Compose `$and` filter for Vector Bucket QueryVectors."""
        clauses: List[Dict[str, Any]] = []

        # Auth scope: upload owned by current user OR any feed (post-filter in PG)
        username = user.username or ""
        owner_candidates = [u for u in [username, *accessible_owners] if u]
        clauses.append(
            {
                "$or": [
                    {"owner_id": {"$in": owner_candidates}},
                    {"owner_id": {"$eq": "__feed__"}},
                ]
            }
        )

        if req.source_types:
            clauses.append({"source_type": {"$in": list(req.source_types)}})

        if req.lang:
            clauses.append({"lang": {"$eq": req.lang}})

        if req.since:
            # raises 400 on invalid values, same contract as Agent v1
            cutoff = _parse_since(req.since)
            clauses.append({"created_ts": {"$gte": str(int(cutoff))}})

        if req.file_ids:
            ids = [str(int(i)) for i in req.file_ids]
            clauses.append(
                {
                    "$and": [
                        {"source_type": {"$nin": ["feed_twitter", "feed_youtube", "feed_news", "feed_rss"]}},
                        {"content_id": {"$in": ids}},
                    ]
                }
            )

        if req.content_ids:
            ids = [str(int(i)) for i in req.content_ids]
            clauses.append(
                {
                    "$and": [
                        {"source_type": {"$in": ["feed_twitter", "feed_youtube", "feed_news", "feed_rss"]}},
                        {"content_id": {"$in": ids}},
                    ]
                }
            )

        if len(clauses) == 1:
            return clauses[0]
        return {"$and": clauses}

    def _resolve_accessible_owners(self, user_id: int) -> List[str]:
        """Return usernames who shared upload files to current user."""
        if user_id < 0:
            return []

        sql = """
        SELECT DISTINCT u.username
        FROM file_shares fs
        JOIN users u ON u.id = fs.shared_by_user_id
        WHERE fs.shared_to_user_id = %s
        """
        with self.db.get_connection_context() as conn:
            cur = conn.cursor()
            cur.execute(sql, (user_id,))
            return [r[0] for r in cur.fetchall() if r[0]]

    def _fetch_authorized_chunks(
        self, user: TokenData, chunk_ids: Sequence[int]
    ) -> Dict[int, Dict[str, Any]]:
        """Return {chunk_id: {title, chapter, page, chunk_text, source_type, content_id}}.

        Enforces:
          - upload chunks owned by current user
          - feed chunks where current user is in subscription_content.user_ids
        """
        if not chunk_ids:
            return {}
        username = user.username or ""
        user_id = user.user_id or -1

        rows: Dict[int, Dict[str, Any]] = {}
        sql = """
        SELECT
            dc.id, dc.source_type, dc.chapter, dc.page, dc.chunk_text,
            dc.file_id, dc.content_id,
            fpd.display_name AS upload_title,
            fpd.filename     AS upload_filename,
            sc.title         AS feed_title,
            sc.author_name   AS feed_author,
            sc.user_ids      AS feed_user_ids,
            ps."user"        AS upload_owner,
            (fs.id IS NOT NULL) AS shared_to_me
        FROM document_chunks dc
        LEFT JOIN file_processing_details fpd ON fpd.id = dc.file_id
        LEFT JOIN processing_stats         ps ON ps.id = fpd.processing_stats_id
        LEFT JOIN subscription_content     sc ON sc.id = dc.content_id
        LEFT JOIN file_shares              fs ON fs.file_id = dc.file_id AND fs.shared_to_user_id = %s
        WHERE dc.id = ANY(%s)
        """
        with self.db.get_connection_context() as conn:
            cur = conn.cursor()
            cur.execute(sql, (user_id, list(chunk_ids)))
            for r in cur.fetchall():
                (
                    cid,
                    source_type,
                    chapter,
                    page,
                    chunk_text,
                    file_id,
                    content_id,
                    upload_title,
                    upload_filename,
                    feed_title,
                    feed_author,
                    feed_user_ids,
                    upload_owner,
                    shared_to_me,
                ) = r

                # Auth check
                if file_id is not None:
                    if (upload_owner or "") != username and not bool(shared_to_me):
                        continue
                    parent_id = int(file_id)
                    title = upload_title or upload_filename
                elif content_id is not None:
                    if user_id < 0 or user_id not in (feed_user_ids or []):
                        continue
                    parent_id = int(content_id)
                    title = feed_title or feed_author
                else:
                    continue

                rows[int(cid)] = {
                    "source_type": source_type,
                    "chapter": chapter,
                    "page": page,
                    "chunk_text": chunk_text,
                    "title": title,
                    "content_id": parent_id,
                }
        return rows

    @staticmethod
    def _assemble_results(
        hits: Sequence[VectorHit],
        rows_by_id: Dict[int, Dict[str, Any]],
        top_k: int,
        min_score: Optional[float],
    ) -> List[SearchHit]:
        out: List[SearchHit] = []
        for h in hits:
            if not h.key.isdigit():
                continue
            row = rows_by_id.get(int(h.key))
            if row is None:
                continue
            # Cosine distance → similarity (alibaba returns cosine distance; smaller = closer)
            score = 1.0 - h.distance if h.distance is not None else 1.0
            if min_score is not None and score < min_score:
                continue
            out.append(
                SearchHit(
                    content_id=int(row["content_id"]),
                    chunk_id=int(h.key),
                    source_type=row["source_type"],
                    title=row.get("title"),
                    chapter=row.get("chapter"),
                    page=row.get("page"),
                    chunk_text=row["chunk_text"],
                    score=round(score, 4),
                    found_via=["vector"],
                )
            )
            if len(out) >= top_k:
                break
        return out

    def _keyword_search(self, user: TokenData, req: SemanticSearchRequest) -> List[int]:
        """Use Daily v1 service to grab keyword-matching content_ids."""
        try:
            from backend.services.agent_service import AgentService

            svc = AgentService()
            data = svc.get_daily_feed(
                user,
                since=req.since,
                q=req.query,
                sources=_v2_source_types_to_v1_sources(req.source_types),
                limit=50,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning(f"keyword_search via Daily v1 failed: {exc}")
            return []

        # AgentDailyItem.id is opaque (encoded type+id); strip to numeric id when possible.
        ids: List[int] = []
        items = getattr(data, "items", []) or []
        for it in items:
            num = _extract_int_id(getattr(it, "id", ""))
            if num is not None:
                ids.append(num)
        return ids

    def _build_keyword_only_hit(self, user: TokenData, content_id: int) -> Optional[SearchHit]:
        """For keyword-only matches (no vector hit), synthesize a hit using the first chunk."""
        username = user.username or ""
        user_id = user.user_id or -1
        sql_upload = """
        SELECT dc.id, dc.source_type, dc.chapter, dc.page, dc.chunk_text,
               fpd.display_name AS title, fpd.filename, ps."user" AS owner,
               (fs.id IS NOT NULL) AS shared_to_me
        FROM document_chunks dc
        JOIN file_processing_details fpd ON fpd.id = dc.file_id
        LEFT JOIN processing_stats   ps  ON ps.id = fpd.processing_stats_id
        LEFT JOIN file_shares        fs  ON fs.file_id = dc.file_id AND fs.shared_to_user_id = %s
        WHERE dc.file_id = %s AND (ps."user" = %s OR fs.id IS NOT NULL)
        ORDER BY dc.chunk_idx ASC
        LIMIT 1
        """
        sql_feed = """
        SELECT dc.id, dc.source_type, dc.chapter, dc.page, dc.chunk_text,
               sc.title AS title, sc.author_name, sc.user_ids
        FROM document_chunks dc
        JOIN subscription_content sc ON sc.id = dc.content_id
        WHERE dc.content_id = %s AND %s = ANY(sc.user_ids)
        ORDER BY dc.chunk_idx ASC
        LIMIT 1
        """
        try:
            with self.db.get_connection_context() as conn:
                cur = conn.cursor()
                cur.execute(sql_upload, (user_id, content_id, username))
                r = cur.fetchone()
                if r:
                    return SearchHit(
                        content_id=content_id,
                        chunk_id=int(r[0]),
                        source_type=r[1],
                        title=r[5] or r[6],
                        chapter=r[2],
                        page=r[3],
                        chunk_text=r[4],
                        score=0.0,  # keyword-only
                        found_via=["keyword"],
                    )
                cur.execute(sql_feed, (content_id, user_id))
                r = cur.fetchone()
                if r:
                    return SearchHit(
                        content_id=content_id,
                        chunk_id=int(r[0]),
                        source_type=r[1],
                        title=r[5] or r[6],
                        chapter=r[2],
                        page=r[3],
                        chunk_text=r[4],
                        score=0.0,
                        found_via=["keyword"],
                    )
        except Exception as exc:  # noqa: BLE001
            logger.warning(f"build_keyword_only_hit failed cid={content_id}: {exc}")
        return None

    # =================== locator helpers ===================
    def _locator_for_file(
        self, user: TokenData, req: ChunksByLocatorRequest
    ) -> ChunksByLocatorResponse:
        username = user.username or ""
        user_id = user.user_id or -1
        sql_parent = """
        SELECT fpd.id, fpd.display_name, fpd.filename, fpd.file_type, ps."user",
               EXISTS (
                   SELECT 1
                   FROM file_shares fs
                   WHERE fs.file_id = fpd.id AND fs.shared_to_user_id = %s
               ) AS shared_to_me
        FROM file_processing_details fpd
        LEFT JOIN processing_stats ps ON ps.id = fpd.processing_stats_id
        WHERE fpd.id = %s
        """
        clauses, params = ["dc.file_id = %s"], [req.file_id]
        self._apply_chapter_page_filters(req, clauses, params)

        with self.db.get_connection_context() as conn:
            cur = conn.cursor()
            cur.execute(sql_parent, (user_id, req.file_id))
            row = cur.fetchone()
            if not row:
                raise HTTPException(404, "file not found")
            if (row[4] or "") != username and not bool(row[5]):
                raise HTTPException(403, "not authorised")
            title = row[1] or row[2]
            source_type = self._source_type_for_upload(row[3])

            sql = f"""
            SELECT dc.id, dc.chunk_idx, dc.chapter, dc.page, dc.chunk_text
            FROM document_chunks dc
            WHERE {' AND '.join(clauses)}
            ORDER BY dc.chunk_idx ASC
            """
            cur.execute(sql, tuple(params))
            chunks_rows = cur.fetchall()

        return self._build_locator_response(
            content_id=int(req.file_id),
            source_type=source_type,
            title=title,
            rows=chunks_rows,
            max_chars=req.max_chars,
        )

    def _locator_for_feed(
        self, user: TokenData, req: ChunksByLocatorRequest
    ) -> ChunksByLocatorResponse:
        user_id = user.user_id or -1
        sql_parent = """
        SELECT id, title, author_name, channel_type, user_ids
        FROM subscription_content WHERE id = %s
        """
        clauses, params = ["dc.content_id = %s"], [req.content_id]
        self._apply_chapter_page_filters(req, clauses, params)

        with self.db.get_connection_context() as conn:
            cur = conn.cursor()
            cur.execute(sql_parent, (req.content_id,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(404, "content not found")
            if user_id < 0 or user_id not in (row[4] or []):
                raise HTTPException(403, "not authorised")
            title = row[1] or row[2]
            source_type = f"feed_{(row[3] or 'rss').lower()}"

            sql = f"""
            SELECT dc.id, dc.chunk_idx, dc.chapter, dc.page, dc.chunk_text
            FROM document_chunks dc
            WHERE {' AND '.join(clauses)}
            ORDER BY dc.chunk_idx ASC
            """
            cur.execute(sql, tuple(params))
            chunks_rows = cur.fetchall()

        return self._build_locator_response(
            content_id=int(req.content_id),
            source_type=source_type,
            title=title,
            rows=chunks_rows,
            max_chars=req.max_chars,
        )

    @staticmethod
    def _apply_chapter_page_filters(
        req: ChunksByLocatorRequest,
        clauses: List[str],
        params: List[Any],
    ) -> None:
        if req.chapter:
            clauses.append("dc.chapter = %s")
            params.append(req.chapter)
        elif req.chapter_prefix:
            clauses.append("dc.chapter LIKE %s")
            params.append(f"{req.chapter_prefix}%")
        if req.page_range:
            clauses.append("dc.page BETWEEN %s AND %s")
            params.append(int(req.page_range[0]))
            params.append(int(req.page_range[1]))
        if req.chunk_idx_range:
            clauses.append("dc.chunk_idx BETWEEN %s AND %s")
            params.append(int(req.chunk_idx_range[0]))
            params.append(int(req.chunk_idx_range[1]))

    @staticmethod
    def _build_locator_response(
        *,
        content_id: int,
        source_type: str,
        title: Optional[str],
        rows: Sequence[Sequence[Any]],
        max_chars: int,
    ) -> ChunksByLocatorResponse:
        out: List[LocatorChunk] = []
        total = 0
        truncated = False
        for r in rows:
            cid, idx, chapter, page, text = int(r[0]), int(r[1]), r[2], r[3], (r[4] or "")
            if total + len(text) > max_chars and out:
                truncated = True
                break
            out.append(
                LocatorChunk(
                    chunk_id=cid,
                    chunk_idx=idx,
                    chapter=chapter,
                    page=page,
                    chunk_text=text,
                )
            )
            total += len(text)
        return ChunksByLocatorResponse(
            content_id=content_id,
            source_type=source_type,
            title=title,
            total_chars=total,
            truncated=truncated,
            chunks=out,
        )

    @staticmethod
    def _source_type_for_upload(file_type: Optional[str]) -> str:
        ft = (file_type or "").lower()
        if ft in {"pdf", "word", "excel", "csv", "document"}:
            return "upload"
        if ft == "text":
            return "upload_text"
        if ft == "web":
            return "upload_web"
        if ft in {"audio", "podcast", "youtube"}:
            return f"upload_{ft}"
        if ft == "image":
            return "upload_image"
        return "upload"


# ───────────────────────── module helpers ──────────────────────────────
# v2 chunk source_type → Daily v1 sources= tokens. The hybrid keyword branch
# delegates to AgentService.get_daily_feed, whose source vocabulary differs
# ('youtube' there means uploaded YouTube videos; feed YouTube is 'youtube_feed';
# v1's 'document' expands to document+image uploads). feed_rss has no v1
# equivalent and is dropped — the call falls back to the user's preset,
# which already includes feed sources.
_V2_SOURCE_TYPE_TO_V1_SOURCES: Dict[str, str] = {
    "upload": "document,audio,text,podcast,web,youtube",
    "upload_text": "text",
    "upload_web": "web",
    "upload_youtube": "youtube",
    "upload_audio": "audio",
    "upload_podcast": "podcast",
    "upload_image": "image",
    "feed_twitter": "twitter",
    "feed_youtube": "youtube_feed",
    "feed_news": "news",
    "feed_zhishixingqiu": "zhishixingqiu",
    "feed_substack": "substack",
    "feed_weibo": "weibo",
}


def _v2_source_types_to_v1_sources(source_types: Optional[Sequence[str]]) -> Optional[str]:
    if not source_types:
        return None
    tokens = []
    for st in source_types:
        mapped = _V2_SOURCE_TYPE_TO_V1_SOURCES.get(st)
        if mapped:
            tokens.append(mapped)
    return ",".join(tokens) if tokens else None


def _parse_since(since: Optional[str]) -> Optional[int]:
    """Return epoch seconds cutoff, or None if unset.

    Raises HTTPException 400 on non-empty unrecognised input, matching the
    Agent v1 contract (_parse_since in agent_service.py).
    """
    if not since:
        return None
    s = since.strip().lower()
    now = datetime.now(timezone.utc)
    m = re.fullmatch(r"(\d+)([hdw])", s)
    if m:
        n, unit = int(m.group(1)), m.group(2)
        if unit == "h":
            cutoff = now - timedelta(hours=n)
        elif unit == "d":
            cutoff = now - timedelta(days=n)
        else:
            cutoff = now - timedelta(weeks=n)
        return int(cutoff.timestamp())
    try:
        dt = datetime.fromisoformat(s.replace("z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    except ValueError:
        pass
    raise HTTPException(
        status_code=400,
        detail=f"Invalid 'since' value '{since}'. "
        "Use Nd (e.g. 7d), Nh (e.g. 24h), Nw (e.g. 2w), or an ISO 8601 datetime.",
    )


_NUM_ID_RE = re.compile(r"(\d+)$")


def _extract_int_id(opaque: str) -> Optional[int]:
    """Daily v1 ids look like 'feed_youtube:12345' or '12345'. Pull trailing int."""
    if not opaque:
        return None
    m = _NUM_ID_RE.search(str(opaque))
    if not m:
        return None
    try:
        return int(m.group(1))
    except ValueError:
        return None
