-- Migration: add_vector_indexing.sql
-- Purpose: Bootstrap data layer for OSS Vector Bucket-backed semantic search.
--
-- This migration:
--   1. Creates document_chunks (PG source of truth for chunk text + metadata)
--   2. Augments file_processing_details and subscription_content with vector_* status columns
--
-- Vectors themselves live in OSS Vector Bucket (bucket=guanhetech-vec, index=content_chunks).
-- PG holds the canonical chunk_text; vector layer is purely a discovery index.
--
-- Auth model:
--   - Upload chunks: document_chunks.owner_username (denormalised from processing_stats."user")
--   - Feed   chunks: owner_username=NULL; authorisation enforced via JOIN to
--                    subscription_content.user_ids (INTEGER[]) at query time.
--
-- Safe to run on live prod: only CREATE TABLE IF NOT EXISTS + ADD COLUMN IF NOT EXISTS
-- + CREATE INDEX CONCURRENTLY. No table rewrites.
--
-- Run order: extension (pg_trgm already enabled by add_agent_search_indexes.sql) -> table -> indexes.

-- ─── 1. document_chunks: PG source of truth for chunk text ─────────────────
CREATE TABLE IF NOT EXISTS document_chunks (
    id              BIGSERIAL    PRIMARY KEY,
    -- Source classification: 'upload' for files, 'feed_*' for subscription content
    source_type     TEXT         NOT NULL,
    -- Exactly one of file_id / content_id must be set (enforced by CHECK below)
    file_id         INTEGER      REFERENCES file_processing_details(id) ON DELETE CASCADE,
    content_id      INTEGER      REFERENCES subscription_content(id)    ON DELETE CASCADE,
    -- Denormalised for upload chunks; NULL for feeds (auth via subscription_content.user_ids)
    owner_username  TEXT,
    -- Position within parent document
    chunk_idx       INTEGER      NOT NULL,
    chapter         TEXT,
    page            INTEGER,
    char_start      INTEGER      NOT NULL,
    char_end        INTEGER      NOT NULL,
    chunk_text      TEXT         NOT NULL,
    token_count     INTEGER,
    -- sha256(parent content) at chunk generation time; used for idempotent re-vectorise
    content_hash    TEXT         NOT NULL,
    -- True once PutVectors to OSS Vector Bucket succeeded for this chunk
    vector_written  BOOLEAN      NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),

    CONSTRAINT chk_doc_chunks_parent_xor
        CHECK ((file_id IS NOT NULL) <> (content_id IS NOT NULL))
);

-- Look-ups by parent for delete cascade reconciliation
CREATE INDEX IF NOT EXISTS idx_doc_chunks_file
    ON document_chunks(file_id)
    WHERE file_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_doc_chunks_content
    ON document_chunks(content_id)
    WHERE content_id IS NOT NULL;

-- Auth-scope filter on upload chunks (semantic_search PG join)
CREATE INDEX IF NOT EXISTS idx_doc_chunks_owner
    ON document_chunks(owner_username)
    WHERE owner_username IS NOT NULL;

-- Locator: get_chunks_by_locator(file_id, chapter)
CREATE INDEX IF NOT EXISTS idx_doc_chunks_chapter
    ON document_chunks(file_id, chapter, chunk_idx);

-- Reconcile: find chunks whose vector_written status diverges from Vector Bucket
CREATE INDEX IF NOT EXISTS idx_doc_chunks_vector_written
    ON document_chunks(vector_written)
    WHERE vector_written = FALSE;

COMMENT ON TABLE  document_chunks                 IS 'Canonical chunk store for RAG. Pairs 1:1 with vectors in OSS Vector Bucket (key=document_chunks.id).';
COMMENT ON COLUMN document_chunks.source_type     IS 'upload | feed_twitter | feed_youtube | feed_news | feed_rss';
COMMENT ON COLUMN document_chunks.owner_username  IS 'For upload chunks only. Feeds auth via subscription_content.user_ids.';
COMMENT ON COLUMN document_chunks.content_hash    IS 'sha256 of parent content at chunk time; mismatch triggers re-vectorise.';
COMMENT ON COLUMN document_chunks.vector_written  IS 'True once PutVectors to Vector Bucket succeeded.';

-- ─── 2. file_processing_details: vector status columns ─────────────────────
ALTER TABLE file_processing_details
    ADD COLUMN IF NOT EXISTS vector_status        TEXT,        -- pending|running|done|skipped|failed
    ADD COLUMN IF NOT EXISTS vector_skip_reason   TEXT,        -- when skipped: too_short|too_large|non_vectorizable_type|disabled
    ADD COLUMN IF NOT EXISTS vector_chunk_count   INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS vector_indexed_at    TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS vector_error         TEXT,
    ADD COLUMN IF NOT EXISTS content_hash         TEXT;

CREATE INDEX IF NOT EXISTS idx_fpd_vector_status
    ON file_processing_details(vector_status)
    WHERE vector_status IS NOT NULL;

COMMENT ON COLUMN file_processing_details.vector_status        IS 'OSS Vector Bucket indexing status: pending|running|done|skipped|failed';
COMMENT ON COLUMN file_processing_details.vector_skip_reason   IS 'When vector_status=skipped, the reason: too_short|too_large|non_vectorizable_type|disabled';
COMMENT ON COLUMN file_processing_details.content_hash         IS 'sha256(content); used to detect re-analyse and trigger re-vectorise.';

-- ─── 3. subscription_content: vector status columns ────────────────────────
ALTER TABLE subscription_content
    ADD COLUMN IF NOT EXISTS vector_status        TEXT,
    ADD COLUMN IF NOT EXISTS vector_skip_reason   TEXT,
    ADD COLUMN IF NOT EXISTS vector_chunk_count   INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS vector_indexed_at    TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS vector_error         TEXT,
    ADD COLUMN IF NOT EXISTS content_hash         TEXT;

CREATE INDEX IF NOT EXISTS idx_sc_vector_status
    ON subscription_content(vector_status)
    WHERE vector_status IS NOT NULL;

COMMENT ON COLUMN subscription_content.vector_status        IS 'OSS Vector Bucket indexing status: pending|running|done|skipped|failed';
COMMENT ON COLUMN subscription_content.vector_skip_reason   IS 'When vector_status=skipped, the reason: too_short|too_large|non_vectorizable_type|disabled';
COMMENT ON COLUMN subscription_content.content_hash         IS 'sha256(content); used to detect re-fetch and trigger re-vectorise.';
