-- Migration: add_agent_search_indexes.sql
-- Purpose: Add GIN/trgm indexes to accelerate ILIKE queries in the Agent v1 search path.
--          Also adds GIN indexes on the JSONB concepts columns for the new tags= filter.
--
-- All indexes use CONCURRENTLY so the migration can run on a live production instance
-- without locking tables. Run each statement separately if your migration runner does
-- not support CONCURRENTLY inside a transaction block (CONCURRENTLY cannot run in a
-- transaction).
--
-- Run order: extension first, then indexes.

-- ─── Enable pg_trgm extension (if not already enabled) ──────────────────────
-- Required for gin_trgm_ops operator class used below.
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- ─── file_processing_details.content  (upload full-text) ────────────────────
-- Accelerates: COALESCE(fpd.content, '') ILIKE %(q_N_M)s
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_fpd_content_trgm
    ON file_processing_details
    USING gin (content gin_trgm_ops);

-- ─── subscription_content.content  (feed full-text) ─────────────────────────
-- Accelerates: COALESCE(sc.content, '') ILIKE %(q_N_M)s
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_sc_content_trgm
    ON subscription_content
    USING gin (content gin_trgm_ops);

-- ─── subscription_content.title  (feed title search) ────────────────────────
-- Accelerates: COALESCE(sc.title, '') ILIKE %(q_N_M)s  (high-selectivity, warm cache)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_sc_title_trgm
    ON subscription_content
    USING gin (title gin_trgm_ops);

-- ─── file_tags.concepts  (upload tags= filter) ──────────────────────────────
-- Accelerates: ft.concepts ?| %(tag_arr)s  (?| on jsonb array)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ft_concepts_gin
    ON file_tags
    USING gin (concepts);

-- ─── subscription_content_tags.concepts  (feed tags= filter) ────────────────
-- Accelerates: sct.concepts ?| %(tag_arr)s
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_sct_concepts_gin
    ON subscription_content_tags
    USING gin (concepts);

-- ─── file_tags.ticker  (upload tickers= filter) ─────────────────────────────
-- Accelerates: ft.ticker ?| %(ticker_arr)s
-- (likely already present from add_performance_indexes.sql — IF NOT EXISTS is safe)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ft_ticker_gin
    ON file_tags
    USING gin (ticker);

-- ─── subscription_content_tags.ticker  (feed tickers= filter) ───────────────
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_sct_ticker_gin
    ON subscription_content_tags
    USING gin (ticker);

-- ─── Verify (run after migration) ───────────────────────────────────────────
-- SELECT indexname, indexdef
-- FROM pg_indexes
-- WHERE tablename IN ('file_processing_details', 'subscription_content',
--                     'file_tags', 'subscription_content_tags')
--   AND indexname LIKE 'idx_%trgm%' OR indexname LIKE 'idx_%gin%'
-- ORDER BY tablename, indexname;
