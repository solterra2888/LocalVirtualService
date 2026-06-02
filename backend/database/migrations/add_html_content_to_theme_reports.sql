-- =====================================================
-- Theme Report — store generated HTML in the DB row
-- =====================================================
-- Created: 2026-06-02
-- Purpose: Serve the report HTML from DB (via backend proxy) without needing
--          to re-fetch from OSS on every request.  OSS upload is kept as a
--          backup/archive; the iframe and share endpoint use this column.
--          PostgreSQL compresses large TEXT values automatically via TOAST.

ALTER TABLE theme_reports
    ADD COLUMN IF NOT EXISTS html_content TEXT;

COMMENT ON COLUMN theme_reports.html_content IS
    'Full self-contained HTML of the generated report (stored for inline serving '
    'without OSS round-trip). NULL for legacy rows generated before this migration.';
