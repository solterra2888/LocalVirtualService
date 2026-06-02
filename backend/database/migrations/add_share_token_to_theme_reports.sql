-- =====================================================
-- Theme Report — add share_token for public sharing
-- =====================================================
-- Created: 2026-06-02
-- Purpose: Allow unauthenticated read access to a specific report
--          via an unguessable random token served by the backend proxy.
--          The token is generated at report completion time (uuid4().hex).

ALTER TABLE theme_reports
    ADD COLUMN IF NOT EXISTS share_token VARCHAR(64);

-- Enforce uniqueness (two reports must never share a token)
CREATE UNIQUE INDEX IF NOT EXISTS idx_theme_reports_share_token
    ON theme_reports(share_token)
    WHERE share_token IS NOT NULL;

COMMENT ON COLUMN theme_reports.share_token IS
    'Unguessable random token (uuid4 hex, 32 chars) for the public share link. '
    'NULL until the report is first requested after this migration.';
