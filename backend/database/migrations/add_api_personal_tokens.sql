-- Personal Access Tokens for Agent / MCP / Skill API (readonly scopes by default)
-- Run manually or rely on db_manager._ensure_api_personal_tokens_table on startup.

CREATE TABLE IF NOT EXISTS api_personal_tokens (
    id           BIGSERIAL PRIMARY KEY,
    user_id      INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    name         TEXT NOT NULL,
    prefix       TEXT NOT NULL,
    token_hash   TEXT NOT NULL,
    scopes       JSONB NOT NULL DEFAULT '["daily:read", "reports:read"]'::jsonb,
    expires_at   TIMESTAMPTZ,
    last_used_at TIMESTAMPTZ,
    revoked_at   TIMESTAMPTZ,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_apt_user ON api_personal_tokens(user_id);
CREATE INDEX IF NOT EXISTS idx_apt_prefix ON api_personal_tokens(prefix);

COMMENT ON TABLE api_personal_tokens IS 'User-issued long-lived API tokens (PAT) for Agent/MCP; plaintext shown only once at creation.';
