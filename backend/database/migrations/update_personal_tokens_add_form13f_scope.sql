-- =====================================================
-- 给存量 Personal Access Token 补 form13f:read scope
-- 创建日期: 2026-11-21
-- 描述: Agent API 新增 /api/v1/agent/form13f/* 端点（PAT scope=form13f:read）。
--   ALLOWED_SCOPES / DEFAULT_SCOPES 已在 personal_token_service.py 同步扩展，
--   但存量 token 的 scopes JSONB 里没有新 scope，调用会 403。
--   本迁移给所有存量 token 追加 form13f:read，并把列默认值改为三 scope
--   （幂等，可重复执行）。
-- =====================================================

UPDATE api_personal_tokens
SET scopes = scopes || '["form13f:read"]'::jsonb
WHERE NOT (scopes ? 'form13f:read');

ALTER TABLE api_personal_tokens
    ALTER COLUMN scopes SET DEFAULT '["daily:read", "reports:read", "form13f:read"]'::jsonb;
