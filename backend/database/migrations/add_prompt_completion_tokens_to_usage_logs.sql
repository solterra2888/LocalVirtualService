-- =====================================================
-- llm_prompt_usage_logs：新增 prompt_tokens / completion_tokens 列
-- =====================================================
-- 创建时间：2026-05-11
-- 背景：
--   原表只存 tokens_used（= total_tokens），
--   无法区分输入/输出 token 各占多少，导致按官方分开计价时无法精确还原。
--   现将 API 响应中 usage.prompt_tokens / usage.completion_tokens
--   单独落库，便于后续成本核算、分析与 BSS 账单对账。
--
-- 向后兼容：
--   新增列均允许 NULL，存量历史行无需回填，不影响现有查询。
--   tokens_used 列保留（=total_tokens），不做修改，旧读取逻辑不受影响。
-- =====================================================

ALTER TABLE llm_prompt_usage_logs
    ADD COLUMN IF NOT EXISTS prompt_tokens     INTEGER,
    ADD COLUMN IF NOT EXISTS completion_tokens INTEGER;

COMMENT ON COLUMN llm_prompt_usage_logs.prompt_tokens
    IS 'API 响应 usage.prompt_tokens（输入 token 数）；历史行为 NULL';
COMMENT ON COLUMN llm_prompt_usage_logs.completion_tokens
    IS 'API 响应 usage.completion_tokens（输出 token 数）；历史行为 NULL';

-- 验证
-- SELECT column_name, data_type, is_nullable
-- FROM information_schema.columns
-- WHERE table_name = 'llm_prompt_usage_logs'
--   AND column_name IN ('tokens_used','prompt_tokens','completion_tokens')
-- ORDER BY ordinal_position;
