-- =====================================================
-- 新增 Claude (Anthropic via CherryIN) LLM Provider
-- =====================================================
-- 创建时间: 2026-05-12
-- 背景:
--   通过 CherryIN 中转站（https://open.cherryin.net）接入 Anthropic Claude 系列
--   模型。CherryIN 暴露 OpenAI 兼容接口（/v1/chat/completions），因此直接复用
--   现有 OpenAI 兼容 Provider 架构，无需引入 anthropic SDK。
--   计费货币为 USD（与 Qwen/Doubao/GLM 的 RMB 不同）。
--
-- 使用方式:
--   - 新部署：db_manager._create_llm_tables() 中已包含 INSERT ... ON CONFLICT DO NOTHING，
--     本脚本仅供已存在 llm_provider_configs 表的老部署执行。
--   - 执行后进入 Admin UI（/admin/llm-config）填入 API Key 并启用。
--
-- 对应代码变更:
--   - backend/core/llm_framework.py: LLMProviderType.CLAUDE, ClaudeProvider 类,
--     _load_claude_config(), _load_from_database() claude 分支
--   - backend/core/llm_pricing.py: _CLAUDE_PRICING (USD)
-- =====================================================

INSERT INTO llm_provider_configs (
    provider_id,
    provider_name,
    display_name,
    api_key,
    base_url,
    enabled,
    default_model,
    available_models,
    max_tokens,
    temperature,
    top_p,
    timeout,
    description,
    icon,
    created_by,
    updated_by,
    is_default_provider,
    fallback_priority
) VALUES (
    'claude',
    'claude',
    'Claude',
    '',
    'https://open.cherryin.net',
    false,
    'anthropic/claude-sonnet-4.6',
    '["anthropic/claude-sonnet-4.6","anthropic/claude-opus-4.7","anthropic/claude-opus-4-20250514","anthropic/claude-sonnet-4-20250514","anthropic/claude-3-5-sonnet-20241022","anthropic/claude-3-5-haiku-20241022"]',
    4096,
    0.7,
    0.9,
    120,
    'Anthropic Claude via CherryIN (open.cherryin.net) — OpenAI-compatible gateway, billed in USD',
    '🤖',
    'system',
    'system',
    false,
    4
)
ON CONFLICT (provider_id) DO UPDATE SET
    display_name      = EXCLUDED.display_name,
    base_url          = EXCLUDED.base_url,
    default_model     = EXCLUDED.default_model,
    available_models  = EXCLUDED.available_models,
    max_tokens        = EXCLUDED.max_tokens,
    top_p             = EXCLUDED.top_p,
    timeout           = EXCLUDED.timeout,
    description       = EXCLUDED.description,
    icon              = EXCLUDED.icon,
    updated_by        = 'migration_add_claude_provider',
    updated_at        = CURRENT_TIMESTAMP;
