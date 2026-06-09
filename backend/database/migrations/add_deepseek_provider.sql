-- =====================================================
-- 新增 DeepSeek LLM Provider
-- =====================================================
-- 创建时间: 2026-06-09
-- 背景:
--   DeepSeek AI 提供 OpenAI 兼容 API 端点 (/v1/chat/completions)，
--   支持模型 deepseek-v4-pro（旗舰）和 deepseek-v4-flash（轻量）。
--   计费货币为人民币（RMB）。
--
-- 使用方式:
--   - 新部署：db_manager._create_llm_tables() 中已包含 INSERT ... ON CONFLICT DO NOTHING，
--     本脚本仅供已存在 llm_provider_configs 表的老部署执行。
--   - 执行后进入 Admin UI（/admin/llm-config）填入 API Key 并启用。
--
-- 对应代码变更:
--   - backend/core/llm_framework.py: LLMProviderType.DEEPSEEK, DeepSeekProvider 类,
--     _load_deepseek_config(), _load_from_database() deepseek 分支
--   - backend/core/llm_pricing.py: _DEEPSEEK_PRICING (RMB)
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
    'deepseek',
    'deepseek',
    'DeepSeek',
    '',
    'https://api.deepseek.com',
    false,
    'deepseek-v4-pro',
    '["deepseek-v4-pro","deepseek-v4-flash"]',
    8000,
    0.7,
    0.9,
    120,
    'DeepSeek AI — OpenAI-compatible API, billed in RMB',
    '🧠',
    'system',
    'system',
    false,
    5
)
ON CONFLICT (provider_id) DO UPDATE SET
    display_name      = EXCLUDED.display_name,
    base_url          = EXCLUDED.base_url,
    default_model     = EXCLUDED.default_model,
    available_models  = EXCLUDED.available_models,
    max_tokens        = EXCLUDED.max_tokens,
    description       = EXCLUDED.description,
    icon              = EXCLUDED.icon,
    updated_by        = 'migration_add_deepseek_provider',
    updated_at        = CURRENT_TIMESTAMP;