-- =====================================================
-- 新增 Gemini LLM Provider（via CherryIN 网关）
-- =====================================================
-- 创建时间: 2026-06-09
-- 背景:
--   Google Gemini 模型通过 CherryIN 聚合网关接入，
--   使用 OpenAI 兼容 API 端点 (/v1/chat/completions)。
--   支持模型：
--     - google/gemini-3.1-pro-preview（旗舰）
--     - google/gemini-3.5-flash（轻量）
--     - google/gemini-3.1-flash-lite-preview（超轻量）
--
-- 使用方式:
--   - 新部署：db_manager._create_llm_tables() 中已包含 INSERT ... ON CONFLICT DO NOTHING，
--     本脚本仅供已存在 llm_provider_configs 表的老部署执行。
--   - 执行后进入 Admin UI（/admin/llm-config）确认 API Key 已配置并启用。
--
-- 注意事项:
--   - Gemini 与 Claude 共用同一 CherryIN API Key
--   - fallback_priority = 6（在 deepseek 之后作为最后一个兜底）
--
-- 对应代码变更:
--   - backend/core/llm_framework.py: LLMProviderType.GEMINI, GeminiProvider 类,
--     _load_gemini_config(), _load_from_database() gemini 分支
--   - backend/core/llm_pricing.py: _GEMINI_PRICING
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
    'gemini',
    'gemini',
    'Google Gemini',
    '',
    'https://open.cherryin.cc',
    false,
    'google/gemini-3.5-flash',
    '["google/gemini-3.1-pro-preview","google/gemini-3.5-flash","google/gemini-3.1-flash-lite-preview"]',
    8000,
    0.7,
    0.9,
    120,
    'Google Gemini via CherryIN — OpenAI-compatible gateway',
    '✨',
    'system',
    'system',
    false,
    6
)
ON CONFLICT (provider_id) DO UPDATE SET
    display_name      = EXCLUDED.display_name,
    base_url          = EXCLUDED.base_url,
    default_model     = EXCLUDED.default_model,
    available_models  = EXCLUDED.available_models,
    max_tokens        = EXCLUDED.max_tokens,
    description       = EXCLUDED.description,
    icon              = EXCLUDED.icon,
    updated_by        = 'migration_add_gemini_provider',
    updated_at        = CURRENT_TIMESTAMP;