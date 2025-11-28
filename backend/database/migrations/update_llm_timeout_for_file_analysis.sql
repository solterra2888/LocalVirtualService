-- =====================================================
-- 更新LLM配置以支持长文件分析
-- =====================================================
-- 用途: 增加timeout以支持大文件分析
-- 创建时间: 2024-11-26
-- =====================================================

-- 1. 更新所有启用的提供商的timeout为180秒（3分钟）
UPDATE llm_provider_configs
SET timeout = 180,
    updated_at = NOW(),
    updated_by = 'system'
WHERE enabled = true;

-- 2. 确保Qwen配置使用长上下文模型
UPDATE llm_provider_configs
SET default_model = 'qwen-max-longcontext',
    available_models = '["qwen-turbo", "qwen-plus", "qwen-max", "qwen-max-longcontext"]'::jsonb,
    max_tokens = 8000,
    updated_at = NOW(),
    updated_by = 'system'
WHERE provider_id = 'qwen' AND enabled = true;

-- 3. 更新GLM配置增加max_tokens
UPDATE llm_provider_configs
SET max_tokens = 8000,
    updated_at = NOW(),
    updated_by = 'system'
WHERE provider_id = 'glm' AND enabled = true;

-- 4. 更新Doubao配置增加max_tokens
UPDATE llm_provider_configs
SET max_tokens = 8000,
    updated_at = NOW(),
    updated_by = 'system'
WHERE provider_id = 'doubao' AND enabled = true;

-- 5. 验证更新结果
DO $$
DECLARE
  provider_record RECORD;
BEGIN
  RAISE NOTICE '=== LLM提供商配置更新结果 ===';
  
  FOR provider_record IN 
    SELECT provider_id, default_model, timeout, max_tokens, enabled
    FROM llm_provider_configs
    ORDER BY provider_id
  LOOP
    RAISE NOTICE '提供商: %, 模型: %, Timeout: %s, MaxTokens: %, 启用: %',
      provider_record.provider_id,
      provider_record.default_model,
      provider_record.timeout,
      provider_record.max_tokens,
      provider_record.enabled;
  END LOOP;
END $$;

