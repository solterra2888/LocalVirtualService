-- =====================================================
-- 添加Qwen 3系列模型支持
-- =====================================================
-- 用途: 更新Qwen提供商配置，添加最新的Qwen 3系列模型
-- 创建时间: 2024-12-08
-- =====================================================

-- 1. 更新Qwen提供商的available_models，添加Qwen 3系列模型
UPDATE llm_provider_configs
SET available_models = '["qwen-turbo", "qwen-plus", "qwen-max", "qwen-max-longcontext", "qwen3-turbo", "qwen3-plus", "qwen3-max"]'::jsonb,
    updated_at = NOW(),
    updated_by = 'system'
WHERE provider_id = 'qwen';

-- 2. 验证更新结果
DO $$
DECLARE
  provider_record RECORD;
BEGIN
  RAISE NOTICE '=== Qwen提供商配置更新结果 ===';
  
  FOR provider_record IN 
    SELECT provider_id, default_model, available_models, enabled
    FROM llm_provider_configs
    WHERE provider_id = 'qwen'
  LOOP
    RAISE NOTICE '提供商: %, 默认模型: %, 可用模型: %, 启用: %',
      provider_record.provider_id,
      provider_record.default_model,
      provider_record.available_models::text,
      provider_record.enabled;
  END LOOP;
  
  IF NOT FOUND THEN
    RAISE NOTICE '警告: 未找到Qwen提供商配置';
  END IF;
END $$;
