-- =====================================================
-- 修复LLM超时配置（仅更新数据库中存在的字段）
-- =====================================================
-- 用途: 增加timeout以支持大文件分析
-- 创建时间: 2025-11-27
-- 说明: retry_count在代码中配置，不在数据库中
-- =====================================================

-- 1. 更新所有启用的提供商配置
UPDATE llm_provider_configs
SET 
    timeout = 180,              -- 读取超时3分钟（从30秒增加）
    max_tokens = 8000,          -- 增加最大token数以支持长输出（从2000增加）
    temperature = 0.5,          -- 降低温度以提高稳定性（从0.7降低）
    updated_at = NOW(),
    updated_by = 'system'
WHERE enabled = true;

-- 2. 特别优化Qwen配置（主要provider）
UPDATE llm_provider_configs
SET 
    default_model = 'qwen-max',  -- 使用qwen-max，更稳定
    available_models = '["qwen-turbo", "qwen-plus", "qwen-max", "qwen-max-longcontext"]'::jsonb,
    max_tokens = 8000,
    temperature = 0.5,
    timeout = 180,
    updated_at = NOW(),
    updated_by = 'system'
WHERE provider_id = 'qwen' AND enabled = true;

-- 3. 优化GLM配置（fallback provider）
UPDATE llm_provider_configs
SET 
    max_tokens = 8000,
    temperature = 0.5,
    timeout = 180,
    updated_at = NOW(),
    updated_by = 'system'
WHERE provider_id = 'glm' AND enabled = true;

-- 4. 优化Doubao配置（第二fallback provider）
UPDATE llm_provider_configs
SET 
    max_tokens = 8000,
    temperature = 0.5,
    timeout = 180,
    updated_at = NOW(),
    updated_by = 'system'
WHERE provider_id = 'doubao' AND enabled = true;

-- 5. 验证更新结果
DO $$
DECLARE
  provider_record RECORD;
BEGIN
  RAISE NOTICE '=== LLM提供商配置优化结果 ===';
  RAISE NOTICE '';
  
  FOR provider_record IN 
    SELECT 
      provider_id, 
      default_model, 
      timeout, 
      max_tokens, 
      temperature,
      enabled
    FROM llm_provider_configs
    ORDER BY provider_id
  LOOP
    RAISE NOTICE '提供商: %', provider_record.provider_id;
    RAISE NOTICE '  - 默认模型: %', provider_record.default_model;
    RAISE NOTICE '  - 读取超时: %秒', provider_record.timeout;
    RAISE NOTICE '  - 最大Token: %', provider_record.max_tokens;
    RAISE NOTICE '  - Temperature: %', provider_record.temperature;
    RAISE NOTICE '  - 状态: %', CASE WHEN provider_record.enabled THEN '✅ 启用' ELSE '❌ 禁用' END;
    RAISE NOTICE '';
  END LOOP;
  
  RAISE NOTICE '=== 配置说明 ===';
  RAISE NOTICE '1. 读取超时时间: 30秒 → 180秒（3分钟）';
  RAISE NOTICE '2. 最大Token数: 2000 → 8000';
  RAISE NOTICE '3. Temperature: 0.7 → 0.5（提高稳定性）';
  RAISE NOTICE '4. Retry次数: 在代码中配置为3次（默认值）';
  RAISE NOTICE '5. 此配置适合大文件分析场景';
  RAISE NOTICE '';
  RAISE NOTICE '⚠️  重要: 必须重启应用服务才能生效！';
END $$;

