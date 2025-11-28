-- =====================================================
-- 优化LLM超时和重试配置以避免read timeout错误
-- =====================================================
-- 用途: 
--   1. 增加timeout以支持大文件和复杂分析
--   2. 减少retry_count以避免总体等待时间过长
--   3. 适配文件分析场景的实际需求
-- 创建时间: 2025-11-27
-- 问题: Read timeout errors causing "All providers failed"
-- =====================================================

-- 策略说明:
-- 文件分析通常需要较长的处理时间，因为：
-- 1. 文件内容可能很长（数万字符）
-- 2. 需要深度分析和结构化输出
-- 3. LLM生成响应需要时间
--
-- 超时配置策略:
-- - Timeout: 180秒（3分钟）- 给LLM足够的时间处理复杂内容
-- - Retry: 2次 - 减少重试次数，避免总体时间过长
-- - 总最大等待时间: 180秒 * 2次 = 6分钟/provider
-- - 3个providers的fallback: 最多18分钟（实际上通常第一个就会成功）

-- 1. 更新所有启用的提供商配置
UPDATE llm_provider_configs
SET 
    timeout = 180,              -- 读取超时3分钟
    retry_count = 2,            -- 重试2次（减少总体等待时间）
    max_tokens = 8000,          -- 增加最大token数以支持长输出
    updated_at = NOW(),
    updated_by = 'system'
WHERE enabled = true;

-- 2. 特别优化Qwen配置（主要provider）
UPDATE llm_provider_configs
SET 
    default_model = 'qwen-max',  -- 使用qwen-max，更稳定
    available_models = '["qwen-turbo", "qwen-plus", "qwen-max", "qwen-max-longcontext"]'::jsonb,
    max_tokens = 8000,
    temperature = 0.5,          -- 稍微降低温度以提高稳定性
    timeout = 180,
    retry_count = 2,
    updated_at = NOW(),
    updated_by = 'system'
WHERE provider_id = 'qwen' AND enabled = true;

-- 3. 优化GLM配置（fallback provider）
UPDATE llm_provider_configs
SET 
    max_tokens = 8000,
    temperature = 0.5,
    timeout = 180,
    retry_count = 2,
    updated_at = NOW(),
    updated_by = 'system'
WHERE provider_id = 'glm' AND enabled = true;

-- 4. 优化Doubao配置（第二fallback provider）
UPDATE llm_provider_configs
SET 
    max_tokens = 8000,
    temperature = 0.5,
    timeout = 180,
    retry_count = 2,
    updated_at = NOW(),
    updated_by = 'system'
WHERE provider_id = 'doubao' AND enabled = true;

-- 5. 验证更新结果
DO $$
DECLARE
  provider_record RECORD;
  total_max_wait_time INTEGER;
BEGIN
  RAISE NOTICE '=== LLM提供商超时和重试配置优化结果 ===';
  RAISE NOTICE '';
  
  FOR provider_record IN 
    SELECT 
      provider_id, 
      default_model, 
      timeout, 
      retry_count,
      max_tokens, 
      temperature,
      enabled
    FROM llm_provider_configs
    ORDER BY provider_id
  LOOP
    -- 计算单个provider的最大等待时间
    total_max_wait_time := provider_record.timeout * (provider_record.retry_count + 1);
    
    RAISE NOTICE '提供商: %', provider_record.provider_id;
    RAISE NOTICE '  - 默认模型: %', provider_record.default_model;
    RAISE NOTICE '  - 读取超时: %秒', provider_record.timeout;
    RAISE NOTICE '  - 重试次数: %次', provider_record.retry_count;
    RAISE NOTICE '  - 最大Token: %', provider_record.max_tokens;
    RAISE NOTICE '  - Temperature: %', provider_record.temperature;
    RAISE NOTICE '  - 单provider最大等待: %秒 (%.1f分钟)', 
      total_max_wait_time, 
      total_max_wait_time::float / 60;
    RAISE NOTICE '  - 状态: %', CASE WHEN provider_record.enabled THEN '✅ 启用' ELSE '❌ 禁用' END;
    RAISE NOTICE '';
  END LOOP;
  
  RAISE NOTICE '=== 配置说明 ===';
  RAISE NOTICE '1. 每次请求超时时间: 180秒（3分钟）';
  RAISE NOTICE '2. 失败后重试: 2次';
  RAISE NOTICE '3. 单个provider最大等待: 540秒（9分钟）';
  RAISE NOTICE '4. 多个providers fallback: 自动切换到下一个';
  RAISE NOTICE '5. 此配置适合大文件分析场景';
END $$;

