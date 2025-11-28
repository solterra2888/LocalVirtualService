-- =====================================================
-- 清理 Global Settings - 简化配置架构
-- =====================================================
-- 用途: 移除多余的参数默认值配置，只保留策略配置
-- 创建时间: 2025-11-28
-- 说明: 
--   Global Settings 只负责策略（provider选择、fallback等）
--   参数默认值由 Provider Config 提供
--   任务特定优化由 Template Recommendations 提供
-- =====================================================

-- 1. 删除多余的参数默认值配置（如果存在）
DELETE FROM llm_global_settings
WHERE setting_key IN (
    'default_temperature',
    'default_max_tokens',
    'default_top_p',
    'default_timeout',
    'default_retry_count',
    'global_timeout',  -- 这个也要删除，timeout应该由provider决定
    'global_retry_count'  -- 重试次数也由provider决定
);

-- 2. 确保必要的策略配置存在
INSERT INTO llm_global_settings (setting_key, setting_value, description, updated_by) VALUES
    ('default_provider', '"qwen"', '默认LLM提供商', 'system'),
    ('fallback_enabled', 'true', '是否启用故障转移', 'system'),
    ('fallback_order', '["qwen", "doubao", "glm"]', '故障转移顺序', 'system'),
    ('enable_logging', 'true', '是否启用详细日志', 'system'),
    ('enable_monitoring', 'true', '是否启用性能监控', 'system')
ON CONFLICT (setting_key) DO UPDATE SET
    updated_at = NOW();

-- 3. 验证最终配置
DO $$
DECLARE
  setting_record RECORD;
BEGIN
  RAISE NOTICE '=== Global Settings 配置清理完成 ===';
  RAISE NOTICE '';
  RAISE NOTICE '保留的策略配置：';
  
  FOR setting_record IN 
    SELECT 
      setting_key, 
      setting_value, 
      description
    FROM llm_global_settings
    ORDER BY setting_key
  LOOP
    RAISE NOTICE '  ✓ % = %', setting_record.setting_key, setting_record.setting_value;
    RAISE NOTICE '    说明: %', setting_record.description;
  END LOOP;
  
  RAISE NOTICE '';
  RAISE NOTICE '=== 配置架构说明 ===';
  RAISE NOTICE '1. Global Settings：只负责系统级策略';
  RAISE NOTICE '   - 选择默认provider (default_provider)';
  RAISE NOTICE '   - 故障转移策略 (fallback_enabled, fallback_order)';
  RAISE NOTICE '   - 系统行为开关 (enable_logging, enable_monitoring)';
  RAISE NOTICE '';
  RAISE NOTICE '2. Provider Config：负责参数默认值';
  RAISE NOTICE '   - 每个provider的 default_model, temperature, max_tokens, timeout 等';
  RAISE NOTICE '   - 这些是provider级别的默认值';
  RAISE NOTICE '';
  RAISE NOTICE '3. Template Recommendations：负责任务特定优化';
  RAISE NOTICE '   - 每个模板的 recommended_model, temperature, max_tokens 等';
  RAISE NOTICE '   - 这些会覆盖provider的默认值';
  RAISE NOTICE '';
  RAISE NOTICE '配置优先级：Runtime Parameters > Template > Provider > (无Global参数默认值)';
  RAISE NOTICE '';
  RAISE NOTICE '⚠️  重要: 必须重启应用服务才能生效！';
END $$;

