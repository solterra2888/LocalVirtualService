-- 为processing_stats表添加模型信息字段
ALTER TABLE processing_stats ADD COLUMN IF NOT EXISTS llm_model VARCHAR(255);
ALTER TABLE processing_stats ADD COLUMN IF NOT EXISTS llm_provider VARCHAR(255);

-- 添加注释
COMMENT ON COLUMN processing_stats.llm_model IS 'LLM模型名称';
COMMENT ON COLUMN processing_stats.llm_provider IS 'LLM提供商名称';










