-- =====================================================
-- Prompt Template 模块数据库迁移脚本
-- 创建日期: 2025-10-01
-- 描述: 创建LLM Prompt模板管理相关的所有表
-- =====================================================

-- 1. Prompt模板分类表
CREATE TABLE IF NOT EXISTS llm_prompt_categories (
    id SERIAL PRIMARY KEY,
    category_code VARCHAR(50) UNIQUE NOT NULL,
    category_name VARCHAR(100) NOT NULL,
    description TEXT,
    icon VARCHAR(50),
    sort_order INTEGER DEFAULT 0,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. Prompt模板主表
CREATE TABLE IF NOT EXISTS llm_prompt_templates (
    id SERIAL PRIMARY KEY,
    template_code VARCHAR(100) UNIQUE NOT NULL,
    template_name VARCHAR(200) NOT NULL,
    category_id INTEGER REFERENCES llm_prompt_categories(id) ON DELETE CASCADE,
    
    -- 模板内容
    system_prompt TEXT,
    user_prompt_template TEXT NOT NULL,
    
    -- 变量定义
    variables JSONB DEFAULT '{}',
    
    -- 预期输出格式
    output_format JSONB DEFAULT '{}',
    
    -- 模板配置
    recommended_provider VARCHAR(50),
    recommended_model VARCHAR(100),
    temperature DECIMAL(3, 2) DEFAULT 0.7,
    max_tokens INTEGER DEFAULT 2000,
    
    -- 权限和范围
    scope VARCHAR(20) NOT NULL DEFAULT 'system',
    created_by INTEGER REFERENCES users(id) ON DELETE SET NULL,
    is_public BOOLEAN DEFAULT false,
    
    -- 版本和状态
    version INTEGER DEFAULT 1,
    parent_template_id INTEGER REFERENCES llm_prompt_templates(id) ON DELETE SET NULL,
    is_active BOOLEAN DEFAULT true,
    is_default BOOLEAN DEFAULT false,
    
    -- 使用统计
    usage_count INTEGER DEFAULT 0,
    success_rate DECIMAL(5, 2),
    avg_cost DECIMAL(10, 6),
    
    -- 元数据
    tags JSONB DEFAULT '[]',
    description TEXT,
    example_input JSONB DEFAULT '{}',
    example_output TEXT,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT valid_scope CHECK (scope IN ('system', 'user', 'shared')),
    CONSTRAINT valid_temperature CHECK (temperature >= 0 AND temperature <= 2),
    CONSTRAINT valid_success_rate CHECK (success_rate >= 0 AND success_rate <= 100)
);

-- 3. 用户模板偏好表
CREATE TABLE IF NOT EXISTS llm_user_template_preferences (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    category_id INTEGER NOT NULL REFERENCES llm_prompt_categories(id) ON DELETE CASCADE,
    preferred_template_id INTEGER REFERENCES llm_prompt_templates(id) ON DELETE SET NULL,
    
    -- 用户自定义参数覆盖
    custom_parameters JSONB DEFAULT '{}',
    
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(user_id, category_id)
);

-- 4. 模板使用日志表
CREATE TABLE IF NOT EXISTS llm_prompt_usage_logs (
    id SERIAL PRIMARY KEY,
    template_id INTEGER REFERENCES llm_prompt_templates(id) ON DELETE CASCADE,
    user_id INTEGER REFERENCES users(id) ON DELETE SET NULL,
    
    -- 执行信息
    provider VARCHAR(50),
    model VARCHAR(100),
    
    -- 输入输出
    rendered_prompt TEXT,
    variables_data JSONB DEFAULT '{}',
    response_content TEXT,
    
    -- 性能指标
    tokens_used INTEGER,
    cost DECIMAL(10, 6),
    response_time_ms INTEGER,
    
    -- 质量评估
    success BOOLEAN DEFAULT true,
    error_message TEXT,
    user_feedback INTEGER,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT valid_user_feedback CHECK (user_feedback >= 1 AND user_feedback <= 5)
);

-- 5. 模板变量库（用于跨模板共享变量定义）
CREATE TABLE IF NOT EXISTS llm_template_variables_library (
    id SERIAL PRIMARY KEY,
    variable_name VARCHAR(100) UNIQUE NOT NULL,
    variable_type VARCHAR(50) NOT NULL,
    description TEXT,
    default_value TEXT,
    validation_rules JSONB DEFAULT '{}',
    examples JSONB DEFAULT '[]',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT valid_variable_type CHECK (variable_type IN ('string', 'integer', 'float', 'boolean', 'array', 'object'))
);

-- 创建索引优化查询性能
CREATE INDEX IF NOT EXISTS idx_templates_category ON llm_prompt_templates(category_id);
CREATE INDEX IF NOT EXISTS idx_templates_scope ON llm_prompt_templates(scope, is_active);
CREATE INDEX IF NOT EXISTS idx_templates_created_by ON llm_prompt_templates(created_by);
CREATE INDEX IF NOT EXISTS idx_templates_code ON llm_prompt_templates(template_code);
CREATE INDEX IF NOT EXISTS idx_user_prefs_user ON llm_user_template_preferences(user_id);
CREATE INDEX IF NOT EXISTS idx_user_prefs_category ON llm_user_template_preferences(category_id);
CREATE INDEX IF NOT EXISTS idx_usage_logs_template ON llm_prompt_usage_logs(template_id);
CREATE INDEX IF NOT EXISTS idx_usage_logs_user ON llm_prompt_usage_logs(user_id);
CREATE INDEX IF NOT EXISTS idx_usage_logs_created ON llm_prompt_usage_logs(created_at DESC);

-- 创建更新时间戳触发器函数
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- 为相关表添加自动更新触发器
DROP TRIGGER IF EXISTS update_llm_prompt_categories_updated_at ON llm_prompt_categories;
CREATE TRIGGER update_llm_prompt_categories_updated_at
    BEFORE UPDATE ON llm_prompt_categories
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_llm_prompt_templates_updated_at ON llm_prompt_templates;
CREATE TRIGGER update_llm_prompt_templates_updated_at
    BEFORE UPDATE ON llm_prompt_templates
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_llm_user_template_preferences_updated_at ON llm_user_template_preferences;
CREATE TRIGGER update_llm_user_template_preferences_updated_at
    BEFORE UPDATE ON llm_user_template_preferences
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- 添加注释
COMMENT ON TABLE llm_prompt_categories IS 'Prompt模板分类表';
COMMENT ON TABLE llm_prompt_templates IS 'Prompt模板主表，存储系统和用户的模板';
COMMENT ON TABLE llm_user_template_preferences IS '用户对不同场景的模板偏好设置';
COMMENT ON TABLE llm_prompt_usage_logs IS '模板使用日志，用于统计和优化';
COMMENT ON TABLE llm_template_variables_library IS '可复用的模板变量库';

