-- =====================================================
-- 用户新闻筛选预设表
-- 创建日期: 2025-11-02
-- 描述: 支持用户保存和管理个性化的新闻筛选预设
-- =====================================================

-- 1. 创建用户新闻筛选预设表
CREATE TABLE IF NOT EXISTS user_news_filter_presets (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    
    -- 预设基本信息
    preset_name VARCHAR(100) NOT NULL,  -- "我的财报追踪"、"重大事件提醒"
    preset_type VARCHAR(20) NOT NULL DEFAULT 'custom',  -- 'system' | 'custom'
    description TEXT,  -- 预设说明
    icon VARCHAR(50),  -- 图标（如 "📊", "🔔", "⭐"）
    
    -- 筛选条件（JSON 格式）
    filter_config JSONB NOT NULL DEFAULT '{}'::jsonb,
    
    -- 应用范围
    scope_type VARCHAR(20) NOT NULL DEFAULT 'global',  -- 'global' | 'symbol_specific'
    symbol_code VARCHAR(20),  -- 如果是股票级别预设，存储股票代码
    
    -- 用户偏好
    is_default BOOLEAN DEFAULT false,  -- 是否为默认预设
    is_favorite BOOLEAN DEFAULT false,  -- 是否收藏
    sort_order INTEGER DEFAULT 0,  -- 显示顺序
    
    -- 统计信息
    usage_count INTEGER DEFAULT 0,  -- 使用次数
    last_used_at TIMESTAMP,  -- 最后使用时间
    
    -- 时间戳
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- 约束：同一用户不能有重名预设（在相同scope下）
    CONSTRAINT unique_user_preset_name UNIQUE(user_id, preset_name, scope_type, symbol_code)
);

-- 2. 创建索引
CREATE INDEX IF NOT EXISTS idx_user_presets_user_id 
ON user_news_filter_presets(user_id);

CREATE INDEX IF NOT EXISTS idx_user_presets_scope 
ON user_news_filter_presets(user_id, scope_type);

CREATE INDEX IF NOT EXISTS idx_user_presets_default 
ON user_news_filter_presets(user_id, is_default) 
WHERE is_default = true;

CREATE INDEX IF NOT EXISTS idx_user_presets_symbol 
ON user_news_filter_presets(user_id, symbol_code) 
WHERE symbol_code IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_user_presets_favorite 
ON user_news_filter_presets(user_id, is_favorite) 
WHERE is_favorite = true;

CREATE INDEX IF NOT EXISTS idx_user_presets_usage 
ON user_news_filter_presets(user_id, usage_count DESC);

-- 3. 创建 GIN 索引用于 filter_config JSON 查询
CREATE INDEX IF NOT EXISTS idx_user_presets_filter_config 
ON user_news_filter_presets USING gin(filter_config);

-- 4. 创建更新时间触发器
CREATE OR REPLACE FUNCTION update_user_preset_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_update_user_preset_updated_at ON user_news_filter_presets;
CREATE TRIGGER trigger_update_user_preset_updated_at
    BEFORE UPDATE ON user_news_filter_presets
    FOR EACH ROW
    EXECUTE FUNCTION update_user_preset_updated_at();

-- 5. 添加约束检查
ALTER TABLE user_news_filter_presets 
DROP CONSTRAINT IF EXISTS check_preset_type;
ALTER TABLE user_news_filter_presets 
ADD CONSTRAINT check_preset_type 
CHECK (preset_type IN ('system', 'custom'));

ALTER TABLE user_news_filter_presets 
DROP CONSTRAINT IF EXISTS check_scope_type;
ALTER TABLE user_news_filter_presets 
ADD CONSTRAINT check_scope_type 
CHECK (scope_type IN ('global', 'symbol_specific'));

ALTER TABLE user_news_filter_presets 
DROP CONSTRAINT IF EXISTS check_symbol_code_when_specific;
ALTER TABLE user_news_filter_presets 
ADD CONSTRAINT check_symbol_code_when_specific 
CHECK (
    (scope_type = 'symbol_specific' AND symbol_code IS NOT NULL) 
    OR 
    (scope_type = 'global' AND symbol_code IS NULL)
);

-- 6. 添加表和列注释
COMMENT ON TABLE user_news_filter_presets IS '用户新闻筛选预设表 - 支持保存和管理个性化的新闻筛选条件';

COMMENT ON COLUMN user_news_filter_presets.preset_name IS '预设名称';
COMMENT ON COLUMN user_news_filter_presets.preset_type IS '预设类型: system(系统预设) | custom(用户自定义)';
COMMENT ON COLUMN user_news_filter_presets.filter_config IS 'JSON格式筛选条件: {event_types, urgency, impact_scopes, min_confidence, only_primary, source_types}';
COMMENT ON COLUMN user_news_filter_presets.scope_type IS '应用范围: global(全局所有新闻) | symbol_specific(特定股票的新闻)';
COMMENT ON COLUMN user_news_filter_presets.symbol_code IS '股票代码，仅当scope_type=symbol_specific时有值';
COMMENT ON COLUMN user_news_filter_presets.is_default IS '是否为默认预设（每个scope下只能有一个默认预设）';
COMMENT ON COLUMN user_news_filter_presets.usage_count IS '使用次数统计';
COMMENT ON COLUMN user_news_filter_presets.last_used_at IS '最后使用时间';

-- 7. 创建示例系统预设（可选，也可以在代码中hardcode）
-- 这里不插入数据，因为系统预设通过API动态返回

-- =====================================================
-- 迁移完成
-- =====================================================
COMMIT;


