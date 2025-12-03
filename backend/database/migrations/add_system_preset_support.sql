-- =====================================================
-- 添加系统预设支持 - 允许 admin 编辑系统预设
-- 创建日期: 2025-12-02
-- 描述: 扩展 user_news_filter_presets 表，支持系统级预设
-- =====================================================

-- 1. 添加新字段
ALTER TABLE user_news_filter_presets 
ADD COLUMN IF NOT EXISTS is_system_global BOOLEAN DEFAULT false;

ALTER TABLE user_news_filter_presets 
ADD COLUMN IF NOT EXISTS created_by_admin_id INTEGER REFERENCES users(id) ON DELETE SET NULL;

-- 2. 修改 user_id 字段，允许 NULL（系统全局预设不属于任何用户）
ALTER TABLE user_news_filter_presets 
ALTER COLUMN user_id DROP NOT NULL;

-- 3. 添加字段注释
COMMENT ON COLUMN user_news_filter_presets.is_system_global IS '是否为系统全局预设（所有用户可见）';
COMMENT ON COLUMN user_news_filter_presets.created_by_admin_id IS '创建该系统预设的 admin 用户 ID';

-- 4. 添加约束：系统全局预设必须 user_id 为 NULL
ALTER TABLE user_news_filter_presets 
DROP CONSTRAINT IF EXISTS check_system_preset_no_user;

ALTER TABLE user_news_filter_presets 
ADD CONSTRAINT check_system_preset_no_user 
CHECK (
    (is_system_global = true AND user_id IS NULL) 
    OR 
    (is_system_global = false AND user_id IS NOT NULL)
);

-- 5. 添加索引
CREATE INDEX IF NOT EXISTS idx_user_presets_system_global 
ON user_news_filter_presets(is_system_global) 
WHERE is_system_global = true;

CREATE INDEX IF NOT EXISTS idx_user_presets_created_by_admin 
ON user_news_filter_presets(created_by_admin_id) 
WHERE created_by_admin_id IS NOT NULL;

-- 6. 插入默认系统预设（从代码迁移到数据库）
INSERT INTO user_news_filter_presets (
    user_id,
    preset_name,
    preset_type,
    description,
    icon,
    filter_config,
    scope_type,
    is_system_global,
    is_default,
    is_favorite,
    sort_order
) VALUES 
    (NULL, '⭐ 重要新闻', 'system', '紧急和及时的高相关度新闻', '⭐', 
     '{"urgency": ["urgent", "timely"], "min_confidence": 0.7, "only_primary": true}'::jsonb,
     'global', true, false, false, 1),
    
    (NULL, '📊 财报季', 'system', '财报发布和业绩相关新闻', '📊',
     '{"event_types": ["earnings_report", "financial_results"], "min_confidence": 0.6}'::jsonb,
     'global', true, false, false, 2),
    
    (NULL, '🤝 并购重组', 'system', '并购、重组、战略投资相关', '🤝',
     '{"event_types": ["merger_acquisition", "strategic_investment"], "urgency": ["urgent", "timely"]}'::jsonb,
     'global', true, false, false, 3),
    
    (NULL, '⚖️ 监管合规', 'system', '监管处罚、合规问题', '⚖️',
     '{"event_types": ["regulatory_news", "legal_issue"], "urgency": ["urgent", "timely"]}'::jsonb,
     'global', true, false, false, 4),
    
    (NULL, '🚀 产品创新', 'system', '新产品、新技术发布', '🚀',
     '{"event_types": ["product_launch", "technology_innovation"]}'::jsonb,
     'global', true, false, false, 5)
ON CONFLICT DO NOTHING;

-- 7. 更新表注释
COMMENT ON TABLE user_news_filter_presets IS '用户新闻筛选预设表 - 支持用户自定义预设和系统全局预设（admin 可编辑）';

-- =====================================================
-- 迁移完成
-- =====================================================
COMMIT;

