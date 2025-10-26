-- =====================================================
-- Theme Management Features Migration
-- 创建日期: 2025-10-26
-- 描述: 添加投资主题管理功能字段
--      1. 启用/禁用匹配功能
--      2. 软删除功能
--      3. 版本控制（编辑冲突检测）
-- =====================================================

-- ============================================================
-- 步骤1: 为主题主表添加管理字段
-- ============================================================

-- 1.1 添加启用/禁用字段
ALTER TABLE futu_investment_themes
ADD COLUMN IF NOT EXISTS is_enabled_for_matching BOOLEAN DEFAULT TRUE NOT NULL;

COMMENT ON COLUMN futu_investment_themes.is_enabled_for_matching IS 
'是否参与新闻匹配：TRUE=参与匹配，FALSE=不参与（用户可在页面配置）';

-- 1.2 添加软删除字段
ALTER TABLE futu_investment_themes
ADD COLUMN IF NOT EXISTS is_deleted BOOLEAN DEFAULT FALSE NOT NULL,
ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMP,
ADD COLUMN IF NOT EXISTS deleted_by VARCHAR(100);

COMMENT ON COLUMN futu_investment_themes.is_deleted IS 
'是否已删除（软删除）：TRUE=已删除，FALSE=正常。删除后爬虫不会重新创建';

COMMENT ON COLUMN futu_investment_themes.deleted_at IS 
'删除时间';

COMMENT ON COLUMN futu_investment_themes.deleted_by IS 
'删除操作的用户名';

-- ============================================================
-- 步骤2: 为向量表添加版本控制字段
-- ============================================================

-- 2.1 添加版本号（用于乐观锁）
ALTER TABLE investment_theme_embeddings
ADD COLUMN IF NOT EXISTS version INTEGER DEFAULT 1 NOT NULL;

COMMENT ON COLUMN investment_theme_embeddings.version IS 
'版本号，用于乐观锁控制并发编辑';

-- 2.2 添加手动编辑追踪字段
ALTER TABLE investment_theme_embeddings
ADD COLUMN IF NOT EXISTS last_manual_edit_at TIMESTAMP,
ADD COLUMN IF NOT EXISTS last_manual_edit_by VARCHAR(100);

COMMENT ON COLUMN investment_theme_embeddings.last_manual_edit_at IS 
'最后一次手动编辑描述的时间';

COMMENT ON COLUMN investment_theme_embeddings.last_manual_edit_by IS 
'最后一次手动编辑描述的用户名';

-- ============================================================
-- 步骤3: 创建索引
-- ============================================================

-- 3.1 主题启用状态索引（加速匹配查询）
CREATE INDEX IF NOT EXISTS idx_futu_themes_enabled 
ON futu_investment_themes(is_enabled_for_matching) 
WHERE is_enabled_for_matching = TRUE;

-- 3.2 软删除状态索引（加速非删除数据查询）
CREATE INDEX IF NOT EXISTS idx_futu_themes_deleted 
ON futu_investment_themes(is_deleted) 
WHERE is_deleted = FALSE;

-- 3.3 组合索引（同时过滤删除状态和启用状态）
CREATE INDEX IF NOT EXISTS idx_futu_themes_active 
ON futu_investment_themes(market, is_deleted, is_enabled_for_matching) 
WHERE is_deleted = FALSE;

-- ============================================================
-- 步骤4: 更新现有数据（确保兼容性）
-- ============================================================

-- 4.1 将所有现有主题设为启用状态
UPDATE futu_investment_themes 
SET is_enabled_for_matching = TRUE 
WHERE is_enabled_for_matching IS NULL;

-- 4.2 将所有现有主题设为未删除状态
UPDATE futu_investment_themes 
SET is_deleted = FALSE 
WHERE is_deleted IS NULL;

-- 4.3 将所有现有向量设置版本号
UPDATE investment_theme_embeddings 
SET version = 1 
WHERE version IS NULL OR version = 0;

-- ============================================================
-- 步骤5: 创建辅助视图（便于查询活跃主题）
-- ============================================================

-- 活跃主题视图（未删除 + 已启用）
CREATE OR REPLACE VIEW active_investment_themes AS
SELECT 
    t.id,
    t.theme_id,
    t.theme_name,
    t.market,
    t.theme_url,
    t.change_percent,
    t.stock_count,
    t.top_stock,
    t.top_stock_change,
    t.is_enabled_for_matching,
    t.created_at,
    t.updated_at,
    t.description,
    t.description_source,
    t.description_updated_at,
    e.embedding_model,
    e.version as embedding_version,
    e.last_manual_edit_at,
    e.last_manual_edit_by,
    CASE WHEN e.theme_id IS NOT NULL THEN TRUE ELSE FALSE END as has_embedding
FROM futu_investment_themes t
LEFT JOIN investment_theme_embeddings e 
    ON t.theme_id = e.theme_id AND t.market = e.market
WHERE t.is_deleted = FALSE;

COMMENT ON VIEW active_investment_themes IS 
'活跃投资主题视图：包含未删除的主题及其向量化状态';

-- ============================================================
-- 步骤6: 数据完整性检查函数
-- ============================================================

-- 函数：检查主题是否可以被启用（必须已向量化）
CREATE OR REPLACE FUNCTION can_enable_theme(p_theme_id VARCHAR, p_market VARCHAR)
RETURNS BOOLEAN AS $$
DECLARE
    has_embedding BOOLEAN;
BEGIN
    SELECT EXISTS(
        SELECT 1 FROM investment_theme_embeddings 
        WHERE theme_id = p_theme_id AND market = p_market
    ) INTO has_embedding;
    
    RETURN has_embedding;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION can_enable_theme IS 
'检查主题是否可以被启用（必须已生成向量）';

-- ============================================================
-- 步骤7: 触发器（可选：自动同步删除状态）
-- ============================================================

-- 触发器函数：主题删除时自动级联软删除向量
CREATE OR REPLACE FUNCTION sync_theme_deletion()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.is_deleted = TRUE AND OLD.is_deleted = FALSE THEN
        -- 记录日志（可选）
        RAISE NOTICE 'Theme % in market % marked as deleted', NEW.theme_id, NEW.market;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 创建触发器
DROP TRIGGER IF EXISTS trigger_sync_theme_deletion ON futu_investment_themes;
CREATE TRIGGER trigger_sync_theme_deletion
    AFTER UPDATE OF is_deleted ON futu_investment_themes
    FOR EACH ROW
    WHEN (NEW.is_deleted = TRUE AND OLD.is_deleted = FALSE)
    EXECUTE FUNCTION sync_theme_deletion();

COMMENT ON TRIGGER trigger_sync_theme_deletion ON futu_investment_themes IS 
'主题软删除时的同步触发器';

-- ============================================================
-- 步骤8: 权限和安全
-- ============================================================

-- 确保只有应用角色可以修改这些字段
-- （根据实际数据库角色配置调整）
-- REVOKE UPDATE (is_deleted, deleted_at, deleted_by) ON futu_investment_themes FROM public;
-- GRANT UPDATE (is_deleted, deleted_at, deleted_by) ON futu_investment_themes TO rc_app_role;

-- ============================================================
-- Migration完成
-- ============================================================

-- 验证语句
DO $$
BEGIN
    RAISE NOTICE '============================================';
    RAISE NOTICE 'Theme Management Features Migration Complete';
    RAISE NOTICE '============================================';
    RAISE NOTICE 'Added columns:';
    RAISE NOTICE '  - futu_investment_themes.is_enabled_for_matching';
    RAISE NOTICE '  - futu_investment_themes.is_deleted';
    RAISE NOTICE '  - futu_investment_themes.deleted_at';
    RAISE NOTICE '  - futu_investment_themes.deleted_by';
    RAISE NOTICE '  - investment_theme_embeddings.version';
    RAISE NOTICE '  - investment_theme_embeddings.last_manual_edit_at';
    RAISE NOTICE '  - investment_theme_embeddings.last_manual_edit_by';
    RAISE NOTICE 'Created indexes: 3';
    RAISE NOTICE 'Created views: active_investment_themes';
    RAISE NOTICE 'Created functions: can_enable_theme';
    RAISE NOTICE 'Created triggers: trigger_sync_theme_deletion';
    RAISE NOTICE '============================================';
END $$;

