-- =====================================================
-- 优化主题系统：简化匹配，使用 Primary Theme 映射
-- 创建日期: 2025-10-30
-- 描述: 简化 theme_master_data，让 LLM 的 primary_theme 直接映射
-- =====================================================
--
-- 核心优化：
-- 1. 简化 theme_master_data 表结构，移除复杂的字符串匹配字段
-- 2. 使用标准化的 category 作为主键概念
-- 3. LLM 的 primary_theme 直接对应 category
-- 4. 使用 is_primary 标记主要主题
--
-- 使用方法:
--   psql -h <host> -U postgres -d rc_database -f optimize_theme_system_primary_theme.sql
-- =====================================================

BEGIN;

-- ============================================================
-- 步骤1: 备份现有数据（如果需要）
-- ============================================================
DO $$
BEGIN
    RAISE NOTICE '==============================================';
    RAISE NOTICE 'Step 1: Backup existing theme_master_data...';
    RAISE NOTICE '==============================================';
END $$;

-- 创建备份表（如果还不存在）
CREATE TABLE IF NOT EXISTS theme_master_data_backup_20251030 AS 
SELECT * FROM theme_master_data;

-- ============================================================
-- 步骤2: 清空并重建 theme_master_data（使用标准分类）
-- ============================================================
DO $$
BEGIN
    RAISE NOTICE '==============================================';
    RAISE NOTICE 'Step 2: Rebuild theme_master_data with standard categories...';
    RAISE NOTICE '==============================================';
END $$;

-- 删除旧的预定义主题（保留用户自定义的）
DELETE FROM theme_master_data WHERE is_system = true;

-- 插入标准化的主题分类（与 LLM primary_theme 对应）
INSERT INTO theme_master_data (
    canonical_name, 
    display_name, 
    category, 
    sub_category, 
    description, 
    is_system, 
    is_active,
    priority
) VALUES
-- 核心分类（与 LLM news_classification 的 primary_theme 对应）
('technology', 'Technology', 'technology', NULL, '科技创新和技术发展', true, true, 100),
('finance', 'Finance', 'finance', NULL, '金融市场和投资动态', true, true, 95),
('economic', 'Economic', 'economic', NULL, '宏观经济政策和指标', true, true, 90),
('energy', 'Energy', 'energy', NULL, '能源资源和新能源', true, true, 85),
('healthcare', 'Healthcare', 'healthcare', NULL, '医疗健康和生物技术', true, true, 80),
('consumer', 'Consumer', 'consumer', NULL, '消费品和零售行业', true, true, 75),
('industrials', 'Industrials', 'industrials', NULL, '工业制造和基础设施', true, true, 70),
('political', 'Political', 'political', NULL, '政治政策和政府决策', true, true, 65),
('geopolitical', 'Geopolitical', 'geopolitical', NULL, '地缘政治和国际关系', true, true, 60),
('regulatory', 'Regulatory', 'regulatory', NULL, '监管政策和法规', true, true, 55),
('corporate', 'Corporate', 'corporate', NULL, '公司运营和管理', true, true, 50),
('conflict', 'Conflict', 'conflict', NULL, '冲突和军事事件', true, true, 45)

ON CONFLICT (canonical_name) 
DO UPDATE SET 
    display_name = EXCLUDED.display_name,
    category = EXCLUDED.category,
    description = EXCLUDED.description,
    is_system = EXCLUDED.is_system,
    is_active = EXCLUDED.is_active,
    priority = EXCLUDED.priority,
    updated_at = CURRENT_TIMESTAMP;

-- ============================================================
-- 步骤3: 确保 news_themes_unified 有 is_primary 字段
-- ============================================================
DO $$
BEGIN
    RAISE NOTICE '==============================================';
    RAISE NOTICE 'Step 3: Add is_primary column if not exists...';
    RAISE NOTICE '==============================================';
    
    -- 检查 is_primary 字段是否存在
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'news_themes_unified' 
        AND column_name = 'is_primary'
    ) THEN
        ALTER TABLE news_themes_unified 
        ADD COLUMN is_primary BOOLEAN DEFAULT false;
        
        RAISE NOTICE '✅ Added is_primary column to news_themes_unified';
    ELSE
        RAISE NOTICE '✅ is_primary column already exists';
    END IF;
END $$;

-- 创建索引优化查询
CREATE INDEX IF NOT EXISTS idx_news_themes_unified_primary 
ON news_themes_unified(news_id, is_primary) 
WHERE is_primary = true;

-- ============================================================
-- 步骤4: 添加约束：每个新闻只能有一个 primary theme
-- ============================================================
DO $$
BEGIN
    RAISE NOTICE '==============================================';
    RAISE NOTICE 'Step 4: Add unique constraint for primary theme...';
    RAISE NOTICE '==============================================';
END $$;

-- 先删除旧约束/索引（如果存在）
DROP INDEX IF EXISTS unique_one_primary_theme_per_news;
ALTER TABLE news_themes_unified DROP CONSTRAINT IF EXISTS unique_one_primary_theme_per_news;

-- 创建唯一部分索引：每个新闻只能有一个 primary theme
-- 注意：必须使用 CREATE UNIQUE INDEX，而不是 UNIQUE 约束
CREATE UNIQUE INDEX IF NOT EXISTS unique_one_primary_theme_per_news
ON news_themes_unified(news_id)
WHERE is_primary = true;

DO $$
BEGIN
    RAISE NOTICE '✅ Added unique index for primary theme';
    RAISE NOTICE '   Each news can only have ONE record with is_primary = true';
END $$;

-- ============================================================
-- 步骤5: 创建辅助函数：根据 category 查找 theme_master_id
-- ============================================================
CREATE OR REPLACE FUNCTION get_theme_master_id_by_category(p_category VARCHAR)
RETURNS INTEGER AS $$
DECLARE
    v_theme_id INTEGER;
BEGIN
    -- 查找对应的 theme_master_id
    SELECT id INTO v_theme_id
    FROM theme_master_data
    WHERE category = p_category
      AND is_active = true
      AND is_system = true
    LIMIT 1;
    
    RETURN v_theme_id;
END;
$$ LANGUAGE plpgsql STABLE;

COMMENT ON FUNCTION get_theme_master_id_by_category IS 
'根据 category（如 technology、finance）查找对应的 theme_master_id';

-- ============================================================
-- 步骤6: 创建辅助函数：插入或更新 primary theme
-- ============================================================
CREATE OR REPLACE FUNCTION upsert_primary_theme(
    p_news_id INTEGER,
    p_category VARCHAR,
    p_confidence_score NUMERIC DEFAULT 0.8000,
    p_metadata JSONB DEFAULT '{}'::jsonb
)
RETURNS INTEGER AS $$
DECLARE
    v_theme_master_id INTEGER;
    v_theme_record_id INTEGER;
BEGIN
    -- 1. 查找对应的 theme_master_id
    v_theme_master_id := get_theme_master_id_by_category(p_category);
    
    IF v_theme_master_id IS NULL THEN
        RAISE EXCEPTION 'Invalid category: %', p_category;
    END IF;
    
    -- 2. 先取消该新闻的其他 primary theme
    UPDATE news_themes_unified 
    SET is_primary = false
    WHERE news_id = p_news_id 
      AND is_primary = true;
    
    -- 3. 插入或更新当前的 primary theme
    INSERT INTO news_themes_unified (
        news_id,
        theme_master_id,
        theme_name,
        canonical_theme_name,
        source_type,
        confidence_score,
        is_primary,
        is_active,
        metadata,
        market
    ) VALUES (
        p_news_id,
        v_theme_master_id,
        p_category,
        p_category,
        'llm_classification',
        p_confidence_score,
        true,  -- 标记为主要主题
        true,
        p_metadata,
        'GLOBAL'  -- 默认市场值
    )
    ON CONFLICT (news_id, source_type, theme_name, market)
    DO UPDATE SET
        theme_master_id = EXCLUDED.theme_master_id,
        canonical_theme_name = EXCLUDED.canonical_theme_name,
        confidence_score = EXCLUDED.confidence_score,
        is_primary = true,
        is_active = true,
        metadata = EXCLUDED.metadata,
        updated_at = CURRENT_TIMESTAMP
    RETURNING id INTO v_theme_record_id;
    
    RETURN v_theme_record_id;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION upsert_primary_theme IS 
'插入或更新新闻的 primary theme，自动处理 is_primary 标记';

-- ============================================================
-- 步骤7: 清理旧触发器（如果存在）
-- ============================================================
DO $$
BEGIN
    RAISE NOTICE '==============================================';
    RAISE NOTICE 'Step 7: Remove old triggers...';
    RAISE NOTICE '==============================================';
END $$;

-- 删除旧触发器
DROP TRIGGER IF EXISTS trigger_auto_normalize_theme_name ON news_themes_unified;
DROP TRIGGER IF EXISTS trigger_auto_normalize_theme_name_v2 ON news_themes_unified;

-- 删除旧触发器函数
DROP FUNCTION IF EXISTS auto_normalize_theme_name();
DROP FUNCTION IF EXISTS auto_normalize_theme_name_v2();

DO $$
BEGIN
    RAISE NOTICE '✅ Old triggers and functions removed';
    RAISE NOTICE '';
    RAISE NOTICE '⚠️  Note: Theme mapping is now handled by application code';
    RAISE NOTICE '   Use ThemeMappingService.upsert_primary_theme() in Python';
END $$;

-- ============================================================
-- 步骤7.1: 创建触发器 - 自动同步 theme_master_id
-- ============================================================
DO $$
BEGIN
    RAISE NOTICE '==============================================';
    RAISE NOTICE 'Step 7.1: Create sync trigger...';
    RAISE NOTICE '==============================================';
END $$;

CREATE OR REPLACE FUNCTION sync_theme_master_id_to_siblings()
RETURNS TRIGGER AS $$
BEGIN
    -- 当设置了 primary theme 后，同步 theme_master_id 到同一新闻的其他记录
    IF NEW.is_primary = true AND NEW.theme_master_id IS NOT NULL THEN
        UPDATE news_themes_unified 
        SET theme_master_id = NEW.theme_master_id,
            updated_at = CURRENT_TIMESTAMP
        WHERE news_id = NEW.news_id 
          AND id != NEW.id  -- 排除当前记录
          AND theme_master_id IS NULL
          AND is_active = true;
        
        RAISE NOTICE '  🔗 Synced theme_master_id=% to % sibling records for news_id=%', 
            NEW.theme_master_id, 
            (SELECT COUNT(*) FROM news_themes_unified 
             WHERE news_id = NEW.news_id 
               AND id != NEW.id 
               AND theme_master_id = NEW.theme_master_id),
            NEW.news_id;
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_sync_theme_master_id ON news_themes_unified;
CREATE TRIGGER trigger_sync_theme_master_id
    AFTER INSERT OR UPDATE OF is_primary, theme_master_id ON news_themes_unified
    FOR EACH ROW
    WHEN (NEW.is_primary = true AND NEW.theme_master_id IS NOT NULL)
    EXECUTE FUNCTION sync_theme_master_id_to_siblings();

COMMENT ON FUNCTION sync_theme_master_id_to_siblings IS 
'触发器函数 - 当设置 primary theme 后，自动同步 theme_master_id 到同一新闻的其他记录';

DO $$
BEGIN
    RAISE NOTICE '✅ Sync trigger created';
END $$;

-- ============================================================
-- 步骤7.2: 同步历史数据的 theme_master_id
-- ============================================================
DO $$
DECLARE
    v_synced_count INTEGER := 0;
BEGIN
    RAISE NOTICE '==============================================';
    RAISE NOTICE 'Step 7.2: Sync historical data...';
    RAISE NOTICE '==============================================';
    
    -- 将已有 primary theme 的 theme_master_id 同步到同一新闻的其他记录
    UPDATE news_themes_unified ntu1
    SET theme_master_id = (
        SELECT theme_master_id 
        FROM news_themes_unified 
        WHERE news_id = ntu1.news_id 
          AND is_primary = true 
          AND theme_master_id IS NOT NULL
        LIMIT 1
    ),
    updated_at = CURRENT_TIMESTAMP
    WHERE ntu1.theme_master_id IS NULL
      AND ntu1.is_active = true
      AND EXISTS (
          SELECT 1 
          FROM news_themes_unified ntu2
          WHERE ntu2.news_id = ntu1.news_id 
            AND ntu2.is_primary = true 
            AND ntu2.theme_master_id IS NOT NULL
      );
    
    GET DIAGNOSTICS v_synced_count = ROW_COUNT;
    
    IF v_synced_count > 0 THEN
        RAISE NOTICE '🔗 已同步 % 条历史记录的 theme_master_id', v_synced_count;
    ELSE
        RAISE NOTICE '  No historical records to sync';
    END IF;
END $$;

-- ============================================================
-- 步骤8: 创建视图：新闻的 primary theme
-- ============================================================
CREATE OR REPLACE VIEW v_news_primary_themes AS
SELECT 
    ntu.news_id,
    ntu.theme_master_id,
    tmd.category as primary_theme,
    tmd.display_name as theme_display_name,
    tmd.description as theme_description,
    ntu.confidence_score,
    ntu.metadata,
    ntu.created_at as assigned_at,
    ntu.updated_at
FROM news_themes_unified ntu
INNER JOIN theme_master_data tmd ON ntu.theme_master_id = tmd.id
WHERE ntu.is_primary = true
  AND ntu.is_active = true;

COMMENT ON VIEW v_news_primary_themes IS 
'新闻主主题视图 - 每条新闻对应一个 primary theme';

-- ============================================================
-- 步骤9: 更新现有视图（如果需要）
-- ============================================================
CREATE OR REPLACE VIEW v_news_themes_full AS
SELECT 
    n.id as news_id,
    n.title as news_title,
    n.date as news_date,
    n.source as news_source,
    n.analysis_status,
    
    ntu.id as theme_record_id,
    ntu.theme_name,
    ntu.canonical_theme_name,
    ntu.source_type,
    ntu.confidence_score,
    ntu.quality_score,
    ntu.rank_in_news,
    ntu.market,
    ntu.is_verified,
    ntu.is_primary,  -- 新增：标记是否为主主题
    
    tmd.canonical_name as master_canonical_name,
    tmd.display_name as master_display_name,
    tmd.category as theme_category,
    tmd.sub_category as theme_sub_category,
    
    ntu.created_at as theme_created_at
FROM news n
INNER JOIN news_themes_unified ntu ON n.id = ntu.news_id
LEFT JOIN theme_master_data tmd ON ntu.theme_master_id = tmd.id
WHERE ntu.is_active = true
ORDER BY n.date DESC, ntu.news_id, ntu.is_primary DESC, ntu.quality_score DESC NULLS LAST;

COMMENT ON VIEW v_news_themes_full IS 
'新闻主题全景视图 - 显示所有来源的主题详情，primary theme 排在前面';

-- ============================================================
-- 步骤10: 输出结果
-- ============================================================
DO $$
DECLARE
    v_theme_count INTEGER;
    v_primary_count INTEGER;
BEGIN
    -- 统计主题数量
    SELECT COUNT(*) INTO v_theme_count
    FROM theme_master_data
    WHERE is_system = true AND is_active = true;
    
    -- 统计已标记的 primary theme 数量
    SELECT COUNT(*) INTO v_primary_count
    FROM news_themes_unified
    WHERE is_primary = true;
    
    RAISE NOTICE '==============================================';
    RAISE NOTICE 'Theme System Optimization Completed!';
    RAISE NOTICE '==============================================';
    RAISE NOTICE 'Standard Categories: %', v_theme_count;
    RAISE NOTICE 'News with Primary Theme: %', v_primary_count;
    RAISE NOTICE '';
    RAISE NOTICE 'New Functions:';
    RAISE NOTICE '  - get_theme_master_id_by_category(category)';
    RAISE NOTICE '  - upsert_primary_theme(news_id, category, ...)';
    RAISE NOTICE '  - sync_theme_master_id_to_siblings() [触发器]';
    RAISE NOTICE '';
    RAISE NOTICE 'New Views:';
    RAISE NOTICE '  - v_news_primary_themes';
    RAISE NOTICE '';
    RAISE NOTICE 'New Triggers:';
    RAISE NOTICE '  - trigger_sync_theme_master_id (自动同步 theme_master_id)';
    RAISE NOTICE '';
    RAISE NOTICE 'Usage Example:';
    RAISE NOTICE '  SELECT upsert_primary_theme(123, ''technology'', 0.85);';
    RAISE NOTICE '';
    RAISE NOTICE 'Feature: Auto Sync theme_master_id';
    RAISE NOTICE '  当设置 primary theme 后，自动同步到同一新闻的其他记录';
    RAISE NOTICE '==============================================';
END $$;

COMMIT;


