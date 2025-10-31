-- =====================================================
-- 重构新闻分类维度存储
-- 创建日期: 2025-10-30
-- 描述: 支持从 news_classification 提取多个维度（event_type、impact_scope 等）
-- =====================================================
--
-- 核心改进：
-- 1. 重命名 canonical_theme_name → llm_classification_title（存储分类维度名）
-- 2. theme_name 存储具体值
-- 3. 调整唯一约束以支持同一新闻的多个分类维度
-- 4. 更新相关视图和函数
--
-- 使用方法:
--   psql -h <host> -U postgres -d rc_database -f refactor_classification_dimensions.sql
-- =====================================================

BEGIN;

-- ============================================================
-- 步骤1: 重命名字段
-- ============================================================
DO $$
BEGIN
    RAISE NOTICE '==============================================';
    RAISE NOTICE 'Step 1: Rename canonical_theme_name to llm_classification_title';
    RAISE NOTICE '==============================================';
    
    -- 检查字段是否已重命名
    IF EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'news_themes_unified' 
        AND column_name = 'canonical_theme_name'
    ) THEN
        ALTER TABLE news_themes_unified 
        RENAME COLUMN canonical_theme_name TO llm_classification_title;
        
        RAISE NOTICE '✅ Renamed canonical_theme_name to llm_classification_title';
    ELSE
        RAISE NOTICE '✅ Field already renamed';
    END IF;
END $$;

-- ============================================================
-- 步骤2: 更新索引
-- ============================================================
DO $$
BEGIN
    RAISE NOTICE '==============================================';
    RAISE NOTICE 'Step 2: Update indexes';
    RAISE NOTICE '==============================================';
END $$;

-- 删除旧索引
DROP INDEX IF EXISTS idx_news_themes_unified_canonical_name;

-- 创建新索引
CREATE INDEX IF NOT EXISTS idx_news_themes_unified_classification_title 
ON news_themes_unified(llm_classification_title);

CREATE INDEX IF NOT EXISTS idx_news_themes_unified_classification_source 
ON news_themes_unified(news_id, source_type, llm_classification_title) 
WHERE source_type = 'llm_classification';

-- ============================================================
-- 步骤3: 调整唯一约束
-- ============================================================
DO $$
BEGIN
    RAISE NOTICE '==============================================';
    RAISE NOTICE 'Step 3: Update unique constraints';
    RAISE NOTICE '==============================================';
    
    -- 删除旧约束
    ALTER TABLE news_themes_unified 
    DROP CONSTRAINT IF EXISTS news_themes_unified_news_id_source_type_theme_name_market_key;
    
    -- 添加新约束（包含 llm_classification_title）
    ALTER TABLE news_themes_unified 
    ADD CONSTRAINT unique_news_classification_dimension 
    UNIQUE (news_id, source_type, llm_classification_title, theme_name, market);
    
    RAISE NOTICE '✅ Updated unique constraint to include llm_classification_title';
END $$;

-- ============================================================
-- 步骤4: 迁移现有数据
-- ============================================================
DO $$
BEGIN
    RAISE NOTICE '==============================================';
    RAISE NOTICE 'Step 4: Migrate existing data';
    RAISE NOTICE '==============================================';
END $$;

-- 为现有的 llm_classification 记录设置 llm_classification_title
UPDATE news_themes_unified
SET llm_classification_title = 'primary_theme'
WHERE source_type = 'llm_classification'
  AND is_primary = true
  AND llm_classification_title IS NULL;

-- 为其他来源的记录，保持 llm_classification_title 为 NULL 或设置为 theme_name
UPDATE news_themes_unified
SET llm_classification_title = COALESCE(llm_classification_title, theme_name)
WHERE source_type != 'llm_classification'
  AND llm_classification_title IS NULL;

-- ============================================================
-- 步骤5: 更新视图
-- ============================================================
DO $$
BEGIN
    RAISE NOTICE '==============================================';
    RAISE NOTICE 'Step 5: Update views';
    RAISE NOTICE '==============================================';
END $$;

-- 更新 v_news_primary_themes 视图（先删除再创建）
DROP VIEW IF EXISTS v_news_primary_themes CASCADE;
CREATE VIEW v_news_primary_themes AS
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
  AND ntu.is_active = true
  AND ntu.source_type = 'llm_classification'
  AND ntu.llm_classification_title = 'primary_theme';

COMMENT ON VIEW v_news_primary_themes IS 
'新闻主主题视图 - 只返回 primary_theme 维度';

-- 更新 v_news_themes_full 视图（先删除再创建）
DROP VIEW IF EXISTS v_news_themes_full CASCADE;
CREATE VIEW v_news_themes_full AS
SELECT 
    n.id as news_id,
    n.title as news_title,
    n.date as news_date,
    n.source as news_source,
    n.analysis_status,
    
    ntu.id as theme_record_id,
    ntu.theme_name,
    ntu.llm_classification_title,  -- 更新字段名
    ntu.source_type,
    ntu.confidence_score,
    ntu.quality_score,
    ntu.rank_in_news,
    ntu.market,
    ntu.is_verified,
    ntu.is_primary,
    
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
'新闻主题全景视图 - 包含所有分类维度（primary_theme、event_type 等）';

-- 更新 v_news_best_themes 视图（先删除再创建）
DROP VIEW IF EXISTS v_news_best_themes CASCADE;
CREATE VIEW v_news_best_themes AS
SELECT 
    ntu.news_id,
    ntu.llm_classification_title as theme_dimension,  -- 新增：标识维度
    ntu.theme_name,
    ntu.theme_master_id,
    
    -- 聚合多源信息
    MAX(ntu.confidence_score) as max_confidence,
    AVG(ntu.confidence_score) as avg_confidence,
    MAX(ntu.quality_score) as max_quality,
    AVG(ntu.quality_score) as avg_quality,
    
    -- 来源统计
    COUNT(DISTINCT ntu.source_type) as source_count,
    ARRAY_AGG(DISTINCT ntu.source_type ORDER BY ntu.source_type) as sources,
    
    -- 市场信息
    ARRAY_AGG(DISTINCT ntu.market) FILTER (WHERE ntu.market IS NOT NULL) as markets,
    
    -- 是否被验证
    BOOL_OR(ntu.is_verified) as is_verified,
    BOOL_OR(ntu.is_primary) as is_primary,
    
    -- 综合得分
    (
        AVG(ntu.confidence_score) * 0.4 +
        AVG(COALESCE(ntu.quality_score, ntu.confidence_score)) * 0.3 +
        (COUNT(DISTINCT ntu.source_type)::FLOAT / 4.0) * 0.2 +
        CASE WHEN BOOL_OR(ntu.is_verified) THEN 0.1 ELSE 0 END
    ) as fusion_score,
    
    MAX(ntu.updated_at) as last_updated
FROM news_themes_unified ntu
WHERE ntu.is_active = true
  AND ntu.theme_name IS NOT NULL
GROUP BY ntu.news_id, ntu.llm_classification_title, ntu.theme_name, ntu.theme_master_id
HAVING COUNT(DISTINCT ntu.source_type) >= 1
ORDER BY ntu.news_id, fusion_score DESC;

COMMENT ON VIEW v_news_best_themes IS 
'新闻最佳主题视图 - 按维度分组，支持多维度分类';

-- ============================================================
-- 步骤6: 创建新视图：新闻分类维度汇总
-- ============================================================
DROP VIEW IF EXISTS v_news_classification_dimensions CASCADE;
CREATE VIEW v_news_classification_dimensions AS
SELECT 
    ntu.news_id,
    
    -- Primary Theme
    MAX(CASE WHEN ntu.llm_classification_title = 'primary_theme' 
        THEN ntu.theme_name END) as primary_theme,
    MAX(CASE WHEN ntu.llm_classification_title = 'primary_theme' 
        THEN ntu.confidence_score END) as primary_theme_confidence,
    
    -- Event Type
    MAX(CASE WHEN ntu.llm_classification_title = 'event_type' 
        THEN ntu.theme_name END) as event_type,
    MAX(CASE WHEN ntu.llm_classification_title = 'event_type' 
        THEN ntu.confidence_score END) as event_type_confidence,
    
    -- Impact Scope
    MAX(CASE WHEN ntu.llm_classification_title = 'impact_scope' 
        THEN ntu.theme_name END) as impact_scope,
    
    -- Urgency
    MAX(CASE WHEN ntu.llm_classification_title = 'urgency' 
        THEN ntu.theme_name END) as urgency,
    
    -- Related Sectors (聚合为数组)
    ARRAY_AGG(ntu.theme_name ORDER BY ntu.created_at) 
        FILTER (WHERE ntu.llm_classification_title = 'related_sectors') as related_sectors,
    
    -- 元数据（取 primary_theme 的 metadata）
    (SELECT metadata FROM news_themes_unified 
     WHERE news_id = ntu.news_id 
       AND source_type = 'llm_classification' 
       AND llm_classification_title = 'primary_theme' 
     LIMIT 1) as metadata,
    MAX(ntu.created_at) as analyzed_at,
    MAX(ntu.updated_at) as updated_at
    
FROM news_themes_unified ntu
WHERE ntu.source_type = 'llm_classification'
  AND ntu.is_active = true
GROUP BY ntu.news_id;

COMMENT ON VIEW v_news_classification_dimensions IS 
'新闻分类维度汇总视图 - 将多条记录聚合为单行，便于查询';

-- ============================================================
-- 步骤7: 更新触发器函数
-- ============================================================
DO $$
BEGIN
    RAISE NOTICE '==============================================';
    RAISE NOTICE 'Step 7: Update trigger functions';
    RAISE NOTICE '==============================================';
END $$;

-- 更新自动标准化函数（保持兼容）
CREATE OR REPLACE FUNCTION auto_normalize_theme_name()
RETURNS TRIGGER AS $$
BEGIN
    -- 对于 llm_classification，不再自动标准化
    IF NEW.source_type = 'llm_classification' THEN
        -- llm_classification_title 由应用层设置
        -- theme_name 保持原值
        RETURN NEW;
    END IF;
    
    -- 对于其他来源，保持原有逻辑
    IF NEW.llm_classification_title IS NULL THEN
        NEW.llm_classification_title := normalize_theme_name(NEW.theme_name);
    END IF;
    
    -- 自动关联 theme_master_id
    IF NEW.theme_master_id IS NULL AND NEW.llm_classification_title IS NOT NULL THEN
        SELECT id INTO NEW.theme_master_id
        FROM theme_master_data
        WHERE canonical_name = NEW.llm_classification_title
          AND is_active = true
        LIMIT 1;
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- ============================================================
-- 步骤8: 输出统计信息
-- ============================================================
DO $$
DECLARE
    v_primary_count INTEGER;
    v_event_type_count INTEGER;
    v_total_dimensions INTEGER;
BEGIN
    -- 统计 primary_theme 数量
    SELECT COUNT(*) INTO v_primary_count
    FROM news_themes_unified
    WHERE llm_classification_title = 'primary_theme';
    
    -- 统计 event_type 数量
    SELECT COUNT(*) INTO v_event_type_count
    FROM news_themes_unified
    WHERE llm_classification_title = 'event_type';
    
    -- 统计总维度记录数
    SELECT COUNT(*) INTO v_total_dimensions
    FROM news_themes_unified
    WHERE source_type = 'llm_classification';
    
    RAISE NOTICE '==============================================';
    RAISE NOTICE 'Classification Dimensions Refactoring Completed!';
    RAISE NOTICE '==============================================';
    RAISE NOTICE 'Statistics:';
    RAISE NOTICE '  - Primary Theme records: %', v_primary_count;
    RAISE NOTICE '  - Event Type records: %', v_event_type_count;
    RAISE NOTICE '  - Total LLM classification records: %', v_total_dimensions;
    RAISE NOTICE '';
    RAISE NOTICE 'New Views:';
    RAISE NOTICE '  - v_news_classification_dimensions (NEW!)';
    RAISE NOTICE '';
    RAISE NOTICE 'Updated Views:';
    RAISE NOTICE '  - v_news_primary_themes';
    RAISE NOTICE '  - v_news_themes_full';
    RAISE NOTICE '  - v_news_best_themes';
    RAISE NOTICE '==============================================';
END $$;

COMMIT;

