-- =====================================================
-- 修复 news_themes_unified 表的 ON CONFLICT 约束问题
-- 创建日期: 2025-10-30
-- 描述: 修复批量插入时的约束不匹配错误
-- =====================================================
--
-- 问题：
-- ON CONFLICT 子句使用了 COALESCE(market, '')，但表约束是 (news_id, source_type, theme_name, market)
-- PostgreSQL 不允许在 ON CONFLICT 中使用函数表达式
--
-- 解决方案：
-- 1. 确保所有插入操作的 market 字段不为 NULL（使用 'GLOBAL' 作为默认值）
-- 2. 重新创建 upsert_primary_theme 函数，添加 market 字段
-- =====================================================

BEGIN;

-- ============================================================
-- 步骤1: 更新现有数据中的 NULL market 值
-- ============================================================
DO $$
DECLARE
    v_updated_count INTEGER;
BEGIN
    RAISE NOTICE '==============================================';
    RAISE NOTICE 'Step 1: Update NULL market values to GLOBAL...';
    RAISE NOTICE '==============================================';
    
    -- 将 market 为 NULL 的记录更新为 'GLOBAL'
    UPDATE news_themes_unified
    SET market = 'GLOBAL'
    WHERE market IS NULL;
    
    GET DIAGNOSTICS v_updated_count = ROW_COUNT;
    
    RAISE NOTICE '✅ Updated % records with NULL market to GLOBAL', v_updated_count;
END $$;

-- ============================================================
-- 步骤2: 重新创建 upsert_primary_theme 函数
-- ============================================================
DO $$
BEGIN
    RAISE NOTICE '==============================================';
    RAISE NOTICE 'Step 2: Recreate upsert_primary_theme function...';
    RAISE NOTICE '==============================================';
END $$;

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
        'GLOBAL'  -- 默认市场值，确保不为 NULL
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
'插入或更新新闻的 primary theme，自动处理 is_primary 标记，market 默认为 GLOBAL';

-- ============================================================
-- 步骤3: 验证修复
-- ============================================================
DO $$
DECLARE
    v_null_count INTEGER;
BEGIN
    RAISE NOTICE '==============================================';
    RAISE NOTICE 'Step 3: Verify fix...';
    RAISE NOTICE '==============================================';
    
    -- 检查是否还有 NULL market 值
    SELECT COUNT(*) INTO v_null_count
    FROM news_themes_unified
    WHERE market IS NULL;
    
    IF v_null_count > 0 THEN
        RAISE WARNING '⚠️  Still have % records with NULL market', v_null_count;
    ELSE
        RAISE NOTICE '✅ No NULL market values found';
    END IF;
    
    -- 测试函数（注释掉，仅用于手动测试）
    -- SELECT upsert_primary_theme(1, 'technology', 0.85, '{"test": true}'::jsonb);
    
    RAISE NOTICE '';
    RAISE NOTICE '==============================================';
    RAISE NOTICE 'Fix Completed!';
    RAISE NOTICE '==============================================';
    RAISE NOTICE 'Summary:';
    RAISE NOTICE '  - Updated NULL market values to GLOBAL';
    RAISE NOTICE '  - Recreated upsert_primary_theme function';
    RAISE NOTICE '  - All ON CONFLICT clauses now match table constraints';
    RAISE NOTICE '';
    RAISE NOTICE 'Next steps:';
    RAISE NOTICE '  1. Restart Celery workers to use updated code';
    RAISE NOTICE '  2. Test news analysis with news_classification template';
    RAISE NOTICE '  3. Monitor logs for "批量插入主题失败" errors';
    RAISE NOTICE '==============================================';
END $$;

COMMIT;

