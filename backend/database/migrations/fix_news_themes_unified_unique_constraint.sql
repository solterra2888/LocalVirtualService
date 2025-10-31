-- =====================================================
-- 修复 news_themes_unified 表的唯一约束
-- 创建日期: 2025-10-31
-- 描述: 添加通用的唯一约束以支持所有来源类型（eodhd_auto_tag, vector_retrieval, llm_classification）
-- =====================================================
--
-- 问题：
-- 当前表只有一个针对 llm_classification 的特殊约束：
--   unique_news_classification_dimension (news_id, source_type, llm_classification_title, theme_name, market)
-- 
-- 但是代码中的 ON CONFLICT 子句使用的是：
--   ON CONFLICT (news_id, source_type, theme_name, market)
--
-- 这导致 eodhd_auto_tag 和 vector_retrieval 的插入失败
--
-- 解决方案：
-- 1. 添加通用的唯一约束 (news_id, source_type, theme_name, market)
-- 2. 保留 llm_classification 的特殊约束（用于分类维度）
-- =====================================================

BEGIN;

-- ============================================================
-- 步骤1: 确保所有记录的 market 字段不为 NULL
-- ============================================================
DO $$
DECLARE
    v_null_market_count INTEGER;
BEGIN
    RAISE NOTICE '==============================================';
    RAISE NOTICE 'Step 1: Update NULL market values to GLOBAL...';
    RAISE NOTICE '==============================================';
    
    -- 更新 NULL market 值
    UPDATE news_themes_unified
    SET market = 'GLOBAL'
    WHERE market IS NULL;
    
    GET DIAGNOSTICS v_null_market_count = ROW_COUNT;
    
    IF v_null_market_count > 0 THEN
        RAISE NOTICE '✅ Updated % records with NULL market to GLOBAL', v_null_market_count;
    ELSE
        RAISE NOTICE '✅ No NULL market values found';
    END IF;
END $$;

-- ============================================================
-- 步骤2: 处理可能存在的重复数据（先清理再添加约束）
-- ============================================================
DO $$
DECLARE
    v_duplicate_count INTEGER;
BEGIN
    RAISE NOTICE '==============================================';
    RAISE NOTICE 'Step 2: Check for duplicate data...';
    RAISE NOTICE '==============================================';
    
    -- 检查所有重复数据（包括 llm_classification）
    SELECT COUNT(DISTINCT news_id || '|' || source_type || '|' || theme_name || '|' || COALESCE(market, 'GLOBAL')) INTO v_duplicate_count
    FROM (
        SELECT news_id, source_type, theme_name, market
        FROM news_themes_unified
        GROUP BY news_id, source_type, theme_name, market
        HAVING COUNT(*) > 1
    ) duplicates;
    
    IF v_duplicate_count > 0 THEN
        RAISE WARNING '⚠️  Found % duplicate groups', v_duplicate_count;
        RAISE NOTICE '   Keeping the record with the highest confidence_score and deleting others...';
        
        -- 删除所有重复数据，保留置信度最高的记录（包括 llm_classification）
        DELETE FROM news_themes_unified ntu1
        WHERE EXISTS (
              SELECT 1
              FROM news_themes_unified ntu2
              WHERE ntu2.news_id = ntu1.news_id
                AND ntu2.source_type = ntu1.source_type
                AND ntu2.theme_name = ntu1.theme_name
                AND COALESCE(ntu2.market, 'GLOBAL') = COALESCE(ntu1.market, 'GLOBAL')
                AND (
                    ntu2.confidence_score > ntu1.confidence_score
                    OR (ntu2.confidence_score = ntu1.confidence_score AND ntu2.id > ntu1.id)
                )
          );
        
        GET DIAGNOSTICS v_duplicate_count = ROW_COUNT;
        RAISE NOTICE '✅ Removed % duplicate records', v_duplicate_count;
    ELSE
        RAISE NOTICE '✅ No duplicate data found';
    END IF;
END $$;

-- ============================================================
-- 步骤3: 检查并添加通用的唯一约束
-- ============================================================

-- ============================================================
-- 步骤4: 添加通用的唯一约束（在清理重复数据后）
-- ============================================================
DO $$
DECLARE
    v_constraint_exists BOOLEAN;
BEGIN
    RAISE NOTICE '==============================================';
    RAISE NOTICE 'Step 4: Check and add general unique constraint...';
    RAISE NOTICE '==============================================';
    
    -- 检查是否已存在通用的唯一约束
    SELECT EXISTS (
        SELECT 1 
        FROM pg_constraint 
        WHERE conrelid = 'news_themes_unified'::regclass
          AND conname = 'unique_news_theme_source_market'
          AND contype = 'u'
    ) INTO v_constraint_exists;
    
    IF NOT v_constraint_exists THEN
        -- 添加通用的唯一约束（支持所有来源类型）
        ALTER TABLE news_themes_unified
        ADD CONSTRAINT unique_news_theme_source_market
        UNIQUE (news_id, source_type, theme_name, market);
        
        RAISE NOTICE '✅ Added unique constraint: unique_news_theme_source_market';
    ELSE
        RAISE NOTICE 'ℹ️  Constraint unique_news_theme_source_market already exists';
    END IF;
END $$;

-- ============================================================
-- 步骤5: 验证约束
-- ============================================================
DO $$
DECLARE
    v_constraint_count INTEGER;
BEGIN
    RAISE NOTICE '==============================================';
    RAISE NOTICE 'Step 4: Verify constraints...';
    RAISE NOTICE '==============================================';
    
    -- 检查唯一约束数量
    SELECT COUNT(*) INTO v_constraint_count
    FROM pg_constraint
    WHERE conrelid = 'news_themes_unified'::regclass
      AND contype = 'u'
      AND conname IN (
          'unique_news_theme_source_market',
          'unique_news_classification_dimension'
      );
    
    RAISE NOTICE 'Found % unique constraints:', v_constraint_count;
    
    -- 列出所有唯一约束
    DECLARE
        rec RECORD;
    BEGIN
        FOR rec IN (
            SELECT conname, pg_get_constraintdef(oid) as definition
            FROM pg_constraint
            WHERE conrelid = 'news_themes_unified'::regclass
              AND contype = 'u'
            ORDER BY conname
        ) LOOP
            RAISE NOTICE '  - %: %', rec.conname, rec.definition;
        END LOOP;
    END;
    
    RAISE NOTICE '';
    RAISE NOTICE '==============================================';
    RAISE NOTICE 'Fix Completed!';
    RAISE NOTICE '==============================================';
    RAISE NOTICE 'Summary:';
    RAISE NOTICE '  - Added unique constraint for all source types';
    RAISE NOTICE '  - Cleaned up duplicate data';
    RAISE NOTICE '  - Updated NULL market values to GLOBAL';
    RAISE NOTICE '';
    RAISE NOTICE 'The constraint unique_news_theme_source_market now supports:';
    RAISE NOTICE '  - eodhd_auto_tag';
    RAISE NOTICE '  - vector_retrieval';
    RAISE NOTICE '  - llm_classification (when not using llm_classification_title)';
    RAISE NOTICE '  - manual';
    RAISE NOTICE '';
    RAISE NOTICE 'Note: llm_classification with llm_classification_title uses';
    RAISE NOTICE '  unique_news_classification_dimension constraint';
    RAISE NOTICE '==============================================';
END $$;

COMMIT;

