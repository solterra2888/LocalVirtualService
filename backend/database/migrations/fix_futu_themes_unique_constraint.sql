-- =====================================================
-- 修复 futu_investment_themes 表的唯一约束
-- 创建日期: 2025-01-27
-- 目的: 确保主题ID+市场的唯一性，避免同一主题在不同日期重复存储
-- =====================================================
--
-- 功能说明：
-- 1. 删除现有的基于日期的唯一约束
-- 2. 创建新的基于主题ID+市场的唯一约束
-- 3. 清理重复数据，保留最新的记录
--
-- 使用方法:
--   psql -h 111.230.48.119 -U postgres -d rc_database -f fix_futu_themes_unique_constraint.sql
-- =====================================================

BEGIN;

-- ============================================================
-- 步骤1: 清理重复数据，保留每个主题的最新记录
-- ============================================================

-- 删除重复的主题记录，只保留最新的
DO $$
BEGIN
    DELETE FROM futu_investment_themes 
    WHERE id NOT IN (
        SELECT DISTINCT ON (theme_id, market) id
        FROM futu_investment_themes
        ORDER BY theme_id, market, scraped_at DESC, created_at DESC
    );
    
    RAISE NOTICE '✅ 已清理重复的主题记录';
END $$;

-- ============================================================
-- 步骤2: 删除现有的唯一约束
-- ============================================================

-- 删除现有的基于日期的唯一约束
DO $$
BEGIN
    -- 删除约束
    IF EXISTS (
        SELECT 1 FROM information_schema.table_constraints 
        WHERE table_name = 'futu_investment_themes' 
        AND constraint_name = 'unique_theme_market_date'
    ) THEN
        ALTER TABLE futu_investment_themes DROP CONSTRAINT unique_theme_market_date;
        RAISE NOTICE '✅ 已删除约束 unique_theme_market_date';
    END IF;
    
    -- 删除索引
    IF EXISTS (
        SELECT 1 FROM pg_indexes 
        WHERE tablename = 'futu_investment_themes' 
        AND indexname = 'unique_theme_market_date_idx'
    ) THEN
        DROP INDEX unique_theme_market_date_idx;
        RAISE NOTICE '✅ 已删除索引 unique_theme_market_date_idx';
    END IF;
    
    RAISE NOTICE '✅ 已删除现有的基于日期的唯一约束和索引';
END $$;

-- ============================================================
-- 步骤3: 创建新的唯一约束
-- ============================================================

-- 添加新的唯一约束：主题ID + 市场
DO $$
BEGIN
    ALTER TABLE futu_investment_themes 
    ADD CONSTRAINT unique_theme_market UNIQUE (theme_id, market);
    
    -- 创建对应的唯一索引
    CREATE UNIQUE INDEX IF NOT EXISTS unique_theme_market_idx 
    ON futu_investment_themes (theme_id, market);
    
    RAISE NOTICE '✅ 已创建新的唯一约束：theme_id + market';
END $$;

-- ============================================================
-- 步骤4: 更新表注释
-- ============================================================

DO $$
BEGIN
    COMMENT ON CONSTRAINT unique_theme_market ON futu_investment_themes IS 
    '主题唯一约束：确保同一主题在同一市场只有一条记录，避免重复存储';
END $$;

-- ============================================================
-- 步骤5: 验证修改结果
-- ============================================================

DO $$
DECLARE
    total_themes INTEGER;
    unique_themes INTEGER;
BEGIN
    -- 统计总记录数
    SELECT COUNT(*) INTO total_themes FROM futu_investment_themes;
    
    -- 统计唯一主题数
    SELECT COUNT(DISTINCT CONCAT(theme_id, '-', market)) INTO unique_themes FROM futu_investment_themes;
    
    RAISE NOTICE '========================================';
    RAISE NOTICE '✅ 唯一约束修复完成！';
    RAISE NOTICE '========================================';
    RAISE NOTICE '统计信息:';
    RAISE NOTICE '  总记录数: %', total_themes;
    RAISE NOTICE '  唯一主题数: %', unique_themes;
    
    IF total_themes = unique_themes THEN
        RAISE NOTICE '  ✅ 数据一致性检查通过：无重复主题';
    ELSE
        RAISE WARNING '  ⚠️  数据一致性检查失败：存在重复主题';
    END IF;
    
    RAISE NOTICE '========================================';
END $$;

-- ============================================================
-- 步骤6: 提交事务
-- ============================================================

COMMIT;

-- ============================================================
-- 验证脚本（可选）
-- ============================================================

-- 查看表结构
-- \d futu_investment_themes

-- 查看约束信息
-- SELECT constraint_name, constraint_type, table_name 
-- FROM information_schema.table_constraints 
-- WHERE table_name = 'futu_investment_themes' AND constraint_type = 'UNIQUE';

-- 查看索引信息
-- SELECT indexname, indexdef 
-- FROM pg_indexes 
-- WHERE tablename = 'futu_investment_themes' AND indexname LIKE '%unique%';

-- 验证唯一性
-- SELECT theme_id, market, COUNT(*) as count
-- FROM futu_investment_themes 
-- GROUP BY theme_id, market 
-- HAVING COUNT(*) > 1;
