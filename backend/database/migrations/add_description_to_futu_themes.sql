-- =====================================================
-- 添加 description 字段到 futu_investment_themes 表
-- 创建日期: 2025-10-22
-- 描述: 为投资主题表添加 LLM 生成的描述字段，用于缓存和向量化
-- =====================================================
--
-- 功能说明：
-- 1. 在 futu_investment_themes 表中添加 description 字段
-- 2. 添加 description_source 字段，标识描述来源（llm_generated/manual/scraped）
-- 3. 添加 description_updated_at 字段，记录描述更新时间
-- 4. 优化向量生成流程，避免重复调用 LLM
--
-- 使用方法:
--   psql -h 111.230.48.119 -U postgres -d rc_database -f add_description_to_futu_themes.sql
-- =====================================================

BEGIN;

-- ============================================================
-- 步骤1: 添加 description 相关字段
-- ============================================================

-- 添加 description 字段（存储 LLM 生成的详细描述）
ALTER TABLE futu_investment_themes 
ADD COLUMN IF NOT EXISTS description TEXT;

-- 添加 description_source 字段（标识描述来源）
ALTER TABLE futu_investment_themes 
ADD COLUMN IF NOT EXISTS description_source VARCHAR(20) DEFAULT NULL;

-- 添加 description_updated_at 字段（记录描述更新时间）
ALTER TABLE futu_investment_themes 
ADD COLUMN IF NOT EXISTS description_updated_at TIMESTAMP DEFAULT NULL;

-- ============================================================
-- 步骤2: 添加字段注释
-- ============================================================

COMMENT ON COLUMN futu_investment_themes.description IS 
'投资主题的详细描述（用于向量化匹配），由 LLM 自动生成或人工编写';

COMMENT ON COLUMN futu_investment_themes.description_source IS 
'描述来源：llm_generated（LLM生成）、manual（人工编写）、scraped（网站爬取）、NULL（未生成）';

COMMENT ON COLUMN futu_investment_themes.description_updated_at IS 
'描述最后更新时间';

-- ============================================================
-- 步骤3: 添加检查约束（可选）
-- ============================================================

-- 确保 description_source 只能是指定的值
ALTER TABLE futu_investment_themes
ADD CONSTRAINT check_description_source 
CHECK (description_source IN ('llm_generated', 'manual', 'scraped') OR description_source IS NULL);

-- ============================================================
-- 步骤4: 创建索引（可选，用于查询优化）
-- ============================================================

-- 为经常查询未生成描述的场景创建索引
CREATE INDEX IF NOT EXISTS idx_futu_themes_description_null 
ON futu_investment_themes(theme_id, market) 
WHERE description IS NULL;

-- 为查询已生成描述的场景创建索引
CREATE INDEX IF NOT EXISTS idx_futu_themes_description_source 
ON futu_investment_themes(description_source) 
WHERE description_source IS NOT NULL;

COMMIT;

-- ============================================================
-- 验证
-- ============================================================

-- 查看表结构
\d futu_investment_themes

-- 查看新添加的字段
SELECT 
    column_name, 
    data_type, 
    character_maximum_length,
    column_default,
    is_nullable
FROM information_schema.columns
WHERE table_name = 'futu_investment_themes' 
AND column_name IN ('description', 'description_source', 'description_updated_at');

-- 查看当前描述状态统计
SELECT 
    market,
    COUNT(*) as total_themes,
    COUNT(description) as has_description,
    COUNT(*) - COUNT(description) as missing_description,
    COUNT(CASE WHEN description_source = 'llm_generated' THEN 1 END) as llm_generated,
    COUNT(CASE WHEN description_source = 'manual' THEN 1 END) as manual,
    COUNT(CASE WHEN description_source = 'scraped' THEN 1 END) as scraped
FROM futu_investment_themes
GROUP BY market
ORDER BY market;

-- ============================================================
-- 回滚脚本（如果需要）
-- ============================================================

/*
BEGIN;

-- 删除索引
DROP INDEX IF EXISTS idx_futu_themes_description_null;
DROP INDEX IF EXISTS idx_futu_themes_description_source;

-- 删除约束
ALTER TABLE futu_investment_themes DROP CONSTRAINT IF EXISTS check_description_source;

-- 删除字段
ALTER TABLE futu_investment_themes DROP COLUMN IF EXISTS description;
ALTER TABLE futu_investment_themes DROP COLUMN IF EXISTS description_source;
ALTER TABLE futu_investment_themes DROP COLUMN IF EXISTS description_updated_at;

COMMIT;
*/

-- ============================================================
-- 使用示例
-- ============================================================

/*
-- 示例1: 查询需要生成描述的主题
SELECT theme_id, theme_name, market
FROM futu_investment_themes
WHERE description IS NULL
LIMIT 10;

-- 示例2: 更新主题描述（LLM 生成）
UPDATE futu_investment_themes
SET 
    description = 'Electric Vehicles investment theme focuses on...',
    description_source = 'llm_generated',
    description_updated_at = NOW()
WHERE theme_id = 'BK1288' AND market = 'US';

-- 示例3: 查看特定主题的描述
SELECT 
    theme_id, 
    theme_name, 
    market,
    LEFT(description, 100) as description_preview,
    description_source,
    description_updated_at
FROM futu_investment_themes
WHERE theme_id = 'BK1288' AND market = 'US';

-- 示例4: 统计描述生成情况
SELECT 
    description_source,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM futu_investment_themes), 2) as percentage
FROM futu_investment_themes
GROUP BY description_source
ORDER BY count DESC;
*/

