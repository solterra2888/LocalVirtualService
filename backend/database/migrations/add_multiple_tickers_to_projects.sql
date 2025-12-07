-- =====================================================
-- Add Multiple Tickers Support to Investment Projects
-- 创建日期: 2025-12-06
-- 描述: 将单个ticker_symbol字段改为支持多个tickers的数组字段
-- =====================================================
-- 
-- 功能说明：
-- 1. 将现有的ticker_symbol字段改为tickers数组
-- 2. 迁移现有数据（将单个ticker转换为数组）
-- 3. 更新索引
--
-- 使用方法:
--   psql -h 111.230.48.119 -U postgres -d rc_database -f add_multiple_tickers_to_projects.sql
-- =====================================================

BEGIN;

-- ============================================================
-- 1. 添加新的tickers数组字段（先保留旧字段以便迁移）
-- ============================================================

ALTER TABLE investment_projects 
ADD COLUMN IF NOT EXISTS tickers VARCHAR(50)[];

-- ============================================================
-- 2. 迁移现有数据：将ticker_symbol转换为tickers数组
-- ============================================================

UPDATE investment_projects
SET tickers = ARRAY[ticker_symbol]
WHERE ticker_symbol IS NOT NULL AND ticker_symbol != '';

-- 如果ticker_symbol为空，设置tickers为空数组
UPDATE investment_projects
SET tickers = ARRAY[]::VARCHAR[]
WHERE ticker_symbol IS NULL OR ticker_symbol = '';

-- ============================================================
-- 3. 删除旧的ticker_symbol字段
-- ============================================================

-- 先删除基于ticker_symbol的索引
DROP INDEX IF EXISTS idx_projects_ticker;

-- 删除旧字段
ALTER TABLE investment_projects
DROP COLUMN IF EXISTS ticker_symbol;

-- ============================================================
-- 4. 创建新的索引（使用GIN索引支持数组查询）
-- ============================================================

-- GIN索引用于高效查询数组包含某个值
CREATE INDEX IF NOT EXISTS idx_projects_tickers ON investment_projects USING GIN(tickers);

-- ============================================================
-- 5. 更新项目编号生成逻辑的注释
-- ============================================================

COMMENT ON COLUMN investment_projects.tickers IS '关联的股票代码列表（支持多个ticker，不分主次）';

-- ============================================================
-- 6. 更新约束检查函数（可选）
-- ============================================================

-- 创建函数：验证项目至少有ticker、industry或sector之一
CREATE OR REPLACE FUNCTION validate_project_target()
RETURNS TRIGGER AS $$
BEGIN
    IF (NEW.tickers IS NULL OR array_length(NEW.tickers, 1) IS NULL OR array_length(NEW.tickers, 1) = 0)
       AND (NEW.industry IS NULL OR NEW.industry = '')
       AND (NEW.sector IS NULL OR NEW.sector = '') THEN
        RAISE EXCEPTION 'Project must have at least one ticker, industry, or sector';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 创建触发器
DROP TRIGGER IF EXISTS trg_validate_project_target ON investment_projects;
CREATE TRIGGER trg_validate_project_target
    BEFORE INSERT OR UPDATE ON investment_projects
    FOR EACH ROW
    EXECUTE FUNCTION validate_project_target();

-- ============================================================
-- 7. 更新视图（如果需要）
-- ============================================================

-- 先删除旧视图，再重新创建
DROP VIEW IF EXISTS v_project_activity_summary;

-- 重新创建项目活动概览视图
CREATE VIEW v_project_activity_summary AS
SELECT 
    p.id as project_id,
    p.project_code,
    p.project_name,
    p.status,
    p.tickers,  -- 添加tickers字段
    COUNT(DISTINCT pm.id) as member_count,
    COUNT(DISTINCT pn.id) as news_count,
    COUNT(DISTINCT pf.id) as file_count,
    COUNT(DISTINCT pms.id) as milestone_count,
    MAX(pms.milestone_date) as latest_milestone_date,
    MAX(pn.added_at) as latest_news_added_at
FROM investment_projects p
LEFT JOIN project_members pm ON p.id = pm.project_id AND pm.status = 'active'
LEFT JOIN project_news_tracking pn ON p.id = pn.project_id
LEFT JOIN project_files pf ON p.id = pf.project_id
LEFT JOIN project_milestones pms ON p.id = pms.project_id
GROUP BY p.id, p.project_code, p.project_name, p.status, p.tickers;

-- ============================================================
-- 完成
-- ============================================================

COMMIT;

-- 显示迁移结果
SELECT 'Multiple tickers support added successfully!' as status;

-- 验证数据
SELECT 
    COUNT(*) as total_projects,
    COUNT(*) FILTER (WHERE tickers IS NOT NULL AND array_length(tickers, 1) > 0) as projects_with_tickers,
    COUNT(*) FILTER (WHERE array_length(tickers, 1) > 1) as projects_with_multiple_tickers
FROM investment_projects;

-- 显示示例数据（前5条）
SELECT 
    id,
    project_code,
    project_name,
    tickers,
    array_length(tickers, 1) as ticker_count
FROM investment_projects
LIMIT 5;

