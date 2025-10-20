-- ============================================================
-- 为news表添加scheduler_source字段
-- ============================================================
-- 
-- 目的: 标识新闻来源的调度器类型
-- 使用场景: 区分新闻是来自eodhd_news调度器还是watchlist_news调度器
--
-- 字段说明:
-- - scheduler_source: VARCHAR(50) 存储调度器类型
--   - 'eodhd_news': EODHD新闻调度器
--   - 'watchlist_news': Watchlist新闻调度器
--   - NULL: 历史数据或手动添加的新闻
--
-- 使用方法:
--   psql -d reversecapital -f add_scheduler_source_to_news.sql
-- ============================================================

BEGIN;

-- ============================================================
-- 步骤1: 检查字段是否已存在
-- ============================================================

DO $$
BEGIN
    -- 检查scheduler_source字段是否已存在
    IF EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'news' 
        AND column_name = 'scheduler_source'
    ) THEN
        RAISE NOTICE '字段 scheduler_source 已存在，跳过添加';
    ELSE
        RAISE NOTICE '字段 scheduler_source 不存在，开始添加';
    END IF;
END $$;

-- ============================================================
-- 步骤2: 添加scheduler_source字段
-- ============================================================

-- 添加scheduler_source字段
ALTER TABLE news 
ADD COLUMN IF NOT EXISTS scheduler_source VARCHAR(50);

-- 添加字段注释
COMMENT ON COLUMN news.scheduler_source IS '新闻来源调度器类型: eodhd_news, watchlist_news';

-- ============================================================
-- 步骤3: 创建索引以提高查询性能
-- ============================================================

-- 为scheduler_source字段创建索引
CREATE INDEX IF NOT EXISTS idx_news_scheduler_source 
ON news(scheduler_source);

-- ============================================================
-- 步骤4: 验证字段添加
-- ============================================================

DO $$
DECLARE
    column_exists BOOLEAN;
    index_exists BOOLEAN;
BEGIN
    -- 检查字段是否存在
    SELECT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'news' 
        AND column_name = 'scheduler_source'
    ) INTO column_exists;
    
    -- 检查索引是否存在
    SELECT EXISTS (
        SELECT 1 FROM pg_indexes 
        WHERE tablename = 'news' 
        AND indexname = 'idx_news_scheduler_source'
    ) INTO index_exists;
    
    IF column_exists AND index_exists THEN
        RAISE NOTICE '========================================';
        RAISE NOTICE '✅ scheduler_source字段和索引添加成功！';
        RAISE NOTICE '========================================';
        RAISE NOTICE '已添加的内容:';
        RAISE NOTICE '  1. scheduler_source字段 (VARCHAR(50))';
        RAISE NOTICE '  2. idx_news_scheduler_source索引';
        RAISE NOTICE '========================================';
    ELSE
        RAISE EXCEPTION '字段或索引添加失败！字段存在: %, 索引存在: %', column_exists, index_exists;
    END IF;
END $$;

-- ============================================================
-- 步骤5: 提交事务
-- ============================================================

COMMIT;

-- ============================================================
-- 验证脚本（可选）
-- ============================================================

-- 查看news表结构
-- \d news

-- 查看scheduler_source字段信息
-- SELECT column_name, data_type, is_nullable, column_default, character_maximum_length
-- FROM information_schema.columns 
-- WHERE table_name = 'news' AND column_name = 'scheduler_source';

-- 查看相关索引
-- SELECT indexname, indexdef 
-- FROM pg_indexes 
-- WHERE tablename = 'news' AND indexname LIKE '%scheduler_source%';

-- ============================================================
-- 回滚脚本（如果需要）
-- ============================================================

-- BEGIN;
-- 
-- -- 删除索引
-- DROP INDEX IF EXISTS idx_news_scheduler_source;
-- 
-- -- 删除字段
-- ALTER TABLE news DROP COLUMN IF EXISTS scheduler_source;
-- 
-- COMMIT;
