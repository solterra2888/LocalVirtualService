-- ============================================================
-- 添加新闻关联表的外键约束
-- ============================================================
-- 
-- 目的: 确保删除新闻时，关联表的数据自动删除（ON DELETE CASCADE）
-- 使用场景: 可选的长期解决方案，提供数据库级别的完整性保证
--
-- 注意事项:
-- 1. 运行此脚本前，必须先清理现有的孤立记录
-- 2. 建议在维护窗口期执行，可能需要短暂停机
-- 3. 在生产环境执行前，请先在测试环境验证
--
-- 使用方法:
--   psql -d reversecapital -f add_news_foreign_keys.sql
-- ============================================================

BEGIN;

-- ============================================================
-- 步骤1: 检查并报告孤立记录数量
-- ============================================================

DO $$
DECLARE
    orphan_count_auto_tags INTEGER;
    orphan_count_symbols INTEGER;
    orphan_count_tags INTEGER;
    total_orphans INTEGER;
BEGIN
    -- 检查 news_auto_tags_relations
    SELECT COUNT(*) INTO orphan_count_auto_tags
    FROM news_auto_tags_relations 
    WHERE news_id NOT IN (SELECT id FROM news);
    
    -- 检查 news_symbols
    SELECT COUNT(*) INTO orphan_count_symbols
    FROM news_symbols 
    WHERE news_id NOT IN (SELECT id FROM news);
    
    -- 检查 news_tags
    SELECT COUNT(*) INTO orphan_count_tags
    FROM news_tags 
    WHERE news_id NOT IN (SELECT id FROM news);
    
    total_orphans := orphan_count_auto_tags + orphan_count_symbols + orphan_count_tags;
    
    RAISE NOTICE '========================================';
    RAISE NOTICE '孤立记录检查结果:';
    RAISE NOTICE '========================================';
    RAISE NOTICE '  news_auto_tags_relations: % 条', orphan_count_auto_tags;
    RAISE NOTICE '  news_symbols:             % 条', orphan_count_symbols;
    RAISE NOTICE '  news_tags:                % 条', orphan_count_tags;
    RAISE NOTICE '========================================';
    RAISE NOTICE '  总计:                     % 条', total_orphans;
    RAISE NOTICE '========================================';
    
    IF total_orphans > 0 THEN
        RAISE EXCEPTION '发现 % 条孤立记录！请先运行 cleanup_orphan_news_relations.py 清理孤立记录', total_orphans;
    END IF;
    
    RAISE NOTICE '✅ 没有发现孤立记录，可以安全添加外键约束';
END $$;

-- ============================================================
-- 步骤2: 检查是否已存在外键约束
-- ============================================================

DO $$
BEGIN
    -- 检查 news_auto_tags_relations
    IF EXISTS (
        SELECT 1 FROM information_schema.table_constraints 
        WHERE table_name = 'news_auto_tags_relations' 
        AND constraint_name = 'fk_news_auto_tags_relations_news_id'
    ) THEN
        RAISE NOTICE '外键约束 fk_news_auto_tags_relations_news_id 已存在，跳过';
    END IF;
    
    -- 检查 news_symbols
    IF EXISTS (
        SELECT 1 FROM information_schema.table_constraints 
        WHERE table_name = 'news_symbols' 
        AND constraint_name = 'fk_news_symbols_news_id'
    ) THEN
        RAISE NOTICE '外键约束 fk_news_symbols_news_id 已存在，跳过';
    END IF;
    
    -- 检查 news_tags
    IF EXISTS (
        SELECT 1 FROM information_schema.table_constraints 
        WHERE table_name = 'news_tags' 
        AND constraint_name = 'fk_news_tags_news_id'
    ) THEN
        RAISE NOTICE '外键约束 fk_news_tags_news_id 已存在，跳过';
    END IF;
END $$;

-- ============================================================
-- 步骤3: 添加外键约束
-- ============================================================

-- 3.1 为 news_auto_tags_relations 添加外键约束
ALTER TABLE news_auto_tags_relations 
DROP CONSTRAINT IF EXISTS fk_news_auto_tags_relations_news_id;

ALTER TABLE news_auto_tags_relations 
ADD CONSTRAINT fk_news_auto_tags_relations_news_id 
FOREIGN KEY (news_id) REFERENCES news(id) ON DELETE CASCADE;

RAISE NOTICE '✅ 已为 news_auto_tags_relations 添加外键约束';

-- 3.2 为 news_symbols 添加外键约束
ALTER TABLE news_symbols 
DROP CONSTRAINT IF EXISTS fk_news_symbols_news_id;

ALTER TABLE news_symbols 
ADD CONSTRAINT fk_news_symbols_news_id 
FOREIGN KEY (news_id) REFERENCES news(id) ON DELETE CASCADE;

RAISE NOTICE '✅ 已为 news_symbols 添加外键约束';

-- 3.3 为 news_tags 添加外键约束
ALTER TABLE news_tags 
DROP CONSTRAINT IF EXISTS fk_news_tags_news_id;

ALTER TABLE news_tags 
ADD CONSTRAINT fk_news_tags_news_id 
FOREIGN KEY (news_id) REFERENCES news(id) ON DELETE CASCADE;

RAISE NOTICE '✅ 已为 news_tags 添加外键约束';

-- ============================================================
-- 步骤4: 验证外键约束
-- ============================================================

DO $$
DECLARE
    constraint_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO constraint_count
    FROM information_schema.table_constraints 
    WHERE constraint_name IN (
        'fk_news_auto_tags_relations_news_id',
        'fk_news_symbols_news_id',
        'fk_news_tags_news_id'
    );
    
    IF constraint_count = 3 THEN
        RAISE NOTICE '========================================';
        RAISE NOTICE '✅ 所有外键约束已成功添加！';
        RAISE NOTICE '========================================';
        RAISE NOTICE '已添加的约束:';
        RAISE NOTICE '  1. fk_news_auto_tags_relations_news_id';
        RAISE NOTICE '  2. fk_news_symbols_news_id';
        RAISE NOTICE '  3. fk_news_tags_news_id';
        RAISE NOTICE '========================================';
    ELSE
        RAISE EXCEPTION '外键约束添加失败！预期3个约束，实际添加了%个', constraint_count;
    END IF;
END $$;

-- ============================================================
-- 步骤5: 提交事务
-- ============================================================

COMMIT;

-- ============================================================
-- 验证脚本（可选）
-- ============================================================

-- 查看所有外键约束
-- SELECT 
--     tc.constraint_name,
--     tc.table_name,
--     kcu.column_name,
--     ccu.table_name AS foreign_table_name,
--     ccu.column_name AS foreign_column_name,
--     rc.delete_rule
-- FROM information_schema.table_constraints AS tc
-- JOIN information_schema.key_column_usage AS kcu
--     ON tc.constraint_name = kcu.constraint_name
-- JOIN information_schema.constraint_column_usage AS ccu
--     ON ccu.constraint_name = tc.constraint_name
-- JOIN information_schema.referential_constraints AS rc
--     ON tc.constraint_name = rc.constraint_name
-- WHERE tc.constraint_type = 'FOREIGN KEY'
--     AND tc.table_name IN ('news_auto_tags_relations', 'news_symbols', 'news_tags')
--     AND ccu.table_name = 'news'
-- ORDER BY tc.table_name, tc.constraint_name;

-- ============================================================
-- 回滚脚本（如果需要）
-- ============================================================

-- BEGIN;
-- 
-- ALTER TABLE news_auto_tags_relations 
-- DROP CONSTRAINT IF EXISTS fk_news_auto_tags_relations_news_id;
-- 
-- ALTER TABLE news_symbols 
-- DROP CONSTRAINT IF EXISTS fk_news_symbols_news_id;
-- 
-- ALTER TABLE news_tags 
-- DROP CONSTRAINT IF EXISTS fk_news_tags_news_id;
-- 
-- COMMIT;

