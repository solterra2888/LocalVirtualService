-- ============================================================
-- 清空所有新闻数据（SQL版本）
-- ============================================================
-- 
-- ⚠️  警告: 此脚本将永久删除所有新闻数据，不可恢复！
--
-- 删除的表:
--   • news（新闻主表）
--   • news_symbols, news_tags, news_auto_tags_relations（关联表）
--   • news_symbols_dict, news_auto_tags（字典表）
--
-- 保留的表（主题管理）:
--   • news_tags_dict（标签字典）
--   • news_topics（主题）
--   • news_topic_keywords（主题关键词）
--
-- 使用方法:
--   psql -d reversecapital -f clear_all_news_data.sql
--
-- 或者交互式执行:
--   psql -d reversecapital
--   \i clear_all_news_data.sql
-- ============================================================

BEGIN;

-- ============================================================
-- 检查并显示当前数据统计
-- ============================================================

DO $$
DECLARE
    news_count INTEGER;
    symbols_count INTEGER;
    tags_count INTEGER;
    auto_tags_rel_count INTEGER;
    symbols_dict_count INTEGER;
    auto_tags_count INTEGER;
    tags_dict_count INTEGER;
    topics_count INTEGER;
    topic_keywords_count INTEGER;
    total_to_delete INTEGER;
BEGIN
    -- 统计将删除的数据
    SELECT COUNT(*) INTO news_count FROM news;
    SELECT COUNT(*) INTO symbols_count FROM news_symbols;
    SELECT COUNT(*) INTO tags_count FROM news_tags;
    SELECT COUNT(*) INTO auto_tags_rel_count FROM news_auto_tags_relations;
    SELECT COUNT(*) INTO symbols_dict_count FROM news_symbols_dict;
    SELECT COUNT(*) INTO auto_tags_count FROM news_auto_tags;
    
    -- 统计将保留的数据
    SELECT COUNT(*) INTO tags_dict_count FROM news_tags_dict;
    SELECT COUNT(*) INTO topics_count FROM news_topics;
    SELECT COUNT(*) INTO topic_keywords_count FROM news_topic_keywords;
    
    total_to_delete := news_count + symbols_count + tags_count + 
                      auto_tags_rel_count + symbols_dict_count + auto_tags_count;
    
    RAISE NOTICE '';
    RAISE NOTICE '========================================';
    RAISE NOTICE '当前新闻数据统计';
    RAISE NOTICE '========================================';
    RAISE NOTICE '';
    RAISE NOTICE '将删除的数据:';
    RAISE NOTICE '----------------------------------------';
    RAISE NOTICE '  news:                      % 条', news_count;
    RAISE NOTICE '  news_symbols:              % 条', symbols_count;
    RAISE NOTICE '  news_tags:                 % 条', tags_count;
    RAISE NOTICE '  news_auto_tags_relations:  % 条', auto_tags_rel_count;
    RAISE NOTICE '  news_symbols_dict:         % 条', symbols_dict_count;
    RAISE NOTICE '  news_auto_tags:            % 条', auto_tags_count;
    RAISE NOTICE '----------------------------------------';
    RAISE NOTICE '  总计:                      % 条', total_to_delete;
    RAISE NOTICE '';
    RAISE NOTICE '将保留的数据（主题管理）:';
    RAISE NOTICE '----------------------------------------';
    RAISE NOTICE '  news_tags_dict:            % 条', tags_dict_count;
    RAISE NOTICE '  news_topics:               % 条', topics_count;
    RAISE NOTICE '  news_topic_keywords:       % 条', topic_keywords_count;
    RAISE NOTICE '========================================';
    RAISE NOTICE '';
    
    IF total_to_delete = 0 THEN
        RAISE NOTICE '✓ 数据库中没有新闻数据，无需清空';
    ELSE
        RAISE NOTICE '⚠️  即将删除 % 条记录！', total_to_delete;
    END IF;
    
    RAISE NOTICE '';
END $$;

-- ============================================================
-- 步骤1: 删除关联表数据
-- ============================================================

DO $$
DECLARE
    deleted_count INTEGER;
BEGIN
    RAISE NOTICE '';
    RAISE NOTICE '========================================';
    RAISE NOTICE '步骤1: 删除关联表数据';
    RAISE NOTICE '========================================';
    
    -- 1.1 删除 news_auto_tags_relations
    DELETE FROM news_auto_tags_relations;
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RAISE NOTICE '✓ 删除 news_auto_tags_relations: % 条', deleted_count;
    
    -- 1.2 删除 news_symbols
    DELETE FROM news_symbols;
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RAISE NOTICE '✓ 删除 news_symbols: % 条', deleted_count;
    
    -- 1.3 删除 news_tags
    DELETE FROM news_tags;
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RAISE NOTICE '✓ 删除 news_tags: % 条', deleted_count;
    
    RAISE NOTICE '';
END $$;

-- ============================================================
-- 步骤2: 删除字典表数据（除了theme相关）
-- ============================================================

DO $$
DECLARE
    deleted_count INTEGER;
    preserved_count INTEGER;
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE '步骤2: 删除字典表数据';
    RAISE NOTICE '========================================';
    
    -- 2.1 删除 news_auto_tags
    DELETE FROM news_auto_tags;
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RAISE NOTICE '✓ 删除 news_auto_tags: % 条', deleted_count;
    
    -- 2.2 删除 news_symbols_dict
    DELETE FROM news_symbols_dict;
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RAISE NOTICE '✓ 删除 news_symbols_dict: % 条', deleted_count;
    
    -- 2.3 保留 news_tags_dict（主题管理使用）
    SELECT COUNT(*) INTO preserved_count FROM news_tags_dict;
    RAISE NOTICE '✓ 保留 news_tags_dict: % 条（主题管理使用）', preserved_count;
    
    RAISE NOTICE '';
END $$;

-- ============================================================
-- 步骤3: 删除主表数据
-- ============================================================

DO $$
DECLARE
    deleted_count INTEGER;
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE '步骤3: 删除主表数据';
    RAISE NOTICE '========================================';
    
    DELETE FROM news;
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RAISE NOTICE '✓ 删除 news: % 条', deleted_count;
    
    RAISE NOTICE '';
END $$;

-- ============================================================
-- 步骤4: 确认主题管理表已保留
-- ============================================================

DO $$
DECLARE
    topics_count INTEGER;
    keywords_count INTEGER;
    tags_dict_count INTEGER;
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE '步骤4: 确认主题管理表已保留';
    RAISE NOTICE '========================================';
    
    SELECT COUNT(*) INTO topics_count FROM news_topics;
    RAISE NOTICE '✓ 保留 news_topics: % 条', topics_count;
    
    SELECT COUNT(*) INTO keywords_count FROM news_topic_keywords;
    RAISE NOTICE '✓ 保留 news_topic_keywords: % 条', keywords_count;
    
    SELECT COUNT(*) INTO tags_dict_count FROM news_tags_dict;
    RAISE NOTICE '✓ 保留 news_tags_dict: % 条', tags_dict_count;
    
    RAISE NOTICE '';
END $$;

-- ============================================================
-- 步骤5: 重置ID序列
-- ============================================================

DO $$
DECLARE
    seq_name TEXT;
    seq_exists BOOLEAN;
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE '步骤5: 重置ID序列';
    RAISE NOTICE '========================================';
    
    -- 重置 news 序列
    SELECT EXISTS (
        SELECT 1 FROM pg_sequences 
        WHERE schemaname = 'public' AND sequencename = 'news_id_seq'
    ) INTO seq_exists;
    
    IF seq_exists THEN
        ALTER SEQUENCE news_id_seq RESTART WITH 1;
        RAISE NOTICE '✓ 重置序列: news_id_seq';
    ELSE
        RAISE NOTICE '⊘ 序列不存在，跳过: news_id_seq';
    END IF;
    
    -- 重置 news_symbols 序列
    SELECT EXISTS (
        SELECT 1 FROM pg_sequences 
        WHERE schemaname = 'public' AND sequencename = 'news_symbols_id_seq'
    ) INTO seq_exists;
    
    IF seq_exists THEN
        ALTER SEQUENCE news_symbols_id_seq RESTART WITH 1;
        RAISE NOTICE '✓ 重置序列: news_symbols_id_seq';
    END IF;
    
    -- 重置 news_tags 序列
    SELECT EXISTS (
        SELECT 1 FROM pg_sequences 
        WHERE schemaname = 'public' AND sequencename = 'news_tags_id_seq'
    ) INTO seq_exists;
    
    IF seq_exists THEN
        ALTER SEQUENCE news_tags_id_seq RESTART WITH 1;
        RAISE NOTICE '✓ 重置序列: news_tags_id_seq';
    END IF;
    
    -- 重置 news_auto_tags_relations 序列
    SELECT EXISTS (
        SELECT 1 FROM pg_sequences 
        WHERE schemaname = 'public' AND sequencename = 'news_auto_tags_relations_id_seq'
    ) INTO seq_exists;
    
    IF seq_exists THEN
        ALTER SEQUENCE news_auto_tags_relations_id_seq RESTART WITH 1;
        RAISE NOTICE '✓ 重置序列: news_auto_tags_relations_id_seq';
    END IF;
    
    -- 重置 news_symbols_dict 序列
    SELECT EXISTS (
        SELECT 1 FROM pg_sequences 
        WHERE schemaname = 'public' AND sequencename = 'news_symbols_dict_id_seq'
    ) INTO seq_exists;
    
    IF seq_exists THEN
        ALTER SEQUENCE news_symbols_dict_id_seq RESTART WITH 1;
        RAISE NOTICE '✓ 重置序列: news_symbols_dict_id_seq';
    END IF;
    
    -- 重置 news_auto_tags 序列
    SELECT EXISTS (
        SELECT 1 FROM pg_sequences 
        WHERE schemaname = 'public' AND sequencename = 'news_auto_tags_id_seq'
    ) INTO seq_exists;
    
    IF seq_exists THEN
        ALTER SEQUENCE news_auto_tags_id_seq RESTART WITH 1;
        RAISE NOTICE '✓ 重置序列: news_auto_tags_id_seq';
    END IF;
    
    RAISE NOTICE '';
END $$;

-- ============================================================
-- 步骤6: 验证结果
-- ============================================================

DO $$
DECLARE
    news_count INTEGER;
    symbols_count INTEGER;
    tags_count INTEGER;
    auto_tags_rel_count INTEGER;
    symbols_dict_count INTEGER;
    auto_tags_count INTEGER;
    all_empty BOOLEAN := true;
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE '步骤6: 验证清理结果';
    RAISE NOTICE '========================================';
    RAISE NOTICE '';
    
    -- 验证应该为空的表
    SELECT COUNT(*) INTO news_count FROM news;
    SELECT COUNT(*) INTO symbols_count FROM news_symbols;
    SELECT COUNT(*) INTO tags_count FROM news_tags;
    SELECT COUNT(*) INTO auto_tags_rel_count FROM news_auto_tags_relations;
    SELECT COUNT(*) INTO symbols_dict_count FROM news_symbols_dict;
    SELECT COUNT(*) INTO auto_tags_count FROM news_auto_tags;
    
    RAISE NOTICE '应该为空的表:';
    RAISE NOTICE '----------------------------------------';
    
    IF news_count = 0 THEN
        RAISE NOTICE '  ✓ news: 空';
    ELSE
        RAISE NOTICE '  ✗ news: % 条（应该为空！）', news_count;
        all_empty := false;
    END IF;
    
    IF symbols_count = 0 THEN
        RAISE NOTICE '  ✓ news_symbols: 空';
    ELSE
        RAISE NOTICE '  ✗ news_symbols: % 条（应该为空！）', symbols_count;
        all_empty := false;
    END IF;
    
    IF tags_count = 0 THEN
        RAISE NOTICE '  ✓ news_tags: 空';
    ELSE
        RAISE NOTICE '  ✗ news_tags: % 条（应该为空！）', tags_count;
        all_empty := false;
    END IF;
    
    IF auto_tags_rel_count = 0 THEN
        RAISE NOTICE '  ✓ news_auto_tags_relations: 空';
    ELSE
        RAISE NOTICE '  ✗ news_auto_tags_relations: % 条（应该为空！）', auto_tags_rel_count;
        all_empty := false;
    END IF;
    
    IF symbols_dict_count = 0 THEN
        RAISE NOTICE '  ✓ news_symbols_dict: 空';
    ELSE
        RAISE NOTICE '  ✗ news_symbols_dict: % 条（应该为空！）', symbols_dict_count;
        all_empty := false;
    END IF;
    
    IF auto_tags_count = 0 THEN
        RAISE NOTICE '  ✓ news_auto_tags: 空';
    ELSE
        RAISE NOTICE '  ✗ news_auto_tags: % 条（应该为空！）', auto_tags_count;
        all_empty := false;
    END IF;
    
    RAISE NOTICE '';
    
    IF all_empty THEN
        RAISE NOTICE '✅ 验证通过: 所有新闻数据已清空';
    ELSE
        RAISE NOTICE '⚠️  警告: 某些表未完全清空，请检查';
    END IF;
    
    RAISE NOTICE '';
    RAISE NOTICE '========================================';
END $$;

-- ============================================================
-- 提交事务
-- ============================================================

COMMIT;

-- 显示最终提示
DO $$
BEGIN
    RAISE NOTICE '';
    RAISE NOTICE '========================================';
    RAISE NOTICE '✅ 清空操作完成！';
    RAISE NOTICE '========================================';
    RAISE NOTICE '';
    RAISE NOTICE '建议执行以下命令优化数据库:';
    RAISE NOTICE '  VACUUM ANALYZE news;';
    RAISE NOTICE '  VACUUM ANALYZE news_symbols;';
    RAISE NOTICE '  VACUUM ANALYZE news_tags;';
    RAISE NOTICE '  VACUUM ANALYZE news_auto_tags_relations;';
    RAISE NOTICE '';
    RAISE NOTICE '或者执行全库优化:';
    RAISE NOTICE '  VACUUM ANALYZE;';
    RAISE NOTICE '========================================';
    RAISE NOTICE '';
END $$;

-- ============================================================
-- 查看清理后的统计信息（可选，注释掉以避免输出过多）
-- ============================================================

-- SELECT 
--     schemaname,
--     tablename,
--     pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
-- FROM pg_tables
-- WHERE tablename LIKE 'news%'
-- ORDER BY tablename;

