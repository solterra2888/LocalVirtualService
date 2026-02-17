-- 删除 subscription_content 表中无用的 subscription_id 字段
-- 
-- 背景：
-- 在多订阅共享内容架构中，subscription_id 字段已经没有意义：
-- - 内容属于作者，不属于单个订阅
-- - user_ids 数组管理可见性
-- - 同一作者的内容对所有订阅者可见
--
-- 修改日期: 2026-02-17
-- 相关文档: docs/MULTI_SUBSCRIPTION_FIX.md

-- 1. 检查当前字段状态（应该全部为 NULL）
SELECT 
    'Before migration' as status,
    COUNT(*) as total_records,
    COUNT(subscription_id) as non_null_count,
    COUNT(*) - COUNT(subscription_id) as null_count
FROM subscription_content;

-- 2. 备份当前表结构（可选，建议在重要环境执行）
-- CREATE TABLE subscription_content_backup_20260217 AS SELECT * FROM subscription_content;

-- 3. 删除 subscription_id 字段
ALTER TABLE subscription_content DROP COLUMN IF EXISTS subscription_id;

-- 4. 验证删除结果
SELECT 
    'After migration' as status,
    column_name,
    data_type,
    is_nullable
FROM information_schema.columns
WHERE table_name = 'subscription_content'
    AND column_name = 'subscription_id';

-- 如果上面的查询返回空结果，说明字段已成功删除

-- 5. 验证表结构（列出所有字段）
SELECT 
    column_name,
    data_type,
    is_nullable,
    column_default
FROM information_schema.columns
WHERE table_name = 'subscription_content'
ORDER BY ordinal_position;

-- 6. 验证数据完整性
SELECT 
    'Data integrity check' as check_type,
    COUNT(*) as total_records,
    COUNT(DISTINCT channel_type) as channel_types,
    COUNT(DISTINCT author_identifier) as unique_authors
FROM subscription_content;

-- 迁移完成！
