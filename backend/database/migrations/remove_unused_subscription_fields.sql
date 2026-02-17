-- ======================================================================
-- 删除 subscription_list 表中的无用字段
-- ======================================================================
-- 
-- 创建时间: 2026-02-17
-- 原因: 在新的 Celery 架构和 Scheduler 统一调度下，以下字段已不再使用
--
-- 删除的字段及原因:
-- 1. priority              - 不再需要，Scheduler 统一调度所有订阅，不基于优先级
-- 2. fetch_interval_minutes - 不再需要，Scheduler 统一 30 分钟调度一次
-- 3. max_items_per_fetch    - 不再需要，Twitter API 返回固定数量的推文
-- 4. next_fetch_at          - 完全未使用，Scheduler 不基于此字段调度
--
-- 保留的字段（仍在使用）:
-- - last_fetched_at         - 记录最后获取时间，在 twitter_subscription_service.py 中更新
-- - total_items_fetched     - 用于统计总获取数量
-- - last_fetch_items_count  - 记录最后一次获取的数量
-- - consecutive_failures    - 跟踪连续失败次数
-- - unread_count            - 未读内容计数
--
-- ======================================================================

-- 1. 备份当前表结构（可选）
-- 在执行删除前，可以先查看当前使用情况
DO $$
DECLARE
    v_priority_stats TEXT;
    v_fetch_interval_stats TEXT;
    v_max_items_stats TEXT;
    v_next_fetch_stats TEXT;
BEGIN
    -- 检查 priority 字段使用情况
    SELECT 
        COUNT(*) as total,
        COUNT(DISTINCT priority) as distinct_values,
        STRING_AGG(DISTINCT priority::text, ', ') as values
    INTO v_priority_stats
    FROM subscription_list;
    
    RAISE NOTICE '📊 priority 字段统计: %', v_priority_stats;
    
    -- 检查 fetch_interval_minutes 字段使用情况
    SELECT 
        COUNT(*) as total,
        COUNT(DISTINCT fetch_interval_minutes) as distinct_values,
        STRING_AGG(DISTINCT fetch_interval_minutes::text, ', ') as values
    INTO v_fetch_interval_stats
    FROM subscription_list;
    
    RAISE NOTICE '📊 fetch_interval_minutes 字段统计: %', v_fetch_interval_stats;
    
    -- 检查 max_items_per_fetch 字段使用情况
    SELECT 
        COUNT(*) as total,
        COUNT(DISTINCT max_items_per_fetch) as distinct_values,
        STRING_AGG(DISTINCT max_items_per_fetch::text, ', ') as values
    INTO v_max_items_stats
    FROM subscription_list;
    
    RAISE NOTICE '📊 max_items_per_fetch 字段统计: %', v_max_items_stats;
    
    -- 检查 next_fetch_at 字段使用情况
    SELECT 
        COUNT(*) as total,
        COUNT(next_fetch_at) as non_null_count
    INTO v_next_fetch_stats
    FROM subscription_list;
    
    RAISE NOTICE '📊 next_fetch_at 字段统计: %', v_next_fetch_stats;
END $$;

-- 2. 删除无用字段
BEGIN;

-- 删除 priority 字段
ALTER TABLE subscription_list 
DROP COLUMN IF EXISTS priority;

RAISE NOTICE '✅ 已删除 priority 字段';

-- 删除 fetch_interval_minutes 字段
ALTER TABLE subscription_list 
DROP COLUMN IF EXISTS fetch_interval_minutes;

RAISE NOTICE '✅ 已删除 fetch_interval_minutes 字段';

-- 删除 max_items_per_fetch 字段
ALTER TABLE subscription_list 
DROP COLUMN IF EXISTS max_items_per_fetch;

RAISE NOTICE '✅ 已删除 max_items_per_fetch 字段';

-- 删除 next_fetch_at 字段
ALTER TABLE subscription_list 
DROP COLUMN IF EXISTS next_fetch_at;

RAISE NOTICE '✅ 已删除 next_fetch_at 字段';

COMMIT;

-- 3. 验证删除结果
DO $$
DECLARE
    v_column_count INTEGER;
BEGIN
    SELECT COUNT(*)
    INTO v_column_count
    FROM information_schema.columns
    WHERE table_name = 'subscription_list'
    AND column_name IN ('priority', 'fetch_interval_minutes', 'max_items_per_fetch', 'next_fetch_at');
    
    IF v_column_count = 0 THEN
        RAISE NOTICE '✅ 验证成功：所有无用字段已删除';
    ELSE
        RAISE WARNING '⚠️  警告：仍有 % 个字段未删除', v_column_count;
    END IF;
END $$;

-- 4. 显示当前表结构
\d subscription_list

-- 5. 统计当前字段数量
SELECT 
    COUNT(*) as total_columns,
    STRING_AGG(column_name, ', ' ORDER BY ordinal_position) as column_names
FROM information_schema.columns
WHERE table_name = 'subscription_list';

-- ======================================================================
-- 完成！
-- ======================================================================
