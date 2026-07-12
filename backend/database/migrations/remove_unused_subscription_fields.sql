-- ======================================================================
-- 删除 subscription_list 表中的无用字段 (2026-07-12 修订版)
-- ======================================================================
--
-- 背景: 旧的统一调度 (Scheduler + Celery) 下, 部分 subscription_list 字段
-- 不再被任何抓取逻辑读取. 本迁移删除"完全死掉"的字段.
--
-- 删除 (真正无用, 7 源都不读, 仅 schema/API/类型残留):
--   1. fetch_interval_minutes - 单订阅刷新周期; 实际由 task_scheduling cron 控制, 此字段从不被读
--   2. next_fetch_at          - 完全未使用, Scheduler 不基于此调度
--
-- 保留 (仍在使用, 旧版迁移误列删除, 2026-07-12 修正):
--   - priority           - 仍用于订阅列表 ORDER BY (api/subscriptions.py 及 twitter 服务),
--                          虽然多数行取默认值, 但 SQL 引用了它, 删除会破坏排序查询.
--   - max_items_per_fetch - YouTube / Podcast 抓取实际读取此字段控制单次条数上限.
--
-- 部署顺序: 先部署"不再引用 fetch_interval_minutes/next_fetch_at"的代码, 再执行本迁移.
-- ======================================================================

-- 1. (可选) 删除前统计, 便于审计
DO $$
DECLARE
    v_interval_stats TEXT;
    v_next_fetch_stats TEXT;
BEGIN
    SELECT
        COUNT(*) AS total,
        COUNT(DISTINCT fetch_interval_minutes) AS distinct_values
    INTO v_interval_stats
    FROM subscription_list;
    RAISE NOTICE '📊 fetch_interval_minutes 统计: %', v_interval_stats;

    SELECT
        COUNT(*) AS total,
        COUNT(next_fetch_at) AS non_null_count
    INTO v_next_fetch_stats
    FROM subscription_list;
    RAISE NOTICE '📊 next_fetch_at 统计 (non_null): %', v_next_fetch_stats;
END $$;

-- 2. 删除两个无用字段
ALTER TABLE subscription_list DROP COLUMN IF EXISTS fetch_interval_minutes;
ALTER TABLE subscription_list DROP COLUMN IF EXISTS next_fetch_at;

-- 3. 验证删除结果 (priority / max_items_per_fetch 应仍存在)
DO $$
DECLARE
    v_dropped   INTEGER;
    v_kept      INTEGER;
BEGIN
    SELECT COUNT(*) INTO v_dropped
    FROM information_schema.columns
    WHERE table_name = 'subscription_list'
      AND column_name IN ('fetch_interval_minutes', 'next_fetch_at');

    SELECT COUNT(*) INTO v_kept
    FROM information_schema.columns
    WHERE table_name = 'subscription_list'
      AND column_name IN ('priority', 'max_items_per_fetch');

    IF v_dropped = 0 AND v_kept = 2 THEN
        RAISE NOTICE '✅ 验证成功: fetch_interval_minutes / next_fetch_at 已删除; priority / max_items_per_fetch 保留';
    ELSE
        RAISE WARNING '⚠️ 异常: dropped=%, kept=% (期望 dropped=0, kept=2)', v_dropped, v_kept;
    END IF;
END $$;

-- 4. 当前 subscription_list 字段清单
SELECT
    COUNT(*) AS total_columns,
    STRING_AGG(column_name, ', ' ORDER BY ordinal_position) AS column_names
FROM information_schema.columns
WHERE table_name = 'subscription_list';
