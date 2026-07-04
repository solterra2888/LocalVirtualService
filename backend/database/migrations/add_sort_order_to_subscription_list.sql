-- =====================================================
-- 为 subscription_list 添加 sort_order 字段
-- 目的: 支持每用户独立的订阅显示排序（左侧订阅栏拖拽自定义顺序）
-- 说明: 按 priority DESC, created_at DESC 回填，保持迁移前 UI 顺序不变（零感知）
-- =====================================================

-- 1. 添加 sort_order 字段
--    不设 DEFAULT：应用层在创建订阅时算 MAX(sort_order)+1 追加到末尾，
--    历史 NULL 由下面的回填补齐，列表查询用 COALESCE(sort_order, 999999) 兜底。
ALTER TABLE subscription_list
ADD COLUMN IF NOT EXISTS sort_order INTEGER;

COMMENT ON COLUMN subscription_list.sort_order IS
'显示排序（每用户独立，越小越靠前；NULL 按 priority/created_at 兜底）';

-- 2. 回填现有订阅的 sort_order（按 user_id 分区，沿用迁移前的 UI 顺序）
--    仅回填 NULL，避免覆盖应用层后续写入的值
DO $$
BEGIN
    WITH ranked AS (
        SELECT
            id,
            ROW_NUMBER() OVER (
                PARTITION BY user_id
                ORDER BY priority DESC, created_at DESC, id DESC
            ) AS rn
        FROM subscription_list
    )
    UPDATE subscription_list AS s
    SET sort_order = ranked.rn
    FROM ranked
    WHERE ranked.id = s.id AND s.sort_order IS NULL;
    RAISE NOTICE 'sort_order backfill done';
END $$;

-- 3. 复合索引：加速"按用户列出订阅并按 sort_order 排序"的查询
CREATE INDEX IF NOT EXISTS idx_subscription_user_sort
    ON subscription_list (user_id, sort_order);

-- 验证
SELECT
    user_id,
    COUNT(*) AS subscription_count,
    COUNT(sort_order) AS filled_count
FROM subscription_list
GROUP BY user_id
ORDER BY user_id;
