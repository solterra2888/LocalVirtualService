-- economic_events 管道修复:
-- 1) 唯一约束加入 comparison 列(NULLS NOT DISTINCT, 需 PG15+, 当前 PG17.9)
--    背景: EODHD 对同一事件返回最多 3 条口径记录(comparison=null 指数级 / 'yoy' / 'mom'),
--    旧 4 列约束导致互相覆盖, 全表 comparison 均为 NULL 且数值口径混乱。
--    现有数据 comparison 全 NULL 且旧约束已保证 4 列唯一, 新约束可直接创建成功, 无需数据去重。
-- 2) economic_events 调度任务: params 增加 lookback_days 回看窗口(回填已发布事件的 actual),
--    cron 从每周日改为每天一次。
-- 幂等, 可重复执行。执行前请确认调度任务不在运行时段(lock_timeout 5s 保护)。

SET lock_timeout = '5s';

BEGIN;

-- 1) 重建唯一约束(保持约束名不变, ON CONFLICT ON CONSTRAINT 引用无需改动)
ALTER TABLE economic_events
    DROP CONSTRAINT IF EXISTS economic_events_unique_constraint;

ALTER TABLE economic_events
    ADD CONSTRAINT economic_events_unique_constraint
    UNIQUE NULLS NOT DISTINCT (date, "time", country, event, comparison);

-- 2a) 调度任务 params 加 lookback_days(已存在则不覆盖)
UPDATE task_scheduling
SET params = params || '{"lookback_days": 14}'::jsonb,
    updated_at = CURRENT_TIMESTAMP
WHERE type = 'economic_events'
  AND NOT (params ? 'lookback_days');

-- 2b) cron 从每周日 12 点改为每天 12 点
UPDATE task_scheduling
SET cron_expression = '0 12 * * *',
    updated_at = CURRENT_TIMESTAMP
WHERE type = 'economic_events'
  AND cron_expression = '0 12 * * 0';

COMMIT;

RESET lock_timeout;

-- 验证
SELECT conname, pg_get_constraintdef(oid) AS definition
FROM pg_constraint
WHERE conrelid = 'economic_events'::regclass
  AND contype = 'u';

SELECT name, cron_expression, params
FROM task_scheduling
WHERE type = 'economic_events';
