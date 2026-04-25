-- ─────────────────────────────────────────────────────────────────────
-- 修复 file_processing_details.created_at 的时区错位问题.
--
-- 背景:
--   服务器部署在香港 (UTC+8). 历史上传代码使用 Python `datetime.now()`
--   写入 created_at — 实际写入的是 HK 本地时间裸数字, 但全系统契约要求
--   是 UTC. 表现:
--     DB 存 17:46 (实际是 HK 17:46) → 后端 ensure_utc 当成 UTC 17:46Z
--     发给前端 → 前端 +8h → 显示 "次日 01:46". 错位 8 小时.
--
-- 修复: 一次性把所有历史 created_at 减去 8 小时 (HK = UTC+8 固定, 无夏令时).
-- 修复后, DB 中存的就真的是 UTC naive, 与新代码 (utcnow_naive()) 写入完全一致.
--
-- 触发条件: 只对在本次代码修复之前 (= 此 migration 运行之前) 已存在的行有效.
-- 后续新插入的行已经是 UTC, 不能再减.
--
-- 详见: docs/instructions/DAILY_MODULE.md §13 时区契约
-- 关联代码改动: backend/api/files.py / backend/tasks/file_tasks.py /
--               backend/workers/file_worker.py 中的 utcnow_naive() 替换.
-- ─────────────────────────────────────────────────────────────────────

BEGIN;

-- 安全检查: 如果 max(created_at) 看起来已经是 UTC (例如 < 当前 HK 时间 - 4h),
-- 说明可能已经迁移过, 提示并回滚.
DO $$
DECLARE
    max_ts timestamp;
    cnt int;
BEGIN
    SELECT MAX(created_at), COUNT(*) INTO max_ts, cnt
    FROM file_processing_details
    WHERE created_at IS NOT NULL;

    RAISE NOTICE '即将迁移 % 行, 最新 created_at = %', cnt, max_ts;

    IF max_ts < (NOW() AT TIME ZONE 'UTC' - INTERVAL '4 hours') THEN
        RAISE EXCEPTION '检测到 max(created_at) 已早于当前 UTC 时间 4h 以上, 可能已迁移过. 中止以防双重减时.';
    END IF;
END $$;

UPDATE file_processing_details
   SET created_at = created_at - INTERVAL '8 hours'
 WHERE created_at IS NOT NULL;

-- 注: processing_stats.timestamp 是 text 列, 不在本 migration 范围;
-- 那里的字符串可以等需要时再独立处理.

COMMIT;
