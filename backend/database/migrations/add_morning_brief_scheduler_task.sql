-- 新增 morning_brief 定时调度任务(Morning Brief / facts-report)
-- 北京时间周二至周六 08:00 触发 facts-report LLM agent 管线,对应美东周一至周五
-- 收盘日的次日晨报。就绪门(show_gate)与 Lark 告警在管线内部处理。
--
-- enabled 初始为 FALSE:cutover 分两步 —— Phase 1-3(建表/注册器/只读API)验证
-- 通过后置 TRUE,观察一次 Scheduler 自然触发成功,再注释服务器 crontab 08:00 行。
--
-- 幂等：若 morning_brief 类型任务已存在则跳过，不重复插入。

DO $$
DECLARE
    v_id TEXT;
    v_ts BIGINT;
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM task_scheduling WHERE type = 'morning_brief'
    ) THEN
        v_ts := EXTRACT(EPOCH FROM NOW())::BIGINT;
        v_id := 'morning_brief_Morning Brief_' || v_ts::TEXT;

        INSERT INTO task_scheduling (
            id, name, type, schedule_type, cron_expression, interval_minutes,
            timezone, enabled, params, created_at, last_run, next_run,
            success_count, fail_count, created_by
        ) VALUES (
            v_id,
            'Morning Brief (facts-report)',
            'morning_brief',
            'cron',
            '0 8 * * 2-6',   -- 周二至周六 08:00 北京时间(美东收盘日 Mon-Fri 的次日晨)
            NULL,
            'Asia/Shanghai',
            FALSE,           -- cutover 前保持关闭;验证后置 TRUE 并注释 crontab
            '{}',
            NOW(),
            NULL,
            NULL,
            0,
            0,
            'migration'
        );

        RAISE NOTICE 'Inserted morning_brief task with id=%', v_id;
    ELSE
        RAISE NOTICE 'morning_brief task already exists, skipping.';
    END IF;
END;
$$;
