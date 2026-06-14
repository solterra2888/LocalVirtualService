-- 新增 daily_market_report 定时调度任务
-- 北京时间每周二至周六 08:00 生成 Daily Market Report，对应美东周一至周五收盘日的次日晨报。
-- 此时所有上游摘要（Bloomberg The Close 05:30、CNBC Closing Bell 05:15、
-- YouTube Author Summary 05:00、Twitter Author Summary 05:30、知识星球 06:30）均已完成。
--
-- 幂等：若 daily_market_report 类型任务已存在则跳过，不重复插入。

DO $$
DECLARE
    v_id TEXT;
    v_ts BIGINT;
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM task_scheduling WHERE type = 'daily_market_report'
    ) THEN
        v_ts := EXTRACT(EPOCH FROM NOW())::BIGINT;
        v_id := 'daily_market_report_Daily Market Report_' || v_ts::TEXT;

        INSERT INTO task_scheduling (
            id,
            name,
            type,
            schedule_type,
            cron_expression,
            interval_minutes,
            timezone,
            enabled,
            params,
            created_at,
            last_run,
            next_run,
            success_count,
            fail_count,
            created_by
        ) VALUES (
            v_id,
            'Daily Market Report',
            'daily_market_report',
            'cron',
            '0 8 * * 2-6',   -- 周二至周六 08:00 北京时间（对应美东收盘日 Mon-Fri 的次日）
            NULL,
            'Asia/Shanghai',
            TRUE,
            '{}',
            NOW(),
            NULL,
            NULL,
            0,
            0,
            'migration'
        );

        RAISE NOTICE 'Inserted daily_market_report task with id=%', v_id;
    ELSE
        RAISE NOTICE 'daily_market_report task already exists, skipping.';
    END IF;
END;
$$;
