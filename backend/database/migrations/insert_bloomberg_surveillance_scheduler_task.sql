-- Bloomberg Surveillance Summary 定时任务
--
-- 节目：Bloomberg Television / YouTube「Bloomberg Surveillance」
--   美东周一至周五 06:00–09:00 ET 直播
--   对应北京：夏令时 18:00–21:00，冬令时 19:00–22:00（无周末场）
-- YouTube 全场 VOD 多在直播结束后 2–3h 入库（北京约 23:00–00:10）。
--
-- 每天一次：Tue–Sat 00:30 Asia/Shanghai（report_date=昨天，24h 窗口）
-- 执行器会跳过周一 00:30，避免空占位。
--
-- Cron: 30 0 * * 2-6    timezone: Asia/Shanghai
-- 幂等：已有 bloomberg_surveillance 行则更新 cron/timezone，否则插入。

DO $$
DECLARE
    v_id TEXT;
    v_ts BIGINT;
BEGIN
    IF EXISTS (
        SELECT 1 FROM task_scheduling WHERE type = 'bloomberg_surveillance'
    ) THEN
        UPDATE task_scheduling
        SET cron_expression = '30 0 * * 2-6',
            timezone = 'Asia/Shanghai',
            schedule_type = 'cron',
            enabled = TRUE,
            description = 'Bloomberg Surveillance AM Brief once daily Tue-Sat 00:30 Beijing after YouTube VOD (previous US session)',
            next_run = NULL,
            updated_at = NOW()
        WHERE type = 'bloomberg_surveillance';

        RAISE NOTICE 'Updated bloomberg_surveillance cron to 30 0 * * 2-6 Asia/Shanghai';
    ELSE
        v_ts := EXTRACT(EPOCH FROM NOW())::BIGINT;
        v_id := 'bloomberg_surveillance_Bloomberg Surveillance Summary_' || v_ts::TEXT;

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
            description,
            created_at,
            last_run,
            next_run,
            success_count,
            fail_count,
            created_by
        ) VALUES (
            v_id,
            'Bloomberg Surveillance Report',
            'bloomberg_surveillance',
            'cron',
            '30 0 * * 2-6',
            NULL,
            'Asia/Shanghai',
            TRUE,
            '{}',
            'Bloomberg Surveillance AM Brief once daily Tue-Sat 00:30 Beijing after YouTube VOD (previous US session)',
            NOW(),
            NULL,
            NULL,
            0,
            0,
            'migration'
        );

        RAISE NOTICE 'Inserted bloomberg_surveillance task with id=%', v_id;
    END IF;
END;
$$;
