-- Bloomberg Surveillance Summary 定时任务
--
-- 节目：Bloomberg Television / YouTube「Bloomberg Surveillance」
--   美东周一至周五 06:00–09:00 ET 直播
--   对应北京：夏令时 18:00–21:00，冬令时 19:00–22:00（无周末场）
-- YouTube VOD 多在节目结束后 1–3h（北京约 22:00–01:00）；FOMC 特辑可更晚。
--
-- 中国用户晚上交易美股（开盘约 21:30 北京 / 冬令时 22:30），希望当晚看到 AM Brief。
-- 成片经常略晚于开盘，因此同一 cron 多次尝试 + 次晨兜底：
--   Mon–Fri 21/22/23  Asia/Shanghai  当晚短窗口（10h，避免并入前一晚）
--   Tue–Sat 09:00     Asia/Shanghai  兜底昨天（24h，含晚到特辑）
-- 执行器会跳过周一 09:00 / 周六晚上，避免空占位。
--
-- Cron: 0 9,21,22,23 * * 1-6    timezone: Asia/Shanghai
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
        SET cron_expression = '0 9,21,22,23 * * 1-6',
            timezone = 'Asia/Shanghai',
            schedule_type = 'cron',
            enabled = TRUE,
            description = 'Bloomberg Surveillance AM Brief: Mon-Fri 21/22/23 Beijing same evening + Tue-Sat 09:00 catch-up for China US-session traders',
            next_run = NULL,
            updated_at = NOW()
        WHERE type = 'bloomberg_surveillance';

        RAISE NOTICE 'Updated bloomberg_surveillance cron to 0 9,21,22,23 * * 1-6 Asia/Shanghai';
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
            'Bloomberg Surveillance Summary',
            'bloomberg_surveillance',
            'cron',
            '0 9,21,22,23 * * 1-6',
            NULL,
            'Asia/Shanghai',
            TRUE,
            '{}',
            'Bloomberg Surveillance AM Brief: Mon-Fri 21/22/23 Beijing same evening + Tue-Sat 09:00 catch-up for China US-session traders',
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
