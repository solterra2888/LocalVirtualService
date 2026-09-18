-- Bloomberg Surveillance Summary：每天一次，北京夜盘后出报
-- 周二至周六 00:30 Asia/Shanghai（report_date=昨天，覆盖 23:00–00:10 全场 VOD）。

UPDATE task_scheduling
SET cron_expression = '30 0 * * 2-6',
    timezone = 'Asia/Shanghai',
    schedule_type = 'cron',
    description = 'Bloomberg Surveillance AM Brief once daily Tue-Sat 00:30 Beijing after YouTube VOD (previous US session)',
    next_run = NULL,
    updated_at = NOW()
WHERE type = 'bloomberg_surveillance';
