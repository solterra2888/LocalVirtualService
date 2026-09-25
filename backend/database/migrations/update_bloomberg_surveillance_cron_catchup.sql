-- Bloomberg Surveillance 生成方式与 The Close / Closing Bell 相同（每次一个任务、
-- report_date=北京自然日、24h 窗口）。时刻单独放在正片通常已入库的 12:30。

UPDATE task_scheduling
SET cron_expression = '30 12 * * 2-6',
    timezone = 'Asia/Shanghai',
    description = 'Bloomberg Surveillance AM Brief Tue-Sat 12:30 Beijing, same generation path as The Close / Closing Bell',
    next_run = NULL,
    updated_at = NOW()
WHERE type = 'bloomberg_surveillance';
