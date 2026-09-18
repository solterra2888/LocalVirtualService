-- 调度器展示名与 Reports 模块对齐（任务 type 不变）

UPDATE task_scheduling SET name = 'Bloomberg The Close Report', updated_at = NOW()
WHERE type = 'the_close_summary';

UPDATE task_scheduling SET name = 'CNBC Closing Bell Report', updated_at = NOW()
WHERE type = 'cnbc_closing_bell';

UPDATE task_scheduling SET name = 'Bloomberg Surveillance Report', updated_at = NOW()
WHERE type = 'bloomberg_surveillance';
