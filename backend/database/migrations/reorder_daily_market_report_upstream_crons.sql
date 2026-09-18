-- 调整 Daily Market Report 与上游订阅摘要的 cron 顺序（Asia/Shanghai）
--
-- 现网问题：daily_market_report 曾为 05:59（周一至周六），与 Twitter 06:00
--   抢跑，且早于 YouTube 06:30 / Substack 07:00 / 知识星球 08:00，
--   导致 My Subscription 标签页拿不到当天用户摘要。
--
-- 产品约束：Daily Market Report 必须在北京时间 06:00 生成。
-- 因此上游摘要全部提前到 06:00 之前（30 分钟错开，YouTube LLM 给 1.5h）：
--   04:00  twitter_author_summary     每天
--   04:30  youtube_author_summary     每天（最耗时）
--   05:00  substack_summary           每天
--   05:30  zhishixingqiu_summary      每天
--   06:00  daily_market_report        周一至周六
--
-- Bloomberg The Close / CNBC Closing Bell 已从 DMR 拆出独立报告，本迁移不改它们。
--
-- 幂等：只更新 cron / timezone / next_run，不改 enabled、params、任务 ID。
-- 应用后必须重启 rc-scheduler，APScheduler 才会从 DB 重载 trigger。

UPDATE task_scheduling
SET cron_expression = '0 4 * * *',
    timezone = 'Asia/Shanghai',
    schedule_type = 'cron',
    description = 'Twitter Daily Report 04:00 Beijing; Phase A author summaries then Phase B user reports, before Daily Market Report 06:00',
    next_run = NULL,
    updated_at = NOW()
WHERE type = 'twitter_author_summary';

UPDATE task_scheduling
SET cron_expression = '30 4 * * *',
    timezone = 'Asia/Shanghai',
    schedule_type = 'cron',
    description = 'YouTube Daily Report 04:30 Beijing; Phase A channel summaries then Phase B user reports, before Daily Market Report 06:00',
    next_run = NULL,
    updated_at = NOW()
WHERE type = 'youtube_author_summary';

UPDATE task_scheduling
SET cron_expression = '0 5 * * *',
    timezone = 'Asia/Shanghai',
    schedule_type = 'cron',
    description = 'Substack daily summary 05:00 Beijing; Phase A publications then Phase B user reports, before Daily Market Report 06:00',
    next_run = NULL,
    updated_at = NOW()
WHERE type = 'substack_summary';

UPDATE task_scheduling
SET cron_expression = '30 5 * * *',
    timezone = 'Asia/Shanghai',
    schedule_type = 'cron',
    description = 'Zhishixingqiu daily summary 05:30 Beijing; Phase A groups then Phase B user reports, before Daily Market Report 06:00',
    next_run = NULL,
    updated_at = NOW()
WHERE type = 'zhishixingqiu_summary';

UPDATE task_scheduling
SET cron_expression = '0 6 * * 1-6',
    timezone = 'Asia/Shanghai',
    schedule_type = 'cron',
    description = 'Daily Market Report 06:00 Beijing Mon-Sat, after Twitter/YouTube/Substack/Zhishixingqiu summaries',
    next_run = NULL,
    updated_at = NOW()
WHERE type = 'daily_market_report';
