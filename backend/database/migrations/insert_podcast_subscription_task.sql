-- =====================================================
-- Podcast Subscription Scheduler Task Configuration
-- =====================================================
-- 为 Podcast RSS 订阅系统添加调度器任务配置
-- valid_task_type：生产环境通常已由 db_manager.ensure_schema 或历史 migration 包含
-- podcast_subscription；若 INSERT 报 CHECK 违反，请先执行合并全部类型的约束 migration。

INSERT INTO task_scheduling (
    id,
    name,
    type,
    schedule_type,
    cron_expression,
    timezone,
    enabled,
    description,
    params,
    created_at,
    created_by
) VALUES (
    'podcast_subscription',
    'Podcast Subscription Feed',
    'podcast_subscription',
    'cron',
    '0 * * * *',  -- 每小时整点执行（与 YouTube 订阅一致）
    'America/New_York',
    true,
    '从用户订阅的 Podcast RSS Feed 获取最新 episode，并对近 48 小时内发布的节目自动 ASR 转写',
    '{}'::jsonb,
    CURRENT_TIMESTAMP,
    'system'
)
ON CONFLICT (id) DO UPDATE SET
    name = EXCLUDED.name,
    schedule_type = EXCLUDED.schedule_type,
    cron_expression = EXCLUDED.cron_expression,
    description = EXCLUDED.description,
    updated_at = CURRENT_TIMESTAMP;

SELECT
    id, name, type, schedule_type, cron_expression,
    enabled, description, created_at
FROM task_scheduling
WHERE type = 'podcast_subscription';
