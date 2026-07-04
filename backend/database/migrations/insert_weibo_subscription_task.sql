-- =====================================================
-- Weibo Subscription Scheduler Task Configuration
-- =====================================================
-- 为微博订阅系统添加调度器任务配置
--
-- 1. 更新 valid_task_type 约束（合并全部已有类型 + weibo_subscription）
--    注：db_manager._update_task_scheduling_constraints 也会在 ensure_schema 时自动合并，
--    此处显式 DROP+ADD 作为权威兜底（见 docs/instructions/SCHEDULER_COMMON_PITFALLS.md #3）。
-- 2. 插入种子任务（每 30 分钟，对齐 zhishixingqiu_subscription）

DO $$
BEGIN
    ALTER TABLE task_scheduling DROP CONSTRAINT IF EXISTS valid_task_type;

    ALTER TABLE task_scheduling ADD CONSTRAINT valid_task_type
    CHECK (type::text = ANY (ARRAY[
        'twitter_subscription'::text,
        'youtube_subscription'::text,
        'economic_events'::text,
        'macro_indicators'::text,
        'eodhd_fundamentals'::text,
        'watchlist_fundamentals'::text,
        'finviz_screener'::text,
        'eodhd_stock_prices'::text,
        'eodhd_eod_stock_prices'::text,
        'market_data'::text,
        'earnings_calendar'::text,
        'ipo_calendar'::text,
        'news_feed'::text,
        'twitter_author_summary'::text,
        'twitter_daily_report'::text,
        'youtube_author_summary'::text,
        'theme_report'::text,
        'market_weekly_report'::text,
        'daily_market_report'::text,
        'the_close_summary'::text,
        'cnbc_closing_bell'::text,
        'zhishixingqiu_summary'::text,
        'podcast_subscription'::text,
        'weibo_subscription'::text,
        -- legacy rows in some deployments
        'cctv_news'::text,
        'eodhd_news'::text,
        'watchlist_news'::text,
        'zhishixingqiu_subscription'::text
    ]::text[]));

    RAISE NOTICE 'weibo_subscription task type added to valid_task_type constraint';
END $$;

-- 插入微博定时任务（每 30 分钟执行）
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
    'weibo_subscription',
    'Weibo Subscription Feed',
    'weibo_subscription',
    'cron',
    '*/30 * * * *',
    'America/New_York',
    true,
    '从用户订阅的微博博主获取最新博文并入库 subscription_content',
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

-- 验证插入结果
SELECT
    id, name, type, schedule_type, cron_expression,
    enabled, description, created_at
FROM task_scheduling
WHERE type = 'weibo_subscription';
