-- =====================================================
-- Substack Subscription Scheduler Task
-- =====================================================
-- 为 Substack 订阅系统添加调度器任务配置

-- 1. 扩展 valid_task_type 约束，加入 substack_subscription
ALTER TABLE task_scheduling DROP CONSTRAINT IF EXISTS valid_task_type;

ALTER TABLE task_scheduling ADD CONSTRAINT valid_task_type CHECK (type IN (
    'twitter_subscription',
    'youtube_subscription',
    'zhishixingqiu_subscription',
    'substack_subscription',
    'economic_events',
    'macro_indicators',
    'eodhd_fundamentals',
    'watchlist_fundamentals',
    'finviz_screener',
    'eodhd_stock_prices',
    'eodhd_eod_stock_prices',
    'market_data',
    'earnings_calendar',
    'ipo_calendar',
    'news_feed',
    'twitter_author_summary',
    'twitter_daily_report',
    'youtube_author_summary',
    'theme_report',
    'market_weekly_report',
    'daily_market_report',
    'the_close_summary',
    'cnbc_closing_bell',
    'zhishixingqiu_summary',
    'substack_summary',
    'podcast_subscription',
    'weibo_subscription',
    -- legacy types still referenced by existing rows
    'cctv_news',
    'eodhd_news',
    'watchlist_news'
));

-- 2. 插入 Substack 定时任务（每30分钟执行）
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
    'substack_subscription',
    'Substack Subscription Feed',
    'substack_subscription',
    'cron',
    '*/30 * * * *',
    'America/New_York',
    true,
    '从管理员共享 Cookie 抓取 Substack 订阅源（文章 + 动态）并入库 subscription_content',
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

-- 3. 验证插入结果
SELECT
    id, name, type, schedule_type, cron_expression,
    enabled, description, created_at
FROM task_scheduling
WHERE type = 'substack_subscription';
