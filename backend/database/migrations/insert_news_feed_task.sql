-- =====================================================
-- News Feed Scheduler Task Configuration
-- =====================================================
-- 为 My Feeds 新闻订阅系统添加调度器任务配置

-- 1. 修改 valid_task_type 约束，添加 news_feed 类型
DO $$
BEGIN
    ALTER TABLE task_scheduling DROP CONSTRAINT IF EXISTS valid_task_type;

    ALTER TABLE task_scheduling ADD CONSTRAINT valid_task_type
    CHECK (type::text = ANY (ARRAY[
        'eodhd_eod_stock_prices'::character varying,
        'watchlist_news'::character varying,
        'watchlist_fundamentals'::character varying,
        'economic_events'::character varying,
        'finviz_screener'::character varying,
        'futu_themes'::character varying,
        'eodhd_stock_prices'::character varying,
        'cctv_news'::character varying,
        'ipo_calendar'::character varying,
        'eodhd_fundamentals'::character varying,
        'news_analysis'::character varying,
        'market_data'::character varying,
        'eodhd_news'::character varying,
        'earnings_calendar'::character varying,
        'macro_indicators'::character varying,
        'twitter_subscription'::character varying,
        'youtube_subscription'::character varying,
        'news_feed'::character varying
    ]::text[]));

    RAISE NOTICE 'news_feed task type added to valid_task_type constraint';
END $$;

-- 2. 插入 News Feed 定时任务（每30分钟执行）
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
    'news_feed',
    'News Feed Auto Fetch',
    'news_feed',
    'cron',
    '*/30 * * * *',
    'America/New_York',
    true,
    '从 EODHD API 拉取新闻并分发到 My Feeds 订阅用户',
    '{"max_results": 200, "recent_days": 1}'::jsonb,
    CURRENT_TIMESTAMP,
    'system'
)
ON CONFLICT (id) DO UPDATE SET
    name = EXCLUDED.name,
    schedule_type = EXCLUDED.schedule_type,
    cron_expression = EXCLUDED.cron_expression,
    description = EXCLUDED.description,
    params = EXCLUDED.params,
    updated_at = CURRENT_TIMESTAMP;

-- 3. 验证插入结果
SELECT
    id, name, type, schedule_type, cron_expression,
    enabled, description, created_at
FROM task_scheduling
WHERE type = 'news_feed'
ORDER BY id;
