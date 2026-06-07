-- =====================================================
-- Zhishixingqiu (ZSXQ) Subscription Scheduler Task
-- =====================================================
-- 为知识星球订阅系统添加调度器任务配置

-- 1. 扩展 valid_task_type 约束，加入 zhishixingqiu_subscription
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
        'news_feed'::character varying,
        'twitter_author_summary'::character varying,
        'twitter_daily_report'::character varying,
        'youtube_author_summary'::character varying,
        'theme_report'::character varying,
        'market_weekly_report'::character varying,
        'watchlist_news'::character varying,
        'zhishixingqiu_subscription'::character varying
    ]::text[]));

    RAISE NOTICE 'zhishixingqiu_subscription task type added to valid_task_type constraint';
END $$;

-- 2. 插入知识星球定时任务（每30分钟执行）
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
    'zhishixingqiu_subscription',
    'Zhishixingqiu Subscription Feed',
    'zhishixingqiu_subscription',
    'cron',
    '*/30 * * * *',
    'America/New_York',
    true,
    '从用户订阅的知识星球获取最新话题并入库 subscription_content',
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
WHERE type = 'zhishixingqiu_subscription';
