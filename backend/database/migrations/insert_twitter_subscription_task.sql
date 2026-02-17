-- =====================================================
-- Twitter Subscription Scheduler Task Configuration
-- =====================================================
-- 为Twitter订阅系统添加调度器任务配置

-- 1. 首先修改 valid_task_type 约束，添加 twitter_subscription 类型
DO $$
BEGIN
    -- 删除旧的约束
    ALTER TABLE task_scheduling DROP CONSTRAINT IF EXISTS valid_task_type;
    
    -- 添加新的约束，包含 twitter_subscription
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
        'twitter_subscription'::character varying
    ]::text[]));
    
    RAISE NOTICE 'Twitter subscription task type added to valid_task_type constraint';
END $$;

-- 2. 插入主要的Twitter订阅任务（每30分钟执行）
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
    'twitter_subscription',  -- id 直接使用 type 作为唯一标识
    'Twitter Subscription Feed',
    'twitter_subscription',
    'cron',
    '*/30 * * * *',  -- 每30分钟执行一次
    'America/New_York',
    true,  -- 默认启用
    '从用户订阅的Twitter账号获取最新推文',
    '{}'::jsonb,
    CURRENT_TIMESTAMP,
    'system'
)
ON CONFLICT (type) DO UPDATE SET
    name = EXCLUDED.name,
    schedule_type = EXCLUDED.schedule_type,
    cron_expression = EXCLUDED.cron_expression,
    description = EXCLUDED.description,
    updated_at = CURRENT_TIMESTAMP;

-- 3. 验证插入结果
SELECT 
    id,
    name, 
    type, 
    schedule_type,
    cron_expression,
    enabled, 
    description,
    created_at
FROM task_scheduling
WHERE type = 'twitter_subscription';
