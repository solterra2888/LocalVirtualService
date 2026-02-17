-- =====================================================
-- YouTube Subscription Scheduler Task Configuration
-- =====================================================
-- 为YouTube订阅系统添加调度器任务配置

-- 1. 首先修改 valid_task_type 约束，添加 youtube_subscription 类型
DO $$
BEGIN
    -- 删除旧的约束
    ALTER TABLE task_scheduling DROP CONSTRAINT IF EXISTS valid_task_type;
    
    -- 添加新的约束，包含 youtube_subscription
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
        'youtube_subscription'::character varying
    ]::text[]));
    
    RAISE NOTICE 'YouTube subscription task type added to valid_task_type constraint';
END $$;

-- 2. 插入主要的YouTube订阅任务（每60分钟执行）
-- YouTube RSS Feed 更新较慢，不需要像Twitter那样频繁获取
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
    'youtube_subscription',  -- id 直接使用 type 作为唯一标识
    'YouTube Subscription Feed',
    'youtube_subscription',
    'cron',
    '0 * * * *',  -- 每小时整点执行一次
    'America/New_York',
    true,  -- 默认启用
    '从用户订阅的YouTube频道获取最新视频',
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

-- 3. 可选：创建测试任务（手动触发，用于测试）
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
    'youtube_subscription_test',
    'YouTube Subscription Feed (Test)',
    'youtube_subscription',
    'cron',
    '0 0 1 1 *',  -- 永不自动执行（1月1日午夜）
    'America/New_York',
    false,  -- 默认禁用
    '手动触发YouTube订阅测试任务',
    '{"force_fetch": true}'::jsonb,
    CURRENT_TIMESTAMP,
    'system'
)
ON CONFLICT (id) DO UPDATE SET
    name = EXCLUDED.name,
    description = EXCLUDED.description,
    params = EXCLUDED.params,
    updated_at = CURRENT_TIMESTAMP;

-- 4. 验证插入结果
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
WHERE type = 'youtube_subscription'
ORDER BY id;

-- 5. 输出配置说明
DO $$
BEGIN
    RAISE NOTICE '';
    RAISE NOTICE '✅ YouTube Subscription Scheduler 配置完成！';
    RAISE NOTICE '';
    RAISE NOTICE '📋 任务配置:';
    RAISE NOTICE '  - 任务ID: youtube_subscription';
    RAISE NOTICE '  - 执行频率: 每小时一次（整点）';
    RAISE NOTICE '  - 时区: America/New_York';
    RAISE NOTICE '  - 状态: 已启用';
    RAISE NOTICE '';
    RAISE NOTICE '🔧 下一步:';
    RAISE NOTICE '  1. 在 scheduler_service.py 中添加 _execute_youtube_subscription_task 方法';
    RAISE NOTICE '  2. 在 task_handlers 字典中注册该方法';
    RAISE NOTICE '  3. 修改 API 允许 youtube_subscription 类型';
    RAISE NOTICE '';
END $$;
