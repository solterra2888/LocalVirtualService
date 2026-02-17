-- 添加 twitter_subscription 任务类型到 task_scheduling 表约束
-- 用于支持 Twitter 订阅调度器功能

-- 1. 删除旧的 valid_task_type 约束
ALTER TABLE task_scheduling DROP CONSTRAINT IF EXISTS valid_task_type;

-- 2. 添加包含 twitter_subscription 的新约束
ALTER TABLE task_scheduling 
ADD CONSTRAINT valid_task_type CHECK (
    type IN (
        'eodhd_news', 
        'watchlist_news', 
        'cctv_news', 
        'twitter_subscription',  -- 新增
        'economic_events', 
        'macro_indicators', 
        'eodhd_fundamentals', 
        'watchlist_fundamentals', 
        'finviz_screener', 
        'news_analysis', 
        'futu_themes', 
        'eodhd_stock_prices', 
        'eodhd_eod_stock_prices', 
        'market_data', 
        'earnings_calendar', 
        'ipo_calendar'
    )
);

-- 验证约束已更新
SELECT constraint_name, check_clause
FROM information_schema.check_constraints
WHERE constraint_name = 'valid_task_type';
