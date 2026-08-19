-- Add fed_dashboard to task_scheduling.valid_task_type constraint
-- Safe to re-run: drops and recreates constraint with merged type list.

ALTER TABLE task_scheduling DROP CONSTRAINT IF EXISTS valid_task_type;

ALTER TABLE task_scheduling ADD CONSTRAINT valid_task_type CHECK (type IN (
    'twitter_subscription',
    'youtube_subscription',
    'zhishixingqiu_subscription',
    'substack_subscription',
    'podcast_subscription',
    'weibo_subscription',
    'zsxq_file_dispatch',
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
    'fed_dashboard',
    -- legacy types still referenced by existing rows
    'cctv_news',
    'eodhd_news',
    'watchlist_news'
));

-- 默认调度行: 每 30 分钟轮询 Fed RSS（发现声明/纪要 + updates 流）
-- id 为应用层生成的 varchar（无默认值），此处显式提供
INSERT INTO task_scheduling (id, name, type, schedule_type, cron_expression, timezone, enabled, params, created_at)
SELECT 'fed_dashboard_Fed Dashboard RSS Poll', 'Fed Dashboard RSS Poll', 'fed_dashboard',
       'cron', '*/30 * * * *', 'UTC', true, '{"mode": "poll"}'::jsonb, CURRENT_TIMESTAMP
WHERE NOT EXISTS (SELECT 1 FROM task_scheduling WHERE type = 'fed_dashboard');
