-- Add substack_summary to task_scheduling.valid_task_type constraint
-- Safe to re-run: drops and recreates constraint with merged type list.

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
