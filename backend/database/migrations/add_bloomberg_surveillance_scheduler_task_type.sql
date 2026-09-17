-- Add bloomberg_surveillance to task_scheduling.valid_task_type constraint
-- Safe to re-run: drops and recreates constraint with merged type list.
-- 清单为线上 task_scheduling 现有 type 的完整超集
-- （2026-09-17 实测含 fed_dashboard / form13f / podcast_subscription / substack_*
--   / weibo_subscription / zhishixingqiu_summary / zsxq_file_dispatch /
--   feeds_topic_summary / daily_market_report 等后续新增类型）。

ALTER TABLE task_scheduling DROP CONSTRAINT IF EXISTS valid_task_type;

ALTER TABLE task_scheduling ADD CONSTRAINT valid_task_type CHECK (type IN (
    -- subscription fetch
    'twitter_subscription',
    'youtube_subscription',
    'podcast_subscription',
    'zhishixingqiu_subscription',
    'substack_subscription',
    'weibo_subscription',
    'zsxq_file_dispatch',
    -- data fetch
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
    'feeds_topic_summary',
    'fed_dashboard',
    'form13f',
    -- summarize / report
    'twitter_author_summary',
    'twitter_daily_report',
    'youtube_author_summary',
    'zhishixingqiu_summary',
    'substack_summary',
    'theme_report',
    'market_weekly_report',
    'daily_market_report',
    'the_close_summary',
    'cnbc_closing_bell',
    'bloomberg_surveillance',
    -- legacy types still referenced by historical rows
    'cctv_news',
    'eodhd_news',
    'watchlist_news'
));
