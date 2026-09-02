-- Add form13f to task_scheduling.valid_task_type constraint
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
    'form13f',
    -- legacy types still referenced by existing rows
    'cctv_news',
    'eodhd_news',
    'watchlist_news'
));

-- 默认调度行: 每工作日 UTC 21:00 轮询 SEC 13F 数据页（发现新季度数据集→下载→导入）
-- SEC 每季度发布一次数据集，工作日每日一次轮询足够；触发成本低（单次 GET）
-- id 为应用层生成的 varchar（无默认值），此处显式提供
INSERT INTO task_scheduling (id, name, type, schedule_type, cron_expression, timezone, enabled, params, created_at)
SELECT 'form13f_Form 13F Quarterly Poll', 'Form 13F Quarterly Poll', 'form13f',
       'cron', '0 21 * * 1-5', 'UTC', true, '{"mode": "poll"}'::jsonb, CURRENT_TIMESTAMP
WHERE NOT EXISTS (SELECT 1 FROM task_scheduling WHERE type = 'form13f');

-- 预设机构（与静态原型 docs/form13f_dashboard/build_data.py PRESETS 一致）
INSERT INTO form13f_presets (label, match_name, sort_order) VALUES
    ('Bridgewater Associates', 'Bridgewater Associates, LP', 1),
    ('Berkshire Hathaway Inc', 'Berkshire Hathaway Inc', 2),
    ('ARK Investment Management', 'ARK Investment Management LLC', 3),
    ('Oaktree Capital Management', 'OAKTREE CAPITAL MANAGEMENT LP', 4),
    ('Appaloosa LP', 'APPALOOSA LP', 5),
    ('Pershing Square Capital Management', 'Pershing Square Capital Management, L.P.', 6),
    ('Baupost Group', 'BAUPOST GROUP LLC/MA', 7),
    ('Temasek Holdings', 'Temasek Holdings (Private) Ltd', 8),
    ('Hillhouse Capital (HHLR)', 'HHLR ADVISORS, LTD.', 9),
    ('Tiger Global Management', 'TIGER GLOBAL MANAGEMENT LLC', 10),
    ('Duquesne Family Office', 'Duquesne Family Office LLC', 11),
    ('Fisher Asset Management', 'Fisher Asset Management, LLC', 12),
    ('Soros Fund Management', 'SOROS FUND MANAGEMENT LLC', 13),
    ('Scion Asset Management', 'Scion Asset Management, LLC', 14),
    ('Baillie Gifford', 'BAILLIE GIFFORD & CO', 15),
    ('Lone Pine Capital', 'LONE PINE CAPITAL LLC', 16),
    ('Third Point LLC', 'Third Point LLC', 17),
    ('Miller Value Partners', 'Miller Value Partners, LLC', 18),
    ('Coatue Management', 'COATUE MANAGEMENT LLC', 19),
    ('Icahn Carl C', 'ICAHN CARL C', 20)
ON CONFLICT (label) DO NOTHING;
