-- =====================================================
-- ZSXQ file pipeline: skip audio + 5-min dispatch scheduler
-- =====================================================
-- 1) zsxq_files 增加 skipped 终态，并把积压音频标为 skipped
-- 2) 新增 zsxq_file_dispatch 定时任务（每 5 分钟补货入队，不绑死抓取周期）

ALTER TABLE zsxq_files DROP CONSTRAINT IF EXISTS valid_zsxq_file_status;
ALTER TABLE zsxq_files ADD CONSTRAINT valid_zsxq_file_status CHECK (
    status IN ('pending', 'downloading', 'downloaded', 'uploaded', 'blocked', 'failed', 'skipped')
);

UPDATE zsxq_files
SET status = 'skipped',
    error = CASE WHEN error = '' THEN 'audio_not_downloaded' ELSE error END,
    locked_at = NULL,
    updated_at = now()
WHERE status NOT IN ('uploaded', 'skipped')
  AND name ~* '\.(mp3|m4a|mp4|wav|aac|flac|ogg|wma|amr|opus|mov|mkv|avi|webm)$';

COMMENT ON COLUMN zsxq_files.status IS
    'pending→downloading→downloaded→uploaded / blocked / failed / skipped(音频不下载)';

-- Scheduler task type
ALTER TABLE task_scheduling DROP CONSTRAINT IF EXISTS valid_task_type;
ALTER TABLE task_scheduling ADD CONSTRAINT valid_task_type CHECK (type::text = ANY (ARRAY[
    'macro_indicators',
    'twitter_author_summary',
    'watchlist_fundamentals',
    'watchlist_news',
    'daily_market_report',
    'eodhd_eod_stock_prices',
    'eodhd_stock_prices',
    'eodhd_news',
    'market_weekly_report',
    'twitter_subscription',
    'cctv_news',
    'zhishixingqiu_summary',
    'earnings_calendar',
    'cnbc_closing_bell',
    'substack_subscription',
    'zhishixingqiu_subscription',
    'zsxq_file_dispatch',
    'weibo_subscription',
    'substack_summary',
    'economic_events',
    'finviz_screener',
    'market_data',
    'ipo_calendar',
    'twitter_daily_report',
    'youtube_author_summary',
    'news_feed',
    'the_close_summary',
    'eodhd_fundamentals',
    'podcast_subscription',
    'youtube_subscription',
    'theme_report'
]::text[]));

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
    'zsxq_file_dispatch',
    'Zhishixingqiu File Download Dispatch',
    'zsxq_file_dispatch',
    'cron',
    '*/5 * * * *',
    'Asia/Shanghai',
    true,
    '每 5 分钟扫描 zsxq_files pending 文档并入队下载（跳过音频）；不依赖话题抓取周期',
    '{}'::jsonb,
    CURRENT_TIMESTAMP,
    'system'
)
ON CONFLICT (id) DO UPDATE SET
    name = EXCLUDED.name,
    schedule_type = EXCLUDED.schedule_type,
    cron_expression = EXCLUDED.cron_expression,
    description = EXCLUDED.description,
    enabled = true,
    updated_at = CURRENT_TIMESTAMP;
