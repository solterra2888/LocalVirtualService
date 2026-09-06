-- Feeds Dashboard: 添加 feeds_topic_summary 调度任务类型 + 默认调度行
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
    'feeds_topic_summary',
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

-- 默认调度行: 每 15 分钟聚合主题快照 (feed_topic_daily_items)。
-- days=7: 覆盖 Dashboard 的「近3天/近一周」范围查询 + 兜住跨午夜入库的尾段内容。
-- cron */15: 聚合零 LLM 纯规则 (7 天全量重建秒级), 高频低成本, 压低「落库→可查」延迟。
-- 幂等 UPSERT — 已部署行也会被更新 (days 提到 7, cron 提到 */15)
-- (本文件由 db_manager._create_feed_topic_tables 在每次启动时重执行)。
-- P0 只做零 LLM 聚合; P1 将在同一 handler 中追加 Phase A 主题日摘要 (feed_topic_daily_summaries)。
INSERT INTO task_scheduling (id, name, type, schedule_type, cron_expression, timezone, enabled, params, created_at)
SELECT 'feeds_topic_summary_auto', 'Feeds Dashboard Topic Aggregation', 'feeds_topic_summary',
       'cron', '*/15 * * * *', 'Asia/Shanghai', true, '{"days": 7}'::jsonb, CURRENT_TIMESTAMP
WHERE NOT EXISTS (SELECT 1 FROM task_scheduling WHERE type = 'feeds_topic_summary');

UPDATE task_scheduling
SET params = CASE
        WHEN (params->>'days') IS NULL OR (params->>'days')::int < 7
            THEN params || '{"days": 7}'::jsonb
        ELSE params
    END,
    cron_expression = '*/15 * * * *',
    updated_at = CURRENT_TIMESTAMP
WHERE type = 'feeds_topic_summary'
  AND id = 'feeds_topic_summary_auto';
