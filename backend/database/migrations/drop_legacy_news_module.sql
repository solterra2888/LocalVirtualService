-- ============================================================
-- drop_legacy_news_module.sql
--
-- 移除旧 News 模块的全部数据库对象。
--
-- ⚠️  生产环境手动执行，执行前请确认：
--   1. 后端代码已部署（不再引用这些表）
--   2. 已做数据库备份（或确认数据不再需要保留）
--
-- 执行顺序：先删除有外键约束的子表，再删除父表。
-- 保留: theme_master_data, investment_theme_embeddings,
--       theme_company_associations, theme_tags, futu_*
-- ============================================================

BEGIN;

-- ----------------------------------------------------------
-- 1. 子表（有外键引用 news 或其他 news_* 表）
-- ----------------------------------------------------------

DROP TABLE IF EXISTS news_auto_tags_relations CASCADE;
DROP TABLE IF EXISTS news_auto_tags       CASCADE;
DROP TABLE IF EXISTS news_tags            CASCADE;
DROP TABLE IF EXISTS news_tags_dict       CASCADE;
DROP TABLE IF EXISTS news_symbols         CASCADE;
DROP TABLE IF EXISTS news_symbols_dict    CASCADE;
DROP TABLE IF EXISTS news_topic_keywords  CASCADE;
DROP TABLE IF EXISTS news_topics          CASCADE;
DROP TABLE IF EXISTS news_theme_matches   CASCADE;
DROP TABLE IF EXISTS news_themes_unified  CASCADE;

-- ----------------------------------------------------------
-- 2. 主表
-- ----------------------------------------------------------

DROP TABLE IF EXISTS news                    CASCADE;
DROP TABLE IF EXISTS user_news_filter_presets CASCADE;

-- ----------------------------------------------------------
-- 3. 清理调度表中的旧任务行
-- ----------------------------------------------------------

DELETE FROM task_scheduling
WHERE type IN ('eodhd_news', 'watchlist_news', 'cctv_news', 'news_analysis');

-- ----------------------------------------------------------
-- 4. 更新 valid_task_type CHECK 约束
--    移除旧四类（eodhd_news / watchlist_news / cctv_news / news_analysis）
--    保留 news_feed 及其他现有类型
-- ----------------------------------------------------------

ALTER TABLE task_scheduling
  DROP CONSTRAINT IF EXISTS valid_task_type;

ALTER TABLE task_scheduling
  ADD CONSTRAINT valid_task_type CHECK (type IN (
    'twitter_subscription',
    'youtube_subscription',
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
    'youtube_author_summary'
  ));

COMMIT;
