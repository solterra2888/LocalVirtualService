-- ============================================================
-- drop_market_weekly_report.sql
--
-- 移除 Weekly Market Report（Market Weekly）功能的全部数据库对象。
-- 对应代码剔除：backend/api/market_weekly*.py、
--   backend/services/market_weekly_service.py、
--   backend/tasks/market_weekly_report_tasks.py。
--
-- ⚠️  生产环境手动执行，执行前请确认：
--   1. 后端代码已部署（不再引用 market_weekly_reports 表）
--   2. 已做数据库备份（或确认历史周报数据不再需要保留；
--      OSS 上已生成的静态 HTML 文件本迁移不动）
--
-- 关联历史迁移：
--   - create_market_weekly_reports_table.sql（建表，本文件回收）
--   - add_market_weekly_report_scheduler_task_type.sql（任务类型，本文件回收）
-- ============================================================

BEGIN;

-- ----------------------------------------------------------
-- 1. 清理调度表中的 market_weekly_report 任务行
--    （task_execution_logs.task_id 外键 ON DELETE CASCADE，会连带清理）
-- ----------------------------------------------------------

DELETE FROM task_scheduling
WHERE type = 'market_weekly_report';

-- ----------------------------------------------------------
-- 2. 删除 feature 专属表（含 updated_at 触发器，随表一并删除）
-- ----------------------------------------------------------

DROP TABLE IF EXISTS market_weekly_reports CASCADE;

-- ----------------------------------------------------------
-- 3. 更新 valid_task_type CHECK 约束：移除 market_weekly_report
--    （db_manager 启动时的 _update_task_scheduling_constraints
--     也会按 all_task_types 重建，此处显式收紧以保证一致性）
-- ----------------------------------------------------------

ALTER TABLE task_scheduling
  DROP CONSTRAINT IF EXISTS valid_task_type;

ALTER TABLE task_scheduling
  ADD CONSTRAINT valid_task_type CHECK (type IN (
    'twitter_subscription',
    'youtube_subscription',
    'youtube_subscription_morning',
    'zhishixingqiu_subscription',
    'zsxq_file_dispatch',
    'substack_subscription',
    'weibo_subscription',
    'podcast_subscription',
    'economic_events',
    'macro_indicators',
    'eodhd_fundamentals',
    'watchlist_fundamentals',
    'earnings_active_fundamentals',
    'fundamentals_rotation',
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
    'daily_market_report',
    'morning_brief',
    'morning_brief_ft_refresh',
    'morning_brief_asia_close',
    'the_close_summary',
    'cnbc_closing_bell',
    'bloomberg_surveillance',
    'bloomberg_surveillance_flash',
    'zhishixingqiu_summary',
    'substack_summary',
    'fed_dashboard',
    'form13f'
  ));

COMMIT;

-- ----------------------------------------------------------
-- 验证
-- ----------------------------------------------------------
-- SELECT count(*) FROM task_scheduling WHERE type = 'market_weekly_report';  -- 应为 0
-- SELECT to_regclass('market_weekly_reports');                                -- 应为 NULL
