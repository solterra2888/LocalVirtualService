-- ============================================================
-- drop_feed_topic_tables.sql
--
-- 移除 Feeds Dashboard 模块（已下线）的全部数据库对象。
--
-- ⚠️  生产环境手动执行，执行前请确认：
--   1. 后端代码已部署（不再引用这些表）
--   2. 已做数据库备份（或确认数据不再需要保留）
--
-- 执行顺序：先删除有外键约束的子表 (feed_topic_daily_items)，再删除父表 (feed_topics)。
-- 注意: valid_task_type CHECK 约束不做静态重建 —
--   db_manager._update_task_scheduling_constraints 会在下次启动时
--   按「表内已有类型 + 代码内置清单」自动重建（见 FUNDAMENTALS_UPDATE_STRATEGY.md 的教训）。
-- ============================================================

BEGIN;

-- ----------------------------------------------------------
-- 1. 模块表（feed_topic_daily_items 有 FK → feed_topics, CASCADE 级联删除）
-- ----------------------------------------------------------

DROP TABLE IF EXISTS feed_topic_daily_items CASCADE;
DROP TABLE IF EXISTS feed_topics          CASCADE;

-- ----------------------------------------------------------
-- 2. 清理调度表中的任务行（否则每 15 分钟报一次「处理器未找到」）
-- ----------------------------------------------------------

DELETE FROM task_scheduling
WHERE type = 'feeds_topic_summary';

COMMIT;
