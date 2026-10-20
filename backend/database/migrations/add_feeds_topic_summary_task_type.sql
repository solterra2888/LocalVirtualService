-- Feeds Dashboard: 添加 feeds_topic_summary 调度任务类型 + 默认调度行
-- 约束管理说明: valid_task_type 约束统一由 db_manager._update_task_scheduling_constraints
-- 在每次启动时按"表内已有类型 + 代码内置清单"合并重建（合并安全）。
-- 此处不再用静态清单 DROP/ADD 约束——静态清单落后于表内类型时会导致
-- ADD CONSTRAINT 违反已存在行, 进而毒化整个启动事务（2026-10 修复的存量 bug）。

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
