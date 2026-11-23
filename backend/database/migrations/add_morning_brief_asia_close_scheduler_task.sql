-- 新增 morning_brief_asia_close 定时调度任务(Morning Brief 涨跌榜港股/A股收盘补数)
-- 北京时间周二至周六 19:30 / 22:30 触发 tools/facts-report/asia_close_refresh.sh:
-- 把今日涨跌榜的港股/A股行从"上一交易日"更新为报告日 D 的当日收盘,并重渲染重发布
-- (share_token 不变)。「当日」= 报告生成日 D(北京);港股/A股 08:00 生成时还没
-- 开盘,收盘后才补。口径见 tools/facts-report/README.md「今日涨跌榜『当日』口径」。
--
-- 两次尝试:脚本按行内容幂等,首次成功后第二次空转(EODHD 当日文件出得晚时由
-- 第二次补全;11-23 实测 19:32 已可得)。周六 D 日港股/A股休市,两次均空转退出。
--
-- 执行顺序:先部署含本类型处理器与白名单的代码(scheduler_service.task_handlers、
-- db_manager._update_task_scheduling_constraints)并重启后端与调度器,再执行本文件;
-- 调度器启动时加载任务(_load_saved_tasks,同类型只注册最新一行),之后再执行需再
-- 重启调度器才会注册。
--
-- 幂等：若 morning_brief_asia_close 类型任务已存在则跳过，不重复插入。
-- 约束: task_scheduling.type 受 CHECK valid_task_type 限制。本文件手工 psql 执行,
-- 用"表内现有类型 ∪ 新类型"就地重建约束后再插入(动态取自表内,不是静态清单,
-- 不会落后于存量行);后端/调度器下次启动按"表内已有类型 + 代码内置清单"合并
-- 重建,类型不丢。切勿把本文件加进启动期自动执行(见 add_form13f_scheduler_task_
-- type.sql 头注:启动事务内 DROP/ADD 约束有毒化风险)。

DO $$
DECLARE
    v_id TEXT;
    v_ts BIGINT;
    v_types TEXT;
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM task_scheduling WHERE type = 'morning_brief_asia_close'
    ) THEN
        SELECT string_agg(DISTINCT quote_literal(type), ', ')
          INTO v_types
          FROM task_scheduling
         WHERE type IS NOT NULL;
        v_types := COALESCE(v_types || ', ', '') || '''morning_brief_asia_close''';

        ALTER TABLE task_scheduling DROP CONSTRAINT IF EXISTS valid_task_type;
        EXECUTE format(
            'ALTER TABLE task_scheduling ADD CONSTRAINT valid_task_type '
            'CHECK (type IN (%s))', v_types);

        v_ts := EXTRACT(EPOCH FROM NOW())::BIGINT;
        v_id := 'morning_brief_asia_close_Morning Brief Asia Close_' || v_ts::TEXT;

        INSERT INTO task_scheduling (
            id, name, type, schedule_type, cron_expression, interval_minutes,
            timezone, enabled, params, created_at, last_run, next_run,
            success_count, fail_count, created_by
        ) VALUES (
            v_id,
            'Morning Brief 港股/A股收盘补数',
            'morning_brief_asia_close',
            'cron',
            '30 19,22 * * 2-6',   -- 周二至周六 19:30 / 22:30 北京时间(报告日 D 当天)
            NULL,
            'Asia/Shanghai',
            TRUE,
            '{}'::jsonb,
            NOW(),
            NULL,
            NULL,
            0,
            0,
            'migration'
        );

        RAISE NOTICE 'Inserted morning_brief_asia_close task with id=%', v_id;
    ELSE
        RAISE NOTICE 'morning_brief_asia_close task already exists, skipping.';
    END IF;
END
$$;
