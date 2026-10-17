-- =====================================================
-- Bloomberg Surveillance 速览版（播客精华集）调度任务
-- =====================================================
-- 创建时间: 2026-10-17
-- 说明:
--   1. task_scheduling.valid_task_type 约束追加 'bloomberg_surveillance_flash'。
--   2. 插入速览版任务：Cron 30 22,23 * * 1-5 Asia/Shanghai。
--      美东周一至周五节目 → 北京周一至周五晚间；22:30 首跑（夏令时精华集
--      21:32–22:06 就绪），23:30 兜底（冬令时精华集约 22:32–23:06 就绪），
--      双触发自动适应夏/冬令时，无需人工切换。
--   3. 完整版任务 cron 调整为 30 7,8,12 * * 2-6（YouTube 全场 VOD 次日
--      上午入库后升级覆盖速览版；12:30 全年兜底）。
-- 幂等：可重复执行。

BEGIN;

-- 1. 追加任务类型（重建 CHECK 约束）
ALTER TABLE task_scheduling DROP CONSTRAINT IF EXISTS valid_task_type;
ALTER TABLE task_scheduling ADD CONSTRAINT valid_task_type CHECK (type::text = ANY (ARRAY[
    'twitter_subscription'::character varying::text,
    'youtube_subscription'::character varying::text,
    'zhishixingqiu_subscription'::character varying::text,
    'substack_subscription'::character varying::text,
    'podcast_subscription'::character varying::text,
    'weibo_subscription'::character varying::text,
    'zsxq_file_dispatch'::character varying::text,
    'economic_events'::character varying::text,
    'macro_indicators'::character varying::text,
    'eodhd_fundamentals'::character varying::text,
    'watchlist_fundamentals'::character varying::text,
    'finviz_screener'::character varying::text,
    'eodhd_stock_prices'::character varying::text,
    'eodhd_eod_stock_prices'::character varying::text,
    'market_data'::character varying::text,
    'earnings_calendar'::character varying::text,
    'ipo_calendar'::character varying::text,
    'news_feed'::character varying::text,
    'twitter_author_summary'::character varying::text,
    'twitter_daily_report'::character varying::text,
    'youtube_author_summary'::character varying::text,
    'theme_report'::character varying::text,
    'market_weekly_report'::character varying::text,
    'daily_market_report'::character varying::text,
    'the_close_summary'::character varying::text,
    'cnbc_closing_bell'::character varying::text,
    'zhishixingqiu_summary'::character varying::text,
    'substack_summary'::character varying::text,
    'fed_dashboard'::character varying::text,
    'form13f'::character varying::text,
    'bloomberg_surveillance'::character varying::text,
    'bloomberg_surveillance_flash'::character varying::text,
    'feeds_topic_summary'::character varying::text,
    'cctv_news'::character varying::text,
    'eodhd_news'::character varying::text,
    'watchlist_news'::character varying::text
]::text[]));

-- 2. 插入速览版任务（幂等：存在则更新 cron）
DO $$
DECLARE
    v_id TEXT;
    v_ts BIGINT;
BEGIN
    IF EXISTS (
        SELECT 1 FROM task_scheduling WHERE type = 'bloomberg_surveillance_flash'
    ) THEN
        UPDATE task_scheduling
        SET cron_expression = '30 22,23 * * 1-5',
            timezone = 'Asia/Shanghai',
            schedule_type = 'cron',
            enabled = TRUE,
            description = 'Bloomberg Surveillance Flash Brief Mon-Fri 22:30/23:30 Beijing from podcast highlight transcript',
            next_run = NULL,
            updated_at = NOW()
        WHERE type = 'bloomberg_surveillance_flash';

        RAISE NOTICE 'Updated bloomberg_surveillance_flash cron to 30 22,23 * * 1-5 Asia/Shanghai';
    ELSE
        v_ts := EXTRACT(EPOCH FROM NOW())::BIGINT;
        v_id := 'bloomberg_surveillance_flash_Surveillance Flash_' || v_ts::TEXT;

        INSERT INTO task_scheduling (
            id, name, type, schedule_type, cron_expression,
            interval_minutes, timezone, enabled, params,
            description, created_at, last_run, next_run,
            success_count, fail_count, created_by
        ) VALUES (
            v_id,
            'Bloomberg Surveillance Flash Report',
            'bloomberg_surveillance_flash',
            'cron',
            '30 22,23 * * 1-5',
            NULL,
            'Asia/Shanghai',
            TRUE,
            '{}',
            'Bloomberg Surveillance Flash Brief Mon-Fri 22:30/23:30 Beijing from podcast highlight transcript',
            NOW(), NULL, NULL, 0, 0,
            'migration'
        );

        RAISE NOTICE 'Inserted bloomberg_surveillance_flash task with id=%', v_id;
    END IF;
END;
$$;

-- 3. 完整版任务调整：次日早间/午间升级 + 兜底（替代 00:30 单次）
UPDATE task_scheduling
SET cron_expression = '30 7,8,12 * * 2-6',
    timezone = 'Asia/Shanghai',
    description = 'Bloomberg Surveillance AM Brief Tue-Sat 07:30/08:30/12:30 Beijing, YouTube full episode upgrade (07:30 EDT / 08:30 EST, 12:30 final)',
    next_run = NULL,
    updated_at = NOW()
WHERE type = 'bloomberg_surveillance';

COMMIT;
