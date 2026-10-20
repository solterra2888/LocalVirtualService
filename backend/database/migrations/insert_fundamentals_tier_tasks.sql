-- Fundamentals 分层更新策略: 新增两个调度任务（详见 docs/instructions/FUNDAMENTALS_UPDATE_STRATEGY.md）
-- T2 财报活跃名单刷新: 每天 18:20（紧跟 18:00 earnings_calendar 抓取后）
-- T4 全量轮换: 每天 01:30, 3000 家/天（美股+港股活跃池约 1.5 万家, 约 5 天一轮）
-- id 沿用 create_cron_task 的 "{type}_{name}_{timestamp}" 复合格式（task_scheduling.id 为 varchar 无默认值）

-- 1) 扩展 valid_task_type 约束（先删后建, 保留原有全部类型并新增两个）
ALTER TABLE task_scheduling DROP CONSTRAINT valid_task_type;
ALTER TABLE task_scheduling ADD CONSTRAINT valid_task_type CHECK (type = ANY (ARRAY[
 'twitter_subscription','youtube_subscription','zhishixingqiu_subscription','substack_subscription',
 'podcast_subscription','weibo_subscription','zsxq_file_dispatch','economic_events','macro_indicators',
 'eodhd_fundamentals','watchlist_fundamentals','earnings_active_fundamentals','fundamentals_rotation',
 'finviz_screener','eodhd_stock_prices','eodhd_eod_stock_prices','market_data','earnings_calendar',
 'ipo_calendar','news_feed','twitter_author_summary','twitter_daily_report','youtube_author_summary',
 'theme_report','market_weekly_report','daily_market_report','the_close_summary','cnbc_closing_bell',
 'zhishixingqiu_summary','substack_summary','fed_dashboard','form13f','bloomberg_surveillance',
 'bloomberg_surveillance_flash','feeds_topic_summary','cctv_news','eodhd_news','watchlist_news']::varchar[]));

-- 2) 插入两行调度配置（按 type 幂等）
INSERT INTO task_scheduling (id, name, type, schedule_type, cron_expression, timezone, enabled, params, created_by, description, created_at, updated_at)
SELECT 'earnings_active_fundamentals_Earnings Active Fundamentals Refresh_' || floor(extract(epoch from now()))::text,
       'Earnings Active Fundamentals Refresh', 'earnings_active_fundamentals', 'cron', '20 18 * * *', 'Asia/Shanghai', TRUE,
       '{"window_back": 7, "window_forward": 21, "stale_days": 7, "max_companies": 800}'::jsonb, 1,
       'T2: earnings_calendar [-7d,+21d] 窗口内公司 fundamentals 超 7 天未刷新时批量单票更新（≤800 家/天, ≤8k EODHD 调用）', now(), now()
WHERE NOT EXISTS (SELECT 1 FROM task_scheduling WHERE type = 'earnings_active_fundamentals');

INSERT INTO task_scheduling (id, name, type, schedule_type, cron_expression, timezone, enabled, params, created_by, description, created_at, updated_at)
SELECT 'fundamentals_rotation_Fundamentals Rotation_' || floor(extract(epoch from now()))::text,
       'Fundamentals Rotation', 'fundamentals_rotation', 'cron', '30 1 * * *', 'Asia/Shanghai', TRUE,
       '{"daily_limit": 3000}'::jsonb, 1,
       'T4: 美股+港股活跃公司 fundamentals 全量轮换, updated_at 最旧优先 3000 家/天（约 3 万 EODHD 调用, 占日限 30%）', now(), now()
WHERE NOT EXISTS (SELECT 1 FROM task_scheduling WHERE type = 'fundamentals_rotation');
