-- ============================================================================
-- 全系统时区契约迁移 (Phase 2)
-- ============================================================================
--
-- 背景:
--   服务器部署在香港 (UTC+8). 历史上有大量 Python 代码用 `datetime.now()` 直接
--   写 `timestamp without time zone` 列, 写入的是 HK 本地时间而非 UTC. 全系统
--   时区契约要求所有 naive timestamp 列存 UTC, 必须把这些"将来时间"行修正回 UTC.
--
-- 安全策略:
--   只修正"明显是 HK 本地"的行: created_at > NOW() AT TIME ZONE 'UTC'
--   (即比当前 UTC 还在未来 — 这种行只可能是 HK 本地误存).
--   这样既能修复"昨天上传的文件显示明天日期"这类用户可见 BUG, 又不会误伤
--   通过 SQL DEFAULT (CURRENT_TIMESTAMP, 在 SET TIMEZONE='UTC' 之后已经是 UTC) 写入的合法行.
--
-- 与 fix_file_processing_details_utc.sql 的关系:
--   那个 migration 已经把 file_processing_details 整表 -8h (因为该表所有行都
--   由 Python datetime.now() 写入). 这里处理其他高优先级用户可见表.
--
-- 执行前提:
--   1. 已部署 backend/utils/datetime_utils.py 中的 install_psycopg2_utc_adapter()
--      → 之后所有 naive datetime 写入自动转 UTC, 不会再产生新的"未来"行.
--   2. 已应用 backend/database/db_manager.py 的 SET TIMEZONE='UTC'.
--   3. 重启所有应用实例 (uvicorn / celery worker / scheduler) 让上述生效.
--
-- 回滚方案:
--   每张表单独事务, 失败可独立回滚. 不可逆操作前都打 RAISE NOTICE.
-- ============================================================================

\set ON_ERROR_STOP on

BEGIN;

-- ──────────────────────────────────────────────────────────────────────────
-- subscription_content: fetched_at + published_at
-- 影响: Daily 模块 / Feeds 模块卡片时间. fetched_at 现在 ~75 行, published_at ~24 行
-- ──────────────────────────────────────────────────────────────────────────
DO $$
DECLARE
    n_fetched int;
    n_published int;
BEGIN
    SELECT count(*) INTO n_fetched FROM subscription_content WHERE fetched_at > NOW() AT TIME ZONE 'UTC';
    SELECT count(*) INTO n_published FROM subscription_content WHERE published_at > NOW() AT TIME ZONE 'UTC';
    RAISE NOTICE 'subscription_content: 待修正 fetched_at=% 行, published_at=% 行', n_fetched, n_published;
END $$;

UPDATE subscription_content
   SET fetched_at = fetched_at - INTERVAL '8 hours'
 WHERE fetched_at > NOW() AT TIME ZONE 'UTC';

UPDATE subscription_content
   SET published_at = published_at - INTERVAL '8 hours'
 WHERE published_at > NOW() AT TIME ZONE 'UTC';

-- ──────────────────────────────────────────────────────────────────────────
-- subscription_list: created_at + updated_at + last_fetched_at + next_fetch_at
-- 影响: Feeds 订阅源管理页. 通常只少数行受影响.
-- ──────────────────────────────────────────────────────────────────────────
DO $$
DECLARE
    n int;
BEGIN
    SELECT count(*) INTO n FROM subscription_list
     WHERE created_at > NOW() AT TIME ZONE 'UTC'
        OR updated_at > NOW() AT TIME ZONE 'UTC'
        OR last_fetched_at > NOW() AT TIME ZONE 'UTC'
        OR next_fetch_at > NOW() AT TIME ZONE 'UTC';
    RAISE NOTICE 'subscription_list: 待修正 % 行 (任一时间列在未来)', n;
END $$;

UPDATE subscription_list SET created_at = created_at - INTERVAL '8 hours'
 WHERE created_at > NOW() AT TIME ZONE 'UTC';
UPDATE subscription_list SET updated_at = updated_at - INTERVAL '8 hours'
 WHERE updated_at > NOW() AT TIME ZONE 'UTC';
UPDATE subscription_list SET last_fetched_at = last_fetched_at - INTERVAL '8 hours'
 WHERE last_fetched_at > NOW() AT TIME ZONE 'UTC';
UPDATE subscription_list SET next_fetch_at = next_fetch_at - INTERVAL '8 hours'
 WHERE next_fetch_at > NOW() AT TIME ZONE 'UTC';

-- ──────────────────────────────────────────────────────────────────────────
-- subscription_content_tags: created_at + tagged_at + updated_at
-- 影响: Feeds 卡片标签信息.
-- ──────────────────────────────────────────────────────────────────────────
UPDATE subscription_content_tags SET created_at = created_at - INTERVAL '8 hours'
 WHERE created_at > NOW() AT TIME ZONE 'UTC';
UPDATE subscription_content_tags SET tagged_at = tagged_at - INTERVAL '8 hours'
 WHERE tagged_at > NOW() AT TIME ZONE 'UTC';
UPDATE subscription_content_tags SET updated_at = updated_at - INTERVAL '8 hours'
 WHERE updated_at > NOW() AT TIME ZONE 'UTC';

-- ──────────────────────────────────────────────────────────────────────────
-- file_tags: created_at + tagged_at + updated_at
-- ──────────────────────────────────────────────────────────────────────────
UPDATE file_tags SET created_at = created_at - INTERVAL '8 hours'
 WHERE created_at > NOW() AT TIME ZONE 'UTC';
UPDATE file_tags SET tagged_at = tagged_at - INTERVAL '8 hours'
 WHERE tagged_at > NOW() AT TIME ZONE 'UTC';
UPDATE file_tags SET updated_at = updated_at - INTERVAL '8 hours'
 WHERE updated_at > NOW() AT TIME ZONE 'UTC';

-- ──────────────────────────────────────────────────────────────────────────
-- 用户偏好/预设/文件夹/分享 等
-- ──────────────────────────────────────────────────────────────────────────
UPDATE user_daily_preferences SET updated_at = updated_at - INTERVAL '8 hours'
 WHERE updated_at > NOW() AT TIME ZONE 'UTC';

UPDATE user_template_preferences SET created_at = created_at - INTERVAL '8 hours'
 WHERE created_at > NOW() AT TIME ZONE 'UTC';
UPDATE user_template_preferences SET updated_at = updated_at - INTERVAL '8 hours'
 WHERE updated_at > NOW() AT TIME ZONE 'UTC';

UPDATE user_news_filter_presets SET created_at = created_at - INTERVAL '8 hours'
 WHERE created_at > NOW() AT TIME ZONE 'UTC';
UPDATE user_news_filter_presets SET updated_at = updated_at - INTERVAL '8 hours'
 WHERE updated_at > NOW() AT TIME ZONE 'UTC';
UPDATE user_news_filter_presets SET last_used_at = last_used_at - INTERVAL '8 hours'
 WHERE last_used_at > NOW() AT TIME ZONE 'UTC';

UPDATE user_execution_folders SET created_at = created_at - INTERVAL '8 hours'
 WHERE created_at > NOW() AT TIME ZONE 'UTC';

UPDATE file_folders SET created_at = created_at - INTERVAL '8 hours'
 WHERE created_at > NOW() AT TIME ZONE 'UTC';

UPDATE file_shares SET created_at = created_at - INTERVAL '8 hours'
 WHERE created_at > NOW() AT TIME ZONE 'UTC';

UPDATE users SET created_at = created_at - INTERVAL '8 hours'
 WHERE created_at > NOW() AT TIME ZONE 'UTC';

-- ──────────────────────────────────────────────────────────────────────────
-- 报表类: twitter / youtube daily reports + author daily summaries + report user config
-- ──────────────────────────────────────────────────────────────────────────
UPDATE twitter_daily_reports SET created_at = created_at - INTERVAL '8 hours'
 WHERE created_at > NOW() AT TIME ZONE 'UTC';
UPDATE twitter_daily_reports SET updated_at = updated_at - INTERVAL '8 hours'
 WHERE updated_at > NOW() AT TIME ZONE 'UTC';

UPDATE youtube_daily_reports SET created_at = created_at - INTERVAL '8 hours'
 WHERE created_at > NOW() AT TIME ZONE 'UTC';
UPDATE youtube_daily_reports SET updated_at = updated_at - INTERVAL '8 hours'
 WHERE updated_at > NOW() AT TIME ZONE 'UTC';

UPDATE twitter_author_daily_summaries SET created_at = created_at - INTERVAL '8 hours'
 WHERE created_at > NOW() AT TIME ZONE 'UTC';
UPDATE twitter_author_daily_summaries SET updated_at = updated_at - INTERVAL '8 hours'
 WHERE updated_at > NOW() AT TIME ZONE 'UTC';

UPDATE youtube_author_daily_summaries SET created_at = created_at - INTERVAL '8 hours'
 WHERE created_at > NOW() AT TIME ZONE 'UTC';
UPDATE youtube_author_daily_summaries SET updated_at = updated_at - INTERVAL '8 hours'
 WHERE updated_at > NOW() AT TIME ZONE 'UTC';

UPDATE twitter_report_user_config SET created_at = created_at - INTERVAL '8 hours'
 WHERE created_at > NOW() AT TIME ZONE 'UTC';
UPDATE twitter_report_user_config SET updated_at = updated_at - INTERVAL '8 hours'
 WHERE updated_at > NOW() AT TIME ZONE 'UTC';

-- ──────────────────────────────────────────────────────────────────────────
-- 新闻 / 主题 / 公司 等内部数据 (用户偶尔可见, 一并修正)
-- ──────────────────────────────────────────────────────────────────────────
UPDATE news SET created_at = created_at - INTERVAL '8 hours' WHERE created_at > NOW() AT TIME ZONE 'UTC';
UPDATE news SET analyzed_at = analyzed_at - INTERVAL '8 hours' WHERE analyzed_at > NOW() AT TIME ZONE 'UTC';
UPDATE news SET updated_at = updated_at - INTERVAL '8 hours' WHERE updated_at > NOW() AT TIME ZONE 'UTC';
-- news.date 由外部 API 给出, 本身就是 UTC 字符串, 不动.

UPDATE news_themes_unified SET created_at = created_at - INTERVAL '8 hours' WHERE created_at > NOW() AT TIME ZONE 'UTC';
UPDATE news_themes_unified SET updated_at = updated_at - INTERVAL '8 hours' WHERE updated_at > NOW() AT TIME ZONE 'UTC';
UPDATE news_themes_unified SET verified_at = verified_at - INTERVAL '8 hours' WHERE verified_at > NOW() AT TIME ZONE 'UTC';

UPDATE theme_master_data SET last_used_at = last_used_at - INTERVAL '8 hours' WHERE last_used_at > NOW() AT TIME ZONE 'UTC';

-- ──────────────────────────────────────────────────────────────────────────
-- 完成: 校验
-- ──────────────────────────────────────────────────────────────────────────
DO $$
DECLARE
    n int;
BEGIN
    SELECT count(*) INTO n FROM subscription_content
     WHERE fetched_at > NOW() AT TIME ZONE 'UTC' OR published_at > NOW() AT TIME ZONE 'UTC';
    IF n > 0 THEN
        RAISE EXCEPTION 'subscription_content 仍有 % 行在未来, 迁移失败', n;
    END IF;
    RAISE NOTICE '✅ migration 完成, 所有"未来时间"行已校正回 UTC';
END $$;

COMMIT;
