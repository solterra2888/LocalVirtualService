-- ============================================================================
-- Twitter 订阅时区漂移再次修复 (Phase 4, 2026-05)
-- ============================================================================
--
-- 背景:
--   2026-05-02 排查 Walter Bloomberg (@DeItaone) Feeds 拉取不及时, 发现:
--     1. twitterapi.io advanced_search 索引延迟 12-20h (致命主因)
--        → 已通过 twitter_subscription_service._calculate_fetch_since_hours
--           加 24h 最小 lookback 修复.
--     2. Twitter Celery worker 在某些批次中 last_fetched_at / fetched_at /
--        published_at 仍被写成 HK 本地时钟 (+8h 偏差)
--        → 说明部分 worker 实例的 psycopg2 UTC 适配器/连接 SET TIMEZONE='UTC'
--           未生效 (部署滞后或环境差异).
--        → 已通过用 Python utcnow() 显式传参 (替代 SQL CURRENT_TIMESTAMP) 根治.
--
-- 本迁移目的:
--   清理 (1)+(2) 期间已经误存为 HK 时钟的存量行, 把它们 -8h 修正回 UTC.
--
-- 安全策略 (与 fix_all_naive_timestamps_to_utc.sql 一致):
--   只命中 "比当前 UTC 还在未来" 的行 — 这种行只可能是 HK 本地误存,
--   不会误改任何合法 UTC 行.
--
-- 执行顺序前提:
--   1. backend/services/twitter_subscription_service.py 已部署 (utcnow 显式传参).
--   2. backend/services/subscription_content_service.py 已部署 (utcnow fetched_at).
--   3. 所有 Celery worker 已重启 (生产 backend + data_fetching 队列 worker).
--      否则旧 worker 还会继续写新的 HK 时钟行进来.
-- ============================================================================

\set ON_ERROR_STOP on

BEGIN;

-- ──────────────────────────────────────────────────────────────────────────
-- subscription_content: 仅 channel_type='twitter' 的 fetched_at + published_at
-- ──────────────────────────────────────────────────────────────────────────
DO $$
DECLARE
    n_fetched   int;
    n_published int;
BEGIN
    SELECT count(*) INTO n_fetched
      FROM subscription_content
     WHERE channel_type = 'twitter'
       AND fetched_at > NOW() AT TIME ZONE 'UTC';

    SELECT count(*) INTO n_published
      FROM subscription_content
     WHERE channel_type = 'twitter'
       AND published_at > NOW() AT TIME ZONE 'UTC';

    RAISE NOTICE 'subscription_content (twitter): 待修正 fetched_at=% 行, published_at=% 行',
                 n_fetched, n_published;
END $$;

UPDATE subscription_content
   SET fetched_at = fetched_at - INTERVAL '8 hours'
 WHERE channel_type = 'twitter'
   AND fetched_at > NOW() AT TIME ZONE 'UTC';

UPDATE subscription_content
   SET published_at = published_at - INTERVAL '8 hours'
 WHERE channel_type = 'twitter'
   AND published_at > NOW() AT TIME ZONE 'UTC';

-- 进一步补漏：published_at > fetched_at + 1h 在物理上不可能 (推文必须先发布
-- 才能被 API 索引/抓取). 这种行也是 HK 时钟误存的残留 (-8h 后 published_at
-- 仍小于 NOW 时, 上面的 "未来时间" 规则抓不到, 但仍然是错的).
DO $$
DECLARE
    n int;
BEGIN
    SELECT count(*) INTO n
      FROM subscription_content
     WHERE channel_type='twitter'
       AND published_at > fetched_at + INTERVAL '1 hour';
    RAISE NOTICE 'subscription_content (twitter): published_at > fetched_at+1h 异常行 % 条 (HK 时钟残留)', n;
END $$;

UPDATE subscription_content
   SET published_at = published_at - INTERVAL '8 hours'
 WHERE channel_type = 'twitter'
   AND published_at > fetched_at + INTERVAL '1 hour';

-- ──────────────────────────────────────────────────────────────────────────
-- subscription_list: 仅 channel_type='twitter' 的 last_fetched_at + next_fetch_at
-- (created_at / updated_at 不会被订阅抓取流程频繁写入, 留给通用 migration)
-- ──────────────────────────────────────────────────────────────────────────
DO $$
DECLARE
    n_last int;
    n_next int;
BEGIN
    SELECT count(*) INTO n_last
      FROM subscription_list
     WHERE channel_type = 'twitter'
       AND last_fetched_at > NOW() AT TIME ZONE 'UTC';

    SELECT count(*) INTO n_next
      FROM subscription_list
     WHERE channel_type = 'twitter'
       AND next_fetch_at > NOW() AT TIME ZONE 'UTC' + INTERVAL '7 hours';
       -- next_fetch_at 合法值最多比 NOW+UTC 大 fetch_interval (默认 30min~2h),
       -- 所以阈值用 +7h 才能区分是 HK 时钟误存 (+8h) 还是合法的预期下次抓取时间.

    RAISE NOTICE 'subscription_list (twitter): 待修正 last_fetched_at=% 行, next_fetch_at=% 行',
                 n_last, n_next;
END $$;

UPDATE subscription_list
   SET last_fetched_at = last_fetched_at - INTERVAL '8 hours'
 WHERE channel_type = 'twitter'
   AND last_fetched_at > NOW() AT TIME ZONE 'UTC';

UPDATE subscription_list
   SET next_fetch_at = next_fetch_at - INTERVAL '8 hours'
 WHERE channel_type = 'twitter'
   AND next_fetch_at > NOW() AT TIME ZONE 'UTC' + INTERVAL '7 hours';

-- ──────────────────────────────────────────────────────────────────────────
-- subscription_fetch_history: fetch_time
-- (高频审计表, 时区错乱会让 fetch_history 的"距今多久"显示离谱)
-- ──────────────────────────────────────────────────────────────────────────
DO $$
DECLARE
    n int;
BEGIN
    SELECT count(*) INTO n
      FROM subscription_fetch_history sfh
      JOIN subscription_list sl ON sfh.subscription_id = sl.id
     WHERE sl.channel_type = 'twitter'
       AND sfh.fetch_time > NOW() AT TIME ZONE 'UTC';
    RAISE NOTICE 'subscription_fetch_history (twitter subs): 待修正 fetch_time=% 行', n;
END $$;

UPDATE subscription_fetch_history sfh
   SET fetch_time = sfh.fetch_time - INTERVAL '8 hours'
  FROM subscription_list sl
 WHERE sfh.subscription_id = sl.id
   AND sl.channel_type = 'twitter'
   AND sfh.fetch_time > NOW() AT TIME ZONE 'UTC';

-- ──────────────────────────────────────────────────────────────────────────
-- 校验: 修复后, 这三张表都不应该再有"未来时间"行
-- ──────────────────────────────────────────────────────────────────────────
DO $$
DECLARE
    bad_sc int;
    bad_sl int;
BEGIN
    SELECT count(*) INTO bad_sc
      FROM subscription_content
     WHERE channel_type='twitter'
       AND (fetched_at > NOW() AT TIME ZONE 'UTC'
            OR published_at > NOW() AT TIME ZONE 'UTC');
    SELECT count(*) INTO bad_sl
      FROM subscription_list
     WHERE channel_type='twitter'
       AND last_fetched_at > NOW() AT TIME ZONE 'UTC';
    IF bad_sc > 0 OR bad_sl > 0 THEN
        RAISE EXCEPTION '修复后仍有"未来时间"行: subscription_content=% subscription_list=%', bad_sc, bad_sl;
    END IF;
    RAISE NOTICE '✅ 修复完成, 所有 Twitter 时间戳都不在未来';
END $$;

COMMIT;
