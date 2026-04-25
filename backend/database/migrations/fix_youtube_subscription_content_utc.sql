-- ============================================================================
-- 修复家用虚拟机 Worker 写入 subscription_content 的 +8h 偏移
-- ============================================================================
--
-- 背景:
--   家用虚拟机的独立 Worker (deployment/local_virtual_service/worker/)
--   有自己的 db.py, 之前 **没有** 在连接上设置 SET TIME ZONE 'UTC'.
--   PostgreSQL 服务器默认 TimeZone='Asia/Shanghai', 因此:
--     - fetched_at = CURRENT_TIMESTAMP  → 写入 HK 本地数值 (而非 UTC).
--     - published_at = aware UTC datetime  → postgres 用 HK session 转成 HK
--       本地数值再丢掉 tzinfo, 写入 +8h 的偏移值.
--   导致 Daily 模块卡片显示 "明天" 的错觉 (例: APR 26 02:01 AM).
--
-- 修复路径已在代码层完成:
--   - deployment/local_virtual_service/worker/db.py: options='-c TimeZone=UTC'
--   - deployment/local_virtual_service/worker/db.py: psycopg2 UTC adapter
--   - deployment/local_virtual_service/worker/services.py: utcnow() fallback
--
-- 本脚本: 修正历史数据.
--
-- 安全策略:
--   只修正 "明显在未来" 的行 (fetched_at / published_at > NOW UTC).
--   家用 Worker 写入 8h 内的行才会处于"未来"状态; 更早的 HK-local 行
--   (在 HK clock 8h+ 前写入) 由于无法与"正确的 UTC 8h+ 前写入"区分,
--   只能保持现状. 这与 fix_all_naive_timestamps_to_utc.sql 的策略一致,
--   因为 Daily/Feeds 通常只看最近几天数据, 较早的偏移已被前一次迁移修复.
--
-- 应用方式:
--   psql -h <DB_HOST> -p <DB_PORT> -U <DB_USER> -d <DB_NAME> \
--     -f backend/database/migrations/fix_youtube_subscription_content_utc.sql
--
-- 幂等性: 重复执行无副作用 (只要新代码已部署、不再产生未来时间).
-- ============================================================================

\set ON_ERROR_STOP on

BEGIN;

DO $$
DECLARE
    n_fetched int;
    n_published int;
BEGIN
    SELECT count(*) INTO n_fetched
      FROM subscription_content
     WHERE channel_type = 'youtube'
       AND fetched_at > NOW() AT TIME ZONE 'UTC';

    SELECT count(*) INTO n_published
      FROM subscription_content
     WHERE channel_type = 'youtube'
       AND published_at > NOW() AT TIME ZONE 'UTC';

    RAISE NOTICE '[youtube] 待修正 fetched_at=% 行, published_at=% 行', n_fetched, n_published;
END $$;

UPDATE subscription_content
   SET fetched_at = fetched_at - INTERVAL '8 hours'
 WHERE channel_type = 'youtube'
   AND fetched_at > NOW() AT TIME ZONE 'UTC';

UPDATE subscription_content
   SET published_at = published_at - INTERVAL '8 hours'
 WHERE channel_type = 'youtube'
   AND published_at > NOW() AT TIME ZONE 'UTC';

-- 验证: 修正后不应再有 youtube 行处于未来
DO $$
DECLARE
    n_left int;
BEGIN
    SELECT count(*) INTO n_left
      FROM subscription_content
     WHERE channel_type = 'youtube'
       AND (fetched_at > NOW() AT TIME ZONE 'UTC'
         OR published_at > NOW() AT TIME ZONE 'UTC');
    IF n_left > 0 THEN
        RAISE WARNING '[youtube] 修正后仍剩 % 行处于未来 (可能 HK 本地写入仍在发生 — 请确认家用 Worker 已重启)', n_left;
    ELSE
        RAISE NOTICE '[youtube] ✅ 修正完成, 无未来时间残留';
    END IF;
END $$;

COMMIT;
