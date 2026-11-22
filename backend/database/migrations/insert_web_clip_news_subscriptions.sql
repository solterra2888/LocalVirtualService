-- =====================================================
-- Web Clipper 人工剪藏新闻源订阅行 (Bloomberg / Reuters)
-- =====================================================
-- 2026-11-22: Chrome 扩展剪藏的 B/R 全文经 web_clip_feed_service 转投
-- subscription_content(channel_type='news', author_name='Bloomberg'/'Reuters')。
-- 晨报 build_snapshot 的读取 JOIN 要求 Admin(user_id=1) 存在 enabled 订阅行,
-- 否则内容入库也不可见 (JOIN: c.author_identifier = l.channel_identifier)。
--
-- metadata.manual_source=true: eodhd_news_feed_service._get_news_subscriptions
-- 据此跳过这些行 —— EODHD /api/news 不提供 B/R 源, 每 30min 空拉无意义。
-- API 创建路径 (subscriptions.py) 的 AVAILABLE_NEWS_SOURCES 校验不含 B/R,
-- 故用 SQL 直插 (幂等, 可重复执行)。

INSERT INTO subscription_list (
    user_id, channel_type, channel_identifier, channel_name,
    channel_description, enabled, priority, max_items_per_fetch,
    filter_config, metadata
)
SELECT
    1, 'news', s.identifier, s.identifier,
    'Web Clipper 人工剪藏全文 (chrome_clip 转投, 非 EODHD 拉取)',
    true, 5, 100,
    '{}'::jsonb,
    jsonb_build_object(
        'manual_source', true,
        'news_source', 'chrome_clip',
        'created_by_migration', 'insert_web_clip_news_subscriptions.sql'
    )
FROM (VALUES
    ('Bloomberg'),
    ('Reuters')
) AS s(identifier)
WHERE NOT EXISTS (
    SELECT 1 FROM subscription_list sl
    WHERE sl.user_id = 1
      AND sl.channel_type = 'news'
      AND sl.channel_identifier = s.identifier
);
