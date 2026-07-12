-- =====================================================
-- Fix Substack custom-domain crawl failures + notes URL drift
-- =====================================================
-- 背景:
--   1. geopoliticaldispatch.com / shrubstack.com 文章轨长期失败
--      (超时 / 裸域 404)，backfill_complete 一直为 false
--   2. Capital Flows notes 写在 capitalflowsresearch.com，
--      用户订阅的是 capitalflows.substack.com → Feeds 看不到
--
-- 本迁移:
--   A. 重置两个失败 publication 的文章锚点，便于 fallback 后重新 backfill
--   B. 把 capitalflowsresearch.com 的 notes 归并到订阅 URL
--   C. 同步 followed_authors.publication_url
--   D. 补齐归并后内容的 user_ids（订阅该 publication 的启用用户）

BEGIN;

-- A. 重置失败源文章锚点（identity 保持 subscription_list 原 URL，抓取靠代码 fallback）
UPDATE substack_publications
SET backfill_complete = false,
    recent_post_ids = '{}',
    last_posts_sync_mode = NULL,
    updated_at = now()
WHERE publication_url IN (
    'https://geopoliticaldispatch.com',
    'https://shrubstack.com'
);

-- B. Capital Flows notes → 订阅 URL
UPDATE subscription_content
SET author_identifier = 'https://capitalflows.substack.com',
    metadata = jsonb_set(
        COALESCE(metadata, '{}'::jsonb),
        '{publication_url}',
        to_jsonb('https://capitalflows.substack.com'::text),
        true
    )
WHERE channel_type = 'substack'
  AND author_identifier = 'https://capitalflowsresearch.com';

-- C. followed author 主刊对齐
UPDATE substack_followed_authors
SET publication_url = 'https://capitalflows.substack.com',
    updated_at = now()
WHERE author_handle = 'capitalflows'
  AND publication_url = 'https://capitalflowsresearch.com';

-- D. 补齐 Capital Flows 内容 user_ids
UPDATE subscription_content sc
SET user_ids = (
    SELECT ARRAY(
        SELECT DISTINCT uid
        FROM unnest(
            COALESCE(sc.user_ids, '{}'::int[])
            || COALESCE((
                SELECT array_agg(DISTINCT sl.user_id)
                FROM subscription_list sl
                WHERE sl.channel_type = 'substack'
                  AND sl.enabled = true
                  AND sl.channel_identifier = 'https://capitalflows.substack.com'
            ), '{}'::int[])
        ) AS uid
    )
)
WHERE sc.channel_type = 'substack'
  AND sc.author_identifier = 'https://capitalflows.substack.com';

-- 同样补齐 geo / shrub 已有 notes 的 user_ids（若此前 orphan）
UPDATE subscription_content sc
SET user_ids = (
    SELECT ARRAY(
        SELECT DISTINCT uid
        FROM unnest(
            COALESCE(sc.user_ids, '{}'::int[])
            || COALESCE((
                SELECT array_agg(DISTINCT sl.user_id)
                FROM subscription_list sl
                WHERE sl.channel_type = 'substack'
                  AND sl.enabled = true
                  AND sl.channel_identifier = sc.author_identifier
            ), '{}'::int[])
        ) AS uid
    )
)
WHERE sc.channel_type = 'substack'
  AND sc.author_identifier IN (
      'https://geopoliticaldispatch.com',
      'https://shrubstack.com'
  );

COMMIT;
