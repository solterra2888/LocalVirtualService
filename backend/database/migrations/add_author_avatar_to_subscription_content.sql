-- =====================================================
-- subscription_content: author_avatar
-- =====================================================
-- 创建时间: 2026-06-05
-- 说明: 存储 Twitter/YouTube 作者头像 URL (profilePicture / channel thumbnail).
--       执行前请用 run_author_avatar_migration.sh，或在低峰期手动执行。
--       lock_timeout 避免长时间阻塞业务读写。

SET lock_timeout = '15s';

ALTER TABLE subscription_content
    ADD COLUMN IF NOT EXISTS author_avatar TEXT;

COMMENT ON COLUMN subscription_content.author_avatar
    IS '作者头像 URL (Twitter profilePicture, YouTube channel thumbnail)';

-- 验证
SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_name = 'subscription_content'
  AND column_name = 'author_avatar';
