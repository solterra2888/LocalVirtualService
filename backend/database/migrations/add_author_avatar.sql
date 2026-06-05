-- =====================================================
-- 为 subscription_content 添加 author_avatar 字段
-- 目的: 存储 Twitter/YouTube 作者的头像 URL
-- =====================================================

-- 添加 author_avatar 字段
ALTER TABLE subscription_content
ADD COLUMN IF NOT EXISTS author_avatar TEXT;

COMMENT ON COLUMN subscription_content.author_avatar IS '作者头像 URL (Twitter profilePicture, YouTube channel thumbnail)';

-- 创建索引（可选，用于按作者头像查询）
-- CREATE INDEX IF NOT EXISTS idx_subscription_content_author_avatar
-- ON subscription_content(author_avatar)
-- WHERE author_avatar IS NOT NULL;

-- 验证
SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_name = 'subscription_content'
AND column_name = 'author_avatar';