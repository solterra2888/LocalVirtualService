-- =====================================================
-- 为 subscription_list 添加 channel_avatar 字段
-- 目的: 存储订阅频道的头像 URL (Twitter profilePicture, YouTube channel thumbnail)
-- =====================================================

-- 添加 channel_avatar 字段
ALTER TABLE subscription_list
ADD COLUMN IF NOT EXISTS channel_avatar TEXT;

COMMENT ON COLUMN subscription_list.channel_avatar IS '频道头像 URL (Twitter profilePicture, YouTube channel thumbnail)';

-- 验证
SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_name = 'subscription_list'
AND column_name = 'channel_avatar';