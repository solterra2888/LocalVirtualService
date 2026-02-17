-- =====================================================
-- 为 subscription_content 添加 Reply（回复）相关字段
-- =====================================================

-- Reply 相关字段
ALTER TABLE subscription_content 
ADD COLUMN IF NOT EXISTS is_reply BOOLEAN DEFAULT false;

ALTER TABLE subscription_content 
ADD COLUMN IF NOT EXISTS in_reply_to_id VARCHAR(255);

ALTER TABLE subscription_content 
ADD COLUMN IF NOT EXISTS in_reply_to_username VARCHAR(255);

ALTER TABLE subscription_content 
ADD COLUMN IF NOT EXISTS in_reply_to_user_id VARCHAR(255);

-- 添加注释
COMMENT ON COLUMN subscription_content.is_reply IS 'Twitter: 是否为回复';
COMMENT ON COLUMN subscription_content.in_reply_to_id IS 'Twitter: 被回复的推文ID';
COMMENT ON COLUMN subscription_content.in_reply_to_username IS 'Twitter: 被回复的用户名';
COMMENT ON COLUMN subscription_content.in_reply_to_user_id IS 'Twitter: 被回复的用户ID';

-- 创建索引（用于查询某条推文的所有回复）
CREATE INDEX IF NOT EXISTS idx_subscription_content_reply 
ON subscription_content(is_reply) 
WHERE channel_type = 'twitter' AND is_reply = true;

CREATE INDEX IF NOT EXISTS idx_subscription_content_in_reply_to 
ON subscription_content(in_reply_to_id) 
WHERE channel_type = 'twitter' AND in_reply_to_id IS NOT NULL;

-- =====================================================
-- 从现有数据中识别 Reply（通过 content 以 @ 开头推断）
-- =====================================================
-- 注意：这只是粗略识别，真实的 is_reply 需要从 API 的 isReply 字段获取

UPDATE subscription_content 
SET is_reply = true
WHERE channel_type = 'twitter'
AND content LIKE '@%'
AND is_retweet = false
AND is_quote = false;

-- 尝试从 content 中提取被回复的用户名
UPDATE subscription_content 
SET in_reply_to_username = substring(content from '^@([^ ]+)')
WHERE is_reply = true
AND in_reply_to_username IS NULL;

-- =====================================================
-- 验证
-- =====================================================
SELECT 
    'Reply 字段统计' as info,
    COUNT(*) as total_twitter,
    COUNT(CASE WHEN is_reply THEN 1 END) as replies,
    COUNT(CASE WHEN is_reply AND in_reply_to_username IS NOT NULL THEN 1 END) as replies_with_context
FROM subscription_content
WHERE channel_type = 'twitter';

-- 查看示例
SELECT 
    id,
    is_reply,
    in_reply_to_username,
    SUBSTRING(content, 1, 60) as content_preview
FROM subscription_content 
WHERE channel_type = 'twitter'
AND is_reply = true
LIMIT 5;
