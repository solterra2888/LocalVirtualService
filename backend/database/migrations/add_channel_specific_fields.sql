-- =====================================================
-- 为 subscription_content 添加渠道特定字段
-- 目的: 保留重要的业务字段，避免过度依赖 JSONB
-- =====================================================

-- =====================================================
-- 1. Twitter 特定字段
-- =====================================================

-- Twitter: 转发信息（重要，需要直接查询和展示）
ALTER TABLE subscription_content 
ADD COLUMN IF NOT EXISTS is_retweet BOOLEAN DEFAULT false;

ALTER TABLE subscription_content 
ADD COLUMN IF NOT EXISTS retweeted_author VARCHAR(255);

ALTER TABLE subscription_content 
ADD COLUMN IF NOT EXISTS retweeted_content TEXT;

ALTER TABLE subscription_content 
ADD COLUMN IF NOT EXISTS retweeted_url TEXT;

-- Twitter: 引用信息（重要，需要直接查询和展示）
ALTER TABLE subscription_content 
ADD COLUMN IF NOT EXISTS is_quote BOOLEAN DEFAULT false;

ALTER TABLE subscription_content 
ADD COLUMN IF NOT EXISTS quoted_author VARCHAR(255);

ALTER TABLE subscription_content 
ADD COLUMN IF NOT EXISTS quoted_content TEXT;

ALTER TABLE subscription_content 
ADD COLUMN IF NOT EXISTS quoted_url TEXT;

-- =====================================================
-- 2. YouTube 特定字段（预留）
-- =====================================================

-- YouTube: 视频时长
ALTER TABLE subscription_content 
ADD COLUMN IF NOT EXISTS duration_seconds INTEGER;

-- YouTube: 频道ID（用于关联）
ALTER TABLE subscription_content 
ADD COLUMN IF NOT EXISTS channel_id VARCHAR(255);

-- =====================================================
-- 3. RSS 特定字段（预留）
-- =====================================================

-- RSS: Feed URL
ALTER TABLE subscription_content 
ADD COLUMN IF NOT EXISTS feed_url TEXT;

-- RSS: GUID
ALTER TABLE subscription_content 
ADD COLUMN IF NOT EXISTS guid VARCHAR(500);

-- RSS: 分类（数组）
ALTER TABLE subscription_content 
ADD COLUMN IF NOT EXISTS categories TEXT[];

-- =====================================================
-- 4. 通用扩展字段（已存在）
-- =====================================================

-- metadata: 存储其他不常用的扩展数据
-- 例如: like_count, reply_count, definition, tags 等

-- =====================================================
-- 5. 从 metadata 迁移 Twitter 数据到新字段
-- =====================================================

-- 从 metadata 中提取并填充 Twitter 特定字段
UPDATE subscription_content 
SET 
    is_retweet = COALESCE((metadata->>'is_retweet')::boolean, false),
    is_quote = COALESCE((metadata->>'is_quote')::boolean, false)
WHERE channel_type = 'twitter'
AND metadata IS NOT NULL;

COMMENT ON COLUMN subscription_content.is_retweet IS 'Twitter: 是否为转推';
COMMENT ON COLUMN subscription_content.retweeted_author IS 'Twitter: 被转推的作者';
COMMENT ON COLUMN subscription_content.retweeted_content IS 'Twitter: 被转推的内容';
COMMENT ON COLUMN subscription_content.retweeted_url IS 'Twitter: 被转推的链接';
COMMENT ON COLUMN subscription_content.is_quote IS 'Twitter: 是否为引用推';
COMMENT ON COLUMN subscription_content.quoted_author IS 'Twitter: 被引用的作者';
COMMENT ON COLUMN subscription_content.quoted_content IS 'Twitter: 被引用的内容';
COMMENT ON COLUMN subscription_content.quoted_url IS 'Twitter: 被引用的链接';
COMMENT ON COLUMN subscription_content.duration_seconds IS 'YouTube: 视频时长（秒）';
COMMENT ON COLUMN subscription_content.channel_id IS 'YouTube: 频道ID';
COMMENT ON COLUMN subscription_content.feed_url IS 'RSS: Feed源地址';
COMMENT ON COLUMN subscription_content.guid IS 'RSS: 全局唯一标识符';
COMMENT ON COLUMN subscription_content.categories IS 'RSS: 分类标签数组';

-- =====================================================
-- 6. 创建索引（按需）
-- =====================================================

-- Twitter: 查询转推
CREATE INDEX IF NOT EXISTS idx_subscription_content_twitter_retweet 
ON subscription_content(is_retweet) 
WHERE channel_type = 'twitter' AND is_retweet = true;

-- Twitter: 查询引用
CREATE INDEX IF NOT EXISTS idx_subscription_content_twitter_quote 
ON subscription_content(is_quote) 
WHERE channel_type = 'twitter' AND is_quote = true;

-- YouTube: 按频道查询
CREATE INDEX IF NOT EXISTS idx_subscription_content_youtube_channel 
ON subscription_content(channel_id) 
WHERE channel_type = 'youtube';

-- =====================================================
-- 7. 验证
-- =====================================================

SELECT 
    'Twitter特定字段' as field_group,
    COUNT(*) as total_records,
    COUNT(CASE WHEN is_retweet THEN 1 END) as retweets,
    COUNT(CASE WHEN is_quote THEN 1 END) as quotes
FROM subscription_content
WHERE channel_type = 'twitter';

-- 显示新增的列
SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_name = 'subscription_content'
AND column_name IN (
    'is_retweet', 'retweeted_author', 'retweeted_content',
    'is_quote', 'quoted_author', 'quoted_content',
    'duration_seconds', 'channel_id', 'feed_url', 'categories'
)
ORDER BY column_name;
