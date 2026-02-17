-- =====================================================
-- 迁移脚本: twitter_tweets -> subscription_content
-- 目的: 将 Twitter 专用表重构为多渠道统一内容表
-- =====================================================

-- =====================================================
-- 1. 重命名表
-- =====================================================
ALTER TABLE twitter_tweets RENAME TO subscription_content;

-- =====================================================
-- 2. 添加新字段
-- =====================================================

-- 添加 channel_type 字段（渠道类型）
ALTER TABLE subscription_content 
ADD COLUMN IF NOT EXISTS channel_type VARCHAR(50) DEFAULT 'twitter' NOT NULL;

COMMENT ON COLUMN subscription_content.channel_type IS '内容渠道类型: twitter, youtube, rss, zhishixingqiu';

-- 添加 content_id 字段（统一的内容唯一标识）
ALTER TABLE subscription_content 
ADD COLUMN IF NOT EXISTS content_id VARCHAR(255);

-- 从现有 tweet_id 迁移数据到 content_id
UPDATE subscription_content 
SET content_id = tweet_id 
WHERE content_id IS NULL;

-- 设置 content_id 为非空
ALTER TABLE subscription_content 
ALTER COLUMN content_id SET NOT NULL;

COMMENT ON COLUMN subscription_content.content_id IS '外部唯一ID: tweet_id, video_id, guid等';

-- 添加 title 字段（标题，Twitter 为空）
ALTER TABLE subscription_content 
ADD COLUMN IF NOT EXISTS title TEXT;

COMMENT ON COLUMN subscription_content.title IS '内容标题（Twitter无，YouTube/RSS有）';

-- 重命名 text -> content（如果需要保持向后兼容，可以保留 text）
-- 这里我们添加 content 字段，暂时保留 text
ALTER TABLE subscription_content 
ADD COLUMN IF NOT EXISTS content TEXT;

-- 从 text 迁移数据到 content
UPDATE subscription_content 
SET content = text 
WHERE content IS NULL;

COMMENT ON COLUMN subscription_content.content IS '主要内容：Tweet文本、视频描述、RSS正文';

-- 添加 author_identifier 字段（作者唯一标识）
ALTER TABLE subscription_content 
ADD COLUMN IF NOT EXISTS author_identifier VARCHAR(255);

-- 从现有 author 字段迁移（假设 author 就是唯一标识）
UPDATE subscription_content 
SET author_identifier = author 
WHERE author_identifier IS NULL;

COMMENT ON COLUMN subscription_content.author_identifier IS '作者唯一标识: Twitter用户名、YouTube频道ID';

-- 添加 author_name 字段（作者显示名称）
ALTER TABLE subscription_content 
ADD COLUMN IF NOT EXISTS author_name VARCHAR(255);

COMMENT ON COLUMN subscription_content.author_name IS '作者显示名称';

-- 重命名 media_url_https -> media_urls（统一为数组格式）
-- 先检查是否已经是数组类型
DO $$
BEGIN
    -- 如果 media_url_https 是 TEXT 类型，需要转换
    IF EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'subscription_content' 
        AND column_name = 'media_url_https'
        AND data_type = 'text'
    ) THEN
        -- 添加新的 media_urls 数组字段
        ALTER TABLE subscription_content 
        ADD COLUMN IF NOT EXISTS media_urls TEXT[];
        
        -- 将逗号分隔的字符串转换为数组
        UPDATE subscription_content 
        SET media_urls = string_to_array(media_url_https, ',')
        WHERE media_url_https IS NOT NULL AND media_url_https != '';
        
        -- 可选: 删除旧字段（建议先保留，确认无误后再删除）
        -- ALTER TABLE subscription_content DROP COLUMN IF EXISTS media_url_https;
    END IF;
END $$;

COMMENT ON COLUMN subscription_content.media_urls IS '媒体URL数组: 图片、视频、音频';

-- 重命名 created_at -> published_at（更符合语义）
ALTER TABLE subscription_content 
RENAME COLUMN created_at TO published_at;

COMMENT ON COLUMN subscription_content.published_at IS '内容发布时间（原始发布时间）';

-- 添加 engagement_count 字段（互动数统计）
ALTER TABLE subscription_content 
ADD COLUMN IF NOT EXISTS engagement_count INTEGER DEFAULT 0;

COMMENT ON COLUMN subscription_content.engagement_count IS '互动数: likes + retweets + comments + shares';

-- 添加 view_count 字段（观看/阅读数）
ALTER TABLE subscription_content 
ADD COLUMN IF NOT EXISTS view_count INTEGER DEFAULT 0;

COMMENT ON COLUMN subscription_content.view_count IS '观看/阅读数';

-- 添加 metadata 字段（JSONB，存储特定渠道的额外数据）
ALTER TABLE subscription_content 
ADD COLUMN IF NOT EXISTS metadata JSONB DEFAULT '{}'::jsonb;

COMMENT ON COLUMN subscription_content.metadata IS '扩展数据JSON: 存储特定渠道的差异化字段';

-- 将 Twitter 特定数据迁移到 metadata
UPDATE subscription_content 
SET metadata = jsonb_build_object(
    'tweet_id', tweet_id,
    'retweet_count', COALESCE((SELECT 0), 0),  -- 如果有这些字段，替换为实际字段
    'like_count', COALESCE((SELECT 0), 0),
    'is_retweet', false
)
WHERE channel_type = 'twitter' AND metadata = '{}'::jsonb;

-- =====================================================
-- 3. 更新约束
-- =====================================================

-- 删除旧的 tweet_id 唯一约束（如果存在）
ALTER TABLE subscription_content 
DROP CONSTRAINT IF EXISTS twitter_tweets_tweet_id_key;

ALTER TABLE subscription_content 
DROP CONSTRAINT IF EXISTS unique_tweet_id;

-- 添加新的组合唯一约束
ALTER TABLE subscription_content 
ADD CONSTRAINT unique_content UNIQUE(channel_type, content_id);

COMMENT ON CONSTRAINT unique_content ON subscription_content IS '内容唯一性约束: 渠道类型 + 内容ID';

-- =====================================================
-- 4. 更新索引
-- =====================================================

-- 创建 channel_type 索引
CREATE INDEX IF NOT EXISTS idx_subscription_content_channel_type 
ON subscription_content(channel_type);

-- 创建组合索引（channel_type + published_at）用于分页查询
CREATE INDEX IF NOT EXISTS idx_subscription_content_channel_published 
ON subscription_content(channel_type, published_at DESC);

-- 更新 user_ids GIN 索引（如果不存在）
CREATE INDEX IF NOT EXISTS idx_subscription_content_user_ids 
ON subscription_content USING GIN(user_ids);

-- 创建 metadata GIN 索引（用于 JSONB 查询）
CREATE INDEX IF NOT EXISTS idx_subscription_content_metadata 
ON subscription_content USING GIN(metadata);

-- 创建部分索引（只索引最近数据，提高性能）
CREATE INDEX IF NOT EXISTS idx_subscription_content_recent 
ON subscription_content(published_at DESC, channel_type)
WHERE published_at > NOW() - INTERVAL '90 days';

-- =====================================================
-- 5. 更新序列（如果需要）
-- =====================================================

-- 重命名序列
ALTER SEQUENCE IF EXISTS twitter_tweets_id_seq 
RENAME TO subscription_content_id_seq;

-- =====================================================
-- 6. 更新外键引用（如果其他表引用了 twitter_tweets）
-- =====================================================

-- 注意: 如果有其他表通过外键引用 twitter_tweets，需要更新
-- 示例:
-- ALTER TABLE some_other_table 
-- RENAME CONSTRAINT fk_twitter_tweets TO fk_subscription_content;

-- =====================================================
-- 7. 更新表注释
-- =====================================================

COMMENT ON TABLE subscription_content IS '多渠道订阅内容统一存储表（Twitter、YouTube、RSS、知识星球等）';

-- =====================================================
-- 8. 验证迁移结果
-- =====================================================

-- 检查数据完整性
DO $$
DECLARE
    total_count INTEGER;
    content_id_null_count INTEGER;
    channel_type_null_count INTEGER;
BEGIN
    -- 统计总记录数
    SELECT COUNT(*) INTO total_count FROM subscription_content;
    
    -- 检查 content_id 是否有 NULL
    SELECT COUNT(*) INTO content_id_null_count 
    FROM subscription_content 
    WHERE content_id IS NULL;
    
    -- 检查 channel_type 是否有 NULL
    SELECT COUNT(*) INTO channel_type_null_count 
    FROM subscription_content 
    WHERE channel_type IS NULL;
    
    -- 输出验证结果
    RAISE NOTICE '=== 迁移验证结果 ===';
    RAISE NOTICE '总记录数: %', total_count;
    RAISE NOTICE 'content_id 为 NULL 的记录: %', content_id_null_count;
    RAISE NOTICE 'channel_type 为 NULL 的记录: %', channel_type_null_count;
    
    IF content_id_null_count > 0 OR channel_type_null_count > 0 THEN
        RAISE WARNING '发现数据完整性问题，请检查！';
    ELSE
        RAISE NOTICE '✓ 数据迁移成功，所有记录完整';
    END IF;
END $$;

-- 查看表结构
\d subscription_content

-- 查看示例数据
SELECT 
    id,
    channel_type,
    content_id,
    SUBSTRING(content, 1, 50) as content_preview,
    author_identifier,
    published_at,
    fetched_at
FROM subscription_content 
LIMIT 5;

-- =====================================================
-- 9. 创建视图（向后兼容，可选）
-- =====================================================

-- 如果有大量代码依赖 twitter_tweets 表名，可以创建视图
CREATE OR REPLACE VIEW twitter_tweets AS
SELECT 
    id,
    content_id as tweet_id,
    content as text,
    author_identifier as author,
    url,
    media_urls,
    published_at as created_at,
    fetched_at,
    subscription_id,
    user_ids,
    read_status,
    metadata
FROM subscription_content
WHERE channel_type = 'twitter';

COMMENT ON VIEW twitter_tweets IS '向后兼容视图: 映射到 subscription_content 表的 Twitter 内容';

-- =====================================================
-- 完成
-- =====================================================

RAISE NOTICE '=== 迁移完成 ===';
RAISE NOTICE '表名: twitter_tweets -> subscription_content';
RAISE NOTICE '请测试相关功能，确认无误后可以删除冗余字段（如 text, media_url_https, tweet_id）';
