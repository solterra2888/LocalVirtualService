-- =====================================================
-- Twitter Subscription System - Database Schema
-- =====================================================
-- 创建时间: 2026-02-15
-- 说明: 支持多种channel类型的订阅系统（Twitter, YouTube, 知识星球等）

-- =====================================================
-- 1. 订阅列表表
-- =====================================================
CREATE TABLE IF NOT EXISTS subscription_list (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    
    -- Channel 相关
    channel_type VARCHAR(50) NOT NULL,  -- 'twitter', 'youtube', 'zhishixingqiu', 'rss'
    channel_identifier VARCHAR(255) NOT NULL,  -- Twitter: @username, YouTube: channel_id
    channel_url TEXT,  -- 完整URL（可选）
    channel_name VARCHAR(255),  -- 显示名称
    channel_description TEXT,  -- 频道描述
    
    -- 订阅配置
    enabled BOOLEAN DEFAULT true,
    priority INTEGER DEFAULT 0,  -- 优先级（0-100，越高越优先）
    
    -- 订阅策略
    fetch_interval_minutes INTEGER DEFAULT 30,  -- 获取间隔（分钟）
    max_items_per_fetch INTEGER DEFAULT 20,  -- 每次最多获取数量
    
    -- 过滤配置（JSON格式，可选）
    filter_config JSONB DEFAULT '{}'::jsonb,  -- 例如: {"keywords": ["AI", "tech"], "exclude": ["spam"]}
    
    -- 元数据
    metadata JSONB DEFAULT '{}'::jsonb,  -- 存储channel特定的元数据
    
    -- 时间戳
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_fetched_at TIMESTAMP,  -- 上次获取时间
    next_fetch_at TIMESTAMP,  -- 下次计划获取时间
    
    -- 统计信息
    total_items_fetched INTEGER DEFAULT 0,  -- 累计获取数量
    last_fetch_items_count INTEGER DEFAULT 0,  -- 上次获取数量
    consecutive_failures INTEGER DEFAULT 0,  -- 连续失败次数
    
    -- 唯一约束：同一用户不能重复订阅同一个channel
    CONSTRAINT unique_user_channel UNIQUE(user_id, channel_type, channel_identifier)
);

-- 为 subscription_list 表创建索引
CREATE INDEX IF NOT EXISTS idx_subscription_user_id ON subscription_list(user_id);
CREATE INDEX IF NOT EXISTS idx_subscription_channel_type ON subscription_list(channel_type);
CREATE INDEX IF NOT EXISTS idx_subscription_enabled ON subscription_list(enabled);
CREATE INDEX IF NOT EXISTS idx_subscription_last_fetched ON subscription_list(last_fetched_at);
CREATE INDEX IF NOT EXISTS idx_subscription_next_fetch ON subscription_list(next_fetch_at);
CREATE INDEX IF NOT EXISTS idx_subscription_channel_identifier ON subscription_list(channel_type, channel_identifier);

-- 添加注释
COMMENT ON TABLE subscription_list IS '多渠道订阅列表（支持Twitter、YouTube、知识星球等）';
COMMENT ON COLUMN subscription_list.channel_type IS '频道类型: twitter, youtube, zhishixingqiu, rss';
COMMENT ON COLUMN subscription_list.channel_identifier IS '频道唯一标识: Twitter用户名、YouTube频道ID等';
COMMENT ON COLUMN subscription_list.priority IS '优先级: 0-100，数值越高优先级越高';
COMMENT ON COLUMN subscription_list.filter_config IS '过滤配置JSON: 关键词、排除词等';

-- =====================================================
-- 2. 订阅获取历史表
-- =====================================================
CREATE TABLE IF NOT EXISTS subscription_fetch_history (
    id SERIAL PRIMARY KEY,
    subscription_id INTEGER NOT NULL REFERENCES subscription_list(id) ON DELETE CASCADE,
    
    -- 执行信息
    fetch_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    execution_duration_ms INTEGER,  -- 执行耗时（毫秒）
    
    -- 获取结果
    items_fetched INTEGER DEFAULT 0,  -- 总获取数量
    items_new INTEGER DEFAULT 0,  -- 新增数量
    items_duplicated INTEGER DEFAULT 0,  -- 重复数量
    items_filtered INTEGER DEFAULT 0,  -- 被过滤数量
    
    -- 状态
    status VARCHAR(20) NOT NULL,  -- 'success', 'partial', 'failed', 'skipped'
    error_message TEXT,  -- 错误信息
    error_code VARCHAR(50),  -- 错误代码（如 API_LIMIT_EXCEEDED）
    
    -- 元数据
    fetch_metadata JSONB DEFAULT '{}'::jsonb  -- 额外的获取信息
);

-- 为 subscription_fetch_history 表创建索引
CREATE INDEX IF NOT EXISTS idx_fetch_history_subscription_id ON subscription_fetch_history(subscription_id);
CREATE INDEX IF NOT EXISTS idx_fetch_history_fetch_time ON subscription_fetch_history(fetch_time);
CREATE INDEX IF NOT EXISTS idx_fetch_history_status ON subscription_fetch_history(status);

COMMENT ON TABLE subscription_fetch_history IS '订阅内容获取历史记录';
COMMENT ON COLUMN subscription_fetch_history.status IS '状态: success(成功), partial(部分成功), failed(失败), skipped(跳过)';

-- =====================================================
-- 3. 修改 twitter_tweets 表
-- =====================================================

-- 添加订阅关联字段
DO $$ 
BEGIN
    -- 添加 subscription_id 字段
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'twitter_tweets' AND column_name = 'subscription_id'
    ) THEN
        ALTER TABLE twitter_tweets ADD COLUMN subscription_id INTEGER REFERENCES subscription_list(id) ON DELETE SET NULL;
        CREATE INDEX idx_twitter_tweets_subscription_id ON twitter_tweets(subscription_id);
        COMMENT ON COLUMN twitter_tweets.subscription_id IS '关联的订阅ID';
    END IF;
    
    -- 添加 user_ids 数组字段（多个用户可能订阅同一作者）
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'twitter_tweets' AND column_name = 'user_ids'
    ) THEN
        ALTER TABLE twitter_tweets ADD COLUMN user_ids INTEGER[] DEFAULT ARRAY[]::INTEGER[];
        CREATE INDEX idx_twitter_tweets_user_ids ON twitter_tweets USING GIN(user_ids);
        COMMENT ON COLUMN twitter_tweets.user_ids IS '订阅该推文的用户ID列表';
    END IF;
    
    -- 添加 fetched_at 字段（记录获取时间）
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'twitter_tweets' AND column_name = 'fetched_at'
    ) THEN
        ALTER TABLE twitter_tweets ADD COLUMN fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
        CREATE INDEX idx_twitter_tweets_fetched_at ON twitter_tweets(fetched_at);
        COMMENT ON COLUMN twitter_tweets.fetched_at IS '推文获取时间';
    END IF;
    
    -- 添加 is_read 字段（用户阅读状态，JSONB格式存储多用户状态）
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'twitter_tweets' AND column_name = 'read_status'
    ) THEN
        ALTER TABLE twitter_tweets ADD COLUMN read_status JSONB DEFAULT '{}'::jsonb;
        COMMENT ON COLUMN twitter_tweets.read_status IS '阅读状态JSON: {user_id: {is_read: true, read_at: timestamp}}';
    END IF;
END $$;

-- =====================================================
-- 4. 创建触发器：自动更新 updated_at
-- =====================================================
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 为 subscription_list 表创建触发器
DROP TRIGGER IF EXISTS update_subscription_list_updated_at ON subscription_list;
CREATE TRIGGER update_subscription_list_updated_at
    BEFORE UPDATE ON subscription_list
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- =====================================================
-- 5. 初始化示例数据（可选）
-- =====================================================
-- 注意：需要确保 user 表中存在对应的用户

-- 示例：为用户1添加Twitter订阅
-- INSERT INTO subscription_list (
--     user_id, channel_type, channel_identifier, channel_name, enabled
-- ) VALUES 
--     (1, 'twitter', 'zerohedge', 'Zero Hedge', true),
--     (1, 'twitter', 'wallstengine', 'Wall St Engine', true),
--     (1, 'twitter', 'NickTimiraos', 'Nick Timiraos', true)
-- ON CONFLICT (user_id, channel_type, channel_identifier) DO NOTHING;

-- =====================================================
-- 6. 查看表结构（验证用）
-- =====================================================
-- \d subscription_list
-- \d subscription_fetch_history
-- \d twitter_tweets
