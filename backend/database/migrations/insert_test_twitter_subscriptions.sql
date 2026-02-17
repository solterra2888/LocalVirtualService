-- 插入测试 Twitter 订阅数据
-- 用于测试 Twitter Subscription 系统功能

-- 假设测试用户 ID 为 1 (admin)
-- 如果你的用户 ID 不同，请修改 user_id 值

-- 1. 订阅 Elon Musk (@elonmusk)
INSERT INTO subscription_list (
    user_id,
    channel_type,
    channel_identifier,
    channel_url,
    channel_name,
    channel_description,
    enabled,
    priority,
    fetch_interval_minutes,
    max_items_per_fetch,
    filter_config,
    metadata
) VALUES (
    1,  -- 修改为你的用户 ID
    'twitter',
    'elonmusk',
    'https://twitter.com/elonmusk',
    'Elon Musk',
    'Tesla CEO, SpaceX Founder',
    true,
    10,
    30,
    50,
    '{"keywords": [], "exclude_retweets": false}'::jsonb,
    '{"follower_count": "170M+", "verified": true}'::jsonb
) ON CONFLICT (user_id, channel_type, channel_identifier) DO UPDATE SET
    enabled = EXCLUDED.enabled,
    updated_at = CURRENT_TIMESTAMP;

-- 2. 订阅 OpenAI (@OpenAI)
INSERT INTO subscription_list (
    user_id,
    channel_type,
    channel_identifier,
    channel_url,
    channel_name,
    channel_description,
    enabled,
    priority,
    fetch_interval_minutes,
    max_items_per_fetch,
    filter_config,
    metadata
) VALUES (
    1,  -- 修改为你的用户 ID
    'twitter',
    'OpenAI',
    'https://twitter.com/OpenAI',
    'OpenAI',
    'Creating safe AGI that benefits all of humanity',
    true,
    9,
    30,
    50,
    '{"keywords": ["AI", "GPT", "ChatGPT"], "exclude_retweets": false}'::jsonb,
    '{"follower_count": "5M+", "verified": true}'::jsonb
) ON CONFLICT (user_id, channel_type, channel_identifier) DO UPDATE SET
    enabled = EXCLUDED.enabled,
    updated_at = CURRENT_TIMESTAMP;

-- 3. 订阅 Sam Altman (@sama)
INSERT INTO subscription_list (
    user_id,
    channel_type,
    channel_identifier,
    channel_url,
    channel_name,
    channel_description,
    enabled,
    priority,
    fetch_interval_minutes,
    max_items_per_fetch,
    filter_config,
    metadata
) VALUES (
    1,  -- 修改为你的用户 ID
    'twitter',
    'sama',
    'https://twitter.com/sama',
    'Sam Altman',
    'OpenAI CEO',
    true,
    8,
    30,
    50,
    '{"keywords": [], "exclude_retweets": false}'::jsonb,
    '{"follower_count": "2M+", "verified": true}'::jsonb
) ON CONFLICT (user_id, channel_type, channel_identifier) DO UPDATE SET
    enabled = EXCLUDED.enabled,
    updated_at = CURRENT_TIMESTAMP;

-- 查看插入的订阅
SELECT 
    id,
    user_id,
    channel_type,
    channel_identifier,
    channel_name,
    enabled,
    priority,
    created_at
FROM subscription_list
WHERE channel_type = 'twitter'
ORDER BY priority DESC, created_at DESC;

-- 统计信息
SELECT 
    COUNT(*) as total_subscriptions,
    COUNT(*) FILTER (WHERE enabled = true) as enabled_subscriptions,
    COUNT(DISTINCT user_id) as unique_users
FROM subscription_list
WHERE channel_type = 'twitter';
