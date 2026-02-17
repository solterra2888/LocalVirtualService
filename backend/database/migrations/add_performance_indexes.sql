-- ===================================================================
-- Performance Optimization: Add Indexes to subscription_content
-- 
-- Purpose: Optimize query performance for Feed Reader and Subscription Management
-- Created: 2024-02-17
-- ===================================================================

-- 1. 添加 GIN 索引用于 user_ids 数组字段查询
-- 这是最重要的索引，用于 "WHERE user_id = ANY(user_ids)" 查询
-- GIN 索引特别适合数组类型的查询
CREATE INDEX IF NOT EXISTS idx_subscription_content_user_ids 
ON subscription_content USING GIN (user_ids);

COMMENT ON INDEX idx_subscription_content_user_ids IS 
'GIN index for fast user_ids array queries (WHERE user_id = ANY(user_ids))';


-- 2. 添加索引用于按发布时间排序
-- Feed Reader 页面需要按 published_at DESC 排序
CREATE INDEX IF NOT EXISTS idx_subscription_content_published_at 
ON subscription_content (published_at DESC);

COMMENT ON INDEX idx_subscription_content_published_at IS 
'Index for ORDER BY published_at DESC queries in Feed Reader';


-- 3. 添加索引用于 subscription_id 查询
-- Feed Reader 需要频繁查询特定订阅的内容
CREATE INDEX IF NOT EXISTS idx_subscription_content_subscription_id 
ON subscription_content (subscription_id);

COMMENT ON INDEX idx_subscription_content_subscription_id IS 
'Index for filtering by subscription_id in Feed Reader';


-- 4. 添加复合索引用于常见查询组合
-- 组合 subscription_id + published_at，优化 Feed Reader 的主要查询
CREATE INDEX IF NOT EXISTS idx_subscription_content_sub_published 
ON subscription_content (subscription_id, published_at DESC);

COMMENT ON INDEX idx_subscription_content_sub_published IS 
'Composite index for subscription_id + published_at queries';


-- 5. 添加索引用于 channel_type 查询
-- 用于按频道类型筛选（twitter, youtube, rss）
CREATE INDEX IF NOT EXISTS idx_subscription_content_channel_type 
ON subscription_content (channel_type);

COMMENT ON INDEX idx_subscription_content_channel_type IS 
'Index for filtering by channel_type';


-- 6. 添加索引用于热度等级查询
-- 用于按热度筛选和排序
CREATE INDEX IF NOT EXISTS idx_subscription_content_heat_level 
ON subscription_content (heat_level);

COMMENT ON INDEX idx_subscription_content_heat_level IS 
'Index for filtering and sorting by heat_level';


-- 7. 添加部分索引用于未读内容查询
-- 只索引未被特定用户读取的内容（使用表达式索引）
-- 注意：这个索引需要根据具体用户 ID 创建，这里提供模板
-- 实际使用时，应该考虑使用独立的 read_status 表而不是 JSONB

-- 示例（暂时注释掉，因为需要动态创建）
-- CREATE INDEX IF NOT EXISTS idx_subscription_content_unread_user_1
-- ON subscription_content ((read_status->>'1' IS NULL OR (read_status->>'1')::jsonb->>'is_read' = 'false'))
-- WHERE '1' = ANY(user_ids);


-- ===================================================================
-- Performance Optimization: Add Indexes to subscription_list
-- ===================================================================

-- 8. 添加复合索引用于获取用户的启用订阅
-- Scheduler 和 Feed Reader 都需要这个查询
CREATE INDEX IF NOT EXISTS idx_subscription_list_user_enabled 
ON subscription_list (user_id, enabled) 
WHERE enabled = true;

COMMENT ON INDEX idx_subscription_list_user_enabled IS 
'Partial index for enabled subscriptions by user';


-- 9. 添加索引用于按优先级和最后获取时间排序
-- Scheduler 需要按优先级和 last_fetched_at 排序选择要获取的订阅
CREATE INDEX IF NOT EXISTS idx_subscription_list_priority_fetched 
ON subscription_list (priority DESC, last_fetched_at ASC NULLS FIRST) 
WHERE enabled = true;

COMMENT ON INDEX idx_subscription_list_priority_fetched IS 
'Index for scheduler task selection by priority and last_fetched_at';


-- 10. 添加索引用于 channel_type 查询
-- 用于按频道类型筛选订阅
CREATE INDEX IF NOT EXISTS idx_subscription_list_channel_type 
ON subscription_list (channel_type);

COMMENT ON INDEX idx_subscription_list_channel_type IS 
'Index for filtering subscriptions by channel_type';


-- ===================================================================
-- Performance Optimization: Add Indexes to subscription_fetch_history
-- ===================================================================

-- 11. 添加索引用于查询订阅的获取历史
-- Subscription Management 页面需要查看每个订阅的历史
CREATE INDEX IF NOT EXISTS idx_fetch_history_subscription_time 
ON subscription_fetch_history (subscription_id, fetch_time DESC);

COMMENT ON INDEX idx_fetch_history_subscription_time IS 
'Index for fetching history by subscription_id ordered by time';


-- 12. 添加索引用于按状态查询
-- 用于统计成功/失败的获取任务
CREATE INDEX IF NOT EXISTS idx_fetch_history_status 
ON subscription_fetch_history (status);

COMMENT ON INDEX idx_fetch_history_status IS 
'Index for filtering fetch history by status';


-- ===================================================================
-- 验证索引创建
-- ===================================================================

-- 查看所有创建的索引
SELECT 
    schemaname,
    tablename,
    indexname,
    indexdef
FROM pg_indexes 
WHERE tablename IN ('subscription_content', 'subscription_list', 'subscription_fetch_history')
ORDER BY tablename, indexname;

-- 分析表以更新统计信息（帮助查询优化器）
ANALYZE subscription_content;
ANALYZE subscription_list;
ANALYZE subscription_fetch_history;

-- ===================================================================
-- 性能提示
-- ===================================================================

/*
索引使用建议：

1. GIN 索引 (user_ids)：
   - 适用于: WHERE user_id = ANY(user_ids)
   - 查询示例: SELECT * FROM subscription_content WHERE 1 = ANY(user_ids)
   
2. B-tree 索引 (published_at DESC)：
   - 适用于: ORDER BY published_at DESC
   - 查询示例: SELECT * FROM subscription_content ORDER BY published_at DESC LIMIT 50
   
3. 复合索引：
   - 适用于: WHERE subscription_id = ? ORDER BY published_at DESC
   - 查询示例: SELECT * FROM subscription_content WHERE subscription_id = 1 ORDER BY published_at DESC
   
4. 部分索引：
   - 适用于: WHERE enabled = true
   - 自动应用于符合条件的查询

定期维护：
- 定期运行 VACUUM ANALYZE 清理和更新统计信息
- 监控索引使用情况: SELECT * FROM pg_stat_user_indexes WHERE schemaname = 'public'
- 检查未使用的索引并考虑删除

查询优化技巧：
- 使用 EXPLAIN ANALYZE 分析查询计划
- 确保查询条件与索引顺序匹配
- 避免在 WHERE 子句中对索引字段使用函数
*/
