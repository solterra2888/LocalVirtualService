-- 为 task_execution_logs 表添加索引以优化 task_type 过滤查询
-- 该索引支持按 task_type 过滤并按 started_at 降序排序的查询场景

-- 创建组合索引：task_type + started_at
-- 用于优化 WHERE task_type IN (...) ORDER BY started_at DESC 查询
CREATE INDEX IF NOT EXISTS idx_task_execution_logs_task_type_started_at 
ON task_execution_logs(task_type, started_at DESC);

-- 验证索引是否创建成功
SELECT 
    schemaname,
    tablename,
    indexname,
    indexdef
FROM pg_indexes 
WHERE tablename = 'task_execution_logs' 
  AND indexname = 'idx_task_execution_logs_task_type_started_at';

-- 分析表统计信息（可选，帮助查询优化器）
ANALYZE task_execution_logs;

