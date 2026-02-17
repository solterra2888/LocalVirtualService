-- ===================================================================
-- Add Unread Count Feature
-- 
-- Purpose: Add unread_count field to subscription_list for better performance
-- Created: 2024-02-17
-- ===================================================================

-- 1. 添加 unread_count 字段到 subscription_list 表
ALTER TABLE subscription_list 
ADD COLUMN IF NOT EXISTS unread_count INT DEFAULT 0;

COMMENT ON COLUMN subscription_list.unread_count IS 
'缓存的未读内容数量，用于快速显示未读计数';


-- 2. 初始化现有订阅的未读计数
-- 计算每个订阅的未读数量并更新
UPDATE subscription_list sl
SET unread_count = (
    SELECT COUNT(*)
    FROM subscription_content sc
    WHERE sc.subscription_id = sl.id
      AND sl.user_id = ANY(sc.user_ids)
      AND (
        sc.read_status->>sl.user_id::text IS NULL 
        OR (sc.read_status->>sl.user_id::text)::jsonb->>'is_read' = 'false'
      )
);

-- 显示更新结果
SELECT 
    id,
    channel_identifier,
    unread_count,
    total_items_fetched
FROM subscription_list
ORDER BY unread_count DESC
LIMIT 10;


-- 3. 创建函数：更新未读计数
CREATE OR REPLACE FUNCTION update_subscription_unread_count(
    p_subscription_id INT,
    p_user_id INT
) RETURNS INT AS $$
DECLARE
    v_unread_count INT;
BEGIN
    -- 计算未读数量
    SELECT COUNT(*)
    INTO v_unread_count
    FROM subscription_content sc
    WHERE sc.subscription_id = p_subscription_id
      AND p_user_id = ANY(sc.user_ids)
      AND (
        sc.read_status->>p_user_id::text IS NULL 
        OR (sc.read_status->>p_user_id::text)::jsonb->>'is_read' = 'false'
      );
    
    -- 更新订阅表
    UPDATE subscription_list
    SET unread_count = v_unread_count
    WHERE id = p_subscription_id
      AND user_id = p_user_id;
    
    RETURN v_unread_count;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION update_subscription_unread_count IS 
'更新指定订阅的未读计数';


-- 4. 创建触发器函数：内容标记为已读时自动更新未读计数
CREATE OR REPLACE FUNCTION trigger_update_unread_count_on_read() 
RETURNS TRIGGER AS $$
DECLARE
    v_user_id INT;
    v_is_read BOOLEAN;
    v_subscription_id INT;
BEGIN
    -- 从 read_status 的变化中提取用户ID
    -- 这个触发器在 read_status 更新时触发
    
    IF TG_OP = 'UPDATE' THEN
        v_subscription_id := NEW.subscription_id;
        
        -- 遍历所有用户ID，检查阅读状态变化
        FOR v_user_id IN SELECT unnest(NEW.user_ids) LOOP
            -- 检查该用户的阅读状态是否发生变化
            IF OLD.read_status->>v_user_id::text IS DISTINCT FROM NEW.read_status->>v_user_id::text THEN
                -- 异步更新未读计数（避免触发器嵌套）
                PERFORM update_subscription_unread_count(v_subscription_id, v_user_id);
            END IF;
        END LOOP;
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 创建触发器（暂时禁用，因为可能影响性能）
-- 建议在应用层手动调用 update_subscription_unread_count 函数
/*
DROP TRIGGER IF EXISTS trg_update_unread_count ON subscription_content;
CREATE TRIGGER trg_update_unread_count
AFTER UPDATE OF read_status ON subscription_content
FOR EACH ROW
EXECUTE FUNCTION trigger_update_unread_count_on_read();
*/


-- 5. 创建函数：批量更新所有订阅的未读计数
CREATE OR REPLACE FUNCTION refresh_all_unread_counts() 
RETURNS TABLE(subscription_id INT, old_count INT, new_count INT) AS $$
BEGIN
    RETURN QUERY
    UPDATE subscription_list sl
    SET unread_count = (
        SELECT COUNT(*)
        FROM subscription_content sc
        WHERE sc.subscription_id = sl.id
          AND sl.user_id = ANY(sc.user_ids)
          AND (
            sc.read_status->>sl.user_id::text IS NULL 
            OR (sc.read_status->>sl.user_id::text)::jsonb->>'is_read' = 'false'
          )
    )
    WHERE sl.unread_count IS DISTINCT FROM (
        SELECT COUNT(*)
        FROM subscription_content sc
        WHERE sc.subscription_id = sl.id
          AND sl.user_id = ANY(sc.user_ids)
          AND (
            sc.read_status->>sl.user_id::text IS NULL 
            OR (sc.read_status->>sl.user_id::text)::jsonb->>'is_read' = 'false'
          )
    )
    RETURNING 
        sl.id,
        sl.unread_count AS old_count,
        (
            SELECT COUNT(*)::INT
            FROM subscription_content sc
            WHERE sc.subscription_id = sl.id
              AND sl.user_id = ANY(sc.user_ids)
              AND (
                sc.read_status->>sl.user_id::text IS NULL 
                OR (sc.read_status->>sl.user_id::text)::jsonb->>'is_read' = 'false'
              )
        ) AS new_count;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION refresh_all_unread_counts IS 
'批量刷新所有订阅的未读计数，返回发生变化的订阅';


-- 6. 创建定期校准任务的建议
-- 建议在 crontab 或应用层定期运行以下命令来校准未读计数：
-- SELECT * FROM refresh_all_unread_counts();


-- ===================================================================
-- 使用示例
-- ===================================================================

/*
-- 1. 更新单个订阅的未读计数
SELECT update_subscription_unread_count(1, 1);

-- 2. 批量刷新所有未读计数
SELECT * FROM refresh_all_unread_counts();

-- 3. 查询未读计数
SELECT 
    id,
    channel_identifier,
    unread_count,
    total_items_fetched
FROM subscription_list
WHERE user_id = 1
ORDER BY unread_count DESC;

-- 4. 验证未读计数准确性
SELECT 
    sl.id,
    sl.channel_identifier,
    sl.unread_count AS cached_count,
    (
        SELECT COUNT(*)
        FROM subscription_content sc
        WHERE sc.subscription_id = sl.id
          AND sl.user_id = ANY(sc.user_ids)
          AND (
            sc.read_status->>sl.user_id::text IS NULL 
            OR (sc.read_status->>sl.user_id::text)::jsonb->>'is_read' = 'false'
          )
    ) AS actual_count,
    sl.unread_count - (
        SELECT COUNT(*)
        FROM subscription_content sc
        WHERE sc.subscription_id = sl.id
          AND sl.user_id = ANY(sc.user_ids)
          AND (
            sc.read_status->>sl.user_id::text IS NULL 
            OR (sc.read_status->>sl.user_id::text)::jsonb->>'is_read' = 'false'
          )
    ) AS difference
FROM subscription_list sl
WHERE user_id = 1;
*/

-- 完成
SELECT 'Unread count feature added successfully!' AS status;
