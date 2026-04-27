-- ===================================================================
-- Add Unread Count Feature
-- 
-- Purpose: Add unread_count field to subscription_list for better performance
-- Created: 2024-02-17
-- Updated: 2026-04-27 (PR-2: 加 subscription_id 列存在守护, 解除与
--          drop_subscription_id_column.sql 的执行顺序耦合; 详见
--          docs/instructions/LLM_REFACTOR_PLAN.md §2.2 Step 4)
-- 
-- 说明:
--   本 migration 第 2/4/5 节的 SQL 都依赖 subscription_content.subscription_id
--   字段。在 multi-subscription-fix 之后, 该字段已由 drop_subscription_id_column.sql
--   删除, 改为通过 author_identifier + user_ids[] 关联. 因此本文件中所有引用
--   subscription_id 的逻辑都用 DO $$ ... END $$ + 列存在检查包起来:
--     - 若字段还在 (旧环境 / migration 尚未跑 drop): 走原逻辑
--     - 若字段已删 (生产现状 / 新环境跑过 drop): 跳过并打印 NOTICE
--   这样无论 add_unread_count.sql 与 drop_subscription_id_column.sql 的
--   执行先后, migration 都不会失败.
-- ===================================================================

-- 1. 添加 unread_count 字段到 subscription_list 表
ALTER TABLE subscription_list 
ADD COLUMN IF NOT EXISTS unread_count INT DEFAULT 0;

COMMENT ON COLUMN subscription_list.unread_count IS 
'缓存的未读内容数量，用于快速显示未读计数';


-- 2. 初始化现有订阅的未读计数 (依赖 subscription_id, 加守护)
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'subscription_content' AND column_name = 'subscription_id'
    ) THEN
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
        RAISE NOTICE 'unread_count initialized via legacy subscription_id path';
    ELSE
        RAISE NOTICE 'subscription_content.subscription_id not present, skipping unread_count initialization. '
                     'Use refresh logic in subscription_content_service.py instead.';
    END IF;
END $$;

-- 显示更新结果 (不依赖 subscription_id, 保留)
SELECT 
    id,
    channel_identifier,
    unread_count,
    total_items_fetched
FROM subscription_list
ORDER BY unread_count DESC
LIMIT 10;


-- 3. 创建函数：更新未读计数 (依赖 subscription_id, 加守护)
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'subscription_content' AND column_name = 'subscription_id'
    ) THEN
        EXECUTE $func$
            CREATE OR REPLACE FUNCTION update_subscription_unread_count(
                p_subscription_id INT,
                p_user_id INT
            ) RETURNS INT AS $body$
            DECLARE
                v_unread_count INT;
            BEGIN
                SELECT COUNT(*)
                INTO v_unread_count
                FROM subscription_content sc
                WHERE sc.subscription_id = p_subscription_id
                  AND p_user_id = ANY(sc.user_ids)
                  AND (
                    sc.read_status->>p_user_id::text IS NULL 
                    OR (sc.read_status->>p_user_id::text)::jsonb->>'is_read' = 'false'
                  );
                
                UPDATE subscription_list
                SET unread_count = v_unread_count
                WHERE id = p_subscription_id
                  AND user_id = p_user_id;
                
                RETURN v_unread_count;
            END;
            $body$ LANGUAGE plpgsql;
        $func$;
        
        EXECUTE $cmt$
            COMMENT ON FUNCTION update_subscription_unread_count IS 
            '更新指定订阅的未读计数 (legacy: 依赖 subscription_content.subscription_id, 已废弃)'
        $cmt$;
    ELSE
        RAISE NOTICE 'subscription_content.subscription_id not present, skipping legacy '
                     'update_subscription_unread_count(int,int) function definition. '
                     'New signature is provided by application layer.';
    END IF;
END $$;


-- 4. 创建触发器函数：内容标记为已读时自动更新未读计数 (依赖 subscription_id, 加守护)
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'subscription_content' AND column_name = 'subscription_id'
    ) THEN
        EXECUTE $func$
            CREATE OR REPLACE FUNCTION trigger_update_unread_count_on_read() 
            RETURNS TRIGGER AS $body$
            DECLARE
                v_user_id INT;
                v_is_read BOOLEAN;
                v_subscription_id INT;
            BEGIN
                IF TG_OP = 'UPDATE' THEN
                    v_subscription_id := NEW.subscription_id;
                    
                    FOR v_user_id IN SELECT unnest(NEW.user_ids) LOOP
                        IF OLD.read_status->>v_user_id::text IS DISTINCT FROM NEW.read_status->>v_user_id::text THEN
                            PERFORM update_subscription_unread_count(v_subscription_id, v_user_id);
                        END IF;
                    END LOOP;
                END IF;
                
                RETURN NEW;
            END;
            $body$ LANGUAGE plpgsql;
        $func$;
    ELSE
        RAISE NOTICE 'subscription_content.subscription_id not present, skipping legacy '
                     'trigger_update_unread_count_on_read() function definition.';
    END IF;
END $$;

-- 创建触发器（暂时禁用，因为可能影响性能）
-- 建议在应用层手动调用 update_subscription_unread_count 函数
/*
DROP TRIGGER IF EXISTS trg_update_unread_count ON subscription_content;
CREATE TRIGGER trg_update_unread_count
AFTER UPDATE OF read_status ON subscription_content
FOR EACH ROW
EXECUTE FUNCTION trigger_update_unread_count_on_read();
*/


-- 5. 创建函数：批量更新所有订阅的未读计数 (依赖 subscription_id, 加守护)
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'subscription_content' AND column_name = 'subscription_id'
    ) THEN
        EXECUTE $func$
            CREATE OR REPLACE FUNCTION refresh_all_unread_counts() 
            RETURNS TABLE(subscription_id INT, old_count INT, new_count INT) AS $body$
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
            $body$ LANGUAGE plpgsql;
        $func$;
        
        EXECUTE $cmt$
            COMMENT ON FUNCTION refresh_all_unread_counts IS 
            '批量刷新所有订阅的未读计数, 返回发生变化的订阅 (legacy: 依赖 subscription_id)'
        $cmt$;
    ELSE
        RAISE NOTICE 'subscription_content.subscription_id not present, skipping legacy '
                     'refresh_all_unread_counts() function definition.';
    END IF;
END $$;


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
