-- 修复所有对 subscription_id 字段的引用
-- 改为使用 channel_type + author_identifier

-- 1. 删除旧的 update_subscription_unread_count 函数
DROP FUNCTION IF EXISTS update_subscription_unread_count(integer, integer);

-- 2. 创建新版本：通过 subscription_list 的 channel_identifier 来计算未读数
CREATE OR REPLACE FUNCTION update_subscription_unread_count(
    p_subscription_id INT,
    p_user_id INT
) RETURNS INT AS $$
DECLARE
    v_unread_count INT;
    v_channel_type VARCHAR;
    v_channel_identifier VARCHAR;
BEGIN
    -- 获取订阅的 channel 信息
    SELECT channel_type, channel_identifier
    INTO v_channel_type, v_channel_identifier
    FROM subscription_list
    WHERE id = p_subscription_id;
    
    -- 如果找不到订阅，返回 0
    IF v_channel_type IS NULL THEN
        RETURN 0;
    END IF;
    
    -- 计算该作者的未读数量
    SELECT COUNT(*)
    INTO v_unread_count
    FROM subscription_content sc
    WHERE sc.channel_type = v_channel_type
      AND sc.author_identifier = v_channel_identifier
      AND p_user_id = ANY(sc.user_ids)
      AND (
        sc.read_status->>p_user_id::text IS NULL 
        OR (sc.read_status->>p_user_id::text)::jsonb->>'is_read' = 'false'
      );
    
    -- 更新订阅表
    UPDATE subscription_list
    SET unread_count = v_unread_count
    WHERE id = p_subscription_id;
    
    RETURN v_unread_count;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION update_subscription_unread_count IS 
'更新指定订阅的未读计数（通过 channel_identifier 查询内容）';


-- 3. 修复 refresh_all_unread_counts 函数
DROP FUNCTION IF EXISTS refresh_all_unread_counts();

CREATE OR REPLACE FUNCTION refresh_all_unread_counts() 
RETURNS TABLE(subscription_id INT, old_count INT, new_count INT) AS $$
BEGIN
    RETURN QUERY
    UPDATE subscription_list sl
    SET unread_count = (
        SELECT COUNT(*)
        FROM subscription_content sc
        WHERE sc.channel_type = sl.channel_type
          AND sc.author_identifier = sl.channel_identifier
          AND sl.user_id = ANY(sc.user_ids)
          AND (
            sc.read_status->>sl.user_id::text IS NULL 
            OR (sc.read_status->>sl.user_id::text)::jsonb->>'is_read' = 'false'
          )
    )
    RETURNING sl.id, sl.unread_count AS old_unread_count, (
        SELECT COUNT(*)
        FROM subscription_content sc
        WHERE sc.channel_type = sl.channel_type
          AND sc.author_identifier = sl.channel_identifier
          AND sl.user_id = ANY(sc.user_ids)
          AND (
            sc.read_status->>sl.user_id::text IS NULL 
            OR (sc.read_status->>sl.user_id::text)::jsonb->>'is_read' = 'false'
          )
    ) AS new_unread_count;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION refresh_all_unread_counts IS 
'批量更新所有订阅的未读计数';


-- 4. 验证修复
SELECT '修复完成！' as status;

-- 测试函数（如果有订阅的话）
DO $$
DECLARE
    v_sub_id INT;
BEGIN
    SELECT id INTO v_sub_id FROM subscription_list LIMIT 1;
    IF v_sub_id IS NOT NULL THEN
        RAISE NOTICE '测试更新订阅 % 的未读计数...', v_sub_id;
        PERFORM update_subscription_unread_count(v_sub_id, 
            (SELECT user_id FROM subscription_list WHERE id = v_sub_id));
        RAISE NOTICE '更新成功！';
    ELSE
        RAISE NOTICE '没有订阅可供测试';
    END IF;
END $$;
