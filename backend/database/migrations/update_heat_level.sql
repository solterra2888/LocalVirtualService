-- 更新现有内容的热度等级
-- 根据 engagement_count 计算 heat_level

-- 热度分级标准：
-- 灰色（level 1）: 低热度 - engagement < 100
-- 蓝色（level 2）: 中等热度 - 100 <= engagement < 1000
-- 橙色（level 3）: 高热度 - 1000 <= engagement < 10000
-- 红色（level 4）: 超高热度 - engagement >= 10000

-- 开始事务
BEGIN;

-- 更新所有内容的热度等级
UPDATE subscription_content
SET heat_level = CASE
    WHEN engagement_count >= 10000 THEN 4  -- 红色 - 超高热度
    WHEN engagement_count >= 1000 THEN 3   -- 橙色 - 高热度
    WHEN engagement_count >= 100 THEN 2    -- 蓝色 - 中等热度
    ELSE 1                                  -- 灰色 - 低热度
END;

-- 显示更新结果统计
SELECT 
    heat_level,
    COUNT(*) as count,
    CASE 
        WHEN heat_level = 1 THEN '灰色 - 低热度 (< 100)'
        WHEN heat_level = 2 THEN '蓝色 - 中等热度 (100-999)'
        WHEN heat_level = 3 THEN '橙色 - 高热度 (1000-9999)'
        WHEN heat_level = 4 THEN '红色 - 超高热度 (≥ 10000)'
        ELSE '未知'
    END as description
FROM subscription_content
GROUP BY heat_level
ORDER BY heat_level;

-- 显示一些样例数据
SELECT 
    id,
    content_id,
    author_name,
    engagement_count,
    heat_level,
    CASE 
        WHEN heat_level = 1 THEN '灰色'
        WHEN heat_level = 2 THEN '蓝色'
        WHEN heat_level = 3 THEN '橙色'
        WHEN heat_level = 4 THEN '红色'
        ELSE '未知'
    END as heat_color,
    published_at
FROM subscription_content
ORDER BY engagement_count DESC
LIMIT 20;

-- 提交事务
COMMIT;
