-- =====================================================
-- 为 investment_theme_embeddings 表添加 group 字段
-- 创建日期: 2025-11-14
-- 目的: 支持对主题名称进行分组归类
-- =====================================================
--
-- 功能说明：
-- 1. 添加 group 字段，用于对主题进行分组（如 AI、crypto 等）
-- 2. group 字段不参与 LLM Description 生成和向量化
-- 3. 仅用于主题的逻辑归类和前端展示
--
-- 使用方法:
--   psql -h 111.230.48.119 -U postgres -d rc_database -f add_group_to_investment_theme_embeddings.sql
-- =====================================================

BEGIN;

-- 添加 group 字段（必填字段，VARCHAR(100)）
ALTER TABLE investment_theme_embeddings 
ADD COLUMN IF NOT EXISTS "group" VARCHAR(100);

-- 为现有数据设置默认值（如果字段已存在且有NULL值）
UPDATE investment_theme_embeddings 
SET "group" = 'Uncategorized' 
WHERE "group" IS NULL;

-- 将字段设置为NOT NULL（必填）
ALTER TABLE investment_theme_embeddings 
ALTER COLUMN "group" SET NOT NULL;

-- 添加字段注释
COMMENT ON COLUMN investment_theme_embeddings."group" IS 
'主题分组标识（如 AI、crypto 等），用于主题归类，不参与向量化（必填字段）';

-- 创建索引以支持按 group 查询
CREATE INDEX IF NOT EXISTS idx_theme_group 
ON investment_theme_embeddings("group");

COMMENT ON INDEX idx_theme_group IS 
'主题分组索引 - 支持按 group 快速查询和筛选';

-- 完成提示
DO $$
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE '✅ group 字段添加完成！';
    RAISE NOTICE '========================================';
    RAISE NOTICE '';
    RAISE NOTICE '已添加：';
    RAISE NOTICE '  1. group VARCHAR(100) - 主题分组字段';
    RAISE NOTICE '  2. idx_theme_group - 分组索引';
    RAISE NOTICE '';
    RAISE NOTICE '注意：';
    RAISE NOTICE '  - group 字段是必填的（NOT NULL）';
    RAISE NOTICE '  - 现有数据的NULL值已设置为 "Uncategorized"';
    RAISE NOTICE '  - group 不参与 LLM Description 和向量化';
    RAISE NOTICE '  - 仅用于主题的逻辑归类';
    RAISE NOTICE '========================================';
END $$;

COMMIT;

