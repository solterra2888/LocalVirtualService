-- =====================================================
-- 将 investment_theme_embeddings 表的 group 字段改为可空
-- 创建日期: 2025-01-XX
-- 目的: 使 group 字段成为可选字段，不再强制要求
-- =====================================================
--
-- 功能说明：
-- 1. 将 group 字段从 NOT NULL 改为可空（NULL）
-- 2. 允许创建主题时不提供 group 值
-- 3. 保持现有数据的 group 值不变
--
-- 使用方法:
--   psql -h 111.230.48.119 -U postgres -d rc_database -f make_group_nullable_in_investment_theme_embeddings.sql
-- =====================================================

BEGIN;

-- 将 group 字段改为可空（移除 NOT NULL 约束）
ALTER TABLE investment_theme_embeddings 
ALTER COLUMN "group" DROP NOT NULL;

-- 更新字段注释
COMMENT ON COLUMN investment_theme_embeddings."group" IS 
'主题分组标识（如 AI、crypto 等），用于主题归类，不参与向量化（可选字段）';

-- 完成提示
DO $$
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE '✅ group 字段已改为可空！';
    RAISE NOTICE '========================================';
    RAISE NOTICE '';
    RAISE NOTICE '已修改：';
    RAISE NOTICE '  - group VARCHAR(100) - 现在是可选字段（可为 NULL）';
    RAISE NOTICE '';
    RAISE NOTICE '注意：';
    RAISE NOTICE '  - 现有数据的 group 值保持不变';
    RAISE NOTICE '  - 新创建的主题可以不提供 group 值';
    RAISE NOTICE '  - group 不参与 LLM Description 和向量化';
    RAISE NOTICE '  - 仅用于主题的逻辑归类';
    RAISE NOTICE '========================================';
END $$;

COMMIT;

