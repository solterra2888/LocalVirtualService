-- =====================================================
-- file_tags.summary 字段补充
-- =====================================================
-- 创建时间: 2026-04-25
-- 说明: 为 Upload Files 链路的 file_tags 表添加 summary 字段,
--       与 subscription_content_tags.summary 对齐, 用于 Daily 聚合视图.
--       目前 content_tag_v1 模板未生成 summary, 字段允许为空; 后续可在模板升级时补全.
--       Daily API 读取顺序: file_tags.summary → additional_tags->>'summary' → reasoning → content 截断.

ALTER TABLE file_tags
    ADD COLUMN IF NOT EXISTS summary TEXT;

COMMENT ON COLUMN file_tags.summary
    IS '50-200 字单句摘要 (与 subscription_content_tags.summary 对齐, Daily 聚合视图使用; 当前 content_tag_v1 未生成, 可后续补)';

-- =====================================================
-- 验证
-- =====================================================
-- \d file_tags
