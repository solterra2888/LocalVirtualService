-- =====================================================
-- file_tags: people / countries / tag_schema_version
-- =====================================================
-- 创建时间: 2026-05-19
-- 说明: Upload Sources 自动打标新增关键人物与国家字段, 与 subscription_content_tags 对齐.

ALTER TABLE file_tags
    ADD COLUMN IF NOT EXISTS people JSONB DEFAULT '[]'::jsonb,
    ADD COLUMN IF NOT EXISTS countries JSONB DEFAULT '[]'::jsonb,
    ADD COLUMN IF NOT EXISTS tag_schema_version VARCHAR(32) DEFAULT 'file_v1';

CREATE INDEX IF NOT EXISTS idx_ft_people_gin
    ON file_tags USING gin(people);
CREATE INDEX IF NOT EXISTS idx_ft_countries_gin
    ON file_tags USING gin(countries);
CREATE INDEX IF NOT EXISTS idx_ft_tag_schema_version
    ON file_tags(tag_schema_version);

COMMENT ON COLUMN file_tags.people
    IS '关键人物 [{name, name_en, role, affiliation?}], name 保留原文写法';
COMMENT ON COLUMN file_tags.countries
    IS '国家/地区英文全名数组, 如 United States, China';
COMMENT ON COLUMN file_tags.tag_schema_version
    IS '打标 schema 版本, Upload 默认 file_v1 (content_tag_v1)';
