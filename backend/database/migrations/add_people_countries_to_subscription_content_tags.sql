-- =====================================================
-- subscription_content_tags: people / countries / tag_schema_version
-- =====================================================
-- 创建时间: 2026-05-19
-- 说明: Feeds 打标新增关键人物与国家维度; 标记所用 prompt schema 版本.

ALTER TABLE subscription_content_tags
    ADD COLUMN IF NOT EXISTS people JSONB DEFAULT '[]'::jsonb,
    ADD COLUMN IF NOT EXISTS countries JSONB DEFAULT '[]'::jsonb,
    ADD COLUMN IF NOT EXISTS tag_schema_version VARCHAR(32) DEFAULT 'feeds_v1';

CREATE INDEX IF NOT EXISTS idx_sct_people_gin
    ON subscription_content_tags USING gin(people);
CREATE INDEX IF NOT EXISTS idx_sct_countries_gin
    ON subscription_content_tags USING gin(countries);
CREATE INDEX IF NOT EXISTS idx_sct_tag_schema_version
    ON subscription_content_tags(tag_schema_version);

COMMENT ON COLUMN subscription_content_tags.people
    IS '关键人物 [{name, name_en, role, affiliation}]';
COMMENT ON COLUMN subscription_content_tags.countries
    IS '相关国家/地区 ISO 3166-1 alpha-2 代码列表, 如 ["US","CN"]';
COMMENT ON COLUMN subscription_content_tags.tag_schema_version
    IS '打标 schema: feeds_v1 | tweet_signal_v1';
