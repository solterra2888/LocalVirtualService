-- =====================================================
-- Subscription Content Tags — Feeds 内容自动打标结果表
-- =====================================================
-- 创建时间: 2026-04-24
-- 说明: 对标 file_tags, 但绑定到 subscription_content (Feeds 内容).
--       用于存储 Twitter / YouTube feed 单条内容的自动打标 + 短摘要结果.
--       一条 subscription_content 最多对应一行 tags (UNIQUE).
--       详见: docs/instructions/FEEDS_AUTO_TAGGING_PLAN.md

CREATE TABLE IF NOT EXISTS subscription_content_tags (
    id SERIAL PRIMARY KEY,
    subscription_content_id INTEGER NOT NULL
        REFERENCES subscription_content(id) ON DELETE CASCADE,

    -- GICS 行业分类 (与 file_tags 对齐)
    sector      TEXT NOT NULL,
    sector_en   TEXT,
    industry    TEXT NOT NULL,
    industry_en TEXT,

    -- 关键标签字段
    ticker          JSONB,                        -- ["NVDA.US","TSLA.US"]
    concepts        JSONB DEFAULT '[]'::jsonb,    -- ["AI基础设施","边缘计算"]
    additional_tags JSONB DEFAULT '{}'::jsonb,    -- {tickers_full, ai_metadata}
    confidence      DOUBLE PRECISION,
    reasoning       TEXT,

    -- Feeds 独有字段
    summary      TEXT,                            -- 50-200 字单句摘要
    channel_type VARCHAR(20) NOT NULL,            -- 冗余存一份, 便于 WHERE 过滤

    -- 溯源
    tagged_by  INTEGER REFERENCES users(id) ON DELETE SET NULL,
    tagged_at  TIMESTAMP(6) WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP(6) WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP(6) WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT check_subscription_content_tags_confidence_range
        CHECK (confidence IS NULL OR (confidence >= 0 AND confidence <= 1)),
    CONSTRAINT unique_subscription_content_tags_content_id
        UNIQUE (subscription_content_id)
);

-- 检索索引
CREATE INDEX IF NOT EXISTS idx_sct_sector_industry
    ON subscription_content_tags(sector, industry);
CREATE INDEX IF NOT EXISTS idx_sct_ticker_gin
    ON subscription_content_tags USING gin(ticker);
CREATE INDEX IF NOT EXISTS idx_sct_concepts_gin
    ON subscription_content_tags USING gin(concepts);
CREATE INDEX IF NOT EXISTS idx_sct_additional_gin
    ON subscription_content_tags USING gin(additional_tags);
CREATE INDEX IF NOT EXISTS idx_sct_channel_type
    ON subscription_content_tags(channel_type);
CREATE INDEX IF NOT EXISTS idx_sct_tagged_at
    ON subscription_content_tags(tagged_at DESC);

-- updated_at 触发器 (复用全局函数)
DROP TRIGGER IF EXISTS update_subscription_content_tags_updated_at
    ON subscription_content_tags;
CREATE TRIGGER update_subscription_content_tags_updated_at
    BEFORE UPDATE ON subscription_content_tags
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

COMMENT ON TABLE  subscription_content_tags
    IS 'Feeds (Twitter/YouTube) 单条内容的 LLM 自动打标 + 摘要结果, 对标 file_tags';
COMMENT ON COLUMN subscription_content_tags.subscription_content_id
    IS 'FK → subscription_content.id, CASCADE 删除';
COMMENT ON COLUMN subscription_content_tags.summary
    IS '50-200 字单句摘要 (Feeds 独有, 与 file_tags 的区别)';
COMMENT ON COLUMN subscription_content_tags.channel_type
    IS '冗余 subscription_content.channel_type, 便于按渠道过滤 (twitter/youtube)';
COMMENT ON COLUMN subscription_content_tags.additional_tags
    IS '{tickers_full: [...], ai_metadata: {total_tokens, total_price, model, provider, response_time}}';
COMMENT ON COLUMN subscription_content_tags.confidence
    IS 'LLM 自评置信度 (0-1), 低于 0.5 前端建议标注 "AI 低置信度"';

-- =====================================================
-- 验证
-- =====================================================
-- \d subscription_content_tags
