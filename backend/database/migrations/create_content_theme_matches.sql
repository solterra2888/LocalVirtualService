-- ============================================================
-- content_theme_matches: 内容（File / Feed）与投资主题的匹配关联表
--
-- 用途:
--   打标完成后自动关联内容到 Investment Themes:
--   - ticker 模式: 内容 tickers 与 theme_company_associations 交集（零 LLM 成本）
--   - vector 模式: 内容 embedding 与主题 embedding 余弦相似度（极低 LLM 成本）
--
-- 设计:
--   全局匹配，不存 user_id。match 是 content↔theme 的客观事实。
--   卡片上直接显示所有匹配到的 theme 标签，不按用户归属过滤。
--   同一主题可通过 ticker 和 vector 两种方式匹配，各存一行。
-- ============================================================

CREATE TABLE IF NOT EXISTS content_theme_matches (
    id              BIGSERIAL       PRIMARY KEY,
    content_type    TEXT            NOT NULL,       -- 'file' | 'feed'
    content_id      INTEGER         NOT NULL,       -- file_processing_details.id 或 subscription_content.id
    theme_id        TEXT            NOT NULL,       -- investment_theme_embeddings.theme_id
    match_mode      TEXT            NOT NULL,       -- 'ticker' | 'vector'
    matched_tickers JSONB           DEFAULT '[]'::jsonb,  -- 导致匹配的具体 ticker 列表（ticker 模式）
    similarity      DOUBLE PRECISION,              -- 向量余弦相似度（ticker 模式为 NULL）
    confidence      DOUBLE PRECISION,              -- 综合置信度：ticker=1.0, vector=similarity
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    -- 同一内容+主题+模式唯一，ticker 和 vector 各占一行
    UNIQUE (content_type, content_id, theme_id, match_mode)
);

-- 查询指定内容的所有匹配主题
CREATE INDEX idx_ctm_content ON content_theme_matches(content_type, content_id);

-- 查询指定主题的所有关联内容（主题详情页 / Theme Report）
CREATE INDEX idx_ctm_theme ON content_theme_matches(theme_id, match_mode);

-- 向量匹配按相似度排序（部分索引，仅 vector 行）
CREATE INDEX idx_ctm_similarity ON content_theme_matches(similarity DESC)
    WHERE match_mode = 'vector';
