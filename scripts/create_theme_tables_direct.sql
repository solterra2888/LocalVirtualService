-- 投资主题向量表创建脚本（可直接在psql中执行）
-- 不依赖pgvector扩展，使用JSONB存储向量数据

BEGIN;

-- 创建投资主题向量表
CREATE TABLE IF NOT EXISTS investment_theme_embeddings (
    id SERIAL PRIMARY KEY,
    theme_id VARCHAR(255) UNIQUE NOT NULL,
    theme_name VARCHAR(255) NOT NULL,
    market VARCHAR(10) NOT NULL CHECK (market IN ('HK', 'US', 'CN')),
    embedding_data JSONB NOT NULL,
    embedding_model VARCHAR(50) DEFAULT 'text-embedding-v3',
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- 添加表注释
COMMENT ON TABLE investment_theme_embeddings IS '投资主题向量表 - 使用JSONB存储1024维向量数据';

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_theme_market ON investment_theme_embeddings(market);
CREATE INDEX IF NOT EXISTS idx_theme_name ON investment_theme_embeddings(theme_name);
CREATE INDEX IF NOT EXISTS idx_theme_updated_at ON investment_theme_embeddings(updated_at);
CREATE INDEX IF NOT EXISTS idx_theme_embedding_gin ON investment_theme_embeddings USING GIN (embedding_data);

-- 创建新闻主题匹配结果表
CREATE TABLE IF NOT EXISTS news_theme_matches (
    id SERIAL PRIMARY KEY,
    news_id INTEGER NOT NULL REFERENCES news(id) ON DELETE CASCADE,
    theme_id VARCHAR(255) NOT NULL,
    theme_name VARCHAR(255) NOT NULL,
    market VARCHAR(10) NOT NULL,
    similarity_score DECIMAL(5, 4) NOT NULL,
    match_rank INTEGER NOT NULL,
    match_method VARCHAR(50) DEFAULT 'vector_search',
    matched_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(news_id, theme_id)
);

-- 创建匹配表索引
CREATE INDEX IF NOT EXISTS idx_news_theme_news_id ON news_theme_matches(news_id);
CREATE INDEX IF NOT EXISTS idx_news_theme_theme_id ON news_theme_matches(theme_id);
CREATE INDEX IF NOT EXISTS idx_news_theme_similarity ON news_theme_matches(similarity_score DESC);
CREATE INDEX IF NOT EXISTS idx_news_theme_matched_at ON news_theme_matches(matched_at);

-- 创建辅助函数
CREATE OR REPLACE FUNCTION validate_embedding_data(data JSONB)
RETURNS BOOLEAN AS $$
BEGIN
    IF jsonb_typeof(data) != 'array' THEN
        RETURN FALSE;
    END IF;
    IF jsonb_array_length(data) != 1024 THEN
        RETURN FALSE;
    END IF;
    RETURN TRUE;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE FUNCTION get_embedding_dimension(data JSONB)
RETURNS INTEGER AS $$
BEGIN
    RETURN jsonb_array_length(data);
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- 创建查询视图
CREATE OR REPLACE VIEW v_news_with_themes AS
SELECT 
    n.id as news_id,
    n.date,
    n.title,
    n.source,
    n.analysis_status,
    ntm.theme_name,
    ntm.market,
    ntm.similarity_score,
    ntm.match_rank,
    ntm.matched_at
FROM news n
LEFT JOIN news_theme_matches ntm ON n.id = ntm.news_id
WHERE ntm.match_rank <= 3
ORDER BY n.date DESC, ntm.match_rank ASC;

-- 添加约束
ALTER TABLE investment_theme_embeddings 
ADD CONSTRAINT chk_embedding_data_format 
CHECK (validate_embedding_data(embedding_data));

ALTER TABLE news_theme_matches 
ADD CONSTRAINT chk_similarity_score_range 
CHECK (similarity_score >= 0.0000 AND similarity_score <= 1.0000);

ALTER TABLE news_theme_matches 
ADD CONSTRAINT chk_match_rank_positive 
CHECK (match_rank > 0);

-- 创建插入函数
CREATE OR REPLACE FUNCTION insert_theme_embedding(
    p_theme_id VARCHAR(255),
    p_theme_name VARCHAR(255),
    p_market VARCHAR(10),
    p_embedding_data JSONB,
    p_embedding_model VARCHAR(50) DEFAULT 'text-embedding-v3'
) RETURNS INTEGER AS $$
DECLARE
    new_id INTEGER;
BEGIN
    IF NOT validate_embedding_data(p_embedding_data) THEN
        RAISE EXCEPTION 'Invalid embedding data format. Expected 1024-dimensional array.';
    END IF;
    
    INSERT INTO investment_theme_embeddings (
        theme_id, theme_name, market, embedding_data, embedding_model
    ) VALUES (
        p_theme_id, p_theme_name, p_market, p_embedding_data, p_embedding_model
    ) ON CONFLICT (theme_id) DO UPDATE SET
        theme_name = EXCLUDED.theme_name,
        market = EXCLUDED.market,
        embedding_data = EXCLUDED.embedding_data,
        embedding_model = EXCLUDED.embedding_model,
        updated_at = NOW()
    RETURNING id INTO new_id;
    
    RETURN new_id;
END;
$$ LANGUAGE plpgsql;

COMMIT;

-- 完成提示
DO $$
BEGIN
    RAISE NOTICE '✅ 投资主题向量表创建完成！';
    RAISE NOTICE '已创建: investment_theme_embeddings, news_theme_matches';
    RAISE NOTICE '已创建: 相关索引、函数和视图';
END $$;
