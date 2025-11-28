-- =====================================================
-- 投资主题向量表（备用版本 - 不依赖pgvector）
-- 创建日期: 2025-10-20
-- 目的: 支持基于Embedding的新闻主题匹配（无pgvector版本）
-- =====================================================
--
-- 功能说明：
-- 1. 存储Futu投资主题的向量表示（使用JSONB）
-- 2. 支持基于余弦相似度的向量检索（Python层计算）
-- 3. 兼容标准PostgreSQL，无需额外扩展
--
-- 使用方法:
--   psql -h 111.230.48.119 -U postgres -d rc_database -f add_investment_theme_embeddings_fallback.sql
-- =====================================================

BEGIN;

-- ============================================================
-- 步骤1: 创建投资主题向量表（JSONB版本）
-- ============================================================

CREATE TABLE IF NOT EXISTS investment_theme_embeddings (
    id SERIAL PRIMARY KEY,
    theme_id VARCHAR(255) UNIQUE NOT NULL,
    theme_name VARCHAR(255) NOT NULL,
    market VARCHAR(10) NOT NULL CHECK (market IN ('HK', 'US', 'CN')),
    embedding_data JSONB NOT NULL,  -- 存储1024维向量数据
    embedding_model VARCHAR(50) DEFAULT 'text-embedding-v3',
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- 添加表注释
COMMENT ON TABLE investment_theme_embeddings IS 
'投资主题向量表 - 使用JSONB存储Qwen text-embedding-v3生成的1024维向量，用于新闻主题匹配';

COMMENT ON COLUMN investment_theme_embeddings.theme_id IS 
'主题唯一标识（来自futu_investment_themes）';

COMMENT ON COLUMN investment_theme_embeddings.theme_name IS 
'主题名称（例如：Electric Vehicles, 5G）';

COMMENT ON COLUMN investment_theme_embeddings.market IS 
'市场代码：HK-香港, US-美国, CN-中国大陆';

COMMENT ON COLUMN investment_theme_embeddings.embedding_data IS 
'JSONB格式的1024维向量数据，例如：[0.1, 0.2, 0.3, ...]';

COMMENT ON COLUMN investment_theme_embeddings.embedding_model IS 
'使用的Embedding模型名称';

-- ============================================================
-- 步骤2: 创建索引
-- ============================================================

-- 创建市场索引（用于过滤特定市场的主题）
CREATE INDEX IF NOT EXISTS idx_theme_market 
ON investment_theme_embeddings(market);

-- 创建主题名称索引（用于快速查询）
CREATE INDEX IF NOT EXISTS idx_theme_name 
ON investment_theme_embeddings(theme_name);

-- 创建更新时间索引（用于增量更新）
CREATE INDEX IF NOT EXISTS idx_theme_updated_at 
ON investment_theme_embeddings(updated_at);

-- 创建JSONB GIN索引（用于向量数据查询）
CREATE INDEX IF NOT EXISTS idx_theme_embedding_gin 
ON investment_theme_embeddings USING GIN (embedding_data);

-- 添加索引注释
COMMENT ON INDEX idx_theme_market IS 
'市场索引 - 支持按市场过滤主题（HK/US/CN）';

COMMENT ON INDEX idx_theme_embedding_gin IS 
'JSONB GIN索引 - 支持向量数据的快速查询';

-- ============================================================
-- 步骤3: 创建新闻主题匹配结果表
-- ============================================================

CREATE TABLE IF NOT EXISTS news_theme_matches (
    id SERIAL PRIMARY KEY,
    news_id INTEGER NOT NULL REFERENCES news(id) ON DELETE CASCADE,
    theme_id VARCHAR(255) NOT NULL,
    theme_name VARCHAR(255) NOT NULL,
    market VARCHAR(10) NOT NULL,
    similarity_score DECIMAL(5, 4) NOT NULL,  -- 0.0000 - 1.0000
    match_rank INTEGER NOT NULL,  -- 1, 2, 3... (Top-K排名)
    match_method VARCHAR(50) DEFAULT 'vector_search',  -- vector_search, llm_confirm
    matched_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(news_id, theme_id)
);

-- 添加表注释
COMMENT ON TABLE news_theme_matches IS 
'新闻主题匹配结果表 - 存储新闻与投资主题的匹配关系';

COMMENT ON COLUMN news_theme_matches.similarity_score IS 
'相似度评分（0-1），基于向量余弦相似度';

COMMENT ON COLUMN news_theme_matches.match_rank IS 
'匹配排名（1=最相关），用于Top-K结果';

COMMENT ON COLUMN news_theme_matches.match_method IS 
'匹配方法：vector_search（纯向量），llm_confirm（LLM确认）';

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_news_theme_news_id 
ON news_theme_matches(news_id);

CREATE INDEX IF NOT EXISTS idx_news_theme_theme_id 
ON news_theme_matches(theme_id);

CREATE INDEX IF NOT EXISTS idx_news_theme_similarity 
ON news_theme_matches(similarity_score DESC);

CREATE INDEX IF NOT EXISTS idx_news_theme_matched_at 
ON news_theme_matches(matched_at);

-- ============================================================
-- 步骤4: 创建辅助函数（JSONB版本）
-- ============================================================

-- 函数：验证向量数据格式
CREATE OR REPLACE FUNCTION validate_embedding_data(data JSONB)
RETURNS BOOLEAN AS $$
BEGIN
    -- 检查是否为数组
    IF jsonb_typeof(data) != 'array' THEN
        RETURN FALSE;
    END IF;
    
    -- 检查数组长度是否为1024
    IF jsonb_array_length(data) != 1024 THEN
        RETURN FALSE;
    END IF;
    
    -- 检查所有元素是否为数字
    RETURN TRUE;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

COMMENT ON FUNCTION validate_embedding_data IS 
'验证JSONB向量数据格式是否正确（1024维数字数组）';

-- 函数：获取向量维度
CREATE OR REPLACE FUNCTION get_embedding_dimension(data JSONB)
RETURNS INTEGER AS $$
BEGIN
    RETURN jsonb_array_length(data);
END;
$$ LANGUAGE plpgsql IMMUTABLE;

COMMENT ON FUNCTION get_embedding_dimension IS 
'获取JSONB向量数据的维度';

-- ============================================================
-- 步骤5: 创建查询视图
-- ============================================================

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
WHERE ntm.match_rank <= 3  -- 只显示Top-3匹配
ORDER BY n.date DESC, ntm.match_rank ASC;

COMMENT ON VIEW v_news_with_themes IS 
'新闻与主题关联视图 - 便于查询每条新闻的Top-3匹配主题';

-- ============================================================
-- 步骤6: 创建数据验证约束
-- ============================================================

-- 添加向量数据验证约束（如果不存在）
DO $$ 
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint 
        WHERE conname = 'chk_embedding_data_format'
    ) THEN
        ALTER TABLE investment_theme_embeddings 
        ADD CONSTRAINT chk_embedding_data_format 
        CHECK (validate_embedding_data(embedding_data));
    END IF;
END $$;

-- 添加相似度分数范围约束（如果不存在）
DO $$ 
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint 
        WHERE conname = 'chk_similarity_score_range'
    ) THEN
        ALTER TABLE news_theme_matches 
        ADD CONSTRAINT chk_similarity_score_range 
        CHECK (similarity_score >= 0.0000 AND similarity_score <= 1.0000);
    END IF;
END $$;

-- 添加匹配排名约束（如果不存在）
DO $$ 
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint 
        WHERE conname = 'chk_match_rank_positive'
    ) THEN
        ALTER TABLE news_theme_matches 
        ADD CONSTRAINT chk_match_rank_positive 
        CHECK (match_rank > 0);
    END IF;
END $$;

-- ============================================================
-- 步骤7: 创建示例数据插入函数
-- ============================================================

-- 函数：插入主题向量数据
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
    -- 验证输入数据
    IF NOT validate_embedding_data(p_embedding_data) THEN
        RAISE EXCEPTION 'Invalid embedding data format. Expected 1024-dimensional array.';
    END IF;
    
    -- 插入数据
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

COMMENT ON FUNCTION insert_theme_embedding IS 
'插入或更新主题向量数据，自动验证格式';

-- ============================================================
-- 完成提示
-- ============================================================

DO $$
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE '✅ 投资主题向量表创建完成（备用版本）！';
    RAISE NOTICE '========================================';
    RAISE NOTICE '';
    RAISE NOTICE '已创建的表和索引：';
    RAISE NOTICE '  1. investment_theme_embeddings - 主题向量表（JSONB版本）';
    RAISE NOTICE '  2. news_theme_matches - 新闻主题匹配表';
    RAISE NOTICE '  3. idx_theme_embedding_gin - JSONB GIN索引';
    RAISE NOTICE '  4. v_news_with_themes - 新闻主题视图';
    RAISE NOTICE '';
    RAISE NOTICE '重要说明：';
    RAISE NOTICE '  - 此版本使用JSONB存储向量数据，无需pgvector扩展';
    RAISE NOTICE '  - 向量相似度计算需要在Python层进行';
    RAISE NOTICE '  - 性能略低于pgvector版本，但兼容性更好';
    RAISE NOTICE '';
    RAISE NOTICE '下一步操作：';
    RAISE NOTICE '  1. 运行初始化脚本生成主题向量：';
    RAISE NOTICE '     python scripts/maintenance/initialize_theme_embeddings.py';
    RAISE NOTICE '  2. 修改Python代码使用JSONB格式';
    RAISE NOTICE '  3. 测试向量检索功能';
    RAISE NOTICE '';
    RAISE NOTICE '查询示例：';
    RAISE NOTICE '  -- 查看所有主题向量';
    RAISE NOTICE '  SELECT theme_name, market, get_embedding_dimension(embedding_data) as dims';
    RAISE NOTICE '  FROM investment_theme_embeddings;';
    RAISE NOTICE '';
    RAISE NOTICE '  -- 插入示例向量数据';
    RAISE NOTICE '  SELECT insert_theme_embedding(';
    RAISE NOTICE '    ''theme_001'', ''Electric Vehicles'', ''US'',';
    RAISE NOTICE '    ''[0.1, 0.2, 0.3, ...]''::jsonb';
    RAISE NOTICE '  );';
    RAISE NOTICE '';
    RAISE NOTICE '  -- 查询某条新闻的匹配主题';
    RAISE NOTICE '  SELECT * FROM v_news_with_themes WHERE news_id = 123;';
    RAISE NOTICE '========================================';
END $$;

COMMIT;
