-- =====================================================
-- 创建投资主题向量表（用于向量检索）
-- 创建日期: 2025-10-20
-- 目的: 支持基于Embedding的新闻主题匹配
-- =====================================================
--
-- 功能说明：
-- 1. 存储Futu投资主题的向量表示
-- 2. 支持基于余弦相似度的向量检索
-- 3. 使用pgvector扩展实现高性能向量搜索
--
-- 使用方法:
--   psql -h 111.230.48.119 -U postgres -d rc_database -f add_investment_theme_embeddings.sql
-- =====================================================

BEGIN;

-- ============================================================
-- 步骤1: 安装pgvector扩展
-- ============================================================

-- 检查并安装pgvector扩展
CREATE EXTENSION IF NOT EXISTS vector;

DO $$
BEGIN
    RAISE NOTICE '✅ pgvector扩展已安装';
END $$;

-- ============================================================
-- 步骤2: 创建投资主题向量表
-- ============================================================

CREATE TABLE IF NOT EXISTS investment_theme_embeddings (
    id SERIAL PRIMARY KEY,
    theme_id VARCHAR(255) UNIQUE NOT NULL,
    theme_name VARCHAR(255) NOT NULL,
    market VARCHAR(10) NOT NULL CHECK (market IN ('HK', 'US', 'CN')),
    embedding_vector vector(1024) NOT NULL,  -- Qwen text-embedding-v3 维度: 1024
    embedding_model VARCHAR(50) DEFAULT 'text-embedding-v3',
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- 添加表注释
COMMENT ON TABLE investment_theme_embeddings IS 
'投资主题向量表 - 使用Qwen text-embedding-v3生成的1024维向量，用于新闻主题匹配';

COMMENT ON COLUMN investment_theme_embeddings.theme_id IS 
'主题唯一标识（来自futu_investment_themes）';

COMMENT ON COLUMN investment_theme_embeddings.theme_name IS 
'主题名称（例如：Electric Vehicles, 5G）';

COMMENT ON COLUMN investment_theme_embeddings.market IS 
'市场代码：HK-香港, US-美国, CN-中国大陆';

COMMENT ON COLUMN investment_theme_embeddings.embedding_vector IS 
'1024维向量表示，用于余弦相似度计算';

COMMENT ON COLUMN investment_theme_embeddings.embedding_model IS 
'使用的Embedding模型名称';

-- ============================================================
-- 步骤3: 创建索引
-- ============================================================

-- 创建向量索引（HNSW算法，速度快，适合生产环境）
-- 参数说明：
--   m=16: 每个节点的最大连接数，越大精度越高但内存越大
--   ef_construction=64: 构建索引时的搜索宽度，越大构建越慢但精度越高
CREATE INDEX IF NOT EXISTS idx_theme_embeddings_vector_hnsw 
ON investment_theme_embeddings 
USING hnsw (embedding_vector vector_cosine_ops)
WITH (m = 16, ef_construction = 64);

-- 创建市场索引（用于过滤特定市场的主题）
CREATE INDEX IF NOT EXISTS idx_theme_market 
ON investment_theme_embeddings(market);

-- 创建主题名称索引（用于快速查询）
CREATE INDEX IF NOT EXISTS idx_theme_name 
ON investment_theme_embeddings(theme_name);

-- 创建更新时间索引（用于增量更新）
CREATE INDEX IF NOT EXISTS idx_theme_updated_at 
ON investment_theme_embeddings(updated_at);

-- 添加索引注释
COMMENT ON INDEX idx_theme_embeddings_vector_hnsw IS 
'HNSW向量索引 - 支持高效的余弦相似度检索（1000个主题约50ms）';

COMMENT ON INDEX idx_theme_market IS 
'市场索引 - 支持按市场过滤主题（HK/US/CN）';

-- ============================================================
-- 步骤4: 创建新闻主题匹配结果表（可选）
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
-- 步骤5: 创建辅助函数
-- ============================================================

-- 函数：计算两个向量的余弦相似度
CREATE OR REPLACE FUNCTION cosine_similarity(
    vec1 vector(1024), 
    vec2 vector(1024)
) RETURNS FLOAT AS $$
BEGIN
    RETURN 1 - (vec1 <=> vec2);
END;
$$ LANGUAGE plpgsql IMMUTABLE;

COMMENT ON FUNCTION cosine_similarity IS 
'计算两个1024维向量的余弦相似度（返回0-1，1表示完全相同）';

-- ============================================================
-- 步骤6: 创建查询视图（便于分析）
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
-- 完成提示
-- ============================================================

DO $$
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE '✅ 投资主题向量表创建完成！';
    RAISE NOTICE '========================================';
    RAISE NOTICE '';
    RAISE NOTICE '已创建的表和索引：';
    RAISE NOTICE '  1. investment_theme_embeddings - 主题向量表';
    RAISE NOTICE '  2. news_theme_matches - 新闻主题匹配表';
    RAISE NOTICE '  3. idx_theme_embeddings_vector_hnsw - HNSW向量索引';
    RAISE NOTICE '  4. v_news_with_themes - 新闻主题视图';
    RAISE NOTICE '';
    RAISE NOTICE '下一步操作：';
    RAISE NOTICE '  1. 运行初始化脚本生成主题向量：';
    RAISE NOTICE '     python scripts/initialize_theme_embeddings.py';
    RAISE NOTICE '  2. 测试向量检索功能';
    RAISE NOTICE '  3. 在新闻分析流程中集成主题匹配';
    RAISE NOTICE '';
    RAISE NOTICE '查询示例：';
    RAISE NOTICE '  -- 查看所有主题向量';
    RAISE NOTICE '  SELECT theme_name, market FROM investment_theme_embeddings;';
    RAISE NOTICE '';
    RAISE NOTICE '  -- 查询某条新闻的匹配主题';
    RAISE NOTICE '  SELECT * FROM v_news_with_themes WHERE news_id = 123;';
    RAISE NOTICE '========================================';
END $$;

COMMIT;

