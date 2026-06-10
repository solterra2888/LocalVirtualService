-- ============================================================
-- 为 investment_theme_embeddings.embedding_vector 添加 HNSW 索引
--
-- 用途:
--   Phase 2 向量匹配的性能优化。HNSW 索引使余弦相似度检索从全表扫描
--   降至近似最近邻查询，在 ~1000 个主题下查询时间从 O(n) 降至 O(log n)。
--
-- 前提:
--   仅在 pgvector 扩展已安装且 embedding_vector 列存在时生效。
--   若使用 JSONB 模式（embedding_data 列），则静默跳过。
--
-- 参数说明:
--   m = 16: 每层最大连接数（平衡召回率与索引大小）
--   ef_construction = 64: 构建时搜索宽度（越高越准但越慢）
--   vector_cosine_ops: 余弦距离操作符 (<=>)
-- ============================================================

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'investment_theme_embeddings'
          AND column_name = 'embedding_vector'
    ) THEN
        CREATE INDEX IF NOT EXISTS idx_ite_embedding_hnsw
            ON investment_theme_embeddings
            USING hnsw (embedding_vector vector_cosine_ops)
            WITH (m = 16, ef_construction = 64);
        RAISE NOTICE 'HNSW index created on investment_theme_embeddings.embedding_vector';
    ELSE
        RAISE NOTICE 'embedding_vector column not found, skipping HNSW index (likely JSONB mode)';
    END IF;
END $$;
