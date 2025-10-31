/*
 * Migration: Create Unified Theme System
 * Description: 创建统一主题管理系统，整合EODHD tags、Vector Retrieval和LLM分类三种主题来源
 * Date: 2025-10-27
 * 
 * 核心表：
 * - theme_master_data: 主题主数据表，管理标准化的主题名称和同义词
 * - news_themes_unified: 统一主题表，整合所有来源的主题数据
 * 
 * 视图：
 * - v_news_best_themes: 每条新闻的最佳主题（融合多源）
 * - v_news_themes_full: 新闻主题全景视图
 */

-- ============================================================
-- 1. 创建主题主数据表 (Theme Master Data)
-- ============================================================
CREATE TABLE IF NOT EXISTS theme_master_data (
    id SERIAL PRIMARY KEY,
    
    -- 标准化名称
    canonical_name VARCHAR(255) UNIQUE NOT NULL,
    display_name VARCHAR(255) NOT NULL,  -- 用于显示的名称
    
    -- 分类和描述
    category VARCHAR(100),  -- 主题分类：Technology, Healthcare, Energy, Finance等
    sub_category VARCHAR(100),
    description TEXT,
    
    -- 同义词映射（用于标准化不同来源的名称）
    aliases JSONB DEFAULT '[]'::jsonb,  -- ["Electric Vehicle", "EV", "新能源汽车", "电动车"]
    
    -- 层级关系
    parent_theme_id INTEGER REFERENCES theme_master_data(id) ON DELETE SET NULL,
    
    -- 元数据
    icon VARCHAR(50),  -- 图标名称
    color VARCHAR(20),  -- 主题颜色
    priority INTEGER DEFAULT 0,  -- 优先级（用于排序）
    
    -- 状态管理
    is_active BOOLEAN DEFAULT true,
    is_system BOOLEAN DEFAULT false,  -- 是否为系统预定义主题
    
    -- 统计信息
    usage_count INTEGER DEFAULT 0,  -- 被使用次数
    last_used_at TIMESTAMP,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100),
    updated_by VARCHAR(100)
);

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_theme_master_canonical_name ON theme_master_data(canonical_name);
CREATE INDEX IF NOT EXISTS idx_theme_master_category ON theme_master_data(category, sub_category);
CREATE INDEX IF NOT EXISTS idx_theme_master_parent ON theme_master_data(parent_theme_id);
CREATE INDEX IF NOT EXISTS idx_theme_master_active ON theme_master_data(is_active) WHERE is_active = true;
CREATE INDEX IF NOT EXISTS idx_theme_master_aliases_gin ON theme_master_data USING gin(aliases);

-- 添加触发器：自动更新updated_at
DROP TRIGGER IF EXISTS update_theme_master_data_updated_at ON theme_master_data;
CREATE TRIGGER update_theme_master_data_updated_at
    BEFORE UPDATE ON theme_master_data
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

COMMENT ON TABLE theme_master_data IS '主题主数据表 - 管理标准化的主题名称、同义词映射和层级关系';
COMMENT ON COLUMN theme_master_data.canonical_name IS '标准化的主题名称（唯一）';
COMMENT ON COLUMN theme_master_data.aliases IS 'JSON数组，存储该主题的所有同义词和别名';
COMMENT ON COLUMN theme_master_data.parent_theme_id IS '父主题ID，支持主题层级（如：AI > Machine Learning）';


-- ============================================================
-- 2. 创建统一主题表 (Unified Themes)
-- ============================================================
CREATE TABLE IF NOT EXISTS news_themes_unified (
    id SERIAL PRIMARY KEY,
    
    -- 关联的新闻
    news_id INTEGER NOT NULL REFERENCES news(id) ON DELETE CASCADE,
    
    -- 主题信息（可以关联主数据，也可以独立存在）
    theme_master_id INTEGER REFERENCES theme_master_data(id) ON DELETE SET NULL,
    theme_name VARCHAR(255) NOT NULL,  -- 原始主题名称
    canonical_theme_name VARCHAR(255),  -- 标准化后的名称
    
    -- 来源信息
    source_type VARCHAR(50) NOT NULL,  -- 'eodhd_auto_tag' | 'vector_retrieval' | 'llm_classification' | 'manual'
    source_id VARCHAR(255),  -- 原始来源的ID引用（如：auto_tag_id, theme_id等）
    source_table VARCHAR(100),  -- 原始数据表名
    
    -- 置信度和质量
    confidence_score NUMERIC(5,4) DEFAULT 0.5000,  -- 0-1，来源提供的置信度
    quality_score NUMERIC(5,4),  -- 0-1，由融合算法计算的综合质量分数
    relevance_score NUMERIC(5,4),  -- 0-1，与新闻的相关度
    
    -- 排名和权重
    rank_in_news INTEGER,  -- 在该新闻中的排名
    weight NUMERIC(5,4) DEFAULT 1.0000,  -- 权重（用于融合计算）
    
    -- 元数据（存储特定来源的额外信息）
    market VARCHAR(10),  -- HK/US/CN/GLOBAL，主要用于vector_retrieval
    metadata JSONB DEFAULT '{}'::jsonb,  -- 存储特定来源的额外字段
    
    -- 状态管理
    is_active BOOLEAN DEFAULT true,
    is_verified BOOLEAN DEFAULT false,  -- 是否经过人工验证
    is_primary BOOLEAN DEFAULT false,  -- 是否为主要主题
    
    -- 验证信息
    verified_by INTEGER REFERENCES users(id) ON DELETE SET NULL,
    verified_at TIMESTAMP,
    verification_note TEXT,
    
    -- 时间戳
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- 确保同一新闻+来源+主题不重复
    UNIQUE(news_id, source_type, theme_name, market)
);

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_news_themes_unified_news_id ON news_themes_unified(news_id);
CREATE INDEX IF NOT EXISTS idx_news_themes_unified_theme_master ON news_themes_unified(theme_master_id);
CREATE INDEX IF NOT EXISTS idx_news_themes_unified_theme_name ON news_themes_unified(theme_name);
CREATE INDEX IF NOT EXISTS idx_news_themes_unified_canonical_name ON news_themes_unified(canonical_theme_name);
CREATE INDEX IF NOT EXISTS idx_news_themes_unified_source_type ON news_themes_unified(source_type);
CREATE INDEX IF NOT EXISTS idx_news_themes_unified_confidence ON news_themes_unified(confidence_score DESC);
CREATE INDEX IF NOT EXISTS idx_news_themes_unified_quality ON news_themes_unified(quality_score DESC);
CREATE INDEX IF NOT EXISTS idx_news_themes_unified_active ON news_themes_unified(is_active, news_id);
CREATE INDEX IF NOT EXISTS idx_news_themes_unified_verified ON news_themes_unified(is_verified) WHERE is_verified = true;
CREATE INDEX IF NOT EXISTS idx_news_themes_unified_primary ON news_themes_unified(news_id, is_primary) WHERE is_primary = true;
CREATE INDEX IF NOT EXISTS idx_news_themes_unified_metadata_gin ON news_themes_unified USING gin(metadata);

-- 添加触发器：自动更新updated_at
DROP TRIGGER IF EXISTS update_news_themes_unified_updated_at ON news_themes_unified;
CREATE TRIGGER update_news_themes_unified_updated_at
    BEFORE UPDATE ON news_themes_unified
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- 添加约束检查（如果已存在则先删除）
DO $$ 
BEGIN
    -- 删除旧约束（如果存在）
    ALTER TABLE news_themes_unified DROP CONSTRAINT IF EXISTS check_source_type;
    ALTER TABLE news_themes_unified DROP CONSTRAINT IF EXISTS check_confidence_range;
    ALTER TABLE news_themes_unified DROP CONSTRAINT IF EXISTS check_quality_range;
    ALTER TABLE news_themes_unified DROP CONSTRAINT IF EXISTS check_relevance_range;
    
    -- 添加新约束
    ALTER TABLE news_themes_unified ADD CONSTRAINT check_source_type 
        CHECK (source_type IN ('eodhd_auto_tag', 'vector_retrieval', 'llm_classification', 'manual', 'hybrid'));
    
    ALTER TABLE news_themes_unified ADD CONSTRAINT check_confidence_range 
        CHECK (confidence_score >= 0 AND confidence_score <= 1);
    
    ALTER TABLE news_themes_unified ADD CONSTRAINT check_quality_range 
        CHECK (quality_score IS NULL OR (quality_score >= 0 AND quality_score <= 1));
    
    ALTER TABLE news_themes_unified ADD CONSTRAINT check_relevance_range 
        CHECK (relevance_score IS NULL OR (relevance_score >= 0 AND relevance_score <= 1));
END $$;

COMMENT ON TABLE news_themes_unified IS '统一主题表 - 整合EODHD、Vector Retrieval、LLM分类等多种来源的主题数据';
COMMENT ON COLUMN news_themes_unified.source_type IS '主题来源类型：eodhd_auto_tag | vector_retrieval | llm_classification | manual | hybrid';
COMMENT ON COLUMN news_themes_unified.confidence_score IS '置信度分数（来自原始来源）';
COMMENT ON COLUMN news_themes_unified.quality_score IS '质量分数（由融合算法计算）';
COMMENT ON COLUMN news_themes_unified.metadata IS 'JSONB格式的额外元数据，如：{"match_rank": 1, "similarity": 0.85}';


-- ============================================================
-- 3. 创建主题聚合统计表（用于性能优化）
-- ============================================================
CREATE TABLE IF NOT EXISTS theme_aggregation_stats (
    id SERIAL PRIMARY KEY,
    
    theme_name VARCHAR(255) NOT NULL,
    canonical_theme_name VARCHAR(255),
    
    -- 统计周期
    period_type VARCHAR(20) NOT NULL,  -- 'daily', 'weekly', 'monthly', 'all_time'
    period_start DATE NOT NULL,
    period_end DATE NOT NULL,
    
    -- 统计数据
    news_count INTEGER DEFAULT 0,  -- 新闻数量
    avg_confidence NUMERIC(5,4),
    avg_quality NUMERIC(5,4),
    
    -- 来源分布
    source_distribution JSONB DEFAULT '{}'::jsonb,  -- {"eodhd": 10, "vector": 8, "llm": 5}
    
    -- 市场分布
    market_distribution JSONB DEFAULT '{}'::jsonb,  -- {"US": 15, "HK": 5, "CN": 3}
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(theme_name, period_type, period_start)
);

CREATE INDEX IF NOT EXISTS idx_theme_agg_name ON theme_aggregation_stats(theme_name);
CREATE INDEX IF NOT EXISTS idx_theme_agg_period ON theme_aggregation_stats(period_type, period_start);
CREATE INDEX IF NOT EXISTS idx_theme_agg_count ON theme_aggregation_stats(news_count DESC);

COMMENT ON TABLE theme_aggregation_stats IS '主题聚合统计表 - 用于Dashboard和性能优化';


-- ============================================================
-- 4. 创建视图：每条新闻的最佳主题（融合多源）
-- ============================================================
CREATE OR REPLACE VIEW v_news_best_themes AS
SELECT 
    ntu.news_id,
    ntu.canonical_theme_name as theme_name,
    ntu.theme_master_id,
    
    -- 聚合多源信息
    MAX(ntu.confidence_score) as max_confidence,
    AVG(ntu.confidence_score) as avg_confidence,
    MAX(ntu.quality_score) as max_quality,
    AVG(ntu.quality_score) as avg_quality,
    
    -- 来源统计
    COUNT(DISTINCT ntu.source_type) as source_count,
    ARRAY_AGG(DISTINCT ntu.source_type ORDER BY ntu.source_type) as sources,
    
    -- 市场信息
    ARRAY_AGG(DISTINCT ntu.market) FILTER (WHERE ntu.market IS NOT NULL) as markets,
    
    -- 是否被验证
    BOOL_OR(ntu.is_verified) as is_verified,
    BOOL_OR(ntu.is_primary) as is_primary,
    
    -- 综合得分（用于排序）
    (
        AVG(ntu.confidence_score) * 0.4 +  -- 平均置信度40%
        AVG(COALESCE(ntu.quality_score, ntu.confidence_score)) * 0.3 +  -- 质量分数30%
        (COUNT(DISTINCT ntu.source_type)::FLOAT / 4.0) * 0.2 +  -- 多源一致性20%
        CASE WHEN BOOL_OR(ntu.is_verified) THEN 0.1 ELSE 0 END  -- 验证加分10%
    ) as fusion_score,
    
    MAX(ntu.updated_at) as last_updated
FROM news_themes_unified ntu
WHERE ntu.is_active = true
  AND ntu.canonical_theme_name IS NOT NULL
GROUP BY ntu.news_id, ntu.canonical_theme_name, ntu.theme_master_id
HAVING COUNT(DISTINCT ntu.source_type) >= 1  -- 至少1个来源
ORDER BY ntu.news_id, fusion_score DESC;

COMMENT ON VIEW v_news_best_themes IS '新闻最佳主题视图 - 融合多源数据，计算综合得分';


-- ============================================================
-- 5. 创建视图：新闻主题全景（包含所有来源明细）
-- ============================================================
CREATE OR REPLACE VIEW v_news_themes_full AS
SELECT 
    n.id as news_id,
    n.title as news_title,
    n.date as news_date,
    n.source as news_source,
    n.analysis_status,
    
    ntu.id as theme_record_id,
    ntu.theme_name,
    ntu.canonical_theme_name,
    ntu.source_type,
    ntu.confidence_score,
    ntu.quality_score,
    ntu.rank_in_news,
    ntu.market,
    ntu.is_verified,
    ntu.is_primary,
    
    tmd.canonical_name as master_canonical_name,
    tmd.display_name as master_display_name,
    tmd.category as theme_category,
    tmd.sub_category as theme_sub_category,
    
    ntu.created_at as theme_created_at
FROM news n
INNER JOIN news_themes_unified ntu ON n.id = ntu.news_id
LEFT JOIN theme_master_data tmd ON ntu.theme_master_id = tmd.id
WHERE ntu.is_active = true
ORDER BY n.date DESC, ntu.news_id, ntu.quality_score DESC NULLS LAST;

COMMENT ON VIEW v_news_themes_full IS '新闻主题全景视图 - 显示所有来源的主题详情，用于分析和调试';


-- ============================================================
-- 6. 创建视图：主题热度排行榜
-- ============================================================
CREATE OR REPLACE VIEW v_theme_trending AS
WITH theme_stats AS (
    SELECT 
        ntu.canonical_theme_name,
        tmd.display_name,
        tmd.category,
        tmd.icon,
        tmd.color,
        ntu.source_type,
        COUNT(DISTINCT ntu.news_id) as source_news_count,
        COUNT(DISTINCT DATE(n.date)) as active_days,
        COUNT(DISTINCT CASE WHEN n.date >= CURRENT_TIMESTAMP - INTERVAL '7 days' THEN ntu.news_id END) as news_count_7d,
        COUNT(DISTINCT CASE WHEN n.date >= CURRENT_TIMESTAMP - INTERVAL '30 days' THEN ntu.news_id END) as news_count_30d,
        AVG(ntu.confidence_score) as avg_confidence,
        AVG(ntu.quality_score) as avg_quality,
        MAX(n.date) as latest_news_date,
        MIN(n.date) as earliest_news_date
    FROM news_themes_unified ntu
    INNER JOIN news n ON ntu.news_id = n.id
    LEFT JOIN theme_master_data tmd ON ntu.theme_master_id = tmd.id
    WHERE ntu.is_active = true
      AND ntu.canonical_theme_name IS NOT NULL
      AND n.date >= CURRENT_TIMESTAMP - INTERVAL '90 days'
    GROUP BY 
        ntu.canonical_theme_name, 
        tmd.display_name, 
        tmd.category,
        tmd.icon,
        tmd.color,
        ntu.source_type
)
SELECT 
    canonical_theme_name as theme_name,
    display_name,
    category,
    icon,
    color,
    SUM(source_news_count) as news_count,
    MAX(active_days) as active_days,
    MAX(news_count_7d) as news_count_7d,
    MAX(news_count_30d) as news_count_30d,
    AVG(avg_confidence) as avg_confidence,
    AVG(avg_quality) as avg_quality,
    JSONB_OBJECT_AGG(source_type, source_news_count) as source_distribution,
    MAX(latest_news_date) as latest_news_date,
    MIN(earliest_news_date) as earliest_news_date
FROM theme_stats
GROUP BY 
    canonical_theme_name,
    display_name,
    category,
    icon,
    color
HAVING SUM(source_news_count) >= 2
ORDER BY MAX(news_count_7d) DESC, SUM(source_news_count) DESC;

COMMENT ON VIEW v_theme_trending IS '主题热度排行榜 - 按新闻数量和时间衰减计算热度';


-- ============================================================
-- 7. 插入一些系统预定义的主题（示例）
-- ============================================================
INSERT INTO theme_master_data (canonical_name, display_name, category, sub_category, description, aliases, is_system, priority) VALUES
-- Technology
('artificial_intelligence', 'Artificial Intelligence', 'Technology', 'AI & ML', 'AI和机器学习相关技术', '["AI", "Machine Learning", "ML", "Deep Learning", "人工智能", "机器学习"]'::jsonb, true, 100),
('5g_technology', '5G Technology', 'Technology', 'Telecom', '5G网络和通信技术', '["5G", "5G Network", "5G通信", "5G网络"]'::jsonb, true, 90),
('blockchain', 'Blockchain', 'Technology', 'Crypto', '区块链和加密货币', '["Crypto", "Cryptocurrency", "Bitcoin", "区块链", "加密货币"]'::jsonb, true, 85),
('cloud_computing', 'Cloud Computing', 'Technology', 'Infrastructure', '云计算和云服务', '["Cloud", "AWS", "Azure", "GCP", "云计算", "云服务"]'::jsonb, true, 80),
('cybersecurity', 'Cybersecurity', 'Technology', 'Security', '网络安全', '["Security", "Cyber Security", "网络安全", "信息安全"]'::jsonb, true, 75),

-- Energy & Environment
('electric_vehicles', 'Electric Vehicles', 'Energy', 'Transportation', '电动汽车和新能源汽车', '["EV", "Electric Car", "新能源汽车", "电动车", "电动汽车"]'::jsonb, true, 95),
('renewable_energy', 'Renewable Energy', 'Energy', 'Clean Energy', '可再生能源', '["Solar", "Wind Energy", "Clean Energy", "可再生能源", "清洁能源"]'::jsonb, true, 85),
('battery_technology', 'Battery Technology', 'Energy', 'Storage', '电池技术和储能', '["Battery", "Energy Storage", "电池", "储能技术"]'::jsonb, true, 80),

-- Healthcare
('biotechnology', 'Biotechnology', 'Healthcare', 'BioTech', '生物技术和制药', '["Biotech", "Pharma", "生物技术", "制药"]'::jsonb, true, 90),
('medical_devices', 'Medical Devices', 'Healthcare', 'MedTech', '医疗器械和设备', '["MedTech", "Medical Equipment", "医疗器械", "医疗设备"]'::jsonb, true, 75),

-- Finance
('fintech', 'FinTech', 'Finance', 'Technology', '金融科技', '["Financial Technology", "Digital Banking", "金融科技", "数字银行"]'::jsonb, true, 85),
('digital_payment', 'Digital Payment', 'Finance', 'Payment', '数字支付', '["Mobile Payment", "E-Payment", "移动支付", "电子支付"]'::jsonb, true, 80),

-- Consumer
('e_commerce', 'E-Commerce', 'Consumer', 'Retail', '电子商务', '["Online Shopping", "电商", "电子商务", "网购"]'::jsonb, true, 80),
('streaming_media', 'Streaming Media', 'Consumer', 'Entertainment', '流媒体服务', '["Streaming", "OTT", "Video Streaming", "流媒体", "视频流"]'::jsonb, true, 75)

ON CONFLICT (canonical_name) DO NOTHING;


-- ============================================================
-- 8. 创建辅助函数：标准化主题名称
-- ============================================================
CREATE OR REPLACE FUNCTION normalize_theme_name(raw_name VARCHAR)
RETURNS VARCHAR AS $$
DECLARE
    normalized VARCHAR;
    master_record RECORD;
BEGIN
    -- 去除前后空格，转小写
    normalized := LOWER(TRIM(raw_name));
    
    -- 查找是否匹配主数据表中的canonical_name
    SELECT canonical_name INTO normalized
    FROM theme_master_data
    WHERE LOWER(canonical_name) = normalized
      AND is_active = true
    LIMIT 1;
    
    IF FOUND THEN
        RETURN normalized;
    END IF;
    
    -- 查找是否在aliases中
    FOR master_record IN 
        SELECT canonical_name, aliases 
        FROM theme_master_data 
        WHERE is_active = true
    LOOP
        IF jsonb_array_length(master_record.aliases) > 0 THEN
            -- 检查是否在aliases数组中（不区分大小写）
            IF EXISTS (
                SELECT 1 
                FROM jsonb_array_elements_text(master_record.aliases) AS alias
                WHERE LOWER(alias) = normalized
            ) THEN
                RETURN master_record.canonical_name;
            END IF;
        END IF;
    END LOOP;
    
    -- 如果没找到匹配，返回原始名称（清理后的）
    RETURN raw_name;
END;
$$ LANGUAGE plpgsql STABLE;

COMMENT ON FUNCTION normalize_theme_name IS '标准化主题名称 - 将原始名称映射到canonical_name';


-- ============================================================
-- 9. 创建辅助函数：计算主题质量分数
-- ============================================================
CREATE OR REPLACE FUNCTION calculate_theme_quality_score(
    p_news_id INTEGER,
    p_theme_name VARCHAR,
    p_source_type VARCHAR,
    p_confidence_score NUMERIC
)
RETURNS NUMERIC AS $$
DECLARE
    v_source_count INTEGER;
    v_avg_confidence NUMERIC;
    v_has_vector_match BOOLEAN;
    v_is_verified BOOLEAN;
    v_quality_score NUMERIC;
    
    -- 不同来源的基础权重
    v_source_weight NUMERIC;
BEGIN
    -- 设置来源权重
    v_source_weight := CASE p_source_type
        WHEN 'manual' THEN 1.5
        WHEN 'vector_retrieval' THEN 1.0
        WHEN 'llm_classification' THEN 0.8
        WHEN 'eodhd_auto_tag' THEN 0.6
        ELSE 0.5
    END;
    
    -- 获取该新闻该主题的来源数量
    SELECT COUNT(DISTINCT source_type)
    INTO v_source_count
    FROM news_themes_unified
    WHERE news_id = p_news_id
      AND canonical_theme_name = p_theme_name
      AND is_active = true;
    
    -- 获取平均置信度
    SELECT AVG(confidence_score)
    INTO v_avg_confidence
    FROM news_themes_unified
    WHERE news_id = p_news_id
      AND canonical_theme_name = p_theme_name
      AND is_active = true;
    
    -- 检查是否有高分vector匹配
    SELECT EXISTS (
        SELECT 1
        FROM news_themes_unified
        WHERE news_id = p_news_id
          AND canonical_theme_name = p_theme_name
          AND source_type = 'vector_retrieval'
          AND confidence_score >= 0.8
          AND is_active = true
    ) INTO v_has_vector_match;
    
    -- 检查是否被验证
    SELECT BOOL_OR(is_verified)
    INTO v_is_verified
    FROM news_themes_unified
    WHERE news_id = p_news_id
      AND canonical_theme_name = p_theme_name
      AND is_active = true;
    
    -- 计算质量分数
    v_quality_score := (
        -- 来源权重 * 置信度 (40%)
        v_source_weight * p_confidence_score * 0.4 +
        
        -- 平均置信度 (30%)
        COALESCE(v_avg_confidence, p_confidence_score) * 0.3 +
        
        -- 多源一致性加分 (20%)
        LEAST(v_source_count::NUMERIC / 3.0, 1.0) * 0.2 +
        
        -- 高质量vector匹配加分 (5%)
        CASE WHEN v_has_vector_match THEN 0.05 ELSE 0 END +
        
        -- 人工验证加分 (5%)
        CASE WHEN v_is_verified THEN 0.05 ELSE 0 END
    );
    
    -- 限制在0-1范围
    RETURN LEAST(GREATEST(v_quality_score, 0.0), 1.0);
END;
$$ LANGUAGE plpgsql STABLE;

COMMENT ON FUNCTION calculate_theme_quality_score IS '计算主题质量分数 - 综合考虑来源、置信度、一致性等因素';


-- ============================================================
-- 10. 创建触发器：自动更新canonical_theme_name
-- ============================================================
CREATE OR REPLACE FUNCTION auto_normalize_theme_name()
RETURNS TRIGGER AS $$
BEGIN
    -- 自动标准化主题名称
    NEW.canonical_theme_name := normalize_theme_name(NEW.theme_name);
    
    -- 如果有匹配的master_data，自动关联
    IF NEW.theme_master_id IS NULL THEN
        SELECT id INTO NEW.theme_master_id
        FROM theme_master_data
        WHERE canonical_name = NEW.canonical_theme_name
          AND is_active = true
        LIMIT 1;
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_auto_normalize_theme_name ON news_themes_unified;
CREATE TRIGGER trigger_auto_normalize_theme_name
    BEFORE INSERT OR UPDATE OF theme_name ON news_themes_unified
    FOR EACH ROW
    EXECUTE FUNCTION auto_normalize_theme_name();

COMMENT ON FUNCTION auto_normalize_theme_name IS '触发器函数 - 自动标准化主题名称并关联主数据';


-- ============================================================
-- 完成
-- ============================================================
-- 输出创建的对象统计
DO $$
BEGIN
    RAISE NOTICE '==============================================';
    RAISE NOTICE 'Unified Theme System Migration Completed!';
    RAISE NOTICE '==============================================';
    RAISE NOTICE 'Created Tables:';
    RAISE NOTICE '  - theme_master_data';
    RAISE NOTICE '  - news_themes_unified';
    RAISE NOTICE '  - theme_aggregation_stats';
    RAISE NOTICE '';
    RAISE NOTICE 'Created Views:';
    RAISE NOTICE '  - v_news_best_themes';
    RAISE NOTICE '  - v_news_themes_full';
    RAISE NOTICE '  - v_theme_trending';
    RAISE NOTICE '';
    RAISE NOTICE 'Created Functions:';
    RAISE NOTICE '  - normalize_theme_name()';
    RAISE NOTICE '  - calculate_theme_quality_score()';
    RAISE NOTICE '  - auto_normalize_theme_name()';
    RAISE NOTICE '';
    RAISE NOTICE 'Inserted sample data:';
    RAISE NOTICE '  - 14 system predefined themes';
    RAISE NOTICE '==============================================';
END $$;

