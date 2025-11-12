-- =====================================================
-- 创建主题-公司关联表
-- 创建日期: 2025-11-11
-- 描述: 支持用户自定义投资主题与股票代码的关联
-- =====================================================
-- 
-- 功能说明：
-- 1. 建立 investment_theme_embeddings 与 companies 的多对多关联
-- 2. 支持用户在创建主题时指定相关股票代码
-- 3. 提供灵活的关联权重和来源标记
--
-- 使用方法:
--   psql -h 111.230.48.119 -U postgres -d rc_database -f create_theme_company_associations.sql
-- =====================================================

BEGIN;

-- ============================================================
-- 创建主题-公司关联表
-- ============================================================

CREATE TABLE IF NOT EXISTS theme_company_associations (
    id SERIAL PRIMARY KEY,
    
    -- 主题信息
    theme_id VARCHAR(255) NOT NULL,  -- 对应 investment_theme_embeddings.theme_id
    
    -- 股票信息
    ticker VARCHAR(50) NOT NULL,     -- 完整 ticker (例如 ABSI.US, 0700.HK)
    market VARCHAR(10) NOT NULL,     -- 市场代码: US, HK, CN
    
    -- 关联元数据
    association_weight DECIMAL(3,2) DEFAULT 1.0 CHECK (association_weight >= 0.0 AND association_weight <= 1.0),
    source VARCHAR(50) DEFAULT 'manual',  -- manual, auto_sector, auto_news, import
    
    -- 验证状态
    is_verified BOOLEAN DEFAULT FALSE,  -- ticker 是否在 companies 表中验证通过
    company_name VARCHAR(255),           -- 缓存的公司名称（来自 companies 表）
    
    -- 审计字段
    created_by VARCHAR(100),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    
    -- 唯一约束：一个主题不能重复关联同一个 ticker
    UNIQUE(theme_id, ticker)
);

-- 添加表注释
COMMENT ON TABLE theme_company_associations IS 
'主题-公司关联表：记录用户自定义投资主题与股票代码的关联关系，支持多对多映射';

-- 添加列注释
COMMENT ON COLUMN theme_company_associations.theme_id IS 
'主题ID，外键关联到 investment_theme_embeddings.theme_id';

COMMENT ON COLUMN theme_company_associations.ticker IS 
'完整股票代码，格式：SYMBOL.MARKET（例如：NVDA.US, 0700.HK）';

COMMENT ON COLUMN theme_company_associations.market IS 
'市场代码：US（美股）, HK（港股）, CN（A股）';

COMMENT ON COLUMN theme_company_associations.association_weight IS 
'关联权重（0.0-1.0），用于表示该股票与主题的相关强度，默认 1.0';

COMMENT ON COLUMN theme_company_associations.source IS 
'关联来源：manual（手动创建）, auto_sector（基于行业自动）, auto_news（基于新闻自动）, import（批量导入）';

COMMENT ON COLUMN theme_company_associations.is_verified IS 
'验证标记：TRUE 表示 ticker 在 companies 表中存在且已验证，FALSE 表示未验证或不存在';

COMMENT ON COLUMN theme_company_associations.company_name IS 
'公司名称缓存，从 companies 表获取，便于快速查询';

-- ============================================================
-- 创建索引
-- ============================================================

-- 主题ID索引（查询某主题关联的所有公司）
CREATE INDEX idx_tca_theme_id ON theme_company_associations(theme_id);

-- Ticker索引（查询某股票关联的所有主题）
CREATE INDEX idx_tca_ticker ON theme_company_associations(ticker);

-- 市场索引（按市场过滤）
CREATE INDEX idx_tca_market ON theme_company_associations(market);

-- 来源索引（按来源统计）
CREATE INDEX idx_tca_source ON theme_company_associations(source);

-- 验证状态索引（查询未验证的关联）
CREATE INDEX idx_tca_is_verified ON theme_company_associations(is_verified);

-- 创建时间索引（按时间排序）
CREATE INDEX idx_tca_created_at ON theme_company_associations(created_at);

-- 复合索引：主题+市场（常用查询组合）
CREATE INDEX idx_tca_theme_market ON theme_company_associations(theme_id, market);

-- 复合索引：ticker+验证状态（用于数据清洗）
CREATE INDEX idx_tca_ticker_verified ON theme_company_associations(ticker, is_verified);

-- ============================================================
-- 创建更新时间触发器
-- ============================================================

-- 创建触发器函数
CREATE OR REPLACE FUNCTION update_theme_company_associations_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 创建触发器
DROP TRIGGER IF EXISTS trigger_update_tca_timestamp ON theme_company_associations;
CREATE TRIGGER trigger_update_tca_timestamp
    BEFORE UPDATE ON theme_company_associations
    FOR EACH ROW
    EXECUTE FUNCTION update_theme_company_associations_updated_at();

COMMENT ON TRIGGER trigger_update_tca_timestamp ON theme_company_associations IS 
'自动更新 updated_at 时间戳';

-- ============================================================
-- 创建外键约束（可选，取决于数据完整性需求）
-- ============================================================

-- 注意：如果 investment_theme_embeddings.theme_id 不是主键或唯一键，
-- 需要先添加唯一约束才能创建外键

-- 检查 theme_id 是否已有唯一约束
DO $$
BEGIN
    -- 尝试为 investment_theme_embeddings.theme_id 添加唯一约束（如果不存在）
    IF NOT EXISTS (
        SELECT 1 
        FROM pg_constraint 
        WHERE conname = 'investment_theme_embeddings_theme_id_key'
    ) THEN
        ALTER TABLE investment_theme_embeddings 
        ADD CONSTRAINT investment_theme_embeddings_theme_id_key UNIQUE (theme_id);
        
        RAISE NOTICE '✅ 为 investment_theme_embeddings.theme_id 添加唯一约束';
    END IF;
    
    -- 添加外键约束
    IF NOT EXISTS (
        SELECT 1 
        FROM pg_constraint 
        WHERE conname = 'fk_tca_theme_id'
    ) THEN
        ALTER TABLE theme_company_associations
        ADD CONSTRAINT fk_tca_theme_id 
        FOREIGN KEY (theme_id) 
        REFERENCES investment_theme_embeddings(theme_id)
        ON DELETE CASCADE;  -- 删除主题时，自动删除关联
        
        RAISE NOTICE '✅ 添加外键约束 fk_tca_theme_id';
    END IF;
END $$;

COMMIT;

-- ============================================================
-- 验证
-- ============================================================

-- 查看表结构
SELECT 
    column_name, 
    data_type, 
    character_maximum_length,
    is_nullable,
    column_default
FROM information_schema.columns
WHERE table_name = 'theme_company_associations'
ORDER BY ordinal_position;

-- 查看索引
SELECT 
    indexname, 
    indexdef
FROM pg_indexes
WHERE tablename = 'theme_company_associations'
ORDER BY indexname;

-- 查看触发器
SELECT 
    trigger_name,
    event_manipulation,
    action_timing,
    action_statement
FROM information_schema.triggers
WHERE event_object_table = 'theme_company_associations';

-- 查看外键约束
SELECT
    conname AS constraint_name,
    contype AS constraint_type,
    pg_get_constraintdef(oid) AS constraint_definition
FROM pg_constraint
WHERE conrelid = 'theme_company_associations'::regclass
ORDER BY conname;

-- ============================================================
-- 示例数据（测试用）
-- ============================================================

-- 插入测试数据（可选）
/*
INSERT INTO theme_company_associations (
    theme_id, ticker, market, association_weight, source, 
    is_verified, company_name, created_by
) VALUES 
    ('AI_CHIPS_001', 'NVDA.US', 'US', 1.0, 'manual', TRUE, 'NVIDIA Corporation', 'admin'),
    ('AI_CHIPS_001', 'AMD.US', 'US', 0.9, 'manual', TRUE, 'Advanced Micro Devices', 'admin'),
    ('AI_CHIPS_001', 'INTC.US', 'US', 0.85, 'manual', TRUE, 'Intel Corporation', 'admin'),
    ('CLEAN_ENERGY_001', 'TSLA.US', 'US', 1.0, 'manual', TRUE, 'Tesla Inc', 'admin'),
    ('CLEAN_ENERGY_001', 'ENPH.US', 'US', 0.9, 'manual', TRUE, 'Enphase Energy', 'admin');

-- 验证插入
SELECT * FROM theme_company_associations ORDER BY created_at DESC;
*/

-- ============================================================
-- 使用说明
-- ============================================================

/*
## 常用查询示例

-- 1. 查询某主题关联的所有公司
SELECT ticker, company_name, association_weight, is_verified
FROM theme_company_associations
WHERE theme_id = 'AI_CHIPS_001'
ORDER BY association_weight DESC;

-- 2. 查询某股票关联的所有主题
SELECT tca.theme_id, ite.theme_name, ite.category, tca.association_weight
FROM theme_company_associations tca
JOIN investment_theme_embeddings ite ON tca.theme_id = ite.theme_id
WHERE tca.ticker = 'NVDA.US'
ORDER BY tca.association_weight DESC;

-- 3. 查询未验证的关联（需要补充公司信息）
SELECT theme_id, ticker, market, created_at
FROM theme_company_associations
WHERE is_verified = FALSE
ORDER BY created_at DESC;

-- 4. 统计各主题关联的公司数量
SELECT 
    tca.theme_id,
    ite.theme_name,
    COUNT(*) as company_count,
    AVG(tca.association_weight) as avg_weight
FROM theme_company_associations tca
JOIN investment_theme_embeddings ite ON tca.theme_id = ite.theme_id
GROUP BY tca.theme_id, ite.theme_name
ORDER BY company_count DESC;

-- 5. 按市场统计关联数量
SELECT 
    market,
    COUNT(*) as association_count,
    COUNT(DISTINCT theme_id) as unique_themes,
    COUNT(DISTINCT ticker) as unique_tickers
FROM theme_company_associations
GROUP BY market
ORDER BY association_count DESC;
*/

