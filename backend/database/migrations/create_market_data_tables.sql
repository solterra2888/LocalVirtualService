-- ==========================================
-- 市场数据表创建脚本
-- 创建时间: 2025-11-02
-- 说明: 用于存储指数、外汇、大宗商品、债券、板块等市场数据
-- ==========================================

-- 1. 创建更新时间触发器函数（如果不存在）
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- ==========================================
-- 2. 资产元数据表
-- ==========================================
CREATE TABLE IF NOT EXISTS market_assets (
    id SERIAL PRIMARY KEY,
    code VARCHAR(50) NOT NULL UNIQUE,           -- 资产代码 (如: SPY.US, EURUSD.FOREX)
    name_en VARCHAR(100),                       -- 英文名称
    name_zh VARCHAR(100),                       -- 中文名称
    asset_type VARCHAR(20) NOT NULL,            -- 资产类型: index/forex/commodity/bond/sector
    category VARCHAR(50),                       -- 细分类别
    description TEXT,                           -- 描述
    is_active BOOLEAN DEFAULT TRUE,             -- 是否活跃
    
    -- 元数据
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 索引
CREATE INDEX IF NOT EXISTS idx_market_assets_asset_type ON market_assets(asset_type);
CREATE INDEX IF NOT EXISTS idx_market_assets_is_active ON market_assets(is_active);
CREATE INDEX IF NOT EXISTS idx_market_assets_category ON market_assets(category);

-- 更新触发器
DROP TRIGGER IF EXISTS update_market_assets_updated_at ON market_assets;
CREATE TRIGGER update_market_assets_updated_at
    BEFORE UPDATE ON market_assets
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- 添加注释
COMMENT ON TABLE market_assets IS '市场资产元数据表，包含指数、外汇、大宗商品、债券、板块等';
COMMENT ON COLUMN market_assets.code IS '资产代码，如 SPY.US, EURUSD.FOREX';
COMMENT ON COLUMN market_assets.asset_type IS '资产类型: index/forex/commodity/bond/sector';


-- ==========================================
-- 3. 实时数据表
-- ==========================================
CREATE TABLE IF NOT EXISTS market_data_realtime (
    id SERIAL PRIMARY KEY,
    code VARCHAR(50) NOT NULL UNIQUE,           -- 资产代码
    asset_type VARCHAR(20),                     -- 资产类型: index/forex/commodity/bond/sector
    asset_name VARCHAR(100),                    -- 资产名称（中文或英文）
    
    -- 价格数据
    open DECIMAL(20, 6),                        -- 开盘价
    high DECIMAL(20, 6),                        -- 最高价
    low DECIMAL(20, 6),                         -- 最低价
    close DECIMAL(20, 6),                       -- 当前价/收盘价
    volume BIGINT,                              -- 成交量
    
    -- 涨跌数据
    previous_close DECIMAL(20, 6),              -- 前一交易日收盘价
    change_amount DECIMAL(20, 6),               -- 涨跌额
    change_percent DECIMAL(10, 4),              -- 涨跌幅(%)
    
    -- 时间戳
    data_timestamp BIGINT,                      -- 数据时间戳（Unix秒）
    data_datetime TIMESTAMP WITH TIME ZONE,     -- 数据时间（转换后）
    gmtoffset INTEGER DEFAULT 0,                -- GMT偏移
    
    -- 元数据
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 索引
CREATE INDEX IF NOT EXISTS idx_market_data_realtime_code ON market_data_realtime(code);
CREATE INDEX IF NOT EXISTS idx_market_data_realtime_asset_type ON market_data_realtime(asset_type);
CREATE INDEX IF NOT EXISTS idx_market_data_realtime_updated_at ON market_data_realtime(updated_at);

-- 更新触发器
DROP TRIGGER IF EXISTS update_market_data_realtime_updated_at ON market_data_realtime;
CREATE TRIGGER update_market_data_realtime_updated_at
    BEFORE UPDATE ON market_data_realtime
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- 添加注释
COMMENT ON TABLE market_data_realtime IS '市场数据实时表，存储最新的价格数据';
COMMENT ON COLUMN market_data_realtime.code IS '资产代码';
COMMENT ON COLUMN market_data_realtime.close IS '当前价格/最新收盘价';
COMMENT ON COLUMN market_data_realtime.data_timestamp IS 'Unix时间戳（秒）';


-- ==========================================
-- 4. 历史日线数据表（EOD）
-- ==========================================
CREATE TABLE IF NOT EXISTS market_data_eod (
    id SERIAL PRIMARY KEY,
    code VARCHAR(50) NOT NULL,                  -- 资产代码
    date DATE NOT NULL,                         -- 交易日期
    asset_type VARCHAR(20),                     -- 资产类型
    
    -- 价格数据
    open DECIMAL(20, 6),                        -- 开盘价
    high DECIMAL(20, 6),                        -- 最高价
    low DECIMAL(20, 6),                         -- 最低价
    close DECIMAL(20, 6),                       -- 收盘价
    adjusted_close DECIMAL(20, 6),              -- 复权收盘价
    volume BIGINT,                              -- 成交量
    
    -- 元数据
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    -- 唯一约束
    CONSTRAINT unique_code_date UNIQUE (code, date)
);

-- 索引
CREATE INDEX IF NOT EXISTS idx_market_data_eod_code ON market_data_eod(code);
CREATE INDEX IF NOT EXISTS idx_market_data_eod_date ON market_data_eod(date);
CREATE INDEX IF NOT EXISTS idx_market_data_eod_code_date ON market_data_eod(code, date);
CREATE INDEX IF NOT EXISTS idx_market_data_eod_asset_type ON market_data_eod(asset_type);

-- 更新触发器
DROP TRIGGER IF EXISTS update_market_data_eod_updated_at ON market_data_eod;
CREATE TRIGGER update_market_data_eod_updated_at
    BEFORE UPDATE ON market_data_eod
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- 添加注释
COMMENT ON TABLE market_data_eod IS '市场数据历史日线表（End of Day）';
COMMENT ON COLUMN market_data_eod.date IS '交易日期';
COMMENT ON COLUMN market_data_eod.adjusted_close IS '复权收盘价';
COMMENT ON CONSTRAINT unique_code_date ON market_data_eod IS '确保每个资产每天只有一条记录';


-- ==========================================
-- 5. 插入初始资产数据
-- ==========================================

-- 清空现有数据（可选，首次运行时注释掉这行）
-- TRUNCATE TABLE market_assets CASCADE;

-- 指数数据
INSERT INTO market_assets (code, name_en, name_zh, asset_type, category, description) VALUES
('SPY.US', 'S&P 500', '标普500', 'index', 'us_equity', 'SPDR S&P 500 ETF Trust'),
('QQQ.US', 'Nasdaq 100', '纳斯达克100', 'index', 'us_equity', 'Invesco QQQ Trust'),
('DIA.US', 'Dow Jones', '道琼斯', 'index', 'us_equity', 'SPDR Dow Jones Industrial Average ETF'),
('FEZ.US', 'STOXX 50', '欧洲斯托克50', 'index', 'eu_equity', 'SPDR EURO STOXX 50 ETF'),
('EWG.US', 'DAX (Germany)', '德国DAX', 'index', 'eu_equity', 'iShares MSCI Germany ETF'),
('EWQ.US', 'CAC 40 (France)', '法国CAC40', 'index', 'eu_equity', 'iShares MSCI France ETF'),
('EWJ.US', 'Nikkei 225', '日经225', 'index', 'asia_equity', 'iShares MSCI Japan ETF'),
('2800.HK', 'Hang Seng', '恒生指数', 'index', 'asia_equity', 'Tracker Fund of Hong Kong')
ON CONFLICT (code) DO NOTHING;

-- 外汇数据
INSERT INTO market_assets (code, name_en, name_zh, asset_type, category, description) VALUES
('USDCNH.FOREX', 'USD/CNH', '美元/离岸人民币', 'forex', 'major_pair', 'US Dollar vs Offshore Chinese Yuan'),
('EURUSD.FOREX', 'EUR/USD', '欧元/美元', 'forex', 'major_pair', 'Euro vs US Dollar'),
('GBPUSD.FOREX', 'GBP/USD', '英镑/美元', 'forex', 'major_pair', 'British Pound vs US Dollar'),
('USDJPY.FOREX', 'USD/JPY', '美元/日元', 'forex', 'major_pair', 'US Dollar vs Japanese Yen')
ON CONFLICT (code) DO NOTHING;

-- 大宗商品数据
INSERT INTO market_assets (code, name_en, name_zh, asset_type, category, description) VALUES
('XAUUSD.FOREX', 'Gold', '黄金', 'commodity', 'precious_metal', 'Gold Spot Price vs US Dollar'),
('XAGUSD.FOREX', 'Silver', '白银', 'commodity', 'precious_metal', 'Silver Spot Price vs US Dollar'),
('HG.US', 'Copper', '铜', 'commodity', 'industrial_metal', 'Copper Futures ETF'),
('BNO.US', 'Brent Oil (BNO ETF)', '布伦特原油ETF', 'commodity', 'energy', 'United States Brent Oil Fund'),
('NG.US', 'Natural Gas', '天然气', 'commodity', 'energy', 'Natural Gas Futures ETF')
ON CONFLICT (code) DO NOTHING;

-- 债券收益率数据
INSERT INTO market_assets (code, name_en, name_zh, asset_type, category, description) VALUES
('US3M.INDX', '3-Month Treasury', '3个月美债收益率', 'bond', 'us_treasury', 'US 3-Month Treasury Yield'),
('US1Y.INDX', '1-Year Treasury', '1年期美债收益率', 'bond', 'us_treasury', 'US 1-Year Treasury Yield'),
('US5Y.INDX', '5-Year Treasury', '5年期美债收益率', 'bond', 'us_treasury', 'US 5-Year Treasury Yield'),
('US10Y.INDX', '10-Year Treasury', '10年期美债收益率', 'bond', 'us_treasury', 'US 10-Year Treasury Yield')
ON CONFLICT (code) DO NOTHING;

-- 板块数据
INSERT INTO market_assets (code, name_en, name_zh, asset_type, category, description) VALUES
('XLK.US', 'Technology', '科技板块', 'sector', 'us_sector', 'Technology Select Sector SPDR Fund'),
('XLF.US', 'Financials', '金融板块', 'sector', 'us_sector', 'Financial Select Sector SPDR Fund'),
('XLV.US', 'Health Care', '医疗板块', 'sector', 'us_sector', 'Health Care Select Sector SPDR Fund'),
('XLY.US', 'Consumer Discretionary', '非必需消费品板块', 'sector', 'us_sector', 'Consumer Discretionary Select Sector SPDR Fund'),
('XLI.US', 'Industrials', '工业板块', 'sector', 'us_sector', 'Industrial Select Sector SPDR Fund'),
('XLP.US', 'Consumer Staples', '必需消费品板块', 'sector', 'us_sector', 'Consumer Staples Select Sector SPDR Fund'),
('XLE.US', 'Energy', '能源板块', 'sector', 'us_sector', 'Energy Select Sector SPDR Fund'),
('XLU.US', 'Utilities', '公用事业板块', 'sector', 'us_sector', 'Utilities Select Sector SPDR Fund'),
('XLRE.US', 'Real Estate', '房地产板块', 'sector', 'us_sector', 'Real Estate Select Sector SPDR Fund'),
('XLB.US', 'Materials', '材料板块', 'sector', 'us_sector', 'Materials Select Sector SPDR Fund'),
('XLC.US', 'Communication Services', '通信服务板块', 'sector', 'us_sector', 'Communication Services Select Sector SPDR Fund')
ON CONFLICT (code) DO NOTHING;


-- ==========================================
-- 6. 创建有用的视图
-- ==========================================

-- 视图1: 实时数据与资产信息的联合视图
CREATE OR REPLACE VIEW v_market_data_realtime_with_info AS
SELECT 
    ma.id as asset_id,
    ma.code,
    ma.name_en,
    ma.name_zh,
    ma.asset_type,
    ma.category,
    mdr.open,
    mdr.high,
    mdr.low,
    mdr.close,
    mdr.volume,
    mdr.previous_close,
    mdr.change_amount,
    mdr.change_percent,
    mdr.data_datetime,
    mdr.updated_at
FROM market_assets ma
LEFT JOIN market_data_realtime mdr ON ma.code = mdr.code
WHERE ma.is_active = TRUE;

COMMENT ON VIEW v_market_data_realtime_with_info IS '实时数据与资产信息的联合视图';


-- 视图2: 按资产类型汇总的市场概览
CREATE OR REPLACE VIEW v_market_overview AS
SELECT 
    asset_type,
    COUNT(*) as total_assets,
    AVG(change_percent) as avg_change_percent,
    MAX(change_percent) as max_change_percent,
    MIN(change_percent) as min_change_percent,
    MAX(updated_at) as last_updated
FROM market_data_realtime
WHERE close IS NOT NULL
GROUP BY asset_type;

COMMENT ON VIEW v_market_overview IS '按资产类型汇总的市场概览';


-- ==========================================
-- 7. 查询数据示例
-- ==========================================

-- 示例1: 查看所有资产
-- SELECT * FROM market_assets ORDER BY asset_type, code;

-- 示例2: 查看实时数据
-- SELECT * FROM v_market_data_realtime_with_info ORDER BY change_percent DESC;

-- 示例3: 查看市场概览
-- SELECT * FROM v_market_overview;

-- 示例4: 查看特定资产的历史数据
-- SELECT * FROM market_data_eod 
-- WHERE code = 'SPY.US' 
-- AND date >= CURRENT_DATE - INTERVAL '30 days'
-- ORDER BY date DESC;

-- 示例5: 查看涨跌幅TOP 10
-- SELECT name_zh, code, change_percent, close, updated_at
-- FROM v_market_data_realtime_with_info
-- WHERE change_percent IS NOT NULL
-- ORDER BY change_percent DESC
-- LIMIT 10;


-- ==========================================
-- 完成
-- ==========================================
-- 脚本执行完成
-- 
-- 下一步:
-- 1. 运行数据获取脚本填充数据
-- 2. 设置定时任务定期更新数据
-- 3. 创建API接口暴露数据
-- ==========================================

