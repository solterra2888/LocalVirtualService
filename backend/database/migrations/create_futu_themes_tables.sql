-- =====================================================
-- Futu Investment Themes 数据库迁移脚本
-- 创建日期: 2025-10-19
-- 描述: 创建富途投资主题和股票数据相关表
-- =====================================================

-- 1. 富途投资主题表
CREATE TABLE IF NOT EXISTS futu_investment_themes (
    id SERIAL PRIMARY KEY,
    
    -- 基本信息
    theme_id VARCHAR(50) NOT NULL,  -- 如: BK1288
    theme_name VARCHAR(200) NOT NULL,
    market VARCHAR(10) NOT NULL,  -- HK, US
    theme_url VARCHAR(500),
    
    -- 市场表现
    change_percent VARCHAR(20),  -- 如: +8.61%
    gainers VARCHAR(20),  -- 上涨股票数
    losers VARCHAR(20),   -- 下跌股票数
    unchanged VARCHAR(20), -- 平盘股票数
    
    -- 领涨股信息
    top_stock VARCHAR(200),
    top_stock_change VARCHAR(200),
    
    -- 股票数量
    stock_count INTEGER DEFAULT 0,
    
    -- 爬取时间
    scraped_at TIMESTAMP NOT NULL,
    
    -- 元数据
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- 唯一约束：同一主题ID、市场和爬取日期应该是唯一的
    CONSTRAINT unique_theme_market_date UNIQUE(theme_id, market, scraped_at)
);

-- 2. 富途主题股票表
CREATE TABLE IF NOT EXISTS futu_theme_stocks (
    id SERIAL PRIMARY KEY,
    
    -- 关联主题
    theme_id INTEGER REFERENCES futu_investment_themes(id) ON DELETE CASCADE,
    
    -- 股票信息
    symbol VARCHAR(50) NOT NULL,
    stock_name VARCHAR(200) NOT NULL,
    stock_url VARCHAR(500),
    market VARCHAR(10) NOT NULL,  -- HK, US
    
    -- 元数据
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- 索引
    CONSTRAINT fk_theme FOREIGN KEY (theme_id) REFERENCES futu_investment_themes(id) ON DELETE CASCADE
);

-- 创建索引以提高查询性能
CREATE INDEX IF NOT EXISTS idx_futu_themes_market ON futu_investment_themes(market);
CREATE INDEX IF NOT EXISTS idx_futu_themes_theme_id ON futu_investment_themes(theme_id);
CREATE INDEX IF NOT EXISTS idx_futu_themes_scraped_at ON futu_investment_themes(scraped_at);
CREATE INDEX IF NOT EXISTS idx_futu_theme_stocks_theme_id ON futu_theme_stocks(theme_id);
CREATE INDEX IF NOT EXISTS idx_futu_theme_stocks_symbol ON futu_theme_stocks(symbol);
CREATE INDEX IF NOT EXISTS idx_futu_theme_stocks_market ON futu_theme_stocks(market);

-- 创建唯一索引确保同一主题ID、市场和日期只能有一条记录
CREATE UNIQUE INDEX IF NOT EXISTS unique_theme_market_date_idx 
ON futu_investment_themes(theme_id, market, DATE(scraped_at));

-- 添加注释
COMMENT ON TABLE futu_investment_themes IS '富途投资主题数据表';
COMMENT ON TABLE futu_theme_stocks IS '富途主题关联股票数据表';

COMMENT ON COLUMN futu_investment_themes.theme_id IS '主题ID，如BK1288';
COMMENT ON COLUMN futu_investment_themes.market IS '市场代码：HK-港股, US-美股';
COMMENT ON COLUMN futu_investment_themes.change_percent IS '主题涨跌幅';
COMMENT ON COLUMN futu_investment_themes.scraped_at IS '数据爬取时间';

COMMENT ON COLUMN futu_theme_stocks.symbol IS '股票代码';
COMMENT ON COLUMN futu_theme_stocks.stock_name IS '股票名称';

