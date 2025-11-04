-- =====================================================
-- IPO Calendar 数据库迁移脚本
-- 创建日期: 2025-11-04
-- 描述: 创建 IPO 日历数据相关表
-- =====================================================

-- 1. IPO 日历表
CREATE TABLE IF NOT EXISTS ipo_calendar (
    id SERIAL PRIMARY KEY,
    
    -- 基本信息
    code VARCHAR(50) NOT NULL,  -- 股票代码，如: MEHA.US, 2630.HK
    name VARCHAR(500) NOT NULL,  -- 公司名称
    exchange VARCHAR(50) NOT NULL,  -- 交易所，如: NASDAQ, NYSE, HKSE
    currency VARCHAR(10),  -- 货币，如: USD, HKD
    
    -- 日期信息
    start_date DATE NOT NULL,  -- IPO 开始日期
    filing_date DATE,  -- 申报日期
    amended_date DATE,  -- 修正日期
    
    -- 价格信息
    price_from DECIMAL(12, 2),  -- 价格下限
    price_to DECIMAL(12, 2),  -- 价格上限
    offer_price DECIMAL(12, 2),  -- 发行价
    
    -- 股票信息
    shares BIGINT,  -- 股票数量
    
    -- 交易类型
    deal_type VARCHAR(50),  -- 交易类型，如: Expected, Priced, Postponed, Withdrawn
    
    -- 元数据
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- 唯一约束：同一股票代码和开始日期应该是唯一的
    CONSTRAINT unique_ipo_code_date UNIQUE(code, start_date)
);

-- 创建索引以提高查询性能
CREATE INDEX IF NOT EXISTS idx_ipo_calendar_code ON ipo_calendar(code);
CREATE INDEX IF NOT EXISTS idx_ipo_calendar_exchange ON ipo_calendar(exchange);
CREATE INDEX IF NOT EXISTS idx_ipo_calendar_start_date ON ipo_calendar(start_date);
CREATE INDEX IF NOT EXISTS idx_ipo_calendar_deal_type ON ipo_calendar(deal_type);
CREATE INDEX IF NOT EXISTS idx_ipo_calendar_created_at ON ipo_calendar(created_at);

-- 添加表注释
COMMENT ON TABLE ipo_calendar IS 'IPO 日历数据表 - 存储从 EODHD API 获取的 IPO 日历数据';
COMMENT ON COLUMN ipo_calendar.code IS '股票代码（EODHD 格式，如: MEHA.US, 2630.HK）';
COMMENT ON COLUMN ipo_calendar.name IS '公司名称';
COMMENT ON COLUMN ipo_calendar.exchange IS '交易所代码（NASDAQ, NYSE, AMEX, HKSE 等）';
COMMENT ON COLUMN ipo_calendar.currency IS '货币代码（USD, HKD 等）';
COMMENT ON COLUMN ipo_calendar.start_date IS 'IPO 开始日期';
COMMENT ON COLUMN ipo_calendar.filing_date IS '申报日期';
COMMENT ON COLUMN ipo_calendar.amended_date IS '修正日期';
COMMENT ON COLUMN ipo_calendar.price_from IS '价格下限';
COMMENT ON COLUMN ipo_calendar.price_to IS '价格上限';
COMMENT ON COLUMN ipo_calendar.offer_price IS '发行价';
COMMENT ON COLUMN ipo_calendar.shares IS '股票数量';
COMMENT ON COLUMN ipo_calendar.deal_type IS '交易类型（Expected, Priced, Postponed, Withdrawn 等）';

