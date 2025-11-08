-- =====================================================
-- Earnings Calendar 数据库迁移脚本
-- 创建日期: 2025-11-08
-- 描述: 创建财报日历数据表（优化版：添加必要的唯一索引）
-- =====================================================

-- 创建财报日历表
CREATE TABLE IF NOT EXISTS earnings_calendar (
    id SERIAL PRIMARY KEY,
    
    -- 股票基本信息
    code VARCHAR(20) NOT NULL,
    name VARCHAR(200),
    exchange VARCHAR(50),
    
    -- 财报日期信息
    report_date DATE NOT NULL,
    date_string VARCHAR(50),
    before_after_market VARCHAR(20),  -- 'Before Market', 'After Market', etc.
    
    -- 货币
    currency VARCHAR(10) DEFAULT 'USD',
    
    -- EPS 数据
    actual DECIMAL(20, 4),
    estimate DECIMAL(20, 4),
    difference DECIMAL(20, 4),
    percent DECIMAL(20, 4),
    
    -- 营收数据
    actual_revenue BIGINT,
    estimate_revenue BIGINT,
    difference_revenue BIGINT,
    percent_revenue DECIMAL(20, 4),
    
    -- 原始数据（JSON格式）
    raw_data JSONB,
    
    -- 数据源和获取时间
    data_source VARCHAR(50) DEFAULT 'eodhd',
    fetch_date DATE NOT NULL,
    
    -- 时间戳
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    -- 🚀 关键优化：唯一约束（确保 ON CONFLICT 高效执行）
    CONSTRAINT unique_earnings_code_date_fetch UNIQUE(code, report_date, fetch_date)
);

-- 🚀 关键优化：创建索引以提高查询性能
CREATE INDEX IF NOT EXISTS idx_earnings_calendar_code 
ON earnings_calendar(code);

CREATE INDEX IF NOT EXISTS idx_earnings_calendar_report_date 
ON earnings_calendar(report_date);

CREATE INDEX IF NOT EXISTS idx_earnings_calendar_fetch_date 
ON earnings_calendar(fetch_date);

CREATE INDEX IF NOT EXISTS idx_earnings_calendar_exchange 
ON earnings_calendar(exchange);

CREATE INDEX IF NOT EXISTS idx_earnings_calendar_date_range 
ON earnings_calendar(report_date, fetch_date);

-- 🚀 创建 GIN 索引用于 JSONB 查询
CREATE INDEX IF NOT EXISTS idx_earnings_calendar_raw_data_gin 
ON earnings_calendar USING GIN (raw_data);

-- 添加注释
COMMENT ON TABLE earnings_calendar IS '财报日历数据表 - 存储美股财报发布日期和预期数据';
COMMENT ON COLUMN earnings_calendar.code IS '股票代码（不含交易所后缀）';
COMMENT ON COLUMN earnings_calendar.report_date IS '财报发布日期';
COMMENT ON COLUMN earnings_calendar.before_after_market IS '发布时间：盘前或盘后';
COMMENT ON COLUMN earnings_calendar.actual IS 'EPS 实际值';
COMMENT ON COLUMN earnings_calendar.estimate IS 'EPS 预期值';
COMMENT ON COLUMN earnings_calendar.actual_revenue IS '营收实际值';
COMMENT ON COLUMN earnings_calendar.estimate_revenue IS '营收预期值';
COMMENT ON COLUMN earnings_calendar.fetch_date IS '数据获取日期';
COMMENT ON CONSTRAINT unique_earnings_code_date_fetch ON earnings_calendar IS '唯一约束：同一股票、同一财报日期、同一获取日期只能有一条记录';

-- 🎯 性能优化说明
-- 1. unique_earnings_code_date_fetch: 确保 ON CONFLICT 可以使用索引，避免全表扫描
-- 2. 多个单列索引：支持常见查询模式
-- 3. 复合索引：优化日期范围查询
-- 4. GIN 索引：支持 JSONB 字段的高效查询

