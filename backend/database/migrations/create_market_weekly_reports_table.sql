-- =====================================================
-- Market Weekly Report 系统 - 数据库 Schema
-- =====================================================
-- 创建时间: 2026-06-06
-- 说明: 记录每次为用户生成的静态 HTML Market Weekly Report（上传至 OSS）

CREATE TABLE IF NOT EXISTS market_weekly_reports (
    id SERIAL PRIMARY KEY,

    -- 用户标识
    user_id VARCHAR(100) NOT NULL,

    -- 报告周期（两个周五）
    week_start DATE NOT NULL,
    week_end DATE NOT NULL,

    -- OSS 对象名称（不存 URL，运行时通过 OSSService.get_signed_url 生成）
    oss_object_name TEXT,

    -- 分享 token（用于公开访问）
    share_token VARCHAR(32),

    -- 报告统计元数据
    metadata JSONB DEFAULT '{}'::jsonb,

    -- 状态: pending | generating | completed | failed
    status VARCHAR(20) DEFAULT 'pending',
    error_message TEXT,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT unique_market_weekly_report_user_week UNIQUE(user_id, week_end)
);

CREATE INDEX IF NOT EXISTS idx_market_weekly_reports_user_week ON market_weekly_reports(user_id, week_end DESC);
CREATE INDEX IF NOT EXISTS idx_market_weekly_reports_status ON market_weekly_reports(status);
CREATE INDEX IF NOT EXISTS idx_market_weekly_reports_week_end ON market_weekly_reports(week_end DESC);
CREATE INDEX IF NOT EXISTS idx_market_weekly_reports_share_token ON market_weekly_reports(share_token);

COMMENT ON TABLE market_weekly_reports IS '每用户每周生成的 Market Weekly Report（静态 HTML 存于 OSS）';
COMMENT ON COLUMN market_weekly_reports.oss_object_name IS 'OSS 对象路径，如 reports/MarketWeekly/gh/report_20260606/report.html';
COMMENT ON COLUMN market_weekly_reports.metadata IS '结构化元数据：generation_time_seconds, week_start, week_end, as_of_date';
COMMENT ON COLUMN market_weekly_reports.share_token IS 'URL-safe token 用于公开分享链接';

-- 自动更新 updated_at
DROP TRIGGER IF EXISTS update_market_weekly_reports_updated_at ON market_weekly_reports;
CREATE TRIGGER update_market_weekly_reports_updated_at
    BEFORE UPDATE ON market_weekly_reports
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- 验证
-- \d market_weekly_reports