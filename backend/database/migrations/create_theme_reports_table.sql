-- =====================================================
-- Theme Report 系统 - 数据库 Schema
-- =====================================================
-- 创建时间: 2026-06-01
-- 说明: 记录每次为用户生成的静态 HTML Theme Report（上传至 OSS）

CREATE TABLE IF NOT EXISTS theme_reports (
    id SERIAL PRIMARY KEY,

    -- 用户标识（对应 investment_theme_embeddings.created_by / theme_tags.user_id）
    user_id VARCHAR(100) NOT NULL,

    -- 报告日期（通常为生成当天）
    report_date DATE NOT NULL,

    -- OSS 对象名称（不存 URL，运行时通过 OSSService.get_signed_url 生成）
    oss_object_name TEXT,

    -- 报告统计
    theme_count INTEGER DEFAULT 0,
    category_count INTEGER DEFAULT 0,
    constituent_count INTEGER DEFAULT 0,

    -- 结构化元数据：生成耗时、各主题名等
    metadata JSONB DEFAULT '{}'::jsonb,

    -- 状态: pending | generating | completed | failed
    status VARCHAR(20) DEFAULT 'pending',
    error_message TEXT,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT unique_theme_report_user_date UNIQUE(user_id, report_date)
);

CREATE INDEX IF NOT EXISTS idx_theme_reports_user_date ON theme_reports(user_id, report_date DESC);
CREATE INDEX IF NOT EXISTS idx_theme_reports_status ON theme_reports(status);
CREATE INDEX IF NOT EXISTS idx_theme_reports_date ON theme_reports(report_date DESC);

COMMENT ON TABLE theme_reports IS '每用户每日生成的 Theme Report（Theme Details 部分），静态 HTML 存于 OSS';
COMMENT ON COLUMN theme_reports.oss_object_name IS 'OSS 对象路径，如 reports/Themes/gh/report_20260601/report_embedded.html';
COMMENT ON COLUMN theme_reports.metadata IS '结构化元数据：generation_time_seconds, theme_names[], as_of_date, price_as_of_date';

-- 自动更新 updated_at
DROP TRIGGER IF EXISTS update_theme_reports_updated_at ON theme_reports;
CREATE TRIGGER update_theme_reports_updated_at
    BEFORE UPDATE ON theme_reports
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- 验证
-- \d theme_reports
