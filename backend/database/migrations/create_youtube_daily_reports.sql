-- =====================================================
-- YouTube Daily Report 系统 - 数据库 Schema
-- =====================================================
-- 创建时间: 2026-03-31
-- 说明: Scheduler 定期为每个用户生成 YouTube Daily Report

-- =====================================================
-- 1. 报告存储表
-- =====================================================
CREATE TABLE IF NOT EXISTS youtube_daily_reports (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    report_date DATE NOT NULL,

    report_content TEXT NOT NULL,

    metadata JSONB DEFAULT '{}'::jsonb,

    status VARCHAR(20) DEFAULT 'completed',
    error_message TEXT,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT unique_yt_user_report_date UNIQUE(user_id, report_date)
);

CREATE INDEX IF NOT EXISTS idx_ytdr_user_date ON youtube_daily_reports(user_id, report_date DESC);
CREATE INDEX IF NOT EXISTS idx_ytdr_report_date ON youtube_daily_reports(report_date DESC);
CREATE INDEX IF NOT EXISTS idx_ytdr_status ON youtube_daily_reports(status);

COMMENT ON TABLE youtube_daily_reports IS 'AI 生成的 YouTube Daily Report（每用户每天一份）';
COMMENT ON COLUMN youtube_daily_reports.report_content IS '报告正文（Markdown 格式）';
COMMENT ON COLUMN youtube_daily_reports.metadata IS '结构化元数据: video_count, channel_count, top_channels, generation_time_seconds, video_references[]';
COMMENT ON COLUMN youtube_daily_reports.status IS '状态: pending, generating, completed, failed';

DROP TRIGGER IF EXISTS update_youtube_daily_reports_updated_at ON youtube_daily_reports;
CREATE TRIGGER update_youtube_daily_reports_updated_at
    BEFORE UPDATE ON youtube_daily_reports
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- =====================================================
-- 验证
-- =====================================================
-- \d youtube_daily_reports
