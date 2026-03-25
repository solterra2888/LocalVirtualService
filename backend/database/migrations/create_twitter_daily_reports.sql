-- =====================================================
-- Twitter Daily Report 系统 - 数据库 Schema
-- =====================================================
-- 创建时间: 2026-03-25
-- 说明: AI Agent 定期为每个用户生成 Twitter Daily Report

-- =====================================================
-- 1. 报告存储表
-- =====================================================
CREATE TABLE IF NOT EXISTS twitter_daily_reports (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    report_date DATE NOT NULL,

    report_content TEXT NOT NULL,

    metadata JSONB DEFAULT '{}'::jsonb,

    status VARCHAR(20) DEFAULT 'completed',
    error_message TEXT,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT unique_user_report_date UNIQUE(user_id, report_date)
);

CREATE INDEX IF NOT EXISTS idx_tdr_user_date ON twitter_daily_reports(user_id, report_date DESC);
CREATE INDEX IF NOT EXISTS idx_tdr_report_date ON twitter_daily_reports(report_date DESC);
CREATE INDEX IF NOT EXISTS idx_tdr_status ON twitter_daily_reports(status);

COMMENT ON TABLE twitter_daily_reports IS 'AI 生成的 Twitter Daily Report（每用户每天一份）';
COMMENT ON COLUMN twitter_daily_reports.report_content IS '报告正文（Markdown 格式）';
COMMENT ON COLUMN twitter_daily_reports.metadata IS '结构化元数据: tweet_count, author_count, top_authors, key_topics, generation_time_seconds, llm_provider, llm_model, token_usage, total_cost, tweet_references[]';
COMMENT ON COLUMN twitter_daily_reports.status IS '状态: pending, generating, completed, failed';

-- 自动更新 updated_at
DROP TRIGGER IF EXISTS update_twitter_daily_reports_updated_at ON twitter_daily_reports;
CREATE TRIGGER update_twitter_daily_reports_updated_at
    BEFORE UPDATE ON twitter_daily_reports
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- =====================================================
-- 2. 用户报告配置表
-- =====================================================
CREATE TABLE IF NOT EXISTS twitter_report_user_config (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL UNIQUE REFERENCES users(id) ON DELETE CASCADE,

    phase1_template_code VARCHAR(100),
    phase2_template_code VARCHAR(100),

    enabled BOOLEAN DEFAULT true,
    report_language VARCHAR(10) DEFAULT 'zh',
    generation_hour INTEGER DEFAULT 6,

    focus_topics TEXT[],
    custom_instructions TEXT,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_truc_user_id ON twitter_report_user_config(user_id);

COMMENT ON TABLE twitter_report_user_config IS '用户 Twitter Daily Report 生成配置';
COMMENT ON COLUMN twitter_report_user_config.phase1_template_code IS 'Phase 1 prompt 模板代码（引用 llm_prompt_templates.template_code），NULL 则用默认';
COMMENT ON COLUMN twitter_report_user_config.phase2_template_code IS 'Phase 2 prompt 模板代码，NULL 则用默认';
COMMENT ON COLUMN twitter_report_user_config.generation_hour IS '每天生成报告的小时（CST，0-23）';
COMMENT ON COLUMN twitter_report_user_config.focus_topics IS '用户关注的重点主题，传入模板变量';
COMMENT ON COLUMN twitter_report_user_config.custom_instructions IS '用户自定义附加指令，注入到 prompt 中';

DROP TRIGGER IF EXISTS update_twitter_report_user_config_updated_at ON twitter_report_user_config;
CREATE TRIGGER update_twitter_report_user_config_updated_at
    BEFORE UPDATE ON twitter_report_user_config
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- =====================================================
-- 验证
-- =====================================================
-- \d twitter_daily_reports
-- \d twitter_report_user_config
