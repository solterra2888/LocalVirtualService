-- Daily Market Report 存储表
-- 每日生成一份全局报告（无 user_id 维度），存储自包含 HTML

CREATE TABLE IF NOT EXISTS daily_market_reports (
    id            SERIAL PRIMARY KEY,
    report_date   DATE NOT NULL,
    status        VARCHAR(20) NOT NULL DEFAULT 'generating',  -- generating / completed / failed
    file_path     VARCHAR(500),               -- 本地文件路径 (reports/daily/YYYYMMDD.html)
    html_content  TEXT,                        -- 自包含 HTML（用于直接从 DB 提供服务）
    metadata      JSONB DEFAULT '{}',         -- 生成统计 (ticker_counts, generation_time_ms 等)
    error_message TEXT,
    share_token   VARCHAR(64),                 -- 公共分享 token
    created_at    TIMESTAMPTZ DEFAULT NOW(),
    updated_at    TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT uq_daily_report_date UNIQUE (report_date)
);

CREATE INDEX IF NOT EXISTS idx_daily_market_reports_date ON daily_market_reports(report_date DESC);
CREATE INDEX IF NOT EXISTS idx_daily_market_reports_token ON daily_market_reports(share_token) WHERE share_token IS NOT NULL;
