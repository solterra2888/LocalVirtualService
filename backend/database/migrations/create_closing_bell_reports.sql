-- Closing Bell Report 存储表
-- CNBC "Closing Bell" 节目每日 PM Brief 独立报告（自 Daily Market Report 的
-- Stock Pulse 标签抽取）。全局单例（无 user_id 维度），每天一份，内容渲染自
-- cnbc_closing_bell_summaries（report_date 唯一键）。

CREATE TABLE IF NOT EXISTS closing_bell_reports (
    id            SERIAL PRIMARY KEY,
    report_date   DATE NOT NULL,
    status        VARCHAR(20) NOT NULL DEFAULT 'generating',  -- generating / completed / failed
    file_path     VARCHAR(500),               -- 本地文件路径 (reports/closing_bell/YYYYMMDD.html)
    html_content  TEXT,                        -- 自包含 HTML（用于直接从 DB 提供服务）
    metadata      JSONB DEFAULT '{}',         -- 生成统计（video 信息、转录字数、耗时等）
    error_message TEXT,
    share_token   VARCHAR(64),                 -- 公共分享 token
    created_at    TIMESTAMPTZ DEFAULT NOW(),
    updated_at    TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT uq_closing_bell_report_date UNIQUE (report_date)
);

CREATE INDEX IF NOT EXISTS idx_closing_bell_reports_date ON closing_bell_reports(report_date DESC);
CREATE INDEX IF NOT EXISTS idx_closing_bell_reports_token ON closing_bell_reports(share_token) WHERE share_token IS NOT NULL;
