-- The Close Report 存储表
-- Bloomberg "The Close" 节目每日 PM Brief 独立报告（自 Daily Market Report 的
-- Close Brief 标签抽取）。全局单例（无 user_id 维度），每天一份，内容渲染自
-- the_close_summaries（report_date 唯一键）。

CREATE TABLE IF NOT EXISTS close_brief_reports (
    id            SERIAL PRIMARY KEY,
    report_date   DATE NOT NULL,
    status        VARCHAR(20) NOT NULL DEFAULT 'generating',  -- generating / completed / failed
    file_path     VARCHAR(500),               -- 本地文件路径 (reports/close_brief/YYYYMMDD.html)
    html_content  TEXT,                        -- 自包含 HTML（用于直接从 DB 提供服务）
    metadata      JSONB DEFAULT '{}',         -- 生成统计（video 信息、转录字数、耗时等）
    error_message TEXT,
    share_token   VARCHAR(64),                 -- 公共分享 token
    created_at    TIMESTAMPTZ DEFAULT NOW(),
    updated_at    TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT uq_close_brief_report_date UNIQUE (report_date)
);

CREATE INDEX IF NOT EXISTS idx_close_brief_reports_date ON close_brief_reports(report_date DESC);
CREATE INDEX IF NOT EXISTS idx_close_brief_reports_token ON close_brief_reports(share_token) WHERE share_token IS NOT NULL;
