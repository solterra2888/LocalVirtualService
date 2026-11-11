-- Morning Brief (facts-report) 存储表
-- facts-report LLM agent 管线的每日晨报(事实/观点分离版)。全局单例(无 user_id),
-- 每天一份;由 task_scheduling(type=morning_brief) 定时触发,管线尾部
-- register_morning_brief.py 回写状态并上传 OSS。
--
-- 日期口径: report_date = 北京自然日(生成日,= 美股交易日 T + 1),
--           metadata.session_date = 美股交易日 T(facts-report 工作区目录名)。

CREATE TABLE IF NOT EXISTS morning_brief_reports (
    id              SERIAL PRIMARY KEY,
    report_date     DATE NOT NULL,
    status          VARCHAR(20) NOT NULL DEFAULT 'generating',  -- generating / completed / failed
    file_path       VARCHAR(500),               -- 本地 report.md 路径 (facts-report/<T>/final/report.md)
    oss_object_name VARCHAR(500),               -- OSS 对象键 reports/MorningBrief/report_YYYYMMDD.html
    html_content    TEXT,                        -- 自包含 HTML(富版优先;OSS 故障时 DB 兜底服务)
    metadata        JSONB DEFAULT '{}',          -- session_date/workspace/token_usage/timings/verify 等
    error_message   TEXT,
    share_token     VARCHAR(64),
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT uq_morning_brief_report_date UNIQUE (report_date)
);

CREATE INDEX IF NOT EXISTS idx_morning_brief_reports_date ON morning_brief_reports(report_date DESC);
CREATE INDEX IF NOT EXISTS idx_morning_brief_reports_token ON morning_brief_reports(share_token) WHERE share_token IS NOT NULL;
