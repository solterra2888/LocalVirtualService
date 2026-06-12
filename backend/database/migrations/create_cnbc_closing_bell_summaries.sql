-- CNBC Closing Bell Summary — CNBC "Closing Bell" 节目每日 PM Brief 摘要
-- 参照 the_close_summaries 表结构，筛选 CNBC Closing Bell 节目转录

CREATE TABLE IF NOT EXISTS cnbc_closing_bell_summaries (
    id                    SERIAL PRIMARY KEY,
    report_date           DATE NOT NULL,                         -- 美东交易日
    video_id              VARCHAR(255),                          -- YouTube video_id
    video_title           TEXT,                                  -- 视频标题
    video_url             TEXT,                                  -- YouTube 链接
    transcript_word_count INTEGER,                               -- 转录字数（英文按空格分词）
    status                VARCHAR(20) NOT NULL DEFAULT 'generating',  -- generating / completed / failed
    summary_content       TEXT,                                  -- LLM 生成的 Markdown 摘要（八段式）
    metadata              JSONB DEFAULT '{}',                    -- 生成统计等
    error_message         TEXT,
    created_at            TIMESTAMPTZ DEFAULT NOW(),
    updated_at            TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT uq_cnbc_closing_bell_date UNIQUE (report_date)
);

COMMENT ON TABLE cnbc_closing_bell_summaries IS 'CNBC Closing Bell 每日 PM Brief 摘要';
COMMENT ON COLUMN cnbc_closing_bell_summaries.report_date IS '美东交易日日期';
COMMENT ON COLUMN cnbc_closing_bell_summaries.video_id IS 'YouTube 视频 ID（subscription_content.content_id）';
COMMENT ON COLUMN cnbc_closing_bell_summaries.summary_content IS 'LLM 生成的八段式 Markdown 摘要';
COMMENT ON COLUMN cnbc_closing_bell_summaries.status IS '状态: generating / completed / failed';
