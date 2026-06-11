-- The Close Summary — Bloomberg "The Close" 节目每日 PM Brief 摘要
-- 对应 Daily Market Report Home Tab（架构文档 §3）

CREATE TABLE IF NOT EXISTS the_close_summaries (
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
    CONSTRAINT uq_the_close_date UNIQUE (report_date)
);

COMMENT ON TABLE the_close_summaries IS 'Bloomberg The Close 每日 PM Brief 摘要';
COMMENT ON COLUMN the_close_summaries.report_date IS '美东交易日日期';
COMMENT ON COLUMN the_close_summaries.video_id IS 'YouTube 视频 ID（subscription_content.content_id）';
COMMENT ON COLUMN the_close_summaries.summary_content IS 'LLM 生成的八段式 Markdown 摘要';
COMMENT ON COLUMN the_close_summaries.status IS '状态: generating / completed / failed';
