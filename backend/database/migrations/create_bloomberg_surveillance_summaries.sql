-- Bloomberg Surveillance Summary — Bloomberg "Surveillance" 节目每日 AM Brief 摘要
-- 参照 the_close_summaries / cnbc_closing_bell_summaries 表结构，
-- 筛选 Bloomberg Television 频道的 Surveillance 节目转录（早间节目，同日可多段合并）

CREATE TABLE IF NOT EXISTS bloomberg_surveillance_summaries (
    id                    SERIAL PRIMARY KEY,
    report_date           DATE NOT NULL,                         -- 北京自然日（与 DMR 口径一致）
    video_id              VARCHAR(255),                          -- YouTube video_id
    video_title           TEXT,                                  -- 视频标题（多段时为合并标题）
    video_url             TEXT,                                  -- YouTube 链接
    transcript_word_count INTEGER,                               -- 转录字数（英文按空格分词）
    status                VARCHAR(20) NOT NULL DEFAULT 'generating',  -- generating / completed / failed
    summary_content       TEXT,                                  -- LLM 生成的 Markdown 摘要（八段式 AM Brief）
    metadata              JSONB DEFAULT '{}',                    -- 生成统计等
    error_message         TEXT,
    created_at            TIMESTAMPTZ DEFAULT NOW(),
    updated_at            TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT uq_bloomberg_surveillance_date UNIQUE (report_date)
);

COMMENT ON TABLE bloomberg_surveillance_summaries IS 'Bloomberg Surveillance 每日 AM Brief 摘要';
COMMENT ON COLUMN bloomberg_surveillance_summaries.report_date IS '北京自然日日期（与 Daily Market Report 口径一致）';
COMMENT ON COLUMN bloomberg_surveillance_summaries.video_id IS 'YouTube 视频 ID（subscription_content.content_id）';
COMMENT ON COLUMN bloomberg_surveillance_summaries.summary_content IS 'LLM 生成的八段式 Markdown AM Brief 摘要';
COMMENT ON COLUMN bloomberg_surveillance_summaries.status IS '状态: generating / completed / failed';
