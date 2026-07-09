-- =====================================================
-- Substack Publication 每日摘要 — Phase 1 缓存表
-- =====================================================
-- 说明: 按 publication_url 预生成当日摘要（文章 + 动态），同一 publication 同一天只调一次 LLM，
--       最终每日摘要由 Phase 2 机械拼接各 publication 摘要生成。

CREATE TABLE IF NOT EXISTS substack_publication_daily_summaries (
    id SERIAL PRIMARY KEY,
    publication_url TEXT NOT NULL,
    publication_name TEXT,
    summary_date DATE NOT NULL,

    summary_content TEXT NOT NULL DEFAULT '',
    post_count INTEGER DEFAULT 0,
    note_count INTEGER DEFAULT 0,

    metadata JSONB DEFAULT '{}'::jsonb,
    status VARCHAR(20) DEFAULT 'pending',
    error_message TEXT,

    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT unique_substack_publication_summary_date UNIQUE (publication_url, summary_date)
);

CREATE INDEX IF NOT EXISTS idx_substack_pds_date_status
    ON substack_publication_daily_summaries (summary_date, status);
CREATE INDEX IF NOT EXISTS idx_substack_pds_publication_url
    ON substack_publication_daily_summaries (publication_url);

COMMENT ON TABLE substack_publication_daily_summaries IS 'Substack publication 每日摘要（Phase 1 缓存，全局共享）';
COMMENT ON COLUMN substack_publication_daily_summaries.publication_url IS 'publication 根 URL（= subscription_content.author_identifier）';
COMMENT ON COLUMN substack_publication_daily_summaries.summary_content IS 'Phase 1 LLM 生成的单 publication 摘要（文章+动态）';
COMMENT ON COLUMN substack_publication_daily_summaries.post_count IS '该 publication 当日纳入摘要的文章数';
COMMENT ON COLUMN substack_publication_daily_summaries.note_count IS '该 publication 当日纳入摘要的动态数';
COMMENT ON COLUMN substack_publication_daily_summaries.status IS '状态: pending, generating, completed, failed';
