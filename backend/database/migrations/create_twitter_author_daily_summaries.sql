-- =====================================================
-- Twitter Author Daily Summaries - 全局作者摘要缓存表
-- =====================================================
-- 创建时间: 2026-03-30
-- 说明: 将 Phase 1 按作者摘要从 per-user 提升为全局共享，
--       同一作者同一天只需调用一次 LLM，所有订阅该作者的用户共享结果。

CREATE TABLE IF NOT EXISTS twitter_author_daily_summaries (
    id SERIAL PRIMARY KEY,
    author_identifier VARCHAR(255) NOT NULL,
    author_name VARCHAR(255),
    summary_date DATE NOT NULL,

    summary_content TEXT NOT NULL DEFAULT '',
    tweet_count INTEGER DEFAULT 0,
    ref_map JSONB DEFAULT '{}'::jsonb,
    top_tweets JSONB DEFAULT '[]'::jsonb,

    metadata JSONB DEFAULT '{}'::jsonb,
    status VARCHAR(20) DEFAULT 'pending',
    error_message TEXT,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT unique_author_summary_date UNIQUE(author_identifier, summary_date)
);

CREATE INDEX IF NOT EXISTS idx_tads_date_status ON twitter_author_daily_summaries(summary_date, status);
CREATE INDEX IF NOT EXISTS idx_tads_author ON twitter_author_daily_summaries(author_identifier);

COMMENT ON TABLE twitter_author_daily_summaries IS '全局 Twitter 作者每日摘要（Phase 1 缓存，多用户共享）';
COMMENT ON COLUMN twitter_author_daily_summaries.summary_content IS 'Phase 1 LLM 生成的摘要文本';
COMMENT ON COLUMN twitter_author_daily_summaries.ref_map IS '该作者当日推文引用映射 {T-001: {url, content, author, time, engagement}}';
COMMENT ON COLUMN twitter_author_daily_summaries.top_tweets IS '该作者当日高互动推文列表 [{ref_id, author, engagement, content_preview, url}]';
COMMENT ON COLUMN twitter_author_daily_summaries.metadata IS '扩展元数据: generation_time_seconds, chunk_count 等';
COMMENT ON COLUMN twitter_author_daily_summaries.status IS '状态: pending, generating, completed, failed';

DROP TRIGGER IF EXISTS update_twitter_author_daily_summaries_updated_at ON twitter_author_daily_summaries;
CREATE TRIGGER update_twitter_author_daily_summaries_updated_at
    BEFORE UPDATE ON twitter_author_daily_summaries
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- =====================================================
-- 验证
-- =====================================================
-- \d twitter_author_daily_summaries
