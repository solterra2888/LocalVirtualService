-- =====================================================
-- YouTube Author Daily Summaries - 全局频道摘要缓存表
-- =====================================================
-- 创建时间: 2026-03-31
-- 说明: 将 Phase 1 按 YouTube 频道摘要提升为全局共享，
--       同一频道 + 同一天只调用一次 LLM，所有订阅该频道的用户共享结果。

CREATE TABLE IF NOT EXISTS youtube_author_daily_summaries (
    id SERIAL PRIMARY KEY,
    author_identifier VARCHAR(255) NOT NULL,
    author_name VARCHAR(255),
    summary_date DATE NOT NULL,

    summary_content TEXT NOT NULL DEFAULT '',
    video_count INTEGER DEFAULT 0,
    ref_map JSONB DEFAULT '{}'::jsonb,
    top_videos JSONB DEFAULT '[]'::jsonb,

    metadata JSONB DEFAULT '{}'::jsonb,
    status VARCHAR(20) DEFAULT 'pending',
    error_message TEXT,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT unique_yt_author_summary_date UNIQUE(author_identifier, summary_date)
);

CREATE INDEX IF NOT EXISTS idx_ytads_date_status ON youtube_author_daily_summaries(summary_date, status);
CREATE INDEX IF NOT EXISTS idx_ytads_author ON youtube_author_daily_summaries(author_identifier);

COMMENT ON TABLE youtube_author_daily_summaries IS '全局 YouTube 频道每日摘要（Phase 1 缓存，多用户共享）';
COMMENT ON COLUMN youtube_author_daily_summaries.author_identifier IS 'YouTube 频道 ID（UC...）';
COMMENT ON COLUMN youtube_author_daily_summaries.summary_content IS 'Phase 1 LLM 生成的摘要文本';
COMMENT ON COLUMN youtube_author_daily_summaries.ref_map IS '该频道当日视频引用映射 {V-001: {url, title, author, has_transcript, ...}}';
COMMENT ON COLUMN youtube_author_daily_summaries.top_videos IS '该频道当日高互动视频列表 [{ref_id, author, views, engagement, title, url}]';
COMMENT ON COLUMN youtube_author_daily_summaries.metadata IS '扩展元数据: generation_time_seconds, chunk_count, videos_with_transcript 等';
COMMENT ON COLUMN youtube_author_daily_summaries.status IS '状态: pending, generating, completed, failed';

DROP TRIGGER IF EXISTS update_youtube_author_daily_summaries_updated_at ON youtube_author_daily_summaries;
CREATE TRIGGER update_youtube_author_daily_summaries_updated_at
    BEFORE UPDATE ON youtube_author_daily_summaries
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- =====================================================
-- 验证
-- =====================================================
-- \d youtube_author_daily_summaries
