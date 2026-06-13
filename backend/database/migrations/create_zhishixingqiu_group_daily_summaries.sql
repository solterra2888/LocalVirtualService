-- =====================================================
-- 知识星球星球每日摘要 — Phase 1 缓存表
-- =====================================================
-- 说明: 按星球(group_id)预生成当日话题摘要，同一星球同一天只调一次 LLM，
--       最终每日摘要由 Phase 2 机械拼接各星球摘要生成。

CREATE TABLE IF NOT EXISTS zhishixingqiu_group_daily_summaries (
    id SERIAL PRIMARY KEY,
    group_id VARCHAR(255) NOT NULL,
    group_name VARCHAR(255),
    summary_date DATE NOT NULL,

    summary_content TEXT NOT NULL DEFAULT '',
    topic_count INTEGER DEFAULT 0,

    metadata JSONB DEFAULT '{}'::jsonb,
    status VARCHAR(20) DEFAULT 'pending',
    error_message TEXT,

    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT unique_zsxq_group_summary_date UNIQUE (group_id, summary_date)
);

CREATE INDEX IF NOT EXISTS idx_zsxq_gds_date_status
    ON zhishixingqiu_group_daily_summaries (summary_date, status);
CREATE INDEX IF NOT EXISTS idx_zsxq_gds_group_id
    ON zhishixingqiu_group_daily_summaries (group_id);

COMMENT ON TABLE zhishixingqiu_group_daily_summaries IS '知识星球星球每日摘要（Phase 1 缓存，全局共享）';
COMMENT ON COLUMN zhishixingqiu_group_daily_summaries.group_id IS '星球 group_id（= subscription_content.author_identifier）';
COMMENT ON COLUMN zhishixingqiu_group_daily_summaries.summary_content IS 'Phase 1 LLM 生成的单星球摘要';
COMMENT ON COLUMN zhishixingqiu_group_daily_summaries.topic_count IS '该星球当日纳入摘要的话题数';
COMMENT ON COLUMN zhishixingqiu_group_daily_summaries.status IS '状态: pending, generating, completed, failed';
