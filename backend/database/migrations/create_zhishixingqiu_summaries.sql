-- 知识星球每日摘要 — 全局单份，按星球(group)聚合当天话题观点
-- 对应 Scheduler task_type: zhishixingqiu_summary

CREATE TABLE IF NOT EXISTS zhishixingqiu_summaries (
    id                    SERIAL PRIMARY KEY,
    report_date           DATE NOT NULL,                         -- 北京自然日
    topic_count           INTEGER,                               -- 当日话题总数
    group_count           INTEGER,                               -- 当日有更新的星球数
    status                VARCHAR(20) NOT NULL DEFAULT 'generating',  -- generating / completed / failed
    summary_content       TEXT,                                  -- LLM 生成的 Markdown 摘要
    metadata              JSONB DEFAULT '{}',                    -- 生成统计、各星球话题数等
    error_message         TEXT,
    created_at            TIMESTAMPTZ DEFAULT NOW(),
    updated_at            TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT uq_zhishixingqiu_date UNIQUE (report_date)
);

COMMENT ON TABLE zhishixingqiu_summaries IS '知识星球每日摘要（全局单份，按星球分组）';
COMMENT ON COLUMN zhishixingqiu_summaries.report_date IS '报告日期（北京自然日）';
COMMENT ON COLUMN zhishixingqiu_summaries.topic_count IS '当日纳入摘要的话题总数';
COMMENT ON COLUMN zhishixingqiu_summaries.group_count IS '当日有话题更新的星球数量';
COMMENT ON COLUMN zhishixingqiu_summaries.summary_content IS 'LLM 生成的 Markdown 摘要';
COMMENT ON COLUMN zhishixingqiu_summaries.status IS '状态: generating / completed / failed';
