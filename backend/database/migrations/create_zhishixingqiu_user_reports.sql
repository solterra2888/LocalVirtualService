-- =====================================================
-- 知识星球每日摘要 — 按用户报告表（Phase 2 输出）
-- =====================================================
-- 说明: 每个用户每天一份报告，机械拼接其订阅星球的 Phase 1 摘要。
--       取代已删除的全局表 zhishixingqiu_summaries。

CREATE TABLE IF NOT EXISTS zhishixingqiu_user_reports (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    report_date DATE NOT NULL,

    report_content TEXT NOT NULL DEFAULT '',
    topic_count INTEGER,
    group_count INTEGER,

    metadata JSONB DEFAULT '{}'::jsonb,
    status VARCHAR(20) NOT NULL DEFAULT 'generating',
    error_message TEXT,

    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT unique_zsxq_user_report_date UNIQUE (user_id, report_date)
);

CREATE INDEX IF NOT EXISTS idx_zsxq_ur_user_date
    ON zhishixingqiu_user_reports (user_id, report_date DESC);
CREATE INDEX IF NOT EXISTS idx_zsxq_ur_report_date
    ON zhishixingqiu_user_reports (report_date DESC);
CREATE INDEX IF NOT EXISTS idx_zsxq_ur_status
    ON zhishixingqiu_user_reports (status);

COMMENT ON TABLE zhishixingqiu_user_reports IS '知识星球每日摘要（每用户每天一份，Phase 2 机械拼接）';
COMMENT ON COLUMN zhishixingqiu_user_reports.report_content IS '报告正文（Markdown，拼接各星球 Phase 1 摘要）';
COMMENT ON COLUMN zhishixingqiu_user_reports.topic_count IS '纳入报告的话题总数';
COMMENT ON COLUMN zhishixingqiu_user_reports.group_count IS '纳入报告的星球数量';
COMMENT ON COLUMN zhishixingqiu_user_reports.metadata IS '生成统计: group_topic_counts, phase2_mode, window 等';
COMMENT ON COLUMN zhishixingqiu_user_reports.status IS '状态: generating / completed / failed';
