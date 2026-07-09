-- =====================================================
-- Substack 每日摘要 — 按用户报告表（Phase 2 输出）
-- =====================================================
-- 说明: 每个用户每天一份报告，机械拼接其订阅 publication 的 Phase 1 摘要。

CREATE TABLE IF NOT EXISTS substack_user_reports (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    report_date DATE NOT NULL,

    report_content TEXT NOT NULL DEFAULT '',
    post_count INTEGER,
    note_count INTEGER,
    publication_count INTEGER,

    metadata JSONB DEFAULT '{}'::jsonb,
    status VARCHAR(20) NOT NULL DEFAULT 'generating',
    error_message TEXT,

    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT unique_substack_user_report_date UNIQUE (user_id, report_date)
);

CREATE INDEX IF NOT EXISTS idx_substack_ur_user_date
    ON substack_user_reports (user_id, report_date DESC);
CREATE INDEX IF NOT EXISTS idx_substack_ur_report_date
    ON substack_user_reports (report_date DESC);
CREATE INDEX IF NOT EXISTS idx_substack_ur_status
    ON substack_user_reports (status);

COMMENT ON TABLE substack_user_reports IS 'Substack 每日摘要（每用户每天一份，Phase 2 机械拼接）';
COMMENT ON COLUMN substack_user_reports.report_content IS '报告正文（Markdown，拼接各 publication Phase 1 摘要）';
COMMENT ON COLUMN substack_user_reports.post_count IS '纳入报告的文章总数';
COMMENT ON COLUMN substack_user_reports.note_count IS '纳入报告的动态总数';
COMMENT ON COLUMN substack_user_reports.publication_count IS '纳入报告的 publication 数量';
COMMENT ON COLUMN substack_user_reports.metadata IS '生成统计: publication_post_counts, publication_note_counts, phase2_mode, window 等';
COMMENT ON COLUMN substack_user_reports.status IS '状态: generating / completed / failed';
