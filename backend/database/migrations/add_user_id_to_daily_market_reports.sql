-- =====================================================
-- Daily Market Report: 添加 per-user 维度
-- =====================================================
-- 说明: 将全局报告改为按用户维度存储（user_id VARCHAR(100)），
--       匹配 theme_reports 模式。每个用户每天一份独立 HTML。
--       背景: 新增 "My Subscription" 标签页（知识星球/Twitter/YouTube 摘要）
--       是 per-user 的，因此整个报告转为 per-user。

-- 1. 添加 user_id 列（先允许 NULL 以便回填）
ALTER TABLE daily_market_reports
    ADD COLUMN IF NOT EXISTS user_id VARCHAR(100);

-- 2. 回填存量行（占位哨兵值 'system'，保留旧数据可查，不与真实用户冲突；
--    后续可用 DELETE FROM daily_market_reports WHERE user_id='system' 清理）
UPDATE daily_market_reports
SET user_id = 'system'
WHERE user_id IS NULL;

-- 3. 回填完成后设为 NOT NULL
ALTER TABLE daily_market_reports
    ALTER COLUMN user_id SET NOT NULL;

-- 4. 删除旧的按日期唯一约束
ALTER TABLE daily_market_reports
    DROP CONSTRAINT IF EXISTS uq_daily_report_date;

-- 5. 新增 per-user 唯一约束
ALTER TABLE daily_market_reports
    ADD CONSTRAINT uq_daily_report_user_date UNIQUE (user_id, report_date);

-- 6. 新增复合索引（per-user 列表/最新报告查询）
CREATE INDEX IF NOT EXISTS idx_daily_market_reports_user_date
    ON daily_market_reports (user_id, report_date DESC);

-- 7. 删除旧的仅日期索引（已被复合索引取代）
DROP INDEX IF EXISTS idx_daily_market_reports_date;

-- 注: share_token 的 partial unique 索引 idx_daily_market_reports_token 保持不变

-- 验证:
-- \d daily_market_reports
