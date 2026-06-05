-- =====================================================
-- Twitter Daily Report - HTML/OSS 存储扩展
-- =====================================================
-- 创建时间: 2026-06-05
-- 说明: 为 Twitter Daily Report 添加 HTML 格式支持，新增 OSS 存储和分享功能

-- 新增字段
ALTER TABLE twitter_daily_reports
    ADD COLUMN IF NOT EXISTS oss_object_name TEXT,
    ADD COLUMN IF NOT EXISTS html_content TEXT,
    ADD COLUMN IF NOT EXISTS share_token VARCHAR(64);

-- 分享 token 索引（唯一）
CREATE UNIQUE INDEX IF NOT EXISTS idx_twitter_reports_share_token
    ON twitter_daily_reports(share_token)
    WHERE share_token IS NOT NULL;

-- 字段注释
COMMENT ON COLUMN twitter_daily_reports.oss_object_name IS 'OSS 对象路径（HTML 报告存储位置）';
COMMENT ON COLUMN twitter_daily_reports.html_content IS 'HTML 格式报告内容（数据库存储，优先读取）';
COMMENT ON COLUMN twitter_daily_reports.share_token IS '公开分享 token（用于 iframe 嵌入）';

-- =====================================================
-- 验证
-- =====================================================
-- \d twitter_daily_reports