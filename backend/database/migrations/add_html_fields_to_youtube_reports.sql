-- 为 youtube_daily_reports 表添加 HTML 存储和分享功能字段
-- 参考: add_html_fields_to_twitter_reports.sql

ALTER TABLE youtube_daily_reports
    ADD COLUMN IF NOT EXISTS oss_object_name TEXT,
    ADD COLUMN IF NOT EXISTS html_content TEXT,
    ADD COLUMN IF NOT EXISTS share_token VARCHAR(64);

-- 创建 share_token 唯一索引（部分索引，仅对非 NULL 值）
CREATE UNIQUE INDEX IF NOT EXISTS idx_youtube_reports_share_token
    ON youtube_daily_reports(share_token)
    WHERE share_token IS NOT NULL;