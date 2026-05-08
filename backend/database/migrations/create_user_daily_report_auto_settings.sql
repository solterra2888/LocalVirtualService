-- =====================================================
-- User Daily Report Auto Settings — Per-user 自动生成开关
-- =====================================================
-- 创建时间: 2026-05-08
--
-- 背景:
--   系统通过 Celery 定时任务自动为所有有订阅的用户生成 Twitter / YouTube
--   Daily Report。本表允许每个用户独立控制是否参与自动生成流程。
--   关闭后仍可在报告页面手动触发生成，不影响手动操作。
--
-- 设计要点:
--   - 每个用户一行，twitter_auto_enabled / youtube_auto_enabled 独立控制
--   - 默认 TRUE：不影响存量用户，开关默认开启
--   - UPSERT 友好：ON CONFLICT DO UPDATE
--
-- 关联:
--   - 后端服务: backend/services/user_daily_report_settings_service.py
--   - API:      backend/api/twitter_daily_reports.py (GET/PUT /settings)
--              backend/api/youtube_daily_reports.py  (GET/PUT /settings)
--   - 过滤点:   TwitterDailyReportService._get_enabled_twitter_user_ids()
--              YouTubeDailyReportService._get_enabled_youtube_user_ids()
-- =====================================================

CREATE TABLE IF NOT EXISTS user_daily_report_auto_settings (
    id                    SERIAL PRIMARY KEY,
    user_id               INTEGER NOT NULL
        REFERENCES users(id) ON DELETE CASCADE,
    twitter_auto_enabled  BOOLEAN NOT NULL DEFAULT FALSE,
    youtube_auto_enabled  BOOLEAN NOT NULL DEFAULT FALSE,

    created_at            TIMESTAMP(6) WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at            TIMESTAMP(6) WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT uniq_udras_user UNIQUE (user_id)
);

CREATE INDEX IF NOT EXISTS idx_udras_twitter_auto
    ON user_daily_report_auto_settings(user_id)
    WHERE twitter_auto_enabled = FALSE;

CREATE INDEX IF NOT EXISTS idx_udras_youtube_auto
    ON user_daily_report_auto_settings(user_id)
    WHERE youtube_auto_enabled = FALSE;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'update_updated_at_column') THEN
        DROP TRIGGER IF EXISTS update_udras_updated_at
            ON user_daily_report_auto_settings;
        CREATE TRIGGER update_udras_updated_at
            BEFORE UPDATE ON user_daily_report_auto_settings
            FOR EACH ROW
            EXECUTE FUNCTION update_updated_at_column();
    ELSE
        RAISE NOTICE 'update_updated_at_column() not found, skipping trigger creation.';
    END IF;
END $$;

COMMENT ON TABLE  user_daily_report_auto_settings
    IS '用户级 Daily Report 自动生成开关';
COMMENT ON COLUMN user_daily_report_auto_settings.user_id
    IS 'FK → users.id, CASCADE 删除';
COMMENT ON COLUMN user_daily_report_auto_settings.twitter_auto_enabled
    IS 'TRUE 时系统自动为该用户生成 Twitter Daily Report；FALSE 时跳过';
COMMENT ON COLUMN user_daily_report_auto_settings.youtube_auto_enabled
    IS 'TRUE 时系统自动为该用户生成 YouTube Daily Report；FALSE 时跳过';
