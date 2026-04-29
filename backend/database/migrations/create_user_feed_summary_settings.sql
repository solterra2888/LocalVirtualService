-- =====================================================
-- User Feed Summary Settings — Per-user × per-channel auto personalization
-- =====================================================
-- 创建时间: 2026-04-29
-- 来源: docs/instructions/LLM_ARCHITECTURE_OVERVIEW.md (PR-N1, Feed 个性化重构)
--
-- 背景:
--   原 PR-7 设计是用户在文章页主动点 "Personalize" 触发, 这导致:
--     - 个性化体验弱: 用户必须每条手动点
--     - Settings 里 user_template_preferences[feed_summary] 形同虚设
--   重构方向 (PR-N): 用户在 Feeds 页齿轮配置中按渠道开启自动个性化,
--   抓取 + 全局打标完成后 Celery 自动 fan-out 派发 per-user 摘要任务.
--
-- 设计要点:
--   - (user_id, channel_type) 唯一: 每个用户对每个渠道一行配置
--   - channel_type 仅限 twitter / youtube (CHECK 约束)
--   - enabled 默认 false: 开关默认关闭, 不影响存量用户
--   - template_id 可空: 用户没指定时走 LLMTemplateResolver 走系统默认
--                       (允许 SET NULL: 模板被删时不阻断)
--   - min_length 可空: 用户级长度门槛, 空时走 env (AUTO_FEED_TAG_MIN_LENGTH_*)
--
-- 关联:
--   - Celery: backend/tasks/feed_summary_tasks.py::fan_out_personalize_summary_task
--   - API:    backend/api/feed_summary_settings.py
--   - 前端:   web-frontend/src/components/feed/FeedSummarySettingsDrawer.tsx
-- =====================================================

CREATE TABLE IF NOT EXISTS user_feed_summary_settings (
    id           SERIAL PRIMARY KEY,
    user_id      INTEGER NOT NULL
        REFERENCES users(id) ON DELETE CASCADE,
    channel_type VARCHAR(50) NOT NULL,
    enabled      BOOLEAN NOT NULL DEFAULT FALSE,
    template_id  INTEGER
        REFERENCES llm_prompt_templates(id) ON DELETE SET NULL,
    min_length   INTEGER,

    created_at   TIMESTAMP(6) WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at   TIMESTAMP(6) WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT uniq_ufss_user_channel
        UNIQUE (user_id, channel_type),
    CONSTRAINT chk_ufss_channel_type
        CHECK (channel_type IN ('twitter', 'youtube'))
);

-- fan-out 任务每条 feed 都要查 (channel_type, enabled=true) 的所有用户,
-- 加 partial index 加速 (只对 enabled=true 建索引, 体积小).
CREATE INDEX IF NOT EXISTS idx_ufss_enabled_channel
    ON user_feed_summary_settings(channel_type, user_id)
    WHERE enabled = TRUE;

-- updated_at 触发器 (复用全局函数, 与其他 user_* 配置表一致)
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'update_updated_at_column') THEN
        DROP TRIGGER IF EXISTS update_ufss_updated_at
            ON user_feed_summary_settings;
        CREATE TRIGGER update_ufss_updated_at
            BEFORE UPDATE ON user_feed_summary_settings
            FOR EACH ROW
            EXECUTE FUNCTION update_updated_at_column();
    ELSE
        RAISE NOTICE 'update_updated_at_column() not found, skipping trigger creation. '
                     'Run create_subscription_content_tags.sql first.';
    END IF;
END $$;

-- 注释
COMMENT ON TABLE  user_feed_summary_settings
    IS '用户 × 渠道 自动个性化摘要开关 (PR-N: Feed 个性化重构)';
COMMENT ON COLUMN user_feed_summary_settings.user_id
    IS 'FK → users.id, CASCADE 删除';
COMMENT ON COLUMN user_feed_summary_settings.channel_type
    IS 'twitter | youtube';
COMMENT ON COLUMN user_feed_summary_settings.enabled
    IS '是否开启自动个性化 (FALSE 时 fan-out 跳过该用户)';
COMMENT ON COLUMN user_feed_summary_settings.template_id
    IS 'FK → llm_prompt_templates.id, 用户选定的私有/系统模板; '
       'NULL 时由 LLMTemplateResolver 自动解析';
COMMENT ON COLUMN user_feed_summary_settings.min_length
    IS '用户级长度门槛 (字符数), NULL 时走 env AUTO_FEED_TAG_MIN_LENGTH_*';

-- =====================================================
-- 验证
-- =====================================================
-- \d+ user_feed_summary_settings
--
-- 测试 fan-out 查询:
--   SELECT s.user_id, s.template_id
--   FROM   user_feed_summary_settings s
--   JOIN   subscription_content sc ON s.user_id = ANY(sc.user_ids)
--   WHERE  sc.id = $1
--     AND  s.channel_type = sc.channel_type
--     AND  s.enabled = TRUE;
