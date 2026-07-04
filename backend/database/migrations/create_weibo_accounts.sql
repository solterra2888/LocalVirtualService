-- =====================================================
-- Weibo Accounts Cache & Sync State Table
-- =====================================================
-- 双重用途：
--   1. 缓存用户订阅的微博博主元数据（昵称/头像，订阅时自动 upsert）
--   2. 保存增量抓取锚点（替代 scripts/参考代码/weibo_script/data/last_seen.json）
--
-- 对齐 zsxq_groups 结构（create_zsxq_groups.sql），差异：
--   - uid 为 VARCHAR(20)（对齐 subscription_list.channel_identifier，避免 join cast）
--   - 订阅时自动 upsert（非管理员预录入），故 enabled 默认 true

CREATE TABLE IF NOT EXISTS weibo_accounts (
    id SERIAL PRIMARY KEY,
    uid VARCHAR(20) NOT NULL UNIQUE,                    -- 微博博主 UID（str，== subscription_list.channel_identifier）
    screen_name VARCHAR(255) NOT NULL DEFAULT '',       -- 博主昵称（显示用）
    avatar TEXT NOT NULL DEFAULT '',                    -- 头像 URL
    credential_id INTEGER REFERENCES weibo_credentials(id) ON DELETE SET NULL,
                                                        -- 关联的凭证（NULL 表示尚未绑定）
    enabled BOOLEAN NOT NULL DEFAULT true,              -- 是否参与抓取

    -- 增量抓取锚点
    backfill_complete BOOLEAN NOT NULL DEFAULT false,
    recent_post_ids BIGINT[] NOT NULL DEFAULT '{}',     -- 最近一批 post_id 列表（增量比对）
    min_create_time TIMESTAMPTZ,                        -- 已抓取最早发博时间
    max_create_time TIMESTAMPTZ,                        -- 已抓取最晚发博时间
    last_sync_at TIMESTAMPTZ,                           -- 最近一次成功同步时间
    last_sync_mode VARCHAR(20),                         -- backfill | incremental | manual

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT valid_weibo_account_sync_mode
        CHECK (last_sync_mode IS NULL OR last_sync_mode IN ('backfill', 'incremental', 'manual'))
);

CREATE INDEX IF NOT EXISTS idx_weibo_accounts_enabled
    ON weibo_accounts (enabled);
CREATE INDEX IF NOT EXISTS idx_weibo_accounts_credential_id
    ON weibo_accounts (credential_id);

COMMENT ON TABLE weibo_accounts IS '微博博主元数据缓存 + 增量抓取同步状态';
COMMENT ON COLUMN weibo_accounts.uid IS '微博博主 UID（字符串，与 subscription_list.channel_identifier 一致）';
COMMENT ON COLUMN weibo_accounts.screen_name IS '博主昵称，订阅时由 crawl4weibo get_user_by_uid 回填';
COMMENT ON COLUMN weibo_accounts.credential_id IS '关联的凭证；NULL 表示尚未绑定';
COMMENT ON COLUMN weibo_accounts.enabled IS '是否参与抓取（管理员可在 /api/admin/weibo/accounts 停用）';
COMMENT ON COLUMN weibo_accounts.backfill_complete IS '是否已完成历史补全';
COMMENT ON COLUMN weibo_accounts.recent_post_ids IS '最近一批 post_id 数组，用于增量抓取停步判断';
COMMENT ON COLUMN weibo_accounts.min_create_time IS '已入库最早博文发布时间';
COMMENT ON COLUMN weibo_accounts.max_create_time IS '已入库最晚博文发布时间';
