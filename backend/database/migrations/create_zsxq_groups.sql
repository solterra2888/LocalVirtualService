-- =====================================================
-- ZSXQ Groups Cache & Sync State Table
-- =====================================================
-- 双重用途：
--   1. 缓存「管理员账号已加入的星球」列表（供用户在 Feeds 界面选择）
--   2. 保存增量抓取锚点（替代 scripts/zsxq/data/group_sync_state.json）

CREATE TABLE IF NOT EXISTS zsxq_groups (
    id SERIAL PRIMARY KEY,
    group_id VARCHAR(50) NOT NULL UNIQUE,               -- 知识星球 group_id（如 "15555452148452"）
    group_name VARCHAR(255) NOT NULL DEFAULT '',        -- 星球名称（显示用）
    credential_id INTEGER REFERENCES zsxq_credentials(id) ON DELETE SET NULL,
                                                        -- 关联的凭证（NULL 表示管理员手动添加）
    enabled BOOLEAN NOT NULL DEFAULT true,              -- 是否对用户开放订阅

    -- 增量抓取锚点（对齐 scripts/zsxq/data/group_sync_state.json 字段）
    backfill_complete BOOLEAN NOT NULL DEFAULT false,
    recent_topic_ids BIGINT[] NOT NULL DEFAULT '{}',    -- 最近一页 topic_id 列表（用于增量比对）
    min_create_time TIMESTAMPTZ,                        -- 已抓取最早发帖时间
    max_create_time TIMESTAMPTZ,                        -- 已抓取最晚发帖时间
    last_sync_at TIMESTAMPTZ,                           -- 最近一次成功同步时间
    last_sync_mode VARCHAR(20),                         -- backfill | incremental | manual

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT valid_zsxq_group_sync_mode CHECK (last_sync_mode IS NULL OR last_sync_mode IN ('backfill', 'incremental', 'manual'))
);

CREATE INDEX IF NOT EXISTS idx_zsxq_groups_enabled
    ON zsxq_groups (enabled);
CREATE INDEX IF NOT EXISTS idx_zsxq_groups_credential_id
    ON zsxq_groups (credential_id);

COMMENT ON TABLE zsxq_groups IS '知识星球列表缓存 + 增量抓取同步状态';
COMMENT ON COLUMN zsxq_groups.group_id IS '知识星球平台 group_id';
COMMENT ON COLUMN zsxq_groups.credential_id IS '关联的管理员凭证；NULL 表示手动维护';
COMMENT ON COLUMN zsxq_groups.enabled IS '是否在 Feeds 界面对普通用户开放订阅';
COMMENT ON COLUMN zsxq_groups.backfill_complete IS '是否已完成历史补全';
COMMENT ON COLUMN zsxq_groups.recent_topic_ids IS '最近一页的 topic_id 数组，用于增量抓取时停步判断';
COMMENT ON COLUMN zsxq_groups.min_create_time IS '已入库最早话题发布时间';
COMMENT ON COLUMN zsxq_groups.max_create_time IS '已入库最晚话题发布时间';
