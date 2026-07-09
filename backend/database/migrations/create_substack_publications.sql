-- =====================================================
-- Substack Publications — 订阅源并集表 + 文章同步锚点
-- =====================================================
-- 双重用途（角色同 zsxq_groups，不含正文）：
--   1. 订阅源并集清单：refresh_publications 把 源A(subscriptions) 与 源B(following 作者主刊)
--      按 publication_url 合并 upsert，is_subscribed / is_followed 标记来源侧。
--   2. 文章轨（源A）增量抓取锚点（替代 scripts/substack/data/pub_sync_state.json）。

CREATE TABLE IF NOT EXISTS substack_publications (
    id SERIAL PRIMARY KEY,
    publication_url TEXT NOT NULL UNIQUE,               -- publication 根 URL（唯一标识，如 https://citrini.substack.com）
    publication_id BIGINT,                              -- 平台数字 ID（来自 subscriptions API，可空）
    publication_name TEXT NOT NULL DEFAULT '',          -- 展示名

    is_subscribed BOOLEAN NOT NULL DEFAULT false,       -- 源 A：账号订阅了该 newsletter（决定文章轨是否抓）
    is_followed   BOOLEAN NOT NULL DEFAULT false,       -- 源 B：有关注作者的主刊是该 publication（决定动态轨是否抓）

    credential_id INTEGER REFERENCES substack_credentials(id) ON DELETE SET NULL,
                                                        -- 关联的凭证（NULL 表示管理员手动添加）
    enabled BOOLEAN NOT NULL DEFAULT true,              -- 是否对用户开放订阅（并集清单统一开关）

    -- 文章轨（源 A）增量抓取锚点
    backfill_complete BOOLEAN NOT NULL DEFAULT false,
    recent_post_ids BIGINT[] NOT NULL DEFAULT '{}',     -- 最近一批 post_id（用于增量比对停步）
    min_post_date TIMESTAMPTZ,                          -- 已抓取最早文章发布时间
    max_post_date TIMESTAMPTZ,                          -- 已抓取最晚文章发布时间
    last_posts_sync_at TIMESTAMPTZ,                     -- 文章轨最近一次成功同步时间
    last_posts_sync_mode VARCHAR(20),                   -- backfill | incremental | manual

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT valid_substack_publication_sync_mode
        CHECK (last_posts_sync_mode IS NULL OR last_posts_sync_mode IN ('backfill', 'incremental', 'manual'))
);

CREATE INDEX IF NOT EXISTS idx_substack_publications_enabled
    ON substack_publications (enabled);
CREATE INDEX IF NOT EXISTS idx_substack_publications_credential_id
    ON substack_publications (credential_id);
CREATE INDEX IF NOT EXISTS idx_substack_publications_subscribed
    ON substack_publications (is_subscribed) WHERE is_subscribed;

COMMENT ON TABLE substack_publications IS 'Substack 订阅源并集（源A订阅 + 源B关注作者主刊）+ 文章同步锚点';
COMMENT ON COLUMN substack_publications.publication_url IS 'publication 根 URL，全局唯一标识';
COMMENT ON COLUMN substack_publications.is_subscribed IS '来自源A：账号订阅了该 newsletter';
COMMENT ON COLUMN substack_publications.is_followed IS '来自源B：有关注作者的主刊是该 publication';
COMMENT ON COLUMN substack_publications.recent_post_ids IS '最近一批 post_id 数组，文章轨增量停步判断';
COMMENT ON COLUMN substack_publications.min_post_date IS '已入库最早文章发布时间';
COMMENT ON COLUMN substack_publications.max_post_date IS '已入库最晚文章发布时间';
