-- =====================================================
-- Substack Followed Authors — 源B 关注作者登记 + 动态同步锚点
-- =====================================================
-- 角色：登记 + 同步锚点（同 zsxq_groups/substack_publications，不含正文）。
-- 动态（Notes）按作者独立 cursor 翻页 / 增量停步，故 per-author 跟踪。
-- 一个 publication 可能有多个关注作者，所以动态锚点不能放在 substack_publications 单行上。

CREATE TABLE IF NOT EXISTS substack_followed_authors (
    id SERIAL PRIMARY KEY,
    substack_user_id BIGINT NOT NULL UNIQUE,            -- 关注作者的 user_id（来自 feed/following，已去掉自己）
    author_handle TEXT NOT NULL DEFAULT '',             -- @handle
    author_name TEXT NOT NULL DEFAULT '',               -- 展示名
    publication_url TEXT NOT NULL DEFAULT 'https://substack.com',
                                                        -- 作者主刊根 URL（无主刊则为 https://substack.com）
                                                        -- 同时驱动 substack_publications.is_followed

    credential_id INTEGER REFERENCES substack_credentials(id) ON DELETE SET NULL,
    enabled BOOLEAN NOT NULL DEFAULT true,              -- 是否抓取该作者动态

    -- 动态轨（源 B）增量抓取锚点
    backfill_complete BOOLEAN NOT NULL DEFAULT false,
    recent_note_ids BIGINT[] NOT NULL DEFAULT '{}',     -- 该作者最近入库 note_id（用于增量比对停步）
    last_notes_sync_at TIMESTAMPTZ,                     -- 动态轨最近一次成功同步时间

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_substack_followed_authors_enabled
    ON substack_followed_authors (enabled);
CREATE INDEX IF NOT EXISTS idx_substack_followed_authors_pub_url
    ON substack_followed_authors (publication_url);
CREATE INDEX IF NOT EXISTS idx_substack_followed_authors_credential_id
    ON substack_followed_authors (credential_id);

COMMENT ON TABLE substack_followed_authors IS 'Substack 源B 关注作者登记 + 动态(Notes)同步锚点';
COMMENT ON COLUMN substack_followed_authors.substack_user_id IS '关注作者 user_id（feed/following，已去自己）';
COMMENT ON COLUMN substack_followed_authors.publication_url IS '作者主刊根 URL；无主刊为 https://substack.com';
COMMENT ON COLUMN substack_followed_authors.recent_note_ids IS '该作者最近入库 note_id 数组，动态轨增量停步判断';
