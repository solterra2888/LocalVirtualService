-- 微博帖子（单表）
CREATE TABLE IF NOT EXISTS weibo_posts (
    post_id          BIGINT PRIMARY KEY,
    user_id          BIGINT NOT NULL,
    screen_name      TEXT NOT NULL DEFAULT '',
    bid              VARCHAR(20) NOT NULL DEFAULT '',
    created_at       TIMESTAMPTZ,
    source           TEXT NOT NULL DEFAULT '',
    text             TEXT NOT NULL DEFAULT '',
    pic_urls         TEXT NOT NULL DEFAULT '',
    video_url        TEXT NOT NULL DEFAULT '',
    is_original      BOOLEAN NOT NULL DEFAULT TRUE,
    create_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_weibo_posts_user_time
    ON weibo_posts (user_id, created_at DESC);

COMMENT ON TABLE weibo_posts IS '微博博主帖子';
COMMENT ON COLUMN weibo_posts.created_at IS '微博侧发布时间';
COMMENT ON COLUMN weibo_posts.create_at IS '首次入库时间';
COMMENT ON COLUMN weibo_posts.updated_at IS '最后一次同步时间';
COMMENT ON COLUMN weibo_posts.pic_urls IS '图片 URL，逗号分隔';
