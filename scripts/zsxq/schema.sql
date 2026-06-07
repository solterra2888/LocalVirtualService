-- 知识星球话题（单表）
CREATE TABLE IF NOT EXISTS zsxq_groups_topics (
    topic_id     BIGINT PRIMARY KEY,
    group_id     BIGINT NOT NULL,
    group_name   TEXT NOT NULL DEFAULT '',
    type         VARCHAR(20) NOT NULL DEFAULT 'talk',
    create_time  TIMESTAMPTZ NOT NULL,
    author       TEXT NOT NULL DEFAULT '',
    title        TEXT NOT NULL DEFAULT '',
    content      TEXT NOT NULL DEFAULT '',
    images       TEXT NOT NULL DEFAULT '',
    files        TEXT NOT NULL DEFAULT '',
    create_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_zsxq_groups_topics_group_time
    ON zsxq_groups_topics (group_id, create_time DESC);

COMMENT ON TABLE zsxq_groups_topics IS '知识星球话题';
COMMENT ON COLUMN zsxq_groups_topics.create_time IS '星球侧发帖时间';
COMMENT ON COLUMN zsxq_groups_topics.create_at IS '首次入库时间';
COMMENT ON COLUMN zsxq_groups_topics.updated_at IS '最后一次同步时间';
COMMENT ON COLUMN zsxq_groups_topics.images IS '图片 URL，逗号分隔';
COMMENT ON COLUMN zsxq_groups_topics.files IS '附件文件名，| 分隔';
