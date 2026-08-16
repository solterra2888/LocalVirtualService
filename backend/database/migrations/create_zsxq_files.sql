-- =====================================================
-- ZSXQ Files Download State Machine
-- =====================================================
-- 知识星球帖子附件文件的下载状态机 + OSS 位置。
--   - 由抓取链路写入 pending 行（_store_topic_batch）
--   - 专用消费者任务（process_zsxq_file）下载并上传到 OSS
--   - file_id 为幂等主键（来自 ZSXQ API talk.files[].file_id）
--   - OSS URL 不回写 subscription_content.metadata（避免 fetch ON CONFLICT 覆盖），
--     读取时由 API 层按 topic_id 合并；下载走 /zsxq-files/{id}/download 即时签名。

-- id 列用 TEXT，对齐 zsxq_groups.group_id (VARCHAR) 与 subscription_content.content_id (TEXT)
-- 的字符串 id 约定，避免 BIGINT 转换问题。
CREATE TABLE IF NOT EXISTS zsxq_files (
    file_id         TEXT PRIMARY KEY,             -- ZSXQ 文件唯一 id（幂等基石）
    group_id        TEXT NOT NULL,                -- 知识星球 group_id
    topic_id        TEXT NOT NULL,                -- 对应 subscription_content.content_id
    name            TEXT NOT NULL DEFAULT '',     -- 原始文件名
    size            BIGINT NOT NULL DEFAULT 0,    -- 预期字节数（大小校验用）
    status          TEXT NOT NULL DEFAULT 'pending',
                    -- pending → downloading → downloaded → uploaded（终态成功）
                    -- blocked（1030 硬限制）/ failed（重试耗尽）
    local_path      TEXT NOT NULL DEFAULT '',     -- 下载到本地的临时路径（上传后清空）
    oss_object_name TEXT NOT NULL DEFAULT '',     -- OSS 固定 key（不存签名 URL，会过期）
    oss_bucket      TEXT NOT NULL DEFAULT '',
    retry_count     INT  NOT NULL DEFAULT 0,
    error           TEXT NOT NULL DEFAULT '',
    locked_at       TIMESTAMPTZ,                  -- FOR UPDATE SKIP LOCKED + reaper 用
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT valid_zsxq_file_status CHECK (
        status IN ('pending', 'downloading', 'downloaded', 'uploaded', 'blocked', 'failed')
    )
);

CREATE INDEX IF NOT EXISTS idx_zsxq_files_status_updated ON zsxq_files (status, updated_at);
CREATE INDEX IF NOT EXISTS idx_zsxq_files_topic          ON zsxq_files (topic_id);

COMMENT ON TABLE  zsxq_files IS '知识星球附件文件下载状态机 + OSS 位置（file_id 幂等主键）';
COMMENT ON COLUMN zsxq_files.file_id IS 'ZSXQ 文件唯一 id（talk.files[].file_id）';
COMMENT ON COLUMN zsxq_files.topic_id IS '对应 subscription_content.content_id';
COMMENT ON COLUMN zsxq_files.status IS 'pending→downloading→downloaded→uploaded / blocked / failed';
COMMENT ON COLUMN zsxq_files.oss_object_name IS 'OSS 固定 key（即时签名走代理端点，不存签名 URL）';
COMMENT ON COLUMN zsxq_files.locked_at IS '处理中加锁时间戳；reaper 据 stale 的 locked_at 回退僵死任务';
