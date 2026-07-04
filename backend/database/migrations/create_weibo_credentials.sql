-- =====================================================
-- Weibo Credentials Table
-- =====================================================
-- 存储微博爬虫账号 Cookie 凭证（Fernet 加密）
-- 每行代表一个微博账号，支持多凭证轮换以抗风控
--
-- 对齐 zsxq_credentials 结构（create_zsxq_credentials.sql）

CREATE TABLE IF NOT EXISTS weibo_credentials (
    id SERIAL PRIMARY KEY,
    label VARCHAR(255) NOT NULL,                        -- 凭证标识（如 "weibo-account-1"）
    encrypted_cookie TEXT NOT NULL,                     -- Fernet 加密后的 Cookie 字符串
    status VARCHAR(20) NOT NULL DEFAULT 'unknown',      -- valid | expired | unknown
    last_validated_at TIMESTAMPTZ,                      -- 最近一次校验时间
    last_error TEXT,                                    -- 最近一次错误信息（如 m.weibo.cn login=false）
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT valid_weibo_credential_status CHECK (status IN ('valid', 'expired', 'unknown'))
);

CREATE INDEX IF NOT EXISTS idx_weibo_credentials_status
    ON weibo_credentials (status);

COMMENT ON TABLE weibo_credentials IS '微博 Cookie 凭证表（Fernet 加密，多账号轮换）';
COMMENT ON COLUMN weibo_credentials.label IS '人可读标识，如微博账号备注名';
COMMENT ON COLUMN weibo_credentials.encrypted_cookie IS 'Fernet 加密的完整 Cookie 字符串（WEIBO_COOKIE_ENC_KEY）';
COMMENT ON COLUMN weibo_credentials.status IS '凭证状态: valid(有效), expired(已过期), unknown(未校验)';
COMMENT ON COLUMN weibo_credentials.last_validated_at IS '最近一次主动校验时间';
COMMENT ON COLUMN weibo_credentials.last_error IS '最近一次抓取/校验失败的错误摘要';
