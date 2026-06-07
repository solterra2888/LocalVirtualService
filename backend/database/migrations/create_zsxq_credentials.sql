-- =====================================================
-- ZSXQ Credentials Table
-- =====================================================
-- 存储知识星球管理员 Cookie 凭证（Fernet 加密）
-- 每行代表一个管理员微信账号，支持多凭证轮换

CREATE TABLE IF NOT EXISTS zsxq_credentials (
    id SERIAL PRIMARY KEY,
    label VARCHAR(255) NOT NULL,                        -- 凭证标识（如 "admin-account-1"）
    encrypted_cookie TEXT NOT NULL,                     -- Fernet 加密后的 Cookie 字符串
    status VARCHAR(20) NOT NULL DEFAULT 'unknown',      -- valid | expired | unknown
    last_validated_at TIMESTAMPTZ,                      -- 最近一次校验时间
    last_error TEXT,                                    -- 最近一次错误信息（如 HTTP 401）
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT valid_zsxq_credential_status CHECK (status IN ('valid', 'expired', 'unknown'))
);

CREATE INDEX IF NOT EXISTS idx_zsxq_credentials_status
    ON zsxq_credentials (status);

COMMENT ON TABLE zsxq_credentials IS '知识星球管理员 Cookie 凭证表（加密存储）';
COMMENT ON COLUMN zsxq_credentials.label IS '人可读标识，如管理员账号名';
COMMENT ON COLUMN zsxq_credentials.encrypted_cookie IS 'Fernet 加密的完整 Cookie 字符串';
COMMENT ON COLUMN zsxq_credentials.status IS '凭证状态: valid(有效), expired(已过期), unknown(未校验)';
COMMENT ON COLUMN zsxq_credentials.last_validated_at IS '最近一次主动校验时间';
COMMENT ON COLUMN zsxq_credentials.last_error IS '最近一次抓取/校验失败的错误摘要';
