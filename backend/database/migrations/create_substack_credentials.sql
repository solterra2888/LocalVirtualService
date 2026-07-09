-- =====================================================
-- Substack Credentials Table
-- =====================================================
-- 存储 Substack 管理员 Cookie 凭证（Fernet 加密）
-- 每行代表一个管理员账号，支持多凭证轮换
-- 与 zsxq_credentials 同构；encrypted_cookie 存加密 JSON {"substack.sid":..,"substack.lli":..}

CREATE TABLE IF NOT EXISTS substack_credentials (
    id SERIAL PRIMARY KEY,
    label VARCHAR(255) NOT NULL,                        -- 凭证标识（如 "admin-account-1"）
    encrypted_cookie TEXT NOT NULL,                     -- Fernet 加密后的 JSON 字符串 {"substack.sid":..,"substack.lli":..}
    status VARCHAR(20) NOT NULL DEFAULT 'unknown',      -- valid | expired | unknown
    last_validated_at TIMESTAMPTZ,                      -- 最近一次校验时间
    last_error TEXT,                                    -- 最近一次错误信息（如 HTTP 401）
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT valid_substack_credential_status CHECK (status IN ('valid', 'expired', 'unknown'))
);

CREATE INDEX IF NOT EXISTS idx_substack_credentials_status
    ON substack_credentials (status);

COMMENT ON TABLE substack_credentials IS 'Substack 管理员 Cookie 凭证表（加密存储，单列 JSON 含 sid+lli）';
COMMENT ON COLUMN substack_credentials.label IS '人可读标识，如管理员账号名';
COMMENT ON COLUMN substack_credentials.encrypted_cookie IS 'Fernet 加密的 JSON {"substack.sid":..,"substack.lli":..}';
COMMENT ON COLUMN substack_credentials.status IS '凭证状态: valid(有效), expired(已过期), unknown(未校验)';
COMMENT ON COLUMN substack_credentials.last_validated_at IS '最近一次主动校验时间';
COMMENT ON COLUMN substack_credentials.last_error IS '最近一次抓取/校验失败的错误摘要';
