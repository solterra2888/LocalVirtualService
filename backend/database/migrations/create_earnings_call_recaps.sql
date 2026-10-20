-- 财报电话会 AI Recap（Earnings Calendar 模块）
-- 每行 = 某公司某季电话会的一次 Recap 结果, (code, period_key) 唯一, 永久缓存
-- status: ok=已生成 | no_transcript=两个数据源都没找到 | error=生成失败
CREATE TABLE IF NOT EXISTS earnings_call_recaps (
    id SERIAL PRIMARY KEY,
    code TEXT NOT NULL,
    period_key TEXT NOT NULL,
    source TEXT NOT NULL DEFAULT 'motley_fool',
    source_url TEXT,
    fiscal_label TEXT,
    call_date DATE,
    transcript_text TEXT,
    recap_md TEXT,
    model TEXT,
    llm_cost DOUBLE PRECISION,
    status TEXT NOT NULL DEFAULT 'pending',
    error_message TEXT,
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_earnings_recap_code_period UNIQUE (code, period_key)
);

CREATE INDEX IF NOT EXISTS idx_earnings_recaps_code_latest
    ON earnings_call_recaps (code, call_date DESC);
