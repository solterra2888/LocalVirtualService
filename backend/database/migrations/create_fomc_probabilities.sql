-- FOMC 概率模块（V2）：会议日历 + 概率每日快照
-- 依赖: create_fomc_dashboard.sql 已建 fomc_decisions（预期差对照用，只读）
-- 幂等: 可重复执行

CREATE TABLE IF NOT EXISTS fomc_meetings (
    id SERIAL PRIMARY KEY,
    central_bank VARCHAR(16) NOT NULL DEFAULT 'FED',
    meeting_date DATE NOT NULL,            -- 会议末日（决议发布日）
    first_day DATE,                        -- 会议首日（对齐 fomc_decisions.meeting_date 语义）
    calendar_year INT NOT NULL,            -- 所属 FOMC 日历年
    is_sep BOOLEAN NOT NULL DEFAULT FALSE, -- 带 Summary of Economic Projections（日历 * 后缀）
    is_notation BOOLEAN NOT NULL DEFAULT FALSE, -- "notation vote"，无利率决议，概率排除
    raw_date_text VARCHAR(64),             -- 原始日期文本（溯源，如 "15-16*"）
    synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (central_bank, meeting_date)
);

-- 已建表环境补列（幂等）
ALTER TABLE fomc_meetings ADD COLUMN IF NOT EXISTS first_day DATE;

CREATE TABLE IF NOT EXISTS fomc_probabilities (
    id SERIAL PRIMARY KEY,
    central_bank VARCHAR(16) NOT NULL DEFAULT 'FED',
    meeting_date DATE NOT NULL,            -- 概率归属会议
    asof_date DATE NOT NULL,               -- 快照日（时序维度）
    p_cut  NUMERIC(5,4),                   -- 冗余列：折线图直查
    p_hold NUMERIC(5,4),
    p_hike NUMERIC(5,4),
    distribution JSONB,                    -- {"-25":0,"0":0.7,"+25":0.3,"overflow":false}
    implied_rate NUMERIC(6,3),             -- 合约隐含月均利率(%)
    effr NUMERIC(6,3),                     -- 快照日使用的 EFFR(%)
    futures_contract TEXT,                 -- 溯源：如 ZQU26.CBT
    futures_price NUMERIC(8,3),
    method TEXT NOT NULL,                  -- 'zq_v1'(在市月合约) / 'zq_front_v1'(连续合约回填)
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (central_bank, meeting_date, asof_date)
);

CREATE INDEX IF NOT EXISTS idx_fomc_probabilities_meeting
    ON fomc_probabilities (central_bank, meeting_date, asof_date DESC);
