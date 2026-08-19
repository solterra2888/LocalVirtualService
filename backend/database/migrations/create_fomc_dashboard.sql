-- =====================================================
-- 央行 Dashboard 数据库迁移脚本
-- 创建日期: 2026-08-19
-- 描述: 创建美联储 FOMC Dashboard 数据表
--   fomc_documents  官方原文层（声明/纪要，不可变）
--   fomc_decisions  结构化抽取层（利率区间/票型/异议，regex 规则可升级重跑）
--   fomc_summaries  LLM 概括层（模板升级可重生成）
--   fed_updates     Fed RSS updates 流
-- 拆表 rationale: 原文/抽取/摘要三者生命周期不同，分表独立失效重算。
-- 幂等，可重复执行。所有表 central_bank 字段预留央行横向扩展（V1 仅 FED）。
-- =====================================================

-- 1) 官方原文层
CREATE TABLE IF NOT EXISTS fomc_documents (
    id                SERIAL PRIMARY KEY,
    central_bank      VARCHAR(16) NOT NULL DEFAULT 'FED',
    doc_type          VARCHAR(20) NOT NULL,                -- 'statement' | 'minutes'
    meeting_date      DATE,                                -- 会议首日
    title             TEXT NOT NULL,
    source_url        TEXT NOT NULL,
    published_at      TIMESTAMPTZ,                         -- Fed 官方发布时间(RSS pubDate)
    raw_content       TEXT,                                -- 清洗后正文
    word_count        INTEGER,
    content_hash      VARCHAR(64),                         -- sha256(规范化正文), 变更检测
    fetch_status      VARCHAR(20) NOT NULL DEFAULT 'pending',  -- pending/fetched/failed
    extraction_method VARCHAR(30) DEFAULT 'bs4_v1',        -- 清洗规则版本, Fed 改版后升 v2 重抓
    error_message     TEXT,
    fetched_at        TIMESTAMPTZ,
    created_at        TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at        TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_fomc_doc UNIQUE (central_bank, source_url),
    CONSTRAINT valid_fomc_doc_type CHECK (doc_type IN ('statement', 'minutes')),
    CONSTRAINT valid_fomc_fetch_status CHECK (fetch_status IN ('pending', 'fetched', 'failed'))
);

CREATE INDEX IF NOT EXISTS idx_fomc_documents_type_date
ON fomc_documents(central_bank, doc_type, meeting_date DESC);

-- 2) 结构化抽取层（仅 statement）
CREATE TABLE IF NOT EXISTS fomc_decisions (
    id                 SERIAL PRIMARY KEY,
    central_bank       VARCHAR(16) NOT NULL DEFAULT 'FED',
    meeting_date       DATE NOT NULL,
    decision_date      DATE,                               -- 声明发布日(会议次日)
    rate_low_bp        INTEGER,                            -- 基点: 350 = 3.50%
    rate_high_bp       INTEGER,                            -- 基点: 375
    decision_direction VARCHAR(10),                        -- 'hike' | 'cut' | 'hold'
    change_bp          INTEGER,                            -- 相对上次, 0 = hold
    vote_for           INTEGER,
    vote_against       INTEGER,
    dissenters         JSONB DEFAULT '[]',                 -- [{name, direction: 'hawkish'|'dovish'}]
    statement_doc_id   INTEGER REFERENCES fomc_documents(id),
    source_url         TEXT,                               -- 声明原文链接(点穿溯源)
    extraction_method  VARCHAR(30) DEFAULT 'regex_v1',
    error_message      TEXT,                               -- 抽取失败落 NULL 字段 + 错误说明, 不阻塞
    extracted_at       TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    created_at         TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at         TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_fomc_decision UNIQUE (central_bank, meeting_date),
    CONSTRAINT valid_fomc_direction CHECK (decision_direction IN ('hike', 'cut', 'hold'))
);

-- 3) LLM 概括层
CREATE TABLE IF NOT EXISTS fomc_summaries (
    id                SERIAL PRIMARY KEY,
    central_bank      VARCHAR(16) NOT NULL DEFAULT 'FED',
    document_id       INTEGER NOT NULL REFERENCES fomc_documents(id),
    doc_type          VARCHAR(20) NOT NULL,
    template_id       VARCHAR(64) NOT NULL,                -- 'fomc_summary_v1'
    model             VARCHAR(64),
    summary_content   TEXT,                                -- 中文 Markdown
    prev_meeting_date DATE,                                -- 概括对照的上次会议
    status            VARCHAR(20) NOT NULL DEFAULT 'generating',  -- generating/completed/failed
    prompt_tokens     INTEGER,
    completion_tokens INTEGER,
    error_message     TEXT,
    created_at        TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at        TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_fomc_summary UNIQUE (document_id, template_id),  -- 模板升级时新旧版本并存对照
    CONSTRAINT valid_fomc_summary_status CHECK (status IN ('generating', 'completed', 'failed'))
);

CREATE INDEX IF NOT EXISTS idx_fomc_summaries_doc
ON fomc_summaries(document_id);

-- 4) Fed RSS updates 流
CREATE TABLE IF NOT EXISTS fed_updates (
    id                  SERIAL PRIMARY KEY,
    central_bank        VARCHAR(16) NOT NULL DEFAULT 'FED',
    guid                TEXT NOT NULL,                     -- RSS entry link
    title               TEXT NOT NULL,
    link                TEXT NOT NULL,
    category            TEXT,
    is_monetary_policy  BOOLEAN DEFAULT FALSE,
    pub_date            TIMESTAMPTZ,
    fetched_at          TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_fed_update UNIQUE (central_bank, guid)
);

CREATE INDEX IF NOT EXISTS idx_fed_updates_pub_date
ON fed_updates(central_bank, pub_date DESC);

-- 注释
COMMENT ON TABLE fomc_documents IS '央行官方原文层: FOMC 声明/纪要全文, 从 federalreserve.gov 抓取';
COMMENT ON COLUMN fomc_documents.content_hash IS 'sha256(规范化正文), 重复抓取时 hash 未变则跳过下游抽取/概括';
COMMENT ON TABLE fomc_decisions IS 'FOMC 利率决议结构化数据, regex 从声明原文抽取; 主数据源, EODHD economic_events 仅作交叉验证';
COMMENT ON COLUMN fomc_decisions.dissenters IS '异议者列表 [{name, direction: hawkish(主张加息)/dovish(主张降息)}]';
COMMENT ON TABLE fomc_summaries IS 'FOMC 声明/纪要 LLM 中文概括, UNIQ(document_id, template_id) 支持模板版本并存';
COMMENT ON TABLE fed_updates IS 'Fed press_all.xml RSS 流, is_monetary_policy 按分类+标题关键词并集过滤';
