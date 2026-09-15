-- =====================================================
-- Form 13F 模块数据库迁移脚本
-- 创建日期: 2026-09-01
-- 描述: SEC Form 13F Data Sets 季度持仓数据表
--   form13f_datasets  数据集管理层（SEC 季度 ZIP 下载/导入状态）
--   form13f_filers    机构维表（申报管理人）
--   form13f_holdings  持仓明细层（全量，聚合到 (filer,quarter,cusip,putcall) 粒度）
--   form13f_snapshots 机构×季度快照层（仪表盘主数据源，导入后预计算）
--   form13f_presets   预设机构（快捷选择）
-- 拆表 rationale: 明细与快照分离——快照服务当前页面（免 join 免排序），
--   明细全量保留支撑未来"按股票反查机构"与重算快照。
-- 幂等，可重复执行。
-- =====================================================

-- 1) 数据集管理层
CREATE TABLE IF NOT EXISTS form13f_datasets (
    id              SERIAL PRIMARY KEY,
    dataset_name    TEXT NOT NULL,                        -- '01JUN2025-31AUG2025_form13f'
    quarter         VARCHAR(6),                           -- '2025Q2'（COVERPAGE 主报告期，导入时校验写入）
    source_url      TEXT NOT NULL,                        -- SEC ZIP 下载地址
    zip_sha256      VARCHAR(64),                          -- 下载完成后校验；重试/重导时一致性检查
    zip_bytes       BIGINT,
    status          VARCHAR(20) NOT NULL DEFAULT 'pending',   -- pending/downloading/downloaded/importing/imported/failed
    filers_count    INTEGER,                              -- 导入统计：机构数
    infotable_rows  BIGINT,                               -- 导入统计：使用的 infotable 行数
    error_message   TEXT,
    downloaded_at   TIMESTAMPTZ,
    imported_at     TIMESTAMPTZ,
    created_at      TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_f13f_dataset UNIQUE (dataset_name),
    CONSTRAINT valid_f13f_status CHECK (status IN (
        'pending', 'downloading', 'downloaded', 'importing', 'imported', 'failed'))
);

CREATE INDEX IF NOT EXISTS idx_f13f_datasets_quarter
ON form13f_datasets(quarter);

-- SEC 页面 ZIP 文件名大小写不稳定，避免 01JUN2025 / 01jun2025 被当成两季
CREATE UNIQUE INDEX IF NOT EXISTS idx_f13f_datasets_name_lower
ON form13f_datasets (LOWER(dataset_name));

-- 2) 机构维表
CREATE TABLE IF NOT EXISTS form13f_filers (
    id                  SERIAL PRIMARY KEY,
    filer_key           VARCHAR(12) NOT NULL,             -- sha1(name.strip())[:12]，前端路由/缓存键
    name_normalized     TEXT NOT NULL,                    -- strip+upper，搜索用
    name_display        TEXT NOT NULL,                    -- 原始 FILINGMANAGER_NAME
    cik                 TEXT,                             -- COVERPAGE 提取（同名多 CIK 取首个）
    latest_quarter      VARCHAR(6),                       -- 快照计算后同步，列表排序用
    latest_total_value  BIGINT,
    is_preset           BOOLEAN DEFAULT FALSE,            -- 是否属于预设机构
    first_seen_at       TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_f13f_filer_key UNIQUE (filer_key)
);

CREATE INDEX IF NOT EXISTS idx_f13f_filers_name_norm
ON form13f_filers(name_normalized text_pattern_ops);

CREATE INDEX IF NOT EXISTS idx_f13f_filers_total
ON form13f_filers(latest_total_value DESC NULLS LAST);

-- 3) 持仓明细层（全量入库）
CREATE TABLE IF NOT EXISTS form13f_holdings (
    id                BIGSERIAL PRIMARY KEY,
    dataset_id        INTEGER NOT NULL REFERENCES form13f_datasets(id),
    quarter           VARCHAR(6) NOT NULL,
    filer_id          INTEGER NOT NULL REFERENCES form13f_filers(id),
    accession_number  TEXT NOT NULL,                      -- 保留 chosen accession 供溯源/amendment 重导
    cusip             VARCHAR(9) NOT NULL,
    issuer_name       TEXT,
    title_of_class    TEXT,
    putcall           VARCHAR(6) DEFAULT '',              -- ''(股) | 'Call' | 'Put'（SEC 原值全词）
    value_usd         BIGINT NOT NULL,                    -- (CUSIP,PUTCALL) 聚合后市值（美元）
    shares            BIGINT,
    leg_count         INTEGER,                            -- 聚合前的原始行数
    sector            VARCHAR(40),                        -- 导入时用 sector_map 分类（GICS 11 类 + ETF 9 类 + Other；最长类别名 31 字符）
    CONSTRAINT uq_f13f_holding UNIQUE (filer_id, quarter, cusip, putcall)
);

-- 未来"按股票反查机构"的反查索引
CREATE INDEX IF NOT EXISTS idx_f13f_holdings_quarter_cusip
ON form13f_holdings(quarter, cusip);

CREATE INDEX IF NOT EXISTS idx_f13f_holdings_filer
ON form13f_holdings(filer_id, quarter);

-- 4) 机构×季度快照层
CREATE TABLE IF NOT EXISTS form13f_snapshots (
    id              SERIAL PRIMARY KEY,
    filer_id        INTEGER NOT NULL REFERENCES form13f_filers(id),
    quarter         VARCHAR(6) NOT NULL,
    total_value     BIGINT,                               -- 组合总市值
    position_count  INTEGER,                              -- 聚合后持仓条数
    sector_alloc    JSONB,                                -- {"Technology": 30.29, ...}（%，含 ETF 9 类资产拆分）
    top20_holdings  JSONB,                                -- 排序后前 20 [{issuer,title,cusip,putcall,value,shares,pct}]
    top5_buys       JSONB,                                -- 与上一有效季度比 [{issuer,cusip,dv,kind,value,spct}]
    top5_sells      JSONB,
    computed_at     TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_f13f_snapshot UNIQUE (filer_id, quarter)
);

CREATE INDEX IF NOT EXISTS idx_f13f_snapshots_quarter
ON form13f_snapshots(quarter);

-- 5) 预设机构
CREATE TABLE IF NOT EXISTS form13f_presets (
    id          SERIAL PRIMARY KEY,
    label       TEXT NOT NULL,                            -- 展示名（如 'Bridgewater Associates'）
    match_name  TEXT NOT NULL,                            -- 申报名（展示/对照用）
    cik         VARCHAR(10),                              -- 冻结 EDGAR CIK，拉取主键
    sort_order  INTEGER DEFAULT 0,
    CONSTRAINT uq_f13f_preset UNIQUE (label)
);

-- 注释
COMMENT ON TABLE form13f_datasets IS '13F 按季度入库批次（edgar_{quarter}），不再对应 SEC ZIP 数据集';
COMMENT ON COLUMN form13f_datasets.quarter IS '报告期末日历季，如 2025Q2';
COMMENT ON TABLE form13f_holdings IS '13F 持仓明细: 按 (filer,quarter,cusip,putcall) 聚合，导入时打好 sector 标签';
COMMENT ON COLUMN form13f_holdings.sector IS '行业/资产分类: GICS 11 板块（CUSIP 映射表）+ ETF 9 类资产（关键词分类）+ Other';
COMMENT ON TABLE form13f_snapshots IS '机构×季度预计算快照: 仪表盘主数据源。top5 买卖对比依赖相邻季度，导入时一次算好';
COMMENT ON COLUMN form13f_snapshots.sector_alloc IS '行业分布百分比 {"Technology": 30.29}，基于全组合（非仅 top20），各季度合计约 100';
