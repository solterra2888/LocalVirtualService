-- =====================================================
-- LLM 核心表 Baseline (反向版本化)
-- =====================================================
-- 创建时间: 2026-04-27
-- 来源: docs/instructions/LLM_REFACTOR_PLAN.md §2 (PR-1)
--
-- 背景:
--   以下 4 张表在生产环境已存在，但其 CREATE TABLE 从未被纳入 migrations
--   版本控制。本 migration 把权威 DDL 落到代码仓库，作为后续所有 LLM 相关
--   schema 改动的基线。
--
-- 设计原则:
--   - 全部 IF NOT EXISTS / CREATE INDEX IF NOT EXISTS, 对已部署环境无副作用
--   - 字段名/默认值/约束名/索引名/触发器名 100% 对齐生产 (来源:
--     /tmp/prod_llm_schema.tsv, 在 PG 17 的 information_schema 上反查)
--   - 不复制生产中已存在的历史脏数据 (例如 file_tags 上孤儿的
--     check_confidence_range 重复约束 — 新环境只建一份干净的)
--
-- 验证步骤:
--   1. 在已部署生产库 BEGIN; \i ...; ROLLBACK; — 应全部 NOTICE skipping
--   2. 在干净的 PG 17 实例运行整个 migrations 目录 — 应能完整建出 LLM 体系
--   3. \d+ 输出应与生产 \d+ 完全一致
--
-- 关联:
--   - 阶段 1 PR-3 会在此基础上为 llm_prompt_templates 增加 module_code 字段
--   - 与 add_summary_to_file_tags.sql / fix_all_naive_timestamps_to_utc.sql
--     无字段冲突 (那两个文件作用于同一字段集合的修补/数据清洗)

-- ==========================================================================
-- 1. llm_prompt_categories — Prompt 模板分类
-- ==========================================================================

CREATE TABLE IF NOT EXISTS llm_prompt_categories (
    id            SERIAL PRIMARY KEY,
    category_code VARCHAR(50)  NOT NULL,
    category_name VARCHAR(100) NOT NULL,
    description   TEXT,
    icon          VARCHAR(50),
    sort_order    INTEGER      DEFAULT 0,
    is_active     BOOLEAN      DEFAULT TRUE,
    created_at    TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    updated_at    TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT llm_prompt_categories_category_code_key UNIQUE (category_code)
);

COMMENT ON TABLE  llm_prompt_categories
    IS 'Prompt 模板分类目录 (UI 分组用); 业务模块绑定见 user_template_preferences.module_code';
COMMENT ON COLUMN llm_prompt_categories.category_code
    IS '分类英文唯一标识, 例如 file_analysis / content_tagging';
COMMENT ON COLUMN llm_prompt_categories.icon
    IS '前端展示图标名 (Ant Design 图标关键字或自定义代码)';

-- ==========================================================================
-- 2. llm_prompt_templates — Prompt 模板主表
-- ==========================================================================

CREATE TABLE IF NOT EXISTS llm_prompt_templates (
    id                    SERIAL PRIMARY KEY,
    template_code         VARCHAR(100) NOT NULL,
    template_name         VARCHAR(200) NOT NULL,
    category_id           INTEGER      REFERENCES llm_prompt_categories(id) ON DELETE CASCADE,

    -- Prompt 主体
    system_prompt         TEXT,
    user_prompt_template  TEXT NOT NULL,
    variables             JSONB        DEFAULT '{}'::jsonb,
    output_format         JSONB        DEFAULT '{}'::jsonb,

    -- 推荐 LLM 配置
    recommended_provider  VARCHAR(50),
    recommended_model     VARCHAR(100),
    temperature           NUMERIC(3,2) DEFAULT 0.7,
    max_tokens            INTEGER      DEFAULT 2000,

    -- 权限与可见性
    scope                 VARCHAR(20)  NOT NULL DEFAULT 'system',
    created_by            INTEGER      REFERENCES users(id) ON DELETE SET NULL,
    is_public             BOOLEAN      DEFAULT FALSE,

    -- 版本管理
    version               INTEGER      DEFAULT 1,
    parent_template_id    INTEGER      REFERENCES llm_prompt_templates(id) ON DELETE SET NULL,

    -- 状态
    is_active             BOOLEAN      DEFAULT TRUE,
    is_default            BOOLEAN      DEFAULT FALSE,

    -- 使用统计
    usage_count           INTEGER      DEFAULT 0,
    success_rate          NUMERIC(5,2),
    avg_cost              NUMERIC(10,6),

    -- 元数据
    tags                  JSONB        DEFAULT '[]'::jsonb,
    description           TEXT,
    example_input         JSONB        DEFAULT '{}'::jsonb,
    example_output        TEXT,

    created_at            TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    updated_at            TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT llm_prompt_templates_template_code_key UNIQUE (template_code),
    CONSTRAINT valid_scope         CHECK ((scope)::text = ANY (ARRAY['system'::text, 'user'::text, 'shared'::text])),
    CONSTRAINT valid_temperature   CHECK ((temperature >= 0::numeric) AND (temperature <= 2::numeric)),
    CONSTRAINT valid_success_rate  CHECK ((success_rate >= 0::numeric) AND (success_rate <= 100::numeric))
);

CREATE INDEX IF NOT EXISTS idx_templates_code
    ON llm_prompt_templates(template_code);
CREATE INDEX IF NOT EXISTS idx_templates_category
    ON llm_prompt_templates(category_id);
CREATE INDEX IF NOT EXISTS idx_templates_scope
    ON llm_prompt_templates(scope, is_active);
CREATE INDEX IF NOT EXISTS idx_templates_created_by
    ON llm_prompt_templates(created_by);

COMMENT ON TABLE  llm_prompt_templates
    IS 'LLM Prompt 模板; 通过 PromptRenderer 渲染后供 LLMService.chat_completion_with_template 使用';
COMMENT ON COLUMN llm_prompt_templates.template_code
    IS '模板英文唯一标识, 例如 content_tag_v1, twitter_daily_phase2_default';
COMMENT ON COLUMN llm_prompt_templates.category_id
    IS 'FK → llm_prompt_categories.id (UI 分组); 业务模块绑定走 user_template_preferences.module_code';
COMMENT ON COLUMN llm_prompt_templates.scope
    IS 'system=系统模板(管理员维护) / user=私有 / shared=用户共享';
COMMENT ON COLUMN llm_prompt_templates.variables
    IS 'JSONB: { var_name: { type, required, default, description } }';
COMMENT ON COLUMN llm_prompt_templates.output_format
    IS 'JSONB: { type: text|json|object|structured_text, json_schema?, ... }';
COMMENT ON COLUMN llm_prompt_templates.is_default
    IS '同一 category 内可标记一个默认模板 (UI 用)';
COMMENT ON COLUMN llm_prompt_templates.parent_template_id
    IS 'self-FK: 用于版本派生 (基于哪个模板 fork 出的新版)';

-- ==========================================================================
-- 3. llm_prompt_usage_logs — Prompt 模板调用日志
-- ==========================================================================

CREATE TABLE IF NOT EXISTS llm_prompt_usage_logs (
    id                SERIAL PRIMARY KEY,
    template_id       INTEGER REFERENCES llm_prompt_templates(id) ON DELETE CASCADE,
    user_id           INTEGER REFERENCES users(id) ON DELETE SET NULL,

    -- LLM 元数据
    provider          VARCHAR(50),
    model             VARCHAR(100),

    -- 调用上下文
    rendered_prompt   TEXT,
    variables_data    JSONB    DEFAULT '{}'::jsonb,
    response_content  TEXT,

    -- 用量与计费
    tokens_used       INTEGER,
    cost              NUMERIC(10,6),
    response_time_ms  INTEGER,

    -- 结果
    success           BOOLEAN  DEFAULT TRUE,
    error_message     TEXT,

    -- 用户反馈 (1-5 评分)
    user_feedback     INTEGER,

    created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT valid_user_feedback CHECK ((user_feedback >= 1) AND (user_feedback <= 5))
);

CREATE INDEX IF NOT EXISTS idx_usage_logs_template
    ON llm_prompt_usage_logs(template_id);
CREATE INDEX IF NOT EXISTS idx_usage_logs_user
    ON llm_prompt_usage_logs(user_id);
CREATE INDEX IF NOT EXISTS idx_usage_logs_created
    ON llm_prompt_usage_logs(created_at DESC);

COMMENT ON TABLE  llm_prompt_usage_logs
    IS '每次 LLM 模板调用一行: template_id + user_id + tokens + cost + 成败';
COMMENT ON COLUMN llm_prompt_usage_logs.template_id
    IS 'FK → llm_prompt_templates.id; CASCADE: 模板被删时日志也删 (生产实际行为)';
COMMENT ON COLUMN llm_prompt_usage_logs.user_id
    IS 'FK → users.id; 系统调度任务无 user 时为 NULL';
COMMENT ON COLUMN llm_prompt_usage_logs.variables_data
    IS '渲染时的实际变量值 (JSON 序列化, 含 ensure_ascii=False)';
COMMENT ON COLUMN llm_prompt_usage_logs.user_feedback
    IS '用户对该次调用结果的评分 (1-5); NULL 表示用户未反馈';

-- ==========================================================================
-- 4. file_tags — Upload Files 自动打标结果
-- ==========================================================================

CREATE TABLE IF NOT EXISTS file_tags (
    id              SERIAL PRIMARY KEY,
    file_id         INTEGER NOT NULL,

    -- GICS 行业分类 (中英文双语)
    -- 注意: sector / industry 在生产是 NOT NULL (反查于 information_schema)
    sector          TEXT NOT NULL,
    industry        TEXT NOT NULL,
    sector_en       TEXT,
    industry_en     TEXT,

    -- 关键标签字段
    ticker          JSONB,                          -- ["NVDA.US","TSLA.US"]
    concepts        JSONB DEFAULT '[]'::jsonb,      -- ["AI芯片","算力"]
    additional_tags JSONB DEFAULT '{}'::jsonb,      -- {tickers_full, ai_metadata}
    -- 注意: 生产是 double precision, 不是 NUMERIC(3,2)
    confidence      DOUBLE PRECISION,
    reasoning       TEXT,

    -- 摘要 (来自 add_summary_to_file_tags.sql)
    summary         TEXT,

    -- 溯源
    tagged_by       INTEGER,
    tagged_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- 显式约束 (与生产对齐)
    CONSTRAINT fk_file_tags_file_id
        FOREIGN KEY (file_id) REFERENCES file_processing_details(id) ON DELETE CASCADE,
    CONSTRAINT fk_file_tags_tagged_by
        FOREIGN KEY (tagged_by) REFERENCES users(id) ON DELETE SET NULL,
    CONSTRAINT check_confidence_range
        CHECK ((confidence IS NULL) OR ((confidence >= 0::double precision) AND (confidence <= 1::double precision)))
);

-- file_id 通过 UNIQUE INDEX 实现单一性 (与生产一致, 不用 UNIQUE 约束)
CREATE UNIQUE INDEX IF NOT EXISTS idx_file_tags_file_id
    ON file_tags(file_id);
CREATE INDEX IF NOT EXISTS idx_file_tags_sector_industry
    ON file_tags(sector, industry);
CREATE INDEX IF NOT EXISTS idx_file_tags_ticker_gin
    ON file_tags USING gin(ticker);
CREATE INDEX IF NOT EXISTS idx_file_tags_additional_tags
    ON file_tags USING gin(additional_tags);

COMMENT ON TABLE  file_tags
    IS 'Upload Files 单文件 LLM 自动打标结果 (1:1 with file_processing_details)';
COMMENT ON COLUMN file_tags.file_id
    IS 'FK → file_processing_details.id, CASCADE 删除';
COMMENT ON COLUMN file_tags.summary
    IS '50-200 字单句摘要 (与 subscription_content_tags.summary 对齐, Daily 聚合视图使用)';
COMMENT ON COLUMN file_tags.additional_tags
    IS '{tickers_full: [...], ai_metadata: {total_tokens, total_price, model, provider, response_time}}';
COMMENT ON COLUMN file_tags.confidence
    IS 'LLM 自评置信度 (0-1, double precision)';

-- ==========================================================================
-- updated_at 自动维护触发器 (与生产对齐 — 仅 categories / templates 有)
-- ==========================================================================
-- 注意:
--   - 生产中 file_tags / llm_prompt_usage_logs 都没有 updated_at 触发器,
--     baseline 不为它们加, 保持一致 (file_tags.updated_at 由应用层显式 SET)
--   - 触发器名与生产对齐: update_llm_prompt_categories_updated_at /
--     update_llm_prompt_templates_updated_at (而非 update_lpc_* / update_lpt_*)

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_proc WHERE proname = 'update_updated_at_column'
    ) THEN
        DROP TRIGGER IF EXISTS update_llm_prompt_categories_updated_at ON llm_prompt_categories;
        CREATE TRIGGER update_llm_prompt_categories_updated_at
            BEFORE UPDATE ON llm_prompt_categories
            FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

        DROP TRIGGER IF EXISTS update_llm_prompt_templates_updated_at ON llm_prompt_templates;
        CREATE TRIGGER update_llm_prompt_templates_updated_at
            BEFORE UPDATE ON llm_prompt_templates
            FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
    ELSE
        RAISE NOTICE 'update_updated_at_column() not found, skipping trigger creation. '
                     'Run create_subscription_content_tags.sql first or create the function manually.';
    END IF;
END $$;

-- ==========================================================================
-- 验证
-- ==========================================================================
-- \d+ llm_prompt_categories
-- \d+ llm_prompt_templates
-- \d+ llm_prompt_usage_logs
-- \d+ file_tags
--
-- 比对生产实际:
--   见 PR-1 验证报告 docs/instructions/LLM_REFACTOR_PLAN.md §2.3
