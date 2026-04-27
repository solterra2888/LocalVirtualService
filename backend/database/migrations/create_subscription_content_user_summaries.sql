-- =====================================================
-- Subscription Content User Summaries — Feeds 个性化摘要 (per-user override)
-- =====================================================
-- 创建时间: 2026-04-27
-- 来源: docs/instructions/LLM_REFACTOR_PLAN.md §5.1 (PR-6)
--
-- 背景:
--   subscription_content_tags 是"全局共享标签": 一条 feed 内容只对应一行 tag,
--   所有订阅该频道的用户看到的都是同一份 (来自首次抓取时的 LLM 打标).
--   这与 Feeds 模块"跨用户内容复用"的核心设计直接冲突 — 用户无法用自己的
--   模板/偏好生成个性化摘要.
--
--   决策 D5.A (双层模型, 见 LLM_REFACTOR_PLAN.md §0):
--     - subscription_content_tags  (保留, 全局共享)         ← 客观分类 + baseline summary
--     - subscription_content_user_summaries (本表, 新增)    ← 主观字段 per-user override
--
--   读取规则 (subscriptions API list_contents):
--     LEFT JOIN scus ON scus.subscription_content_id = sc.id AND scus.user_id = ?
--     summary       = COALESCE(scus.summary, sct.summary)
--     reasoning     = COALESCE(scus.reasoning, sct.reasoning)
--     summary_source= scus.id IS NOT NULL ? 'personalized' : 'global'
--   (前端可用 summary_source 显示 "Personalized" 徽标)
--
--   写入触发:
--     - 用户主动 "Re-summarize with my template" (POST /api/subscriptions/contents/{id}/personalize)
--     - (可选) 加星/收藏 feed 时自动派发 personalize_feed_summary_task
--
-- 设计要点:
--   - 仅存"主观"字段 (summary / reasoning / custom_concepts), 客观字段
--     (sector / industry / ticker) 仍走 subscription_content_tags 全局共享,
--     避免跨用户筛选时出现"用户A筛Tech看不到用户B同条feed"的不一致问题.
--   - UNIQUE(subscription_content_id, user_id): 一用户一条内容最多一份覆盖
--   - template_id 可空: 历史数据 / 模板被删时不阻断
--
-- 关联:
--   - PR-3 已注册 module_code='feed_summary' (见 template_preference_service.py)
--   - PR-7 会创建 API + 前端按钮 + Settings 偏好选择器

CREATE TABLE IF NOT EXISTS subscription_content_user_summaries (
    id SERIAL PRIMARY KEY,
    subscription_content_id INTEGER NOT NULL
        REFERENCES subscription_content(id) ON DELETE CASCADE,
    user_id INTEGER NOT NULL
        REFERENCES users(id) ON DELETE CASCADE,
    template_id INTEGER
        REFERENCES llm_prompt_templates(id) ON DELETE SET NULL,

    -- 主观字段 (per-user 覆盖, 不影响他人视图)
    summary         TEXT NOT NULL,
    reasoning       TEXT,
    custom_concepts JSONB DEFAULT '[]'::jsonb,

    -- AI 元数据 (透明度 + 成本审计)
    -- {total_tokens, total_price, model, provider, response_time, currency}
    ai_metadata     JSONB DEFAULT '{}'::jsonb,

    tagged_at  TIMESTAMP(6) WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP(6) WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP(6) WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT uniq_scus_content_user
        UNIQUE (subscription_content_id, user_id)
);

-- 检索索引
CREATE INDEX IF NOT EXISTS idx_scus_user_id
    ON subscription_content_user_summaries(user_id);
CREATE INDEX IF NOT EXISTS idx_scus_content_id
    ON subscription_content_user_summaries(subscription_content_id);
CREATE INDEX IF NOT EXISTS idx_scus_template_id
    ON subscription_content_user_summaries(template_id);
CREATE INDEX IF NOT EXISTS idx_scus_user_tagged_at
    ON subscription_content_user_summaries(user_id, tagged_at DESC);

-- updated_at 触发器 (复用全局函数, 与 subscription_content_tags 风格一致)
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'update_updated_at_column') THEN
        DROP TRIGGER IF EXISTS update_scus_updated_at
            ON subscription_content_user_summaries;
        CREATE TRIGGER update_scus_updated_at
            BEFORE UPDATE ON subscription_content_user_summaries
            FOR EACH ROW
            EXECUTE FUNCTION update_updated_at_column();
    ELSE
        RAISE NOTICE 'update_updated_at_column() not found, skipping trigger creation. '
                     'Run create_subscription_content_tags.sql first.';
    END IF;
END $$;

-- 注释
COMMENT ON TABLE  subscription_content_user_summaries
    IS 'Feeds per-user 个性化 summary 覆盖层 (与 subscription_content_tags 全局共享层并存)';
COMMENT ON COLUMN subscription_content_user_summaries.subscription_content_id
    IS 'FK → subscription_content.id, CASCADE 删除';
COMMENT ON COLUMN subscription_content_user_summaries.user_id
    IS 'FK → users.id, 个性化所属用户';
COMMENT ON COLUMN subscription_content_user_summaries.template_id
    IS 'FK → llm_prompt_templates.id, 生成本条 summary 时使用的模板; SET NULL 容忍模板删除';
COMMENT ON COLUMN subscription_content_user_summaries.summary
    IS '50-200 字单句个性化摘要; 与 subscription_content_tags.summary 字段对齐';
COMMENT ON COLUMN subscription_content_user_summaries.reasoning
    IS '主观解读 / 投资视角说明 (可选)';
COMMENT ON COLUMN subscription_content_user_summaries.custom_concepts
    IS '用户自定义概念列表 (与全局 concepts 互补, 例如个人关注的细分主题)';
COMMENT ON COLUMN subscription_content_user_summaries.ai_metadata
    IS '{total_tokens, total_price, model, provider, response_time, currency}';

-- =====================================================
-- 验证 (手动跑)
-- =====================================================
-- \d+ subscription_content_user_summaries
--
-- 读取规则示例:
--   SELECT
--     sc.id, sc.title, sc.content,
--     sct.sector, sct.industry, sct.ticker,
--     COALESCE(scus.summary, sct.summary) AS summary,
--     COALESCE(scus.reasoning, sct.reasoning) AS reasoning,
--     CASE WHEN scus.id IS NOT NULL THEN 'personalized' ELSE 'global' END AS summary_source,
--     scus.template_id AS summary_template_id
--   FROM subscription_content sc
--   LEFT JOIN subscription_content_tags sct ON sct.subscription_content_id = sc.id
--   LEFT JOIN subscription_content_user_summaries scus
--     ON scus.subscription_content_id = sc.id AND scus.user_id = $1
--   WHERE $1 = ANY(sc.user_ids)
--   ORDER BY sc.published_at DESC;
