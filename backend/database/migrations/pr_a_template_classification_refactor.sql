-- =====================================================
-- PR-A: Template classification refactor
-- =====================================================
-- 创建时间: 2026-04-27
-- 来源: docs/instructions/LLM_REFACTOR_PLAN.md (Phase 2 / D1=A, D7=A, D8=B)
--
-- 背景:
--   把 Prompt Template 体系从 system/user/shared 三态简化为
--   "SYSTEM_FIXED 不可删 + USER_OWNED 每用户一份" 双态. 这一 PR 仅做
--   数据模型基础重构, 不影响在跑业务. 后续 PR-B 会基于此做用户模板
--   seeding, PR-C 重构前端.
--
-- 变更总览:
--   1) 归档 3 个孤儿 file_analysis 子模板 (D1=A)
--   2) 把 audio_analysis_v1 从 file_analysis 模块拆出 → audio_analysis 模块 (D8=B)
--   3) 为 feed_summary / youtube_summary 模块种子系统默认模板
--      (PR-B 用户初始化时 clone 自此)
--   4) 加 user-scope 模板唯一约束 (D7=A: 每用户每模块至多 1 行)
--
-- 不在本 PR 的工作:
--   - SYSTEM_ONLY_MODULES 变更 (file_naming/file_classify/news_summary/feed_tagging
--     改为不可定制) → 走 Python 配置 (template_preference_service.py), 无 DDL
--   - 用户私有模板 seeding → PR-B
--   - subscription_content_user_summaries 扩展到 youtube_summary → PR-E
--
-- 安全性:
--   全部 IF NOT EXISTS / WHERE NOT EXISTS, 重复执行无副作用.

BEGIN;

-- ==========================================================================
-- 1. 归档 3 个孤儿 file_analysis 子模板
-- ==========================================================================
-- 这些模板原本提供"财报/行业/投资研究"三种细分分析能力, 但新模型下
-- file_analysis 仅保留单一通用模板 (file_analysis_v1). 用户若需要细分场景,
-- 应通过编辑自己的 user-owned file_analysis 模板 + 模板变量实现.
-- 仅 set is_active=false, 不物理删除, 保留历史 usage_log 关联.

UPDATE llm_prompt_templates
SET is_active = false, updated_at = NOW()
WHERE template_code IN (
    'file_analysis_financial_report_v1',
    'file_analysis_industry_research_v1',
    'file_analysis_investment_research_v1'
)
AND is_active = true;

-- ==========================================================================
-- 2. audio_analysis_v1: file_analysis 模块 → audio_analysis 模块 (D8=B)
-- ==========================================================================
-- 让音频分析与文档分析解耦, 用户可独立编辑各自的模板.
-- 仅当前 module_code 仍为 file_analysis 时迁移, 幂等.

UPDATE llm_prompt_templates
SET module_code = 'audio_analysis', updated_at = NOW()
WHERE template_code = 'audio_analysis_v1'
  AND module_code = 'file_analysis';

-- ==========================================================================
-- 3. 为 feed_summary / youtube_summary 模块种子系统默认模板
-- ==========================================================================
-- 这两个 module 在 MODULE_REGISTRY 已存在 (feed_summary 已有, youtube_summary
-- 是 PR-A 新增), 但没有系统默认模板, 导致用户无法 seed 自己的副本.
-- 这里写入两个 sensible default, PR-B 的 UserTemplateSeederService 会以
-- 此为模板克隆每用户的私有副本.
-- category_id = 18 (content_tagging, 已确认存在).

-- 3.1 feed_summary_v1 — Twitter / 通用 feed item per-user 摘要默认模板
INSERT INTO llm_prompt_templates (
    template_code, template_name, category_id, module_code, scope,
    system_prompt, user_prompt_template,
    variables, output_format,
    recommended_provider, recommended_model, temperature, max_tokens,
    is_active, is_default, is_public, description, tags
)
SELECT
    'feed_summary_v1',
    'Feed 个性化摘要 (默认)',
    18,
    'feed_summary',
    'system',
    '你是一名专业的金融信息整理助手, 服务于二级市场研究员. 输出务必客观、信息密度高, 不堆砌套话.',
    '请为以下来自 {{channel_type}} 的内容生成一段我的个性化摘要.

作者: {{author}}
标题: {{title}}
内容:
{{text}}

请按以下结构输出 (Markdown):
**核心观点**: 一句话, 不超过 30 字
**关键事实**: 3-5 条要点, 每条 ≤ 25 字
**潜在影响**: 从投资视角解读对市场/标的的可能影响, 1-2 句',
    -- variables 元数据 (前端 InsertVariablePopover 用), 由
    -- FeedSummaryPersonalizationService._build_template_variables 注入实际值.
    '{
        "channel_type": {
            "type": "string",
            "required": false,
            "description": "渠道类型 (twitter / youtube / rss 等), 由系统自动注入"
        },
        "author": {
            "type": "string",
            "required": false,
            "description": "Feed 作者名 (Twitter 用户名 / 频道名等)"
        },
        "title": {
            "type": "string",
            "required": false,
            "description": "Feed 标题 (Twitter 通常为空; YouTube/RSS 有标题)"
        },
        "text": {
            "type": "string",
            "required": true,
            "description": "Feed 正文 (Twitter 已合并转推/引用上下文; 其他渠道为内容主体)"
        }
    }'::jsonb,
    '{"type": "text"}'::jsonb,
    'qwen', 'qwen-plus', 0.5, 800,
    true, false, true,
    '系统级 Twitter / 通用 feed item per-user 摘要默认模板. 用户可在 My Templates 中编辑自己的副本.',
    '[]'::jsonb
WHERE NOT EXISTS (
    SELECT 1 FROM llm_prompt_templates WHERE template_code = 'feed_summary_v1'
);

-- 3.2 youtube_summary_v1 — YouTube 视频 per-user 摘要默认模板
INSERT INTO llm_prompt_templates (
    template_code, template_name, category_id, module_code, scope,
    system_prompt, user_prompt_template,
    variables, output_format,
    recommended_provider, recommended_model, temperature, max_tokens,
    is_active, is_default, is_public, description, tags
)
SELECT
    'youtube_summary_v1',
    'YouTube 视频个性化摘要 (默认)',
    18,
    'youtube_summary',
    'system',
    '你是一名专业的视频内容分析助手, 擅长把长视频压缩为高密度的信息卡片, 便于研究员快速判断是否需要观看.',
    '请基于以下 YouTube 视频信息为我生成一份个性化摘要.

频道: {{channel}}
标题: {{title}}
视频描述: {{description}}
字幕/文字稿:
{{transcript}}

请按以下结构输出 (Markdown):
**一句话观点**: ≤ 35 字
**视频要点**: 不超过 5 条, 每条 ≤ 30 字
**关键引用/数据**: 截取视频中提到的具体数字、引语, 各≤ 20 字
**投资视角**: 1-2 句, 该视频对当前市场/标的判断的潜在影响',
    -- variables 元数据 (前端 InsertVariablePopover 用), 由
    -- FeedSummaryPersonalizationService._build_template_variables 注入实际值.
    '{
        "channel": {
            "type": "string",
            "required": false,
            "description": "YouTube 频道名 (来自 subscription_content.author_name)"
        },
        "title": {
            "type": "string",
            "required": false,
            "description": "视频标题"
        },
        "description": {
            "type": "string",
            "required": false,
            "description": "视频描述 / 简介 (来自 subscription_content.content; 可能为空)"
        },
        "transcript": {
            "type": "string",
            "required": true,
            "description": "视频字幕 / 文字稿 (不含标题)"
        }
    }'::jsonb,
    '{"type": "text"}'::jsonb,
    'qwen', 'qwen-plus', 0.5, 1500,
    true, false, true,
    '系统级 YouTube 视频 per-user 摘要默认模板. 用户可在 My Templates 中编辑自己的副本.',
    '[]'::jsonb
WHERE NOT EXISTS (
    SELECT 1 FROM llm_prompt_templates WHERE template_code = 'youtube_summary_v1'
);

-- ==========================================================================
-- 4. user-scope 模板唯一约束 (D7=A)
-- ==========================================================================
-- 强制每个用户每个 module 最多有 1 行 user-scope 模板.
-- 用 partial unique index, 仅约束 scope='user', 不影响 system 模板可多个.
-- 注意: 现有 3 条 user-scope 模板里 user 2 (Kevin) 在 twitter_phase2 模块下
-- 有 2 行 (user_twitter_phase2_1776063604220, user_twitter_phase2_1776063616833),
-- 创建索引会失败. 这里先做去重: 保留 updated_at 最新的一行, 把其他行
-- 设为 inactive 并改 module_code (避免冲突).

WITH ranked AS (
    SELECT
        id,
        ROW_NUMBER() OVER (
            PARTITION BY created_by, module_code
            ORDER BY updated_at DESC, id DESC
        ) AS rn
    FROM llm_prompt_templates
    WHERE scope = 'user'
)
UPDATE llm_prompt_templates t
SET
    is_active = false,
    -- 把多余的旧版本 module_code 改为 NULL, 既不参与新唯一索引也保留历史
    module_code = NULL,
    updated_at = NOW()
FROM ranked r
WHERE t.id = r.id AND r.rn > 1;

CREATE UNIQUE INDEX IF NOT EXISTS uq_user_scope_module
    ON llm_prompt_templates (created_by, module_code)
    WHERE scope = 'user' AND module_code IS NOT NULL;

COMMENT ON INDEX uq_user_scope_module IS
    'PR-A (D7=A): 每用户每 module 最多 1 行 user-scope 模板; 重置=覆盖, 无历史';

-- ==========================================================================
-- 5. 验证
-- ==========================================================================
DO $$
DECLARE
    archived_orphans INT;
    audio_module_count INT;
    feed_summary_seeded INT;
    youtube_summary_seeded INT;
    user_dupes INT;
    distinct_modules INT;
BEGIN
    SELECT COUNT(*) INTO archived_orphans
        FROM llm_prompt_templates
        WHERE template_code IN (
            'file_analysis_financial_report_v1',
            'file_analysis_industry_research_v1',
            'file_analysis_investment_research_v1'
        ) AND is_active = false;

    SELECT COUNT(*) INTO audio_module_count
        FROM llm_prompt_templates
        WHERE module_code = 'audio_analysis' AND is_active = true;

    SELECT COUNT(*) INTO feed_summary_seeded
        FROM llm_prompt_templates WHERE template_code = 'feed_summary_v1';

    SELECT COUNT(*) INTO youtube_summary_seeded
        FROM llm_prompt_templates WHERE template_code = 'youtube_summary_v1';

    SELECT COUNT(*) INTO user_dupes
        FROM (
            SELECT created_by, module_code, COUNT(*) AS cnt
              FROM llm_prompt_templates
             WHERE scope = 'user' AND module_code IS NOT NULL
             GROUP BY created_by, module_code
            HAVING COUNT(*) > 1
        ) sub;

    SELECT COUNT(DISTINCT module_code) INTO distinct_modules
        FROM llm_prompt_templates
        WHERE module_code IS NOT NULL AND is_active = true;

    RAISE NOTICE '==== PR-A verification ====';
    RAISE NOTICE '  - 3 orphan templates archived         : % (expect 3)', archived_orphans;
    RAISE NOTICE '  - audio_analysis module templates     : % (expect 1)', audio_module_count;
    RAISE NOTICE '  - feed_summary_v1 seeded              : % (expect 1)', feed_summary_seeded;
    RAISE NOTICE '  - youtube_summary_v1 seeded           : % (expect 1)', youtube_summary_seeded;
    RAISE NOTICE '  - user-scope (created_by, module_code) duplicates: % (expect 0)', user_dupes;
    RAISE NOTICE '  - distinct active module_codes        : %', distinct_modules;

    IF archived_orphans <> 3 OR audio_module_count <> 1
       OR feed_summary_seeded <> 1 OR youtube_summary_seeded <> 1
       OR user_dupes <> 0 THEN
        RAISE EXCEPTION 'PR-A verification failed; rolling back';
    END IF;
END $$;

COMMIT;

-- ==========================================================================
-- 验证 SQL (手动跑)
-- ==========================================================================
-- SELECT module_code, scope, COUNT(*) FROM llm_prompt_templates
--   WHERE is_active GROUP BY module_code, scope ORDER BY module_code, scope;
-- SELECT created_by, module_code, COUNT(*) FROM llm_prompt_templates
--   WHERE scope='user' GROUP BY created_by, module_code HAVING COUNT(*) > 1;
