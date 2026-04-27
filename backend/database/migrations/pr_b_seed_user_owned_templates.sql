-- =====================================================
-- PR-B: Seed user-owned templates for all existing users
-- =====================================================
-- 创建时间: 2026-04-27
-- 来源: docs/instructions/LLM_REFACTOR_PLAN.md (Phase 2 / D2=C+B)
--
-- 目的:
--   把"每用户每 USER_OWNED module → 1 行 user-scope 模板"的双态模型一次性
--   回填到生产环境. 9 用户 × 7 USER_OWNED 模块 = 63 候选行, 减去当前已存在的
--   2 行 user-scope (Kevin 的 file_analysis / twitter_phase2) = 61 新增行.
--
-- USER_OWNED 模块 (与 template_preference_service.USER_OWNED_MODULES 对齐):
--   twitter_phase2, youtube_phase2, file_analysis, audio_analysis,
--   file_tagging, feed_summary, youtube_summary
--
-- 命名约定 (新):
--   template_code = 'user_{user_id}_{module_code}'
--   例: user_2_file_analysis, user_8_youtube_summary
--   (旧的 'user_{module_code}_{ts}' 命名仅保留给历史已存在行, 不强制重命名)
--
-- 唯一性保证:
--   依赖 PR-A 创建的 partial unique index uq_user_scope_module
--   (created_by, module_code) WHERE scope='user'
--
-- 安全性:
--   WHERE NOT EXISTS 重复执行无副作用.
--   parent_template_id 字段记录从哪个 system 模板 clone 而来, 便于"重置为默认"功能.
--
-- 事务:
--   本文件 *不* 包含 BEGIN/COMMIT, 由调用方控制 (建议用 psql -1 单事务跑).

-- ==========================================================================
-- 1. 为每个用户在每个 USER_OWNED 模块下 clone 系统默认模板为 user-scope 行
-- ==========================================================================

WITH module_defaults(module_code, default_template_code) AS (
    VALUES
        ('twitter_phase2',  'twitter_daily_phase2_default'),
        ('youtube_phase2',  'youtube_daily_phase2_default'),
        ('file_analysis',   'file_analysis_v1'),
        ('audio_analysis',  'audio_analysis_v1'),
        ('file_tagging',    'content_tag_v1'),
        ('feed_summary',    'feed_summary_v1'),
        ('youtube_summary', 'youtube_summary_v1')
),
system_seeds AS (
    SELECT
        t.id              AS sys_template_id,
        t.template_code   AS sys_template_code,
        t.template_name   AS sys_template_name,
        t.category_id,
        t.module_code,
        t.system_prompt,
        t.user_prompt_template,
        t.variables,
        t.output_format,
        t.recommended_provider,
        t.recommended_model,
        t.temperature,
        t.max_tokens,
        t.tags
    FROM llm_prompt_templates t
    JOIN module_defaults d
      ON d.default_template_code = t.template_code
     AND d.module_code = t.module_code
    WHERE t.scope = 'system'
      AND t.is_active = true
)
INSERT INTO llm_prompt_templates (
    template_code, template_name, category_id, module_code, scope,
    system_prompt, user_prompt_template, variables, output_format,
    recommended_provider, recommended_model, temperature, max_tokens,
    is_active, is_default, is_public,
    description, tags,
    created_by, parent_template_id
)
SELECT
    'user_' || u.id || '_' || s.module_code,
    s.sys_template_name || ' (我的)',
    s.category_id,
    s.module_code,
    'user',
    s.system_prompt,
    s.user_prompt_template,
    s.variables,
    s.output_format,
    s.recommended_provider,
    s.recommended_model,
    s.temperature,
    s.max_tokens,
    true,
    false,
    false,
    '克隆自系统默认 ' || s.sys_template_code || ', 可在 My Templates 自由修改',
    s.tags,
    u.id,
    s.sys_template_id
FROM users u
CROSS JOIN system_seeds s
WHERE NOT EXISTS (
    SELECT 1 FROM llm_prompt_templates ut
    WHERE ut.created_by = u.id
      AND ut.module_code = s.module_code
      AND ut.scope = 'user'
      AND ut.is_active = true
);

-- ==========================================================================
-- 2. 验证
-- ==========================================================================
DO $$
DECLARE
    user_count INT;
    expected_per_user INT := 7;  -- USER_OWNED 模块数量
    user_owned_count INT;
    missing_pairs INT;
    distinct_users INT;
BEGIN
    SELECT COUNT(*) INTO user_count FROM users;

    SELECT COUNT(*) INTO user_owned_count
        FROM llm_prompt_templates
        WHERE scope = 'user'
          AND is_active = true
          AND module_code IN (
              'twitter_phase2', 'youtube_phase2', 'file_analysis',
              'audio_analysis', 'file_tagging', 'feed_summary', 'youtube_summary'
          );

    SELECT COUNT(DISTINCT created_by) INTO distinct_users
        FROM llm_prompt_templates
        WHERE scope = 'user'
          AND is_active = true
          AND module_code IN (
              'twitter_phase2', 'youtube_phase2', 'file_analysis',
              'audio_analysis', 'file_tagging', 'feed_summary', 'youtube_summary'
          );

    -- 每个用户应有 7 个 USER_OWNED 模板
    SELECT COUNT(*) INTO missing_pairs
    FROM (
        SELECT u.id AS user_id, m.module_code
          FROM users u
          CROSS JOIN (VALUES
              ('twitter_phase2'), ('youtube_phase2'), ('file_analysis'),
              ('audio_analysis'), ('file_tagging'), ('feed_summary'),
              ('youtube_summary')
          ) m(module_code)
        EXCEPT
        SELECT created_by, module_code
          FROM llm_prompt_templates
         WHERE scope = 'user' AND is_active = true
    ) gaps;

    RAISE NOTICE '==== PR-B verification ====';
    RAISE NOTICE '  - total users                             : %', user_count;
    RAISE NOTICE '  - total user-owned active templates       : % (expect %)',
                 user_owned_count, user_count * expected_per_user;
    RAISE NOTICE '  - distinct users with user-owned templates: % (expect %)',
                 distinct_users, user_count;
    RAISE NOTICE '  - missing (user, module) pairs            : % (expect 0)', missing_pairs;

    IF missing_pairs <> 0 THEN
        RAISE EXCEPTION 'PR-B verification failed: % missing pairs', missing_pairs;
    END IF;
    IF user_owned_count < user_count * expected_per_user THEN
        RAISE EXCEPTION 'PR-B verification failed: user_owned_count % < expected %',
                        user_owned_count, user_count * expected_per_user;
    END IF;
END $$;

-- 验证 SQL (手动跑):
-- SELECT created_by, module_code, template_code FROM llm_prompt_templates
--  WHERE scope='user' AND is_active=true ORDER BY created_by, module_code;
-- SELECT COUNT(*) FROM llm_prompt_templates WHERE scope='user' AND is_active;
