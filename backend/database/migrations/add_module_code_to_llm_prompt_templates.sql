-- =====================================================
-- llm_prompt_templates: 增加 module_code 字段
-- =====================================================
-- 创建时间: 2026-04-27
-- 来源: docs/instructions/LLM_REFACTOR_PLAN.md §3.3 (PR-3 ①)
--
-- 背景:
--   现有 user_template_preferences.module_code 是"用户偏好与业务模块"的绑定键,
--   而 llm_prompt_templates.category_id (FK → llm_prompt_categories) 仅是 UI
--   分类目录. 两者并非一一对应:
--     - 一个 category 可能对应多个 module (例如 file_analysis category 同时
--       服务 file_analysis module 和音频自动分析)
--     - 用户私有模板 (scope='user') 没法仅靠 category_code 推断属于哪个 module
--
--   引入 module_code 列后:
--     - 前端 TemplateSelector 改为按 module_code 过滤 (而非 category_code)
--     - LLMTemplateResolver (PR-3 ②) 可直接按 module_code 查找模板
--     - category_id 保留, 仅作 UI 分组
--
-- 已知 module_code 取值 (与 backend/services/template_preference_service.py
-- 的 MODULE_REGISTRY 一一对齐, PR-3 ③):
--   file_analysis, file_tagging, file_naming, file_classify,
--   feed_tagging, feed_summary,
--   twitter_phase1, twitter_phase2,
--   youtube_phase1, youtube_phase2,
--   news_summary, news_classification,
--   theme_description
--
-- 数据迁移策略:
--   - 系统模板 (scope='system'): 按 template_code 精确匹配赋值
--   - 用户模板 (scope='user'): 按 'user_{module_code}_{timestamp}' 命名约定
--     提取 module_code, fallback 用 category_code

-- ==========================================================================
-- 1. 添加 module_code 列 (允许 NULL, 历史数据迁移后再视情况收紧)
-- ==========================================================================

ALTER TABLE llm_prompt_templates
    ADD COLUMN IF NOT EXISTS module_code VARCHAR(64);

COMMENT ON COLUMN llm_prompt_templates.module_code
    IS '业务模块标识, 与 user_template_preferences.module_code 对齐; 用于 LLMTemplateResolver 解析模板';

CREATE INDEX IF NOT EXISTS idx_lpt_module_code
    ON llm_prompt_templates(module_code);
CREATE INDEX IF NOT EXISTS idx_lpt_module_active
    ON llm_prompt_templates(module_code, is_active);

-- ==========================================================================
-- 2. 数据迁移: 系统模板 (scope='system') 按 template_code 精确匹配
-- ==========================================================================
-- 仅在 module_code 为 NULL 时回填, 不覆盖已有值 (允许重复执行)

-- File 模块
UPDATE llm_prompt_templates SET module_code = 'file_tagging'
    WHERE template_code = 'content_tag_v1' AND module_code IS NULL;

UPDATE llm_prompt_templates SET module_code = 'file_naming'
    WHERE template_code = 'auto_name_v1' AND module_code IS NULL;

UPDATE llm_prompt_templates SET module_code = 'file_classify'
    WHERE template_code = 'auto_classify_v1' AND module_code IS NULL;

UPDATE llm_prompt_templates SET module_code = 'file_analysis'
    WHERE template_code IN (
        'audio_analysis_v1',
        'file_analysis_v1',
        'file_analysis_financial_report_v1',
        'file_analysis_industry_research_v1',
        'file_analysis_investment_research_v1'
    ) AND module_code IS NULL;

-- Feed 模块
UPDATE llm_prompt_templates SET module_code = 'feed_tagging'
    WHERE template_code = 'content_tag_feeds_v1' AND module_code IS NULL;

-- Daily Report 模块
UPDATE llm_prompt_templates SET module_code = 'twitter_phase1'
    WHERE template_code = 'twitter_daily_phase1_default' AND module_code IS NULL;

UPDATE llm_prompt_templates SET module_code = 'twitter_phase2'
    WHERE template_code = 'twitter_daily_phase2_default' AND module_code IS NULL;

UPDATE llm_prompt_templates SET module_code = 'youtube_phase1'
    WHERE template_code = 'youtube_daily_phase1_default' AND module_code IS NULL;

UPDATE llm_prompt_templates SET module_code = 'youtube_phase2'
    WHERE template_code = 'youtube_daily_phase2_default' AND module_code IS NULL;

-- News 模块
UPDATE llm_prompt_templates SET module_code = 'news_summary'
    WHERE template_code IN ('news_summary_v1', 'news_summary_safe_v1', 'news_cctv_summary_v1')
      AND module_code IS NULL;

UPDATE llm_prompt_templates SET module_code = 'news_classification'
    WHERE template_code = 'news_classification' AND module_code IS NULL;

-- Theme 模块
UPDATE llm_prompt_templates SET module_code = 'theme_description'
    WHERE template_code = 'theme_description_generator_v1' AND module_code IS NULL;

-- ==========================================================================
-- 3. 数据迁移: 用户模板 (scope='user') 按命名约定推断
-- ==========================================================================
-- 约定: 用户模板 template_code 格式为 'user_{module_code}_{timestamp}'
--   例如: user_twitter_phase2_1775224220111  → module_code='twitter_phase2'
--         user_file_analysis_1776595968028   → module_code='file_analysis'
-- 注意: module_code 本身可能含下划线 (twitter_phase2, file_analysis), 不能用
--       SPLIT_PART(_,2) 简单切. 这里用 substring + regex 提取.

UPDATE llm_prompt_templates
SET module_code = (
    -- 去掉前缀 'user_' 和尾部 '_<纯数字时间戳>'
    -- 正则: 匹配 'user_(.*)_\d+$', 捕获中间部分
    substring(template_code FROM 'user_(.*)_[0-9]+$')
)
WHERE scope = 'user'
  AND module_code IS NULL
  AND template_code ~ '^user_.+_[0-9]+$';

-- ==========================================================================
-- 4. 兜底: 仍未赋值的模板 (用 category_code 作为最后回退)
-- ==========================================================================
-- 例如: 自定义未按命名约定的模板, 或 category 与 module 不一致的边缘情况

UPDATE llm_prompt_templates t
SET module_code = c.category_code
FROM llm_prompt_categories c
WHERE t.category_id = c.id
  AND t.module_code IS NULL
  AND c.category_code IS NOT NULL;

-- ==========================================================================
-- 5. 验证 (DO 块输出统计, 不影响事务)
-- ==========================================================================

DO $$
DECLARE
    total_count INT;
    null_count INT;
    distinct_modules INT;
BEGIN
    SELECT COUNT(*) INTO total_count FROM llm_prompt_templates;
    SELECT COUNT(*) INTO null_count FROM llm_prompt_templates WHERE module_code IS NULL;
    SELECT COUNT(DISTINCT module_code) INTO distinct_modules FROM llm_prompt_templates WHERE module_code IS NOT NULL;

    RAISE NOTICE 'llm_prompt_templates module_code migration: total=%, null=%, distinct_modules=%',
                 total_count, null_count, distinct_modules;

    IF null_count > 0 THEN
        RAISE NOTICE 'WARNING: % templates still have NULL module_code, run this query to inspect:',
                     null_count;
        RAISE NOTICE '  SELECT id, template_code, scope FROM llm_prompt_templates WHERE module_code IS NULL;';
    END IF;
END $$;

-- ==========================================================================
-- 验证 SQL (手动跑)
-- ==========================================================================
-- SELECT module_code, COUNT(*) FROM llm_prompt_templates GROUP BY module_code ORDER BY module_code;
-- SELECT id, template_code, scope, module_code FROM llm_prompt_templates WHERE module_code IS NULL;
