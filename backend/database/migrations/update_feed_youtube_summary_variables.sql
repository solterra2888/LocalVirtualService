-- =====================================================
-- Backfill variables metadata for feed_summary / youtube_summary templates
-- =====================================================
-- 创建时间: 2026-04-29
-- 背景:
--   pr_a_template_classification_refactor.sql 创建 feed_summary_v1 / youtube_summary_v1
--   时把 variables 字段写成 '{}'::jsonb (空对象), 同时 PR-B 的
--   UserTemplateSeederService 把同样的空 variables 复制到了所有用户的私有副本.
--
--   导致前端 InsertVariablePopover 显示 "该模板未定义变量",
--   用户在 PromptTemplatesPage 编辑模板时不知道有哪些占位符可用.
--
--   同时, 服务层 (PR-A 方案 B) 已经按渠道注入完整变量到 LLM 调用的
--   custom_variables, 但模板里没有 variables 元数据 → UI 上看不到.
--
--   本迁移把 variables 元数据回填到所有 feed_summary / youtube_summary
--   作用域的模板上 (system 种子 + 全部 user 副本), 让 UI 可见 + 与 service
--   层的占位符注入对齐.
--
-- 注意:
--   - 不修改 user_prompt_template / system_prompt 文本本身,
--     仅更新 variables 列, 用户已经手动改过 prompt 文本的副本不会被破坏.
--   - 仅在 variables 字段为空 ('{}' / NULL) 时回填, 避免覆盖用户后续可能
--     手动添加的变量定义.
-- =====================================================

BEGIN;

-- 1. feed_summary 模块: 系统种子 + 所有用户副本
UPDATE llm_prompt_templates
SET
    variables = '{
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
    updated_at = CURRENT_TIMESTAMP
WHERE module_code = 'feed_summary'
  AND (variables IS NULL OR variables = '{}'::jsonb);

-- 2. youtube_summary 模块: 系统种子 + 所有用户副本
UPDATE llm_prompt_templates
SET
    variables = '{
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
    updated_at = CURRENT_TIMESTAMP
WHERE module_code = 'youtube_summary'
  AND (variables IS NULL OR variables = '{}'::jsonb);

-- =====================================================
-- 验证
-- =====================================================
DO $$
DECLARE
    feed_summary_total INT;
    feed_summary_filled INT;
    youtube_summary_total INT;
    youtube_summary_filled INT;
BEGIN
    SELECT COUNT(*) INTO feed_summary_total
        FROM llm_prompt_templates WHERE module_code = 'feed_summary';
    SELECT COUNT(*) INTO feed_summary_filled
        FROM llm_prompt_templates
        WHERE module_code = 'feed_summary'
          AND variables ? 'text';

    SELECT COUNT(*) INTO youtube_summary_total
        FROM llm_prompt_templates WHERE module_code = 'youtube_summary';
    SELECT COUNT(*) INTO youtube_summary_filled
        FROM llm_prompt_templates
        WHERE module_code = 'youtube_summary'
          AND variables ? 'transcript';

    RAISE NOTICE '=== Variables Backfill Summary ===';
    RAISE NOTICE '  feed_summary    : % / % rows have variables.text',
        feed_summary_filled, feed_summary_total;
    RAISE NOTICE '  youtube_summary : % / % rows have variables.transcript',
        youtube_summary_filled, youtube_summary_total;

    IF feed_summary_filled < feed_summary_total
       OR youtube_summary_filled < youtube_summary_total THEN
        RAISE WARNING '部分模板未被回填 (variables 字段非空且未声明 text/transcript). '
                      '请人工检查这些行是否被用户自定义覆盖.';
    END IF;
END $$;

COMMIT;
