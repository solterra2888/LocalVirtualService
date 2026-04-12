-- =====================================================
-- Auto Name V1 — LLM Prompt Template 种子数据
-- =====================================================
-- 创建时间: 2026-04-12
-- 说明: 插入文件自动命名所需的 prompt 模板。
--       模板变量使用 {{variable_name}} 格式，由 prompt_renderer 渲染。
--       若已存在同名 template_code，则跳过（ON CONFLICT DO NOTHING）。

INSERT INTO llm_prompt_templates (
    template_code, template_name, category_id,
    system_prompt, user_prompt_template,
    variables, output_format,
    recommended_provider, recommended_model,
    temperature, max_tokens,
    scope, is_public, is_active, is_default,
    description, tags
) VALUES (
    'auto_name_v1',
    '文件自动命名 V1',
    NULL,
    '你是一位文件命名专家。你的任务是根据文件内容生成一个简洁、有意义的中文标题作为文件名。

规则：
1. 标题应准确概括文件的核心主题，长度控制在 5-25 个字之间
2. 使用中文，简洁明了，避免冗余修饰词
3. 不要包含文件扩展名（如 .pdf、.docx 等）
4. 不要包含日期前缀、编号等无关信息
5. 不要使用引号、括号或其他特殊标点符号包裹标题
6. 直接输出标题文本，不要输出任何解释、前缀或额外内容
7. 如果内容是音频转写或视频字幕，根据讨论主题命名，而不是"某某访谈录音"这种形式
8. 如果内容是网页文章，提取文章核心主题命名',
    '请根据以下文件内容生成一个合适的文件名标题。

文件类型: {{file_type}}
原始文件名: {{original_filename}}

文件内容（部分）:
{{file_content}}

请直接输出文件名标题（不含扩展名），不要输出其他任何内容。',
    '{"file_content": {"type": "string", "required": true, "description": "文件内容（截断后）"}, "file_type": {"type": "string", "required": true, "description": "文件类型（pdf/doc/excel/audio/youtube/web）"}, "original_filename": {"type": "string", "required": false, "default": "", "description": "原始文件名"}}',
    '{"type": "text"}',
    NULL, NULL,
    0.3, 100,
    'system', true, true, true,
    '根据文件内容自动生成简洁有意义的中文文件名',
    '["auto_name", "file_naming", "utility"]'
)
ON CONFLICT (template_code) DO NOTHING;
