-- =====================================================
-- Auto Classify V1 — LLM Prompt Template 种子数据
-- =====================================================
-- 创建时间: 2026-04-13
-- 说明: 插入文件自动归类所需的 prompt 模板。
--       根据文件内容和用户文件夹列表，自动将文件归入最匹配的文件夹。
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
    'auto_classify_v1',
    '文件自动归类 V1',
    NULL,
    '你是一位文件分类专家。你的任务是根据文件内容，将文件归入用户预定义的文件夹中最匹配的一个。

规则：
1. 你会收到一组文件夹名称和一段文件内容
2. 你必须从给定的文件夹列表中选择最匹配的一个
3. 输出必须严格等于文件夹列表中的某一个名称，不可修改、翻译或创造新名称
4. 如果文件内容无法明确归入任何文件夹，输出 Uncategorized
5. 只输出文件夹名称，不要输出任何解释、标点或额外内容

分类参考（仅供理解文件夹语义，实际以用户文件夹名称为准）：
- 会议纪要：会议记录、电话会议、讨论纪要、earnings call 等
- 新闻：新闻报道、媒体文章、行业动态、市场快讯等
- 投资策略：投资建议、策略分析、资产配置、宏观展望等
- 研究报告：深度研究、行业分析、公司研报、数据分析报告等
- 复盘：交易复盘、操作总结、盈亏分析、持仓回顾等',
    '可选的文件夹列表：
{{folder_list}}

文件类型: {{file_type}}
原始文件名: {{original_filename}}

文件内容（部分）:
{{file_content}}

请从上述文件夹列表中选择最匹配的一个，直接输出文件夹名称。如果无法归类，输出 Uncategorized。',
    '{"file_content": {"type": "string", "required": true, "description": "文件内容（截断后）"}, "folder_list": {"type": "string", "required": true, "description": "用户文件夹名称列表，逗号分隔"}, "file_type": {"type": "string", "required": true, "description": "文件类型（pdf/doc/excel/audio/youtube/web）"}, "original_filename": {"type": "string", "required": false, "default": "", "description": "原始文件名"}}',
    '{"type": "text"}',
    NULL, NULL,
    0.1, 50,
    'system', true, true, true,
    '根据文件内容和用户文件夹列表，自动将文件归入最匹配的文件夹',
    '["auto_classify", "file_folder", "utility"]'
)
ON CONFLICT (template_code) DO NOTHING;
