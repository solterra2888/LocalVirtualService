-- =====================================================
-- Substack Phase 1 — 单 publication 当日摘要 LLM 模板
-- =====================================================
-- 说明: 输入为单个 publication 当天所有文章(post) + 动态(note)，输出为该 publication 核心观点摘要。
--       Phase 2 机械拼接各 publication 摘要。

INSERT INTO llm_prompt_templates (
    template_code, template_name, category_id,
    system_prompt, user_prompt_template,
    variables, output_format,
    recommended_provider, recommended_model,
    temperature, max_tokens,
    scope, module_code,
    is_public, is_active, is_default,
    description, tags
) VALUES (
    'substack_publication_phase1',
    'Substack Phase1 — 单 publication 当日摘要（文章+动态）',
    NULL,

    -- system_prompt
    $SP$你是一名服务于投资组合经理（PM）的研究分析师，专注于从单个 Substack publication 中提炼当日核心投资观点与市场信息。

Substack 是海外主流 Newsletter + Notes 平台，各 publication 由不同分析师、基金经理、研究员运营。每个 publication 当日内容包含两类：
- 文章（post）：长文章，可能为付费订阅专属
- 动态（note）：作者发布的短评论 / 转发，类似短帖

你的任务是基于**单个 publication**当日全部文章与动态，生成精炼的结构化摘要。

输出语言要求:
- 中文投资语境用中文，英文原帖保留英文；Ticker、公司名、人名、英文专有名词保留英文（如 NVDA、FOMC、CPI）
- 保留具体数字（%、点位、估值倍数等），删除空话套话

输出原则:
1. 信息密度优先：每条观点尽量保留可证伪的判断和可操作的结论
2. 区分事实陈述 vs 主观预测 vs 操作建议
3. 多条内容需归纳合并，避免简单罗列原文
4. 文章为主、动态为辅；动态可作补充观点，但勿与文章重复
5. 若当日无实质投资内容，简要标注「无实质投资观点」$SP$,

    -- user_prompt_template
    $UP$请基于以下 Substack publication「{{publication_name}}」在 {{report_date}} 的内容（{{post_count}} 篇文章 + {{note_count}} 条动态），生成该 publication 当日观点摘要。

Publication 名称: {{publication_name}}
日期: {{report_date}}
文章数: {{post_count}}
动态数: {{note_count}}

当日内容（文章 + 动态）:
{{posts_text}}

请输出 Markdown 格式摘要:
- 用 3-8 条 bullet 概括该 publication 当日核心观点
- 每条尽量包含：观点 + 依据/逻辑 + 涉及标的（如有）
- 保留关键原话引语（如有）
- 不要输出跨 publication 综合内容，仅聚焦本 publication$UP$,

    -- variables
    '{
        "publication_name": {"type": "string", "required": true, "description": "publication 名称"},
        "report_date": {"type": "string", "required": true, "description": "报告日期 (YYYY-MM-DD)"},
        "post_count": {"type": "string", "required": true, "description": "当日文章数量"},
        "note_count": {"type": "string", "required": true, "description": "当日动态数量"},
        "posts_text": {"type": "string", "required": true, "description": "当日文章+动态拼接文本"}
    }',

    -- output_format
    '{"type": "markdown"}',

    -- recommended_provider, recommended_model
    NULL, NULL,

    -- temperature, max_tokens
    0.30, 2000,

    -- scope, module_code
    'system', 'substack_summary',

    -- is_public, is_active, is_default
    true, true, true,

    -- description
    'Substack Phase1: 单 publication 当日文章+动态精炼摘要',

    -- tags
    '["substack", "phase1", "publication_summary", "investment_research"]'
)
ON CONFLICT (template_code) DO UPDATE SET
    system_prompt        = EXCLUDED.system_prompt,
    user_prompt_template = EXCLUDED.user_prompt_template,
    temperature          = EXCLUDED.temperature,
    max_tokens           = EXCLUDED.max_tokens,
    recommended_provider = EXCLUDED.recommended_provider,
    recommended_model    = EXCLUDED.recommended_model,
    description          = EXCLUDED.description,
    updated_at           = NOW();
