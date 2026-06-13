-- =====================================================
-- 知识星球每日摘要 — LLM Prompt Template
-- =====================================================
-- 说明: 输入为按星球分组拼接的当天话题文本，输出为按星球分段的聚合摘要.
--       使用 ON CONFLICT DO UPDATE 便于后续迭代 prompt.

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
    'zhishixingqiu_daily_brief',
    '知识星球每日摘要 — 按星球聚合观点',
    NULL,

    -- system_prompt
    $SP$你是一名服务于投资组合经理（PM）的研究分析师，专注于从知识星球（付费社群）中提炼当日各星球的核心投资观点与市场信息。

知识星球是中文投资社群平台，各星球由不同分析师、基金经理、研究员运营，内容涵盖宏观、行业、个股、策略等，信息密度高且观点多元。

你的任务是基于当日各星球话题，生成一份结构化每日摘要。输出必须**按星球分段**，并在文末给出跨星球综合提要。

输出语言要求:
- 主体使用中文
- Ticker、公司名、人名、英文专有名词保留英文（如 NVDA、FOMC、CPI）
- 保留具体数字（%、点位、估值倍数等），删除空话套话

输出原则:
1. 信息密度优先：每条观点尽量保留可证伪的判断和可操作的结论
2. 区分事实陈述 vs 主观预测 vs 操作建议
3. 同一星球内多条话题需归纳合并，避免简单罗列原文
4. 跨星球出现相同主题时，在综合提要中标注共识与分歧
5. 若某星球当日仅闲聊/无投资相关内容，可简要标注「无实质投资观点」$SP$,

    -- user_prompt_template
    $UP$请基于以下知识星球当日话题（已按星球分组）生成 {{report_date}} 的每日摘要。

日期: {{report_date}}

各星球话题:
{{topics_by_group}}

请严格按以下结构输出 Markdown 格式摘要:

## 按星球观点

对每个有实质内容的星球，使用二级标题:
### {星球名称}
- 用 3-6 条 bullet 概括该星球当日核心观点
- 每条尽量包含：观点 + 依据/逻辑 + 涉及标的（如有）
- 保留关键原话引语（如有）

（仅输出当日有话题的星球；无实质投资内容的星球可一行带过）

## 跨星球综合提要

用 1-2 段话总结:
- 今日各星球共同关注的主线主题
- 跨星球的主要共识
- 跨星球的主要分歧（如有）
- PM 明日应重点跟踪的 1-2 件事$UP$,

    -- variables
    '{
        "report_date": {"type": "string", "required": true, "description": "报告日期 (YYYY-MM-DD)"},
        "topics_by_group": {"type": "string", "required": true, "description": "按星球分组拼接的当日话题文本"}
    }',

    -- output_format
    '{"type": "markdown"}',

    -- recommended_provider, recommended_model
    NULL, NULL,

    -- temperature, max_tokens
    0.30, 4000,

    -- scope, module_code
    'system', 'zhishixingqiu_summary',

    -- is_public, is_active, is_default
    true, true, true,

    -- description
    '知识星球每日摘要: 按星球分组聚合当天话题观点，附跨星球综合提要',

    -- tags
    '["zhishixingqiu", "daily_summary", "group_aggregation", "investment_research"]'
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
