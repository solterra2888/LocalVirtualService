-- =====================================================
-- 知识星球 Phase 1 — 单星球话题摘要 LLM 模板
-- =====================================================
-- 说明: 输入为单个星球当天所有话题，输出为该星球核心观点摘要。
--       Phase 2 机械拼接各星球摘要，不再使用 zhishixingqiu_daily_brief。

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
    'zhishixingqiu_group_phase1',
    '知识星球 Phase1 — 单星球当日话题摘要',
    NULL,

    -- system_prompt
    $SP$你是一名服务于投资组合经理（PM）的研究分析师，专注于从知识星球（付费社群）单个星球中提炼当日核心投资观点与市场信息。

知识星球是中文投资社群平台，各星球由不同分析师、基金经理、研究员运营，内容涵盖宏观、行业、个股、策略等。

你的任务是基于**单个星球**当日全部话题，生成精炼的结构化摘要。

输出语言要求:
- 主体使用中文
- Ticker、公司名、人名、英文专有名词保留英文（如 NVDA、FOMC、CPI）
- 保留具体数字（%、点位、估值倍数等），删除空话套话

输出原则:
1. 信息密度优先：每条观点尽量保留可证伪的判断和可操作的结论
2. 区分事实陈述 vs 主观预测 vs 操作建议
3. 多条话题需归纳合并，避免简单罗列原文
4. 若当日仅闲聊/无投资相关内容，简要标注「无实质投资观点」$SP$,

    -- user_prompt_template
    $UP$请基于以下知识星球「{{group_name}}」在 {{report_date}} 的 {{topic_count}} 条话题，生成该星球当日观点摘要。

星球名称: {{group_name}}
日期: {{report_date}}
话题数: {{topic_count}}

当日话题:
{{topics_text}}

请输出 Markdown 格式摘要:
- 用 3-8 条 bullet 概括该星球当日核心观点
- 每条尽量包含：观点 + 依据/逻辑 + 涉及标的（如有）
- 保留关键原话引语（如有）
- 不要输出跨星球综合内容，仅聚焦本星球$UP$,

    -- variables
    '{
        "group_name": {"type": "string", "required": true, "description": "星球名称"},
        "report_date": {"type": "string", "required": true, "description": "报告日期 (YYYY-MM-DD)"},
        "topic_count": {"type": "string", "required": true, "description": "当日话题数量"},
        "topics_text": {"type": "string", "required": true, "description": "该星球当日话题文本"}
    }',

    -- output_format
    '{"type": "markdown"}',

    -- recommended_provider, recommended_model
    NULL, NULL,

    -- temperature, max_tokens
    0.30, 2000,

    -- scope, module_code
    'system', 'zhishixingqiu_summary',

    -- is_public, is_active, is_default
    true, true, true,

    -- description
    '知识星球 Phase1: 单星球当日话题精炼摘要',

    -- tags
    '["zhishixingqiu", "phase1", "group_summary", "investment_research"]'
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
