-- =====================================================
-- 优化新闻分析Prompt模板
-- 创建日期: 2025-10-12
-- 目的: 减少触发LLM内容审查的可能性
-- =====================================================

-- 更新news_summary_v1模板，使用更中性、客观的表述
-- 避免可能触发内容审查的敏感词汇和表达方式

UPDATE llm_prompt_templates
SET 
    system_prompt = '你是一位专业的金融信息分析助手。你的任务是客观、中立地分析金融新闻内容，提取关键信息，并进行专业评估。

重要原则：
1. 保持客观中立，避免主观判断和情绪化表达
2. 使用专业金融术语，避免夸张或极端的描述
3. 聚焦于事实和数据，减少推测性内容
4. 对市场影响的评估要基于客观分析
5. 避免使用可能引起争议的词汇',

    user_prompt_template = '请对以下金融新闻进行专业分析：

【新闻标题】
{{title}}

【新闻内容】
{{content}}

【相关股票代码】
{{symbols}}

请按照以下格式输出分析结果（限{{max_length}}字以内）：

1. 核心要点
- 列出3-5个关键信息点
- 使用客观、中立的表述
- 避免情绪化词汇

2. 涉及的关键主体
- 列出新闻中提到的主要公司、机构或个人

3. 重要数字和数据
- 提取新闻中的关键数字、百分比、时间等量化信息

4. 市场影响评估
- 评估方向：正面/负面/中性
- 影响程度：1-5分（1=极小，5=重大）
- 简要说明理由（基于客观事实）

5. 一句话总结
- 用一句话客观总结新闻的核心内容

注意事项：
- 保持专业、客观的分析态度
- 避免使用极端或情绪化的词汇
- 聚焦于可验证的事实和数据
- 对不确定的信息要明确标注',

    description = '优化版新闻分析模板 - 减少内容审查失败率
主要改进：
1. 强化客观中立的要求
2. 避免使用可能触发审查的敏感词汇
3. 更注重事实和数据的提取
4. 规范化输出格式
5. 明确禁止情绪化表达',

    updated_at = CURRENT_TIMESTAMP
WHERE template_code = 'news_summary_v1';

-- 创建一个新的备用模板（更保守的版本）
INSERT INTO llm_prompt_templates (
    template_code,
    template_name,
    category_id,
    system_prompt,
    user_prompt_template,
    variables,
    output_format,
    recommended_provider,
    recommended_model,
    temperature,
    max_tokens,
    scope,
    is_public,
    is_active,
    description,
    tags
)
VALUES (
    'news_summary_safe_v1',
    '新闻分析模板（保守版）',
    (SELECT id FROM llm_prompt_categories WHERE category_code = 'news_analysis' LIMIT 1),
    
    '你是一位金融信息整理助手。请客观整理新闻内容中的关键信息。',
    
    '请整理以下新闻的关键信息：

标题：{{title}}
内容：{{content}}
股票代码：{{symbols}}

请提取以下信息（限{{max_length}}字）：
1. 主要事件
2. 涉及主体
3. 关键数据
4. 市场方向（仅标注：积极/消极/中性）
5. 简要概括',
    
    '{
        "title": {"type": "string", "description": "新闻标题"},
        "content": {"type": "string", "description": "新闻内容"},
        "symbols": {"type": "string", "description": "相关股票代码"},
        "max_length": {"type": "integer", "description": "最大字数", "default": 100}
    }'::jsonb,
    
    '{}'::jsonb,
    
    'doubao',
    'doubao-seed-1-6-250615',
    0.3,
    2000,
    'system',
    true,
    true,
    
    '极度保守的新闻分析模板，最大程度避免内容审查问题。
特点：
1. 极简化的系统提示
2. 最小化主观表达
3. 只做信息提取，不做评论
4. 更低的temperature参数
5. 适用于敏感新闻内容',
    
    '["news", "analysis", "safe", "conservative"]'::jsonb
)
ON CONFLICT (template_code) DO UPDATE SET
    updated_at = CURRENT_TIMESTAMP;

-- 添加模板切换逻辑的注释
COMMENT ON TABLE llm_prompt_templates IS '
Prompt模板管理表

内容审查失败时的处理策略：
1. 首次失败：使用原模板重试（可能是临时问题）
2. 再次失败：切换到保守版模板（news_summary_safe_v1）
3. 仍然失败：标记为内容敏感，需要人工审核
';


