-- =====================================================
-- 添加CCTV新闻分析Prompt模板
-- 创建日期: 2025-11-25
-- 目的: 为CCTV新闻联播创建专门的LLM分析模板
-- =====================================================

-- 插入CCTV新闻分析模板
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
    is_default,
    description,
    tags,
    example_input,
    example_output
)
VALUES (
    'news_cctv_summary_v1',
    'CCTV新闻联播分析模板',
    (SELECT id FROM llm_prompt_categories WHERE category_code = 'news_analysis' LIMIT 1),
    
    -- System Prompt
    '你是一位专业的宏观经济和政策分析助手。你的任务是客观、专业地分析CCTV新闻联播的内容，提取关键的政策信息、经济数据和市场方向。

重要原则：
1. 保持客观中立，聚焦于政策和经济层面的分析
2. 使用专业的经济金融术语
3. 重点关注对资本市场和产业的潜在影响
4. 提取关键的数字、政策信号和时间节点
5. 将宏观政策与微观市场联系起来',
    
    -- User Prompt Template
    '请对以下CCTV新闻联播内容进行专业分析：

【新闻标题】
{{title}}

【新闻内容】
{{content}}

【新闻日期】
{{date}}

请按照以下格式输出分析结果（限{{max_length}}字以内）：

1. 核心政策要点
- 列出3-5个关键政策信息或经济信号
- 使用客观、专业的表述
- 突出政策的重点和方向

2. 涉及的重点领域和行业
- 列出新闻中提到的主要产业、行业或经济领域
- 标注是否有明确的政策支持或引导

3. 关键数字和目标
- 提取新闻中的重要数字、百分比、时间节点
- 包括经济增长目标、政策指标等

4. 市场和产业影响评估
- 影响方向：积极/中性/观察
- 影响强度：1-5分（1=信息性，5=重大政策）
- 潜在受益行业或概念板块
- 简要说明影响逻辑

5. 投资主题和关键词
- 提取3-5个相关的投资主题或概念（如：新能源、数字经济、先进制造等）
- 列出3-5个关键词标签（用于主题分类）

6. 一句话总结
- 用一句话客观总结新闻的核心信息和市场意义

注意事项：
- 保持专业、客观的分析态度
- 聚焦于政策信号和经济含义
- 避免过度解读或主观臆测
- 对于政策预期要明确标注不确定性',
    
    -- Variables JSON
    '{
        "title": {
            "type": "string", 
            "required": true,
            "description": "新闻标题"
        },
        "content": {
            "type": "string", 
            "required": true,
            "description": "新闻正文内容"
        },
        "date": {
            "type": "string",
            "required": false,
            "description": "新闻发布日期"
        },
        "max_length": {
            "type": "integer", 
            "required": false,
            "default": 150,
            "description": "分析结果最大字数"
        }
    }'::jsonb,
    
    -- Output Format
    '{
        "type": "text",
        "json_schema": null
    }'::jsonb,
    
    -- LLM Configuration
    'qwen',  -- 推荐使用通义千问（适合中文政策分析）
    'qwen-plus',  -- 使用plus模型以获得更好的分析能力
    0.5,  -- 较低温度以保持客观性
    4000,  -- 较大token数以支持详细分析
    
    -- Scope and visibility
    'system',
    true,  -- is_public
    true,  -- is_active
    false, -- is_default
    
    -- Description
    'CCTV新闻联播专用分析模板
    
特点：
1. 针对CCTV新闻联播的内容特点优化
2. 聚焦政策信号和宏观经济信息
3. 提取投资主题和行业影响
4. 自动识别关键的经济数据和政策目标
5. 将宏观新闻与资本市场联系起来

适用场景：
- CCTV新闻联播的自动化分析
- 政策新闻的快速解读
- 宏观经济信息的结构化提取
- 投资主题的自动识别

输出结构：
- 政策要点
- 涉及领域
- 关键数据
- 市场影响
- 投资主题
- 一句话总结',
    
    -- Tags
    '["cctv", "news", "policy", "macro", "analysis", "china"]'::jsonb,
    
    -- Example Input
    '{
        "title": "【新思想引领新征程·非凡十四五】我国加快建设现代化产业体系",
        "content": "习近平总书记指出，现代化产业体系是现代化国家的物质技术基础，必须把发展经济的着力点放在实体经济上。十四五期间，我国以科技创新为引领、以实体经济为根基，加快建设现代化产业体系。传统产业高端化、智能化转型成效显著，超过八成的钢铁企业建成智能集控中心。",
        "date": "2025-11-24",
        "max_length": 150
    }'::jsonb,
    
    -- Example Output
    '1. 核心政策要点
- 强调现代化产业体系建设，突出实体经济的重要性
- 以科技创新为引领推动产业升级
- 传统产业加速智能化、高端化转型
- 八成钢铁企业实现智能化，展现数字化转型成果

2. 涉及的重点领域和行业
- 先进制造业（有明确政策支持）
- 工业互联网和数字化转型（重点发展方向）
- 钢铁、装备制造等传统产业（智能化升级）

3. 关键数字和目标
- 八成钢铁企业建成智能集控中心
- "十四五"期间持续推进
- 智能工厂和数字化车间建设加速

4. 市场和产业影响评估
- 影响方向：积极
- 影响强度：4分（重要政策导向）
- 潜在受益：工业互联网、智能制造、高端装备、工业软件
- 影响逻辑：政策明确支持传统产业智能化升级，相关技术和解决方案提供商将受益

5. 投资主题和关键词
- 投资主题：智能制造、工业互联网、数字化转型、先进制造、实体经济
- 关键词：现代化产业体系、科技创新、智能化、高端化、实体经济

6. 一句话总结
十四五期间我国加快建设现代化产业体系，传统产业智能化转型成效显著，智能制造和工业互联网迎来发展机遇。'
)
ON CONFLICT (template_code) DO UPDATE SET
    template_name = EXCLUDED.template_name,
    system_prompt = EXCLUDED.system_prompt,
    user_prompt_template = EXCLUDED.user_prompt_template,
    variables = EXCLUDED.variables,
    output_format = EXCLUDED.output_format,
    recommended_provider = EXCLUDED.recommended_provider,
    recommended_model = EXCLUDED.recommended_model,
    temperature = EXCLUDED.temperature,
    max_tokens = EXCLUDED.max_tokens,
    description = EXCLUDED.description,
    tags = EXCLUDED.tags,
    example_input = EXCLUDED.example_input,
    example_output = EXCLUDED.example_output,
    is_active = EXCLUDED.is_active,
    updated_at = CURRENT_TIMESTAMP;

-- 添加注释
COMMENT ON TABLE llm_prompt_templates IS 'LLM Prompt模板表 - 包含news_cctv_summary_v1专用于CCTV新闻联播分析';

-- 验证插入
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM llm_prompt_templates WHERE template_code = 'news_cctv_summary_v1') THEN
        RAISE NOTICE '✅ CCTV新闻分析模板创建成功！';
        RAISE NOTICE '   模板代码: news_cctv_summary_v1';
        RAISE NOTICE '   推荐模型: qwen-plus';
        RAISE NOTICE '   适用场景: CCTV新闻联播自动化分析';
    ELSE
        RAISE EXCEPTION '❌ CCTV新闻分析模板创建失败！';
    END IF;
END $$;

