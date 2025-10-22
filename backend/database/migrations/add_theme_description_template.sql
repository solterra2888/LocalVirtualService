-- =====================================================
-- 投资主题描述生成模板
-- 创建日期: 2025-10-22
-- 描述: 添加主题分析分类和LLM主题描述生成模板
-- =====================================================
-- 
-- 功能说明：
-- 1. 创建"投资主题分析"分类
-- 2. 创建通用的主题描述生成模板（支持中英文）
-- 3. 用于自动生成投资主题的详细描述，提升向量匹配质量
--
-- 使用方法:
--   psql -h 111.230.48.119 -U postgres -d rc_database -f add_theme_description_template.sql
-- =====================================================

BEGIN;

-- ============================================================
-- 步骤1: 创建"投资主题分析"分类
-- ============================================================

INSERT INTO llm_prompt_categories (
    category_code, 
    category_name, 
    description, 
    icon,
    sort_order,
    is_active
) VALUES (
    'theme_analysis',
    '投资主题分析',
    '用于生成投资主题的详细描述，提升主题向量匹配质量和语义理解能力',
    '📊',
    10,
    true
)
ON CONFLICT (category_code) DO UPDATE SET
    category_name = EXCLUDED.category_name,
    description = EXCLUDED.description,
    icon = EXCLUDED.icon,
    updated_at = CURRENT_TIMESTAMP;

-- ============================================================
-- 步骤2: 创建主题描述生成模板（通用版，支持中英文）
-- ============================================================

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
    is_active,
    is_default,
    is_public,
    description,
    tags,
    example_input,
    example_output
) VALUES (
    'theme_description_generator_v1',
    '投资主题描述生成器 v1',
    (SELECT id FROM llm_prompt_categories WHERE category_code = 'theme_analysis'),
    
    -- System Prompt
    'You are a professional financial analyst specializing in investment themes and market sectors. Your task is to generate comprehensive, accurate descriptions of investment themes that will be used for semantic matching with financial news articles.

Guidelines:
1. Provide factual, professional descriptions
2. Include both English and Chinese key terms when relevant
3. Focus on industry characteristics, applications, and representative companies
4. Optimize for semantic search and vector similarity matching
5. Keep descriptions concise but informative (150-250 words)',

    -- User Prompt Template
    'Generate a detailed description for the following investment theme:

**Theme Name:** {{theme_name}}
**Market:** {{market}}
**Related Stocks:** {{stock_symbols}}
**Representative Companies:** {{company_names}}

Please provide a comprehensive description that includes:
1. Core focus and scope of this investment theme
2. Key technologies, products, or services involved
3. Main industry sectors and typical applications
4. Representative companies and their roles in the theme
5. Market trends and growth drivers

Format the description as a single continuous paragraph, suitable for semantic embedding and vector search.',

    -- Variables Definition
    '{
        "theme_name": {
            "type": "string", 
            "required": true, 
            "description": "投资主题名称（中文或英文）"
        },
        "market": {
            "type": "string", 
            "required": true, 
            "description": "市场代码（HK=香港, US=美国, CN=中国）"
        },
        "stock_symbols": {
            "type": "string", 
            "required": false, 
            "default": "Not available", 
            "description": "相关股票代码列表（逗号分隔）"
        },
        "company_names": {
            "type": "string", 
            "required": false, 
            "default": "Not available", 
            "description": "相关公司名称列表（逗号分隔）"
        }
    }'::jsonb,
    
    -- Output Format
    '{
        "type": "text",
        "description": "返回单段文本描述，无需JSON格式"
    }'::jsonb,
    
    -- LLM Configuration
    'qwen',              -- 推荐使用Qwen（成本低，中英文支持好）
    'qwen-plus',         -- qwen-plus 平衡成本和质量
    0.3,                 -- 低温度确保描述稳定一致
    600,                 -- 最多600 tokens（约200-250词）
    
    -- Template Metadata
    'system',            -- system级别模板
    true,                -- 激活
    true,                -- 设为默认
    false,               -- 系统级不公开
    
    -- Description
    '自动生成投资主题的详细描述，支持中英文混合输出。用于向量化嵌入，提升新闻主题匹配的准确度。',
    
    -- Tags
    '["投资主题", "向量匹配", "语义分析", "自动化"]'::jsonb,
    
    -- Example Input
    '{
        "theme_name": "Electric Vehicles (电动汽车)",
        "market": "US",
        "stock_symbols": "TSLA, NIO, RIVN, LCID, F, GM",
        "company_names": "Tesla, NIO, Rivian, Lucid Motors, Ford, General Motors"
    }'::jsonb,
    
    -- Example Output
    'Electric Vehicles (电动汽车) investment theme focuses on companies involved in the development, manufacturing, and sale of battery electric vehicles (BEVs), plug-in hybrid electric vehicles (PHEVs), and related technologies including charging infrastructure and battery systems. This sector encompasses automotive manufacturers transitioning to electrification, pure-play EV startups, and suppliers of critical components. Key companies include Tesla (TSLA), the global leader in EV production and autonomous driving technology; NIO (NIO), a premium Chinese EV manufacturer; Rivian (RIVN), specializing in electric trucks and SUVs; and Lucid Motors (LCID), focusing on luxury electric sedans. Traditional automakers like Ford (F) and General Motors (GM) are also investing heavily in EV platforms. The theme benefits from global carbon emission reduction policies, government incentives, advancing battery technology (higher energy density, lower costs), expanding charging infrastructure, and growing consumer adoption driven by total cost of ownership advantages and environmental concerns.'
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
    updated_at = CURRENT_TIMESTAMP;

COMMIT;

-- ============================================================
-- 验证
-- ============================================================

-- 查看创建的分类
SELECT * FROM llm_prompt_categories WHERE category_code = 'theme_analysis';

-- 查看创建的模板
SELECT 
    id, 
    template_code, 
    template_name, 
    recommended_provider,
    recommended_model,
    temperature,
    max_tokens,
    is_active
FROM llm_prompt_templates 
WHERE template_code = 'theme_description_generator_v1';

-- ============================================================
-- 说明文档
-- ============================================================

-- 模板使用示例（Python代码）:
/*
from backend.core.llm_framework import llm_service

response = await llm_service.chat_completion_with_template(
    template_code='theme_description_generator_v1',
    variables={
        'theme_name': 'Artificial Intelligence',
        'market': 'US',
        'stock_symbols': 'NVDA, MSFT, GOOGL, META',
        'company_names': 'NVIDIA, Microsoft, Google, Meta'
    }
)

description = response.content.strip()
print(description)
*/

