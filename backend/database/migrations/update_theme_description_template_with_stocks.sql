-- =====================================================
-- 更新投资主题描述生成模板，添加股票信息参数
-- 创建日期: 2025-11-11
-- 描述: 在 theme_description_generator_v1 模板中添加 stock_symbols 和 company_names 变量
-- =====================================================
-- 
-- 功能说明：
-- 1. 更新 theme_description_generator_v1 模板的 variables 和 user_prompt_template
-- 2. 添加 stock_symbols 和 company_names 参数，用于生成更精准的主题描述
--
-- 使用方法:
--   psql -h 111.230.48.119 -U postgres -d rc_database -f update_theme_description_template_with_stocks.sql
-- =====================================================

BEGIN;

-- ============================================================
-- 更新 theme_description_generator_v1 模板
-- ============================================================

UPDATE llm_prompt_templates
SET 
    -- 更新 User Prompt Template，添加股票信息
    user_prompt_template = 'Generate a detailed description for the following investment theme:

**Theme Name:** {{theme_name}}
**Category:** {{category}}
**Related Stocks:** {{stock_symbols}}
**Representative Companies:** {{company_names}}

Please provide a comprehensive description that includes:
1. Core focus and scope of this investment theme within the {{category}} sector
2. Key technologies, products, or services involved
3. Main industry sectors and typical applications
4. Representative companies and their roles in the theme (if provided)
5. Market trends and growth drivers specific to this theme
6. Why this theme is important in the {{category}} context

Format the description as a single continuous paragraph, suitable for semantic embedding and vector search. The description should highlight the theme''s relevance and unique characteristics within its category.',

    -- 更新 Variables Definition，添加股票信息
    variables = '{
        "theme_name": {
            "type": "string", ....
            "required": true, 
            "description": "投资主题名称（中文或英文）"
        },
        "category": {
            "type": "string", 
            "required": true, 
            "description": "主题所属分类/类别（例如：Technology, Healthcare, Finance, Energy 等）"
        },
        "stock_symbols": {
            "type": "string", 
            "required": false,
            "default": "Not available",
            "description": "相关股票代码列表（逗号分隔，例如：NVDA.US, AMD.US, INTC.US）"
        },
        "company_names": {
            "type": "string", 
            "required": false,
            "default": "Not available",
            "description": "相关公司名称列表（逗号分隔，例如：NVIDIA, AMD, Intel）"
        }
    }'::jsonb,
    
    -- 更新示例输入
    example_input = '{
        "theme_name": "AI Chips",
        "category": "Technology",
        "stock_symbols": "NVDA.US, AMD.US, INTC.US",
        "company_names": "NVIDIA, AMD, Intel"
    }'::jsonb,
    
    -- 更新示例输出
    example_output = 'AI Chips is a transformative investment theme within the Technology category, focusing on companies developing specialized semiconductor chips designed for artificial intelligence workloads, including GPUs, TPUs, and custom AI accelerators. This sector encompasses leading chip manufacturers like NVIDIA (NVDA.US), which dominates the AI GPU market with its H100 and A100 series; AMD (AMD.US), offering competitive MI300 series AI accelerators; and Intel (INTC.US), developing Gaudi AI chips and Xeon processors optimized for AI inference. The theme represents a fundamental shift in computing architecture, driven by the explosive growth of large language models, generative AI applications, autonomous vehicles, and data center AI infrastructure. Key technologies include high-bandwidth memory (HBM), advanced packaging (chiplets), specialized tensor cores, and energy-efficient designs. As AI adoption accelerates across industries—from cloud computing and enterprise software to robotics and edge computing—demand for AI chips is expected to grow exponentially, with the market projected to reach hundreds of billions by 2030. This theme is critical for investors seeking exposure to the AI revolution, as semiconductor companies provide the essential hardware foundation enabling all AI innovations.',
    
    -- 更新时间戳
    updated_at = CURRENT_TIMESTAMP

WHERE template_code = 'theme_description_generator_v1';

-- 检查更新是否成功
DO $$
DECLARE
    updated_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO updated_count
    FROM llm_prompt_templates
    WHERE template_code = 'theme_description_generator_v1';
    
    IF updated_count > 0 THEN
        RAISE NOTICE '✅ 成功更新 theme_description_generator_v1 模板，添加 stock_symbols 和 company_names 参数';
    ELSE
        RAISE WARNING '⚠️  未找到 theme_description_generator_v1 模板';
    END IF;
END $$;

COMMIT;

-- ============================================================
-- 验证
-- ============================================================

-- 查看更新后的模板
SELECT 
    template_code,
    template_name,
    variables,
    example_input,
    updated_at
FROM llm_prompt_templates
WHERE template_code = 'theme_description_generator_v1';

-- 查看 user_prompt_template（输出太长，可选）
-- SELECT user_prompt_template 
-- FROM llm_prompt_templates 
-- WHERE template_code = 'theme_description_generator_v1';

