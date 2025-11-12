   -- =====================================================
-- 更新投资主题描述生成模板，添加 category 参数
-- 创建日期: 2025-11-11
-- 描述: 在 theme_description_generator_v1 模板中添加 category 变量
-- =====================================================
-- 
-- 功能说明：
-- 1. 更新 theme_description_generator_v1 模板的 variables 和 user_prompt_template
-- 2. 添加 category 参数，用于生成更精准、更符合分类的主题描述
--
-- 使用方法:
--   psql -h 111.230.48.119 -U postgres -d rc_database -f update_theme_description_template_with_category.sql
-- =====================================================

BEGIN;

-- ============================================================
-- 更新 theme_description_generator_v1 模板
-- ============================================================

UPDATE llm_prompt_templates
SET 
    -- 更新 User Prompt Template，添加 category
    user_prompt_template = 'Generate a detailed description for the following investment theme:

**Theme Name:** {{theme_name}}
**Category:** {{category}}

Please provide a comprehensive description that includes:
1. Core focus and scope of this investment theme within the {{category}} sector
2. Key technologies, products, or services involved
3. Main industry sectors and typical applications
4. Market trends and growth drivers specific to this theme
5. Why this theme is important in the {{category}} context

Format the description as a single continuous paragraph, suitable for semantic embedding and vector search. The description should highlight the theme''s relevance and unique characteristics within its category.',

    -- 更新 Variables Definition，添加 category
    variables = '{
        "theme_name": {
            "type": "string", 
            "required": true, 
            "description": "投资主题名称（中文或英文）"
        },
        "category": {
            "type": "string", 
            "required": true, 
            "description": "主题所属分类/类别（例如：Technology, Healthcare, Finance, Energy 等）"
        }
    }'::jsonb,
    
    -- 更新示例输入
    example_input = '{
        "theme_name": "Electric Vehicles (电动汽车)",
        "category": "Transportation & Clean Energy"
    }'::jsonb,
    
    -- 更新示例输出
    example_output = 'Electric Vehicles (电动汽车) is a transformative investment theme within the Transportation & Clean Energy category, focusing on companies involved in the development, manufacturing, and sale of battery electric vehicles (BEVs), plug-in hybrid electric vehicles (PHEVs), and related technologies including charging infrastructure and advanced battery systems. This sector encompasses automotive manufacturers transitioning to electrification, pure-play EV startups, and suppliers of critical components such as lithium-ion batteries, electric motors, and power electronics. The theme represents a fundamental shift in the automotive industry, driven by global carbon emission reduction policies, government incentives for zero-emission vehicles, rapid advancements in battery technology (higher energy density, lower costs, faster charging), expanding charging infrastructure networks, and growing consumer adoption motivated by total cost of ownership advantages and environmental concerns. As a cornerstone of the clean energy transition, electric vehicles are expected to dominate the automotive market by 2030-2035, making this theme critical for investors seeking exposure to sustainable transportation and the decarbonization of the transport sector.',
    
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
        RAISE NOTICE '✅ 成功更新 theme_description_generator_v1 模板，添加 category 参数';
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


