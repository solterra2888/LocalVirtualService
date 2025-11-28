-- =====================================================
-- 添加文件分析分类和默认模板
-- =====================================================
-- 用途: 为文件处理功能添加本地LLM分析支持
-- 创建时间: 2024-11-26
-- =====================================================

-- 1. 创建文件分析分类
INSERT INTO llm_prompt_categories 
(category_code, category_name, description, icon, sort_order, is_active)
VALUES 
('file_analysis', '文件分析', '用于分析文档内容、提取关键信息、生成结构化报告等场景', '📄', 100, true)
ON CONFLICT (category_code) DO NOTHING;

-- 2. 创建默认文件分析模板 v1
WITH category AS (
  SELECT id FROM llm_prompt_categories WHERE category_code = 'file_analysis'
)
INSERT INTO llm_prompt_templates 
(template_code, template_name, category_id, system_prompt, user_prompt_template, 
 variables, output_format, recommended_provider, recommended_model, 
 temperature, max_tokens, scope, is_active, is_default, version, created_by)
SELECT 
  'file_analysis_v1',
  '通用文件分析 v1',
  category.id,
  '你是一个专业的文档分析助手，擅长分析各类文档内容，提取关键信息，并生成结构化的分析报告。请仔细阅读提供的文档内容，并提供清晰、全面的分析。',
  '请分析以下文档内容：

{{file_content}}

{{additional_instructions}}',
  jsonb_build_object(
    'file_content', jsonb_build_object(
      'type', 'string',
      'required', true,
      'description', '文件的markdown内容'
    ),
    'additional_instructions', jsonb_build_object(
      'type', 'string',
      'required', false,
      'default', '请提供详细的分析报告，包括：
1. 文档主题和核心内容
2. 关键信息提取
3. 重要发现和见解
4. 总结和建议',
      'description', '额外的分析指令'
    )
  ),
  jsonb_build_object(
    'type', 'text'
  ),
  'qwen',
  'qwen-max-longcontext',
  0.7,
  4000,
  'system',
  true,
  true,
  1,
  NULL  -- created_by设为NULL（系统创建）
FROM category
ON CONFLICT (template_code) DO NOTHING;

-- 3. 创建专业财报分析模板 v1
WITH category AS (
  SELECT id FROM llm_prompt_categories WHERE category_code = 'file_analysis'
)
INSERT INTO llm_prompt_templates 
(template_code, template_name, category_id, system_prompt, user_prompt_template, 
 variables, output_format, recommended_provider, recommended_model, 
 temperature, max_tokens, scope, is_active, is_default, version, created_by)
SELECT 
  'file_analysis_financial_report_v1',
  '财报分析 v1',
  category.id,
  '你是一个专业的财务分析师，精通财务报表分析、财务比率计算和财务健康评估。请基于提供的财务报告，进行深入的财务分析。',
  '请分析以下财务报告：

{{file_content}}

分析要求：
1. 财务数据提取（收入、利润、资产、负债等关键指标）
2. 财务比率分析（盈利能力、偿债能力、运营效率）
3. 同比/环比变化分析
4. 风险识别
5. 投资建议

{{sector}}{{industry}}{{ticker}}',
  jsonb_build_object(
    'file_content', jsonb_build_object(
      'type', 'string',
      'required', true,
      'description', '财务报告的markdown内容'
    ),
    'sector', jsonb_build_object(
      'type', 'string',
      'required', false,
      'default', '',
      'description', '公司所属行业（Sector）'
    ),
    'industry', jsonb_build_object(
      'type', 'string',
      'required', false,
      'default', '',
      'description', '公司所属子行业（Industry）'
    ),
    'ticker', jsonb_build_object(
      'type', 'string',
      'required', false,
      'default', '',
      'description', '公司股票代码'
    )
  ),
  jsonb_build_object(
    'type', 'text'
  ),
  'qwen',
  'qwen-max-longcontext',
  0.5,
  6000,
  'system',
  true,
  false,
  1,
  NULL  -- created_by设为NULL（系统创建）
FROM category
ON CONFLICT (template_code) DO NOTHING;

-- 4. 创建研究报告摘要模板 v1
WITH category AS (
  SELECT id FROM llm_prompt_categories WHERE category_code = 'file_analysis'
)
INSERT INTO llm_prompt_templates 
(template_code, template_name, category_id, system_prompt, user_prompt_template, 
 variables, output_format, recommended_provider, recommended_model, 
 temperature, max_tokens, scope, is_active, is_default, version, created_by)
SELECT 
  'file_analysis_research_summary_v1',
  '研究报告摘要 v1',
  category.id,
  '你是一个专业的投资研究分析师，擅长快速提取研究报告的核心观点、数据和结论。',
  '请对以下研究报告进行摘要：

{{file_content}}

摘要要求：
1. 核心观点（3-5个要点）
2. 关键数据和预测
3. 投资评级和目标价（如有）
4. 风险因素
5. 一句话总结

{{max_length}}',
  jsonb_build_object(
    'file_content', jsonb_build_object(
      'type', 'string',
      'required', true,
      'description', '研究报告的markdown内容'
    ),
    'max_length', jsonb_build_object(
      'type', 'string',
      'required', false,
      'default', '摘要长度控制在500字以内。',
      'description', '摘要长度要求'
    )
  ),
  jsonb_build_object(
    'type', 'text'
  ),
  'qwen',
  'qwen-plus',
  0.3,
  2000,
  'system',
  true,
  false,
  1,
  NULL  -- created_by设为NULL（系统创建）
FROM category
ON CONFLICT (template_code) DO NOTHING;

-- 5. 验证插入结果
DO $$
DECLARE
  category_count INTEGER;
  template_count INTEGER;
BEGIN
  SELECT COUNT(*) INTO category_count 
  FROM llm_prompt_categories 
  WHERE category_code = 'file_analysis';
  
  SELECT COUNT(*) INTO template_count 
  FROM llm_prompt_templates 
  WHERE template_code IN ('file_analysis_v1', 'file_analysis_financial_report_v1', 'file_analysis_research_summary_v1');
  
  RAISE NOTICE '文件分析分类数量: %', category_count;
  RAISE NOTICE '文件分析模板数量: %', template_count;
  
  IF category_count = 0 THEN
    RAISE EXCEPTION '文件分析分类创建失败';
  END IF;
  
  IF template_count < 3 THEN
    RAISE WARNING '部分文件分析模板创建失败，已创建: %', template_count;
  END IF;
END $$;

