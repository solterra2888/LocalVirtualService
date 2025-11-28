-- =====================================================
-- 添加投行研究报告分析模板
-- =====================================================
-- 用途: 专门用于解读投行和券商研究报告
-- 创建时间: 2024-11-26
-- =====================================================

-- 创建投行研究报告分析模板
WITH category AS (
  SELECT id FROM llm_prompt_categories WHERE category_code = 'file_analysis'
)
INSERT INTO llm_prompt_templates 
(template_code, template_name, category_id, system_prompt, user_prompt_template, 
 variables, output_format, recommended_provider, recommended_model, 
 temperature, max_tokens, scope, is_active, is_default, version, 
 description, tags, example_input)
SELECT 
  'file_analysis_investment_research_v1',
  '投行研究报告解读 v1',
  category.id,
  '你是一位资深的投资分析师，拥有丰富的卖方研究报告解读经验。你擅长从投行和券商的研究报告中提取关键投资逻辑、评级变化、目标价预测，并评估报告的投资价值和风险提示。你的分析客观、专业、有深度。',
  '请深入解读以下{{report_type}}研究报告：

{{file_content}}

---

**报告基本信息（如有）：**
- 所属行业：{{sector}}{{industry}}
- 覆盖标的：{{ticker}}
- 报告类型：{{report_type}}

---

**分析要求：**

请按照以下结构提供专业的报告解读：

## 一、报告核心要素
1. **研究机构与分析师**
   - 机构名称
   - 分析师姓名
   - 报告发布日期

2. **投资评级与目标价**
   - 当前评级（买入/增持/中性/减持）
   - 评级变化（如有调整，说明理由）
   - 目标价格及测算依据
   - 预期涨幅空间

## 二、核心投资逻辑
1. **主要观点**（3-5个核心要点）
   - 提炼报告最重要的投资论点
   - 突出新的信息或观点变化

2. **业绩预测与盈利模型**
   - 营收预测（近3年）
   - 利润预测（净利润、EPS等）
   - 关键假设和驱动因素
   - 与市场一致预期的差异

3. **估值分析**
   - 采用的估值方法（PE/PB/PS/DCF等）
   - 估值水平与历史/同业对比
   - 估值合理性判断

## 三、行业与竞争分析
1. **行业趋势**
   - 行业景气度判断
   - 政策影响
   - 市场空间与增长前景

2. **公司竞争地位**
   - 市场份额
   - 竞争优势/护城河
   - 与主要竞争对手对比

## 四、风险因素
1. **核心风险点**（列举报告提及的主要风险）
2. **风险评估**（评估这些风险的影响程度）
3. **应对策略**（如报告提及）

## 五、数据亮点
- 关键财务数据（营收、利润、毛利率等）
- 重要业务数据（用户数、市占率等）
- 同比/环比变化趋势

## 六、投资价值评估
1. **报告质量评价**
   - 逻辑严密性
   - 数据支撑充分性
   - 创新观点程度

2. **可操作性建议**
   - 适合的投资者类型（价值/成长/趋势）
   - 建议关注的时间窗口
   - 需要跟踪的关键指标

3. **一句话总结**
   - 用一句话概括报告的核心投资建议

---

{{analysis_depth}}

**注意事项：**
- 如果报告内容不完整或无法提取某些信息，请明确说明
- 保持客观中立，同时提供你的专业判断
- 数据要准确引用，不要臆造
- 突出报告的独特观点和增量信息',
  jsonb_build_object(
    'file_content', jsonb_build_object(
      'type', 'string',
      'required', true,
      'description', '研究报告的markdown内容'
    ),
    'report_type', jsonb_build_object(
      'type', 'string',
      'required', false,
      'default', '公司研究报告',
      'description', '报告类型：公司研究报告/行业深度报告/策略报告/宏观报告等'
    ),
    'sector', jsonb_build_object(
      'type', 'string',
      'required', false,
      'default', '',
      'description', '所属行业（Sector）'
    ),
    'industry', jsonb_build_object(
      'type', 'string',
      'required', false,
      'default', '',
      'description', '所属子行业（Industry）'
    ),
    'ticker', jsonb_build_object(
      'type', 'string',
      'required', false,
      'default', '',
      'description', '覆盖的公司股票代码'
    ),
    'analysis_depth', jsonb_build_object(
      'type', 'string',
      'required', false,
      'default', '请提供深度分析，包含专业判断和投资建议。',
      'description', '分析深度要求：简要/标准/深度'
    )
  ),
  jsonb_build_object(
    'type', 'structured_text',
    'description', '结构化文本输出，按标题分节'
  ),
  'qwen',
  'qwen-plus',
  0.5,
  4000,
  'system',
  true,
  false,
  1,
  '专门用于解读投行和券商研究报告，提取投资评级、目标价、核心逻辑、风险因素等关键信息，适合专业投资者快速把握报告要点。',
  '["研究报告", "投行", "券商", "投资分析", "评级", "目标价"]'::jsonb,
  jsonb_build_object(
    'report_type', '公司研究报告',
    'sector', '科技',
    'industry', '人工智能',
    'ticker', 'NVDA',
    'analysis_depth', '请提供深度分析，重点关注业绩预测和估值合理性。'
  )
FROM category
WHERE NOT EXISTS (
  SELECT 1 FROM llm_prompt_templates 
  WHERE template_code = 'file_analysis_investment_research_v1'
);

-- 验证插入结果
DO $$
DECLARE
  template_count INTEGER;
BEGIN
  SELECT COUNT(*) INTO template_count 
  FROM llm_prompt_templates 
  WHERE template_code = 'file_analysis_investment_research_v1';
  
  IF template_count > 0 THEN
    RAISE NOTICE '✅ 投行研究报告分析模板创建成功';
  ELSE
    RAISE WARNING '⚠️ 投行研究报告分析模板创建失败或已存在';
  END IF;
END $$;

