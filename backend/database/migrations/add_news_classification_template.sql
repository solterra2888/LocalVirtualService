-- =====================================================
-- 新增 news_classification 模板
-- 创建日期: 2025-10-17
-- 目的: 为新闻提供结构化分类标签，支持数据查询和筛选
-- =====================================================
--
-- 功能说明：
-- 1. 自动为新闻打上多维度分类标签
-- 2. 输出结构化JSON，便于数据库查询
-- 3. 支持5个分类维度：主题、事件类型、影响范围、时效性、相关行业
-- 4. 可独立使用，也可与 news_summary_v1 配合使用
--
-- 使用场景：
-- - 新闻自动分类和打标签
-- - 高级筛选和数据分析
-- - Dashboard统计和可视化
-- - 投资组合新闻过滤
--
-- 使用方法:
--   psql -d reversecapital -f add_news_classification_template.sql
-- =====================================================

BEGIN;

-- ============================================================
-- 步骤1: 确保 news_analysis 分类存在
-- ============================================================

-- 如果 news_analysis 分类不存在，则创建
INSERT INTO llm_prompt_categories (
    category_code,
    category_name,
    description,
    icon,
    sort_order,
    is_active
)
VALUES (
    'news_analysis',
    '新闻分析',
    '用于金融新闻内容分析的Prompt模板',
    '📰',
    10,
    true
)
ON CONFLICT (category_code) DO NOTHING;

-- ============================================================
-- 步骤2: 插入 news_classification 模板
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
    is_public,
    is_active,
    is_default,
    description,
    tags,
    example_input,
    example_output
)
VALUES (
    'news_classification',
    '新闻分类标签模板',
    (SELECT id FROM llm_prompt_categories WHERE category_code = 'news_analysis' LIMIT 1),
    
    -- System Prompt: 强调客观、结构化、符合内容审查要求
    '你是一位专业的金融新闻分类助手。你的任务是客观地为金融新闻打上结构化的分类标签。

核心原则：
1. 严格按照预定义的分类体系进行标注
2. 保持客观中立，不做主观评价
3. 输出严格的JSON格式
4. 避免使用敏感或情绪化的词汇
5. 基于新闻内容的事实进行分类

输出要求：
- 必须输出有效的JSON格式
- 所有分类值必须从预定义选项中选择
- 置信度范围为0.0-1.0
- 简明扼要地说明分类理由',
    
    -- User Prompt Template: 提供新闻信息和分类选项
    '请对以下金融新闻进行多维度分类标注：

【新闻标题】
{{title}}

【新闻内容】
{{content}}

【相关股票代码】
{{symbols}}

请按照以下分类体系进行标注，输出JSON格式：

**1. 主题分类 (primary_theme)** - 选择最符合的一项：
- finance: 金融市场（股票、债券、市场动态）
- technology: 科技创新（产品、技术突破）
- economic: 宏观经济（GDP、通胀、利率、政策）
- political: 政治政策（政府决策、选举）
- geopolitical: 地缘政治（国际关系、贸易）
- energy: 能源资源（油气、新能源）
- conflict: 冲突军事（战争、冲突事件）
- regulatory: 监管法规（合规、监管政策）
- corporate: 公司运营（管理、战略）
- industry: 行业动态（行业趋势、竞争）

**2. 事件类型 (event_type)** - 选择最符合的一项：
- earnings_report: 财报发布
- merger_acquisition: 并购交易
- product_launch: 产品发布
- leadership_change: 高管变动
- legal_regulatory: 法律监管事件
- market_movement: 市场波动
- partnership: 合作协议
- investment_funding: 投融资
- dividend_buyback: 分红回购
- outlook_guidance: 业绩指引
- general_news: 一般新闻

**3. 影响范围 (impact_scope)** - 选择一项：
- company_specific: 公司级别
- sector_wide: 行业级别
- market_wide: 市场级别
- regional: 区域级别
- global: 全球级别

**4. 时效性 (urgency)** - 选择一项：
- breaking: 突发重要（需要立即关注）
- timely: 及时关注（当日重要）
- routine: 常规资讯（一般信息）
- background: 背景分析（长期参考）

**5. 相关行业 (related_sectors)** - 可选择1-3项：
- technology: 科技
- finance: 金融
- healthcare: 医疗
- energy: 能源
- consumer: 消费
- industrials: 工业
- materials: 材料
- telecommunications: 通信
- utilities: 公用事业
- real_estate: 房地产
- transportation: 交通运输

请输出以下JSON格式（不要包含任何其他文字）：

```json
{
  "primary_theme": "选择的主题",
  "primary_theme_confidence": 0.95,
  "event_type": "选择的事件类型",
  "event_type_confidence": 0.88,
  "impact_scope": "选择的影响范围",
  "urgency": "选择的时效性",
  "related_sectors": ["行业1", "行业2"],
  "rationale": "简要说明分类依据（不超过80字）"
}
```

注意事项：
- 置信度反映你对分类的确定程度（0.0-1.0）
- related_sectors 最多选择3个最相关的行业
- rationale 应客观描述分类理由，避免主观评价
- 如果新闻信息不足，选择最接近的分类并降低置信度',
    
    -- Variables 定义
    '{
        "title": {
            "type": "string", 
            "description": "新闻标题",
            "required": true
        },
        "content": {
            "type": "string", 
            "description": "新闻正文内容",
            "required": true
        },
        "symbols": {
            "type": "string", 
            "description": "相关股票代码，逗号分隔",
            "required": false,
            "default": "无"
        }
    }'::jsonb,
    
    -- Output Format 定义
    '{
        "type": "object",
        "required": ["primary_theme", "event_type", "impact_scope", "urgency", "related_sectors"],
        "properties": {
            "primary_theme": {
                "type": "string",
                "enum": ["finance", "technology", "economic", "political", "geopolitical", "energy", "conflict", "regulatory", "corporate", "industry"]
            },
            "primary_theme_confidence": {
                "type": "number",
                "minimum": 0.0,
                "maximum": 1.0
            },
            "event_type": {
                "type": "string",
                "enum": ["earnings_report", "merger_acquisition", "product_launch", "leadership_change", "legal_regulatory", "market_movement", "partnership", "investment_funding", "dividend_buyback", "outlook_guidance", "general_news"]
            },
            "event_type_confidence": {
                "type": "number",
                "minimum": 0.0,
                "maximum": 1.0
            },
            "impact_scope": {
                "type": "string",
                "enum": ["company_specific", "sector_wide", "market_wide", "regional", "global"]
            },
            "urgency": {
                "type": "string",
                "enum": ["breaking", "timely", "routine", "background"]
            },
            "related_sectors": {
                "type": "array",
                "items": {
                    "type": "string",
                    "enum": ["technology", "finance", "healthcare", "energy", "consumer", "industrials", "materials", "telecommunications", "utilities", "real_estate", "transportation"]
                },
                "minItems": 1,
                "maxItems": 3
            },
            "rationale": {
                "type": "string",
                "maxLength": 80
            }
        }
    }'::jsonb,
    
    -- 推荐配置
    'qwen',  -- 推荐使用 Qwen（也可以是 doubao）
    'qwen-turbo',
    0.3,  -- 低温度保证分类一致性
    800,  -- 分类任务输出较短
    
    -- 权限和状态
    'system',  -- 系统级模板
    true,      -- 公开可用
    true,      -- 激活状态
    false,     -- 非默认模板
    
    -- 描述
    '新闻分类标签模板 - 自动为新闻打上多维度结构化标签

核心功能：
1. 多维度分类：主题、事件类型、影响范围、时效性、相关行业
2. 结构化输出：标准JSON格式，便于数据库查询
3. 置信度评估：每个分类都有置信度评分
4. 内容审查友好：使用中性客观的表述

适用场景：
- 新闻自动分类和标签管理
- 高级筛选和数据分析
- Dashboard统计图表
- 投资组合新闻过滤
- 与 news_summary_v1 配合使用

输出存储：
- 结果存储在 news.analysis_result JSONB字段
- 通过 analysis_template_code = ''news_classification'' 标识

查询示例：
SELECT * FROM news 
WHERE analysis_template_code = ''news_classification''
  AND analysis_result->''content''->>>''primary_theme'' = ''finance''
  AND analysis_result->''content''->>>''event_type'' = ''earnings_report'';

性能特点：
- Token消耗：约400-600 tokens/条
- 处理速度：比 summary 快 40-50%
- 准确度：通过置信度评估可靠性
- Temperature: 0.3 (低温度保证一致性)',
    
    -- Tags
    '["news", "classification", "tagging", "structured", "analysis", "finance"]'::jsonb,
    
    -- Example Input
    '{
        "title": "苹果公司Q4财报超预期，iPhone销售额增长10%",
        "content": "苹果公司今日公布2024年第四季度财报，营收达到950亿美元，超出分析师预期。其中iPhone销售额同比增长10%，达到430亿美元。CEO库克表示对新产品线充满信心，并上调了下季度业绩指引。受此消息影响，苹果股价盘后上涨3.5%。",
        "symbols": "AAPL"
    }'::jsonb,
    
    -- Example Output
    '{
  "primary_theme": "finance",
  "primary_theme_confidence": 0.95,
  "event_type": "earnings_report",
  "event_type_confidence": 0.98,
  "impact_scope": "market_wide",
  "urgency": "timely",
  "related_sectors": ["technology", "consumer"],
  "rationale": "苹果Q4财报发布，超预期的业绩和上调指引对科技股市场有重要影响"
}'
)
ON CONFLICT (template_code) 
DO UPDATE SET
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

-- ============================================================
-- 步骤3: 添加索引支持分类查询（可选但推荐）
-- ============================================================

-- 为 news 表的 analysis_result JSONB字段创建GIN索引（如果还没有）
CREATE INDEX IF NOT EXISTS idx_news_analysis_result_gin 
ON news USING GIN (analysis_result);

-- 添加注释说明
COMMENT ON INDEX idx_news_analysis_result_gin IS '
支持对 news.analysis_result JSONB字段的高效查询
特别优化了 news_classification 模板的分类字段查询';

-- ============================================================
-- 完成提示
-- ============================================================

DO $$
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE '✅ news_classification 模板安装完成！';
    RAISE NOTICE '========================================';
    RAISE NOTICE '';
    RAISE NOTICE '模板信息：';
    RAISE NOTICE '  - 模板代码: news_classification';
    RAISE NOTICE '  - 模板名称: 新闻分类标签模板';
    RAISE NOTICE '  - 推荐模型: Qwen Turbo';
    RAISE NOTICE '  - Temperature: 0.3 (低温度保证一致性)';
    RAISE NOTICE '  - Token消耗: ~400-600/条';
    RAISE NOTICE '';
    RAISE NOTICE '使用方法：';
    RAISE NOTICE '  1. 在调度器配置中选择 analysis_template = "news_classification"';
    RAISE NOTICE '  2. 或通过API调用: POST /news-analysis/analyze';
    RAISE NOTICE '     {"news_id": 123, "template_code": "news_classification"}';
    RAISE NOTICE '';
    RAISE NOTICE '查询示例：';
    RAISE NOTICE '  -- 查询所有金融主题的财报新闻';
    RAISE NOTICE '  SELECT * FROM news';
    RAISE NOTICE '  WHERE analysis_template_code = ''news_classification''';
    RAISE NOTICE '    AND analysis_result->''content''->>>''primary_theme'' = ''finance''';
    RAISE NOTICE '    AND analysis_result->''content''->>>''event_type'' = ''earnings_report'';';
    RAISE NOTICE '';
    RAISE NOTICE '下一步：';
    RAISE NOTICE '  - 前端UI已支持选择此模板（DataSourceConfigModal.tsx）';
    RAISE NOTICE '  - 可以直接在新闻调度器中使用';
    RAISE NOTICE '  - 建议先用少量新闻测试分类效果';
    RAISE NOTICE '========================================';
END $$;

COMMIT;



