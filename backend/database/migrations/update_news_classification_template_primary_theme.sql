-- =====================================================
-- 更新 news_classification 模板：统一 primary_theme 选项
-- 创建日期: 2025-10-30
-- 描述: 让 primary_theme 的选项与 theme_master_data.category 对应
-- =====================================================

BEGIN;

-- ============================================================
-- 更新 news_classification 模板的 primary_theme 选项
-- ============================================================

UPDATE llm_prompt_templates
SET 
    user_prompt_template = '请对以下金融新闻进行多维度分类标注：

【新闻标题】
{{title}}

【新闻内容】
{{content}}

【相关股票代码】
{{symbols}}

请按照以下分类体系进行标注，输出JSON格式：

**1. 主题分类 (primary_theme)** - 选择最符合的一项：
- technology: 科技创新（产品、技术突破、AI、5G等）
- finance: 金融市场（股票、债券、市场动态、投资）
- economic: 宏观经济（GDP、通胀、利率、经济政策）
- energy: 能源资源（石油、天然气、新能源、电动车）
- healthcare: 医疗健康（医药、生物技术、医疗器械）
- consumer: 消费零售（电商、零售、消费品）
- industrials: 工业制造（制造业、基础设施、物流）
- political: 政治政策（政府决策、选举、政策）
- geopolitical: 地缘政治（国际关系、贸易冲突）
- regulatory: 监管法规（合规、监管政策、法律）
- corporate: 公司运营（管理、战略、人事）
- conflict: 冲突军事（战争、冲突事件）

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
- primary_theme 必须从上述12个选项中选择
- related_sectors 最多选择3个最相关的行业
- rationale 应客观描述分类理由，避免主观评价
- 如果新闻信息不足，选择最接近的分类并降低置信度',

    output_format = '{
        "type": "object",
        "required": ["primary_theme", "event_type", "impact_scope", "urgency", "related_sectors"],
        "properties": {
            "primary_theme": {
                "type": "string",
                "enum": ["technology", "finance", "economic", "energy", "healthcare", "consumer", "industrials", "political", "geopolitical", "regulatory", "corporate", "conflict"]
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
    
    updated_at = CURRENT_TIMESTAMP

WHERE template_code = 'news_classification';

-- ============================================================
-- 输出结果
-- ============================================================
DO $$
DECLARE
    v_count INTEGER;
BEGIN
    -- 检查模板是否存在并已更新
    SELECT COUNT(*) INTO v_count
    FROM llm_prompt_templates
    WHERE template_code = 'news_classification';
    
    IF v_count > 0 THEN
        RAISE NOTICE '✅ news_classification template updated successfully!';
        RAISE NOTICE '';
        RAISE NOTICE 'Updated primary_theme options:';
        RAISE NOTICE '  - technology, finance, economic, energy';
        RAISE NOTICE '  - healthcare, consumer, industrials';
        RAISE NOTICE '  - political, geopolitical, regulatory';
        RAISE NOTICE '  - corporate, conflict';
        RAISE NOTICE '';
        RAISE NOTICE 'These match theme_master_data.category values';
    ELSE
        RAISE WARNING '⚠️  news_classification template not found!';
    END IF;
END $$;

COMMIT;


