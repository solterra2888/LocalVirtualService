-- =====================================================
-- 优化新闻分类模板：添加新闻价值分类 (Fact/Opinion)
-- 创建日期: 2025-12-06
-- 目的: 区分事实性新闻和观点性文章，提高新闻质量筛选
-- =====================================================

-- 更新 news_classification 模板，添加 news_value 维度
UPDATE llm_prompt_templates
SET 
    system_prompt = '你是一位专业的金融新闻分类助手。你的任务是客观地为金融新闻打上结构化的分类标签。

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

    user_prompt_template = '请对以下金融新闻进行多维度分类标注：

【新闻标题】
{{title}}

【新闻内容】
{{content}}

【相关股票代码】
{{symbols}}

请按照以下分类体系进行标注，输出JSON格式：

**1. 新闻价值类型 (news_value)** - 选择最符合的一项：
- fact: 事实性新闻（公司公告、财报、并购、高管变动、分析师评级、市场数据、官方声明等客观事实）
- opinion: 观点性文章（分析师观点、投资建议、市场评论、预测性文章、"最佳股票"推荐等主观内容）

**判断标准**：
- Fact: 基于已发生的事实，可验证的信息，官方发布的数据或声明
  * 示例：公司财报发布、并购交易宣布、分析师评级上调/下调、CEO离职、产品发布会
  * 特征：包含具体数据、官方声明、已确认的事件
  
- Opinion: 基于分析和预测的主观判断，投资建议，未来展望
  * 示例：「最值得买入的AI股票」「为什么XX股票会翻倍」「市场将如何反应」
  * 特征：包含"应该"、"可能"、"预计"、"最佳"等主观词汇

**2. 主题分类 (primary_theme)** - 选择最符合的一项：
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

**3. 事件类型 (event_type)** - 选择最符合的一项：
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
- analyst_rating: 分析师评级变动
- general_news: 一般新闻

**4. 影响范围 (impact_scope)** - 选择一项：
- company_specific: 公司级别
- sector_wide: 行业级别
- market_wide: 市场级别
- regional: 区域级别
- global: 全球级别

**5. 时效性 (urgency)** - 选择一项：
- breaking: 突发重要（需要立即关注）
- timely: 及时关注（当日重要）
- routine: 常规资讯（一般信息）
- background: 背景分析（长期参考）

**6. 相关行业 (related_sectors)** - 可选择1-3项：
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

**7. 相关国家/地区 (related_countries)** - 可选择1-5个最相关的国家/地区：
- china: 中国
- usa: 美国
- japan: 日本
- germany: 德国
- uk: 英国
- france: 法国
- india: 印度
- russia: 俄罗斯
- brazil: 巴西
- canada: 加拿大
- australia: 澳大利亚
- south_korea: 韩国
- mexico: 墨西哥
- indonesia: 印度尼西亚
- saudi_arabia: 沙特阿拉伯
- turkey: 土耳其
- switzerland: 瑞士
- taiwan: 中国台湾
- hong_kong: 中国香港
- singapore: 新加坡
- eu: 欧盟
- middle_east: 中东地区
- africa: 非洲
- south_america: 南美洲
- southeast_asia: 东南亚
- global: 全球多国

请输出以下JSON格式（不要包含任何其他文字）：

```json
{
  "news_value": "fact或opinion",
  "news_value_confidence": 0.95,
  "primary_theme": "选择的主题",
  "primary_theme_confidence": 0.95,
  "event_type": "选择的事件类型",
  "event_type_confidence": 0.88,
  "impact_scope": "选择的影响范围",
  "urgency": "选择的时效性",
  "related_sectors": ["行业1", "行业2"],
  "related_countries": ["国家1", "国家2"],
  "rationale": "简要说明分类依据，特别是news_value的判断理由（不超过100字）"
}
```

注意事项：
- news_value 是最重要的新维度，请仔细判断
- 置信度反映你对分类的确定程度（0.0-1.0）
- related_sectors 最多选择3个最相关的行业
- related_countries 最多选择5个最相关的国家/地区，必须基于新闻内容中明确提及或直接相关的国家
- rationale 应客观描述分类理由，特别说明为何判断为fact或opinion
- 如果新闻信息不足，选择最接近的分类并降低置信度',

    description = '新闻分类标签模板 v2 - 增强版
主要改进：
1. 新增 news_value 维度：区分事实性新闻(fact)和观点性文章(opinion)
2. 提供详细的判断标准和示例
3. 优化事件类型，新增 analyst_rating（分析师评级变动）
4. 强化对新闻来源质量的识别能力
5. 帮助用户过滤低价值的观点性文章

使用场景：
- 优先展示事实性新闻（公司公告、财报、并购等）
- 过滤低质量的观点文章（如"最佳股票推荐"）
- 保留高质量来源的分析文章（可通过news_source配合使用）',

    updated_at = CURRENT_TIMESTAMP,
    version = 2
WHERE template_code = 'news_classification';

-- 记录更新日志
DO $$
BEGIN
    RAISE NOTICE '==============================================';
    RAISE NOTICE 'news_classification 模板已更新到 v2';
    RAISE NOTICE '新增功能：';
    RAISE NOTICE '  1. news_value 维度（fact/opinion）';
    RAISE NOTICE '  2. 详细的判断标准和示例';
    RAISE NOTICE '  3. analyst_rating 事件类型';
    RAISE NOTICE '使用建议：';
    RAISE NOTICE '  - 在新闻过滤中优先展示 news_value=fact 的新闻';
    RAISE NOTICE '  - 对于 opinion 类型，可结合 news_source 判断质量';
    RAISE NOTICE '  - 分析师评级变动应归类为 fact + analyst_rating';
    RAISE NOTICE '==============================================';
END $$;

COMMIT;

