-- =====================================================
-- content_tag_feeds_v1: 新增 people / countries 字段
-- =====================================================
-- 创建时间: 2026-05-19
-- 说明: 升级重型 Feeds 打标模板, YouTube + 长 Twitter 也产出关键人物与国家.

UPDATE llm_prompt_templates
SET
    system_prompt = system_prompt || E'

**关键人物 (people) 提取规则:**
1. 提取内容中被讨论的政治人物、央行官员、CEO、分析师等 (不仅是推文作者).
2. 每条: name(中文常用名), name_en(英文全名), role(politician|central_banker|ceo|executive|analyst|other), affiliation(可选).
3. 示例: 鲍威尔/Jerome Powell → central_banker; 特朗普/Donald Trump → politician.
4. 不要编造未出现的人物; 无则返回 [].

**国家/地区 (countries) 提取规则:**
1. 使用 ISO 3166-1 alpha-2: US, CN, JP, EU, UK, SG, HK, TW, KR, IR, RU, UA 等.
2. 仅提取内容明确涉及的国家 (关税、外交、央行政策、地缘冲突、区域市场).
3. 不要仅因 ticker 推断国家; 无则返回 [].',
    user_prompt_template = replace(
        replace(
            replace(
                user_prompt_template,
                '### 3. 投资概念/主题（Concepts）',
                '### 3. 关键人物（People）
- 被讨论的重要人物, 每条含 name / name_en / role / affiliation(可选).
- role: politician | central_banker | ceo | executive | analyst | other
- 无则返回 [].

### 4. 国家/地区（Countries）
- ISO 3166-1 alpha-2 代码数组, 如 ["US","CN","JP"].
- 仅内容明确涉及的国家; 无则返回 [].

### 5. 投资概念/主题（Concepts）'
            ),
            '### 4. 置信度与摘要',
            '### 6. 置信度与摘要'
        ),
        '"concepts": ["AI芯片", "数据中心"],
  "confidence": 0.85,
  "summary": "用一段话准确概括内容核心..."',
        '"concepts": ["AI芯片", "数据中心"],
  "people": [{"name": "鲍威尔", "name_en": "Jerome Powell", "role": "central_banker", "affiliation": "美联储"}],
  "countries": ["US"],
  "confidence": 0.85,
  "summary": "用一段话准确概括内容核心..."'
    ),
    output_format = '{"type": "json", "json_schema": {"type": "object", "required": ["sector", "sector_en", "industry", "industry_en", "tickers", "concepts", "people", "countries", "confidence", "summary"], "properties": {"sector": {"type": "string"}, "sector_en": {"type": "string"}, "industry": {"type": "string"}, "industry_en": {"type": "string"}, "tickers": {"type": "array", "items": {"type": "string"}}, "concepts": {"type": "array", "items": {"type": "string"}}, "people": {"type": "array", "items": {"type": "object", "properties": {"name": {"type": "string"}, "name_en": {"type": "string"}, "role": {"type": "string"}, "affiliation": {"type": ["string", "null"]}}}}, "countries": {"type": "array", "items": {"type": "string"}}, "confidence": {"type": "number", "minimum": 0, "maximum": 1}, "summary": {"type": "string"}}}}',
    updated_at = CURRENT_TIMESTAMP
WHERE template_code = 'content_tag_feeds_v1';
