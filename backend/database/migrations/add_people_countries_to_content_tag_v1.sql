-- =====================================================
-- content_tag_v1: 新增 people / countries 字段
-- =====================================================
-- 创建时间: 2026-05-19
-- 说明: 就地升级 Upload Sources 默认打标模板, 输出关键人物与国家 (与 Feeds 规则对齐).

UPDATE llm_prompt_templates
SET
    system_prompt = system_prompt || E'

**关键人物 (people) 提取规则:**
1. 提取文档中被讨论的政治人物、央行官员、CEO、分析师、经济学家等 (不仅是文档作者).
2. 每条: name(**原文中出现的名字原样保留**,如 Trump 就写 Trump、Powell 就写 Powell), name_en(英文全名), role, affiliation(可选).
3. role 枚举: politician | central_banker | ceo | executive | analyst | economist | journalist | founder | other
4. 示例: "Trump" → name:"Trump", name_en:"Donald Trump", role:"politician"
        "Powell" → name:"Powell", name_en:"Jerome Powell", role:"central_banker", affiliation:"Federal Reserve"
        "马云" → name:"马云", name_en:"Jack Ma", role:"executive"
5. 不要编造未出现的人物; 无则返回 [].

**国家/地区 (countries) 提取规则:**
1. 使用英文全名: United States, China, Japan, European Union, United Kingdom, Singapore, Hong Kong, Taiwan, South Korea, Iran, Russia, Ukraine, Australia, India, Germany, France 等.
2. 仅提取文档**明确涉及**的国家/地区 (关税、外交、央行政策、地缘冲突、区域市场、公司总部所在地讨论等).
3. 不要仅因 ticker 或公司名推断国家; 无则返回 [].',
    user_prompt_template = replace(
        replace(
            replace(
                user_prompt_template,
                '### 3. 投资概念/主题（Concepts）',
                '### 3. 关键人物（People）
- 被讨论的重要人物, 每条含 name / name_en / role / affiliation(可选).
- name: 原文中出现的写法原样保留; name_en: 英文全名.
- role: politician | central_banker | ceo | executive | analyst | economist | journalist | founder | other
- 无则返回 [].

### 4. 国家/地区（Countries）
- 国家英文全名数组, 如 ["United States", "China", "Japan"].
- 仅文档明确涉及的国家; 无则返回 [].

### 5. 投资概念/主题（Concepts）'
            ),
            '### 4. 置信度与内容摘要',
            '### 6. 置信度与内容摘要'
        ),
        '"concepts": ["概念1", "概念2", "概念3"],
  "confidence": 0.85,
  "summary": "用一段话准确概括文档核心内容..."',
        '"concepts": ["概念1", "概念2", "概念3"],
  "people": [{"name": "Powell", "name_en": "Jerome Powell", "role": "central_banker", "affiliation": "Federal Reserve"}],
  "countries": ["United States"],
  "confidence": 0.85,
  "summary": "用一段话准确概括文档核心内容..."'
    ),
    output_format = '{"type": "json", "json_schema": {"type": "object", "required": ["sector", "sector_en", "industry", "industry_en", "tickers", "concepts", "people", "countries", "confidence", "summary"], "properties": {"sector": {"type": "string"}, "sector_en": {"type": "string"}, "industry": {"type": "string"}, "industry_en": {"type": "string"}, "tickers": {"type": "array", "items": {"type": "string"}}, "concepts": {"type": "array", "items": {"type": "string"}}, "people": {"type": "array", "items": {"type": "object", "properties": {"name": {"type": "string"}, "name_en": {"type": "string"}, "role": {"type": "string"}, "affiliation": {"type": ["string", "null"]}}}}, "countries": {"type": "array", "items": {"type": "string"}}, "confidence": {"type": "number", "minimum": 0, "maximum": 1}, "summary": {"type": "string"}}}}',
    updated_at = CURRENT_TIMESTAMP
WHERE template_code = 'content_tag_v1';
