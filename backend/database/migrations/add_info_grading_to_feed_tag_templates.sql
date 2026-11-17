-- =====================================================
-- Feeds 打标模板: 新增信息分级 (L/E 双轴) 输出
-- =====================================================
-- 创建时间: 2026-11-03
-- 说明: 落地 tools/morning-brief/INFO_GRADING_STANDARD.md 的 L/E 双轴分级标准,
--       令 content_tag_feeds_v1 与 tweet_signal_v1 两个打标模板额外输出:
--         info_level     L1-L4  候选重要度 (对内容所聚焦标的/行业的冲击潜力;
--                                 消费端按具体标的浮动重定, 打标阶段产全局候选值)
--         evidence_level E1-E3  证据等级 (来源属性与可独立核验性, 与重要度正交)
--       并新增 author_context 变量 (作者标识, E 级来源判断线索).
--
-- 兼容性 (方案 A, 渐进):
--   - tweet_signal_v1 保留 importance 字段 (FEEDS 模块自有产物),
--     prompt 中定义映射 L1→high / L2→medium / L3,L4→low 保证两者一致
--   - output_format 的 json_schema 仅把新字段加入 properties, 不加入 required,
--     LLM 偶发缺失时由 FeedContentTaggingService._normalize_tag_data 容错为 NULL
--   - 存量标签行不回填, NULL = 未评级
--
-- 依赖: add_info_grading_to_subscription_content_tags.sql (先加列)
-- 模式: 对标 add_people_countries_to_content_tag_feeds_v1.sql
--       (system_prompt 追加 + user_prompt 锚点 replace + output_format/variables 整体 SET),
--       末尾 DO 块校验锚点命中, 未命中则报错阻止静默失败.
--       注意: 本迁移要求 insert_tweet_signal_v1_template.sql 已应用 (否则 UPDATE 0 行,
--       校验块会显式报错).

-- ─────────────────────────────────────────────
-- 1. content_tag_feeds_v1 (重型模板: youtube/podcast/news/长推)
-- ─────────────────────────────────────────────
UPDATE llm_prompt_templates
SET
    system_prompt = system_prompt || E'

**信息分级 (L/E 双轴) 规则:**
对每条内容额外输出两个**正交**等级 (重要度与证据不可互相替代: 重要的信息可能只是传闻, 可靠的信息可能只是例行披露).

L 轴 — info_level, 候选重要度. 评"对内容所聚焦标的/行业的投资判断冲击潜力" (消费端会按具体标的浮动重定, 此处给全局候选值):
- L1: 可能改变投资逻辑、盈利模型或核心风险判断 (重大业绩/指引变化、监管决定、并购、地缘重大事件)
- L2: 有实质影响, 但暂不足以改变模型判断 (行业边际变化、同业信号、二线数据)
- L3: 具有同行交流或市场情报价值, 通常不足以改变方向判断 (板块情绪、次要动态)
- L4: 例行记录或低价值噪音 (纯社交、闲聊、无投资信息)

E 轴 — evidence_level, 证据等级. **只看来源属性与可独立核验性**, 不看内容听起来多合理:
- E1: 一手、可直接核验的公司或官方证据 (公告、监管披露、交易所文件、央行/政府声明原文)
- E2: 可靠专业来源, 或多个独立来源相互印证 (权威财经媒体报道、官方信息被转述)
- E3: 单一消息源、社交媒体观点或市场传闻 (KOL 个人观点、无法核验)

E 轴判定规则:
- 转述降档: [引用]/[转推] 段落中的一手信息, 对本条内容按 E2 计; 但作者本身即官方账号时除外
- 作者字段 (若给出) 是来源线索: 官方机构账号 (央行/交易所/公司 IR/官方媒体) 发布自身领域信息 → 至少 E2; 个人 KOL 观点 → E3
- fail closed: 无法判断来源属性时保守给 E3, 不要猜测来源',
    user_prompt_template = replace(
        replace(
            replace(
                replace(
                    user_prompt_template,
                    '**channel_type**: `{{channel_type}}`',
                    '**channel_type**: `{{channel_type}}`
**作者** (来源线索, 用于 E 级判断; 可能为空): {{author_context}}'
                ),
                '### 6. 置信度与摘要',
                '### 6. 信息分级（L/E 双轴）
- **info_level**: L1-L4 枚举, 见 system_prompt 分级规则 (L 轴 = 候选重要度).
- **evidence_level**: E1-E3 枚举, 只看来源属性 (E 轴 = 证据等级); 作者是官方机构账号或内容含官方原文 → E1/E2, 个人观点/传闻 → E3.

### 7. 置信度与摘要'
            ),
            '  "countries": ["US"],
  "confidence": 0.85,',
            '  "countries": ["US"],
  "info_level": "L2",
  "evidence_level": "E2",
  "confidence": 0.85,'
        ),
        '- summary 必填，50-200 字，单句概括内容要点。',
        '- summary 必填，50-200 字，单句概括内容要点。
- info_level / evidence_level 必填，枚举值 L1-L4 / E1-E3。'
    ),
    output_format = '{"type": "json", "json_schema": {"type": "object", "required": ["sector", "sector_en", "industry", "industry_en", "tickers", "concepts", "people", "countries", "confidence", "summary"], "properties": {"sector": {"type": "string"}, "sector_en": {"type": "string"}, "industry": {"type": "string"}, "industry_en": {"type": "string"}, "tickers": {"type": "array", "items": {"type": "string"}}, "concepts": {"type": "array", "items": {"type": "string"}}, "people": {"type": "array", "items": {"type": "object", "properties": {"name": {"type": "string"}, "name_en": {"type": "string"}, "role": {"type": "string"}, "affiliation": {"type": ["string", "null"]}}}}, "countries": {"type": "array", "items": {"type": "string"}}, "info_level": {"type": ["string", "null"], "enum": ["L1", "L2", "L3", "L4", null], "description": "候选重要度 L1-L4 (对内容所聚焦标的/行业的投资判断冲击潜力)"}, "evidence_level": {"type": ["string", "null"], "enum": ["E1", "E2", "E3", null], "description": "证据等级 E1-E3 (来源属性与可独立核验性)"}, "confidence": {"type": "number", "minimum": 0, "maximum": 1}, "summary": {"type": "string"}}}}',
    variables = '{"file_content": {"type": "string", "required": true, "description": "需要分析的 Feeds 内容 (推文/视频字幕), 已组装好上下文 (Twitter 的 quoted/retweeted 已拼接, YouTube 的 title+transcript 已拼接)"}, "channel_type": {"type": "string", "required": true, "description": "渠道类型: twitter | youtube"}, "author_context": {"type": "string", "required": false, "default": "", "description": "作者标识 (name @handle), E 级来源判断线索"}, "additional_context": {"type": "string", "required": false, "default": "", "description": "额外的上下文信息, 如用户已知的行业或公司信息"}}',
    description = 'Feeds (Twitter/YouTube) 单条内容自动打标 + 短摘要. 对标 content_tag_v1, 针对短文本与 ASR 场景优化, 新增强制 summary 字段; 2026-11 新增 L/E 信息分级 (info_level/evidence_level)',
    updated_at = CURRENT_TIMESTAMP
WHERE template_code = 'content_tag_feeds_v1';

-- ─────────────────────────────────────────────
-- 2. tweet_signal_v1 (轻量模板: twitter 短推)
-- ─────────────────────────────────────────────
UPDATE llm_prompt_templates
SET
    system_prompt = system_prompt || E'

**信息分级 (L/E 双轴):**
在 importance 之外, 额外输出两个**正交**等级:

L 轴 — info_level (L1-L4), 候选重要度, 评"对内容所聚焦标的/行业的投资判断冲击潜力":
- L1: 可能改变投资逻辑、盈利模型或核心风险判断 (breaking 级事件、重大数据、监管决定)
- L2: 有实质影响, 但暂不足以改变模型判断 (明确行业观点、同业信号)
- L3: 同行交流/市场情报价值 (情绪、次要动态)
- L4: 纯社交/噪音
与 importance 的映射 (**先定 info_level, 再按映射填 importance**, 保证两者一致):
  L1 → high; L2 → medium; L3/L4 → low

E 轴 — evidence_level (E1-E3), 证据等级, **只看来源属性与可核验性**, 与重要度无关:
- E1: 一手官方证据 (公告/监管披露/央行声明原文, 官方账号直接发布)
- E2: 可靠专业来源或多个独立来源相互印证 (权威媒体报道、官方信息被转述)
- E3: 单一来源观点/传闻 (KOL 个人观点、市场传闻) — 默认保守档
- 转述降档: [转推]/[引用] 段内的一手信息按 E2 计; 作者即官方账号时除外
- fail closed: 无法判断来源属性 → E3, 不要猜测来源',
    user_prompt_template = replace(
        replace(
            replace(
                user_prompt_template,
                '**channel_type**: `twitter`',
                '**channel_type**: `twitter`
**作者** (来源线索, 用于 E 级判断; 可能为空): {{author_context}}'
            ),
            '8. confidence: 0-1',
            '8. info_level / evidence_level: 必填枚举 (L1-L4 / E1-E3); importance 由 info_level 映射得出 (L1→high, L2→medium, L3/L4→low)
9. confidence: 0-1'
        ),
        '  "importance": "high",',
        '  "importance": "high",
  "info_level": "L1",
  "evidence_level": "E2",'
    ),
    output_format = '{"type": "json", "json_schema": {"type": "object", "required": ["tickers", "concepts", "people", "countries", "stance", "signal_type", "importance", "summary", "confidence"], "properties": {"sector": {"type": ["string", "null"]}, "sector_en": {"type": ["string", "null"]}, "industry": {"type": ["string", "null"]}, "industry_en": {"type": ["string", "null"]}, "tickers": {"type": "array", "items": {"type": "string"}}, "concepts": {"type": "array", "items": {"type": "string"}}, "people": {"type": "array", "items": {"type": "object", "properties": {"name": {"type": "string"}, "name_en": {"type": "string"}, "role": {"type": "string"}, "affiliation": {"type": ["string", "null"]}}}}, "countries": {"type": "array", "items": {"type": "string"}}, "stance": {"type": "string", "enum": ["bullish", "bearish", "neutral", "mixed", "none"]}, "signal_type": {"type": "string", "enum": ["announcement", "opinion", "forwarding", "reaction", "question", "data"]}, "importance": {"type": "string", "enum": ["high", "medium", "low"]}, "info_level": {"type": ["string", "null"], "enum": ["L1", "L2", "L3", "L4", null], "description": "候选重要度 L1-L4; importance 由其映射 (L1→high, L2→medium, L3/L4→low)"}, "evidence_level": {"type": ["string", "null"], "enum": ["E1", "E2", "E3", null], "description": "证据等级 E1-E3 (来源属性与可独立核验性)"}, "summary": {"type": "string"}, "confidence": {"type": "number", "minimum": 0, "maximum": 1}}}}',
    variables = '{"file_content": {"type": "string", "required": true}, "channel_type": {"type": "string", "required": true}, "author_context": {"type": "string", "required": false, "default": "", "description": "作者标识 (name @handle), E 级来源判断线索"}, "additional_context": {"type": "string", "required": false, "default": ""}}',
    description = 'Twitter 短推/速报轻量信号提取: stance/signal_type/importance/people/countries; 与 content_tag_feeds_v1 智能路由; 2026-11 新增 L/E 信息分级 (info_level/evidence_level)',
    updated_at = CURRENT_TIMESTAMP
WHERE template_code = 'tweet_signal_v1';

-- ─────────────────────────────────────────────
-- 3. 锚点命中校验 (replace 未命中时静默无变化, 必须显式报错)
-- ─────────────────────────────────────────────
DO $$
DECLARE
    v_prompt TEXT;
    v_format TEXT;
    v_vars   TEXT;
BEGIN
    SELECT user_prompt_template, output_format::text, variables::text
      INTO v_prompt, v_format, v_vars
      FROM llm_prompt_templates WHERE template_code = 'content_tag_feeds_v1';
    IF v_prompt IS NULL THEN
        RAISE EXCEPTION 'content_tag_feeds_v1 模板不存在, 请先应用 insert_content_tag_feeds_v1_template.sql';
    END IF;
    IF v_prompt NOT LIKE '%info_level%'
       OR v_prompt NOT LIKE '%author_context%'
       OR v_prompt NOT LIKE '%### 7. 置信度与摘要%' THEN
        RAISE EXCEPTION 'content_tag_feeds_v1 user_prompt 锚点未全部命中 (模板可能被人工改过), 请核对模板现状后修正本迁移的 replace 锚点';
    END IF;
    IF v_format NOT LIKE '%info_level%' OR v_vars NOT LIKE '%author_context%' THEN
        RAISE EXCEPTION 'content_tag_feeds_v1 output_format/variables 升级未生效';
    END IF;

    SELECT user_prompt_template, output_format::text, variables::text
      INTO v_prompt, v_format, v_vars
      FROM llm_prompt_templates WHERE template_code = 'tweet_signal_v1';
    IF v_prompt IS NULL THEN
        RAISE EXCEPTION 'tweet_signal_v1 模板不存在, 请先应用 insert_tweet_signal_v1_template.sql';
    END IF;
    IF v_prompt NOT LIKE '%info_level%'
       OR v_prompt NOT LIKE '%author_context%'
       OR v_prompt NOT LIKE '%9. confidence: 0-1%' THEN
        RAISE EXCEPTION 'tweet_signal_v1 user_prompt 锚点未全部命中 (模板可能被人工改过), 请核对模板现状后修正本迁移的 replace 锚点';
    END IF;
    IF v_format NOT LIKE '%info_level%' OR v_vars NOT LIKE '%author_context%' THEN
        RAISE EXCEPTION 'tweet_signal_v1 output_format/variables 升级未生效';
    END IF;
END $$;

-- =====================================================
-- 验证
-- =====================================================
-- SELECT template_code,
--        user_prompt_template LIKE '%info_level%'            AS prompt_ok,
--        output_format::text LIKE '%evidence_level%'         AS format_ok,
--        variables::text LIKE '%author_context%'             AS vars_ok
-- FROM llm_prompt_templates
-- WHERE template_code IN ('content_tag_feeds_v1', 'tweet_signal_v1');
