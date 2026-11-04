-- =====================================================
-- 修复: feeds 打标模板 L/E 输出强制 (示例前置 + 必填)
-- =====================================================
-- 创建时间: 2026-11-03
-- 背景: add_info_grading_to_feed_tag_templates.sql 的两个 replace 锚点未命中
--       (线上模板曾被人工修改: 示例 countries 为 ["United States"] 而非种子的 ["US"],
--        重要提醒 summary 为 "100-200 字" 而非 "50-200 字"), 导致:
--         - feeds_v1 的 JSON 输出示例中没有 info_level/evidence_level
--         - 重要提醒中没有 L/E 必填提示
--       实测 qwen-plus 对示例形状的遵循远强于 prose: ~80% 响应缺少 L/E 字段
--       (llm_prompt_usage_logs: 07:00 时段 1916 次调用仅 427 次包含 info_level)。
--       tweet_signal_v1 两个锚点均命中, 不受影响。
-- 修复策略:
--   1. L/E 提到 JSON 示例**最前**两个字段 (模型对示例开头权重最高)
--   2. 说明条目与重要提醒加"必填"强调
--   3. output_format 的 required 补齐 (parse_output 虽不强制, 但保持 schema 诚实)
--   4. (代码侧) FeedContentTaggingService.tag_content 对缺失 L/E raise 触发 Celery 自动重试
-- 校验: 本迁移用**精确示例串**校验 (不再用会被 prose 命中的宽松 LIKE)。

-- ─────────────────────────────────────────────
-- 1. content_tag_feeds_v1
-- ─────────────────────────────────────────────
UPDATE llm_prompt_templates
SET
    user_prompt_template = replace(
        replace(
            replace(
                user_prompt_template,
                -- 1a. 示例块: L/E 前置为首两个字段 (锚点取自线上真实模板文本)
                '{
  "sector": "中文行业分类",',
                '{
  "info_level": "L2",
  "evidence_level": "E2",
  "sector": "中文行业分类",'
            ),
            -- 1b. 说明条目加必填强调
            '- **info_level**: L1-L4 枚举, 见 system_prompt 分级规则 (L 轴 = 候选重要度).',
            '- **info_level**（必填）: L1-L4 枚举, 见 system_prompt 分级规则 (L 轴 = 候选重要度); 必须作为输出 JSON 的前两个字段之一。'
        ),
        -- 1c. 重要提醒追加 L/E 必填 (锚点取自线上真实模板文本 "100-200 字")
        '- confidence 必须是 0-1 的浮点数。',
        '- confidence 必须是 0-1 的浮点数。
- info_level / evidence_level 必填，枚举值 L1-L4 / E1-E3，两个都不可缺少。'
    ),
    output_format = '{"type": "json", "json_schema": {"type": "object", "required": ["info_level", "evidence_level", "sector", "sector_en", "industry", "industry_en", "tickers", "concepts", "people", "countries", "confidence", "summary"], "properties": {"info_level": {"type": "string", "enum": ["L1", "L2", "L3", "L4"], "description": "候选重要度 L1-L4 (对内容所聚焦标的/行业的投资判断冲击潜力)"}, "evidence_level": {"type": "string", "enum": ["E1", "E2", "E3"], "description": "证据等级 E1-E3 (来源属性与可独立核验性)"}, "sector": {"type": "string"}, "sector_en": {"type": "string"}, "industry": {"type": "string"}, "industry_en": {"type": "string"}, "tickers": {"type": "array", "items": {"type": "string"}}, "concepts": {"type": "array", "items": {"type": "string"}}, "people": {"type": "array", "items": {"type": "object", "properties": {"name": {"type": "string"}, "name_en": {"type": "string"}, "role": {"type": "string"}, "affiliation": {"type": ["string", "null"]}}}}, "countries": {"type": "array", "items": {"type": "string"}}, "confidence": {"type": "number", "minimum": 0, "maximum": 1}, "summary": {"type": "string"}}}}',
    updated_at = CURRENT_TIMESTAMP
WHERE template_code = 'content_tag_feeds_v1';

-- ─────────────────────────────────────────────
-- 2. tweet_signal_v1: 示例 L/E 前置 + 去重原中位字段
-- ─────────────────────────────────────────────
UPDATE llm_prompt_templates
SET
    user_prompt_template = replace(
        replace(
            user_prompt_template,
            -- 2a. L/E 前置 (原示例中 info_level 位于 importance 之后, 命中过; 前置增强)
            '{
  "sector": null,',
            '{
  "info_level": "L1",
  "evidence_level": "E2",
  "sector": null,'
        ),
        -- 2b. 移除原中位位置的重复字段, 避免示例出现重复键
        '  "importance": "high",
  "info_level": "L1",
  "evidence_level": "E2",',
        '  "importance": "high",'
    ),
    output_format = '{"type": "json", "json_schema": {"type": "object", "required": ["info_level", "evidence_level", "tickers", "concepts", "people", "countries", "stance", "signal_type", "importance", "summary", "confidence"], "properties": {"info_level": {"type": "string", "enum": ["L1", "L2", "L3", "L4"], "description": "候选重要度 L1-L4; importance 由其映射 (L1→high, L2→medium, L3/L4→low)"}, "evidence_level": {"type": "string", "enum": ["E1", "E2", "E3"], "description": "证据等级 E1-E3 (来源属性与可独立核验性)"}, "sector": {"type": ["string", "null"]}, "sector_en": {"type": ["string", "null"]}, "industry": {"type": ["string", "null"]}, "industry_en": {"type": ["string", "null"]}, "tickers": {"type": "array", "items": {"type": "string"}}, "concepts": {"type": "array", "items": {"type": "string"}}, "people": {"type": "array", "items": {"type": "object", "properties": {"name": {"type": "string"}, "name_en": {"type": "string"}, "role": {"type": "string"}, "affiliation": {"type": ["string", "null"]}}}}, "countries": {"type": "array", "items": {"type": "string"}}, "stance": {"type": "string", "enum": ["bullish", "bearish", "neutral", "mixed", "none"]}, "signal_type": {"type": "string", "enum": ["announcement", "opinion", "forwarding", "reaction", "question", "data"]}, "importance": {"type": "string", "enum": ["high", "medium", "low"]}, "summary": {"type": "string"}, "confidence": {"type": "number", "minimum": 0, "maximum": 1}}}}',
    updated_at = CURRENT_TIMESTAMP
WHERE template_code = 'tweet_signal_v1';

-- ─────────────────────────────────────────────
-- 3. 强校验: 示例块必须以 L/E 开头 (精确串, 不接受 prose 命中)
-- ─────────────────────────────────────────────
DO $$
DECLARE
    v_prompt TEXT;
BEGIN
    SELECT user_prompt_template INTO v_prompt
      FROM llm_prompt_templates WHERE template_code = 'content_tag_feeds_v1';
    IF v_prompt IS NULL OR v_prompt NOT LIKE '%
  "info_level": "L2",
  "evidence_level": "E2",
  "sector": "中文行业分类",%' THEN
        RAISE EXCEPTION 'content_tag_feeds_v1 示例块 L/E 前置失败 (锚点未命中), 请核对模板文本';
    END IF;
    IF v_prompt NOT LIKE '%两个都不可缺少%' THEN
        RAISE EXCEPTION 'content_tag_feeds_v1 重要提醒追加失败';
    END IF;

    SELECT user_prompt_template INTO v_prompt
      FROM llm_prompt_templates WHERE template_code = 'tweet_signal_v1';
    IF v_prompt IS NULL OR v_prompt NOT LIKE '%
  "info_level": "L1",
  "evidence_level": "E2",
  "sector": null,%' THEN
        RAISE EXCEPTION 'tweet_signal_v1 示例块 L/E 前置失败 (锚点未命中), 请核对模板文本';
    END IF;
    -- 不应残留重复的中位字段
    IF v_prompt LIKE '%"importance": "high",
  "info_level": "L1",
  "evidence_level": "E2",%' THEN
        RAISE EXCEPTION 'tweet_signal_v1 示例存在重复 L/E 字段未清理';
    END IF;
END $$;

-- =====================================================
-- 验证
-- =====================================================
-- SELECT template_code,
--        (regexp_count(user_prompt_template, 'info_level') >= 3) AS le_in_example_and_bullets,
--        output_format::text LIKE '%\"required\": [\"info_level%' AS in_required
-- FROM llm_prompt_templates
-- WHERE template_code IN ('content_tag_feeds_v1','tweet_signal_v1');
