-- ============================================================
-- add_youtube_analysis_module.sql
-- ============================================================
-- 创建时间: 2026-05-02
--
-- 目的:
--   新增独立的 youtube_analysis USER_OWNED 模块，专服务
--   Upload Sources → YouTube Link 的深度分析链路，与
--   Feeds 的 youtube_summary 模块完全解耦。
--
--   模板变量约定：与 audio_analysis_v1 保持一致，只用 {{file_content}}
--   （由系统自动注入转写文本，用户无需手动填写）。
--   system_prompt clone 自 youtube_summary_v1（Bloomberg-view 风格），
--   user_prompt_template 重新编写，移除 {{channel}}/{{title}}/
--   {{description}}/{{transcript}} 等需要手动填写的字段。
--
--   Step 0: 修复已运行过本 migration 的库（UPDATE 幂等）。
--   Step 1: 插入 youtube_analysis_v1 系统种子模板。
--   Step 2: 为所有现有用户批量初始化 user-scope 私有行。
--
-- 幂等: WHERE NOT EXISTS / 条件 WHERE 保证重复执行无副作用。
-- 事务: 本文件不包含 BEGIN/COMMIT，由调用方控制。
-- ============================================================

-- ==========================================================
-- Step 0: 修复已创建的 youtube_analysis 模板（若存在）
--         将 {{transcript}}/{{channel}}/{{title}}/{{description}}
--         统一替换为 {{file_content}}，使行为与 audio_analysis_v1 一致。
-- ==========================================================

UPDATE llm_prompt_templates
SET
    user_prompt_template = REPLACE(
        REPLACE(
            REPLACE(
                REPLACE(
                    REPLACE(user_prompt_template,
                        E'频道: {{channel}}\n标题: {{title}}\n视频描述: {{description}}\n字幕 / 文字稿:\n{{transcript}}',
                        E'字幕 / 文字稿:\n{{file_content}}'
                    ),
                    '{{transcript}}',
                    '{{file_content}}'
                ),
                '{{channel}}',
                ''
            ),
            '{{title}}',
            ''
        ),
        '{{description}}',
        ''
    ),
    variables = '{"file_content": {"type": "string", "required": true, "description": "视频字幕 / 文字稿（由系统自动注入，无需手动填写）"}}'::jsonb,
    updated_at = NOW()
WHERE module_code = 'youtube_analysis'
  AND is_active = true
  AND (
      user_prompt_template LIKE '%{{transcript}}%'
      OR variables::text LIKE '%transcript%'
      OR variables::text LIKE '%channel%'
  );

-- ==========================================================
-- Step 1: 插入 youtube_analysis_v1 系统种子模板
--         system_prompt clone 自 youtube_summary_v1（Bloomberg-view）
--         user_prompt_template 与 audio_analysis_v1 约定一致：只用 {{file_content}}
-- ==========================================================

INSERT INTO llm_prompt_templates (
    template_code,
    template_name,
    category_id,
    module_code,
    scope,
    system_prompt,
    user_prompt_template,
    variables,
    output_format,
    recommended_provider,
    recommended_model,
    temperature,
    max_tokens,
    is_active,
    is_default,
    is_public,
    description,
    tags,
    parent_template_id
)
SELECT
    'youtube_analysis_v1',
    'YouTube 视频深度分析 (默认)',
    category_id,
    'youtube_analysis',
    'system',
    system_prompt,   -- clone Bloomberg-view system_prompt from youtube_summary_v1
    -- user_prompt_template: 只用 {{file_content}}，与 audio_analysis_v1 约定一致
    E'请基于以下 YouTube 视频字幕 / 文字稿为我生成一份结构化深度分析。\n\n字幕 / 文字稿:\n{{file_content}}\n\n请按以下结构输出 (Markdown)。\n若视频不涉及某一角色 (例如纯产业访谈不涉及 D)，直接省略该小节，不要写"无 / N/A"。\n\n**节目角色**: 从 A/B/C/D/E 中选 1-2 项，多角色用 ` + ` 连接 (例: `A 定价逻辑 + E 政策约束`); 并用 ≤ 15 字说明判断依据。\n\n**一句话核心**: ≤ 40 字，概括视频对市场的核心结论。\n\n**定价逻辑** (角色含 A 时必填):\n- 视频认为关键资产应被定成什么价格，以及为什么。\n- 列出支撑论据中的关键宏观/利率/估值变量 (≤ 3 条，含具体数值)。\n\n**仓位与情绪** (角色含 B 时必填):\n- 资金当前偏向哪一边。\n- 共识强度: `拥挤` / `分歧` / `观望`，并说明依据。\n\n**产业 / 事件转译** (角色含 C 时必填):\n- 把视频讨论内容翻译成可交易变量 (≤ 4 条，每条 ≤ 35 字)。\n- 涉及具体公司或 ticker 时务必列出。\n\n**收盘共识 / 政策边界** (角色含 D 或 E 时必填):\n- D: 当天市场选择了哪个解释版本。\n- E: 政策给出了什么硬约束 / 红线 / 可承受区间。\n\n**关键数据 / 原话引语**: 3-5 条\n- 截取具体数字、百分比、价位，或重要嘉宾原话。\n- 嘉宾原话格式: `> "原话" — 嘉宾姓名 (机构 / 头衔)`\n- 每条 ≤ 35 字。\n\n**对我的投资启示**: 2-3 句\n- 该视频对当前持仓 / 观察标的的实际意义。',
    -- variables: 只声明 file_content，由系统自动注入，测试界面不显示多余字段
    '{"file_content": {"type": "string", "required": true, "description": "视频字幕 / 文字稿（由系统自动注入，无需手动填写）"}}'::jsonb,
    '{"type": "text"}'::jsonb,
    recommended_provider,
    recommended_model,
    temperature,
    max_tokens,
    true,
    false,
    true,
    '系统级 YouTube Upload 深度分析默认模板（Bloomberg-view 风格）。'
        || '与 Feeds youtube_summary 模块完全独立，只用 {{file_content}}，系统自动注入转写文本。',
    tags,
    id  -- parent_template_id 指向 youtube_summary_v1，便于后续"重置"参考
FROM llm_prompt_templates
WHERE template_code = 'youtube_summary_v1'
  AND scope = 'system'
  AND NOT EXISTS (
    SELECT 1 FROM llm_prompt_templates
    WHERE template_code = 'youtube_analysis_v1'
  );

-- ==========================================================
-- Step 2: 为所有现有用户批量初始化 user-scope 私有行
--         命名约定: user_{user_id}_youtube_analysis
-- ==========================================================

WITH seed AS (
    SELECT
        id              AS sys_template_id,
        template_name   AS sys_template_name,
        category_id,
        system_prompt,
        user_prompt_template,
        variables,
        output_format,
        recommended_provider,
        recommended_model,
        temperature,
        max_tokens,
        tags
    FROM llm_prompt_templates
    WHERE template_code = 'youtube_analysis_v1'
      AND scope = 'system'
      AND is_active = true
)
INSERT INTO llm_prompt_templates (
    template_code,
    template_name,
    category_id,
    module_code,
    scope,
    system_prompt,
    user_prompt_template,
    variables,
    output_format,
    recommended_provider,
    recommended_model,
    temperature,
    max_tokens,
    is_active,
    is_default,
    is_public,
    description,
    tags,
    created_by,
    parent_template_id
)
SELECT
    'user_' || u.id || '_youtube_analysis',
    s.sys_template_name || ' (我的)',
    s.category_id,
    'youtube_analysis',
    'user',
    s.system_prompt,
    s.user_prompt_template,
    s.variables,
    s.output_format,
    s.recommended_provider,
    s.recommended_model,
    s.temperature,
    s.max_tokens,
    true,
    false,
    false,
    '克隆自系统默认 youtube_analysis_v1，可在 My Templates 自由修改。与 Feeds youtube_summary 模板独立。',
    s.tags,
    u.id,
    s.sys_template_id
FROM users u
CROSS JOIN seed s
WHERE NOT EXISTS (
    SELECT 1 FROM llm_prompt_templates ut
    WHERE ut.created_by = u.id
      AND ut.module_code = 'youtube_analysis'
      AND ut.scope = 'user'
      AND ut.is_active = true
);

-- ==========================================================
-- 验证
-- ==========================================================
DO $$
DECLARE
    user_count      INT;
    seed_exists     INT;
    user_rows       INT;
    missing_pairs   INT;
    bad_vars        INT;
BEGIN
    SELECT COUNT(*) INTO user_count FROM users;

    SELECT COUNT(*) INTO seed_exists
    FROM llm_prompt_templates
    WHERE template_code = 'youtube_analysis_v1'
      AND scope = 'system'
      AND is_active = true;

    SELECT COUNT(*) INTO user_rows
    FROM llm_prompt_templates
    WHERE module_code = 'youtube_analysis'
      AND scope = 'user'
      AND is_active = true;

    SELECT COUNT(*) INTO missing_pairs
    FROM (
        SELECT u.id AS user_id
        FROM users u
        EXCEPT
        SELECT created_by
        FROM llm_prompt_templates
        WHERE module_code = 'youtube_analysis'
          AND scope = 'user'
          AND is_active = true
    ) gaps;

    -- 检查是否还有模板包含旧变量（{{transcript}} 等）
    SELECT COUNT(*) INTO bad_vars
    FROM llm_prompt_templates
    WHERE module_code = 'youtube_analysis'
      AND is_active = true
      AND (
          user_prompt_template LIKE '%{{transcript}}%'
          OR variables::text LIKE '%transcript%'
          OR variables::text LIKE '%channel%'
      );

    RAISE NOTICE '==== youtube_analysis module verification ====';
    RAISE NOTICE '  - system seed youtube_analysis_v1     : % (expect 1)', seed_exists;
    RAISE NOTICE '  - total users                          : %', user_count;
    RAISE NOTICE '  - user-scope youtube_analysis rows     : % (expect %)', user_rows, user_count;
    RAISE NOTICE '  - users missing youtube_analysis row   : % (expect 0)', missing_pairs;
    RAISE NOTICE '  - rows with old transcript variables   : % (expect 0)', bad_vars;

    IF seed_exists = 0 THEN
        RAISE EXCEPTION 'youtube_analysis_v1 system seed not found';
    END IF;
    IF missing_pairs <> 0 THEN
        RAISE EXCEPTION 'verification failed: % users missing youtube_analysis user-scope row', missing_pairs;
    END IF;
    IF bad_vars <> 0 THEN
        RAISE EXCEPTION 'verification failed: % rows still contain old {{transcript}}/{{channel}} variables', bad_vars;
    END IF;
END $$;
