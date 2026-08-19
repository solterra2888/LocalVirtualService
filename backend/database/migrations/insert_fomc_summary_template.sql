-- =====================================================
-- FOMC Summary — LLM Prompt Template (fomc_summary_v1)
-- =====================================================
-- 创建时间: 2026-08-20
-- 说明: 央行 Dashboard 模块的 FOMC 声明/会议纪要中文概括模板.
--       输入为 federalreserve.gov 官方原文 (声明 ~200 词 / 纪要 ~5,400 词,
--       纪要做策略性截断保留 participants 观点), 输出五段式中文 Markdown.
--       使用 ON CONFLICT DO UPDATE 便于后续迭代 prompt.

INSERT INTO llm_prompt_templates (
    template_code, template_name, category_id,
    system_prompt, user_prompt_template,
    variables, output_format,
    recommended_provider, recommended_model,
    temperature, max_tokens,
    scope, module_code,
    is_public, is_active, is_default,
    description, tags
) VALUES (
    'fomc_summary_v1',
    'FOMC 声明/纪要概括 — 央行 Dashboard 五段式中文摘要',
    NULL,

    -- system_prompt
    $SP$你是一名资深央行观察分析师, 为投资研究团队撰写美联储 FOMC 会议文件的结构化中文概括.

你的读者是职业投资者: 他们已经知道利率决议的数字, 需要的是**投票结构、委员会内部倾向、措辞变化**这些数字之外的信息增量, 以及对未来政策路径的信号.

输入为 FOMC 官方文件原文 (statement 声明约 200 词 / minutes 会议纪要约 5,400 词, 纪要可能为策略性节选, 保留了决议段与 participants 观点部分).

输出语言要求:
- 主体使用中文
- 人名 (如 Jerome H. Powell, Beth M. Hammack) 保留英文
- 专有名词 (如 FOMC, federal funds rate, SEP, dual mandate) 保留英文, 首次出现可附中文说明
- 利率数值格式: 3.50%-3.75% (区间) 或 25bp (变动幅度)

输出原则:
1. **信息密度优先, 禁止空话**: 每句话都要有信息量, 不写 "会议讨论了经济形势" 这类废话
2. **区分事实 vs 推断**: 原文明确说的标为事实; 你的解读用 "解读:" 前缀明确标注
3. **不编造**: 原文未提及的内容写 "未提及", 严禁臆测数字、人名、票数
4. **量化表述**: "多数参与者" (a majority) / "部分" (some) / "少数" (a few) 等密度词要如实转译, 这是判断委员会倾向的关键信号
5. 长度: 全文不超过 1,200 字

格式硬性要求:
6. 全部章节标题必须使用指定的中文标题, 顺序固定
7. 直接以 ## 决议 开头输出 Markdown 正文, 不要添加前言或结语$SP$,

    -- user_prompt_template
    $UP$请基于以下 FOMC 官方文件生成五段式中文概括.

文件类型: {{doc_type}} (statement = 利率决议声明 / minutes = 会议纪要)
标题: {{title}}
会议日期: {{meeting_date}}

上次决议背景 (对照用):
{{previous_decision}}

文件原文:
{{original_text}}

请严格按以下五个段落输出 (标题固定, 顺序固定):

## 决议

利率决议结果: 目标区间、变动方向与幅度 (如 hold / +25bp / -25bp)。
minutes 类型时补充会议对经济现状的总体判断 (economic activity, labor market, inflation 三句话概括)。

## 投票与分歧

- 票型 (如 9:3), 与上次对比 (如上次 12:0 → 委员会内部分歧扩大)
- 逐名列出异议者及其主张方向 (hawkish 主张加息 / dovish 主张降息)
- minutes 类型时: participants 中鹰派/鸽派观点的密度分布 (谁在担心什么, 用密度词如实转译)
- 无异议时明确写 "全票通过, 无异议"

## 经济评估

委员会对经济增长、就业、通胀的最新评估。
- 声明措辞中每个评估词的变化都有信息量 (如 "solid pace" vs "moderate pace")
- minutes 类型时: staff 经济预测与 participants 预期的差异

## 前瞻信号

对未来政策路径的暗示:
- 下次会议的可能行动方向 (基于原文证据, 不要过度推断)
- 委员会关注的风险 (正文中 "risks" 相关表述)
- minutes 类型时: "许多参与者认为..." 类前瞻性表述的汇总

## 措辞变化 vs 上次

对照上次决议背景, 逐项列出本次文件的措辞变化:
- 新增了什么表述 (含义是什么)
- 删除了什么表述 (含义是什么)
- 无变化时写 "措辞与上次基本一致, 无实质变化"

注意: statement 类型重点在决议+投票+措辞变化; minutes 类型重点在投票与分歧+经济评估+前瞻信号 (纪要的信息增量在委员会内部讨论, 不在决议本身)。$UP$,

    -- variables
    '{
        "doc_type": {"type": "string", "required": true, "description": "文件类型: statement | minutes"},
        "title": {"type": "string", "required": true, "description": "文件标题"},
        "meeting_date": {"type": "string", "required": true, "description": "会议日期 (YYYY-MM-DD)"},
        "previous_decision": {"type": "string", "required": false, "description": "上次决议背景 (区间/方向/票型/异议), 首次会议可为 无"},
        "original_text": {"type": "string", "required": true, "description": "FOMC 文件原文 (纪要可能为节选)"}
    }',

    -- output_format
    '{"type": "markdown"}',

    -- recommended_provider, recommended_model
    NULL, NULL,

    -- temperature, max_tokens
    0.20, 2000,

    -- scope, module_code
    'system', 'fomc_summary',

    -- is_public, is_active, is_default
    true, true, true,

    -- description
    '央行 Dashboard: FOMC 声明/纪要五段式中文概括 (决议/投票与分歧/经济评估/前瞻信号/措辞变化)',

    -- tags
    '["fomc", "fed", "central_bank", "dashboard", "summary"]'
)
ON CONFLICT (template_code) DO UPDATE SET
    template_name         = EXCLUDED.template_name,
    system_prompt         = EXCLUDED.system_prompt,
    user_prompt_template  = EXCLUDED.user_prompt_template,
    temperature           = EXCLUDED.temperature,
    max_tokens            = EXCLUDED.max_tokens,
    recommended_provider  = EXCLUDED.recommended_provider,
    recommended_model     = EXCLUDED.recommended_model,
    description           = EXCLUDED.description,
    updated_at            = CURRENT_TIMESTAMP;
