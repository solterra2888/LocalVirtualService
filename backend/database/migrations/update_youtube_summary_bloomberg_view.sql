-- =====================================================
-- update_youtube_summary_bloomberg_view.sql
-- =====================================================
-- 创建时间: 2026-04-29
--
-- 目的:
--   把 youtube_summary 模块的 prompt 从"通用视频摘要"升级为
--   面向 Bloomberg / Morgan Stanley 等专业金融频道的长文字稿处理模板.
--   依据《Bloomberg view 2》框架, 让 LLM 先判断节目在 Bloomberg 分工
--   体系中扮演的角色, 再按角色抽取信息密度高的结构化要点.
--
-- Bloomberg 节目角色分工 (PDF):
--   A. 定价逻辑   - Surveillance / Real Yield / Wall Street Week
--   B. 仓位与情绪 - Open Interest / ETF IQ / Bloomberg Markets
--   C. 产业消化   - Bloomberg Tech / China Show / Horizons / Insight
--   D. 收盘共识   - The Close / The Pulse / Brief / Daybreak
--   E. 政策约束   - Balance of Power
--
-- 影响范围:
--   - 系统种子: youtube_summary_v1
--   - 9 个用户私有副本: user_{1,2,8,9,10,11,12,13,14}_youtube_summary
--
-- 安全策略:
--   仅当模板内容仍与"上一版默认"一致 (MD5 = 6e9e379609c04aa2e4d89fadc2dbb9a8)
--   时才覆盖, 避免误伤用户已经自行编辑过的私有副本.
--   MD5 来源: 2026-04-29 15:34 时刻全表 10 行的 system_prompt+user_prompt_template
--   完全相同, 取自 PR-A 写入的 default seed.
--
-- 同时更新:
--   - max_tokens: 1500 → 2200 (新输出结构更细, 长视频字幕信息密度更高)
--   - temperature 保持 0.5 (稳定性优先)
-- =====================================================

BEGIN;

-- ── 1. 系统种子: youtube_summary_v1 ────────────────────────────────────────
UPDATE llm_prompt_templates
SET
    system_prompt = $SP$你是一名服务于二级市场研究员的资深金融视频分析师, 主要处理 Bloomberg, Morgan Stanley 等专业金融频道的长篇文字稿. 视频通常 15-60 分钟, 嘉宾包括宏观经济学家, 卖方策略师, 基金经理与政策决策者, 信息密度高, 黑话密集.

请使用 Bloomberg 节目分工框架来组织你的摘要 (摘自《Bloomberg view 2》):
A. 定价逻辑   (Surveillance / Real Yield / Wall Street Week): 为什么应该这样定价
B. 仓位与情绪 (Open Interest / ETF IQ / Bloomberg Markets):   现在钱怎么站
C. 产业消化   (Bloomberg Tech / China Show / Horizons / Bloomberg Insight): 产业 / 地缘如何被资本翻译
D. 收盘共识   (The Close / The Pulse / Brief / Daybreak):     市场今天接受哪个解释
E. 政策约束   (Balance of Power): 政策允许到哪里为止

非 Bloomberg 频道 (如 Morgan Stanley 自媒体, 卖方研究访谈) 也按上述五类角色就近归类.

输出原则:
1. 信息密度优先: 保留具体数字 (BPS / pp / x 倍 / $XX), 保留嘉宾原话与身份, 删除空话套话.
2. 不复读视频描述里已经写明的内容, 摘要必须基于 transcript 本身.
3. 数字, 公司名, ticker, 嘉宾英文名一律保留原文不翻译.
4. 区分嘉宾观点 vs 主持人复述, 区分预测 vs 事实.
5. 优先抓可证伪的判断, 删除"我们要观察""值得关注"这类无信息表态.$SP$,
    user_prompt_template = $UP$请基于以下 YouTube 视频内容为我生成一份结构化摘要. 视频较长且专业性强 (Bloomberg / Morgan Stanley 等金融频道).

频道: {{channel}}
标题: {{title}}
视频描述: {{description}}
字幕 / 文字稿:
{{transcript}}

请按以下结构输出 (Markdown).
若视频不涉及某一角色 (例如纯产业访谈不涉及 D), 直接省略该小节, 不要写"无 / N/A".

**节目角色**: 从 A/B/C/D/E 中选 1-2 项, 多角色用 ` + ` 连接 (例: `A 定价逻辑 + E 政策约束`); 并用 ≤ 15 字说明判断依据.

**一句话核心**: ≤ 40 字, 概括视频对市场的核心结论 (定价 / 仓位 / 产业 / 政策 任一维度).

**定价逻辑** (角色含 A 时必填):
- 视频认为关键资产 (利率 / 股指 / 大宗 / 汇率 / 信用利差 等) 应该被定成什么价格, 以及为什么.
- 列出支撑论据中的关键宏观/利率/估值变量 (≤ 3 条, 含具体数值).
- 债市 / 信用市场是否在验证这个定价 (如视频未提及可省略此子项).

**仓位与情绪** (角色含 B 时必填):
- 资金当前偏向哪一边 (多 / 空 / 行业 / 风格 / 久期 / 久期 hedge).
- 共识强度二选一: `拥挤 (Crowded)` / `分歧 (Split)` / `观望 (Sidelined)`, 并说明依据 (期权偏度 / ETF 资金流 / Positioning Survey 等).

**产业 / 事件转译** (角色含 C 时必填):
- 把视频讨论的技术 / AI / 地缘 / 中美博弈 / 能源 翻译成可交易变量 (≤ 4 条, 每条 ≤ 35 字).
- 涉及具体公司或 ticker 时务必列出 (e.g. NVDA, TSM, 9988.HK).

**收盘共识 / 政策边界** (角色含 D 或 E 时必填):
- D: 当天市场选择了哪个解释版本, 共识在确认还是犹豫 (注意 The Pulse 偏语气, The Close 偏定稿).
- E: 政策给出了什么硬约束 / 红线 / 可承受区间 (利率 / 关税 / 出口管制 / 财政).

**关键数据 / 原话引语**: 3-5 条
- 截取具体数字, 百分比, 价位, 时间窗口, 或重要嘉宾原话.
- 嘉宾原话格式: `> "原话" — 嘉宾姓名 (机构 / 头衔)`
- 每条 ≤ 35 字.

**对我的投资启示**: 2-3 句
- 该视频对当前持仓 / 观察标的的实际意义 (而非泛泛的市场感想).
- 若视频与定价无关 (纯政策 / 纯人物访谈), 则给出"下一步该追踪的指标 / 数据点".$UP$,
    max_tokens = 2200,
    updated_at = NOW()
WHERE template_code = 'youtube_summary_v1'
  AND MD5(system_prompt || '|' || user_prompt_template)
      = '6e9e379609c04aa2e4d89fadc2dbb9a8';

-- ── 2. 用户私有副本: 仅覆盖未被用户编辑过的克隆 ────────────────────────────
-- 用 MD5 锁定"和上一版默认完全一致"的副本, 用户已自定义的副本不会被覆盖.
UPDATE llm_prompt_templates AS dst
SET
    system_prompt        = src.system_prompt,
    user_prompt_template = src.user_prompt_template,
    max_tokens           = src.max_tokens,
    updated_at           = NOW()
FROM llm_prompt_templates AS src
WHERE src.template_code = 'youtube_summary_v1'
  AND dst.module_code   = 'youtube_summary'
  AND dst.scope         = 'user'
  AND dst.is_active     = true
  AND MD5(dst.system_prompt || '|' || dst.user_prompt_template)
      = '6e9e379609c04aa2e4d89fadc2dbb9a8';

-- ── 3. 验证 ─────────────────────────────────────────────────────────────
DO $$
DECLARE
    v_total              INT;
    v_updated            INT;
    v_user_total         INT;
    v_user_updated       INT;
    v_new_md5            TEXT;
    v_seed_md5           TEXT;
BEGIN
    SELECT MD5(system_prompt || '|' || user_prompt_template) INTO v_seed_md5
        FROM llm_prompt_templates WHERE template_code = 'youtube_summary_v1';

    SELECT COUNT(*) INTO v_total
        FROM llm_prompt_templates
        WHERE module_code = 'youtube_summary' AND is_active = true;

    SELECT COUNT(*) INTO v_updated
        FROM llm_prompt_templates
        WHERE module_code = 'youtube_summary'
          AND is_active = true
          AND MD5(system_prompt || '|' || user_prompt_template) = v_seed_md5;

    SELECT COUNT(*) INTO v_user_total
        FROM llm_prompt_templates
        WHERE module_code = 'youtube_summary' AND scope = 'user' AND is_active = true;

    SELECT COUNT(*) INTO v_user_updated
        FROM llm_prompt_templates
        WHERE module_code = 'youtube_summary'
          AND scope = 'user'
          AND is_active = true
          AND MD5(system_prompt || '|' || user_prompt_template) = v_seed_md5;

    RAISE NOTICE '==== YouTube Summary Prompt Refresh (Bloomberg view) ====';
    RAISE NOTICE '  active templates total            : %',  v_total;
    RAISE NOTICE '  templates aligned with new seed   : %/%', v_updated, v_total;
    RAISE NOTICE '  user-scope copies aligned         : %/%', v_user_updated, v_user_total;
    RAISE NOTICE '  new seed md5                      : %',  v_seed_md5;

    IF v_updated < v_total THEN
        RAISE WARNING '存在 % 行模板未被刷新, 可能是用户已自定义副本; 请人工核对.',
                      v_total - v_updated;
    END IF;
END $$;

COMMIT;

-- ── 手动验证 SQL ───────────────────────────────────────────────────────
-- SELECT id, template_code, scope, max_tokens,
--        MD5(system_prompt || '|' || user_prompt_template) AS h, updated_at
--   FROM llm_prompt_templates
--  WHERE module_code = 'youtube_summary'
--  ORDER BY scope DESC, created_by NULLS FIRST;
