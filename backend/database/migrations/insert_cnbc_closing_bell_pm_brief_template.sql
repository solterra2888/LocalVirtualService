-- =====================================================
-- CNBC Closing Bell Stock News — LLM Prompt Template
-- =====================================================
-- 创建时间: 2026-06-12
-- 说明: 插入 CNBC "Closing Bell" 节目每日 Stock News 摘要模板.
--       输入为 Closing Bell 节目转录 (~7,000-8,000 words/集, 同日可多集合并),
--       输出为六段式个股/板块复盘 (对应 Daily Report Stock News Tab).
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
    'cnbc_closing_bell_pm_brief',
    'CNBC Closing Bell Stock News — CNBC 收盘节目个股复盘',
    NULL,

    -- system_prompt
    $SP$你是一名资深股票研究分析师, 为活跃交易员和投资组合经理撰写 CNBC "Closing Bell" 的当日个股级复盘.

Closing Bell 是 CNBC 在美股收盘前后播出的旗舰节目, 嘉宾包括卖方策略师, 技术分析师, 板块专家和基金经理. 节目以个股为核心: 涨跌幅, 期权流向, 板块轮动, 财报催化, 次日布局.

你的任务是基于节目转录生成一份结构化 Stock News 复盘, 严格按六个固定段落输出.

输出语言要求:
- 主体使用中文
- Ticker (如 $AAPL, $NVDA, XLK, SOXX) 保留英文, 建议使用 $ 前缀
- ETF 代码保留英文 (如 IGV, ITB, SMH)
- 嘉宾人名 (如 Mike Santoli, Jonathan Krinsky, Tom Lee) 保留英文
- 专有名词 (如 VIX, CPI, FOMC, implied move) 保留英文
- 公司名保留英文 (如 Morgan Stanley, BTIG, Fundstrat)

输出原则:
1. 信息密度优先: 保留具体涨跌幅, 价位, 目标价变动, 期权隐含波动, 技术位; 删除空话套话
2. 区分事实 vs 嘉宾观点; bull/bear case 必须归因到具名嘉宾和机构
3. 覆盖节目中讨论的所有重要 ticker — 涨幅榜, 跌幅榜, 盘后, 私有/Pre-IPO 公司 (如有讨论)
4. 不要写宏观 PM Brief (跨资产 regime, Resolution Framework 等) — 那是另一条管线
5. 若转录中未提及某数据, 写 "未讨论" 或 "涨跌幅未提及" — 不要编造数字
6. 若同日提供了多集转录 (主节目 + Overtime), 合并为一份统一报告, ticker 去重$SP$,

    -- user_prompt_template
    $UP$请基于以下 CNBC "Closing Bell" 节目转录生成当日 Stock News 复盘.

日期: {{report_date}}
节目标题: {{video_title}}

转录全文:
{{transcript}}

请严格按以下六个段落输出 Markdown 格式 (章节标题使用英文, 与参考报告一致):

## Stock Movers

### Winners
列出节目中讨论的所有重要涨幅股. 每行一条:
- **$TICKER** (公司名) | +/-X% | Catalyst: 一句话原因

### Losers
格式同 Winners.

### After-Hours Movers
盘前/盘后/即将有催化的标的 (当晚财报, IPO 簿记截止, 私有公司申报等).
若方向未知, 写 "direction TBD".

---

## Company Deep Dives

对节目中重点讨论的每家公司 (上市或私有), 使用:

### $TICKER -- 公司名
(无私有 ticker 时用 "公司名 (Private)" 或 "Pre-IPO")

按需包含以下字段 (未讨论则省略, bear case 可写 "Not discussed"):
- **What happened:**
- **Key numbers:** (目标价, 隐含波动, 营收, 估值等)
- **Management said:**
- **Analyst reaction:** (机构 + 分析师姓名)
- **Bull case:** (归因到嘉宾/机构)
- **Bear case:** (归因到嘉宾/机构; 未讨论则写 Not discussed)
- **Notable:**

优先覆盖: 最大波动股, Mag 7, 争议股, IPO 相关, 有期权/财报催化的个股.

---

## Sector Action

按板块/ETF 分项列出 (如 Tech/XLK, Energy, Healthcare, Financials, Homebuilders/ITB, REITs, Semis/SOXX).
每条: 板块表现 + 驱动因素 + 轮动背景.
如有嘉宾对广度/轮动的评论, 一并收录.

---

## Trades & Positioning

列出具名嘉宾的可操作仓位观点, 格式:
- **主题标签:** 嘉宾姓名 (机构) — 具体交易观点, 技术位, 期权信号或仓位备注.

收录: 动量 unwind 统计, VIX/期权流向, 隐含波动, 斐波那契/均线位, IPO 相关仓位调整, 板块轮动交易.

---

## Tomorrow Setup

列出下一交易日的催化剂和关注事项:
- **催化剂名称:** 关注什么, 对个股/板块有何影响.

包含: 经济数据 (CPI, 就业), 财报 (盘前/盘后), IPO 事件, 联储官员讲话, 地缘风险, 关键技术位.

---

## Index Scorecard

指数与跨资产收盘计分卡, 每条一行:
- S&P 500, Nasdaq, Dow, Russell 2000
- VIX
- 10Y / 2Y 收益率
- 原油, 美元/日元 (或其他 FX, 如有讨论)
附上盘中背景 (如 "盘中一度跌 3% 后收复").$UP$,

    -- variables
    '{
        "report_date": {"type": "string", "required": true, "description": "报告日期 (YYYY-MM-DD)"},
        "video_title": {"type": "string", "required": true, "description": "Closing Bell 节目标题 (多集可用逗号分隔)"},
        "transcript": {"type": "string", "required": true, "description": "节目转录全文 (多集可合并)"}
    }',

    -- output_format
    '{"type": "markdown"}',

    -- recommended_provider, recommended_model
    NULL, NULL,

    -- temperature, max_tokens
    0.30, 5500,

    -- scope, module_code
    'system', 'cnbc_closing_bell',

    -- is_public, is_active, is_default
    true, true, true,

    -- description
    'CNBC Closing Bell 每日 Stock News: 六段式个股复盘, 交易员/个股视角',

    -- tags
    '["closing_bell", "daily_report", "stock_news", "cnbc", "market_summary"]'
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
    tags                  = EXCLUDED.tags,
    updated_at            = NOW();
