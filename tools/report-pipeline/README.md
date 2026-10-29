# report-pipeline — CNT 风格 Daily Market Brief 生成管线(v4)

用无头 ZCode(`zcode -p`)作为"超级 LLM 系统调用",按**章节分步生成 + 核验门控**定时产出市场简报。对齐 CNT **现行版式**(2026-10-12 之后)。

| 报告部分 | 章节 | 叙事 / 数据源 |
|---|---|---|
| OVERALL MARKET | narrative / drivers / positioning | **The Close**(叙事) + 跨资产/曲线/fed_odds/广度(数字锚) |
| EQUITY MARKET | sector_action / movers / deep_dives | **Closing Bell**(movers 抽节目点名表) + Finviz 全成分/财报日历(数字锚) |
| MACROS | macro | **经济日历 + macro_pillars**; Releases 反应可引 **The Close 摘要**; Rate Expectations / 跨资产复盘仅周五或极端行情 |

## 全流程图

```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 第一层 · 上游数据摄入(仓库现有服务,日常定时运行,不在本管线内)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  Bloomberg TV ──► the_close / cnbc_closing_bell 摘要表
  EODHD 经济日历 / 行情 · Finviz screener · earnings_calendar

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 第二层 · Stage 0 冻结快照  脚本: build_snapshot.py + fetch_macro_pillars.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  The Close / Closing Bell:按美东场次匹配 T(摘要去掉客套前言)
  不取 Surveillance,不拉节目转录
  market 拆成两块,各自容错:
    cross_asset ← market_data_eod(≤T,可回溯)
    finviz ← 当前状态表(snapshot_date ≤ T+1;历史日期无快照)
    S&P 500 全成分 + 广度;Top movers 限市值 ≥ $20 亿
  经济日历(近3天+未来7天,保留 comparison,time_et 为美东)
    构建前回填 [T-2,T] 的 actual(写生产库;--no-backfill 跳过)
  财报日历:美股普通股、去重;前一交易日盘后+当日 ≥$100 亿,
    未来 5 天 ≥$500 亿
  macro_pillars: 曲线 / FRED ranks+extras / fed_odds(+前一交易日 prior) /
                 FOMC / auction(FiscalData,仅 Note/Bond) /
                 release_series(环比百分比) / nowcasts(失败回退日历 GDPNow)
  prior_odds ← next_fomc_odds.prior,没有时回退上一期快照
  ▼
  backend/reports/pipeline/{date}/snapshot.json

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 第三层 · 生成与核验  脚本: run_pipeline.py + prompts.py + verify_checks.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  三组并行(墙钟 ≈ 最慢一组;不挂转录):
    overall_market → 01+02+03  The Close 一遍: narrative / drivers / positioning
    company_desk   → 04+06     Closing Bell 一遍: sector_action / deep_dives
    macro          → 07        经济日历 + macro_pillars + The Close 反应引语
  movers         → 05  从 Closing Bell 摘要的 Stock Movers 表抽出(非 Finviz 榜)
  ▼
  每节同会话写 sections/NN_*.claims.json(数字台账:value + snapshot 路径)
  verify_checks.py(确定性,秒级) → check_results.json
    台账逐条按路径对数 · 正文 %/bp 数字覆盖兜底 · 下一场 FOMC 概率 ·
    三元组/since/块矩阵/标签矩阵/结构 · cn 标题语言
  有 high fail → 不调 LLM,直接按清单 --resume 修订
  全绿 → --llm-verify light(默认,只读七节正文,不挂快照/转录)| off | full
  Gate → assemble(正文无口径块) + render_charts.py → final/
```

## 产物目录

```
backend/reports/pipeline/{date}/
├── snapshot.json
├── state.json
├── sections/01..07_*.md
├── sections/*.claims.json   # 数字台账(movers 确定性渲染,无台账)
├── verify/check_results.json
├── verify/verdict.json
└── final/
    ├── report.md
    ├── report.audit.md   # 口径块 + 审计注释 sidecar
    └── charts/*.png
```

## 文件

| 文件 | 作用 |
|---|---|
| `build_snapshot.py` | Stage 0: 多源冻结快照(摘要,无转录) |
| `fetch_macro_pillars.py` | 宏观底座(曲线/FRED/概率/拍卖/release_series) |
| `prompts.py` | 章节任务书 + 叙事核验任务书; SECTION_SOURCE_MATRIX |
| `run_pipeline.py` | 编排器:块矩阵、movers、门控、组装 |
| `verify_checks.py` | 确定性数字/矩阵/结构对账 |
| `render_charts.py` | matplotlib 确定性出图 |

## 使用

```bash
PY=/root/miniconda3/envs/guanhe/bin/python
ZCODE=/root/.zcode/server/bin/zcode

$PY tools/report-pipeline/build_snapshot.py [--date 2026-10-28] [--no-backfill]
$PY tools/report-pipeline/run_pipeline.py 2026-10-27 --style en
$PY tools/report-pipeline/run_pipeline.py 2026-10-27 --llm-verify off   # 只用确定性核验
$PY tools/report-pipeline/run_pipeline.py 2026-10-27 --fake-zcode   # 状态机自测
$PY tools/report-pipeline/render_charts.py backend/reports/pipeline/2026-10-27
```

退出码:`0` 已发布 / `1` 运行错误 / `2` 核验未通过 → `NEEDS_HUMAN_REVIEW.md`。

## 设计规则(改代码前先读)

1. **快照不可变** — 生成与核验只读同一份冻结数据。
2. **文件即契约** — Agent 只写指定文件;编排器只读文件。
3. **步级幂等** — `state.json` 断点续跑。
4. **门控是纯代码** — 发布/重试/转人工不交给 LLM。
5. **数字红线** — 每个数字进台账、`verify_checks.py` 按路径对账;路径精确命中却
   数值不符即 high。「不可得」禁止替代指标。加息概率:会议概率只用
   `next_fomc_odds`(会后整月合约;决议日当天跳到下一场),`fed_odds.contracts`
   是月均合约,不得称作某场会议的概率。
6. **Macros 块矩阵** — Releases = T 日美国一线、按事件族优先级取 ≤2
   (FOMC > 非农 > CPI > PCE > GDP > … > 耐用品 > 房屋;排除 Ex-*/Non-Defense
   分项;同族其他口径放 related);Rate Expectations / 跨资产复盘 = 周五或
   概率≥10pt / 10Y≥8bp 变动;Forward Catalysts 恒在;拍卖并入 Forward Catalysts。
7. **审计分层** — 正文 CNT 表面文体;口径块与 `<!-- audit: -->` 进 sidecar。
8. **不拉转录、不用 Surveillance** — 写作只读 The Close / Closing Bell 摘要;
   Macros 反应引语只引 The Close。三组写作 agent 并行,墙钟 ≈ 最慢一组。
9. **时间口径** — 经济日历的 `time` 是 UTC,正文只引用 `time_et`(美东)。
   同一事件名、不同 `comparison`(mom / yoy / 水平)是不同口径,不得混用。
10. **回填写库** — `build_snapshot.py` 默认对生产 `economic_events` 做
    [T-2, T] 的 COALESCE upsert;调试用 `--no-backfill`。
11. **节目按美东场次匹配** — 摘要表 `report_date` 是北京生成日(T 场次 stamped
    T+1);`_show_summary` 以标题日期或"stamped 日前一个工作日"为场次,写出
    `session_date` / `lag_days`,落后时记入 errors。

## 已知边界

- nowcasts 网页常抓不到;回退经济日历里的 Atlanta Fed GDPNow 行(只有 GDP,没有 CPI/PCE nowcast)。
- Finviz 是整表刷新的当前状态,不能回溯历史交易日。测管线用最新有快照的日期。
- Finviz 热力图、真 RRG、ETF/个股 K 线未做。现有图是跨资产面板、板块表、
  1M 对 1D 的轮动散点(RRG 的简化替代)、Release 柱状图、个股收盘价折线。
- Today's Movers 抽 Closing Bell 摘要的 Stock Movers 表。摘要是占位(播客无转录)
  时写不可得,不回退 Finviz 全市场榜。Finviz 热力图未做。
- 盘中时间戳 / CDS / 10-Q 承诺等一手材料不在本管线范围内。
- 不拉节目转录:Management said / Analyst / 卖方反应只在摘要里有原话时才写,
  否则省略。Macros 不再引用 Surveillance。
