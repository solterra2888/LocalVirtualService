# report-pipeline — CNT 风格 Daily Market Brief 生成管线(v4)

用无头 ZCode(`zcode -p`)作为"超级 LLM 系统调用",按**章节分步生成 + 核验门控**定时产出市场简报。对齐 CNT **现行版式**(2026-10-12 之后)。

| 报告部分 | 章节 | 叙事 / 数据源 |
|---|---|---|
| OVERALL MARKET | narrative / drivers / positioning | **The Close**(叙事) + 跨资产/曲线/fed_odds/广度(数字锚) |
| EQUITY MARKET | sector_action / movers / deep_dives | **Closing Bell** + Finviz 全成分/财报日历 |
| MACROS | macro | **经济日历 + macro_pillars**; Releases 反应可引 **Surveillance / The Close**; Rate Expectations / 跨资产复盘仅周五或极端行情 |

## 全流程图

```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 第一层 · 上游数据摄入(仓库现有服务,日常定时运行,不在本管线内)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  Bloomberg TV ──► the_close / surveillance / cnbc_closing_bell 摘要表
  EODHD 经济日历 / 行情 · Finviz screener · earnings_calendar

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 第二层 · Stage 0 冻结快照  脚本: build_snapshot.py + fetch_macro_pillars.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  The Close / Closing Bell(≤T,摘要去掉客套前言)
  Surveillance:按节目标题里的美东播出日期匹配 T
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
  transcripts/*.txt:先删旧文件,断连重试,超限留头 60%+尾 40%
    (按 agent 挂载,不进 JSON)
  ▼
  backend/reports/pipeline/{date}/snapshot.json

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 第三层 · 生成与核验  脚本: run_pipeline.py + prompts.py + verify_checks.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  overall_market → 01+02  narrative(速览) + drivers(分析+Take)
  positioning    → 03
  sector_action  → 04  主题块(无记分牌)
  movers         → 05  确定性长表(Ticker/Move/Catalyst)
  deep_dives     → 06  What happened / Key numbers / Management / Analyst
  macro          → 07  块矩阵: Releases(美国一线白名单≤2) /
                       Rate Expectations+跨资产(周五|极端) / Forward Catalysts(恒在)
  ▼
  verify_checks.py(确定性) → check_results.json
  核验 Agent(叙事质量) → verdict.json; 编排器合并 high fails
  Gate → assemble(正文无口径块) + render_charts.py → final/
```

## 产物目录

```
backend/reports/pipeline/{date}/
├── snapshot.json
├── transcripts/*.txt
├── state.json
├── sections/01..07_*.md
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
| `build_snapshot.py` | Stage 0: 多源冻结快照 + 转录附件 |
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
$PY tools/report-pipeline/run_pipeline.py 2026-10-27 --fake-zcode   # 状态机自测
$PY tools/report-pipeline/render_charts.py backend/reports/pipeline/2026-10-27
```

退出码:`0` 已发布 / `1` 运行错误 / `2` 核验未通过 → `NEEDS_HUMAN_REVIEW.md`。

## 设计规则(改代码前先读)

1. **快照不可变** — 生成与核验只读同一份冻结数据。
2. **文件即契约** — Agent 只写指定文件;编排器只读文件。
3. **步级幂等** — `state.json` 断点续跑。
4. **门控是纯代码** — 发布/重试/转人工不交给 LLM。
5. **数字红线** — `verify_checks.py` 对账;「不可得」禁止替代指标。
6. **Macros 块矩阵** — Releases = T 日美国一线、按事件族优先级取 ≤2
   (FOMC > 非农 > CPI > PCE > GDP > … > 耐用品 > 房屋;排除 Ex-*/Non-Defense
   分项;同族其他口径放 related);Rate Expectations / 跨资产复盘 = 周五或
   概率≥10pt / 10Y≥8bp 变动;Forward Catalysts 恒在;拍卖并入 Forward Catalysts。
7. **审计分层** — 正文 CNT 表面文体;口径块与 `<!-- audit: -->` 进 sidecar。
8. **转录按 agent 挂载** — 不塞进共用 snapshot.json,避免每个调用重复付费。
9. **时间口径** — 经济日历的 `time` 是 UTC,正文只引用 `time_et`(美东)。
   同一事件名、不同 `comparison`(mom / yoy / 水平)是不同口径,不得混用。
10. **回填写库** — `build_snapshot.py` 默认对生产 `economic_events` 做
    [T-2, T] 的 COALESCE upsert;调试用 `--no-backfill`。

## 已知边界

- nowcasts 网页常抓不到;回退经济日历里的 Atlanta Fed GDPNow 行(只有 GDP,没有 CPI/PCE nowcast)。
- Finviz 是整表刷新的当前状态,不能回溯历史交易日。测管线用最新有快照的日期。
- Finviz 热力图、真 RRG、ETF/个股 K 线未做。现有图是跨资产面板、板块表、
  1M 对 1D 的轮动散点(RRG 的简化替代)、Release 柱状图、个股收盘价折线。
- Movers 长表的催化剂只在快照有对应财报(T-1 盘后或 T 盘前)时填写,其余写「信源未提及」。
- 盘中时间戳 / CDS / 10-Q 承诺等一手材料不在本管线范围内。
- Closing Bell 当日摘要可能本身就没有转录(上游缺失),Sector Action / Deep Dives 会变薄。
