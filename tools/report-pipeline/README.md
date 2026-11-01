# report-pipeline — CNT 风格 Daily Market Brief 生成管线(v6)

用无头 ZCode(`zcode -p`)作为"超级 LLM 系统调用",按**章节分步生成 + 核验门控**定时产出市场简报。对齐 CNT **现行版式**(2026-10-12 之后)。v5 起节目正文**只用清洗转录、不读摘要表**(The Close youtube 自识别 + Closing Bell 主段/Overtime 播客双行直查);v6 增**新闻事件层**(`sources.news`:twitter 快讯 flash + Yahoo/Seeking Alpha 全文 wire + SA 电话会纪要 calls),drivers 双引擎选题(节目块 + 新闻事件块)。

| 报告部分 | 章节 | 叙事 / 数据源 |
|---|---|---|
| OVERALL MARKET | narrative / drivers / positioning | **The Close 转录**(叙事) + 跨资产/曲线/fed_odds/广度(数字锚) |
| EQUITY MARKET | sector_action / movers / deep_dives | **Closing Bell 主段+Overtime 转录**(movers 三表从转录写) + Finviz 全成分/财报日历(数字锚) |
| MACROS | macro | **经济日历 + macro_pillars**; Releases 反应可引 **The Close 转录**; Rate Expectations / 跨资产复盘仅周五或极端行情 |

## 全流程图

```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 第一层 · 上游数据摄入(仓库现有服务,日常定时运行,不在本管线内)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  YouTube(Bloomberg Television) ──► subscription_content(youtube 行)
  CNBC Closing Bell 播客 feed ────► subscription_content(podcast 行,主段+OT 各一行)
  EODHD 经济日历 / 行情 · Finviz screener · earnings_calendar

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 第二层 · Stage 0 冻结快照  脚本: build_snapshot.py + fetch_macro_pillars.py
                                    + transcript_prep.py(转录直查与清洗)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  节目转录直查(不经摘要表,按节目当晚 UTC 发布时间锚定 T,无 T+1 戳错位):
    the_close         youtube + Bloomberg Television 频道 + T 晚 21:00-23:00
                      UTC 窗口 + 转录≥50k + 开场白 "THIS IS THE CLOSE" 自识别
                      (同窗口有 Surveillance/Businessweek 等干扰项)
    cnbc_closing_bell podcast 行,标题带 T 的 MM/DD/YY 后缀(主段与 Overtime 行
                      都有;日间节目行无后缀天然排除);published_at 早=主段、
                      晚=OT。OT 标题有时是纯副标题(无 "Closing Bell" 字样),
                      不可按标题关键字过滤
  Layer 1 确定性清洗后内联:
    sources.the_close.clean_transcript
    sources.cnbc_closing_bell.clean_transcript(主段) / clean_transcript_ot(OT)
  market 拆成两块,各自容错:
    cross_asset ← market_data_eod(≤T,可回溯)
    finviz ← 当前状态表(snapshot_date ≤ T+1;历史日期无快照)
    S&P 500 全成分 + 广度;Top movers 限市值 ≥ $20 亿
  经济日历(近3天+未来7天,保留 comparison,time_et 为美东)
    构建前回填 [T-2,T] 的 actual(写生产库;--no-backfill 跳过)
  财报日历:美股普通股、去重;前一交易日盘后+当日 ≥$100 亿,
    未来 5 天 ≥$500 亿
  news(新闻事件层,v6):
    flash ← twitter 白名单 9 账号(DeItaone/LiveSquawk/Wall St Engine/
            Kobeissi/The Information/Timiraos/RenMac/Earnings Whispers/
            Daily Shot),窗口 T 00:00-24:00 ET,RT 去重 + $TICKER 提取
    wire  ← news 频道全文(Yahoo Finance + Seeking Alpha),窗口
            [T-1 20:00 ET, T 24:00 ET),≥800 字符截 1200
    calls ← SA 电话会纪要(正文 "Earnings Call Insights" 特征 + ≥8k 字符),
            仅保留财报日历命中 ticker,截 8000,≤6 篇
    已知边界:T 晚间盘后财报的 SA 纪要 T+1 白天才到,赶不上晨间构建 ——
    此类 Management said 仍由 CB Overtime 转录覆盖
  macro_pillars: 曲线 / FRED ranks+extras / fed_odds(+前一交易日 prior) /
                 FOMC / auction(FiscalData,仅 Note/Bond) /
                 release_series(环比百分比) / nowcasts(失败回退日历 GDPNow)
  prior_odds ← next_fomc_odds.prior,没有时回退上一期快照
  ▼
  backend/reports/pipeline/{date}/snapshot.json

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 第三层 · 生成与核验  脚本: run_pipeline.py + prompts.py + verify_checks.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  三组并行(墙钟 ≈ 最慢一组):
    overall_market → 01+02+03  The Close 转录: narrative / drivers / positioning
    company_desk   → 04+05+06  Closing Bell 主段+OT 转录:
                               sector_action / movers(三表) / deep_dives
    macro          → 07        经济日历 + macro_pillars + The Close 反应引语
  ▼
  每节同会话写 sections/NN_*.claims.json(数字台账:value + snapshot 路径;
  节目数字路径 = the_close.clean_transcript / cnbc_closing_bell.clean_transcript[_ot])
  verify_checks.py(确定性,秒级) → check_results.json
    台账逐条按路径对数(节目/news 文本按 digits-in-blob) · 正文 %/bp 数字
    覆盖兜底 · 下一场 FOMC 概率 · 三元组/since/块矩阵/标签矩阵/
    结构(含 movers 三表)· cn 标题语言
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
| `us_close_chart.py` | CNT 同款 "US close" 总览图(FT 表格风,7 分组×Last/1D/1W/1M/YTD) |
| `finviz_map_chart.py` | CNT 同款 finviz 板块热力图(Equity Market 首图,自绘) |

## 使用

```bash
PY=/root/miniconda3/envs/guanhe/bin/python
ZCODE=/root/.zcode/server/bin/zcode

$PY tools/report-pipeline/build_snapshot.py [--date 2026-10-28] [--no-backfill]
$PY tools/report-pipeline/run_pipeline.py 2026-10-27 --style en
$PY tools/report-pipeline/run_pipeline.py 2026-10-27 --llm-verify off   # 只用确定性核验
$PY tools/report-pipeline/run_pipeline.py 2026-10-27 --fake-zcode   # 状态机自测
$PY tools/report-pipeline/render_charts.py backend/reports/pipeline/2026-10-27   # 01_us_close 自动走 FT 版,失败回落旧简版
$PY tools/report-pipeline/us_close_chart.py --workspace backend/reports/pipeline/2026-10-30   # 单独重出 final/charts/01_us_close.png
```

`us_close_chart.py` 数据映射(逆向 CNT 10-30 图并逐格数值验证):Last/1D 与板块行
perf 列取 finviz 快照(MAX(snapshot_date)≤T+1);1W/1M=T-7/T-30 日历日前最近收盘;
ETF 的 W/M/YTD 用 adjusted_close 总回报;利率行优先 FRED 财政部曲线(DGS2/10/30,
发布滞后时自动回落 EODHD 并在页脚注明);DXY 走 Yahoo;MOVE 无源跳过。
嵌入版式与 CNT 一致:01_us_close 放正文 "## Overall Market" 标题下(顶部),
02_finviz_map 放 "## Equity Market" 段、紧贴 Sector Action 上方,不使用顶部
Charts 图廊;03/04/05 系列图表暂停生成(待逐张优化,函数保留在 render_charts.py)。

`finviz_map_chart.py` 背景:elite.finviz.com/map 在本服务器 IP 上被 Cloudflare
交互式 Turnstile 拦截(无头/有头/stealth/curl_cffi 均实测不通),无法程序化截图;
故改从 finviz_screener 快照自绘同款热力图。布局规则(从用户实拍截图倒推并经
视觉验收确认):宇宙 = map · s&p500 · 1-day performance(index_name 含 'S&P 500',
排除 Shell Companies);板块层按实拍 4 列组强制分列 —— [Technology] |
[Comm Services, Financial, Consumer Cyclical] | [Healthcare, Consumer
Defensive, Energy] | [Industrials, Utilities, Real Estate, Basic Materials],
列宽 ∝ 组市值;行业/个股在块内市值降序 squarify,最大者恒在左上(画时镜像);
配色自 CNT 原图采样,±3% 即达最亮档。

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
8. **转录为正文、不用 Surveillance、不读摘要表** — 写作只读 The Close /
   Closing Bell(主段+OT)清洗转录;Macros 反应引语只引 The Close 转录。
   三组写作 agent 并行,墙钟 ≈ 最慢一组。
9. **时间口径** — 经济日历的 `time` 是 UTC,正文只引用 `time_et`(美东)。
   同一事件名、不同 `comparison`(mom / yoy / 水平)是不同口径,不得混用。
10. **回填写库** — `build_snapshot.py` 默认对生产 `economic_events` 做
    [T-2, T] 的 COALESCE upsert;调试用 `--no-backfill`。
11. **节目转录按当晚 UTC 锚定** — 转录行自身 `published_at` 即美东场次当晚,
    直查匹配无摘要表 T+1 北京戳问题;缺行(上游 ingest 滞后/断更)记入
    `errors`,该节目章节按缺源降级,不回退摘要。

## 已知边界

- nowcasts 网页常抓不到;回退经济日历里的 Atlanta Fed GDPNow 行(只有 GDP,没有 CPI/PCE nowcast)。
- Finviz 是整表刷新的当前状态,不能回溯历史交易日。测管线用最新有快照的日期。
- Finviz 热力图、真 RRG、ETF/个股 K 线未做。现有图是跨资产面板、板块表、
  1M 对 1D 的轮动散点(RRG 的简化替代)、Release 柱状图、个股收盘价折线。
- Today's Movers 由 company_desk 从 Closing Bell 转录的节目点名写三表
  (Winners / Losers / After-Hours),走台账核验;无转录时按「不可得」降级,
  不回退 Finviz 全市场榜。
- 节目转录 ingest 偶发滞后(历史批次晚 5-7 天):cron/手动构建应晚于当晚
  转录入库(北京时间次日上午 10 点后实测稳定);缺哪行记 `errors`。
- 节目口播数字是约数且偶有口误/ASR 断词(如 "SERVICE. NOW"),引用时以
  数据层口径为锚、转录数字逐条入台账对账。
- 盘中时间戳 / CDS / 10-Q 承诺等一手材料不在本管线范围内。
- Management said / Analyst / 卖方反应只在转录里有原话时才写,否则省略。
  Macros 不引用 Surveillance。
