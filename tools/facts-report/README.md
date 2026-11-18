# facts-report — 事实/观点分离版晨报

GuanHe-Daily-Report 的独立变体,用来落地郑翔需求文档「痛点 2:事实与观点混杂」
(见同目录 `郑翔需求文档.md`)。编排、核验、溯源(L/E 标记)与日报完全相同,
只改内容规则,且**不新增章节**——避免在日报已有内容上再叠一层重复。

日报目录 `tools/GuanHe-Daily-Report/` 与产物 `backend/reports/guanhe-daily-report/`
不受影响;两条管线代码独立、锁独立、产物目录独立。

## 与日报的差别

| 位置 | 日报 | facts-report |
|---|---|---|
| 01 节 | 「今日叙事」:2–3 句结论,无数字 | 「今日要闻」:3–5 条纯事实(谁、做了什么、关键数字),无观点/因果/渲染词 |
| 报告标题 | 两句编辑化结论 | 两句事实,句间不带因果 |
| 02 / 04 / 08 / 09 节 | 嘉宾、分析师观点可夹在事实句里 | 事实行只写事实;具名观点统一收进块末一行 `**观点：**` |
| 核心驱动 vs 今日要闻 | — | 驱动块可回指要闻事实,但不得原句原数字复述 |
| 03 持仓、06 个股深挖、11 展望 | — | 不变(本身就是解读层 / 已有分析师专栏) |

事实与观点的界定写在 `prompts.py` 公共规则第 12 条:当事方对自身的表态(央行谈政策、
公司谈自己的指引)算事实;分析师、策略师、嘉宾、KOL、研报、目标价、评级、情景概率,
以及对大盘涨跌「为什么」的解释(除非原因本身是当日事件)算观点。

## 核验

`verify_checks.py` 新增确定性检查:

- 「今日要闻」出现观点/预测/因果/渲染措辞(认为、有望、引发、导致、意味着、暴跌、
  恐慌…)→ **high**,触发修订轮。
- 02 / 04 / 08 / 09 节的事实行(不含 `**观点：**` / `**推断：**` 行)出现观点措辞
  (认为、看好、目标价、归因、源于…)→ medium,进复核备注,不门控。
  「预计」不在词表里:公司自己的指引属于事实。

LLM 轻量核验另加两条 medium:事实行里夹带具名观点;要闻条目在后文被原样复述。

## 运行

快照默认**借用日报同日快照**(复制 `snapshot.json` + `transcripts/`),两份报告数字
同源,也不重复调外部 API;日报当日没有快照时才需要自建。

```bash
cd tools/facts-report
$PY run_pipeline.py 2026-11-05 --style cn       # 自动从 guanhe-daily-report/2026-11-05 借快照
$PY run_pipeline.py 2026-11-05 --no-seed        # 不借;需先自建快照
$PY build_snapshot.py --date 2026-11-05         # 自建 → backend/reports/facts-report/2026-11-05/
$PY run_pipeline.py 2026-11-05 --fake-zcode     # 不调 LLM,冒烟测试编排与门控
```

其余参数与日报 `run_pipeline.py` 相同(`--no-plan`、`--no-provenance`、`--llm-verify`…)。

## 行情新鲜度自愈(2026-11-13 事故)

事故链:market_data 定时任务在「美股收盘(北京 T+1 05:00)→ 晨报构建(08:00)」
窗口内只有 05:20 一抓,EODHD 当日 EOD 发布偶发晚于该时点(2026-11-13 五个
code 缺 T 日收盘,横幅出街,数据 11:20 才入库)。三层防护:

1. **调度加密**:market_data cron 由 `20 5,11,14,19` 加 07:20 一抓
   (`20 5,7,11,14,19 * * *`,= ET 18:20,收盘后 2h20m、晨报前 40min)。
2. **快照自愈**:`build_snapshot.py` 发现活报告缺 T 日收盘 → 每 5min 对 stale
   code 直拉 EODHD/Yahoo EOD 幂等 upsert(`ON CONFLICT (code,date)`),
   轮询至 `--quotes-deadline`(默认 09:30,≤ 转录门 09:40,不推高总最坏时延);
   ⚠️(首次缺)/🔴(超截止)Lark 同步状态,等待轮数记入
   `notes.market.cross_asset_wait`。历史重跑、过截止点的白天补跑不等待;
   `--no-quotes-wait` 可整体关闭。
3. **失败告警**:scheduler 对 market_data 执行失败推 Lark(服务状态监测群;
   `SCHEDULER_LARK_FAIL_ALERT_TYPES` 逗号分隔可调,默认仅 market_data)。

## 定时

`cron_daily.sh` 排在日报之后,优先借用日报 6 小时内写出的最新快照,否则自建:

```
20 10 * * 2-6 /bin/bash /opt/guanhetech/tools/facts-report/cron_daily.sh \
    >> /opt/guanhetech/backend/reports/facts-report/cron.log 2>&1
```

## 同步日报的改动

本目录是 2026-11-07 从 GuanHe-Daily-Report 复制的。日报后续的修复如需带过来,
对比时注意本目录有意改动的位置:`prompts.py`(规则 12、01 节任务、02/04/06/08/09
节措辞、轻量核验)、`verify_checks.py`(01 节标题、`_fact_line_views`)、
`run_pipeline.py`(产物目录、`seed_snapshot`、标题)、`build_snapshot.py` 与
`gen_pipeline_card.py`(产物目录)、`cron_daily.sh`。

## 涨跌榜/大盘总览 涨跌口径备忘(2026-11-13 排查定案)

- W/M/YTD 价格基准 = Yahoo 原始收盘(拆股已追溯、股息不复权,与 Google Finance
  网页版/行情软件一致);`_rank_perf_spark` 在 Yahoo 日线未滚出 T 日 bar 时用
  EODHD bulk 收盘拼接锚点(容差 0.5% 核对,见函数注释)。
- YTD 基准 = **上年末最后一个收盘**(行业标准,与 Google Finance 网页版/雅虎/
  彭博一致)。Google **搜索面板**的 YTD 用年内首个交易日收盘,会恒差 1-2pp
  (2026-11-12 实测:NVDA 我们 -10.89%/面板 -12.00%,AMZN +15.87%/+18.08%,
  均精确等于 1/2 收盘口径)——口径差异,非计算错误,产品决策为不跟随。
- 窗口口径(2026-11-13 用户决策:**统一对齐 Yahoo Finance**,涨跌榜与 FT 表同套):
  "5日" = 5 个交易日前收盘(原"1周"7 日历天,已改列名);"1月" = 减一个自然月
  (原 T-30;31 天月份两口径差一天);YTD = 上年末(本就与 Yahoo 一致)。
  Google 搜索面板 1M=T-30、YTD=年内首日、5D 无法反推,各家互有出入,不作为基准。
  例外:FT 表 ETF 行价格基准仍为复权总回报(GuanHe 版式),板块行用 finviz
  perf 列 —— 与 Yahoo 个股页的纯价差口径存在股息级差异,属有意保留。
- "1 月"列 = T-30 日历天窗口 —— 与 Google 搜索面板一致(NVDA -27.28%/AMZN
  +3.43% 双票精确验证);**Yahoo Finance 页面用"减一个自然月"**(MU 实测:
  其 -19.27% = 10/12 收盘 989.04,我们 -16.23% = 10/13 收盘 953.19,MU 该日
  单日 -3.7% 故差 3pp)。10 月等 31 天月份两口径边界差一天,大波动日可见
  1-4pp 差异;各平台口径互有出入,产品决策为维持 T-30 不跟随单一平台。
- 涨跌榜实时刷新(2026-11-13):build_snapshot 每行落盘近 45 根收盘迷你序列
  (perf_refs),register 写 metadata.rank_live(EODHD 实时代码 + 序列,行序对齐
  rank_kline),/shared/{token}/quotes 响应新增 rank 字段 —— 服务端按**行情实际
  日期**现算 5日/1月窗口(港/A 股收盘早于查看时刻,锚点随最新收盘平移,实测
  9988.HK 与 Yahoo 四项逐位一致),页面打开时就地刷新价格/当日/5日/1月/YTD,
  接口失败静默保留快照;EODHD 批量 chunk 12→40(FT+涨跌榜共 ~317 码 ≈ 8 次调用,
  60s 进程缓存共享)。
- FT 表实时刷新同口径升级(2026-11-13):refs 增加迷你收盘序列(最新 45 根、
  **升序** —— 降序序列取 s[-45:] 会拿到最旧 45 根,已踩坑),服务端与涨跌榜
  共用 _window_changes(按行情实际日期现算窗口);沪市系指数(SSEC/SSE50/
  CSI300/500/1000)、日经、富时、罗素的实时代码切 YAHOO: 前缀(EODHD
  realtime 闭市恒 NA,降级会拿到 14:00 盘中入库行而非收盘终值,上证实测
  3642.63 vs 终值 3634.2)。深市指数/HSI/KOSPI/DAX 等 EODHD realtime 正常。
- 走势图补当日点(2026-11-13):实时刷新到达后客户端重绘 sparkline(Python
  _spark_svg 的 JS 镜像 __sparkHTML,FT 表与涨跌榜共用)——收盘序列 + 实时价
  追加为末点(与末点同值不追加);__RANK__ 载荷为此新增 sp 数组(63 收盘)。
  实测:FT 上证圆点随 3634.35 下移、涨跌榜 9988 spark 64 点尾值 90.55。
- FT 弹窗补当日 bar(2026-11-13):/quotes 行新增 o/h/l/dt(EODHD 实时或 Yahoo
  日线末根),点击 FT 行时若行情日期晚于冻结历史,弹窗历史追加当日蜡烛
  (无 OHLC 资产追加收盘点),与涨跌榜点击蜡烛同观感;美股收盘后 dt=T 不追加。
- FT 表加密货币组(2026-11-18):商品之后新增「加密货币」组(BTC/ETH/BNB,
  英文行名)。DB 无加密数据,走 DXY 同款直连:历史 Yahoo 优先(bar 实时累积,
  UTC 日切 —— T bar 在北京 08:00 构建时刻恰为终值,as_of=T 恒成立不触发
  新鲜度横幅)、EODHD .CC 回退(两源 382 日逐日对账偏差 <0.0001%,数值等价);
  实时刷新 code 用 EODHD realtime 码(BTC-USD.CC 等,24/7 恒数值,prev 与
  EOD T 收盘逐位一致)。1D = vs 前一 UTC 日收盘(24h 口径,Yahoo 币页同款)。
  md2html TV_INDEX 加 BITSTAMP:BTCUSD/ETHUSD、BINANCE:BNBUSDT 链接;
  组内行两源皆失时省略并在 PNG 脚注记缺失个数。
