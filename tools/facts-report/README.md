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

## 宏观数据节选题白名单扩容(2026-11-21 修复)

T=11-11/11-12/11-20 三期「回顾 · 宏观数据」出"今日无重点",但快照里 T 日
带 actual 的一线发布其实都在 —— `select_country_releases` 漏斗太窄:11-12
初请+成屋销售、11-20 密歇根信心+欧盟消费者信心被 11 族白名单漏掉,11-11
当天唯一的德国 CPI 被"四经济体"国家口径排除。三层修复(全部在该函数内):

1. 族白名单补 `claims`(初请/续请)、`housing`(成屋/新屋/营建/开工)、
   `sentiment`(密歇根/消费者·企业信心/ZEW/IFO),周度数据排在月度一线后;
2. 国家口径 US/EU/CN/JP → **加 DE**(快照本收五国,德国全国口径发布与
   欧盟聚合分列);
3. **兜底层**:白名单整体选空时回退注入 T 日全部带 actual 事件
   (有 forecast 优先、主指标名短优先,family="other")—— 有真数据的日子
   永不出空节,未来白名单外的新指标也兜得住;真无数据的平静日仍走占位。

离线回放 11-05~11-20 全部 12 份快照:三期空窗全部补上,其余日期只增不减。

## 发布链接推送到 Lark 群(2026-11-21)

`MB_LINK_LARK=1`(.env):`register_morning_brief.py` 首发注册成功(该
report_date 首次落 completed)后,推一张**小卡片**到 lark_alert 同群
(服务状态监测群):深蓝标题条 `📰 Morning Brief <report_date>` + 美股
交易日 + 用法一句话(`_MB_USAGE`)+ 「查看报告」按钮 open_url 打开
分享链接 `https://guanhecapital.hk/api/morning-brief-report/shared/{token}`。要点:

- **为何用卡片**:webhook 机器人文本消息不做链接 unfurl(平台限制:
  链接预览是应用级能力,自定义机器人收不到消息事件),卡片按钮是
  webhook 下唯一能拿到"卡片观感"的形态(`lark_notifier.send_card`);
- **色调对齐报告页**:标题条为自定色 `#1b4b7d`(报告 Header
  `--bg-header` 渐变锚点,白字)。Lark 官方 header(1.0/2.0 均)只有
  预设色板,深蓝满宽标题条由 **JSON 2.0** 自造(`_brief_link_card`):
  column `background_style` 引用 `config.style.color` 自定 RGBA +
  `padding` 内边距,标题白字走 2.0 富文本 `<font color=white>`(1.0
  分栏无 padding、markdown 无字色,不够看;2.0 需客户端 ≥7.20,旧客户端
  卡片标题可见但内容显示升级提示);
- **首发才推**:先查 `get_report_by_date`,已有 completed 行则跳过 ——
  ft_refresh 10:30 补数重注册、手动补挂不会重复打扰(token 本就保持不变);
- `--no-oss` 联调不推;推送在数据新鲜度 ⚠️ 告警之后发出,顺序不颠倒;
- 推送失败只记日志,不影响注册;删掉 .env 该行即整体停推;
- **追加目标群**:`MB_LINK_LARK_WEBHOOK=<webhook>`(.env,逗号分隔可多个)
  —— 报告卡片在默认服务状态监测群之外加推;目标群需自建自定义机器人
  (不要开签名校验);告警仍只进默认群。

## GLM 内容审核 1301 → Gemini 单发兜底(2026-11-19 事故)

事故链:geopolitics 章节素材(俄乌/美伊/制裁/APEC 三边会晤中文快讯原文)
触发 GLM 内容审核 `[1301]`,zcode CLI 只以 "Turn execution failed (traceId:…)"
退出(1301 详情仅在 `~/.zcode/cli/log/zcode-<日期>.jsonl`);当日 plan 与
geopolitics 三次重跑全灭,单章节失败放倒整条 40-60 分钟管线。实测 CherryIN
网关(backend `llm_provider_configs` 表已启用的 gemini/claude)对同一批素材
不拦截 —— `llm_fallback.py` 据此提供四层防护:

1. **失败分类**(`classify_zcode_failure`):stderr 直接匹配 1301/sensitive,
   未命中再按 traceId 追查当日 zcode CLI jsonl 日志。审核拦截不再走无意义
   的同 payload 20s 重试;根因提取(`extract_root_cause`)过滤 node warning,
   真实报错进 cron.log 与 Lark 告警。
2. **写作/修订兜底**(`_run_one_agent`/`revise_sections`):被拦(或分类漏判
   但重试后仍持久失败)的 agent 改由 Gemini 单发完成 —— 原任务书不变,交付
   契约改写为"无工具、快照内联、单 JSON 输出全文件",落盘后照走
   `section_done` 文件契约 + 确定性核验,产物不合格则整线失败转人工(不静默
   丢章节)。state 记 `mode: "gemini-fallback"`。
3. **plan 兜底**(`_plan_once`):plan digest 内联地缘主题标题同样会撞 1301,
   直接同 prompt Gemini 单发;失败仍走 unavailable 降级(不阻断)。
4. **核验降级**(`run_verifier`):LLM 叙事核验被拦 → 跳过(确定性台账核验
   照常),verdict 只含确定性结论 —— 意见本就是 annotate 语义,不拦发布。

配置(env 覆盖 > DB `llm_provider_configs`):`MB_FALLBACK_PROVIDER`(默认
gemini;claude 亦实测可用)、`MB_FALLBACK_MODEL/BASE_URL/API_KEY/TIMEOUT`;
DB 读不到则兜底禁用、行为与旧版一致。演练开关 `MB_FALLBACK_FORCE=<agent>`
(如 geopolitics;`1`=全部)强制走兜底路径,不烧 GLM 调用即可端到端演练:

```bash
MB_FALLBACK_FORCE=geopolitics $PY run_pipeline.py 2026-11-18 --style cn
```

单测:`python -I test_llm_fallback.py`(离线,不联网)。

## 按 agent 路由(2026-11-19 起:outlook 常驻 Claude sonnet-5.5)

`MB_AGENT_PROVIDER=outlook:anthropic/claude-sonnet-5.5`(已写入 .env;格式
`agent:model[,agent:model…]`,模型前缀 anthropic/… google/… 决定取 DB 哪行
凭据)。命中的 agent 单发直连为**主路径**,优先级变为三级:

```
sonnet-5.5 单发(主) → GLM-5.3 agent(报错/解析失败回落) → Gemini 单发(GLM 撞 1301 兜底)
```

- 写作与修订(`revise_sections`)同模型同路由,风格/口径一致;
- outlook 单发输出长,**max_tokens 固定 32000**(16384 实测截断 JSON 信封);
- **信封缺陷原地重试**(2026-11-20 生产首日实测:claims 字段空字符串):
  校验失败(空内容/漏文件/坏 JSON)时带"上次错在哪"的提示重试一次,仍失败
  才降级;原始响应(成败均)落盘 `<ws>/fallback_raw/<agent>_{write,revise}_*.txt`
  备查;
- 第二段合成需要的第一段成品由 `_routed_section_context` 自动内联
  (清单与 OUTLOOK_TASK 一致);
- state 记 `mode: "routed:<model>"`。CherryIN 价目:sonnet-5.5 $2/$10 per
  1M,outlook 单发实测 ~157K in / 11K out ≈ $0.42/次(89-156s)。
- 撤销路由:删 .env 中该行即回 GLM 全管线路径,无其它残留。
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
- FT 表加密货币组(2026-11-18):商品之后新增「加密货币」组,顺序 BTC/ETH/
  SOL/USDC/USD1/BNB/DOGE(英文行名)。DB 无加密数据,走 DXY 同款直连:历史
  Yahoo 优先(bar 实时累积,UTC 日切 —— T bar 在北京 08:00 构建时刻恰为终值,
  as_of=T 恒成立不触发新鲜度横幅)、EODHD .CC 回退(主流币两源逐日偏差
  <0.001%)。实时刷新 code 用 EODHD realtime 码(24/7 恒数值,prev 与 EOD
  收盘一致),并入 /quotes 现有批量 chunk。USD1 无常规代码,两源均以合成码
  USD136148-USD 提供(EODHD 加 .CC 后缀;两源收盘偏差 ~0.03%,稳定币正常)。
  <10 美元币种收盘 4 位小数且带行级 dp 属性(fmtLast/FT_HIST 透传,实时刷新
  同精度;2dp 对 0.06 美元的 DOGE 会丢失全部信息)。md2html TV_INDEX:
  BITSTAMP:BTCUSD/ETHUSD + BINANCE:SOLUSDT/USDCUSDT/USD1USDT/BNBUSDT/
  DOGEUSDT。组内行两源皆失时省略并在 PNG 脚注记缺失个数。1D = vs 前一 UTC
  日收盘(24h 口径,Yahoo 币页同款)。
- FT 表组内排序 + 移动端涨跌榜卡片走势图(2026-11-18):FT「当日/5日/1月/
  YTD」列头可点排序(交互同涨跌榜:▼/▲ 切换、空值垫底、**组内排序**不动
  分组);移动端卡片区顶部同款 chips。行预埋 data-g/w/m/y 数值属性为排序键,
  实时覆盖时同步更新属性但不重排已排序视图(按 data-ri 就地改格,与行序
  无关);排序改表格 DOM 行序后 __ftSyncCards() 同序重建移动端卡片。涨跌榜
  移动端 rkcard 行 1 右缘加 rc-spark 走势图(64×22,数据=桌面表 r.sv,
  实时刷新随 render() 重建自动重绘)。
