# guanhe-daily-report — 盘前晨报项目(xiao 式宏观地缘晨报;cron 未装,手动跑通中)

用无头 ZCode(`zcode -p`)作为"超级 LLM 系统调用",按**章节分步生成 + 核验门控**定时产出市场简报。01-06 节对齐 GuanHe **现行版式**(2026-10-12 之后);v8 起 **Macros 推倒重来**,按 `report_from_xiao.md` 参考稿改为「回顾/当下/展望」五文件结构(中文标题、直接给投资建议)。v5 起节目正文**只用清洗转录、不读摘要表**(The Close youtube 自识别 + Closing Bell 主段/Overtime 播客双行直查);v6 增**新闻事件层**(`sources.news`:twitter 快讯 flash + Yahoo/Seeking Alpha 全文 wire + SA 电话会纪要 calls);v8 增**宏观地缘素材层**(`sources.news_macro`:digest 晨间日报 + research 宏观解读 + geo 地缘快讯(Admin 订阅,中英))、**FOMC 表**(`sources.fomc`:决议/定价路径/声明)与**四经济体指标表底座**(`sources.macro_dashboard`)。

| 报告部分 | 章节 | 叙事 / 数据源 |
|---|---|---|
| OVERALL MARKET | narrative / drivers / positioning | **The Close 转录**(叙事) + 跨资产/曲线/fed_odds/广度(数字锚) |
| EQUITY MARKET | sector_action / movers / deep_dives | 按市场重要性选题:Finviz 全成分涨跌 + **Closing Bell 主段+Overtime 转录** + 新闻层(催化剂、评级/目标价); movers 由成分股快照确定性渲染 |
| MACROS(v8) | 宏观速览(确定性) / 回顾·宏观数据 / 回顾·地缘政治 / 美联储观察 / 展望与策略 | 速览=macro_dashboard+fomc+cross_asset 纯代码渲染(四经济体表+商品+前瞻日历);宏观数据=多国日历+券商宏观日报(边际变化);地缘=中文 geo 层主题块(命中量开块)+焦点事件;FOMC=决议表(投票/异议)+声明措辞对比+定价路径;展望=第二段综合读前节成品,大事预判/主线跟踪/投资建议 |

v7(2026-11,对照 CNT 参考稿评估后)文风契约:正文只给原始出处(Reuters/FT/CNBC/券商),
管线标签与生产过程用语(snapshot/transcript/on air/节目名/不可得)不进正文,由
`verify_checks.py` 以 high 拦截;可追溯性全部落在 audit 注释 + claims 台账。
v8 追加:Macros 各节**完全不读节目转录**;展望节判断句必须读作判断(「建议」「预计」),
数字仍逐条入台账,投资建议以「(以上为本报观点,不构成投资建议。)」收尾。

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
  news(新闻事件层,v7 — **无白名单**,Admin 订阅 + L/E 分级驱动):
    flash ← Admin(user_id=1)订阅的全部 twitter(~35 账号,含中文宏观源),
            窗口 T 00:00-24:00 ET,RT 去重 + $TICKER 提取;L4(噪音)排除,
            溢出按 L1>L2>L3/未评级 + 带$TICKER 保留(≤300)
    wire  ← Admin 订阅 news 全文源(Yahoo/SA/CNBC/CCTV/Globenewswire),
            窗口 [T-1 20:00 ET, T 24:00 ET),≥800 字符截 1200;top50 按
            L1>L2>L3/未评级 + 时序保留(替代旧版时序"先到先得"——实测
            窗口合格条目可达千级,旧法会把发得早的重要报道挤掉)
    calls ← SA 电话会纪要(正文 "Earnings Call Insights" 特征 + ≥8k 字符),
            仅保留财报日历命中 ticker,截 8000,≤6 篇
    **消费方(v8.3)**:flash 服务全部 LLM 节;wire/calls 服务 01-03 与
    07-11,**04/06(第二部分)不再读 wire/calls/The Close** —— 两期 claims
    实测三者仅贡献 ~8% 事实却占 ~56% 文本素材;第二部分文本源收窄为
    CB 双转录 + flash,数字仍锚定 finviz/财报日历/跨资产结构化层。
    SOURCE RESTRICTION 同步**物理化**:run_pipeline 按 `agent_snapshot_filter`
    为每个写作/修订 agent 生成 `snapshot.{agent}.json` 切片挂载(白名单外
    素材不进入其上下文;核验器仍用全量快照,claims 台账仍对全量校验)。
    实测切片体积:company_desk 680K→351K,macro 类 250-312K
    条目携带 info_level/evidence_level(见 INFO_GRADING_STANDARD.md);
    E3(单一来源/未证实)素材的任务书规则要求以"据报道/未证实"口径书写
    已知边界:T 晚间盘后财报的 SA 纪要 T+1 白天才到,赶不上晨间构建 ——
    此类 Management said 仍由 CB Overtime 转录覆盖
  macro_pillars: 曲线 / FRED ranks+extras / fed_odds(+前一交易日 prior) /
                 FOMC / auction(FiscalData,仅 Note/Bond) /
                 release_series(环比百分比) / nowcasts(失败回退日历 GDPNow)
  prior_odds ← next_fomc_odds.prior,没有时回退上一期快照
  macro_dashboard(v8):四经济体(US/EU/CN/JP)× 指标族白名单(政策利率/增长/
                 通胀/PMI/就业),[T-75,T] 最新 actual,绕过 impact=3 过滤
                 (ECB/BoJ/LPR 利率行 impact 只有 1-2)
  fomc(v8):    fomc_decisions 最近 4 场(利率区间/投票比/异议理由)+
                 fomc_probabilities 未来会议定价路径(asof≤T 最新一日)+
                 最近两份声明全文(措辞对比用,截 5000 字符)
  news_macro(宏观地缘素材层,v8.2;**源池 = Admin(user_id=1)且 enabled
    的全部订阅,SQL JOIN 动态派生**;选材 = 两级漏斗,不再靠关键字正则):
    ① 轻扫(零 LLM):Admin 订阅 × [T,T+1 06:00) UTC × 5 种文本类型,
      LEFT JOIN subscription_content_tags 取「AI 分析轻量字段」——
      title + summary + concepts + countries + L/E 分级(不取原文;
      L4 排除;twitter<180/news<200 字符不进分拣,归 flash/wire 层)
    ② triage(一次分块并行 LLM 调用,~500 条/块×4 并发):逐条判定
      pool ∈ {geo, research, digest} + 开放集中文主题 + 重要度 1-3 +
      是否需读全文;判定写 triage.json(可审计、可复用、断点不重花)
    ③ 组池:入选条目才回库取全文(digest ≤4×6000 / research ≤8×2500
      (news 排除、推特 ≥250 字符) / geo ≤40×1200,重要度优先);
      条目带 theme/importance/evidence_level
    - _geo_stats 按 theme 开块(开放集,新事件天然可开块;≥3 条门槛保留),
      正则 GEO_THEME_DEFS 仅兜底无 theme 条目(回退路径产物)
    - geo 层明确排除央行内容(归 research)——央行素材属 08/10 节
    - 回退:--no-triage 或 triage 失败 → v8.1 关键字正则路径,管线不断
    - 成本:实测 10-30 全量 1910 候选→147 入选,4 块并行 ~8-10 分钟,
      输入 ≈ 0.2-0.3M tokens(占整期 ~3%),输出仅入选条目 ~10k tokens
    已知边界:周五 T 的隔夜要闻下周一晨才发,digest 层可能仍偏少,
    由 geo/research + 英文 wire 兜底(triage 版已能捞到无固定标题模式
    的晨报,10-30 实测 digest 0→2)
  ▼
  backend/reports/guanhe-daily-report/{date}/snapshot.json

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 第三层 · 生成与核验  脚本: run_pipeline.py + prompts.py + verify_checks.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  编辑计划(v8.4,写作前一次轻量 LLM):plan_input.md(结构化数字 + 带条目 id
    的新闻标题,不含转录)→ plan.json:4–8 个故事、每个故事唯一归属节 +
    可一句带过的节 + 规范数值(entity/metric/value/path);规范数值按快照核实
    后才注入任务书与参与核验(≤12 条,超时 300s);已有 plan.json 复用;失败不阻断;
    --no-plan 关闭
  两段编排(第一段并行,墙钟 ≈ 最慢一组;第二段单跑综合):
    overall_market  → 01+02+03  The Close 转录: narrative / drivers / positioning
    company_desk    → 04+06     Closing Bell 主段+OT 转录 + 成分股 + 新闻层:
                                sector_action / deep_dives
    movers          → 05        确定性渲染(S&P 500 ≥$100 亿,涨跌各 8 只 + EPS 对预期)
    macro_dashboard → 07        确定性渲染:四经济体表 + 商品行情 + 利率与汇率(par curve/2s10s/FX)
                                + 本周前瞻表(含看点列与下周大市值财报)(零 LLM)
    macro_review    → 08        回顾·宏观数据:多国 [T] 日 actual 边际变化 +
                                券商宏观解读(块注入 review_releases,每国 ≤2 族);
                                无选题 → 编排器直接写占位(平静日确定性渲染,锁定)
    geopolitics     → 09        回顾·地缘政治:主题块(geo 层命中 ≥3 开块)+
                                焦点事件(选举类命中 ≥2)
    fed             → 10        美联储观察:利率与决议(fomc.decisions)+
                                定价路径(next_fomc_odds/合约)+官员表态;
                                决议在 [T-2,T] 开「决议深读」(声明对比+异议);
                                无深读、无官员表态 → 代码渲染两块(平静日,锁定)
    ——— 第二段 ———
    outlook         → 11        展望与策略:读 01/02/04/07-10 成品 + 快照数字,
                                大事预判 / 主线跟踪(可省) / 投资建议(直接建议)
  ▼
  每节同会话写 sections/NN_*.claims.json(数字台账:value + snapshot 路径
  + 可选 entity/metric;节目数字路径 = the_close.clean_transcript /
  cnbc_closing_bell.clean_transcript[_ot];新闻数字按条目引用 news.flash[id=f12] /
  news_macro.geo[id=g3],条目 id 由 build_snapshot 编号,旧快照由编排器补)
  verify_checks.py(确定性,秒级) → check_results.json
    台账逐条按路径对数:结构化路径精确对值;新闻按被引条目对数(含 万/亿/M/B/
    million 换算,精度以台账写法为准),条目里没有即 high;整桶引用记 medium ·
    跨节一致性:同 entity+metric 跨节数值不同 → 行情类(move/ah_move/close/
    level/yield/odds)high、财报类 medium,参照 = 当日收盘数据锚点(成分股/跨资产/
    财政部曲线,有锚点时单节也核;盘前盘中报价须标 pm_move)> 计划规范值 >
    已按路径核实值 > 多数节,只路由偏离的节 · 美债全篇只用 macro_pillars.par_curve · 平静日代码渲染的 08/10 跳过台账 · 正文 %/bp 数字
    覆盖兜底 · 下一场 FOMC 概率 · 三元组/since/v8 块矩阵(国家边际变化/地缘主题/
    决议深读/展望必备块) · 正文管线标签/生产过程用语(含宏观素材层词汇)/动词后
    带符号 · audit 来源白名单 · 结构(07-11 中文标题) · cn 标题语言
  有 high fail → 不调 LLM,直接按清单 --resume 修订
  全绿 → --llm-verify annotate(默认:只读全部节正文,不挂快照/转录;能路由到
    可修订节的 high 驱动且只驱动一轮修订,此后只跑确定性核验;其余意见标
    advisory 进复核备注)| light(同款,high 触发修订)| off | full
  Gate(v8.4)→ 只看 high:无 high 即发布,medium/low 与确定性残留写入
    report.audit.md「Review notes」,不驱动修订、不进正文;high 只路由到
    可修订节 —— 确定性节(05/07 及平静日的 08/10)不修,占位节(块矩阵为空的合同输出)只接受
    确定性核验的问题;high 全落在锁定节 → 直接转人工,不空转修订轮次
  Gate → assemble(正文无口径块) + render_charts.py → final/
```

## 产物目录

```
backend/reports/guanhe-daily-report/{date}/
├── snapshot.json
├── state.json
├── plan_input.md / plan.json   # 编辑计划(故事归属 + 规范数值)
├── sections/01..11_*.md
├── sections/*.claims.json   # 数字台账(movers/macro_dashboard 确定性渲染,无台账)
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
| `us_close_chart.py` | GuanHe 同款 "US close" 总览图(FT 表格风,7 分组×Last/1D/1W/1M/YTD) |
| `finviz_map_chart.py` | GuanHe 同款 finviz 板块热力图(Equity Market 首图,自绘) |

## 使用

```bash
PY=/root/miniconda3/envs/guanhe/bin/python
ZCODE=/root/.zcode/server/bin/zcode

$PY tools/guanhe-daily-report/build_snapshot.py [--date 2026-10-28] [--no-backfill]
$PY tools/guanhe-daily-report/run_pipeline.py 2026-10-27            # 默认 --style cn
$PY tools/guanhe-daily-report/run_pipeline.py 2026-10-27 --llm-verify off   # 只用确定性核验
$PY tools/guanhe-daily-report/run_pipeline.py 2026-10-27 --no-plan          # 跳过编辑计划
$PY tools/guanhe-daily-report/run_pipeline.py 2026-10-27 --fake-zcode   # 状态机自测
$PY tools/guanhe-daily-report/render_charts.py backend/reports/guanhe-daily-report/2026-10-27   # 01_markets 自动走 FT 版,失败回落旧简版
$PY tools/guanhe-daily-report/us_close_chart.py --workspace backend/reports/guanhe-daily-report/2026-10-30   # 单独重出 final/charts/01_markets.png
```

`us_close_chart.py` 数据映射(逆向 GuanHe 10-30 图并逐格数值验证):Last/1D 与板块行
perf 列取 finviz 快照(MAX(snapshot_date)≤T+1);1W/1M=T-7/T-30 日历日前最近收盘;
ETF 的 W/M/YTD 用 adjusted_close 总回报;利率行优先 FRED 财政部曲线(DGS2/10/30,
发布滞后时自动回落 EODHD 并在页脚注明);DXY 走 Yahoo;MOVE 无源跳过。
嵌入版式与 GuanHe 一致:01_us_close 放正文 "## Overall Market" 标题下(顶部),
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
配色自 GuanHe 原图采样,±3% 即达最亮档。

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
6. **Macros 块矩阵(v8)** — `run_pipeline.macro_blocks` 纯代码判定:
   review_releases = 四经济体 T 日有 actual 的一线事件,按族优先级(利率决议 >
   就业 > CPI > PCE > GDP > JOLTS > PMI > PPI > 零售 > 工业 > 贸易)每国 ≤2,
   排除分项、yoy 优先;日历 actual 滞后时,build_snapshot 用 T 日 flash 的
   「数值 (Est. …, Prior …)」快讯回填(actual_source 记来源条目,量级/期别/
   国家/比较口径不符不回填);同一事件因 DST/改时重复的日历行去重;geo_themes = 中文 geo 层关键词命中 ≥3 开主题块(美伊/俄乌/中东/
   贸易制裁);focal_events = 选举类命中 ≥2 且过半条目未归入已开主题时开「焦点事件」;fomc_deep = 决议在
   [T-2, T] 开「决议深读」(声明对比 + 异议理由);展望(大事预判/投资建议)
   恒在。07 宏观速览 = macro_dashboard/fomc/cross_asset 确定性渲染,零 LLM。
7. **审计分层** — 正文只有读者文体与原始出处;管线标签、口径块与
   `<!-- audit: -->` 全部进 sidecar,正文出现管线标签或生产过程用语即 high
   (v8 中文禁词追加了素材/管线/入库/白名单/订阅源/抓取/星球/早报)。
8. **转录为素材、不用 Surveillance、不读摘要表** — 节目只读 The Close /
   Closing Bell(主段+OT)清洗转录,与新闻层、成分股数据一起作为素材;
   选题按市场重要性,不按节目时长。**v8 起 Macros(07-11)完全不读节目转录**。
   两段写作:第一段六组并行(墙钟 ≈ 最慢一组),第二段 outlook 读成品综合。
9. **时间口径** — 经济日历的 `time` 是 UTC,正文只引用 `time_et`(美东)。
   同一事件名、不同 `comparison`(mom / yoy / 水平)是不同口径,不得混用。
   宏观地缘素材层 `published_at` 一律按 UTC 存储比较;digest 锚定北京 T+1 晨。
10. **回填写库** — `build_snapshot.py` 默认对生产 `economic_events` 做
    [T-2, T] 的 COALESCE upsert;调试用 `--no-backfill`。
11. **节目转录按当晚 UTC 锚定** — 转录行自身 `published_at` 即美东场次当晚,
    直查匹配无摘要表 T+1 北京戳问题;缺行(上游 ingest 滞后/断更)记入
    `errors`,该节目章节按缺源降级,不回退摘要。
12. **观点红线(v8)** — 展望节判断句必须读作判断(「建议」「预计」),不得
    伪装成报道事实;每个数字仍进台账;无结构化口径的数据(EIA 库存、伦敦现货
    金银、BOJ/ECB 概率、民调席位)只在素材有原文时引用,无则整句省略,
    **禁止替代指标**。
13. **订阅纪律(v8.1)** — 素材层只消费 **Admin(user_id=1)且 enabled 的订阅**,
    由 `build_snapshot._news_macro_snapshot` 对 `subscription_list` 做 SQL JOIN
    动态派生,不硬编码作者白名单;订阅含中英文源(星球/推特/news 频道/
    substack/youtube),筛选模式中英双语。Admin 补订/退订即自动生效,
    新增素材源无需改代码。v7 起 news 层(flash/wire)同样由 Admin 订阅
    派生,全管线无任何硬编码信源白名单;信源质量由入库打标的 L/E 分级
    控制(L4 排除、溢出按 L 级排序,见 INFO_GRADING_STANDARD.md)。

## 已知边界

- nowcasts 网页常抓不到;回退经济日历里的 Atlanta Fed GDPNow 行(只有 GDP,没有 CPI/PCE nowcast)。
- Finviz 是整表刷新的当前状态,不能回溯历史交易日。测管线用最新有快照的日期。
- Finviz 热力图、真 RRG、ETF/个股 K 线未做。现有图是跨资产面板、板块表、
  1M 对 1D 的轮动散点(RRG 的简化替代)、Release 柱状图、个股收盘价折线。
- Today's Movers(v7)由编排器从 S&P 500 成分股快照确定性渲染 Winners / Losers
  两表(市值 ≥$100 亿,各 8 只),Earnings 栏只陈述 EPS 对预期、不写 beat/miss;
  没有盘后行情源,故不出 After-Hours 表。
- 节目转录 ingest 偶发滞后(历史批次晚 5-7 天):cron/手动构建应晚于当晚
  转录入库(北京时间次日上午 10 点后实测稳定);缺哪行记 `errors`。
- 节目口播数字是约数且偶有口误/ASR 断词(如 "SERVICE. NOW"),引用时以
  数据层口径为锚、转录数字逐条入台账对账。
- 盘中时间戳 / CDS / 10-Q 承诺等一手材料不在本管线范围内。
- Management said / Analyst / 卖方反应只在转录里有原话时才写,否则省略。
  Macros 不引用 Surveillance。
- **周五 T 的中文 digest 空窗(v8)**:隔夜全球要闻/利率早报仅工作日晨发布,
  周六构建时 digest 层常空(notes 记录),由 geo/research + 英文 wire 兜底。
- **贵金属/油价口径(v8)**:商品行情条用 COMEX/NYMEX 期货连续(GC1/SI1/
  BZ1/CL1),非伦敦现货;xiao 参考稿的现货金银与期货收盘有细微价差,正文
  引用时明确期货口径,不得写成现货价。
- **无结构化源的数据(v8)**:EIA 原油库存、伦敦现货金银、BOJ/ECB 加息概率、
  民调/席位数字 —— 只在新闻/研报素材有原文时引用(出处+入台账),无则整句
  省略,禁止替代指标。
- 宏观地缘素材层完全由 Admin(user_id=1)的 enabled 订阅决定:某订阅停更
  (如微博财经7X24、洪灝的宏观策略等历史死源)只影响该源自身命中量,
  层内其余订阅自动补位;Admin 补订新源(如专职地缘刊)无需改代码。
