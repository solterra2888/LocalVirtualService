# Earnings Calendar 独立模块

独立的美股财报日历展示模块,入口在侧边栏 **Earnings Calendar**(`/markets/earnings-calendar`)。

数据来源为 `earnings_calendar` 表(EODHD `/calendar/earnings`,由 `Earnings Calendar Auto Fetch` 定时任务更新,cron `0 6,18 * * *`——每日 06:00 与 18:00 各一次,窗口 [today-7, today+30]。06:00 一档用于回填隔夜美股 AMC 财报的实际 EPS,使绿/红点早上即可出现)。该表此前只有入库、无任何查询展示出口,本模块为其补齐了查询 API 与前端页面。

## 功能

- **三档范围**(默认 My Watchlist):
  - `watchlist`:`target_tracking_config` 中启用的 `primaryticker`(与 Market Weekly 周报同口径)
  - `theme`:`theme_company_associations` 中已验证的 ticker
  - `us`:全部美股(代码不含交易所后缀;按 `companies.type` 滤掉优先股系列)
  - `hk`:全部港股(`.HK` 后缀代码)。注意:港股在 `earnings_calendar` 表中无名称/Logo,公司信息来自 `companies` 表 join——**该表港股按"裸代码 + primaryticker 带后缀"存储**,故 join 条件为 `c.code = ec.code OR c.primaryticker = ec.code`;Logo 覆盖约 29%(226/777),这是 EODHD 港股 Logo 的全部覆盖,缺失的源头就没有(已实测,回填无意义);EODHD 不返回港股货币标识(EPS 为港币计价);港股财报季集中在 3 月/8 月,10 月等淡季窗口内事件很少属正常
- **双视图 → 三视图**(Calendar / Month / Table):
  - Calendar:周一至周五五列,每天一个透明灰底卡片框(不分盘前/盘后),卡片为 EarningsHub 风格紧凑竖卡(方形 logo + 代码,无 logo 时用 ticker 哈希着色的首字母方块);已公布财报右上角绿/红圆点标 beat/miss;BMO/AMC、EPS、惊喜率在 hover 提示;支持前后翻周
  - **按日查看**(2026-10,对齐 EarningsHub):**点击整个日期栏**(表头或股票区任意位置)选中该日,再点取消;**整列高亮**(导航蓝系描边+浅蓝底,点股票卡片本身不触发,直接开详情);右栏 Feed 切换为只显示该日财报(标题 `Earnings · Oct 21`);**默认选中规则**:进入/翻动 Calendar 视图时,当周含今天则默认选中今天,否则不选;月视图点某天跳周视图时直接选中该天;切到 Month/Table 视图自动取消;不做 Today 标记
  - **Month**:整月网格(**仅周一至周五五列**,对齐 EarningsHub,周末财报在 Table 视图可见),每日显示财报家数 + 市值 Top N 公司 chip,背景按当日家数热度着色;点日期跳转该天所在周的 Calendar 视图;顶部显示当月总家数。市值优先取 `company_highlights.marketcapitalization`(EODHD,覆盖约 80%),缺失回退 `finviz_screener.market_cap × 1e6`
  - Table:日期、ticker、公司、BMO/AMC、EPS 预期/实际、Surprise%,窗口与 Calendar 一致(所选周周一~周五,三视图数量一致)
- **筛选**(2026-10 简化):范围(Watchlist/All US/Hong Kong,Theme 后端仍支持但 UI 已移除)、ticker/公司名搜索、**市值筛选**(No Filter/$100B+/$30B+/$10B+/$1B+,**默认 $30B+**,三视图+Feed 通用)。原盘前盘后/已公布筛选已移除——发布时点改由周视图股票卡**左上角小太阳(BMO)/月亮(AMC)**标识。市值口径:`MCAP_USD_LATERAL` 就近取 company_highlights(港股代码在 companies 表不带 .HK 后缀,按 replace 匹配),美股缺失回退 finviz ×1e6;**币种按 companies.currencycode 换算成 USD(HKD÷7.8、CNY÷7.1 静态汇率,分级筛选对精度不敏感),其余视为 USD**;无市值数据的公司在启用筛选时被排除。同时修复了月视图 Top N 此前用本币市值排序的问题(港股 HKD 市值曾虚高 ~7.8 倍)
- **查询控件**:按全站查询控件规范(`docs/instructions/UI_STYLE_SPEC.md` 一、查询控件规范,Earnings Calendar 为参考实现)——控制栏容器加 `.app-controls` 类(`styles/control-style.css` 全局层),Segmented 蓝色系全站自动生效;页面容器顶部 padding 为 0,查询卡片顶边与导航栏顶边(viewport y=12px)对齐
- **标签胶囊**(2026-10-21):模块内所有文字标签按全站标签胶囊规范(`docs/instructions/UI_STYLE_SPEC.md` 二、标签胶囊规范,参考实现为 Sources 详情面板 Info → Tags)——CompanyPanel 的 AI/period/cached 徽标(`signal-meta`/`ticker`)、图表标题币种标记(`country`,如 HKD/CNY)、Table 视图 Pending/Today 胶囊(`signal-meta`/`signal-neutral`),统一走 `shared/aiTagChipStyles.ts` 的 `aiTagChipStyle`,不再自写胶囊样式
- **Actual 回退机制**(2026-10):EODHD 日历接口对部分公司(如 GPC 2026Q2)公布后一直不回填实际值,7 天回填窗口一过即永久缺失(过去一年 51 家/94 条)。展示层兜底:日历/表格/Feed 的 actual 为空时,按 report_date ±4 天从 `company_earnings_history`(fundamentals 管线)就近借用实际值,并现场计算 difference/surprise%(响应附 `actual_from_history: true` 标记);reported 筛选同样认回退值。未来事件与已有 actual 的行不受影响。实现:`earnings_calendar_service.py` 的 `ACTUAL_FALLBACK_LATERAL`(get_earnings_calendar_view 与 get_earnings_feed 共用)
- **Ticker 详情栏 + Earnings Feed**(右侧固定栏,对齐 EarningsHub 布局):内容区左右两列——**左列 = 查询筛选栏(顶部与导航栏齐平,右侧与下方视图卡片对齐) + Calendar/Month/Table 视图卡片**;**右列 = 440px 股票信息栏,独立整列、顶边与左侧导航栏齐平**(不随筛选栏下移)。
  - **默认态 = Earnings Feed**:当前窗口的财报事件流卡片(已公布在前/未公布在后),每张含 logo、ticker+公司名、相对日期(Today/in 3d)+BMO/AMC、实时价格与涨跌(stock_prices_realtime,覆盖 watchlist 名单)、REV(最近季度实际营收,来自 company_income_statement,**仅美元报告公司**——EODHD 报表按本币,非美元直接展示会误导)、EPS est→actual+惊喜率;点击卡片进入该公司详情
  - **选中态 = 公司详情(Tab 分类,对齐 EarningsHub /quote 页导航)**:公司头部(logo/名称/行业/实时价格/官网链接)与公司描述(两行折叠)为公共区,下方四个 Tab:
    - **Earnings**(默认):下次财报(日期+BMO/AMC+预期EPS+当季营收预估)、EPS/Revenue 柱状图切换;柱状图下方分 **Overview / History** 两个子分区(2026-10,对齐 EarningsHub)——Overview = 最新一期结果卡片(季度+日期+EPS 大数字 vs est+惊喜率+REV,beat绿/miss红左边框卡片,无 BEAT/MISS/AMC 标签)
      + **EDGAR 资料外链行**(2026-10:Report=8-K EX-99.1 新闻稿 / Slides=EX-99.2 演示稿(部分公司无) / 最近 10-Q·10-Q,`edgar_materials_service.py` 纯 requests 零依赖,ticker→CIK 全量映射缓存 7 天、申报列表与附件清单缓存 1h、请求间隔 0.12s 限速,SEC fair-access UA 沿用 form13f 的 `SEC_USER_AGENT`;探测脚本 `backend/scripts/test_edgar_materials.py`)+ AI Recap 区块(**所点事件在未来日期时隐藏**——未公布事件的"最新电话会"只是上一季旧内容;点击入口从周视图卡/表格行/Feed 卡携带 report_date 传入);History = 全部历史季度表(Quarter/Date/EPS/Surprise%)
    - **Info**:关键指标 3 列网格(市值/P/E/Fwd P/E/P/S/PEG/目标价/REV TTM+同比/EPS TTM+同比/每股股息+股息率——Fwd P/E 与 P/S 取 finviz 快照,缺失时用现价÷次年EPS预估、市值÷REV TTM 兜底)、52周区间+Beta、Statistics 网格(当日成交量/前收盘/员工数/做空股数/做空占流通比/做空数据日期)
    - **Analysts**:评级分布(分数/目标价/五档条)、一致预期(优先 '0q' 当季行,EPS区间/家数/30天上调下调)
    - **Dividends**:股息汇总 chips(每股/股息率/TTM 分红总额/同比——TTM 由 cash flow 最近 4 季加总,之前 4 季算同比)+ 近 12 季分红支付柱状图(company_cash_flow.dividendspaid 取 ABS,EODHD 符号正负混杂;币种随报表本币,如腾讯为 CNY)
    Tab 在切换 ticker 时保持(便于对比同一维度)。数据来自 fundamentals 体系,未抓取基本面的公司显示 No data
  - Feed 跟随当前视图窗口(周视图=所选周周一~周五,与 Calendar/Table 同窗口保持数量一致;月视图=当月)与范围/搜索,不受盘前盘后/已公布筛选影响;窄屏(<1100px)右栏移到下方

## API

```
GET /api/markets/earnings-calendar
    from_date / to_date   # 日期窗口,默认今天-7 ~ 今天+14
    scope=watchlist|theme|us|hk
    before_after=before|after
    search=<ticker或公司名>
    reported=yes|no        # yes=已公布(有实际EPS) / no=未公布
    limit / offset         # 默认500,上限2000

GET /api/markets/earnings-calendar/month-summary
    month=YYYY-MM          # 默认当月
    scope=watchlist|theme|us|hk
    search=<ticker或公司名>
    top_n=5                # 每日市值 Top N,1-10

GET /api/markets/earnings-calendar/company-panel
    code=<ticker>          # 公司详情面板(美股裸代码/港股带.HK)

GET /api/markets/earnings-calendar/feed
    from_date / to_date    # 默认今天-7 ~ 今天+14天
    scope / search / limit # 同上, limit上限300

POST /api/markets/earnings-calendar/company-recap
    code=<ticker>          # 生成/获取最新电话会 AI Recap(美股裸代码)
```

**AI Recap(按需生成, 对齐 EarningsHub 的 AI Summary/Recap)**:CompanyPanel 底部 "AI Recap" 区块,点击按钮按需生成最新一期财报电话会的中文 Markdown 回顾(一句话总结/关键数字/管理层要点/Q&A 焦点/风险/基调),带 "AI generated, may be inaccurate" 免责标签。数据源链(按优先级):**① Motley Fool**(快,纯 HTTP 秒级——个股 quote 页内嵌 transcript 历史 URL;覆盖 ~1000-1500 家/季,部分公司缺季度如 GPC/CB 2026Q2) → **② Yahoo Finance**(2026-10 新增,MF 缺失或落后时补位:个股 earnings 页发现链接 + 无头浏览器渲染正文约 8-10 秒;最新几季免费全文/历史存档付费,Recap 只取最新场故免费层够用;实测当天公布的电话会当晚已上线) → **③ AlphaVantage**(兜底;免费 key 实测无数据,付费后自动生效,日限保护 20 次)。两源择优按季度比较('q2-2026' → (2026,2)),MF 落后或缺失时 Yahoo 季度更新则用 Yahoo。LLM 走中央 llm_service(默认 qwen-max,可用 EARNINGS_RECAP_MODEL 环境变量覆盖;注意 qwen3-max 等推理型默认模型会把 max_tokens 耗在思考上导致空正文)。结果按 (code, period_key) 永久缓存于 `earnings_call_recaps` 表(迁移: create_earnings_call_recaps.sql)——同公司同季只抓取一次,重复点击秒回;无结果负缓存 24h;港股/带后缀代码返回 unsupported。**Staleness 标记**:MF 对部分公司存在季度覆盖缺口(如 ITW 缺 Q2-2026),当最新可获取 transcript 落后最近已公布财报超过 45 天时,响应附 `stale: true` + `latest_report_date`,前端显示橙色警示("数据源未覆盖最新季度"),新 transcript 上线后点"重新获取"自动生成新一季。全流程首次约 10-45 秒(transcript 抓取+LLM),费用约 $0.03/次(自动进 LLM usage 日志)。实现:`backend/services/earnings_recap_service.py` + markets.py 端点。

返回 `{ items: [...], meta: { total, latest_fetch_date, ... } }`,需登录(JWT)。

实现位置:
- 服务:`backend/services/earnings_calendar_service.py` → `get_earnings_calendar_view`
- 端点:`backend/api/markets.py` → `GET /earnings-calendar`
- 前端:`web-frontend/src/pages/EarningsCalendarPage.tsx`、`components/EarningsCalendar/WeekBoard.tsx`

## 数据口径要点

- **多快照去重**:每日抓取按 `(code, report_date, fetch_date)` 各留一行,查询用 `DISTINCT ON (code, report_date)` 取最新快照,否则同一财报会重复出现。
- **reported 筛选在最新快照之后应用**,否则已公布公司会命中旧快照、被误判为未公布。
- **实际 EPS 回填**:`fetch_custom_period_earnings` 的抓取窗口从 `today-7` 开始(原先只抓未来)。EODHD 只在财报发布后才回填 actual,只向前抓会导致已公布财报的 actual 永远缺失。窗口改后,每日任务会自然回填过去 7 天的公布结果。
- **前瞻窗口 30 天**:调度任务参数 `days=30`(2026-10-19 由 7 调整),窗口为 `[today-7, today+30]`。EODHD 源头约发布未来 4 周排期,`days=7` 会让月视图只能看到半个多月。单日抓取约 2 万行,30 分钟超时内可完成。
- **营收列未展示**:EODHD calendar 接口实际不回填营收字段(全为 NULL),UI 不设计营收列。
- **优先股去重**(仅 `us` 范围):EODHD 会把同一家公司的每个上市证券各返回一条(如 BAC、BAC-PB、BAC-PE、BAC-PK…),查询按 `companies.type` 只保留 `Common Stock`(BRK-B、BF-B 这类双类普通股不受影响);另过滤不在 companies 表、以 RP 结尾且基础代码是普通股的存托凭证变体(如 BACRP → BAC),不能按 RP 后缀一刀切,会误伤 NRP/TRP 等正常股票。
- **companies join 口径**:美股 `code` 为裸代码直接匹配;港股/国际代码需走 `primaryticker`(带后缀)匹配,故 join 为 `c.code = ec.code OR c.primaryticker = ec.code`。两列均有唯一约束,不会产生行重复。港股 Logo 缺口回填脚本见 `scripts/maintenance/backfill_hk_companies.py`(实测 EODHD 对缺失部分无 Logo,通常无可回填)。
- psycopg2 把 `%` 当占位符,SQL 字面量中的 `%` 需写成 `%%`(如 `NOT LIKE '%%.%%'`)。
