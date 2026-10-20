# Earnings Calendar 独立模块

独立的美股财报日历展示模块,入口在侧边栏 **Earnings Calendar**(`/markets/earnings-calendar`)。

数据来源为 `earnings_calendar` 表(EODHD `/calendar/earnings`,每日 18:00 由 `Earnings Calendar Auto Fetch` 定时任务更新)。该表此前只有入库、无任何查询展示出口,本模块为其补齐了查询 API 与前端页面。

## 功能

- **三档范围**(默认 My Watchlist):
  - `watchlist`:`target_tracking_config` 中启用的 `primaryticker`(与 Market Weekly 周报同口径)
  - `theme`:`theme_company_associations` 中已验证的 ticker
  - `us`:全部美股(代码不含交易所后缀;按 `companies.type` 滤掉优先股系列)
  - `hk`:全部港股(`.HK` 后缀代码)。注意:港股在 `earnings_calendar` 表中无名称/Logo,公司信息来自 `companies` 表 join——**该表港股按"裸代码 + primaryticker 带后缀"存储**,故 join 条件为 `c.code = ec.code OR c.primaryticker = ec.code`;Logo 覆盖约 29%(226/777),这是 EODHD 港股 Logo 的全部覆盖,缺失的源头就没有(已实测,回填无意义);EODHD 不返回港股货币标识(EPS 为港币计价);港股财报季集中在 3 月/8 月,10 月等淡季窗口内事件很少属正常
- **双视图 → 三视图**(Calendar / Month / Table):
  - Calendar:周一至周五五列,每天一个透明灰底卡片框(不分盘前/盘后),卡片为 EarningsHub 风格紧凑竖卡(方形 logo + 代码,无 logo 时用 ticker 哈希着色的首字母方块);已公布财报右上角绿/红圆点标 beat/miss;BMO/AMC、EPS、惊喜率在 hover 提示;支持前后翻周
  - **Month**:整月网格(**仅周一至周五五列**,对齐 EarningsHub,周末财报在 Table 视图可见),每日显示财报家数 + 市值 Top N 公司 chip,背景按当日家数热度着色;点日期跳转该天所在周的 Calendar 视图;顶部显示当月总家数。市值优先取 `company_highlights.marketcapitalization`(EODHD,覆盖约 80%),缺失回退 `finviz_screener.market_cap × 1e6`
  - Table:日期、ticker、公司、BMO/AMC、EPS 预期/实际、Surprise%,近两周窗口
- **筛选**:盘前/盘后、已公布/未公布、ticker/公司名搜索(Month 视图只应用范围与搜索——月视图是总览,明细筛选不参与)
- **Ticker 详情栏 + Earnings Feed**(右侧固定栏,对齐 EarningsHub 布局):内容区左右分栏,左侧为 Calendar/Month/Table 视图,右侧 440px 常驻。
  - **默认态 = Earnings Feed**:当前窗口的财报事件流卡片(已公布在前/未公布在后),每张含 logo、ticker+公司名、相对日期(Today/in 3d)+BMO/AMC、实时价格与涨跌(stock_prices_realtime,覆盖 watchlist 名单)、REV(最近季度实际营收,来自 company_income_statement,**仅美元报告公司**——EODHD 报表按本币,非美元直接展示会误导)、EPS est→actual+惊喜率;点击卡片进入该公司详情
  - **选中态 = 公司详情(Tab 分类,对齐 EarningsHub /quote 页导航)**:公司头部(logo/名称/行业/实时价格/官网链接)与公司描述(两行折叠)为公共区,下方四个 Tab:
    - **Earnings**(默认):下次财报(日期+BMO/AMC+预期EPS+当季营收预估,取 company_earnings_trend 的 '0q' 行 revenueestimateavg)、近 12 季 EPS/Revenue 历史图表切换(recharts,beat绿/miss红)+ "Beat estimates N of 17 quarters" 统计、AI Recap 区块
    - **Info**:关键指标 3 列网格(市值/P/E/Fwd P/E/P/S/PEG/目标价/REV TTM+同比/EPS TTM+同比/每股股息+股息率——Fwd P/E 与 P/S 取 finviz 快照,缺失时用现价÷次年EPS预估、市值÷REV TTM 兜底)、52周区间+Beta、Statistics 网格(当日成交量/前收盘/员工数/做空股数/做空占流通比/做空数据日期)
    - **Analysts**:评级分布(分数/目标价/五档条)、一致预期(优先 '0q' 当季行,EPS区间/家数/30天上调下调)
    - **Dividends**:股息汇总 chips(每股/股息率/TTM 分红总额/同比——TTM 由 cash flow 最近 4 季加总,之前 4 季算同比)+ 近 12 季分红支付柱状图(company_cash_flow.dividendspaid 取 ABS,EODHD 符号正负混杂;币种随报表本币,如腾讯为 CNY)
    Tab 在切换 ticker 时保持(便于对比同一维度)。数据来自 fundamentals 体系,未抓取基本面的公司显示 No data
  - Feed 跟随当前视图窗口(周视图=上周一~本周五;月视图=当月)与范围/搜索,不受盘前盘后/已公布筛选影响;窄屏(<1100px)右栏移到下方

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

**AI Recap(按需生成, 对齐 EarningsHub 的 AI Summary/Recap)**:CompanyPanel 底部 "AI Recap" 区块,点击按钮按需生成最新一期财报电话会的中文 Markdown 回顾(一句话总结/关键数字/管理层要点/Q&A 焦点/风险/基调),带 "AI generated, may be inaccurate" 免责标签。数据源链:**Motley Fool(主源,免费网页——个股 quote 页内嵌该 ticker 的 transcript 历史 URL,文章页无付费墙)→ AlphaVantage EARNINGS_CALL_TRANSCRIPT(兜底;2026-10 实测免费 key 端点可调但所有 ticker/季度均返回空数组,等价无数据,留作将来付费升级的通道,日限额保护 20 次)**。LLM 走中央 llm_service(默认 qwen-max,可用 EARNINGS_RECAP_MODEL 环境变量覆盖;注意 qwen3-max 等推理型默认模型会把 max_tokens 耗在思考上导致空正文)。结果按 (code, period_key) 永久缓存于 `earnings_call_recaps` 表(迁移: create_earnings_call_recaps.sql)——同公司同季只抓取一次,重复点击秒回;无结果负缓存 24h;港股/带后缀代码返回 unsupported。**Staleness 标记**:MF 对部分公司存在季度覆盖缺口(如 ITW 缺 Q2-2026),当最新可获取 transcript 落后最近已公布财报超过 45 天时,响应附 `stale: true` + `latest_report_date`,前端显示橙色警示("数据源未覆盖最新季度"),新 transcript 上线后点"重新获取"自动生成新一季。全流程首次约 10-45 秒(transcript 抓取+LLM),费用约 $0.03/次(自动进 LLM usage 日志)。实现:`backend/services/earnings_recap_service.py` + markets.py 端点。

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
