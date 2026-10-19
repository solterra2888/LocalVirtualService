# Earnings Calendar 独立模块

独立的美股财报日历展示模块,入口在侧边栏 **Earnings Calendar**(`/markets/earnings-calendar`)。

数据来源为 `earnings_calendar` 表(EODHD `/calendar/earnings`,每日 18:00 由 `Earnings Calendar Auto Fetch` 定时任务更新)。该表此前只有入库、无任何查询展示出口,本模块为其补齐了查询 API 与前端页面。

## 功能

- **三档范围**(默认 My Watchlist):
  - `watchlist`:`target_tracking_config` 中启用的 `primaryticker`(与 Market Weekly 周报同口径)
  - `theme`:`theme_company_associations` 中已验证的 ticker
  - `us`:全部美股(代码不含交易所后缀;按 `companies.type` 滤掉优先股系列)
  - `hk`:全部港股(`.HK` 后缀代码)。注意:港股在 `earnings_calendar` 与 `companies` 表中均无公司名称/Logo,前端按代码展示;EODHD 不返回港股货币标识(EPS 为港币计价);港股财报季集中在 3 月/8 月,10 月等淡季窗口内事件很少属正常
- **双视图**:
  - Calendar:周一至周五五列,盘前(☀)/盘后(🌙)分组,卡片含 logo、ticker、EPS 预期或实际值;已公布财报显示实际值与惊喜率(▲绿/▼红);支持前后翻周
  - Table:日期、ticker、公司、BMO/AMC、EPS 预期/实际、Surprise%,近两周窗口
- **筛选**:盘前/盘后、已公布/未公布、ticker/公司名搜索

## API

```
GET /api/markets/earnings-calendar
    from_date / to_date   # 日期窗口,默认今天-7 ~ 今天+14
    scope=watchlist|theme|us|hk
    before_after=before|after
    search=<ticker或公司名>
    reported=yes|no        # yes=已公布(有实际EPS) / no=未公布
    limit / offset         # 默认500,上限2000
```

返回 `{ items: [...], meta: { total, latest_fetch_date, ... } }`,需登录(JWT)。

实现位置:
- 服务:`backend/services/earnings_calendar_service.py` → `get_earnings_calendar_view`
- 端点:`backend/api/markets.py` → `GET /earnings-calendar`
- 前端:`web-frontend/src/pages/EarningsCalendarPage.tsx`、`components/EarningsCalendar/WeekBoard.tsx`

## 数据口径要点

- **多快照去重**:每日抓取按 `(code, report_date, fetch_date)` 各留一行,查询用 `DISTINCT ON (code, report_date)` 取最新快照,否则同一财报会重复出现。
- **reported 筛选在最新快照之后应用**,否则已公布公司会命中旧快照、被误判为未公布。
- **实际 EPS 回填**:`fetch_custom_period_earnings` 的抓取窗口从 `today-7` 开始(原先只抓未来)。EODHD 只在财报发布后才回填 actual,只向前抓会导致已公布财报的 actual 永远缺失。窗口改后,每日任务会自然回填过去 7 天的公布结果。
- **营收列未展示**:EODHD calendar 接口实际不回填营收字段(全为 NULL),UI 不设计营收列。
- **优先股去重**(仅 `us` 范围):EODHD 会把同一家公司的每个上市证券各返回一条(如 BAC、BAC-PB、BAC-PE、BAC-PK…),查询按 `companies.type` 只保留 `Common Stock`(BRK-B、BF-B 这类双类普通股不受影响);另过滤不在 companies 表、以 RP 结尾且基础代码是普通股的存托凭证变体(如 BACRP → BAC),不能按 RP 后缀一刀切,会误伤 NRP/TRP 等正常股票。
- psycopg2 把 `%` 当占位符,SQL 字面量中的 `%` 需写成 `%%`(如 `NOT LIKE '%%.%%'`)。
