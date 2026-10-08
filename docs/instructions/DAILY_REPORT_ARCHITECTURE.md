# Daily Market Report 整体架构

本文档说明参考系统 [dashboard.cntlabs.io/reports/DailyReport/](https://dashboard.cntlabs.io/reports/DailyReport/) 的 **Daily Market Report** 四 Tab 整体架构、数据来源推断、生成管线，以及与本项目的实现差距。

Market Overview 的 Finviz 字段映射与验算细节见 companion 文档 [DAILY_REPORT.md](./DAILY_REPORT.md)。

---

## 1. 顶层结构

Daily Market Report 是一个**单页 HTML 报告**（参考系统约 19MB，含大量内嵌 Plotly 历史价格数据）。页面由四个主 Tab、左侧锚点导航、日期选择器、字体缩放和暗色模式组成。

四个 Tab 对应**两条完全不同的数据管线**：

| 管线类型 | Tab | 特点 |
|---------|-----|------|
| **叙事型（LLM）** | Home / Stock News / Macros | 预生成 JSON → 前端 `fetch` → Markdown 渲染 |
| **量化型（结构化）** | Market Overview | 生成时内嵌 HTML + Plotly 数据 |

### 1.1 文件与 URL 结构（参考系统）

```
/reports/DailyReport/{YYYY-MM-DD}/daily_report_{YYYY-MM-DD}.html   # 主页面
/reports/DailyReport/{YYYY-MM-DD}/the_close_summary.json           # Home
/reports/DailyReport/{YYYY-MM-DD}/closing_bell_summary.json        # Stock News
/reports/DailyReport/{YYYY-MM-DD}/daily_macro_review.json          # Macros
```

报告日期 `YYYY-MM-DD` 为**美东交易日（ET）**。周末通常无叙事型 JSON。

### 1.2 前端加载逻辑

Home、Stock News、Macros 在页面加载时异步 fetch 对应 JSON，再用自定义 Markdown 解析器渲染：

```javascript
// Home — Bloomberg The Close PM Brief
loadCloseSummary()  → fetch('.../the_close_summary.json')  → renderCloseSummary()

// Stock News — CNBC Closing Bell
loadStockNews()     → fetch('.../closing_bell_summary.json') → renderStockNews()

// Macros — 每日宏观复盘
loadMacroReview()   → fetch('.../daily_macro_review.json')  → renderMacroReview()
```

Market Overview 的数据在 `daily_report_{date}.html` 生成时已**内嵌**，不依赖额外 JSON 请求。

### 1.3 架构总览

```
┌─────────────────────────────────────────────────────────────────┐
│                    daily_report_{date}.html                        │
│  ┌──────────┬────────────┬──────────────────┬──────────────┐    │
│  │   Home   │ Stock News │ Market Overview  │    Macros    │    │
│  └────┬─────┴─────┬──────┴────────┬─────────┴──────┬───────┘    │
└───────┼───────────┼───────────────┼────────────────┼────────────┘
        │           │               │                │
   JSON fetch  JSON fetch      内嵌 HTML       JSON fetch
        │           │               │                │
        ▼           ▼               ▼                ▼
 Pipeline A   Pipeline A      Pipeline B       Pipeline C
 (LLM 叙事)   (LLM 叙事)      (Finviz+EODHD)   (日历+TV+LLM)
```

---

## 2. 四 Tab 对照总表

| Tab | 核心问题 | 主数据源 | 存储格式 | 左侧导航要点 | 本项目状态 |
|-----|---------|---------|---------|-------------|-----------|
| **Market Temperature** | 全球资产温度如何？（本地新增，非参考系统功能） | `market_data_eod` 表（EODHD） | 内嵌 HTML + Lightweight Charts 迷你卡片 | Risk / Equity Indexes（按国家）/ FX / Commodities / US Treasuries | ✅ 已实现（2026-08） |
| **Home** | 今天市场发生了什么？PM 怎么看？ | Bloomberg **The Close** 转录 | `the_close_summary.json` | Narrative, Drivers, Cross-Asset, Positioning, Disagreements, PM Takeaway | ❌ 未实现 |
| **Stock News** | 哪些股票在动？为什么？ | CNBC **Closing Bell**（+ Overtime）转录 | `closing_bell_summary.json` | Stock Movers, Deep Dives, Sectors, Positioning, Tomorrow, Scorecard | ❌ 未实现 |
| **Market Overview** | 资产/板块/主题表现如何？ | **Finviz Elite** + 历史价格 | 内嵌 HTML + Plotly | Today's Perf, Charts, Performers, Internals, Overnight/Intraday | ⚠️ 部分实现 |
| **Macros** | 宏观数据说了什么？明天看什么？ | 经济日历 + 官方数据 + 多档 TV + LLM | `daily_macro_review.json` | Releases, Forward Catalysts, PM Takeaway | ❌ 未实现 |

三个叙事 Tab 的 JSON 结构统一为：

```json
{ "date": "2026-06-09", "content": "## Markdown 正文..." }
```

### 2.1 Market Temperature（本项目新增，2026-08）

参考系统没有此 Tab；为本地新增的**首个默认 Tab**（市场温度感知）。每品种一张迷你走势图卡片（网格排列，卡片含名称/最新收盘/日涨跌幅 + 1 年日线蜡烛图，全局 3M/6M/1Y 切换），分组与代码定义见 `daily_market_report_service.py` 的 `MARKET_TEMPERATURE_GROUPS`：

| 分组 | 品种（EODHD code） |
|------|-------------------|
| Risk | VIX（`VIX.INDX`） |
| Equity Indexes | 美国 S&P 500 / NASDAQ Composite / NASDAQ 100 / PHLX Semiconductor / Russell 2000（`GSPC.INDX`/`IXIC.INDX`/`NDX.INDX`/`SOX.INDX`/`RUT.INDX`）；中国 IH/IF/IC/IM 四大股指期货 → 标的指数（`SSE50/CSI300/CSI500/CSI1000.INDX`）；Others（日本 Nikkei 225 `N225.INDX`、韩国 KOSPI `KS11.INDX`、英国 FTSE 100 `FTSE.INDX`、德国 DAX `GDAXI.INDX`，卡片副标注明国家） |
| Foreign Exchange | USD/JPY、USD/CNH、GBP/USD、EUR/USD（`*.FOREX`） |
| Commodities | Gold/WTI/Brent/Silver/Copper/Natural Gas（`GC1/CL1/BZ1/SI1/HG1/NG1.COMMODITY` 期货连续合约，Yahoo 路由，副标注交易所 COMEX/NYMEX） |
| US Treasuries | 2Y/5Y/10Y/30Y 收益率（`US2Y/US5Y/US10Y/US30Y.INDX`） |

要点：

- 数据全部来自 `market_data_eod`（`_fetch_chart_data_by_code` 按 code 直查；与 Market Movers 的 ETF 走势共用底层查询，fallback `stock_prices_eod`）；**Russell 2000（`RUT.INDX`）、英国 FTSE 100（`FTSE.INDX`）与 6 个商品期货连续合约（`GC1/CL1/BZ1/SI1/HG1/NG1.COMMODITY`，对应 Finviz futures 同口径）例外来自 Yahoo Finance chart API**（EODHD 无 FTSE Russell 授权、无 futures 数据）——`market_data_service.YAHOO_CHART_CODES` 定义路由，`fetch_realtime_data` / `fetch_eod_data` 按分码走 Yahoo，入库与其他资产同表同 upsert，定时采集与回填（`backfill_market_data_eod.py` 同样识别该路由）均自动覆盖；商品卡片 TradingView symbol 用交易所连续合约（`COMEX:GC1!` 等）；
- 每卡片内嵌最近 260 个交易日（`MT_MAX_BARS`），蜡烛图配色与 Market Movers 查看器一致（涨绿跌红）；
- 卡片标题为链接，点击跳转 TradingView 对应 symbol 的图表页（日线 + `range=6M`），映射为 `MARKET_TEMPERATURE_GROUPS` 每张卡片的第 5 个字段（如 `TVC:SPX`、`SSE:000300`、`AMEX:GLD`、`TVC:US02Y`）；同时卡片图表已关闭 Lightweight Charts 的 attribution logo（`layout.attributionLogo: false`）——许可证要求的 TradingView 归属链接由标题链接承担；
- 工具栏 3M/6M/1Y 旁有 **MA5 / MA10 / MA30 叠加开关**（`toggleTempMA`，默认全关；SMA 从全量 bars 计算再切到可见区间，复用 `calcSMA`；配色与 Market Movers 的 MA 按钮一致）和 **− / + 卡片大小按钮**（`adjustTempCols`，调整 `--mt-cols` CSS 变量 2~4 列，默认 3；列变化后 `requestAnimationFrame` 重渲染图表以适配新宽度）；
- 卡片数据缺失不失败——渲染 "No data" 占位，`metadata.temperature_cards_found` / `temperature_missing_codes` 记录覆盖情况。美股五个指数（`GSPC` / `IXIC` / `NDX` / `SOX` / `RUT`）的最后一根早于应有交易日时，整份日报记失败，不把旧日期标成 completed。新鲜度规则、cron 和打开页面时的刷新见 [EODHD_MARKET_DATA_DEVELOPMENT_GUIDE.md](./EODHD_MARKET_DATA_DEVELOPMENT_GUIDE.md) 的「2026-10 行情新鲜度」。

---

## 3. Home Tab — Bloomberg "The Close" PM Brief

### 3.1 数据源

| 项目 | 说明 |
|------|------|
| 电视节目 | Bloomberg **"The Close"**（Bloomberg Television，约美东 4pm 收盘后） |
| YouTube 频道 | Bloomberg Television (`UCIALMKvObZNtJ6AmdCLP7Lg`) |
| 转录平台 | [Transcripta](https://dashboard.cntlabs.io/transcripta.html)（`podcast_data.js` 中 `id=theclose`，`name=Bloomberg The Close`） |
| 典型转录量 | ~14,000 words/集 |

**2026-06-09 验证示例：**

| 字段 | 值 |
|------|-----|
| Episode 标题 | *Stocks Pare Tech-Led Drop as Rotation Gains Speed \| The Close* |
| 转录字数 | 14,299 words |
| YouTube | `https://www.youtube.com/watch?v=KC19qtFQrXU` |

**独立报告（2026-09）：** The Close PM Brief 以独立报告「The Close Report」呈现
（`/reports/close-brief`），内容渲染自 `the_close_summaries`，由
`generate_the_close_summary_task` 后置钩子生成/更新（见 §10.1）。
同月起 DMR 移除 Close Brief 标签页。

**独立报告（2026-09，续）：** CNBC Closing Bell 同样以独立报告「Closing Bell Report」
呈现（`/reports/closing-bell`），内容渲染自 `cnbc_closing_bell_summaries`，由
`generate_cnbc_closing_bell_task` 后置钩子生成/更新，DMR 同步移除 Stock Pulse 标签页
（DMR 现为 Temperature / Feed Digest / Market Movers 三个标签）。

**独立报告（2026-09，第三次）：** Bloomberg Surveillance（早间节目，参考系统中仅作
Macros 辅助输入）以独立报告「Bloomberg Surveillance Report」呈现（`/reports/surveillance`），
内容渲染自 `bloomberg_surveillance_summaries`，由
`generate_bloomberg_surveillance_task` 后置钩子生成/更新；该摘要管线为新建
（scheduler 任务类型 `bloomberg_surveillance`，不进入 DMR）。

本项目 `backend/services/content_adapters/youtube_adapter.py` 将标题后缀 `The Close` 映射为 `Bloomberg The Close`，与 Transcripta 命名一致。详见 [FEEDS_MODULE.md](./FEEDS_MODULE.md) §7.5。

### 3.2 生成管线

```
Bloomberg Television YouTube
  → ASR / YouTube 字幕 → .txt 转录（Transcripta）
  → LLM Prompt A（PM Brief 专用模板）
  → the_close_summary.json
  → 前端 renderCloseSummary() + renderMarkdownContent()
```

**重要：** Daily Report 的 Home 摘要**不是**直接复用 Transcripta 节目摘要。Transcripta 使用 `Top 5 Market-Relevant Takeaways` 格式；Home 使用独立的 PM Brief 八段式结构，是对**同一转录的二次 LLM 加工**。

### 3.3 内容结构（固定八段）

```
Main Narrative
→ Top Market Drivers（约 5 条）
→ Cross-Asset Signals（股 / 债 / 信用 / 商品 / FX）
→ Positioning / Flow
→ Key Disagreements
→ Resolution Framework
→ Time Horizons（短期 1–3 天 / 结构性数周–数月）
→ PM Takeaway
```

### 3.4 内容特征

- **跨资产、PM 视角**：市场叙事、资产配置框架、策略师观点（Schwab、Citi、Amaranth、Wells Fargo、Evercore 等）
- **宏观催化剂**：CPI positioning、SpaceX IPO 流动性、地缘 flash crash
- **少有个股 deep dive**：与 Stock News 分工明确；同日事件（如 AAPL WWDC）可能在 Home 作为驱动因素提及，但无 bull/bear case 模板

---

## 4. Stock News Tab — CNBC "Closing Bell"

### 4.1 数据源

| 项目 | 说明 |
|------|------|
| 电视节目 | CNBC **"Closing Bell"**；部分日期另有 **Closing Bell Overtime** |
| 转录平台 | Transcripta（`id=closingbell`，`name=Closing Bell`） |
| 典型转录量 | ~7,000–8,000 words/集 |
| 采集方式 | 部分 episode 有 YouTube URL；许多 `url: null`，可能使用 CNBC 直采音频或其他录制渠道 |

**2026-06-09 验证示例（同日 2 集，可能合并进一份摘要）：**

| 标题 | 字数 |
|------|------|
| *Closing Bell: The State of the Tech Trade* | 7,623 |
| *Tech Swoons in Early Trading Before Paring Losses Into Close* | 7,845 |

Overtime 节目在 Transcripta 中归入同一 `closingbell` ID，而非独立节目。

### 4.2 生成管线

```
CNBC Closing Bell（+ 可选 Overtime）
  → 转录（同日多集可合并）
  → LLM Prompt B（Stock News 专用模板）
  → closing_bell_summary.json
  → 前端 renderStockNews()（含特殊表格渲染）
```

### 4.3 内容结构

```
Stock Movers（Winners / Losers / After-Hours）
→ Company Deep Dives（bull / bear case 模板）
→ Sector Action
→ Trades & Positioning
→ Tomorrow Setup
→ Index Scorecard
```

**Company Deep Dive 模板字段：** What happened / Key numbers / Management said / Analyst reaction / Bull case / Bear case / Notable

### 4.4 内容特征

- **具名嘉宾**：Mike Santoli、Jonathan Krinsky (BTIG)、Tom Lee (Fundstrat)、Glenn Cature (Light Street)、Lo Toney (PlexoCapital)、Jackson Ader (KeyBanc) 等
- **交易细节**：VIX 期权 call/put 比、ORCL implied move 12%、Fibonacci 回撤位
- **板块轮动**：XLK、ITB、REITs、9/11 sectors green
- **明日 catalyst**：CPI、ORCL earnings、SpaceX book close

### 4.5 与 Home 的分工

| 维度 | Home (The Close) | Stock News (Closing Bell) |
|------|------------------|---------------------------|
| 视角 | PM / 资产配置 | 交易员 / 个股 |
| 粒度 | 市场叙事 | Ticker 级 |
| AAPL 示例 | 驱动因素之一（-3.6%） | 完整 deep dive（Morgan Stanley PT $360 等） |
| 重叠 | 同日事件会重复出现 | 但深度和格式不同 |

---

## 5. Market Overview Tab — 量化数据面板

### 5.1 数据源

| 数据 | 来源 | 存储 |
|------|------|------|
| 三表截面数据（Price / Day / Week / RSI 等） | **Finviz Elite API**（v=152 全市场 CSV） | `finviz_screener` 表（TRUNCATE + RELOAD 每日快照） |
| 累计收益曲线 | **EODHD** 历史 OHLCV | 生成时查询，内嵌 Plotly `chart_data` |
| ^VIX | Finviz 标准导出**不含** | 需其他数据源单独获取 |

字段映射、Ticker 分组、验算方法详见 [DAILY_REPORT.md](./DAILY_REPORT.md)。

### 5.2 生成管线

```
Finviz Elite API → finviz_screener 表
EODHD OHLCV      → 图表数据查询
        ↓
DailyMarketReportService.generate()（服务端 Jinja2 渲染）
        ↓
自包含 HTML（表格 + Plotly chart_data 内嵌）
```

**与叙事 Tab 的区别：** 不走 LLM；在 HTML 生成阶段一次性写入，前端无额外 fetch。

### 5.3 子面板

| 子面板 | 内容 | 参考 Dashboard | 本项目 |
|--------|------|---------------|--------|
| **Today's Perf** | US Assets / US Sector / Thematic ETFs 三表 | ✅ | ✅ |
| **Charts** | Plotly 累计收益曲线（1D/5D/1M/3M/YTD/1Y/ALL） | ✅ | ✅ |
| **Performers** | SPY/QQQ/DIA/IWM 等指数 Top/Bottom 5 贡献者 | ✅ | ❌ |
| **Internals** | NYSE/NASDAQ 涨跌家数等（base64 图片） | ✅ | ❌ |
| **Overnight/Intraday** | 隔夜 vs 日内收益分解 | ✅ | ❌ |

**Ticker 分组：**

- US Assets（16）：SHY, IEF, TLT, UUP, SPY, QQQ, IWM, USO, BNO, UNG, CPER, GLD, SLV, HYG, LQD（+ ^VIX）
- US Sector（11）：XLC–XLU
- Thematic ETFs（19）：ARKK, SMH, SOXX 等

---

## 6. Macros Tab — 每日宏观复盘

### 6.1 数据源（多源合成，非单一节目）

| 层级 | 来源 | 作用 |
|------|------|------|
| **硬数据** | 经济日历 API（EODHD Economic Events 等） | Releases Today：Actual / Estimate / Prior |
| **硬数据** | 官方发布稿（NFIB、BEA、Census、NY Fed、CBO 等） | Numbers / Details 表格与分项解读 |
| **软信息** | Bloomberg Surveillance | Sell-Side & Strategist Commentary |
| **软信息** | Bloomberg The Close / CNBC Closing Bell | 数据发布后的市场反应引用 |
| **合成** | LLM Prompt C | Forward Catalysts、PM Takeaway |

Macros 是四个 Tab 中**输入源最复杂**的：不是单一电视节目转录，而是日历 + 官方数据 + 多档 TV 节目的 LLM 合成。

### 6.2 生成管线

```
经济日历（当日发布 + 前瞻）
  + 官方 press release / 数据详情
  + 多档 TV 转录（Surveillance / The Close / Closing Bell / Overtime）
        ↓
  LLM Prompt C（Daily Economic Review 模板）
        ↓
  daily_macro_review.json
        ↓
  前端 renderMacroReview() + renderMarkdownContent()
```

### 6.3 内容结构

```
Releases Today
→ 各指标详解（Numbers / Details / Sell-Side Reaction）
→ Market Context & Cross-Asset Color
→ Sell-Side & Strategist Commentary
→ Forward Catalysts
→ PM Takeaway
```

### 6.4 与 Home / Stock News 的区别

| 维度 | Home / Stock News | Macros |
|------|-------------------|--------|
| 主输入 | **单一收盘节目**转录 | **日历 + 官方数据 + 多档 TV** |
| 焦点 | 收盘市场叙事 / 个股 | 经济数据解读 + 前瞻催化剂 |
| 重叠 | — | 会引用 Closing Bell / The Close 对数据的市场反应（如 Existing Home Sales beat） |

---

## 7. 共享基础设施：Transcripta

[Transcripta](https://dashboard.cntlabs.io/transcripta.html)（`dashboard.cntlabs.io/podcast_data.js`）是参考系统的**电视/播客转录平台**，为三个叙事 Tab 提供原始转录输入。

| 节目 | Transcripta ID | 服务于 |
|------|---------------|--------|
| Bloomberg The Close | `theclose` | **Home** |
| CNBC Closing Bell（含 Overtime） | `closingbell` | **Stock News**；Macros 辅助 |
| Bloomberg Surveillance | `bloombergsurveillanc` | **Macros** 辅助 |

**管线分层：**

```
Layer 1: YouTube / 音频 → ASR → .txt 转录文件
Layer 2: Transcripta 摘要（Top 5 Takeaways）— 独立产品，非 Daily Report 直接使用
Layer 3: Daily Report 专用 LLM（PM Brief / Stock News / Macro Review）— 更重、更结构化的 prompt
```

本项目 Feeds 模块的 YouTube 节目名提取已与 Transcripta 对齐（见 `youtube_adapter.py` 中 `_TRANSCRIPTA_CANONICAL`）。

---

## 8. 生成时序（典型交易日）

| 阶段 | 时间（美东 ET） | 动作 |
|------|----------------|------|
| 市场收盘 | ~16:00 | The Close、Closing Bell 播出 |
| 数据采集 | 16:30–18:00 | Finviz screener 刷新；EODHD 价格更新 |
| 转录完成 | ~17:30–19:00 | 电视节目 ASR 完成 |
| JSON 生成 | ~18:00–20:00 | 三个 LLM pipeline 产出 JSON |
| HTML 组装 | ~19:00+ | `daily_report_{date}.html` 合并四 Tab |

**日期约定：**

- URL / JSON 中的日期 = **美东交易日**
- `finviz_screener` 通常在北京时间次日早晨刷新，对应前一 US 交易日（详见 [DAILY_REPORT.md](./DAILY_REPORT.md) §5）
- 周末无 Home / Stock News / Macros JSON

---

## 9. 四 Tab 信息互补关系

| 读者问题 | 优先 Tab | 补充 Tab |
|---------|---------|---------|
| 今天市场主线是什么？ | **Home** | Market Overview |
| AAPL 为什么跌 3.6%？ | **Stock News** | Home |
| CPI 明天预期多少？ | **Macros** | Home |
| SPY 本周跌了多少？ | **Market Overview** | — |
| 小企业与大盘经济是否分化？ | **Macros**（如 NFIB） | Home |

```
当日市场事件
    ├── Home          → 为什么这样走？（PM 框架）
    ├── Stock News    → 哪些股票？怎么交易？
    ├── Macros        → 数据说了什么？明天看什么？
    └── Market Overview → 数字是多少？图表验证
```

---

## 10. 本项目实现现状

### 10.1 已有组件

| 组件 | 路径 / 说明 |
|------|------------|
| Market Overview 生成 | `backend/services/daily_market_report_service.py` |
| HTML 模板 | `backend/templates/daily_market_report.html.j2` |
| Celery 任务 | `backend/tasks/daily_market_report_tasks.py`（`data_fetching` 队列） |
| Scheduler 任务类型 | `daily_market_report`（`scheduler_service.py`） |
| API | `backend/api/daily_market_report.py`（`/daily-market-report/*`） |
| 存储 | `daily_market_reports` 表 + `reports/daily/YYYYMMDD.html` |
| 公开分享 | `GET /daily-market-report/shared/{token}` |
| Finviz 数据 | `finviz_screener` 表 |
| 图表数据 | EODHD OHLCV 查询 |
| YouTube 转录 + LLM | Feeds / `youtube_author_summary_service`（可复用模式） |
| 经济事件 | `EconomicEventsService`（EODHD） |
| The Close Report（独立报告） | `backend/services/close_brief_report_service.py` + `backend/templates/close_brief_report.html.j2` + `backend/api/close_brief_report.py`（`/close-brief-report/*`）；存储 `close_brief_reports` 表 + `reports/close_brief/YYYYMMDD.html`；生成挂在 `generate_the_close_summary_task` 后置钩子（无独立调度器），占位摘要不渲染，`report_date` 新鲜度判断幂等；2026-09 起 DMR 不再包含 Close Brief 标签页 |
| Closing Bell Report（独立报告） | `backend/services/closing_bell_report_service.py` + `backend/templates/closing_bell_report.html.j2` + `backend/api/closing_bell_report.py`（`/closing-bell-report/*`）；存储 `closing_bell_reports` 表 + `reports/closing_bell/YYYYMMDD.html`；生成挂在 `generate_cnbc_closing_bell_task` 后置钩子（无独立调度器），机制同 The Close Report；2026-09 起 DMR 不再包含 Stock Pulse 标签页 |
| Bloomberg Surveillance Summary（摘要管线） | `backend/services/bloomberg_surveillance_service.py` + `backend/tasks/bloomberg_surveillance_tasks.py`（`generate_bloomberg_surveillance_summary`，`ai_processing` 队列）；存储 `bloomberg_surveillance_summaries` 表；scheduler 任务类型 `bloomberg_surveillance`（直播美东 6-9am ET 工作日；cron `30 12 * * 2-6` Asia/Shanghai：周二至周六 12:30 一次。生成方式与 The Close / Closing Bell 相同：report_date=北京自然日，截至当前的 24h 窗口，占位摘要不渲染报告）；LLM 模板 `bloomberg_surveillance_am_brief`；同日多段转录合并，截断阈值 120,000 chars |
| Surveillance Report（独立报告） | `backend/services/surveillance_report_service.py` + `backend/templates/surveillance_report.html.j2` + `backend/api/surveillance_report.py`（`/surveillance-report/*`）；存储 `surveillance_reports` 表 + `reports/surveillance/YYYYMMDD.html`；生成挂在 `generate_bloomberg_surveillance_task` 后置钩子（无独立调度器），机制同 The Close / Closing Bell Report；不进入 DMR |

### 10.2 与参考系统差距

| 功能 | 参考 Dashboard | 本项目 |
|------|---------------|--------|
| 四 Tab 外壳 + 左侧导航 | ✅ | ❌（当前模板仅 Market Overview 子 Tab） |
| Home（`the_close_summary.json`） | ✅ | ❌ |
| Stock News（`closing_bell_summary.json`） | ✅ | ❌ |
| Macros（`daily_macro_review.json`） | ✅ | ❌ |
| Market Overview 三表 + Charts | ✅ | ✅ |
| ETF 中文说明注释 | ❌ | ✅（额外功能） |
| Performers / Internals / Overnight | ✅ | ❌ |
| ^VIX | ✅ | ❌（代码中 TODO） |

### 10.3 本地生成链路（当前）

```
Scheduler
  → Celery: generate_daily_market_report_task
    → DailyMarketReportService.generate()
      → 查询 finviz_screener
      → 查询 EODHD OHLCV（图表）
      → Jinja2 渲染 HTML
      → 写入 reports/daily/ + daily_market_reports 表
```

当前 `generate()` **仅覆盖 Market Overview**；未生成三个 JSON，也未组装四 Tab 外壳。

---

## 11. 复现路线图（建议优先级）

| 优先级 | 模块 | 主要依赖 | 说明 |
|--------|------|---------|------|
| **P0** | 四 Tab 外壳 + JSON fetch 框架 | 改 `daily_market_report.html.j2` | 对齐参考 UI；叙事 Tab 可先占位 |
| **P1** | Home pipeline | YouTube The Close 订阅 + LLM Prompt A | Transcripta 已有节目映射 |
| **P1** | Stock News pipeline | CNBC Closing Bell 订阅 + LLM Prompt B | 需处理同日多集合并 |
| **P2** | Macros pipeline | `EconomicEventsService` + 多源 TV + LLM Prompt C | 输入源最复杂 |
| **P2** | Market Overview 补齐 | Performers、Internals、^VIX | 量化面板剩余子模块 |
| **P3** | 统一 `generate()` 编排 | 四 Tab 合并生成、JSON 与 HTML 同目录发布 | 小改动，放在 pipeline 就绪后 |

---

## 12. 关键结论

1. **Dashboard 是外部参考系统**：`dashboard.cntlabs.io` 的 Daily Market Report 不由本代码库生成；本项目 Theme Report 等模块在设计上参考了该 Dashboard。

2. **两条管线**：叙事 Tab（Home / Stock News / Macros）= 转录 + LLM → JSON；Market Overview = Finviz + EODHD → 服务端渲染内嵌 HTML。

3. **Transcripta 是共享转录层**：Home 和 Stock News 的原始输入来自 Bloomberg The Close 和 CNBC Closing Bell 的电视转录；Daily Report 使用**独立 LLM prompt**，不复用 Transcripta 的 `Top 5 Takeaways` 摘要。

4. **Macros 最复杂**：经济日历 + 官方数据 + 多档 TV（Surveillance、The Close、Closing Bell）合成，不是单一节目 pipeline。

5. **本项目进度**：Market Overview 核心三表 + Performance Charts 已实现；叙事三 Tab 及四 Tab 整合尚未开始。

6. **字段级验算**：Market Overview 数据已与 Finviz 三方交叉验证，详见 [DAILY_REPORT.md](./DAILY_REPORT.md)。

---

## 13. 相关文档

| 文档 | 内容 |
|------|------|
| [DAILY_REPORT.md](./DAILY_REPORT.md) | Market Overview Finviz 字段映射、验算、时区约定 |
| [FEEDS_MODULE.md](./FEEDS_MODULE.md) | YouTube 订阅、转录、节目名提取（Transcripta 对齐） |
| [LLM_ARCHITECTURE_OVERVIEW.md](./LLM_ARCHITECTURE_OVERVIEW.md) | LLM 模板与调用框架 |
| [SCHEDULER_DEVELOPMENT_GUIDE.md](./SCHEDULER_DEVELOPMENT_GUIDE.md) | Scheduler 任务开发 |
