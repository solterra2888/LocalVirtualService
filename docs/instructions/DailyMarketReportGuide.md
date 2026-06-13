# Daily Market Report — 数据源与生成细节指南

本文档记录对参考系统 [dashboard.cntlabs.io](https://dashboard.cntlabs.io/reports/DailyReport/) Daily Market Report 的**数据源逆向分析结果**，重点说明 `performance_data.csv` 的确切来源、数据特征，以及各组件的数据管线映射关系。

整体架构与四 Tab 设计见 companion 文档 [DAILY_REPORT_ARCHITECTURE.md](./DAILY_REPORT_ARCHITECTURE.md)。

---

## 1. 核心发现：performance_data.csv 来自 Finviz Elite API

### 1.1 数据流全链路

```
Finviz Elite API (v=152 全市场 CSV 导出)
  → finviz_screener_service.download_finviz_data()
  → finviz_screener 表 (TRUNCATE + RELOAD 每日快照)
  → DailyMarketReportService 查询 finviz_screener
  → performance_data.csv (Ticker 日/周/月/年涨跌幅)
```

### 1.2 代码证据

**字段映射** — `finviz_screener_service.py` 第 224-293 行的 `field_mappings`，将 Finviz CSV 原始列名直接映射到数据库列：

| 数据库列 (`finviz_screener`) | Finviz CSV 原始列名 |
|---|---|
| `performance_week` | `Performance (Week)` |
| `performance_month` | `Performance (Month)` |
| `performance_year` | `Performance (Year)` |
| `change_percent` | `Change` |
| `change_from_open` | `Change from Open` |
| `gap` | `Gap` |
| `sma_20_day` | `20-Day Simple Moving Average` |
| `sma_50_day` | `50-Day Simple Moving Average` |
| `sma_200_day` | `200-Day Simple Moving Average` |
| `rsi_14` | `Relative Strength Index (14)` |

**报告生成** — `daily_market_report_service.py` 第 386-390 行直接查询 `finviz_screener` 表：

```python
_QUERY_COLUMNS = """
    ticker, price, change_percent, change_from_open, gap,
    performance_week, performance_month, performance_year,
    sma_20_day, sma_50_day, sma_200_day, rsi_14
"""
```

**CSV 导出** — `performance_data.csv` 包含 `ticker, change, performance_week, performance_month, performance_year` 五列，是从 `finviz_screener` 全表导出的精简版，覆盖全市场 ~11,000 个 ticker。

### 1.3 数据特征佐证

| 特征 | 观察值 | 含义 |
|------|--------|------|
| Ticker 总量 | **11,286** | Finviz Elite 全市场 screener 典型覆盖量 |
| 小数精度分布 | 4 位小数占 **76%** | Finviz 百分比输出的标准精度 |
| 浮点精度瑕疵 | 11% 的值含 `9999999...` 尾巴 | Python `float("2.01")/100` → `0.0200999999999999`（IEEE 754 经典误差） |
| 非标准 Ticker | `-WI`（When Issued）、`-U`（Units）、`-A/-B`（双级股） | Finviz screener 独有覆盖范围 |
| 指数 Ticker | 仅 `^VIX` 一个 | 代码注释："^VIX 不在 finviz_screener 中，后续从其他数据源获取" |
| 空值 | `performance_year` 空 15.7%，`performance_month` 空 2.1% | 新上市 / 数据不足的 ticker（Finviz 原始数据即如此） |

**浮点瑕疵示例（与 Finviz 数据处理一致）：**

```
0.0200999999999999   ← 来自 "2.01%" → strip("%") → float()/100
0.0197999999999999   ← 来自 "1.98%"
-0.0951999999999999  ← 来自 "-9.52%"
```

这种瑕疵模式是 Finviz 字符串百分比 → Python float 转换的**指纹特征**，EODHD API 返回的数值通常精度更高且不会出现此类误差。

---

## 2. EODHD 在 Daily Report 中的角色

EODHD **不是** `performance_data.csv` 的数据源，但在 Market Overview Tab 中承担关键职责：

### 2.1 OHLCV 日线图表数据

```
EODHD API → market_data_eod / stock_prices_eod 表
  → DailyMarketReportService._fetch_chart_data()  (第 446-469 行)
  → Plotly 累计收益曲线（内嵌到 HTML）
```

`TICKER_EODHD_MAP`（第 383 行）将 ETF ticker 映射为 EODHD 代码格式 `{TICKER}.US`：

```python
TICKER_EODHD_MAP: Dict[str, str] = {t: f"{t}.US" for t in ALL_TICKERS}
```

### 2.2 其他 EODHD 用途

| 功能 | 数据 | 表 |
|------|------|-----|
| 基本面数据 | Fundamentals API | `eodhd_fundamentals_cache` |
| 历史股价 | EOD (End-of-Day) API | `stock_prices_eod` |
| 经济日历 | Economic Events API | Macros Tab 的经济数据发布 |
| 新闻 | News API | `news_feed` 表 |

---

## 3. Finviz vs EODHD 职责分工

| 维度 | Finviz Elite | EODHD |
|------|-------------|-------|
| **在 Daily Report 中的角色** | 排行/指标面板 | 历史图表 + 宏观数据 |
| **数据类型** | 截面快照（全市场 screener） | 时间序列（OHLCV 日线） |
| **覆盖范围** | ~11,000 ticker（全 US 市场） | 按需查询（报告关注的 ETF） |
| **更新频率** | 每日 TRUNCATE + RELOAD | 增量追加 |
| **存储表** | `finviz_screener` | `market_data_eod` / `stock_prices_eod` |
| **输出格式** | CSV → PostgreSQL VARCHAR/NUMERIC | JSON API → PostgreSQL |
| **精度特征** | 4 位小数，有浮点瑕疵 | 更多小数位，值更"干净" |
| **特色字段** | RSI、SMA、Gap、Change from Open、52W Range | OHLCV、Volume、Adj Close |

**简单口诀：排行用 Finviz，画图用 EODHD。**

---

## 4. Finviz Screener 数据管线详解

### 4.1 下载与入库

```
finviz_screener_service.download_finviz_data()
  → HTTPS GET Finviz Elite API（需 finviz_api_key）
  → 返回 CSV（~11,000 行 × 60+ 列）
  → 解析 + 清洗（strip %, strip ", 处理 -）
  → COPY 到 finviz_screener 表（TRUNCATE + INSERT）
  → snapshot_date = CURRENT_DATE
```

### 4.2 关键表结构 (`finviz_screener`)

完整建表 SQL 见 `finviz_screener_service.py` 第 117-190 行。核心列：

| 列名 | 类型 | 说明 |
|------|------|------|
| `ticker` | VARCHAR(20) | 股票/ETF 代码 |
| `price` | NUMERIC | 当前价格 |
| `change_percent` | VARCHAR(20) | 日涨跌幅（原始如 `"1.80%"`) |
| `change_from_open` | VARCHAR(20) | 相对开盘价变化 |
| `gap` | VARCHAR(20) | 开盘跳空 |
| `performance_week` | VARCHAR(20) | 周涨跌幅 |
| `performance_month` | VARCHAR(20) | 月涨跌幅 |
| `performance_year` | VARCHAR(20) | 年涨跌幅 |
| `sma_20/50/200_day` | NUMERIC | 简单移动均线 |
| `rsi_14` | NUMERIC | 14 日 RSI |
| `market_cap` | NUMERIC | 市值 |
| `sector` / `industry` | VARCHAR | GICS 行业分类 |
| `snapshot_date` | DATE | 快照日期 |

### 4.3 数据清洗规则

见 `finviz_screener_service.py` 第 295-320 行：

- 空值或 `-` → PostgreSQL `NULL`
- 百分比字段：strip `%` 后存为 VARCHAR
- 数值字段：验证 `float()` 合法性，非法值 → `NULL`

---

## 5. performance_data.csv 结构

### 5.1 文件格式

```csv
ticker,change,performance_week,performance_month,performance_year
A,-0.0157,-0.0637,0.1475,0.0821
AA,0.0523,-0.1159,0.0200999999999999,1.4001
...
^VIX,-0.12511246018023325,-0.09623429441180553,0.12630360812671948,0.07880133408554751
```

### 5.2 列说明

| 列 | 含义 | 对应 `finviz_screener` 列 |
|----|------|--------------------------|
| `ticker` | 股票代码 | `ticker` |
| `change` | 日涨跌幅（小数） | `change_percent` |
| `performance_week` | 周涨跌幅 | `performance_week` |
| `performance_month` | 月涨跌幅 | `performance_month` |
| `performance_year` | 年涨跌幅 | `performance_year` |

**注意：** `change` 列名为 `change` 而非 `change_percent`，且值为小数形式（如 `-0.0157` = -1.57%），与 `finviz_screener` 表中 VARCHAR 形式的 `"1.80%"` 不同——CSV 导出时已做过 `/100` 转换。

### 5.3 特殊 Ticker

| 类型 | 示例 | 说明 |
|------|------|------|
| 双级股 | `BRK-A`, `BRK-B`, `BF-A`, `BF-B` | Finviz 标准格式（连字符） |
| When Issued | `FDX-WI`, `NVRI-WI` | 待上市交易 |
| Units | `DGAC-U`, `OTAI-U` | SPAC 单位 |
| 指数 | `^VIX` | 代码中标注为需"其他数据源" |

---

## 6. 相关代码文件索引

| 文件 | 职责 |
|------|------|
| `backend/services/finviz_screener_service.py` | Finviz API 下载 + `finviz_screener` 表管理 |
| `backend/services/daily_market_report_service.py` | Market Overview 报告生成（查询 Finviz + EODHD） |
| `backend/services/eodhd_fundamentals_service.py` | EODHD 基本面数据 |
| `backend/tasks/data_tasks.py` | Celery 数据获取任务 |
| `backend/api/screener.py` | Finviz Screener 前端 API |
| `backend/api/daily_market_report.py` | Daily Report 前端 API |

---

## 7. daily_macro_review.json 数据源逆向分析

Macros Tab 的内容由 `daily_macro_review.json` 驱动（Markdown 格式，约 11K 字）。与 `performance_data.csv`（结构化数值数据）不同，这是一份 **LLM 综合生成** 的研究报告，融合了至少四类异构数据源。

### 7.1 数据源拆解

#### 源 A：结构化经济数据 API（数字层）

**内容指纹：**
- "Releases Today" 列表包含精确发布时间（`08:30 ET`, `08:15 ET`, `10:30 ET`）
- Numbers 表格为 `Actual / Estimate / Prior / Prior Revised` 四列格式
- PPI 分项拆解极其精细（Final Demand Goods +2.8%, Gasoline +23.4%, Intermediate Demand Stage 1 +3.2%）

**推断来源：**
- **经济日历 API** — Trading Economics、FRED（St. Louis Fed）或 EODHD Economic Events API，提供发布时间表和 consensus estimates
- **BLS 官方 PPI 报告** — 分项拆解（Goods vs Services vs Intermediate Demand）直接对应 BLS 月度 PPI 新闻稿标准格式
- **DOL (劳工部)** — 初请失业金（Initial / Continuing Claims）
- **ECB 官方发布** — 三档利率（Deposit Facility / Main Refinancing / Marginal Lending）
- **EIA** — 天然气库存
- **Freddie Mac** — 30 年固定房贷利率
- **USDA WASDE** — 农产品供需报告

#### 源 B：金融电视 / 直播 ASR 转录（观点层）

**内容指纹：**
- 分析师引述带有**节目名称和出场上下文**：
  - `Jay Hatfield (Infrastructure Capital Advisors, Bloomberg Surveillance, post-data)`
- Lagarde 逐句引述为**口语化风格**，来自 ECB 新闻发布会实况：
  - `"Conflict is weighing on activity, especially services. Manufacturing has held up."`
  - `"We are beginning to see broadening of inflation. We will be extremely attentive."`
- `ECB Sources (post-decision)` — Bloomberg/Reuters 记者通过消息源获取的独家信息

**推断来源：**
- **Bloomberg TV** 音频转录（Surveillance 等节目）
- **CNBC** 音频转录（Squawk Box, Closing Bell 等）
- **ECB / Fed 新闻发布会直播**转录
- 使用 **Whisper / Fun-ASR** 等 ASR 引擎进行实时语音转文字（与 GuanHe 的 YouTube 转录管线架构一致）

#### 源 C：社交媒体 / 新闻快讯 Feed（事件层）

**内容指纹：**
- **Kobeissi Letter** 被多次引述 — Twitter/X 上的高影响力金融评论账号
- **Nick Timiraos** (WSJ "Fed whisperer") — PCE Bridge 分析通常首发在 Twitter 或 WSJ 实时博客
- **RenMac** (Renaissance Macro Research) — 通过研究笔记或 Twitter 发布
- 地缘政治时间线包含**分钟级时间戳**：`08:23 ET Trump threatens Kharg Island`, `13:29 ET Trump cancels scheduled strikes`

**推断来源：**
- **Twitter/X API** — 追踪 Kobeissi、Timiraos、各卖方分析师账号
- **Bloomberg Terminal News Wire** 或 **Reuters Wire** — 分钟级新闻时间线
- **Sell-side research distribution** — RenMac、BofA、Fitch 等研究报告

#### 源 D：卖方研究报告

**内容指纹：**
- 带有具体数字的卖方观点：`BofA: Raised 2030 server CPU TAM forecast to >$170B (from $125B)`
- 评级变更和目标价调整
- 行业/主题研判

**推断来源：**
- **Bloomberg Terminal** 卖方研究摘要
- 或通过 **卖方研究聚合 API**（如 Visible Alpha、FactSet）获取

### 7.2 LLM 综合层

**输出特征：**
- 结构化 Markdown，格式高度统一（标题层级、表格、加粗要点、分隔线）
- 每个数据发布遵循固定模板：`Headline → Numbers 表格 → Details → Sell-Side Reaction`
- 包含跨数据源的**综合分析**（如从 PPI + CPI 分项推导 PCE Bridge）
- `PM Takeaway` 部分为 LLM 的合成判断，而非单一来源摘录
- `Forward Catalysts` 将未来 1-4 周的关键事件按日期编排

**推断生成流程：**
- 使用精心设计的 **Prompt Template**，要求 LLM 按固定结构输出
- Prompt 大致结构：`"Given today's economic releases [structured data], TV transcriptions [ASR text], and market commentary [social/wire text], generate a structured daily macro review with: Releases Today, detailed breakdown per release (Numbers → Details → Sell-Side Reaction), Forward Catalysts, and PM Takeaway"`

### 7.3 推断管线架构

```
┌─────────────────────────────────────────────────────────────┐
│                    DATA INGESTION LAYER                      │
├───────────────┬─────────────────┬───────────────────────────┤
│ 经济数据 API   │ 金融电视 ASR     │ 新闻/社交媒体 Feed          │
│ (Trading Econ,│ (Bloomberg TV,  │ (Twitter/X,               │
│  FRED, BLS,   │  CNBC, ECB/Fed  │  Bloomberg Wire,          │
│  EODHD Econ   │  press conf     │  Reuters, Kobeissi,       │
│  Calendar)    │  live streams)  │  sell-side research)      │
├───────────────┴─────────────────┴───────────────────────────┤
│         SCHEDULER (定时触发，对齐经济数据发布时间)               │
├─────────────────────────────────────────────────────────────┤
│         LLM SYNTHESIS (Prompt Template)                      │
│         输入: 结构化数据 + ASR 转录 + 新闻快讯                  │
│         输出: 结构化 Markdown (固定模板)                       │
├─────────────────────────────────────────────────────────────┤
│         daily_macro_review.json  ({"date": "...", "content": "...(markdown)"})
└─────────────────────────────────────────────────────────────┘
```

### 7.4 与 the_close_summary.json / closing_bell_summary.json 的对比

三份 JSON 文件的**数据管线模式一致**，差异仅在输入内容的侧重：

| 维度 | `daily_macro_review.json` | `the_close_summary.json` | `closing_bell_summary.json` |
|------|--------------------------|--------------------------|----------------------------|
| **对应 Tab** | Macros | Home (The Close) | Stock News |
| **核心输入** | 经济数据发布 + 央行决议 | 全市场跨资产走势 + 重大事件 | 个股涨跌 + 期权异动 |
| **数据侧重** | 宏观数字 + 政策解读 | 叙事综合 + 分歧观点 | 个股催化剂 + 催化原因 |
| **TV 转录来源** | Bloomberg Surveillance、ECB 发布会 | CNBC Closing Bell、Bloomberg | CNBC Closing Bell |
| **LLM 输出结构** | Numbers → Details → Sell-Side → Forward | Narrative → Drivers → Cross-Asset → Positioning → Disagreements → Resolution → Takeaway | Winners → Losers → After-Hours → Options |
| **PM Takeaway** | 宏观方向判断 | 全局多空判断 | 个股操作建议 |

三者的共同特征：
- 均为 `{"date": "...", "content": "...(markdown)"}` 格式
- Markdown 内含 `##` 二级标题分节、`###` 三级标题细分
- 卖方/分析师引述带具体姓名和机构归属
- 前端通过 `fetch()` 异步加载 JSON，再由 JS Markdown 渲染器转为 HTML

### 7.5 原始数据可见性

经探测，页面**仅暴露 LLM 的最终产出，不暴露任何上游原始输入数据**。

#### 目录完整文件清单

```
2026-06-11/
├── charts/                        # ~100 张个股/ETF 图表截图 (.png)
├── closing_bell_summary.json      # Stock News Tab — LLM 输出
├── daily_macro_review.json        # Macros Tab — LLM 输出
├── daily_macro_review.md          # 同上内容，纯 Markdown 格式（与 JSON content 字段一致）
├── daily_report_2026-06-11.html   # 主页面（JS 动态加载上述 JSON）
├── the_close_summary.json         # Home Tab — LLM 输出
├── dji_components.csv             # 道指成分股数据
├── qqq_components.csv             # 纳指100成分股数据
├── spy_components.csv             # 标普500成分股数据
└── performance_data.csv           # 全市场涨跌幅数据
```

#### 不可见的上游数据

以下中间产物仅在服务端管线内部流转，不会输出到静态文件目录：

| 数据类型 | 说明 | 推断存储位置 |
|----------|------|-------------|
| 经济日历 API 原始响应 | Trading Economics / FRED / EODHD 返回的 JSON | 服务端数据库或内存 |
| BLS / DOL 官方数据 | PPI 分项、初请失业金等结构化数据 | 服务端数据库 |
| ECB / Fed 发布会 ASR 转录 | 实时音频转文字的原始文本 | 服务端临时存储 |
| Bloomberg / CNBC TV 转录 | 金融电视节目的 ASR 原始输出 | 服务端临时存储 |
| Twitter/X Feed | Kobeissi、Timiraos 等账号的实时抓取 | 服务端数据库 |
| Bloomberg/Reuters Wire | 新闻快讯的原始 feed | 服务端数据库 |
| 卖方研究原始报告 | BofA、RenMac、Fitch 等的完整研报 | 服务端数据库 |
| LLM Prompt + 中间调用记录 | 输入组装和 LLM 请求日志 | 服务端日志系统 |

经确认不存在 `raw/`、`transcripts/`、`sources/`、`data/`、`input/` 等子目录（均返回 HTTP 404）。`daily_macro_review.md` 与 `daily_macro_review.json` 的 `content` 字段内容一致，均为同一份 LLM 输出的两种格式包装。

**结论：** 页面呈现的是 `LLM OUT`（合成后的研究报告），无法回溯 `LLM IN`（原始数据 + 转录 + feed）。如需复现或验证 LLM 输出，必须独立重建上游数据管线。

---

## 8. 相关文档

| 文档 | 内容 |
|------|------|
| [DAILY_REPORT_ARCHITECTURE.md](./DAILY_REPORT_ARCHITECTURE.md) | 四 Tab 整体架构、管线设计、实现差距分析 |
| [DAILY_REPORT.md](./DAILY_REPORT.md) | Market Overview Finviz 字段映射与验算 |
| [FEEDS_MODULE.md](./FEEDS_MODULE.md) | YouTube 订阅与电视节目转录（Home / Stock News 数据源） |
| [LLM_ARCHITECTURE_OVERVIEW.md](./LLM_ARCHITECTURE_OVERVIEW.md) | LLM 模板框架（叙事 Tab 的 Prompt 设计参考） |
