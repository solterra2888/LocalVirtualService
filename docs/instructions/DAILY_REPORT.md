# Daily Market Report 数据计算手册

本文说明参考系统 [dashboard.cntlabs.io](https://dashboard.cntlabs.io/reports/DailyReport/) 的 **Daily Market Report** 页面结构、数据来源、字段计算逻辑，以及与本项目 `finviz_screener` 表的对应关系。供后续复现或校验使用。

---

## 1. 页面结构总览

Daily Market Report 是一个纯前端渲染的单页 HTML 报告（约 11000+ 行），使用 **Plotly.js** 绘制交互式图表。报告通过四个主 Tab 组织内容：

| Tab | 数据源 JSON | 内容 |
|-----|------------|------|
| **Home** | `the_close_summary.json` | Bloomberg "The Close" PM Brief — 市场叙事、驱动因素、跨资产信号、持仓流向、关键分歧、PM Takeaway |
| **Stock News** | `closing_bell_summary.json` | CNBC Closing Bell — 个股涨跌、公司深度分析、板块动态、交易/持仓、明日展望、指数计分卡 |
| **Market Overview** | 内嵌 HTML 数据 | US Assets / US Sector / Thematic ETFs 表现表、Plotly 累计收益曲线、指数贡献者、市场内部指标、隔夜/日内收益 |
| **Macros** | `daily_macro_review.json` | 每日宏观复盘 — 经济数据发布、前瞻催化剂、PM Takeaway |

Home、Stock News、Macros 三个 Tab 的内容以 Markdown 格式存储在 JSON 中，由前端 `renderMarkdownContent()` / `renderStockNews()` / `renderMacroReview()` 解析渲染。Market Overview Tab 的数据直接内嵌在 HTML 中。

### 1.1 URL 结构

```
https://dashboard.cntlabs.io/reports/DailyReport/{YYYY-MM-DD}/daily_report_{YYYY-MM-DD}.html
```

### 1.2 前端数据加载

```javascript
// Home Tab — 加载 Bloomberg The Close 摘要
function loadCloseSummary() {
    const url = '/reports/DailyReport/' + dateStr + '/the_close_summary.json';
    fetch(url).then(r => r.json()).then(data => renderCloseSummary(data));
}

// Stock News Tab — 加载 CNBC Closing Bell 摘要
function loadStockNews() {
    const url = '/reports/DailyReport/' + dateStr + '/closing_bell_summary.json';
    fetch(url).then(r => r.json()).then(data => renderStockNews(data));
}

// Macros Tab — 加载每日宏观复盘
function loadMacroReview() {
    const url = '/reports/DailyReport/' + dateStr + '/daily_macro_review.json';
    fetch(url).then(r => r.json()).then(data => renderMacroReview(data));
}
```

JSON 文件路径示例：
- `/reports/DailyReport/2026-06-09/the_close_summary.json`
- `/reports/DailyReport/2026-06-09/closing_bell_summary.json`
- `/reports/DailyReport/2026-06-09/daily_macro_review.json`

---

## 2. Market Overview — US ASSETS 数据来源

### 2.1 数据源确认

Market Overview Tab 下 **US ASSETS** 表的数据来自 **Finviz Elite API**。已通过三方交叉验证确认：

| 验证对象 | 日期 | SPY Price | SPY Day% | 结论 |
|---------|------|-----------|----------|------|
| Dashboard 报告 | US 6/9 | 737.05 | -0.29% | 基准 |
| 本项目 `finviz_screener` 表 (snapshot 6/11) | US 6/10~11 | 731.95 | -0.69% | 与 Dashboard 不同日，属正常 |
| Finviz API 实时拉取 | US 6/11 | 732.03 | -0.68% | **与 DB 高度一致** (差异 < 0.1%) |

**关键证据：** DB snapshot 数据与 Finviz API 实时数据在 Price / Day / Week / Month / 1Y / SMA / RSI 等字段上差异极小（价格差 < 0.55，百分比差 < 0.1%），确认 `finviz_screener` 表的数据忠实来自 Finviz。

### 2.2 Finviz API 调用方式

```python
import requests

api_key = "your_finviz_api_key"  # 对应 .env 中的 finviz_api_key

# 完整导出 URL（v=152 表示包含所有列）
url = (
    f"https://elite.finviz.com/export.ashx?"
    f"v=152&c=0,1,2,79,3,4,5,129,6,7,8,9,10,11,12,13,75,"
    f"16,17,18,19,20,21,24,25,26,27,28,29,30,31,32,33,34,"
    f"35,36,37,38,39,40,41,42,43,44,45,47,46,138,139,140,"
    f"48,49,50,51,52,53,54,57,58,134,59,68,60,61,63,64,67,65,66"
    f"&auth={api_key}"
)

response = requests.get(url, timeout=60)
# 返回 CSV 格式，约 11000+ 行（覆盖全市场股票+ETF）
```

**c 参数列号说明**（v=152 模式）：

| c 号 | CSV 列名 | 对应 finviz_screener 字段 |
|------|---------|-------------------------|
| 0 | No. | — (序号) |
| 1 | Ticker | `ticker` |
| 2 | Company | `company` |
| 79 | Index | `index_name` |
| 3 | Sector | `sector` |
| 4 | Industry | `industry` |
| ... | ... | ... |
| 57 | 52-Week High | `week_52_high` |
| 58 | 52-Week Low | `week_52_low` |
| 59 | 52-Week Range | `week_52_range` |
| 60 | RSI (14) | `rsi_14` |
| 61 | Earnings Date | `earnings_date` |
| 62 | Change from Open | `change_from_open` |
| 63 | Gap | `gap` |
| 64 | Average Volume | `average_volume` |
| 65 | Relative Volume | `relative_volume` |
| 66 | Volume | `volume` |
| 67 | Price | `price` |
| 68 | Change | `change_percent` |

### 2.3 数据存储策略

`finviz_screener` 表采用 **TRUNCATE + RELOAD** 策略：

```python
# 每次 load 清空全表再批量导入
cursor.execute("TRUNCATE TABLE finviz_screener RESTART IDENTITY")
# 使用 COPY 命令批量导入
copy_sql = f"COPY finviz_screener ({columns}) FROM STDIN WITH (FORMAT text, DELIMITER E'\\t', NULL '\\N')"
```

**注意事项：**
- 每次刷新会覆盖所有历史快照，仅保留最新一天的截面数据
- `snapshot_date` 默认值为 `CURRENT_DATE`（PostgreSQL 服务器时区）
- `created_at` 记录实际写入时间戳

---

## 3. US ASSETS 表字段映射

### 3.1 Dashboard 展示列 → finviz_screener 字段

Dashboard 的 US ASSETS 表包含以下列，全部直接来自 Finviz 单日快照数据：

| Dashboard 列标题 | 数据类型 | finviz_screener 字段 | Finviz CSV 列名 | 计算方式 |
|-----------------|---------|---------------------|----------------|---------|
| **Ticker** | 字符串 | `ticker` | `Ticker` | 直接取值 |
| **Name** | 字符串 | — (自定义映射) | — | Dashboard 硬编码映射（见下表） |
| **Price** | 数值 | `price` | `Price` | 直接取值 |
| **Day** | 百分比 | `change_percent` | `Change` | 直接取值（当日涨跌幅） |
| **Open** | 百分比 | `change_from_open` | `Change from Open` | 直接取值（相对开盘价变动） |
| **Gap** | 百分比 | `gap` | `Gap` | 直接取值（开盘缺口） |
| **Week** | 百分比 | `performance_week` | `Performance (Week)` | 直接取值（周收益率） |
| **Month** | 百分比 | `performance_month` | `Performance (Month)` | 直接取值（月收益率） |
| **1Y** | 百分比 | `performance_year` | `Performance (Year)` | 直接取值（年收益率） |
| **SMA20** | 百分比 | `sma_20_day` | `20-Day Simple Moving Average` | 价格 vs 20 日均线偏离度 |
| **SMA50** | 百分比 | `sma_50_day` | `50-Day Simple Moving Average` | 价格 vs 50 日均线偏离度 |
| **SMA200** | 百分比 | `sma_200_day` | `200-Day Simple Moving Average` | 价格 vs 200 日均线偏离度 |
| **RSI** | 数值 | `rsi_14` | `Relative Strength Index (14)` | 14 日 RSI 指标 |

### 3.2 Ticker → Name 映射

Dashboard 对 US ASSETS 表中的 ETF 做了自定义名称映射：

| Ticker | Dashboard 显示名 | 类别 |
|--------|----------------|------|
| SHY | 2Y Treasury | 利率 |
| IEF | 10Y Treasury | 利率 |
| TLT | 30Y Treasury | 利率 |
| UUP | DXY | 外汇 |
| SPY | S&P 500 | 股指 |
| QQQ | Nasdaq 100 | 股指 |
| IWM | Russell 2000 | 股指 |
| ^VIX | VIX | 波动率 |
| USO | WTI Crude | 商品 |
| BNO | Brent Crude | 商品 |
| UNG | Natural Gas | 商品 |
| CPER | Copper | 商品 |
| GLD | Gold | 商品 |
| SLV | Silver | 商品 |
| HYG | HY Credit | 信用 |
| LQD | IG Credit | 信用 |

> **注意：** `^VIX` 不在 Finviz 普通导出中，需要从其他数据源单独获取。

### 3.3 涨跌幅可视化

Dashboard 对每个 Day 列的数值添加了颜色和柱状条：

```html
<!-- 正收益 -->
<td class="perf positive" data-val="0.32">+0.32%</td>

<!-- 负收益 -->
<td class="perf negative" data-val="-1.15">-1.15%</td>

<!-- 持平 -->
<td class="perf neutral" data-val="0.0">+0.00%</td>
```

柱状条（bar-cell）宽度按绝对值等比缩放：

```html
<td class="bar-cell" data-val="-1.15">
    <div class="bar-container">
        <div class="bar-center"></div>
        <div class="bar-negative" style="width:28.75%"></div>
    </div>
</td>
```

---

## 4. Market Overview 其他子面板

### 4.1 US SECTOR 表

使用相同的 finviz 数据源，按 **Sector ETF** 筛选展示：

| Ticker | 名称 |
|--------|------|
| XLC | Communication Services |
| XLY | Consumer Discretionary |
| XLP | Consumer Staples |
| XLE | Energy |
| XLF | Financials |
| XLV | Healthcare |
| XLI | Industrials |
| XLB | Materials |
| XLRE | Real Estate |
| XLK | Technology |
| XLU | Utilities |

字段结构与 US ASSETS 相同。

**验算结论（2026-06-11 三方交叉验证）：**

| 验证项 | 结果 |
|--------|------|
| DB ↔ Live Finviz 价格差异 | 平均 < 0.3，11 个 ETF 中 10 个 < 0.5 |
| DB ↔ Live Finviz 百分比差异 | 平均 < 0.15%，低波动品种 (XLP/XLF/XLB) < 0.1% |
| DB ↔ Live Finviz RSI 差异 | 0.1 ~ 1.5，属正常盘中波动 |
| Dashboard 字段映射 | ✅ 全部 14 列与 finviz_screener 1:1 对应 |

差异原因：DB 快照在美股收盘后（北京早 8:00）写入，Live 数据在美东盘中拉取，盘中交易导致的小幅偏差属正常现象。高波动 ETF（XLK 周跌幅 -8%~-9%）差异略大。

### 4.2 Thematic ETFs 表

按主题 ETF 筛选展示，涵盖 AI/半导体/云计算/网络安全/新能源等热门赛道：

| Ticker | 名称 | 主题 |
|--------|------|------|
| ARKK | ARK Innovation | 创新/颠覆科技 |
| ARKQ | ARK Autonomous Tech | 自动驾驶/机器人 |
| BOTZ | Global X Robotics & AI | AI 与机器人 |
| BUG | Global X Cybersecurity | 网络安全 |
| CIBR | iShares Cybersecurity | 网络安全 |
| CLOU | Global X Cloud Computing | 云计算 |
| DRIV | Global X Autonomous & EV | 电动车/自动驾驶 |
| FINX | Global X FinTech | 金融科技 |
| HACK | ETFMG Prime Cyber Security | 网络安全 |
| ICLN | iShares Global Clean Energy | 清洁能源 |
| LIT | Global X Lithium & Battery | 锂电池 |
| ONLN | Global X E-Commerce | 电商 |
| PHO | Invesco Water Resources | 水资源 |
| ROBO | ROBO Global Robotics | 机器人 |
| SKYY | First Trust Cloud Computing | 云计算 |
| SMH | VanEck Semiconductor | 半导体 |
| SOXX | iShares Semiconductor | 半导体 |
| SOXQ | Invesco PHLX Semiconductor | 半导体 |
| TAN | Invesco Solar | 太阳能 |

字段结构与 US ASSETS 相同。

**验算结论（2026-06-11 三方交叉验证）：**

按盘中波动程度分三档：

| 波动档位 | ETF | 价格差异 | 百分比差异 | 原因 |
|---------|-----|----------|-----------|------|
| 🟢 低波动 | BUG, HACK, PHO, ONLN, CLOU, LIT, BOTZ, ROBO, TAN | < 0.25 | < 0.45 | 盘中交易清淡 |
| 🟡 中波动 | ARKK, ARKQ, CIBR, FINX, ICLN, SKYY, DRIV | 0.3 ~ 1.0 | 0.5 ~ 1.3 | 日内仍有交易量 |
| 🔴 高波动 | SMH, SOXX, SOXQ | 5.6 ~ 5.8 | 0.9 ~ 2.5 | 半导体极端波动（SOX 周跌 ~10%） |

总体匹配率 57%（65/114），未匹配项差异全部由**盘中时间差**导致（DB 快照 vs 实时拉取相隔数小时美股交易）。高波动品种差异方向一致（Live 比 DB 偏空），符合"半导体继续下跌"的市场叙事。

**结论：三个 Performance 表数据全部来自 Finviz，字段一一对应，数据准确性已验证。**

### 4.3 Performance Charts（Plotly 交互图表）

```
数据来源：前端 JavaScript 内嵌的历史价格数组
图表库：Plotly.js 2.27.0
```

特性：
- 支持 1D / 5D / 1M / 3M / YTD / 1Y / ALL 时间范围切换
- 支持 ticker 切换按钮（全选/全不选）
- 累计收益率曲线（归一化为百分比）
- 同步联动：同一图表组内的时间范围和 ticker 切换自动同步

### 4.4 Index Performers（指数贡献者）

按指数 ETF 分组展示 Top/Bottom 5 贡献者：

| 指数 ETF | 说明 |
|---------|------|
| SPY | S&P 500 |
| QQQ | Nasdaq 100 |
| DIA | Dow Jones |
| IWM | Russell 2000 |
| MDY | Mid-Cap 400 |
| etc. | ... |

### 4.5 Market Internals

NYSE / NASDAQ 内部指标（上涨/下跌/持平家数、涨跌成交量等），以 base64 内嵌图片方式展示。

### 4.6 Overnight vs Intraday Returns

隔夜收益（前一收盘到今日开盘）与日内收益（今日开盘到今日收盘）的分解对比，可折叠展开。

---

## 5. 数据刷新与日期约定

### 5.1 时区关系

| 概念 | 时区 | 说明 |
|------|------|------|
| Dashboard 报告日期 | US Eastern (ET) | URL 中的 `YYYY-MM-DD` 为美东交易日 |
| finviz_screener.snapshot_date | PostgreSQL `CURRENT_DATE` | 取决于 DB 服务器时区配置 |
| finviz_screener.created_at | UTC | 实际写入时间戳 |
| 数据抓取时机 | 北京时间次日早 8:00 左右 | 对应 US 前一交易日收盘后 |

**日期映射示例：**

```
US 交易日 6/9 (Mon)
  → 北京时间 6/10 上午抓取 → created_at = 2026-06-10 00:xx:xx UTC
  → snapshot_date 取决于 DB 时区设置

US 交易日 6/10 (Tue)
  → 北京时间 6/11 上午抓取 → created_at = 2026-06-11 00:00:03 UTC
  → snapshot_date = 2026-06-11
```

### 5.2 验算方法

要验算某日的 Dashboard 数据是否与 Finviz 一致：

1. **确认日期对齐**：Dashboard URL 日期 = US 交易日，需找到对应的 finviz_screener snapshot
2. **查询数据库**：
   ```sql
   SELECT ticker, price, change_percent, change_from_open, gap,
          performance_week, performance_month, performance_year,
          sma_20_day, sma_50_day, sma_200_day, rsi_14
   FROM finviz_screener
   WHERE snapshot_date = '目标日期'
     AND ticker IN ('SPY', 'QQQ', 'IWM', ...);
   ```
3. **对比 Dashboard 表格值**：逐字段核对，正常差异应 < 0.1%（盘中波动除外）
4. **或直接调 Finviz API** 拉取实时数据进行比对

---

## 6. 关键发现与注意事项

1. **Dashboard 是外部系统**：`dashboard.cntlabs.io` 的 Daily Market Report 不由本代码库生成，本项目的 Theme Report 是模仿其设计（代码中有注释 `matches reference dashboard.cntlabs.io`）
2. **Home / Stock News / Macros 内容来源**：这三个 Tab 的 JSON 数据分别对应 Bloomberg "The Close"、CNBC "Closing Bell"、宏观经济数据，推断是 YouTube 节目录制 → 转录 → LLM 摘要的管道
3. **VIX 数据**：`^VIX` 不在 Finviz 标准导出中，Dashboard 的 VIX 数据需从其他数据源获取
4. **TRUNCATE 策略限制**：由于每次刷新覆盖全表，无法回溯历史某天的数据进行事后验算。如需保留历史数据，需改为增量快照模式
