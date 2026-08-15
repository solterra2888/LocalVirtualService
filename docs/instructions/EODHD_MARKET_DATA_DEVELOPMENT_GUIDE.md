# EODHD Market Data 接入开发指南

> 本文档定义在 GuanHe 平台**新增 / 维护 EODHD 市场数据资产**（指数/外汇/商品/债券/板块等）并通过 Scheduler 定期更新至数据库的标准开发流程。
> 2026-08 由 VIX（`VIX.INDX`）接入起沉淀，已积累 7 个案例（指数/商品/美债/外汇，含错误注册修复与资产移除），新增资产一律按此流程执行。
>
> 相关文档：[SCHEDULER_DEVELOPMENT_GUIDE.md](./SCHEDULER_DEVELOPMENT_GUIDE.md)（新增 task_type 时的全栈 checklist）、[SCHEDULER_COMMON_PITFALLS.md](./SCHEDULER_COMMON_PITFALLS.md)（调度器通用踩坑）。

---

## 核心结论（先记住这个）

**新增一个 EODHD 资产通常不需要动调度器。** `market_data` 调度任务已存在且按资产类型（index/forex/commodity/bond/sector）批量拉取，注册进 `market_data_service.py` 的 MAP 字典后，下个调度窗口自动生效。改动量：**1 行注册 + 1 次历史回填 + 1 次集成测试**。

---

## 架构总览

```
调度器服务 (scheduler_main.py, :8001)
  └─ scheduler_service.py  task_handlers['market_data'] → _execute_market_data_task
       └─ task_scheduling 表  "Market Data Auto Fetch"（cron: 0 7,14,19 * * 1-5 Asia/Shanghai）
            └─ MarketDataService.fetch_and_store_market_data(asset_types, ...)
                 ├─ 并发抓取（ThreadPoolExecutor, max_workers=10）
                 │    ├─ /api/real-time/{code}   → market_data_realtime（快照, PK=code）
                 │    └─ /api/eod/{code}         → market_data_eod（日线, UNIQUE(code,date)）
                 └─ execute_values + ON CONFLICT ... DO UPDATE（幂等 upsert）
```

### 关键代码位置

| 位置 | 作用 |
|------|------|
| `backend/services/market_data_service.py:25-70` | `INDEX_MAP` / `FOREX_MAP` / `COMMODITY_MAP` / `BOND_YIELD_MAP` / `SECTOR_MAP` 资产注册表 |
| `market_data_service.py::fetch_and_store_market_data` | 主入口，调度器直接调用 |
| `market_data_service.py::store_eod_data` | EOD 批量 upsert（`ON CONFLICT (code, date)`） |
| `market_data_service.py::fetch_eod_data` | 注意用 `utcnow()` 取日期（HK 服务器跨日问题，见代码注释） |
| `task_scheduling` 表 `type='market_data'` 行 | 调度配置（数据库驱动，非代码） |

### 数据表：`market_data_eod`

| 列 | 类型 | 说明 |
|----|------|------|
| `code` | varchar(50) | EODHD 代码，如 `VIX.INDX`、`SPY.US`、`US10Y.INDX` |
| `date` | date | 交易日 |
| `asset_type` | varchar(20) | `index` / `forex` / `commodity` / `bond` / `sector` |
| `open/high/low/close/adjusted_close` | numeric(20,6) | OHLC |
| `volume` | bigint | 成交量（指数类为 0） |
| 唯一约束 | `unique_code_date (code, date)` | upsert 幂等的基础 |

另有 `market_data_realtime`（快照表，PK=code）存实时行情，注册后同样自动更新。

---

## 标准开发流程（六步）

### Step 0：验证数据可用性（必做，先 curl 再写代码）

用 `.env` 里的 `EODHD_API_TOKEN` 直接 curl，确认三件事：**订阅覆盖该资产、代码正确、字段完整**。

```bash
TOKEN=$(grep '^EODHD_API_TOKEN=' .env | cut -d= -f2)

# 实时（返回应含 open/high/low/close/previousClose/change）
curl -s "https://eodhd.com/api/real-time/VIX.INDX?api_token=$TOKEN&fmt=json"

# 历史日线（返回应为 JSON 数组，含 date/open/high/low/close/adjusted_close/volume）
curl -s "https://eodhd.com/api/eod/VIX.INDX?api_token=$TOKEN&fmt=json&period=d&from=2026-08-01&to=2026-08-14"
```

**排查经验（VIX 接入实录）：**

| 现象 | 结论 |
|------|------|
| `Unauthorized`（连 AAPL.US 都 401） | token 失效/填错，与资产无关。`.env` 里可能存在多个 key（如向后兼容的 `API_KEY=` 别名），逐一排查 |
| `Ticker Not Found.`（404） | 代码写法错误，或**订阅包不含该数据源**。判别法：连 `ES1.COMM`（最基础期货连续合约）都 404 → 订阅包问题（如 Futures/Commodities 是独立付费包），不是代码问题 |
| 实时返回 200 但全 `"NA"` + EOD 返回 `[]` | 代码存在于系统但**EOD 行情数据源不覆盖**该资产。见下方"查官方代码的正确姿势" |
| 返回 200 但零数据行 | 代码存在于系统但该数据源不覆盖（如 screener 类接口查指数） |
| **期货符号直接加 `.US` 返回 200 且有价格** | ⚠️ **危险陷阱**：会被解析成同名的美股股票！如 `HG.US`=Hamilton Insurance Group（保险股）、`NG.US`=NovaGold（金矿股）、`WTI.US`=W&T Offshore（油气股）。商品期货符号（HG/NG/CL 等）加 `.US` 不是商品数据，**必须核对官方 exchange-symbol-list 里该 Code 的 Type/Name** |
| 搜索接口 `/api/search/xxx` 找不到 | 正常——搜索默认只索引股票/ETF，**不索引指数和期货**，直接按 EODHD 命名规则试代码 |

**查官方代码的正确姿势（勿猜代码）**：

1. 官网 Fundamentals 可用指数 Excel（[下载](https://eodhd.com/financial-apis/wp-content/uploads/2024/10/EODHD-Fundamentals_-Available-Index-list-with-component-details.xlsx)）可快速确认代码写法（如 `N225.INDX`、`GDAXI.INDX`），**但它只代表 Fundamentals 覆盖，不代表 EOD 行情覆盖**（`FTSE.INDX` 在列表里但行情全 NA）；
2. 权威清单是 Exchanges API：`curl "https://eodhd.com/api/exchange-symbol-list/INDX?api_token=$TOKEN&fmt=json"`（全量 1600+ 指数，含 Country/Currency）。**清单里没有的品牌指数（如 FTSE 系列、KOSPI 的别名写法）行情也不覆盖**；
3. 最终以实时 + EOD 两个接口实测为准（Step 0 的两条 curl）。

**EODHD 代码命名规则速查：**

| 资产 | 格式 | 示例 |
|------|------|------|
| 美股 | `{TICKER}.US` | `AAPL.US` |
| 美股 ETF | `{TICKER}.US` | `SPY.US` |
| 指数 | `{CODE}.INDX` | `VIX.INDX`、`GSPC.INDX`（即 Yahoo 的 `^GSPC`） |
| 外汇 | `{PAIR}.FOREX` | `USDCNH.FOREX` |
| 国债收益率 | `{TENOR}.INDX` 或 `{TENOR}.GBOND` | `US10Y.INDX`、`US20Y.GBOND`（同源同值，但每期限需实测哪个后缀可用） |
| 港股 | `{CODE}.HK` | `2800.HK` |
| 期货连续合约 | `{SYM}{N}.COMM` | `ES1.COMM`（需期货订阅包） |
| 商品现货外汇对 | `{SYM}USD.FOREX` | `XAUUSD.FOREX`（金）、`XBRUSD.FOREX`（Brent）；覆盖不全，实测为准 |
| 商品 ETF | `{TICKER}.US` | `GLD.US`（金）、`USO.US`（WTI） |

> 其他平台符号映射：Yahoo `^VIX` ↔ EODHD `VIX.INDX`；Finviz `futures?t=VX` ↔ EODHD `VX1.COMM`（期货，需订阅包）。

### Step 1：确定 asset_type 与 category

`asset_type` 决定该资产被哪个调度分组拉取、归入哪张报表分区，**必须从五个既有值中选**：

| asset_type | 对应 MAP | category 现有值示例 |
|-----------|----------|---------------------|
| `index` | `INDEX_MAP` | `us_equity` / `eu_equity` / `asia_equity` / `china_equity` / `volatility` |
| `forex` | `FOREX_MAP` | `major_pair` |
| `commodity` | `COMMODITY_MAP` | `precious_metal` / `industrial_metal` / `energy` |
| `bond` | `BOND_YIELD_MAP` | `us_treasury` |
| `sector` | `SECTOR_MAP` | `us_sector` |

`category` 是自由字符串，用于前端/报表分组，语义清晰即可（如 VIX 用 `volatility`，期限结构类如 VIX3M 建议也归 `volatility`）。

### Step 2：注册资产（唯一必须的代码改动）

在 `backend/services/market_data_service.py` 对应 MAP 加一行：

```python
INDEX_MAP = {
    ...
    "VIX": ("VIX.INDX", "index", "volatility"),   # 显示名是 dict 的 key；值 = (EODHD代码, asset_type, category)
}
```

格式说明：**显示名是 dict 的 key**，值是三元组 `(code, asset_type, category)`。第二个元素是 MAP 名对应 asset_type 的复述，运行时会被统一覆盖为类型字面量（见 `fetch_and_store_market_data` 中 `all_assets.update` 的展开），但注册时保持一致，避免误读。

命名惯例：真指数显示名加 "Index" 后缀（"Nikkei 225 Index"，与既有 ETF 代理 "Nikkei 225" 区分）；ETF 显示名加 "(XXX ETF)" 后缀（"Gold (GLD ETF)"）。

### Step 3：历史回填（一次性脚本）

调度任务每次只拉最近 `eod_days`（当前配置 7）天，历史数据需脚本回填。使用**通用参数化脚本** `scripts/backfill_market_data_eod.py`，支持一次回填多个代码：

```bash
python scripts/backfill_market_data_eod.py --codes GSPC.INDX,NDX.INDX --asset-type index --dry-run  # 预览
python scripts/backfill_market_data_eod.py --codes GSPC.INDX,NDX.INDX --asset-type index           # 入库（默认3年）
python scripts/backfill_market_data_eod.py --codes VIX.INDX --asset-type index --years 5          # 指定年数
```

脚本要点（勿省略）：

1. **upsert 幂等**：`ON CONFLICT (code, date) DO UPDATE`，脚本可重复执行，与调度增量无缝衔接；
2. **DatabaseManager 必须显式传参**：
   ```python
   from backend.utils.database_config import get_database_config
   db = DatabaseManager(db_conn_params=get_database_config())
   # ❌ DatabaseManager() 无参 → db_conn_params={} → 连 localhost socket → 失败
   ```
3. **日期用 `utcnow()`**（`backend.utils.datetime_utils`），勿用 `datetime.now()`（HK 服务器跨日会少拉一天）；
4. **字段清洗复用服务口径**：`safe_float`/`safe_int` 处理 `"NA"`/空串；
5. 先 `--dry-run` 看条数与首末日，再正式入库；
6. 多代码回填时单个拉取失败不阻断整体（会打错误日志后继续）。

### Step 4：集成测试（本地跑通全链路）

使用**通用参数化测试** `backend/tests/test_market_data_asset_integration.py`，四层验证：

| 层 | 验证 | 意义 |
|----|------|------|
| 注册 | MAP 三元组结构、值 | 防拼写/结构错误 |
| API 解析 | 真实返回经 `parse_realtime_data`/`parse_eod_data` 解析 | 防字段不兼容（如全 `"NA"`） |
| 全链路 | `fetch_and_store_market_data([asset_type], eod_days=7)` | 与调度器完全同参调用，0 失败才算过 |
| 数据库 | 新资产入库条数/日期范围、**既有资产未受影响** | 防回归 |

```bash
python backend/tests/test_market_data_asset_integration.py --codes GSPC.INDX,NDX.INDX --asset-type index
```

### Step 5：确认调度配置（通常零改动）

注册进 MAP 后，已有 `market_data` 调度任务自动覆盖新资产。确认方式：

```sql
SELECT id, cron_expression, timezone, enabled, last_run, next_run, params
FROM task_scheduling WHERE type = 'market_data';
-- 关注 params: {"asset_types": null} 表示拉全部类型（新资产自动包含）
-- 若 asset_types 指定了列表，需确认新资产的 asset_type 在列表中
```

仅当需要**新的调度频率/独立任务**时才动调度器，那属于新增 task_type，走 [SCHEDULER_DEVELOPMENT_GUIDE.md](./SCHEDULER_DEVELOPMENT_GUIDE.md) 的全栈 checklist。

### Step 6：部署

```bash
# 服务器上（43.99.37.76）
sudo systemctl restart rc-scheduler   # 或 scheduler/restart_scheduler.sh
# 部署后复跑集成测试确认远程环境
python backend/tests/test_market_data_asset_integration.py --codes <新资产codes> --asset-type <类型>
```

⚠️ **本地开发期注意**：本地与远程共用同一个 PostgreSQL。本地跑集成测试写入的数据是真实生效的（upsert 幂等，无害）；但**本地调试期间勿让本地 Scheduler 与远程 Scheduler 同时运行**，详见 SCHEDULER_DEVELOPMENT_GUIDE 的双调度器警告。

---

## 接入后消费路径（数据能被谁用到）

| 消费方 | 表 | 说明 |
|--------|-----|------|
| `daily_market_report_service` | `market_data_eod` | 日报 OHLCV（优先 market_data_eod，不足 fallback stock_prices_eod） |
| `market_weekly_service` | `market_data_eod` + `market_data_realtime` | 周报（eod LEFT JOIN realtime） |
| 前端 Markets 页 | 实时/历史接口 | `backend/api/markets.py` |

---

## Checklist（新增一个资产，逐项勾选）

- [ ] Step 0：curl 验证实时 + 历史接口（订阅覆盖、代码正确、字段完整）
- [ ] Step 1：确定 asset_type（五选一）与 category（语义分组）
- [ ] Step 2：`market_data_service.py` 对应 MAP 加一行（唯一代码改动）
- [ ] Step 3：通用回填脚本 `--codes ... --asset-type ...`，`--dry-run` 后正式回填
- [ ] Step 4：通用集成测试跑通（全部 ✅，含既有资产无回归）
- [ ] Step 5：查 `task_scheduling` 确认 `asset_types=null` 或已覆盖新类型
- [ ] Step 6：部署重启 `rc-scheduler`，远程复跑集成测试
- [ ] （可选）更新本文档的 category 现有值表 / 代码命名规则表，并在文末追加案例
- [ ] （若是移除/替换资产）走"移除 / 替换资产"章节，grep 全库引用后再清数据

---

## 案例：VIX 接入实录（2026-08-14）

**需求**：获取 `^VIX` 指数（Yahoo 符号）行情，入库并定期更新。

**过程**：

1. Finviz Elite API 实测：`export/screener` 可用但**不含期货/指数**（官方 API 仅 Screener/Portfolio/Groups/Options/News），`futures` 页面被 Cloudflare 挡 → 放弃 Finviz，转 EODHD；
2. EODHD 实测：`VIX.INDX` 实时/日线/周线全通，历史至 1990 年；`VX1.COMM`（VX 期货）404 → 定位为订阅包不含期货，非代码问题 → 以 `VIX.INDX`（现货指数）满足需求；
3. 中途 token 401：`.env` 的 `EODHD_API_TOKEN` 已失效，更换后恢复（`649657...`）；
4. 注册 `"VIX": ("VIX.INDX", "index", "volatility")` 进 `INDEX_MAP`；
5. 回填 3 年共 **778 个交易日**（2023-08-15 ~ 2026-08-14）；
6. 集成测试 **16/16 通过**：全链路 9 指数并发抓取 0 失败、SPY 等既有资产无回归。

**产出文件**：

| 文件 | 用途 |
|------|------|
| `backend/services/market_data_service.py`（+1 行） | VIX 注册 |
| `scripts/backfill_market_data_eod.py` | 历史回填（通用，由 VIX 专用版泛化而来，专用版已删） |
| `backend/tests/test_market_data_asset_integration.py` | 集成测试（通用，由 VIX 专用版泛化而来，专用版已删） |

**案例 2：GSPC/NDX 指数接入（2026-08-14）**

需求来源是 Finviz 的 ES/NQ **期货**页面。因 EODHD 订阅包不含期货，按"期货 → 标的指数"映射用 `GSPC.INDX`（S&P 500）/ `NDX.INDX`（Nasdaq 100）替代；注册名与既有 ETF 代理（SPY/QQQ）区分（"S&P 500 Index" / "Nasdaq 100 Index"）。各回填 3 年 752 个交易日，参数化集成测试 25/25 通过。后续同类 Finviz 期货需求（如 YM→DJI.INDX、RTY→RUT.INDX）同此映射。

**案例 3：N225/KS11/GDAXI 接入 + FTSE 排查（2026-08-14）**

- 猜测的 `NIKKEI.INDX`/`KOSPI.INDX`/`DAX.INDX` 全部失败；查官方清单确认真实代码：`N225.INDX`（Nikkei 225）、`KS11.INDX`（KOSPI，与 Yahoo `^KS11` 同码）、`GDAXI.INDX`（DAX）。各回填 3 年，集成测试 36/36 通过。
- **FTSE 100 排查记录**：`FTSE.INDX` 实时全 NA、EOD 空返回——官网 Fundamentals 列表里有它，但 EOD 行情不覆盖；Exchanges API 的 INDX 全量清单（1668 个）里 UK 只有 Cboe UK 系列（`BUK*`），无任何 FTSE 品牌指数。**已验证可用的备选**（用户暂缓接入）：`ISF.LSE`（iShares Core FTSE 100 UCITS ETF，精确跟踪，GBP 计价）、`BUK100P.INDX`（Cboe UK 100，成分同 100 家公司，EUR 计价）。

**案例 4：A 股四大指数接入（2026-08-14）**

需求来源是中金所股指期货代码（IH/IF/IC/IM），按"期货→标的指数"映射接入：`SSE50.INDX`（上证50/IH）、`CSI300.INDX`（沪深300/IF）、`CSI500.INDX`（中证500/IC）、`CSI1000.INDX`（中证1000/IM）。官方 INDX 清单里中国指数是语义化代码（`SSE*`/`CSI*`/`SSEC` 等），非国内的六位数字代码。各回填 3 年（727~733 个交易日），集成测试 47/47 通过。新开 category `china_equity`。

**⚠️ A 股指数实时接口的时段行为**：非交易时段 OHLC/涨跌幅全 NA、仅 `previousClose` 有值（日/美指数收盘后仍保留当日值）。现有 `parse_realtime_data` 的校验是 close **或** previous_close 至少一个非空，快照可正常入库。调度三次窗口（北京时间 7/14/19）中只有 14:00 在 A 股盘中、实时字段完整；EOD 日线不受影响。消费方对 A 股指数应以 EOD 为准。

**案例 5：大宗商品全家桶接入 + 商品符号陷阱修复（2026-08-14）**

需求：金/WTI/Brent/银/铜/天然气六大商品。**先查官方文档再选路线**（用户要求）：

1. **官方 Commodities API（FRED 源）判定为不兼容**：`/api/commodities/historical/{CODE}` 是 beta 接口，返回 `{meta, data:[{date, value}]}` 单值格式——无 OHLC/成交量、无实时接口、金银系列已被 FRED 停更（LBMA 授权）、能源延迟约 1 周（EIA 周二批量发布）、铜只有月频。与 `market_data_eod` 的 OHLCV + realtime 管线结构不兼容；
2. **期货连续合约不可用**：`.COMM` 系订阅不含（VX1/CL1 实测 404，同 VIX 接入时结论）；
3. **可用路线实测**：商品现货外汇对覆盖不全——`XBRUSD.FOREX`（Brent）✅ 可用，但 `XTIUSD`（WTI）/`XCUUSD`（铜）/`XNGUSD`（天然气）全 NA；商品 ETF 全线可用（GLD/SLV/CPER/USO/BNO/UNG）；
4. **发现并修复既有错误注册**：`"Copper"→HG.US` 实为 Hamilton 保险股、`"Natural Gas"→NG.US` 实为 NovaGold 金矿股（期货符号加 `.US` 被解析成同名美股），已错采一年多。用户确认后删除错误数据（eod 552+747 条、realtime 4 条）；
5. **用户选型：全部用商品 ETF**（弃用金银现货对 XAU/XAG）。最终注册：金 `GLD.US`、银 `SLV.US`、铜 `CPER.US`、WTI `USO.US`、Brent `BNO.US`、天然气 `UNG.US`，各回填 3 年 752 交易日（共 4512 条）；
6. 集成测试 **69/69 通过**（全链路 6 商品并发 0 失败，SPY/VIX 等既有资产无回归）。

**选型原则（商品类资产通用）**：能拿到现货外汇对就用现货对（最准），拿不到用商品 ETF（短期涨跌同步，长期有费率/滚动损耗）；期货 `.COMM` 需独立订阅包。**ETF 显示名统一加 "(XXX ETF)" 后缀**，与指数的 "…Index" 后缀惯例一致。备选现货对备忘：Brent=`XBRUSD.FOREX`（已验证可用，用户选 ETF 方案后未启用）。

**案例 6：美债期限补全 2Y/30Y（2026-08-15）**

官方渠道确认（用户要求先查官方文档）：国债收益率有**两套体系**——Economic Data API（`economic.eodhd.com`，GBOND 交易所，240 个符号、美债 12 个期限：1M/3M/6M/1Y/2Y/3Y/5Y/7Y/10Y/20Y/30Y）和标准行情接口（real-time + eod）。我们管线用标准行情接口，`.INDX` 与 `.GBOND` 后缀实测**同源同值**（US10Y 两后缀逐日对照一致）。

**⚠️ 混合后缀陷阱**：`US20Y.INDX` 不可用（realtime 全 NA + eod 404）但 `US20Y.GBOND` 全链路可用——同一资产族不同期限可能分属不同后缀，**每个期限都要单独实测**，不能从其他期限的后缀类推。20Y 已接入后因用户不需要又移除（代码+数据已删，`.GBOND` 后缀路线已验证可用，将来要恢复一条命令即可）。最终注册：`US2Y.INDX`、`US30Y.INDX`（category 复用 `us_treasury`），各回填 3 年（832/753 交易日），集成测试 36/36 通过（含 20Y 时 bond 全族并发 0 失败）。收益率曲线现覆盖 3M/1Y/2Y/5Y/10Y/30Y 六个期限。

**案例 7：四大外汇对历史统一补齐（2026-08-15）**

需求四对货币（USD/JPY、人民币 USDCNH、GBP/USD、EUR/USD）**全部已在 `FOREX_MAP` 注册**（系统上线时即有），官方 FOREX 货币清单（1100+ 对）确认全覆盖。本案例属于**零代码改动的历史补齐**：直接用通用回填脚本统一到 3 年（836/1006/1016/1023 交易日，共 3881 条），集成测试 47/47 通过（forex 全族 4 对并发 0 失败、既有资产无回归）。回填时 GBPUSD/USDJPY 首次遇 requests 瞬时 SSL EOF 错误（curl 同 URL 正常），重试即恢复——大回填失败先重试再排查。官方 FOREX 清单另含 `USDCNY.FOREX`（在岸人民币）与 `XAUUSD`/`XAGUSD`（金银现货，商品案例 5 中弃用）同列，说明 `.FOREX` 后缀是外汇+现货对的混合清单，选型时注意区分。

**候选后续资产**（同一流程可直接接入）：`VIX3M.INDX`（VIX 3月期，期限结构信号）、`VVIX.INDX`（波动率的波动率）——两者实时接口已验证可用；`SSEC.INDX`（上证综指）、`SZI.INDX`（深证成指）、`CNT.INDX`（创业板指）——已确认在官方清单中。

---

## 移除 / 替换资产（逆向操作）

接入之外，两类常见维护操作（案例 5 替换 HG/NG、案例 6 移除 20Y 的经验沉淀）：

### 移除资产

1. MAP 里删掉对应行（唯一代码改动）；
2. 清数据（需用户确认后执行）：
   ```sql
   DELETE FROM market_data_eod WHERE code = '<CODE>';
   DELETE FROM market_data_realtime WHERE code = '<CODE>';
   ```
3. 注册层复跑集成测试确认干净：`python backend/tests/test_market_data_asset_integration.py --codes <同MAP剩余的code> --asset-type <类型> --skip-integration`。

### 替换资产（换代码，如 HG.US → CPER.US）

1. 先按六步流程把新代码接好（注册 + 回填 + 测试全绿）；
2. 再按"移除资产"清掉旧代码的注册与数据；
3. **先 grep 全库引用**：`grep -rn "<旧code>" backend/ web-frontend/src/ scripts/`——日报/周报按 `asset_type` 泛化查询通常无硬编码引用，但必须确认后才能删。

### 孤儿数据识别

不在任何 MAP 注册表里的 code 会停止增量更新，残留在表中。识别方法：

```sql
-- asset_type 为空或数据停更的 code，对照 MAP 注册表人工核对
SELECT code, asset_type, COUNT(*), MIN(date), MAX(date)
FROM market_data_eod GROUP BY code, asset_type
HAVING MAX(date) < CURRENT_DATE - INTERVAL '14 days';
```

已知孤儿：`RSP.US`（612 条，2026-06 停更，asset_type 为空）——消费方按 asset_type 过滤不受影响，留存待清理。
