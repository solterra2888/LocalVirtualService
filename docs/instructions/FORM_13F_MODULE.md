# Form 13F 模块（SEC 季度持仓）开发说明

> **创建**：2026-09-02
> **状态**：V1 已完成开发并实测通过（4 个季度、908 万行持仓已入库）
> **相关文档**：`SCHEDULER_DEVELOPMENT_GUIDE.md`（调度器）、`CENTRAL_BANK_DASHBOARD_MODULE.md`（架构蓝本）
> **原型验证**：`参考数据/form13f_dashboard/`（静态 HTML 原型，本模块的 UI/解析规则来源）

---

## 1. 模块概述

从 [SEC Form 13F Data Sets](https://www.sec.gov/data-research/sec-markets-data/form-13f-data-sets) 下载季度数据集 ZIP（~80MB/季），流式解析 TSV 导入**全量持仓**（~230 万行/季），按内置 GICS 映射 + ETF 关键词规则打行业标签，预计算每机构×季度快照（行业分布 / top20 持仓 / top5 买卖），前端 `/form13f` 页面展示。

**数据形态**（与 FOMC 模块的差异：纯结构化数值，无 LLM 环节）：

| 数据 | 来源 | 层 |
|---|---|---|
| 数据集状态机 | SEC 13F 数据页轮询发现 ZIP | `form13f_datasets` |
| 机构维表 | COVERPAGE.tsv 的 FILINGMANAGER_NAME | `form13f_filers` |
| 持仓明细（全量） | INFOTABLE.tsv 按 (filer,quarter,cusip,putcall) 聚合 | `form13f_holdings` |
| 机构快照 | 导入后 SQL 预计算 | `form13f_snapshots` |

**关键背景**（为什么这样设计）：

- SEC 每季度发布一次数据集（季末后约 1.5 个月），**工作日每日轮询一次即可**，无需像 FOMC 那样 30 分钟
- SEC 数据**不含行业信息**——行业分类靠内置 CUSIP→GICS 映射表（~1,400 只重仓股，按市值覆盖 13F 全市场 ~98%）+ ETF issuer/title 关键词识别（JPM/SCHW/BLK 等运营公司白名单排除），未匹配归 "Other"
- 仪表盘需要全组合行业分布（top50 覆盖率对分散组合只有 60%），所以**持仓必须全量入库**而非只存 top N
- top5 买卖依赖相邻季度对比，导入时一次算好存 JSONB，查询侧免 join 免排序

---

## 2. 数据链路

```
SEC 13F 数据页 每工作日 UTC 21:00 轮询
  → poll_sec_datasets()    发现新季度 ZIP → form13f_datasets(pending)
  → download_dataset()     流式下载 + sha256 → form13f_datasets(downloaded)
  → import_dataset()       COVERPAGE 定主报告期 + INFOTABLE 流式聚合 → form13f_holdings
  → compute_snapshots()    SQL 预计算行业分布/top20/top5 → form13f_snapshots
```

### SEC 数据集解析规则（已用多组样本验证，**改动前必读**）

规则移植自原型 `build_data.py`，与 SEC 原始数据核对一致：

1. **主报告期提取**：每个 ZIP 的 COVERPAGE.tsv 里 `REPORTCALENDARORQUARTER` 取**众数**——其余行是往年季度的补报申报，不属于本数据集
2. **机构多申报取舍**：同一 FILINGMANAGER_NAME 同期多份申报（amendment/重复），取 `ISAMENDMENT=Y` 优先 → `AMENDMENTNO` 最大 → 最后出现
3. **聚合粒度**：`(filer, quarter, cusip, putcall)`——SEC 原始行有拆腿（OTHERMANAGER），聚合后每季 ~230 万行（原始 ~320 万行）
4. **quarter 标签**：COVERPAGE `REPORTCALENDARORQUARTER` 的**日历季**，`31-MAR-2026` → `2026Q1`（`quarter_of()`）。不要把 SEC 页面申报窗口名（如 "2026 March April May 13F"）当成 Q2——那是 Mar–May 申报窗口，对应截至 3/31 的持仓 = Q1。真正的 2026Q2 应对应 `01jun2026-31aug2026_form13f.zip` / "2026 June July August 13F"
5. **ZIP 文件名（2024 年换过）**：页面展示名 2024 年起改为月份短语，href 则两套——2024+ 为 `01mar2026-31may2026_form13f.zip`，2013–2023 为 `2023q4_form13f.zip`。日常 poll 只自动入库窗口文件名；`backfill` 才发现 legacy。解析用 `*_form13f.zip` 兜底
6. **filer_key**：`sha1(name.strip())[:12]`（`filer_key_of()`），与原型一致
7. **PUTCALL 原值**：SEC 用全词 `'Call'`/`'Put'`（非单字母），列宽 VARCHAR(6)
8. **sector 最长类别名 31 字符**（`Investment Grade Corporate Bond`），列宽 VARCHAR(40)

### 快照计算的性能要点

- **行业分布**：SQL `GROUP BY filer_id, sector`，勿在 Python 逐机构循环
- **top20**：SQL 窗口函数 `ROW_NUMBER() OVER (PARTITION BY filer_id ORDER BY value_usd DESC)`
- **top5 买卖**：SQL 端 FULL JOIN 两季持仓 + 窗口函数，只回传 ~4 万行。两个已踩过的坑：
  - `PARTITION BY` 必须用 `COALESCE(c.filer_id, p.filer_id)`——exited（上期有本期清仓）的 c 侧为 NULL，直接 `PARTITION BY c.filer_id` 会把清仓行归到 NULL 分区**全部丢失**
  - `prev_value` 必须并入主 SQL 输出——逐行回查上期市值是 4 万次往返（N+1），单季快照会从 ~100s 卡到 13 分钟
- 实测单季快照 ~100 秒（旧版拉全量到本地对比需 ~11 分钟）

---

## 3. 数据库

迁移：`migrations/create_form13f_tables.sql` + `migrations/add_form13f_scheduler_task_type.sql`（CHECK 约束加 `form13f` + 默认调度行 + 20 家预设机构）。`db_manager.py::_create_form13f_tables` 在 `ensure_schema` 时执行。

| 表 | 职责 | 幂等 key |
|---|---|---|
| `form13f_datasets` | 数据集状态机（pending→downloading→downloaded→importing→imported/failed） | `dataset_name` |
| `form13f_filers` | 机构维表（filer_key/name_normalized/latest 汇总/is_preset） | `filer_key` |
| `form13f_holdings` | 全量持仓明细（含 sector 标签、accession 溯源） | `(filer_id, quarter, cusip, putcall)` |
| `form13f_snapshots` | 仪表盘主数据源（sector_alloc/top20/top5 JSONB） | `(filer_id, quarter)` |
| `form13f_presets` | 预设机构（Bridgewater 等 20 家） | `label` |

**拆表 rationale**：快照服务当前页面（免 join）；明细全量保留支撑未来「按股票反查机构」（`(quarter, cusip)` 索引已建）与重算快照（改 TopN 策略时 `ON CONFLICT DO UPDATE` 幂等覆盖）。

**已入库规模**（2026-09-02）：4 季 × holdings 908 万行 / snapshots 33,060 条 / filers 11,550 家。

---

## 4. 文件清单

### 新增（8 个）

| 文件 | 职责 |
|---|---|
| `backend/services/form13f_service.py` | 单类 Form13fService：poll/download/import/snapshots + 查询侧（manifest/search/detail） |
| `backend/tasks/form13f_tasks.py` | 6 个 Celery 任务（全 `data_fetching`，import 带 `time_limit=7200`） |
| `backend/api/form13f.py` | 4 个 endpoint（`/api/form13f/*`，JWT 保护） |
| `backend/utils/form13f_sector_map.py` | CUSIP→GICS 映射（~1,400 条）+ `is_fund()`/`classify_etf()` 分类器 |
| `backend/database/migrations/create_form13f_tables.sql` | 五张表 DDL |
| `backend/database/migrations/add_form13f_scheduler_task_type.sql` | 调度类型 + 预设机构 |
| `web-frontend/src/services/form13f.ts` | form13fAPI + TS 类型 |
| `web-frontend/src/pages/Form13FPage.tsx` | 仪表盘页面（antd + recharts 100% 堆叠柱） |

### 修改（集成点）

| 文件 | 改动 |
|---|---|
| `backend/database/db_manager.py` | ensure_schema 加 `_create_form13f_tables`；task_type 列表加 `form13f` |
| `backend/celery_app.py` | 显式导入 `form13f_tasks`（worker 必须注册，否则 `NotRegistered`） |
| `backend/celery_config.py` | task_routes 加 6 个任务 → data_fetching |
| `backend/main.py` | 注册 router（prefix `/api/form13f`） |
| `backend/services/scheduler_service.py` | task_handlers 加 `form13f`；执行器按 `params.mode` 分发 poll/retry |
| `backend/api/scheduler.py` | 两处 task_type 正则加 `form13f` |
| `web-frontend/src/App.tsx` | 路由 `/form13f` |
| `web-frontend/src/components/Layout.tsx` | 侧边栏菜单 "13F Filings"（FundViewOutlined）+ 高亮归一化 |
| `web-frontend/src/components/DataSourceConfigModal.tsx` | 4 处注册（类型联合/configs/renderParamFields/handleSubmit） |
| `web-frontend/src/pages/SchedulerPage.tsx` | 数据源卡片 + 6 处类型/条件注册 |

---

## 5. API

全部 `Depends(get_current_user)`：

| Endpoint | 说明 |
|---|---|
| `GET /api/form13f/manifest?top_n=200` | 季度列表 + 20 预设机构 + 规模 TopN 摘要（**不**全量下发 1.1 万家） |
| `GET /api/form13f/filers?q=&page=&page_size=` | 机构搜索（name_normalized ILIKE，按规模降序分页） |
| `GET /api/form13f/filers/{filer_id}` | 机构全季度快照（quarter → sector_alloc/top20/top5） |
| `POST /api/form13f/refresh` | task_map：`poll` / `import`(dataset_id) / `reaggregate`(dataset_id,quarter) / `backfill` / `retry` |

**快照 JSONB 结构**：

```jsonc
// sector_alloc: {"Technology": 30.29, ...}  百分比，合计≈100
// top20_holdings: [{issuer, title, cusip, putcall, value, shares, pct}]
// top5_buys/sells: [{issuer, cusip, putcall, dv, kind, value, spct}]
//   kind: increase(增持) / new(新建仓) / decrease(减持) / exited(清仓)
//   dv: 市值变动（美元），spct: 相对上期市值的变动率
```

首季度（数据集中最早一期）无对比基准，buys/sells 为空数组——前端显示"无数据"是预期行为。

---

## 6. SEC 下载合规

- **User-Agent 必须是 `公司名 邮箱` 格式**（SEC fair-access 政策），不能用平台其他服务的浏览器 UA。常量 `SEC_USER_AGENT`（service 顶部，支持 `SEC_USER_AGENT` 环境变量覆盖）
- 速率 ≤10 req/s——本模块每工作日一次页面轮询 + 每季一次 80MB 下载，远低于限
- ZIP 下载后 sha256 入库 + `zipfile.testzip()` 校验；BadZipFile → status=failed 留 error_message
- poll 解析不到 ZIP 链接时**抛异常**（防 SEC 改版后静默空跑），由调度日志告警
- ZIP 存 `backend/data/form13f/`（不入 git）

---

## 7. 行业分类（sector_map）

三类规则（`backend/utils/form13f_sector_map.py`）：

1. **ETF/基金识别** `is_fund(issuer, title)`：issuer 前缀（SPDR/ISHARES/VANGUARD…）+ 正则关键词（FDS/TRUST/ETF…），带 REIT/银行**运营公司白名单排除**（JPM/SCHW/STT/BLK/MORGAN STANLEY 等同名前缀的股票）
2. **ETF 细分** `classify_etf()`：文本关键词 → 9 类资产（Precious Metals/Government Bond/High-Yield/IG Corp Bond/Broad EM/Broad Developed World/Large Cap Blend 等，参考 MacroMicro 图例）
3. **股票 GICS** `CUSIP_SECTOR`：~1,400 只重仓股 CUSIP → 11 板块；未匹配 → `Other`

**补充映射**：preset 机构出现新未覆盖重仓股时（Other 占比异常升高），往 `CUSIP_SECTOR` 加一行后重跑：

```python
svc = Form13fService()
svc.compute_snapshots(dataset_id)   # ON CONFLICT 幂等覆盖
# 注意：只改快照不够——holdings.sector 是导入时写入的，需先重导该季：
# svc.import_dataset(dataset_id, local_dir=...)（或等 SEC 重发时自然刷新）
```

---

## 8. 常见问题（开发中实际踩过的坑）

| 坑 | 症状 | 修复 |
|---|---|---|
| `fetch_one` 返回裸 tuple | `TypeError: tuple indices must be integers` | 平台惯例 `dict(zip(cols, rows[0]))`（见 `_get_dataset`） |
| `psycopg2.extras` 未导入 | `AttributeError: module 'psycopg2 has no attribute extras'` | 文件头显式 `import psycopg2.extras`（earnings_calendar 是函数内局部导入） |
| SUM() 返回 Decimal | `TypeError: Object of type Decimal is not JSON serializable` | 组装 JSONB 前 `int()` |
| sector 列 VARCHAR(30) 溢出 | `StringDataRightTruncation` | 最长类别名 31 字符，列宽 40 |
| putcall 列 VARCHAR(1) 溢出 | 同上 | SEC 原值 `Call`/`Put` 全词，列宽 6 |
| 长任务连接静默断开 | 进程 CPU 0%、DB 无活跃查询、卡 30+ 分钟 | 远端 DB 走公网，TCP 可能被中间设备掐断；重跑即可（幂等）；Celery 任务有 `max_retries=1` |
| 长脚本运行中编辑源码 | 后台进程行为与当前文件不符（曾导致快照写入空数组） | Python 进程持有启动时的模块快照；**跑长任务时勿中途改它 import 的模块**，改完重跑验证 |

---

## 9. 运维

```bash
# 手动触发（需 JWT，或在服务器上用 celery call）
curl -X POST /api/form13f/refresh -d '{"type": "poll"}'          # 发现新季度
curl -X POST /api/form13f/refresh -d '{"type": "backfill"}'      # 回填全部历史
curl -X POST /api/form13f/refresh -d '{"type": "reaggregate", "dataset_id": 4}'

# 本地已有 TSV 目录时的离线导入（调试/补数，跳过下载环节）
python3 -c "
import sys; sys.path.insert(0, '.')
from backend.services.form13f_service import Form13fService
svc = Form13fService()
svc.import_dataset(1, local_dir='/path/to/01JUN2025-31AUG2025_form13f')"

# 数据质量检查：Other 占比（正常 5-10%，映射表缺漏时会升高）
psql -c "SELECT quarter, AVG((sector_alloc->>'Other')::numeric)::numeric(5,2) AS avg_other_pct
         FROM form13f_snapshots GROUP BY quarter;"
```

**调度**：`Form 13F Quarterly Poll`（cron `0 21 * * 1-5` UTC，mode=poll）。poll 自带补推逻辑：pending/failed → 下载、downloaded 未导入 → 导入、imported 无快照 → 聚合，worker 崩溃后下一窗口自愈。

**V1.1 候选**（未做）：SEC 重发修正版 ZIP 时自动比对 `zip_sha256` 触发重导；「按股票反查机构」查询页。
