# Agent API v0.2 优化说明

本文档记录 `guanhe-mcp` 与 `Agent HTTP API` 在 v0.2.0 中的全面优化内容，包含变更动机、架构说明、破坏性变更清单、数据库迁移记录和完整的测试指南。

---

## 目录

1. [背景与动机](#1-背景与动机)
2. [整体架构变化](#2-整体架构变化)
3. [破坏性变更（v0.1 → v0.2）](#3-破坏性变更v01--v02)
4. [新增功能详解](#4-新增功能详解)
   - [4.1 daily_feed 合并搜索](#41-daily_feed-合并搜索)
   - [4.2 新增过滤参数](#42-新增过滤参数)
   - [4.3 服务端去重](#43-服务端去重)
   - [4.4 批量获取 daily_items_batch](#44-批量获取-daily_items_batch)
   - [4.5 多路并行搜索 daily_multi_search](#45-多路并行搜索-daily_multi_search)
   - [4.6 自省工具 list_subscriptions / list_sources](#46-自省工具-list_subscriptions--list_sources)
   - [4.7 零结果诊断 diagnostics](#47-零结果诊断-diagnostics)
   - [4.8 reports 端点合并](#48-reports-端点合并)
5. [数据库迁移记录](#5-数据库迁移记录)
6. [MCP 工具清单（v0.2）](#6-mcp-工具清单v02)
7. [文件变更清单](#7-文件变更清单)
8. [测试说明](#8-测试说明)
   - [8.1 后端 API 测试](#81-后端-api-测试)
   - [8.2 MCP 服务端 Smoke 测试](#82-mcp-服务端-smoke-测试)
   - [8.3 手动端对端测试](#83-手动端对端测试)
   - [8.4 数据库索引验证](#84-数据库索引验证)
9. [部署步骤](#9-部署步骤)

---

## 1. 背景与动机

v0.1 版本的 `guanhe-mcp` 在实际使用（Hermes Agent 多轮对话）中暴露出以下问题：

| 问题类别 | 具体表现 |
|---------|---------|
| **工具混淆** | `daily_feed` 和 `search` 两个工具分开定义，LLM 经常在两者之间错误选择 |
| **过滤能力弱** | 只支持单个 `ticker` 过滤，无法按 `tags`（概念标签）或 `author` 筛选 |
| **多轮低效** | 获取多个条目详情需要 N 次 `daily_item` 调用，token 消耗高 |
| **无自省能力** | LLM 不知道用户有哪些订阅频道、哪些数据来源，只能盲目搜索 |
| **零结果不透明** | 查询无结果时不提供任何诊断信息，LLM 无法判断是参数问题还是数据缺失 |
| **索引缺失** | `content`/`title` 字段的全文搜索走全表扫描，`concepts` JSONB 数组过滤无 GIN 索引 |
| **reports 端点冗余** | Twitter 和 YouTube 各有一套 reports 路由（共 6 个端点），语义重复 |

v0.2.0 允许破坏性变更，在彻底解决上述问题的同时，将工具数从 7 个精简至 9 个（功能增加但更聚焦）。

---

## 2. 整体架构变化

```
┌─────────────────────────────────────────────────────────────┐
│                     AI 客户端（Hermes / Cursor）               │
│  调用 MCP stdio 工具 (guanhe-mcp v0.2)                        │
└──────────────────────────┬──────────────────────────────────┘
                           │ HTTP (httpx)
                           ▼
┌─────────────────────────────────────────────────────────────┐
│           FastAPI  /api/v1/agent  (agent_v1.py)             │
│  - GET  /daily          ← daily_feed（含 q 全文搜索）          │
│  - POST /daily/batch    ← daily_items_batch（新）             │
│  - POST /daily/multi    ← daily_multi_search（新）            │
│  - GET  /daily/{id}     ← daily_item                        │
│  - GET  /subscriptions  ← list_subscriptions（新）           │
│  - GET  /sources        ← list_sources（新）                  │
│  - GET  /reports        ← report_list（合并）                 │
│  - GET  /reports/latest ← report_latest（合并）              │
│  - GET  /reports/{date} ← report_by_date（合并）             │
│  - 410  /search         ← 旧路径，升级提示                    │
│  - 410  /reports/twitter-daily/*  ← 旧路径，升级提示          │
│  - 410  /reports/youtube-daily/*  ← 旧路径，升级提示          │
└──────────────────────────┬──────────────────────────────────┘
                           │ psycopg2
                           ▼
┌─────────────────────────────────────────────────────────────┐
│            PostgreSQL  guanhe_database                      │
│  + pg_trgm 扩展（新）                                        │
│  + GIN trgm 索引  content / title（新）                      │
│  + GIN 索引       concepts JSONB（新）                       │
└─────────────────────────────────────────────────────────────┘
```

---

## 3. 破坏性变更（v0.1 → v0.2）

> 以下变更会导致旧版 MCP 客户端在不更新的情况下调用失败。

| 变更项 | v0.1 行为 | v0.2 行为 |
|--------|-----------|-----------|
| `GET /search` 端点 | 独立端点，接受 `q` 参数 | **已移除**，返回 `410 Gone`；请改用 `GET /daily?q=...` |
| `GET /reports/twitter-daily/*` | 独立 3 个端点 | **已移除**，返回 `410 Gone`；请改用 `GET /reports?kind=twitter` |
| `GET /reports/youtube-daily/*` | 独立 3 个端点 | **已移除**，返回 `410 Gone`；请改用 `GET /reports?kind=youtube` |
| `ticker` 参数（单值） | `GET /daily?ticker=NVDA` | 改为 `tickers`，支持逗号分隔多值：`tickers=NVDA,AAPL` |
| `since` 无效值 | 被忽略，返回默认结果 | 返回 `HTTP 400`，明确报错 |
| MCP 包版本 | `0.1.0` | `0.2.0`（需重新 `pip install`） |

旧路径返回的 `410` 响应体示例：

```json
{
  "detail": "This endpoint has been removed in v0.2. Use GET /api/v1/agent/daily?q=... instead."
}
```

---

## 4. 新增功能详解

### 4.1 daily_feed 合并搜索

`GET /api/v1/agent/daily` 现在同时承担时间线浏览和全文搜索两个职责：

- **不传 `q`**：按时间倒序返回订阅 feed + 上传文件的聚合时间轴
- **传 `q`**：在时间轴上做全文搜索（利用 GIN trgm 索引加速 `ILIKE`）

`q` 参数支持：
- 单词：`q=NVIDIA`
- 空格分隔多词（AND 语义）：`q=NVIDIA earnings`
- 减号排除：`q=AI -crypto`（排除含 crypto 的结果）

### 4.2 新增过滤参数

| 参数 | 类型 | 说明 |
|------|------|------|
| `tickers` | `string`（逗号分隔） | 按股票代码过滤，如 `NVDA.US,AAPL.US` |
| `tags` | `string`（逗号分隔） | 按概念标签过滤（匹配 `concepts` JSONB 数组） |
| `author` | `string` | 按作者名 / 账号模糊匹配（ILIKE） |
| `sources` | `string`（逗号分隔） | 按来源类型过滤（`twitter`/`youtube`/`news` 等） |

`since` 参数在 v0.2 中支持更丰富的格式：

| 格式 | 示例 | 含义 |
|------|------|------|
| 相对小时 | `48h` | 最近 48 小时 |
| 相对天数 | `7d` | 最近 7 天 |
| 相对周数 | `2w` | 最近 2 周 |
| ISO 8601 | `2026-05-01T00:00:00Z` | 精确时间点 |

### 4.3 服务端去重

`dedup=true`（默认）：按 `external_url` 在服务端去重，同一 URL 只保留最新一条。`NULL` URL 的条目不参与去重（保留全部）。

### 4.4 批量获取 daily_items_batch

`POST /api/v1/agent/daily/batch`

```json
{
  "ids": ["feed_123", "upload_456", "feed_789"],
  "include_content": true
}
```

- 单次请求最多 10 个 `id`
- `id` 前缀自动路由（`feed_` → `subscription_content`，`upload_` → `file_processing_details`）
- 避免 LLM 为获取多条详情发出 N 次串行请求

### 4.5 多路并行搜索 daily_multi_search

`POST /api/v1/agent/daily/multi`

```json
{
  "groups": [
    {"q": "NVIDIA earnings", "since": "7d"},
    {"tickers": "AAPL.US", "since": "7d"},
    {"tags": "AI", "sources": "twitter", "since": "3d"}
  ],
  "dedup_by": "url",
  "limit_per_group": 10
}
```

- 最多 5 个 `group`，每组独立执行查询
- `dedup_by=url`：跨组按 URL 全局去重
- `dedup_by=id`：跨组按 ID 全局去重
- 响应包含每组命中计数统计（`per_group_stats`）

### 4.6 自省工具 list_subscriptions / list_sources

`GET /api/v1/agent/subscriptions`：返回用户当前启用的所有订阅频道（channel_type + channel_identifier + display_name）。

`GET /api/v1/agent/sources`：返回全局预设的数据来源类型，以及过去 30 天各来源的内容计数。

这两个端点让 LLM 在搜索前先了解"有什么数据"，从而构造更精准的查询，避免无效搜索。

### 4.7 零结果诊断 diagnostics

当 `total == 0` 且 `q` 参数存在时，`GET /daily` 响应体中会附带 `diagnostics` 字段：

```json
{
  "items": [],
  "total": 0,
  "has_more": false,
  "diagnostics": {
    "q_terms_checked": ["NVIDIA", "earnings"],
    "terms_with_hits": ["NVIDIA"],
    "terms_without_hits": ["earnings"],
    "hint": "Term 'earnings' matched 0 documents. Try broader terms or extend the time range."
  }
}
```

LLM 可利用该字段判断：是某个词本身无数据（可放宽），还是组合条件过严（可拆分查询）。

### 4.8 reports 端点合并

6 个 twitter/youtube 专用端点合并为 3 个通用端点，用 `kind` 参数区分：

| 端点 | 说明 |
|------|------|
| `GET /api/v1/agent/reports?kind=twitter` | 报告列表 |
| `GET /api/v1/agent/reports/latest?kind=youtube` | 最新一份报告 |
| `GET /api/v1/agent/reports/{date}?kind=twitter` | 指定日期报告 |

`kind` 枚举值：`twitter`（默认）、`youtube`。

---

## 5. 数据库迁移记录

**迁移时间**：2026-05-18  
**执行方式**：Python 脚本（`scripts/run_agent_indexes.py`），`autocommit=True`，每条 `CREATE INDEX CONCURRENTLY` 独立执行，不阻塞生产写入。

**迁移脚本**：
- SQL 定义：`backend/database/migrations/add_agent_search_indexes.sql`
- 执行脚本（Bash）：`backend/database/migrations/run_agent_search_indexes.sh`
- 执行脚本（Python）：`scripts/run_agent_indexes.py`（实际使用）

**执行结果**：

| 索引名 | 表 | 类型 | 大小 | 状态 |
|--------|----|------|------|------|
| `idx_fpd_content_trgm` | `file_processing_details` | GIN trgm | 12 MB | valid（新建） |
| `idx_sc_content_trgm` | `subscription_content` | GIN trgm | 141 MB | valid（新建）|
| `idx_sc_title_trgm` | `subscription_content` | GIN trgm | 15 MB | valid（新建）|
| `idx_ft_concepts_gin` | `file_tags` | GIN jsonb | 56 kB | valid（新建）|
| `idx_ft_people_gin` | `file_tags` | GIN jsonb | — | 2026-05-19，`people` JSONB |
| `idx_ft_countries_gin` | `file_tags` | GIN jsonb | — | 2026-05-19，`countries` JSONB |
| `idx_ft_tag_schema_version` | `file_tags` | btree | — | 2026-05-19，`tag_schema_version` |
| `idx_sct_concepts_gin` | `subscription_content_tags` | GIN jsonb | 544 kB | 已存在，跳过 |
| `idx_sct_ticker_gin` | `subscription_content_tags` | GIN jsonb | 152 kB | 已存在，跳过 |
| `idx_ft_ticker_gin` | `file_tags` | GIN jsonb | — | 与 `idx_file_tags_ticker_gin` 重复，跳过 |

> **注意**：迁移已完成，**无需重新执行**。验证脚本：`scripts/verify_indexes.py`。

---

## 6. MCP 工具清单（v0.2）

| 工具名 | 对应端点 | 用途 |
|--------|---------|------|
| `daily_feed` | `GET /daily` | 时间轴浏览 + 全文搜索（合并自旧 `search`） |
| `daily_item` | `GET /daily/{id}` | 获取单条内容详情 |
| `daily_items_batch` | `POST /daily/batch` | 批量获取多条内容详情（最多 10 条） |
| `daily_multi_search` | `POST /daily/multi` | 多条件并行搜索 + 跨组去重 |
| `list_subscriptions` | `GET /subscriptions` | 查看用户已启用的订阅频道 |
| `list_sources` | `GET /sources` | 查看全局数据来源及 30 天计数 |
| `report_latest` | `GET /reports/latest` | 最新一份日报 |
| `report_by_date` | `GET /reports/{date}` | 指定日期日报 |
| `report_list` | `GET /reports` | 日报列表（可分页） |

---

## 7. 文件变更清单

| 文件 | 变更类型 | 说明 |
|------|---------|------|
| `backend/schemas/agent.py` | 扩展 | 新增 8 个 Schema 类；`AgentDailyFeedResponse` 增加 `diagnostics` 字段 |
| `backend/api/agent_v1.py` | 重写 | 新增 5 个端点，添加 3 个 `410 Gone` 路由，合并 reports |
| `backend/api/daily.py` | 扩展 | `_build_filter_clauses` 新增 `tags`、`author` 参数 |
| `backend/services/agent_service.py` | 重写 | `_parse_since` 增强，新增 5 个 service 方法，`diagnostics` 逻辑 |
| `tools/guanhe-mcp/guanhe_mcp/server.py` | 重写 | 9 工具定义，`_call_tool` 支持 POST + JSON body |
| `tools/guanhe-mcp/guanhe_mcp/__init__.py` | 版本 | `0.1.0` → `0.2.0` |
| `tools/guanhe-mcp/pyproject.toml` | 版本 | `0.1.0` → `0.2.0` |
| `tools/guanhe-mcp/setup.py` | 版本 | `0.1.0` → `0.2.0` |
| `tools/guanhe-mcp/README.md` | 重写 | 工具表、破坏性变更、示例全部更新 |
| `docs/agent/GUANHE_DAILY.md` | 重写 | 工具表 9 个，5 条搜索范式，新增 introspection / multi_search 用法 |
| `web-frontend/src/pages/AgentIntegrationPage.tsx` | 更新 | 端点展示表格更新至 v0.2 路径 |
| `backend/database/migrations/add_agent_search_indexes.sql` | 新增 | GIN 索引 DDL 定义 |
| `backend/database/migrations/run_agent_search_indexes.sh` | 新增 | Bash 执行脚本 |
| `scripts/run_agent_indexes.py` | 新增 | Python 执行脚本（实际用于生产迁移） |
| `scripts/verify_indexes.py` | 新增 | 迁移后验证脚本 |
| `backend/tests/test_agent_v1.py` | 新增 | FastAPI 层集成测试骨架 |
| `tools/guanhe-mcp/tests/test_server_smoke.py` | 新增 | MCP server smoke 测试 |

---

## 8. 测试说明

### 8.1 后端 API 测试

**文件**：`backend/tests/test_agent_v1.py`

**策略**：使用 `fastapi.testclient.TestClient` + `unittest.mock` monkeypatch `AgentService`，不依赖真实数据库连接，专注验证 HTTP 路由、参数解析、响应格式。

**环境准备**：

```bash
# 在项目根目录
pip install pytest httpx fastapi
```

**运行方式**：

```bash
# 全部测试
pytest backend/tests/test_agent_v1.py -v

# 只跑某个测试类
pytest backend/tests/test_agent_v1.py::TestParseSince -v

# 只跑某个测试方法
pytest backend/tests/test_agent_v1.py::TestDailyFeedEndpoint::test_tickers_multi_value -v
```

**测试覆盖的场景**：

| 测试类 | 覆盖点 |
|--------|-------|
| `TestDailyFeedEndpoint` | `tickers` 多值传递、`tags` 过滤、`dedup` 参数、`since` 各格式 |
| `TestParseSince` | `7d`/`48h`/`2w`/ISO 8601 合法值、非法值返回 400 |
| `TestDiagnosticsInResponse` | `total==0` 时响应体含 `diagnostics` 字段 |
| `TestMultiSearch` | 多 group 请求、跨组 URL 去重 |
| `TestGoneStubs` | `/search`、旧 reports 路径返回 `410` |
| `TestBatchEndpoint` | ids 超限（>10）返回 422、空列表校验 |

**预期输出示例**：

```
backend/tests/test_agent_v1.py::TestParseSince::test_valid_relative_days PASSED
backend/tests/test_agent_v1.py::TestParseSince::test_valid_relative_hours PASSED
backend/tests/test_agent_v1.py::TestGoneStubs::test_search_returns_410 PASSED
...
```

### 8.2 MCP 服务端 Smoke 测试

**文件**：`tools/guanhe-mcp/tests/test_server_smoke.py`

**策略**：patch `httpx.Client`，直接调用 `srv._call_tool()`，验证每个工具是否发出正确的 HTTP 方法、路径和请求体，不发起真实网络请求。

**环境准备**：

```bash
cd tools/guanhe-mcp
pip install -e .
pip install pytest
```

**运行方式**：

```bash
# 从 tools/guanhe-mcp 目录
pytest tests/test_server_smoke.py -v

# 从项目根目录运行（需设置 PYTHONPATH）
$env:PYTHONPATH = "tools/guanhe-mcp"   # PowerShell
pytest tools/guanhe-mcp/tests/test_server_smoke.py -v
```

**测试覆盖的场景**：

| 测试类 | 覆盖点 |
|--------|-------|
| `TestDailyFeed` | GET 请求路径、`q`/`since`/`tickers` 参数映射 |
| `TestDailyItem` | GET `/daily/{id}` 路径构造 |
| `TestDailyItemsBatch` | POST body 含 `ids` 数组 |
| `TestDailyMultiSearch` | POST body 含 `groups` 数组 |
| `TestListSubscriptions` | GET `/subscriptions` 路径 |
| `TestListSources` | GET `/sources` 路径 |
| `TestReportLatest` | GET `/reports/latest?kind=...` |
| `TestReportByDate` | GET `/reports/{date}?kind=...` |
| `TestReportList` | GET `/reports?kind=...` |
| `TestUnknownTool` | 未知工具名抛出异常 |
| `TestToolsList` | TOOLS 列表中所有工具名唯一、有 description |
| `TestInitialize` | 子进程握手，verify `serverInfo.version == "0.2.0"` |

### 8.3 手动端对端测试

部署后，通过直接 HTTP 请求验证各端点：

**1. 验证 daily_feed 基本请求**

```bash
curl -H "Authorization: Bearer <your_pat>" \
  "https://<server>/api/v1/agent/daily?since=7d&limit=5"
```

预期：返回 `items` 数组，`total >= 0`。

**2. 验证全文搜索 + diagnostics**

```bash
curl -H "Authorization: Bearer <your_pat>" \
  "https://<server>/api/v1/agent/daily?q=XYZNONEXISTENTTERM123&since=30d"
```

预期：`total == 0`，响应体含 `diagnostics.terms_without_hits`。

**3. 验证多值 tickers**

```bash
curl -H "Authorization: Bearer <your_pat>" \
  "https://<server>/api/v1/agent/daily?tickers=NVDA.US,AAPL.US&since=7d"
```

**4. 验证 410 Gone**

```bash
curl -H "Authorization: Bearer <your_pat>" \
  "https://<server>/api/v1/agent/search?q=test"
```

预期：HTTP 410，body 含 `"detail": "This endpoint has been removed"`。

**5. 验证批量获取**

```bash
curl -X POST -H "Authorization: Bearer <your_pat>" \
  -H "Content-Type: application/json" \
  -d '{"ids": ["feed_1", "feed_2"], "include_content": false}' \
  "https://<server>/api/v1/agent/daily/batch"
```

**6. 验证自省端点**

```bash
curl -H "Authorization: Bearer <your_pat>" \
  "https://<server>/api/v1/agent/subscriptions"

curl -H "Authorization: Bearer <your_pat>" \
  "https://<server>/api/v1/agent/sources"
```

### 8.4 数据库索引验证

迁移已完成，随时可用以下脚本重新验证：

```bash
python scripts/verify_indexes.py
```

预期输出末尾应为：

```
[ALL EXPECTED INDEXES PRESENT AND VALID]
```

如有索引显示 `[INVALID]`，说明 `CREATE INDEX CONCURRENTLY` 曾被中断，需手动清理：

```sql
-- 查找 INVALID 索引
SELECT indexrelname FROM pg_stat_user_indexes sui
JOIN pg_index pi ON pi.indexrelid = sui.indexrelid
WHERE NOT pi.indisvalid;

-- 删除后重建
DROP INDEX CONCURRENTLY idx_xxx;
-- 再次运行 scripts/run_agent_indexes.py 重建
```

---

## 9. 部署步骤

### 后端部署

```bash
# 1. 代码已在本地 git，推送到服务器
git push origin <your-branch>

# 2. 服务器上拉取并重启 FastAPI 服务
git pull
systemctl restart guanhe-backend   # 或 supervisor、pm2 等实际管理方式
```

> **数据库迁移已完成，无需再执行。**

### MCP 客户端（Hermes / Cursor）升级

```bash
# 在 Hermes 或本地 MCP 运行环境中
pip install --upgrade "path/to/tools/guanhe-mcp"
# 或 editable 模式（代码同步时无需重装）
pip install -e "path/to/tools/guanhe-mcp"

# 重启 MCP 宿主（完全退出 Cursor 再重开，或重启 Hermes 进程）
```

### 验证 MCP 版本

```bash
python -c "import guanhe_mcp; print(guanhe_mcp.__version__)"
# 应输出: 0.2.0
```
