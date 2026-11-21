# Form 13F 模块（SEC 季度持仓）开发说明

> **创建**：2026-09-02
> **更新**：2026-10-22 — 前端样式接入全站 UI 规范（查询控件 + 标签胶囊）；09-16 — 13F-NT 跟随 successor CIK；前端不再展示 top5 买卖
> **相关文档**：`SCHEDULER_DEVELOPMENT_GUIDE.md`（调度器）、`UI_STYLE_SPEC.md`（UI 样式规范）

---

## 1. 模块概述

用 [edgartools](https://github.com/dgunning/edgartools) 按 **20 家关注机构的冻结 CIK** 从 EDGAR 拉取 `13F-HR` / `13F-HR/A`，折叠 amendment 后写入该机构全部持仓，按内置 GICS 映射 + ETF 关键词打行业标签，预计算机构×季度快照（行业分布 / top20），前端 `/form13f` 展示。

**数据形态**（纯结构化数值，无 LLM）：

| 数据 | 来源 | 层 |
|---|---|---|
| 季度批次 | 同步时按 `edgar_{quarter}` 记账 | `form13f_datasets` |
| 机构维表 | `form13f_presets.cik` upsert | `form13f_filers` |
| 持仓明细 | 13F infotable 按 (filer,quarter,cusip,putcall) | `form13f_holdings` |
| 机构快照 | 导入后 SQL 预计算 | `form13f_snapshots` |
| 关注名单 | 20 家 label + 冻结 CIK | `form13f_presets` |

**为什么不再全量下 ZIP**：

- 产品只展示这 20 家，全市场每季 ~80MB / ~230 万行对仪表盘没有收益
- 单家 13F-HR 在申报当天就出现在 EDGAR（季末后 45 天内），不必等 SEC Data Sets 打包
- 身份用 CIK，避免申报名大小写 / `13F NOTICE` 盖掉 holdings 的漏数
- `value_usd` 存 **美元**（edgartools `holdings` 已折算），与页面 `Value ($)` 一致

默认回填起点：`FORM13F_SINCE_QUARTER=2024Q1`。

---

## 2. 数据链路

```
每工作日 UTC 21:00  form13f_poll
  → sync_preset_filings()
       对 20 个 CIK：Company(cik).get_filings(form="13F-HR", amendments=True)
       按 report_period 分组 → fold_period_filings()
       写入 form13f_holdings（accession 未变则跳过）
  → compute_snapshots(quarter)   受影响季度
```

### 申报取舍（改动前必读）

1. **身份**：`form13f_presets.cik`（10 位）。`filer_key = cik`。禁止再用 `FILINGMANAGER_NAME` 做主键。
2. **默认拉 13F-HR / 13F-HR/A**。`13F-NT` 无 infotable，不入库。若某季只有 NT，则读取 cover page 的 `otherManager.cik`，改拉该主体同季 13F-HR（Pershing 2026Q2：`0001336528` NT → `0002026053` Pershing Square Inc.）。Combination Report 的 EDGAR 类型仍是 `13F-HR`。历史有 infotable 的季度不覆盖，避免 Holdco 早期单票申报盖掉原主体组合。
3. **同一 CIK + 同一报告期多份申报**，按申报日 / 是否修订 / amendment 号排序后折叠：
   - 原文、`RESTATEMENT`、未标明类型的 `/A`：**整表替换**
   - `NEW HOLDINGS`：按 `(cusip, putcall)` **并入**（保密仓补披露；不能当成整季持仓）
4. **quarter 标签**：`period_of_report` 的日历季，`2026-03-31` → `2026Q1`（`quarter_of()`）
5. **聚合粒度**：edgartools `.holdings` 已按 CUSIP+PutCall 聚合多 manager 拆腿
6. **PUTCALL**：`''` / `'Call'` / `'Put'`
7. **幂等**：同一 filer×quarter 的 accession 未变则跳过；变了则 DELETE + INSERT（RESTATEMENT 可能删掉旧 CUSIP）

### 快照

- 行业分布：SQL `GROUP BY filer_id, sector`
- top20：窗口函数按 `value_usd DESC`
- 前端不再展示 top5 买卖（口径曾多次调整，页面只保留行业分布与 Top 20）
- 前端 Δ Shares 用全量持仓的 `prev_shares`，不是上季 top20

---

## 3. 数据库

迁移：`create_form13f_tables.sql` + `add_form13f_scheduler_task_type.sql` + `add_form13f_preset_cik.sql`。`db_manager.py::_create_form13f_tables` 在 `ensure_schema` 时执行。

| 表 | 职责 | 幂等 key |
|---|---|---|
| `form13f_datasets` | 季度批次 `edgar_{quarter}` | `dataset_name` |
| `form13f_filers` | 机构维表（CIK / latest 汇总 / is_preset） | `filer_key`（= CIK） |
| `form13f_holdings` | 持仓明细（sector、accession） | `(filer_id, quarter, cusip, putcall)` |
| `form13f_snapshots` | 仪表盘 JSONB | `(filer_id, quarter)` |
| `form13f_presets` | 关注机构（Bridgewater 等） | `label`；CIK 唯一 |

**20 家 CIK**（与 `add_form13f_preset_cik.sql` 一致）：

| 展示名 | CIK |
|---|---|
| Bridgewater Associates | 0001350694 |
| Berkshire Hathaway Inc | 0001067983 |
| ARK Investment Management | 0001697748 |
| Oaktree Capital Management | 0000949509 |
| Appaloosa LP | 0001656456 |
| Pershing Square Capital Management | 0001336528 |
| Baupost Group | 0001061768 |
| Temasek Holdings | 0001021944 |
| Hillhouse Capital (HHLR) | 0001762304 |
| Tiger Global Management | 0001167483 |
| Duquesne Family Office | 0001536411 |
| Fisher Asset Management | 0000850529 |
| Soros Fund Management | 0001029160 |
| Scion Asset Management | 0001649339 |
| Baillie Gifford | 0001088875 |
| Lone Pine Capital | 0001061165 |
| Third Point LLC | 0001040273 |
| Miller Value Partners | 0001135778 |
| Coatue Management | 0001135730 |
| Icahn Carl C | 0000921669 |

---

## 4. 文件清单

| 文件 | 职责 |
|---|---|
| `backend/services/form13f_service.py` | CIK 同步、amendment 折叠、快照、查询 |
| `backend/tasks/form13f_tasks.py` | Celery：`form13f_poll` / `form13f_aggregate`（其余名为兼容别名） |
| `backend/api/form13f.py` | `/api/form13f/*` |
| `backend/api/agent_v1.py` | Agent 只读端点 `/api/v1/agent/form13f/*`（2026-11-21 起；`AgentService` 经 service_registry 复用本服务单例） |
| `backend/utils/form13f_sector_map.py` | CUSIP→GICS + ETF 分类 |
| `web-frontend/src/pages/Form13FPage.tsx` | 仪表盘 |
| `web-frontend/src/components/form13f/Form13FSectorChart.tsx` | 行业分布堆叠柱图（echarts） |

**前端样式**（2026-10-22 接入全站 UI 规范 `UI_STYLE_SPEC.md`）：机构切换用 `.app-ctl-pills` 可换行胶囊切换组（Segmented 放不下，该组件以本页为参考实现；原页面标题/副标题与顶栏机构搜索 Select/刷新按钮已按需求移除，机构只能通过预设胶囊切换）；持仓行内 New/Put/Call 与股份类别（CL A 等）标签走 `shared/aiTagChipStyles`（New=`signal-neutral`、Put=`signal-bearish`、Call=`signal-bullish`、类别=`signal-meta`）；行业分布柱图 bar 透明度 0.8（`Form13FSectorChart` 的 `BAR_OPACITY`）。注意 Δ Shares 涨跌色保持本模块红涨绿跌（`THEME_COLORS`），与标签色板互不影响。

---

## 5. API

全部 `Depends(get_current_user)`：

| Endpoint | 说明 |
|---|---|
| `GET /api/form13f/manifest` | 季度列表 + 20 预设机构 |
| `GET /api/form13f/filers?q=` | 仅搜索预设机构 |
| `GET /api/form13f/filers/{id}` | 全季度快照 |
| `POST /api/form13f/refresh` | `poll` / `reaggregate`（`quarter` 可选）；`backfill`/`retry`/`import` 等同 poll |

**Agent 只读端点**（JWT 或 PAT scope `form13f:read`，guanhe-mcp ≥ 0.6.0 的 `form13f_manifest` / `form13f_filer` 工具走这里）：

| Endpoint | 说明 |
|---|---|
| `GET /api/v1/agent/form13f/manifest` | 精简清单（quarters + presets，含 filer_id） |
| `GET /api/v1/agent/form13f/filers/{id}?quarter=` | 快照（响应结构与 web 端一致，额外含 `prev_shares` / top5 买卖；quarter 过滤在 `AgentService.form13f_filer_detail` 响应层做） |

存量 PAT 补 scope：`migrations/update_personal_tokens_add_form13f_scope.sql`。

快照 JSONB 仍含 `top5_buys` / `top5_sells`（后端继续写入），前端 `/form13f` 已不再展示（Agent 端点会返回）。

---

## 6. SEC / EDGAR 合规

- Identity：`SEC_USER_AGENT`（`名字 邮箱`），传给 `edgar.set_identity`
- 速率：edgartools 自带限速；20 家远低于 10 req/s
- 依赖：`edgartools>=5.40.0`（`amendment_type` 从该版本起可用）

---

## 7. 行业分类

三类规则见 `backend/utils/form13f_sector_map.py`（未改）：基金关键词 → ETF 9 类；股票 CUSIP → GICS 11；未匹配 `Other`。

补映射后重跑快照：

```python
svc = Form13fService()
svc.recompute_all_snapshots()
```

若 holdings.sector 已过时，对该 CIK 再跑一次 `sync_filer`（先改 accession 或手动 DELETE 该季持仓）。

---

## 8. 运维

```bash
# 手动触发
curl -X POST /api/form13f/refresh -d '{"type": "poll"}'
curl -X POST /api/form13f/refresh -d '{"type": "reaggregate", "quarter": "2026Q1"}'

# 进程内同步（调试）
python3 -c "
from backend.services.form13f_service import Form13fService
print(Form13fService().sync_preset_filings())
"

# Other 占比
psql -c "SELECT quarter, AVG((sector_alloc->>'Other')::numeric)::numeric(5,2) AS avg_other_pct
         FROM form13f_snapshots GROUP BY quarter;"
```

**调度**：`Form 13F Quarterly Poll`（cron `0 21 * * 1-5` UTC，mode=poll）。

清空测试数据（保留 presets）：

```sql
TRUNCATE form13f_holdings, form13f_snapshots, form13f_filers, form13f_datasets
  RESTART IDENTITY CASCADE;
```
