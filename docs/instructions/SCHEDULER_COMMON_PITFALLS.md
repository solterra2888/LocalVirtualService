# Scheduler 开发常见错误与踩坑清单

> 本文档汇总在 GuanHe 平台新增或修改 **Scheduler 定时任务** 时容易遗漏的改动点，以及对应的真实故障现象与排查方法。  
> 完整开发流程见 [SCHEDULER_DEVELOPMENT_GUIDE.md](./SCHEDULER_DEVELOPMENT_GUIDE.md)。

---

## 为什么 Scheduler 特别容易「漏改一处就全挂」

Scheduler 功能横跨 **独立调度器服务、主 Web 应用、前端、PostgreSQL、Redis/Celery** 五层。新增一个 `task_type` 时，任意一层漏注册，表现各不相同：

| 漏改位置 | 典型现象 |
|----------|----------|
| `scheduler_service.task_handlers` | 打开 Scheduler 页面即 500；日志：`object has no attribute '_execute_xxx_task'` |
| `task_scheduling.valid_task_type` | 创建策略成功前失败；DB 插入违反 CHECK 约束 |
| `SchedulerPage` 配置白名单 | 点击齿轮提示 `Data source configuration feature coming soon` |
| `celery_app.py` 显式导入 | 调度器显示 success，但 Celery 任务 `FAILURE`；结果表无数据 |
| `celery_config.py` 队列路由 | 任务进错队列或无人消费 |
| 上游数据字段（如 `show_name`） | 调度与 Celery 均成功，但业务返回「当日无数据」占位文案 |

**原则：把新增 `task_type` 当成一次全栈 checklist，而不是只改 `scheduler_service.py`。**

---

## 全栈改动 Checklist（新增 task_type 必查）

复制此表到 PR 描述或开发笔记，逐项勾选。

### 后端 — 调度器服务（端口 8001）

- [ ] `backend/services/scheduler_service.py`
  - [ ] `self.task_handlers` 注册 `'your_task_type': self._execute_your_task`
  - [ ] 实现 **独立** 的 `async def _execute_your_task(...)` 方法（勿嵌套进其他 handler）
  - [ ] 若走 Celery：`.delay()` 后返回 `celery_task_id`；同步逻辑用 `run_in_executor` 避免阻塞事件循环
- [ ] `backend/api/scheduler.py`
  - [ ] `CreateCronTaskRequest` / 相关 Request 的 `task_type` 正则 pattern 包含新类型
  - [ ] （可选）专用快捷端点 `/tasks/your-feature` 与默认配置端点
- [ ] `backend/database/migrations/add_<task>_scheduler_task_type.sql`
  - [ ] `ALTER TABLE task_scheduling` 更新 `valid_task_type` CHECK（合并全部已有类型，勿只写新类型）
- [ ] `backend/database/db_manager.py`
  - [ ] `all_task_types` 列表加入新类型（`ensure_schema` 时自动合并约束）

### 后端 — Celery（若任务异步执行）

- [ ] `backend/tasks/your_feature_tasks.py` — 定义 `@app.task(name="your_task_name", queue="...")`
- [ ] `backend/celery_app.py` — **显式** `from backend.tasks import your_feature_tasks`（`autodiscover_tasks` 不足以保证 worker 注册）
- [ ] `backend/celery_config.py` — `task_routes` 将任务名路由到正确队列（如 `ai_processing`）
- [ ] 部署后 **重启对应 Celery worker**（如 `rc-celery-ai`）

### 前端 — Scheduler 管理页

- [ ] `web-frontend/src/pages/SchedulerPage.tsx`
  - [ ] `dataSources` 数组增加数据源卡片（`id` / `type` / `quickActions`）
  - [ ] `showQuickActionModal` 配置白名单包含新 `dataSource.id`（否则显示 coming soon）
  - [ ] `getRelatedTasks` / `syncDataSourceStatus` / `toggleDataSource` / Switch `Tooltip` 等 **所有** `sourceIdLower === '...' && taskType === '...'` 映射块
  - [ ] `DataSourceConfigModal` 的 `dataSourceType` 类型断言包含新类型
- [ ] `web-frontend/src/components/DataSourceConfigModal.tsx`
  - [ ] `dataSourceConfigs` 增加默认 Cron、名称、描述
  - [ ] `handleSubmit` 分支：调用 `createCronTask` 或专用 API
- [ ] `web-frontend/src/services/scheduler.ts` — `ScheduledTask.type` 联合类型
- [ ] `web-frontend/src/components/TaskExecutionHistory.tsx` — 展示名称 / 图标（如有）
- [ ] 执行 `npm run build` 并确认生产环境加载新 build

### 验证命令

```bash
# 1. SchedulerService 能初始化（handler 已注册）
cd backend && python -c "
from backend.services.scheduler_service import SchedulerService
s = SchedulerService()
assert 'your_task_type' in s.task_handlers
print('OK')
"

# 2. DB 约束包含新类型
psql ... -c "SELECT pg_get_constraintdef(oid) FROM pg_constraint
  WHERE conname = 'valid_task_type' AND conrelid = 'task_scheduling'::regclass;"

# 3. Celery 任务已注册（在 worker 进程或导入 celery_app 后）
python -c "
from backend.celery_app import app
from backend.tasks import your_feature_tasks  # 确保已导入
print('generate_your_task' in app.tasks)
"

# 4. 调度器任务列表 API
curl -s http://127.0.0.1:8001/api/scheduler/tasks | head -c 500

# 5. 查看 Celery 任务最终状态（调度 success ≠ 业务 success）
python -c "
from celery.result import AsyncResult
from backend.celery_app import app
r = AsyncResult('<celery-task-id>', app=app)
print(r.state, r.result, r.traceback)
"
```

---

## 真实案例与根因

以下均来自生产/开发环境真实故障，可作为 Code Review 对照。

### 1. Handler 方法缺失或误嵌套 — 页面无法加载

**现象**

```
获取定时任务列表失败: 'SchedulerService' object has no attribute '_execute_economic_events_task'
```

进入 Scheduler 管理页即 500；`scheduler_proxy` 转发失败。

**根因**

`task_handlers` 注册了 `self._execute_economic_events_task`，但实现代码被误粘贴进 `_execute_the_close_summary_task` 的 `except` 块之后，形成不可达的嵌套代码，**没有独立的 `async def _execute_economic_events_task` 方法**。

**修复**

- 将经济事件逻辑提取为独立方法，正确结束上一个 handler 的 `except` 块。
- 合并代码时重点检查：每个 `task_handlers` 键都有 **同级缩进** 的 `async def _execute_*` 方法。

**易错场景**

- 从其他 handler 复制粘贴大段 `try/except` 时多留了一段 docstring + `try`。
- 只加了 `task_handlers` 一行，忘记写方法体。

---

### 2. 前端配置白名单遗漏 — 「coming soon」

**现象**

点击 **Configure The Close Summary Schedule** 齿轮按钮：

```
Data source configuration feature coming soon
```

同时若先拨动 Switch，可能看到：

```
No scheduled tasks found for the_close_summary, please configure data fetch strategy first
```

**根因**

`SchedulerPage.tsx` 的 `showQuickActionModal` 用长串 `if (dataSource.id === '...')` 决定是打开 `DataSourceConfigModal` 还是提示 coming soon。  
`the_close_summary` 已在 `DataSourceConfigModal` 内实现配置逻辑，但 **未加入白名单**。

**修复**

在 `showQuickActionModal` 白名单中加入 `the_close_summary`（与 `market_weekly_report`、`theme_report` 同级处理）。

**同类遗漏**

- `DataSourceConfigModal` 的 `dataSourceType` 类型断言未包含新类型 → TypeScript 编译或运行时配置对象取不到 `dataSourceConfigs[type]`。
- `toggleDataSource` / Switch `Tooltip` 的 task 映射未同步 → 开关行为与提示文案不一致。

---

### 3. 数据库 `valid_task_type` 未更新 — 创建策略失败

**现象**

前端提交 Cron 配置后 API 400/500；PostgreSQL 报 CHECK 约束违反。

**根因**

`task_scheduling.type` 有 `valid_task_type` CHECK。新增 `the_close_summary` 时只改了 Python/前端，**未执行 migration** 更新约束。

**修复**

1. 新增 `backend/database/migrations/add_<name>_scheduler_task_type.sql`（DROP + 重建约束，**合并所有历史类型**）。
2. 更新 `db_manager.py` 中 `all_task_types` 列表。
3. 在目标环境 **手动执行** migration SQL。

**注意**

- 约束是替换式更新，新 SQL 必须包含 `zhishixingqiu_subscription`、`market_weekly_report` 等已有类型，不能只写新类型。
- `db_manager.ensure_schema` 会尝试合并约束，但已有约束时可能 rollback 保留旧版；**仍以 migration 为准**。

---

### 4. Celery 任务未在 Worker 注册 — 调度成功但无结果

**现象**

`app.log` / scheduler 日志：

```
✅ The Close Summary: 已提交 Celery 异步任务 953690a2-...
任务执行成功: the_close_summary_..., 耗时: 62ms
```

但：

- `the_close_summaries` 表无新记录（或仅有占位行）。
- `celery_ai_worker.log` 无对应任务日志。
- `AsyncResult(task_id).state` 为 **`FAILURE`**，`result` 为任务名字符串 `'generate_the_close_summary'`（典型的 **NotRegistered** 表现）。

**根因**

`backend/tasks/the_close_summary_tasks.py` 已编写，`celery_config.py` 已配置队列路由，但 **`celery_app.py` 未像其他 report 任务一样显式 import**：

```python
# 已有示例（需仿照添加）
from backend.tasks import market_weekly_report_tasks
from backend.tasks import daily_market_report_tasks
# 遗漏：
# from backend.tasks import the_close_summary_tasks
```

本项目的 Celery worker **依赖显式导入** 注册任务；仅 `autodiscover_tasks(['backend.tasks'])` 不足以保证所有子模块在 worker 启动时被加载。

**修复**

1. 在 `celery_app.py` 增加显式 import 与成功日志。
2. **重启** `rc-celery-ai`（或对应队列 worker）。
3. 用 `AsyncResult` 或 Flower 确认 `state == SUCCESS`。

**重要认知**

> **Scheduler 的「任务执行成功」只表示 handler 跑完（通常是把任务丢进 Celery 队列），不代表下游 LLM/写库已完成。**  
> 验证业务结果必须查 **结果表** 或 **Celery 终态**。

---

### 5. 报告类任务的上游数据依赖 — 「成功」但只有占位文案

**现象**

Celery 与调度均成功，`the_close_summaries.status = completed`，但内容为：

```
当日无 The Close 节目（可能为非交易日或节目尚未上传）。
```

**根因**

`TheCloseSummaryService._fetch_the_close_videos` 使用：

```sql
metadata->>'show_name' = 'Bloomberg The Close'
```

而近期入库的 Bloomberg 视频（如标题含 `| The Close 6/10/2026`）**`show_name` 为 NULL**（历史 5 月数据有值，6 月新数据未回填）。查询命中 0 条，走「无节目」分支。

**教训**

- 报告类 Scheduler 除注册任务外，还要确认 **数据管道字段**（`show_name`、转录 `transcript`、窗口日期 `report_date` vs `published_at` 时区）。
- 开发时应用 **有转录的真实 content_id** 在目标 `report_date` 下跑一遍 `Service.generate(force=True)`，不要只看 Scheduler 绿灯。

**排查 SQL 示例**

```sql
SELECT content_id, title, published_at,
       metadata->>'show_name' AS show_name,
       LENGTH(transcript) AS transcript_len
FROM subscription_content
WHERE channel_type = 'youtube'
  AND author_identifier = 'UCIALMKvObZNtJ6AmdCLP7Lg'
  AND title ILIKE '%The Close%'
ORDER BY published_at DESC
LIMIT 5;
```

---

### 6. 方法体缩进 / 复制粘贴 — 隐性语法合法但逻辑错误

**现象**

Python 文件能 import，但某个 handler「永远少一个方法」；或某段逻辑永远不会执行。

**检查方法**

```bash
# 每个 task_handlers 键都应有对应 def
rg "task_handlers\s*=" backend/services/scheduler_service.py -A 30
rg "def _execute_" backend/services/scheduler_service.py
# 对比 keys 与 def 名称是否一一对应
```

---

## Code Review 快速对照（Reviewer 用）

| 问题 | 问开发者 |
|------|----------|
| Handler 是否独立方法？ | `task_handlers` 每个 key 是否有同级 `async def _execute_*`？ |
| DB 约束？ | migration 是否已写且将在部署环境执行？ |
| 前端白名单？ | `showQuickActionModal` 是否包含新 `dataSource.id`？ |
| 映射是否全？ | `SchedulerPage` 里搜 `taskType ===`，新类型是否都加了？ |
| Celery？ | `celery_app.py` 是否显式 import？`celery_config` 队列？worker 重启计划？ |
| 验收标准？ | 除 Scheduler 日志 success 外，结果表 / Celery SUCCESS 是否验证？ |
| 数据依赖？ | 报告类任务是否用真实数据跑通过 `Service.generate`？ |

---

## 相关文件索引

| 层级 | 文件 |
|------|------|
| 调度核心 | `backend/services/scheduler_service.py` |
| 调度 API | `backend/api/scheduler.py` |
| API 代理 | `backend/api/scheduler_proxy.py` |
| DB 约束 | `backend/database/migrations/add_*_scheduler_task_type.sql`、`backend/database/db_manager.py` |
| Celery 入口 | `backend/celery_app.py`、`backend/celery_config.py`、`backend/tasks/*_tasks.py` |
| 前端页面 | `web-frontend/src/pages/SchedulerPage.tsx` |
| 配置弹窗 | `web-frontend/src/components/DataSourceConfigModal.tsx` |
| API 客户端 | `web-frontend/src/services/scheduler.ts` |

---

## 延伸阅读

- [SCHEDULER_DEVELOPMENT_GUIDE.md](./SCHEDULER_DEVELOPMENT_GUIDE.md) — 完整开发流程与架构
- [SUMMARY_SCHEDULER_DEVELOPMENT_GUIDE.md](./SUMMARY_SCHEDULER_DEVELOPMENT_GUIDE.md) — **定期 Summarize 类任务**（YouTube/Twitter/星球/The Close/CNBC）统一契约与实现标准
- [TIMEZONE_OPTIMIZATION.md](./TIMEZONE_OPTIMIZATION.md) — 报告日期与内容窗口时区约定
- [DAILY_REPORT_ARCHITECTURE.md](./DAILY_REPORT_ARCHITECTURE.md) — The Close / Daily Report 类报告架构
