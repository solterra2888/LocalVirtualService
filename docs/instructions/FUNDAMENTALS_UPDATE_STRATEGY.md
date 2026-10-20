# Fundamentals 分层更新策略

> 2026-10-20 上线。背景：此前全库只有 53 只 watchlist 股票有 fundamentals 更新（周一三五的
> Watchlist Fundamentals Weekly Update），其余约 2 万家公司冻结在 2025-09/2025-11 的两次手动
> 批量导入（GE 案例因此停在 2025-11-02）。本策略按"10 人团队使用模式 + EODHD 日限 10 万次
> 调用 + 4C/7G 单机"设计。

## 成本口径（EODHD）

- 每家公司全量 fundamentals = **1 次 HTTP 请求**（12 种数据类型合并 filter），按 EODHD 计费
  规则记 **10 次调用**。
- 日限 100,000 次调用 ⇒ 理论上限 1 万家/天。
- celery `data_fetching` 队列并发=2（自带节流，~1.5s/家），3000 家约 40 分钟。

## 四层策略与预算（总计 ≈ 4 万调用/天，占日限 40%）

| 层 | 任务（task_scheduling.type） | 对象 | 频率 | 预算 |
|---|---|---|---|---|
| T1 | `watchlist_fundamentals`（存量保留） | 53 只自选 | 周一三五 06:15 | ~230/天 |
| T2 | `earnings_active_fundamentals` | earnings_calendar [-7d,+21d] 窗口内、fundamentals 超 7 天未刷新，上限 800 家/天 | 每天 18:20（紧跟 18:00 日历抓取） | ≤8k/天 |
| T3 | （无调度，代码内嵌） | 面板打开时公司超 3 天未刷新 → fire-and-forget 单票更新 | 按需 | 数百/天 |
| T4 | `fundamentals_rotation` | 美股(NYSE/NASDAQ/US)+港股 活跃公司（池 14,955 家），updated_at 最旧优先 3000 家/天 | 每天 01:30 | 30k/天，**5 天一轮** |

### T2/T4 共同设计
- 两者都是"查名单 → celery group fan-out `update_single_ticker_fundamentals_task`"，
  **不等待子任务完成**（避免父任务占住 data 队列 2 并发中的 1 个槽位）。
- 幂等：T2 按 `companies.updated_at < now()-stale_days` 过滤；T4 按 `updated_at ASC` 排序天然错峰。
- 港股 exchange 传 'HK'（EODHD 符号 `0700.HK`），美股传 'US'。

### T3 点击静默刷新
`earnings_calendar_service.get_company_panel` → `_maybe_lazy_refresh`：
仅 NYSE/NASDAQ/US/HK 市场、`companies.updated_at` 超 3 天才入队；进程内 {code: ts}
字典 10 分钟粗去重；异常静默。用户当次看到旧数据，数秒后重开即新（实测 GE 10 秒内
highlights 从 2025-11-02 刷到当天）。

## 调参

| 参数 | 位置 | 默认 | 说明 |
|---|---|---|---|
| window_back / window_forward | task_scheduling params | 7 / 21 | T2 窗口 |
| stale_days | 同上 | 7 | T2 新鲜度门槛 |
| max_companies | 同上 | 800 | T2 单日上限 |
| daily_limit | 同上 | 3000 | T4 单日上限（改动即调预算） |
| 3 天 / 10 分钟 | earnings_calendar_service 硬编码 | — | T3 门槛与去重窗口 |

任务管理界面可直接改 params 后立即生效（handler 读 params）。

## 已知边界

- **HK/A股无 analyst ratings**：EODHD 源限制（全库 0 行），watchlist 53 只中 12 只港股/A股
  ratings 永远为空，前端已显示 No data。
- A 股（SHE/SHG 3108 家）不进 T4 轮换池（2026-10 决策）。
- `companies.updated_at` 是所有层的统一新鲜度标记——任何进程写 companies 行都会推迟刷新
  （T4 排序按同字段，自洽）。
- 每股派息历史（EODHD `/api/div/`）未接——Dividends Tab 仍用 cash flow 总额口径，
  作为独立任务另行实施。

## 实现位置

- 任务：`backend/tasks/data_tasks.py` → `update_earnings_active_fundamentals_task` / `fundamentals_rotation_task`
- 调度：`backend/services/scheduler_service.py` → handler map 的 `earnings_active_fundamentals` / `fundamentals_rotation`
- 迁移：`backend/database/migrations/insert_fundamentals_tier_tasks.sql`（约束扩展 + 两行插入，幂等）
- T3：`backend/services/earnings_calendar_service.py` → `_maybe_lazy_refresh`
- **管理界面**：Scheduler Management 有 "Earnings Active Fundamentals" / "Fundamentals Rotation" 两张卡片
  （描述含预算口径, Configure 对应创建端点 `POST /api/scheduler/tasks/earnings-active-fundamentals` 与
  `/tasks/fundamentals-rotation`——scheduler.py 本体 + scheduler_proxy.py 转发各一份; 任务已存在时创建
  端点按"同类型仅一条"规则返回提示）。Monitoring → Scheduler Logs 动态读 task_type, 两个任务首次
  运行后自动出现在执行历史里。卡片状态同步用精确匹配（原 `includes('fundamentals')` 模糊匹配已改为
  精确, 避免三个 fundamentals 任务的状态混到同一张卡片）。

## 附带修复的存量 bug（2026-10-20）

`add_form13f_scheduler_task_type.sql` 与 `add_feeds_topic_summary_task_type.sql` 在每次启动时
用**静态清单** DROP/ADD `valid_task_type` 约束，清单落后于表内实际类型（缺 bloomberg_surveillance
等）导致 ADD 违反已存在行、毒化整个启动事务（Form 13F / Feeds / file_folders / display_name
级联报错，自 Bloomberg 任务上线以来一直存在）。修复：删除这两个 SQL 的约束重建段，约束统一由
`db_manager._update_task_scheduling_constraints` 按"表内已有类型 + 代码内置清单"合并重建
（`db_manager.py` 的 `all_task_types` 同步补入 `earnings_active_fundamentals`、`fundamentals_rotation`）。
