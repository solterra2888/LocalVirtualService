# Theme Report 功能手册

本文说明 **Theme Report** 在系统中的定位、数据来源、收益率计算规则、生成流程、存储方式及运维要点。内容以当前仓库实现为准。

---

## 1. 功能概述

**Theme Report** 为每个用户生成一份静态 HTML 报告，对标参考系统 [Theme Performance Dashboard](https://dashboard.cntlabs.io/reports/Themes/)，包含 **Executive Summary**、**Theme Performance**、**Theme Details** 与 **Individual Stock Charts**（内嵌 OHLCV K 线图）。

报告内容：

- **Executive Summary**：各周期最佳主题 KPI、3M 最差主题、1M/3M 正收益广度、组合级 PM Read
- 每个主题（Theme）的 **PM Read** 摘要（1M/3M 类别领先、Top/Bottom constituents）
- **Breakdown**：按 Category 等权聚合的 1D / 1W / 1M / 3M / 1Y 收益率
- **Constituents**：各成分股的多周期收益率明细

与 **Market Weekly** 的区别：

| 维度 | Market Weekly | Theme Report |
|------|---------------|--------------|
| 输出格式 | JSON（前端动态渲染） | 自包含静态 HTML |
| 存储 | Redis 缓存 + API 实时计算 | 阿里云 OSS + `theme_reports` 表 |
| 标签 | `Market Weekly` | `Theme Report` |
| 周期逻辑 | WoW（周五对周五）、90 天相对表现 | 1D/1W/1M/3M/1Y 简单收益率 |

触发方式：

| 方式 | 行为 |
|------|------|
| **Celery Beat** | 每周一 06:00 UTC（北京时间 14:00）为所有打标用户生成 |
| **前端手动触发** | Reports → Theme Report → Generate Report |
| **API** | `POST /api/theme-report/generate` |
| **Celery 任务** | `generate_all_theme_reports`（可指定 `user_id` / `report_date`） |

---

## 2. 前置条件

### 2.1 数据库迁移

首次部署需手动执行：

```bash
psql -h <host> -U postgres -d guanhe_database \
  -f backend/database/migrations/create_theme_reports_table.sql
```

### 2.2 主题与标签配置

报告仅包含当前用户打了 **`Theme Report`** 标签的主题，机制与 `Market Weekly` 标签相同：

1. 在 **Investment Themes** 页面为需要的主题添加 `Theme Report` 标签
2. 标签写入 `theme_tags` 表（`user_id` + `theme_id` + `tag_name`）
3. 仅 `theme_company_associations.is_verified = TRUE` 的 ticker 参与计算

数据模型关系：

```
investment_theme_embeddings          theme_name  → 报告中的主题名（行业大类）
        │                              category    → Breakdown 中的分类
        ├── theme_tags                 tag_name = 'Theme Report'
        └── theme_company_associations ticker      → Constituents
```

### 2.3 EOD 历史数据

3M / 1Y 收益率需要足够长的 EOD 历史。首次使用前建议执行一次回填：

- 前端：**Backfill History** 按钮（默认 400 天）
- API：`POST /api/theme-report/backfill`
- Celery：`backfill_theme_report_eod_history`

底层调用 `fetch_eodhd_eod_stock_prices_task`，会从 `theme_company_associations` 与 watchlist 汇总 ticker，写入 `stock_prices_eod`（`period = 'd'`）。

---

## 3. 架构与数据流

```
theme_tags ('Theme Report')
        │
        ▼
ThemeReportService.generate_and_upload()
        │
        ├── 1. _get_tagged_themes()          ← investment_theme_embeddings + associations
        ├── 2. _compute_ticker_returns()   ← stock_prices_eod (close, unadjusted)
        ├── 3. _build_theme_details()      ← Category 等权聚合 + PM Read insights
        ├── 3b. _compute_theme_performance() ← Performance 表 / 累计曲线
        ├── 3b2. _build_executive_summary()  ← KPI 卡片 + 组合级 PM Read
        ├── 3c. _fetch_stock_chart_data()   ← OHLCV 内嵌 K 线图 (STOCK_CHART_DATA)
        ├── 4. _render_html()              ← Jinja2: theme_report.html.j2
        ├── 5. OSSService.upload_bytes()     → reports/Themes/{user_id}/report_{date}/...
        └── 6. UPDATE theme_reports          ← status / metadata / counts
```

前端通过 API 获取 OSS **签名 URL**（24h 有效），在 iframe 中嵌入最新报告。

---

## 4. 收益率计算规则

> **已知 Bug（已修复）**
>
> | Bug | 根因 | 修复 |
> |-----|------|------|
> | **1W 全部显示 0.0%** | 报告在周一生成时，`price_as_of` 恰好是周一，WTD 逻辑把 `monday_of_week` 当作 start，与 end 相同 → 收益为 0 | 改为固定 7 日历天回溯（与参考 dashboard 一致），删除 WTD 特殊逻辑 |
> | **1Y 数值虚高 2–8×** | `MAX_GAP_DAYS=10` 过滤掉了历史不足 364 天的新上市股（负收益居多），产生幸存者偏差 | 1Y 不应用 gap 限制；新上市股用其最早可用价格参与等权平均 |

价格来源：`stock_prices_eod`，字段 `close`（**未复权**），`period = 'd'`。

锚定日期：所有周期以 **`price_as_of`**（数据库中该批 ticker 的最新实际交易日）为终点，**不是**日历 `report_date`。避免周末/节假日或当日 EOD 尚未入库时出现 1D = 0% 等问题。

### 4.1 周期定义

与参考 Theme Performance Dashboard 对齐，采用 **「周倍数」日历天数** 框架：

| 周期 | 定义 | 日历天数 | 说明 |
|------|------|----------|------|
| **1D** | 前一交易日 | 1 | `price_as_of` 与 `price_as_of - 1d` 的最近交易日 close |
| **1W** | Week-To-Date | — | 本周一（`weekday()`）起第一个有数据的交易日 → `price_as_of` |
| **1M** | 3 周 | 21 | `price_as_of - 21d` 的最近交易日 close |
| **3M** | 9 周 | 63 | `price_as_of - 63d` 的最近交易日 close |
| **1Y** | 52 周 | 364 | `price_as_of - 364d` 的最近交易日 close |

代码常量见 `backend/services/theme_report_service.py` 中的 `RETURN_HORIZONS`。

### 4.2 计算公式

单 ticker 简单收益率：

```
return = (end_close / start_close - 1) × 100
```

Category / Theme 层级：**等权平均**（仅对有效非 `None` 的 ticker 收益率取算术平均）。

Constituents 表按 **3M** 降序排列；Breakdown 表按 **3M** 降序并赋 rank。

### 4.3 缺失数据处理

- Pivot 后对价格序列 `ffill().bfill()`，模拟停牌期间价格不变（与 Market Weekly 一致）
- **`MAX_GAP_DAYS = 10`**：若起点实际数据距目标锚点超过 10 个日历日，该周期返回 `None`（显示 `n/a`），避免跨越大段数据缺口时使用陈旧 ffill 价格

### 4.4 与参考 Dashboard 的差异

- **1D / 1W / 1M / 3M**：已与参考 dashboard 多 ticker 交叉验证一致
- **1Y**：参考系统可能使用复权价格；本系统使用 unadjusted `close`，长周期若遇拆股可能略有偏差
- **时区**：`report_date` 使用 UTC 今日（`utc_today()`）；参考 dashboard 以纽约时间生成，OSS 路径日期可能相差一天，但收益率锚定 `price_as_of`，对短周期影响可忽略

---

## 5. PM Read 摘要逻辑

每个主题生成 4 条 insight：

1. **1M category leadership**：1M 收益率最高 / 最低的 Category
2. **3M category leadership**：3M 收益率最高 / 最低的 Category
3. **Top 1M constituents**：1M 收益率 Top 3 股票
4. **Bottom 1M constituents**：1M 收益率 Bottom 3 股票

注意：Breakdown 列表按 3M 排序，但 PM Read 的 1M/3M 类别领先项需 **按对应周期重新排序** 后再取首尾，不可直接复用 3M 排序后的列表顺序。

---

## 6. 数据存储

### 6.1 `theme_reports` 表

| 字段 | 说明 |
|------|------|
| `user_id` | 用户名，与 `theme_tags.user_id` 一致 |
| `report_date` | 报告日期（通常 UTC 当天） |
| `oss_object_name` | OSS 路径，不存 URL |
| `theme_count` / `category_count` / `constituent_count` | 统计 |
| `metadata` | JSONB：`generation_time_seconds`、`theme_names`、`price_as_of_date` 等 |
| `status` | `pending` / `generating` / `completed` / `failed` |

唯一约束：`(user_id, report_date)` — 同一用户同一天重复生成会 upsert 覆盖。

### 6.2 OSS 路径

```
reports/Themes/{user_id}/report_{YYYYMMDD}/report_embedded.html
```

示例：

```
reports/Themes/Kevin/report_20260601/report_embedded.html
```

访问时通过 `OSSService.get_signed_url(..., content_disposition='inline')` 生成临时 URL。

---

## 7. API 接口

路由前缀：`/api`（见 `backend/main.py`）

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/theme-report/list?limit=20` | 当前用户历史报告列表（含签名 URL） |
| GET | `/theme-report/latest` | 最新一份 `completed` 报告 |
| POST | `/theme-report/generate` | 异步触发生成，body 可选 `{ "report_date": "YYYY-MM-DD" }` |
| POST | `/theme-report/backfill` | 异步触发 EOD 回填，body 可选 `{ "days_back": 400 }` |

均需 JWT 认证（`get_current_user`）。

---

## 8. Celery 任务

| 任务名 | Queue | 说明 |
|--------|-------|------|
| `generate_all_theme_reports` | `data_fetching` | 遍历或指定用户生成报告 |
| `backfill_theme_report_eod_history` | `data_fetching` | 触发 EOD 历史回填 |

Beat 调度（`backend/celery_config.py`）：

```python
'generate-theme-reports-weekly': {
    'task': 'generate_all_theme_reports',
    'schedule': crontab(hour=6, minute=0, day_of_week=1),  # 周一 06:00 UTC
},
```

### 8.1 手动执行示例

**生成报告（单用户，同步调试）：**

```bash
cd /path/to/guanhetech
PYTHONPATH=. python3 -c "
from backend.services.theme_report_service import ThemeReportService
svc = ThemeReportService()
print(svc.generate_and_upload(user_id='Kevin'))
"
```

**Celery 异步生成：**

```python
from backend.tasks.theme_report_tasks import generate_all_theme_reports_task
generate_all_theme_reports_task.apply_async(kwargs={'user_id': 'Kevin'})
```

**EOD 回填：**

```python
from backend.tasks.theme_report_tasks import backfill_theme_report_eod_history_task
backfill_theme_report_eod_history_task.apply_async(kwargs={'days_back': 400})
```

Worker 需监听 `data_fetching` 队列：

```bash
celery -A backend.celery_app worker --queues=data_fetching --loglevel=info
```

---

## 9. 前端

| 路径 | 组件 |
|------|------|
| `/reports/theme` | `web-frontend/src/pages/ThemeReportPage.tsx` |
| API 客户端 | `web-frontend/src/services/themeReport.ts` |
| 导航 | `ReportsLayout.tsx` → Theme Report 标签 |

页面功能：

- 历史报告表格（日期、状态、主题数、OSS 链接）
- iframe 嵌入最新报告
- **Generate Report**（可选日期）
- **Backfill History**（400 天 EOD）
- **Refresh**

### 9.1 外观（Light / Dark）

报告 HTML 自带 **Light / Dark** 切换（侧栏顶部 + 报告标题右侧），选择保存在浏览器 `localStorage`（键 `theme-report-color-mode`），默认 **Light**。

**与 GuanHe 应用主题无关**：网站侧栏的主题开关只影响本网站外壳，不改变报告 iframe 内配色。

- Chart.js 累计图在切换主题时自动重建配色
- **Stock Charts** K 线区保持黑底；外围卡片/弹窗随主题变色

需重新 **Generate** 后，新报告 HTML 才包含切换按钮。

---

## 10. 关键代码文件

| 文件 | 职责 |
|------|------|
| `backend/services/theme_report_service.py` | 核心：取数、算收益、聚合、渲染、上传 |
| `backend/templates/theme_report.html.j2` | 静态 HTML 模板（内联 CSS；**跟随系统** light/dark，`prefers-color-scheme`） |
| `backend/tasks/theme_report_tasks.py` | Celery 任务入口 |
| `backend/api/theme_report.py` | FastAPI 路由 |
| `backend/celery_config.py` | 任务路由 + Beat 调度 |
| `backend/database/migrations/create_theme_reports_table.sql` | 表结构迁移 |

依赖：`Jinja2>=3.1.0`（见 `requirements.txt`）

---

## 11. 运维与排错

### 11.1 常见问题

| 现象 | 可能原因 | 处理 |
|------|----------|------|
| 404 No completed report | 用户无 `Theme Report` 标签或未生成 | 打标签后触发生成 |
| 1M/3M/1Y 大量 `n/a` | EOD 历史不足或数据缺口 | 执行 Backfill History |
| 1D = 0% | 旧版以 `report_date` 为锚（已修复） | 确认使用最新 `theme_report_service.py` |
| PM Read 1M 类别与参考不一致 | 误用 3M 排序列表取 1M 领先（已修复） | 重新生成报告 |
| 生成任务无响应 | Celery worker 未启动或未监听 `data_fetching` | 启动 worker + beat |
| OSS iframe 空白 | 签名 URL 过期或对象不存在 | 刷新页面；检查 `theme_reports.status` |

### 11.2 状态检查 SQL

```sql
-- 某用户最近报告
SELECT report_date, status, theme_count, constituent_count, error_message, created_at
FROM theme_reports
WHERE user_id = 'Kevin'
ORDER BY report_date DESC
LIMIT 5;

-- 打标主题数量
SELECT COUNT(DISTINCT tt.theme_id)
FROM theme_tags tt
WHERE tt.user_id = 'Kevin' AND tt.tag_name = 'Theme Report';

-- 某 ticker EOD 覆盖（示例）
SELECT MIN(date::date), MAX(date::date), COUNT(*)
FROM stock_prices_eod
WHERE code = 'ALAB.US' AND period = 'd';
```

### 11.3 与 Market Weekly 的对照

Market Weekly（`market_weekly_service.py`）在主题收益率上采用类似思路：

- 从 `theme_tags` + `theme_company_associations` 取 ticker
- `stock_prices_eod` + pandas pivot + `ffill/bfill`
- 等权平均聚合到主题层

差异在于周期定义：Market Weekly 的 WoW 使用 **数据库中最近两个周五**；Theme Report 的 1W 使用 **WTD**，1M/3M 使用 **21/63 日历天**，与 Theme Performance Dashboard 对齐。

---

## 12. 参考样本

- 参考 Dashboard：`https://dashboard.cntlabs.io/reports/Themes/report_20260531/report_embedded.html`
- 本地生成样本：`reports_Themes_Kevin_report_20260601_report_embedded.html`（项目根目录，调试用）

验证时可对比 AI Data Centers 下 Compute Semiconductors / Networking Connectivity 等 Category 及 ALAB、AMD、AVGO 等 Constituent 的 1D/1W/1M/3M 数值。
