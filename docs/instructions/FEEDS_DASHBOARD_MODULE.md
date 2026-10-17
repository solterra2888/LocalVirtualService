# Feeds Dashboard 模块文档（按主题聚合的重要信息视图）

> 创建：2026-09-06 · 对应 P0 落地版本
> 前置阅读：[FEEDS_MODULE.md](./FEEDS_MODULE.md)（订阅抓取与 auto-tag 链路）、[SUMMARY_SCHEDULER_DEVELOPMENT_GUIDE.md](./SUMMARY_SCHEDULER_DEVELOPMENT_GUIDE.md)（report_date 时区口径）

## 概述

Feeds Dashboard（前端 `/feeds-dashboard`，导航 "Feeds Dashboard"）解决**订阅信息看不完**的问题：把 auto-tag 产物（`subscription_content_tags`）按「主题 × 重要性」重新组织，用户按主题扫重要内容，点击条目经抽屉预览原文。

**设计立场**：不使用 `content_theme_matches`（无 embedding、无用户维护的投资主题库）。主题从内容标签自身**涌现**——auto-tag 已经为每条内容付过 LLM 成本，Dashboard 只是把这份产物重组，**读路径零 LLM**。

| 维度 | 说明 |
|------|------|
| 主表 | `feed_topics`（主题词典）+ `feed_topic_daily_items`（每日条目快照）|
| 读路径 | 纯 SQL join，毫秒级，零 LLM |
| 写路径 | 聚合器（零 LLM 规则）每 15 分钟全量重建近 7 天快照 |
| 重要性 | twitter 采信 `tweet_signal_v1` LLM 判断；其余渠道规则评分 |
| P1 规划 | Phase A 主题日摘要（`feed_topic_daily_summaries`，LLM）、needs_review 管理界面 |

## 1. 模块全景

```
【既有链路, 不动】
  7源抓取 → subscription_content → auto-tag → subscription_content_tags
                                            (concepts/tickers/summary/importance[twitter])
       │
       ▼  task_scheduling: feeds_topic_summary (cron */15, 每次重建近7天)
┌────────────────────────────────────────────────────────────┐
│ 聚合器 FeedTopicAggregationService (零 LLM)                  │
│  1. 拉窗口内已打标内容 (concepts 非空, 全局内容池)             │
│  2. concepts 归一化: 精确(词典键/别名) → 包含式兜底(最长命中)   │
│     ├ 命中 → 归入既有主题; 包含式命中 → 词自动学习进别名        │
│     └ 未命中且 ≥3条/天 → auto 新建主题 (needs_review)         │
│  3. importance 评分 (规则, twitter 例外)                      │
│  4. DELETE + 批量 INSERT 当日快照 (幂等收敛)                  │
│  ► feed_topic_daily_items                                    │
└──────────────┬─────────────────────────────────────────────┘
               ▼ 读时组装 (零 LLM, 严格 user_ids ∩ enabled 订阅)
┌────────────────────────────────────────────────────────────┐
│ 前端 /feeds-dashboard                                        │
│  左栏: 我的Watchlist(命中数) / 词云(按主题折并) / 热门Ticker    │
│  右侧: 主题卡片(importance badge 排序) ⇄ 词过滤扁平列表        │
│  点击条目 → DailyPreviewDrawer 原文预览 (复用 Daily 渲染器)    │
│  每 5 分钟自动刷新 + 聚焦刷新 (阅读中不打扰)                    │
└────────────────────────────────────────────────────────────┘
```

## 2. 数据库表

### 2.1 `feed_topics` — 主题词典

迁移：`create_feed_topic_tables.sql`（db_manager 启动自动执行）。

| 字段 | 说明 |
|------|------|
| `topic_key` | UNIQUE。curated 用英文 slug（`geopolitics`）；auto 直接用词本身 |
| `name_cn` / `name_en` | 展示名（如 `地缘政治` / `Geopolitics`）|
| `aliases` | JSONB 数组。curated 人工定义 + 聚合器**自动学习**（单 topic 上限 20 条/次）|
| `source` | `curated`（seed 脚本）/ `auto`（高频未命中词自动新建）|
| `needs_review` | auto 主题待管理员确认/合并（**管理界面尚未建设，见 §9**）|
| `is_enabled` | 关闭后不再聚合/展示 |

### 2.2 `feed_topic_daily_items` — 每日条目快照

| 字段 | 说明 |
|------|------|
| `report_date` | 北京自然日标签（与摘要类任务统一口径）|
| `topic_id` / `subscription_content_id` | UNIQUE(report_date, topic_id, content) |
| `is_primary_topic` | 条目命中多主题时仅 concepts 顺序首位计主 |
| `importance` | `high` / `medium` / `low` |
| `score` | 0–1 数值分，排序用 |

一条内容命中 N 个主题写 N 行（主题视图各计一次）；`unique_content_count` 才是去重口径。一条内容只属于其发布日的快照，跨天不重复。

## 3. 聚合器（`backend/services/feed_topic_aggregation_service.py`）

### 3.1 归一化（核心，2026-09-06 修复后语义）

```
概念词 → _match_topic:
  ① 精确等值: norm(word) == norm(词典键 topic_key/name_cn/name_en/alias)  [strip+lower]
  ② 包含式兜底: 词典键是概念词的真子串 → 取最长命中 (最具体)
     长度下限: 中文键 ≥2 字, ASCII 键 ≥3 字 (防 "AI" ⊆ "maintain" 误合并)
  ③ 均未命中 → 计入未匹配词频, ≥AUTO_TOPIC_MIN_COUNT(3)条/天 → auto 新建主题
```

- 典型：`地缘政治风险` / `能源地缘政治` / `东南亚地缘` 全部折入 `地缘政治`（实测 36 个变体 100% 折并）
- **别名自动学习**：包含式命中的原词写进该主题 `aliases`（下次直接精确命中，词典随真实 LLM 词汇自增长）
- 读路径（词云折并）经 `build_topic_matcher()` 复用**同一套匹配逻辑**，保证与聚合不漂移

### 3.2 importance 评分（零 LLM）

- **twitter**：`additional_tags.tweet_signal.importance`（`tweet_signal_v1` 模板 LLM 判断）直接采信，映射 high=0.90 / medium=0.65 / low=0.35
- **其余渠道**规则分：`base 0.50 + tickers 0.12 + people 0.06 + countries 0.03 + summary 0.04 + confidence×0.08 + engagement对数(≤0.15) + heat_level×0.006(≤0.06) + youtube/podcast有transcript 0.04`；阈值 high≥0.75 / medium≥0.55

### 3.3 幂等

每次执行 DELETE 当日全部快照行再重建——词典禁用、评分变化、engagement 更新都随下次 tick 收敛。

## 4. 调度

| 项 | 值 |
|----|----|
| task_type | `feeds_topic_summary` |
| cron | `*/15 * * * *`（tz Asia/Shanghai）|
| params | `{"days": 7}` —— 每次 tick 全量重建近 7 天（覆盖「近一周」查询 + 跨午夜尾段）|
| Celery | `aggregate_feed_topics`（N 天）/ `aggregate_feed_topics_single`（单日回填），队列 `data_fetching` |
| 迁移自更新 | `add_feeds_topic_summary_task_type.sql` 由 `db_manager` 每次启动重执行，会把已部署行的 days 提到 7、cron 提到 */15 |

⚠ **scheduler 不热加载**：`_load_saved_tasks` 仅启动时执行一次，cron/params 变更必须**重启 `rc-scheduler`** 才生效。聚合代码在 `rc-celery-data` worker 上跑，改聚合器必须重启该 worker。

## 5. REST API（`backend/api/feeds_dashboard.py`，prefix `/api/feeds-dashboard`）

| Method | Path | 说明 |
|--------|------|------|
| GET | `/` | 主题分组视图：`date`(单日) 或 `days`(1/3/7) + `min_importance` + `channels` + `items_per_topic`；返回主题统计 + top 条目（条目 `id=feed_<content_id>` 与 DailyPreviewDrawer 复合 ID 兼容）|
| GET | `/wordcloud` | 词云词频：概念词**按主题折并**（计数=内容条数）+ 散词 + 热门 ticker（top15）+ **watchlist 符号及当日命中数**（0 条也返回）|
| GET | `/items` | 扁平条目：`word`+`word_type`（精确词过滤，ticker 走 smart formats `?|` 变体匹配）或 `topic_id`（主题过滤，命中全部变体），二者互斥 |
| GET | `/topics/{topic_id}/items` | 主题内条目分页 |
| POST | `/generate` | admin 手动触发（`date` 单日 / `days` 近 N 天），派发 Celery |

**可见性口径**（所有读端点一致，与 Daily 相同）：`user_id = ANY(sc.user_ids)` + `EXISTS(subscription_list enabled)`。

**多日期语义**：`report_date = ANY(dates)`；一条内容只属于发布日，跨天不重复计数。

## 6. 词云（`web-frontend/src/components/feedsDashboard/WordCloudPanel.tsx`）

三段式（自上而下）：

1. **我的 Watchlist**：`target_tracking_config`（与 `/dashboard` 同表同排序）符号 + 当日命中数；0 条置灰虚线（"关注的票今天没动静"本身是信息）。点击走 ticker 过滤——**smart formats 变体匹配**（watchlist 存 `NVDA` 无后缀，标签列是 `NVDA.US`，复用 `dashboard.py::get_smart_ticker_formats` + JSONB `?|`）
2. **今日热词**：概念词按主题折并后展示主题名（字号∝词频，单色顺序蓝已过对比度校验）；点击折并词 = `topic_id` 过滤；散词（未命中词典的新词）走精确词过滤
3. **热门 Ticker**：全站频次 top15

配色遵循 dataviz 规范：light `#5598e7→#104281` / dark `#9ec5f4→#256abf`（validate_palette ordinal 校验通过）；词用 `<button>`（键盘可达），命中区 ≥24px，hover ghost wash，重载保持原帧降透明度。

## 7. 前端（`web-frontend/src/pages/FeedsDashboardPage.tsx`）

- 日期范围：**当天（默认）/ 近3天 / 近一周**；importance：全部 / 中+ / 仅高
- 主题卡片：importance badge（高/中/低）排序，可折叠，「查看全部」分页
- 条目点击 → `DailyPreviewDrawer`（复用 `/api/daily/items/feed_<id>` 详情与 9 种原文渲染器），顶栏 "Open in Feeds" 深链
- **自动刷新**：每 5 分钟静默 + `visibilitychange` 聚焦即刷（距上次 >5min）；**阅读中不打扰**——抽屉打开 / 词过滤中 / 请求进行中跳过本轮；`load()` 有 in-flight 防重入
- admin「生成快照」按钮（days=7）
- 路由 `/feeds-dashboard`（桌面+移动同页响应式；移动端底部导航未加入口——6 项已满）

## 8. 部署清单（新环境 / 更新）

```bash
# 1. 表结构: 随 backend 启动自动建 (db_manager._create_feed_topic_tables)
# 2. ⚠ 词典 seed (手动步骤, 幂等 — 忘跑的后果见 §9 FAQ):
python scripts/seed_feed_topics.py               # curated 基线 45 主题
python scripts/seed_feed_topics.py --scan        # 查看历史 concepts 频次报告
python scripts/seed_feed_topics.py --scan --write # 高频未覆盖词自动补 (needs_review)
# 3. 重启: rc-webapp (API) + rc-celery-data (聚合 worker) + rc-scheduler (cron 变更)
# 4. 前端 npm run deploy 部署
# 5. 首次回填: 页面「生成快照」或 POST /api/feeds-dashboard/generate {"days": 7}
```

## 9. 运维 FAQ / 已知问题

| 现象 | 原因与处理 |
|------|-----------|
| 同一主题出现多张卡片 / 词典里一堆变体（如 `地缘政治` + `地缘政治风险`）| **seed 没跑**导致词典 100% auto（2026-09-06 实例：707 个全 auto 主题）；或旧版聚合器（仅精确匹配）在跑。处理：跑 seed → 删 `source='auto'` 主题（daily_items 级联清）→ 重聚合 7 天 → 部署新聚合器代码 |
| 词云仍见同主题变体词 | 散词 = 未命中词典的新词（正常，下次聚合 ≥3 条/天会成主题并被包含式折并）；若大量出现则查聚合器版本 |
| 「近一周」某几天为空 | 快照只重建近 7 天且依赖 cron 实际在跑；确认 rc-scheduler 已重启（15min 周期）、rc-celery-data 消费 `data_fetching` |
| 修改聚合器/词云后不生效 | 聚合在 `rc-celery-data`、API 在 `rc-webapp`、cron 在 `rc-scheduler`——三处都可能持旧代码 |
| `needs_review` 主题无人处理 | 管理 UI 未建（P1.5）；临时用 SQL 审阅/合并，别名学习已大幅减少变体产生 |
| watchlist 点击 0 结果 | watchlist 存无后缀符号（`NVDA`），已走 smart formats 变体匹配；确认新 API 代码已部署 |
| 词云计数与手数不一致 | 折并词计数 = **内容条数**（一条内容多变体只计 1），非词次 |

## 10. 与其他模块的边界

| 维度 | Feeds Dashboard（本文档）| Daily（时间轴聚合）| content_theme_matches（弃用）|
|------|--------------------------|--------------------|------------------------------|
| 组织方式 | 主题 × 重要性 | published_at 时间轴 | 内容 ↔ 用户投资主题匹配行 |
| 主题来源 | concepts 涌现 + 词典归一化 | 无主题 | 用户维护的 investment themes |
| 匹配方式 | SQL 归一化（精确+包含式）| 无 | ticker 重叠 + embedding 余弦 |
| LLM | 读路径 0；P1 加主题摘要 | 0 | 匹配本身 0（依赖已有 embedding）|

## 11. Roadmap

- **P1**：Phase A 主题日摘要（`feed_topic_daily_summaries`，模板 `feed_topic_digest_v1`，`[T-xx]` 引用回链；`content_ids_hash` 变化检测避免重复调 LLM；全局共享 ~20–60 次/天）
- **P1.5**：`needs_review` 主题管理界面（审阅/合并/禁用 auto 主题）
- **P2**：importance 升级为 LLM 字段（`feeds_v2` schema + 回填）、用户偏好（pin/mute 主题、渠道权重）、Agent API 暴露
