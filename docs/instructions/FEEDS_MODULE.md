# Feeds 订阅模块完整文档

## 概述

Feeds 模块（即前端 "My Feeds" / `/feeds`）是一个**多渠道内容订阅聚合系统**，统一管理用户在不同平台（Twitter、YouTube、News、RSS、知识星球）的订阅源，将异构内容归一化存储到同一张 `subscription_content` 表，并在前端以统一的阅读器形式呈现。

与 "文件上传 + LLM 分析"（见 `docs/instructions/UPLOAD_FILES_AND_LLM_ANALYSIS_FLOW.md`）、"新闻分析"（见 `docs/instructions/NEWS_ANALYSIS_FLOW.md`）相比，Feeds 模块的核心职责是**持续、被动、按频道**地把外部内容同步进来并提供阅读体验，不自动触发 LLM 打标/分类/命名（区别见[第 7 节](#7-与其他模块的边界)）。

## 1. 模块全景

```
┌──────────────────────────────────────────────────────────────────┐
│                         Feeds 订阅模块                            │
└──────────────────────────────────────────────────────────────────┘

【入口层】
    │
    ├─ 前端用户交互（增/删/改订阅、阅读、标记已读、手动重转 ASR）
    │   └─ web-frontend/src/pages/FeedReaderPage.tsx  (桌面)
    │   └─ web-frontend/src/pages/MobileFeedsPage.tsx (移动端)
    │
    ├─ REST API  (backend/api/subscriptions.py,  prefix=/api/subscriptions)
    │   └─ 列表 / 创建 / 更新 / 删除 / 内容列表 / 标记已读 / 获取字幕 / 重转 ASR
    │
    └─ 定时调度（task_scheduling 表 + scheduler_service）
        ├─ youtube_subscription    → fetch_all_youtube_subscriptions
        ├─ twitter_subscription    → fetch_twitter_subscription
        └─ news_feed               → EODHDNewsFeedService.fetch_and_distribute_news
    │
    ▼
┌──────────────────────────────────────────────────────────────────┐
│            抓取层（Celery Tasks + 本地家用 Worker）                │
└──────────────────────────────────────────────────────────────────┘
    │
    ├─ Twitter     → TwitterSubscriptionService.fetch_and_store_tweets
    │                (queue: data_fetching, 远程 Worker)
    │
    ├─ YouTube     → fetch_youtube_subscription / fetch_all_youtube_subscriptions
    │                (queue: youtube_fetching, 家用 Worker)
    │                └─ 字幕 fallback → 必要时派发 transcribe_youtube_feed_task
    │                   详见 deployment/local_virtual_service/README.md
    │
    └─ News        → EODHDNewsFeedService
                     (无 Celery，asyncio 线程池同步拉取 + 分发)
    │
    ▼
┌──────────────────────────────────────────────────────────────────┐
│               归一化存储（subscription_content 表）                │
│   SubscriptionContentService.save_content → 按 channel_type        │
│   调用 ContentAdapter，ON CONFLICT 合并 user_ids[]                 │
└──────────────────────────────────────────────────────────────────┘
    │
    ▼
【读取 & 阅读层】
    ├─ GET /api/subscriptions/{id}/contents                （列表，仅返回 has_transcript）
    ├─ GET /api/subscriptions/contents/{id}/transcript     （YouTube 字幕全文懒加载）
    ├─ POST /api/subscriptions/contents/{id}/mark-read     （标记已读/未读）
    └─ POST /api/subscriptions/contents/{id}/transcribe    （YouTube 手动重转 ASR）
    │
    ▼
┌──────────────────────────────────────────────────────────────────┐
│                    LLM 调用层（三条独立链路）                      │
└──────────────────────────────────────────────────────────────────┘
    │
    ├─ ① Feed 内容打标 & 摘要（FeedContentTaggingService）
    │   触发：用户手动 / 批量重打标 API（不自动触发）
    │   输入：subscription_content.content / transcript（Twitter / YouTube）
    │   模型：LocalLLMAnalysisService → prompt template: content_tag_feeds_v1
    │   输出：subscription_content_tags
    │         (sector / industry / tickers / concepts / summary / confidence)
    │   Celery：tag_feed_content / batch_tag_feed_content  (queue: file_processing)
    │   API：/api/subscriptions/contents/{id}/tags 系列
    │
    ├─ ② Twitter Daily Report（两阶段）
    │   调度：task_scheduling 类型 twitter_author_summary（阶段A完后链式触发阶段B）
    │   API：POST /reports/twitter-daily/generate（手动触发）
    │   │
    │   ├─ 阶段 A：TwitterAuthorSummaryService（全局作者摘要预生成）
    │   │   每作者 + 每天只调一次 LLM（所有订阅该作者的用户共用结果）
    │   │   模板：twitter_daily_phase1_default
    │   │   输入：当日该作者的全部 tweets（最多 30 条/chunk）
    │   │   输出：twitter_author_daily_summaries 表
    │   │
    │   └─ 阶段 B：TwitterDailyReportService（用户综合报告）
    │       读取阶段 A 预计算摘要 → 全局引用重编号 → Phase 2 综合分析
    │       模板：twitter_daily_phase2_default（用户可通过偏好自定义）
    │       输出：twitter_daily_reports 表
    │       API：GET /reports/twitter-daily/{date}
    │
    └─ ③ YouTube Daily Report（两阶段，结构同 Twitter）
        调度：task_scheduling 类型 youtube_author_summary（阶段A完后链式触发阶段B）
        API：POST /reports/youtube-daily/generate（手动触发）
        │
        ├─ 阶段 A：YouTubeAuthorSummaryService（全局频道摘要预生成）
        │   每频道 + 每天只调一次 LLM（所有订阅该频道的用户共用结果）
        │   模板：youtube_daily_phase1_default
        │   输入：当日该频道新视频标题 + transcript（最多 10 条/chunk）
        │   输出：youtube_author_daily_summaries 表
        │
        └─ 阶段 B：YouTubeDailyReportService（用户综合报告）
            读取阶段 A 预计算摘要 → Phase 2 综合分析
            输出：youtube_daily_reports 表
            API：GET /reports/youtube-daily/{date}
```

## 2. 数据库表结构

### 2.1 `subscription_list` — 用户订阅列表

定义在 `backend/database/migrations/create_subscription_tables.sql`，后续由 `add_unread_count.sql` 追加 `unread_count` 列。

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | serial PK | 订阅 ID |
| `user_id` | int FK → users | 所属用户 |
| `channel_type` | varchar | `twitter` / `youtube` / `news` / `rss` / `zhishixingqiu` |
| `channel_identifier` | varchar | 频道唯一标识（Twitter handle / YouTube channelId / News source 等）|
| `channel_name` | varchar | 展示名（Twitter 自动从 API 拉取，YouTube 自 RSS 提取）|
| `channel_description` | text | 描述 |
| `enabled` | bool | 是否启用；关闭后不再抓取也不计入列表刷新 |
| `priority` | int | 预留排序优先级 |
| `fetch_interval_minutes` | int | 预留：单订阅刷新周期（当前统一由 scheduler 控制）|
| `max_items_per_fetch` | int | 单次抓取条数上限 |
| `filter_config` | jsonb | 过滤配置（如 News 的关键词、YouTube 的最小时长等）|
| `metadata` | jsonb | 渠道专用元数据 |
| `last_fetched_at` / `next_fetch_at` | timestamptz | 抓取调度时间戳 |
| `total_items_fetched` | int | 累计抓取条数 |
| `last_fetch_items_count` | int | 最近一次抓取条数 |
| `consecutive_failures` | int | 连续失败次数（风控降级用）|
| `unread_count` | int | **未读数**，由触发器维护，见 2.3 |
| `created_at` / `updated_at` | timestamptz | — |
| `UNIQUE (user_id, channel_type, channel_identifier)` | | 避免重复订阅 |

### 2.2 `subscription_content` — 归一化内容表

起源于 `twitter_tweets`，由 `migrate_to_subscription_content.sql` 演进而来。核心列：

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | serial PK | 内部主键 |
| `channel_type` | varchar | 与 `subscription_list.channel_type` 对齐 |
| `content_id` | varchar | 平台侧唯一 ID（tweet_id / videoId / news GUID）|
| `author_identifier` | varchar | 频道标识，回连 `subscription_list.channel_identifier` |
| `author_name` | varchar | 作者展示名 |
| `title` / `content` | text | 标题/正文 |
| `user_ids` | int[] | **被哪些 user 订阅**（同一内容复用，见 2.4）|
| `read_status` | jsonb | `{ "<user_id>": true/false }` 形式 |
| `published_at` | timestamptz | 发布时间 |
| `engagement_stats` | jsonb | 点赞/评论/观看等渠道指标 |
| `heat_level` | int | 热度等级（News/Twitter 用）|
| `categories` | text[] | 分类标签（News 用）|
| YouTube 专用 | — | `transcript` / `transcript_data` / `duration_seconds` / `channel_id` / `feed_url` |
| Twitter 专用 | — | `retweet_*` / `quote_*` / `reply_*` / `media_urls` / … |
| `UNIQUE (channel_type, content_id)` | | 跨用户去重的关键 |

### 2.3 `subscription_fetch_history` + 未读数维护

- `subscription_fetch_history`：每次抓取一行（fetched/new/updated/skipped 计数、错误、耗时）。暴露给 `GET /api/subscriptions/{id}/history`。
- 数据库函数 `update_subscription_unread_count(user_id, channel_type, author_identifier)` + 触发器：`read_status` 或新内容插入时自动刷新 `subscription_list.unread_count`，前端只需读一列。

### 2.4 内容复用机制（重要）

**同一 channel 的同一条内容只入一行**。第二个用户订阅同一频道时，后端不会重新抓一遍，而是通过：

- `add_user_to_existing_content_task`：把新 `user_id` 追加到历史内容的 `user_ids[]`；
- `smart_wait_and_add_user_task`：若另一个用户正在抓取，等待 ≤60s 再 merge。

这两个 Celery 任务定义在 `backend/tasks/subscription_tasks.py`，由 `POST /api/subscriptions/` 在 "有其他用户同订阅 → 已有内容 / 即将产生内容" 时触发（见 `create_subscription` 逻辑）。

## 3. REST API（`/api/subscriptions`）

路由注册在 `backend/main.py`，全部实现在 `backend/api/subscriptions.py`。

| Method | Path | 作用 |
|--------|------|------|
| `GET` | `/` | 当前用户订阅列表，可按 `channel_type` / `enabled` 过滤，返回含 `unread_count` |
| `GET` | `/news-sources` | 列出 EODHD 支持的 News 源（供新建订阅时下拉选择）|
| `GET` | `/{subscription_id}` | 单条订阅详情（检查 owner）|
| `POST` | `/` | **创建订阅**：校验渠道有效性、`UNIQUE` 去重、智能复用已有内容、必要时派发抓取任务（见 [§5](#5-新增订阅时的智能派发决策)）|
| `PUT` | `/{subscription_id}` | 部分更新（改名、启用/停用、filter_config、metadata 等）|
| `DELETE` | `/{subscription_id}` | 删除订阅（内容表不删，可能被其他用户继续使用）|
| `GET` | `/stats/overview` | 统计聚合（总数、按渠道、失败数）|
| `GET` | `/{subscription_id}/history` | 抓取历史（`subscription_fetch_history`）|
| `GET` | `/tasks/{task_id}/status` | 查询抓取任务 `AsyncResult`（前端轮询 "抓取中"）|
| `GET` | `/{subscription_id}/contents` | 分页内容列表，支持 `only_unread`；YouTube 只返回 `has_transcript` 不返回全文 |
| `POST` | `/contents/{content_id}/mark-read` | 标记已读 / 未读（修改 `read_status.{user_id}`）|
| `GET` | `/contents/{content_db_id}/transcript` | **YouTube 字幕懒加载**：返回 `transcript` + `language` + `source`（api/ytdlp/asr）|
| `GET` | `/contents/tweet/{tweet_id}` | Twitter：按 `content_id` 取一条（用于引用/回复上下文）|
| `POST` | `/contents/{content_db_id}/transcribe` | **YouTube 手动重转 ASR**，派发 `transcribe_youtube_feed_task`，返回 `task_id` 供前端轮询 |

> ⚠ **路由顺序坑**：`GET /{subscription_id}` 注册在 `GET /stats/overview` 前面，FastAPI 会把 `stats` 当作非法 int 参数报 `422`。如要启用 stats 接口，需在路由定义里把 `/stats/overview` 放到 `/{subscription_id}` 之前。

### 请求/响应模型（节选）

定义在 `backend/api/subscriptions.py` 顶部：

- `CreateSubscriptionRequest`：`channel_type` / `channel_identifier` / `channel_name?` / `channel_description?` / `enabled` / `filter_config?` / `metadata?`
- `SubscriptionResponse`：返回全部 `subscription_list` 字段 + `unread_count`
- `UpdateSubscriptionRequest` / `SubscriptionStatsResponse` / `FetchHistoryResponse`

## 4. 后端服务分层

| 文件 | 职责 |
|------|------|
| `backend/services/subscription_content_service.py` | **归一化存储层**。`save_content` / `_save_normalized_content`（根据 `channel_type` 分发到 `ContentAdapter`，`ON CONFLICT (channel_type, content_id)` 合并 `user_ids`）；`update_transcript`（写 YouTube 字幕）；`mark_as_read`；`update_subscription_unread_count` 调用等。所有渠道统一出入口。|
| `backend/services/youtube_subscription_service.py` | **服务器端** YouTube feed 实现：RSS → Data API v3 fallback、频道校验、`fetch_all_subscriptions()`（在远程家用 Worker 上执行，详见第 6 节）。 |
| `backend/services/twitter_subscription_service.py` | Twitter API 拉取、正文/媒体/引用树、写入 `subscription_content` |
| `backend/services/eodhd_news_feed_service.py` | 从 EODHD News API 拉取 → 经 `NewsAdapter` → 写入 `subscription_content`。暴露 `AVAILABLE_NEWS_SOURCES` 给前端 News 源下拉 |
| `backend/services/content_adapters/*.py` | 各渠道的字段归一化器：`TwitterAdapter` / `YouTubeAdapter` / `NewsAdapter` |
| `backend/services/youtube_service.py` | YouTube 字幕/元信息工具类（被 API 与批量任务共用）|
| `backend/services/scheduler_service.py` | 将 `task_scheduling` 里的 `youtube_subscription` / `twitter_subscription` / `news_feed` 类型映射到对应的 Celery 派发函数 |

## 5. 新增订阅时的智能派发决策

`POST /api/subscriptions/` 创建订阅后会判断当前渠道是否**已经有其他用户的内容**，用不同策略避免重复抓取：

```
新建订阅 (channel_type, channel_identifier, user_id)
    │
    ▼
 校验合法性（Twitter 存在 / YouTube RSS 可达 / News 源在白名单）
    │
    ▼
 INSERT INTO subscription_list (ON CONFLICT → 409)
    │
    ▼
 查询 subscription_content WHERE (channel_type, author_identifier)
    │
    ├─ 有历史内容                 → add_user_to_existing_content_task
    │   （把 user_id append 进 user_ids[]，不拉新数据）
    │
    ├─ 另一用户正在抓取            → smart_wait_and_add_user_task
    │   （等 ≤60s，若成功则 merge；否则触发真抓取）
    │
    └─ 无内容 & 无同时在抓           → 触发真抓取
        │
        ├─ twitter  → fetch_twitter_subscription_task            (queue: data_fetching)
        ├─ news     → add_user_to_existing_content_task
        │             (News 通常早有内容；若无则依赖定时 news_feed 任务)
        └─ youtube  → 灰度：
                     YOUTUBE_SUBSCRIPTION_USE_LOCAL=true  (默认)
                        → celery send_task('fetch_youtube_subscription', ...)
                          → 走 celery_config 路由 → youtube_fetching → 家用 Worker
                     YOUTUBE_SUBSCRIPTION_USE_LOCAL=false (紧急回滚)
                        → fetch_youtube_subscription_task.apply_async(queue='data_fetching')
                          → 远程 Worker 旧路径（远程服务器无法直连 YouTube，此分支现已几乎无法工作）
```

关键代码：`backend/api/subscriptions.py::create_subscription` 第 ~490–600 行（Twitter、YouTube、News 三分支）。

## 6. 定时抓取链路

**Celery Beat 本身只跑 `cleanup_llm_usage_logs`**（见 `backend/celery_config.py::beat_schedule`），Feeds 的周期抓取不在那里。

真正的调度来自 `task_scheduling` 表 + `scheduler_service`（详见 `docs/instructions/SCHEDULER_DEVELOPMENT_GUIDE.md`）：

| 调度任务类型 | 默认 cron | 执行器 | 产出 |
|-------------|----------|--------|------|
| `youtube_subscription` | `0 * * * *`（每小时，种子数据见 `backend/database/migrations/insert_youtube_subscription_task.sql`）| `_execute_youtube_subscription_task` → `fetch_all_youtube_subscriptions_task.delay()` | 全量扫描所有启用的 YouTube 订阅，拉最新视频并做字幕 fallback |
| `twitter_subscription` | 由用户配置 | `_execute_twitter_subscription_task` → `fetch_twitter_subscription_task.delay(sub_id, …)` | 单订阅 Twitter 拉取 |
| `news_feed` | 由用户配置 | `_execute_news_feed_task` → `EODHDNewsFeedService.fetch_and_distribute_news`（run_in_executor 同步调用）| News 批量入 `subscription_content` |

前端 `SchedulerPage.tsx` + `DataSourceConfigModal.tsx` 可配置这些调度项。

### 6.1 Celery 任务清单（`backend/tasks/subscription_tasks.py` + `file_tasks.py`）

| 任务名 | 队列（由 `backend/celery_config.py` 路由）| time_limit / soft | 重试 | 作用 |
|--------|-----------------------------|-------|------|------|
| `fetch_twitter_subscription` | `data_fetching` | 900 / 780 | 3 × 60s | 单订阅 Twitter 拉取 |
| `add_user_to_existing_twitter_content`（已通用化为 `add_user_to_existing_content_task`）| `data_fetching` | 60 / 45 | 2 | 合并 `user_ids`，不拉新数据 |
| `smart_wait_and_add_user` | `data_fetching` | 120 / 90 | 1 | 等待他人抓取 ≤60s 再合并 |
| `fetch_youtube_subscription` | `youtube_fetching` | 60 / 30 | 0 | **派发器**：远程服务器上的任务壳，用 `send_task` 把同名任务再扔到 `youtube_fetching` 队列由家用 Worker 消费 |
| `fetch_all_youtube_subscriptions` | `youtube_fetching` | **4200 / 3600**（70m / 60m）| 2 × 120s | 批量 YouTube 订阅；实际在家用 Worker 上 `concurrency=1` + Webshare 串行跑，详见 local_virtual_service/README.md |
| `fetch_youtube_transcripts_batch` | `youtube_fetching` | **2400 / 2100**（40m / 35m）| 2 | 批量补抓字幕，失败派发 ASR |
| `transcribe_youtube_feed_task` | `youtube_transcription` ★ | 由 `.env` 控制 | 1 × 120s | YouTube Fun-ASR 转录（`file_tasks.py` 里写的是 `file_processing` 默认队列，但 `celery_config.CELERY_TASK_ROUTES` 覆盖为 `youtube_transcription`，路由表优先）|

★ `transcribe_youtube_feed_task` 探测视频时长 > 30 min 时会 `apply_async(queue='youtube_transcription_long')` 转发到长队列，详见 `deployment/local_virtual_service/README.md` 的 "长视频自动路由" 一节。

### 6.2 `fetch_all_youtube_subscriptions` 为什么 time_limit=70min

历史值 600s / 540s 是 `concurrency=2` + 直连时代的配置，现在家用 Worker 跑 `concurrency=1` + Webshare 住宅代理（参见 local_virtual_service/README.md "启用 Webshare 住宅代理"）后：

- Webshare 每次 transcript-api 请求 5–30s；
- 批量 ~20 频道 × ~15 视频 + 偶发 ASR fallback；
- 实测整批 30–60 min。

Celery 会把 `@task(time_limit=...)` 作为消息头下发，压过家用 Worker 本地 `.env` 里的全局默认，**所以 70m/60m 写在主项目 stub 上是必须的**，改家用侧 `.env` 无效（local README 的 "⚠ 远端 Worker 的 task time_limit 实际受主项目 stub 控制" 一节有完整说明）。

## 7. YouTube Feed 抓取 & 转录完整流程（涉及本地虚拟机 Worker）

> ⬇ 本节的详细时序、队列划分、字幕/ASR 降级、Webshare 代理、长视频路由、失败重试分类等**全部权威描述**在
> **[`deployment/local_virtual_service/README.md`](../../deployment/local_virtual_service/README.md)**。
> 本节仅提供**从 Feeds 模块视角出发的总览**，不做重复文档化。

### 7.1 为什么需要家用虚拟机 Worker

远程云服务器的 IP 段早已被 YouTube 大面积风控（RSS 连通率极低、transcript-api 批量 429 / IP ban、yt-dlp 被拦在 player endpoint），所以 YouTube 相关的 **所有** 抓取任务（字幕 + 音频下载 + ASR）都下沉到部署在**家庭网络**里的一台独立虚拟机 Worker，仅靠 Redis + PostgreSQL 与主站通信。

该 Worker 的代码独立维护在 `deployment/local_virtual_service/`，同时同步到独立仓库 `solterra2888/LocalVirtualService`，**不依赖主项目 `backend/`**，只复用任务名。

### 7.2 Feeds 与 Upload Link 两条链路共用这台 Worker

本地 Worker 同时服务两条业务（任务名、字幕 fallback、ASR 队列全共用）：

| 链路 | 入口 | 主任务 | 落库目标 |
|------|------|--------|---------|
| **Feed 订阅（本模块）** | `Scheduler / POST /api/subscriptions` | `fetch_all_youtube_subscriptions` / `fetch_youtube_subscription` → `transcribe_youtube_feed_task` | `subscription_content.transcript / transcript_data` |
| **Upload YouTube Link**（非本模块，对比参考）| `/api/files/upload-youtube` | `fetch_youtube_file_transcript_task` → `transcribe_youtube_file_asr_task` → 远程 `finalize_youtube_file_task` | `file_processing_details.content` + 触发 auto-tag/name/classify |

**关键区别**：Feed 链路**没有** finalize 回调、**没有** 自动打标/分类；Upload 链路有。

### 7.3 Feeds 链路在本地 Worker 的大致走向

```
Scheduler (hourly) / POST /api/subscriptions
    │
    ▼
fetch_all_youtube_subscriptions / fetch_youtube_subscription   (queue: youtube_fetching)
    │
    ├─ RSS 拉视频列表
    │    └─ RSS 404/500 → YouTube Data API v3 fallback（主 Key → 备 Key 自动切换）
    │
    ├─ 逐条视频判断是否已在 DB（按 UNIQUE(channel_type, content_id)）
    │
    ├─ 新视频：
    │    Stage 1: youtube-transcript-api（走 /api/timedtext，可选 Webshare 住宅代理）
    │    Stage 2: yt-dlp 字幕 fallback（走 player endpoint，独立限流）
    │    永久错误（私有 / 直播 / 会员专属）→ 跳过
    │    临时错误（429 / IP block / 无字幕）→ 派发 ASR
    │
    └─ 写入 subscription_content（transcript + transcript_data）
                │
                ▼
   （仅失败降级 ASR 时）transcribe_youtube_feed_task   (queue: youtube_transcription)
        │
        ├─ 元信息探测：is_live / is_upcoming → 永久跳过
        ├─ 时长 > 30 min → 转发 youtube_transcription_long（Long Worker）
        └─ 否则：yt-dlp 下载音频 → 阿里云 OSS → DashScope Fun-ASR
                │
                ▼
           SubscriptionContentService.update_transcript
           （写回 subscription_content.transcript / transcript_data，source=asr）
```

> **完整流程图、队列表、日志样例、重试分类、错误码语义、Webshare 启用步骤、所有 `.env` 可调参数（`ASR_LONG_VIDEO_THRESHOLD_SECONDS`、`ASR_POLL_TIMEOUT_MINUTES` / `_LONG`、`WORKER_MAIN_CONCURRENCY`、`YTDLP_IMPERSONATE`、`YOUTUBE_DATA_API_KEY_BACKUP` 等）见：
> [`deployment/local_virtual_service/README.md`](../../deployment/local_virtual_service/README.md)**

### 7.4 用户上传 vs Feed 批量的隔离（priority queues）

本地 Worker 自 v2026-04 起把 "用户上传链路" 与 "Feed 批量链路" 用**独立队列 + 独立 worker 进程**隔离：

| 队列 | 用途 | 属于 |
|------|------|------|
| `youtube_priority` / `youtube_transcription_priority` | Upload YouTube Link（用户交互）| **不属于 Feeds 模块** |
| `youtube_fetching` / `youtube_transcription` | Feed 批量 + 字幕降级 ASR | **属于 Feeds 模块** |
| `youtube_transcription_long` | 长视频共享 | 两条链路共用 |

因此 "用户上传一条 YouTube 链接" 永远不会被 Feeds 的 30–60 min 批量任务排队阻塞，反之亦然。这是理解 local Worker 架构图的关键。

## 8. 前端结构

| 文件 | 作用 |
|------|------|
| `web-frontend/src/pages/FeedReaderPage.tsx` | **桌面版 `/feeds`**：三栏布局（订阅列表 / 内容列表 / 正文），增/改/删订阅 Modal，未读数轮询 |
| `web-frontend/src/pages/MobileFeedsPage.tsx` | 移动端聚合入口 |
| `web-frontend/src/pages/MobileFeedsListView.tsx` | 移动端订阅列表 |
| `web-frontend/src/pages/MobileFeedArticlePage.tsx` | 移动端文章详情（深链）|
| `web-frontend/src/hooks/useSubscriptionFeed.ts` | 加载 / 分页 / 轮询未读的复用 hook（移动端）|
| `web-frontend/src/services/subscriptions.ts` | **全部 REST API 客户端**；TypeScript 类型定义（`Subscription`、`SubscriptionContent` 等）|
| `web-frontend/src/components/feed/FeedArticleBody.tsx` | 正文渲染（按 `channel_type` 切换），YouTube 里提供 "加载字幕" / "手动重转 ASR" 按钮 |
| `web-frontend/src/components/feed/feedChannelConfig.tsx` | 渠道配置：图标、颜色、展示字段映射 |
| `web-frontend/src/components/feed/feedUtils.ts` | 时间格式化、摘要截断等辅助 |
| `web-frontend/src/components/feed/FeedTweetContent.tsx` | Twitter 富文本/媒体/引用树渲染 |
| `web-frontend/src/components/feed/FeedSourceIconPicker.tsx` | 新建订阅时选择图标 |
| `web-frontend/src/pages/SchedulerPage.tsx` + `DataSourceConfigModal.tsx` | 配置 `youtube_subscription` / `twitter_subscription` / `news_feed` 定时任务 |
| `web-frontend/src/pages/SubscriptionsPage.tsx` | **已废弃**，功能合并进 `/feeds`。页面内有跳转提示 |

**典型前端调用序列**（用户进入 `/feeds`）：

1. `GET /api/subscriptions/` → 渲染左侧订阅列表 + 未读数
2. 点击某订阅 → `GET /api/subscriptions/{id}/contents?limit=20&offset=0` → 内容列表
3. 点击某 YouTube 内容 → `GET /api/subscriptions/contents/{content_db_id}/transcript` → 加载全文字幕
4. 已读 → `POST /api/subscriptions/contents/{content_id}/mark-read`（触发 `unread_count` 触发器）
5. 没有字幕 → `POST /api/subscriptions/contents/{content_db_id}/transcribe` → 拿 `task_id` 轮询 `GET /api/subscriptions/tasks/{task_id}/status`
6. 右上角轮询刷未读：定时调用 `GET /api/subscriptions/`

## 9. 与其他模块的边界

| 比较维度 | Feeds 模块（本文档）| 文件上传（UPLOAD_FILES_AND_LLM_ANALYSIS_FLOW）| 新闻分析（NEWS_ANALYSIS_FLOW）|
|---------|--------------------|-----------------------------------------------|-------------------------------|
| 数据源 | 外部平台被动订阅 | 用户主动上传 / 粘贴 YouTube 链接 | EODHD News API + 内置 `news` 表 |
| 主表 | `subscription_content` | `file_processing_details` + `processing_stats` | `news` |
| YouTube 抓取 | 家用 Worker（`youtube_fetching` / `youtube_transcription`）| 家用 Worker（priority 队列 `youtube_priority` / `youtube_transcription_priority`）| — |
| 自动 LLM 打标 / 分类 / 命名 | **否** | **是**（`finalize_youtube_file_task` → `content_tagging` / `content_classify`）| 独立的 `NewsAnalysisService` |
| 个性化标签（`personalized_tags`）| 不自动调用，但可作为用户阅读后手动二次处理入口 | 自动调用（详见 `PERSONALIZED_TAGS_IMPLEMENTATION.md`）| 不参与 |
| 定时触发 | scheduler_service（youtube/twitter/news 三类型）| 无（用户触发）| scheduler_service (news_analysis) |

**如果将来要让 Feeds YouTube 字幕成功后自动打标/分类**，需要在 `subscription_content_service.update_transcript` 成功分支派发类似 `finalize_youtube_file_task` 的回调，并把 `subscription_content` 的 id 适配到 `content_tagging_service`/`content_classify_service`（当前两者只读 `file_processing_details`）。

## 10. LLM 调用详解

Feeds 模块涉及 **四条** 独立的 LLM 链路，相互解耦：3 条自动 / 调度触发 + 1 条用户主动个性化。

### 10.1 Feed 内容打标 & 全局摘要（baseline）

| 维度 | 说明 |
|------|------|
| 服务 | `backend/services/feed_content_tagging_service.py` → `FeedContentTaggingService` |
| LLM 底层 | `LocalLLMAnalysisService`（重构后内部委托 `chat_completion_with_template`，自动写 usage_log）|
| Prompt 模板 | `content_tag_feeds_v1`；module_code = `feed_tagging`，用户可在 Settings 设默认偏好 |
| 支持渠道 | `twitter` / `youtube`（news/rss 本轮不做，直接短路）|
| 输入拼装 | Twitter：`content` + 引用/转推树；YouTube：`title + transcript`，两者均有最小长度门槛与最大字符截断 |
| 输出字段 | `sector` / `sector_en` / `industry` / `industry_en` / `tickers[]` / `concepts[]` / `summary` / `confidence` / `reasoning` |
| 存储表 | `subscription_content_tags`（UPSERT on `subscription_content_id`，**全局共享，跨用户复用**）|
| Celery 任务 | `tag_feed_content`（单条）/ `batch_tag_feed_content`（批量梯度派发）均在 `file_processing` 队列 |
| 触发方式 | 用户在前端手动触发 / 运维批量回填；**不自动随抓取触发** |
| 幂等 | 默认已有标签则跳过，`force=True` 可强制重跑 |
| 环境变量 | `AUTO_FEED_TAG_ENABLED`（开关，默认 true）/ `AUTO_FEED_TAG_CHANNELS`（白名单，默认 twitter,youtube）/ `AUTO_FEED_TAG_MAX_CHARS`（最大输入字符，默认 6000）/ `AUTO_FEED_TAG_MIN_LENGTH_TWITTER` / `AUTO_FEED_TAG_MIN_LENGTH_YOUTUBE` |
| API | `GET/POST/PATCH/DELETE /api/subscriptions/contents/{id}/tags`（见 `backend/api/subscription_content_tags.py`）|

### 10.1.X ⭐ Per-User 个性化 Summary（PR-6/PR-7 新增）

> 来源：[LLM_REFACTOR_PLAN.md §5](./LLM_REFACTOR_PLAN.md#5-阶段-3feed-summary-个性化4-5-工作日)
>
> **设计目标**：在不破坏 Feeds 跨用户内容复用机制的前提下，让每个用户用自己的模板生成专属 summary。

#### 双层模型

```
subscription_content_tags (全局共享, 1:1 with content)        ← 客观分类 + baseline summary
subscription_content_user_summaries (per-user, 1:N with user) ← 主观字段覆盖
```

**关键设计**：客观字段（sector / industry / ticker）保持全局共享，避免不同用户对同一条 feed 标到不同行业导致跨用户筛选体验不一致。仅 summary / reasoning / custom_concepts 实现 per-user。

#### 链路

```
用户在 FeedArticleBody 点 "Personalize" 按钮
    ↓
弹窗选 feed_summary 模板 (UploadAnalysisTemplatePicker moduleCode='feed_summary')
    ↓
POST /api/subscriptions/contents/{id}/personalize
    ↓
personalize_feed_summary_task (Celery, queue: file_processing)
    ↓
FeedSummaryPersonalizationService.generate_for_user(content_id, user_id, runtime_template_id)
    ├─ LLMTemplateResolver: runtime > user_template_preferences[feed_summary] > 抛 ResolveError
    │   (feed_summary 无系统默认; 必须用户主动选模板或先设偏好)
    ├─ FeedContentTaggingService._build_tagging_text() (复用 channel adapter)
    └─ LocalLLMAnalysisService → chat_completion_with_template → usage_log (含 user_id)
    ↓
UPSERT subscription_content_user_summaries (UNIQUE content_id+user_id)
    ↓
前端轮询 GET /personalized-summary 拿结果, 局部刷新 + 显示 ✨ Personalized 徽标
```

#### API（PR-7 新增）

| Method | Path | 作用 |
|--------|------|------|
| `POST` | `/api/subscriptions/contents/{content_id}/personalize` | 异步派发个性化任务 |
| `GET` | `/api/subscriptions/contents/{content_id}/personalized-summary` | 读当前用户的 personalized 行（404 = 无个性化）|
| `DELETE` | `/api/subscriptions/contents/{content_id}/personalized-summary` | 取消个性化（回退到全局）|

#### 列表读取（自动叠加）

`GET /api/subscriptions/{id}/contents` 现在每条返回新字段：

```json
{
  "id": 1025739,
  "title": "...",
  "tags": { "sector": "...", "summary": "...(全局)" },
  "summary_source": "personalized" | "global",
  "personalized_summary": "...(用户自己生成的)",
  "personalized_reasoning": "...(可选, 主观解读)",
  "personalized_template_id": 99,
  "personalized_summary_id": 7,
  "personalized_tagged_at": "2026-04-27T15:30:00"
}
```

实现：`backend/services/subscription_content_service.py::_attach_personalized_summaries`（叠加层模式，零破坏现有 SELECT）。

#### 关键限制

| 限制 | 说明 |
|------|------|
| `feed_summary` 无系统默认模板 | 防止"个性化"沦为"二次跑全局模板"；用户必须先在 PromptTemplatesPage 创建 `feed_summary` 模板，或在 Settings 设偏好 |
| 仅覆盖主观字段 | 客观字段（sector/industry/ticker）从不 per-user，保护跨用户筛选一致性 |
| 不影响其他用户视图 | 用户 A 的个性化 summary 仅 user_id=A 可见，B/C/... 看到的还是全局 baseline |
| 成本可控 | 仅用户主动 "Re-summarize" 才触发，N 条收藏 ≠ N 条订阅 ×N 用户的爆炸 |
| 环境变量 | `FEED_SUMMARY_PERSONALIZATION_ENABLED`（默认 true，紧急回滚开关）|

### 10.2 Twitter Daily Report

两阶段解耦架构，目标是「同一作者同一天只调 LLM 一次，结果全局共享」。

**阶段 A — TwitterAuthorSummaryService**（全局，per-author）

| 维度 | 说明 |
|------|------|
| 服务 | `backend/services/twitter_author_summary_service.py` |
| LLM 底层 | `backend/core/llm_framework::llm_service` |
| Prompt 模板 | `twitter_daily_phase1_default` |
| 输入 | 当日该作者所有 tweets（最多 `MAX_TWEETS_PER_AUTHOR_CHUNK=30` 条/chunk），从 `subscription_content` 拉取 |
| 输出 | `twitter_author_daily_summaries` 表（每作者每天一行，所有订阅该作者的用户共用）|
| Token 上限 | `MAX_TOKENS_OUTPUT_PHASE1=2000` |
| 模板变更检测 | 若 prompt 模板 `updated_at` 晚于已有摘要 `updated_at`，则重新生成 |

**阶段 B — TwitterDailyReportService**（per-user，自动链式触发）

| 维度 | 说明 |
|------|------|
| 服务 | `backend/services/twitter_daily_report_service.py` |
| LLM 底层 | `backend/core/llm_framework::llm_service` |
| Prompt 模板 | `twitter_daily_phase2_default`（用户可通过 `TemplatePreferenceService` 自定义 `twitter_phase2` 偏好）|
| 输入 | 当日所有作者的阶段 A 摘要（引用编号重整为 `[T-001]` 格式）；缺失时自动 fallback 到即时生成并缓存 |
| 输出 | `twitter_daily_reports` 表（每用户每天一行）|
| Token 上限 | `MAX_TOKENS_OUTPUT_PHASE2=4000` |

**调度与 API：**
- 调度：`task_scheduling` 类型 `twitter_author_summary`，阶段 A 执行完后自动链式触发当日各用户的阶段 B
- 手动触发：`POST /reports/twitter-daily/generate`
- 读取：`GET /reports/twitter-daily/latest` / `GET /reports/twitter-daily/{date}`

### 10.3 YouTube Daily Report

结构与 Twitter Daily Report 完全对称，关键参数如下。

**阶段 A — YouTubeAuthorSummaryService**（全局，per-channel）

| 维度 | 说明 |
|------|------|
| 服务 | `backend/services/youtube_author_summary_service.py` |
| Prompt 模板 | `youtube_daily_phase1_default` |
| 输入 | 当日该频道新视频（最多 `MAX_VIDEOS_PER_CHANNEL_CHUNK=10` 条/chunk），`title + transcript`（无字幕则跳过该视频）|
| 输出 | `youtube_author_daily_summaries` 表（所有订阅该频道的用户共用）|
| Token 上限 | `MAX_TOKENS_OUTPUT_PHASE1=3000` |

**阶段 B — YouTubeDailyReportService**（per-user，自动链式触发）

| 维度 | 说明 |
|------|------|
| 服务 | `backend/services/youtube_daily_report_service.py` |
| 输入 | 当日所有频道的阶段 A 摘要 |
| 输出 | `youtube_daily_reports` 表（每用户每天一行）|

**调度与 API：**
- 调度：`task_scheduling` 类型 `youtube_author_summary`，阶段 A 完后自动链式触发阶段 B
- 手动触发：`POST /reports/youtube-daily/generate`
- 读取：`GET /reports/youtube-daily/latest` / `GET /reports/youtube-daily/{date}`

### 10.4 三条链路对比

| 维度 | ① Feed 内容打标 | ② Twitter Daily Report | ③ YouTube Daily Report |
|------|----------------|------------------------|------------------------|
| 粒度 | 单条内容 | 每用户每天 | 每用户每天 |
| 触发 | 手动/批量 | 定时（阶段A→B链式）| 定时（阶段A→B链式）|
| LLM 调用次数 | 每条内容 1 次 | 每作者每天 1 次（阶段A全局共享）→ 每用户 1 次（阶段B）| 每频道每天 1 次（阶段A全局共享）→ 每用户 1 次（阶段B）|
| 存储目标 | `subscription_content_tags` | `twitter_author_daily_summaries` + `twitter_daily_reports` | `youtube_author_daily_summaries` + `youtube_daily_reports` |
| 与订阅内容关系 | 直接标注内容本身 | 汇总内容后二次分析 | 汇总内容后二次分析 |
| 依赖字幕 | YouTube 链路依赖 `transcript` 存在 | 否（仅用 tweet 文本）| 是（无 transcript 的视频会跳过）|

## 11. 配置与环境变量

| 变量 | 位置 | 默认 | 说明 |
|------|------|------|------|
| `YOUTUBE_SUBSCRIPTION_USE_LOCAL` | 远程主服务器 `.env` | `true` | `POST /api/subscriptions` 新建 YouTube 订阅时首抓是否走家用 Worker（`false` = 走远程旧路径紧急回滚；远程服务器现基本无法直连 YouTube）|
| `YOUTUBE_UPLOAD_USE_LOCAL` | 同上 | `true` | （非 Feeds）Upload Link 路径的同类开关 |
| `YOUTUBE_PROXY` / `HTTPS_PROXY` | 远程主服务器 | — | `backend/services/youtube_subscription_service.py` 在远程直连兜底时使用；**不作用于 local Worker，local Worker 有自己的 `.env`** |
| `YOUTUBE_DATA_API_KEY` | 远程 & local | — | YouTube Data API v3 主 Key（RSS 连通率低，此 Key 是主力）|
| `YOUTUBE_DATA_API_KEY_BACKUP` | local `.env` | — | 主 Key 配额耗尽自动切换 |
| `WEBSHARE_PROXY_USERNAME` / `_PASSWORD` / `_FILTER_IP_LOCATIONS` | local `.env` | — | 仅代理 transcript-api 请求，详见 local README |
| `ASR_LONG_VIDEO_THRESHOLD_SECONDS` | local `.env` | `1800` | 触发长视频队列的时长阈值 |
| `ASR_POLL_TIMEOUT_MINUTES` / `_LONG` | local `.env` | `5` / `60` | Fun-ASR 轮询上限（短 / 长队列）|
| `DASHSCOPE_API_KEY` | 远程 `file_tasks.py` 与 local `.env` | — | Fun-ASR 调用 |
| `OSS_ACCESS_KEY_ID` / `OSS_ACCESS_KEY_SECRET` / `OSS_BUCKET_NAME` / `OSS_ENDPOINT` / `OSS_ASR_PUBLIC_READ` | 两端共用 | — | 音频中转 |
| 其他 local `.env` 可调项（Worker 并发、task_time_limit、yt-dlp 超时/重试、curl-cffi impersonation 等）| | | **完整清单见 local README 第 "可调环境变量" 一节** |

## 12. 常见运维场景速查

| 场景 | 排查方向 |
|------|---------|
| 前端添加 YouTube 订阅后长时间没内容 | 1) 远程 API 日志 `fetch_youtube_subscription` 是否派发；2) local Worker `logs/worker.log` 是否消费 `youtube_fetching` 队列；3) Webshare 配置；4) `YOUTUBE_DATA_API_KEY` 是否耗尽配额（日志会出现 "Data API v3 Key #1 配额已用尽"）|
| 某视频始终没有字幕 | 1) `GET /api/subscriptions/contents/{id}/transcript` 看 `source`；2) 若为空：查 local Worker 日志里 `video={id}` 的字幕 fallback 结果（api → yt-dlp → ASR 三段）；3) `POST /api/subscriptions/contents/{id}/transcribe` 手动重转 ASR |
| `fetch_all_youtube_subscriptions` 被 `SIGKILL` | 检查主项目 `backend/tasks/subscription_tasks.py` 上的 `@task(time_limit=...)`，Celery 消息头会压过 local `.env`。详见 local README 的 "⚠ 远端 Worker 的 task time_limit 实际受主项目 stub 控制" |
| 长视频 ASR 超时频繁 | 调高 local `.env` 的 `ASR_POLL_TIMEOUT_MINUTES_LONG` 到 90–120 |
| transcript-api 大面积 IP 封禁 | 按 local README "启用 Webshare 住宅代理" 一节配置 `WEBSHARE_PROXY_USERNAME/PASSWORD` |
| Feed 批量任务堵塞用户上传 | 本身不会，已由 `youtube_priority` / `youtube_transcription_priority` 独立 worker 隔离。若被打破，检查 local Worker 是否正确启动了 Priority Worker 进程（local README "上传插队队列" 一节）|
| 未读数不对 | 检查 DB 里 `update_subscription_unread_count` 触发器是否存在；或手动 `UPDATE subscription_list SET unread_count = ...` 重算 |
| News 订阅新增后无内容 | News 依赖定时 `news_feed` 任务，不走单订阅首抓；确认 `task_scheduling` 里 `news_feed` 是否启用并在正常 tick |

## 13. 相关文档索引

- **[`deployment/local_virtual_service/README.md`](../../deployment/local_virtual_service/README.md)** — YouTube Feed 与 Upload Link 的家用 Worker 权威文档（队列架构、字幕/ASR 降级、Webshare、长视频路由、全部 `.env`）
- `docs/instructions/UPLOAD_FILES_AND_LLM_ANALYSIS_FLOW.md` — 对比：用户上传链路
- `docs/instructions/NEWS_ANALYSIS_FLOW.md` — 对比：新闻 LLM 分析管线（独立于 Feeds 的 `news` 订阅）
- `docs/instructions/SCHEDULER_DEVELOPMENT_GUIDE.md` — Scheduler 任务类型扩展与 `task_scheduling` 表
- `docs/instructions/PERSONALIZED_TAGS_IMPLEMENTATION.md` — 个性化标签（主要服务于 Upload 链路，可选对接 Feeds）
- `docs/instructions/TWITTER_DAILY_REPORT_MANUAL.md` — 基于 Feeds `subscription_content` 的 Twitter 日报
- `backend/database/migrations/create_subscription_tables.sql` / `migrate_to_subscription_content.sql` / `add_unread_count.sql` / `insert_youtube_subscription_task.sql` — DB schema 演进
