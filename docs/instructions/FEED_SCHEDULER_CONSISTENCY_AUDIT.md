# Feeds 抓取 Scheduler 一致性排查报告

> 本文档对 7 个 Feeds 抓取类 Scheduler 的处理逻辑进行横向一致性排查,列出差异矩阵与优化建议。
> 排查日期:2026-07-12。排查方式:逐源追踪 `scheduler_service._execute_*_task` → Celery task → Service → DB 写入 → 下游 AI 触发的完整路径。

> **修复状态（2026-07-12，P0+P1 已实施）** — 实现见 [FEEDS_MODULE.md](./FEEDS_MODULE.md) 顶部「2026-07-12 更新」与代码改动。
>
> - **已修复（P0）**：P0-1 YouTube Feed 打标接入（`fetch_youtube_transcripts_batch_task` + `transcribe_youtube_feed_task` → `finalize_youtube_feed_task`，审计漏报的 ASR 路径一并修）；P0-2 `trigger_auto_news_processing` Celery 化 + `backfill_untagged_news_task` 自愈 sweep（防 `save_content` ON CONFLICT 去重后的永久漏标）；P0-3② CCTV `published_at` 北京日→UTC（`day_window_utc`）；P0-4 `backfill_podcast_transcripts_task` + `POST /api/subscriptions/{id}/backfill-transcripts`；P0-5 Substack notes 归一化查找键 + 未命中回退归属。
> - **已修复（P1）**：P1-1/P1-2/P1-3/P1-4 合并 —— 新建 `backend/services/subscription_status_service.py`（`record_fetch_outcome` / `_auto` / `_for_channel`），7 源统一回写 `subscription_list` 状态（`last_fetched_at`=utcnow、`consecutive_failures` 失败自增）+ `subscription_fetch_history`。
> - **核对修正（与原文档判断不同）**：P0-3①（`datetime.now()` 拼 CCTV URL）**非 bug**（HK 服务器 +8 恰为北京日），未改；P1-4 现网**无数据损坏**（`db_manager` 每连接 `SET TimeZone=UTC`，`CURRENT_TIMESTAMP` 已是 UTC），仅统一为 `utcnow()` 硬化；P3-f（Twitter 大小写）**驳回** —— L394/L628 已用 `lower()=lower()`，非缺陷。
> - **已修复（P2/P3 摘选，2026-07-13）**：P2-2 删除死字段 `fetch_interval_minutes` / `next_fetch_at`（修订 `remove_unused_subscription_fields.sql` 仅删这两列，**保留** `priority` / `max_items_per_fetch`）；P3-a 单订阅路径 enabled 校验（YouTube/Podcast `fetch_and_store_*` 跳过 disabled 订阅、不抓取不写状态）+ 微博 `_aggregate_accounts` 用 `NOT EXISTS` 排除 `weibo_accounts.enabled=false` 的博主（管理员禁用生效）。
> - **延后（未做）**：P2（filter_config / force_fetch 语义 / users.is_active）、P3（finalize 改名、分页、Twitter 切片、删陈旧文件）、P2-4（微博/Podcast/News 新建日报 Scheduler）。

**相关文档**

| 文档 | 用途 |
|------|------|
| [FEEDS_MODULE.md](./FEEDS_MODULE.md) | Feeds 订阅模块完整设计契约(存储表、内容复用、抓取链路) |
| [SCHEDULER_DEVELOPMENT_GUIDE.md](./SCHEDULER_DEVELOPMENT_GUIDE.md) | Scheduler 全栈注册与 `task_scheduling` 表 |
| [SUMMARY_SCHEDULER_DEVELOPMENT_GUIDE.md](./SUMMARY_SCHEDULER_DEVELOPMENT_GUIDE.md) | 定期 Summarize(日报)类 Scheduler 统一契约 |
| [SCHEDULER_COMMON_PITFALLS.md](./SCHEDULER_COMMON_PITFALLS.md) | Scheduler 全栈 Checklist 与故障案例 |

---

## 0. 排查范围

7 个 Feeds 抓取 Scheduler(均在 `backend/services/scheduler_service.py` 注册 handler):

| 调度器 | task_type | 入口函数(行号) | 主要 Service |
|--------|-----------|----------------|--------------|
| Twitter | `twitter_subscription` | `_execute_twitter_subscription_task` (940) | `twitter_subscription_service.py` |
| YouTube | `youtube_subscription` | `_execute_youtube_subscription_task` (1123) | `youtube_subscription_service.py` |
| 知识星球 | `zhishixingqiu_subscription` | `_execute_zhishixingqiu_subscription_task` (987) | `zsxq_subscription_service.py` |
| Substack | `substack_subscription` | `_execute_substack_subscription_task` (1035) | `substack_subscription_service.py` |
| 微博 | `weibo_subscription` | `_execute_weibo_subscription_task` (1075) | `weibo_subscription_service.py` |
| Podcast | `podcast_subscription` | `_execute_podcast_subscription_task` (1719) | `podcast_subscription_service.py` |
| News Feed | `news_feed` | `_execute_news_feed_task` (1172) | `eodhd_news_feed_service.py` + `cctv_news_feed_service.py` |

> 说明:Twitter / YouTube / 知识星球 各有独立的「日报摘要」Scheduler(`twitter_author_summary` / `youtube_author_summary` / `zhishixingqiu_summary`),Substack 有 `substack_summary`;它们与抓取 Scheduler 解耦,本文仅在「下游 AI 触发」维度提及。

---

## 1. 调度器分发层:3 种模式

| 模式 | 调度器 | 行为 |
|------|--------|------|
| **A 单任务,传 None=「全部」** | Twitter / 知识星球 / Substack / 微博 | `fetch_*_subscription_task.delay(subscription_id=None, user_id=None, force_fetch=False)`;「处理全部」逻辑在 task 内部 |
| **B 双任务分支** | YouTube / Podcast | 有 `subscription_id`/`user_id` → 单订阅 task;否则 → `fetch_all_*_subscriptions_task.delay()`(批量分支**不传** `force_fetch`) |
| **C 进程内同步,无 Celery** | News | `run_in_executor` 在 scheduler 进程内串行跑 EODHD→CCTV,无 `celery_task_id`,参数命名也不同(`max_results`/`recent_days`/`cctv_*`) |

---

## 2. 一致性矩阵(7 调度器 × 16 维度)

缩写:TW=Twitter · YT=YouTube · 星=知识星球 · SUB=Substack · 微=微博 · POD=Podcast · News=News

| # | 维度 | TW | YT | 星 | SUB | 微 | POD | News |
|---|------|----|----|----|----|----|----|----|
| 1 | 批量过滤 `enabled=true` | ✓ | ✓ | ✓ | ✓(+followed_authors) | ✓⚠忽略 acct.enabled | ✓ | ✓ |
| 2 | 用户活跃检查 | ✗ | ✗(无 is_active 列) | ✗ | ✗ | ✗ | ✗ | ✗ |
| 3 | 增量游标 | `last_fetched_at` 回看窗(≥6h) | ✗ 每次拉最新 N | `recent_topic_ids`(zsxq_groups) | `recent_post/note_ids` | `recent_post_ids`(weibo_accounts) | ✗ 全量 RSS 重抓 | ✗ 滚动 recent_days 窗 |
| 4 | 单订阅冷却/限流 | ✗ | ✗ | ✗ | ✗ | ✗ | ✗ | ✗ |
| 5 | 去重 | 预检 + DB upsert | 仅 DB upsert | 预检 + DB upsert | 预检 + DB upsert | 预检 + DB upsert | 仅 DB upsert | 仅 DB upsert |
| 6 | 内容存储表 | subscription_content | subscription_content | subscription_content | subscription_content | subscription_content | subscription_content | subscription_content |
| 7 | 抓取后打标/向量化 | ✓ inline | ✗ **未触发** | ✓ inline | ✓ inline | ✓ inline | ✓ 经 ASR→finalize(<48h) | ✓ trigger_auto_news_processing |
| 8 | 摘要日报调度器 | ✓ 独立 | ✓ 独立 | ✓ 独立 | ✓ 独立 | ✗ 无 | ✗ 无 | ✗ 无 |
| 9 | `subscription_list` 状态更新 | ✓ 全 | ✓ 部分 | ✗ 不写 | ✗ 不写 | ✗ 不写 | ✓ 部分 | ✗ 不写(仅刷 unread) |
| 10 | `subscription_fetch_history` 写入 | ✓ | ✗ | ✗ | ✗ | ✗ | ✗ | ✗ |
| 11 | `consecutive_failures` | 失败+1 / 成功清零 | 仅清零(死) | N/A | N/A | N/A | 仅清零(死) | N/A |
| 12 | `last_fetched_at` 时区写法 | `utcnow()` 安全 | `CURRENT_TIMESTAMP` ⚠ | per-source 表 | per-source 表 | per-source 表 | `CURRENT_TIMESTAMP` ⚠ | 不写;CCTV `datetime.now()` ⚠ |
| 13 | 并发 | 顺序 | 顺序 | 顺序 | 顺序 | 顺序 | 顺序(ASR 异步) | 顺序 |
| 14 | 错误处理 / Celery 重试 | per-author 续;3×60s | per-channel 续;2×60/120s | per-group 续;2×120s | per-source 续;2×120s | per-account 续;2×120s | per-feed 续;2×60/120s(批量吞异常→重试不触发) | per-article 续;无 Celery/无重试 |
| 15 | 内容过滤 | ✗ 死字段 | 仅 max_items | backfill 1 天窗 | 仅 count cap | backfill 1 天窗 | recent_days7 + max100 | recent_days + max_results |
| 16 | 全局 vs 按用户 | 全局按源 + user_ids fan-out | 同 | 同 | 同 | 同 | 同 | 同 |

**统一项(7 源一致)**

- **存储**:全部写入共享表 `subscription_content`,以 `channel_type` 区分。
- **去重**:统一 `(channel_type, content_id)` 上的 `ON CONFLICT DO UPDATE`(upsert,合并 `user_ids`)。`content_id` 分别 = tweet_id / video_id / topic_id / post_id(+`note_` 前缀)/ post_id / episode guid / 文章 URL。
- **并发**:全部顺序抓取(POD 的 ASR 是 fire-and-forget 经 Celery 跨 worker 并行,但抓取主循环串行)。
- **全局抓取**:全部「按源抓一次 → `user_ids` 数组 fan-out」,非按用户重复抓取。
- **批量过滤**:7 源在批量(定时)路径下全部过滤 `subscription_list.enabled = true`。

---

## 3. 关键不一致问题(按严重度排序)

### 🔴 P0 — 功能性 Bug

#### P0-1 YouTube 抓取后不触发打标/向量化
- **现象**:`fetch_youtube_transcripts_batch_task`(`backend/tasks/subscription_tasks.py:422`)写完 `subscription_content.transcript` 后直接返回,**从不调用** `finalize_youtube_feed_task`(该 task 在 `:500` 专门负责调 `trigger_auto_feed_tagging` → `tag_feed_content` + 向量化)。
- **根因**:`finalize_youtube_feed_task` 目前**仅被 Podcast 转录路径调用**(`subscription_tasks.py:1043-1047`),YouTube 字幕批处理路径未接入。Twitter / 知识星球 / Substack / 微博 / News 都在抓取后直接 `trigger_auto_feed_tagging`,唯独 YouTube 缺失。
- **影响**:YouTube feed 内容永远没有 AI 标签 / 摘要 / 向量嵌入,跨渠道筛选(`tickers=`/`concepts=`)与语义检索覆盖不到 YouTube。
- **修复方向**:在 `fetch_youtube_transcripts_batch_task` 成功写完字幕后派发 `finalize_youtube_feed_task`(或直接 `trigger_auto_feed_tagging`),与 Podcast 对齐。

#### P0-2 News 打标触发是同步函数,进程中断会静默丢打标
- **现象**:`trigger_auto_news_processing`(`backend/tasks/file_tasks.py:532`)是普通同步 Python 函数(非 Celery task),在 scheduler 进程内执行,内部再 `.delay()` 真正的 `tag_feed_content` / `map_news_tags` / `vectorize_feed_content`。
- **影响**:若 scheduler 进程在「DB insert 新内容」与「调用 `trigger_auto_news_processing`」之间被杀/重启,新内容**静默跳过**打标与向量化,且无任何恢复或补漏路径。
- **修复方向**:改为入库后立即 `.delay()` 一个独立 Celery task 来承载触发逻辑;或至少保证 insert 与 trigger 的原子性/补漏机制。

#### P0-3 CCTV 时区双重错误
- **现象①**:`CCTVNewsFeedService` 用 `datetime.now()`(`cctv_news_feed_service.py:195`)而非 `utcnow()`——这正是 EODHD 侧注释(`eodhd_news_feed_service.py:99`)明确警告的 HK 本地时钟 bug。
- **现象②**:把北京时间的 `published_at` 误标 `+00:00`(`cctv_news_feed_service.py:369`),即把 +8 时间当 UTC 存储。
- **影响**:CCTV 文章时间偏移、日报日期窗口错位。
- **修复方向**:统一 `utcnow()`;`published_at` 按北京时区解析后转 UTC 存储。

#### P0-4 Podcast >48h 新剧集永不处理
- **现象**:仅 `PODCAST_ASR_RECENT_HOURS=48h`(`podcast_subscription_service.py:60-61`)内的剧集被转写/打标;超过 48h 的新入库剧集只存正文,永不转写、不打标、不向量化。
- **影响**:新订阅某播客时,历史剧集(>48h)全部无 AI 处理;无 catch-up 路径。
- **修复方向**:增加历史回填入口(手动或批量任务)。

#### P0-5 Substack notes 孤儿(user_ids=[])
- **现象**:`_crawl_notes_for_author` 用 `user_ids_by_pub.get(pub_url, [])` 反查订阅用户(`substack_subscription_service.py:573`)。若某 followed author 的 `publication_url` 与任何已订阅 publication 不匹配,其 note 以 `user_ids=[]` 入库。
- **影响**:该 note 不属于任何用户的 feed,仅全局可查。
- **修复方向**:按 author 直接关联订阅用户,而非经 publication_url 间接反查。

---

### 🟠 P1 — 一致性分裂

#### P1-1 `subscription_list` 状态维护三组分裂(最系统性)
| 组 | 调度器 | 行为 |
|----|--------|------|
| 组1 完整 | **TW** | 写 `last_fetched_at` / `last_fetch_items_count` / `total_items_fetched` / `consecutive_failures`(失败自增、成功清零)+ 写 `subscription_fetch_history` |
| 组2 部分 | **YT、POD** | 写 `subscription_list` 但 `consecutive_failures` 仅清零不自增、`subscription_fetch_history` 从不写 |
| 组3 完全不写 | **星球、SUB、微博、News** | 状态写在 per-source 表(`zsxq_groups` / `substack_publications`+`substack_followed_authors` / `weibo_accounts` / 无),`subscription_list` 一字不动(News 仅刷 `unread_count`) |

- **影响**:`subscription_list.last_fetched_at` 有索引用于调度优先级排序(`add_performance_indexes.sql:91`)。对组2/组3 的源,该字段恒为 NULL/陈旧,任何按 `last_fetched_at` 排序或按 `consecutive_failures` 做健康检查/熔断的逻辑失效或误判。
- **修复方向**:无论是否用 per-source 游标表,每次抓取都应回写 `subscription_list` 的统一状态字段 + 写一行 `subscription_fetch_history`。

#### P1-2 `consecutive_failures` 仅 Twitter 正确
- TW:失败 +1、成功清零(`twitter_subscription_service.py:810-825`)。
- YT / POD:失败路径只 `continue`(`youtube_subscription_service.py:1154-1157`、`podcast_subscription_service.py:656-660`),不自增;字段恒为 0,熔断信号失效。
- 星球 / SUB / 微博 / News:不用 `subscription_list`,该字段 N/A。
- **修复方向**:统一失败自增逻辑。

#### P1-3 `subscription_fetch_history` 仅 Twitter 写
- 仅 `_record_fetch_history`(`twitter_subscription_service.py:850-856`)写入;其余 6 源从不写。
- **影响**:`GET /api/subscriptions/{id}/history` 对非 Twitter 源无数据,跨源抓取审计不一致。
- **修复方向**:所有源在抓取结束写一行 history(fetched/new/updated/skipped/错误/耗时)。

#### P1-4 `last_fetched_at` 时区写法三种
- `utcnow()`(Python 端,安全):TW、News-EODHD。
- SQL `CURRENT_TIMESTAMP`:YT(`youtube_subscription_service.py:966,1139`)、POD(`podcast_subscription_service.py:440,641`)。`last_fetched_at` 列为 naive `TIMESTAMP`(`create_subscription_tables.sql:38`),在 HK worker 未 `SET TIME ZONE='UTC'` 时会写入 +8 值——`subscription_content_service.py:118-123` 已有注释明确警告此问题,但 YT/POD 未采纳。
- `datetime.now()`(HK 本地 bug):CCTV(`cctv_news_feed_service.py:195`)。
- **修复方向**:统一用 Python `utcnow()`(与 `datetime_utils.py` 契约一致)。

---

### 🟡 P2 — 跨全部源的设计缺口

#### P2-1 无用户活跃检查
- 7 源全部不 join `users` 表;`users` 表本身也没有 `is_active` / `status` 列(`backend/auth/user_auth.py:243-250` 仅有 id/username/password_hash/salt/is_admin/created_at/avatar_url/token_version)。
- **影响**:禁用/删除用户的订阅仍被抓取,内容行 `user_ids` 仍累积其 id,浪费 API 配额、污染未读数。
- **修复方向**:在 `users` 表引入 `is_active`,抓取 SQL 统一 join 过滤。

#### P2-2 无单订阅冷却
- `subscription_list.fetch_interval_minutes` / `next_fetch_at` 是死字段(7 源都不读);频率纯靠 scheduler cron + 内联 sleep。
- **修复方向**:要么实现单订阅 `next_fetch_at` 跳过逻辑,要么从 schema/UI 移除该字段。

#### P2-3 `filter_config` / `max_items_per_fetch` 多数未实现
- TW:全死(`max_items_per_fetch`、`filter_config` SELECT 后从不使用)。
- YT / POD / News:仅用 `max_items`。
- 星球 / SUB / 微:仅靠分页/日期窗。
- **修复方向**:统一实现或下线。

#### P2-4 摘要日报覆盖不全
- 有独立日报 Scheduler:TW / YT / 星球 / SUB。
- 无:微博 / Podcast / News。
- **修复方向**:决定是否为缺失源增加日报(与 `daily_market_report` 对齐),否则文档化「无日报」。

#### P2-5 `force_fetch` 语义不一
- TW / 星球:切换 backfill 模式。
- YT / POD:完全 no-op(参数收了不读)。
- SUB:强制拉最新 30 条。
- News:无此概念。
- **修复方向**:统一 `force_fetch` 语义。

---

### ⚪ P3 — 细节问题

- **微博 `weibo_accounts.enabled` 被忽略**:`_aggregate_accounts`(`weibo_subscription_service.py:581-597`)只过滤 `subscription_list.enabled`,管理员在 `/api/admin/weibo/accounts` 禁用某博主对定时抓取无效。
- **YT / POD 单订阅手动路径不检查 `enabled`**:`fetch_and_store_videos`(`youtube_subscription_service.py:848-856`)、`fetch_and_store_episodes`(`podcast_subscription_service.py:300`)按 `id=%s` 取,disabled 订阅仍可被抓。
- **Podcast 复用 YouTube 命名的 `finalize_youtube_feed_task`**(`subscription_tasks.py:1043`):功能共享,命名误导。
- **Substack posts 无真分页**:`get_posts(limit=N)`(`substack_subscription_service.py:517,523`),`INCREMENT_MAX_POSTS=20`;两次抓取间若发布 >20 篇,中间漏抓。
- **微博 backfill 每次从 page 1 重启**:无 cursor 续传,靠 ON CONFLICT 幂等;配合 `BACKFILL_MAX_PAGES=10` 上限,高产博主 backfill 可能永远追不上 page-1 更新速度。
- **YouTube `max_items` 默认值不一致**:schema 默认 20(`create_subscription_tables.sql:27`)、service 默认 15(`youtube_subscription_service.py:888`)、RSS 自然上限 15。
- **Podcast 计数不一致**:单订阅路径用 `episodes_new`(全部新,`:445`),批量路径用 `new_count`(仅 <48h ASR 合格,`:636`)→ `total_items_fetched` / `last_fetch_items_count` 在批量模式下漏计 >48h 新剧集。
- **Twitter `max_pages` 参数失效**:传入 `fetch_tweets_from_api`(`:662`)但函数内从不引用;切片仅由 `since_hours` / `SLICE_HOURS` 驱动。
- **Twitter 单切片 20 条上限静默丢弃**:`has_next_page=True` 仅 warning(`:474-482`),不子切片,超出推文静默漏抓。
- **Twitter 大小写**:`aggregate_subscriptions_by_author` 强制小写(`:296`),DB 存原始大小写,需 `lower(author_identifier)` 比较(`:394,628`)避免误判首抓触发 168h 全量。
- **`scheduler_service_twitter_patch.py` 是陈旧文档**:描述的 ThreadPoolExecutor 方案非现网代码(现网走 Celery `:963`)。

---

## 4. 优化建议(方向,待实现)

> **进度（2026-07-12）**：下方第 1/2/3/4/5/6 项（P0-1/P0-3/P1-1~P1-4/P0-2/P0-4/P0-5）**已实施**，详见顶部「修复状态」与 FEEDS_MODULE.md。第 7/8/9 项（P2-1/P2-2·P2-3/P3）**延后**。

按优先级排列:

1. **【P0-1】修复 YouTube 打标断链** — `fetch_youtube_transcripts_batch_task` 写完字幕后派发 `finalize_youtube_feed_task`,与 Podcast 对齐。*(影响最大、改动最聚焦)*
2. **【P0-3】CCTV 时区修正** — `datetime.now()` → `utcnow()`;`published_at` 按北京时区解析后转 UTC。
3. **【P1-1 / P1-2 / P1-3 / P1-4】统一 `subscription_list` 状态契约** — 抽取公共 `_update_subscription_status(sub_id, success, new_count, error)` + `_record_fetch_history(...)` helper,7 源统一调用;时区统一 `utcnow()`;失败自增 `consecutive_failures`。
4. **【P0-2】News 打标触发改 Celery task** — 入库后 `.delay()`,避免进程内同步断裂。
5. **【P0-4】Podcast 历史 catch-up** — 增加 >48h 剧集的转写/打标回填入口。
6. **【P0-5】Substack notes user_ids 反查修正** — 按 author 直接关联订阅用户。
7. **【P2-1】用户活跃检查** — `users` 表加 `is_active`,抓取 SQL 统一 join。
8. **【P2-2 / P2-3】死字段清理** — `fetch_interval_minutes` / `next_fetch_at` / `filter_config` 要么实现要么下线。
9. **【P3】单订阅路径补 `enabled` 校验;微博补 `weibo_accounts.enabled`;`force_fetch` 语义统一;YouTube `max_items` 默认值统一;Podcast 计数修正;Twitter `max_pages` / 20 条上限子切片。**

---

## 5. 各源关键代码定位

| 源 | 调度入口 | Celery task | Service | 状态写入位置 |
|----|----------|-------------|---------|--------------|
| TW | `scheduler_service.py:940` | `subscription_tasks.py:22`(`fetch_twitter_subscription`, data_fetching, 3×60s, 900/780s) | `twitter_subscription_service.py:551` | `subscription_list`(`:795-827`)+ `subscription_fetch_history`(`:850-856`) |
| YT | `scheduler_service.py:1123` | `subscription_tasks.py:304`(单)/ `:371`(批量, youtube_fetching, 2×60/120s) | `youtube_subscription_service.py` | `subscription_list`(`:963-971`/`:1136-1144`);history 不写 |
| 星 | `scheduler_service.py:987` | `subscription_tasks.py:552`(`fetch_zhishixingqiu_subscription`, data_fetching, 2×120s, 3600s) | `zsxq_subscription_service.py:660` | `zsxq_groups`(`:478-484`);`subscription_list` 不写 |
| SUB | `scheduler_service.py:1035` | `subscription_tasks.py:634`(`fetch_substack_subscription`, data_fetching, 2×120s, 3600s) | `substack_subscription_service.py:628` | `substack_publications`/`substack_followed_authors`(`:304-361`);`subscription_list` 不写 |
| 微 | `scheduler_service.py:1075` | `subscription_tasks.py:718`(`fetch_weibo_subscription`, data_fetching, 2×120s, 3600s) | `weibo_subscription_service.py:741` | `weibo_accounts`(`:470-504`);`subscription_list` 不写 |
| POD | `scheduler_service.py:1719` | `subscription_tasks.py:858`(批量, data_fetching, 2×120s) | `podcast_subscription_service.py:524` | `subscription_list`(`:638-646`);history 不写 |
| News | `scheduler_service.py:1172` | 无 Celery(进程内 `run_in_executor`) | `eodhd_news_feed_service.py:54` + `cctv_news_feed_service.py:75` | 仅 `unread_count`(`eodhd:288-291`/`cctv:431-434`);`subscription_list` 状态不写 |

---

## 6. 版本记录

| 日期 | 说明 |
|------|------|
| 2026-07-12 | 初版:7 源 16 维度一致性矩阵 + P0/P1/P2/P3 分级问题 + 优化方向(仅排查,未改代码) |
| 2026-07-12 | **P0+P1 修复实施**:P0-1/P0-2/P0-3②/P0-4/P0-5 + P1-1~P1-4(统一 `subscription_status_service`)。修正 P0-3①/P1-4/P3-f 的误判。详见顶部「修复状态」与 FEEDS_MODULE.md。P2/P3/P2-4 延后。 |
| 2026-07-13 | **P2-2 + P3-a 修复实施**:P2-2 删死字段 `fetch_interval_minutes`/`next_fetch_at`(保留 priority/max_items_per_fetch,修订迁移);P3-a 单订阅路径 enabled 校验 + 微博 `weibo_accounts.enabled` 生效(`NOT EXISTS`)。新增真实 DB 集成测试。剩余 P2/P3/P2-4 延后。 |
