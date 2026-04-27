# Daily 聚合视图模块完整文档

## 概述

Daily 模块（前端 `/daily`，是登录后的**默认落地页**）是一个**只读的跨源信息流聚合视图**，把当前用户的两条已有数据链路——

- **Upload Files**：用户主动上传的文件 / Web Link / YouTube Link / 笔记（详见 `docs/instructions/UPLOAD_FILES_AND_LLM_ANALYSIS_FLOW.md`）
- **Feeds 订阅**：被动同步的 Twitter / News / YouTube 等订阅内容（详见 `docs/instructions/FEEDS_MODULE.md`）

——通过同一条按 `published_at` 倒序的时间轴卡片流呈现，并提供"按用户预设的来源类型过滤（Preset）"作为个性化入口。设计上参考 [AI Notes Daily](https://dashboard.cntlabs.io/GeminiBlog/index.html) 的 "Summaries" 视图。

**与 Feeds 模块的核心区别：**

| 维度 | Daily（本文档）| Feeds (`docs/instructions/FEEDS_MODULE.md`) |
|------|----------------|--------------------------------------------|
| 数据来源 | **聚合**（Upload + Feeds 两条链路）| 仅 Feeds 订阅 |
| 写入数据 | **不写**任何业务数据，只读 SQL UNION | 写 `subscription_content`、`subscription_list` 等 |
| LLM 调用 | **零** LLM 调用，仅复用 `file_tags.summary` / `subscription_content_tags.summary` | 三条独立 LLM 链路 |
| 定时任务 | **零** 定时任务 | scheduler 驱动的多种抓取任务 |
| 前端职责 | 跨源时间轴 + 卡片视图 + 来源筛选 + Preset | 订阅 CRUD、阅读器、未读管理 |
| 跳转 | 卡片点击跳到对应详情页（Sources / Feeds）| 自身就是详情入口 |

## 1. 模块全景

```
┌──────────────────────────────────────────────────────────────────┐
│                         Daily 聚合视图模块                         │
└──────────────────────────────────────────────────────────────────┘

【前端入口】
    │
    ├─ 桌面 /daily   → web-frontend/src/pages/DailyPage.tsx
    └─ 移动 /daily   → web-frontend/src/pages/MobileDailyPage.tsx
        │
        ├─ NavBar 第一项（Layout.tsx / MobileBottomNav.tsx）
        ├─ 默认落地页（App.tsx：未指定路径时 Navigate to /daily）
        ├─ 顶部搜索（标题 / 摘要 / 正文 ILIKE）
        ├─ 来源 chip 行（受 Preset 限制）
        ├─ 齿轮 ⚙ → DailyPresetModal（选择/保存 source_subtypes）
        ├─ 按日期分组的卡片列表（DailyCard）
        └─ 滚动到底部 IntersectionObserver → 加载下一页
    │
    ▼
┌──────────────────────────────────────────────────────────────────┐
│      REST API   (backend/api/daily.py,  prefix=/api/daily)       │
└──────────────────────────────────────────────────────────────────┘
    │
    ├─ GET  /api/daily/feed     聚合信息流（分页倒序）
    ├─ GET  /api/daily/stats    各 source_subtype 的卡片计数
    ├─ GET  /api/daily/preset   读取当前用户预设
    └─ PUT  /api/daily/preset   保存预设（UPSERT）
    │
    ▼
┌──────────────────────────────────────────────────────────────────┐
│              数据聚合（SQL UNION ALL，纯只读）                     │
└──────────────────────────────────────────────────────────────────┘
    │
    ├─ Upload 链路：file_processing_details
    │                ⨯ JOIN processing_stats   (鉴权 ps."user")
    │                ⨯ LEFT JOIN file_tags     (LLM 摘要 / tickers / concepts)
    │
    ├─ Feed   链路：subscription_content       (user_id = ANY(user_ids))
    │                ⨯ LEFT JOIN subscription_content_tags  (LLM 摘要 / tickers)
    │
    └─ 单查询批量反查 subscription_list.id
       （为 Feed 卡片解析跳转目标，避免 N+1）
    │
    ▼
┌──────────────────────────────────────────────────────────────────┐
│       卡片点击预览（DailyPreviewDrawer，就地抽屉，不离开页面）       │
└──────────────────────────────────────────────────────────────────┘
    ├─ 桌面 → 右侧 720px Drawer
    └─ 移动 → bottom sheet (92vh)
        │
        ├─ 上半：标题 / 作者 / 时间 / X min read
        ├─ 中段：AI Summary（缺失时降级提示并直接展开下方原文）
        │        + sector / industry / tickers / concepts
        ├─ 下半：原文区，按 source_subtype 分发到 9 种渲染器
        │        (DailyOriginalView 内部 switch，详见 §7)
        ├─ 顶栏：‹ Prev / Next › （键盘 ↑/↓ 或 J/K，桌面）
        ├─ 顶栏：Open in Sources / Open in Feeds   ← 旧跳转路径作为深度入口保留
        └─ 顶栏：Close（Esc）
        │
        ▼ 切换或打开抽屉时调用：
        GET /api/daily/items/{id}    返回 DailyItemDetail
            ├─ upload 子结构：file_processing_details.content + processing_stats.final_report
            └─ feed   子结构：subscription_content + subscription_content_tags（与 Feeds 同形）

【深度入口（顶栏 "Open in …" 才走，普通点击不再跳走）】
    ├─ execution     → /files?execution=<execution_id>          (Upload 详情)
    ├─ feed_article  桌面 → /feeds (state.dailyTarget)
    │               移动 → /feeds/article/:subId/:contentId    (深链)
    └─ external_url  → window.open(url)                         (兜底外链)

【二进制 / 大资源懒加载（只在抽屉打开后按需取）】
    ├─ image / audio / pdf / youtube(upload) → GET /api/files/original/{file_id}
    │                                          (file_url + asr_with_diarization)
    └─ youtube(feed) transcript               → GET /api/subscriptions/contents/{id}/transcript
```

## 2. 数据库表结构

Daily 模块本身**只新增了一张 per-user 偏好表**，其它都是只读消费现有表。

### 2.1 `user_daily_preferences` — Daily 预设（NEW）

定义在 `backend/database/migrations/create_user_daily_preferences.sql`。

| 字段 | 类型 | 说明 |
|------|------|------|
| `user_id` | `int` PK FK → `users(id)` ON DELETE CASCADE | 单用户单行 |
| `source_subtypes` | `text[]` NOT NULL DEFAULT `'{}'` | 用户勾选的 9 种 source_subtype 子集（合法值见 §3）|
| `updated_at` | `timestamp` DEFAULT NOW() | 最近修改时间 |

**语义约定：**

- `source_subtypes = []`（或无记录）→ **未设置 / 等同于"展示全部"**（默认行为）
- 非空数组 → 仅展示这些子类型的内容（chip 行也只显示这些）
- "9 项全选" 在 UI 层等同于"清除"，保存时下发 `[]`

**写入路径：**只通过 `PUT /api/daily/preset` 的 `INSERT … ON CONFLICT (user_id) DO UPDATE`。

### 2.2 只读消费的现有表

| 表 | 来自 | Daily 用途 |
|----|------|-----------|
| `file_processing_details` | Upload Files 链路 | 标题、内容片段、`file_type` → source_subtype 映射、外链 |
| `processing_stats` | 同上 | 鉴权（`ps."user" = current_user.username`），并提供跳转用的 `execution_id` |
| `file_tags` | 同上 | LLM 标签：`summary` / `reasoning` / `sector` / `industry` / `ticker` / `concepts` / `confidence` |
| `subscription_content` | Feeds 链路 | 标题、正文 / transcript、`channel_type` → source_subtype 映射、`author_*`、`url` |
| `subscription_content_tags` | 同上 | LLM 标签：`summary` / `sector` / `industry` / `ticker` / `concepts` / `confidence` |
| `subscription_list` | 同上 | 单查询批量反查 `id`（为 Feed 卡片提供跳转目标）|

### 2.3 `file_tags.summary` 字段补充（NEW）

为了让 Daily 同时显示 Upload 与 Feed 两侧的 AI 摘要，本模块附带一次轻量迁移给 `file_tags` 加了 `summary` 列（`backend/database/migrations/add_summary_to_file_tags.sql`），与 `subscription_content_tags.summary` 对齐。**当前 `content_tag_v1` 模板还未生成该字段**，因此前端会按以下回退链显示文本：

```
file_tags.summary
   ↓ 缺失
file_tags.reasoning  (老数据兜底)
   ↓ 缺失
file_processing_details.content 前 220 字  (snippet)
```

参见 `backend/api/daily.py::_row_to_item` 中的 `summary_text` / `snippet_text` 分支。

## 3. source_subtype 约定（9 种）

前端徽章、Preset 选项、API 查询参数、SQL 过滤都围绕这 9 个统一标识展开。定义在 `backend/schemas/daily.py::SourceSubtype` 与 `web-frontend/src/services/daily.ts::DailySourceSubtype`。

| `source_subtype` | 来源 | 对应 DB 字段值 | 徽章 label | 说明 |
|------------------|------|---------------|-----------|------|
| `document` | upload | `file_type ∈ {pdf, word, excel, csv, document}` | `DOC` | 文档（PDF / Word / Excel / CSV）|
| `image` | upload | `file_type = image` | `IMG` | 图片（OCR 后入 content）|
| `audio` | upload | `file_type = audio` | `AUDIO` | 音频（ASR 后入 content）|
| `youtube` | upload | `file_type = youtube` | `YT` | Upload 上传的 YouTube 链接 |
| `web` | upload | `file_type = web` | `WEB` | Upload 上传的网页链接 |
| `text` | upload | `file_type = text` | `NOTE` | "My Notes / Thoughts"（粘贴文本）|
| `twitter` | feed | `channel_type = twitter` | `X` | Twitter / X 订阅 |
| `news` | feed | `channel_type ∈ {news, rss}` | `NEWS` | News / RSS 订阅 |
| `youtube_feed` | feed | `channel_type = youtube` | `YT` | YouTube 频道订阅 |

> **Upload `youtube` 与 Feed `youtube_feed` 的徽章共享 `YT` label**（用户视角都是 YouTube），但内部 `source_subtype` 区分以保证点击跳转目标正确（Upload → `/files`，Feed → `/feeds`）。两套映射表分别为 `_UPLOAD_FILE_TYPE_MAP`、`_UPLOAD_SUBTYPE_TO_DB_TYPES`、`_FEED_CHANNEL_TYPE_MAP`，在 `backend/api/daily.py` 顶部。

## 4. REST API（`/api/daily`）

挂载在 `backend/main.py`：

```python
app.include_router(daily.router, prefix="/api/daily", tags=["Daily 聚合视图"])
```

全部实现在 `backend/api/daily.py`，全部受 `Depends(get_current_user)` 保护。

| Method | Path | 作用 |
|--------|------|------|
| `GET` | `/feed` | **聚合信息流**：UNION ALL upload + feed，按 `published_at DESC` 分页倒序，每条返回 `DailyFeedItem` |
| `GET` | `/stats` | **顶部统计**：当前过滤条件下每个 `source_subtype` 的卡片数（chip 行 count 用）|
| `GET` | `/preset` | 读取当前用户的 Daily preset；无记录返回 `{ source_subtypes: [] }` |
| `PUT` | `/preset` | UPSERT 当前用户的 Daily preset；空数组 = 清除（等同未设置）|
| `GET` | `/items/{id}` | **卡片详情**（NEW，Drawer 预览用）：返回 `DailyItemDetail` = `DailyFeedItem` + 来源专属 `upload`/`feed` 子结构 |

### 4.1 `GET /api/daily/feed`

| Query 参数 | 类型 | 默认 | 说明 |
|------------|------|------|------|
| `date_from` | ISO 8601 | — | 起始时间 |
| `date_to` | ISO 8601 | — | 结束时间 |
| `source_subtypes` | 多次重复 | — | 过滤来源（多选）；FastAPI 风格：`?source_subtypes=document&source_subtypes=news`，**不是** `[]=` |
| `q` | string ≤200 | — | 标题/摘要/正文 ILIKE 模糊搜索 |
| `tickers` | 多次重复 | — | Ticker 多选过滤（如 `?tickers=NVDA.US`），用 JSONB `?\|` 操作符 |
| `limit` | 1–100 | 30 | 每页条数 |
| `offset` | ≥0 | 0 | 偏移量 |

**响应结构（`DailyFeedResponse`）：**

```json
{
  "items": [
    {
      "id": "upload_123 | feed_4567",      // 复合 ID
      "source": "upload | feed",
      "source_subtype": "document | news | ...",
      "title": "...",
      "summary": "AI 50–200 字摘要 (可能为 null)",
      "snippet": "无摘要时回退展示的内容片段 (可能为 null)",
      "author_name": "Feed 作者展示名 (Upload 永远为 null)",
      "author_identifier": "Feed 作者标识",
      "sector": "GICS 一级行业",
      "industry": "GICS 子行业",
      "tickers": ["NVDA.US", ...],
      "concepts": ["AI Infrastructure", ...],
      "confidence": 0.85,
      "published_at": "2026-04-25T07:00:00Z",
      "reading_time_min": 4,
      "navigate": {
        "type": "execution | feed_article | external_url",
        "execution_id": 42,                // type=execution 时
        "subscription_id": 9, "content_id": 1234,  // type=feed_article 时
        "external_url": "https://..."      // type=external_url 时
      }
    }
  ],
  "total": 158,
  "has_more": true,
  "limit": 30,
  "offset": 0
}
```

### 4.2 `GET /api/daily/stats`

参数同 `/feed` 但**不接受 `source_subtypes`**（统计要给出全部子类型的总量供 chip 展示）。

```json
{
  "total": 158,
  "by_source": [
    { "source_subtype": "news", "count": 80 },
    { "source_subtype": "document", "count": 27 },
    ...
  ]
}
```

### 4.3 `GET / PUT /api/daily/preset`

```json
// GET 响应 / PUT 请求体 / PUT 响应 都是同一形状
{ "source_subtypes": ["document", "news", "twitter"] }
```

- Pydantic 强制 9 种合法字面量，非法值 422 拒绝
- PUT 端会**二次去重 + 过滤非法值**，并保留稳定顺序
- **PUT 空数组 = 清除 preset**（等同未设置 / 展示全部）

### 4.4 `GET /api/daily/items/{item_id}` （Drawer 预览用，NEW）

- `item_id` 形如 `upload_<file_id>` 或 `feed_<content_id>`，与列表 `DailyFeedItem.id` 完全一致
- 内部按前缀分发到 `file_processing_details` 或 `subscription_content` 的单条查询
- **鉴权口径与列表 endpoint 一致**：upload 走 `processing_stats."user"`，feed 走 `%(user_id)s = ANY(sc.user_ids)`
- 非法格式 → 400；不存在或无权 → 404 / 403

**响应结构（`DailyItemDetail`，扩展自 `DailyFeedItem`）：**

```json
{
  // —— 列表条目同形字段 ——
  "id": "upload_123",
  "source": "upload",
  "source_subtype": "document",
  "title": "...",
  "summary": "AI 摘要 (可能为 null)",
  "snippet": "无摘要时回退展示",
  "tickers": ["NVDA.US"], "concepts": [...],
  "sector": "...", "industry": "...",
  "published_at": "...", "reading_time_min": 4,
  "navigate": { "type": "execution | feed_article | external_url", ... },

  // —— 来源专属子结构（二选一非空） ——
  "upload": {
    "file_id": 123,
    "file_type_raw": "pdf",                  // 用于前端区分 PDF / image / audio / youtube …
    "display_name": "...", "filename": "...",
    "original_file_path": "...",             // OSS key / 本地路径 / 外链兜底
    "content": "Markdown 正文 / OCR / 网页正文 / 笔记",
    "final_report": "processing_stats.final_report (执行级 AI 报告)"
  },
  "feed": {
    "content_db_id": 4567,
    "channel_type": "twitter | youtube | news | rss",
    "content_id": "tweet_id / video_id / news guid",
    "title": "...", "content": "...",
    "author_name": "...", "author_identifier": "...",
    "url": "...", "media_urls": [...],
    "metadata": { ... },                     // 平台特定 JSONB (Twitter 互动数 / YouTube 统计 / News 情感)
    "categories": [...],                     // News / RSS tags
    "is_retweet": false, "retweeted_author": null, ...,   // Twitter 特定
    "has_transcript": true,                  // YouTube 是否能懒加载 transcript
    "tags": { "sector": "...", "tickers": [...], "concepts": [...], "summary": "...", ... }
  }
}
```

**注意（不返回的字段）：**

- 二进制 / 大资源（image / audio / pdf 的 `file_url` + audio / youtube 的 ASR JSON）**不在此返回**，前端打开抽屉后按需调 `GET /api/files/original/{file_id}` 懒加载（与现有 Sources 详情页同口径）
- YouTube 的 transcript 同样懒加载 `GET /api/subscriptions/contents/{id}/transcript`，由抽屉里的 "Load transcript" 按钮触发

## 5. 关键 SQL 设计

### 5.1 字段对齐与 UNION ALL

两侧 `SELECT` 各自有 20 列，**列序与类型严格对齐**（见 `backend/api/daily.py` 的 `_UPLOAD_SQL` / `_FEED_SQL` 顶部注释表）。这是 UNION ALL 正确执行的前提。

```sql
WITH upload_items AS (  /* _UPLOAD_SQL + 动态 WHERE */ ),
     feed_items   AS (  /* _FEED_SQL   + 动态 WHERE */ ),
     merged       AS (
        SELECT * FROM upload_items
        UNION ALL
        SELECT * FROM feed_items
     )
SELECT * FROM merged
ORDER BY published_at DESC NULLS LAST
LIMIT %(limit)s OFFSET %(offset)s
```

### 5.2 鉴权约定

- **Upload 侧**：`processing_stats."user" = %(username)s`（按用户名鉴权，与 `/api/files` 一致）
- **Feed 侧**：`%(user_id)s = ANY(sc.user_ids)`（订阅内容跨用户复用，由 `subscription_content.user_ids[]` 持有）

两侧鉴权方式不同是历史遗留，被同一个 endpoint 用 `current_user.username` + `current_user.user_id` 同时下发解决。

### 5.3 动态 WHERE：`_build_filter_clauses`

`backend/api/daily.py::_build_filter_clauses` 同时为两侧拼 WHERE 子句。`source_subtypes` 过滤的关键点：

- 前端传 `document` → 后端展开为 `fpd.file_type IN ('pdf','word','excel','csv','document')`
- 前端传 `news` → 后端展开为 `sc.channel_type IN ('news','rss')`
- 若某侧没有任何匹配 subtype（例如只选了 `twitter`，则 upload 侧理论上没有匹配）→ 该侧拼 `AND FALSE` 短路，避免 `IN ()` 语法错误

### 5.4 N+1 优化：`_resolve_subscription_ids`

Feed 卡片点击需要跳转到 `/feeds/article/:sub_id/:content_id`，而 `subscription_id` 不在 `subscription_content` 上（按 `(channel_type, channel_identifier)` 反查 `subscription_list`）。如果对每条卡片单独查会变成 N+1。

实际做法：当前页所有 Feed 行的 `(channel_type, channel_identifier)` 去重 → **一次** `WHERE channel_identifier = ANY(%s)` 批量查回 → 内存里建 map → 渲染时 O(1) 查表。

### 5.5 摘要回退链

```
1. file_tags.summary  /  subscription_content_tags.summary    (LLM 摘要)
2. file_tags.reasoning                                          (Upload 老数据兜底)
3. LEFT(content, 400) → 截断到 220 字 → "…"                    (snippet)
```

`reading_time_min` 估算：`max(1, ceil(content_length / 400))`，与 AI Notes Daily 的 "X min read" 对齐。

## 6. 后端文件结构

| 文件 | 职责 |
|------|------|
| `backend/api/daily.py` | 所有 Daily API 实现：UNION SQL 拼接、动态 WHERE、N+1 反查、行 → `DailyFeedItem` 转换、Preset CRUD、`GET /items/{id}` 单条详情分发（`_fetch_upload_detail` / `_fetch_feed_detail`）|
| `backend/schemas/daily.py` | Pydantic 模型：`DailyFeedItem` / `DailyNavigate` / `DailyFeedResponse` / `DailySourceCount` / `DailyStatsResponse` / `DailyPresetResponse` / `DailyPresetUpdate` / `DailyItemDetail` / `DailyItemDetailUpload` / `DailyItemDetailFeed`，以及枚举 `SourceType` / `SourceSubtype` |
| `backend/database/migrations/create_user_daily_preferences.sql` | NEW：preset 表 |
| `backend/database/migrations/add_summary_to_file_tags.sql` | NEW：补 `file_tags.summary` 字段，对齐 Feed 侧摘要 |
| `backend/main.py` | 注册 `/api/daily` router |

> Daily 模块**没有**自己的 service 层、没有 Celery 任务、没有 scheduler 任务。所有"业务"都是一段 SQL + 一层数据装配。

## 7. 前端文件结构

| 文件 | 作用 |
|------|------|
| `web-frontend/src/pages/DailyPage.tsx` | 桌面 `/daily` 主页面：标题区 + 搜索 + 来源 chip + 齿轮入口 + 卡片列表 + 无限滚动；卡片点击 → 打开 `DailyPreviewDrawer`（不再 navigate）|
| `web-frontend/src/pages/MobileDailyPage.tsx` | 移动 `/daily`：单列紧凑布局 + 横向滚动 chip 行 + 同样的搜索 / 齿轮 / 无限滚动；卡片点击 → 打开 `DailyPreviewDrawer`（`placement="bottom"`）|
| `web-frontend/src/services/daily.ts` | API 客户端：`listDailyFeed` / `getDailyStats` / `getDailyPreset` / `updateDailyPreset` / `getDailyItemDetail`（NEW）；TS 类型与后端 Pydantic 严格对齐；`buildQueryString` 手工构造 FastAPI 兼容的多值 query（不带 `[]`）|
| `web-frontend/src/components/daily/DailyCard.tsx` | 单条卡片渲染：右上角 `SourceBadge` + 标题 + 摘要 / snippet + tickers / concepts + 时间 + reading time |
| `web-frontend/src/components/daily/SourceBadge.tsx` | 来源徽章：紧凑 / 可点击 / active 状态 / 计数后缀 / Tooltip |
| `web-frontend/src/components/daily/sourceBadgeConfig.ts` | 9 种 subtype 的配色（浅 / 深主题各一组）+ 短代号 + 完整名称 + 显示顺序 |
| `web-frontend/src/components/daily/dailyTheme.ts` | Daily 模块独立调色板（与 Sources / Feeds 风格一致但卡片密度更紧凑）|
| `web-frontend/src/components/daily/DailyPresetModal.tsx` | Preset 配置弹窗：分组（Upload sources / Subscriptions）+ 复用 `SourceBadge` 做整块勾选 + Clear / Select all / Save |
| `web-frontend/src/components/daily/DailyPreviewDrawer.tsx` | **NEW** 卡片点击预览抽屉：桌面右侧 720px / 移动 bottom sheet (92vh)；顶栏 Prev/Next + Open in Sources/Feeds + Close；中段 AI Summary（缺失降级提示）+ tickers / concepts；下半 dispatch 到 `DailyOriginalView`；键盘快捷键 Esc / ↑↓ / J K |
| `web-frontend/src/components/daily/DailyOriginalView.tsx` | **NEW** 原文区分发器：按 9 种 `source_subtype` 路由到内置子组件（`DocumentOriginalView` / `ImageOriginalView` / `AudioOriginalView` / `YoutubeUploadOriginalView` / `WebOriginalView` / `TextNoteOriginalView` / `TwitterOriginalView` / `NewsOriginalView` / `YoutubeFeedOriginalView`）；按需懒加载 `/files/original/{id}` 与 `/subscriptions/contents/{id}/transcript`；只读，不复用 FeedArticleBody 的写入操作 |
| `web-frontend/src/components/daily/dailyPreviewUtils.ts` | **NEW** 抽屉用小工具：`extractYouTubeId` / `flattenAsr`（兼容 sentences / transcript 两种 ASR JSON 形态）/ `formatTimestampMs` |
| `web-frontend/src/App.tsx` | 路由：`<Route index>` → `Navigate to /daily`；`<Route path="daily">` 桌面 / 移动二选一组件挂载；admin 与 404 兜底也跳 `/daily` |
| `web-frontend/src/components/Layout.tsx` | 桌面 NavBar 第一项 `Daily`（`ThunderboltOutlined` 图标）|
| `web-frontend/src/components/mobile/navigation/MobileBottomNav.tsx` | 移动底部导航第一项 `Daily` |

### 典型前端调用序列（用户进入 `/daily`）

1. `getDailyPreset()` → 拿到 `presetSubtypes`，**不立刻**发 feed 请求，而是先存 state
2. `effectiveSubtypes = activeSubtypes.length ? activeSubtypes : (presetSubtypes 非空 ? presetSubtypes : undefined)`
3. `Promise.all([listDailyFeed({...}), getDailyStats({...})])` → 渲染卡片列表 + chip 计数
4. 用户滚动到底部 → IntersectionObserver 触发 `listDailyFeed({ offset: offsetRef.current })` 加载下一页
5. 用户点 chip 收窄 → 重新计算 `effectiveSubtypes` → 重新加载第一页
6. 用户点齿轮 → `DailyPresetModal` 打开 → 保存 → `updateDailyPreset(...)` → 同步 `presetSubtypes` + 清掉落在新 preset 之外的 stale chip 选择 → 重新加载第一页

**卡片点击预览（Drawer）的调用序列：**

1. `setPreviewItem(item)` → `<DailyPreviewDrawer>` 打开
2. 抽屉立刻用列表已有的 `DailyFeedItem` 字段渲染 above-the-fold（标题 / 作者 / 时间 / 已有 summary / tickers），避免空白闪烁
3. `getDailyItemDetail(item.id)` → 拿到 `DailyItemDetail`，刷新 AI Summary 与下半 Original 区
4. 抽屉里的 `DailyOriginalView` 按 `source_subtype` 分发；image / audio / pdf / youtube(upload) 触发 `filesAPI.getOriginalFileInfo(file_id)` 懒加载 `file_url` 与 `asr_with_diarization`
5. youtube(feed) "Load transcript" 按钮触发 `subscriptionsAPI.getSubscriptionContentTranscript(content_db_id)` 懒加载字幕
6. 用户按 `↑/k` 或 `↓/j` → 父更新 `previewItem` → 第 3 步起重新拉新条目；`requestSeqRef` 防止过期请求覆盖最新选中
7. 用户按 `Esc` 或点 `×` → `setPreviewItem(null)` 关闭；点顶栏 `Open in Sources/Feeds` → 走旧的 `navigate(...)` 深度入口

## 8. Preset 功能（核心交互）

Preset 让用户为 Daily 信息流持久化一份"长期偏好"，与"当下临时筛选"分离。完整设计取舍记录在 [Daily 信息流模块完整开发](562814c5-7eda-4737-8fb2-ea0934eb7ee2)。

### 8.1 模式 A：Preset 限制范围 + chip 临时收窄

- **Preset = 长期范围（持久化到 DB）**：决定 chip 行显示哪些类型，决定列表加载哪些类型
- **chip = 临时筛选（仅本次会话）**：在 preset 范围内进一步收窄；离开页面再回来恢复 preset 全显
- 两者是**包含关系**：临时 chip 选择必须是 preset 的子集

### 8.2 状态机

```
presetSubtypes = null         (尚未拉到, gate first load)
   ↓ getDailyPreset()
presetSubtypes = [...]        ([] 等同"全部")
   ↓
effectiveSubtypes = activeSubtypes.length > 0
                       ? activeSubtypes                   // chip 临时收窄优先
                       : (presetSubtypes.length > 0
                            ? presetSubtypes              // preset 范围
                            : undefined)                  // = 全部
   ↓
chipRow = preset.length > 0 ? preset : ALL_9_TYPES        // chip 行被 preset 限制
   ↓
PUT /preset 保存 → setActiveSubtypes(prev → prev ∩ newPreset)  // 清除 stale 临时选择
```

### 8.3 modal 的 "全选" 语义

`DailyPresetModal` 中"9 项全选"在 UI 显示提示 `"All types selected · equivalent to no preset (will clear)"`，保存时**实际下发空数组**，与"未设置"语义统一，避免 DB 里出现两种意义相同的状态。

## 9. 路由与默认落地

```tsx
// web-frontend/src/App.tsx
<Route index element={<Navigate to="/daily" replace />} />     // 登录后默认页
<Route path="daily" element={<DailyComponent />} />            // 桌面 / 移动按 isMobile 切换
// ...
if (!adminUser?.is_admin) return <Navigate to="/daily" replace />  // 普通用户从 admin 入口被踢回
<Route path="*" element={<Navigate to="/daily" replace />} />  // 404 兜底
```

NavBar 顺序统一调整为：`Daily → Sources → Feeds → ...`（详见 `Layout.tsx` 与 `MobileBottomNav.tsx`）。

## 10. 与其他模块的边界

| 比较维度 | Daily（本文档）| Feeds | Upload Files |
|---------|----------------|-------|--------------|
| 数据写入 | **零** | 所有 `subscription_*` 表 | `file_processing_details` / `processing_stats` |
| LLM 调用 | **零** | 内容打标 + Twitter/YouTube Daily Report | auto-tag / auto-name / auto-classify |
| 定时任务 | **零** | youtube/twitter/news_feed 三大类 | 无 |
| Celery 任务 | **零** | `fetch_*_subscription` 系列 | `add_files_to_be_processed_task` 系列 |
| 路由前缀 | `/api/daily` | `/api/subscriptions` | `/api/files` |
| 跳转关系 | 默认**就地预览**（Drawer）；顶栏 "Open in …" 作为深度入口跳到 `/files` 或 `/feeds` | 自身就是详情页 | 自身就是详情页 |
| 鉴权方式 | 同时按 `username`（Upload）和 `user_id`（Feed）| `user_id` | `username` |
| 页面顺序 | 默认落地，NavBar 第一 | NavBar 第三 | NavBar 第二 |
| 卡片交互 | 点击 → 就地 `DailyPreviewDrawer` 预览（顶栏 "Open in …" 才跳出）| 自身详情页 | 自身详情页 |

**未来扩展空间：**

- **Preset 形态升级**：当前是单字段 `source_subtypes TEXT[]`；若后续要加日期范围默认值、tickers 默认值、关键词等，可平滑迁移到 JSONB `config` 列
- **后端复用 Preset**：邮件 / 推送类的"每日摘要"功能可以读 `user_daily_preferences` 决定向用户推送哪些来源，与 UI 一致
- **跨源去重**：当前 UNION ALL 不去重；若同一篇 Web Link 既被 Upload 也被某 News Feed 抓到，会出现两张卡片（按 `published_at` 接近排在一起）。如要 "url 级去重" 需在合并 SQL 上加 `DISTINCT ON (canonical_url)`

## 11. 配置与环境变量

Daily 模块**不引入任何新环境变量**，只复用现有的 DB 连接配置。

需要确认的依赖项（均已存在）：

| 依赖 | 出处 | Daily 用法 |
|------|------|-----------|
| `DB_HOST` / `DB_PORT` / `DB_NAME` / `DB_USER` / `DB_PASSWORD` | `.env` | 通过 `DatabaseManager(get_database_config())` 复用 |
| `get_current_user` JWT 鉴权 | `backend/auth/jwt_utils.py` | 所有 4 个 endpoint 都依赖 |
| `subscription_content.user_ids` 数组的正确维护 | Feeds 模块 | Daily 鉴权直接读 |
| `processing_stats."user"` 字段 | Upload 模块 | Daily 鉴权直接读 |

## 12. 常见运维场景速查

| 场景 | 排查方向 |
|------|---------|
| 用户进入 `/daily` 看不到任何内容 | 1) 是否 Upload + Feed 都为空？`SELECT COUNT(*) FROM file_processing_details fpd JOIN processing_stats ps ON fpd.processing_stats_id = ps.id WHERE ps."user" = '<user>'` 与 `SELECT COUNT(*) FROM subscription_content WHERE <uid> = ANY(user_ids)`；2) Preset 是否过滤太严？`SELECT source_subtypes FROM user_daily_preferences WHERE user_id = <uid>`；3) 看后端日志 `[daily.list_daily_feed] 查询失败` |
| 某条 Upload 文件的卡片点击后跳到 Sources 但找不到 | `processing_stats_id` 在 `file_processing_details` 上为 NULL；这种数据是异常态，应在 Upload 链路修，而不是 Daily 兼容 |
| 某条 Feed 卡片点击没反应 / 跳到外链 | `_resolve_subscription_ids` 反查不到 → `subscription_list` 已被用户删除（但 `subscription_content.user_ids[]` 还含他）；前端会 fallback 到 `external_url`（如有），否则显示 "No navigation target" |
| 顶部 chip 数字与列表对不上 | `/stats` 不接受 `source_subtypes` 参数（按设计），列表如果叠加了 Preset 或临时筛选，二者口径不同。这是预期行为 |
| Preset 保存后 chip 行还是 9 个 | 检查后端是否回写成功：`PUT /api/daily/preset` 应返回 `source_subtypes` 数组；前端 `handlePresetSaved` 必须 `setPresetSubtypes(next)` 才能让 `visibleSubtypeOrder` 重算 |
| `column ft.summary does not exist` | 迁移 `add_summary_to_file_tags.sql` 没跑过；执行 `psql -f backend/database/migrations/add_summary_to_file_tags.sql` |
| 前端搜索框输入立刻发请求 | 应该有 `SEARCH_DEBOUNCE_MS=350` 防抖；检查 `setSearchInput` 与 `setSearchQuery` 的 `useEffect` 是否被改坏 |
| 滚动到底部不触发加载下一页 | `IntersectionObserver` 的 `sentinelRef` 与"滚动兜底" `document.addEventListener('scroll', …, capture=true)` 双保险，二者都不工作时检查 Ant Design Layout 的 Content 是否被改成不可滚动；详见 `DailyPage.tsx` 内的双 useEffect 注释 |
| Daily 用户量大后 SQL 慢 | `_UPLOAD_SQL` / `_FEED_SQL` 的 JOIN 与排序在大表下需要复合索引：`(processing_stats_id, created_at)` on `file_processing_details`、`((user_ids), published_at DESC)` on `subscription_content`（GIN + BRIN 组合）；可分两步：先 EXPLAIN ANALYZE 当前查询定位 |

## 13. 时区契约（系统级，必读）

> **背景**：当前所有应用服务器（含家庭虚拟服务器）部署在香港（IANA `Asia/Hong_Kong`，UTC+8）。
> 用户主要分布在**美国 + 中国大陆**两个时段。
> 如果时间处理不规范，会出现"分组日期与卡片时间显示不一致"、
> "美国用户深夜看到错位 1 天的报告"等用户可见的 BUG。

### 13.1 三层契约

| 层 | 规则 | 实现位置 |
|---|---|---|
| **存储** | DB 中所有 `timestamp [without time zone]` 列存的都是 **UTC**。<br>每条 psycopg2 连接初始化时强制 `SET TIME ZONE 'UTC'`，使 `NOW()` / Python naive `datetime` 入库口径恒为 UTC。 | `backend/database/db_manager.py::get_connection`（`-c TimeZone=UTC`） |
| **传输** | 后端 API 输出 `datetime` 一律带 UTC `tzinfo`，Pydantic 序列化为 ISO 8601（`...Z` / `+00:00`）。<br>从 DB 读出的 naive datetime 必须先经 `ensure_utc()`。 | `backend/utils/datetime_utils.py::ensure_utc`<br>`backend/api/daily.py::_row_to_item` |
| **展示** | 前端按"用户时区"渲染。<br>默认浏览器 `Intl.DateTimeFormat().resolvedOptions().timeZone`，可由 `DashboardSettingsContext.userTimezone` 覆盖。<br>分组 key、分组标题、卡片内时间都走同一个 tz。 | `web-frontend/src/utils/datetime.ts`<br>`web-frontend/src/contexts/DashboardSettingsContext.tsx` |

### 13.2 禁用 / 推荐对照表

| ❌ 禁止 | ✅ 推荐 | 原因 |
|---|---|---|
| `datetime.now()`（Python） | `utcnow()` from `backend/utils/datetime_utils` | 服务器在 HK，naive `datetime.now()` 比 UTC 大 8h，写入 naive UTC 列时会偏移 |
| `datetime.utcnow()`（Python） | `utcnow()`（aware）或 `utcnow_naive()` | 3.12 已 deprecated，且不带 tzinfo 容易误用 |
| `date.today()` 算"昨天" | `yesterday_in_tz(tz)` | `date.today()` 取 server local（HK）；美国用户 PST 23:00 时 server 已是次日，会拿错日期 |
| 前端 `new Date(iso).toLocaleDateString('en-US', ...)` | `formatDateLabel(iso, tz)` | 写死 `'en-US'`，缺乏一致 tz 入口 |
| 前端 `iso.slice(0, 10)` 当分组 key | `dateKeyInTz(iso, tz)` | 切的是 ISO 字符串里的 UTC 日期，与卡片显示的 local 时间错位 |
| `WHERE published_at >= '2026-04-25'`（依赖 server local 解释） | `WHERE published_at >= %s AND published_at < %s` 配合 `day_window_utc(date, tz)` | 自然日窗口必须显式按用户 tz 换算 |

### 13.3 Daily Report 的"昨天"

`POST /api/reports/twitter-daily/generate` / `POST /api/reports/youtube-daily/generate`
新增 `tz` 字段（IANA 名）：

```json
{ "report_date": null, "tz": "America/Los_Angeles", "force": false }
```

- 不传 `report_date` 时，按 `yesterday_in_tz(tz)` 算；不传 `tz` 时回退 UTC（不再用 server local）。
- 前端在调用时应传 `tz: useDashboardSettings().userTimezone`。
- Scheduler / cron 路径同样切到 `yesterday_in_tz(params.get('tz'))`，默认 UTC。
  若需要"按某用户时区生成"，应在 schedule params 显式传 `tz`。

### 13.4 待办（schema 长期演进）

下列工作不在本次内：
- [ ] 把 `subscription_content.published_at` / `file_processing_details.created_at` 等核心列从
  `timestamp without time zone` 迁移到 `timestamptz`。
  迁移 SQL 模板：
  ```sql
  ALTER TABLE subscription_content
    ALTER COLUMN published_at TYPE timestamptz USING published_at AT TIME ZONE 'UTC';
  ```
  迁移后可去掉 `_row_to_item` 中的 `ensure_utc` 包装（驱动会自动带 tz），并把
  `db_manager` 的 `SET TIME ZONE 'UTC'` 改为防御性配置（仍建议保留）。
- [ ] `twitter_daily_reports.report_date` / `youtube_daily_reports.report_date` 当前是 `DATE` 列，
  天然不带 tz。若未来要支持"用户 A 的 4-25 报告"和"用户 B 的 4-25 报告"分别落库（同一作者两版），
  需要新增 `report_date_tz`（IANA 名）共同作为复合唯一键。

## 14. 相关文档索引

- `docs/instructions/UPLOAD_FILES_AND_LLM_ANALYSIS_FLOW.md` — Upload Files 模块（数据来源 ①）
- `docs/instructions/FEEDS_MODULE.md` — Feeds 订阅模块（数据来源 ②）
- `docs/instructions/NEWS_ANALYSIS_FLOW.md` — News 独立分析管线（与 Daily 无直接耦合）
- `backend/database/migrations/create_user_daily_preferences.sql` — Preset 表
- `backend/database/migrations/add_summary_to_file_tags.sql` — Upload 摘要字段补齐
- `backend/api/daily.py` — 所有 Daily API（含 SQL）
- `backend/schemas/daily.py` — Pydantic 模型与 source_subtype 枚举
- `backend/utils/datetime_utils.py` — 后端时区工具（utcnow / ensure_utc / yesterday_in_tz / day_window_utc）
- `web-frontend/src/utils/datetime.ts` — 前端时区工具（dateKeyInTz / formatDateLabel / formatTime）
