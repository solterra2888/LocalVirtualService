# Feeds 内容自动打标 + 摘要 开发方案

> 对标模块：Upload Link 的 auto-tag 链路（`ContentTaggingService` + `tag_file_content_task` + `trigger_auto_tagging`，详见 `docs/instructions/UPLOAD_FILES_AND_LLM_ANALYSIS_FLOW.md` §4.3 / §4.7）。
> 适用范围：`subscription_content` 表中 `channel_type IN ('twitter', 'youtube')` 的内容。**News 本轮不做**（理由见 §12）。

## 1. 目标与非目标

### 目标

- 对 Feeds 入库的每条 Twitter tweet 与 YouTube 视频（有字幕的），**自动**生成：
  - 结构化标签：`sector` / `sector_en` / `industry` / `industry_en` / `tickers` / `concepts` / `confidence` / `reasoning`（与 `file_tags` schema 对齐）
  - **短摘要 `summary`**（50–200 中文字符，投资视角单句概括）
- 结果写入独立子表 `subscription_content_tags`（与 `file_tags` 对称，通过 `subscription_content_id` 外键）
- 异步触发、幂等、失败可重试，**不阻塞** Feeds 抓取主流程
- 提供手动重跑 / 编辑 / 删除 / 批量回填 的 API 与前端入口

### 非目标

- 不为 `channel_type='news'` 打标（News 已有独立的 `NewsAnalysisService` 管线，见 `docs/instructions/NEWS_ANALYSIS_FLOW.md`，另做 RFC）
- 不替代现有的作者日报摘要 `twitter_daily_phase1_default` / `youtube_daily_phase1_default`（它们是**作者×日期**粒度，本方案是**单条内容**粒度，两者并存互补）
- 不做 per-user 个性化标签（`subscription_content.user_ids[]` 共享一行，打标也共享；个性化偏好未来另建 `user_content_preferences`）

## 2. 现状盘点（2026-04 真实统计）

| channel_type | 总数 | 中位正文长度 | 备注 |
|--------------|------|-------------|------|
| `twitter` | 21,090 | 114 字符 | is_retweet=4,556 / is_quote=1,567 / 原创 14,967 |
| `youtube` | 2,849 | 1,322（`content` 简介）/ **783 条有 `transcript`** | 只对有字幕的 27% 打标 |
| `news` | 778 | 3,513 | 本轮不动 |

**两个关键实现点**：

1. **Twitter 中位 114 字符**—几乎全是短推文，LLM 打标必须把 `content + [引用] quoted_content + [转推] retweeted_content` 拼成完整上下文才喂，否则纯 RT 或引用推几乎是空值。
2. **YouTube 打标基于 `transcript` 不是 `content`**—`content` 字段存的是视频简介（有 2849 条）；真正能产出标签的是字幕（只 783 条），需要先判空 + 长度门槛。

> **与 `transcript_cleaned` 的区分**（`docs/instructions/YOUTUBE_TRANSCRIPT_CLEANING.md`）：`transcript_cleaned` 是面向**阅读端**的清洗副本；**打标/摘要拼接若仍从 `subscription_content` 直读，默认使用 `transcript` 原始列**。若希望 LLM 输入也使用清洗版，需要在 `FeedContentTaggingService` 等调用处显式改为读 `transcript_cleaned`（并处理 NULL 回退）。

## 3. 架构决策（核心：YouTube Feed 必须走远程回调）

### 3.1 Upload Link 与 Feeds 的对比

| 链路 | 字幕/转录写库的位置 | 自动打标触发点 |
|------|-----------------------|------------------|
| Upload YouTube Link | 家用 Worker 写 → **回调远程 `finalize_youtube_file_task`** → 触发 `trigger_auto_tagging` | 远程 `backend/tasks/file_tasks.py::finalize_youtube_file_task` ~2978 行 |
| **Feed YouTube**（当前）| 家用 Worker 直接 `UPDATE subscription_content`（`deployment/local_virtual_service/worker/services.py::ContentStore.update_transcript` ~L1130）| **无回调，无远程任务，现状 = 远程完全不知情** |

### 3.2 本方案的总体架构

```
┌──────────────────────────── 远程服务器 ─────────────────────────────┐
│                                                                    │
│  Twitter 入库  (backend/services/twitter_subscription_service.py)   │
│  fetch_and_store_tweets                                            │
│     └─ save_tweet_to_db → save_content → (content_id, is_new)      │
│        └─ 收集 new_content_ids                                     │
│           └─ trigger_auto_feed_tagging(new_ids)   ★ hook 1         │
│                                                                    │
│  YouTube Feed 回调入口（新建）                                       │
│  finalize_youtube_feed_task  (queue: file_processing, 新建)         │
│     └─ trigger_auto_feed_tagging([content_id])    ★ hook 2         │
│                                                                    │
│  手动重跑 API                                                        │
│  POST /api/subscriptions/contents/{id}/retag      ★ hook 3         │
│                                                                    │
│  所有 hook 都进入:                                                  │
│    tag_feed_content_task (queue: file_processing, 新建)             │
│      └─ FeedContentTaggingService.tag_content                       │
│         └─ LLM (template: content_tag_feeds_v1)                    │
│         └─ UPSERT subscription_content_tags                        │
└────────────────────────────────────────────────────────────────────┘
         ▲                                         │
         │ app.send_task                           │ UPDATE subscription_content
         │ ("finalize_youtube_feed_task",          │
         │  queue="file_processing")               │
         │                                         │
┌────────┴───────── 家用 Worker（必须新增 1 处回调）─────────────────┐
│                                                                    │
│  ContentStore.update_transcript   (services.py L1130)              │
│     └─ UPDATE subscription_content SET transcript=...              │
│     └─ ★ 新增: 成功后 app.send_task("finalize_youtube_feed_task",   │
│                  args=(content_db_id,), queue="file_processing")   │
└────────────────────────────────────────────────────────────────────┘
```

### 3.3 为什么要新建 `finalize_youtube_feed_task` 而不是让家用 Worker 直接 send `tag_feed_content_task`

1. **职责解耦**：finalize 任务日后可以扩展到 auto-name / auto-classify / 通知用户等，跟 Upload Link 的 `finalize_youtube_file_task` 保持架构对称。
2. **部署边界清晰**：家用 Worker 只知道任务名和队列，不需要知道打标 service 的存在。未来重构 `trigger_auto_feed_tagging` 不用动家用 Worker 代码。
3. **降级开关集中**：`AUTO_FEED_TAG_ENABLED` 只在远程生效，家用 Worker 无需感知。

### 3.4 不改现有 Feeds 抓取链路的原则

所有 hook 都挂在**落库成功的尾部**，且**失败只 log 不 raise**（`trigger_auto_feed_tagging` 本身已有 try/except 包裹，复用 Upload 的模式）。Feeds 抓取的稳定性不被打标功能拖累。

## 4. 数据库设计

### 4.1 新表 `subscription_content_tags`

```sql
-- backend/database/migrations/create_subscription_content_tags.sql

CREATE TABLE subscription_content_tags (
    id                      SERIAL PRIMARY KEY,
    subscription_content_id INTEGER NOT NULL
        REFERENCES subscription_content(id) ON DELETE CASCADE,

    -- 与 file_tags 对齐的字段（2026-05-19 两侧均含 people / countries / tag_schema_version）
    sector          TEXT NOT NULL,
    sector_en       TEXT,
    industry        TEXT NOT NULL,
    industry_en     TEXT,
    ticker          JSONB,                      -- ["AAPL","NVDA"]
    concepts        JSONB DEFAULT '[]'::jsonb,  -- ["AI基础设施","边缘计算"]
    additional_tags JSONB DEFAULT '{}'::jsonb,  -- {tickers_full, ai_metadata}
    confidence      DOUBLE PRECISION
        CHECK (confidence IS NULL OR (confidence >= 0 AND confidence <= 1)),
    reasoning       TEXT,

    -- Feeds 独有
    summary         TEXT,                        -- 50-200 字短摘要
    channel_type    VARCHAR(20) NOT NULL,        -- 冗余便于 WHERE 过滤

    -- 溯源
    tagged_by       INTEGER REFERENCES users(id) ON DELETE SET NULL,
    tagged_at       TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    created_at      TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,

    UNIQUE (subscription_content_id)
);

CREATE INDEX idx_sct_sector_industry ON subscription_content_tags(sector, industry);
CREATE INDEX idx_sct_ticker_gin      ON subscription_content_tags USING gin(ticker);
CREATE INDEX idx_sct_concepts_gin    ON subscription_content_tags USING gin(concepts);
CREATE INDEX idx_sct_channel_type    ON subscription_content_tags(channel_type);
CREATE INDEX idx_sct_tagged_at       ON subscription_content_tags(tagged_at DESC);
```

**不改 `subscription_content` 本身的列**。理由：
- 标签是独立关注点，放子表便于批量重算、删除重建、独立索引
- 既有 21k+ 行加 NOT NULL 成本高，nullable 列又没意义
- 数组型字段（ticker / concepts）在子表上能直接建 GIN 索引，搜索效率更好

### 4.2 Prompt 模板 `content_tag_feeds_v1`

**复制 `content_tag_v1` 再差异化**，写入 `llm_prompt_templates`。

| 字段 | `content_tag_v1` | `content_tag_feeds_v1` |
|------|-----------------|------------------------|
| `template_code` | `content_tag_v1` | `content_tag_feeds_v1` |
| `variables` | `file_content`, `additional_context` | `content_text`, `channel_type`（`twitter`/`youtube`）, `additional_context` |
| `output_format.required` | sector/sector_en/industry/industry_en/tickers/concepts/confidence/reasoning | 同 v1 **+ `summary`** |
| `max_tokens` | 2000 | **1200**（摘要短，字段少） |
| `recommended_model` | 同 v1 | 可配置，Twitter 短文本用更便宜的模型 |

**System Prompt 要点**（直接写进模板 `system_prompt`）：

> 你是金融领域的内容分析助手。输入是来自订阅 feed 的一条**短内容**（推文或视频字幕片段），提炼投资相关信息并返回 JSON。
> - 若 `channel_type == twitter`：内容中 `[引用 @xxx]` / `[转推 @xxx]` 标记作者；Tweet 通常较短，识别不确定时降低 confidence，允许 tickers 为空数组。
> - 若 `channel_type == youtube`：这是视频字幕（可能来自 ASR 有识别误差，如 "NVDA" 可能被错识为 "n via"）；基于语义提炼关键观点，tickers 出现在识别不确定时放弃而不是硬猜。
> - `summary`：50–200 中文字符的**单句**要点，不要描述作者而是内容本身。例："英伟达 Q3 数据中心营收同比增长 200%，GPU 仍供不应求"。

**迁移脚本**：`backend/database/migrations/insert_content_tag_feeds_v1_template.sql`（完整 INSERT 模板行，含 `variables` / `output_format` / `recommended_model`）。

**先做**：在 dev 库用 20 条真实样本（10 Twitter + 10 YouTube 字幕片段）手工调 prompt，达到 ≥80% summary 合理、≥70% tickers 不误识别的门槛再上线。

### 4.3 为什么不复用 `content_tag_v1`

两者 schema 95% 相似，差别只在多一个 `summary`。**权衡**：

| 方案 | 优点 | 缺点 |
|------|------|------|
| 复用 v1，让 summary 成为可选 | 少一份模板 | Prompt 会被两种文本风格污染；未来想只迭代 Feeds 风格需要分叉 |
| **独立 `content_tag_feeds_v1`**（推荐）| Twitter/YouTube 风格提示独立优化；模型/max_tokens 独立 | 多一份模板 |

**选独立模板**。成本只是多维护一行 SQL。

## 5. 打标文本组装（Channel Adapter）

在新服务 `backend/services/feed_content_tagging_service.py` 里实现：

### 5.1 Twitter

```python
def _build_tagging_text(row: dict) -> tuple[str, str] | None:
    """返回 (content_text, channel_type) 或 None（跳过）"""
    parts = []
    main = (row.get('content') or '').strip()
    if main:
        parts.append(main)

    if row.get('is_quote') and row.get('quoted_content'):
        parts.append(f"\n[引用 @{row.get('quoted_author') or ''}]\n{row['quoted_content']}")
    if row.get('is_retweet') and row.get('retweeted_content'):
        parts.append(f"\n[转推 @{row.get('retweeted_author') or ''}]\n{row['retweeted_content']}")

    text = '\n'.join(parts).strip()
    if len(text) < int(os.getenv('AUTO_FEED_TAG_MIN_LENGTH_TWITTER', '50')):
        return None
    return text, 'twitter'
```

**跳过规则**：组装后 < 50 字符（可配）→ 跳过纯表情 / 纯链接 / 纯 "👍 +1"。

### 5.2 YouTube

```python
transcript = (row.get('transcript') or '').strip()
if not transcript:
    return None
if len(transcript) < int(os.getenv('AUTO_FEED_TAG_MIN_LENGTH_YOUTUBE', '200')):
    return None
title = row.get('title') or ''
text = f"{title}\n\n{transcript}" if title else transcript

max_chars = int(os.getenv('AUTO_FEED_TAG_MAX_CHARS', '6000'))
if len(text) > max_chars:
    text = text[:max_chars]
return text, 'youtube'
```

**跳过规则**：无字幕 / 字幕 < 200 字符 → 跳过。  
**截断规则**：与 Upload 对齐的 6000 字符。

### 5.3 News / RSS

`channel_type in ('news', 'rss')` → 直接 `return None`，保留分支位便于将来接入。

## 6. 后端服务层与任务层

### 6.1 新文件 `backend/services/feed_content_tagging_service.py`

类 `FeedContentTaggingService`，方法签名对标 `ContentTaggingService`：

| 方法 | 说明 |
|------|------|
| `get_content(content_id)` | SELECT `subscription_content`（含 quoted/retweeted/transcript/title/channel_type）|
| `_build_tagging_text(row)` | 见 §5，返回 `(text, channel_type)` 或 None |
| `async tag_content(content_id, additional_context="", force=False)` | 主入口：读行 → 组装 → 门槛 → LLM → 保存 |
| `get_existing_tags(content_id)` | SELECT `subscription_content_tags`，用于幂等 |
| `save_tags(content_id, channel_type, tag_data, ai_metadata, tagged_by=None)` | UPSERT `subscription_content_tags` |
| `update_tags(content_id, updates, tagged_by)` | 手工编辑 |
| `delete_tags(content_id)` | 删除 |

**LLM 调用方式**（**复用** `LocalLLMAnalysisService`）：

```python
self.llm_service = LocalLLMAnalysisService(
    template_code='content_tag_feeds_v1',
    log_callback=log_callback,
)
analysis_result = await self.llm_service.analyze_file_content(
    file_content=text,
    file_name=f"feed:{channel_type}:{content_id}",  # 仅日志用
    custom_variables={
        'content_text': text,
        'channel_type': channel_type,
        'additional_context': additional_context,
    },
)
```

**解析输出**：与 `ContentTaggingService.tag_file_content` 同样的 JSON 剥壳 + 校验 `required_fields = ['sector', 'industry', 'tickers']`，**新增校验 `summary` 是否存在**（缺失时允许，记 warning）。

### 6.2 新文件 `backend/tasks/feed_tagging_tasks.py`

```python
@app.task(
    bind=True,
    name='tag_feed_content',
    queue='file_processing',   # 复用 Upload 的 worker 池，不额外起 worker
    autoretry_for=(Exception,),
    retry_kwargs={'max_retries': 3, 'countdown': 30},
    retry_backoff=True,
    retry_backoff_max=120,
    retry_jitter=True,
)
def tag_feed_content_task(self, content_id, username=None,
                          additional_context="", force=False):
    """结构完全对标 tag_file_content_task"""
    # 1. 启用性检查: AUTO_FEED_TAG_ENABLED
    # 2. 异步 run tagging_service.tag_content(...)
    # 3. 幂等: force=False 时若 get_existing_tags() 非空则 return skipped
    # 4. 长度门槛: 由 _build_tagging_text 返回 None 判定
    # 5. 失败 → autoretry_for 兜底

@app.task(
    bind=True,
    name='batch_tag_feed_content',
    queue='file_processing',
)
def batch_tag_feed_content_task(self, content_ids, force=False):
    """存量回填 + 批量重跑。内部对每个 id 独立派 tag_feed_content_task
    (带 countdown 梯度), 避免单次 task 吃满长超时。"""
```

### 6.3 触发钩子 `trigger_auto_feed_tagging`

为了**与 Upload 的运维认知保持对称**，建议放在 `backend/tasks/file_tasks.py`（与 `trigger_auto_tagging` 并列），内部导入 `feed_tagging_tasks` 避免循环引用：

```python
def trigger_auto_feed_tagging(content_ids, username=None, delay=2):
    """Feeds 版 trigger_auto_tagging（见 backend/tasks/file_tasks.py::trigger_auto_tagging）"""
    if os.getenv('AUTO_FEED_TAG_ENABLED', 'true').lower() != 'true':
        info("Feeds 自动打标已禁用", source="feed_auto_tagging")
        return

    allowed = os.getenv('AUTO_FEED_TAG_CHANNELS', 'twitter,youtube').split(',')
    # 注意: 这里不按 channel 过滤（调用方已过滤好）；allowed 只给 service 层作二次兜底

    if not isinstance(content_ids, list):
        content_ids = [content_ids]
    if not content_ids:
        return

    from backend.tasks.feed_tagging_tasks import tag_feed_content_task
    for cid in content_ids:
        try:
            tag_feed_content_task.apply_async(
                args=(cid, username),
                countdown=delay,
            )
            info(f"已触发 Feeds 打标: content_id={cid}, delay={delay}s",
                 source="feed_auto_tagging")
        except Exception as e:
            error(f"触发 Feeds 打标失败: content_id={cid} error={e}",
                  source="feed_auto_tagging")
```

### 6.4 Hook 点 1：Twitter 入库

**现状**：`backend/services/twitter_subscription_service.py::save_tweet_to_db` 只返回 `is_new: bool`，**丢掉了 `content_id`**。

**改动**：

```python
# twitter_subscription_service.py  L486-505

def save_tweet_to_db(self, conn, cursor, tweet, user_ids, subscription_ids):
    try:
        content_id, is_new = self.content_service.save_content(
            channel_type='twitter',
            raw_data=tweet,
            user_ids=user_ids,
        )
        # ... existing log ...
        return content_id, is_new         # ← 改动: 返回 tuple
    except Exception:
        return None, False                # ← 改动
```

在 `fetch_and_store_tweets`（L507+）的入库循环里收集 `new_content_ids = []`，每条 `is_new=True` 时 `new_content_ids.append(content_id)`。循环结束后：

```python
if new_content_ids and 'twitter' in os.getenv(
    'AUTO_FEED_TAG_CHANNELS', 'twitter,youtube'
).split(','):
    from backend.tasks.file_tasks import trigger_auto_feed_tagging
    trigger_auto_feed_tagging(new_content_ids, delay=2)
```

**注意**：`save_tweet_to_db` 当前所有 caller 都在同一文件内（`fetch_and_store_tweets` 的几个分支），修改返回签名影响可控。需要 grep 全仓确认无外部调用。

### 6.5 Hook 点 2：YouTube Feed 字幕完成（跨集群回调）

#### 6.5.1 家用 Worker 改动（**一处**）

`deployment/local_virtual_service/worker/services.py::ContentStore.update_transcript`（L1130 附近）：

```python
def update_transcript(self, content_db_id: int, transcript: str,
                      transcript_data: Dict[str, Any]) -> None:
    with self.db.connection() as conn:
        conn.cursor().execute(
            "UPDATE subscription_content SET transcript=%s, transcript_data=%s WHERE id=%s",
            (transcript, Json(transcript_data), content_db_id),
        )

    # ─── 新增 ───
    # 通知远程: Feed YouTube 字幕已落库, 触发自动打标
    try:
        from .celery_app import app
        app.send_task(
            "finalize_youtube_feed_task",
            args=(content_db_id,),
            queue="file_processing",
        )
        log.info("signal_feed_ready → finalize_youtube_feed_task: content=%d",
                 content_db_id)
    except Exception as e:
        log.warning("signal finalize_youtube_feed_task failed: content=%d error=%s",
                    content_db_id, e)
```

**必须**把同步调用模式选为 "成功后单独 send_task"——不能把打标逻辑直接跑在家用 Worker 上（家用 VM 没有主项目依赖）。

#### 6.5.2 远程新建 `finalize_youtube_feed_task`

放到 `backend/tasks/subscription_tasks.py`（而不是 `file_tasks.py`，因为它服务 Feeds）：

```python
@app.task(
    bind=True,
    name='finalize_youtube_feed_task',
    queue='file_processing',
    max_retries=2,
    default_retry_delay=30,
    time_limit=120,
    soft_time_limit=90,
)
def finalize_youtube_feed_task(self, subscription_content_id: int) -> Dict[str, Any]:
    """
    对标 finalize_youtube_file_task。
    家用 Worker 写完 transcript 后回调此任务, 触发 auto-feed-tagging。
    未来可扩展: auto-name / auto-classify / notify-user。
    """
    from backend.tasks.file_tasks import trigger_auto_feed_tagging
    trigger_auto_feed_tagging([subscription_content_id], delay=2)
    return {'ok': True, 'content_id': subscription_content_id}
```

并在 `backend/celery_config.py::CELERY_TASK_ROUTES` 里加路由：

```python
'finalize_youtube_feed_task': {'queue': 'file_processing'},
```

### 6.6 Hook 点 3：手动重跑 API（见 §7）

### 6.7 幂等性总览

| 场景 | 保护机制 |
|------|---------|
| 同一条内容被多个 hook 同时触发 | `tag_feed_content_task` 内 `force=False` 时 `get_existing_tags()` 非空即 return skipped |
| Twitter 抓取重跑（`ON CONFLICT DO UPDATE`）| `save_content` 返回 `is_new=False`，不入 `new_content_ids`，不再触发 |
| YouTube 字幕被 ASR 覆盖写入第二次 | `update_transcript` 会再次回调 → 走到 task 内幂等短路 |
| 手动 retag (force=True) | UPSERT 覆盖旧行 |
| task 自身失败重试（网络 / LLM 抖动）| `autoretry_for=(Exception,)`, `max_retries=3`, 指数退避 + jitter |

## 7. API 设计

新路由文件 `backend/api/subscription_content_tags.py`，**CRUD 模式完全对标 `backend/api/file_tags.py`**：

| Method | Path | 作用 |
|--------|------|------|
| `GET` | `/api/subscriptions/contents/{content_id}/tags` | 返回单条打标结果（含 summary）|
| `POST` | `/api/subscriptions/contents/{content_id}/retag` | 异步重跑，body `{ force: true, additional_context?: str }`，返回 `{ task_id }` |
| `PUT` | `/api/subscriptions/contents/{content_id}/tags` | 手工编辑 sector / industry / tickers / concepts / summary |
| `DELETE` | `/api/subscriptions/contents/{content_id}/tags` | 删除（前端 "清除标签重新生成" 用）|
| `POST` | `/api/subscriptions/contents/batch-retag` | body `{ content_ids: [...], force: bool }`，batch；返回 `{ batch_task_id }` |
| `GET` | `/api/subscriptions/contents/tags/distinct-tickers` | 返回所有出现过的 ticker（前端筛选自动补全）|
| `GET` | `/api/subscriptions/contents/tags/distinct-concepts` | 同上，concepts |

**同时扩展**现有列表接口：

- `GET /api/subscriptions/{sub_id}/contents` → 在返回里 `LEFT JOIN subscription_content_tags` 追加字段 `tags: { sector, industry, tickers, concepts, summary, confidence } | null`，前端一次拉完不用二次请求。

**任务状态端点**：复用 `GET /api/subscriptions/tasks/{task_id}/status`（已有）。

## 8. 前端集成

| 位置 | 改动 |
|------|------|
| `web-frontend/src/services/subscriptions.ts` | 新增 TS 类型 `SubscriptionContentTags`（镜像 `FileTags` + `summary`）；新增 API 方法：`getContentTags / retagContent / updateContentTags / deleteContentTags / batchRetag / getDistinctTickers` |
| `web-frontend/src/components/feed/FeedArticleBody.tsx` | 正文上方新增 "AI 摘要 / 标签" 卡片：summary + sector badge + industry badge + `ReportTickerTags`（复用）+ concepts chips；右上角 "重新打标" 按钮（loading 灰态 + 轮询）|
| `web-frontend/src/pages/FeedReaderPage.tsx` | 订阅内容列表行下增加 tag chips（仅 sector + top-2 tickers，节省空间）；左侧订阅栏下方增加"按行业 / 按 ticker"筛选（调 `distinct-*` API）|
| `web-frontend/src/pages/MobileFeedArticlePage.tsx` | 同 FeedArticleBody 的移动端布局 |

**前端轮询**：点击"重新打标" → `POST /retag` 拿 `task_id` → 每 2s `GET /api/subscriptions/tasks/{task_id}/status` → 成功后重新请求 `/contents/{id}/tags` 刷新卡片。

## 9. 存量回填策略

启用前先回填 21,090 Twitter + 783 YouTube（有字幕的）。

**脚本**：`backend/scripts/backfill_feed_tags.py`

```bash
# Twitter: dry run 先看总数
python -m backend.scripts.backfill_feed_tags \
    --channel twitter --min-length 50 --dry-run

# 正式跑, 每批 50, 批间 5s, 断点续跑
python -m backend.scripts.backfill_feed_tags \
    --channel twitter --batch-size 50 --batch-interval 5 \
    --state-file /tmp/backfill_twitter.state
```

**内部逻辑**：

1. `SELECT id FROM subscription_content sc LEFT JOIN subscription_content_tags sct ON sc.id = sct.subscription_content_id WHERE sc.channel_type = %s AND sct.id IS NULL AND (...长度过滤...) ORDER BY sc.id`
2. 按 `--batch-size` 切片，每批 `tag_feed_content_task.apply_async(args=(cid,), countdown=batch_idx * batch_interval)`
3. 断点：每 200 条把 `last_id` 写入 `--state-file`，中断后从 `--resume` 读起

**成本估算**（参考值，以模板实际 token 用量为准）：

| 渠道 | 条数 | 平均输入 | 输入 tokens/条 | 输出 tokens/条 | 总 tokens |
|------|------|---------|---------------|----------------|-----------|
| Twitter | 21,090 | 200 字符 | ~500 | ~300 | **~17M** |
| YouTube | 783 | 3000 字符（截 6000）| ~4000 | ~400 | **~3.5M** |

按 qwen-plus / glm-4-plus 的 **$1.5/M input + $3/M output** 粗估，约 **$60–$80 一次性**回填费用。建议：

- 先跑 **100 条 sample** → 人工抽检 sector/tickers/summary 合理率
- 合理率 ≥ 80% 再进全量回填，分 5–10 批，每批间隔 30 min 观察 LLM 账单日报

## 10. 环境变量清单

| 变量 | 默认 | 说明 |
|------|------|------|
| `AUTO_FEED_TAG_ENABLED` | `true` | 总开关，关掉后 `trigger_auto_feed_tagging` 直接 return |
| `AUTO_FEED_TAG_CHANNELS` | `twitter,youtube` | 白名单。灰度时可以只写 `twitter` 单独灰 |
| `AUTO_FEED_TAG_MIN_LENGTH_TWITTER` | `50` | Twitter 组装后最短字符数 |
| `AUTO_FEED_TAG_MIN_LENGTH_YOUTUBE` | `200` | YouTube transcript 最短字符数 |
| `AUTO_FEED_TAG_MAX_CHARS` | `6000` | LLM 输入上限（对齐 Upload 的 `AUTO_TAG_CONTENT_MAX_CHARS`）|
| `AUTO_FEED_TAG_TEMPLATE_CODE` | `content_tag_feeds_v1` | 便于以后灰度切 v2 |
| `AUTO_FEED_TAG_DELAY` | `2` | apply_async countdown，留给 DB 事务提交的余量 |

所有变量读取方式与 Upload 一致：`os.getenv(...)`，无需改 config 文件。

## 11. 分阶段实施 checklist

### Phase 0 — Prompt 调优 + Schema（0.5 天）

- [ ] 写 `create_subscription_content_tags.sql` 迁移，应用到 dev 库
- [ ] 写 `insert_content_tag_feeds_v1_template.sql`，应用到 dev 库
- [ ] 手工用 **10 Twitter + 5 YouTube 字幕** 样本 curl 一次 `chat_completion_with_template`，肉眼核对 sector / tickers / summary 合理性
- [ ] 合格门槛：tickers 无假阳性 ≥ 90%，summary 可读 ≥ 80%

### Phase 1 — 后端服务 + 任务（1 天）

- [ ] `backend/services/feed_content_tagging_service.py`（`FeedContentTaggingService` + `_build_tagging_text`）
- [ ] `backend/tasks/feed_tagging_tasks.py`（`tag_feed_content_task` + `batch_tag_feed_content_task`）
- [ ] `backend/tasks/file_tasks.py::trigger_auto_feed_tagging` 并列添加
- [ ] `backend/tasks/subscription_tasks.py::finalize_youtube_feed_task` 新增 + 在 `celery_config.py` 加路由
- [ ] 单测：3 个 channel adapter 分支 + 2 个幂等场景 + 1 个 LLM mock

### Phase 2 — Hook 接入（0.5 天）

- [ ] `twitter_subscription_service.py::save_tweet_to_db` 返回 tuple；`fetch_and_store_tweets` 收集 `new_content_ids` → `trigger_auto_feed_tagging`
- [ ] **家用 Worker** `ContentStore.update_transcript` 尾部 `send_task("finalize_youtube_feed_task", ...)` —— **同步更新 `deployment/local_virtual_service/` 下的 git subtree**（见 `deployment/local_virtual_service/README.md` "日常更新推送"）
- [ ] 家用 VM `pkill -f celery` + 重启，日志里确认 `signal_feed_ready → finalize_youtube_feed_task` 成功
- [ ] 远程确认 `finalize_youtube_feed_task` 进入 `file_processing` 队列并被消费

### Phase 3 — API + 前端（1 天）

- [ ] `backend/api/subscription_content_tags.py`（7 个端点）
- [ ] 扩展 `GET /api/subscriptions/{id}/contents` LEFT JOIN tags
- [ ] 前端 `services/subscriptions.ts` 类型 + API 方法
- [ ] `FeedArticleBody.tsx` 渲染 summary + tags + retag 按钮（轮询）
- [ ] `FeedReaderPage.tsx` 列表行 tag chips + 行业/ticker 筛选
- [ ] `MobileFeedArticlePage.tsx` 同步

### Phase 4 — 回填（按需，生产）

- [ ] `backend/scripts/backfill_feed_tags.py`，dry-run 对齐预期条数
- [ ] Twitter 先跑 100 sample → 人工抽检 → 全量分 5–10 批
- [ ] YouTube 783 条一次跑完即可（规模小）

### Phase 5 — 上线观察（持续 1 周）

- [ ] 日志面板 `source="feed_auto_tagging"` 的 completed / skipped / failed 比例
- [ ] `SELECT AVG(confidence), COUNT(*) FROM subscription_content_tags GROUP BY channel_type`——confidence 均值 < 0.6 或 skipped > 50% 需重审 prompt
- [ ] `llm_prompt_usage_logs`（如接入）按 `template_code='content_tag_feeds_v1'` 查成本曲线
- [ ] 若 Twitter skip > 50%（门槛过严）→ 考虑把 `AUTO_FEED_TAG_MIN_LENGTH_TWITTER` 降到 30，或对纯 RT 启用 "仅给转推内容打标" 的特殊路径

## 12. 风险与待确认

| # | 风险 | 应对 |
|---|------|------|
| 1 | **Twitter 短内容打标价值低**：中位 114 字符，大量社交闲聊 | Phase 1 上线后观察 skipped 率；>50% 时抬门槛或加语义质量门槛（LLM 判断 "是否含投资信息"）|
| 2 | **YouTube ASR 错别字**：Fun-ASR 把 "NVDA" 识成 "n via" | Prompt 明确要求 "tickers 不确定时放弃"，前端显示时对 low-confidence 加 "⚠ AI 低置信度" 标记 |
| 3 | **家用 Worker 推送延迟 / 失败**：`send_task` 失败不影响 UPDATE，但会漏打标 | 回补脚本 `backfill_feed_tags --only-missing`；监控 `subscription_content` 有 transcript 但 `subscription_content_tags` 无行的数量 |
| 4 | **LLM 账单突增**：上线后 Twitter 每天新增 ~500 条自动触发 | 灰度先只开 `AUTO_FEED_TAG_CHANNELS=twitter`，观察 7 天成本；`AUTO_FEED_TAG_ENABLED=false` 随时一键关停 |
| 5 | **News 未纳入**：News median 3513 字符其实更有打标价值 | 待 Twitter/YouTube 稳定后，开 RFC 评估"News 订阅是否复用 `news_analysis_service` 的分析结果"，避免双 LLM 调用 |
| 6 | **用户隔离**：`user_ids[]` 共享一行，打标也共享，无法做 "某用户不要这个行业" | 本方案明确非目标；未来个性化偏好另建 `user_content_preferences` 表，与标签正交 |
| 7 | **`content_tag_v1` 模板在 DB 不在 seed SQL**：本仓库无 `INSERT` migration | `insert_content_tag_feeds_v1_template.sql` 需要用 **DB 里现有的 v1 行作为参考**（`SELECT ... FROM llm_prompt_templates WHERE template_code='content_tag_v1'` 拷贝后修改写新 INSERT），而不是从本仓库找 |
| 8 | **子集群广播**：`finalize_youtube_feed_task` 必须存在于**远程** worker（`file_processing` 队列），家用 Worker 不消费它 | `backend/celery_config.py::CELERY_TASK_ROUTES` 是权威路由；家用 Worker 的 `tasks.py` 只用 `app.send_task`（按名）不 import 任务函数，**无需**在家用端复制一份 stub |

## 13. 对标 & 引用

**Upload 侧实现（对标来源）**：
- `backend/services/content_tagging_service.py`
- `backend/tasks/tagging_tasks.py`（`tag_file_content_task`）
- `backend/tasks/file_tasks.py`（`trigger_auto_tagging` + `finalize_youtube_file_task` ~L2978）
- `backend/api/file_tags.py`

**Feeds 侧改动对象**：
- `backend/services/subscription_content_service.py`（仅 `save_content` 的 caller 改造收集 new_content_ids，函数本身不动）
- `backend/services/twitter_subscription_service.py::save_tweet_to_db` / `fetch_and_store_tweets`（返回签名 + 循环后 hook）
- `backend/tasks/subscription_tasks.py`（新增 `finalize_youtube_feed_task`）
- `backend/celery_config.py`（新增路由）
- `backend/api/subscriptions.py`（`/contents` 接口扩展 LEFT JOIN 返回 tags）
- `deployment/local_virtual_service/worker/services.py::ContentStore.update_transcript`（**家用 Worker 唯一改动**）
- `web-frontend/src/components/feed/FeedArticleBody.tsx` / `FeedReaderPage.tsx` / `MobileFeedArticlePage.tsx` / `services/subscriptions.ts`

**相关文档**：
- `docs/instructions/FEEDS_MODULE.md`（本方案完成后应回写一个 "§X 自动打标" 子节，引用本文档）
- `docs/instructions/UPLOAD_FILES_AND_LLM_ANALYSIS_FLOW.md`（§4.3 / §4.7 是对标原型）
- `docs/instructions/PERSONALIZED_TAGS_IMPLEMENTATION.md`（与本方案正交，不冲突）
- `deployment/local_virtual_service/README.md`（家用 Worker 部署流程 + subtree push）
