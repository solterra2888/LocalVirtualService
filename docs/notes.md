# 信息
PGPASSWORD='1q23l,Sc45+@!' psql -h 43.99.37.76 -p 5432 -U postgres -d guanhe_database

---

# Feeds 内容自动打标 + 摘要 开发方案

## 1. 背景与目标

**现状**：`content_tag_v1` 打标链路目前只服务于 **文件上传**（`file_processing_details` → `file_tags`），由 `ContentTaggingService` + `tag_file_content_task` 完成（见 `backend/services/content_tagging_service.py` 与 `backend/tasks/tagging_tasks.py`）。Feeds 模块的 Twitter / YouTube 内容入库到 `subscription_content`，但没有任何自动打标/摘要。

**目标**：复用 `content_tag_v1` 的成熟设计，对 Feeds 抓取到的 **Twitter / YouTube** 内容（News 暂不处理）：

- **自动生成结构化标签**：sector / industry / tickers / concepts / confidence
- **新增自动摘要**：summary（50–200 字短摘要，Tweet 直接提炼观点；YouTube 概括字幕要点）
- **写入独立的 `subscription_content_tags` 表**（与 `file_tags` 对称）
- **异步触发，幂等，可手动重跑**，失败不影响主流程
- **只对有效内容打标**：过短的纯 RT、没有字幕的视频、已有标签的内容 → 跳过

## 2. 当前数据盘点（2026-04 真实统计）

| channel_type | 总数 | 中位正文长度 | 备注 |
|--------------|------|-------------|------|
| `twitter` | 21,090 | 114 字符 | 其中 is_retweet=4,556 / is_quote=1,567，原创 14,967 |
| `youtube` | 2,849 | 1,322（`content` 简介）/ 783 条有 `transcript` | 仅 27% 有字幕；打标应基于 `transcript` 而非 `content` |
| `news` | 778 | 3,513 | **本轮不做** |

**关键实现点**：
- Twitter 中位数 114 字符 = 几乎全是短推文，LLM 打标需要把 **`content` + `quoted_content` + `retweeted_content`** 拼成完整上下文再喂，否则纯 RT 或引用推几乎全是空。
- YouTube 要打标的字段是 `subscription_content.transcript`（不是 `content`），需要先判空 + 长度门槛。

## 3. 架构设计（对标 Upload Link → finalize → auto-tag 的模式）

```
┌──────────────────────────────────────────────────────────┐
│                    Feeds 入库（已有）                      │
│                                                          │
│  Twitter:  TwitterSubscriptionService.fetch_and_store    │
│            → SubscriptionContentService.save_content     │
│                                                          │
│  YouTube:  家用 Worker fetch_youtube_subscription         │
│            → caption / yt-dlp / ASR                      │
│            → SubscriptionContentService.update_transcript│
└──────────────────────────┬───────────────────────────────┘
                           │ 新增 Hook
                           ▼
┌──────────────────────────────────────────────────────────┐
│      trigger_auto_feed_tagging(content_ids, …)            │
│      (backend/tasks/feed_tagging_tasks.py 新建)          │
│                                                          │
│      内部: 逐条 .apply_async('tag_feed_content_task',     │
│             queue='file_processing', countdown=2)        │
└──────────────────────────┬───────────────────────────────┘
                           │ Celery
                           ▼
┌──────────────────────────────────────────────────────────┐
│         tag_feed_content_task   (新建)                   │
│         queue='file_processing', max_retries=3,          │
│         retry_backoff, retry_jitter                      │
│                                                          │
│   步骤：                                                  │
│   1) 读 subscription_content 行                          │
│   2) 组装打标文本 (见 §5 Channel Adapter)                 │
│   3) 门槛检查：长度 < N / is_live / 已有标签（非 force）   │
│   4) 调 LLM: template_code='content_tag_feeds_v1'        │
│      (新建模板，复用 v1 的 schema + 新增 summary 字段)    │
│   5) UPSERT subscription_content_tags                    │
└──────────────────────────┬───────────────────────────────┘
                           │
                           ▼
          subscription_content_tags (新表, FK→subscription_content.id)
```

**关键原则**：
1. **不改动现有 Feeds 抓取链路的内部逻辑**，只在落库成功的尾巴 hook 一下 `trigger_auto_feed_tagging`；失败只记日志不 raise。
2. **队列复用 `file_processing`**：与 Upload Link 的 auto-tag 共用 worker 池，不额外起 worker。上限通过 `AUTO_FEED_TAG_ENABLED` 快速开关。
3. **新增独立 tagging service（而非改 ContentTaggingService）**：因为表结构和读取源不同，强行复用会把原服务改花。两者都继承 `LocalLLMAnalysisService` 的调用方式。

## 4. 数据库设计

### 4.1 新表：`subscription_content_tags`

完全镜像 `file_tags`，把 `file_id` 换成 `subscription_content_id`，新增 `summary` 列。

```sql
CREATE TABLE subscription_content_tags (
    id                      SERIAL PRIMARY KEY,
    subscription_content_id INTEGER NOT NULL
        REFERENCES subscription_content(id) ON DELETE CASCADE,

    -- 行业分类（与 file_tags 对齐）
    sector          TEXT NOT NULL,
    sector_en       TEXT,
    industry        TEXT NOT NULL,
    industry_en     TEXT,

    -- 关键 tag 字段
    ticker          JSONB,              -- ["AAPL","NVDA"]
    concepts        JSONB DEFAULT '[]', -- ["AI基础设施","边缘计算"]
    additional_tags JSONB DEFAULT '{}', -- {tickers_full, ai_metadata}
    confidence      DOUBLE PRECISION CHECK (confidence IS NULL OR (confidence >= 0 AND confidence <= 1)),
    reasoning       TEXT,

    -- Feeds 独有字段
    summary         TEXT,               -- 50–200 字短摘要（新能力）
    channel_type    VARCHAR(20) NOT NULL,  -- 冗余存一份，便于按 channel 过滤

    -- 溯源
    tagged_by       INTEGER REFERENCES users(id) ON DELETE SET NULL,
    tagged_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE (subscription_content_id)
);

CREATE INDEX idx_sct_sector_industry ON subscription_content_tags(sector, industry);
CREATE INDEX idx_sct_ticker_gin      ON subscription_content_tags USING gin(ticker);
CREATE INDEX idx_sct_concepts_gin    ON subscription_content_tags USING gin(concepts);
CREATE INDEX idx_sct_channel_type    ON subscription_content_tags(channel_type);
CREATE INDEX idx_sct_additional_gin  ON subscription_content_tags USING gin(additional_tags);
```

**迁移脚本**：`backend/database/migrations/create_subscription_content_tags.sql`

### 4.2 要不要改 `subscription_content` 本身？

**不改**。理由：
- 标签是独立关注点，放子表便于批量重算、独立读取、删除重建。
- 既有 21k+ 行，加 NOT NULL 列成本高；加 nullable 列又没意义。
- `subscription_content.metadata` jsonb 放不进数组型 ticker/concepts 的 GIN 索引语义，搜索不方便。

### 4.3 Prompt 模板：`content_tag_feeds_v1`

**复制 `content_tag_v1`** 写回 `llm_prompt_templates`，差异：

| 字段 | v1 | feeds_v1 |
|------|----|----------|
| `template_code` | `content_tag_v1` | `content_tag_feeds_v1` |
| `variables` | `file_content`, `additional_context` | `content_text`, `channel_type`（twitter/youtube）, `additional_context` |
| `user_prompt_template` | 针对长文档 | 针对 Tweet / YouTube 字幕的风格提示（见下） |
| `output_format.required` | sector/sector_en/industry/industry_en/tickers/concepts/confidence/reasoning | 上面 8 个 **+ `summary`** |
| `recommended_model` | 同 v1 | 可改用更便宜的模型（Twitter 正文极短，小模型足够）|
| `max_tokens` | 2000 | 1200 |

**Prompt 要点**（建议写入模板）：
- System：强调 "这是来自订阅 feed 的短内容（推文或视频字幕），提炼投资相关信息"。
- 如果 `channel_type == 'twitter'`：提示 "tweet 可能包含引用或转推，已在内容中以 `[引用] / [转推]` 前缀标注"。
- 如果 `channel_type == 'youtube'`：提示 "这是视频字幕（可能来自 ASR，有识别误差），基于语义提炼关键观点"。
- 多加一个字段 `summary`：50–200 中文字符的单句摘要。
- 其他字段语义保持与 `content_tag_v1` 一致，便于前端渲染层复用 `ReportTickerTags` / 现有 sector 渲染。

**迁移脚本**：`backend/database/migrations/insert_content_tag_feeds_v1_template.sql`（INSERT 一行，带完整 variables / output_format）。

## 5. Channel Adapter：打标文本怎么组装

新建 `backend/services/feed_content_tagging_service.py::_build_tagging_text`：

### 5.1 Twitter

```python
def _build_tagging_text(row: dict) -> tuple[str, str]:
    """返回 (content_text, channel_type)"""
    parts = []
    main = (row.get('content') or '').strip()
    if main:
        parts.append(main)

    if row.get('is_quote') and row.get('quoted_content'):
        parts.append(f"\n[引用 @{row.get('quoted_author') or ''}]\n{row['quoted_content']}")
    if row.get('is_retweet') and row.get('retweeted_content'):
        parts.append(f"\n[转推 @{row.get('retweeted_author') or ''}]\n{row['retweeted_content']}")

    return '\n'.join(parts).strip(), 'twitter'
```

**跳过规则**：组装后 < `AUTO_FEED_TAG_MIN_LENGTH_TWITTER`（默认 `50`）字符 → skip（纯 "👍" / 纯表情 / 纯链接）。

### 5.2 YouTube

```python
# YouTube 直接用 transcript；没有 transcript 不打标
transcript = (row.get('transcript') or '').strip()
if not transcript:
    return None  # skip
# 可选：附加 title 作为上下文
full = f"{row.get('title') or ''}\n\n{transcript}"
return full, 'youtube'
```

**跳过规则**：`transcript` 为空 或 长度 < `AUTO_FEED_TAG_MIN_LENGTH_YOUTUBE`（默认 `200`）→ skip。  
**截断规则**：超过 `AUTO_FEED_TAG_MAX_CHARS`（默认 `6000`，与 Upload 对齐）→ 截前 6000 字符。

### 5.3 News

本轮不做。方案里留好 `channel_type='news'` 的分支位置，直接 return None。

## 6. 服务层与任务层

### 6.1 `backend/services/feed_content_tagging_service.py`（新建）

类 `FeedContentTaggingService`，方法签名基本对标 `ContentTaggingService`：

| 方法 | 说明 |
|------|------|
| `get_content(content_id)` | SELECT from `subscription_content`，含 quoted/retweeted 字段 |
| `_build_tagging_text(row)` | 见 §5，返回 `(text, channel_type)` 或 None |
| `tag_content(content_id, additional_context='', force=False)` | 异步主入口：取数据 → 组装 → 门槛 → LLM → 保存 |
| `get_existing_tags(content_id)` | SELECT from `subscription_content_tags` |
| `save_tags(content_id, channel_type, tag_data, ai_metadata, tagged_by)` | UPSERT to `subscription_content_tags` |
| `update_tags(...)` / `delete_tags(...)` | 手工编辑用 |

**LLM 调用**：`LocalLLMAnalysisService(template_code='content_tag_feeds_v1').analyze_file_content(file_content=text, file_name=f"feed:{channel_type}:{content_id}", custom_variables={'content_text': text, 'channel_type': ch, 'additional_context': additional_context})`。

> 复用 `analyze_file_content` 不需要改它；`file_name` 只做日志标识。

**解析输出**：与 `ContentTaggingService.tag_file_content` 同样的 JSON 剥壳 + 校验 required 字段，多校验一个 `summary`。

### 6.2 `backend/tasks/feed_tagging_tasks.py`（新建）

```python
@app.task(
    bind=True,
    name='tag_feed_content',
    queue='file_processing',          # 复用 Upload 的 worker 池
    autoretry_for=(Exception,),
    retry_kwargs={'max_retries': 3, 'countdown': 30},
    retry_backoff=True,
    retry_backoff_max=120,
    retry_jitter=True,
)
def tag_feed_content_task(self, content_id, username=None,
                          additional_context="", force=False):
    """
    Feeds 内容自动打标 + 摘要 Celery 任务
    
    结构与 tag_file_content_task 对等，差异：
    - 读 subscription_content 而非 file_processing_details
    - 空内容/过短/已有标签 → skipped
    - 调 FeedContentTaggingService
    """
    ...  # 实现镜像 tag_file_content_task，但字段名换一下


@app.task(bind=True, name='batch_tag_feed_content', queue='file_processing')
def batch_tag_feed_content_task(self, content_ids, ...):
    """批量版本，用于回填存量。"""
    ...
```

### 6.3 触发钩子：`trigger_auto_feed_tagging`

在 `backend/tasks/file_tasks.py`（与 `trigger_auto_tagging` 并列，复用同一个模块便于运维认知）或新文件 `backend/tasks/feed_tagging_tasks.py` 顶部：

```python
def trigger_auto_feed_tagging(content_ids, username=None, delay=2):
    """Feeds 版 trigger_auto_tagging"""
    if os.getenv('AUTO_FEED_TAG_ENABLED', 'true').lower() != 'true':
        info("Feeds 自动打标已禁用", source="feed_auto_tagging")
        return
    if not isinstance(content_ids, list):
        content_ids = [content_ids]
    if not content_ids:
        return
    from backend.tasks.feed_tagging_tasks import tag_feed_content_task
    for cid in content_ids:
        try:
            tag_feed_content_task.apply_async(args=(cid, username), countdown=delay)
            info(f"已触发 Feeds 打标: content_id={cid}", source="feed_auto_tagging")
        except Exception as e:
            error(f"触发 Feeds 打标失败: content_id={cid} error={e}", source="feed_auto_tagging")
```

### 6.4 在哪里 hook

| 入口 | 文件 | 在哪行 hook |
|------|------|------------|
| Twitter 写入 | `backend/services/twitter_subscription_service.py`（或 `SubscriptionContentService.save_content` 的 Twitter 分支）| 成功 INSERT 后收集 `new_content_ids`，函数返回前调 `trigger_auto_feed_tagging(new_content_ids, username=None)` |
| YouTube 字幕 / ASR 完成 | `backend/services/subscription_content_service.py::update_transcript` | 更新成功后，若 `transcript` 非空且长度过门槛，调 `trigger_auto_feed_tagging([content_id])` |
| YouTube Feed ASR 任务尾部 | `backend/tasks/file_tasks.py::transcribe_youtube_feed_task` | 在写入 transcript 的分支尾部加 hook（与 update_transcript 二选一，避免双触发）|
| **手动重跑** | API：`POST /api/subscriptions/contents/{content_id}/retag` | 后端直接 `tag_feed_content_task.apply_async(..., force=True)` |

**重复触发保护**：`tag_feed_content_task` 内部在 `force=False` 时先查 `get_existing_tags`，有就 return skipped。所以上述两个 YouTube hook 即便同时存在也不会重复消费 token。

## 7. API 设计

新建路由文件 `backend/api/subscription_content_tags.py`，挂在 `/api/subscriptions/contents/tags`（或与现有 subscriptions 路由合并）。**CRUD 风格对标 `backend/api/file_tags.py`**：

| Method | Path | 作用 |
|--------|------|------|
| `GET`  | `/api/subscriptions/contents/{content_id}/tags` | 返回单条打标结果（含 summary）|
| `POST` | `/api/subscriptions/contents/{content_id}/retag` | 异步重跑，body `{ force: true, additional_context?: str }`，返回 `task_id` |
| `PUT`  | `/api/subscriptions/contents/{content_id}/tags` | 手工编辑 sector / tickers / concepts / summary |
| `DELETE` | `/api/subscriptions/contents/{content_id}/tags` | 删除标签 |
| `POST` | `/api/subscriptions/contents/batch-retag` | body `{ content_ids: [...], force: bool }`，批量，返回 `batch_task_id` |
| `GET`  | `/api/subscriptions/contents/tags/distinct-tickers` | 给前端自动补全（类似 `/api/file-tags/distinct-tickers`）|
| `GET`  | `/api/subscriptions/contents/tags/distinct-concepts` | 同上，concepts |
| `GET`  | `/api/subscriptions/tasks/{task_id}/status` | 复用已有的 Celery 任务状态端点；若不想复用就再暴露一个 |

**内容列表接口增强**：`GET /api/subscriptions/{sub_id}/contents` 在当前返回里加 `tags` 字段（LEFT JOIN `subscription_content_tags`），前端一次拉完不再二次请求。

## 8. 前端集成

| 位置 | 改动 |
|------|------|
| `web-frontend/src/services/subscriptions.ts` | 新增类型 `SubscriptionContentTags { sector, sector_en, industry, industry_en, tickers, concepts, confidence, reasoning, summary, tagged_at }`；新增 API: `getContentTags / retagContent / updateContentTags / deleteContentTags / batchRetag` |
| `web-frontend/src/components/feed/FeedArticleBody.tsx` | 正文上方渲染 **Summary** 卡片（如果有）+ 标签行（sector / industry / tickers / concepts），tickers 直接复用 `ReportTickerTags` 组件；右上角 "重新打标" 按钮（loading 时灰化）|
| `web-frontend/src/pages/FeedReaderPage.tsx` | 订阅内容列表行下增加轻量 tag chips（仅显示 sector + top2 tickers，节省空间）；可按 ticker 筛选 |
| `web-frontend/src/pages/MobileFeedArticlePage.tsx` | 同 FeedArticleBody，移动端布局 |
| （可选）`FeedReaderPage` 左侧过滤 | 新增 "按行业 / 按 ticker" 筛选，调 `distinct-tickers` / `distinct-concepts` |

**前端轮询**：点击 "重新打标" 后拿到 `task_id`，复用 `GET /api/subscriptions/tasks/{task_id}/status` 每 2s 轮询，成功后刷新 tags。

## 9. 存量回填策略

启用新功能前，存量数据（21k+ Twitter / 783 有字幕的 YouTube）需要一次性打标。

**方案**：写运维脚本 `backend/scripts/backfill_feed_tags.py`：

```bash
python -m backend.scripts.backfill_feed_tags \
    --channel twitter \
    --batch-size 50 \
    --min-length 50 \
    --concurrency 4 \
    --dry-run
```

内部逻辑：
1. 扫 `subscription_content` 按 `channel_type` 过滤、`id` 升序分页。
2. `LEFT JOIN subscription_content_tags` 排除已有标签的行。
3. 按门槛（长度、has_transcript）过滤。
4. 每批 `batch_tag_feed_content_task.apply_async(args=(ids,))`，`countdown = batch_index * 5` 降低瞬时 QPS。
5. 支持断点续跑（记录最后处理的 `content_id` 到本地 state 文件）。

**成本估算**（给参考值，最终以模板实际 token 用量为准）：
- Twitter：组装后平均 200 字符，调用 `qwen-plus` 级别模型约 500 tokens/条 × 21k ≈ 10M tokens。
- YouTube：783 × 平均 3000 字符（字幕）≈ 15 M tokens。
- 总计约 **$20–$50**（按 qwen/glm 的价格区间），可以接受。建议分批跑，先跑 200 条 sample 验证 prompt 效果再全量。

## 10. 环境变量 & 灰度

| 变量 | 默认 | 说明 |
|------|------|------|
| `AUTO_FEED_TAG_ENABLED` | `true` | 总开关；关掉后 `trigger_auto_feed_tagging` 直接 return |
| `AUTO_FEED_TAG_CHANNELS` | `twitter,youtube` | 白名单，便于只灰 Twitter |
| `AUTO_FEED_TAG_MIN_LENGTH_TWITTER` | `50` | 组装后文本最短字符数，低于则 skip |
| `AUTO_FEED_TAG_MIN_LENGTH_YOUTUBE` | `200` | transcript 最短字符数 |
| `AUTO_FEED_TAG_MAX_CHARS` | `6000` | LLM 输入上限，超过截断 |
| `AUTO_FEED_TAG_TEMPLATE_CODE` | `content_tag_feeds_v1` | 便于未来迭代 v2 |
| `AUTO_FEED_TAG_MODEL` | —（走模板默认）| 紧急切小模型时覆盖 |

## 11. 分阶段实施 checklist

### Phase 0 — Prompt & 数据结构（0.5 天）
- [ ] 写 `content_tag_feeds_v1` 模板 SQL（参考 v1 + summary 字段 + Twitter/YouTube 风格提示）
- [ ] 小样本手工验证：10 条 Twitter + 5 条 YouTube 字幕，肉眼检查 summary + sector/tickers 合理
- [ ] 写 `create_subscription_content_tags.sql` 迁移
- [ ] 应用到 dev 库

### Phase 1 — 后端服务 + 任务（1 天）
- [ ] `FeedContentTaggingService`
- [ ] `tag_feed_content_task` + `batch_tag_feed_content_task`
- [ ] `trigger_auto_feed_tagging` hook
- [ ] 在 Twitter 入库 + YouTube `update_transcript` 两个点接入 hook（用 `AUTO_FEED_TAG_CHANNELS` 环境变量可控）
- [ ] 单测：3 个 channel adapter 分支

### Phase 2 — API + 前端（1 天）
- [ ] 新路由 `subscription_content_tags.py`（GET/POST retag/PUT/DELETE/distinct-*）
- [ ] 在 `GET /subscriptions/{id}/contents` LEFT JOIN tags 返回
- [ ] 前端 `services/subscriptions.ts` 新增方法 + 类型
- [ ] `FeedArticleBody.tsx` 渲染 summary + tags + retag 按钮
- [ ] 桌面 / 移动端列表行 tag chips

### Phase 3 — 回填（按需，灰度）
- [ ] 回填脚本 `backfill_feed_tags.py`，先 dry-run 核对计数
- [ ] 先跑 200 条 sample → 人工抽检
- [ ] 全量回填 Twitter（分 5–10 批、每批间隔 30 min 观察 LLM 账单）
- [ ] 全量回填 YouTube（783 条一次性即可）

### Phase 4 — 上线后观察
- [ ] 日志面板：`source="feed_auto_tagging"` 的成功/失败/跳过比例
- [ ] 检查 `subscription_content_tags.confidence` 分布：< 0.5 的比例过高 → 重审 prompt
- [ ] LLM 成本日报里新增 "feed_tagging" 条目，看与 Upload 的比值

## 12. 风险 & 待确认

1. **Twitter 短内容的打标价值**：median 114 字符、大量 "Good take" 之类的社交回复其实没打标价值。建议在 Phase 1 上线后观察 `skipped / completed` 比例，如果 skip > 50% 就把门槛抬到 80–100 字符。
2. **YouTube ASR 错别字**：Fun-ASR 对英文 ticker 识别率一般（"NVDA" 可能被识别为 "n via"）。Prompt 里需要要求 LLM "识别不确定时降低 confidence，允许 tickers 为空"。
3. **用户隔离**：`subscription_content` 是多用户共享一行（`user_ids[]`），tag 也跟着共享，没有 per-user 标签。如果未来要做 "我屏蔽某行业" 之类的个人偏好，需要新建 `user_content_preferences` 而不是拆 tag 表。
4. **content_tag_v1 vs feeds_v1 是否可合并**：两者 schema 95% 相似，主要是 feeds_v1 多一个 summary。如果 v1 模板允许 "summary 可选返回"，可以不开新模板，改让 `ContentTaggingService` 也接受可选 summary 字段。**权衡**：独立模板便于独立迭代 prompt（feeds 文本风格不同），推荐先拆开。
5. **News 引入时序**：News 内容长度 median 3513（接近 Upload），打标更有价值；但 News 表另有 `news_analysis_service` 的独立分析管线，和 feeds 走的是两套。建议等 Twitter/YouTube 跑稳后，再开一个"news_feed 订阅内容是否复用 news_analysis 结果" 的专门 RFC，避免重复分析。
6. **家用 Worker YouTube ASR 路径**：`update_transcript` 目前在远程 `backend/services/subscription_content_service.py` 运行（本地 Worker 通过主库直接 UPDATE 或回调 RPC）。需要核对：家用 Worker 完成 ASR 后，是**哪个进程**调用 `update_transcript` / 直接 SQL。如果是本地 Worker 直接 SQL，那么 `trigger_auto_feed_tagging` 要改挂在 **主服务器的某个被回调的地方**（比如新建一个 `finalize_youtube_feed_task` 远程回调任务，对标 `finalize_youtube_file_task`），否则 hook 永远不会在主站触发。
    - **开发前必查**：`deployment/local_virtual_service/worker/tasks.py::transcribe_youtube_feed_task` 尾部是如何把 transcript 写回 DB 的？如果是直接 UPDATE，Phase 1 需要先补一个回调任务。

## 13. 引用 & 对标

- Upload 侧实现（本方案对标对象）：
  - `backend/services/content_tagging_service.py`
  - `backend/tasks/tagging_tasks.py`
  - `backend/tasks/file_tasks.py::trigger_auto_tagging`
  - `backend/tasks/file_tasks.py::finalize_youtube_file_task`（YouTube Upload Link 的 auto-tag 触发点，Feeds YouTube 等效触发点设计来源）
  - `backend/api/file_tags.py`（API 设计对标）
- Feeds 侧改动对象：
  - `backend/services/subscription_content_service.py::update_transcript`
  - `backend/services/twitter_subscription_service.py`
  - `backend/api/subscriptions.py`
  - `web-frontend/src/components/feed/FeedArticleBody.tsx`
  - `web-frontend/src/services/subscriptions.ts`
- 相关文档：
  - `docs/instructions/FEEDS_MODULE.md`（§7 会新增"自动打标"子节）
  - `docs/instructions/UPLOAD_FILES_AND_LLM_ANALYSIS_FLOW.md`
  - `deployment/local_virtual_service/README.md`（YouTube ASR 路径）
