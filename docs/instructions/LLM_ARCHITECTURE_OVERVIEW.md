# LLM 模型使用整体架构文档

> 最后更新：2026-04-27（重构完成版）
>
> ⚠️ **2026-04-27 大重构**：本文档基于 [LLM_REFACTOR_PLAN.md](./LLM_REFACTOR_PLAN.md)
> 完成的 8-PR 重构，关键变化已在第 0 节标注。完整重构成果见
> [LLM_REFACTOR_SUMMARY.md](./LLM_REFACTOR_SUMMARY.md)。

本文档梳理系统中所有模块对 LLM 大语言模型的完整调用架构、数据流向及各业务链路，供开发者快速理解整体 AI 能力设计。

---

## 目录

0. [⭐ 重构后核心变化（2026-04-27）](#0--重构后核心变化2026-04-27)
1. [整体架构总览](#1-整体架构总览)
2. [LLM 核心框架层](#2-llm-核心框架层)
3. [Prompt 模板系统](#3-prompt-模板系统)
4. [业务模块 LLM 链路](#4-业务模块-llm-链路)
   - 4.1 [Upload Files — 文件上传与分析](#41-upload-files--文件上传与分析)
   - 4.2 [Feeds — 订阅内容打标](#42-feeds--订阅内容打标)
   - 4.3 [Twitter Daily Report](#43-twitter-daily-report)
   - 4.4 [YouTube Daily Report](#44-youtube-daily-report)
   - 4.5 [News Analysis — 新闻分析（暂弃用）](#45-news-analysis--新闻分析暂弃用)
5. [LLM 调用路径对比](#5-llm-调用路径对比)
6. [所有 Prompt 模板速查](#6-所有-prompt-模板速查)
7. [配置与环境变量](#7-配置与环境变量)
8. [相关源码索引](#8-相关源码索引)

---

## 0. ⭐ 重构后核心变化（2026-04-27）

> 阅读建议：先看本节理解新模型，再看后续详细章节（部分流程图已被新机制取代）。

### 0.1 LLM 调用入口统一

```
改造前：
    Path A (LocalLLMAnalysisService) ── PromptRender → ConfigResolver → chat_completion
                                        ❌ 无 usage_log
    Path B (chat_completion_with_template)
                                        ✅ 完整 usage_log

改造后：
    所有路径 → LocalLLM (语义包装) → chat_completion_with_template (中央入口)
                                        ✅ 写 usage_log
                                        ✅ ConfigResolver
                                        ✅ parse_output
```

### 0.2 新增 LLMTemplateResolver — 三级 fallback

```
LLMTemplateResolver.resolve(module_code, user_id, runtime_template_id?)
        ↓
   runtime_template_id  (调用方运行时显式传; 优先级最高)
        ↓ 未传
   user_template_preferences[user_id, module_code]  (用户偏好)
        ↓ 未设
   SYSTEM_DEFAULTS[module_code]  (代码常量, 兜底)
        ↓ 未配
   ResolveError
```

文件：`backend/services/llm_template_resolver.py`

### 0.3 MODULE_REGISTRY 扩展（5 → 13）

```
[已有, 重构前]
    twitter_phase1 / twitter_phase2 / youtube_phase1 / youtube_phase2 / file_analysis

[新增, 重构后]
    file_tagging   - 文件自动打标 (默认 content_tag_v1)
    file_naming    - 文件自动命名 (默认 auto_name_v1)
    file_classify  - 文件自动归类 (默认 auto_classify_v1)
    feed_tagging   - Feed 内容打标 (默认 content_tag_feeds_v1)
    feed_summary   - Feed per-user 个性化摘要 (无系统默认, 用户必须自建)
    news_summary   - 新闻摘要 (默认 news_summary_v1)
    news_classification  - 新闻分类维度 (system_only)
    theme_description    - 主题描述生成 (system_only)
```

文件：`backend/services/template_preference_service.py`

### 0.4 `llm_prompt_templates.module_code` 字段

新增 `module_code VARCHAR(64)` 列，与 `MODULE_REGISTRY` / `user_template_preferences.module_code` 对齐。前端 `TemplateSelector` / `UploadAnalysisTemplatePicker` 改为按 `moduleCode` 过滤模板。

迁移：`add_module_code_to_llm_prompt_templates.sql`（生产 23 模板全部赋值，0 NULL）

### 0.5 双层 Feed Summary 模型

```
subscription_content_tags (全局共享, 不变)         ← 客观分类 + baseline summary
subscription_content_user_summaries (PR-6 新表)   ← per-user 主观字段覆盖

读取规则:
    summary       = COALESCE(user_summaries.summary, tags.summary)
    summary_source = 'personalized' | 'global'
```

迁移：`create_subscription_content_user_summaries.sql`

### 0.6 4 路触发链路对齐（修复历史不一致）

```
改造前: process_web_clip / PATCH content / Web Link 触发不一致
改造后: 所有 content 写入完成的链路都触发 4 路 (tag/name/classify/analyze)
        每路都接 LLMTemplateResolver, 走 runtime > preference > default
```

### 0.7 用户标识 user_id 透传

`LocalLLMAnalysisService.analyze_file_content(user_id=...)` 全链路打通——所有 service / task 通过 `username → user_id` 转换 (`backend/utils/user_resolver.py`，LRU 缓存)，最终进入 `llm_prompt_usage_logs.user_id`，可追溯到具体用户。

### 0.8 已清理的孤儿对象

| 对象 | 原因 | 清理 migration |
|------|------|---------------|
| `twitter_report_user_config` | Python 完全没引用，全部行 phase config 为空 | `cleanup_legacy_template_orphans.sql` |
| `update_subscription_unread_count(int,int)` | 函数体引用已删除字段 | 同上 |
| `refresh_all_unread_counts()` | 同上 | 同上 |
| `trigger_update_unread_count_on_read()` | 同上 | 同上 |

---

## 1. 整体架构总览

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           业务模块层（LLM 调用入口）                              │
├───────────────┬──────────────────┬────────────────────┬──────────────────────────┤
│  Upload Files │   Feeds 订阅模块 │  Twitter / YouTube │   News Analysis          │
│  文件分析链路  │  内容打标链路    │  Daily Report 链路 │   新闻分析链路（暂弃用）   │
└───────┬───────┴────────┬─────────┴────────────────────┴───────┬──────────────────┘
        │                │                                       │
        │ LocalLLMAnalysisService            llm_service.chat_completion_with_template
        │                │                                       │
        └────────────────┴───────────────────┬───────────────────┘
                                             │
                         ┌───────────────────▼───────────────────┐
                         │       backend/core/llm_framework.py    │
                         │         LLMService（全局单例）          │
                         │                                         │
                         │  ┌─────────────┐  ┌──────────────┐     │
                         │  │  Qwen 通义  │  │ Doubao 豆包  │     │
                         │  │ (DashScope) │  │ (Volcengine) │     │
                         │  └─────────────┘  └──────────────┘     │
                         │       ┌──────────────┐                  │
                         │       │  GLM 智谱AI  │                  │
                         │       │   (Zhipu)    │                  │
                         │       └──────────────┘                  │
                         │                                         │
                         │  配置来源：DB（优先）→ 环境变量（回退）  │
                         │  降级策略：主Provider失败 → fallback链  │
                         └─────────────────────────────────────────┘
                                             │
                         ┌───────────────────▼───────────────────┐
                         │      Prompt Template 系统              │
                         │  DB表: llm_prompt_templates            │
                         │  渲染: PromptRenderer ({{变量}} 替换)  │
                         │  配置: ConfigResolver (优先级仲裁)     │
                         └───────────────────────────────────────┘
                                             │
                         ┌───────────────────▼───────────────────┐
                         │            LLM Provider APIs           │
                         │  Qwen: DashScope Chat + Embedding API  │
                         │  Doubao: Volcengine Ark API v3         │
                         │  GLM: Zhipu PAAS API v4               │
                         └───────────────────────────────────────┘
```

---

## 2. LLM 核心框架层

### 2.1 框架文件

**`backend/core/llm_framework.py`** — 系统唯一的 LLM 服务单例，约 1083 行。

### 2.2 支持的 Provider

| Provider | 枚举值 | API Endpoint | 特殊能力 |
|---------|--------|-------------|---------|
| **Qwen（通义千问）** | `qwen` | DashScope `/compatible-mode/v1/chat/completions` | ✅ 支持 Embedding（text-embedding-v3，1024 维）|
| **Doubao（豆包）** | `doubao` | Volcengine Ark `/api/v3/chat/completions` | — |
| **GLM（智谱 AI）** | `glm` | Zhipu PAAS `/api/paas/v4/chat/completions` | — |

### 2.3 配置加载顺序

```
LLMService 启动
    │
    ├─ 1. load_project_env()（加载 .env 文件）
    │
    ├─ 2. _load_from_database()
    │       ├─ 从 DB 读取 provider 配置（api_key / base_url / 模型参数）
    │       ├─ 读取全局设置：default_provider / fallback_enabled / fallback_order
    │       └─ 成功 → 使用 DB 配置（管理员可通过 Admin UI 热更新）
    │
    └─ 3. 若 DB 失败 → _load_from_env()
            ├─ 读取 QWEN_API_KEY / DOUBAO_API_KEY / GLM_API_KEY 等
            └─ 读取 DEFAULT_LLM_PROVIDER / LLM_FALLBACK_ORDER 等
```

### 2.4 调用路由与降级策略

```
chat_completion(messages, provider=X, model=Y)
    │
    ├─ 使用指定 provider（或 default_provider）调用
    │
    ├─ 成功 → 返回 LLMResponse
    │
    └─ 失败 且 fallback_enabled=True
            │
            └─ _try_fallback_providers()
                    ├─ 按 fallback_order 列表逐个尝试（跳过失败的 provider）
                    ├─ fallback 时 model=None（各 provider 使用自身默认模型）
                    └─ 全部失败 → 抛出异常
```

### 2.5 两条 LLM 调用路径

系统存在**两条并行的 LLM 调用路径**，各有分工：

```
路径 A：LocalLLMAnalysisService
    适用场景：文件分析、内容打标（Upload Files 模块 + Feeds 打标）
    特点：面向"文件内容"驱动，通过 template_id/code 加载模板
    调用链：
    LocalLLMAnalysisService.analyze_file_content()
        → PromptRenderer.render(template_id/code, variables)
        → ConfigResolver.resolve_config()
        → llm_service.chat_completion(messages)

路径 B：llm_service.chat_completion_with_template()
    适用场景：业务报告生成（Twitter/YouTube Daily Report、News Analysis）
    特点：面向"业务数据"驱动，统一走 LLMService 内部模板加载
    调用链：
    llm_service.chat_completion_with_template(template_code, variables, ...)
        → PromptRenderer.render()
        → ConfigResolver.resolve_config()
        → llm_service.chat_completion(messages)
        → 记录 llm_prompt_usage_logs
```

### 2.6 Embedding 能力

Embedding **仅 Qwen provider** 支持，用于新闻投资主题向量匹配：

```
investment_theme_embedding_service.py
    → llm_service.providers['qwen'].generate_embeddings(texts, model, input_type)
    → DashScope Embeddings API (text-embedding-v3, 1024 维)
    → pgvector 余弦相似度检索
```

---

## 3. Prompt 模板系统

### 3.1 存储结构

所有 Prompt 模板存储在数据库表 `llm_prompt_templates`：

| 字段 | 说明 |
|------|------|
| `template_code` | 模板唯一标识（如 `content_tag_v1`） |
| `system_prompt` | 系统提示词 |
| `user_prompt_template` | 用户提示词（含 `{{变量}}` 占位符） |
| `variables` | JSONB，变量定义（required/default/type） |
| `recommended_provider` | 推荐 Provider |
| `recommended_model` | 推荐模型 |
| `temperature` | 温度参数 |
| `max_tokens` | 最大 token 数 |
| `output_format` | 输出格式定义（json/text/object） |

### 3.2 渲染流程

```
PromptRenderer.render(template_id_or_code, variables)
    │
    ├─ 1. 从 DB 查询模板（按 id 或 template_code + is_active=true）
    ├─ 2. _merge_default_values()（填充缺省变量值）
    ├─ 3. 替换 {{变量名}} 占位符（regex: \{\{(\w+)\}\}）
    └─ 4. 返回 RenderedPrompt
            ├─ system_prompt（渲染后）
            ├─ user_prompt（渲染后）
            └─ recommended_config（provider / model / temperature / max_tokens）
```

### 3.3 配置优先级仲裁

```
ConfigResolver.resolve_config(rendered_prompt, runtime_overrides)

优先级（低 → 高）：
Provider DB 默认值 → 模板推荐配置 → 运行时传入参数

示例：
  provider=DB默认(qwen) < recommended_provider=doubao < runtime.provider=glm
  → 最终使用 glm
```

---

## 4. 业务模块 LLM 链路

### 4.1 Upload Files — 文件上传与分析

#### 4.1.1 自动并行 LLM 任务（4 路）

文件 `content` 写入完成后，系统自动并行触发 4 路独立 Celery 任务：

```
file_processing_details.content 写入完成
        │
        ├───────────────────┬──────────────────┬──────────────────────────────────────┐
        ▼                   ▼                  ▼                                      ▼
[路1] Auto Tag          [路2] Auto Name    [路3] Auto Classify                [路4] Auto Analyze
tag_file_content        name_file_content  classify_file_content              analyze_document/audio_task
queue: file_processing  queue: file_processing  queue: file_processing        queue: file_processing
        │                   │                  │                                      │
        ▼                   ▼                  ▼                                      ▼
LocalLLMAnalysisService LocalLLMAnalysisService LocalLLMAnalysisService        LocalLLMAnalysisService
模板: content_tag_v1    模板: auto_name_v1  模板: auto_classify_v1             模板: 用户选择
(固定)                  (固定)             (固定)                               (音频默认 audio_analysis_v1)
        │                   │                  │                                      │
        ▼                   ▼                  ▼                                      ▼
  file_tags 表         display_name 字段   user_execution_folders 表        processing_stats.final_report
  (sector/industry/    (AI 生成的展示名)   (自动归类到用户文件夹)             (AI 深度分析报告)
   tickers/concepts)
```

#### 4.1.2 各文件类型的触发链路

```
┌─────────────────────────────────────────────────────────────────────┐
│              文件上传类型 → 内容提取 → 触发 4 路并行 LLM             │
└─────────────────────────────────────────────────────────────────────┘

【文档类 PDF/Word/Excel/CSV】
POST /api/files/upload-and-process-document
    → upload_and_process_document_task (Celery)
    → LlamaService (LlamaParse云端) 或 QuickParseService (本地)
    → file_processing_details.content 写入
    → trigger_auto_tagging + trigger_auto_naming + trigger_auto_classify + trigger_auto_analyze

【音频 mp3/wav/m4a】
POST /api/files/upload-audio
    → upload_and_transcribe_audio_task (Celery)
    → ASRService (Fun-ASR / DashScope)
    → file_processing_details.content 写入（含 asr_with_diarization）
    → trigger_auto_tagging + trigger_auto_naming + trigger_auto_classify + trigger_auto_analyze

【YouTube 链接】
POST /api/files/upload-youtube
    → 派单到本地 Worker: fetch_youtube_file_transcript_task (youtube_fetching)
    │       ├─ youtube-transcript-api 拉字幕 → 成功
    │       └─ 无字幕 → transcribe_youtube_file_asr_task (youtube_transcription)
    │                       → yt-dlp 下载音频 → DashScope Fun-ASR
    → signal_upload_file_ready() 回调远程
    → finalize_youtube_file_task (file_processing)
    → trigger_auto_tagging + trigger_auto_naming + trigger_auto_classify + trigger_auto_analyze

【网页链接】
POST /api/files/upload-web-link
    → scrape_and_process_web_link_task (Celery)
    → WebScraperService (httpx → Playwright 降级)
    → file_processing_details.content 写入（纯文本）
    → trigger_auto_tagging + trigger_auto_naming + trigger_auto_classify + trigger_auto_analyze

【文本笔记】
POST /api/files/create-text-note
    → 同步写入 file_processing_details.content
    → 同步调用: trigger_auto_tagging + trigger_auto_naming + trigger_auto_classify + trigger_auto_analyze
```

#### 4.1.3 用户主动分析（Re-analyze）

```
FilesPage 右侧面板选择 Prompt Template
    → POST /api/files/document/analyze 或 /audio/analyze
    → analyze_document_task / analyze_audio_task (skip_post_trigger=False)
    → LocalLLMAnalysisService
    → 用户选择的模板（动态）
    → processing_stats.final_report 写入
    → 分析完成后再次触发 tag + name + classify（不递归 analyze）
```

#### 4.1.4 Auto Tag 详细流程

```
tag_file_content_task(file_id, username, force=False)
    │
    ├─ 检查 AUTO_TAG_ENABLED 开关
    ├─ 检查 content 长度 ≥ AUTO_TAG_MIN_CONTENT_LENGTH (默认 100)
    ├─ 幂等检查：file_tags 表已有记录且非 force → 跳过
    ├─ 截断 content 至 AUTO_TAG_CONTENT_MAX_CHARS (默认 6000 字符)
    │
    ├─ ContentTaggingService → LocalLLMAnalysisService (template: content_tag_v1)
    │       → LLM 返回 JSON：{ sector, industry, tickers, concepts, confidence, reasoning }
    │
    └─ 写入 file_tags 表 (ON CONFLICT file_id DO UPDATE)
```

---

### 4.2 Feeds — 订阅内容打标

Feeds 模块的 LLM 打标**不随抓取自动触发**，需手动或批量触发。

```
【触发方式】
手动触发: POST /api/subscriptions/contents/{id}/tags
批量触发: 运维批量 API → batch_tag_feed_content (Celery, queue: file_processing)

【链路】
FeedContentTaggingService
    │
    ├─ 支持渠道: twitter / youtube（news/rss 直接跳过）
    │
    ├─ 输入拼装：
    │   ├─ Twitter: content + 引用/转推树（≥ AUTO_FEED_TAG_MIN_LENGTH_TWITTER 字符）
    │   └─ YouTube: title + transcript（≥ AUTO_FEED_TAG_MIN_LENGTH_YOUTUBE 字符）
    │   截断至 AUTO_FEED_TAG_MAX_CHARS (默认 6000 字符)
    │
    ├─ LocalLLMAnalysisService (template: content_tag_feeds_v1)
    │       → LLM 返回 JSON：{
    │             sector / sector_en / industry / industry_en /
    │             tickers[] / concepts[] / summary / confidence / reasoning
    │         }
    │
    └─ 写入 subscription_content_tags 表 (UPSERT on subscription_content_id)
```

---

### 4.3 Twitter Daily Report

**两阶段解耦**：阶段 A 全局共享（每作者/天调一次 LLM），阶段 B 每用户个性化。

```
┌─────────────────────────────────────────────────────────────────────┐
│                  Twitter Daily Report 两阶段流程                     │
└─────────────────────────────────────────────────────────────────────┘

【触发方式】
调度: task_scheduling 表，类型 twitter_author_summary
手动: POST /reports/twitter-daily/generate

        ▼
┌───────────────────────────────────────────────────────────────────┐
│  阶段 A：TwitterAuthorSummaryService（全局，per-author）           │
│  service: backend/services/twitter_author_summary_service.py      │
├───────────────────────────────────────────────────────────────────┤
│                                                                   │
│  输入: subscription_content 表中当日该作者全部 tweets             │
│        (最多 MAX_TWEETS_PER_AUTHOR_CHUNK=30 条/chunk)             │
│                                                                   │
│  LLM: llm_service.chat_completion_with_template()                │
│  模板: twitter_daily_phase1_default                               │
│  Max Tokens: MAX_TOKENS_OUTPUT_PHASE1=2000                        │
│                                                                   │
│  输出: twitter_author_daily_summaries 表                          │
│        (每作者每天一行，所有订阅该作者的用户共用)                  │
│                                                                   │
│  模板变更检测：prompt updated_at > 摘要 updated_at → 重新生成     │
└────────────────────────────┬──────────────────────────────────────┘
                             │ 阶段 A 完成后 → 自动链式触发阶段 B
                             ▼
┌───────────────────────────────────────────────────────────────────┐
│  阶段 B：TwitterDailyReportService（per-user）                    │
│  service: backend/services/twitter_daily_report_service.py       │
├───────────────────────────────────────────────────────────────────┤
│                                                                   │
│  输入: 当日所有关注作者的阶段 A 摘要                               │
│        引用编号统一整理为 [T-001] 格式                             │
│        缺失时自动即时生成并缓存                                    │
│                                                                   │
│  LLM: llm_service.chat_completion_with_template()                │
│  模板: twitter_daily_phase2_default                               │
│        (用户可通过 TemplatePreferenceService 自定义 phase2 偏好)  │
│  Max Tokens: MAX_TOKENS_OUTPUT_PHASE2=4000                        │
│                                                                   │
│  输出: twitter_daily_reports 表（每用户每天一行）                  │
└───────────────────────────────────────────────────────────────────┘

读取 API:
  GET /reports/twitter-daily/latest
  GET /reports/twitter-daily/{date}
```

---

### 4.4 YouTube Daily Report

与 Twitter Daily Report 结构**完全对称**。

```
┌─────────────────────────────────────────────────────────────────────┐
│                  YouTube Daily Report 两阶段流程                     │
└─────────────────────────────────────────────────────────────────────┘

【触发方式】
调度: task_scheduling 表，类型 youtube_author_summary
手动: POST /reports/youtube-daily/generate

        ▼
┌───────────────────────────────────────────────────────────────────┐
│  阶段 A：YouTubeAuthorSummaryService（全局，per-channel）          │
│  service: backend/services/youtube_author_summary_service.py     │
├───────────────────────────────────────────────────────────────────┤
│                                                                   │
│  输入: 当日该频道新视频（最多 MAX_VIDEOS_PER_CHANNEL_CHUNK=10 条/chunk）│
│        title + transcript（无 transcript 的视频跳过）              │
│                                                                   │
│  LLM: llm_service.chat_completion_with_template()                │
│  模板: youtube_daily_phase1_default                               │
│  Max Tokens: MAX_TOKENS_OUTPUT_PHASE1=3000                        │
│                                                                   │
│  输出: youtube_author_daily_summaries 表                          │
│        (每频道每天一行，所有订阅该频道的用户共用)                  │
└────────────────────────────┬──────────────────────────────────────┘
                             │ 阶段 A 完成后 → 自动链式触发阶段 B
                             ▼
┌───────────────────────────────────────────────────────────────────┐
│  阶段 B：YouTubeDailyReportService（per-user）                    │
│  service: backend/services/youtube_daily_report_service.py       │
├───────────────────────────────────────────────────────────────────┤
│                                                                   │
│  输入: 当日所有订阅频道的阶段 A 摘要                               │
│                                                                   │
│  LLM: llm_service.chat_completion_with_template()                │
│  模板: youtube_daily_phase2_default                               │
│                                                                   │
│  输出: youtube_daily_reports 表（每用户每天一行）                  │
└───────────────────────────────────────────────────────────────────┘

读取 API:
  GET /reports/youtube-daily/latest
  GET /reports/youtube-daily/{date}
```

---

### 4.5 News Analysis — 新闻分析（暂弃用）

> ⚠️ 当前状态：该模块暂时弃用，但架构完整，文档保留供参考。

```
┌─────────────────────────────────────────────────────────────────────┐
│                      新闻分析完整流程                                 │
└─────────────────────────────────────────────────────────────────────┘

【触发方式】
API:      POST /api/news-analysis/analyze（单条）
           POST /api/news-analysis/analyze-batch（批量）
Celery:   analyze_news_by_ids_task / analyze_unanalyzed_news_task
           queue: ai_processing
调度器:   scheduler_service._execute_news_analysis_task

        ▼
┌──────────────────────────────────────────────────────────────────┐
│  NewsAnalysisService.analyze_news()                              │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│  步骤 1：从 news 表获取数据（title + content + symbols）          │
│                                                                  │
│  步骤 2：UPDATE news SET analysis_status = 'processing'          │
│                                                                  │
│  步骤 3：LLM 分析                                                │
│    llm_service.chat_completion_with_template()                   │
│    模板: news_summary_v1 (默认) / news_classification            │
│    变量: {{title}} / {{content}} / {{symbols}} / {{max_length}}  │
│    重试: ≤2次；内容审查失败 → 切换 news_summary_safe_v1          │
│                                                                  │
│  步骤 4：保存结果                                                 │
│    UPDATE news SET                                               │
│      analysis_result (JSONB，支持多模板)                         │
│      analysis_status = 'completed'                               │
│      is_analyzed = TRUE                                          │
│                                                                  │
│  步骤 5（可选）：LLM 分类维度（news_classification 模板）         │
│    → ThemeMappingService → news_classification_dimensions 表     │
│                                                                  │
│  步骤 6（可选）：投资主题向量匹配                                  │
│    InvestmentThemeEmbeddingService.match_news_to_themes()        │
│    ├─ 新闻向量：Qwen text-embedding-v3（title + 前500字符）       │
│    ├─ 与 investment_theme_embeddings 做余弦相似度检索             │
│    │   过滤：is_enabled_for_matching = TRUE                      │
│    │   Top-K=2，相似度阈值≥0.5                                   │
│    └─ 结果写入 news_theme_matches → 同步到 news_themes_unified   │
│                                                                  │
│  步骤 7：ThemeUnificationService.sync_all_sources()              │
│    合并多来源主题（eodhd_auto_tag / vector_retrieval /           │
│                    llm_classification / manual）                  │
│    → news_themes_unified 表                                      │
└──────────────────────────────────────────────────────────────────┘
```

---

## 5. LLM 调用路径对比

| 维度 | Upload Files 自动任务 | Feeds 内容打标 | Twitter/YouTube Report | News Analysis |
|------|----------------------|----------------|------------------------|---------------|
| **调用路径** | `LocalLLMAnalysisService` | `LocalLLMAnalysisService` | `llm_service.chat_completion_with_template` | `llm_service.chat_completion_with_template` |
| **触发方式** | 文件 content 写入后自动 | 手动/批量触发 | 调度器定时触发（两阶段） | 调度器/API 触发 |
| **模板固定/动态** | 固定（tag/name/classify） + 用户选择（analyze） | 固定（content_tag_feeds_v1） | 固定（phase1/phase2 default） + 用户可自定义 phase2 | 固定（news_summary_v1）+ 失败切换 |
| **输入来源** | `file_processing_details.content` | `subscription_content.content` / `transcript` | `subscription_content` tweets / video transcripts | `news.content` |
| **输出目标** | `file_tags` / `display_name` / `user_execution_folders` / `processing_stats.final_report` | `subscription_content_tags` | `twitter/youtube_author_daily_summaries` + `twitter/youtube_daily_reports` | `news.analysis_result` + `news_themes_unified` |
| **幂等保护** | ✅ 有（各自字段检查）| ✅ 有（force 参数） | ✅ 有（模板变更检测） | ✅ 有（is_analyzed） |
| **Celery 队列** | `file_processing` | `file_processing` | 由 scheduler 派发 | `ai_processing` |
| **使用 Embedding** | ❌ | ❌ | ❌ | ✅ Qwen text-embedding-v3 |

---

## 6. 所有 Prompt 模板速查

| 模板 Code | 所属模块 | 输入变量 | 输出格式 | 调用服务 |
|-----------|---------|----------|---------|---------|
| `content_tag_v1` | Upload Files 自动打标 | `{{file_content}}` | JSON: sector/industry/tickers/concepts | `ContentTaggingService` |
| `auto_name_v1` | Upload Files 自动命名 | `{{file_content}}` `{{file_type}}` `{{original_filename}}` | text (≤80字) | `ContentNamingService` |
| `auto_classify_v1` | Upload Files 自动归类 | `{{file_content}}` `{{folder_list}}` `{{file_type}}` | text (文件夹名) | `ContentClassifyService` |
| `audio_analysis_v1` | 音频/YouTube 深度分析（默认） | `{{file_content}}` | 分析报告 | `LocalLLMAnalysisService` |
| `content_tag_feeds_v1` | Feeds 内容打标 | `{{content}}` | JSON: sector/industry/tickers/summary | `FeedContentTaggingService` |
| `twitter_daily_phase1_default` | Twitter Report 阶段 A | tweets 文本 | 作者摘要文本 | `TwitterAuthorSummaryService` |
| `twitter_daily_phase2_default` | Twitter Report 阶段 B | 阶段 A 摘要合集 | 综合报告 Markdown | `TwitterDailyReportService` |
| `youtube_daily_phase1_default` | YouTube Report 阶段 A | 视频标题 + transcript | 频道摘要文本 | `YouTubeAuthorSummaryService` |
| `youtube_daily_phase2_default` | YouTube Report 阶段 B | 阶段 A 摘要合集 | 综合报告 Markdown | `YouTubeDailyReportService` |
| `news_summary_v1` | News Analysis（主模板） | `{{title}}` `{{content}}` `{{symbols}}` | 分析报告 | `NewsAnalysisService` |
| `news_summary_safe_v1` | News Analysis（保守模板，失败切换）| 同上 | 分析报告（更中性） | `NewsAnalysisService` |
| `news_classification` | News Analysis 分类维度 | `{{title}}` `{{content}}` `{{symbols}}` | JSON 分类维度 | `NewsAnalysisService` |
| 用户自定义模板 | 用户手动分析任意文件 | `{{file_content}}` + 自定义 | 用户定义 | `LocalLLMAnalysisService` |

> 用户可通过 **PromptTemplatesPage** 管理和测试所有模板。

---

## 7. 配置与环境变量

### 7.1 LLM Provider 核心配置

| 变量 | Provider | 说明 |
|------|---------|------|
| `DEFAULT_LLM_PROVIDER` | 全局 | 默认 Provider（`qwen` / `doubao` / `glm`） |
| `LLM_FALLBACK_ENABLED` | 全局 | 是否启用 Provider 降级（默认 `true`） |
| `LLM_FALLBACK_ORDER` | 全局 | 降级顺序（默认 `qwen,doubao,glm`） |
| `QWEN_API_KEY` / `QWEN_ENABLED` | Qwen | DashScope API Key |
| `QWEN_BASE_URL` | Qwen | API 基础 URL |
| `QWEN_DEFAULT_MODEL` | Qwen | 默认模型名 |
| `DOUBAO_API_KEY` / `DOUBAO_ENABLED` | Doubao | Volcengine API Key |
| `DOUBAO_BASE_URL` | Doubao | API 基础 URL |
| `DOUBAO_DEFAULT_MODEL` | Doubao | 默认模型名 |
| `GLM_API_KEY` / `GLM_ENABLED` | GLM | Zhipu API Key |
| `GLM_BASE_URL` | GLM | API 基础 URL |
| `GLM_DEFAULT_MODEL` | GLM | 默认模型名 |

> ⚠️ **DB 配置优先于环境变量**。Admin UI (`/admin/llm-config`) 可热更新 DB 中的 Provider 配置并触发 `reload_from_database()`。

### 7.2 Auto Task 开关（Upload Files 模块）

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `AUTO_TAG_ENABLED` | `true` | 自动打标签总开关 |
| `AUTO_NAME_ENABLED` | `true` | 自动命名总开关 |
| `AUTO_CLASSIFY_ENABLED` | `true` | 自动归类总开关 |
| `AUTO_ANALYZE_ENABLED` | `true` | 自动深度分析总开关 |
| `AUTO_TAG_CONTENT_MAX_CHARS` | `6000` | 打标签 LLM 最大输入字符 |
| `AUTO_NAME_CONTENT_MAX_CHARS` | `3000` | 命名 LLM 最大输入字符 |
| `AUTO_CLASSIFY_CONTENT_MAX_CHARS` | `3000` | 归类 LLM 最大输入字符 |

### 7.3 Feeds 打标开关

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `AUTO_FEED_TAG_ENABLED` | `true` | Feeds 打标总开关 |
| `AUTO_FEED_TAG_CHANNELS` | `twitter,youtube` | 参与打标的渠道白名单 |
| `AUTO_FEED_TAG_MAX_CHARS` | `6000` | 最大输入字符 |
| `AUTO_FEED_TAG_MIN_LENGTH_TWITTER` | — | Twitter 内容最小字符门槛 |
| `AUTO_FEED_TAG_MIN_LENGTH_YOUTUBE` | — | YouTube 内容最小字符门槛 |

---

## 8. 相关源码索引

### 核心框架

| 文件 | 说明 |
|------|------|
| `backend/core/llm_framework.py` | LLM 服务单例，Provider 实现，路由降级，全局 `llm_service` |
| `backend/core/prompt_renderer.py` | Prompt 模板渲染器，`{{变量}}` 替换，输出格式解析 |
| `backend/core/config_resolver.py` | 模型配置优先级仲裁（Provider→Template→Runtime） |
| `backend/services/local_llm_analysis_service.py` | 面向文件内容的 LLM 分析服务（路径 A） |
| `backend/services/llm_prompt_template_service.py` | 模板 CRUD + 调用日志记录 |
| `backend/api/llm_config.py` | LLM 配置管理 Admin API，Provider 测试，调用日志查询 |
| `backend/api/llm_prompt_templates.py` | 模板 CRUD + 预览测试 API |

### Upload Files 模块

| 文件 | 说明 |
|------|------|
| `backend/tasks/file_tasks.py` | 上传/解析 Celery 任务，`trigger_auto_*` 函数集 |
| `backend/tasks/tagging_tasks.py` | `tag_file_content_task` |
| `backend/tasks/naming_tasks.py` | `name_file_content_task` |
| `backend/tasks/classify_tasks.py` | `classify_file_content_task` |
| `backend/services/content_tagging_service.py` | 打标签业务逻辑 |
| `backend/services/content_naming_service.py` | 自动命名业务逻辑 |
| `backend/services/content_classify_service.py` | 自动归类业务逻辑 |

### Feeds 模块

| 文件 | 说明 |
|------|------|
| `backend/services/feed_content_tagging_service.py` | Feeds 内容打标服务 |
| `backend/services/twitter_author_summary_service.py` | Twitter Report 阶段 A |
| `backend/services/twitter_daily_report_service.py` | Twitter Report 阶段 B |
| `backend/services/youtube_author_summary_service.py` | YouTube Report 阶段 A |
| `backend/services/youtube_daily_report_service.py` | YouTube Report 阶段 B |
| `backend/api/subscription_content_tags.py` | Feeds 打标 API |

### News Analysis 模块

| 文件 | 说明 |
|------|------|
| `backend/services/news_analysis_service.py` | 新闻分析核心服务 |
| `backend/services/investment_theme_embedding_service.py` | 主题向量生成与检索（Qwen Embedding） |
| `backend/services/theme_unification_service.py` | 多源主题统一服务 |
| `backend/tasks/news_tasks.py` | 新闻分析 Celery 任务 |
| `backend/api/news_analysis.py` | 新闻分析 API |

---

## 附：完整 LLM 调用数据流

```
                    ┌──────────────┐
                    │   前端 / API  │
                    └──────┬───────┘
                           │
              ┌────────────┴─────────────────┐
              │                              │
    ┌─────────▼──────────┐      ┌────────────▼──────────┐
    │  Celery Worker      │      │  FastAPI（同步调用）    │
    │  (file_processing   │      │  (create-text-note 等) │
    │   ai_processing 等) │      └────────────┬──────────┘
    └─────────┬──────────┘                   │
              │                              │
              └──────────────┬───────────────┘
                             │
                ┌────────────▼────────────────┐
                │  业务服务层                  │
                │  ContentTaggingService       │
                │  FeedContentTaggingService   │
                │  TwitterAuthorSummaryService │
                │  YouTubeAuthorSummaryService │
                │  NewsAnalysisService 等      │
                └────────────┬────────────────┘
                             │
              ┌──────────────┴──────────────────┐
              │                                 │
  ┌───────────▼──────────┐     ┌────────────────▼──────────┐
  │ LocalLLMAnalysis     │     │ llm_service               │
  │ Service (路径 A)     │     │ .chat_completion_with     │
  │                      │     │  _template() (路径 B)     │
  └───────────┬──────────┘     └────────────────┬──────────┘
              │                                 │
              └──────────────┬──────────────────┘
                             │
                ┌────────────▼────────────────┐
                │  PromptRenderer              │
                │  DB: llm_prompt_templates    │
                │  → 渲染 system + user prompt │
                └────────────┬────────────────┘
                             │
                ┌────────────▼────────────────┐
                │  ConfigResolver              │
                │  Provider→Template→Runtime  │
                └────────────┬────────────────┘
                             │
                ┌────────────▼────────────────┐
                │  LLMService.chat_completion  │
                │  → 选择 Provider             │
                │  → 失败自动降级              │
                └────────────┬────────────────┘
                             │
              ┌──────────────┼──────────────────┐
              ▼              ▼                  ▼
       Qwen API          Doubao API          GLM API
    (DashScope)       (Volcengine Ark)    (Zhipu PAAS)
```
