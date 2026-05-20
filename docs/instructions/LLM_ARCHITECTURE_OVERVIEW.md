# LLM 模型使用整体架构文档

> 最后更新：2026-05-19（Upload `file_tagging` / `content_tag_v1` 新增 people/countries；用户私有模板与系统模板同步说明）
>
> **演进脉络**：2026-04-27 完成 LLM 调用统一、Resolver、MODULE_REGISTRY、`module_code` 等基础重构（见 [LLM_REFACTOR_PLAN.md](./LLM_REFACTOR_PLAN.md)）；随后在 **PR-A ~ PR-E** 中落地 **SYSTEM_FIXED（系统固定）/ USER_OWNED（用户私有）** 双态模型、按渠道路由的 Feed 摘要、以及 Upload 弹窗级开关；**PR-F** 引入可选的 per-user 限速（默认关闭，见 §0.9）。

本文档梳理系统中所有模块对 LLM 大语言模型的完整调用架构、数据流向及各业务链路，供开发者快速理解整体 AI 能力设计。

---

## 目录

0. [⭐ 重构后核心变化](#0--重构后核心变化)
1. [整体架构总览](#1-整体架构总览)
2. [LLM 核心框架层](#2-llm-核心框架层)
3. [Prompt 模板系统](#3-prompt-模板系统)
4. [业务模块 LLM 链路](#4-业务模块-llm-链路)
   - 4.1 [Upload Files — 文件上传与分析](#41-upload-files--文件上传与分析)
   - 4.2 [Feeds — 打标与个性化摘要（Hybrid）](#42-feeds--打标与个性化摘要hybrid)
   - 4.3 [Twitter Daily Report](#43-twitter-daily-report)
   - 4.4 [YouTube Daily Report](#44-youtube-daily-report)
   - 4.5 [News Analysis — 新闻分析（暂弃用）](#45-news-analysis--新闻分析暂弃用)
5. [LLM 调用路径对比](#5-llm-调用路径对比)
6. [所有 Prompt 模板速查](#6-所有-prompt-模板速查)
7. [配置与环境变量](#7-配置与环境变量)
8. [相关源码索引](#8-相关源码索引)

---

## 0. ⭐ 重构后核心变化

> 阅读建议：先看本节理解 **双态模板** 与 **Resolver 优先级**，再看第 4 节各业务链路。

### 0.1 LLM 调用入口统一

```
改造前：
    Path A (LocalLLMAnalysisService) ── PromptRender → ConfigResolver → chat_completion
                                        ❌ 无 usage_log
    Path B (chat_completion_with_template)
                                        ✅ 完整 usage_log

改造后：
    LocalLLMAnalysisService（语义包装）内部委托 chat_completion_with_template
        → ✅ usage_log（含 user_id）
        → ✅ ConfigResolver
        → ✅ parse_output（按模板 output_format）
```

### 0.2 LLMTemplateResolver — 优先级（Phase 2）

**`user_template_preferences` 表在 Resolver 路径上已废弃**：USER_OWNED 模块下，每用户每 `module_code` 仅 **一行** `scope='user'` 的 `llm_prompt_templates` 记录；用户通过「我的模板」或上传弹窗侧栏编辑该行表达偏好。旧 preferences 表仍保留，仅供历史数据或兼容查询，**新代码不读**。

```
LLMTemplateResolver.resolve(module_code, user_id, runtime_template_id?)

1) runtime_template_id（若传入且模板存在且 active）     → source=runtime   （最高）
2) USER_OWNED 模块 且 user_id 有效：
      查 user-scope 模板；缺失则 UserTemplateSeederService 懒 seed
      → source=user_owned
3) SYSTEM_DEFAULTS[module_code] → DB 按 template_code 加载
      → source=default

SYSTEM_ONLY_MODULES（SYSTEM_FIXED）：跳过第 2 步，只认系统 scope 模板 + admin 后台修改。
```

文件：`backend/services/llm_template_resolver.py`

### 0.3 双态模型：SYSTEM_FIXED vs USER_OWNED

| 类型 | 含义 | 谁能改 prompt | DB |
|------|------|---------------|-----|
| **SYSTEM_FIXED** | 全站共享、客观一致 | 仅 Admin（`/llm/prompts/system-templates`） | `scope='system'` |
| **USER_OWNED** | 每用户一份当前内容，重置=覆盖、无版本历史 | 用户（`/llm/prompts/my-templates/...`） | `scope='user'`，`(created_by, module_code)`  partial unique |

**USER_OWNED 模块（8 个）**：`twitter_phase2`, `youtube_phase2`, `file_analysis`, `audio_analysis`, `youtube_analysis`, `file_tagging`, `feed_summary`, `youtube_summary`。

**SYSTEM_FIXED 典型模块**：`feed_tagging`（Hybrid 决策：全局 tag）、`file_naming`, `file_classify`, `news_summary`, `twitter_phase1`, `youtube_phase1`, `news_classification`, `theme_description` 等。

约束与种子迁移：`pr_a_template_classification_refactor.sql`（partial unique、归档孤儿模板、`audio_analysis` 拆模块、`feed_summary_v1` / `youtube_summary_v1` 种子）、`pr_b_seed_user_owned_templates.sql`（为新模型批量克隆用户私有模板）。

文件：`backend/services/template_preference_service.py`（`SYSTEM_ONLY_MODULES`, `USER_OWNED_MODULES`, `MODULE_REGISTRY`）

### 0.4 `llm_prompt_templates.module_code`

`module_code` 与 `MODULE_REGISTRY` 对齐；列表 API 返回 `module_code` / `category_code`，前端 `UploadAnalysisTemplatePicker` 按模块加载 **当前用户的私有模板** 或展示系统模块说明。

迁移：`add_module_code_to_llm_prompt_templates.sql`

### 0.5 Feeds：Hybrid（全局 Tag + Per-user 摘要）

- **打标 `feed_tagging`**：走 **SYSTEM_FIXED** 模板（如 `content_tag_feeds_v1`），结果写入 **`subscription_content_tags`**，全站订阅者共用同一套客观标签/summary 基线。**抓取后自动派发**（`AUTO_FEED_TAG_ENABLED=true` 默认开启）。
- **个性化摘要**：写入 **`subscription_content_user_summaries`**。按内容渠道选择 **USER_OWNED** 模块：
  - Twitter 类 → `feed_summary`（系统种子 `feed_summary_v1`）
  - YouTube 视频 → `youtube_summary`（系统种子 `youtube_summary_v1`，per-video、per-user，**仅用于 Feeds**；Upload YouTube Link 使用独立的 `youtube_analysis` 模块）

  > ⚠ **个性化摘要默认为手动触发**（`FEED_SUMMARY_AUTO_FANOUT_ENABLED=false`）。用户在文章页点击 "Generate AI Analysis" 主按钮手动触发单条。如需恢复抓取后自动 fan-out，将 env 改为 `true`（无需重新部署）。

读取侧仍可采用 `COALESCE(用户摘要, 全局 tags.summary)` 一类策略；路由逻辑见 `FeedSummaryPersonalizationService` 中 `_CHANNEL_TO_MODULE`。

### 0.6 上传弹窗：`auto_analyze_enabled` / `auto_tag_enabled`

除环境变量总开关（`AUTO_ANALYZE_ENABLED` / `AUTO_TAG_ENABLED`）外，各上传 API 接受 **每次请求** 布尔字段（默认 `true`）。若为 `false`，`trigger_auto_analyze` / `trigger_auto_tagging` **直接跳过**，不解析模板、不调 LLM（与「开关关闭」语义一致）。详见 `backend/api/files.py`、`backend/tasks/file_tasks.py`。

前端：`UploadAnalysisTemplatePicker` 的开关默认 **首次打开为 ON**；关闭弹窗时父组件应重置 picker state，避免 Ant Design Modal 子组件不卸载导致的状态残留。

### 0.7 Prompt 管理 API 与 UI

- **用户**：`GET/PUT /llm/prompts/my-templates`、`POST .../reset` — 仅 **USER_OWNED** 模块。
- **管理员**：`GET/PUT /llm/prompts/system-templates/{template_id}` — **SYSTEM_FIXED** 系统行。
- **`POST /llm/prompts/templates`（通用创建）**：对普通用户 Deprecated，仅 admin 用于维护 system 模板等场景。

前端 **PromptTemplatesPage**：「我的模板」Tab 仅 7 张用户卡片；「系统模板 (Admin)」为管理员扁平列表（含「立即生效 / 仅作种子」标签说明）。**UploadAnalysisTemplatePicker** / **FilesPage** 侧栏：侧滑 **Drawer** 内编辑私有模板，**InsertVariablePopover** 支持向 prompt 插入 `{{变量}}`。

### 0.8 4 路触发与用户标识

文件 `content` 就绪后并行触发 tag / name / classify / analyze；其中 **analyze、tag** 在 USER_OWNED 模块上经 Resolver 取用户私有模板（**name/classify** 仍为 SYSTEM_FIXED）。`username → user_id`：`backend/utils/user_resolver.py`，保证 `llm_prompt_usage_logs.user_id` 可观测。

### 0.9 LLMRateLimiter（PR-F，默认关闭）

`LLMService.chat_completion_with_template` 可在传入 `user_id` 时做 **per-user × per-provider** 每分钟窗口限速（Redis INCR）。**仅当** 环境变量 **`LLM_USER_RATE_LIMIT_PER_MIN` 显式设为大于 0 的整数** 时生效；未设置或为 0 → 完全跳过（便于上线后先看 `llm_prompt_usage_logs` 再定阈值）。超限 → `LLMRateLimitExceeded` → FastAPI 429 + `Retry-After`。

文件：`backend/core/llm_rate_limiter.py`

### 0.10 已清理的孤儿对象（历史）

| 对象 | 原因 | 清理 migration |
|------|------|---------------|
| `twitter_report_user_config` 等 | 无引用或引用已删字段 | `cleanup_legacy_template_orphans.sql` |

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
                         │       ┌──────────────┐  ┌──────────────┐ │
                         │       │  GLM 智谱AI  │  │ Claude（可选）│ │
                         │       │   (Zhipu)    │  │ CherryIN 等   │ │
                         │       └──────────────┘  │ OpenAI 兼容   │ │
                         │                         └──────────────┘ │
                         │                                         │
                         │  配置来源：DB（优先）→ 环境变量（回退）  │
                         │  降级策略：主Provider失败 → fallback链  │
                         └─────────────────────────────────────────┘
                                             │
                         ┌───────────────────▼───────────────────┐
                         │      Prompt Template 系统              │
                         │  DB: llm_prompt_templates            │
                         │  LLMTemplateResolver（runtime→user→default）│
                         │  渲染: PromptRenderer ({{变量}} 替换)  │
                         │  配置: ConfigResolver (优先级仲裁)     │
                         └───────────────────────────────────────┘
                                             │
                         ┌───────────────────▼───────────────────┐
                         │            LLM Provider APIs           │
                         │  Qwen: DashScope Chat + Embedding API  │
                         │  Doubao: Volcengine Ark API v3         │
                         │  GLM: Zhipu PAAS API v4               │
                         │  Claude: OpenAI 兼容 `/v1/chat/completions` │
                         │    （如 CherryIN → Bedrock，计费 USD）   │
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
| **Claude（Anthropic，经聚合网关）** | `claude` | 网关 `{base_url}/v1/chat/completions`（OpenAI 兼容；生产示例：CherryIN `https://open.cherryin.net`） | 请求体**不传** `top_p`（与 `temperature` 二选一，Bedrock 后端限制）；`llm_pricing` 中按 **USD** 估算；需在 DB `llm_provider_configs` 中 **enabled=true** 且填好 API Key，Celery Worker 进程才会加载该 Provider |

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
            ├─ 读取 QWEN_API_KEY / DOUBAO_API_KEY / GLM_API_KEY / CLAUDE_API_KEY 等
            └─ 读取 DEFAULT_LLM_PROVIDER / LLM_FALLBACK_ORDER 等（可含 `claude`）
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

### 2.5 「两条路径」在产品语义上仍区分，在实现上已统一

- **语义路径 A**：`LocalLLMAnalysisService.analyze_file_content()` — 面向「文件 / Feed 正文」类输入，参数里带 `template_id` 或由上游已解析好的模板。
- **语义路径 B**：`llm_service.chat_completion_with_template()` — 面向日报、新闻分析等「结构化业务输入」。

**实现上**：路径 A **内部直接调用**路径 B，因此 **usage_log、ConfigResolver、output_format 解析** 与路径 B 一致，不再存在「A 无日志」的分叉。

```
LocalLLMAnalysisService.analyze_file_content(..., user_id=..., template_id=...)
    → await llm_service.chat_completion_with_template(template_id=..., variables=..., user_id=...)

TwitterAuthorSummaryService 等
    → await llm_service.chat_completion_with_template(...)
        →（可选）LLMRateLimiter.check_and_consume（当 user_id 非空且 env 打开时）
        → PromptRenderer + ConfigResolver + chat_completion
        → llm_prompt_usage_logs
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
| `scope` | `system`（系统模板）/ `user`（用户私有，USER_OWNED 模块一人一轨）|
| `module_code` | 与 `MODULE_REGISTRY` 对齐的业务模块键 |
| `created_by` | 用户私有模板时为用户 ID |

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
模板: LLMTemplateResolver(file_tagging)   系统默认 auto_name_v1   系统默认 auto_classify_v1   Resolver: file_analysis 或 audio_analysis
(USER_OWNED)             (SYSTEM_FIXED)                    (SYSTEM_FIXED)              (USER_OWNED；YouTube/音频走 audio_analysis)
        │                   │                  │                                      │
        ▼                   ▼                  ▼                                      ▼
  file_tags 表         display_name 字段   user_execution_folders 表        processing_stats.final_report
  (sector/industry/    (AI 生成的展示名)   (自动归类到用户文件夹)             (AI 深度分析报告)
   tickers/concepts)
```

**`trigger_auto_analyze`（第 4 路）在 runtime 未传 `template_id` 时的行为：**

- **纯音频**：降级为系统模板 `audio_analysis_v1`（与历史一致）。
- **YouTube**：调用 `LLMTemplateResolver.resolve('youtube_analysis', user_id)`，使用用户私有 `youtube_analysis` 行或系统默认 `youtube_analysis_v1`（Bloomberg-view 风格，**独立于 Feeds `youtube_summary` 模块**）。`analyze_audio_task` 额外注入 `transcript`/`title`/`channel`/`description` 变量别名。
- **文档 / 网页 / 文本**（含 Chrome Web Clipper）：调用 `LLMTemplateResolver.resolve('file_analysis', user_id)`，使用用户私有 `file_analysis` 行或系统默认 `file_analysis_v1`。上传弹窗若选了分析模板，则以 **runtime** 覆盖 Resolver（最高优先）。

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
FilesPage 右侧面板 / 中间弹窗 / Re-analyze：UploadAnalysisTemplatePicker（moduleCode=file_analysis 或 audio_analysis）
    → POST /api/files/document/analyze 或 /audio/analyze
    → analyze_document_task / analyze_audio_task (skip_post_trigger=False)
    → LLMTemplateResolver → LocalLLMAnalysisService（默认使用当前用户对应模块的私有模板）
    → processing_stats.final_report 写入
    → 分析完成后再次触发 tag + name + classify（不递归 analyze）
```

#### 4.1.4 Auto Tag 详细流程

```
tag_file_content_task(file_id, username, force=False, template_id=None, ...)
    │
    ├─ 若本次请求/上层链路传入 auto_tag_enabled=False → 直接跳过（不调 LLM）
    ├─ 检查 AUTO_TAG_ENABLED 环境总开关
    ├─ 检查 content 长度 ≥ AUTO_TAG_MIN_CONTENT_LENGTH (默认 100)
    ├─ 幂等检查：file_tags 表已有记录且非 force → 跳过
    ├─ 截断 content 至 AUTO_TAG_CONTENT_MAX_CHARS (默认 6000 字符)
    │
    ├─ LLMTemplateResolver.resolve('file_tagging', user_id, runtime_template_id=template_id)
    ├─ ContentTaggingService → LocalLLMAnalysisService（解析得到的 template_id）
    │       → LLM 返回 JSON：{
    │             sector, sector_en, industry, industry_en,
    │             tickers, concepts, people, countries,
    │             confidence, summary
    │           }
    │         people: [{ name, name_en, role, affiliation? }]  // name 保留原文写法
    │         countries: ["United States", "China", ...]         // 英文全名，非 ISO 缩写
    │
    └─ 写入 file_tags 表 (ON CONFLICT file_id DO UPDATE)
          含 people / countries / tag_schema_version (默认 file_v1) / summary
```

**说明**：上传 API 可将弹窗中 `file_tagging` 解析结果以 `template_id`（及变量）写入 Celery `task_info`，作为 Resolver 的 **runtime** 优先级；未传时则使用用户私有 `user_*_file_tagging` 模板（`source=user_owned`）或系统默认 `content_tag_v1`。

**系统模板升级后的用户私有模板同步（2026-05-19）：** `file_tagging` 为 USER_OWNED 模块，每用户私有行由 `content_tag_v1` 种子克隆。升级系统 `content_tag_v1` 后须同步所有 `scope=user` 且 `module_code=file_tagging` 的行的 prompt 字段，否则用户仍用旧版 people/countries 规则。见 [UPLOAD_FILES_AND_LLM_ANALYSIS_FLOW.md §4.3](./UPLOAD_FILES_AND_LLM_ANALYSIS_FLOW.md#43-自动打标签auto-tagging) 中的运维 SQL。

---

### 4.2 Feeds — 打标与个性化摘要（Hybrid）

#### 4.2.1 全局打标（SYSTEM_FIXED：`feed_tagging`）

Feeds 的全局 LLM 打标**在 Twitter 新推文入库 / YouTube 字幕写入后自动触发**（`trigger_auto_feed_tagging` → `tag_feed_content_task`，受 `AUTO_FEED_TAG_ENABLED=true` 控制）；也可在前端手动触发 Re-tag 或由运维批量回填。

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
    ├─ LLMTemplateResolver('feed_tagging', user_id) → 系统模板 content_tag_feeds_v1（仅 admin 可调文案）
    ├─ LocalLLMAnalysisService
    │       → LLM 返回 JSON：{
    │             sector / sector_en / industry / industry_en /
    │             tickers[] / concepts[] / summary / confidence / reasoning
    │         }
    │
    └─ 写入 subscription_content_tags 表 (UPSERT on subscription_content_id)
```

#### 4.2.2 个性化摘要（USER_OWNED：`feed_summary` / `youtube_summary`） — PR-N 重构

**默认行为（成本优先）：手动触发**（`FEED_SUMMARY_AUTO_FANOUT_ENABLED=false`）。用户在文章页点击 "Generate AI Analysis" 按钮触发单条个性化，无自动 fan-out 成本。如需恢复自动批量模式，将该 env 设为 `true`。

由 `FeedSummaryPersonalizationService` 根据 **`channel_type`** 选择模块：

- Twitter 类渠道 → **`feed_summary`**（种子模板 `feed_summary_v1`，每人一份可编辑私有行）
- YouTube 视频 → **`youtube_summary`**（种子模板 `youtube_summary_v1`，per-video、per-user）

模板解析：`LLMTemplateResolver.resolve(module_code, user_id, runtime_template_id?)`。结果写入 **`subscription_content_user_summaries`**，读取时可与 `subscription_content_tags.summary` 组合展示（例如用户无摘要时回退全局基线）。

##### 数据流（PR-N，默认手动触发模式）

```
抓取 feed → upsert subscription_content
        ↓
        tag_feed_content_task                                 (全局, 1 次, 自动)
            写 subscription_content_tags  + baseline summary
                ↓ 仅当 FEED_SUMMARY_AUTO_FANOUT_ENABLED=true 时才继续
        fan_out_personalize_summary_task                      (分发器, 默认跳过)
            ├─ 读 subscription_content (channel_type / user_ids / 长度)
            ├─ JOIN  user_feed_summary_settings
            │         WHERE channel_type=$ch AND enabled=TRUE
            │           AND user_id = ANY(content.user_ids)
            ├─ 长度门槛: max(env_min, user.min_length)
            └─ for each eligible user (countdown 阶梯, 防雪崩):
                  personalize_feed_summary_task.apply_async(content_id, user_id)
                    ↓
                    写 subscription_content_user_summaries

【手动触发路径（默认模式）】
用户在文章页点 "Generate AI Analysis"
        ↓
POST /api/subscriptions/contents/{id}/personalize
        ↓
personalize_feed_summary_task (同上, 仅对当前用户 × 当前内容)
```

##### 用户控制：`user_feed_summary_settings`（PR-N1 新表）

| 字段 | 说明 |
|------|------|
| `(user_id, channel_type)` UNIQUE | 每用户对每渠道一行 |
| `channel_type` | `twitter` / `youtube` (CHECK 约束) |
| `enabled` | **总开关**: FALSE 时 fan-out 跳过该用户 |
| `template_id` | 用户选定的私有/系统模板; NULL 时由 `LLMTemplateResolver` 自动解析 |
| `min_length` | 用户级长度门槛 (NULL 时走 env `AUTO_FEED_TAG_MIN_LENGTH_*`) |

**API**：`GET / GET /:channel / PUT /:channel /api/feeds/summary-settings`

##### 前端 UX（PR-N3 / N4）

- **Feeds 页右上角齿轮 → `FeedSummarySettingsDrawer`**：保存模板偏好与长度门槛（手动触发时使用）。Drawer 顶部提示条注明集群已默认关闭自动 fan-out，开关仅在 `FEED_SUMMARY_AUTO_FANOUT_ENABLED=true` 时生效。
- **文章页 `FeedArticleBody` 中「Generate AI Analysis」主按钮**（Twitter / YouTube 渠道始终可见）：
  - 未生成 → **"Generate AI Analysis"**（蓝色主按钮）
  - 已生成 → **"Re-analyze"**（灰色默认按钮）
  - 点击后弹出模板选择，提交 `POST /api/subscriptions/contents/{id}/personalize`，前端轮询直至结果落库

##### 模板变量映射（PR-A 方案 B）

服务层 `_build_template_variables(row, channel)` 按渠道注入到 `custom_variables`，模板可用占位符：

| 渠道 | 占位符 |
|------|--------|
| Twitter | `{{channel_type}}` `{{author}}` `{{title}}` `{{text}}`（含转推/引用合并） |
| YouTube | `{{channel_type}}` `{{channel}}` `{{title}}` `{{description}}` `{{transcript}}`（不含 title） |

模板的 `variables` JSONB 元数据已对应回填，前端 `InsertVariablePopover` 会显示完整变量列表。**自动派发场景下用户不应也无需手动填这些字段**，所以 `FeedSummarySettingsDrawer` 与 personalize 弹窗均传 `showVariablesEditor={false}`。

##### 成本守护

| 层级 | 机制 |
|------|------|
| **集群级（主要）** | `FEED_SUMMARY_AUTO_FANOUT_ENABLED=false`（**默认**）：完全阻断 fan-out，个性化 LLM 调用降为零 |
| 全局总闸 | `FEED_SUMMARY_PERSONALIZATION_ENABLED=true`：连手动入口也挡；关则整个功能停 |
| 用户级 | `user_feed_summary_settings.enabled = FALSE`：仅在 fan-out 启用时才有意义 |
| 内容级 | `max(env_min, user.min_length)` 长度门槛：短内容直接跳过 |
| 时序 | fan-out 内 `countdown = 2 + i*2` 阶梯派发（防雪崩，仅 fan-out 启用时） |
| LLM 级 | 复用现有 `LLM_USER_RATE_LIMIT_PER_MIN`（per-user × per-provider，§7.4） |

##### 失败兜底

- `tag_feed_content_task` 失败 → 不派发 fan-out（只在 `result.skipped == false` 才派发）
- `fan_out_personalize_summary_task` 失败 → 自身 autoretry 2 次，不影响 tag 主链
- `personalize_feed_summary_task` 失败 → autoretry 3 次，最终失败时 `subscription_content_user_summaries` 无新行，用户可在文章页再次点击 "Generate AI Analysis" 重试

##### Daily 模块的消费方式（PR-D1 / PR-D2）

`subscription_content_user_summaries` **不仅在 Feeds 文章页消费**，Daily 抽屉也已对接：

| 数据 | 抽屉段位 | 字段来源 |
|------|----------|----------|
| 一段话摘要 + sector / industry / tickers / concepts / **people / countries** | **AI Summary**（上段） | `subscription_content_tags.*` / `file_tags.*`（全局共享，打标产物）；Feed 详情 `feed.tags`，Upload 详情 `upload.tags`（2026-05-19） |
| 长篇 LLM 报告 / per-user 个性化摘要 | **AI Analysis**（下段） | Upload: `processing_stats.final_report`；Feed: `subscription_content_user_summaries.summary`（带 "Personalized" 徽标） |
| 原始内容（推文 / 视频字幕 / 文档正文 等） | **Original**（底段，可折叠） | `subscription_content.*` / `file_processing_details.*` |

**显隐规则**：上下两段独立，缺失则该段静默隐藏；Original 段在 AI Analysis 段存在时**默认折叠**，否则展开（兜底体验）。

实现路径：
- 后端 `GET /api/daily/items/{id}` 在 `_fetch_feed_detail` 内 `LEFT JOIN subscription_content_user_summaries ... AND scus.user_id = $current_user`，新增字段 `personalized_summary` / `personalized_template_id` / `personalized_at`。
- 前端 `DailyAIAnalysisSection.tsx`（PR-D2 新组件）在 `DailyPreviewDrawer` 内消费这些字段，提供 [复制] / [全屏阅读] 操作；Original 段用 antd `Collapse ghost` 包裹。

> Daily 抽屉**不主动触发** `personalize_feed_summary_task` —— 该字段填充完全由 PR-N 的自动 fan-out 链路负责。如果某条 feed 在抽屉里没有 AI Analysis 段，常见原因：用户未在齿轮里开启该渠道 / 内容长度未到门槛 / fan-out 仍在排队。

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
│        (用户在「我的模板」维护 twitter_phase2 私有行)             │
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

| 维度 | Upload Files 自动任务 | Feeds（打标 + 摘要） | Twitter/YouTube Report | News Analysis |
|------|----------------------|----------------|------------------------|---------------|
| **调用路径** | `LocalLLMAnalysisService` | `LocalLLMAnalysisService` | `llm_service.chat_completion_with_template` | `llm_service.chat_completion_with_template` |
| **触发方式** | 文件 content 写入后自动 | Tag: 抓取后自动；**摘要: 全局 tag 完成后 fan-out 自动派发**（PR-N），失败/缺失时文章页兜底按钮可手动 Re-generate | 调度器定时触发（两阶段） | 调度器/API 触发 |
| **模板固定/动态** | **Tag/Analyze：USER_OWNED（Resolver）**；Name/Classify：**SYSTEM_FIXED** | **Tag：SYSTEM_FIXED**（admin）；**摘要：USER_OWNED**（按渠道 feed_summary / youtube_summary，按用户开关 `user_feed_summary_settings.enabled` 决定是否自动派发） | Phase1 固定；Phase2 **USER_OWNED**（每用户私有模板） | 固定（news_summary_v1）+ 失败切换 |
| **输入来源** | `file_processing_details.content` | `subscription_content.content` / `transcript` | `subscription_content` tweets / video transcripts | `news.content` |
| **输出目标** | `file_tags` / `display_name` / `user_execution_folders` / `processing_stats.final_report` | `subscription_content_tags` + **`subscription_content_user_summaries`（个性化摘要）** | `twitter/youtube_author_daily_summaries` + `twitter/youtube_daily_reports` | `news.analysis_result` + `news_themes_unified` |
| **幂等保护** | ✅ 有（各自字段检查）| ✅ 有（force 参数） | ✅ 有（模板变更检测） | ✅ 有（is_analyzed） |
| **Celery 队列** | `file_processing` | `file_processing` | 由 scheduler 派发 | `ai_processing` |
| **使用 Embedding** | ❌ | ❌ | ❌ | ✅ Qwen text-embedding-v3 |

---

## 6. 所有 Prompt 模板速查

| 模板 Code | 所属模块 | 输入变量 | 输出格式 | 调用服务 |
|-----------|---------|----------|---------|---------|
| `content_tag_v1` | `file_tagging`（系统默认种子；每用户 `user_*_file_tagging` 由此克隆，升级后需 SQL 同步） | `{{file_content}}` `{{additional_context}}` | JSON: sector/sector_en/industry/industry_en/tickers/concepts/**people/countries**/confidence/summary | `ContentTaggingService` |
| `auto_name_v1` | `file_naming`（SYSTEM_FIXED） | `{{file_content}}` `{{file_type}}` `{{original_filename}}` | text (≤80字) | `ContentNamingService` |
| `auto_classify_v1` | `file_classify`（SYSTEM_FIXED） | `{{file_content}}` `{{folder_list}}` `{{file_type}}` | text (文件夹名) | `ContentClassifyService` |
| `content_tag_feeds_v1` | `feed_tagging`（SYSTEM_FIXED） | `{{content}}` | JSON: sector/industry/tickers/summary | `FeedContentTaggingService` |
| `file_analysis_v1` | `file_analysis`（通用文档分析默认） | `{{file_content}}` 等 | 分析报告 | `LocalLLMAnalysisService` |
| `audio_analysis_v1` | `audio_analysis`（纯音频转写分析默认） | `{{file_content}}` | 分析报告 | `LocalLLMAnalysisService` |
| `youtube_analysis_v1` | `youtube_analysis`（**Upload YouTube Link 深度分析默认，USER_OWNED**，clone 自 `youtube_summary_v1`）| `{{file_content}}` / `{{transcript}}` `{{title}}` `{{channel}}` `{{description}}` | 分析报告 | `LocalLLMAnalysisService` |
| `feed_summary_v1` | `feed_summary`（Twitter 类 Feed 个性化摘要种子） | 依模板 | 摘要文本 | `FeedSummaryPersonalizationService` |
| `youtube_summary_v1` | `youtube_summary`（**Feeds** YouTube 单条 per-user 摘要种子，与 `youtube_analysis` 独立） | 依模板 | 摘要文本 | `FeedSummaryPersonalizationService` |
| `twitter_daily_phase1_default` | Twitter Report 阶段 A | tweets 文本 | 作者摘要文本 | `TwitterAuthorSummaryService` |
| `twitter_daily_phase2_default` | `twitter_phase2`（日报 Phase2，USER_OWNED 种子） | 阶段 A 摘要合集 | 综合报告 Markdown | `TwitterDailyReportService` |
| `youtube_daily_phase1_default` | YouTube Report 阶段 A | 视频标题 + transcript | 频道摘要文本 | `YouTubeAuthorSummaryService` |
| `youtube_daily_phase2_default` | `youtube_phase2`（USER_OWNED 种子） | 阶段 A 摘要合集 | 综合报告 Markdown | `YouTubeDailyReportService` |
| `news_summary_v1` | `news_summary`（SYSTEM_FIXED） | `{{title}}` `{{content}}` `{{symbols}}` | 分析报告 | `NewsAnalysisService` |
| `news_summary_safe_v1` | News Analysis（保守模板，失败切换）| 同上 | 分析报告（更中性） | `NewsAnalysisService` |
| `news_classification` | `news_classification`（SYSTEM_FIXED） | `{{title}}` `{{content}}` `{{symbols}}` | JSON 分类维度 | `NewsAnalysisService` |
| 用户私有模板行 | USER_OWNED 模块（`scope=user`） | 依各模块 variables | 依模板 | 经 `LLMTemplateResolver` 的各类服务 |

用户在 **PromptTemplatesPage → 我的模板** 编辑 8 个 USER_OWNED 模块；管理员在 **系统模板 (Admin)** 或 `PUT /system-templates` 编辑 SYSTEM_FIXED 文案。

---

## 7. 配置与环境变量

### 7.1 LLM Provider 核心配置

| 变量 | Provider | 说明 |
|------|---------|------|
| `DEFAULT_LLM_PROVIDER` | 全局 | 默认 Provider（`qwen` / `doubao` / `glm` / `claude`） |
| `LLM_FALLBACK_ENABLED` | 全局 | 是否启用 Provider 降级（默认 `true`） |
| `LLM_FALLBACK_ORDER` | 全局 | 降级顺序（示例含 Claude：`qwen,doubao,glm,claude`；以 DB `llm_global_settings` 为准） |
| `QWEN_API_KEY` / `QWEN_ENABLED` | Qwen | DashScope API Key |
| `QWEN_BASE_URL` | Qwen | API 基础 URL |
| `QWEN_DEFAULT_MODEL` | Qwen | 默认模型名 |
| `DOUBAO_API_KEY` / `DOUBAO_ENABLED` | Doubao | Volcengine API Key |
| `DOUBAO_BASE_URL` | Doubao | API 基础 URL |
| `DOUBAO_DEFAULT_MODEL` | Doubao | 默认模型名 |
| `GLM_API_KEY` / `GLM_ENABLED` | GLM | Zhipu API Key |
| `GLM_BASE_URL` | GLM | API 基础 URL |
| `GLM_DEFAULT_MODEL` | GLM | 默认模型名 |
| `CLAUDE_API_KEY` / `CLAUDE_ENABLED` | Claude | 聚合网关 API Key（如 CherryIN） |
| `CLAUDE_BASE_URL` | Claude | 网关根 URL（默认 `https://open.cherryin.net`，实际请求 `{base}/v1/chat/completions`） |
| `CLAUDE_DEFAULT_MODEL` | Claude | 默认模型（如 `anthropic/claude-sonnet-4.6`；网关约定名） |

> ⚠️ **DB 配置优先于环境变量**。Admin UI (`/admin/llm-config`) 可热更新 DB 中的 Provider 配置并触发 `reload_from_database()`。  
> ⚠️ **Celery Worker** 启动时从 DB 加载 **仅 `enabled=true` 且有 API Key** 的 Provider；若模板 `recommended_provider=claude` 但 Claude 未启用，会出现 `Provider claude not found or not enabled`。保存 LLM 配置后需 **重启 Worker**（或依赖生产环境 systemd 重载）使新 Provider 生效。

### 7.2 Auto Task 开关（Upload Files 模块）

**两层语义**：

1. **环境变量**（运维/全局）：下表各 `AUTO_*`，关则对应 Celery 路径整体不执行。
2. **单次上传请求体**（用户弹窗）：`auto_analyze_enabled` / `auto_tag_enabled`（默认 `true`）。任一为 `false` 时，即使全局开关打开，**本次上传**也不会派发对应 `trigger_auto_analyze` / `trigger_auto_tagging` 的 LLM 工作。

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `AUTO_TAG_ENABLED` | `true` | 自动打标签总开关 |
| `AUTO_NAME_ENABLED` | `true` | 自动命名总开关 |
| `AUTO_CLASSIFY_ENABLED` | `true` | 自动归类总开关 |
| `AUTO_ANALYZE_ENABLED` | `true` | 自动深度分析总开关 |
| `AUTO_TAG_CONTENT_MAX_CHARS` | `6000` | 打标签 LLM 最大输入字符 |
| `AUTO_NAME_CONTENT_MAX_CHARS` | `3000` | 命名 LLM 最大输入字符 |
| `AUTO_CLASSIFY_CONTENT_MAX_CHARS` | `3000` | 归类 LLM 最大输入字符 |

### 7.3 Feeds 打标 / 个性化开关

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `AUTO_FEED_TAG_ENABLED` | `true` | Feeds **全局打标**总开关（twitter/youtube 抓取后自动派发 `tag_feed_content_task`） |
| `AUTO_FEED_TAG_CHANNELS` | `twitter,youtube` | 参与打标的渠道白名单 |
| `AUTO_FEED_TAG_MAX_CHARS` | `6000` | 最大输入字符 |
| `AUTO_FEED_TAG_MIN_LENGTH_TWITTER` | `50` | Twitter 内容最小字符门槛 |
| `AUTO_FEED_TAG_MIN_LENGTH_YOUTUBE` | `200` | YouTube 内容最小字符门槛 |
| `TWEET_SIGNAL_V1_ENABLED` | `false` | Twitter 短推智能路由：开启后 &lt;250 字且无 `$cashtag` 走 `tweet_signal_v1`，否则仍走 `content_tag_feeds_v1` |
| `TWEET_SIGNAL_HEAVY_THRESHOLD_CHARS` | `250` | 重型模板字符阈值（与 cashtag 二选一触发重型） |
| `TWEET_SIGNAL_MODEL_OVERRIDE` | _(空)_ | 覆盖 `tweet_signal_v1` 推荐模型 |
| `FEED_SUMMARY_AUTO_FANOUT_ENABLED` | **`false`** | **(新增)** 全局打标成功后是否自动 fan-out per-user 个性化摘要。**默认关闭，节省 LLM 成本**。改为 `true` 恢复旧自动批量行为，无需重部署。**手动触发入口不受此开关影响**。 |
| `FEED_SUMMARY_PERSONALIZATION_ENABLED` | `true` | 个性化摘要功能本身总闸；关则连手动入口也被拦截（仅紧急回滚用）。与 `FEED_SUMMARY_AUTO_FANOUT_ENABLED` 区别：本开关连手动都挡，后者仅挡自动 fan-out。 |

> **三层开关关系**（AND 逻辑，从外到内）：
>
> `FEED_SUMMARY_PERSONALIZATION_ENABLED`（功能总闸）
> → `FEED_SUMMARY_AUTO_FANOUT_ENABLED`（自动 fan-out 子开关，**默认 false**）
> → `user_feed_summary_settings.enabled`（用户级，仅在 fan-out 启用时才有意义）
>
> 当前默认配置：全局打标自动 ✓，个性化摘要手动 ✓（零自动 LLM 成本）。

### 7.4 LLM 调用限速（可选，默认关闭）

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `LLM_USER_RATE_LIMIT_PER_MIN` | *未设置* | 仅当设为 **正整数** 时启用：每用户每 Provider 每分钟最大 `chat_completion_with_template` 次数（Redis）；`user_id=None` 的系统调用不计入。未设置或非正数 → 不限速。依赖 `REDIS_URL`。 |

### 7.5 开发与生产：Celery / Redis 与 LLM 排障要点

与「长 LLM / 损坏任务元数据」相关的运维行为（避免损坏的 `celery-task-meta-*` 导致 Worker 启动即崩、或 `TimeoutStopSec` 过短导致长 LLM 被 SIGKILL 后写入异常结果）：

| 场景 | 行为说明 |
|------|----------|
| **本地** | `development/stop_local_dev.sh`：在**停止本地 Redis 之前**清理 `celery-task-meta-*`、队列与 `unacked*`；`development/start_local_dev.sh`：启动 file/ai Worker 前再次清理旧 meta，并轮询日志直至 `ready.` |
| **生产** | `stop_services.sh` / `start_services.sh` / `restart_services.sh`：在启停 Celery 前后清理 Redis 任务元数据；`deployment/templates/rc-celery-ai.service.tpl` / `rc-celery-data.service.tpl`：`ExecStartPre` 每次进程启动前清理；**延长** `TimeoutStopSec` 并为 Worker 配置 `--soft-time-limit` / `--time-limit`，避免 `systemctl stop` 在长 LLM 期间把 Worker 硬杀 |
| **DB 迁移** | `backend/database/migrations/add_prompt_completion_tokens_to_usage_logs.sql`：若未执行，`llm_prompt_usage_logs` 写入 `prompt_tokens`/`completion_tokens` 会报错（不影响主链路，但日志表写入失败） |

部署后请执行 `sudo bash deployment/deploy.sh && sudo ./restart_services.sh` 以安装更新后的 systemd 单元。

---

## 8. 相关源码索引

### 核心框架

| 文件 | 说明 |
|------|------|
| `backend/core/llm_framework.py` | LLM 服务单例，Provider 实现，路由降级，全局 `llm_service` |
| `backend/core/prompt_renderer.py` | Prompt 模板渲染器，`{{变量}}` 替换，输出格式解析 |
| `backend/core/config_resolver.py` | 模型配置优先级仲裁（Provider→Template→Runtime） |
| `backend/core/llm_rate_limiter.py` | Per-user × per-provider 可选限速（§7.4） |
| `backend/services/llm_template_resolver.py` | 统一模板解析：runtime → user_owned → system default |
| `backend/services/user_template_seeder_service.py` | USER_OWNED 懒初始化 / 重置为系统种子 |
| `backend/services/template_preference_service.py` | `MODULE_REGISTRY`、`SYSTEM_DEFAULTS`、模块类型判定 |
| `backend/utils/user_resolver.py` | `username → user_id`（Celery / API 透传 usage_log） |
| `backend/services/local_llm_analysis_service.py` | 面向文件/正文的分析入口（委托 `chat_completion_with_template`） |
| `backend/services/llm_prompt_template_service.py` | 模板 CRUD + 调用日志记录 |
| `backend/api/llm_config.py` | LLM 配置管理 Admin API，Provider 测试（含 `claude`），调用日志查询 |
| `backend/api/llm_prompt_templates.py` | 通用模板 API；**`/my-templates`、`/system-templates`**（Phase 2） |
| `backend/core/llm_pricing.py` | 各 Provider 模型单价（含 Claude USD） |
| `backend/database/migrations/add_claude_provider.sql` | 插入/更新 `claude` 行（CherryIN 等网关） |
| `backend/database/migrations/add_prompt_completion_tokens_to_usage_logs.sql` | `llm_prompt_usage_logs` 增加 token 列（未执行则日志写入失败） |

### 前端（Prompt / Upload）

| 文件 | 说明 |
|------|------|
| `web-frontend/src/pages/PromptTemplatesPage.tsx` | 我的模板 / 系统模板 (Admin)、编辑 Drawer |
| `web-frontend/src/pages/LLMConfigPage.tsx` | Admin LLM 配置、连接测试、降级顺序 |
| `web-frontend/src/components/UploadAnalysisTemplatePicker.tsx` | 上传/文件页分析或打标：开关 + 侧栏编辑 Drawer |
| `web-frontend/src/components/InsertVariablePopover.tsx` | 模板编辑器内插入 `{{变量}}` |
| `web-frontend/src/services/promptTemplateService.ts` | `my-templates` / `system-templates` API 客户端 |
| `web-frontend/src/components/feed/FeedArticleBody.tsx` | Feed 摘要：`feed_summary` / `youtube_summary` 动态 moduleCode |

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
| `backend/services/feed_content_tagging_service.py` | Feeds **全局打标**（`feed_tagging`） |
| `backend/services/feed_summary_personalization_service.py` | Feeds **个性化摘要**（`feed_summary` / `youtube_summary`） |
| `backend/services/user_feed_summary_settings_service.py` | **(PR-N1)** 用户 × 渠道 个性化开关/模板/长度门槛 CRUD |
| `backend/tasks/feed_tagging_tasks.py` | `tag_feed_content_task` (末尾链式触发 fan-out) |
| `backend/tasks/feed_summary_tasks.py` | `personalize_feed_summary_task` + **(PR-N2)** `fan_out_personalize_summary_task` |
| `backend/api/subscription_content_tags.py` | Feeds 打标 API + 兜底 personalize 触发 |
| `backend/api/feed_summary_settings.py` | **(PR-N1)** GET/PUT `/api/feeds/summary-settings/{channel}` |
| `backend/services/twitter_author_summary_service.py` | Twitter Report 阶段 A |
| `backend/services/twitter_daily_report_service.py` | Twitter Report 阶段 B |
| `backend/services/youtube_author_summary_service.py` | YouTube Report 阶段 A |
| `backend/services/youtube_daily_report_service.py` | YouTube Report 阶段 B |
| `web-frontend/src/components/feed/FeedSummarySettingsDrawer.tsx` | **(PR-N3)** Feeds 页右上角齿轮 → 用户级个性化设置抽屉 |
| `web-frontend/src/services/feedSummarySettings.ts` | **(PR-N3)** 设置 API 客户端 |

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
                │  FeedSummaryPersonalizationService    │
                │  NewsAnalysisService 等      │
                └────────────┬────────────────┘
                             │
              ┌──────────────┴──────────────────┐
              │                                 │
  ┌──────────────────────┐     ┌─────────────────────────────┐
  │ LocalLLMAnalysis     │────►│ llm_service                 │
  │ Service              │     │ .chat_completion_with_      │
  │（内部委托）          │     │  template()                 │
  └──────────────────────┘     └──────────────┬──────────────┘
                                              │ 可选 LLMRateLimiter
                                              ▼
                ┌─────────────────────────────▼───┐
                │  PromptRenderer              │
                │  DB: llm_prompt_templates    │
                │  （模板已由 LLMTemplateResolver 选中）│
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
