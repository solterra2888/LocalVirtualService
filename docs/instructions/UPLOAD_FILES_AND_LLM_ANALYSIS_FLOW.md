# Upload Files 模块 & LLM 分析流程说明

> 最后更新：2026-04-14

本文档梳理系统中 **Upload Files** 模块对各类文件（PDF、Word、Excel、CSV/交易报告、音频、YouTube、网页链接）以及 **用户直接撰写的文本笔记（My Notes / Thoughts，`file_type='text'`）** 的完整处理流程，以及处理完成后通过 **Prompt Template** 调用 LLM 进行分析（包括用户主动触发的内容分析和系统自动触发的并行分析任务）的完整链路。

---

## 目录

1. [总体架构](#1-总体架构)
2. [文件上传与内容提取](#2-文件上传与内容提取)
   - 2.1 [文档类（PDF / Word / Excel / CSV / 图片）](#21-文档类pdf--word--excel--csv--图片)（内文含 **交易报告** 小节）
   - 2.2 [音频文件](#22-音频文件)
   - 2.3 [YouTube 链接](#23-youtube-链接)
   - 2.4 [网页链接（Web Link）](#24-网页链接web-link)
   - 2.5 [文本笔记（My Notes / Thoughts）](#25-文本笔记my-notes--thoughts)
3. [数据存储模型](#3-数据存储模型)
4. [Prompt Template 与 LLM 分析流程](#4-prompt-template-与-llm-分析流程)
   - 4.1 [用户主动触发的内容分析](#41-用户主动触发的内容分析)
   - 4.2 [系统自动触发的并行 LLM 任务](#42-系统自动触发的并行-llm-任务)
   - 4.3 [自动打标签（Auto Tagging）](#43-自动打标签auto-tagging)
   - 4.4 [自动命名（Auto Naming）](#44-自动命名auto-naming)
   - 4.5 [并行任务触发时机汇总](#45-并行任务触发时机汇总)
   - 4.6 [LLM 调用核心链路](#46-llm-调用核心链路)
   - 4.7 [Prompt Template 变量约定](#47-prompt-template-变量约定)
   - 4.8 [模板测试（前端 PromptTemplatesPage）](#48-模板测试前端-prompttemplatespage)
   - 4.9 [Remote Agent 路径（可选）](#49-remote-agent-路径可选)
5. [前端服务层](#5-前端服务层)
6. [任务执行与文件夹展示（Tasks API）](#6-任务执行与文件夹展示tasks-api)
7. [API 端点速查表](#7-api-端点速查表)
8. [关键注意事项](#8-关键注意事项)

---

## 1. 总体架构

```
前端 (React)
    │
    │  HTTP REST
    ▼
FastAPI 后端 (backend/api/files.py)
    │
    │  Celery 异步任务
    ▼
file_tasks.py  ──►  FileProcessingWorker  ──►  解析/转写服务
                         │                      ├─ LlamaService     (LlamaParse 云端，文档)
                         │                      ├─ QuickParseService (本地快速解析，文档)
                         │                      ├─ ASRService        (Fun-ASR，音频)
                         │                      ├─ YouTubeService    (字幕/yt-dlp，YouTube)
                         │                      └─ WebScraperService (httpx/Playwright，网页)
                         │
                         ▼
              PostgreSQL: file_processing_details.content 写入完成
                         │
              ┌──────────┴──────────────────────┬───────────────────────────────┐
              │ 自动触发（系统并行，无需用户干预）│                               │
              ▼                                ▼                               ▼
   tag_file_content_task            name_file_content_task       classify_file_content_task
   (queue: file_processing)         (queue: file_processing)     (queue: file_processing)
   模板: content_tag_v1 (固定)      模板: auto_name_v1 (固定)    模板: auto_classify_v1 (固定)
   → LLM 返回 JSON                 → LLM 返回文件标题文本        → LLM 返回文件夹名称
   → 写入 file_tags 表             → 写入 display_name           → 写入 user_execution_folders
              │                                │                               │
              └────────────────────────────────┴───────────────────────────────┘
                         │
                         │  用户主动触发（FilesPage 选择模板后点击分析）
                         ▼
              analyze_document_task / analyze_audio_task
              (Celery queue: file_processing)
              模板: template_id（用户在前端选择，动态）
                         │
                         ▼
              LocalLLMAnalysisService
                    ├─ PromptRenderer.render()      ← 渲染 Prompt Template（替换 {{变量}}）
                    └─ llm_service.chat_completion() ← LLM 调用
                         │
                         ▼
              processing_stats.final_report（分析报告写回 DB）
              file_processing_details（ai_time / total_tokens / cost 更新）
                         │
                         │  分析完成后再次触发并行任务
                         ▼
          trigger_auto_tagging + trigger_auto_naming + trigger_auto_classify（同上并行执行）
```

> **补充：** 用户通过 `POST /api/files/create-text-note` 创建的 **文本笔记**（`file_type='text'`）**不经过**上图中的 `FileProcessingWorker` 与 Celery 解析任务；在同步 API 内写入 `content` 后直接调用 `trigger_auto_tagging` / `trigger_auto_naming` / `trigger_auto_classify`，与「`content` 非空写入完成后」的并行任务一致。

---

## 2. 文件上传与内容提取

### 2.1 文档类（PDF / Word / Excel / CSV / 图片）

#### 支持格式

`.pdf`, `.doc`, `.docx`, `.xlsx`, `.xls`, `.csv`, 常见图片格式（JPG/PNG 等）

#### 上传入口

| 场景 | API 端点 | 说明 |
|------|----------|------|
| 仅上传（不解析） | `POST /api/files/upload` | 文件存 OSS，`status='uploaded'`，`content` 为空 |
| 上传并解析（推荐） | `POST /api/files/upload-and-process-document` | 上传 + 异步解析一步完成 |
| 对已上传文件批量解析 | `POST /api/files/process` | 传入已有文件路径列表 |

#### 解析流程（`FileProcessingWorker.process_files()`）

```
OSS Key → 下载到临时文件
    │
    ├─ processing_mode = 'llamaparse'（默认）
    │       └─ LlamaService.process_file()
    │               ├─ 上传至 LlamaCloud /api/parsing/upload
    │               ├─ 轮询任务状态 (poll_job_status)
    │               └─ get_markdown_result() → Markdown 文本
    │
    └─ processing_mode = 'quick'（本地快速模式）
            └─ QuickParseService.process_file()
                    ├─ PDF    → pymupdf4llm.to_markdown()
                    ├─ Word   → mammoth.convert_to_markdown()
                    ├─ Excel  → openpyxl → 每个 Sheet 转为 Markdown 表格
                    └─ CSV    → 纯文本表格 / IB 交易报告特殊解析路径
```

> **图片**：快速模式仅生成占位文本，OCR 能力需使用 LlamaParse 模式。

#### 交易报告（IB Activity Statement CSV）

**交易报告**指 Interactive Brokers 导出的 **活动账单 / Activity Statement**（`.csv`）。与普通 CSV 表格不同：系统在 **Quick Process（本地快速模式）** 下会识别该格式，将原始 CSV 解析为 **单行 JSON 字符串** 写入 `file_processing_details.content`，供前端识别为「IB 活动报告」并展示专用视图（含 **复盘** Tab 等）。实现位于 `backend/services/trading_report_extractor.py`，由 `QuickParseService._parse_csv()` 调用。

| 项目 | 说明 |
|------|------|
| **上传入口** | 与普通文档相同：`POST /api/files/upload-and-process-document` 或 `POST /api/files/process`（文件扩展名为 `.csv`） |
| **识别条件** | 原始文本前若干字符内同时包含：`Interactive Brokers` 或 `BrokerName`，以及 `Activity Statement` 或中文 `活动账单` / `活动报告`（见 `is_ib_trading_report()`） |
| **解析路径** | 仅当 `processing_mode = 'quick'` 时走结构化提取：`extract_tables_from_content()` → `to_structured_content()`。输出 JSON 含 `_type: "ib_trading_report"`、`tables`、`orders`、`markdown` 等字段，与前端 `IBReportData` / `isIBReportContent()` 约定一致 |
| **LlamaParse 模式** | 若 `processing_mode = 'llamaparse'`，CSV 走 LlamaCloud 解析，结果为普通 Markdown/文本，**不会**生成上述 JSON，前端无法识别为交易报告专用内容；需使用 Quick Process 才能启用交易报告链路 |
| **默认行为** | 前端 `UploadFilesModal` 默认 `processing_mode: 'quick'`，一般上传即可命中结构化路径；若用户手动切换为 LlamaParse，IB CSV 将失去结构化语义 |

**与其他流程的关系：**

- **存储**：仍写入 `file_processing_details.content`（值为 JSON 字符串），`file_type` 仍为 `'document'`（与普通 CSV 一致）。
- **并行 LLM**：`content` 非空后仍会触发自动打标签 / 自动命名 / 自动归类（与其它文档相同）；送入模板的 `file_content` 即为该 JSON 字符串（或截断后的片段）。
- **用户主动分析**：可走 `POST /api/files/document/analyze`，行为与普通文档一致。
- **复盘与图表**：文件详情侧「复盘」、K 线、成交标注等见专项说明：[TRADE_REVIEW_MODULE.md](./TRADE_REVIEW_MODULE.md)。

#### 解析结果存储

- `file_processing_details.content` = 提取的 Markdown/纯文本
- `file_processing_details.status` = `'completed'` | `'failed'`

---

### 2.2 音频文件

#### 支持格式

常见音频格式（mp3、wav、m4a 等，具体以 OSS 上传配置为准）

#### 上传与转写流程

```
POST /api/files/upload-audio
    │
    └─ Celery: upload_and_transcribe_audio_task
            ├─ 1. 写入临时文件
            ├─ 2. OSSService.upload_audio_file() → OSS 对象键
            ├─ 3. 获取签名/公开 URL
            ├─ 4. ASRService.submit_transcription_task()   ← Fun-ASR
            ├─ 5. ASRService.get_transcription_results()   ← 轮询结果
            ├─ 6. 有 Diarization 时：_build_asr_structured_data()
            │       → asr_with_diarization (JSONB，含说话人 + 时间戳)
            └─ 7. ASRService.format_transcription_result(..., timestamped_only=True)
                    → file_processing_details.content（带时间戳的文本行）
```

#### 关键字段

| 字段 | 内容 |
|------|------|
| `file_type` | `'audio'` |
| `content` | 带时间戳的转写文本（每行一段） |
| `asr_with_diarization` | JSONB，含说话人标签 + 时间戳的结构化数据（用于 LLM 分析） |
| `original_file_path` | OSS 对象键（非签名 URL） |
| `file_storage_type` | `'oss'` |

---

### 2.3 YouTube 链接

> **架构说明（2026-04 优化）**：YouTube 字幕拉取与音频下载 / ASR 均涉及访问 YouTube，远程服务器 IP 会被频繁封禁。该功能已下沉到家用 `local_virtual_service` Worker（复用 Feed 订阅的同一套 Worker）。远程只做「同步建记录 → 派单 → 收尾」。`YOUTUBE_UPLOAD_USE_LOCAL=false` 可回滚到远程旧实现作为紧急出口。

#### 流程（新路径，默认）

```
POST /api/files/upload-youtube                                 (远程 FastAPI)
    ├─ 同步 INSERT processing_stats (rc_agent_status='processing')
    ├─ 同步 INSERT file_processing_details (status='processing')
    ├─ send_task('fetch_youtube_file_transcript_task')
    │       → 本地 youtube_fetching 队列
    └─ 立即返回 { task_id, file_id, processing_stats_id, video_id }
                    │
                    ▼
local_virtual_service Worker (家用, 可直连 YouTube)
    │
    ├─ fetch_youtube_file_transcript_task  (youtube_fetching)
    │       ├─ _fetch_transcript_with_fallback()
    │       │       ├─ youtube-transcript-api → 成功
    │       │       └─ yt-dlp caption 兜底   → 成功
    │       │
    │       ├─ 成功 → ContentStore.update_file_transcript()
    │       │         → signal_upload_file_ready(outcome='completed')
    │       │
    │       ├─ 永久失败 (直播 / 私有) → signal_upload_file_ready(outcome='failed')
    │       │
    │       └─ 无字幕且可 ASR → send_task('transcribe_youtube_file_asr_task')
    │                           (不 signal, 终态由 ASR 任务负责)
    │
    └─ transcribe_youtube_file_asr_task    (youtube_transcription)
            ├─ probe_youtube_metadata()   (直播 → 永久 failed)
            ├─ 长视频 → 转发 youtube_transcription_long 队列
            ├─ yt-dlp 下载音频 → OSS → Fun-ASR
            ├─ ContentStore.update_file_asr()
            └─ signal_upload_file_ready(outcome=success/failed)
                    │
                    ▼
finalize_youtube_file_task  (远程 file_processing 队列)
    ├─ UPDATE processing_stats SET rc_agent_status, total_time, upload_params
    └─ outcome='completed' 时并行触发:
            trigger_auto_tagging / trigger_auto_naming / trigger_auto_classify
```

#### 关键字段

| 字段 | 内容 |
|------|------|
| `file_type` | `'youtube'` |
| `original_file_path` | YouTube 视频 URL |
| `status` | `'processing'` → `'completed'` / `'failed'`（由本地 Worker 写入）|
| `content` | 字幕/转写全文 |
| `asr_with_diarization` | 字幕结构 JSON（字幕路径）或 Fun-ASR 结构化 JSON（ASR 路径）|
| `processing_stats.upload_params.caption_source` | `caption_api` / `caption_ytdlp` / `asr` / `asr_live_skip` ... |
| `processing_stats.upload_params.asr_used` | `true/false` |

#### 前端轮询

返回的 `file_id` 优先用于轮询 `GET /api/files/upload-youtube/status/{file_id}`，可跨「字幕→ASR」整条链路拿到真实终态；没有 `file_id`（回滚路径）时降级到 `GET /api/files/tasks/{task_id}/status`。

完成后的自动打标签 / 自动命名 / 自动归类由远程 `finalize_youtube_file_task` 统一触发，**无论字幕路径还是 ASR 路径都会触发**（修复了旧版遗漏）。

---

### 2.4 网页链接（Web Link）

#### 抓取流程

```
POST /api/files/upload-web-link
    │
    └─ Celery: scrape_and_process_web_link_task
            ├─ WebScraperService.smart_fetch()
            │       ├─ 优先：httpx 直接抓取
            │       └─ 降级：Playwright（JS 渲染页面）
            ├─ Readability 提取正文 HTML
            ├─ inline_images() → build_full_html()
            ├─ OSSService.upload_bytes() 存储 HTML 快照
            ├─ DB: task_name='Web Link', file_type='web'
            └─ WebScraperService.html_to_plain_text()
                    → file_processing_details.content（纯文本）
```

#### Chrome 扩展剪藏（备用路径）

```
POST /api/files/upload-web-clip
    └─ Celery: process_web_clip_task
            └─ 正则去除 HTML 标签 → 纯文本 → content
```

---

### 2.5 文本笔记（My Notes / Thoughts）

用户**无需上传文件**，在桌面端浮动菜单或移动端上传面板中选择 **「My Notes / Thoughts」**（原「Edit Text」入口），在弹窗中填写可选标题与用户撰写的 Markdown 正文并保存。

#### 流程

```
POST /api/files/create-text-note
    Body: { title?: string, content: string }   # title 为空时服务端使用「Untitled Note」作为文件名
    │
    ├─ 同步写入 processing_stats（task_name='Text Note', rc_agent_status='completed' 等）
    ├─ 同步写入 file_processing_details：
    │       file_type = 'text'
    │       status = 'completed'
    │       content = 用户 Markdown
    │       original_file_path / OSS 可为空（无原始文件对象）
    │
    └─ 事务提交后调用（与异步上传路径相同）：
            trigger_auto_tagging + trigger_auto_naming + trigger_auto_classify
```

不经过 `FileProcessingWorker` 与 Celery 解析任务；并行 LLM 任务仍走 `file_processing` 队列。

#### 前端要点

- 桌面：`Layout.tsx` 上传菜单第一项；`CreateTextNoteModal.tsx`；保存成功后派发 `guanhe:task-executions-refresh`，`FilesPage` 订阅并刷新左侧 Sources（含重置类型筛选为 All，避免新建 `text` 被当前类型 Tab 过滤掉）。
- 移动端：`MobileSourcesPage.tsx` 同步菜单与弹窗；同样监听上述事件以刷新列表（因移动端路由为 `/files` 但挂载的是 `MobileSourcesPage`，不能只依赖 `FilesPage` 内监听）。
- `file_type='text'` 在 **Original** 面板由 `OriginalTextNotePanel` 直接渲染 Markdown，支持 **Edit** 编辑保存（`PATCH /api/files/details/{file_id}/content`）。

---

## 3. 数据存储模型

### `processing_stats`（每次执行一行）

| 字段 | 说明 |
|------|------|
| `id` | 主键 |
| `task_name` | 任务类型（如 `'ASR Transcription'`, `'Web Link'`） |
| `final_report` | LLM 输出的最终分析报告 |
| `rc_agent_status` | Agent 执行状态 |
| `total_tokens` / `total_price` | Token 用量与费用 |
| `llm_model` / `llm_provider` | 使用的模型与供应商 |
| `upload_params` | JSON 文本，记录上传参数（如 `audio_url`、`model` 等） |
| `"user"` | 所属用户 |

### `file_processing_details`（每个文件一行）

| 字段 | 说明 |
|------|------|
| `id` | 主键 |
| `content` | 提取的 Markdown 或纯文本，供展示和 LLM 使用 |
| `content_length` | 内容字符数 |
| `asr_with_diarization` | JSONB，音频/YouTube 的结构化转写（含说话人+时间戳） |
| `original_file_path` | OSS 对象键或 URL |
| `file_storage_type` | `'oss'` / `'local'` |
| `oss_bucket` | OSS Bucket 名称 |
| `file_type` | `'document'` / `'audio'` / `'youtube'` / `'web'` / `'text'`（用户撰写的文本笔记，无 OSS 原文） |
| `status` | `'uploaded'` / `'processing'` / `'completed'` / `'failed'` |
| `task_id` | Celery 任务 ID |
| `processing_time` | 处理耗时（秒） |

---

## 4. Prompt Template 与 LLM 分析流程

文件内容写入 DB 后，系统会触发两类 LLM 分析：
- **系统自动触发（并行）**：自动打标签 + 自动命名 + **自动归类**（`classify_file_content_task`），无需用户干预，`content` 非空写入并满足各任务门槛后即触发。
- **用户主动触发**：用户在 FilesPage 选择 Prompt Template，手动发起内容深度分析。

---

### 4.1 用户主动触发的内容分析

| 文件类型 | 分析 API | Celery 任务名 |
|----------|----------|---------------|
| 文档（PDF/Word/Excel/Web） | `POST /api/files/document/analyze` | `analyze_document_async` |
| 音频 / YouTube | `POST /api/files/audio/analyze` | `analyze_audio_async` |

两个任务逻辑相同，差异在于音频路径优先读取 `asr_with_diarization`（含说话人标签）。

```
用户选择模板 → FilesPage → POST /api/files/*/analyze
    │
    └─ Celery: analyze_document_task / analyze_audio_task
            │
            ├─ 【文档可选】若 content 为空 → 先调用 LlamaParse 补充解析
            │
            ├─ 读取 file_processing_details.content
            │   （音频/YouTube：优先 asr_with_diarization，格式化为"说话人+时间+文本"）
            │
            ├─ LocalLLMAnalysisService.process_all_contents(results, template_id=用户选择)
            │       └─ 详见 4.7 LLM 调用核心链路
            │
            ├─ DB 写回：
            │       ├─ processing_stats.final_report    ← LLM 分析报告
            │       ├─ processing_stats.total_tokens / total_price / llm_model / llm_provider
            │       └─ file_processing_details.ai_time / total_tokens / cost
            │
            └─ 分析完成后再次触发：
                    trigger_auto_tagging(file_detail_id)   ─┐ 并行执行
                    trigger_auto_naming(file_detail_id)    ─┘ （见 4.3 / 4.4）
```

---

### 4.2 系统自动触发的并行 LLM 任务

每当某个文件的 `content` 字段非空写入完成，系统会**同时**（`countdown` 延迟后）投递三个独立 Celery 消息，互不依赖、可并发执行：

```
content 写入完成
        │
        ├─────────────────────────────────┬──────────────────────────────────┐
        ▼                                 ▼                                  ▼
trigger_auto_tagging              trigger_auto_naming              trigger_auto_classify
        │                                 │                                  │
        ▼                                 ▼                                  ▼
tag_file_content_task          name_file_content_task       classify_file_content_task
(queue: file_processing)       (queue: file_processing)     (queue: file_processing)
        │                                 │                                  │
        ▼                                 ▼                                  ▼
（详见 4.3）                       （详见 4.4）                        （详见 4.5）
```

**环境变量开关：**

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `AUTO_TAG_ENABLED` | `true` | 是否启用自动打标签 |
| `AUTO_NAME_ENABLED` | `true` | 是否启用自动命名 |
| `AUTO_CLASSIFY_ENABLED` | `true` | 是否启用自动归类 |
| `AUTO_TAG_DELAY` | `2`（秒） | 打标签任务延迟投递时间 |
| `AUTO_NAME_DELAY` | `2`（秒） | 命名任务延迟投递时间 |
| `AUTO_CLASSIFY_DELAY` | `2`（秒） | 归类任务延迟投递时间 |
| `AUTO_TAG_MIN_CONTENT_LENGTH` | `100` | 内容低于此字数跳过打标签 |
| `AUTO_NAME_MIN_CONTENT_LENGTH` | `50` | 内容低于此字数跳过命名 |
| `AUTO_CLASSIFY_MIN_CONTENT_LENGTH` | `50` | 内容低于此字数跳过归类 |
| `AUTO_TAG_CONTENT_MAX_CHARS` | `6000` | 送入打标签 LLM 的最大字符数 |
| `AUTO_NAME_CONTENT_MAX_CHARS` | `3000` | 送入命名 LLM 的最大字符数 |
| `AUTO_CLASSIFY_CONTENT_MAX_CHARS` | `3000` | 送入归类 LLM 的最大字符数 |

---

### 4.3 自动打标签（Auto Tagging）

**Celery 任务：** `tag_file_content`（`tagging_tasks.py`，queue: `file_processing`）  
**Prompt Template：** 固定使用 `content_tag_v1`  
**服务：** `ContentTaggingService`（`backend/services/content_tagging_service.py`）

```
tag_file_content_task(file_id, username, force=False)
    │
    ├─ 1. 检查 AUTO_TAG_ENABLED（force=True 时跳过检查）
    ├─ 2. 检查 content 长度 ≥ AUTO_TAG_MIN_CONTENT_LENGTH
    ├─ 3. 读取 file_processing_details.content
    ├─ 4. 若非 force 且 file_tags 表已有记录 → 跳过（幂等保护）
    ├─ 5. 截断 content 至 AUTO_TAG_CONTENT_MAX_CHARS 字符
    │
    ├─ 6. ContentTaggingService.tag_file_content()
    │       └─ LocalLLMAnalysisService(template_code='content_tag_v1')
    │               └─ 详见 4.7 LLM 调用核心链路
    │
    ├─ 7. 解析 LLM 返回 JSON：
    │       必填：sector, industry, tickers
    │       选填：sector_en, industry_en, concepts, confidence, reasoning
    │
    └─ 8. ContentTaggingService.save_tags(file_id, tags)
            └─ INSERT INTO file_tags ... ON CONFLICT (file_id) DO UPDATE
               写入字段：sector, sector_en, industry, industry_en,
                         ticker(JSONB), concepts, additional_tags(JSON),
                         confidence, reasoning, tagged_by, tagged_at
```

**手动触发 API（非自动流程）：**
- `POST /api/file-tags/retag` → 单文件重新打标签
- `POST /api/file-tags/retag-batch` → 批量重新打标签

---

### 4.4 自动命名（Auto Naming）

**Celery 任务：** `name_file_content`（`naming_tasks.py`，queue: `file_processing`）  
**Prompt Template：** 固定使用 `auto_name_v1`  
**服务：** `ContentNamingService`（`backend/services/content_naming_service.py`）

```
name_file_content_task(file_id, username, force=False)
    │
    ├─ 1. 检查 AUTO_NAME_ENABLED（force=True 时跳过检查）
    ├─ 2. 检查 content 长度 ≥ AUTO_NAME_MIN_CONTENT_LENGTH
    ├─ 3. 读取 file_processing_details（content, file_type, original_filename）
    ├─ 4. 若非 force 且 display_name 已有值 → 跳过（幂等保护）
    ├─ 5. 截断 content 至 AUTO_NAME_CONTENT_MAX_CHARS 字符
    │
    ├─ 6. ContentNamingService.name_file_content()
    │       模板变量：file_content, file_type, original_filename
    │       └─ LocalLLMAnalysisService(template_code='auto_name_v1')
    │               └─ 详见 4.7 LLM 调用核心链路
    │
    ├─ 7. sanitize_name(report_content)
    │       ├─ 去除首尾引号、换行符、非法文件名字符
    │       └─ 截断至 80 字符
    │
    └─ 8. ContentNamingService.save_display_name(file_id, display_name)
            └─ UPDATE file_processing_details SET display_name = %s WHERE id = %s
```

**手动触发 API（非自动流程）：**
- `POST /api/file-tags/auto-name` → 单文件重新命名
- `POST /api/file-tags/auto-name-batch` → 批量重新命名

---

### 4.5 自动归类（Auto Classify）

**Celery 任务：** `classify_file_content`（`classify_tasks.py`，queue: `file_processing`）  
**Prompt Template：** 固定使用 `auto_classify_v1`  
**服务：** `ContentClassifyService`（`backend/services/content_classify_service.py`）

```
classify_file_content_task(file_id, username, force=False)
    │
    ├─ 1. 检查 AUTO_CLASSIFY_ENABLED（force=True 时跳过检查）
    ├─ 2. 检查 content 长度 ≥ AUTO_CLASSIFY_MIN_CONTENT_LENGTH
    ├─ 3. 读取 file_processing_details（content, file_type, filename, processing_stats_id）
    ├─ 4. 若非 force 且 user_execution_folders 中已有 (username, processing_stats_id) 记录 → 跳过（幂等保护）
    ├─ 5. 查询用户文件夹列表 file_folders WHERE username = %s
    │      若用户无文件夹 → 跳过（Uncategorized）
    ├─ 6. 截断 content 至 AUTO_CLASSIFY_CONTENT_MAX_CHARS 字符
    │
    ├─ 7. ContentClassifyService.classify_file_content()
    │       模板变量：file_content, folder_list, file_type, original_filename
    │       └─ LocalLLMAnalysisService(template_code='auto_classify_v1')
    │               └─ 详见 4.7 LLM 调用核心链路
    │
    ├─ 8. 解析 LLM 返回的文件夹名称，与用户文件夹列表精确匹配
    │       ├─ 匹配成功 → UPSERT user_execution_folders
    │       └─ 匹配失败 → 视为 Uncategorized（不写入映射）
    │
    └─ 写入目标表：user_execution_folders (username, processing_stats_id, folder_id)
```

**手动触发 API（非自动流程）：**
- `POST /api/file-tags/auto-classify` → 单文件重新归类
- `POST /api/file-tags/auto-classify-batch` → 批量重新归类

---

### 4.6 并行任务触发时机汇总

| 触发来源（Celery 任务） | 触发条件 | 触发的并行任务 |
|------------------------|----------|----------------|
| `process_files_task`（批量解析文档） | 解析完成，查 `file_processing_details` where `task_id=self.request.id AND status='completed' AND content != ''` | `trigger_auto_tagging` + `trigger_auto_naming` + `trigger_auto_classify` |
| `upload_and_process_document_task`（上传并解析） | 解析完成，同上查询 | 同上 |
| `upload_and_transcribe_audio_task`（音频转写） | ASR 写入 `content` 后 | 同上（传 `file_detail_id`） |
| `scrape_and_process_web_link_task`（网页抓取） | HTML 转纯文本写入 `content` 后 | 同上 |
| `process_web_clip_task`（Chrome 剪藏） | 去标签写入 `content` 后 | 同上 |
| `finalize_youtube_file_task`（YouTube 链接，由本地 Worker 回调） | 字幕/ASR 写入 `content` 后，仅 outcome='completed' 触发 | 同上 |
| `analyze_document_task`（用户触发文档分析） | LLM 分析报告写入后 | 同上 |
| `analyze_audio_task`（用户触发音频分析） | LLM 分析报告写入后 | 同上 |
| `reparse_document_task`（重新解析） | 重新解析写入 `content` 后 | 同上 |
| `POST /api/files/create-text-note`（**同步 HTTP**，非 Celery） | 插入 `file_processing_details` 并写入 `content` 后，在 API 内直接调用 | 同上 |

> **已修复（2026-04）：** Upload YouTube Link 的字幕路径、ASR 路径、以及 `POST /api/files/{file_id}/transcribe-asr` 手动重转，最终都会通过本地 Worker 回调远程 `finalize_youtube_file_task`，触发 auto-tag / auto-name / auto-classify。原先「`transcribe_youtube_file_asr_task` 完成后不触发」的已知遗漏已随之修复。

---

### 4.7 LLM 调用核心链路

所有 LLM 分析（用户主动 + 系统自动）均经过同一条调用链：

```
LocalLLMAnalysisService.analyze_file_content(file_content, template_id/code, variables)
    │
    ├─ 1. 组装变量：variables['file_content'] = file_content（+ 其他业务变量）
    │
    ├─ 2. PromptRenderer.render(template_id, variables)
    │       ├─ 从 DB 加载 llm_prompt_templates（system_prompt + user_prompt）
    │       ├─ 替换 {{变量名}} 占位符
    │       └─ 返回 RenderedPrompt(system_prompt, user_prompt, recommended_config)
    │
    ├─ 3. ConfigResolver（可选）
    │       └─ 可覆盖 provider / model / temperature / max_tokens
    │
    └─ 4. llm_service.chat_completion(messages=[LLMMessage(system), LLMMessage(user)])
            └─ 返回 report_content（LLM 输出文本）

process_all_contents(successful_results, ...)
    └─ 循环每个文件 → analyze_file_content()
       注意：当前实现只将【最后一个文件】的 report_content 写入 processing_stats.final_report
```

---

### 4.8 Prompt Template 变量约定

模板文本中使用 `{{变量名}}` 格式，常用变量：

| 变量名 | 使用场景 | 来源 |
|--------|----------|------|
| `{{file_content}}` | 全部分析模板 | 文件提取的 Markdown/纯文本（音频为格式化转写文本） |
| `{{file_type}}` | `auto_name_v1`, `auto_classify_v1` | `file_processing_details.file_type` |
| `{{original_filename}}` | `auto_name_v1`, `auto_classify_v1` | 原始文件名 |
| `{{folder_list}}` | `auto_classify_v1` | 用户 `file_folders` 表中的文件夹名称，逗号分隔 |
| `{{additional_context}}` | `content_tag_v1` | 可选附加上下文（默认为空） |
| 其他自定义变量 | 用户自定义模板 | 由调用方通过 `template_variables` 传入 |

> 音频/YouTube 分析中，`file_content` 会被格式化为含说话人标签与时间戳的文本再送入模板。  
> 音频分析若未传 `template_id`，默认使用内置模板 `audio_analysis_v1`。

---

### 4.9 模板测试（前端 PromptTemplatesPage）

```
POST /api/llm-prompt-templates/templates/test
    ├─ PromptRenderer.render(template_id, sample_variables)
    └─ llm_service.chat_completion()
```

> 此路径使用**手动输入的样本变量**，不读取真实文件内容；真实文件分析在 **FilesPage** 中通过 `analyzeDocument` / `analyzeAudio` 触发。

---

### 4.10 Remote Agent 路径（可选）

当 `analysis_mode == 'remote_agent'` 时，跳过本地 LLM，改用 `RCAgentService.process_all_contents()`（Dify/外部 Chat API），但文件内容同样来自 Markdown 提取结果。

---

## 5. 前端服务层

文件：`web-frontend/src/services/files.ts`

| 函数 | 对应后端 API |
|------|-------------|
| `uploadFiles()` | `POST /api/files/upload` |
| `uploadAndProcessDocument()` | `POST /api/files/upload-and-process-document` |
| `uploadAudioFile()` | `POST /api/files/upload-audio` |
| `uploadYouTubeLink()` | `POST /api/files/upload-youtube` |
| `uploadWebLink()` | `POST /api/files/upload-web-link` |
| `createTextNote()` | `POST /api/files/create-text-note` |
| `updateFileDetailContent(fileId, content, triggerLlm?)` | `PATCH /api/files/details/{file_id}/content`（第三参数为可选 `trigger_llm`，用于保存后再次触发并行 LLM） |
| `processFiles()` | `POST /api/files/process` |
| `analyzeAudio()` | `POST /api/files/audio/analyze` |
| `analyzeDocument()` | `POST /api/files/document/analyze` |
| `getTaskStatus()` | 轮询 Celery 任务状态 |
| `getAudioAnalysisStatus()` | 轮询音频分析状态 |
| `getDocumentAnalysisStatus()` | 轮询文档分析状态 |

**Info 面板 · Basic Information**：`ExecutionDetailsPanel`（`FilesPage.tsx`，与移动端 `MobileSourceFileDetail` 共用）在 **Document Type** 下方展示 **Folder Type**，数据来自任务执行上的 `folder_name`（见第 6 节），对应自动归类任务成功匹配后写入的 `user_execution_folders`；无匹配或未写入时展示 **Uncategorized**。

---

## 6. 任务执行与文件夹展示（Tasks API）

任务列表与部分 UI 依赖 `GET /api/tasks/executions` 返回的 `folder_id` / `folder_name`（子查询 `user_execution_folders` + `file_folders`）。

**单条执行详情** `GET /api/tasks/executions/{execution_id}` 与列表使用同一套文件夹字段，便于右侧 **Info** 在未重新拉列表时也能显示 **Folder Type**。

> 说明：`user_execution_folders` 也可由用户**手动**「移动到文件夹」更新；此时 **Folder Type** 展示的是**当前**所属文件夹，与列表一致，而非单独存储「仅自动归类结果」的历史字段。

---

## 7. API 端点速查表

### 文件上传与解析

| 功能 | 方法 + 路径 | Celery 任务 |
|------|-------------|-------------|
| 仅上传文档 | `POST /api/files/upload` | — |
| 上传并解析文档 | `POST /api/files/upload-and-process-document` | `upload_and_process_document_task` |
| 批量解析已上传文件 | `POST /api/files/process` | `process_files_task` |
| 上传音频并转写（ASR） | `POST /api/files/upload-audio` | `upload_and_transcribe_audio_task` |
| 上传 YouTube 链接 | `POST /api/files/upload-youtube` | 默认：本地 `fetch_youtube_file_transcript_task` → 必要时 `transcribe_youtube_file_asr_task` → 远程 `finalize_youtube_file_task`；`YOUTUBE_UPLOAD_USE_LOCAL=false` 回滚到远程 `upload_and_process_youtube_task` |
| 上传网页链接 | `POST /api/files/upload-web-link` | `scrape_and_process_web_link_task` |
| 上传 Chrome 剪藏 | `POST /api/files/upload-web-clip` | `process_web_clip_task` |
| 创建文本笔记（无上传文件） | `POST /api/files/create-text-note` | —（同步 API，成功后 API 内触发并行 LLM 任务） |

### 内容更新（Markdown 编辑）

| 功能 | 方法 + 路径 | 说明 |
|------|-------------|------|
| 更新文件正文（Content / Original 编辑保存） | `PATCH /api/files/details/{file_id}/content` | Body：`content`；可选 `trigger_llm`（默认 `false`），为 `true` 时在保存后再次触发 `trigger_auto_tagging` / `trigger_auto_naming` / `trigger_auto_classify` |

### 用户主动触发的 LLM 分析

| 功能 | 方法 + 路径 | Celery 任务 | Prompt Template |
|------|-------------|-------------|-----------------|
| 文档内容分析 | `POST /api/files/document/analyze` | `analyze_document_async` | 用户选择（动态） |
| 音频/YouTube 内容分析 | `POST /api/files/audio/analyze` | `analyze_audio_async` | 用户选择，默认 `audio_analysis_v1` |
| 测试 Prompt Template | `POST /api/llm-prompt-templates/templates/test` | — | 用户选择（动态） |

### 系统自动触发的并行 LLM 任务

| 功能 | 触发方式 | Celery 任务 | Prompt Template |
|------|----------|-------------|-----------------|
| 自动打标签 | 系统自动（content 写入后） | `tag_file_content` | `content_tag_v1`（固定） |
| 自动命名 | 系统自动（content 写入后） | `name_file_content` | `auto_name_v1`（固定） |
| 自动归类 | 系统自动（content 写入后） | `classify_file_content` | `auto_classify_v1`（固定） |

### 手动触发打标签/命名（管理接口）

| 功能 | 方法 + 路径 | Celery 任务 |
|------|-------------|-------------|
| 单文件重新打标签 | `POST /api/file-tags/retag` | `tag_file_content` |
| 批量重新打标签 | `POST /api/file-tags/retag-batch` | `batch_tag_files_task` |
| 单文件重新命名 | `POST /api/file-tags/auto-name` | `name_file_content` |
| 批量重新命名 | `POST /api/file-tags/auto-name-batch` | `batch_name_files_task` |
| 单文件重新归类 | `POST /api/file-tags/auto-classify` | `classify_file_content` |
| 批量重新归类 | `POST /api/file-tags/auto-classify-batch` | `batch_classify_files_task` |

---

## 8. 关键注意事项

1. **`upload_and_process_document` 与 LLM 分析的参数传递问题**  
   `upload_and_process_document` 会将 `template_id` 和 `analysis_mode` 写入 `task_info`，但 `FileProcessingWorker` 从 `params` 读取这两个字段。当前前端 `UploadFilesModal` 默认设置 `use_ai_agent: false`，故解析阶段不触发 LLM，行为正确。若未来需在解析阶段直接调用 LLM，须将 `template_id` / `analysis_mode` 同步写入 `params`。

2. **`process_all_contents` 只保留最后一个文件的分析结果**  
   当前实现批量分析时，仅将**最后一个文件**的 `report_content` 写入 `processing_stats.final_report`。如需合并多文件报告，需修改该方法。

3. **音频/YouTube 优先使用 `asr_with_diarization`**  
   LLM 分析时优先读取结构化的 `asr_with_diarization`（含说话人标签），若不存在则降级为纯文本 `content`。

4. **解析模式选择**  
   - `llamaparse`（默认）：云端 API，支持复杂 PDF 和图片 OCR，但需要 LlamaCloud API Key。  
   - `quick`：本地解析，速度更快，不支持图片 OCR，复杂 PDF 排版效果较差。

5. **`file_processing_details.original_file_path` 存储规则**  
   - 文档/音频：存 OSS **对象键**（非签名 URL），使用时需重新生成签名 URL。  
   - YouTube：存原始视频 URL。  
   - Web Link：存原始网页 URL。

6. **IB 交易报告 CSV 与解析模式**  
   结构化交易报告仅在 **Quick Process** 下生成；使用 **LlamaParse** 上传同一文件会得到普通文本，前端不会显示 IB 专用视图与复盘 Tab。需要结构化能力时勿将 `processing_mode` 改为 `llamaparse`。

7. **自动打标签/命名/归类的幂等保护**  
   - 打标签：若 `file_tags` 表中已有该 `file_id` 的记录，且非 `force=True`，则跳过，不重复调用 LLM。  
   - 自动命名：若 `file_processing_details.display_name` 已有值，且非 `force=True`，则跳过。  
   - 自动归类：若 `user_execution_folders` 表中已有该 `(username, processing_stats_id)` 的记录，且非 `force=True`，则跳过。  
   - 手动通过管理 API 触发时可使用 `force=True` 覆盖。

8. ~~`transcribe_youtube_file_asr_task` 缺少自动触发~~（**已于 2026-04 修复**）  
   所有 Upload YouTube Link 的终态都通过本地 Worker 派发远程 `finalize_youtube_file_task`（queue=`file_processing`）统一处理，在 `outcome='completed'` 时触发 `trigger_auto_tagging / trigger_auto_naming / trigger_auto_classify`。字幕路径、yt-dlp 兜底路径、Fun-ASR 路径、以及手动 `POST /api/files/{file_id}/transcribe-asr` 都走同一条回调。

9. **并行自动任务对 content 的截断规则**  
   - 用户主动分析（`analyze_document_task` / `analyze_audio_task`）：不截断，传入完整 `content`。  
   - 自动打标签（`tag_file_content`）：截断至 `AUTO_TAG_CONTENT_MAX_CHARS`（默认 6000 字符）。  
   - 自动命名（`name_file_content`）：截断至 `AUTO_NAME_CONTENT_MAX_CHARS`（默认 3000 字符）。  
   - 自动归类（`classify_file_content`）：截断至 `AUTO_CLASSIFY_CONTENT_MAX_CHARS`（默认 3000 字符）。

10. **文本笔记 `create-text-note`**  
    不经过 OSS 上传与文档解析流水线；`file_type` 固定为 `'text'`。保存成功后须刷新 Sources 列表：桌面端通过自定义事件 `guanhe:task-executions-refresh`（见 `CreateTextNoteModal.tsx` 导出常量），`FilesPage` 与 `MobileSourcesPage` 均需监听。

11. **`PATCH .../content` 与 `trigger_llm`**  
    更新 `content` 时可选 `trigger_llm: true`，在保存后再次投递并行打标签/命名/归类（适用于用户大幅改写正文后希望重新跑一遍自动任务）。

12. **`GET /api/tasks/executions/{id}` 与文件夹字段**  
    与列表接口一致返回 `folder_id` / `folder_name`，供 **Info → Folder Type** 与侧边栏筛选对齐；若仅依赖列表刷新而不同步单条详情，会出现 Folder Type 空白，故单条 API 必须带齐该字段。

---

## 相关源码文件

| 文件 | 说明 |
|------|------|
| `backend/api/files.py` | HTTP 入口；含 `create-text-note`、`PATCH .../content`（可选 `trigger_llm`） |
| `backend/api/tasks.py` | 任务执行列表与单条详情；`folder_id` / `folder_name` 子查询 |
| `backend/api/file_tags.py` | 打标签/命名/归类的 HTTP 管理接口 |
| `backend/tasks/file_tasks.py` | Celery 任务定义（上传/解析/分析），含 `trigger_auto_tagging` / `trigger_auto_naming` / `trigger_auto_classify` |
| `backend/tasks/tagging_tasks.py` | `tag_file_content_task` Celery 任务 |
| `backend/tasks/naming_tasks.py` | `name_file_content_task` Celery 任务 |
| `backend/tasks/classify_tasks.py` | `classify_file_content_task` Celery 任务 |
| `backend/workers/file_worker.py` | `FileProcessingWorker`，核心解析调度器 |
| `backend/core/llama_service.py` | LlamaParse 云端解析服务 |
| `backend/core/quick_parse_service.py` | 本地快速解析服务（含 CSV → IB 交易报告结构化） |
| `backend/services/trading_report_extractor.py` | IB Activity Statement CSV → `ib_trading_report` JSON |
| `backend/core/asr_service.py` | Fun-ASR 音频转写服务 |
| `backend/services/youtube_service.py` | YouTube 字幕/音频下载服务 |
| `backend/services/web_scraper_service.py` | 网页抓取服务 |
| `backend/services/local_llm_analysis_service.py` | LLM 分析主服务（用户分析 + 自动打标签/命名共用） |
| `backend/services/content_tagging_service.py` | 打标签业务逻辑，固定模板 `content_tag_v1` |
| `backend/services/content_naming_service.py` | 自动命名业务逻辑，固定模板 `auto_name_v1` |
| `backend/services/content_classify_service.py` | 自动归类业务逻辑，固定模板 `auto_classify_v1` |
| `backend/core/prompt_renderer.py` | Prompt Template 渲染器（`{{变量}}` 替换） |
| `backend/core/llm_framework.py` | LLM 调用框架（`llm_service.chat_completion`） |
| `backend/database/db_manager.py` | 数据库操作 |
| `backend/schemas/file.py` | 请求/响应 Schema |
| `web-frontend/src/services/files.ts` | 前端文件服务层（含 `createTextNote`、`updateFileDetailContent`） |
| `web-frontend/src/components/CreateTextNoteModal.tsx` | 文本笔记弹窗；`TASK_EXECUTIONS_REFRESH_EVENT` |
| `web-frontend/src/components/Layout.tsx` | 桌面浮动上传菜单（My Notes / Thoughts 置顶） |
| `web-frontend/src/pages/MobileSourcesPage.tsx` | 移动端 Sources 与上传菜单 |
| `web-frontend/src/pages/FilesPage.tsx` | PC Sources；`ExecutionDetailsPanel`（含 Folder Type）、`OriginalTextNotePanel` |
| `web-frontend/src/pages/PromptTemplatesPage.tsx` | Prompt Template 管理与测试页面 |
