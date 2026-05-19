# Upload Files 模块 & LLM 分析流程说明

> 最后更新：2026-05-09（新增 `podcast` 文件类型，Apple Podcasts 单集上传链路；远程服务器直接 yt-dlp 下载，无需本地 Worker）

本文档梳理系统中 **Upload Files** 模块对各类文件（PDF、Word、Excel、CSV/交易报告、音频、YouTube、Apple Podcasts、网页链接）以及 **用户直接撰写的文本笔记（My Notes / Thoughts，`file_type='text'`）** 的完整处理流程，以及处理完成后通过 **Prompt Template** 调用 LLM 进行分析（包括用户主动触发的内容分析和系统自动触发的并行分析任务）的完整链路。

---

## 目录

1. [总体架构](#1-总体架构)
2. [文件上传与内容提取](#2-文件上传与内容提取)
   - 2.1 [文档类（PDF / Word / Excel / CSV / 图片）](#21-文档类pdf--word--excel--csv--图片)（内文含 **交易报告** 小节）
   - 2.2 [音频文件](#22-音频文件)
   - 2.3 [YouTube 链接](#23-youtube-链接)
   - 2.4 [网页链接（Web Link）](#24-网页链接web-link)
   - 2.5 [文本笔记（My Notes / Thoughts）](#25-文本笔记my-notes--thoughts)
   - 2.6 [Apple Podcasts 单集链接](#26-apple-podcasts-单集链接)
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
              ┌──────────┬───────────────┬─────────────────────────────┬───────────────────────────────┐
              │ 自动触发（系统并行，4 路 LLM，无需用户干预，互不阻塞）                              │
              ▼          ▼               ▼                             ▼                               ▼
   tag_file_content_task   name_file_content_task   classify_file_content_task    analyze_document_task / analyze_audio_task
   模板: content_tag_v1    模板: auto_name_v1       模板: auto_classify_v1         文档/网页/文本: LLMTemplateResolver(file_analysis)
   (固定)                  (固定)                   (固定)                          或弹窗 runtime；音频缺省 audio_analysis_v1
                                                                                    YouTube 缺省 LLMTemplateResolver(youtube_analysis)
   → 写 file_tags 表       → 写 display_name        → 写 user_execution_folders    → 写 processing_stats.final_report
                                                                                    + ai_time / total_tokens / cost
              │                                                                                       │
              └───────────────────────────────────────┬───────────────────────────────────────────────┘
                                                     ▼
                                       前端右侧面板自动刷新对应字段
```

> **第 4 路（analyze_document_task / analyze_audio_task）触发方式：**
> 由 `trigger_auto_analyze` 统一投递，与前 3 路同节奏 (`AUTO_ANALYZE_DELAY` 默认 2 s)。
> - **文档 / 网页 / 文本笔记：** 上传弹窗里 `<UploadAnalysisTemplatePicker />` 选的模板作为 **runtime**（最高优先）；未选时 `trigger_auto_analyze` 调用 `LLMTemplateResolver.resolve('file_analysis', user_id)`，使用用户私有 `file_analysis` 模板或系统默认 `file_analysis_v1`。Chrome Web Clipper（`process_web_clip_task`）无弹窗，依赖该 fallback。若 `auto_analyze_enabled=False`、全局 `AUTO_ANALYZE_ENABLED=false`、或 username 无法解析且 Resolver 失败，则不投递。
> - **音频：** 未选 runtime 模板则降级 `audio_analysis_v1`，与历史行为一致。
> - **YouTube：** 未选 runtime 模板则走 `LLMTemplateResolver.resolve('youtube_analysis', user_id)`，取用户私有 `youtube_analysis` 行或系统默认 `youtube_analysis_v1`（Bloomberg-view 风格，与 Feeds `youtube_summary` 模块完全独立）。
>
> **避免重复入队：** `trigger_auto_analyze` 投递 `analyze_*_task` 时带 `skip_post_trigger=True`，
> analyze 任务完成后**不再**重复投 tag/name/classify。但若 `analyze_document_task` 内部走了
> LlamaParse 兜底（content 在任务内被新写入），则强制覆盖该开关，给新 content 补打 tag/name/classify。
>
> **用户主动触发（保留）：** FilesPage 右侧面板 "Re-analyze with different template" 仍走原 API
> （`POST /api/files/document/analyze` 或 `/audio/analyze`），调用时不传 `skip_post_trigger`，
> 末尾依旧会触发 tag/name/classify，与历史行为一致。

> **环境变量：**
> - `AUTO_ANALYZE_ENABLED`（默认 `true`）：总开关，关闭后 5 条上传链路均跳过 analyze。
> - `AUTO_ANALYZE_DELAY`（默认 `2`）：触发延迟秒，与 `AUTO_TAG_DELAY` / `AUTO_NAME_DELAY` 对齐。
> - `AUTO_ANALYZE_MIN_CONTENT_LENGTH`（默认 `100`）：content 长度门槛，低于该值且
>   `asr_with_diarization` 为空时跳过，避免空内容浪费 LLM 调用。

> **补充：** 用户通过 `POST /api/files/create-text-note` 创建的 **文本笔记**（`file_type='text'`）**不经过**上图中的 `FileProcessingWorker` 与 Celery 解析任务；在同步 API 内写入 `content` 后直接调用 `trigger_auto_tagging` / `trigger_auto_naming` / `trigger_auto_classify` / `trigger_auto_analyze`，与「`content` 非空写入完成后」的 4 路并行任务一致。

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

### 2.6 Apple Podcasts 单集链接

#### 设计决策：无需本地 Worker

Apple Podcasts 走的是 **iTunes 公开 API + 第三方 Podcast CDN**，CDN 对数据中心 IP 无封锁，与 YouTube（对数据中心 IP 有严格封锁）截然不同。因此 Apple Podcast 处理链路**无需**走本地家庭 IP Worker，远程服务器直接 yt-dlp 下载即可。

#### 上传入口

`POST /api/files/upload-apple-podcast`

- URL 必须包含 `?i=<episode_id>`（Apple 单集标识）
- 支持格式：`https://podcasts.apple.com/{locale}/podcast/{title}/id{podcast_id}?i={episode_id}`

#### 处理流程

```
前端 UploadApplePodcastModal
    │  POST /api/files/upload-apple-podcast
    │  Body: { apple_url, auto_analyze_template_id, auto_analyze_enabled,
    │          auto_tag_template_id, auto_tag_enabled, ... }
    ▼
backend/api/files.py: upload_apple_podcast_link()
    ├─ ApplePodcastService.extract_episode_id(url)  # 正则校验 + 提取 episode_id
    └─ upload_and_transcribe_apple_podcast_task.apply_async(queue='file_processing')
                │
                ├─ Step 5%   ApplePodcastService.fetch_metadata() → title / series / duration
                ├─ Step 10%  ApplePodcastService.download_audio() → /tmp/uploads/{episode}.mp3
                │               └─ yt-dlp + iTunes API + 第三方 CDN（无 IP 封锁）
                ├─ Step 40%  OSSService.upload_audio_file() → object_name + signed_url
                ├─ Step 50%  ASRService.submit_transcription_task(diarization_enabled=True)
                ├─ Step 60-90% get_transcription_results() 轮询
                ├─ Step 93%  格式化 ASR + 构建 asr_with_diarization 结构化数据
                ├─ Step 95%  INSERT processing_stats (task_name='Apple Podcast Processing')
                │            INSERT file_processing_details (file_type='podcast', file_storage_type='oss')
                ├─ Step 100% 清理临时文件
                │
                └─ 触发 4 路并行 LLM 任务（与 audio 一致）：
                        trigger_auto_tagging  (模板: content_tag_v1)
                        trigger_auto_naming   (模板: auto_name_v1)
                        trigger_auto_classify (模板: auto_classify_v1)
                        trigger_auto_analyze  (runtime 或缺省 audio_analysis_v1)
```

#### 错误分类

| 错误类型 | 示例 | 处理方式 |
|----------|------|----------|
| `PermanentDownloadError` | 付费内容、地区锁、404 | 任务标记 `failed`，不重试 |
| `TransientDownloadError` | 网络超时、CDN 5xx | 任务标记 `failed`，可手动重试 |

#### `upload_params` 字段（`processing_stats` 内）

| 字段 | 说明 |
|------|------|
| `apple_url` | 原始链接 |
| `episode_id` | Apple 单集 ID |
| `series` | Podcast 节目名称（来自 yt-dlp metadata） |
| `episode` | Episode 标题 |
| `podcast_thumbnail` | 封面图 URL |
| `object_name` | OSS 对象键 |
| `auto_analyze_template_id` / `auto_tag_template_id` 等 | 与 audio/youtube 一致 |

#### 重分析（Re-analyze）

`POST /api/files/audio/analyze` 接受 `file_type='podcast'`（等同 `audio`），使用 `audio_analysis_v1` 或用户指定模板。

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
| `file_type` | `'document'` / `'audio'` / `'youtube'` / `'podcast'` / `'web'` / `'text'`（用户撰写的文本笔记，无 OSS 原文） |
| `status` | `'uploaded'` / `'processing'` / `'completed'` / `'failed'` |
| `task_id` | Celery 任务 ID |
| `processing_time` | 处理耗时（秒） |

### `file_tags`（每个文件一行，自动打标产物）

| 字段 | 说明 |
|------|------|
| `file_id` | 关联 `file_processing_details.id`（唯一） |
| `sector` / `sector_en` / `industry` / `industry_en` | GICS 中英文行业分类 |
| `ticker` | JSONB 股票代码数组 |
| `concepts` | JSONB 投资概念/主题 |
| `people` | JSONB 关键人物 `[{name, name_en, role, affiliation?}]`，`name` 保留原文写法 |
| `countries` | JSONB 国家英文全名数组，如 `["United States", "China"]` |
| `tag_schema_version` | 打标 schema 版本，Upload 默认 `file_v1`（`content_tag_v1`） |
| `confidence` / `reasoning` / `summary` | 置信度、推理说明、摘要 |
| `additional_tags` | 扩展 JSONB（含 `tickers_full`、`ai_metadata` 等） |

---

## 4. Prompt Template 与 LLM 分析流程

文件内容写入 DB 后，系统会触发两类 LLM 分析：
- **系统自动触发（并行）**：自动打标签 + 自动命名 + **自动归类**（`classify_file_content_task`），无需用户干预，`content` 非空写入并满足各任务门槛后即触发。
- **用户主动触发**：用户在 FilesPage 选择 Prompt Template，手动发起内容深度分析。

---

### 4.1 内容深度分析（analyze_document_task / analyze_audio_task）

| 文件类型 | 分析 API | Celery 任务名 |
|----------|----------|---------------|
| 文档（PDF/Word/Excel/Web/Text Note） | `POST /api/files/document/analyze` | `analyze_document_async` |
| 音频 / YouTube | `POST /api/files/audio/analyze` | `analyze_audio_async` |

两个任务逻辑相同，差异在于音频路径优先读取 `asr_with_diarization`（含说话人标签）。

**两种触发路径，统一任务实现：**

| 触发路径 | 入口 | `template_id` 来源 | `skip_post_trigger` |
|----------|------|--------------------|---------------------|
| **自动并行链路** | `trigger_auto_analyze` (file_tasks.py) | 弹窗 **runtime**；未传则 `LLMTemplateResolver('file_analysis')`（文档/网页/文本）或 `audio_analysis_v1`（音频/YouTube 缺省） | `True`（已在并行链路里同步触发 tag/name/classify，避免重复入队）|
| **用户主动 Re-analyze** | FilesPage 右侧面板 → `POST /api/files/*/analyze` | 右侧 `TemplateSelector` 选中值 | `False`（保留历史行为：分析完成后再补一轮 tag/name/classify）|

```
       (自动)                                  (用户)
trigger_auto_analyze                  POST /api/files/*/analyze
   skip_post_trigger=True                skip_post_trigger=False
        │                                       │
        └────────────────┬──────────────────────┘
                         ▼
            analyze_document_task / analyze_audio_task
            │
            ├─ 【文档可选】若 content 为空 → 先调用 LlamaParse 补充解析
            │   └─ content_was_filled_by_llama=True (内部标记)
            │       覆盖 skip_post_trigger，强制末尾再触发 tag/name/classify 给新 content 打标
            │
            ├─ 读取 file_processing_details.content
            │   （音频/YouTube：优先 asr_with_diarization，格式化为"说话人+时间+文本"）
            │
            ├─ LocalLLMAnalysisService.process_all_contents(results, template_id)
            │       └─ 详见 4.7 LLM 调用核心链路
            │
            ├─ DB 写回：
            │       ├─ processing_stats.final_report    ← LLM 分析报告
            │       ├─ processing_stats.rc_agent_status = 'completed' / 'failed'
            │       ├─ processing_stats.total_tokens / total_price / llm_model / llm_provider
            │       └─ file_processing_details.ai_time / total_tokens / cost
            │
            └─ 末尾按需触发 tag/name/classify：
                    if (not skip_post_trigger) or content_was_filled_by_llama:
                        trigger_auto_tagging / naming / classify
```

> **`rc_agent_status` 状态机：**
> - 自动并行链路投递前，`trigger_auto_analyze` 把 `processing_stats.rc_agent_status` 置为 `'analyzing'`，与 `POST /api/files/{document,audio}/analyze` 手动入口对齐；
> - 任务完成根据 LLM 调用结果写 `'completed'` 或 `'failed'`；投递失败会把 `'analyzing'` 回滚为 `'failed'`，避免永远卡 analyzing；
> - **前端两处感知此中间态**：
>   1. **FilesPage 右侧 AI Analysis 面板**：`rc_agent_status === 'analyzing'` 时显示 "AI Analyzing..." 中间态；`'failed'` 时显示 "The previous AI analysis failed. Pick a template and try again." 并暴露 `Re-analyze` 按钮。
>   2. **NavBar 顶部 processing bar**（`TaskProgressPopover`）：上传 Celery 主任务结束时若 `rc_agent_status === 'analyzing'`，把 task 的 `status` 切成 `'ai_processing'`、`progress=95`，文案显示 "AI analyzing…"，等 `rc_agent_status` 落到 `'completed'` / `'failed'` 后再走真正的完成/失败流程；YouTube 链路的 `rcFinalized` 判定同样排除 `'analyzing'`。
>   - 后端 `GET /api/files/tasks/{task_id}/status` 通过 `file_processing_details.task_id → processing_stats.rc_agent_status` 把这个状态返给前端轮询。

---

### 4.2 系统自动触发的并行 LLM 任务（4 路并行）

每当某个文件的 `content` 字段非空写入完成，系统会**同时**（`countdown` 延迟后）投递最多 4 个独立 Celery 消息，互不依赖、可并发执行。其中 **analyze** 路：文档/网页/文本在 runtime 未传时由 `LLMTemplateResolver('file_analysis')` 解析模板；音频/YouTube 未传 runtime 时用 `audio_analysis_v1`；若用户关闭 `auto_analyze_enabled` 或环境变量关闭全局开关，则不投递。

```
content 写入完成
        │
        ├─────────────────────────┬──────────────────────────┬─────────────────────────────────┐
        ▼                         ▼                          ▼                                 ▼
trigger_auto_tagging      trigger_auto_naming      trigger_auto_classify             trigger_auto_analyze
        │                         │                          │                                 │
        ▼                         ▼                          ▼                                 ▼
tag_file_content_task   name_file_content_task   classify_file_content_task   analyze_document_task / analyze_audio_task
(queue: file_processing) (queue: file_processing) (queue: file_processing)    (queue: file_processing,
                                                                               skip_post_trigger=True)
        │                         │                          │                                 │
        ▼                         ▼                          ▼                                 ▼
（详见 4.3）                （详见 4.4）                （详见 4.5）                       （详见 4.1）
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
| `AUTO_ANALYZE_ENABLED` | `true` | 是否启用自动深度分析（4 路并行的第 4 路）|
| `AUTO_ANALYZE_DELAY` | `2`（秒） | analyze 任务延迟投递时间 |
| `AUTO_ANALYZE_MIN_CONTENT_LENGTH` | `100` | 内容低于此字数 **且** 无 `asr_with_diarization` 时跳过 analyze |

---

### 4.3 自动打标签（Auto Tagging）

> ⭐ **PR-5 重构（2026-04-27）**：模板不再硬编码 `content_tag_v1`，改走 `LLMTemplateResolver` 三级 fallback。详见 [LLM_REFACTOR_PLAN.md §4](./LLM_REFACTOR_PLAN.md#4-阶段-2file-tag-用户自定义2-3-工作日)。

**Celery 任务：** `tag_file_content`（`tagging_tasks.py`，queue: `file_processing`）
**module_code：** `file_tagging`（向 `MODULE_REGISTRY` 注册）
**Prompt Template：** 优先级 `runtime > user_template_preferences[file_tagging] > content_tag_v1`
**服务：** `ContentTaggingService`（`backend/services/content_tagging_service.py`）

```
tag_file_content_task(file_id, username, force=False, template_id=None, template_variables=None)
    │
    ├─ 1. 检查 AUTO_TAG_ENABLED（force=True 时跳过检查）
    ├─ 2. 检查 content 长度 ≥ AUTO_TAG_MIN_CONTENT_LENGTH
    ├─ 3. 读取 file_processing_details.content
    │
    ├─ 4. 【PR-5 新增】username → user_id (user_resolver.py LRU)
    │     LLMTemplateResolver.resolve('file_tagging', user_id, runtime_template_id)
    │       → ResolvedTemplate(template_id, source='runtime'|'preference'|'default')
    │
    ├─ 5. 若非 force 且 file_tags 表已有记录 → 跳过（幂等保护）
    ├─ 6. 截断 content 至 AUTO_TAG_CONTENT_MAX_CHARS 字符
    │
    ├─ 7. ContentTaggingService(template_id=resolved.template_id).tag_file_content(user_id=user_id)
    │       └─ LocalLLMAnalysisService → chat_completion_with_template
    │             ├─ 写 llm_prompt_usage_logs (含 user_id 归属) ⭐ PR-4
    │             └─ 详见 4.7 LLM 调用核心链路
    │
    ├─ 8. 解析 LLM 返回 JSON
    │
    └─ 9. ContentTaggingService.save_tags(file_id, tags)
            └─ INSERT INTO file_tags ... ON CONFLICT (file_id) DO UPDATE
                  (含 people / countries / tag_schema_version='file_v1')
```

**`content_tag_v1` 输出字段（2026-05-19 扩展）：** 除原有 `sector/sector_en/industry/industry_en/tickers/concepts/confidence/summary` 外，新增：
- **`people`**：文档中被讨论的关键人物，`name` 保留原文写法（如 Trump、Powell），`name_en` 为英文全名。
- **`countries`**：国家英文全名数组（如 `United States`、`China`），不使用 ISO 缩写。
- 无相关人物/国家时返回 `[]`。

**触发链路接 template_id：** 5 个上传 API 入参均增加 `auto_tag_template_id` / `auto_tag_template_variables`：
- `POST /api/files/upload-and-process-document`
- `POST /api/files/upload-audio`
- `POST /api/files/upload-youtube`
- `POST /api/files/upload-web-link`
- `POST /api/files/create-text-note`

参数沿调用链透传到 `trigger_auto_tagging` → `tag_file_content_task`。

**手动触发 API：**
- `POST /api/file-tags/retag` → 单文件重新打标签（**PR-5 增加 `template_id` 参数**）
- `POST /api/file-tags/retag-batch` → 批量重新打标签（同上）

**用户偏好层：** `user_template_preferences` 中 `module_code='file_tagging'` 的行作为该用户的默认模板。在 PromptTemplatesPage 设置后，所有该用户上传的文件（即使没在弹窗选模板）都用这个偏好。

**前端入口：**
- 5 个上传弹窗增加第二个 `<UploadAnalysisTemplatePicker moduleCode="file_tagging" />`
- FilesPage 右侧 Tags 面板增加 "Retag with template" 按钮 + Modal

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
| `process_files_task`（批量解析文档，`POST /api/files/process`） | 解析完成，查 `file_processing_details` where `task_id=self.request.id AND status='completed' AND content != ''` | **仅** `trigger_auto_tagging` + `trigger_auto_naming` + `trigger_auto_classify`（当前实现**未**调用 `trigger_auto_analyze`；需深度分析请走 FilesPage 手动 Re-analyze 或上传并解析弹窗） |
| `upload_and_process_document_task`（上传并解析） | 解析完成，查 `file_processing_details` where `task_id=self.request.id AND status='completed' AND content != ''` | 四路：`trigger_auto_tagging` + `trigger_auto_naming` + `trigger_auto_classify` + `trigger_auto_analyze`（弹窗 runtime 或 `LLMTemplateResolver('file_analysis')`） |
| `upload_and_transcribe_audio_task`（音频转写） | ASR 写入 `content` 后 | 四路；`trigger_auto_analyze` 为弹窗 runtime 或缺省 `audio_analysis_v1` |
| `upload_and_transcribe_apple_podcast_task`（Apple Podcasts 下载+转写） | ASR 写入 `content` 后；`file_type='podcast'` | 四路齐全；`trigger_auto_analyze` 为弹窗 runtime 或缺省 `audio_analysis_v1` |
| `scrape_and_process_web_link_task`（网页抓取） | HTML 转纯文本写入 `content` 后 | 四路；`trigger_auto_analyze` 为弹窗 runtime 或 `LLMTemplateResolver('file_analysis')` |
| `process_web_clip_task`（Chrome 剪藏） | 去标签写入 `content` 后 | 四路齐全：`trigger_auto_tagging` + `trigger_auto_naming` + `trigger_auto_classify` + `trigger_auto_analyze`（无弹窗 runtime，`trigger_auto_analyze` 内走 `LLMTemplateResolver('file_analysis')`） |
| `finalize_youtube_file_task`（YouTube 链接，由本地 Worker 回调） | 字幕/ASR 写入 `content` 后，仅 outcome='completed' 触发；从 `processing_stats.upload_params.auto_analyze_template_id` 读模板 | `trigger_auto_tagging` + `trigger_auto_naming` + `trigger_auto_classify` + `trigger_auto_analyze`（默认 `LLMTemplateResolver('youtube_analysis')` → `youtube_analysis_v1`）|
| `analyze_document_task`（用户 Re-analyze 文档） | `skip_post_trigger=False`（API 入口）→ LLM 分析报告写入后 | `trigger_auto_tagging` + `trigger_auto_naming` + `trigger_auto_classify`（不再递归触发 analyze）|
| `analyze_document_task`（自动并行链路） | `skip_post_trigger=True` 且未走 LlamaParse 兜底 | **不再触发**（避免与上层并行链路重复）|
| `analyze_document_task`（自动链路 + 任务内 LlamaParse） | `skip_post_trigger=True` 但 `content_was_filled_by_llama=True` | `trigger_auto_tagging` + `trigger_auto_naming` + `trigger_auto_classify`（给新 content 补打标）|
| `analyze_audio_task`（用户 Re-analyze 音频） | `skip_post_trigger=False`（API 入口） | `trigger_auto_tagging` + `trigger_auto_naming` + `trigger_auto_classify` |
| `analyze_audio_task`（自动并行链路） | `skip_post_trigger=True` | **不再触发** |
| `reparse_document_task`（重新解析） | 重新解析写入 `content` 后 | **仅** `trigger_auto_tagging` + `trigger_auto_naming` + `trigger_auto_classify`（当前实现**未**调用 `trigger_auto_analyze`） |
| `POST /api/files/create-text-note`（**同步 HTTP**，非 Celery） | 插入 `file_processing_details` 并写入 `content` 后，在 API 内直接调用 | 四路齐全；`trigger_auto_analyze` 为弹窗 runtime 或 `LLMTemplateResolver('file_analysis')` |

> **`trigger_auto_analyze` 是否投递：** 用户侧 `auto_analyze_enabled` 与全局 `AUTO_ANALYZE_ENABLED` 为真时，文档/网页/文本走 **runtime 或** `LLMTemplateResolver('file_analysis')`；纯音频可走 runtime 或缺省 `audio_analysis_v1`；YouTube 可走 runtime 或 `LLMTemplateResolver('youtube_analysis')`（缺省 `youtube_analysis_v1`，独立于 Feeds `youtube_summary` 模块）。`POST /api/files/process` 触发的 `process_files_task` 当前不调用本函数。

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
| `uploadAndProcessDocument(payload, analyzeOptions?)` | `POST /api/files/upload-and-process-document` |
| `uploadAudioFile(file, analyzeOptions?)` | `POST /api/files/upload-audio` |
| `uploadYouTubeLink(payload, analyzeOptions?)` | `POST /api/files/upload-youtube` |
| `uploadApplePodcastLink(appleUrl, analyzeOptions?)` | `POST /api/files/upload-apple-podcast` |
| `uploadWebLink(payload, analyzeOptions?)` | `POST /api/files/upload-web-link` |
| `createTextNote(payload, analyzeOptions?)` | `POST /api/files/create-text-note` |

> 5 个上传方法均接受可选 `analyzeOptions = { auto_analyze_template_id?, auto_analyze_template_variables? }`。前端在 5 个上传弹窗中通过共用组件 `<UploadAnalysisTemplatePicker mode="optional" />`（`web-frontend/src/components/UploadAnalysisTemplatePicker.tsx`）让用户选择模板；该组件包了 `<TemplateSelector autoSelectDefault={false} />`（默认不预选：表示不传 **runtime** `template_id`；后端 `trigger_auto_analyze` 会对文档/网页/文本再调用 `LLMTemplateResolver('file_analysis')`，使用用户私有或系统默认 `file_analysis_v1`，而非整路跳过）+ `<TemplateVariablesEditor />`。用户显式选的 `auto_analyze_template_id` 沿上传任务透传为 runtime（最高优先）；YouTube 终态在远程 `finalize_youtube_file_task` 回调，模板参数先写入 `processing_stats.upload_params.auto_analyze_template_id` / `auto_analyze_template_variables` 再读出。
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
| 上传 Apple Podcasts 单集链接 | `POST /api/files/upload-apple-podcast` | `upload_and_transcribe_apple_podcast_task`（远程直接 yt-dlp，无需本地 Worker）|
| 上传网页链接 | `POST /api/files/upload-web-link` | `scrape_and_process_web_link_task` |
| 上传 Chrome 剪藏 | `POST /api/files/upload-web-clip` | `process_web_clip_task` |
| 创建文本笔记（无上传文件） | `POST /api/files/create-text-note` | —（同步 API，成功后 API 内触发并行 LLM 任务） |

### 内容更新（Markdown 编辑）

| 功能 | 方法 + 路径 | 说明 |
|------|-------------|------|
| 更新文件正文（Content / Original 编辑保存） | `PATCH /api/files/details/{file_id}/content` | Body：`content`；可选 `trigger_llm`（默认 `false`），为 `true` 时在保存后再次触发 `trigger_auto_tagging` / `trigger_auto_naming` / `trigger_auto_classify` |

### 用户主动触发的 LLM 分析（FilesPage 右侧 Re-analyze）

| 功能 | 方法 + 路径 | Celery 任务 | Prompt Template |
|------|-------------|-------------|-----------------|
| 文档内容分析 | `POST /api/files/document/analyze` | `analyze_document_async` | 用户选择（动态），`skip_post_trigger=False` |
| 音频/YouTube 内容分析 | `POST /api/files/audio/analyze` | `analyze_audio_async` | 用户选择，默认 `audio_analysis_v1`，`skip_post_trigger=False` |
| 测试 Prompt Template | `POST /api/llm-prompt-templates/templates/test` | — | 用户选择（动态） |

### 系统自动触发的并行 LLM 任务（4 路并行）

| 功能 | 触发方式 | Celery 任务 | Prompt Template |
|------|----------|-------------|-----------------|
| 自动打标签 | 系统自动（content 写入后） | `tag_file_content` | `content_tag_v1`（固定） |
| 自动命名 | 系统自动（content 写入后） | `name_file_content` | `auto_name_v1`（固定） |
| 自动归类 | 系统自动（content 写入后） | `classify_file_content` | `auto_classify_v1`（固定） |
| 自动深度分析 | 系统自动（content 写入后，且用户在上传弹窗选择了模板，或音频/YouTube 缺省） | `analyze_document_async` / `analyze_audio_async`（`skip_post_trigger=True`） | 用户选择（动态），纯音频缺省 `audio_analysis_v1`，YouTube 缺省 `LLMTemplateResolver('youtube_analysis')` → `youtube_analysis_v1` |

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

13. **Apple Podcasts 与 YouTube 的架构差异**  
    YouTube 对数据中心 IP 有严格封锁，故 `upload-youtube` 通过本地家庭 IP Worker 下载；Apple Podcasts 使用公开 iTunes API + 第三方 Podcast CDN，无此限制，`upload-apple-podcast` 直接在远程服务器由 Celery 任务调用 yt-dlp 下载，**不需要** `local_virtual_service` Worker 参与。
    - `file_type='podcast'` 在 `trigger_auto_analyze` 中被视为与 `audio` 同类（`is_audio_like`），缺省模板为 `audio_analysis_v1`。
    - FilesPage 通过常量 `AUDIO_LIKE_TYPES = ['audio', 'podcast']` 让 podcast 文件复用 ASR 转写视图、重分析面板、音频播放器。

14. **`file_tags.people` / `file_tags.countries`（2026-05-19）**  
    - 由 `content_tag_v1` 模板在自动打标时写入；`tag_schema_version` 默认为 `file_v1`。  
    - FilesPage 右侧 **Tags** 面板与 Daily 抽屉（Upload 来源）展示 People / Countries 标签。  
    - 历史记录无此字段时，可在 FilesPage 点 **Retag** 或调用 `POST /api/file-tags/retag-batch?force=true` 按需补打。

---

## 运维提示（分析卡死 / Worker 异常）

若上传后长时间停在「AI analyzing」或 Celery Worker 反复重启，常见原因包括：**Redis 中损坏或过期的 `celery-task-meta-*`**、**模板推荐 Provider 在 DB 中未启用**、**HTTP 层 `timeout` 远小于实际 LLM 耗时**。本地优先用 `development/stop_local_dev.sh` / `start_local_dev.sh`；生产环境用仓库根目录 `restart_services.sh`（或 `stop_services.sh` / `start_services.sh`），并在修改 `deployment/templates/rc-celery-*.service.tpl` 后执行 `sudo bash deployment/deploy.sh && sudo systemctl daemon-reload && sudo ./restart_services.sh`。更完整的说明见 `docs/instructions/LLM_ARCHITECTURE_OVERVIEW.md` §7.5。

## 相关源码文件

| 文件 | 说明 |
|------|------|
| `backend/api/files.py` | HTTP 入口；含 `create-text-note`、`PATCH .../content`（可选 `trigger_llm`） |
| `backend/api/tasks.py` | 任务执行列表与单条详情；`folder_id` / `folder_name` 子查询 |
| `backend/api/file_tags.py` | 打标签/命名/归类的 HTTP 管理接口 |
| `backend/tasks/file_tasks.py` | Celery 任务定义（上传/解析/分析），含 `trigger_auto_tagging` / `trigger_auto_naming` / `trigger_auto_classify` / `trigger_auto_analyze`，以及 `analyze_document_task` / `analyze_audio_task` 的 `skip_post_trigger` 与（仅文档）`content_was_filled_by_llama` 控制 |
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
| `web-frontend/src/services/files.ts` | 前端文件服务层（含 `createTextNote`、`updateFileDetailContent`，5 个上传方法均接受 `analyzeOptions`） |
| `web-frontend/src/components/UploadAnalysisTemplatePicker.tsx` | 5 个上传弹窗共用的 AI Analysis 模板选择器（包 `TemplateSelector` + `TemplateVariablesEditor`，`autoSelectDefault={false}`） |
| `web-frontend/src/components/UploadFilesModal.tsx` | Upload & Process Files 弹窗（已接入 picker） |
| `web-frontend/src/components/UploadAudioModal.tsx` | Upload Audio Files 弹窗（已接入 picker，缺省提示走 audio_analysis_v1） |
| `web-frontend/src/components/UploadYouTubeLinkModal.tsx` | Upload YouTube Link 弹窗（已接入 picker，模板参数随 `upload_params` 持久化）|
| `web-frontend/src/components/UploadWebLinkModal.tsx` | Web Link 弹窗（已接入 picker） |
| `web-frontend/src/components/CreateTextNoteModal.tsx` | 文本笔记弹窗；`TASK_EXECUTIONS_REFRESH_EVENT`，已接入 picker |
| `web-frontend/src/components/TemplateSelector.tsx` | 通用模板选择器，新增 `autoSelectDefault` 控制是否自动选默认模板 |
| `web-frontend/src/components/Layout.tsx` | 桌面浮动上传菜单（My Notes / Thoughts 置顶） |
| `web-frontend/src/pages/MobileSourcesPage.tsx` | 移动端 Sources 与上传菜单 |
| `web-frontend/src/pages/FilesPage.tsx` | PC Sources；`ExecutionDetailsPanel`（含 Folder Type）、`OriginalTextNotePanel`；右侧 AI Analysis 面板感知 `rc_agent_status='analyzing'` 中间态与 `failed` fallback 文案，仍保留 Re-analyze 入口 |
| `web-frontend/src/contexts/FileProcessingContext.tsx` | NavBar 任务轮询；处理 `rc_agent_status='analyzing'` 中间态（status → `'ai_processing'`），YouTube `rcFinalized` 排除 `analyzing` |
| `web-frontend/src/components/TaskProgressPopover.tsx` | NavBar 顶部任务列表弹层；新增 `'ai_processing'` 状态文案（"AI analyzing…"）|
| `web-frontend/src/pages/PromptTemplatesPage.tsx` | Prompt Template 管理与测试页面 |
