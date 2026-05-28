# Vector RAG 系统说明

本文档描述基于 **阿里云 OSS Vector Bucket** 的 RAG（Retrieval-Augmented Generation）系统的完整架构、数据流、配置要求及运维操作，适用于开发者与运维人员。

---

## 目录

1. [背景与设计目标](#1-背景与设计目标)
2. [整体架构](#2-整体架构)
3. [数据流详解](#3-数据流详解)
   - [3.0 Ebook 专用路径（长文档 / 电子书）](#30-ebook-专用路径长文档--电子书)
   - [3.1 文件上传向量化](#31-文件上传向量化)
   - [3.2 Feed 内容向量化](#32-feed-内容向量化)
   - [3.3 语义搜索流程](#33-语义搜索流程)
4. [核心组件](#4-核心组件)
5. [数据库结构](#5-数据库结构)
6. [环境变量配置](#6-环境变量配置)
7. [自动触发规则（5条判定）](#7-自动触发规则5条判定)
8. [MCP 工具（v0.3.0）](#8-mcp-工具v030)
9. [API 端点（v2）](#9-api-端点v2)
10. [Celery 任务清单](#10-celery-任务清单)
11. [运维操作](#11-运维操作)
12. [阿里云 OSS Vector Bucket 配置](#12-阿里云-oss-vector-bucket-配置)
13. [文件变更清单](#13-文件变更清单)

---

## 1. 背景与设计目标

### 问题

用户可上传超百万字的电子书或长文档。直接将全文塞入 LLM 上下文窗口无法实现，且第三方 Agent（通过 GuanHe MCP 调用）无法对这类长文档进行语义检索。

### 解决方案

引入 **分块（Chunking）+ 向量化（Embedding）+ 向量数据库（OSS Vector Bucket）** 三层管道：

1. 文档分块成 ~600 字的语义片段
2. 每个片段通过 Qwen `text-embedding-v3` 生成 1024 维向量
3. 向量与元数据写入 OSS Vector Bucket 索引，原始文本写入 PostgreSQL
4. Agent 提问时将问题向量化，在 Vector Bucket 中做近邻检索，取回最相关的文本片段

### 核心原则

- **PostgreSQL 是真相层**：所有文本内容、元数据、状态均以 PG 为准
- **OSS Vector Bucket 是索引层**：只存向量 + 轻量过滤元数据，不存原文
- **幂等性**：以 `content_hash`（SHA-256）跳过未变更内容的重复处理
- **非侵入**：v1 API 与现有逻辑保持不变，v2 以新端点并行引入

---

## 2. 整体架构

```
┌─────────────────────────────────────────────────────────────────┐
│                         数据入口                                 │
│   Upload Sources          Feeds Subscriptions                   │
│  (PDF/Word/TXT/MD…)    (YouTube/Twitter/RSS…)                   │
└────────────┬────────────────────┬───────────────────────────────┘
             │ 文件处理完成        │ Feed 抓取完成
             ▼                    ▼
     ┌───────────────────────────────────┐
     │        vectorize_triggers.py      │  ← 5条判定规则
     │  trigger_auto_vectorize_file/feed │
     └──────────────┬────────────────────┘
                    │ 满足条件则入队
                    ▼
     ┌───────────────────────────────────┐
     │  Celery AI Worker (ai_processing) │
     │   vectorize_file_content_task     │
     │   vectorize_feed_content_task     │
     └──────────────┬────────────────────┘
                    │
          ┌─────────┴──────────┐
          ▼                    ▼
  ┌───────────────┐   ┌────────────────────┐
  │ ChunkingService│   │  Qwen Embedding API │
  │ ~600字/块      │   │ text-embedding-v3  │
  │ MD标题感知     │   │ 1024维向量          │
  └───────┬───────┘   └────────┬───────────┘
          │                    │
          ▼                    ▼
  ┌───────────────┐   ┌────────────────────┐
  │  PostgreSQL   │   │ OSS Vector Bucket  │
  │document_chunks│   │   contentchunks    │
  │  (原文+状态)   │   │  (向量+元数据)      │
  └───────────────┘   └────────────────────┘
                              │
                    语义检索请求
                              ▼
     ┌───────────────────────────────────┐
     │      VectorSearchService          │
     │  semantic_search / hybrid_search  │
     └──────────────┬────────────────────┘
                    │
          ┌─────────┴──────────┐
          ▼                    ▼
  ┌───────────────┐   ┌────────────────────┐
  │ FastAPI v2    │   │  GuanHe MCP v0.3.0  │
  │/api/v2/agent/ │   │ semantic_search 等  │
  └───────────────┘   └────────────────────┘
```

---

## 3. 数据流详解

### 3.0 Ebook 专用路径（长文档 / 电子书）

当上传文件解析完成后，`file_tasks.py` 会判断内容是否超过 **EBOOK_MIN_CHARS（默认 10 万字符）**，满足则走独立的 Map-Reduce 路径，跳过普通全量 AI Analysis。

```
文件上传解析完成（Quick Parse，全页无截断）
        │
        ├── [所有文件] → trigger_auto_vectorize_file
        │                   → Chunking → Qwen Embedding → OSS Vector Bucket
        │
        └── [判断 content_len >= EBOOK_MIN_CHARS？]
                │
        否（普通文档）       是（电子书 / 长研报）
                │                   │
    trigger_auto_analyze        rc_agent_status = 'analyzing'
    （用户选模板才触发）              │
                              延迟 EBOOK_MAP_REDUCE_DELAY 秒（默认 150s，等向量化先完成）
                                    │
                          ebook_map_reduce_task（Celery ai_processing）
                                    │
                      ┌─────────────┴──────────────┐
                   MAP 阶段                     REDUCE 阶段
          按 chapter 读 document_chunks    所有章节摘要拼接
          每章调一次 LLM (qwen-turbo)      再调一次 LLM → 全书概览
          生成 chapter_summary             写入 processing_stats.final_report
                      └─────────────┬──────────────┘
                              rc_agent_status = 'success'
```

**触发阈值（环境变量）**

| 变量 | 默认 | 说明 |
|------|------|------|
| `EBOOK_MIN_CHARS` | `100000` | 内容字符数门槛 |
| `EBOOK_MAP_REDUCE_DELAY` | `150` | 等向量化完成的延迟（秒） |
| `EBOOK_MAP_MAX_CHARS` | `8000` | 单章 Map 输入上限 |
| `EBOOK_REDUCE_MAX_CHARS` | `32000` | Reduce 输入上限 |

**成本估算（The Nvidia Way, 246 页 / 1110 chunks / 46 章）**

| 方案 | LLM | 费用 |
|------|-----|------|
| 传统全量 AI Analysis | Claude Sonnet | ~$0.07 USD，有上下文溢出风险 |
| Ebook Map-Reduce | qwen-turbo × 47次 | ~0.085 RMB，无溢出 |

> **Quick Parse 注意**：Quick Parse 模式下系统不限制页数（`max_pages=0`），确保电子书全文被解析入库。`Max Pages` 参数仅对 LlamaParse（云端按页计费）有效。

---

### 3.1 文件上传向量化

```
POST /files/upload
        │
        ▼
file_tasks.py → process_files_async
        │
        ├─ analyze_document_async / analyze_audio_async
        │
        └─ _trigger_vectorize_files()
                │
                ▼
        trigger_auto_vectorize_file()  ← 5条判定
                │  通过
                ▼
        vectorize_file_content_task.apply_async(
            args=[file_id], countdown=2
        )
                │
                ▼
        DocumentVectorizeService._vectorize()
          1. 加载 file_processing_details
          2. 计算 content_hash，跳过未变更
          3. 清理失败/过时的旧 chunks
          4. ChunkingService.chunk_text()
          5. INSERT document_chunks（PG）
          6. Qwen embed（批次=10）
          7. VectorBucketClient.upsert_vectors()
          8. UPDATE file_processing_details SET
               vector_status='done',
               vector_chunk_count=N,
               vector_indexed_at=NOW(),
               content_hash=<hash>
```

### 3.2 Feed 内容向量化

Feed 向量化触发点位于 `trigger_auto_feed_tagging` 之后，逻辑与文件一致，区别：
- 父表为 `subscription_content`（`content_id` 关联）
- `source_type` 标记为各 Feed 类型（`youtube`、`twitter`、`rss` 等）

### 3.3 语义搜索流程

```
MCP: semantic_search(query="价值投资方法论")
        │
        ▼
POST /api/v2/agent/semantic-search
        │
        ▼
VectorSearchService.semantic_search()
  1. Qwen embed(query) → 1024维向量
  2. VectorBucketClient.query_vectors(
       vector=q_vec, top_k=top_k,
       filter={owner_username: user.username}  ← 预过滤
     )
  3. 取回 vector keys → 解析 file_id/content_id
  4. SELECT document_chunks + JOIN parent 表
     WHERE owner = user.username              ← 后过滤（二次授权校验）
  5. 返回 SemanticSearchResponse(results=[SearchHit...])
```

**双层授权**：Vector Bucket 元数据预过滤（速度快）+ PostgreSQL JOIN 后过滤（精确权限）

---

## 4. 核心组件

| 文件 | 职责 |
|------|------|
| `backend/services/vector_bucket_client.py` | OSS Vector Bucket SDK 封装，upsert / query / delete |
| `backend/services/chunking_service.py` | 文本分块，支持 Markdown 标题感知 + 普通文本滑窗 |
| `backend/services/document_vectorize_service.py` | 向量化编排：分块→嵌入→上传→写状态 |
| `backend/services/vector_search_service.py` | 语义搜索、混合搜索、定位检索 |
| `backend/tasks/vectorize_tasks.py` | Celery 任务入口（文件/Feed 向量化、删除、对账） |
| `backend/tasks/vectorize_triggers.py` | 5条判定规则，决定是否触发向量化 |
| `backend/api/agent_v2_search.py` | FastAPI v2 路由 |
| `backend/schemas/agent_search.py` | 请求/响应 Pydantic Schema |
| `tools/guanhe-mcp/guanhe_mcp/server.py` | MCP 工具注册与 dispatch |

---

## 5. 数据库结构

### `document_chunks` 表

文本片段的 PostgreSQL 存储（真相层）。

| 列 | 类型 | 说明 |
|----|------|------|
| `id` | bigint PK | 自增主键，即 vector key |
| `source_type` | text | `upload` / `youtube` / `twitter` / `rss` 等 |
| `file_id` | integer | 关联 `file_processing_details.id`（文件类） |
| `content_id` | integer | 关联 `subscription_content.id`（Feed 类） |
| `owner_username` | text | 内容所有者用户名（权限过滤） |
| `chunk_idx` | integer | 块在文档中的序号（0-based） |
| `chapter` | text | Markdown 标题 / 章节名（可为 NULL） |
| `page` | integer | 页码（PDF 类，可为 NULL） |
| `char_start` | integer | 在原文中的起始字符偏移 |
| `char_end` | integer | 在原文中的结束字符偏移 |
| `chunk_text` | text | 实际文本片段 |
| `token_count` | integer | 估算 token 数 |
| `content_hash` | text | 父文档 SHA-256，用于幂等判断 |
| `vector_written` | boolean | 是否已成功写入 OSS Vector Bucket |
| `created_at` | timestamptz | 写入时间 |

### `file_processing_details` 新增列

| 列 | 类型 | 说明 |
|----|------|------|
| `vector_status` | text | `NULL` / `running` / `done` / `skipped` / `failed` |
| `vector_skip_reason` | text | 跳过原因（`too_short` / `type_not_supported` 等） |
| `vector_chunk_count` | integer | 向量化的分块数量 |
| `vector_indexed_at` | timestamptz | 最近一次向量化完成时间 |
| `vector_error` | text | 失败时的错误信息（截断至 1000 字符） |
| `content_hash` | text | 内容 SHA-256（幂等性） |

`subscription_content` 新增相同的 6 个列。

---

## 6. 环境变量配置

在 `/opt/guanhetech/.env` 中配置以下变量：

```bash
# ── OSS Vector Bucket ─────────────────────────────────────────────
OSS_VECTOR_BUCKET=guanhetech-vec          # Bucket 名称
OSS_VECTOR_REGION=cn-hongkong             # Bucket 所在地域
OSS_VECTOR_INDEX=contentchunks            # 索引表名（不能含下划线）
OSS_VECTOR_ACCOUNT_ID=5893560335603697    # 阿里云主账号 UID

# OSS 访问凭证（与普通 OSS 共用）
OSS_ACCESS_KEY_ID=<your-ak>
OSS_ACCESS_KEY_SECRET=<your-sk>

# ── 向量化行为 ────────────────────────────────────────────────────
AUTO_VECTORIZE_ENABLED=true               # 总开关
AUTO_VECTORIZE_DELAY=2                    # 触发延迟（秒）
AUTO_VECTORIZE_MIN_CONTENT_LENGTH=3000    # 内容最短字数（否则跳过）
AUTO_VECTORIZE_FORCE_LONG_THRESHOLD=32000 # 超过此字数强制向量化
AUTO_VECTORIZE_MAX_CONTENT_LENGTH=10000000 # 内容上限（超出则跳过）

# ── 分块参数 ──────────────────────────────────────────────────────
CHUNK_TARGET_CHARS=600    # 目标块大小（字符数）
CHUNK_OVERLAP_CHARS=80    # 相邻块重叠字符数
```

> **`OSS_VECTOR_INDEX` 命名限制**：阿里云 OSS Vector Bucket 索引表名不允许包含下划线，请使用驼峰或纯小写字母。

---

## 7. 自动触发规则（5条判定）

文件或 Feed 处理完成后，`vectorize_triggers.py` 依次检查以下 5 条规则，**全部通过才触发**向量化任务：

| # | 规则 | 参数 |
|---|------|------|
| 1 | **全局开关** | `AUTO_VECTORIZE_ENABLED=true` |
| 2 | **用户设置** | 用户未明确禁用向量化 |
| 3 | **类型白名单** | 文件类型支持：`pdf`、`docx`、`txt`、`md`、`epub`、`podcast`、`youtube`、`twitter`、`rss` 等 |
| 4 | **内容长度** | `MIN_CONTENT_LENGTH ≤ 内容字数 ≤ MAX_CONTENT_LENGTH`；超过 `FORCE_LONG_THRESHOLD` 则无视白名单强制向量化 |
| 5 | **幂等性** | `vector_status = 'done'` 且 `content_hash` 未变更则跳过 |

---

## 8. MCP 工具（v0.3.0）

GuanHe MCP 新增 3 个工具，与 v1 工具并存：

### `semantic_search`

概念级语义检索，适合问题无法用关键词精确描述时使用。

```json
{
  "query": "价值投资方法论",
  "top_k": 10,
  "source_types": ["upload"],
  "since": "30d",
  "min_score": 0.3
}
```

### `hybrid_search`

关键词搜索（Daily v1 ILIKE/pg_trgm）+ 语义搜索，RRF 算法融合排序。适合查询中同时包含专有名词（股票代码、人名）和概念描述时使用。

```json
{
  "query": "Powell 降息预期",
  "top_k": 10,
  "since": "7d"
}
```

### `chunks_by_locator`

按已知位置（章节、页码、块序号范围）精确取回连续片段。适合 `semantic_search` 已定位到某章节、需要读取完整段落时使用。

```json
{
  "file_id": 1357,
  "chapter": "第三章 价值评估",
  "chunk_idx_from": 0,
  "chunk_idx_to": 10
}
```

### 工具选择决策树

```
是否能用关键词精确描述？
    └─ 否 → semantic_search
    └─ 是（含专有名词 + 概念混合）→ hybrid_search
    └─ 已知章节/页码位置 → chunks_by_locator
    └─ 短文档/Feed 全文 → daily_items_batch（v1）
```

> **Citation Rule**：返回结果均为 chunk 级片段，务必在回答中引用 `content_id` 以便用户在 GuanHe UI 中核实原文来源。

---

## 9. API 端点（v2）

所有 v2 端点均需 PAT（Personal Access Token）认证，scope：`daily:read`。

### `POST /api/v2/agent/semantic-search`

**Request**

```json
{
  "query": "string（必填）",
  "top_k": 10,
  "source_types": ["upload", "youtube"],
  "file_ids": [1357],
  "content_ids": [12345],
  "since": "30d | 2026-01-01",
  "lang": "zh | en",
  "min_score": 0.3
}
```

**Response**

```json
{
  "results": [
    {
      "content_id": 1357,
      "chunk_id": 676,
      "source_type": "upload",
      "title": "投资策略手册",
      "chapter": "第三章",
      "page": null,
      "chunk_text": "价值投资的核心在于...",
      "score": 0.4833,
      "found_via": ["vector"]
    }
  ]
}
```

### `POST /api/v2/agent/hybrid-search`

Request 同 `semantic-search`，额外支持 `keyword_weight`（默认 0.3）和 `semantic_weight`（默认 0.7）。

### `POST /api/v2/agent/chunks-by-locator`

**Request**

```json
{
  "file_id": 1357,
  "content_id": null,
  "chapter": "第三章",
  "page": null,
  "chunk_idx_from": 0,
  "chunk_idx_to": 20
}
```

**Response** 同 semantic-search 格式，`found_via: ["locator"]`。

---

## 10. Celery 任务清单

所有向量化任务路由到 `ai_processing` 队列（`rc-celery-ai.service`）。

| 任务名 | 触发场景 | 参数 |
|--------|----------|------|
| `vectorize_file_content` | 文件处理完成后自动触发 | `file_id: int` |
| `vectorize_feed_content` | Feed 抓取完成后自动触发 | `content_id: int` |
| `delete_file_vectors` | 删除文件时清理 | `file_id: int` |
| `delete_feed_vectors` | 删除 Feed 内容时清理 | `content_id: int` |
| `delete_vectors_by_keys` | 批量删除指定 vector keys | `vector_keys: List[int]` |
| `reconcile_vector_bucket` | 每日 04:00 UTC 自动对账 | 无 |

**Celery Beat 定时任务**

```python
# celery_config.py
"reconcile-vector-bucket-daily": {
    "task": "reconcile_vector_bucket",
    "schedule": crontab(hour=4, minute=0),
}
```

---

## 11. 运维操作

### 手动触发单个文件向量化

```python
from backend.tasks.vectorize_tasks import vectorize_file_content_task

# file_id 对应 file_processing_details.id
result = vectorize_file_content_task.delay(1357)
print(result.id)
```

### 批量向量化历史存量文件

```sql
-- 查找未向量化的大文件
SELECT fpd.id, fpd.filename, fpd.content_length
FROM file_processing_details fpd
WHERE fpd.content IS NOT NULL
  AND fpd.content_length > 3000
  AND fpd.vector_status IS NULL
ORDER BY fpd.content_length DESC;
```

然后逐一调用任务，或通过 `reconcile_vector_bucket` 任务触发批量对账。

### 重置向量化状态（重新处理）

```sql
-- 强制重新向量化（清空 hash 和状态）
UPDATE file_processing_details
SET vector_status = NULL, content_hash = NULL
WHERE id = 1357;
```

再次触发 `vectorize_file_content_task.delay(1357)`。

### 查看向量化状态统计

```sql
SELECT vector_status, COUNT(*)
FROM file_processing_details
GROUP BY vector_status
ORDER BY count DESC;

SELECT vector_status, COUNT(*)
FROM subscription_content
GROUP BY vector_status
ORDER BY count DESC;
```

### 查看 document_chunks 统计

```sql
SELECT source_type,
       COUNT(*) AS total_chunks,
       COUNT(CASE WHEN vector_written THEN 1 END) AS written,
       COUNT(DISTINCT COALESCE(file_id, content_id)) AS parent_docs
FROM document_chunks
GROUP BY source_type;
```

### 检查日志

```bash
# Celery AI worker（向量化任务执行日志）
tail -f /opt/guanhetech/logs/celery_ai_worker.log | grep -i vector

# FastAPI 后端（v2 API 请求日志）
tail -f /opt/guanhetech/logs/app.log | grep -i "v2/agent"
```

### 重启相关服务

```bash
# 重启后端（加载新代码）
sudo systemctl restart rc-webapp.service

# 重启向量化 Celery worker
sudo systemctl restart rc-celery-ai.service

# 重启 Beat 调度器（reconcile 定时任务）
sudo systemctl restart rc-scheduler.service
```

---

## 12. 阿里云 OSS Vector Bucket 配置

### Bucket 信息

| 参数 | 值 |
|------|-----|
| Bucket 名称 | `guanhetech-vec` |
| 地域 | 香港（`cn-hongkong`） |
| 索引表名 | `contentchunks` |
| 向量维度 | 1024 |
| 距离度量 | Cosine |
| 嵌入模型 | Qwen `text-embedding-v3` |

### RAM 权限要求

`OSS_ACCESS_KEY_ID` 对应的 RAM 用户需具备以下 OSS Vector Bucket 权限：

```json
{
  "Action": [
    "oss:PutObject",
    "oss:GetObject",
    "oss:DeleteObject",
    "oss:ListObjects"
  ],
  "Resource": "acs:oss:*:*:guanhetech-vec/*"
}
```

### Account UID 查询方式

```bash
# 通过 STS API 自动获取（无需登录控制台）
python3 -c "
import os, requests, hmac, hashlib, base64, uuid
from datetime import datetime, timezone
from backend.utils.env_loader import load_project_env
load_project_env()
ak = os.getenv('OSS_ACCESS_KEY_ID')
sk = os.getenv('OSS_ACCESS_KEY_SECRET')
# ... (见 deployment/.env.secrets 注释)
"
```

或在阿里云控制台：右上角头像 → **Account Management** → **Security Settings** → **Account ID**。

### 连通性验证

```python
from backend.utils.env_loader import load_project_env
load_project_env()
from backend.services.vector_bucket_client import get_default_client
import alibabacloud_oss_v2.vectors.models as m

c = get_default_client()
print(f"available: {c.available}")  # 应为 True

req = m.ListVectorIndexesRequest(bucket=c.bucket)
res = c._client.list_vector_indexes(req)
print(f"indexes: {res.indexes}")    # 应包含 contentchunks
```

---

## 13. 文件变更清单

### 新建文件

| 文件 | 说明 |
|------|------|
| `backend/database/migrations/add_vector_indexing.sql` | DB 迁移：`document_chunks` 表 + 6 个 vector 状态列 |
| `backend/services/vector_bucket_client.py` | OSS Vector Bucket SDK 封装 |
| `backend/services/chunking_service.py` | 文本分块服务 |
| `backend/services/document_vectorize_service.py` | 向量化编排服务 |
| `backend/tasks/vectorize_tasks.py` | Celery 向量化任务 |
| `backend/tasks/vectorize_triggers.py` | 自动触发判定逻辑 |
| `backend/tasks/ebook_tasks.py` | Ebook Map-Reduce 章节摘要 Celery 任务 |
| `backend/schemas/agent_search.py` | v2 API Pydantic Schema |
| `backend/services/vector_search_service.py` | 语义/混合/定位搜索服务 |
| `backend/api/agent_v2_search.py` | FastAPI v2 路由 |

### 修改文件

| 文件 | 变更内容 |
|------|----------|
| `backend/celery_app.py` | 注册 `vectorize_tasks` 模块 |
| `backend/celery_config.py` | 添加 task_routes 路由 + reconcile beat |
| `backend/tasks/file_tasks.py` | 接入 `_trigger_vectorize_files` + ebook 判定 + Map-Reduce 投递 |
| `backend/api/files.py` | PUT /content 和 POST /create-text-note 后触发 |
| `backend/api/tasks.py` | DELETE 时队列清理 Vector Bucket |
| `backend/main.py` | 注册 `agent_v2_search` 路由 |
| `tools/guanhe-mcp/guanhe_mcp/server.py` | 新增 3 个工具，版本升至 v0.3.0 |
| `deployment/config.defaults.env` | 新增 `AUTO_VECTORIZE_*`、`CHUNK_*`、`OSS_VECTOR_*` |
| `deployment/.env.secrets` | 填入 `OSS_VECTOR_BUCKET`、`OSS_VECTOR_ACCOUNT_ID` 等 |
| `.env`（项目根） | 追加 Vector Bucket 环境变量（运行时生效） |
| `requirements.txt` | 添加 `alibabacloud-oss-v2>=1.3.0` |
