# 新闻分析整体流程文档

## 概述

本文档详细梳理了新闻分析的整体流程，包括服务调用、Prompt模板使用、数据库表操作等各个环节。

## 流程图

```
┌─────────────────────────────────────────────────────────────────┐
│                    新闻分析流程总览                                │
└─────────────────────────────────────────────────────────────────┘

【入口层】
    │
    ├─ API接口触发
    │   ├─ POST /api/news-analysis/analyze (单条分析)
    │   └─ POST /api/news-analysis/analyze-batch (批量分析)
    │
    ├─ Celery任务触发
    │   ├─ analyze_news_by_ids_task (指定ID列表)
    │   └─ analyze_unanalyzed_news_task (未分析新闻)
    │
    └─ 调度器触发
        └─ scheduler_service._execute_news_analysis_task
    │
    ▼
┌─────────────────────────────────────────────────────────────────┐
│  NewsAnalysisService.analyze_news()                              │
│  核心分析服务                                                     │
└─────────────────────────────────────────────────────────────────┘
    │
    ├─ 步骤1: 获取新闻数据
    │   ├─ 查询表: news
    │   ├─ 查询字段: id, title, content, source
    │   └─ 获取相关股票代码
    │       ├─ 查询表: news_symbols, news_symbols_dict
    │       └─ JOIN获取: symbol_code
    │
    ├─ 步骤2: 更新分析状态
    │   └─ UPDATE news SET analysis_status = 'processing'
    │
    ├─ 步骤3: 准备Prompt变量
    │   └─ variables = {
    │         "title": news_data['title'],
    │         "content": news_data['content'],
    │         "symbols": symbols_str,
    │         "max_length": 100 (如果模板是news_summary_v1)
    │       }
    │
    ├─ 步骤4: 调用LLM分析
    │   │
    │   ├─ 服务: llm_service.chat_completion_with_template()
    │   │
    │   ├─ Prompt模板渲染
    │   │   ├─ 服务: PromptRenderer.render()
    │   │   ├─ 查询表: llm_prompt_templates
    │   │   ├─ 模板代码: template_code (默认: "news_summary_v1")
    │   │   ├─ 模板字段:
    │   │   │   ├─ system_prompt (系统提示词)
    │   │   │   ├─ user_prompt_template (用户提示词模板)
    │   │   │   ├─ variables (变量定义)
    │   │   │   ├─ recommended_provider (推荐提供商)
    │   │   │   ├─ recommended_model (推荐模型)
    │   │   │   ├─ temperature (温度参数)
    │   │   │   └─ max_tokens (最大token数)
    │   │   │
    │   │   └─ 变量替换: {{title}}, {{content}}, {{symbols}}
    │   │
    │   ├─ LLM调用
    │   │   ├─ 提供商: Qwen / Doubao / GLM
    │   │   ├─ 模型: 根据模板配置选择
    │   │   └─ 返回: LLMResponse
    │   │       ├─ content (分析结果文本)
    │   │       ├─ provider (提供商)
    │   │       ├─ model (模型)
    │   │       ├─ usage (token使用量)
    │   │       └─ response_time (响应时间)
    │   │
    │   └─ 重试机制
    │       ├─ 最大重试次数: 2次
    │       ├─ 内容审查失败检测
    │       └─ 智能模板切换
    │           └─ 失败时切换到: news_summary_safe_v1
    │
    ├─ 步骤5: 保存分析结果
    │   │
    │   ├─ 查询现有结果
    │   │   └─ SELECT analysis_result FROM news WHERE id = %s
    │   │
    │   ├─ 合并多模板结果 (支持多模板分析)
    │   │   └─ existing_result[template_code] = {
    │   │         "content": response.content,
    │   │         "template_code": template_code,
    │   │         "provider": response.provider,
    │   │         "model": response.model,
    │   │         "tokens": response.usage.total_tokens,
    │   │         "temperature": template_temperature,
    │   │         "retry_count": attempt,
    │   │         "analyzed_at": datetime.now().isoformat()
    │   │       }
    │   │
    │   └─ 更新数据库
    │       └─ UPDATE news SET
    │             analysis_result = %s::jsonb,
    │             analyzed_at = %s,
    │             analysis_template_code = %s,
    │             analysis_status = 'completed',
    │             analysis_error = NULL,
    │             is_analyzed = TRUE
    │           WHERE id = %s
    │
    ├─ 步骤6: 处理分类维度 (如果模板是news_classification)
    │   │
    │   ├─ 解析LLM返回的JSON
    │   │   └─ 提取维度:
    │   │       ├─ primary_theme (主要主题)
    │   │       ├─ event_type (事件类型)
    │   │       ├─ impact_scope (影响范围)
    │   │       ├─ urgency (紧急程度)
    │   │       ├─ related_sectors (相关行业)
    │   │       ├─ related_countries (相关国家)
    │   │       └─ rationale (理由)
    │   │
    │   └─ 保存到分类维度表
    │       ├─ 服务: ThemeMappingService.upsert_classification_dimensions()
    │       └─ 表: news_classification_dimensions
    │
    ├─ 步骤7: 投资主题匹配 (如果启用)
    │   │
    │   ├─ 服务: InvestmentThemeEmbeddingService.match_news_to_themes()
    │   │
    │   ├─ 生成新闻向量
    │   │   ├─ 文本: news_title + content_preview (前500字符)
    │   │   ├─ 模型: text-embedding-v3 (Qwen)
    │   │   ├─ 维度: 1024
    │   │   └─ 模式: "document" (与主题向量保持一致)
    │   │
    │   ├─ 向量相似度检索
    │   │   ├─ 查询表: investment_theme_embeddings
    │   │   ├─ 过滤条件: is_enabled_for_matching = TRUE
    │   │   │   └─ ⚠️ 重要: 只匹配Theme Settings中设置为"Enabled"的主题
    │   │   │       - 前端页面: InvestmentThemesPage 中的 "Enabled" 列
    │   │   │       - 数据库字段: investment_theme_embeddings.is_enabled_for_matching
    │   │   │       - 默认值: TRUE (新主题默认启用)
    │   │   │       - 管理员可通过UI切换启用/禁用状态
    │   │   ├─ 匹配对象: 主题向量 (基于 theme_name + description 生成)
    │   │   │   └─ ⚠️ 关键: 匹配的是整个主题的语义向量，不是单独的theme_name或category
    │   │   │       - 主题向量生成时使用: theme_name + description (增强文本)
    │   │   │       - 新闻向量: 标题 + 内容前500字符
    │   │   │       - 通过余弦相似度比较两个向量的语义相似性
    │   │   ├─ 相似度计算: 余弦相似度 (cosine similarity)
    │   │   ├─ Top-K: 2 (默认)
    │   │   ├─ 最小相似度阈值: 0.5
    │   │   └─ 排序: 按相似度降序
    │   │
    │   ├─ 保存匹配结果 (第一步)
    │   │   └─ INSERT INTO news_theme_matches
    │   │         (news_id, theme_id, theme_name, category,
    │   │          similarity_score, match_rank, match_method)
    │   │       VALUES (...)
    │   │   └─ ⚠️ 注意: 这是中间表，存储原始匹配结果
    │   │
    │   └─ 同步到统一表 (第二步，在步骤8中执行)
    │       └─ 通过 ThemeUnificationService.sync_vector_matches() 同步
    │           └─ 从 news_theme_matches 读取数据
    │           └─ 插入到 news_themes_unified
    │               └─ source_type = 'vector_retrieval'
    │               └─ confidence_score = similarity_score
    │               └─ theme_name 从 news_theme_matches.theme_name 读取
    │
    └─ 步骤8: 同步主题到统一表
        │
        ├─ 服务: ThemeUnificationService.sync_all_sources()
        │
        ├─ 数据源同步 (按顺序执行)
        │   ├─ eodhd_auto_tag (EODHD自动标签)
        │   │   └─ 从 news_auto_tags 表读取
        │   ├─ vector_retrieval (向量检索结果) ⚠️
        │   │   └─ 从 news_theme_matches 表读取
        │   │   └─ 方法: sync_vector_matches()
        │   │   └─ 字段映射:
        │   │       ├─ theme_name ← news_theme_matches.theme_name
        │   │       ├─ source_type = 'vector_retrieval'
        │   │       ├─ confidence_score ← similarity_score
        │   │       └─ rank_in_news ← match_rank
        │   ├─ llm_classification (LLM分类结果)
        │   │   └─ 从 news.analysis_result 字段读取
        │   └─ manual (手动添加)
        │
        └─ 保存到统一表
            └─ INSERT/UPDATE news_themes_unified
                  (news_id, theme_name, source_type, 
                   confidence_score, is_verified, is_primary, ...)
            └─ ⚠️ 注意: vector_retrieval 源的主题名称来自 news_theme_matches.theme_name
    │
    ▼
┌─────────────────────────────────────────────────────────────────┐
│                        完成                                       │
│  返回结果: {success: True, news_id, analysis, theme_matches}   │
└─────────────────────────────────────────────────────────────────┘
```

## 详细流程说明

### 1. 入口层

#### 1.1 API接口
- **文件**: `backend/api/news_analysis.py`
- **端点**:
  - `POST /api/news-analysis/analyze` - 分析单条新闻
  - `POST /api/news-analysis/analyze-batch` - 批量分析新闻
- **认证**: 需要JWT Token (`get_current_user`)

#### 1.2 Celery任务
- **文件**: `backend/tasks/news_tasks.py`
- **任务**:
  - `analyze_news_by_ids_task` - 分析指定ID的新闻列表
  - `analyze_unanalyzed_news_task` - 分析未分析的新闻
- **队列**: `ai_processing`
- **并发控制**: 支持 `max_concurrent` 参数

#### 1.3 调度器
- **文件**: `backend/services/scheduler_service.py`
- **方法**: `_execute_news_analysis_task()`
- **触发**: 定时任务或手动触发

### 2. 核心服务层

#### 2.1 NewsAnalysisService
- **文件**: `backend/services/news_analysis_service.py`
- **主要方法**:
  - `analyze_news()` - 单条新闻分析
  - `batch_analyze_news()` - 批量分析
  - `get_unanalyzed_news()` - 获取未分析新闻

### 3. Prompt模板系统

#### 3.1 模板表结构
- **表名**: `llm_prompt_templates`
- **关键字段**:
  - `template_code` - 模板代码 (如: "news_summary_v1")
  - `system_prompt` - 系统提示词
  - `user_prompt_template` - 用户提示词模板 (支持变量: `{{title}}`, `{{content}}`, `{{symbols}}`)
  - `variables` - 变量定义 (JSONB)
  - `recommended_provider` - 推荐提供商
  - `recommended_model` - 推荐模型
  - `temperature` - 温度参数
  - `max_tokens` - 最大token数

#### 3.2 常用模板
- **news_summary_v1**: 标准新闻分析模板
  - 输出: 核心要点、关键主体、重要数据、市场影响评估、一句话总结
- **news_summary_safe_v1**: 保守版模板 (用于内容审查失败时)
  - 特点: 更中性、客观的表述
- **news_classification**: 新闻分类模板
  - 输出: JSON格式的分类维度

#### 3.3 Prompt渲染
- **服务**: `PromptRenderer.render()`
- **文件**: `backend/core/prompt_renderer.py`
- **流程**:
  1. 从数据库加载模板
  2. 验证变量
  3. 合并默认值
  4. 替换模板变量 (`{{variable_name}}`)
  5. 返回渲染后的prompt

### 4. LLM调用

#### 4.1 LLM服务
- **文件**: `backend/core/llm_framework.py`
- **方法**: `llm_service.chat_completion_with_template()`
- **支持的提供商**:
  - Qwen (通义千问)
  - Doubao (豆包)
  - GLM (智谱AI)

#### 4.2 调用流程
1. 根据模板配置选择提供商和模型
2. 构建消息列表 (system + user prompt)
3. 发送HTTP请求到LLM API
4. 解析响应，返回 `LLMResponse` 对象

#### 4.3 错误处理
- **内容审查检测**: 检测是否为内容审查错误
- **重试机制**: 最多重试2次
- **模板切换**: 失败时切换到保守模板

### 5. 数据库表操作

#### 5.1 news表 (主表)
- **分析结果字段**:
  - `analysis_result` (JSONB) - 分析结果，支持多模板格式
  - `analysis_status` - 分析状态: pending/processing/completed/failed
  - `analysis_template_code` - 已使用的模板代码 (逗号分隔)
  - `analyzed_at` - 分析时间
  - `analysis_error` - 错误信息 (JSONB)
  - `is_analyzed` - 是否已分析 (布尔值)

#### 5.2 news_theme_matches表 (主题匹配结果)
- **字段**:
  - `news_id` - 新闻ID
  - `theme_id` - 主题ID
  - `theme_name` - 主题名称
  - `market` - 市场代码 (HK/US/CN)
  - `similarity_score` - 相似度评分 (0-1)
  - `match_rank` - 匹配排名 (1=最相关)
  - `match_method` - 匹配方法 (vector_search/llm_confirm)

#### 5.3 news_themes_unified表 (统一主题表)
- **字段**:
  - `news_id` - 新闻ID
  - `theme_name` - 主题名称
  - `canonical_theme_name` - 规范化主题名称
  - `source_type` - 来源类型: eodhd_auto_tag/vector_retrieval/llm_classification/manual
  - `confidence_score` - 置信度 (0-1)
  - `quality_score` - 质量评分
  - `is_verified` - 是否已验证
  - `is_primary` - 是否为主主题

#### 5.4 news_classification_dimensions表 (分类维度)
- **字段**:
  - `news_id` - 新闻ID
  - `dimension_title` - 维度名称 (primary_theme/event_type/impact_scope等)
  - `dimension_value` - 维度值
  - `confidence_score` - 置信度

#### 5.5 investment_theme_embeddings表 (主题向量)
- **字段**:
  - `theme_id` - 主题ID
  - `theme_name` - 主题名称
  - `embedding_vector` - 向量数据 (1024维，pgvector类型)
  - `embedding_data` - 向量数据 (JSONB格式，备用)
  - `embedding_model` - 向量模型 (text-embedding-v3)
  - `is_enabled_for_matching` - 是否启用匹配
    - ⚠️ **关键字段**: 控制主题是否参与新闻匹配
    - `TRUE`: 主题参与匹配 (前端显示为"Enabled")
    - `FALSE`: 主题不参与匹配 (前端显示为"Disabled")
    - 默认值: `TRUE` (新主题默认启用)
    - 管理员可在前端页面 (InvestmentThemesPage) 切换此状态

### 6. 主题匹配流程

#### 6.0 主题匹配机制说明 ⚠️

**Q: 匹配时是匹配theme name还是category？**

**A: 匹配的是整个主题的语义向量，不是单独的theme_name或category。**

- **主题向量生成**: 使用 `theme_name + description` (增强文本) 生成向量
- **新闻向量生成**: 使用 `标题 + 内容前500字符` 生成向量
- **匹配方式**: 通过余弦相似度比较两个向量的语义相似性
- **匹配结果**: 返回匹配的主题（包含theme_id, theme_name, category等信息）

**Q: 匹配结果保存到哪个表？**

**A: 分两步保存：**

1. **第一步**: 保存到 `news_theme_matches` 表（中间表）
   - 存储原始匹配结果
   - 字段: news_id, theme_id, theme_name, category, similarity_score, match_rank

2. **第二步**: 同步到 `news_themes_unified` 表（统一表）
   - 通过 `ThemeUnificationService.sync_vector_matches()` 同步
   - 从 `news_theme_matches` 读取数据
   - 字段映射:
     - `theme_name` ← `news_theme_matches.theme_name`
     - `source_type` = 'vector_retrieval'
     - `confidence_score` ← `similarity_score`
     - `rank_in_news` ← `match_rank`

#### 6.0.1 主题启用状态控制 ⚠️
- **关键机制**: 只有 `is_enabled_for_matching = TRUE` 的主题才会参与新闻匹配
- **前端管理**: 
  - 页面: `InvestmentThemesPage` (投资主题管理页面)
  - 列名: "Enabled" 列
  - 操作: 管理员可通过 Switch 开关切换主题的启用/禁用状态
- **数据库字段**: `investment_theme_embeddings.is_enabled_for_matching`
- **默认值**: `TRUE` (新主题默认启用)
- **匹配过滤**: 在向量检索时，SQL查询会添加 `WHERE is_enabled_for_matching = TRUE` 条件
- **影响**: 
  - 禁用的主题不会出现在匹配结果中
  - 已禁用的主题，即使相似度很高也不会被匹配
  - 管理员可以通过禁用主题来排除某些主题参与匹配

#### 6.1 向量生成
- **服务**: `InvestmentThemeEmbeddingService`
- **文件**: `backend/services/investment_theme_embedding_service.py`
- **流程**:
  1. **新闻向量生成**:
     - 构建新闻文本: 标题 + 内容前500字符
     - 调用Qwen embedding API生成向量
     - 使用 "document" 模式确保与主题向量在同一向量空间
  2. **主题向量生成** (在初始化时):
     - 构建主题文本: `theme_name + description` (增强文本)
     - 如果只有theme_name没有description，则仅使用theme_name
     - 同样使用 "document" 模式生成向量
     - ⚠️ **关键**: 匹配的是整个主题的语义向量，不是单独的theme_name或category

#### 6.2 向量检索
- **方法**: `match_news_to_themes()`
- **流程**:
  1. 检测数据库模式 (pgvector 或 JSONB)
  2. 如果是pgvector: 使用向量操作符 `<=>` 计算余弦距离
  3. 如果是JSONB: 在Python中计算余弦相似度
  4. **过滤启用状态**: `is_enabled_for_matching = TRUE`
     - ⚠️ **重要**: 只匹配Theme Settings中设置为"Enabled"的主题
     - 禁用状态的主题不会参与匹配
     - 管理员可在前端页面切换主题的启用/禁用状态
  5. 排序: 按相似度降序
  6. 取Top-K结果 (默认K=2)
  7. 过滤: 相似度 >= 最小阈值 (默认0.5)

#### 6.3 结果保存
- **第一步: 保存到中间表**
  - 表: `news_theme_matches`
  - 字段:
    - `news_id` - 新闻ID
    - `theme_id` - 主题ID
    - `theme_name` - 主题名称 (从 investment_theme_embeddings 读取)
    - `category` - 主题分类 (从 investment_theme_embeddings 读取)
    - `similarity_score` - 相似度评分 (0-1)
    - `match_rank` - 匹配排名 (1=最相关)
    - `match_method` - 匹配方法 ('vector_search')
- **第二步: 同步到统一表** (在步骤8中执行)
  - 通过 `ThemeUnificationService.sync_vector_matches()` 同步
  - 从 `news_theme_matches` 读取数据
  - 插入到 `news_themes_unified` 表
  - 字段映射:
    - `theme_name` ← `news_theme_matches.theme_name`
    - `source_type` = 'vector_retrieval'
    - `confidence_score` ← `news_theme_matches.similarity_score`
    - `rank_in_news` ← `news_theme_matches.match_rank`
    - `metadata` 包含 category 等信息

### 7. 主题统一服务

#### 7.1 服务
- **服务**: `ThemeUnificationService`
- **文件**: `backend/services/theme_unification_service.py`
- **方法**: `sync_all_sources()`

#### 7.2 同步流程
1. 从多个数据源收集主题:
   - `eodhd_auto_tag` - EODHD自动标签
   - `vector_retrieval` - 向量检索结果 (从 `news_theme_matches`)
   - `llm_classification` - LLM分类结果 (从 `analysis_result`)
   - `manual` - 手动添加
2. 去重和合并
3. 保存到 `news_themes_unified` 表

## 关键配置

### 模板配置示例
```sql
-- news_summary_v1 模板
SELECT template_code, system_prompt, user_prompt_template, variables
FROM llm_prompt_templates
WHERE template_code = 'news_summary_v1';
```

### 分析参数
- **默认模板**: `news_summary_v1`
- **最大重试次数**: 2次
- **主题匹配Top-K**: 2
- **最小相似度阈值**: 0.5
- **向量维度**: 1024
- **向量模型**: text-embedding-v3

## 错误处理

### 内容审查失败
- **检测**: `_is_content_filter_error()` 方法
- **处理**: 
  - 输入内容问题: 不重试
  - 输出内容问题: 重试 + 切换模板
  - 所有提供商失败: 标记为失败

### 分析失败
- **状态更新**: `analysis_status = 'failed'`
- **错误保存**: `analysis_error` 字段 (JSONB格式)
- **可重试**: 根据错误类型判断

## 性能优化

### 批量分析
- **并发控制**: 使用 `asyncio.Semaphore` 限制并发数
- **默认并发数**: 5-8 (可配置)
- **延迟**: 每次分析后延迟0.5秒，避免API限流

### 向量检索
- **pgvector**: 使用PostgreSQL扩展，高性能向量搜索
- **JSONB备用**: 如果pgvector不可用，使用JSONB存储 + Python计算

## 相关文件

### 核心服务
- `backend/services/news_analysis_service.py` - 新闻分析服务
- `backend/services/investment_theme_embedding_service.py` - 主题向量服务
- `backend/services/theme_unification_service.py` - 主题统一服务
- `backend/core/prompt_renderer.py` - Prompt渲染器
- `backend/core/llm_framework.py` - LLM框架

### 任务
- `backend/tasks/news_tasks.py` - Celery任务

### API
- `backend/api/news_analysis.py` - 新闻分析API

### 数据库迁移
- `backend/database/migrations/add_investment_theme_embeddings.sql` - 主题向量表
- `backend/database/migrations/create_unified_theme_system.sql` - 统一主题表
- `backend/database/migrations/optimize_news_summary_template.sql` - 模板优化

## 总结

新闻分析流程是一个多步骤的异步处理流程，涉及：
1. **数据获取**: 从数据库获取新闻和股票信息
2. **Prompt渲染**: 使用模板和变量生成提示词
3. **LLM调用**: 调用大语言模型进行分析
4. **结果保存**: 保存分析结果到数据库
5. **主题匹配**: 基于向量检索匹配投资主题
6. **主题统一**: 同步多个来源的主题到统一表

整个流程支持重试、错误处理、并发控制等机制，确保分析的可靠性和性能。

