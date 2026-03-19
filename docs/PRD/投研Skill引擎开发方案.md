# 投研 Skill 引擎 + 智能对话 + 定制化报告 — 整体开发方案

> 版本：v1.1  
> 日期：2026-03-18  
> 状态：方案讨论阶段  
> 关联文档：`需求文档.md`、`新闻质量过滤方案.md`

---

## 一、背景与目标

### 1.1 业务背景

公司投资部和交易部提出了一系列投研系统需求（详见 `需求文档.md`），核心诉求可归纳为：

- **Jikun 需求**：Twitter 摘要、Top Stories、重大事件分析、主题追踪、市场总览
- **李晔需求**：三级信息管理（未读/已读/想法）、标签体系、标的自动识别、主题投资组合、前瞻事件日历

### 1.2 系统使用场景

投资人员和交易人员主要通过两种方式使用系统：

1. **对话交互**：通过 Chat 界面提问，勾选数据源，AI 从内部数据库或外部 API/MCP 获取数据后回答
2. **定时报告**：根据用户的 preference、关注的 tag/主题/market data，定期自动生成个性化报告

### 1.3 设计目标

在现有架构上**渐进式扩展**，不重写已有模块

- 引入 **Skill 引擎**作为"数据源"与"AI 对话"之间的编排层
- 每个分析能力封装为可组合、可复用、可定时触发的 Skill
- 支持多数据源路由：内部数据库 + 外部 API + MCP（未来）

---

## 二、当前系统能力与缺口分析

### 2.1 已有能力

| 维度 | 已有能力 | 关键模块 |
|------|---------|---------|
| **数据采集** | 18 种 Scheduler 任务（EODHD/Twitter/YouTube/RSS/CCTV/宏观等）、文件上传解析 | `scheduler_service.py`、`subscription_*`、`eodhd_*` |
| **AI 分析** | LLM Framework（Qwen/Doubao/GLM）、Prompt Template 体系、新闻分析（分类+向量匹配）、RC Agent（Dify） | `llm_framework.py`、`rc_agent_service.py`、`news_analysis_service.py` |
| **主题/标签** | `theme_master_data` + 四源融合、file_tags 体系、向量 embedding 匹配 | `theme_fusion_service.py`、`investment_theme_embedding_service.py` |
| **报告** | Market Weekly（按需生成、用户级缓存） | `market_weekly_service.py` |
| **任务调度** | APScheduler（18 种任务类型、cron/interval）+ Celery（20+ 异步任务） | `scheduler_service.py`、`celery_app.py` |
| **用户系统** | 认证、LLM 模板偏好、新闻 preset、Dashboard 设置 | `authStore`、`llm_prompt_template_service.py` |
| **前端** | 15+ 页面（Files/News/Markets/Themes/Dashboard/Scheduler 等） | React + Ant Design + Zustand |

### 2.2 缺口

| 缺口 | 对应需求 | 优先级 |
|------|---------|-------|
| **无通用对话界面** — RC Agent 仅用于文件处理，用户没有"提问"入口 | 所有需求的交互入口 | P0 |
| **无 Skill/Plugin 编排系统** — 无法将多步骤分析流程封装为可复用单元 | 所有需求的编排层 | P0 |
| **无 Daily/Weekly Summary** — 仅有按需 Market Weekly | Jikun #1 | P1 |
| **无信息阅读状态管理** — 无未读/已读/想法的概念 | 李晔 #1-#3 | P1 |
| **无分享机制** — 无法与同事共享单条信息 | 李晔 #4 | P1 |
| **无 Tag→Portfolio 能力** — 标签未与投资标的组合关联 | 李晔 #6、Jikun #4 | P2 |
| **无前瞻事件日历** — 无法记录"未来可能发生什么" | 李晔 #19-21 行 | P2 |
| **无文章级新闻质量过滤** — 仅有主题映射质量分，缺少入库端预筛与 LLM 质量评分 | Jikun #2 Top Stories、所有报告类 Skill 的新闻筛选 | P1 |
| **无用户行为反馈闭环** — 无法基于用户阅读/收录/点赞等行为优化推荐 | Layer 3 个性化 + 来源信誉校准 | P2 |
| **无 MCP 集成** | 未来扩展 | P3 |

---

## 三、架构总览

### 3.1 整体架构图

```
┌─────────────────────────────────────────────────────────────┐
│                        前端 (React)                          │
│  ┌──────────┐  ┌──────────────┐  ┌────────────────────────┐ │
│  │ Chat UI  │  │ Report Center│  │ 现有页面(News/Markets/…)│ │
│  └────┬─────┘  └──────┬───────┘  └────────────────────────┘ │
│       │               │                                      │
│  数据源选择面板   报告偏好设置                                 │
└───────┼───────────────┼──────────────────────────────────────┘
        │               │
        ▼               ▼
┌─────────────────────────────────────────────────────────────┐
│                    Skill 编排层 (新增)                        │
│                                                             │
│  ┌─────────────┐  ┌──────────────┐  ┌───────────────────┐  │
│  │ Skill Router │  │ Skill Engine │  │ Skill Registry    │  │
│  │ (意图识别+   │  │ (执行引擎)   │  │ (Skill 注册/发现) │  │
│  │  数据源路由)  │  │              │  │                   │  │
│  └──────┬──────┘  └──────┬───────┘  └───────────────────┘  │
│         │                │                                   │
│  ┌──────┴────────────────┴──────────────────────────────┐   │
│  │              Skill Instances (可插拔)                  │   │
│  │  ┌────────────┐ ┌────────────┐ ┌──────────────────┐  │   │
│  │  │ 市场简报    │ │ 事件分析    │ │ 主题追踪          │  │   │
│  │  │ Skill      │ │ Skill      │ │ Skill            │  │   │
│  │  └────────────┘ └────────────┘ └──────────────────┘  │   │
│  │  ┌────────────┐ ┌────────────┐ ┌──────────────────┐  │   │
│  │  │ 信息管理    │ │ 前瞻事件    │ │ 标的识别 (基础)   │  │   │
│  │  │ Skill      │ │ Skill      │ │ Skill            │  │   │
│  │  └────────────┘ └────────────┘ └──────────────────┘  │   │
│  └──────────────────────────────────────────────────────┘   │
└─────────────────────────┬───────────────────────────────────┘
                          │
        ┌─────────────────┼─────────────────┐
        ▼                 ▼                 ▼
┌──────────────┐ ┌──────────────┐ ┌──────────────────┐
│  现有 Services │ │  现有数据库    │ │  外部 API / MCP  │
│  (36+ 个)     │ │  (PostgreSQL) │ │  (未来扩展)      │
└──────────────┘ └──────────────┘ └──────────────────┘
```

### 3.2 Skill 引擎在系统中的定位

Skill 引擎是**编排层**，不是替代层。它的职责是：

1. **意图识别**：用户的自然语言问题 → 匹配最合适的 Skill
2. **数据源路由**：根据 Skill 声明的 `required_data_sources` + 用户勾选的数据源，调用对应 Service 获取数据
3. **执行编排**：按 Skill 定义的步骤顺序执行（数据获取 → LLM 分析 → 结果组装）
4. **双模式触发**：同一 Skill 既支持对话触发，也支持 Scheduler 定时触发

---

## 四、Skill 引擎内核设计

### 4.1 后端模块结构

```
backend/
├── skills/
│   ├── __init__.py
│   ├── engine.py              # Skill 执行引擎
│   ├── registry.py            # Skill 注册表（内存 + DB）
│   ├── router.py              # 意图识别 + Skill 路由
│   ├── base_skill.py          # Skill 基类定义
│   ├── models.py              # Skill 相关 Pydantic 模型
│   ├── data_source_resolver.py # 数据源解析器
│   └── builtin/               # 内置 Skills
│       ├── __init__.py
│       ├── general_qa.py              # 通用问答
│       ├── market_briefing.py         # 市场简报
│       ├── event_analysis.py          # 事件深度分析
│       ├── theme_tracker.py           # 主题追踪
│       ├── info_management.py         # 信息管理
│       ├── forward_events.py          # 前瞻事件
│       ├── ticker_recognition.py      # 标的识别（基础 Skill）
│       ├── news_quality_scorer.py     # 新闻质量评分（基础 Skill，详见新闻质量过滤方案）
│       └── personalized_news_feed.py  # 个性化新闻推荐
```

### 4.2 Skill 基类

```python
from abc import ABC, abstractmethod
from typing import Optional
from pydantic import BaseModel


class SkillContext(BaseModel):
    user_id: str
    query: str                                    # 用户问题（对话模式下）
    selected_data_sources: list[str]              # 用户勾选的数据源
    parameters: dict = {}                         # 额外参数
    conversation_history: list[dict] = []         # 对话历史
    user_preferences: dict = {}                   # 用户偏好（关注的 tags 等）
    schedule_config: Optional[dict] = None        # 定时触发配置


class SkillResult(BaseModel):
    content: str                                  # 主要输出内容
    structured_data: Optional[dict] = None        # 结构化数据（图表、表格等）
    data_sources_used: list[str] = []             # 实际使用的数据源
    referenced_tickers: list[str] = []            # 引用的标的
    referenced_tags: list[str] = []               # 引用的主题标签
    tokens_used: int = 0
    execution_time_ms: int = 0


class BaseSkill(ABC):
    skill_id: str
    name: str
    description: str

    # Meta Data — 声明式数据源需求
    required_data_sources: list[str]              # ["news", "stock_prices", "themes"]
    optional_data_sources: list[str]              # ["twitter", "macro_indicators"]

    # 触发方式
    trigger_modes: list[str]                      # ["chat", "schedule", "api"]

    # 复用现有 llm_prompt_templates 中的模板
    prompt_template_codes: list[str]

    @abstractmethod
    async def execute(self, context: SkillContext) -> SkillResult:
        """核心执行逻辑"""
        raise NotImplementedError

    async def validate_inputs(self, context: SkillContext) -> bool:
        """输入校验，子类可覆写"""
        return True

    def get_required_data_sources(self) -> list[str]:
        """返回 Skill 需要的数据源（用于前端显示和路由校验）"""
        return self.required_data_sources
```

### 4.3 Skill Router（意图路由）

```python
class SkillRouter:
    """
    将用户的自然语言输入路由到最合适的 Skill。
    策略：
      1. 关键词匹配（快速路径）
      2. LLM 意图分类（兜底路径）
      3. 用户显式选择（前端可指定 skill_id）
    """

    async def route(self, query: str, selected_data_sources: list[str],
                    explicit_skill_id: str = None) -> str:
        # 1. 用户显式指定
        if explicit_skill_id:
            return explicit_skill_id

        # 2. 关键词快速匹配
        skill_id = self._keyword_match(query)
        if skill_id:
            return skill_id

        # 3. LLM 意图分类
        skill_id = await self._llm_classify(query, selected_data_sources)
        return skill_id or "general_qa"
```

### 4.4 数据源解析器

```python
class DataSourceResolver:
    """
    将 Skill 声明的数据源名称解析为实际的 Service 调用。
    统一管理内部数据库、外部 API、MCP 数据源。
    """

    # 数据源名称 → Service 方法的映射
    SOURCE_MAP = {
        "news":                ("EODHDNewsService",        "search_news"),
        "twitter":             ("SubscriptionContentService", "get_twitter_content"),
        "youtube":             ("SubscriptionContentService", "get_youtube_content"),
        "stock_prices":        ("EODHDStockPricesService", "get_prices"),
        "eod_prices":          ("EODHDEODStockPricesService", "get_eod_prices"),
        "economic_events":     ("EconomicEventsService",   "get_events"),
        "macro_indicators":    ("MacroIndicatorsService",  "get_indicators"),
        "fundamentals":        ("EODHDFundamentalsService","get_fundamentals"),
        "earnings_calendar":   ("EarningsCalendarService", "get_calendar"),
        "ipo_calendar":        ("IPOCalendarService",      "get_calendar"),
        "themes":              ("ThemeFusionService",      "get_fused_themes"),
        "investment_themes":   ("InvestmentThemeEmbeddingService", "match_news_to_themes"),
        "file_content":        ("FileProcessingService",   "search_files"),
        "market_data":         ("MarketDataService",       "get_market_data"),
        "research_items":      ("ResearchItemService",     "search_items"),
        "forward_events":      ("ForwardEventService",     "get_events"),
    }

    # 数据源级别的默认质量过滤参数（Skill 或用户可覆盖）
    SOURCE_DEFAULT_FILTERS = {
        "news": {
            "min_quality_score": 3.0,      # 默认最低质量分（详见新闻质量过滤方案 Layer 2）
            "source_tiers": None,          # None = 不限；["top","high"] = 仅优质来源
            "is_actionable": None,         # None = 不限；True = 仅可执行新闻
        },
    }

    async def resolve(self, source_name: str, params: dict) -> dict:
        """根据数据源名称调用对应 Service 获取数据。
        params 中可传入质量过滤参数，未传则使用 SOURCE_DEFAULT_FILTERS。
        """
        service_class, method_name = self.SOURCE_MAP[source_name]
        service = get_service(service_class)
        method = getattr(service, method_name)
        # 合并默认质量过滤（news 数据源自动注入 quality 参数）
        if source_name in self.SOURCE_DEFAULT_FILTERS:
            defaults = self.SOURCE_DEFAULT_FILTERS[source_name]
            for k, v in defaults.items():
                if k not in params and v is not None:
                    params[k] = v
        return await method(**params)
```

### 4.5 数据库新增表

```sql
-- ================================================================
-- Skill 引擎相关表
-- ================================================================

-- Skill 注册表
CREATE TABLE skill_registry (
    id SERIAL PRIMARY KEY,
    skill_id VARCHAR(100) UNIQUE NOT NULL,
    name VARCHAR(200) NOT NULL,
    description TEXT,
    category VARCHAR(50),                          -- 'analysis', 'report', 'tracking', 'management'
    required_data_sources JSONB DEFAULT '[]',
    optional_data_sources JSONB DEFAULT '[]',
    trigger_modes JSONB DEFAULT '["chat"]',        -- ["chat", "schedule", "api"]
    default_parameters JSONB DEFAULT '{}',
    prompt_template_codes JSONB DEFAULT '[]',
    is_active BOOLEAN DEFAULT TRUE,
    is_system BOOLEAN DEFAULT TRUE,                -- 系统内置 vs 用户自定义
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Skill 执行日志
CREATE TABLE skill_execution_logs (
    id SERIAL PRIMARY KEY,
    skill_id VARCHAR(100) REFERENCES skill_registry(skill_id),
    user_id VARCHAR(100),
    trigger_mode VARCHAR(20),                      -- 'chat', 'schedule', 'api'
    input_context JSONB,
    output_result JSONB,
    data_sources_used JSONB,
    tokens_used INTEGER DEFAULT 0,
    execution_time_ms INTEGER DEFAULT 0,
    status VARCHAR(20) DEFAULT 'pending',          -- 'pending', 'running', 'success', 'failed'
    error_message TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_skill_exec_user ON skill_execution_logs(user_id, created_at DESC);
CREATE INDEX idx_skill_exec_skill ON skill_execution_logs(skill_id, status);

-- ================================================================
-- 新闻质量过滤相关表（详见 新闻质量过滤方案.md）
-- ================================================================

-- 来源信誉评分表（Layer 1 规则过滤的核心）
CREATE TABLE news_source_scores (
    id SERIAL PRIMARY KEY,
    source_name VARCHAR(200) UNIQUE NOT NULL,
    reputation_score FLOAT DEFAULT 0.5,        -- 0-1
    tier VARCHAR(10) DEFAULT 'medium',         -- 'top','high','medium','low','blacklist'
    total_articles INTEGER DEFAULT 0,
    avg_content_length FLOAT DEFAULT 0,
    avg_quality_score FLOAT,                   -- 由反馈闭环定期回写
    user_save_rate FLOAT,                      -- 被用户收录比例
    is_active BOOLEAN DEFAULT TRUE,
    notes TEXT,
    updated_at TIMESTAMP DEFAULT NOW()
);

-- news 表扩展字段
ALTER TABLE news ADD COLUMN IF NOT EXISTS pre_filter_score FLOAT;
ALTER TABLE news ADD COLUMN IF NOT EXISTS source_tier VARCHAR(10);
ALTER TABLE news ADD COLUMN IF NOT EXISTS pre_filter_meta JSONB;
ALTER TABLE news ADD COLUMN IF NOT EXISTS quality_score FLOAT;

CREATE INDEX idx_news_pre_filter_score ON news(pre_filter_score DESC);
CREATE INDEX idx_news_source_tier ON news(source_tier);
CREATE INDEX idx_news_quality_score ON news(quality_score DESC);

-- 用户行为反馈表（反馈闭环）
CREATE TABLE news_user_interactions (
    id SERIAL PRIMARY KEY,
    news_id INTEGER REFERENCES news(id),
    user_id VARCHAR(100) NOT NULL,
    interaction_type VARCHAR(30) NOT NULL,      -- 'view','read_full','save','share','dismiss','thumbs_up','thumbs_down'
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(news_id, user_id, interaction_type)
);

CREATE INDEX idx_news_interactions_user ON news_user_interactions(user_id, created_at DESC);
CREATE INDEX idx_news_interactions_news ON news_user_interactions(news_id);

-- 用户 Skill 偏好（定时报告配置）
CREATE TABLE user_skill_preferences (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR(100) NOT NULL,
    skill_id VARCHAR(100) REFERENCES skill_registry(skill_id),
    custom_parameters JSONB DEFAULT '{}',          -- 用户自定义默认参数
    schedule_config JSONB,                         -- {"cron": "0 7 * * 1-5", "timezone": "America/New_York"}
    subscribed_tags JSONB DEFAULT '[]',            -- 关注的 tag 列表
    is_enabled BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(user_id, skill_id)
);
```

---

## 五、对话系统设计

### 5.1 对话数据模型

```sql
-- 对话表
CREATE TABLE conversations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id VARCHAR(100) NOT NULL,
    title VARCHAR(500),
    skill_id VARCHAR(100),                         -- 关联的 Skill（自动路由时填充）
    selected_data_sources JSONB DEFAULT '[]',
    is_archived BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_conversations_user ON conversations(user_id, updated_at DESC);

-- 对话消息表
CREATE TABLE conversation_messages (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    conversation_id UUID REFERENCES conversations(id) ON DELETE CASCADE,
    role VARCHAR(20) NOT NULL,                     -- 'user', 'assistant', 'system'
    content TEXT NOT NULL,
    skill_execution_id INTEGER,                    -- 关联 skill_execution_logs
    metadata JSONB DEFAULT '{}',                   -- 引用的数据源、标的等
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_messages_conversation ON conversation_messages(conversation_id, created_at);
```

### 5.2 对话 API

```
POST   /api/chat/conversations                    # 创建新对话
GET    /api/chat/conversations                    # 获取对话列表
GET    /api/chat/conversations/{id}               # 获取对话详情（含消息）
DELETE /api/chat/conversations/{id}               # 删除对话
POST   /api/chat/conversations/{id}/send          # 发送消息（SSE 流式返回）
PUT    /api/chat/conversations/{id}/data-sources   # 更新对话的数据源选择
```

### 5.3 对话执行流程

```
用户输入问题 + 勾选数据源
       │
       ▼
POST /api/chat/conversations/{id}/send  (SSE streaming)
       │
       ▼
┌─ Skill Router ─────────────────────────────────┐
│  1. 检查用户是否显式指定了 skill_id              │
│  2. 关键词快速匹配（"市场简报"→market_briefing）  │
│  3. LLM 意图分类（兜底）                         │
│  4. 返回 matched skill_id                       │
└─────────────────────────┬───────────────────────┘
                          │
                          ▼
┌─ Skill Engine ──────────────────────────────────┐
│  1. skill.validate_inputs(context)              │
│  2. DataSourceResolver 根据                      │
│     selected_data_sources ∩ skill.required_data_sources │
│     从对应 Service 获取数据                       │
│  3. 组装 prompt（引用 llm_prompt_templates）      │
│  4. 调用 LLM Framework 生成回答（流式）           │
│  5. 记录 skill_execution_logs                    │
└─────────────────────────┬───────────────────────┘
                          │
                          ▼
保存 conversation_messages → SSE 流式推送给前端
```

### 5.4 前端 Chat UI

**新增页面**：`ChatPage` (`/chat`) — 一级导航

| 区域 | 功能 |
|------|------|
| **左侧栏** | 对话列表（历史 conversations），新建对话按钮 |
| **中间主区域** | 消息流（支持 Markdown 渲染、代码块、表格），流式输出动画，输入框 |
| **右侧面板**（可折叠） | 数据源选择（checkbox 分组），Skill 手动选择（可选），当前对话参数设置 |

**数据源选择面板分组**：

```
📰 新闻类
  ☑ EODHD News
  ☑ CCTV News
  ☐ Watchlist News

📱 社交媒体
  ☑ Twitter 订阅
  ☐ YouTube 订阅

📈 行情数据
  ☑ Stock Prices
  ☐ Economic Events
  ☐ Macro Indicators
  ☐ Market Data

📊 基本面
  ☐ Fundamentals
  ☐ Earnings Calendar
  ☐ IPO Calendar

📁 内部资料
  ☐ 已上传文件
  ☐ 研究笔记/想法

🏷️ 主题
  ☑ Investment Themes
```

---

## 六、分阶段实施计划

---

### Phase 0：基础设施层（3-4 周）

> **目标**：建立 Skill 引擎内核 + 通用对话界面，为后续所有功能提供基础。

#### 0.1 后端任务

| 任务 | 工作量 | 说明 |
|------|-------|------|
| `BaseSkill` + `SkillContext` + `SkillResult` 模型定义 | 2d | Skill 基类和数据模型 |
| `SkillRegistry` 实现（内存注册 + DB 持久化） | 2d | Skill 的注册、发现、管理 |
| `SkillEngine` 执行引擎 | 3d | 编排执行流程、错误处理、日志记录 |
| `SkillRouter` 意图路由 | 3d | 关键词匹配 + LLM 分类 |
| `DataSourceResolver` 数据源解析器 | 2d | 将数据源名称映射到现有 Service |
| `GeneralQASkill` 通用问答 Skill | 2d | 兜底 Skill，基于选中数据源回答问题 |
| Chat API（conversations CRUD + send） | 3d | 对话管理 + SSE 流式接口 |
| 数据库迁移脚本 | 1d | 新增 Skill 相关表 + 对话相关表 |

#### 0.2 新闻质量过滤 Layer 1（可与 0.1 并行）

> 详见 `新闻质量过滤方案.md`，Layer 1 零 LLM 成本、实施快、效果立竿见影。

| 任务 | 工作量 | 说明 |
|------|-------|------|
| `news_source_scores` 表 + 预置来源评分数据 | 1d | 建表 + 初始化 top/high/medium/low 来源列表 |
| `eodhd_news_service` 增加 `_calculate_pre_filter_score()` | 2d | 在 `_process_article_for_db()` 中计算规则评分 |
| `news` 表新增 `pre_filter_score` / `source_tier` / `quality_score` 字段 | 0.5d | DB 迁移脚本 |
| Scheduler 任务参数支持 `pre_filter_threshold` / `pre_filter_action` | 1d | 可配置 mark/skip/separate 策略 |
| 存量新闻回填 `pre_filter_score` 的一次性脚本 | 0.5d | 对已有新闻补算分数 |

#### 0.3 前端任务

| 任务 | 工作量 | 说明 |
|------|-------|------|
| ChatPage 页面框架 | 3d | 三栏布局、路由、导航集成 |
| 对话消息组件 | 2d | 消息流渲染（Markdown）、流式输出 |
| 数据源选择面板 | 2d | Checkbox 分组、状态持久化 |
| 对话列表 & 管理 | 1d | 历史对话、新建、删除 |
| API Service 层 | 1d | chat API 对接 |
| News 页面增加 source_tier 筛选 + quality_score 滑块 | 1d | 复用 ThemeFilterPanel 扩展，支持 `min_quality_score`、`source_tiers`、`is_actionable` |

#### 0.4 集成与测试

- Skill Engine 与现有 `llm_framework.py` 的集成测试
- Chat SSE 流式输出端到端测试
- 数据源路由 → 现有 Service 的联调
- Layer 1 新闻规则过滤验证：对比过滤前后的新闻噪音比
- `news_classification` 模板扩展（增加 `quality_scores` 输出字段）联调 — 对应新闻质量过滤方案 Layer 2

---

### Phase 1：信息管理体系（4-5 周）

> **对应需求**：李晔 #1-#5（三个数据库 + 标签 + 重要性 + 分享 + 自动识别标的）

#### 1.1 数据模型

```sql
-- 投研信息统一表（"三个数据库"的统一实现）
CREATE TABLE research_items (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id VARCHAR(100) NOT NULL,

    -- 内容
    title VARCHAR(500),
    content TEXT,
    source_type VARCHAR(50),           -- 'news', 'twitter', 'youtube', 'file', 'manual', 'idea'
    source_id VARCHAR(200),            -- 原始数据 ID（news.id, subscription_content.id 等）
    source_url TEXT,

    -- 状态（对应三个"数据库"）
    status VARCHAR(20) NOT NULL,       -- 'unread', 'read', 'idea'
    importance VARCHAR(10),            -- 'high', 'medium', 'low'

    -- 标签（复用 theme_master_data 体系）
    tags JSONB DEFAULT '[]',
    auto_tags JSONB DEFAULT '[]',

    -- 投资标的关联
    tickers JSONB DEFAULT '[]',        -- [{"symbol": "AAPL.US", "confidence": 0.9}]

    -- 分享
    is_shared BOOLEAN DEFAULT FALSE,
    shared_at TIMESTAMP,

    -- 元数据
    notes TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    read_at TIMESTAMP,

    -- 语义搜索
    embedding_vector VECTOR(1024)
);

CREATE INDEX idx_research_items_user_status ON research_items(user_id, status);
CREATE INDEX idx_research_items_tags ON research_items USING GIN(tags);
CREATE INDEX idx_research_items_tickers ON research_items USING GIN(tickers);
CREATE INDEX idx_research_items_importance ON research_items(user_id, importance);
CREATE INDEX idx_research_items_shared ON research_items(is_shared) WHERE is_shared = TRUE;

-- 分享可见性
CREATE TABLE research_item_shares (
    id SERIAL PRIMARY KEY,
    item_id UUID REFERENCES research_items(id) ON DELETE CASCADE,
    shared_by VARCHAR(100) NOT NULL,
    shared_to VARCHAR(100),            -- NULL = 全公司可见
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(item_id, shared_by, shared_to)
);

-- 标的-标签关联表（需求 #5 的核心 — 自动将 tag 贴到投资标的上）
CREATE TABLE ticker_tag_associations (
    id SERIAL PRIMARY KEY,
    ticker_symbol VARCHAR(50) NOT NULL,
    tag_name VARCHAR(200) NOT NULL,    -- 对应 theme_master_data.canonical_name
    source_item_id UUID,               -- 来源的 research_item
    confidence FLOAT DEFAULT 1.0,
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(ticker_symbol, tag_name, source_item_id)
);

CREATE INDEX idx_ticker_tag_symbol ON ticker_tag_associations(ticker_symbol);
CREATE INDEX idx_ticker_tag_name ON ticker_tag_associations(tag_name);
```

#### 1.2 新增 Skills

| Skill ID | 名称 | 功能 |
|----------|------|------|
| `info_management` | 信息管理 Skill | 收录信息、状态流转、标签管理、搜索查询 |
| `ticker_recognition` | 标的识别 Skill（基础） | NER 提取标的 → 映射到标准 ticker → 关联 tag |

**信息管理 Skill 支持的对话指令**：

- "帮我把这条新闻存到未读列表，标记为高重要性"
- "我最近一周高重要性的未读信息有哪些？"
- "把我关于 AI 芯片的想法记录一下：..."
- "把这条信息分享给团队"
- "搜索所有关于半导体的已读信息"

**标的识别 Skill**（被其他 Skill 组合调用）：

- 复用现有 `news_symbols_dict` 表作为标的字典
- 复用现有 `ContentTaggingService` 的 LLM 标签逻辑
- 识别结果写入 `ticker_tag_associations`

**与新闻质量过滤的联动**（详见 `新闻质量过滤方案.md` Layer 3 + 反馈闭环）：

- 用户从 News 页面将新闻**收录到 `research_items`** 时，同步写入 `news_user_interactions`（`interaction_type='save'`）
- 用户在 News 页面对新闻点 👍/👎 / 隐藏时，同步写入 `news_user_interactions`
- 这些行为信号将用于：(a) 定期回更 `news_source_scores` 的来源信誉；(b) 计算 Layer 3 个性化相关性
- `research_items` 的批量收录 API（`batch-collect`）应默认按 `quality_score DESC` 排序候选新闻

#### 1.3 前端新增

- **ResearchHub Page** (`/research`) — 信息管理中心
  - Tab 切换：未读 / 已读 / 想法
  - 左侧：按 tag 分组的信息列表，支持 importance 筛选
  - 右侧：内容详情 + 关联标的 + 手动编辑标签
  - 顶部：快速收录按钮
  - 分享开关 + 团队共享 Feed
- **现有页面增强**：
  - News 页面：新增"收录到研究库"按钮
  - Feeds 页面：新增"收录到研究库"按钮
  - Files 页面：处理结果可一键收录

#### 1.4 后端 API

```
POST   /api/research/items                        # 创建研究条目
GET    /api/research/items                        # 查询（支持 status/importance/tags/tickers 筛选）
PUT    /api/research/items/{id}                   # 更新（状态/重要性/标签等）
DELETE /api/research/items/{id}                   # 删除
POST   /api/research/items/{id}/share             # 分享
GET    /api/research/shared                       # 获取团队分享的信息流
POST   /api/research/items/batch-collect          # 从 news/twitter/file 批量收录
POST   /api/research/items/{id}/auto-tag          # 触发自动标签+标的识别
```

---

### Phase 2：主题驱动的市场追踪（5-6 周）

> **对应需求**：Jikun #4-#5、李晔 #6-#7、需求文档第17行

#### 2.1 Tag → Portfolio 引擎

**核心数据流**：

```
theme_master_data + ticker_tag_associations
           │
           ▼
resolve_theme_tickers()  ← 聚合每个 tag 下所有标的
           │
           ▼
construct_portfolio()    ← 从 EODHD 获取市值，计算 EW/VW 权重
           │
           ▼
theme_portfolios 表      ← 保存组合配置
           │
           ▼
Scheduler 定时任务:
  calc_portfolio_nav()   ← 每日收盘后计算净值
           │
           ▼
theme_portfolio_nav 表   ← 时序数据，供前端绘图
```

**数据模型**：

```sql
-- 主题投资组合
CREATE TABLE theme_portfolios (
    id SERIAL PRIMARY KEY,
    theme_id INTEGER,                              -- REFERENCES theme_master_data(id)
    tag_name VARCHAR(200) NOT NULL,

    -- 组合成分
    tickers JSONB NOT NULL,                        -- [{"symbol": "AAPL.US", "weight_ew": 0.1, "weight_vw": 0.15, "market_cap": 3e12}]
    ticker_count INTEGER DEFAULT 0,

    -- 组合表现快照
    latest_performance JSONB,                      -- {"1d": 0.02, "1w": -0.01, "1m": 0.05, "3m": 0.12}
    latest_snapshot_at TIMESTAMP,

    -- 配置
    auto_update BOOLEAN DEFAULT TRUE,
    last_rebalanced_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),

    UNIQUE(tag_name)
);

-- 主题组合每日净值
CREATE TABLE theme_portfolio_nav (
    id SERIAL PRIMARY KEY,
    portfolio_id INTEGER REFERENCES theme_portfolios(id) ON DELETE CASCADE,
    nav_date DATE NOT NULL,
    nav_ew FLOAT,                                  -- 等权净值
    nav_vw FLOAT,                                  -- 市值加权净值
    daily_return_ew FLOAT,
    daily_return_vw FLOAT,
    turnover FLOAT,
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(portfolio_id, nav_date)
);

CREATE INDEX idx_portfolio_nav_date ON theme_portfolio_nav(portfolio_id, nav_date DESC);
```

#### 2.2 新增 Skills

| Skill ID | 名称 | 对话示例 |
|----------|------|---------|
| `theme_tracker` | 主题追踪 Skill | "AI 芯片主题最近表现怎么样？" "半导体主题对关税新闻反应如何？" |
| `market_tracker` | 市场追踪 Skill | "今天市场情况怎么样？" "我关注的主题里哪个表现最好？" |

**主题追踪 Skill 执行步骤**：

1. 解析用户指定的 tag → 从 `theme_portfolios` 获取组合
2. 从 `theme_portfolio_nav` 获取净值走势
3. 从 `news_themes_unified` 获取该 tag 的近期新闻
4. 如有特定事件 → 计算事件前后的价格变化
5. 调用 LLM 生成分析报告

**市场追踪 Skill**：

- 在现有 `MarketWeeklyService.generate_report()` 基础上增加个性化维度
- 加入用户关注的 tags 对应组合表现
- 加入 watchlist 标的的当日表现

#### 2.3 Scheduler 新增任务

| 任务类型 | 频率 | 功能 |
|---------|------|------|
| `portfolio_rebalance` | 每周 | 根据 `ticker_tag_associations` 重新构建组合权重 |
| `portfolio_nav_calc` | 每日收盘后 | 计算各主题组合的当日净值 |

#### 2.4 前端新增

- **ThemeDashboard**（扩展现有 `InvestmentThemesPage`）
  - 新 Tab："主题组合表现"
  - EW/VW 净值曲线（复用 `lightweight-charts`）
  - 主题间对比视图（多条曲线叠加）
  - 点击某天 → 显示当天相关新闻
  - 主题组合成分表（当前持仓 + 权重）

---

### Phase 3：智能报告 + 前瞻事件日历（4-5 周）

> **对应需求**：Jikun #1（Daily/Weekly Summary）、李晔 #19-21 行（未来事件日历）

#### 3.1 个性化定时报告

**用户偏好配置**（复用 `user_skill_preferences` 表）：

```json
{
  "skill_id": "daily_briefing",
  "schedule_config": {
    "cron": "0 7 * * 1-5",
    "timezone": "America/New_York"
  },
  "custom_parameters": {
    "include_sections": ["market_overview", "my_themes", "top_stories", "twitter_digest"],
    "focus_tags": ["AI_chips", "geopolitics_iran", "us_macro"],
    "watchlist_symbols": ["NVDA.US", "TSM.US", "ASML.AS"],
    "language": "zh"
  }
}
```

**Scheduler 集成**：

在现有 `scheduler_service.py` 的 `task_handlers` 中新增：

```python
"skill_execution": self._execute_skill_task
```

执行逻辑：

1. 从 `user_skill_preferences` 加载用户配置
2. 构建 `SkillContext`（`trigger_mode="schedule"`）
3. 调用 `SkillEngine.execute()`
4. 结果存入 `skill_execution_logs`
5. 推送通知（前端 WebSocket / 邮件 / 未来可扩展）

**报告类 Skills**：

| Skill ID | 名称 | 默认频率 | 内容 |
|----------|------|---------|------|
| `daily_briefing` | 每日市场简报 | 工作日早7点 | 市场概览 + 关注主题表现 + **高质量新闻精选**（`quality_score >= 3.5`）+ Twitter 摘要 |
| `weekly_digest` | 每周投研摘要 | 周一早8点 | 一周回顾 + 主题排行 + 下周前瞻 + **本周 Top Stories**（按 `quality_score` 排序取 Top N） |
| `theme_alert` | 主题异动提醒 | 实时 / 每小时 | 关注主题 EW/VW 组合异常波动 |
| `twitter_digest` | Twitter 精选摘要 | 每日 | 关注账号 tweets 按主题聚类摘要 |
| `news_quality_scorer` | 新闻质量评分 | 跟随新闻入库 | Layer 1 规则评分 + Layer 2 LLM 质量评分（详见 `新闻质量过滤方案.md`） |
| `personalized_news_feed` | 个性化新闻推荐 | 对话触发 / 每日 | 综合 quality_score + 用户 tag/watchlist 匹配 + 行为反馈，生成个性化新闻流 |

#### 3.2 前瞻事件日历

```sql
CREATE TABLE forward_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- 事件内容
    title VARCHAR(500) NOT NULL,
    description TEXT,
    event_type VARCHAR(50),            -- 'scheduled', 'predicted', 'catalyst'

    -- 时间（支持精确日期和范围）
    date_start DATE,
    date_end DATE,
    date_precision VARCHAR(20),        -- 'exact', 'week', 'month', 'quarter', 'range'

    -- 关联
    tags JSONB DEFAULT '[]',
    tickers JSONB DEFAULT '[]',

    -- 元数据
    source VARCHAR(50),                -- 'system', 'user', 'team'
    source_id VARCHAR(200),            -- 如来自 economic_events / earnings_calendar
    importance VARCHAR(10),
    created_by VARCHAR(100),
    is_shared BOOLEAN DEFAULT TRUE,

    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_forward_events_dates ON forward_events(date_start, date_end);
CREATE INDEX idx_forward_events_tags ON forward_events USING GIN(tags);
CREATE INDEX idx_forward_events_type ON forward_events(event_type);
```

**数据自动填充**：
- 从现有 `economic_events` 表同步经济数据发布日程 → `source='system'`
- 从现有 `earnings_calendar` 表同步财报日程 → `source='system'`
- 从现有 `ipo_calendar` 表同步 IPO → `source='system'`
- 用户手动添加预判事件 → `source='user'`

**前瞻事件 Skill**（`forward_events_tracker`）：
- 对话触发："未来三个月有哪些可能影响 AI 主题的事件？"
- 定时触发：每周生成"下周重要事件预告"

#### 3.3 前端新增

- **ReportCenter Page** (`/reports`) — 报告中心
  - 报告订阅管理（选择 Skill、配置频率、设置参数）
  - 历史报告列表（从 `skill_execution_logs` 获取）
  - 报告详情查看（Markdown 渲染）
- **EventCalendar Page** (`/calendar`) — 前瞻事件日历
  - 横轴时间、纵轴按 tag 分组
  - 视图切换：1个月 / 3个月 / 6个月 / 1年
  - 确定事件用实心标记，预判事件用虚线范围标记
  - 事件详情弹窗 → 关联的 research_items + theme_portfolios 表现

---

### Phase 4：高级能力扩展（6-8 周，按需拆分）

#### 4.1 MCP 集成

在 `DataSourceResolver` 中新增 MCP 数据源类型：

```python
class MCPDataSource:
    """作为 Skill 的外部数据源之一"""
    async def query(self, server_name: str, tool_name: str, params: dict) -> dict:
        # 通过 MCP 协议调用外部 Server
        pass

# DataSourceResolver 扩展
SOURCE_MAP["mcp:finance_data"] = ("MCPDataSource", "query")
```

- 用户在 Chat 的数据源选择面板中可以看到已注册的 MCP 服务
- Skill 可以在 `required_data_sources` 中声明 `mcp:server_name` 类型

#### 4.2 主题级因子分析（长期规划）

对应需求文档第17行的长期规划：

| 因子 | 数据来源 | 计算层级 |
|------|---------|---------|
| Momentum (1m/3m/6m/12m) | `theme_portfolio_nav` | 主题组合 |
| Value (P/E, P/B) | `eodhd_fundamentals_service` → 组合加权 | 主题组合 |
| Profitability (ROE, margins) | `eodhd_fundamentals_service` → 组合加权 | 主题组合 |
| Idiosyncratic Vol | `eod_stock_prices` → 组合残差波动率 | 主题组合 |
| Technical (RSI, MACD) | `theme_portfolio_nav` → 技术指标计算 | 主题组合 |
| Macro Beta | `macro_indicators` + `theme_portfolio_nav` → 回归 | 主题组合 |

#### 4.3 爬虫数据源框架

在 Scheduler 中新增 `crawler` task type，支持注册自定义爬虫脚本。

---

## 七、需求与功能映射总表

| 需求编号 | 需求描述 | 对应 Phase | 对应 Skill | 关键模块 |
|---------|---------|-----------|-----------|---------|
| Jikun #1 | Twitter Feed → Daily/Weekly Summary | Phase 3 | `daily_briefing`, `twitter_digest` | SubscriptionContentService + Skill Engine |
| Jikun #2 | Top Stories + event analysis on demand | Phase 0+1 | `general_qa`, `event_analysis`, `personalized_news_feed` | Chat UI + News Service + **新闻质量过滤三层漏斗**（`quality_score` 排序 Top Stories） |
| Jikun #3 | Major events timeline + analysis | Phase 0 | `event_analysis` | News + Themes + LLM |
| Jikun #4 | Theme Tracker | Phase 2 | `theme_tracker` | theme_portfolios + theme_portfolio_nav |
| Jikun #5 | Market Tracker | Phase 2 | `market_tracker` | MarketWeeklyService + 个性化扩展 |
| 李晔 #1-3 | 三个数据库（未读/已读/想法）+ 标签 + 重要性 | Phase 1 | `info_management` | research_items 表 |
| 李晔 #4 | 信息分享机制 | Phase 1 | `info_management` | research_item_shares 表 |
| 李晔 #5 | 自动识别投资标的并关联标签 | Phase 1 | `ticker_recognition`（基础 Skill） | ticker_tag_associations 表 |
| 李晔 #6 | 主题 → 标的 → EW/VW 组合 | Phase 2 | `theme_tracker` | theme_portfolios + theme_portfolio_nav |
| 李晔 #7 | 主题自动关联新闻 | Phase 2 | `theme_tracker` | news_themes_unified + 向量匹配 |
| 李晔 P17 | 主题级因子分析 | Phase 4 | `theme_factor_analysis` | 长期规划 |
| 李晔 P19-21 | 前瞻事件日历 | Phase 3 | `forward_events_tracker` | forward_events 表 |
| 通用 | 对话式交互 | Phase 0 | 所有 Skills | Chat UI + Skill Router |
| 通用 | 定时个性化报告 | Phase 3 | 报告类 Skills | Scheduler + user_skill_preferences |
| 通用 | 新闻质量过滤（横切） | Phase 0 Layer 1 + Phase 1 反馈 | `news_quality_scorer`、`personalized_news_feed` | 新闻质量过滤方案三层漏斗 + 反馈闭环 |

---

## 八、与现有代码的集成原则

1. **不重写，做桥接**：Skill 的执行逻辑调用现有 Service，而非重新实现。例如 `market_briefing` Skill 内部调用 `MarketWeeklyService.generate_report()`。

2. **复用 LLM 模板体系**：所有 Skill 的 prompt 注册为 `llm_prompt_templates`，复用现有的模板管理、版本控制、用户偏好机制。

3. **复用主题融合体系**：标签匹配直接调用 `ThemeFusionService` 和 `InvestmentThemeEmbeddingService`。

4. **复用 Scheduler + Celery**：定时报告通过现有 APScheduler 触发（新增 `skill_execution` task type），耗时操作通过 Celery 异步执行（新增 `skill_execution` 队列）。

5. **前端渐进增强**：Chat 页面是新增的，其他现有页面（News/Feeds/Files/Themes）增加"一键收录"、"发送到对话"等入口按钮。

6. **新闻质量作为横切关注点**（详见 `新闻质量过滤方案.md`）：新闻质量过滤不是一个独立 Phase，而是贯穿多个阶段的基础能力 — Phase 0 实施 Layer 1 规则过滤 + Layer 2 LLM 评分；Phase 1 打通用户行为反馈闭环；Phase 3 的报告类 Skill 统一通过 `DataSourceResolver` 的默认质量参数获取高质量新闻。所有需要 `news` 数据源的 Skill，默认经过质量过滤。

---

## 九、开发优先级与时间线

```
时间线 ──────────────────────────────────────────────────────────────→

Phase 0 (3-4w)         Phase 1 (4-5w)          Phase 2 (5-6w)         Phase 3 (4-5w)         Phase 4
┌──────────────┐   ┌──────────────────┐   ┌────────────────┐   ┌──────────────────┐   ┌──────────┐
│ Skill 引擎内核│   │ 信息管理体系      │   │ 主题追踪+组合   │   │ 定时报告+前瞻日历 │   │ MCP      │
│ Chat UI      │   │ (三库+标签+分享)  │   │ Tag→Portfolio  │   │ (报告使用质量分   │   │ 因子分析  │
│ 通用 QA Skill │   │ 标的识别 Skill    │   │ 市场追踪 Skill  │   │  筛选 Top Stories)│   │ 爬虫框架  │
│ + 新闻过滤    │   │ + 用户行为反馈    │   │                │   │                  │   │          │
│   Layer 1+2  │   │   闭环联动        │   │                │   │                  │   │          │
└──────────────┘   └──────────────────┘   └────────────────┘   └──────────────────┘   └──────────┘
       ↑                                                                                     ↑
  最高优先级                                                                              按需推进
  解锁对话 + 新闻降噪                                                                    长期规划

  ──── 新闻质量过滤（横切关注点，贯穿 Phase 0→1→3）────────────────────
  Phase 0: Layer 1 规则过滤 + Layer 2 LLM 评分
  Phase 1: Layer 3 用户行为反馈闭环（save/share/thumbs → news_source_scores 回更）
  Phase 3: 报告类 Skill 默认使用 quality_score 筛选新闻
```

**关键依赖关系**：

- Phase 0 是所有后续 Phase 的前置条件
- Phase 1 和 Phase 2 可以并行开发（如有两位开发者）
- Phase 3 依赖 Phase 1+2 的数据（有内容才能生成报告）
- Phase 4 可按需独立推进

---

## 十、风险与注意事项

| 风险 | 影响 | 缓解措施 |
|------|------|---------|
| Skill Router 意图识别准确率不足 | 路由到错误的 Skill | 提供用户手动选择 Skill 的能力作为兜底 |
| 主题组合标的数据覆盖不全 | Portfolio 净值计算不准 | 优先支持 EODHD 覆盖的市场（US/HK/EU） |
| LLM 调用成本随 Skill 使用量增长 | 运营成本上升 | 增加缓存层 + 控制单次 Skill 的 token 上限；新闻质量评分优先合并到 `news_classification` 模板（一次调用产出分类+质量分），Layer 1 预筛低分新闻不进入 LLM 评分以控制调用量 |
| 前端页面过多导致导航复杂 | 用户体验下降 | Chat 作为统一入口，减少页面跳转 |
| 数据源过多导致 Skill 执行时间过长 | 对话响应慢 | DataSourceResolver 支持并行获取 + 超时控制 |

---

## 十一、修订记录

| 版本 | 日期 | 说明 |
|------|------|------|
| v1.0 | 2026-03-18 | 初稿：Skill 引擎内核 + 对话系统 + 四阶段实施计划 |
| v1.1 | 2026-03-18 | 整合新闻质量过滤方案：2.2 缺口新增质量过滤项；4.1 新增 `news_quality_scorer` / `personalized_news_feed` Skills；4.4 DataSourceResolver 增加默认质量过滤参数；4.5 新增 `news_source_scores`、`news_user_interactions` 表及 `news` 表扩展字段；Phase 0 新增 Layer 1 并行任务；Phase 1 新增用户行为反馈联动；Phase 3 报告类 Skill 明确使用 `quality_score`；集成原则新增横切关注点说明；时间线图更新 |
