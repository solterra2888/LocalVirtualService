# LLM 体系整体重构实施方案

> 版本：v1.0（2026-04-27）
> 决策依据：见 [LLM_ARCHITECTURE_OVERVIEW.md](./LLM_ARCHITECTURE_OVERVIEW.md) 第 1-7 节排查与评估
> 实施范围：4 个阶段、8 个 PR、预计 12-15 工作日

---

## 0. 决策汇总

| # | 决策点 | 选择 | 含义 |
|---|--------|------|------|
| Q1 | 用户标识 | **D1.C** | 仅新增表强制 INT FK，旧表保持现状 |
| Q2 | LLM 调用入口 | **D2.B** | `LocalLLMAnalysisService` 内部改调 `chat_completion_with_template`，对外 API 保持不变 |
| Q3 | 模板解析机制 | **D3.A** | 引入统一 `LLMTemplateResolver`，三级 fallback |
| Q4 | Category 命名 | **D4.A** | `llm_prompt_templates` 增 `module_code` 字段；废弃 `twitter_report_user_config` 孤儿表 |
| Q5 | Feed 个性化 | **D5.A** | 新增 `subscription_content_user_summaries`（仅 summary/reasoning per-user） |
| Q6 | File Tag 自定义 | **D6.A** | 上传弹窗 picker + 用户偏好 |
| Q7 | Tag schema | **D7.A** | `file_tags` / `subscription_content_tags` 两表共存 |
| Q8 | Schema 治理 | **D8.A** | 阶段 0 立刻全量补 migration |
| R1 | Web Clip 触发 analyze | **A** | 与 web-link 一致 |
| R2 | PATCH content `trigger_llm` 触发 analyze | **A** | 与 content 写入路径一致 |

---

## 1. 总体架构（改造后）

```
┌──────────────────────────────────────────────────────────────────────┐
│                           业务调用方                                  │
│ Upload Files / Feeds / Daily Reports / News Analysis / 用户主动重打   │
└──────────────────────────┬───────────────────────────────────────────┘
                           │ 参数: module_code + (可选 runtime_template_id)
                           ▼
┌──────────────────────────────────────────────────────────────────────┐
│           LLMTemplateResolver  (新增统一模板解析层)                   │
│   resolve(user_id, module_code, runtime_template_id?) →               │
│     { template_id, template_code, source: 'runtime'|'preference'|     │
│        'default', recommended_config }                                │
└──────────────────────────┬───────────────────────────────────────────┘
                           │ 解析后的 template_id
                           ▼
┌──────────────────────────────────────────────────────────────────────┐
│   LocalLLMAnalysisService  (重构: 仅作语义包装层)                     │
│   - analyze_file_content(file_content, ...) 仍存（兼容）              │
│   - 内部改调 llm_service.chat_completion_with_template()              │
│   - 自动写 llm_prompt_usage_logs                                      │
└──────────────────────────┬───────────────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────────────┐
│   LLMService.chat_completion_with_template (唯一中央入口)              │
│   PromptRenderer → ConfigResolver → chat_completion → usage_log        │
└──────────────────────────────────────────────────────────────────────┘
```

---

## 2. 阶段 0：Schema 治理（前置必做，1-2 工作日）

### 2.1 目标

把 4 张核心 LLM 表的 DDL **从生产库反向导出并入版本控制**，建立 schema 的单一真相来源。

### 2.2 操作步骤

#### Step 1：从生产库导出真实 DDL

```bash
# 在 SSH 至生产库可达的环境下：
PGPASSWORD='1q23l,Sc45+@!' pg_dump \
    -h 43.99.37.76 -p 5432 -U postgres -d guanhe_database \
    --schema-only --no-owner --no-acl \
    -t llm_prompt_templates \
    -t llm_prompt_categories \
    -t llm_prompt_usage_logs \
    -t file_tags \
    > /tmp/baseline_llm_dump.sql

# 然后人工 review，把 CREATE TABLE / INDEX / TRIGGER / COMMENT
# 提取到下面的 baseline migration 文件
```

#### Step 2：新建 baseline migration

文件：`backend/database/migrations/000_baseline_llm_tables.sql`

预期内容骨架（**实际字段以 dump 为准**，下面是基于代码反推的最小集合）：

```sql
-- =====================================================
-- Baseline: LLM Core Tables (v1.0 - 2026-04-27)
-- =====================================================
-- 说明: 把生产库已存在但未在 migrations 中版本化的 LLM 核心表
--       反向写入版本控制，作为后续所有 LLM migration 的 baseline。
--       原则: IF NOT EXISTS 兼容已部署环境；CREATE TABLE 仅在新环境生效。

-- ─────────────────────────────────────────────────────
-- 1. llm_prompt_categories
-- ─────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS llm_prompt_categories (
    id           SERIAL PRIMARY KEY,
    category_code VARCHAR(64) NOT NULL UNIQUE,
    category_name VARCHAR(128) NOT NULL,
    description   TEXT,
    icon          VARCHAR(64),
    sort_order    INT DEFAULT 0,
    is_active     BOOLEAN DEFAULT TRUE,
    created_at    TIMESTAMP DEFAULT NOW(),
    updated_at    TIMESTAMP DEFAULT NOW()
);

-- ─────────────────────────────────────────────────────
-- 2. llm_prompt_templates
-- ─────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS llm_prompt_templates (
    id                    SERIAL PRIMARY KEY,
    template_code         VARCHAR(128) NOT NULL UNIQUE,
    template_name         VARCHAR(255) NOT NULL,
    category_id           INT REFERENCES llm_prompt_categories(id) ON DELETE SET NULL,
    -- module_code: 阶段 1 新增字段（见 §3.3）
    -- module_code        VARCHAR(64),
    system_prompt         TEXT,
    user_prompt_template  TEXT NOT NULL,
    variables             JSONB DEFAULT '{}'::jsonb,
    output_format         JSONB DEFAULT '{"type":"text"}'::jsonb,
    description           TEXT,
    scope                 VARCHAR(16) DEFAULT 'user'
                          CHECK (scope IN ('system','user','shared')),
    created_by            INT REFERENCES users(id) ON DELETE SET NULL,
    is_public             BOOLEAN DEFAULT FALSE,
    is_active             BOOLEAN DEFAULT TRUE,
    is_default            BOOLEAN DEFAULT FALSE,
    recommended_provider  VARCHAR(32),
    recommended_model     VARCHAR(128),
    temperature           NUMERIC(3,2) DEFAULT 0.7,
    max_tokens            INT DEFAULT 2000,
    version               INT DEFAULT 1,
    parent_template_id    INT REFERENCES llm_prompt_templates(id) ON DELETE SET NULL,
    tags                  JSONB DEFAULT '[]'::jsonb,
    example_input         TEXT,
    example_output        TEXT,
    usage_count           INT DEFAULT 0,
    success_rate          NUMERIC(5,2),
    avg_cost              NUMERIC(10,6),
    created_at            TIMESTAMP DEFAULT NOW(),
    updated_at            TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_lpt_template_code ON llm_prompt_templates(template_code);
CREATE INDEX IF NOT EXISTS idx_lpt_category ON llm_prompt_templates(category_id);
CREATE INDEX IF NOT EXISTS idx_lpt_scope_active ON llm_prompt_templates(scope, is_active);

-- ─────────────────────────────────────────────────────
-- 3. llm_prompt_usage_logs
-- ─────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS llm_prompt_usage_logs (
    id                SERIAL PRIMARY KEY,
    template_id       INT REFERENCES llm_prompt_templates(id) ON DELETE SET NULL,
    user_id           INT REFERENCES users(id) ON DELETE SET NULL,
    rendered_prompt   TEXT,
    variables_data    JSONB,
    response_content  TEXT,
    provider          VARCHAR(32),
    model             VARCHAR(128),
    tokens_used       INT DEFAULT 0,
    cost              NUMERIC(10,6) DEFAULT 0,
    response_time_ms  INT DEFAULT 0,
    success           BOOLEAN DEFAULT TRUE,
    error_message     TEXT,
    created_at        TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_lpul_template_id ON llm_prompt_usage_logs(template_id);
CREATE INDEX IF NOT EXISTS idx_lpul_user_id ON llm_prompt_usage_logs(user_id);
CREATE INDEX IF NOT EXISTS idx_lpul_created_at ON llm_prompt_usage_logs(created_at DESC);

-- ─────────────────────────────────────────────────────
-- 4. file_tags
-- ─────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS file_tags (
    id              SERIAL PRIMARY KEY,
    file_id         INT NOT NULL UNIQUE
                    REFERENCES file_processing_details(id) ON DELETE CASCADE,
    sector          TEXT,
    sector_en       TEXT,
    industry        TEXT,
    industry_en     TEXT,
    ticker          JSONB,
    concepts        JSONB DEFAULT '[]'::jsonb,
    additional_tags JSONB DEFAULT '{}'::jsonb,
    confidence      NUMERIC(3,2),
    reasoning       TEXT,
    summary         TEXT,
    tagged_by       INT REFERENCES users(id) ON DELETE SET NULL,
    tagged_at       TIMESTAMP DEFAULT NOW(),
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_ft_sector_industry ON file_tags(sector, industry);
CREATE INDEX IF NOT EXISTS idx_ft_ticker_gin ON file_tags USING gin(ticker);
```

#### Step 3：补齐 db_manager.py 缺失列

新增 migration `001_processing_stats_llm_columns.sql`：

```sql
ALTER TABLE processing_stats
    ADD COLUMN IF NOT EXISTS llm_model VARCHAR(128),
    ADD COLUMN IF NOT EXISTS llm_provider VARCHAR(32);

ALTER TABLE file_processing_details
    ADD COLUMN IF NOT EXISTS file_storage_type VARCHAR(16) DEFAULT 'local',
    ADD COLUMN IF NOT EXISTS oss_bucket VARCHAR(128);

CREATE INDEX IF NOT EXISTS idx_fpd_storage_type ON file_processing_details(file_storage_type);
```

同时**同步更新** `backend/database/db_manager.py::initialize_database()` 的基线 CREATE 语句，确保新环境一次到位。

#### Step 4：修复 add_unread_count.sql 的迁移顺序冲突

```sql
-- 在 add_unread_count.sql 顶部加守护：
-- 注意：本 migration 假设 subscription_content.subscription_id 仍存在；
-- 若已先跑 drop_subscription_id_column.sql，跳过那段计数初始化（已由触发器维护）
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.columns
               WHERE table_name='subscription_content' AND column_name='subscription_id') THEN
        -- 旧的初始化逻辑
        UPDATE subscription_list sl SET unread_count = (
            SELECT COUNT(*) FROM subscription_content sc
            WHERE sc.subscription_id = sl.id AND ...
        );
    END IF;
END $$;
```

### 2.3 验收

- [ ] 在干净的 PostgreSQL 实例上**仅执行 migrations 目录**能完整建出 LLM 体系
- [ ] `pg_dump --schema-only` 在 dev 与 baseline migration 输出 diff 为空（除时间戳）
- [ ] CI 增加 schema 漂移检测（可选，长期建议）

### 2.4 阶段 0 风险

| 风险 | 缓解 |
|------|------|
| 反向 dump 出来的字段与代码读写不一致（隐式列）| 用代码 grep `INSERT INTO file_tags` / `UPDATE processing_stats` 校验 |
| 已部署环境因 IF NOT EXISTS 不会重建 | 仅生效于新环境；旧环境靠 ALTER 补 |
| dump 包含本地脏数据 | `--schema-only` 已规避 |

---

## 3. 阶段 1：LLM 基建统一（3-5 工作日）

### 3.1 新增 `LLMTemplateResolver`

文件：`backend/services/llm_template_resolver.py`（新建）

```python
"""
统一模板解析层：所有 LLM 调用前先经由此服务确定最终模板。

优先级:
  runtime_template_id (调用方运行时传入)
       ↓ 未传
  user_template_preferences[user_id, module_code] (用户偏好)
       ↓ 未设
  MODULE_REGISTRY[module_code].default_template_code (系统默认)
       ↓ 未配
  抛错
"""
from dataclasses import dataclass
from typing import Optional, Literal
from backend.services.template_preference_service import TemplatePreferenceService
from backend.services.llm_prompt_template_service import LLMPromptTemplateService

ResolveSource = Literal['runtime', 'preference', 'default']

@dataclass
class ResolvedTemplate:
    template_id: int
    template_code: str
    source: ResolveSource
    module_code: str

class LLMTemplateResolver:
    def __init__(self):
        self.pref = TemplatePreferenceService()
        self.tpl = LLMPromptTemplateService()

    def resolve(
        self,
        user_id: int,
        module_code: str,
        runtime_template_id: Optional[int] = None,
    ) -> ResolvedTemplate:
        # 1. runtime
        if runtime_template_id:
            t = self.tpl.get_template(runtime_template_id)
            if not t or not t.get('is_active'):
                raise ValueError(f"runtime template {runtime_template_id} not found or inactive")
            return ResolvedTemplate(t['id'], t['template_code'], 'runtime', module_code)

        # 2. user preference
        pref = self.pref.get_preference(user_id, module_code) if user_id else None
        if pref:
            return ResolvedTemplate(pref['template_id'], pref['template_code'], 'preference', module_code)

        # 3. system default
        default_code = self.pref.SYSTEM_DEFAULTS.get(module_code)
        if not default_code:
            # 检查 MODULE_REGISTRY 中是否有 default_template_code
            module = next((m for m in self.pref.MODULE_REGISTRY if m['module_code'] == module_code), None)
            default_code = module and module.get('default_template_code')
        if not default_code:
            raise ValueError(f"no default template for module '{module_code}'")
        t = self.tpl.get_template_by_code(default_code)
        if not t:
            raise ValueError(f"system default template '{default_code}' not found")
        return ResolvedTemplate(t['id'], t['template_code'], 'default', module_code)
```

### 3.2 扩展 `MODULE_REGISTRY`

文件：`backend/services/template_preference_service.py`

```python
MODULE_REGISTRY: List[Dict[str, Any]] = [
    # —— Daily Reports（已有，保留）——
    {'module_code': 'twitter_phase1', 'configurable': False, 'default_template_code': 'twitter_daily_phase1_default'},
    {'module_code': 'twitter_phase2', 'configurable': True,  'default_template_code': 'twitter_daily_phase2_default'},
    {'module_code': 'youtube_phase1', 'configurable': False, 'default_template_code': 'youtube_daily_phase1_default'},
    {'module_code': 'youtube_phase2', 'configurable': True,  'default_template_code': 'youtube_daily_phase2_default'},

    # —— File Module（修复孤儿 + 新增）——
    {'module_code': 'file_analysis',  'configurable': True,  'default_template_code': 'audio_analysis_v1'},
    {'module_code': 'file_tagging',   'configurable': True,  'default_template_code': 'content_tag_v1'},      # ⭐新增
    {'module_code': 'file_naming',    'configurable': True,  'default_template_code': 'auto_name_v1'},        # ⭐新增（可选）
    {'module_code': 'file_classify',  'configurable': True,  'default_template_code': 'auto_classify_v1'},    # ⭐新增（可选）

    # —— Feed Module（新增）——
    {'module_code': 'feed_tagging',   'configurable': True,  'default_template_code': 'content_tag_feeds_v1'},  # ⭐新增
    {'module_code': 'feed_summary',   'configurable': True,  'default_template_code': None},                    # ⭐新增

    # —— News Module（修复孤儿）——
    {'module_code': 'news_summary',   'configurable': True,  'default_template_code': 'news_summary_v1'},     # ⭐新增
]

# SYSTEM_ONLY_MODULES 保持不变
SYSTEM_ONLY_MODULES = frozenset({'twitter_phase1', 'youtube_phase1'})
```

### 3.3 给 `llm_prompt_templates` 添加 `module_code` 字段

新增 migration `010_add_module_code_to_templates.sql`：

```sql
ALTER TABLE llm_prompt_templates
    ADD COLUMN IF NOT EXISTS module_code VARCHAR(64);

CREATE INDEX IF NOT EXISTS idx_lpt_module_code ON llm_prompt_templates(module_code);

-- 数据迁移：根据已知 template_code 推断 module_code
UPDATE llm_prompt_templates SET module_code = 'file_tagging'  WHERE template_code = 'content_tag_v1';
UPDATE llm_prompt_templates SET module_code = 'file_naming'   WHERE template_code = 'auto_name_v1';
UPDATE llm_prompt_templates SET module_code = 'file_classify' WHERE template_code = 'auto_classify_v1';
UPDATE llm_prompt_templates SET module_code = 'file_analysis' WHERE template_code = 'audio_analysis_v1';
UPDATE llm_prompt_templates SET module_code = 'feed_tagging'  WHERE template_code = 'content_tag_feeds_v1';
UPDATE llm_prompt_templates SET module_code = 'twitter_phase1' WHERE template_code = 'twitter_daily_phase1_default';
UPDATE llm_prompt_templates SET module_code = 'twitter_phase2' WHERE template_code = 'twitter_daily_phase2_default';
UPDATE llm_prompt_templates SET module_code = 'youtube_phase1' WHERE template_code = 'youtube_daily_phase1_default';
UPDATE llm_prompt_templates SET module_code = 'youtube_phase2' WHERE template_code = 'youtube_daily_phase2_default';
UPDATE llm_prompt_templates SET module_code = 'news_summary'  WHERE template_code IN ('news_summary_v1', 'news_summary_safe_v1', 'news_classification');

-- 用户自建模板按 user_{module_code}_xxx 命名约定推断
UPDATE llm_prompt_templates
SET module_code = SPLIT_PART(template_code, '_', 2)
WHERE template_code LIKE 'user_%' AND module_code IS NULL;
```

前端 `TemplateSelector` 改造：增加 `moduleCode` 参数（与 `categoryCode` 并存一段时间，逐步迁移），按 `module_code` 过滤。

### 3.4 废弃 `twitter_report_user_config`

新增 migration `011_drop_twitter_report_user_config.sql`：

```sql
-- twitter_report_user_config 是孤儿表，从未被 Python 引用过；
-- 用户偏好已统一走 user_template_preferences[twitter_phase1|twitter_phase2]
-- 安全删除前先检查是否真的没有数据/未被外部依赖

DO $$
DECLARE
    row_count INT;
BEGIN
    SELECT COUNT(*) INTO row_count FROM twitter_report_user_config;
    IF row_count > 0 THEN
        RAISE NOTICE 'twitter_report_user_config has % rows; manual review required before drop', row_count;
        -- 不自动删除，留给运维确认
    ELSE
        DROP TABLE IF EXISTS twitter_report_user_config CASCADE;
        RAISE NOTICE 'twitter_report_user_config dropped';
    END IF;
END $$;
```

### 3.5 `LocalLLMAnalysisService` 内部重构（保持外部 API）

文件：`backend/services/local_llm_analysis_service.py`

```python
class LocalLLMAnalysisService:
    """
    重构说明 (2026-04-27):
    - 对外 API 完全不变 (analyze_file_content / process_all_contents)
    - 内部实现改为调用 llm_service.chat_completion_with_template()
    - 自动写 llm_prompt_usage_logs (此前是该服务唯一的"日志缺口")
    - ConfigResolver 不再内联调用，由 chat_completion_with_template 统一处理
    """

    async def analyze_file_content(self, file_content, file_name=None,
                                    custom_variables=None, user_id=None,
                                    provider=None, model=None):
        from backend.core.llm_framework import llm_service

        variables = dict(custom_variables or {})
        variables['file_content'] = file_content

        # 直接走中央入口
        response = await llm_service.chat_completion_with_template(
            template_id=self.template_id,
            template_code=self.template_code,
            variables=variables,
            user_id=user_id,
            provider=provider,
            model=model,
        )

        return {
            'report_content': response.content,
            'total_tokens': response.usage.total_tokens if response.usage else 0,
            'total_price': getattr(response, 'cost', 0),
            'response_time': response.response_time,
            'model': response.model,
            'provider': response.provider,
            'parsed_data': getattr(response, 'parsed_data', None),
        }
```

### 3.6 修复 4 路触发不一致（R1 + R2）

文件：`backend/tasks/file_tasks.py`

```python
# process_web_clip_task (R1)：在 trigger_auto_tagging 之后增加 trigger_auto_analyze
@app.task(...)
def process_web_clip_task(...):
    # ... 抓取写 content ...
    if file_id and content:
        trigger_auto_tagging(file_id, username, delay=...)
        trigger_auto_naming(file_id, username, delay=...)
        trigger_auto_classify(file_id, username, delay=...)
        # ⭐ R1 新增
        trigger_auto_analyze(
            file_detail_id=file_id,
            execution_id=execution_id,
            file_type='web',
            template_id=task_info.get('auto_analyze_template_id'),
            template_variables=task_info.get('auto_analyze_template_variables'),
            username=username,
        )
```

文件：`backend/api/files.py::patch_file_content`

```python
# R2: PATCH .../content 的 trigger_llm 也触发 analyze
@router.patch("/details/{file_id}/content")
async def patch_file_content(file_id, payload, current_user):
    # ... 写 content ...
    if payload.trigger_llm:
        trigger_auto_tagging([file_id], current_user.username, delay=2)
        trigger_auto_naming([file_id], current_user.username, delay=2)
        trigger_auto_classify([file_id], current_user.username, delay=2)
        # ⭐ R2 新增
        # 用 user_template_preferences['file_analysis'] 作为兜底
        trigger_auto_analyze(
            file_detail_id=file_id,
            execution_id=processing_stats_id,
            file_type=file_type,
            template_id=None,  # 让 trigger_auto_analyze 走偏好/默认
            username=current_user.username,
        )
```

### 3.7 任务 status 枚举统一

文件：`backend/tasks/tagging_tasks.py` / `naming_tasks.py` / `classify_tasks.py`

```python
# 统一返回值规范
TASK_STATUS = Literal['completed', 'skipped', 'failed', 'partial']

# 三个任务的 skip 分支统一改为 status='skipped'
return {'status': 'skipped', 'reason': '已有标签且非 force', ...}
```

### 3.8 阶段 1 验收

- [ ] `LLMTemplateResolver.resolve()` 单元测试覆盖 3 种 source
- [ ] `LocalLLMAnalysisService` 重构后所有调用方零改动
- [ ] `llm_prompt_usage_logs` 表能看到原本路径 A 业务（File Tag/Name/Classify、Feed Tag）的调用记录
- [ ] `analyze_document_task` 在用户未传 template_id 时能 fallback 到 `file_analysis` 偏好（孤儿模块激活）
- [ ] `process_web_clip_task` 完成后能看到 4 路并行 LLM 全跑
- [ ] `PATCH .../content` 的 `trigger_llm=true` 触发完整 4 路

---

## 4. 阶段 2：File Tag 用户自定义（2-3 工作日）

### 4.1 后端改动

#### 4.1.1 任务接受 template_id

`backend/tasks/tagging_tasks.py::tag_file_content_task`

```python
@app.task(...)
def tag_file_content_task(
    file_id, username=None,
    additional_context="",
    force=False,
    template_id=None,           # ⭐新增
    template_variables=None,    # ⭐新增
):
    # 通过 LLMTemplateResolver 解析最终模板
    user_id = _get_user_id_by_username(username)
    resolver = LLMTemplateResolver()
    resolved = resolver.resolve(user_id, 'file_tagging', template_id)

    service = ContentTaggingService(
        db_manager=...,
        template_id=resolved.template_id,    # ⭐ 用解析结果
        log_callback=...,
    )
    # ... 其余不变 ...
```

#### 4.1.2 Service 接受 template_id

`backend/services/content_tagging_service.py`

```python
class ContentTaggingService:
    DEFAULT_TEMPLATE_CODE = 'content_tag_v1'  # 不再用类常量

    def __init__(self, db_manager, log_callback=None,
                 template_id=None, template_code=None):
        self.template_id = template_id
        self.template_code = template_code or self.DEFAULT_TEMPLATE_CODE
        self.llm_service = LocalLLMAnalysisService(
            template_id=self.template_id,
            template_code=self.template_code if not self.template_id else None,
            log_callback=log_callback,
        )
```

#### 4.1.3 trigger_auto_tagging 透传 template_id

`backend/tasks/file_tasks.py`

```python
def trigger_auto_tagging(
    file_detail_ids,
    username=None,
    delay=2,
    template_id=None,            # ⭐新增
    template_variables=None,     # ⭐新增
):
    for fid in file_detail_ids:
        tag_file_content_task.apply_async(
            args=(fid, username),
            kwargs={
                'template_id': template_id,
                'template_variables': template_variables,
            },
            countdown=delay,
        )
```

所有 `trigger_auto_tagging(...)` 的调用方改为从 `task_info.get('auto_tag_template_id')` 或 `processing_stats.upload_params.auto_tag_template_id` 读取并透传。

#### 4.1.4 上传 API 入参

5 个上传端点 (`upload-and-process-document` / `upload-audio` / `upload-youtube` / `upload-web-link` / `create-text-note`) 都新增可选字段：

```python
class UploadDocumentRequest(BaseModel):
    # ... 已有字段 ...
    auto_tag_template_id: Optional[int] = None
    auto_tag_template_variables: Optional[Dict[str, Any]] = None
    # auto_analyze_* 已存在
```

把这两个字段持久化到 `processing_stats.upload_params`，供后续 task 透传。

#### 4.1.5 Manual Retag API 增加 template_id

`backend/api/file_tags.py`

```python
class RetagRequest(BaseModel):
    file_id: int
    force: bool = False
    template_id: Optional[int] = None      # ⭐新增
    template_variables: Optional[Dict] = None  # ⭐新增

@router.post("/retag")
async def retag(req: RetagRequest, current_user):
    tag_file_content_task.apply_async(
        args=(req.file_id, current_user.username),
        kwargs={
            'force': req.force,
            'template_id': req.template_id,
            'template_variables': req.template_variables,
        },
    )
```

### 4.2 前端改动

#### 4.2.1 5 个上传弹窗增加 picker

文件：`UploadFilesModal.tsx` / `UploadAudioModal.tsx` / `UploadYouTubeLinkModal.tsx` / `UploadWebLinkModal.tsx` / `CreateTextNoteModal.tsx`

```tsx
// 在已有的 <UploadAnalysisTemplatePicker mode="optional" /> 下方
<UploadAnalysisTemplatePicker
  moduleCode="file_tagging"             // ⭐新 prop（替代 categoryCode）
  mode="optional"
  label="AI Tagging Template"
  hint="Optional. Override default tagging logic with your own template."
  value={tagTemplate}
  onChange={setTagTemplate}
/>
```

`UploadAnalysisTemplatePicker` 组件本身需扩展支持 `moduleCode`（现 prop 是 `categoryCode`，过渡期两者择一）。

#### 4.2.2 调用上传 API 时透传

`web-frontend/src/services/files.ts`

```typescript
export interface UploadAnalyzeOptions {
  auto_analyze_template_id?: number;
  auto_analyze_template_variables?: Record<string, any>;
  auto_tag_template_id?: number;            // ⭐新增
  auto_tag_template_variables?: Record<string, any>;  // ⭐新增
}
```

#### 4.2.3 FilesPage 右侧新增 "Re-tag" 入口

`FilesPage.tsx` 右侧 Tags 面板增加按钮：

```tsx
<Button onClick={() => setRetagModalOpen(true)}>Re-tag with template</Button>

<RetagModal
  fileId={selectedFile.id}
  onConfirm={(templateId, vars) =>
    fileTagsService.retag({
      file_id: selectedFile.id,
      template_id: templateId,
      template_variables: vars,
      force: true,
    })
  }
/>
```

### 4.3 阶段 2 验收

- [ ] 上传文档时选择自定义打标模板，`file_tags` 写入对应 `template_id` 元数据
- [ ] 用户偏好生效：用户在 Settings 设了 `file_tagging` 偏好后，上传不选模板也用偏好
- [ ] FilesPage Re-tag 按钮可用
- [ ] 兼容性：旧调用（不传 template_id）仍跑 `content_tag_v1`

---

## 5. 阶段 3：Feed Summary 个性化（4-5 工作日）

### 5.1 数据库设计

新建 migration `020_create_subscription_content_user_summaries.sql`：

```sql
CREATE TABLE IF NOT EXISTS subscription_content_user_summaries (
    id                       SERIAL PRIMARY KEY,
    subscription_content_id  INT NOT NULL
                             REFERENCES subscription_content(id) ON DELETE CASCADE,
    user_id                  INT NOT NULL
                             REFERENCES users(id) ON DELETE CASCADE,
    template_id              INT
                             REFERENCES llm_prompt_templates(id) ON DELETE SET NULL,

    -- 仅存"主观"字段；客观字段（sector/industry/ticker）从 baseline 读
    summary                  TEXT NOT NULL,
    reasoning                TEXT,
    custom_concepts          JSONB DEFAULT '[]'::jsonb,

    ai_metadata              JSONB DEFAULT '{}'::jsonb,
    -- {total_tokens, total_price, model, provider, response_time}

    tagged_at                TIMESTAMP DEFAULT NOW(),
    created_at               TIMESTAMP DEFAULT NOW(),
    updated_at               TIMESTAMP DEFAULT NOW(),

    CONSTRAINT uniq_scus_content_user UNIQUE(subscription_content_id, user_id)
);

CREATE INDEX IF NOT EXISTS idx_scus_user_id ON subscription_content_user_summaries(user_id);
CREATE INDEX IF NOT EXISTS idx_scus_content_id ON subscription_content_user_summaries(subscription_content_id);
CREATE INDEX IF NOT EXISTS idx_scus_template_id ON subscription_content_user_summaries(template_id);
CREATE INDEX IF NOT EXISTS idx_scus_tagged_at ON subscription_content_user_summaries(tagged_at DESC);

DROP TRIGGER IF EXISTS update_scus_updated_at ON subscription_content_user_summaries;
CREATE TRIGGER update_scus_updated_at
    BEFORE UPDATE ON subscription_content_user_summaries
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

COMMENT ON TABLE subscription_content_user_summaries IS
    'Per-user 个性化 summary/reasoning，叠加于全局 subscription_content_tags 之上';
```

### 5.2 后端服务

新建 `backend/services/feed_summary_personalization_service.py`：

```python
class FeedSummaryPersonalizationService:
    """
    Per-user feed summary 个性化服务。
    与 FeedContentTaggingService 互补：
    - FeedContentTaggingService → subscription_content_tags（全局，客观+基础摘要）
    - FeedSummaryPersonalizationService → subscription_content_user_summaries（per-user，主观）
    """

    DEFAULT_MODULE_CODE = 'feed_summary'

    def __init__(self, db_manager):
        self.db = db_manager
        self.resolver = LLMTemplateResolver()

    async def generate_for_user(
        self,
        content_id: int,
        user_id: int,
        runtime_template_id: Optional[int] = None,
        template_variables: Optional[Dict] = None,
        force: bool = False,
    ) -> Dict[str, Any]:
        # 1. 解析模板
        resolved = self.resolver.resolve(user_id, self.DEFAULT_MODULE_CODE, runtime_template_id)

        # 2. 幂等检查
        if not force:
            existing = self._get_existing(content_id, user_id)
            if existing and existing['template_id'] == resolved.template_id:
                return {'success': True, 'skipped': True, 'existing': existing}

        # 3. 拉内容（与 FeedContentTaggingService 共享文本组装逻辑，提取到 _build_text）
        from backend.services.feed_content_tagging_service import FeedContentTaggingService
        text_built = FeedContentTaggingService(self.db)._build_tagging_text(
            self._fetch_content_row(content_id)
        )
        if not text_built:
            return {'success': True, 'skipped': True, 'reason': 'unsupported channel or content too short'}
        text, channel_type = text_built

        # 4. 调用 LLM（走中央入口）
        from backend.core.llm_framework import llm_service
        response = await llm_service.chat_completion_with_template(
            template_id=resolved.template_id,
            variables={
                'file_content': text,
                'channel_type': channel_type,
                **(template_variables or {}),
            },
            user_id=user_id,
        )

        # 5. 解析 + UPSERT
        parsed = response.parsed_data or self._parse_json(response.content)
        return self._upsert(content_id, user_id, resolved.template_id, parsed, response)
```

### 5.3 Celery 任务

新建 `backend/tasks/feed_personalization_tasks.py`：

```python
@app.task(name='personalize_feed_summary', queue='file_processing',
          time_limit=120, soft_time_limit=90)
def personalize_feed_summary_task(content_id, user_id,
                                   template_id=None, template_variables=None, force=False):
    service = FeedSummaryPersonalizationService(db_manager)
    return asyncio.run(service.generate_for_user(
        content_id, user_id, template_id, template_variables, force
    ))
```

### 5.4 API

新增 `backend/api/subscription_content_summaries.py`（或加到现有 `subscription_content_tags.py`）：

```python
@router.post("/contents/{content_id}/personalize")
async def personalize_summary(
    content_id: int,
    payload: PersonalizeRequest,
    current_user
):
    """生成/更新当前用户对该 content 的个性化 summary"""
    user_id = current_user.id
    task = personalize_feed_summary_task.delay(
        content_id, user_id,
        template_id=payload.template_id,
        template_variables=payload.template_variables,
        force=payload.force,
    )
    return {'task_id': task.id, 'content_id': content_id}


@router.get("/contents/{content_id}/personalized-summary")
async def get_personalized_summary(content_id: int, current_user):
    """读取当前用户的个性化 summary（如果有）"""
    # SELECT * FROM subscription_content_user_summaries
    # WHERE subscription_content_id = %s AND user_id = %s
```

### 5.5 改造内容列表读取

`backend/api/subscriptions.py::list_contents`

```python
# 联表 LEFT JOIN，让用户视图自动叠加个性化 summary
sql = """
    SELECT
        sc.*,
        sct.sector, sct.industry, sct.ticker, sct.concepts, sct.confidence,
        COALESCE(scus.summary, sct.summary) AS summary,
        COALESCE(scus.reasoning, sct.reasoning) AS reasoning,
        CASE WHEN scus.id IS NOT NULL THEN 'personalized' ELSE 'global' END AS summary_source,
        scus.template_id AS summary_template_id
    FROM subscription_content sc
    LEFT JOIN subscription_content_tags sct ON sct.subscription_content_id = sc.id
    LEFT JOIN subscription_content_user_summaries scus
        ON scus.subscription_content_id = sc.id AND scus.user_id = %s
    WHERE %s = ANY(sc.user_ids) AND sc.author_identifier = %s
    ORDER BY sc.published_at DESC
    LIMIT %s OFFSET %s
"""
```

### 5.6 前端

#### 5.6.1 Feed 详情页 "Re-summarize" 按钮

`FeedArticleBody.tsx`：

```tsx
{summarySource === 'global' && (
  <Button onClick={onPersonalize}>
    ✨ Re-summarize with my template
  </Button>
)}
{summarySource === 'personalized' && (
  <Tag color="purple">Personalized</Tag>
)}
```

#### 5.6.2 Settings 页偏好选择

新增 `SettingsPage.tsx` 或扩展现有 PromptTemplatesPage：

```tsx
<TemplatePickerByModule
  moduleCode="feed_summary"
  label="My Feed Summary Template"
  description="Used when you click 'Re-summarize' on a feed item"
  value={feedSummaryPref}
  onChange={(tid) => promptTemplateService.setPreference('feed_summary', tid)}
/>
```

#### 5.6.3 加星自动触发（可选，T2 完整版）

```tsx
// 用户加星 feed item 时
await markFavorite(contentId);
// 自动派发个性化任务
await api.post(`/contents/${contentId}/personalize`, {});
```

### 5.7 阶段 3 验收

- [ ] 用户点 "Re-summarize" 后，feed 列表里该条显示个性化 summary 和 "Personalized" 徽标
- [ ] 其他用户看同一条 feed 仍是 baseline summary
- [ ] 客观字段（sector/industry/ticker）跨用户保持一致（不被 per-user 影响）
- [ ] 用户偏好生效：设了 `feed_summary` 偏好后点 Re-summarize 用偏好模板
- [ ] 模板失效（is_active=false）时 fallback 到 baseline，UI 提示

---

## 6. 阶段 4：清理与文档（1-2 工作日，可与阶段 3 并行）

### 6.1 文档更新

- [ ] 更新 `LLM_ARCHITECTURE_OVERVIEW.md`：合并新的统一架构
- [ ] 更新 `FEEDS_MODULE.md`：补充个性化 summary 章节
- [ ] 更新 `UPLOAD_FILES_AND_LLM_ANALYSIS_FLOW.md`：补充用户自定义 tag 章节
- [ ] 新建 `LLM_TEMPLATE_PREFERENCE_GUIDE.md`：用户视角的偏好配置说明

### 6.2 代码清理

- [ ] 删除 `twitter_report_user_config` 表相关 schema 注释（如果数据已确认无用）
- [ ] 清理 `category_code` 在前端的残留引用（统一改 `module_code`）
- [ ] 增加 `LLMTemplateResolver` 在监控面板的使用统计展示

### 6.3 监控增强

- [ ] PromptTemplatesPage 增加 "按 module_code 的 usage 统计" 维度
- [ ] 调用日志查询支持按 module_code 过滤
- [ ] Admin 面板能看到所有用户的偏好设置概览

---

## 7. PR 拆分计划（8 个 PR，便于代码 review 和回滚）

| PR | 标题 | 阶段 | 改动规模 | 依赖 |
|----|------|------|---------|------|
| **PR-1** | `chore(db): baseline migrations for LLM core tables` | 0 | S（仅 SQL）| — |
| **PR-2** | `chore(db): fix processing_stats / file_processing_details missing columns + add_unread_count guard` | 0 | S | PR-1 |
| **PR-3** | `feat(llm): add LLMTemplateResolver + extend MODULE_REGISTRY + add module_code to templates` | 1 | M | PR-1 |
| **PR-4** | `refactor(llm): LocalLLMAnalysisService routes through chat_completion_with_template + status enum unify + R1/R2 fixes` | 1 | M | PR-3 |
| **PR-5** | `feat(file-tag): user-customizable file tagging template (upload modal + retag API + FilesPage Re-tag)` | 2 | L | PR-4 |
| **PR-6** | `feat(feed): create subscription_content_user_summaries + FeedSummaryPersonalizationService` | 3 | M | PR-4 |
| **PR-7** | `feat(feed): personalize API + frontend Re-summarize button + Settings preference` | 3 | L | PR-6 |
| **PR-8** | `docs: update architecture docs + cleanup twitter_report_user_config + monitoring` | 4 | S | PR-7 |

**PR-1 ~ PR-4 是基础设施**，业务可见效果是 0；PR-5 起对用户可见。

---

## 8. 上线、灰度、回滚策略

### 8.1 灰度策略

每个 PR 上线后用环境变量控制：

| 环境变量 | 默认 | 阶段 | 作用 |
|---------|------|------|------|
| `LLM_USE_UNIFIED_RESOLVER` | `true` | 1 | 关闭则回退到旧的硬编码模板逻辑 |
| `LLM_LOG_USAGE_FOR_LOCAL_PATH` | `true` | 1 | 关闭则不写 path A 的 usage_log（紧急回退）|
| `FILE_TAG_USER_CUSTOM_ENABLED` | `false` → `true` | 2 | 灰度上线 file_tag picker |
| `FEED_SUMMARY_PERSONALIZATION_ENABLED` | `false` → `true` | 3 | 灰度上线 feed personalize |

### 8.2 回滚步骤

| 阶段 | 回滚方式 |
|------|---------|
| 阶段 0 | DDL 是 `IF NOT EXISTS`，无破坏性；如需回滚仅删除新增列即可 |
| 阶段 1 | 设环境变量 `LLM_USE_UNIFIED_RESOLVER=false`，旧逻辑保留兜底；`LocalLLMAnalysisService` 内部双轨开关 |
| 阶段 2 | 设 `FILE_TAG_USER_CUSTOM_ENABLED=false`，前端 picker 隐藏；后端 task 接收到 template_id 也按默认走 |
| 阶段 3 | 设 `FEED_SUMMARY_PERSONALIZATION_ENABLED=false`，API 返回 503；新表数据保留可重启用 |

### 8.3 上线检查清单

每个 PR 部署后必查：

```
□ pg_dump --schema-only diff = 0
□ Celery worker 启动无报错（migrations 已跑）
□ 关键 LLM 调用接口手测（Upload Doc + 触发 4 路）
□ 监控指标：llm_prompt_usage_logs 写入速率正常
□ 错误日志：resolver 未抛 "no default template for module" 系列错误
□ 回滚开关测试：环境变量切换后行为符合预期
```

---

## 9. 测试用例骨架

### 9.1 Resolver 单测（PR-3）

```python
# backend/tests/test_llm_template_resolver.py
class TestLLMTemplateResolver:
    def test_resolve_runtime_priority(self):
        # 即使有 preference，runtime 仍优先
        ...

    def test_resolve_preference_fallback(self):
        # 没传 runtime → 用 user_template_preferences
        ...

    def test_resolve_default_fallback(self):
        # 既没 runtime 也没 preference → 用 SYSTEM_DEFAULTS
        ...

    def test_resolve_invalid_module_raises(self):
        # module_code 不存在抛错
        ...

    def test_resolve_inactive_runtime_template_raises(self):
        # runtime template_id 指向 is_active=false 的模板
        ...
```

### 9.2 Path A → B 集成测（PR-4）

```python
class TestLocalLLMAnalysisRefactor:
    async def test_legacy_api_still_works(self):
        # 老调用方零改动通过
        ...

    async def test_usage_log_written(self):
        # 重构后路径 A 能写 usage_log
        log_count_before = ...
        await service.analyze_file_content(...)
        log_count_after = ...
        assert log_count_after == log_count_before + 1
```

### 9.3 Feed 个性化端到端（PR-7）

```python
class TestFeedPersonalization:
    async def test_user_a_personalize_user_b_unaffected(self):
        # 双用户场景：A 个性化后 B 仍看 baseline
        ...

    async def test_objective_fields_remain_global(self):
        # 个性化不影响 sector/industry/ticker
        ...

    async def test_template_inactive_falls_back_to_baseline(self):
        # 用户偏好的模板被禁用 → 回退到 baseline summary
        ...
```

### 9.4 R1/R2 回归测（PR-4）

```python
class TestFourPathTriggers:
    async def test_web_clip_triggers_all_four(self):
        # R1: process_web_clip_task 完成后能看到 analyze 任务
        ...

    async def test_patch_content_triggers_all_four(self):
        # R2: PATCH .../content 的 trigger_llm=true 触发 analyze
        ...
```

---

## 10. 工作量与里程碑

| 阶段 | PR | 工作量 | 里程碑 |
|------|----|--------|------|
| 阶段 0 | PR-1, PR-2 | 1-2 天 | M0: Schema 单一真相来源建立 |
| 阶段 1 | PR-3, PR-4 | 3-5 天 | M1: 统一调用链路 + 4 路触发一致 |
| 阶段 2 | PR-5 | 2-3 天 | M2: File Tag 用户自定义可用 |
| 阶段 3 | PR-6, PR-7 | 4-5 天 | M3: Feed Summary 个性化可用 |
| 阶段 4 | PR-8 | 1-2 天 | M4: 文档与清理完成 |
| **总计** | 8 PR | **12-17 天** | |

可并行：阶段 2 (PR-5) 与阶段 3 (PR-6, PR-7) 可同时开发，因不互相依赖。

---

## 11. 关键风险与对策

| 风险 | 概率 | 影响 | 对策 |
|------|------|------|------|
| 阶段 0 反向 dump 出来的 DDL 与代码读写不一致 | 中 | 高 | dump 后用 grep 反查关键 INSERT/UPDATE，列出所有运行时使用列；diff 后补齐 |
| `LocalLLMAnalysisService` 重构导致下游字段 breaking | 低 | 高 | 内部改造保持返回值结构；增加单测覆盖每个调用方 |
| `module_code` 字段迁移时部分模板归类不准确 | 中 | 低 | UPDATE 后人工 review 一遍前端模板列表 |
| Feed 个性化 LLM 成本意外暴涨 | 中 | 中 | 增加每用户每日限流（默认 50 次/天，可配） |
| `twitter_report_user_config` 删除后某历史数据丢失 | 低 | 低 | 删除前 `pg_dump -t` 备份；migration 只在空表时自动 drop |
| 用户偏好链路打通后历史"file_analysis 偏好"突然生效 | 中 | 中 | 上线前清空所有 file_analysis 偏好（DELETE FROM user_template_preferences WHERE module_code='file_analysis' AND created_at < ?） |

---

## 12. 决策遗留与后续

### 已确认范围内
- ✅ 所有 8 个核心决策 + 2 个产品决策已锁定
- ✅ 阶段 0-4 全流程已规划

### 未来可能的延伸（不在本次范围）
- 模板共享市场（用户互相分享模板）
- LLM 调用成本面板（按 module_code / user / template 维度）
- News Analysis 模块的同等改造（当前文档显示已弃用）
- 对 username/user_id 双轨的全量迁移（D1.A）

---

## 附录 A：影响文件清单

### 新增文件

| 类型 | 路径 |
|------|------|
| Service | `backend/services/llm_template_resolver.py` |
| Service | `backend/services/feed_summary_personalization_service.py` |
| Task | `backend/tasks/feed_personalization_tasks.py` |
| API | `backend/api/subscription_content_summaries.py`（或合并到 subscription_content_tags.py） |
| Migration | `backend/database/migrations/000_baseline_llm_tables.sql` |
| Migration | `backend/database/migrations/001_processing_stats_llm_columns.sql` |
| Migration | `backend/database/migrations/010_add_module_code_to_templates.sql` |
| Migration | `backend/database/migrations/011_drop_twitter_report_user_config.sql` |
| Migration | `backend/database/migrations/020_create_subscription_content_user_summaries.sql` |
| Test | `backend/tests/test_llm_template_resolver.py` |
| Test | `backend/tests/test_local_llm_analysis_refactor.py` |
| Test | `backend/tests/test_feed_personalization.py` |
| Component | `web-frontend/src/components/feed/FeedRePersonalizeButton.tsx` |
| Doc | `docs/instructions/LLM_TEMPLATE_PREFERENCE_GUIDE.md` |

### 修改文件

| 文件 | 改动概要 |
|------|---------|
| `backend/services/local_llm_analysis_service.py` | 内部改调中央入口 |
| `backend/services/template_preference_service.py` | 扩 MODULE_REGISTRY |
| `backend/services/content_tagging_service.py` | 接受 template_id |
| `backend/services/feed_content_tagging_service.py` | 抽出 _build_text 复用 |
| `backend/tasks/tagging_tasks.py` / `naming_tasks.py` / `classify_tasks.py` | 接受 template_id + status 枚举统一 |
| `backend/tasks/file_tasks.py` | trigger_auto_* 增 template_id；R1/R2 修复 |
| `backend/api/files.py` | 5 个上传 API 增 auto_tag_template_id；PATCH content R2 修复 |
| `backend/api/file_tags.py` | retag API 增 template_id |
| `backend/api/subscriptions.py` | list_contents 联表 user_summaries |
| `backend/database/migrations/add_unread_count.sql` | 加迁移顺序守护 |
| `backend/database/db_manager.py` | 修补基线 CREATE 缺列 |
| `web-frontend/src/components/UploadAnalysisTemplatePicker.tsx` | 支持 moduleCode prop |
| `web-frontend/src/components/TemplateSelector.tsx` | 同上 |
| `web-frontend/src/services/files.ts` | 上传方法接受 auto_tag_* |
| `web-frontend/src/components/UploadFilesModal.tsx` 等 5 个弹窗 | 增加 file_tagging picker |
| `web-frontend/src/pages/FilesPage.tsx` | Re-tag 按钮 |
| `web-frontend/src/components/feed/FeedArticleBody.tsx` | Re-summarize 按钮 + 徽标 |

---

> 本文档为 v1.0 实施大纲。每个 PR 启动前应 review 对应章节，并根据生产数据库的实际状态调整 baseline migration 的字段细节。
