# Presets Management 新闻筛选逻辑说明（已移除 — 2026-05-31）

> ⚠️ **本功能已于 2026-05-31 随旧 News 模块一同移除。**
> - `user_news_filter_presets` 表已 DROP
> - `/api/user/news-presets` API 已删除
> - `NewsPage.tsx` 及相关 preset 组件已删除
> - 新闻内容已迁移至 Feeds 模块（`subscription_content` 表）
> 以下内容仅作历史参考。

---

## 原始说明（历史参考）

Presets Management 允许用户保存和管理个性化的新闻筛选条件，通过预设快速筛选新闻。本文档说明通过预设筛选新闻的完整流程、调用的服务和数据库表关系。

## 一、整体流程

### 1.1 前端流程

```
用户点击预设标签
    ↓
handleApplyPreset (NewsPage.tsx)
    ↓
将 FilterConfig 转换为 ThemeFilterState
    ↓
调用 loadFilteredNews(true, newFilters)
    ↓
构建请求参数，调用 themesAPI.filterNews(request)
    ↓
显示筛选结果
```

### 1.2 后端流程

```
POST /themes/filter/news (themes_unified.py)
    ↓
解析 ThemeFilterRequest
    ↓
构建多维度筛选子查询
    ↓
使用 INTERSECT 组合子查询（AND逻辑）
    ↓
执行SQL查询，返回筛选结果
```

## 二、服务调用链

### 2.1 前端服务

**文件**: `web-frontend/src/services/themes.ts` (通过 `themesAPI`)

**主要调用**:
- `themesAPI.filterNews(request)` - 发送筛选请求到后端

**请求参数** (`ThemeFilterRequest`):
```typescript
{
  categories?: string[];              // 主题分类
  event_types?: string[];            // 事件类型
  impact_scopes?: string[];          // 影响范围
  urgency_levels?: string[];         // 时效性
  related_sectors?: string[];        // 相关行业
  related_countries?: string[];      // 相关国家
  source_types?: string[];           // 来源类型
  scheduler_sources?: string[];     // 调度器来源
  ticker_symbols?: string[];         // Ticker符号列表
  use_watchlist?: boolean;          // 是否使用watchlist
  watchlist_groups?: string[];       // Watchlist分组
  ticker_filter_mode?: 'include' | 'exclude';  // 过滤模式
  dimension_operators?: DimensionOperators;    // 维度操作符
  search_text?: string;              // 搜索文本
  min_confidence?: number;           // 最低置信度
  only_primary?: boolean;            // 只显示主要主题
  market?: string;                   // 市场筛选
  start_date?: string;               // 开始日期
  end_date?: string;                 // 结束日期
  page?: number;                     // 页码
  page_size?: number;                // 每页数量
}
```

### 2.2 后端API

**文件**: `backend/api/themes_unified.py`

**端点**: `POST /themes/filter/news`

**处理函数**: `filter_news_by_themes(request: ThemeFilterRequest)`

**核心逻辑**:
1. 解析筛选条件，构建维度子查询
2. 根据 `dimension_operators` 决定维度内是 OR 还是 AND
3. 不同维度之间使用 AND 逻辑（通过 INTERSECT）
4. 执行查询并返回结果

## 三、数据库表结构

### 3.1 核心表

#### `user_news_filter_presets` - 预设表
存储用户保存的筛选预设配置。

**主要字段**:
- `id`: 预设ID
- `user_id`: 用户ID（系统预设为NULL）
- `preset_name`: 预设名称
- `preset_type`: 预设类型 ('system' | 'custom')
- `filter_config`: JSONB格式的筛选配置
- `scope_type`: 应用范围 ('global' | 'symbol_specific')
- `symbol_code`: 股票代码（仅symbol_specific时）
- `is_default`: 是否默认
- `is_favorite`: 是否收藏
- `usage_count`: 使用次数
- `last_used_at`: 最后使用时间

**索引**:
- `idx_user_presets_user_id`: 用户ID索引
- `idx_user_presets_filter_config`: GIN索引用于JSON查询

#### `news` - 新闻主表
存储新闻基本信息。

**主要字段**:
- `id`: 新闻ID
- `title`: 标题
- `content`: 内容
- `date`: 发布日期
- `source`: 来源
- `scheduler_source`: 调度器来源 ('eodhd_news', 'watchlist_news', 'cctv_news')
- `analysis_result`: AI分析结果（JSONB）
- `created_at`, `updated_at`: 时间戳

#### `news_themes_unified` - 统一主题表
整合所有来源的主题数据（EODHD、Vector Retrieval、LLM分类等）。

**主要字段**:
- `id`: 记录ID
- `news_id`: 新闻ID (FK → news.id)
- `theme_master_id`: 主题主数据ID (FK → theme_master_data.id)
- `theme_name`: 主题名称
- `canonical_theme_name`: 标准化主题名称
- `source_type`: 来源类型 ('eodhd_auto_tag', 'vector_retrieval', 'llm_classification', 'manual')
- `llm_classification_title`: LLM分类维度 ('event_type', 'impact_scope', 'urgency', 'related_sectors', 'related_countries')
- `confidence_score`: 置信度 (0-1)
- `quality_score`: 质量分数 (0-1)
- `is_primary`: 是否主要主题
- `is_active`: 是否激活
- `market`: 市场 (HK/US/CN/GLOBAL)

**索引**:
- `idx_news_themes_unified_news_id`: 新闻ID索引
- `idx_news_themes_unified_theme_master`: 主题主数据索引
- `idx_news_themes_unified_source_type`: 来源类型索引
- `idx_news_themes_unified_primary`: 主要主题索引

#### `theme_master_data` - 主题主数据表
管理标准化的主题名称和分类。

**主要字段**:
- `id`: 主数据ID
- `canonical_name`: 标准化名称（唯一）
- `display_name`: 显示名称
- `category`: 主题分类 ('Technology', 'Healthcare', 'Energy', 'Finance'等)
- `aliases`: 同义词数组（JSONB）
- `is_active`: 是否激活

#### `news_symbols` - 新闻股票关联表
关联新闻和股票代码。

**主要字段**:
- `news_id`: 新闻ID (FK → news.id)
- `symbol_id`: 股票代码ID (FK → news_symbols_dict.id)

#### `news_symbols_dict` - 股票代码字典表
存储股票代码信息。

**主要字段**:
- `id`: 股票代码ID
- `symbol_code`: 股票代码 (如 'AAPL.US', '0700.HK')

#### `target_tracking_config` - Watchlist表
存储用户的watchlist配置。

**主要字段**:
- `id`: 记录ID
- `user_id`: 用户ID (FK → users.id)
- `symbol`: 股票代码
- `group_name`: 分组名称
- `enabled`: 是否启用

### 3.2 表关系图

```
users
  ↓ (1:N)
user_news_filter_presets
  ↓ (filter_config JSONB)
  ↓ (应用筛选条件)
  ↓
news ←── news_themes_unified ──→ theme_master_data
  ↓                                    ↓
  ↓ (N:M)                          category (用于categories筛选)
  ↓
news_symbols ──→ news_symbols_dict
  ↓
  ↓ (ticker筛选)
  ↓
target_tracking_config (watchlist)
```

## 四、筛选逻辑详解

### 4.1 维度筛选

系统支持以下筛选维度：

1. **categories** (主题分类)
   - 来源: `theme_master_data.category`
   - 查询: JOIN `news_themes_unified` + `theme_master_data`

2. **event_types** (事件类型)
   - 来源: `news_themes_unified.llm_classification_title = 'event_type'`
   - 查询: `news_themes_unified.theme_name IN (...)`

3. **impact_scopes** (影响范围)
   - 来源: `news_themes_unified.llm_classification_title = 'impact_scope'`
   - 查询: `news_themes_unified.theme_name IN (...)`

4. **urgency_levels** (时效性)
   - 来源: `news_themes_unified.llm_classification_title = 'urgency'`
   - 查询: `news_themes_unified.theme_name IN (...)`

5. **related_sectors** (相关行业)
   - 来源: `news_themes_unified.llm_classification_title = 'related_sectors'`
   - 查询: `news_themes_unified.theme_name IN (...)`

6. **related_countries** (相关国家)
   - 来源: `news_themes_unified.llm_classification_title = 'related_countries'`
   - 查询: `news_themes_unified.theme_name IN (...)`

7. **source_types** (来源类型)
   - 来源: `news_themes_unified.source_type`
   - 查询: `news_themes_unified.source_type IN (...)`

8. **scheduler_sources** (调度器来源)
   - 来源: `news.scheduler_source`
   - 查询: `news.scheduler_source IN (...)`

### 4.2 维度操作符

每个维度内部支持 OR/AND 逻辑：

- **OR** (默认): 新闻包含任一指定值即可
  ```sql
  SELECT DISTINCT news_id FROM news_themes_unified
  WHERE theme_name IN ('value1', 'value2')
  ```

- **AND**: 新闻必须同时包含所有指定值
  ```sql
  SELECT news_id FROM (
    SELECT news_id, COUNT(DISTINCT theme_name) as match_count
    FROM news_themes_unified
    WHERE theme_name IN ('value1', 'value2')
    GROUP BY news_id
  ) sub WHERE match_count = 2
  ```

### 4.3 Ticker筛选

支持两种方式：

1. **手动指定Ticker列表**
   - 参数: `ticker_symbols: ['AAPL.US', '0700.HK']`
   - 查询: JOIN `news_symbols` + `news_symbols_dict`
   - 匹配: 使用 `LIKE` 进行模糊匹配（支持部分匹配）

2. **使用Watchlist**
   - 参数: `use_watchlist: true`, `watchlist_groups?: ['Technology']`
   - 查询: 从 `target_tracking_config` 获取用户的watchlist tickers
   - 然后与手动指定的tickers合并

**过滤模式**:
- `include` (默认): 只显示包含指定ticker的新闻
- `exclude`: 排除包含指定ticker的新闻

### 4.4 搜索文本

- 参数: `search_text: string`
- 查询: 在 `news.title`, `news.content`, `news_symbols_dict.symbol_code` 中搜索
- 使用: `ILIKE '%search_text%'` (不区分大小写)

### 4.5 其他筛选条件

- `min_confidence`: 最低置信度 (`news_themes_unified.confidence_score >= min_confidence`)
- `only_primary`: 只显示主要主题 (`news_themes_unified.is_primary = true`)
- `market`: 市场筛选 (`news_themes_unified.market = market`)
- `start_date`, `end_date`: 日期范围 (`news.date BETWEEN start_date AND end_date`)

### 4.6 查询组合逻辑

所有筛选条件通过 **INTERSECT** 组合，实现 AND 逻辑：

```sql
SELECT news_id FROM (categories子查询)
INTERSECT
SELECT news_id FROM (event_types子查询)
INTERSECT
SELECT news_id FROM (ticker子查询)
INTERSECT
SELECT news_id FROM (search子查询)
...
```

最终查询结构：

```sql
SELECT DISTINCT n.id, n.title, n.content, ...
FROM news n
INNER JOIN (组合后的子查询) AS filtered_news ON n.id = filtered_news.news_id
INNER JOIN news_themes_unified ntu ON n.id = ntu.news_id
WHERE ntu.is_active = true
  AND ntu.confidence_score >= min_confidence
  AND (其他基础条件)
ORDER BY n.date DESC
LIMIT page_size OFFSET offset
```

## 五、预设应用流程

### 5.1 加载预设

**前端**: `NewsPage.tsx` → `loadPresets()`
**API**: `GET /user/news-presets/?include_system=true`
**返回**: 预设列表（系统预设 + 用户自定义预设）

### 5.2 应用预设

1. 用户点击预设标签
2. `handleApplyPreset(preset)` 被调用
3. 将 `preset.filter_config` 转换为 `ThemeFilterState`
4. 更新状态并调用 `loadFilteredNews(true, newFilters)`
5. 如果不是系统预设，调用 `newsPresetsAPI.usePreset(preset.id)` 更新使用统计

### 5.3 预设配置结构

```typescript
FilterConfig {
  categories?: string[];
  event_types?: string[];
  impact_scopes?: string[];
  urgency?: string[];
  related_sectors?: string[];
  related_countries?: string[];
  source_types?: string[];
  scheduler_sources?: string[];
  ticker_symbols?: string[];
  use_watchlist?: boolean;
  watchlist_groups?: string[];
  ticker_filter_mode?: 'include' | 'exclude';
  dimension_operators?: {
    categories?: 'OR' | 'AND';
    event_types?: 'OR' | 'AND';
    // ... 其他维度
  };
  min_confidence?: number;
  only_primary?: boolean;
  market?: string;
}
```

## 六、关键代码位置

### 6.1 前端

- **预设管理组件**: `web-frontend/src/components/PresetsManager.tsx`
- **创建预设组件**: `web-frontend/src/components/CreatePresetModal.tsx`
- **筛选配置编辑器**: `web-frontend/src/components/FilterConfigEditor.tsx`
- **新闻页面**: `web-frontend/src/pages/NewsPage.tsx`
  - `handleApplyPreset()`: 应用预设
  - `loadFilteredNews()`: 加载筛选新闻
- **类型定义**: `web-frontend/src/types/newsPresets.ts`
- **API服务**: `web-frontend/src/services/newsPresets.ts`

### 6.2 后端

- **预设API**: `backend/api/user_news_presets.py`
  - `GET /user/news-presets/`: 获取预设列表
  - `POST /user/news-presets/`: 创建预设
  - `PUT /user/news-presets/{id}`: 更新预设
  - `DELETE /user/news-presets/{id}`: 删除预设
  - `POST /user/news-presets/{id}/use`: 更新使用统计
- **筛选API**: `backend/api/themes_unified.py`
  - `POST /themes/filter/news`: 多维度主题筛选新闻
- **Schema定义**: `backend/schemas/user_news_presets.py`

### 6.3 数据库

- **预设表创建**: `backend/database/migrations/create_user_news_filter_presets.sql`
- **统一主题系统**: `backend/database/migrations/create_unified_theme_system.sql`
- **系统预设支持**: `backend/database/migrations/add_system_preset_support.sql`

## 七、性能优化

1. **索引优化**:
   - `news_themes_unified` 表的多列索引
   - `user_news_filter_presets.filter_config` 的 GIN 索引
   - `news.date` 索引用于日期筛选

2. **查询优化**:
   - 使用子查询 + INTERSECT 避免复杂的 JOIN
   - 分页查询减少数据传输
   - 使用 DISTINCT 避免重复结果

3. **缓存策略**:
   - 预设列表可以缓存（用户级别）
   - 维度值列表可以缓存（全局）

## 八、注意事项

1. **预设作用域**:
   - `global`: 应用于所有新闻
   - `symbol_specific`: 仅应用于特定股票的新闻

2. **系统预设**:
   - 由管理员创建，所有用户可见
   - 存储在数据库，`is_system_global = true`
   - 用户不能编辑或删除系统预设

3. **维度操作符**:
   - 默认所有维度使用 OR 逻辑
   - 可以通过 `dimension_operators` 配置每个维度的逻辑

4. **Ticker匹配**:
   - 使用 `LIKE` 进行模糊匹配，支持部分匹配
   - 例如: 'AAPL' 可以匹配 'AAPL.US'

5. **日期范围**:
   - 预设不保存日期范围（每次应用时使用当前日期）
   - 用户可以在筛选面板中手动设置日期范围

## 九、示例

### 9.1 预设配置示例

```json
{
  "preset_name": "财报追踪",
  "filter_config": {
    "event_types": ["earnings_report", "financial_results"],
    "urgency": ["urgent", "timely"],
    "min_confidence": 0.7,
    "only_primary": true,
    "dimension_operators": {
      "event_types": "OR",
      "urgency": "OR"
    }
  },
  "scope_type": "global"
}
```

### 9.2 筛选请求示例

```json
{
  "categories": ["financial", "technology"],
  "event_types": ["earnings_report"],
  "min_confidence": 0.7,
  "only_primary": true,
  "use_watchlist": true,
  "watchlist_groups": ["Technology"],
  "ticker_filter_mode": "include",
  "dimension_operators": {
    "categories": "OR"
  },
  "page": 1,
  "page_size": 20
}
```

---

**文档版本**: 1.0  
**最后更新**: 2025-12-05  
**维护者**: RC Project Team

