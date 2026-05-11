# 调度器开发流程指南

> 本文档总结了在本系统中开发新调度器任务的完整流程和注意事项，以 Futu Themes 调度器为实例。

## 系统架构概述

本系统采用**独立调度器服务**架构：
- **主 Web 应用** (端口 8000): 提供前端页面和 API 代理
- **独立调度器服务** (端口 8001): 负责任务调度和执行
- **通信方式**: 主应用通过 HTTP 代理转发请求到调度器服务

```
前端 → 主应用 API (scheduler_proxy) → 调度器服务 API (scheduler) → 任务执行
```

---

## ⚠️ 严重警告：禁止两台机器的调度器同时连接同一个数据库

> **这是导致任务重复执行最常见的根本原因，必须在部署前理解清楚。**

### 问题描述

本系统使用**共享 PostgreSQL 数据库**（`DB_HOST` 指向同一台远程服务器）。如果在不同机器上同时运行多个 Scheduler 进程（例如本地 Mac Mini + 远程服务器），且它们连接的是**同一个数据库**，则：

| 机器 | Scheduler | 数据库 | 是否互相感知 |
|------|-----------|--------|-------------|
| 本地 Mac Mini | ✅ 运行 | `43.99.37.76:5432`（共享） | ❌ **不互相感知** |
| 远程 43.99.37.76 | ✅ 运行 | `43.99.37.76:5432`（共享） | ❌ **不互相感知** |

两个 Scheduler 会在**同一 cron 时刻各自独立触发同一任务**，在 `task_execution_logs` 产生两条执行记录，提交两个独立的 Celery Task ID。

### 真实案例

```
# task_execution_logs 里同一时间出现两条 youtube_subscription 执行记录：
13:00:00  youtube_subscription  "已提交 Celery 异步任务 e4d28967..."  ← 来自远程服务器
13:00:01  youtube_subscription  "已提交 Celery 异步任务 ef1dd933..."  ← 来自本地 Mac Mini
```

`e4d28967` 在本地日志中完全找不到，就是因为它由远程机器的 Scheduler 提交。

### 部署规则

```
✅ 允许：只有一台机器的 Scheduler 进程运行，其他机器只跑 Celery Worker
❌ 禁止：多台机器同时运行 Scheduler 且连接同一个 PostgreSQL 数据库
```

**如果在本地开发测试：必须将远程服务器的 Scheduler 服务进程完全停止（kill 进程），
而不仅仅是"禁用调度"或"关闭 APScheduler"。**

---

## 并发与幂等保护（重要）

调度器在生产环境必须**只跑一个进程**。但为了防止「双 scheduler 实例 / lifespan 二次启动 / 手动 trigger 与 cron 撞车 / 多机器部署误操作」等情况悄悄放大，
`backend/services/scheduler_service.py::_execute_task` 在每次执行前都会通过 **PostgreSQL 数据库周期窗口锁**来保证幂等性。

### 锁机制：PostgreSQL INSERT ON CONFLICT（跨机器分布式锁）

锁不再依赖 Redis（各机器的 Redis 是本地独立的，无法跨机器协调），改为利用**所有调度实例共享的同一个 PostgreSQL 数据库**来实现真正的分布式互斥：

**原理**：`task_execution_logs` 表上有唯一索引：
```sql
CREATE UNIQUE INDEX uq_task_execution_period
  ON task_execution_logs (task_type, period_key)
  WHERE period_key IS NOT NULL;
```

`_execute_task` 在执行任务时执行：
```sql
INSERT INTO task_execution_logs (..., period_key)
VALUES (..., 'cron:202605111300')
ON CONFLICT (task_type, period_key) WHERE period_key IS NOT NULL DO NOTHING
RETURNING id;
```

- **RETURNING id 返回一行** → 当前实例获得锁，继续执行
- **RETURNING id 返回空（冲突）** → 另一实例已在此周期执行，当前实例跳过，写入一条 `status='skipped'` 日志

| 触发类型 | period_key 格式 |
|----------|-----------------|
| `cron`     | `cron:YYYYMMDDHHMM`（当前 cron fire_time） |
| `interval` | `int:{floor(epoch / 周期秒)}`（时间桶编号） |
| 拿不到 trigger 时 | `None`（退化为无锁保护，不影响执行） |

### 关键性质

- **跨机器生效**：所有调度实例共享同一个 PostgreSQL，DB 唯一约束天然实现跨机器互斥，彻底解决多机器重复执行问题。
- **同一个周期窗口内只允许成功执行一次**：第二次触发冲突后直接写一条 `status='skipped'`，便于在监控/SQL 中排查「重复触发」。
- **下一周期自动解锁**：下一次 cron tick 使用新的 `period_key`，自然进入新窗口，无需手动释放锁。
- **任务失败也保留锁**：避免立即重试浪费外部 API 配额；下一次正常 cron tick 进入新窗口后会自动重试。
- **对开发者透明**：只要新任务通过标准 `_add_job_to_scheduler` 注册（Cron/Interval Trigger），就自动享受这一保护。

### 排查重复触发

```sql
-- 查看某任务的执行分布
SELECT date_trunc('hour', started_at), status, COUNT(*)
FROM task_execution_logs
WHERE task_type = 'youtube_subscription'
GROUP BY 1, 2
ORDER BY 1 DESC;

-- 同一 period_key 下如果同时出现 success + skipped，说明有重复触发
-- skipped 记录的 error_message 里会记录触发的 period_key 值
SELECT started_at, status, error_message
FROM task_execution_logs
WHERE task_type = 'youtube_subscription'
ORDER BY started_at DESC LIMIT 20;
```

> 如果看到同一时间窗里同时出现 `success` 和 `skipped`，说明上游有重复触发源（通常是双 Scheduler 进程），**任务本身是安全的**，但应回到部署层面消除冗余进程（参见上方「严重警告」章节）。

此外，`SchedulerService.start()` 与 `get_scheduler_service()` 都加了幂等/并发保护，单进程内不会因 lifespan 被多次调用而把同一任务 add_job 多次。

---

## 开发流程总览

开发一个新的调度器任务需要以下步骤：

### 阶段 1: 数据库设计 (Backend)
### 阶段 2: 业务逻辑实现 (Backend)
### 阶段 3: 调度器集成 (Backend)
### 阶段 4: API 接口开发 (Backend)
### 阶段 5: API 代理配置 (Backend)
### 阶段 6: 前端界面开发 (Frontend)

---

## 详细步骤

### 阶段 1: 数据库设计

#### 1.1 创建数据表 SQL 脚本

**文件路径**: `backend/database/migrations/create_[feature]_tables.sql`

```sql
-- 示例：Futu Themes 数据表
CREATE TABLE IF NOT EXISTS futu_investment_themes (
    id SERIAL PRIMARY KEY,
    theme_id VARCHAR(50) NOT NULL,
    theme_name VARCHAR(200) NOT NULL,
    market VARCHAR(10) NOT NULL,
    -- 其他字段...
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_theme_market_date UNIQUE(theme_id, market, scraped_at)
);

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_futu_themes_market ON futu_investment_themes(market);
```

#### 1.2 更新数据库管理器

**文件**: `backend/database/db_manager.py`

需要修改两处：

**A. 添加表创建方法**
```python
def _create_futu_themes_tables(self, conn):
    """创建Futu投资主题相关表"""
    # 读取并执行 SQL 脚本
    migration_file = os.path.join(
        os.path.dirname(__file__), 
        'migrations/create_futu_themes_tables.sql'
    )
    with open(migration_file, 'r', encoding='utf-8') as f:
        sql = f.read()
    cursor = conn.cursor()
    cursor.execute(sql)
```

**B. 在 `ensure_schema` 中调用**
```python
def ensure_schema(self):
    conn = self.get_connection()
    # ... 其他表创建 ...
    self._create_futu_themes_tables(conn)  # ← 新增
    conn.commit()
```

**C. 更新任务类型约束**

在 `_create_scheduler_tables` 和 `_update_task_scheduling_constraints` 方法中：
```python
CONSTRAINT valid_task_type CHECK (type IN (
    'eodhd_news', 
    'watchlist_news', 
    'economic_events', 
    'macro_indicators', 
    'eodhd_fundamentals', 
    'finviz_screener', 
    'news_analysis', 
    'futu_themes',  # ← 新增
    'eodhd_stock_prices', 
    'eodhd_eod_stock_prices'
)),
```

---

### 阶段 2: 业务逻辑实现

#### 2.1 创建服务类

**文件**: `backend/services/[feature]_service.py`

```python
class FutuThemesService:
    """Futu投资主题数据服务"""
    
    def __init__(self):
        self.db_manager = DatabaseManager()
        self.logger = get_logger("futu_themes_service")
    
    def fetch_and_store_themes(self, 
                               markets: Optional[List[str]] = None,
                               max_pages: Optional[int] = None,
                               scrape_stocks: bool = True) -> Dict[str, Any]:
        """
        主要业务逻辑方法
        
        返回格式：
        {
            'success': True/False,
            'message': '执行结果消息',
            'data': {...}  # 可选的返回数据
        }
        """
        try:
            # 实现业务逻辑
            # 1. 获取数据
            # 2. 处理数据
            # 3. 存储到数据库
            
            return {
                'success': True,
                'message': f'成功处理 {total_count} 条记录',
                'data': {'count': total_count}
            }
        except Exception as e:
            self.logger.error(f"执行失败: {str(e)}")
            return {
                'success': False,
                'message': f'执行失败: {str(e)}'
            }

# 创建服务实例的工厂函数
def create_futu_themes_service() -> FutuThemesService:
    """创建服务实例"""
    return FutuThemesService()
```

**注意事项**：
- 所有数据库操作要有事务管理
- 添加详细的日志记录
- 异常处理要完整
- 返回格式统一为 `{'success': bool, 'message': str, 'data': dict}`

---

### 阶段 3: 调度器集成

#### 3.1 注册任务处理器

**文件**: `backend/services/scheduler_service.py`

**A. 在 `__init__` 方法中注册**
```python
def __init__(self):
    # ... 其他初始化 ...
    self.task_handlers = {
        'eodhd_news': self._execute_eodhd_news_task,
        'watchlist_news': self._execute_watchlist_news_task,
        'futu_themes': self._execute_futu_themes_task,  # ← 新增
        # ... 其他处理器 ...
    }
```

**B. 实现任务执行方法**
```python
async def _execute_futu_themes_task(self, params: Dict[str, Any]) -> Dict[str, Any]:
    """执行Futu投资主题数据获取任务"""
    try:
        logger.info("开始执行富途投资主题数据获取任务", params=params)
        
        # 导入服务
        from backend.services.futu_themes_service import create_futu_themes_service
        futu_service = create_futu_themes_service()
        
        # 提取参数
        markets = params.get('markets')
        max_pages = params.get('max_pages')
        scrape_stocks = params.get('scrape_stocks', True)
        
        # 关键：在线程池中执行同步方法（避免阻塞事件循环）
        import asyncio
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor() as executor:
            result = await asyncio.get_event_loop().run_in_executor(
                executor,
                futu_service.fetch_and_store_themes,
                markets,
                max_pages,
                scrape_stocks
            )
        
        if result['success']:
            message = f"✅ {result['message']}"
            logger.info("任务执行成功", message=message)
            return {
                'success': True,
                'message': message,
                'data': result.get('data')
            }
        else:
            logger.error("任务执行失败", error=result['message'])
            return {
                'success': False,
                'message': f"❌ {result['message']}"
            }
            
    except Exception as e:
        error_msg = f"任务执行异常: {str(e)}"
        logger.error(error_msg, exception=str(e))
        return {
            'success': False,
            'message': f"❌ {error_msg}",
            'data': None
        }
```

**⚠️ 重要注意事项**：
- 如果服务方法是**同步**的（不是 async），必须使用 `run_in_executor` 在线程池中执行
- 避免阻塞 asyncio 事件循环
- 长时间运行的任务（如网络爬虫）建议使用线程池

---

### 阶段 4: API 接口开发

#### 4.1 定义请求模型

**文件**: `backend/api/scheduler.py`

```python
from pydantic import BaseModel, Field
from typing import Optional, List

class CreateFutuThemesTaskRequest(BaseModel):
    """创建Futu投资主题定时任务请求模型"""
    name: str = Field(default="Futu Themes Auto Fetch", description="任务名称")
    cron_expression: str = Field(default="0 2 * * *", description="Cron表达式")
    timezone: str = Field(default="Asia/Hong_Kong", description="时区")
    enabled: bool = Field(default=True, description="是否启用")
    
    # 任务特定参数
    markets: Optional[List[str]] = Field(default=None, description="市场列表")
    max_pages: Optional[int] = Field(default=None, description="最大页数", ge=1, le=50)
    scrape_stocks: bool = Field(default=True, description="是否爬取股票")
```

#### 4.2 创建 API 端点

```python
@router.post("/tasks/futu-themes", summary="创建Futu投资主题定时任务")
async def create_futu_themes_task(
    request: CreateFutuThemesTaskRequest,
    current_user=Depends(get_current_user)  # JWT 认证
):
    """创建任务的 API 端点"""
    try:
        logger.info("创建任务请求", 
                   user=current_user.username,
                   task_name=request.name)
        
        scheduler_service = await get_scheduler_service()
        
        # 构建任务参数
        task_params = {
            'markets': request.markets,
            'max_pages': request.max_pages,
            'scrape_stocks': request.scrape_stocks
        }
        
        # 创建任务
        result = await scheduler_service.create_cron_task(
            task_name=request.name,
            task_type="futu_themes",  # 任务类型标识
            cron_expression=request.cron_expression,
            timezone=request.timezone,
            enabled=request.enabled,
            task_params=task_params,
            created_by=current_user.username
        )
        
        if result['success']:
            return {
                "success": True,
                "message": result['message'],
                "data": {
                    "task_id": result['task_id'],
                    "task_config": result['task_config']
                }
            }
        else:
            raise HTTPException(status_code=400, detail=result.get('message'))
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("创建任务失败", exception=str(e))
        raise HTTPException(status_code=500, detail=f"创建失败: {str(e)}")
```

**4.3 更新 Cron 任务请求模型**

在 `CreateCronTaskRequest` 中更新 `task_type` 的验证模式：
```python
class CreateCronTaskRequest(BaseModel):
    name: str = Field(..., description="任务名称")
    task_type: str = Field(
        ..., 
        description="任务类型",
        pattern="^(eodhd_news|watchlist_news|economic_events|macro_indicators|eodhd_fundamentals|finviz_screener|news_analysis|futu_themes|eodhd_stock_prices|eodhd_eod_stock_prices)$"
        # ↑ 在这里添加新任务类型
    )
```

---

### 阶段 5: API 代理配置

#### 5.1 添加代理路由

**文件**: `backend/api/scheduler_proxy.py`

```python
@router.post("/tasks/futu-themes", summary="创建Futu投资主题定时任务")
async def create_futu_themes_task_proxy(request: Request, current_user=Depends(get_current_user)):
    """代理创建Futu投资主题定时任务"""
    try:
        auth_headers = get_auth_headers(request)
        json_data = await request.json()
        
        # 转发到调度器服务
        result = await scheduler_client.post(
            "/scheduler/tasks/futu-themes", 
            json_data=json_data, 
            headers=auth_headers
        )
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500, 
            detail=f"创建Futu投资主题任务代理失败: {str(e)}"
        )

@router.post("/tasks/futu-themes/default", summary="创建默认Futu投资主题定时任务")
async def create_default_futu_themes_task_proxy(request: Request, current_user=Depends(get_current_user)):
    """代理创建默认任务（可选）"""
    try:
        auth_headers = get_auth_headers(request)
        result = await scheduler_client.post(
            "/scheduler/tasks/futu-themes/default", 
            headers=auth_headers
        )
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500, 
            detail=f"创建默认Futu投资主题任务代理失败: {str(e)}"
        )
```

**⚠️ 关键点**：
- 代理路由必须与调度器服务的路由一致
- 必须转发认证头（JWT token）
- 路径格式：`/tasks/{task-type}` 和 `/tasks/{task-type}/default`

---

### 阶段 6: 前端界面开发

#### 6.1 更新服务层类型定义

**文件**: `web-frontend/src/services/scheduler.ts`

**A. 更新 ScheduledTask 接口**
```typescript
export interface ScheduledTask {
  id: string;
  name: string;
  type: 'eodhd_news' | 'watchlist_news' | 'economic_events' | 
        'macro_indicators' | 'eodhd_fundamentals' | 'finviz_screener' | 
        'eodhd_stock_prices' | 'futu_themes';  // ← 添加新类型
  schedule_type: 'cron' | 'interval';
  // ... 其他字段
}
```

**B. 定义请求参数接口**
```typescript
export interface CreateFutuThemesTaskParams {
  name?: string;
  cron_expression?: string;
  timezone?: string;
  enabled?: boolean;
  markets?: string[];
  max_pages?: number;
  scrape_stocks?: boolean;
}
```

**C. 添加 API 方法**
```typescript
export class SchedulerAPI {
  // ... 其他方法 ...
  
  async createFutuThemesTask(params: CreateFutuThemesTaskParams): Promise<TaskCreateResponse> {
    try {
      const response = await api.post('/scheduler/tasks/futu-themes', params);
      return {
        success: true,
        message: response.data?.message || '创建Futu投资主题定时任务成功',
        data: response.data?.data
      };
    } catch (error: any) {
      console.error('创建Futu投资主题定时任务失败:', error);
      return {
        success: false,
        message: error.response?.data?.detail || error.message || '创建失败'
      };
    }
  }
  
  async createDefaultFutuThemesTask(): Promise<TaskCreateResponse> {
    try {
      const response = await api.post('/scheduler/tasks/futu-themes/default');
      return {
        success: true,
        message: response.data?.message || '创建默认任务成功',
        data: response.data?.data
      };
    } catch (error: any) {
      console.error('创建默认任务失败:', error);
      return {
        success: false,
        message: error.response?.data?.detail || error.message || '创建失败'
      };
    }
  }
}
```

---

#### 6.2 添加数据源配置

**文件**: `web-frontend/src/pages/SchedulerPage.tsx`

**A. 更新 DataSource 接口**
```typescript
interface DataSource {
  id: string;
  name: string;
  type: 'eodhd' | 'watchlist' | 'alpha_vantage' | 'finviz' | 
        'finviz_screener' | 'eodhd_events' | 'eodhd_macro' | 
        'eodhd_fundamentals' | 'eodhd_stock_prices' | 
        'eodhd_eod_stock_prices' | 'futu_themes' | 'other';  // ← 添加
  // ... 其他字段
}
```

**B. 在 dataSources 数组中添加配置**
```typescript
const [dataSources, setDataSources] = useState<DataSource[]>([
  // ... 其他数据源 ...
  {
    id: 'futu_themes',
    name: 'Futu Investment Themes',
    type: 'futu_themes',
    description: 'Get investment themes and related stocks from Futu website (HK and US markets)',
    enabled: true,
    status: 'idle',
    quickActions: [
      {
        id: 'configure_futu_themes_schedule',
        name: 'Configure Futu Themes Fetch',
        description: 'Set up periodic investment themes fetching from Futu (daily at 2 AM HKT)',
        endpoint: '/scheduler/tasks/futu-themes',
        params: {},
        icon: <GlobalOutlined />
      }
    ]
  },
]);
```

**C. 添加到配置支持列表**
```typescript
const showQuickActionModal = (action: QuickAction, dataSource: DataSource) => {
  if ((action.id.includes('configure') && action.id.includes('schedule')) || 
      action.id === 'update_fundamentals_data') {
    
    if (dataSource.id === 'eodhd_news' || 
        dataSource.id === 'watchlist_news' || 
        dataSource.id === 'eodhd_events' || 
        dataSource.id === 'eodhd_macro' || 
        dataSource.id === 'eodhd_fundamentals' || 
        dataSource.id === 'finviz_screener' || 
        dataSource.id === 'eodhd_stock_prices' || 
        dataSource.id === 'eodhd_eod_stock_prices' ||
        dataSource.id === 'futu_themes') {  // ← 添加
      
      setSelectedDataSource(dataSource);
      setDataSourceConfigVisible(true);
    }
  }
};
```

**D. 更新类型断言**
```typescript
<DataSourceConfigModal
  open={dataSourceConfigVisible}
  onCancel={() => {
    setDataSourceConfigVisible(false);
    setSelectedDataSource(null);
  }}
  onSuccess={handleDataSourceConfigSuccess}
  dataSourceType={selectedDataSource.id as 
    'eodhd_news' | 'watchlist_news' | 'eodhd_events' | 
    'eodhd_macro' | 'eodhd_fundamentals' | 'finviz_screener' | 
    'eodhd_stock_prices' | 'eodhd_eod_stock_prices' | 'futu_themes'}  // ← 添加
  dataSourceName={selectedDataSource.name}
/>
```

---

#### 6.3 配置模态框实现

**文件**: `web-frontend/src/components/DataSourceConfigModal.tsx`

**A. 更新接口定义**
```typescript
interface DataSourceConfigModalProps {
  open: boolean;
  onCancel: () => void;
  onSuccess: (taskId: string) => void;
  dataSourceType: 'eodhd_news' | 'watchlist_news' | 'eodhd_events' | 
                  'eodhd_macro' | 'eodhd_fundamentals' | 'finviz_screener' | 
                  'eodhd_stock_prices' | 'eodhd_eod_stock_prices' | 'futu_themes';  // ← 添加
  dataSourceName: string;
}
```

**B. 添加数据源配置**
```typescript
const dataSourceConfigs = {
  // ... 其他配置 ...
  futu_themes: {
    icon: <DatabaseOutlined style={{ color: '#eb2f96' }} />,
    params: ['markets', 'max_pages', 'scrape_stocks'],
    defaultSchedule: '0 2 * * *',
    defaultName: 'Futu Themes Auto Fetch',
    description: 'Get investment themes and related stocks from Futu website (HK and US markets, daily at 2 AM HKT)'
  }
};
```

**C. 实现参数表单**
```typescript
const renderTaskParamsStep = () => {
  const renderParamFields = () => {
    switch (dataSourceType) {
      // ... 其他 case ...
      
      case 'futu_themes':
        return (
          <>
            <Alert
              message="Futu Investment Themes Scraper"
              type="info"
              showIcon
              style={{ marginBottom: 16 }}
              description="Automatically scrape investment themes and related stocks from Futu website. Supports HK and US markets with daily full updates."
            />
            
            <Form.Item
              label="Markets"
              name={['taskParams', 'markets']}
              extra="Select markets to scrape. Leave empty to scrape all available markets (HK, US)."
            >
              <Select 
                mode="multiple"
                placeholder="Select markets (leave empty for all)"
                allowClear
              >
                <Option value="HK">🇭🇰 Hong Kong (HK)</Option>
                <Option value="US">🇺🇸 United States (US)</Option>
              </Select>
            </Form.Item>
            
            <Form.Item
              label="Max Pages Per Market"
              name={['taskParams', 'max_pages']}
              extra="Maximum number of pages to scrape per market (1-50). Leave empty to scrape all pages."
            >
              <InputNumber 
                min={1} 
                max={50} 
                placeholder="Leave empty for all pages" 
                style={{ width: '100%' }}
              />
            </Form.Item>
            
            <Form.Item
              label="Scrape Stocks"
              name={['taskParams', 'scrape_stocks']}
              valuePropName="checked"
              extra="Enable to scrape detailed stock list for each theme. Disable to only fetch theme metadata."
              initialValue={true}
            >
              <Switch 
                checkedChildren="Enabled" 
                unCheckedChildren="Disabled" 
              />
            </Form.Item>
          </>
        );
        
      default:
        return null;
    }
  };
  
  return (
    <div>
      {/* ... 标题和描述 ... */}
      {renderParamFields()}
      {/* ... 提示信息 ... */}
    </div>
  );
};
```

**D. 实现提交逻辑**
```typescript
const handleSubmit = async (data: Partial<ConfigFormData>) => {
  try {
    setLoading(true);
    
    let result;
    
    // ... 其他任务类型处理 ...
    
    if (dataSourceType === 'futu_themes') {
      // Futu投资主题任务的专用创建逻辑
      const futuThemesParams = {
        name: currentConfig.defaultName,
        cron_expression: data.cronExpression!,
        timezone: data.timezone!,
        enabled: true,
        markets: data.taskParams?.markets,
        max_pages: data.taskParams?.max_pages,
        scrape_stocks: data.taskParams?.scrape_stocks !== undefined ? 
                      data.taskParams.scrape_stocks : true
      };
      result = await schedulerAPI.createFutuThemesTask(futuThemesParams);
    }
    
    // ... 结果处理 ...
    
  } catch (error) {
    // ... 错误处理 ...
  } finally {
    setLoading(false);
  }
};
```

---

## 常见问题和注意事项

### 1. "Method Not Allowed" 错误

**原因**: 缺少 API 代理路由

**解决方案**: 
- 确保在 `backend/api/scheduler_proxy.py` 中添加了对应的代理路由
- 路由路径必须与调度器服务的路由一致
- 记得重启主 Web 应用服务

### 2. 任务执行阻塞问题

**原因**: 同步方法在异步环境中阻塞了事件循环

**解决方案**:
```python
# ✅ 正确：使用线程池
import asyncio
import concurrent.futures

with concurrent.futures.ThreadPoolExecutor() as executor:
    result = await asyncio.get_event_loop().run_in_executor(
        executor,
        sync_method,  # 同步方法
        arg1,
        arg2
    )

# ❌ 错误：直接调用同步方法
result = sync_method(arg1, arg2)  # 会阻塞事件循环
```

### 3. JWT 认证失败

**原因**: 
- 代理路由未转发认证头
- 前端未正确发送 token

**解决方案**:
```python
# 在代理路由中
auth_headers = get_auth_headers(request)
result = await scheduler_client.post(endpoint, json_data=json_data, headers=auth_headers)
```

### 4. 数据库约束冲突

**原因**: 未更新 `valid_task_type` 约束

**解决方案**:
- 在 `db_manager.py` 的两个方法中都要更新
  - `_create_scheduler_tables`
  - `_update_task_scheduling_constraints`

### 5. 前端类型错误

**原因**: TypeScript 类型定义不一致

**解决方案**:
- 确保所有涉及任务类型的地方都添加了新类型
- 使用相同的顺序定义联合类型
- 检查：
  - `ScheduledTask` 接口
  - `DataSource` 接口
  - `DataSourceConfigModalProps` 接口
  - 类型断言位置

---

## 测试验证清单

### 后端测试

- [ ] 数据库表创建成功
  ```bash
  # 连接数据库检查
  psql -U your_user -d your_db
  \dt  # 列出所有表
  \d table_name  # 查看表结构
  ```

- [ ] 服务方法单独运行正常
  ```python
  # 创建测试脚本
  from backend.services.futu_themes_service import create_futu_themes_service
  service = create_futu_themes_service()
  result = service.fetch_and_store_themes()
  print(result)
  ```

- [ ] API 端点响应正常
  ```bash
  # 使用 curl 测试
  curl -X POST "http://localhost:8001/api/scheduler/tasks/futu-themes" \
       -H "Authorization: Bearer YOUR_TOKEN" \
       -H "Content-Type: application/json" \
       -d '{"name": "Test Task"}'
  ```

### 前端测试

- [ ] TypeScript 编译无错误
  ```bash
  cd web-frontend
  npm run build
  ```

- [ ] 配置模态框显示正常
- [ ] 表单验证工作正常
- [ ] 任务创建成功
- [ ] 任务在列表中显示

### 集成测试

- [ ] 端到端创建任务流程
- [ ] 任务按计划执行
- [ ] 执行日志正确记录
- [ ] 错误处理符合预期

---

## 开发工具和命令

### 启动服务

```bash
# 开发模式
cd /Users/kevinliu/rcproject/development

# 启动主应用
./start_webapp_dev.sh

# 启动调度器服务
./start_scheduler_dev.sh

# 启动前端
./start_frontend_dev.sh
```

### 查看日志

```bash
# 主应用日志
tail -f logs/app.log

# 调度器日志
tail -f scheduler/logs/*.log

# Celery 日志（如果使用）
tail -f logs/celery_worker.log
```

### 数据库操作

```bash
# 连接数据库
psql -U your_user -d your_db

# 查看任务
SELECT id, name, type, enabled, next_run FROM task_scheduling;

# 查看执行日志
SELECT * FROM task_execution_logs ORDER BY started_at DESC LIMIT 10;

# 重置任务（谨慎使用）
DELETE FROM task_scheduling WHERE type = 'futu_themes';
```

---

## 代码审查要点

在提交代码前，请检查：

### 代码质量
- [ ] 所有函数都有文档字符串
- [ ] 日志记录完整（info、error 级别）
- [ ] 异常处理覆盖完整
- [ ] 没有硬编码的配置（使用环境变量或配置文件）

### 数据库
- [ ] 表有适当的索引
- [ ] 字段类型合理
- [ ] 唯一约束正确
- [ ] 外键关系清晰

### API
- [ ] 端点有清晰的文档
- [ ] 请求/响应模型定义完整
- [ ] 认证和授权正确实现
- [ ] 错误响应格式统一

### 前端
- [ ] TypeScript 类型完整
- [ ] 组件可复用性好
- [ ] 用户体验友好（加载状态、错误提示）
- [ ] 表单验证完整

---

## 文档要求

为新功能创建以下文档：

1. **功能说明文档** (如 `FUTU_THEMES_SCHEDULER.md`)
   - 功能概述
   - 使用方法
   - API 参考
   - 配置说明

2. **快速入门文档** (如 `FUTU_THEMES_QUICKSTART.md`)
   - 5 分钟上手指南
   - 常见使用场景
   - 故障排查

3. **数据库模式文档**
   - 表结构说明
   - 字段含义
   - 关系图

---

## 总结

开发新调度器的核心步骤：
1. **数据库** → 2. **服务** → 3. **调度器** → 4. **API** → 5. **代理** → 6. **前端**

关键注意事项：
- ⚠️ **单调度器原则**：同一个 PostgreSQL 数据库在任意时刻只能有一个 Scheduler 进程在运行，否则同一任务会被重复执行
- ✅ 独立调度器服务架构：需要同时配置 `scheduler.py` 和 `scheduler_proxy.py`
- ✅ 线程池执行：同步方法必须用 `run_in_executor`
- ✅ 类型一致性：前后端类型定义要对应
- ✅ 认证转发：代理必须转发 JWT token
- ✅ 完整测试：后端、前端、集成都要测试

---

## 相关文档

- [Futu Themes Scheduler 功能文档](./FUTU_THEMES_SCHEDULER.md)
- [Futu Themes 快速入门](./FUTU_THEMES_QUICKSTART.md)
- [APScheduler 官方文档](https://apscheduler.readthedocs.io/)

---

**版本**: 1.1  
**最后更新**: 2026-05-11  
**作者**: Development Team  
**示例项目**: Futu Investment Themes Scheduler  
**变更记录**: v1.1 — 将周期锁从 Redis 改为 PostgreSQL INSERT ON CONFLICT，新增"双调度器陷阱"警告章节

