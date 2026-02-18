# RC Project 服务器部署问题排查与修复记录

> 记录日期：2026-02-18  
> 服务器：111.230.48.119  
> 排查环境：本地代码正常，远程服务器异常

---

## 问题一：上传文件 / Scheduler 均报 "Authentication required."

### 现象
- 上传文档文件时，前端提示：`上传文档文件失败: Authentication required.`
- Scheduler 执行定时任务时，同样出现 `Authentication required.` 错误

### 根本原因
服务器上的 Redis 开启了 `requirepass` 密码保护，但项目 `.env` 文件中未配置 `REDIS_URL`，导致代码使用无密码的默认地址 `redis://localhost:6379/0` 连接 Redis，被 Redis 拒绝并返回 `AuthenticationError: Authentication required.`

**影响链路：**
| 场景 | 触发点 | 报错来源 |
|------|--------|---------|
| 上传文件 | `apply_async()` 提交 Celery 任务时连接 Redis Broker | `redis.AuthenticationError` |
| Scheduler 执行任务 | `redis_client.set(lock_key, ...)` 获取分布式锁 | 同上 |

### 修复方案
在 `.env` 文件中新增 `REDIS_URL` 配置项，格式如下：

```env
# Redis密码格式：redis://:密码@localhost:6379/0
REDIS_URL=redis://:YOUR_PASSWORD@localhost:6379/0
```

> ⚠️ 注意：密码前必须有冒号 `:`（无用户名格式）

---

## 问题二：文件上传任务一直卡在 0%，不执行

### 现象
- 文件上传后，后端日志显示任务已提交（`task_id=...`）
- Celery Worker 均已连接 Redis ✅
- 但任务状态永远停留在 0%，没有任何进展

### 根本原因
`rc-celery-ai` systemd 服务的 `--queues` 参数只配置了 `ai_processing`，遗漏了 `file_processing` 队列。文件上传任务被路由到 `file_processing` 队列后无人消费，永远挂起。

**任务路由配置（`backend/celery_config.py`）：**
```python
'backend.tasks.file_tasks.*': {'queue': 'file_processing'},   # 文件处理
'backend.tasks.news_tasks.*': {'queue': 'ai_processing'},      # AI分析
'backend.tasks.data_tasks.*': {'queue': 'data_fetching'},      # 数据获取
```

**服务器上的错误配置：**
```ini
# /etc/systemd/system/rc-celery-ai.service
--queues=ai_processing        # ❌ 缺少 file_processing
```

### 修复方案

```bash
# 修改 service 文件
sudo sed -i 's/--queues=ai_processing/--queues=file_processing,ai_processing/' \
    /etc/systemd/system/rc-celery-ai.service

# 重新加载并重启
sudo systemctl daemon-reload && sudo systemctl restart rc-celery-ai
```

---

## 问题三：Scheduler Proxy 部分路由未转发认证 Token

### 现象
任务日志、执行统计等接口在调用时返回 401/403 错误。

### 根本原因
`backend/api/scheduler_proxy.py` 中多个代理路由在验证了前端用户身份后，向 Scheduler 子服务转发请求时遗漏了 `Authorization` header。

**受影响的路由：**
- `scheduler_status_proxy`
- `get_task_logs_proxy`
- `get_all_execution_logs_proxy`
- `get_execution_summary_proxy`
- `download_execution_logs_csv_proxy`
- `get_table_structure_proxy`

### 修复方案
为以上路由添加 `request: Request` 参数，并在请求中附带 `auth_headers`：

```python
# 修复前
async def get_task_logs_proxy(task_id: str, ...):
    result = await scheduler_client.get(f"/scheduler/tasks/{task_id}/logs", params=params)

# 修复后
async def get_task_logs_proxy(request: Request, task_id: str, ...):
    auth_headers = get_auth_headers(request)
    result = await scheduler_client.get(f"/scheduler/tasks/{task_id}/logs", params=params, headers=auth_headers)
```

---

## 配套优化：部署脚本重构

### 修改文件清单

| 文件 | 修改内容 |
|------|---------|
| `start_services.sh` | 移除 `set -e`；Redis 失败改为 `exit 1`；`sleep` 改为 `wait_for_service()` 轮询；前端失败降级为警告；访问地址改为 Nginx 入口 |
| `stop_services.sh` | 停止后验证服务状态；补充清理 8001 端口（调度器）；抽取 `cleanup_port()` 函数 |
| `restart_services.sh` | 逐个重启替代一条命令全量重启；等待就绪后再检查状态 |
| `restart_backend.sh` | 完全重写：删除 Mac 路径硬编码，改用 `systemctl`；改为仅重启后端四个服务 |

---

## 配套优化：deployment 服务配置补全

### 新增文件

| 文件 | 说明 |
|------|------|
| `deployment/rc-celery-ai.service` | 新增入库，`--queues=file_processing,ai_processing` |
| `deployment/rc-celery-data.service` | 新增入库，`--queues=data_fetching`，concurrency=4 |

### 修改文件

| 文件 | 修改内容 |
|------|---------|
| `deployment/rc-webapp.service` | 新增 `After=redis.service`；新增 `--workers 2`；新增 `KillSignal`/`TimeoutStopSec` |
| `deployment/rc-scheduler.service` | 新增 `After=redis.service` 及 `Wants=redis.service` |
| `deployment/rc-frontend.service` | `REACT_APP_API_BASE_URL` 改为相对路径 `/api` |

### 删除文件

| 文件 | 原因 |
|------|------|
| `deployment/rc-database.service` | 与系统自带 `postgresql.service` 重复，启动脚本也未使用，存在冲突风险 |

---

## 服务器更新操作手册

### 首次部署后同步 service 文件

```bash
sudo cp /opt/rcproject/deployment/rc-celery-ai.service   /etc/systemd/system/
sudo cp /opt/rcproject/deployment/rc-celery-data.service /etc/systemd/system/
sudo cp /opt/rcproject/deployment/rc-webapp.service      /etc/systemd/system/
sudo cp /opt/rcproject/deployment/rc-scheduler.service   /etc/systemd/system/
sudo cp /opt/rcproject/deployment/rc-frontend.service    /etc/systemd/system/
sudo systemctl daemon-reload
```

### 日常部署新代码

```bash
# 拉取代码
cd /opt/rcproject && git pull

# 仅重启后端（不影响前端/数据库/Redis）
sudo ./restart_backend.sh
```

### 全量重启（配置变更后）

```bash
sudo ./restart_services.sh
```

### 查看服务状态

```bash
systemctl status 'rc-*'
journalctl -u rc-celery-ai -f      # 文件处理/AI Worker 日志
journalctl -u rc-celery-data -f    # 数据获取 Worker 日志
journalctl -u rc-webapp -f         # Web 应用日志
journalctl -u rc-scheduler -f      # 调度器日志
```
