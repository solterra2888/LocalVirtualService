# 权限系统说明

> 最后更新：2026-05-10

---

## 目录

1. [权限模型概述](#1-权限模型概述)
2. [认证机制](#2-认证机制)
3. [后端：完整管理员专用模块](#3-后端完整管理员专用模块)
4. [后端：普通用户可访问，部分操作需管理员](#4-后端普通用户可访问部分操作需管理员)
5. [后端：普通用户行级隔离](#5-后端普通用户行级隔离)
6. [投资项目协作权限（独立体系）](#6-投资项目协作权限独立体系)
7. [前端路由与 UI 控制](#7-前端路由与-ui-控制)
8. [已知不一致 / 待修复](#8-已知不一致--待修复)
9. [快速对照表](#9-快速对照表)

---

## 1. 权限模型概述

系统存在**两套独立的权限体系**，切勿混淆：

| 体系 | 字段 | 作用域 | 说明 |
|------|------|--------|------|
| **全局系统管理员** | `users.is_admin` | 整个应用 | 布尔值，用于管控系统级功能 |
| **项目协作角色** | `project_members.role` | 单个投资项目内 | `owner / admin / member`，与全局 `is_admin` 完全无关 |

---

## 2. 认证机制

### 2.1 数据存储

```sql
-- users 表（backend/auth/user_auth.py 建表）
is_admin BOOLEAN DEFAULT FALSE

-- 默认管理员账号来自环境变量
DEFAULT_ADMIN_USER / DEFAULT_ADMIN_PASSWORD
```

- 用户名为 `admin` 的账户**不允许**被取消管理员权限，也不允许修改用户名（`backend/api/users.py`）。

### 2.2 JWT 签发

登录时将 `is_admin` 写入 token payload：

```python
# backend/api/auth.py
access_token = create_access_token(data={
    "sub": user_info['username'],
    "user_id": user_info['id'],
    "is_admin": user_info.get('is_admin', False),
    "tv": new_tv,          # 会话版本号，多设备登录时使旧 token 失效
})
```

Token 有效期：**8 小时**（`ACCESS_TOKEN_EXPIRE_MINUTES = 480`）。

> **注意**：`is_admin` 以**登录时 JWT 为准**。若在数据库中变更了用户的管理员状态，需重新登录才生效。

### 2.3 后端鉴权依赖

```python
# backend/auth/jwt_utils.py

async def get_current_user(...) -> TokenData:
    """验证 JWT，检查 session 版本，返回 TokenData"""

async def get_current_admin_user(current_user = Depends(get_current_user)) -> TokenData:
    """在 get_current_user 基础上额外校验 is_admin，否则返回 HTTP 403"""
```

所有需要鉴权的接口至少使用 `get_current_user`；管理员专用接口使用 `get_current_admin_user`。

### 2.4 前端存储

登录成功后，`authStore`（zustand）将 `user.is_admin` 存入 `localStorage`，所有 UI 逻辑均读此字段。

---

## 3. 后端：完整管理员专用模块

以下模块 / 接口**全部**使用 `get_current_admin_user`，非管理员调用一律 **HTTP 403**。

### 3.1 用户管理 `/api/users`

| 接口 | 说明 |
|------|------|
| `GET /` | 获取所有用户列表 |
| `POST /` | 创建新用户 |
| `GET /{user_id}` | 获取指定用户详情 |
| `PUT /{user_id}` | 更新用户信息（含 `is_admin`、密码、用户名）|
| `DELETE /{user_id}` | 删除用户 |

**例外（普通用户可访问）**：

| 接口 | 说明 |
|------|------|
| `GET /me/profile` | 查看自己的资料 |
| `PUT /me/profile` | 更新自己的用户名 / 头像 |
| `POST /me/avatar` | 上传自己的头像 |
| `GET /shareable` | 获取可分享的用户列表（文件分享用）|
| `PUT /{user_id}/password` | 修改密码（改他人需 `is_admin`）|

### 3.2 LLM 全局配置 `/api/llm/...`（`llm_config.py`）

整个 Router 通过 `dependencies=[Depends(get_current_admin_user)]` 保护，包含：
- LLM Provider 配置（CRUD）
- 模型参数管理
- API Key 管理
- 全局配置导入 / 导出
- 配置重载

### 3.3 系统日志 `/api/logs/...`

| 接口 | 说明 |
|------|------|
| `GET /api/logs/list` | 获取日志文件列表 |
| `GET /api/logs/{log_type}` | 读取日志文件内容 |
| `DELETE /api/logs/{log_type}` | 清空日志文件 |

### 3.4 Celery / Redis 监控 `/api/monitoring/...`

| 接口 | 说明 |
|------|------|
| `GET /api/monitoring/queues` | 所有队列积压长度 |
| `GET /api/monitoring/workers` | Worker 列表与心跳 |
| `GET /api/monitoring/summary` | 综合监控数据 |
| `POST /api/monitoring/queues/{name}/purge` | 清空指定队列（含二次确认）|

### 3.5 新闻批量删除

| 接口 | 说明 |
|------|------|
| `POST /api/news/delete-by-date-range/preview` | 预览按日期范围待删数据量 |
| `DELETE /api/news/delete-by-date-range` | 执行批量删除（不可逆）|

### 3.6 投资主题管理端点（部分）

| 接口 | 说明 |
|------|------|
| `GET /api/investment-themes/users` | 获取所有 `created_by` 用户列表 |
| `POST /{theme_id}/verify-ticker` | 验证并补全 Ticker 数据 |
| `PATCH /{theme_id}/enabled` | 启用 / 禁用主题（影响新闻匹配）|
| `POST /batch-enabled` | 批量启用 / 禁用主题 |

### 3.7 LLM Prompt 模板——分类与系统模板

| 接口 | 说明 |
|------|------|
| `POST /api/llm/prompts/categories` | 创建模板分类 |
| `GET /api/llm/prompts/system-templates` | 列出所有 system-scope 模板 |
| `PUT /api/llm/prompts/system-templates/{module_code}` | 编辑系统模板内容 |

### 3.8 调试接口

| 接口 | 说明 |
|------|------|
| `GET /api/debug/config` | 查看数据库/JWT 配置摘要 |

### 3.9 调度器执行日志代理 `/api/scheduler/...`（`scheduler_proxy.py`）

以下接口代理独立 Scheduler 服务上的**执行历史 / 日志**，**仅管理员**（`get_current_admin_user`）；普通用户 **403**。

| 接口 | 说明 |
|------|------|
| `GET /api/scheduler/tasks/{task_id}/logs` | 单任务执行日志 |
| `GET /api/scheduler/execution-logs` | 全部执行日志 |
| `GET /api/scheduler/execution-logs/summary` | 执行统计摘要 |
| `GET /api/scheduler/execution-logs/download` | CSV 下载 |
| `DELETE /api/scheduler/execution-logs/clear` | 清空执行日志 |

> 调度任务的创建 / 启用 / 禁用 / 删除等其余 `/api/scheduler/...` 代理仍为登录用户可用（`get_current_user`）。前端 **`/scheduler`**、**`/scheduler/logs`** 已由 **`AdminRoute`** 守卫，仅管理员进入；若客户端绕过页面直接调任务 CRUD API，行为仍以服务端为准（见 §8.4）。

---

## 4. 后端：普通用户可访问，部分操作需管理员

### 4.1 新闻预设 `/api/news-presets`

| 操作 | 权限 |
|------|------|
| 读取预设（含系统预设）| 所有登录用户 |
| 创建自定义预设 | 所有登录用户 |
| **创建系统预设**（`is_system_preset: true`）| **仅管理员** |
| 编辑自己的预设 | 本人 |
| **编辑系统预设** | **仅管理员** |
| 删除自己的预设 | 本人 |
| **删除系统预设** | **仅管理员** |

### 4.2 LLM Prompt 模板 `/api/llm/prompts/templates`

| 操作 | 权限 |
|------|------|
| 查看模板列表 | 所有登录用户（service 层按 `is_admin` 过滤可见范围）|
| 查看模板详情 | 所有登录用户 |
| **创建 system-scope 模板** | **仅管理员**（deprecated 接口，建议走系统模板入口）|
| 创建 user-scope 模板（`PUT /my-templates/{module_code}`）| 所有登录用户 |
| 更新 system 模板 | **仅管理员** |
| 更新 user 模板 | 本人或管理员 |
| 删除 system 模板 | **仅管理员** |
| 删除 user 模板 | 本人或管理员 |
| 设置个人模板偏好 | 所有登录用户（普通用户只能选**自己创建**的；管理员还可选 system 模板）|

> `SYSTEM_FIXED` 模块（硬编码的核心模块）禁止任何人通过普通接口修改，只能走 admin 的 `/system-templates` 入口。

### 4.3 经济事件

| 操作 | 权限 |
|------|------|
| 查询经济事件数据 | 所有登录用户 |
| **`POST /economic-events/fetch`**（从 EODHD 拉取）| **仅管理员** |
| **`DELETE /economic-events/cleanup`**（清理旧数据）| **仅管理员** |

### 4.4 Market Weekly 报告

| 操作 | 权限 |
|------|------|
| 获取、刷新报告 | 所有登录用户 |
| `GET /watchlist-stocks?user_id=X` | 查自己的：所有用户；查他人的：**仅管理员** |

### 4.5 Agent 凭据（`db_manager`）

| 操作 | 权限 |
|------|------|
| 查看凭据 | 自己创建的 + 共享的 + 默认的 |
| **查看全部凭据** | **管理员可见所有** |
| 删除凭据 | 本人（非本人 **403**）|
| **删除任意凭据** | **仅管理员** |

---

## 5. 后端：普通用户行级隔离

### 5.1 文件管理 (`/api/files`)

文件访问通过 `_check_file_access` 函数控制，返回 `'owner' | 'shared' | None`：

```python
def _check_file_access(cursor, file_id, user_id, username, is_admin) -> str:
    # 1. 是否为 processing_stats.user == username → 'owner'
    # 2. 是否在 file_shares 表中 → 'shared'
    # 否则返回 None（无权限）
```

> **重要**：函数签名接收 `is_admin`，但函数体**未使用**该参数。即管理员**不能**凭 `is_admin` 绕过文件访问限制，也无法查看他人的私有文件。文件权限完全由「所有者 + 分享表」决定。

| 操作 | 权限要求 |
|------|----------|
| 查看 / 下载文件 | `owner` 或 `shared` |
| 分享文件 | `owner` 或 `shared`（支持再分享）|
| 取消分享 | 仅 `owner` |
| 查看分享列表 | `owner` 或 `shared` |

### 5.2 任务执行记录 (`/api/tasks`)

所有接口使用 `get_current_user`，行级隔离由 SQL 查询过滤 `username`：

| 操作 | 权限 |
|------|------|
| 查看执行列表 | 只返回当前用户的记录 |
| 查看执行详情 | 本人（或文件被分享给当前用户）|
| 删除执行记录 | 仅本人 |
| 更新 AI 分析报告内容 | 仅本人 |
| 重命名执行记录 | 仅本人 |

### 5.3 投资主题 (`/api/investment-themes`)

| 操作 | 非管理员 | 管理员 |
|------|----------|--------|
| **列表**（`GET /themes`）| 默认只见自己创建；`show_shared=true` 可加入他人分享 | 默认全库；可用 `created_by` 多选筛选 |
| 编辑描述 / 向量化 | 仅本人创建的主题 | 所有主题 |
| 更新 Tickers | 仅本人创建的主题 | 所有主题 |
| 删除主题 | 仅本人创建的主题 | 所有主题 |
| 分享主题 | 仅本人创建的主题 | 所有主题 |
| 批量打标签 | 自己创建的 + 已分享的主题 | 所有主题 |
| `can_edit` / `can_delete` / `can_share` 字段 | `created_by == username` | 始终 `true` |

---

## 6. 投资项目协作权限（独立体系）

> 此体系与全局 `users.is_admin` **完全无关**，由 `project_members.role` 驱动。

### 6.1 角色定义

| 角色 | 说明 |
|------|------|
| `owner` | 项目创建者，拥有全部权限 |
| `admin` | 管理员级成员（**项目内**管理员，≠ 系统管理员）|
| `member` | 普通成员，默认只读，可按需开放 `can_edit` / `can_invite` |

### 6.2 权限矩阵

| 操作 | owner | admin | member（默认）| member（can_edit）| member（can_invite）|
|------|:-----:|:-----:|:-------------:|:------------------:|:--------------------:|
| `view`（查看）| ✅ | ✅ | ✅ | ✅ | ✅ |
| `edit`（编辑内容）| ✅ | ✅ | ❌ | ✅ | ❌ |
| `invite`（邀请成员）| ✅ | ✅ | ❌ | ❌ | ✅ |
| `manage`（管理设置）| ✅ | ✅ | ❌ | ❌ | ❌ |
| `delete`（删除项目）| ✅ | ❌ | ❌ | ❌ | ❌ |
| `transfer`（转让所有权）| ✅ | ❌ | ❌ | ❌ | ❌ |

### 6.3 成员管理约束

| 操作 | owner | admin |
|------|-------|-------|
| 修改/移除任意成员 | ✅ | 仅非 owner 成员 |
| 移除 owner | 不可移除自己（需先转让）| ❌ |

---

## 7. 前端路由与 UI 控制

### 7.1 AdminRoute 强制守卫

以下页面受 `AdminRoute` 保护：非管理员访问时**重定向到 `/daily`**。

```
/users            → UsersPage（用户管理）
/llm-config       → LLMConfigPage（LLM 配置）
/system-logs      → SystemLogsPage（系统日志）
/scheduler        → SchedulerPage（调度器管理）
/scheduler/logs   → SchedulerLogsPage（调度器执行日志）
```

### 7.2 头像下拉菜单（`Layout.tsx` 用户菜单）

以下路径仅在 **`user.is_admin === true`** 时出现在头像下拉中：
- **LLM Settings**（`/llm-config`）
- **System Log Files**（`/system-logs`）
- **Scheduler Management**（`/scheduler`）
- **Scheduler Logs**（`/scheduler/logs`）
- **User Management**（`/users`）

头部会显示红色 **Admin** 角标。

### 7.3 页面内 admin-only UI

| 页面/组件 | 管理员专有功能 |
|-----------|----------------|
| `NewsPage` | 「按日期范围删除新闻」按钮 |
| `PresetsManager` | 可选创建「System Preset」；可编辑/删除系统预设 |
| `InvestmentThemesPage` | 全局筛选 `created_by` 多用户；批量管理操作 |
| `PromptTemplatesPage` | 系统模板 Tab 及编辑入口 |
| `SchedulerPage` / `SchedulerLogsPage` | 仅管理员可进入（`AdminRoute`）|
| `FundamentalsPage` | 部分管理操作按钮 |

---

## 8. 已知不一致 / 待修复

### 8.1 `PresetPanel.tsx` 用用户名判断管理员

```tsx
// 当前实现（不正确）
const isAdmin = user?.username === 'admin' || user?.username === 'administrator';

// 应改为
const isAdmin = user?.is_admin === true;
```

**影响**：若系统存在用户名非 `admin`/`administrator` 的管理员账号，该面板的「管理员操作」入口不会显示。

### 8.2 `_check_file_access` 接受但未使用 `is_admin`

当前文件访问完全由「所有者 + 分享表」决定，管理员无法查看他人的私有文件。若需「超级管理员可查看所有文件」，需在该函数内补充逻辑：

```python
def _check_file_access(cursor, file_id, user_id, username, is_admin) -> str:
    # ...
    if is_admin:        # ← 目前此处缺失
        return 'admin'  # 可视需求添加
```

### 8.3 `backend/api/tasks.py` 遗留 import

```python
from backend.auth.jwt_utils import get_current_user, get_current_admin_user
```

`get_current_admin_user` 在该文件中**从未被使用**，属于遗留 import，可清理。

### 8.4 `scheduler_proxy.py`：任务 CRUD 仍为登录用户

调度器**执行日志**相关代理（`/tasks/{id}/logs`、`/execution-logs` 等）已使用 `get_current_admin_user`。  
创建 / 启用 / 禁用 / 删除定时任务等其余代理仍为 `get_current_user`；若需后端一并收紧为仅管理员，需单独变更产品与权限设计。

### 8.5 `FundamentalsPage.tsx` admin 判断逻辑冗余

该页从 localStorage 多字段猜测管理员身份，与主站 `authStore.user.is_admin` 不统一，建议统一使用 `useAuthStore` hook。

---

## 9. 快速对照表

### 9.1 全局系统管理员（`users.is_admin`）

| 功能区域 | 管理员 | 普通用户 |
|----------|:------:|:-------:|
| 用户列表/创建/删除/修改角色 | ✅ | ❌ |
| LLM 全局配置（Provider/Key/模型）| ✅ | ❌ |
| 系统日志查看/清空 | ✅ | ❌ |
| 调度器执行日志 API（`/api/scheduler/.../logs`、`execution-logs` 等）| ✅ | ❌（403）|
| 调度器管理页面（`/scheduler`）与执行日志页（`/scheduler/logs`）| ✅ | ❌（重定向）|
| Celery/Redis 监控/清队列 | ✅ | ❌ |
| 新闻批量按日期删除 | ✅ | ❌ |
| 调试配置接口 | ✅ | ❌ |
| 创建/编辑/删除系统新闻预设 | ✅ | ❌ |
| 系统 Prompt 模板分类创建 | ✅ | ❌ |
| 系统 Prompt 模板编辑 | ✅ | ❌ |
| 经济事件数据拉取/清理 | ✅ | ❌ |
| 投资主题全库管理 | ✅ | 仅自有 |
| 查看他人 Market Weekly watchlist | ✅ | 仅自己 |
| 修改他人密码 | ✅ | ❌ |
| Agent 凭据全局查看/删除 | ✅ | 仅自己相关 |
| 文件查看/下载（他人私有文件）| ❌（与管理员无关）| ❌ |
| 前端 `/users`、`/llm-config`、`/system-logs`、`/scheduler`、`/scheduler/logs` 路由 | ✅ | 重定向 `/daily` |

### 9.2 项目协作角色（`project_members.role`，与系统管理员无关）

| 操作 | owner | project admin | member |
|------|:-----:|:-------------:|:------:|
| 查看项目 | ✅ | ✅ | ✅ |
| 编辑内容 | ✅ | ✅ | 按 `can_edit` |
| 邀请成员 | ✅ | ✅ | 按 `can_invite` |
| 管理项目设置 | ✅ | ✅ | ❌ |
| 删除项目 | ✅ | ❌ | ❌ |
| 转让所有权 | ✅ | ❌ | ❌ |
