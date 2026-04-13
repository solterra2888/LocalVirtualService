# Upload Files：用户级文件夹（User Execution Folders）说明文档

## 概述

Upload Files 模块中的 **Folders** 功能用于按用户组织「任务执行记录」（`processing_stats` 一条即一次 execution）。历史上文件夹归属写在 `file_processing_details.folder_id` 上，属于**全局一条记录**，被分享者无法在不改写所有者数据的前提下，把共享文件归入**自己的**文件夹。

本方案采用 **方案 B**：统一使用 **`user_execution_folders`** 表，按「用户名 + execution」记录每个用户对同一批文件的文件夹视图，便于后续复杂查询与扩展。

---

## 解决的问题

| 问题 | 说明 |
|------|------|
| 共享文件无法归入自己的文件夹 | 旧模型在 `file_processing_details` 上写 `folder_id`，会覆盖所有者视图。 |
| 查询语义不清晰 | 文件夹与「谁在看」绑定后，列表筛选、计数、展示均以当前登录用户为准。 |

---

## 数据模型

### 1. `file_folders`（仍在使用）

存储文件夹**定义**：名称、颜色、排序等，按 `username` 隔离。

- 新建表 **`user_execution_folders`** 通过外键 `folder_id` 引用 `file_folders.id`。
- 所有创建 / 重命名 / 删除文件夹的 API 仍直接操作本表。

### 2. `user_execution_folders`（新表，唯一事实来源）

按 **用户 + execution** 记录文件夹归属。

| 字段 | 说明 |
|------|------|
| `username` | 当前视图所属用户 |
| `processing_stats_id` | 任务执行 ID（与列表中的 execution 一致） |
| `folder_id` | 指向 `file_folders.id`；为 `NULL` 时表示不在映射表中（见「Uncategorized」语义） |

约束：`UNIQUE (username, processing_stats_id)`。

语义要点：

- **所有者**与**被分享者**各自可有独立一行；互不影响。
- 归入「Uncategorized」时：删除该用户在 `user_execution_folders` 中对应行（无行即未分类）。

### 3. `file_processing_details.folder_id`（已废弃）

迁移脚本已将历史数据迁入 `user_execution_folders` 并将该列清空。

- **应用代码不再读写该列**。
- 列与外键可在一段时间稳定运行后删除（见文末「可选清理」）。

---

## 迁移脚本

**路径：** `backend/database/migrations/create_user_execution_folders.sql`

**主要步骤：**

1. 创建表 `user_execution_folders` 及索引。
2. 从 `file_processing_details` 与 `processing_stats` 关联出所有者 `username`，将原 `folder_id` 迁移为 `(username, processing_stats_id, folder_id)`（去重插入）。
3. 将 `file_processing_details.folder_id` 全部置为 `NULL`。

**生产环境执行：** 在维护窗口由 DBA 或具备权限的角色执行该 SQL 文件（与项目内其他 migration 一致）。

---

## 后端变更摘要

### `backend/api/file_folders.py`

- **列表文件夹** `GET /files/folders`：`file_count` 改为统计 `user_execution_folders` 中 `username = 当前用户` 且 `folder_id = 该文件夹` 的行数。
- **移动** `PATCH /files/folders/move/{execution_id}`、**批量移动** `PATCH /files/folders/batch-move`：
  - 使用 `_filter_accessible_execution_ids`：execution 须为**当前用户拥有**，或通过 `file_shares` **分享给当前用户**。
  - 写入 `user_execution_folders`：`folder_id` 非空时 `INSERT ... ON CONFLICT DO UPDATE`；为空时 `DELETE` 对应映射（Uncategorized）。
- **更新文件夹** `PATCH /files/folders/{id}`：`file_count` 同上。
- **删除文件夹** `DELETE /files/folders/{id}`：删除 `user_execution_folders` 中 `folder_id = 该 id` 的行，再删 `file_folders` 行（不再更新 `file_processing_details`）。

### `backend/api/tasks.py`

- **任务列表** `GET /tasks/executions`：
  - 返回的 `folder_id` / `folder_name` 改为基于 **`user_execution_folders` + 当前用户 `username`** 的子查询。
  - 查询参数 `folder_id`：
    - 数字：筛选「当前用户在该 execution 下映射到指定文件夹」；
    - `uncategorized`：筛选「当前用户**没有** `user_execution_folders` 行」的 execution（在仍有权访问的前提下）。

---

## 前端变更摘要

**文件：** `web-frontend/src/pages/FilesPage.tsx`

- 共享文件（`access_type === 'shared'`）同样展示 **Move to Folder** 菜单，调用同一套 API；后端会写入当前用户名下的 `user_execution_folders`。

**服务层：** `web-frontend/src/services/files.ts` 中文件夹相关 API 路径未变，无需改签名。

---

## 与旧列、旧表的关系

| 对象 | 是否仍使用 | 说明 |
|------|------------|------|
| `file_folders` | **是** | 文件夹定义与所有权仍在此表。 |
| `user_execution_folders` | **是** | 每个用户对 execution 的文件夹归属。 |
| `file_processing_details.folder_id` | **否（应用层）** | 已迁移并清空；仅保留列便于观察期回滚。 |

---

## 可选清理（稳定运行后）

确认无回滚需求后，可删除废弃列及外键，减少歧义：

```sql
ALTER TABLE file_processing_details
  DROP CONSTRAINT IF EXISTS file_processing_details_folder_id_fkey;

ALTER TABLE file_processing_details
  DROP COLUMN IF EXISTS folder_id;
```

执行前请再次确认代码库与线上库中无任何引用该列的逻辑或报表。

---

## 回滚思路（仅供参考）

若需在迁移后短期内回滚数据语义（不推荐长期双写）：

1. 从 `user_execution_folders` 按所有者 `username` 与 `processing_stats_id` 将 `folder_id` 写回 `file_processing_details`（需约定多文件同一 execution 时取哪条明细，历史行为为「整 execution 同一 folder」）。
2. 恢复旧版应用代码。

实际回滚应以当时备份与业务约定为准；上线前建议做好数据库备份。

---

## 相关文件索引

| 类型 | 路径 |
|------|------|
| 迁移 SQL | `backend/database/migrations/create_user_execution_folders.sql` |
| 文件夹 API | `backend/api/file_folders.py` |
| 任务列表 / 文件夹筛选 | `backend/api/tasks.py` |
| 任务响应模型 | `backend/schemas/task.py`（`folder_id` / `folder_name` 字段含义为「当前用户视图」） |
| 前端 Upload Files | `web-frontend/src/pages/FilesPage.tsx` |
| 前端 API 封装 | `web-frontend/src/services/files.ts` |

---

*文档版本：与 `user_execution_folders` 上线实现同步；若 schema 或 API 有变更请同步更新本文。*
