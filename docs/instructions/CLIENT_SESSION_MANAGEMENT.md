# 多客户端会话管理说明

> 最后更新：2026-10-15

---

## 目录

1. [机制概述](#1-机制概述)
2. [客户端类型](#2-客户端类型)
3. [数据库存储](#3-数据库存储)
4. [登录与校验流程](#4-登录与校验流程)
5. [前端上报](#5-前端上报)
6. [兼容性与迁移](#6-兼容性与迁移)
7. [测试](#7-测试)

---

## 1. 机制概述

会话版本按**客户端类型独立计数**：同一类型内新登录会踢掉旧会话（收到 `SESSION_REPLACED` 401），不同类型之间互不影响，可同时在线。

典型场景：用户在 PC 浏览器登录后，再用手机浏览器登录，**PC 端不会被踢掉**；反之亦然。两台手机（同为 mobile 类型）之间仍然互踢。

## 2. 客户端类型

| 类型 | 来源 | 会话行为 |
|------|------|----------|
| `web` | PC 浏览器（缺省值） | 同类型互踢 |
| `mobile` | 手机浏览器（前端按 UA 识别上报） | 同类型互踢 |
| `extension` | Chrome 扩展（登录请求显式携带） | **从不递增计数**，多个扩展可同时在线（扩展有自动重登机制，递增会导致互相踢出死循环） |

类型归一化见 `backend/auth/user_auth.py` 的 `normalize_client_type()`：`client_type` 缺失或为未知值时一律按 `web` 处理。

## 3. 数据库存储

`users` 表三个独立列，列名映射由 `SESSION_VERSION_COLUMNS` 白名单定义（列名仅来自该白名单，杜绝 SQL 注入）：

```python
# backend/auth/user_auth.py
SESSION_VERSION_COLUMNS = {
    "web": "token_version",             # 复用原列，存量数据无需迁移
    "mobile": "token_version_mobile",   # 新增列
    "extension": "token_version_extension",  # 新增列，永不递增
}
```

建表 / 列自迁移由 `UserAuth._ensure_session_version_columns()` 完成（`information_schema` 检查 + `ALTER TABLE ... ADD COLUMN ... DEFAULT 1`，幂等），在 `setup_users_table()` 启动建表和每次读写版本号时都会执行。

## 4. 登录与校验流程

### 4.1 登录（`POST /api/auth/login`）

1. 按 `client_type` 归一化为 web / mobile / extension；
2. `extension`：只读自身计数（不递增）；`web` / `mobile`：递增各自计数；
3. JWT payload 写入 `ct`（客户端类型）与 `tv`（该类型的版本号）。

### 4.2 校验（`backend/auth/jwt_utils.py::verify_token`）

1. 解出 `ct` 声明并归一化（**旧 token 无 `ct`，按 `web` 处理**，与拆分前行为一致）；
2. 读取该类型对应的版本列，与 token 中的 `tv` 比对；
3. 不一致 → 401 + `SESSION_REPLACED` 标识，前端弹英文 Modal 引导重新登录（`web-frontend/src/utils/authSession.ts`）。

Token 有效期仍为 **8 小时**（`ACCESS_TOKEN_EXPIRE_MINUTES = 480`），无刷新机制。

## 5. 前端上报

`web-frontend/src/utils/authSession.ts::detectClientType()` 按移动端 UA 正则返回 `mobile` / `web`；`authStore.login` 将其作为 `client_type` 一并提交。Chrome 扩展登录继续显式携带 `client_type: 'extension'`。

> 注意：识别依据是 **UA 而非视口宽度**——PC 上把窗口缩窄不应改变会话类型；iPad 新版 UA 呈现为桌面 Mac，会被归为 `web`。

## 6. 兼容性与迁移

- 存量 `token_version` 列直接作为 web 计数复用，无需数据迁移；
- 旧 token（无 `ct`）：按 web 校验，行为与升级前完全一致；
- 旧扩展 token（无 `ct`）：升级后先按 web 校验，若被 web 登录踢出，扩展会自动重登并拿到带 `ct: extension` 的新 token；
- 部署后首次登录时自动补齐 `token_version_mobile` / `token_version_extension` 两列（要求应用账号对 `users` 表有 ALTER 权限，与既有 `avatar_url` 列迁移一致）。

## 7. 测试

`backend/tests/test_session_version.py`：类型归一化 / 列映射、`verify_token` 按 `ct` 校验对应计数、旧 token 回退 web、同类型版本不一致抛 `SESSION_REPLACED`、登录端点三个分支（mobile 递增、extension 不递增、缺省 web）。

```bash
pytest backend/tests/test_session_version.py -v
```
