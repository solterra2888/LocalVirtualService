# 群消息入库（Chat Group Messages）功能手册

本文说明本次更新引入的 **群聊消息统一入库** 能力：接收飞书（Lark）群消息 Webhook，以及通过 HTTP 批量写入微信等其他来源的消息，落库到 PostgreSQL 表 `chat_group_messages`。

对应提交：`2177e41` — *lark群消息回调接口 和 微信等其他消息的入库接口*。

涉及文件：

| 文件 | 职责 |
|------|------|
| `backend/api/chat_group_messages.py` | FastAPI 路由与请求体校验 |
| `backend/services/chat_group_messages_service.py` | 归一化、去重、建表、Upsert |
| `backend/api/__init__.py` | 导出 `chat_group_messages` 模块 |
| `backend/main.py` | 导入模块（见 §8 部署注意） |

---

## 1. 功能概述

```
                    ┌─────────────────────────────────────────┐
                    │         chat_group_messages 表           │
                    │  (source, message_id) 唯一 → 幂等 Upsert │
                    └─────────────────────────────────────────┘
                                        ▲
                                        │
                    ┌───────────────────┴───────────────────┐
                    │     ChatGroupMessagesService           │
                    │  normalize → dedupe → INSERT ... ON     │
                    │  CONFLICT DO UPDATE                     │
                    └───────────────────┬───────────────────┘
                          ▲                           ▲
              Lark Webhook│                           │HTTP JSON
                          │                           │
        POST .../lark/callback              POST .../batch
        (原始 Lark payload)                 (标准化字段列表)
```

**设计目标**

1. **多来源统一 schema**：`source` 区分 `lark` / `wechat` / `external` 等。
2. **幂等写入**：当 `message_id` 非空时，以 `(source, message_id)` 为业务主键，重复回调会 **更新** 而非重复插入。
3. **Lark 专用解析**：按 `message_type`（text / image / post / file…）拆出 `msg_type`、`content`、`link_path`、`file_path`。
4. **懒建表**：首次写入时 `CREATE TABLE IF NOT EXISTS` + 索引，无需单独 migration（适合快速上线，生产环境也可改为显式迁移）。

---

## 2. HTTP API

路由定义前缀：`/chat-group-messages`（注册到 FastAPI 时通常再加 `/api`，见 §8）。

### 2.1 飞书回调 — `POST /chat-group-messages/lark/callback`

**用途**：配置在飞书开放平台「事件订阅」里的 Request URL。

**处理顺序（ASCII）**

```
  Lark POST JSON
       │
       ▼
  ┌────────────────────────────┐
  │ payload.challenge 存在？    │──yes──► 返回 {"challenge": "..."}  (URL 校验)
  └────────────────────────────┘
       │ no
       ▼
  ┌────────────────────────────┐
  │ type == url_verification？  │──yes──► 同上
  └────────────────────────────┘
       │ no
       ▼
  ChatGroupMessagesService.save_lark_callback()
       │
       ▼
  event.message.chat_type == "group" ?
       │
      no ──► ignored=1, 不入库
       │
      yes
       ▼
  normalize_lark_payload() → save_messages([record])
       │
       ▼
  {"success": true, "data": { processed, inserted, updated, ... }}
```

**仅处理群聊**：`chat_type != "group"` 时直接返回 `ignored: 1`，不抛错。

### 2.2 批量入库 — `POST /chat-group-messages/batch`

**用途**：微信机器人、内部脚本、其他 IM 等 **已结构化** 的消息批量写入。

**请求体示例**

```json
{
  "messages": [
    {
      "source": "wechat",
      "message_id": "wx-msg-001",
      "chat_id": "group@chatroom",
      "chat_name": "投研交流群",
      "sender_id": "user_openid_xxx",
      "sender_name": "张三",
      "send_time": "2026-06-03T08:30:00+00:00",
      "msg_type": "link",
      "content": "请看这篇 https://example.com/report",
      "link_path": "https://example.com/report",
      "file_path": null
    }
  ]
}
```

**字段说明**

| 字段 | 必填 | 说明 |
|------|------|------|
| `source` | 否（默认 `external`） | 来源标识 |
| `message_id` | 否 | 有值则参与 `(source, message_id)` 去重；无值则每次插入新行 |
| `chat_id` | 是 | 群 ID |
| `chat_name` | 否 | 群名称 |
| `sender_id` | 是 | 发送者 ID |
| `sender_name` | 否 | 发送者昵称 |
| `send_time` | 是 | ISO8601 / Unix 秒 / Unix 毫秒 |
| `msg_type` | 是 | `text` / `link` / `image` / `file` / `其他` 等 |
| `content` | 否 | 文本摘要 |
| `link_path` | 否 | 消息内主链接 |
| `file_path` | 否 | 文件 key 或路径（Lark 的 `image_key` / `file_key`） |

**响应 `data` 统计字段**

| 字段 | 含义 |
|------|------|
| `processed` | 本批实际执行 Upsert 的行数 |
| `inserted` | 新插入行数（`RETURNING (xmax = 0)`） |
| `updated` | 冲突后更新行数 |
| `skipped` | 校验失败被跳过的输入条数 |
| `duplicates_removed` | 批内按 `(source, message_id)` 去重合并条数 |
| `ignored` | 仅 Lark 回调：非群消息时为 1 |

---

## 3. 服务层核心流程

### 3.1 `save_messages` 批处理流水线

```
  messages[]
       │
       ▼
  for each: _normalize_external_message()
       │  (缺 source/chat_id/sender_id/msg_type/send_time → skip)
       ▼
  _dedupe_batch()
       │  同批 duplicate (source, message_id) 只保留最后一条
       │  (避免 PG ON CONFLICT 同一行 touched 两次报错)
       ▼
  _ensure_table()  ──► CREATE TABLE + INDEX IF NOT EXISTS
       ▼
  execute_values INSERT ... ON CONFLICT (source, message_id)
       DO UPDATE SET ... updated_at = now()
       ▼
  commit → 返回 inserted / updated 统计
```

### 3.2 Lark Payload 归一化

`normalize_lark_payload()` 从 Webhook 中提取：

```
payload.event
    ├── message
    │     ├── message_id, chat_id, chat_name, create_time
    │     ├── chat_type  (须为 "group")
    │     ├── message_type  → handle_lark_message()
    │     └── content (JSON 字符串或对象)
    └── sender
          ├── sender_id.{open_id|user_id|union_id}
          └── sender_name
```

固定 `source = "lark"`，`send_time` 由 `create_time` 毫秒时间戳转 UTC。

### 3.3 Lark 消息类型 → 表字段（`handle_lark_message`）

```
  message_type
       │
       ├── "text"     ──► 正文 text；含 http(s) URL → msg_type=link + link_path
       │
       ├── "image"    ──► msg_type=image, content="[图片]", file_path=image_key
       │
       ├── "post"     ──► 富文本块遍历 (text/a/at/img/file...)
       │                 拼成多行 content；推断 text/link/image/file/其他
       │
       ├── file/folder/audio/media/sticker
       │              ──► msg_type=file, file_path=file_key, content=文件名或占位
       │
       └── 其他       ──► msg_type=其他, content=兜底 JSON/字段
```

**Post 富文本块标签映射（节选）**

| Lark tag | 写入 content 片段 |
|----------|-------------------|
| `text` | 原文 |
| `a` | 链接文字或 href |
| `at` | `@用户名` |
| `img` | `[图片]` |
| `file` / `folder` | `[文件]` |
| `media` | `[视频]` |
| `emotion` | `[表情:xxx]` |

---

## 4. 数据库表结构

表名：`chat_group_messages`（首次写入自动创建）

```
┌──────────────────────────────────────────────────────────────┐
│ chat_group_messages                                           │
├─────────────────┬────────────────────────────────────────────┤
│ id              │ BIGSERIAL PK                                │
│ source          │ VARCHAR(50) NOT NULL                        │
│ message_id      │ VARCHAR(255)  ──┐                           │
│ chat_id         │ VARCHAR(255) NOT NULL                       │
│ chat_name       │ VARCHAR(255)                                │
│ sender_id       │ VARCHAR(255) NOT NULL                       │
│ sender_name     │ VARCHAR(255)                                │
│ send_time       │ TIMESTAMP(6) NOT NULL                       │
│ msg_type        │ VARCHAR(50) NOT NULL                        │
│ content         │ TEXT                                        │
│ link_path       │ VARCHAR(1000)                               │
│ file_path       │ VARCHAR(500)                                │
│ created_at      │ TIMESTAMP(6) DEFAULT now()                  │
│ updated_at      │ TIMESTAMP(6) DEFAULT now()                  │
└─────────────────┴────────────────────────────────────────────┘
         UNIQUE (source, message_id) WHERE message_id IS NOT NULL
         INDEX: source, chat_id, sender_id, msg_type, send_time DESC
```

**幂等语义**：同一 `source + message_id` 再次写入会更新群名、发送者、内容等字段，并刷新 `updated_at`。

**无 `message_id` 的消息**：不参与唯一约束，每次 batch 都会插入新行（适合无法拿到稳定 ID 的来源，但需注意重复数据）。

---

## 5. 模块依赖关系

```
backend/main.py
    └── import chat_group_messages          (模块已导入)

backend/api/__init__.py
    └── export chat_group_messages

backend/api/chat_group_messages.py
    ├── APIRouter prefix=/chat-group-messages
    ├── POST /lark/callback
    ├── POST /batch
    └── ChatGroupMessagesService

backend/services/chat_group_messages_service.py
    ├── DatabaseManager + get_database_config()
    ├── datetime_utils.ensure_utc / UTC
    └── handle_lark_message() 及 _handle_* 解析函数
```

---

## 6. 飞书侧配置要点（运维）

1. 在飞书应用后台订阅 **接收消息** 类事件（需机器人进群）。
2. Request URL 指向：`https://<你的域名>/api/chat-group-messages/lark/callback`（以实际 `include_router` 前缀为准）。
3. 首次保存会触发 **URL 校验**：服务原样返回 `challenge` 字段。
4. 确保后端进程能访问配置中的 PostgreSQL（与主站共用 `get_database_config()`）。
5. 回调接口当前 **无 JWT 校验**（典型 Webhook 模式）；生产环境建议在网关层校验飞书签名 Token，或于路由内增加验签逻辑（当前代码未实现）。

---

## 7. 调用示例

### 7.1 批量写入（curl）

```bash
curl -X POST 'http://127.0.0.1:8000/api/chat-group-messages/batch' \
  -H 'Content-Type: application/json' \
  -d '{
    "messages": [{
      "source": "external",
      "message_id": "demo-1",
      "chat_id": "chat-demo",
      "sender_id": "user-1",
      "send_time": 1748908800,
      "msg_type": "text",
      "content": "hello"
    }]
  }'
```

### 7.2 飞书 URL 校验（模拟）

```bash
curl -X POST 'http://127.0.0.1:8000/api/chat-group-messages/lark/callback' \
  -H 'Content-Type: application/json' \
  -d '{"challenge": "test-challenge-token"}'
# 期望: {"challenge":"test-challenge-token"}
```

---

## 8. 部署注意：路由注册

当前 `backend/main.py` **仅 import 了** `chat_group_messages`，**尚未** `app.include_router(...)`。

要使 API 对外可用，需在 `main.py` 路由注册区增加一行，例如：

```python
app.include_router(chat_group_messages.router, prefix="/api", tags=["群消息"])
```

完整路径将为：

- `POST /api/chat-group-messages/lark/callback`
- `POST /api/chat-group-messages/batch`

部署后可用 `/docs`（Swagger）在「群消息」分组下自测。

---

## 9. 错误与日志

| 场景 | 行为 |
|------|------|
| 单条字段校验失败 | 计入 `skipped`，不中断整批 |
| 数据库连接失败 | 500，`rollback` |
| Lark 非群消息 | HTTP 200，`ignored: 1` |
| 处理异常 | API 层 `HTTPException(500)`，日志 `chat_group_messages_api` / `chat_group_messages_service` |

---

## 10. 后续可扩展方向（未实现）

- 查询 API：按 `chat_id` / 时间范围 / `msg_type` 分页检索
- 与 Daily Feed、Agent 检索打通，将群消息纳入语义索引
- 飞书事件验签、加密解密
- 将 `CREATE TABLE` 迁到正式 Alembic/SQL 迁移脚本

---

*文档版本：2026-06-03，与 commit `2177e41` 代码一致。*
