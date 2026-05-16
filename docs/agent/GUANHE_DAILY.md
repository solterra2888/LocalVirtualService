# GuanHe Daily — Agent Skill

这个 skill 让 Cursor、Claude Code、Codex 等 AI 助手能够读取你在 **GuanHe** 平台上积累的信息——包括你订阅的 Feed（Twitter、YouTube、新闻等）和你自己上传的文件（PDF、文档、图片等）——并根据你的自然语言问题给出总结和洞察。

> **前置要求**：本 Skill 通过 MCP（`guanhe-mcp`）执行实际数据请求。使用前请先按「Agent 接入 → MCP」页面配置好 MCP，并在 `mcp.json` 中填入有效的 `GUANHE_TOKEN`（PAT）。

---

## 数据范围

MCP 工具只返回当前账号有权限访问的数据，包含以下三类：
- **自己上传的文件**：PDF、图片、音频、文档等，经平台处理后的内容
- **他人分享给你的文件**：其他用户明确分享到你账号的上传内容
- **已启用的 Feed 订阅**：订阅列表中状态为「已启用」的 Twitter、YouTube、新闻等频道；已停用（Disabled）的订阅不在结果中

---

## 用户使用场景

用户在 GuanHe 网页端做了两件事：
1. **订阅 Feed**：关注了若干 Twitter 账号、YouTube 频道、新闻源等。
2. **上传文件**：上传了 PDF 研报、文档、图片等供平台处理。

这两部分内容会合并进入每日「Daily」时间线，用户可以直接用自然语言向 AI 提问，无需知道任何 API 或工具名称。

---

## 典型用户问法 → 工具映射

| 用户会这样说 | 应调用的工具 | 关键参数 |
|---|---|---|
| 今天有什么值得关注的？最近有什么新动态？ | `daily_feed` | `since=24h` |
| 这周的信息帮我总结一下 | `daily_feed` | `since=7d`（默认，可省略） |
| 帮我找找关于 NVDA / 英伟达的内容 | `search` | `q=NVDA` |
| 我上传的文件里有没有关于 XX 的内容 | `search` | `q=XX, sources=pdf` |
| 近期美国访华有什么消息 | `search` | `q="美国 OR 访华 OR Trump OR 中美"`, `since=14d` |
| 只看 Twitter 上的内容 | `daily_feed` | `sources=twitter` |
| 只看我上传的文件 | `daily_feed` | `sources=pdf,image,text,audio` |
| **这条具体讲了什么 / 展开第 N 条 / 给我看原文** | **`daily_item`** | **`id`（取上一轮列表里的 id）** |
| **那个 Bloomberg / YouTube 视频的 transcript 是什么** | **`daily_item`** | **`id`，读返回里的 `transcript` 字段** |
| **那份 PDF / 我上传的研报里关于 XX 怎么说** | **先 `search` 拿 id → `daily_item`** | **`q=XX` → `id`** |
| 帮我看看今天的 Twitter 日报 | `twitter_daily_latest` | — |
| 帮我看看 5 月 10 号的 Twitter 日报 | `twitter_daily_by_date` | `date=2026-05-10` |
| 帮我看看最新的 YouTube 日报 | `youtube_daily_latest` | — |
| 有哪些日报可以看？ | `twitter_daily_list` | — |

> **重要**：用户不会说"调用 daily_feed"——他们只会说"帮我看看今天的内容"。AI 助手负责根据语义自动选择正确的工具和参数。

---

## 列表 → 详情下钻模式（核心）

绝大多数追问都遵循这条路径：

1. 第一轮 `daily_feed` / `search` 返回**轻量列表**（标题 + 摘要 + 链接 + id + tickers/tags）；
2. AI 助手把整段列表（**特别是每条的 `id`**）记在工作记忆里展示给用户；
3. 用户对其中一条追问"具体讲了什么 / 全文 / 字幕 / 给我细节"时，**直接用上一轮列表里的 `id` 调 `daily_item`**，**不要**再调 `daily_feed` / `search` 兜圈子；
4. 如果上下文里已经找不到 id（对话太长被压缩），先 `search` 用关键词重定位拿到 id，再 `daily_item`。

> **何时主动下钻**：用户的问题用列表里的 `summary` 回答不了（要原文、要原话、要时间戳、要数字细节、要"具体怎么说"），就该调 `daily_item`。

---

## 工具说明

### `daily_feed` — 每日时间线
获取用户的 Daily 时间线（Feed 订阅 + 上传文件合并）。

参数（全部可选）：
- `since`：时间窗口，`24h` / `7d` / `30d` / ISO 日期时间。默认 7d。
- `sources`：逗号分隔的来源子类型，如 `twitter,youtube_feed,pdf`。不填则使用用户在网页端设置的默认 preset。
- `q`：关键词过滤（与 `search` 不同，这是在时间线基础上过滤）。语法同 `search`：空格=AND，`OR`=任一，`"短语"`=精确。
- `ticker`：按股票代码过滤，如 `NVDA`。
- `limit`：返回条数，默认 20，最大 100。
- `cursor`：翻页游标，从上次返回的 `next_cursor` 获取。

### `search` — 关键词搜索
在所有内容（标题、摘要、正文）中做全文搜索。

参数：`q`（必填）、`since`、`sources`、`ticker`、`limit`、`cursor`（同上）。

**`q` 语法（重要）**：

| 输入格式 | 语义 | 示例 |
|---|---|---|
| 单词 | 子串匹配 | `Trump` |
| 空格分隔 | AND（每个词都要命中） | `美国 访华` |
| `OR`（大写）分隔 | OR（任一词命中即可） | `美国 OR 访华 OR Trump` |
| 双引号短语 | 精确子串（空格视为一体） | `"Air Force One"` |

> ⚠️ **不要把整句话当 `q` 传**：`q="帮我找近期美国访华的重要消息"` 会被当字面字符串匹配，永远 0 条。  
> 正确做法：**提取 3–6 个核心关键词，用 `OR` 连接**，如 `q="美国 OR 访华 OR Trump OR 中美 OR 黄仁勋"`, `since=14d`。  
> 如果 0 条，先放宽（减少 AND 词 或 改 OR），而非改写整句。

### `daily_item` — 单条详情下钻
按 `id` 取**一条** Daily 内容的完整字段，用于用户对某条追问细节、要原文 / 字幕 / 全文时。

参数：
- `id`（必填）：来自 `daily_feed` / `search` 返回的 `id`，形如 `feed_88247` 或 `upload_123`。

返回（在列表条目字段之外，额外有）：
- `content`：主正文。
  - `upload` → `file_processing_details.content`（Markdown 正文 / OCR / 网页解析 / 笔记）；
  - `twitter` / `news` → `subscription_content.content`（推文 / 新闻全文）；
  - `youtube`（订阅）→ `subscription_content.content`（视频描述）。
- `transcript`：纯文本字幕 / 转写。
  - `youtube`（订阅）→ `subscription_content.transcript_cleaned`（回退 `transcript`）；
  - `upload` 的 `youtube` / `audio` / `podcast` → `file_processing_details.asr_with_diarization` 拍平成行；
  - 没有字幕的类型（推文 / PDF 等）= `null`。
- `content_truncated` / `transcript_truncated`：`true` 表示字段被截断，需要全文请引导用户跳 `url`。
- `body_excerpt`（向后兼容字段，旧客户端用，新客户端优先 `content`）。

### `twitter_daily_latest` / `twitter_daily_by_date` / `twitter_daily_list`
获取 Twitter 每日汇总报告。`by_date` 参数为 `date`（`YYYY-MM-DD`）。

### `youtube_daily_latest` / `youtube_daily_by_date`
获取 YouTube 每日汇总报告，用法同上。

---

## 来源类型参考

`sources=` 参数可用的值（逗号分隔多个）：

| source 值 | 含义 |
|---|---|
| `twitter` | 订阅的 Twitter Feed |
| `youtube` / `youtube_feed` | 订阅的 YouTube Feed |
| `web` / `news` / `podcast` | 订阅的网页 / 新闻 / 播客 Feed |
| `pdf` | 用户上传的 PDF |
| `image` | 用户上传的图片 |
| `audio` | 用户上传的音频 |
| `text` | 用户上传的文本文件 |

---

## 返回结构

### 列表条目（`daily_feed` / `search`）

```json
{
  "id": "feed_12345",
  "title": "...",
  "summary": "...",
  "url": "https://...",
  "source": "twitter",
  "author": "@handle",
  "published_at": "2026-05-13T04:00:00+00:00",
  "tickers": ["NVDA.US"],
  "tags": ["AI", "Chips"]
}
```

### 详情条目（`daily_item`）

在列表字段基础上额外包含 `content` / `transcript` / `*_truncated`：

```json
{
  "id": "feed_88247",
  "title": "Horizons Middle East — Trump China Visit Special",
  "summary": "...",
  "url": "https://youtube.com/watch?v=...",
  "source": "youtube",
  "author": "Bloomberg Television",
  "published_at": "2026-05-13T05:30:00+00:00",
  "tickers": ["NVDA.US", "AAPL.US", "TSLA.US", "BLK.US"],
  "tags": ["China", "Tariffs", "CEO"],
  "content": "本期节目讨论特朗普访华代表团构成 ...",
  "content_truncated": false,
  "transcript": "Welcome to Horizons Middle East ...\nJensen Huang said NVIDIA will ...",
  "transcript_truncated": true,
  "body_excerpt": "本期节目讨论特朗普访华代表团构成 ..."
}
```

---

## 回答格式指引

拿到数据后，向用户呈现时：
1. 用 3–8 条要点，每条格式：**标题** — 一句话摘要（来源）[链接]
2. 提到投资标的时，只引用 JSON 里 `tickers` 字段的内容，不自行推断股票代码
3. 若内容较多，先给整体概述，再分类展开（如：「上传文件」「Twitter」「YouTube」各自有什么）
4. 没有查到内容时，如实告知并建议用户检查订阅设置或扩大时间范围

**追问 / 下钻场景的呈现方式**：

5. 用户对某条追问"具体讲了什么 / 全文 / 字幕"时，先调 `daily_item` 拿 `content` / `transcript`；
6. 围绕 `content` / `transcript` 字段**直接引用**关键段落或原话，避免再说一遍 `summary`；
7. 引用 transcript 时优先标注时间戳（若可识别）和讲话人；同时附 `url`，方便用户跳去原视频核对；
8. 看到 `content_truncated: true` 或 `transcript_truncated: true` 时，明确告知用户"以上为节选，完整内容请打开链接"，不要假装看到了全部。

---

## 配置（管理员 / 开发者）

使用 MCP（Cursor / Claude Code）接入时，需要在 `~/.cursor/mcp.json` 里配置：

| 环境变量 | 说明 |
|---|---|
| `GUANHE_TOKEN` | 在网页端「用户菜单 → API Keys」创建，以 `ght_` 开头 |
| `GUANHE_BASE_URL` | 服务地址，如 `https://your-host`（不含结尾斜杠） |

PAT 默认限速：60 次/分钟。

---

## 安全提示

- PAT 是长期有效的密钥，等同于密码，不要分享或提交到代码仓库。
- 可在「用户菜单 → API Keys」随时查看、创建或撤销 PAT。

---

## MCP 接入文档

详见仓库 `tools/guanhe-mcp/README.md`。
