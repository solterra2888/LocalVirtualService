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
| 只看 Twitter 上的内容 | `daily_feed` | `sources=twitter` |
| 只看我上传的文件 | `daily_feed` | `sources=pdf,image,text,audio` |
| 帮我看看今天的 Twitter 日报 | `twitter_daily_latest` | — |
| 帮我看看 5 月 10 号的 Twitter 日报 | `twitter_daily_by_date` | `date=2026-05-10` |
| 帮我看看最新的 YouTube 日报 | `youtube_daily_latest` | — |
| 有哪些日报可以看？ | `twitter_daily_list` | — |

> **重要**：用户不会说"调用 daily_feed"——他们只会说"帮我看看今天的内容"。AI 助手负责根据语义自动选择正确的工具和参数。

---

## 工具说明

### `daily_feed` — 每日时间线
获取用户的 Daily 时间线（Feed 订阅 + 上传文件合并）。

参数（全部可选）：
- `since`：时间窗口，`24h` / `7d` / `30d` / ISO 日期时间。默认 7d。
- `sources`：逗号分隔的来源子类型，如 `twitter,youtube_feed,pdf`。不填则使用用户在网页端设置的默认 preset。
- `q`：关键词过滤（与 `search` 不同，这是在时间线基础上过滤）。
- `ticker`：按股票代码过滤，如 `NVDA`。
- `limit`：返回条数，默认 20，最大 100。
- `cursor`：翻页游标，从上次返回的 `next_cursor` 获取。

### `search` — 关键词搜索
在所有内容（标题、摘要、正文）中做全文搜索。

参数：`q`（必填）、`since`、`sources`、`ticker`、`limit`、`cursor`（同上）。

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

## 返回结构（单条内容）

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

---

## 回答格式指引

拿到数据后，向用户呈现时：
1. 用 3–8 条要点，每条格式：**标题** — 一句话摘要（来源）[链接]
2. 提到投资标的时，只引用 JSON 里 `tickers` 字段的内容，不自行推断股票代码
3. 若内容较多，先给整体概述，再分类展开（如：「上传文件」「Twitter」「YouTube」各自有什么）
4. 没有查到内容时，如实告知并建议用户检查订阅设置或扩大时间范围

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
