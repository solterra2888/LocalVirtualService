# GuanHe Daily — Agent Skill (v0.2)

这个 skill 让 Cursor、Claude Code、Codex 等 AI 助手能够读取你在 **GuanHe** 平台上积累的信息——包括你订阅的 Feed（Twitter、YouTube、新闻等）和你自己上传的文件（PDF、文档、图片等）——并根据你的自然语言问题给出总结和洞察。

> **前置要求**：本 Skill 通过 MCP（`guanhe-mcp >= 0.2.0`）执行实际数据请求。使用前请先按「Agent 接入 → MCP」页面配置好 MCP，并在 `mcp.json` 中填入有效的 `GUANHE_TOKEN`（PAT）。

---

## 数据范围

MCP 工具只返回当前账号有权限访问的数据：
- **自己上传的文件**：PDF、图片、音频、文档等，经平台处理后的内容
- **他人分享给你的文件**：其他用户明确分享到你账号的上传内容
- **已启用的 Feed 订阅**：订阅列表中状态为「已启用」的 Twitter、YouTube、新闻等频道；已停用的订阅不在结果中

---

## 典型用户问法 → 工具映射

| 用户会这样说 | 应调用的工具 | 关键参数 |
|---|---|---|
| 今天有什么值得关注的？ | `daily_feed` | `since=24h` |
| 这周的信息帮我总结 | `daily_feed` | `since=7d`（默认） |
| 帮我找关于 NVDA / 英伟达的内容 | `daily_feed` | `q=NVDA OR 英伟达` |
| 只看 Twitter 内容 | `daily_feed` | `sources=twitter` |
| 只看我上传的文件 | `daily_feed` | `sources=pdf,image,text,audio` |
| 近期美国访华有什么消息（多角度） | `daily_multi_search` | 见「多组搜索」示例 |
| 这条具体讲了什么 / 全文 / 字幕 | `daily_item` | `id`（上一轮列表的 id） |
| 帮我看这几条的全文 | `daily_items_batch` | `ids=[...]` |
| 帮我看今天的 Twitter 日报 | `report_latest` | `kind=twitter` |
| 帮我看 5 月 10 号的 YouTube 日报 | `report_by_date` | `kind=youtube, date=2026-05-10` |
| 有哪些日报可以看？ | `report_list` | `kind=twitter` |
| 我订阅了哪些频道？ | `list_subscriptions` | — |
| 哪些来源有内容 / 我的 preset 是什么？ | `list_sources` | — |

> **重要**：用户不会说"调用 daily_feed"——他们只会说"帮我看看今天的内容"。AI 助手负责根据语义自动选择正确的工具和参数。

---

## 五大搜索范式

### ① 入口收敛

先判断用户意图，选一个入口，不做防御性并行调用。

| 用户意图 | 入口 |
|---|---|
| 「最近 XX 有什么消息」 | `daily_feed q=XX` |
| 「今天/这周有什么事」 | `daily_feed since=24h/7d` |
| 「从多个角度分析一个话题」 | `daily_multi_search` |
| 「帮我看日报」 | `report_latest / report_by_date` |

0 条时才换入口或放宽参数。**不要**同时调用 `daily_feed` 和 `daily_multi_search` 搜同一话题。

### ② 渐进式精度

从最窄约束开始，逐步放宽直到信息量够用：

```
最窄: tickers=NVDA.US + since=7d + q=黄仁勋
      ↓ 0 条或不够
放宽: 去掉 tickers，只用 q=黄仁勋 OR Jensen
      ↓ 还不够
最宽: daily_multi_search，多组分维度覆盖
```

0 条时先查看 `diagnostics` 字段，它会告诉你哪个词的覆盖量是 0。

### ③ 已知-未知边界

每次输出明确区分：
- **已知**：搜到的、可引用的、带 id 和 url 来源的
- **未知**：搜了但没找到的、单一来源未验证的、超出订阅源覆盖范围的

搜到 N 条≠这就是全部——主动声明缺口，特别是结果 < 5 条时。

遇到 0 条时，先调用 `list_subscriptions` 确认该频道是否已启用，再调用 `list_sources` 确认该 source 在 preset 里，再考虑扩大时间窗口。

### ④ 多组并行 → 用 `daily_multi_search`

当话题有多个维度（人物、事件、议题），用 `daily_multi_search` 一次提交多组查询，服务端自动合并去重，并在 `per_group` 里告诉你每组命中多少条：

```json
{
  "groups": [
    {"q": "Greer OR USTR OR 贸易代表", "since": "14d"},
    {"q": "黄仁勋 OR Jensen OR 库克 OR Cook OR 马斯克 OR Musk", "since": "14d"},
    {"q": "访华 OR Beijing summit OR 北京峰会 OR 中美会晤", "since": "14d"}
  ],
  "dedup_by": "url"
}
```

`per_group` 为空的组说明该维度在你的订阅源中没有覆盖——这本身就是有价值的信息。

### ⑤ 源外补充

GuanHe 信息不足以回答核心问题时，主动触发 web_search / web_extract 补充，并标注来源：

- 0 条或有效信息 < 3 条，且用户问的是事实性问题
- 核心消息仅 1 个来源 → 需要外部交叉验证
- 时间线存在明显缺口，且该时间段应有公开信息

---

## 工具说明

### `daily_feed` — 每日时间线 + 关键词搜索

获取用户的 Daily 时间线（Feed 订阅 + 上传文件合并）。`q` 可选；有 `q` 时相当于全文搜索。

参数（全部可选）：
- `since`：时间窗口，`24h / 48h / 7d / 14d / 30d / 2w` 或 ISO 日期时间。默认 7d。非法值返回 400。
- `q`：关键词过滤。**语法**：空格=AND，`OR`=任一，`"短语"`=精确。不要传整句话。
- `sources`：逗号分隔，如 `twitter,youtube_feed,pdf`。不填则使用网页端 preset。
- `tickers`：逗号分隔，**必须含交易所后缀**，如 `NVDA.US,9988.HK`。裸代码不会命中。
- `tags`：逗号分隔概念标签（任一匹配），从上一轮结果的 `tags` 字段或 `list_subscriptions` 获取。
- `author`：按作者名/账号过滤，子串匹配，如 `author=Bloomberg`。仅适用于 Feed 条目。
- `limit`：1–100，默认 20。
- `cursor`：翻页游标，来自上次返回的 `next_cursor`。
- `dedup`：默认 true，自动去除重复 URL 的条目。

**`q` 语法示例**：

| 输入 | 语义 |
|---|---|
| `NVDA` | 包含 NVDA |
| `美国 访华` | 同时包含「美国」和「访华」 |
| `美国 OR 访华 OR Trump` | 包含任一 |
| `"Air Force One" China` | 包含精确短语 + 单词 |

> ⚠️ `q="帮我找近期美国访华的重要消息"` → 被当字面字符串匹配，永远 0 条。正确做法：提取 3–6 个核心词用 OR 连接。

### `daily_item` — 单条详情下钻

按 id 取一条 Daily 内容的完整字段。id 来自 `daily_feed` / `daily_multi_search` 返回的 `id` 字段（如 `feed_88247`、`upload_123`）。

返回额外字段：
- `content`：主正文（上传文件的 Markdown/OCR/笔记，Twitter/新闻的全文，YouTube 的视频描述）
- `transcript`：纯文本字幕（YouTube 订阅 / 音频 / podcast 上传）；推文/PDF 为 null
- `content_truncated` / `transcript_truncated`：true 时表示字段被截断，完整内容请用 `url` 跳转

**何时下钻**：摘要 `summary` 回答不了问题时（要原文、原话、时间戳、数字细节）。

### `daily_items_batch` — 批量详情

一次请求获取最多 10 条完整详情。比 N 次串行 `daily_item` 更高效。

参数：`ids`（字符串数组，最多 10 个）

### `daily_multi_search` — 多组并行搜索

同时提交 1–5 组搜索参数，服务端并行执行、合并结果、按 url 或 id 去重后按时间排序返回。

参数：
- `groups`：数组，每组支持 `q, since, sources, tickers, tags, author, limit`
- `dedup_by`：`"url"`（默认）或 `"id"`
- `limit_per_group`：每组最多返回多少条，默认 50

返回额外字段：`per_group`——每组原始命中数，用于判断哪个搜索角度贡献了有效信息。

### `list_subscriptions` — 订阅频道清单

返回所有已启用的订阅频道，附带每个频道最近 30 天的条目数。

用途：
- 确认某个 Twitter 账号 / YouTube 频道是否已订阅且处于启用状态
- 在 0 命中时排查「没订阅」vs「真的没内容」

### `list_sources` — 来源类型清单

返回所有可用 source 类型，以及它们是否在用户当前 Daily preset 中、最近 30 天有多少条目。

用途：
- 在使用 `sources=` 参数前确认该来源有内容
- 了解用户当前 preset 包含哪些来源

### `report_latest` / `report_by_date` / `report_list`

获取每日汇总报告。

所有报告工具必须传 `kind` 参数：
- `kind=twitter`：Twitter 日报
- `kind=youtube`：YouTube 日报

`report_by_date` 还需要 `date=YYYY-MM-DD`。

---

## 来源类型参考

| source 值 | 含义 | 适用于 sources= 参数 |
|---|---|---|
| `twitter` | 订阅的 Twitter Feed | ✓ |
| `youtube` / `youtube_feed` | 订阅的 YouTube Feed | ✓ |
| `news` / `web` / `podcast` | 订阅的网页 / 新闻 / 播客 | ✓ |
| `pdf` | 用户上传的 PDF | ✓ |
| `image` | 用户上传的图片 | ✓ |
| `audio` | 用户上传的音频 | ✓ |
| `text` | 用户上传的文本文件 | ✓ |

---

## 列表 → 详情下钻模式（核心）

1. 第一轮 `daily_feed` / `daily_multi_search` 返回**轻量列表**（标题 + 摘要 + id + tickers/tags）；
2. 助手把每条的 **`id`** 记在工作记忆中；
3. 用户追问「具体讲了什么 / 全文 / 字幕」时，**直接用上一轮的 `id` 调 `daily_item` 或 `daily_items_batch`**，不要再重新搜索；
4. 如果上下文里找不到 id（对话太长被压缩），先用关键词 `daily_feed` 重定位，拿到 id 再下钻。

---

## 返回结构

### 列表条目（`daily_feed` / `daily_multi_search`）

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

### 详情条目（`daily_item` / `daily_items_batch`）

在列表字段基础上额外包含 `content` / `transcript` / `*_truncated`：

```json
{
  "id": "feed_88247",
  "content": "本期节目讨论特朗普访华代表团构成 ...",
  "content_truncated": false,
  "transcript": "Welcome to Horizons Middle East ...",
  "transcript_truncated": true
}
```

### 0 命中诊断（`diagnostics`）

当 `total == 0` 且有 `q` 时，响应包含：

```json
{
  "diagnostics": {
    "parsed_q_groups": [{"op": "and", "terms": ["美国", "访华", "重要"]}],
    "terms_in_corpus": {"美国": 1280, "访华": 47, "重要": 9831},
    "since_used": "2026-05-10T...",
    "sources_used": ["twitter", "youtube_feed", "news"],
    "hint": "Term(s) ['重要'] have 0 matches ... consider removing them."
  }
}
```

用 `terms_in_corpus` 判断哪个词过于限制——0 值的词是瓶颈，改 OR 或去掉。

---

## 安全提示

- PAT 是长期有效的密钥，等同于密码，不要分享或提交到代码仓库。
- 可在「用户菜单 → API Keys」随时查看、创建或撤销 PAT。
