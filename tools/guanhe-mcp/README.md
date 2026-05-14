# guanhe-mcp

Model Context Protocol (stdio) server that wraps the GuanHe **readonly** Agent HTTP API (`/api/v1/agent`).

## Requirements

- Python **>= 3.10**
- `httpx` (installed automatically with this package)
- A **Personal Access Token** (`ght_...`) from the web app: **User menu → API Keys**

## Install

MCP 以 stdio 子进程形式运行在 **Cursor / Claude Desktop 所在的本地机器**上，直接从 GuanHe 服务器下载安装，无需 GitHub 访问权限：

```bash
pip install "https://guanhecapital.hk/downloads/guanhe-mcp.whl"
```

> `httpx` 作为依赖会自动安装，无需单独 `pip install httpx`。

安装后查出 Python 绝对路径，配置 `mcp.json` 时使用：

```bash
which python   # 激活目标虚拟环境后执行
```

### 从本地源码安装（开发调试用）

若已克隆仓库并需要修改代码：

```bash
cd tools/guanhe-mcp
pip install -e .
```

## Environment

| Variable | Required | Description |
|----------|----------|-------------|
| `GUANHE_TOKEN` | Yes | PAT starting with `ght_` |
| `GUANHE_BASE_URL` | Yes | API origin, e.g. `https://your-host` (no trailing slash) |

## Run (stdio MCP)

```bash
export GUANHE_TOKEN='ght_...'
export GUANHE_BASE_URL='https://your-host'
guanhe-mcp
```

## Cursor (`~/.cursor/mcp.json`)

与本地 `development/dev.conf` 中的 **`rcTool`** 环境一致（路径按本机修改）：

```json
{
  "mcpServers": {
    "guanhe": {
      "command": "/path/to/your/python",
      "args": ["-m", "guanhe_mcp.server"],
      "env": {
        "GUANHE_TOKEN": "ght_xxxxxxxx",
        "GUANHE_BASE_URL": "http://43.99.37.76",
        "PYTHONUNBUFFERED": "1"
      }
    }
  }
}
```

If `guanhe-mcp` is not on `PATH`, use the full path to the console script or:

```json
"command": "python3",
"args": ["-m", "guanhe_mcp.server"],
"env": { ... }
```

## Claude Desktop (`claude_desktop_config.json`)

```json
{
  "mcpServers": {
    "guanhe": {
      "command": "/usr/bin/env",
      "args": ["guanhe-mcp"],
      "env": {
        "GUANHE_TOKEN": "ght_xxxxxxxx",
        "GUANHE_BASE_URL": "https://your-host"
      }
    }
  }
}
```

Adjust `command`/`args` to your Python environment (e.g. conda shim or `python3 -m guanhe_mcp.server`).

## OpenAI Codex CLI

Codex MCP configuration varies by version. Point it at the same `command` + `env` as Cursor, or run `guanhe-mcp` under `npx`-style wrappers if you add one.

## Tools (8)

| Tool | Maps to |
|------|---------|
| `daily_feed` | `GET /api/v1/agent/daily` |
| `daily_item` | `GET /api/v1/agent/daily/{id}` |
| `search` | `GET /api/v1/agent/search` |
| `twitter_daily_latest` | `GET /api/v1/agent/reports/twitter-daily/latest` |
| `twitter_daily_by_date` | `GET /api/v1/agent/reports/twitter-daily/{date}` |
| `youtube_daily_latest` | `GET /api/v1/agent/reports/youtube-daily/latest` |
| `youtube_daily_by_date` | `GET /api/v1/agent/reports/youtube-daily/{date}` |
| `twitter_daily_list` | `GET /api/v1/agent/reports/twitter-daily` |

Tool implementations return **JSON strings** (MCP content).

## Skill

See repository `docs/agent/GUANHE_DAILY.md` (also served at `/agent/skill/GUANHE_DAILY.md` when the API is running).
