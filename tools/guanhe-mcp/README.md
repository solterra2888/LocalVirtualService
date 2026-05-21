# guanhe-mcp v0.2.1

Model Context Protocol (stdio) server that wraps the GuanHe **readonly** Agent HTTP API (`/api/v1/agent`).

## Breaking changes from v0.1

| v0.1 tool | v0.2 replacement |
|---|---|
| `search` | `daily_feed` with `q=` parameter |
| `twitter_daily_latest` | `report_latest` with `kind=twitter` |
| `twitter_daily_by_date` | `report_by_date` with `kind=twitter` |
| `twitter_daily_list` | `report_list` with `kind=twitter` |
| `youtube_daily_latest` | `report_latest` with `kind=youtube` |
| `youtube_daily_by_date` | `report_by_date` with `kind=youtube` |
| `ticker=NVDA` (single) | `tickers=NVDA.US,AAPL.US` (multi, with exchange suffix) |

Old HTTP paths return **410 Gone** with an upgrade hint.

## Requirements

- Python **>= 3.10**
- `httpx` (installed automatically)
- A **Personal Access Token** (`ght_...`) from the web app: **User menu → API Keys**

## Install

```bash
# From the published wheel:
pip install "https://guanhecapital.hk/downloads/guanhe_mcp-0.2.1-py3-none-any.whl"

# From local source (dev):
cd tools/guanhe-mcp
pip install -e .
```

## Environment variables

| Variable | Required | Description |
|----------|----------|-------------|
| `GUANHE_TOKEN` | Yes | PAT starting with `ght_` |
| `GUANHE_BASE_URL` | Yes | API origin, e.g. `https://your-host` (no trailing slash) |

## Cursor (`~/.cursor/mcp.json`)

```json
{
  "mcpServers": {
    "guanhe": {
      "command": "/path/to/your/python",
      "args": ["-m", "guanhe_mcp.server"],
      "env": {
        "GUANHE_TOKEN": "ght_xxxxxxxx",
        "GUANHE_BASE_URL": "https://www.guanhecapital.hk",
        "PYTHONUNBUFFERED": "1"
      }
    }
  }
}
```

Replace `/path/to/your/python` with the output of `which python` (or `where python` on Windows) after activating your target virtual environment.

## Claude Desktop (`claude_desktop_config.json`)

```json
{
  "mcpServers": {
    "guanhe": {
      "command": "/usr/bin/env",
      "args": ["guanhe-mcp"],
      "env": {
        "GUANHE_TOKEN": "ght_xxxxxxxx",
        "GUANHE_BASE_URL": "https://www.guanhecapital.hk"
      }
    }
  }
}
```

## Tools (9)

| Tool | Description | Maps to |
|------|-------------|---------|
| `daily_feed` | Time-ordered feed + optional keyword search | `GET /api/v1/agent/daily` |
| `daily_item` | Full content + transcript for one item | `GET /api/v1/agent/daily/{id}` |
| `daily_items_batch` | Full detail for up to 10 items at once | `POST /api/v1/agent/daily/batch` |
| `daily_multi_search` | 1–5 parallel search groups, server-side dedup | `POST /api/v1/agent/daily/multi` |
| `list_subscriptions` | Enabled feed channels with 30-day counts | `GET /api/v1/agent/subscriptions` |
| `list_sources` | Source types, preset membership, 30-day counts | `GET /api/v1/agent/sources` |
| `report_latest` | Latest daily report (kind=twitter\|youtube) | `GET /api/v1/agent/reports/latest?kind=` |
| `report_by_date` | Daily report for YYYY-MM-DD | `GET /api/v1/agent/reports/{date}?kind=` |
| `report_list` | Paginated list of available reports | `GET /api/v1/agent/reports?kind=` |

## Quick usage examples

```bash
# Set env
export GUANHE_TOKEN="ght_..."
export GUANHE_BASE_URL="https://www.guanhecapital.hk"

# Run the server (it reads JSON-RPC from stdin)
python -m guanhe_mcp.server
```

For smoke testing via the fallback script:

```bash
python scripts/call_guanhe_mcp.py daily_feed '{"since": "24h", "limit": 5}'
python scripts/call_guanhe_mcp.py daily_multi_search '{"groups": [{"q": "NVDA OR 英伟达", "since": "7d"}, {"q": "Greer OR USTR OR 贸易代表", "since": "7d"}]}'
python scripts/call_guanhe_mcp.py report_latest '{"kind": "twitter"}'
python scripts/call_guanhe_mcp.py list_subscriptions '{}'
```

## Key parameter notes

**`q` syntax** — space = AND (all terms must match), `OR` (uppercase) = any match, `"phrase"` = exact substring.
- Good: `q=NVDA OR 英伟达`
- Bad: `q=帮我找近期美国访华的重要消息` (treated as a literal string, will return 0 results)

**`tickers`** — must include exchange suffix: `NVDA.US`, `9988.HK`, `600519.SS`, `0700.HK`.
If unsure of the suffix, use `q=NVDA` first and read the `tickers` field in the response.

**`people`** — comma-separated person names (OR logic). Use original-form names from content:
`people=Trump,Musk` (not `特朗普`). More precise than `q=Trump` full-text search.
Read the `people` field in list responses to discover valid names.

**`countries`** — comma-separated country **full English names** (OR logic):
`countries=United States,China`. NOT ISO codes (`US` will not match).

**`since`** — `24h`, `48h`, `7d`, `14d`, `30d`, `2w`, or ISO 8601. Invalid values return 400.

**`sources`** — comma-separated: `twitter`, `youtube`, `youtube_feed`, `news`, `pdf`, `image`, `audio`, `text`, `podcast`.
Omit to use your web-app Daily preset.

## Skill documentation

Full usage guide with search strategies and worked examples:

```
https://www.guanhecapital.hk/agent/skill/GUANHE_DAILY.md
```
