# -*- coding: utf-8 -*-
"""
GuanHe MCP stdio server v0.2.0 (JSON-RPC over NDJSON).

Wraps the GuanHe Agent HTTP API /api/v1/agent/*.
No dependency on the official `mcp` PyPI package — only httpx.

Wire format: one JSON object per line (NDJSON), matching the MCP Python SDK and Cursor.

Breaking changes from v0.1:
  - search tool removed (use daily_feed with q=)
  - twitter_daily_* / youtube_daily_* removed (use report_latest/report_by_date/report_list with kind=)
  - ticker param → tickers (comma-separated multi-value)
  - New tools: daily_items_batch, daily_multi_search, list_subscriptions, list_sources
  - report_* tools now require kind= parameter
"""

from __future__ import annotations

import json
import os
import sys
from typing import Any, Dict, List, Optional, Union

PROTOCOL_VERSION = "2024-11-05"
SERVER_VERSION = "0.2.1"

_Q_SYNTAX_HINT = (
    "q syntax: space=AND (all terms match), OR (uppercase)=any match, "
    "\"phrase\"=exact substring. "
    "Pass 2-6 keywords joined by OR — NEVER pass a full sentence. "
    "Bad: q=\"帮我找近期美国访华的重要消息\" (treated literally, will return 0). "
    "Good: q=\"美国 OR 访华 OR Trump OR 中美\"."
)

_TICKERS_HINT = (
    "Must include exchange suffix: NVDA.US, 9988.HK, 600519.SS, 0700.HK. "
    "Bare codes like 'NVDA' will NOT match. "
    "If unsure of suffix, omit tickers and use q=NVDA instead, "
    "then read the tickers field in the response to get the correct format."
)

_SINCE_HINT = (
    "Time window. Examples: 24h, 48h, 7d, 14d, 30d, 2w. "
    "Or an ISO 8601 datetime. Default is 7d. "
    "Invalid values return HTTP 400."
)

_SOURCES_HINT = (
    "Comma-separated source types to filter by. "
    "Feed types: twitter, youtube, news. "
    "Upload types: document (PDF/Word/Excel/CSV + images), image, audio, text, podcast, web. "
    "Aliases accepted for document: pdf, word, doc, docx, excel, xls, xlsx, csv, img, photo. "
    "'document' expands to cover all uploaded files including images. "
    "In daily_feed responses, uploaded files appear with their specific format as source: "
    "'pdf', 'word', 'excel', or 'image' — NOT a generic 'pdf'. "
    "Omit to use your web-app Daily preset. "
    "Use list_sources to see which sources are active in your preset."
)

_TAGS_HINT = (
    "Comma-separated concept/theme tags (OR logic — item must match ANY tag). "
    "Tags come from the tags field in daily_feed / daily_multi_search responses. "
    "Use list_subscriptions or a prior search result to discover valid tag values."
)

_AUTHOR_HINT = (
    "Substring match on author name or handle. "
    "Examples: author=Bloomberg, author=@elonmusk. "
    "Only applies to feed items (Twitter, YouTube, news). "
    "Upload items have no author and will be excluded when this filter is set."
)

_PEOPLE_HINT = (
    "Comma-separated person names (OR logic — match ANY). "
    "Use original-form names from content: people=Trump,Musk,Xi Jinping. "
    "Matches structured people tags — more precise than q=Trump full-text search. "
    "Read the people field in daily_feed responses to discover valid names."
)

_COUNTRIES_HINT = (
    "Comma-separated country full English names (OR logic). "
    "Example: countries=United States,China. NOT ISO codes — use 'United States' not 'US'. "
    "Read the countries field in daily_feed responses to discover valid values."
)


def _env() -> tuple[str, str]:
    base = os.environ.get("GUANHE_BASE_URL", "").rstrip("/")
    token = os.environ.get("GUANHE_TOKEN", "").strip()
    if not base or not token:
        sys.stderr.write("GUANHE_BASE_URL and GUANHE_TOKEN must be set.\n")
        sys.exit(1)
    return base, token


def _client():
    """Lazy import httpx so MCP handshake works even without network."""
    import httpx

    base, token = _env()
    return httpx.Client(
        base_url=base,
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
        timeout=120.0,
    )


def _read_message() -> Optional[Union[Dict[str, Any], List[Any]]]:
    """Read one MCP NDJSON message from stdin."""
    line = sys.stdin.buffer.readline()
    if not line:
        return None
    if line.startswith(b"\xef\xbb\xbf"):
        line = line[3:]
    stripped = line.strip()
    if not stripped:
        return _read_message()
    if stripped.lower().startswith(b"content-length:"):
        n = int(stripped.split(b":", 1)[1].strip())
        sys.stdin.buffer.readline()
        body = sys.stdin.buffer.read(n)
        return json.loads(body.decode("utf-8"))
    return json.loads(stripped.decode("utf-8"))


def _write_message(payload: Union[Dict[str, Any], List[Any]]) -> None:
    sys.stdout.write(json.dumps(payload, ensure_ascii=False) + "\n")
    sys.stdout.flush()


_write_framed = _write_message


# ---------------------------------------------------------------------------
# Tool definitions
# ---------------------------------------------------------------------------

TOOLS: List[Dict[str, Any]] = [
    # 1 ─── daily_feed ────────────────────────────────────────────────────
    {
        "name": "daily_feed",
        "description": (
            "Retrieve the user's Daily timeline (feed subscriptions + uploaded files, merged). "
            "Use for: 'what happened today/this week', time-based browsing, source filtering. "
            "When q is set, acts as a full-text search over the timeline. "
            "DO NOT call in parallel with daily_multi_search for the same topic — pick one."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "since": {"type": "string", "description": _SINCE_HINT},
                "q": {"type": "string", "description": _Q_SYNTAX_HINT},
                "sources": {"type": "string", "description": _SOURCES_HINT},
                "tickers": {"type": "string", "description": _TICKERS_HINT},
                "tags": {"type": "string", "description": _TAGS_HINT},
                "author": {"type": "string", "description": _AUTHOR_HINT},
                "people": {"type": "string", "description": _PEOPLE_HINT},
                "countries": {"type": "string", "description": _COUNTRIES_HINT},
                "limit": {"type": "integer", "default": 20, "minimum": 1, "maximum": 100},
                "cursor": {"type": "string", "description": "Pagination cursor from previous next_cursor."},
                "dedup": {
                    "type": "boolean",
                    "default": True,
                    "description": "Collapse duplicate URLs from the result. Default true.",
                },
            },
        },
    },
    # 2 ─── daily_item ────────────────────────────────────────────────────
    {
        "name": "daily_item",
        "description": (
            "Get full content + transcript for a single Daily item by id. "
            "Use when the user asks for 'details', 'full text', 'original', 'transcript', "
            "or a follow-up question the list-level summary cannot answer. "
            "id comes from a prior daily_feed or daily_multi_search result (e.g. 'feed_88247', 'upload_123'). "
            "For multiple items at once use daily_items_batch instead."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "id": {
                    "type": "string",
                    "description": "Item id from a prior daily_feed/daily_multi_search response. E.g. 'feed_88247' or 'upload_123'.",
                }
            },
            "required": ["id"],
        },
    },
    # 3 ─── daily_items_batch ─────────────────────────────────────────────
    {
        "name": "daily_items_batch",
        "description": (
            "Fetch full detail for multiple Daily items in a single round-trip. "
            "Use instead of N serial daily_item calls when drilling down on 2–10 results. "
            "ids come from prior daily_feed or daily_multi_search results."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "ids": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "List of item ids. Max 10. E.g. ['feed_88247', 'feed_88250', 'upload_123'].",
                    "maxItems": 10,
                }
            },
            "required": ["ids"],
        },
    },
    # 4 ─── daily_multi_search ────────────────────────────────────────────
    {
        "name": "daily_multi_search",
        "description": (
            "Run 1–5 search groups in parallel, then merge and deduplicate server-side. "
            "Use when covering multiple topic angles in one request "
            "(e.g. trade + geopolitics + event timeline for a single story). "
            "Returns per_group hit counts so you can see which angle contributed most. "
            "Each group supports the same parameters as daily_feed."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "groups": {
                    "type": "array",
                    "description": "1–5 search groups. Each group is a set of daily_feed parameters.",
                    "maxItems": 5,
                    "items": {
                        "type": "object",
                        "properties": {
                            "q": {"type": "string", "description": _Q_SYNTAX_HINT},
                            "since": {"type": "string", "description": _SINCE_HINT},
                            "sources": {"type": "string", "description": _SOURCES_HINT},
                            "tickers": {"type": "string", "description": _TICKERS_HINT},
                            "tags": {"type": "string", "description": _TAGS_HINT},
                            "author": {"type": "string", "description": _AUTHOR_HINT},
                            "people": {"type": "string", "description": _PEOPLE_HINT},
                            "countries": {"type": "string", "description": _COUNTRIES_HINT},
                            "limit": {"type": "integer", "default": 50, "minimum": 1, "maximum": 100},
                        },
                    },
                },
                "dedup_by": {
                    "type": "string",
                    "enum": ["url", "id"],
                    "default": "url",
                    "description": "Deduplication key across merged results. 'url' (default) or 'id'.",
                },
                "limit_per_group": {
                    "type": "integer",
                    "default": 50,
                    "minimum": 1,
                    "maximum": 100,
                    "description": "Max results per group before merge.",
                },
            },
            "required": ["groups"],
        },
    },
    # 5 ─── list_subscriptions ────────────────────────────────────────────
    {
        "name": "list_subscriptions",
        "description": (
            "List all enabled feed subscriptions with 30-day item counts. "
            "Use to diagnose 0-result searches: check whether the relevant channel "
            "is subscribed and active. "
            "Also reveals which Twitter accounts, YouTube channels, and news sources "
            "are part of the user's feed."
        ),
        "inputSchema": {"type": "object", "properties": {}},
    },
    # 6 ─── list_sources ──────────────────────────────────────────────────
    {
        "name": "list_sources",
        "description": (
            "List available source types with whether they are in the user's Daily preset "
            "and how many items each has in the last 30 days. "
            "Use before filtering with sources= to confirm a source has active content."
        ),
        "inputSchema": {"type": "object", "properties": {}},
    },
    # 7 ─── report_latest ─────────────────────────────────────────────────
    {
        "name": "report_latest",
        "description": (
            "Get the latest completed daily report for the given kind. "
            "Use for: 'show me today's Twitter/YouTube daily'."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "kind": {
                    "type": "string",
                    "enum": ["twitter", "youtube"],
                    "description": "Report type: 'twitter' or 'youtube'.",
                }
            },
            "required": ["kind"],
        },
    },
    # 8 ─── report_by_date ────────────────────────────────────────────────
    {
        "name": "report_by_date",
        "description": "Get the daily report for a specific date.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "kind": {
                    "type": "string",
                    "enum": ["twitter", "youtube"],
                    "description": "Report type: 'twitter' or 'youtube'.",
                },
                "date": {
                    "type": "string",
                    "description": "Date in YYYY-MM-DD format. E.g. '2026-05-10'.",
                },
            },
            "required": ["kind", "date"],
        },
    },
    # 9 ─── report_list ───────────────────────────────────────────────────
    {
        "name": "report_list",
        "description": "List available daily reports with pagination.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "kind": {
                    "type": "string",
                    "enum": ["twitter", "youtube"],
                    "description": "Report type: 'twitter' or 'youtube'.",
                },
                "limit": {"type": "integer", "default": 30, "minimum": 1, "maximum": 100},
                "offset": {"type": "integer", "default": 0, "minimum": 0},
            },
            "required": ["kind"],
        },
    },
]


# ---------------------------------------------------------------------------
# Tool dispatch
# ---------------------------------------------------------------------------

def _call_tool(name: str, arguments: Dict[str, Any], client: Any) -> str:
    import httpx

    args = arguments or {}

    # Helper to build GET params, skipping None values
    def _gp(*keys):
        return {k: args[k] for k in keys if args.get(k) is not None}

    if name == "daily_feed":
        r = client.get(
            "/api/v1/agent/daily",
            params=_gp(
                "since",
                "q",
                "sources",
                "tickers",
                "tags",
                "author",
                "people",
                "countries",
                "limit",
                "cursor",
                "dedup",
            ),
        )
        r.raise_for_status()
        return r.text

    if name == "daily_item":
        rid = args["id"]
        r = client.get(f"/api/v1/agent/daily/{rid}")
        r.raise_for_status()
        return r.text

    if name == "daily_items_batch":
        ids = args.get("ids", [])
        r = client.post("/api/v1/agent/daily/batch", json={"ids": ids})
        r.raise_for_status()
        return r.text

    if name == "daily_multi_search":
        payload: Dict[str, Any] = {
            "groups": args.get("groups", []),
            "dedup_by": args.get("dedup_by", "url"),
            "limit_per_group": args.get("limit_per_group", 50),
        }
        r = client.post("/api/v1/agent/daily/multi", json=payload)
        r.raise_for_status()
        return r.text

    if name == "list_subscriptions":
        r = client.get("/api/v1/agent/subscriptions")
        r.raise_for_status()
        return r.text

    if name == "list_sources":
        r = client.get("/api/v1/agent/sources")
        r.raise_for_status()
        return r.text

    if name == "report_latest":
        r = client.get("/api/v1/agent/reports/latest", params={"kind": args["kind"]})
        r.raise_for_status()
        return r.text

    if name == "report_by_date":
        r = client.get(
            f"/api/v1/agent/reports/{args['date']}",
            params={"kind": args["kind"]},
        )
        r.raise_for_status()
        return r.text

    if name == "report_list":
        r = client.get(
            "/api/v1/agent/reports",
            params={
                "kind": args["kind"],
                "limit": args.get("limit", 30),
                "offset": args.get("offset", 0),
            },
        )
        r.raise_for_status()
        return r.text

    raise ValueError(f"unknown tool {name!r}")


# ---------------------------------------------------------------------------
# MCP server loop
# ---------------------------------------------------------------------------

def run() -> None:
    client: Any = None
    initialized = False

    def dispatch(msg: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        nonlocal client, initialized
        mid = msg.get("id")
        method = msg.get("method")
        params = msg.get("params") or {}
        if not isinstance(params, dict):
            params = {}

        if mid is None and isinstance(method, str) and method.startswith("notifications/"):
            return None

        if method == "initialize":
            initialized = True
            req_pv = params.get("protocolVersion") or PROTOCOL_VERSION
            return {
                "jsonrpc": "2.0",
                "id": mid,
                "result": {
                    "protocolVersion": req_pv,
                    "capabilities": {"tools": {"listChanged": False}},
                    "serverInfo": {
                        "name": "guanhe-mcp",
                        "version": SERVER_VERSION,
                        "title": "GuanHe Daily MCP",
                    },
                    "instructions": (
                        "This server provides read-only access to your GuanHe Daily feed, "
                        "feed subscriptions, uploaded files, and daily reports. "
                        "All tools are prefixed with the context of what they return. "
                        "Start with list_sources or list_subscriptions to understand your data coverage "
                        "before searching. Use daily_multi_search for multi-angle research tasks."
                    ),
                },
            }

        if method == "notifications/initialized":
            return None

        if method == "resources/list":
            return {"jsonrpc": "2.0", "id": mid, "result": {"resources": []}}
        if method == "prompts/list":
            return {"jsonrpc": "2.0", "id": mid, "result": {"prompts": []}}

        if method == "tools/list":
            return {
                "jsonrpc": "2.0",
                "id": mid,
                "result": {"tools": TOOLS},
            }

        if method == "tools/call":
            if not initialized:
                return {
                    "jsonrpc": "2.0",
                    "id": mid,
                    "error": {"code": -32002, "message": "Server not initialized"},
                }
            if client is None:
                try:
                    client = _client()
                except SystemExit:
                    raise
                except Exception as e:
                    return {
                        "jsonrpc": "2.0",
                        "id": mid,
                        "error": {
                            "code": -32603,
                            "message": (
                                f"Cannot init HTTP client: {e}. "
                                "Install dependencies: pip install httpx && "
                                "pip install -e tools/guanhe-mcp"
                            ),
                        },
                    }
            name = params.get("name")
            arguments = params.get("arguments") or {}
            try:
                import httpx

                text = _call_tool(str(name), dict(arguments), client)
                return {
                    "jsonrpc": "2.0",
                    "id": mid,
                    "result": {
                        "content": [{"type": "text", "text": text}],
                        "isError": False,
                    },
                }
            except httpx.HTTPStatusError as e:
                body = e.response.text if e.response is not None else str(e)
                return {
                    "jsonrpc": "2.0",
                    "id": mid,
                    "result": {
                        "content": [{"type": "text", "text": body}],
                        "isError": True,
                    },
                }
            except Exception as e:
                return {
                    "jsonrpc": "2.0",
                    "id": mid,
                    "error": {"code": -32603, "message": str(e)},
                }

        if method == "ping":
            return {"jsonrpc": "2.0", "id": mid, "result": {}}

        if isinstance(method, str) and method.startswith("$/"):
            if mid is not None:
                return {"jsonrpc": "2.0", "id": mid, "result": {}}
            return None

        if mid is not None:
            return {
                "jsonrpc": "2.0",
                "id": mid,
                "error": {"code": -32601, "message": f"Method not found: {method}"},
            }
        return None

    while True:
        try:
            raw = _read_message()
        except (EOFError, json.JSONDecodeError):
            break
        if raw is None:
            break

        if isinstance(raw, list):
            items = [m for m in raw if isinstance(m, dict)]
            if not items:
                sys.stderr.write("guanhe-mcp: empty batch array, ignoring.\n")
                sys.stderr.flush()
                continue
            out: List[Dict[str, Any]] = []
            for item in items:
                try:
                    resp = dispatch(item)
                    if resp is not None:
                        out.append(resp)
                except Exception as e:
                    import traceback

                    sys.stderr.write(f"guanhe-mcp: handler error: {e}\n")
                    traceback.print_exc(file=sys.stderr)
                    sys.stderr.flush()
                    mid_err = item.get("id")
                    if mid_err is not None:
                        out.append(
                            {
                                "jsonrpc": "2.0",
                                "id": mid_err,
                                "error": {"code": -32603, "message": str(e)},
                            }
                        )
            if out:
                _write_framed(out)
            continue

        if isinstance(raw, dict):
            try:
                resp = dispatch(raw)
                if resp is not None:
                    _write_framed(resp)
            except Exception as e:
                import traceback

                sys.stderr.write(f"guanhe-mcp: handler error: {e}\n")
                traceback.print_exc(file=sys.stderr)
                sys.stderr.flush()
                mid_err = raw.get("id")
                if mid_err is not None:
                    try:
                        _write_framed(
                            {
                                "jsonrpc": "2.0",
                                "id": mid_err,
                                "error": {"code": -32603, "message": str(e)},
                            }
                        )
                    except Exception:
                        pass
            continue

        sys.stderr.write(f"guanhe-mcp: unexpected message type {type(raw)}\n")
        sys.stderr.flush()
        break

    if client is not None:
        client.close()


def main() -> None:
    run()


if __name__ == "__main__":
    main()
