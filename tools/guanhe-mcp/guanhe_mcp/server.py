# -*- coding: utf-8 -*-
"""
Minimal MCP stdio server (JSON-RPC over NDJSON) for GuanHe Agent API.
No dependency on the official `mcp` PyPI package — only httpx.
Wire format: one JSON object per line (NDJSON), matching the MCP Python SDK and Cursor.
"""

from __future__ import annotations

import json
import os
import sys
from typing import Any, Dict, List, Optional, Union

PROTOCOL_VERSION = "2024-11-05"


def _env() -> tuple[str, str]:
    base = os.environ.get("GUANHE_BASE_URL", "").rstrip("/")
    token = os.environ.get("GUANHE_TOKEN", "").strip()
    if not base or not token:
        sys.stderr.write("GUANHE_BASE_URL and GUANHE_TOKEN must be set.\n")
        sys.exit(1)
    return base, token


def _client():
    """Lazy import httpx so MCP handshake works even if import order differs."""
    import httpx

    base, token = _env()
    return httpx.Client(
        base_url=base,
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
        timeout=120.0,
    )


def _read_message() -> Optional[Union[Dict[str, Any], List[Any]]]:
    """Read one MCP NDJSON message from stdin (one JSON value per line).
    Also handles Content-Length framing for compatibility with older clients."""
    line = sys.stdin.buffer.readline()
    if not line:
        return None
    # Strip BOM if present
    if line.startswith(b"\xef\xbb\xbf"):
        line = line[3:]
    # Skip blank lines
    stripped = line.strip()
    if not stripped:
        return _read_message()
    # Content-Length framing (LSP-style) — consume header + blank + body
    if stripped.lower().startswith(b"content-length:"):
        n = int(stripped.split(b":", 1)[1].strip())
        sys.stdin.buffer.readline()  # blank separator
        body = sys.stdin.buffer.read(n)
        return json.loads(body.decode("utf-8"))
    # NDJSON (one JSON per line) — standard MCP Python SDK format
    return json.loads(stripped.decode("utf-8"))


def _write_message(payload: Union[Dict[str, Any], List[Any]]) -> None:
    """Write one MCP message as NDJSON (JSON + newline), matching the MCP Python SDK."""
    sys.stdout.write(json.dumps(payload, ensure_ascii=False) + "\n")
    sys.stdout.flush()


# Keep old name as alias for backward compat within this file
_write_framed = _write_message


TOOLS: List[Dict[str, Any]] = [
    {
        "name": "daily_feed",
        "description": (
            "Merged Daily timeline (uploads + feeds). "
            "Optional since (24h|7d|30d|ISO), sources (comma subtypes), ticker, limit, cursor. "
            "q syntax: space = AND, OR (uppercase) = any match, \"phrase\" = exact. "
            "E.g. q=`美国 OR 访华` returns items matching either word."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "since": {"type": "string"},
                "sources": {"type": "string"},
                "q": {"type": "string"},
                "ticker": {"type": "string"},
                "limit": {"type": "integer", "default": 20},
                "cursor": {"type": "string"},
            },
        },
    },
    {
        "name": "daily_item",
        "description": (
            "Drill down into a single Daily item to read its full body and transcript. "
            "Pass `id` from a prior daily_feed / search result (e.g. 'upload_123' or 'feed_456'). "
            "Returns the same fields as daily_feed plus: `content` (main body — "
            "file_processing_details.content for uploads, subscription_content.content for "
            "twitter/news, the YouTube description for youtube_feed) and `transcript` "
            "(YouTube transcript_cleaned or flattened ASR for audio/youtube uploads). "
            "Use this whenever the user asks for 'details', 'full text', 'original', "
            "'transcript', or asks a follow-up question that the list-level summary can't answer."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "id": {
                    "type": "string",
                    "description": "Item id from a prior daily_feed/search response, e.g. 'feed_88247' or 'upload_123'.",
                }
            },
            "required": ["id"],
        },
    },
    {
        "name": "search",
        "description": (
            "Search Daily content (title / summary / body). "
            "q syntax: space = AND (all terms must match), "
            "OR (uppercase) = any term matches, "
            "double-quoted phrase = exact substring. "
            "Examples: `美国 访华` (both words), "
            "`美国 OR 访华 OR Trump` (any word), "
            '`"Air Force One" China` (phrase + word). '
            "IMPORTANT: pass one short keyword or a few keywords joined by OR — "
            "never pass a full sentence or a long Boolean expression as a single token."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "q": {"type": "string"},
                "since": {"type": "string"},
                "sources": {"type": "string"},
                "ticker": {"type": "string"},
                "limit": {"type": "integer", "default": 20},
                "cursor": {"type": "string"},
            },
            "required": ["q"],
        },
    },
    {
        "name": "twitter_daily_latest",
        "description": "Latest completed Twitter daily report for the user.",
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "twitter_daily_by_date",
        "description": "Twitter daily report for YYYY-MM-DD.",
        "inputSchema": {
            "type": "object",
            "properties": {"date": {"type": "string"}},
            "required": ["date"],
        },
    },
    {
        "name": "twitter_daily_list",
        "description": "List Twitter daily reports (pagination).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "limit": {"type": "integer", "default": 30},
                "offset": {"type": "integer", "default": 0},
            },
        },
    },
    {
        "name": "youtube_daily_latest",
        "description": "Latest completed YouTube daily report.",
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "youtube_daily_by_date",
        "description": "YouTube daily report for YYYY-MM-DD.",
        "inputSchema": {
            "type": "object",
            "properties": {"date": {"type": "string"}},
            "required": ["date"],
        },
    },
]


def _call_tool(name: str, arguments: Dict[str, Any], client: Any) -> str:
    args = arguments or {}
    if name == "daily_feed":
        r = client.get(
            "/api/v1/agent/daily",
            params={k: args[k] for k in ("since", "sources", "q", "ticker", "limit", "cursor") if args.get(k) is not None},
        )
        r.raise_for_status()
        return r.text
    if name == "daily_item":
        rid = args["id"]
        r = client.get(f"/api/v1/agent/daily/{rid}")
        r.raise_for_status()
        return r.text
    if name == "search":
        r = client.get(
            "/api/v1/agent/search",
            params={k: args[k] for k in ("q", "since", "sources", "ticker", "limit", "cursor") if args.get(k) is not None},
        )
        r.raise_for_status()
        return r.text
    if name == "twitter_daily_latest":
        r = client.get("/api/v1/agent/reports/twitter-daily/latest")
        r.raise_for_status()
        return r.text
    if name == "twitter_daily_by_date":
        r = client.get(f"/api/v1/agent/reports/twitter-daily/{args['date']}")
        r.raise_for_status()
        return r.text
    if name == "twitter_daily_list":
        r = client.get(
            "/api/v1/agent/reports/twitter-daily",
            params={"limit": args.get("limit", 30), "offset": args.get("offset", 0)},
        )
        r.raise_for_status()
        return r.text
    if name == "youtube_daily_latest":
        r = client.get("/api/v1/agent/reports/youtube-daily/latest")
        r.raise_for_status()
        return r.text
    if name == "youtube_daily_by_date":
        r = client.get(f"/api/v1/agent/reports/youtube-daily/{args['date']}")
        r.raise_for_status()
        return r.text
    raise ValueError(f"unknown tool {name}")


def run() -> None:
    # Defer HTTP client until first tools/call so initialize / tools/list work without httpx
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
                    "serverInfo": {"name": "guanhe-mcp", "version": "0.1.0"},
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
                                "In rcTool run: pip install httpx && pip install -e tools/guanhe-mcp"
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
            # JSON-RPC batch: MCP requires one framed message whose body is a JSON array of responses.
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
