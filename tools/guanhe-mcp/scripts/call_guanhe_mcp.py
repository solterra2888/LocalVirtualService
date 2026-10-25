#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Smoke-test guanhe-mcp from the command line.

Spawns the stdio MCP server as a subprocess, performs the initialize
handshake, invokes one tool, and prints the raw JSON-RPC response.

Usage:
    export GUANHE_TOKEN="ght_..."
    export GUANHE_BASE_URL="https://www.guanhecapital.hk"
    python scripts/call_guanhe_mcp.py <tool> '<json-args>'

Examples:
    python scripts/call_guanhe_mcp.py daily_feed '{"since": "24h", "limit": 5}'
    python scripts/call_guanhe_mcp.py report_latest '{"kind": "market_daily"}'
    python scripts/call_guanhe_mcp.py semantic_search '{"query": "margin of safety", "top_k": 5}'

Exit codes: 0 = tool succeeded, 1 = tool reported isError / no response,
2 = bad command line.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys

# Keep in sync with guanhe_mcp/server.py
PROTOCOL_VERSION = "2024-11-05"
TOOL_CALL_ID = 2


def main() -> int:
    if len(sys.argv) < 2:
        sys.stderr.write(__doc__)
        return 2
    tool = sys.argv[1]
    raw_args = sys.argv[2] if len(sys.argv) > 2 else "{}"
    try:
        arguments = json.loads(raw_args)
    except json.JSONDecodeError as e:
        sys.stderr.write(f"Invalid JSON args: {e}\n")
        return 2

    if not os.environ.get("GUANHE_TOKEN") or not os.environ.get("GUANHE_BASE_URL"):
        sys.stderr.write("GUANHE_TOKEN and GUANHE_BASE_URL must be set.\n")
        return 2

    messages = [
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {"protocolVersion": PROTOCOL_VERSION, "capabilities": {}},
        },
        {"jsonrpc": "2.0", "method": "notifications/initialized"},
        {
            "jsonrpc": "2.0",
            "id": TOOL_CALL_ID,
            "method": "tools/call",
            "params": {"name": tool, "arguments": arguments},
        },
    ]
    payload = "".join(json.dumps(m, ensure_ascii=False) + "\n" for m in messages)

    pkg_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    env = os.environ.copy()
    env["PYTHONPATH"] = pkg_root + os.pathsep + env.get("PYTHONPATH", "")
    env.setdefault("PYTHONUNBUFFERED", "1")

    try:
        proc = subprocess.run(
            [sys.executable, "-m", "guanhe_mcp.server"],
            input=payload,
            capture_output=True,
            text=True,
            env=env,
            cwd=pkg_root,
            timeout=180,
        )
    except subprocess.TimeoutExpired:
        sys.stderr.write("Timed out waiting for the MCP server.\n")
        return 1

    if proc.stderr:
        sys.stderr.write(proc.stderr)

    for line in proc.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            msg = json.loads(line)
        except json.JSONDecodeError:
            continue
        if msg.get("id") == TOOL_CALL_ID:
            print(json.dumps(msg, ensure_ascii=False, indent=2))
            if "error" in msg:
                return 1
            return 0 if not msg.get("result", {}).get("isError") else 1

    sys.stderr.write("No tools/call response received.\n")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
