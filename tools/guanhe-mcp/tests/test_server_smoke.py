# -*- coding: utf-8 -*-
"""
Smoke tests for guanhe_mcp.server v0.2.

Strategy: patch httpx.Client so no real HTTP requests are made.
Each test sends a JSON-RPC message through the dispatch() function
and asserts the correct HTTP method / path / body was used.

Run:
    cd tools/guanhe-mcp
    pip install -e .
    pytest tests/test_server_smoke.py -v
"""

from __future__ import annotations

import importlib
import json
import sys
import types
from unittest.mock import MagicMock, patch, call

import pytest

# ---------------------------------------------------------------------------
# Import the module under test
# ---------------------------------------------------------------------------
import guanhe_mcp.server as srv


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_client_mock(response_text: str = '{"ok": true}') -> MagicMock:
    """Return a mock httpx.Client whose get/post return a fake response."""
    resp = MagicMock()
    resp.text = response_text
    resp.raise_for_status = MagicMock()

    client = MagicMock()
    client.get.return_value = resp
    client.post.return_value = resp
    return client


def _dispatch_tool(client: MagicMock, tool_name: str, arguments: dict) -> dict:
    """Exercise the inner dispatch() closure with a tools/call message."""
    # We reconstruct a minimal server state by replaying initialize then tools/call.
    captured_responses = []

    def fake_write(payload):
        captured_responses.append(payload)

    with patch.object(srv, '_write_message', side_effect=fake_write), \
         patch.object(srv, '_write_framed', side_effect=fake_write):

        # Manually call _call_tool (the part that actually hits HTTP)
        result_text = srv._call_tool(tool_name, arguments, client)

    return json.loads(result_text)


# ---------------------------------------------------------------------------
# Tool → HTTP mapping tests
# ---------------------------------------------------------------------------

class TestDailyFeed:
    def test_get_with_no_params(self):
        client = _make_client_mock()
        srv._call_tool("daily_feed", {}, client)
        client.get.assert_called_once()
        args, kwargs = client.get.call_args
        assert args[0] == "/api/v1/agent/daily"
        assert kwargs.get("params") == {}

    def test_get_with_all_params(self):
        client = _make_client_mock()
        srv._call_tool(
            "daily_feed",
            {"since": "14d", "q": "NVDA OR 英伟达", "sources": "twitter",
             "tickers": "NVDA.US", "tags": "AI", "author": "Bloomberg",
             "people": "Trump,Musk", "countries": "United States,China",
             "limit": 50, "cursor": "abc", "dedup": False},
            client,
        )
        _, kwargs = client.get.call_args
        p = kwargs["params"]
        assert p["since"] == "14d"
        assert p["q"] == "NVDA OR 英伟达"
        assert p["tickers"] == "NVDA.US"
        assert p["tags"] == "AI"
        assert p["author"] == "Bloomberg"
        assert p["people"] == "Trump,Musk"
        assert p["countries"] == "United States,China"
        assert p["dedup"] is False

    def test_none_params_excluded(self):
        client = _make_client_mock()
        srv._call_tool("daily_feed", {"since": "7d"}, client)
        _, kwargs = client.get.call_args
        p = kwargs["params"]
        assert "q" not in p
        assert "tickers" not in p


class TestDailyItem:
    def test_uses_item_id_in_path(self):
        client = _make_client_mock()
        srv._call_tool("daily_item", {"id": "feed_88247"}, client)
        client.get.assert_called_once_with("/api/v1/agent/daily/feed_88247")

    def test_upload_id(self):
        client = _make_client_mock()
        srv._call_tool("daily_item", {"id": "upload_123"}, client)
        client.get.assert_called_once_with("/api/v1/agent/daily/upload_123")


class TestDailyItemsBatch:
    def test_post_with_ids(self):
        client = _make_client_mock()
        ids = ["feed_1", "feed_2", "upload_3"]
        srv._call_tool("daily_items_batch", {"ids": ids}, client)
        client.post.assert_called_once()
        args, kwargs = client.post.call_args
        assert args[0] == "/api/v1/agent/daily/batch"
        assert kwargs["json"] == {"ids": ids}


class TestDailyMultiSearch:
    def test_post_with_groups(self):
        client = _make_client_mock()
        groups = [
            {"q": "NVDA OR 英伟达", "since": "7d"},
            {"q": "Greer OR USTR", "since": "14d"},
        ]
        srv._call_tool(
            "daily_multi_search",
            {"groups": groups, "dedup_by": "url", "limit_per_group": 50},
            client,
        )
        client.post.assert_called_once()
        args, kwargs = client.post.call_args
        assert args[0] == "/api/v1/agent/daily/multi"
        body = kwargs["json"]
        assert body["groups"] == groups
        assert body["dedup_by"] == "url"
        assert body["limit_per_group"] == 50

    def test_defaults_applied(self):
        client = _make_client_mock()
        srv._call_tool("daily_multi_search", {"groups": [{"q": "test"}]}, client)
        _, kwargs = client.post.call_args
        body = kwargs["json"]
        assert body["dedup_by"] == "url"
        assert body["limit_per_group"] == 50


class TestListSubscriptions:
    def test_get_correct_path(self):
        client = _make_client_mock()
        srv._call_tool("list_subscriptions", {}, client)
        client.get.assert_called_once_with("/api/v1/agent/subscriptions")


class TestListSources:
    def test_get_correct_path(self):
        client = _make_client_mock()
        srv._call_tool("list_sources", {}, client)
        client.get.assert_called_once_with("/api/v1/agent/sources")


class TestReportLatest:
    def test_twitter(self):
        client = _make_client_mock()
        srv._call_tool("report_latest", {"kind": "twitter"}, client)
        client.get.assert_called_once()
        args, kwargs = client.get.call_args
        assert args[0] == "/api/v1/agent/reports/latest"
        assert kwargs["params"] == {"kind": "twitter"}

    def test_youtube(self):
        client = _make_client_mock()
        srv._call_tool("report_latest", {"kind": "youtube"}, client)
        _, kwargs = client.get.call_args
        assert kwargs["params"] == {"kind": "youtube"}

    def test_market(self):
        client = _make_client_mock()
        srv._call_tool("report_latest", {"kind": "market"}, client)
        _, kwargs = client.get.call_args
        assert kwargs["params"] == {"kind": "market"}

    def test_market_weekly(self):
        client = _make_client_mock()
        srv._call_tool("report_latest", {"kind": "market_weekly"}, client)
        _, kwargs = client.get.call_args
        assert kwargs["params"] == {"kind": "market_weekly"}

    def test_theme(self):
        client = _make_client_mock()
        srv._call_tool("report_latest", {"kind": "theme"}, client)
        _, kwargs = client.get.call_args
        assert kwargs["params"] == {"kind": "theme"}

    def test_the_close(self):
        client = _make_client_mock()
        srv._call_tool("report_latest", {"kind": "the_close"}, client)
        _, kwargs = client.get.call_args
        assert kwargs["params"] == {"kind": "the_close"}

    def test_closing_bell(self):
        client = _make_client_mock()
        srv._call_tool("report_latest", {"kind": "closing_bell"}, client)
        _, kwargs = client.get.call_args
        assert kwargs["params"] == {"kind": "closing_bell"}

    def test_bloomberg_surveillance(self):
        client = _make_client_mock()
        srv._call_tool("report_latest", {"kind": "bloomberg_surveillance"}, client)
        _, kwargs = client.get.call_args
        assert kwargs["params"] == {"kind": "bloomberg_surveillance"}


class TestReportByDate:
    def test_path_and_params(self):
        client = _make_client_mock()
        srv._call_tool("report_by_date", {"kind": "twitter", "date": "2026-05-10"}, client)
        client.get.assert_called_once()
        args, kwargs = client.get.call_args
        assert args[0] == "/api/v1/agent/reports/2026-05-10"
        assert kwargs["params"]["kind"] == "twitter"

    def test_market_weekly(self):
        client = _make_client_mock()
        srv._call_tool("report_by_date", {"kind": "market_weekly", "date": "2026-06-13"}, client)
        args, kwargs = client.get.call_args
        assert args[0] == "/api/v1/agent/reports/2026-06-13"
        assert kwargs["params"]["kind"] == "market_weekly"


class TestReportList:
    def test_default_pagination(self):
        client = _make_client_mock()
        srv._call_tool("report_list", {"kind": "youtube"}, client)
        _, kwargs = client.get.call_args
        assert kwargs["params"]["kind"] == "youtube"
        assert kwargs["params"]["limit"] == 30
        assert kwargs["params"]["offset"] == 0

    def test_custom_pagination(self):
        client = _make_client_mock()
        srv._call_tool("report_list", {"kind": "twitter", "limit": 10, "offset": 20}, client)
        _, kwargs = client.get.call_args
        assert kwargs["params"]["limit"] == 10
        assert kwargs["params"]["offset"] == 20

    def test_theme(self):
        client = _make_client_mock()
        srv._call_tool("report_list", {"kind": "theme"}, client)
        _, kwargs = client.get.call_args
        assert kwargs["params"]["kind"] == "theme"
        assert kwargs["params"]["limit"] == 30
        assert kwargs["params"]["offset"] == 0


class TestUnknownTool:
    def test_raises_value_error(self):
        client = _make_client_mock()
        with pytest.raises(ValueError, match="unknown tool"):
            srv._call_tool("nonexistent_tool", {}, client)


# ---------------------------------------------------------------------------
# Tool listing
# ---------------------------------------------------------------------------

class TestToolsList:
    def test_twelve_tools_defined(self):
        assert len(srv.TOOLS) == 12

    def test_tool_names(self):
        names = {t["name"] for t in srv.TOOLS}
        expected = {
            "daily_feed", "daily_item", "daily_items_batch", "daily_multi_search",
            "list_subscriptions", "list_sources",
            "report_latest", "report_by_date", "report_list",
            "semantic_search", "hybrid_search", "chunks_by_locator",
        }
        assert names == expected

    def test_no_legacy_tool_names(self):
        names = {t["name"] for t in srv.TOOLS}
        legacy = {"search", "twitter_daily_latest", "twitter_daily_by_date",
                  "twitter_daily_list", "youtube_daily_latest", "youtube_daily_by_date"}
        assert names.isdisjoint(legacy), f"Legacy tools still present: {names & legacy}"

    def test_every_tool_has_description(self):
        for tool in srv.TOOLS:
            assert tool.get("description"), f"Tool {tool['name']} missing description"

    def test_required_fields_declared(self):
        tool = next(t for t in srv.TOOLS if t["name"] == "daily_item")
        assert "id" in tool["inputSchema"].get("required", [])

        tool = next(t for t in srv.TOOLS if t["name"] == "report_latest")
        assert "kind" in tool["inputSchema"].get("required", [])

        tool = next(t for t in srv.TOOLS if t["name"] == "daily_items_batch")
        assert "ids" in tool["inputSchema"].get("required", [])

    def test_report_kinds_canonical(self):
        """All three report tools expose the 8 canonical kinds."""
        expected = [
            "twitter_daily", "youtube_daily", "market_daily", "market_weekly",
            "theme", "the_close", "closing_bell", "bloomberg_surveillance",
        ]
        for name in ("report_latest", "report_by_date", "report_list"):
            tool = next(t for t in srv.TOOLS if t["name"] == name)
            kind_prop = tool["inputSchema"]["properties"]["kind"]
            assert kind_prop["enum"] == expected, f"{name} enum mismatch: {kind_prop['enum']}"


# ---------------------------------------------------------------------------
# Initialize response
# ---------------------------------------------------------------------------

class TestInitialize:
    def test_server_info_fields(self):
        """initialize result must include title and instructions (for host tool registration).

        We drive the server via subprocess so we don't need to patch pytest's stdin.
        """
        import subprocess
        import os

        msg_init = json.dumps({
            "jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {"protocolVersion": "2024-11-05", "capabilities": {}},
        })
        msg_notify = json.dumps({"jsonrpc": "2.0", "method": "notifications/initialized"})

        # Locate the package root (tools/guanhe-mcp) so the subprocess can import guanhe_mcp.
        pkg_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

        env = os.environ.copy()
        env["GUANHE_TOKEN"] = "ght_test_fake"
        env["GUANHE_BASE_URL"] = "http://localhost:9999"
        env["PYTHONUNBUFFERED"] = "1"
        env["PYTHONPATH"] = pkg_root + os.pathsep + env.get("PYTHONPATH", "")

        proc = subprocess.Popen(
            [sys.executable, "-m", "guanhe_mcp.server"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
            cwd=pkg_root,
            text=True,
        )
        stdin_data = msg_init + "\n" + msg_notify + "\n"
        try:
            stdout, _ = proc.communicate(input=stdin_data, timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            stdout, _ = proc.communicate()

        lines = [l.strip() for l in stdout.splitlines() if l.strip()]
        assert lines, "Server produced no output"

        init_resp = json.loads(lines[0])
        result = init_resp.get("result", {})
        assert "serverInfo" in result, "serverInfo missing from initialize response"
        assert result["serverInfo"].get("title"), "serverInfo.title must be set"
        assert result.get("instructions"), "instructions must be set"

    def test_version_is_0_5_0(self):
        assert srv.SERVER_VERSION == "0.5.0"

    def test_daily_feed_has_people_countries_hints(self):
        tool = next(t for t in srv.TOOLS if t["name"] == "daily_feed")
        props = tool["inputSchema"]["properties"]
        assert "people" in props
        assert "countries" in props
        assert "Trump" in props["people"]["description"]
