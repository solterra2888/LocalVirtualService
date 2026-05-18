# -*- coding: utf-8 -*-
"""
Minimal test skeleton for Agent v1 HTTP API.

Strategy: monkeypatch AgentService so no real DB connections are made.
Each test verifies HTTP-layer behaviour (routing, query-param parsing,
response shape) independently of the database.

Run:
    pip install pytest httpx fastapi
    pytest backend/tests/test_agent_v1.py -v
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

# ---------------------------------------------------------------------------
# App bootstrap — import lazily to avoid DB connections at module load time
# ---------------------------------------------------------------------------

def _build_test_client():
    """Import the FastAPI app and return a TestClient with auth bypassed."""
    from fastapi import FastAPI
    from backend.api import agent_v1

    app = FastAPI()
    app.include_router(agent_v1.router)
    return TestClient(app, raise_server_exceptions=True)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

FAKE_TOKEN_DATA = MagicMock()
FAKE_TOKEN_DATA.user_id = 1
FAKE_TOKEN_DATA.username = "testuser"


def _mock_item(id_: str = "feed_1") -> Dict[str, Any]:
    return {
        "id": id_,
        "title": "Test item",
        "summary": "Summary",
        "url": "https://example.com",
        "source": "twitter",
        "author": "@test",
        "published_at": datetime(2026, 5, 18, 0, 0, tzinfo=timezone.utc),
        "tickers": ["NVDA.US"],
        "tags": ["AI"],
    }


def _mock_feed_response(items=None, total=0, diagnostics=None):
    from backend.schemas.agent import AgentDailyFeedResponse, AgentDailyItem

    mock_items = [AgentDailyItem(**_mock_item(f"feed_{i}")) for i in range(len(items or []))]
    return AgentDailyFeedResponse(
        items=mock_items,
        total=total or len(mock_items),
        has_more=False,
        diagnostics=diagnostics,
    )


# ---------------------------------------------------------------------------
# Helper: patch auth + service together
# ---------------------------------------------------------------------------

from contextlib import contextmanager


@contextmanager
def _patched_service(method_name: str, return_value: Any):
    """Patch auth to pass through and AgentService.<method_name> to return return_value."""
    from backend.auth import pat_auth

    with patch.object(
        pat_auth, "require_pat_scopes", return_value=lambda: FAKE_TOKEN_DATA
    ):
        with patch(
            f"backend.api.agent_v1.AgentService.{method_name}",
            return_value=return_value,
        ) as mock_method:
            yield mock_method


# ---------------------------------------------------------------------------
# Tests: GET /api/v1/agent/daily
# ---------------------------------------------------------------------------

class TestDailyFeedEndpoint:
    def test_returns_200(self):
        client = _build_test_client()
        with _patched_service("get_daily_feed", _mock_feed_response(items=["x"], total=1)):
            resp = client.get(
                "/api/v1/agent/daily",
                params={"since": "7d"},
                headers={"Authorization": "Bearer ght_test"},
            )
        assert resp.status_code == 200
        data = resp.json()
        assert "items" in data
        assert "total" in data

    def test_tickers_multi_value_passed_through(self):
        """tickers=NVDA.US,AAPL.US should reach the service as a single comma-separated string."""
        client = _build_test_client()
        with _patched_service("get_daily_feed", _mock_feed_response()) as mock_svc:
            client.get(
                "/api/v1/agent/daily",
                params={"tickers": "NVDA.US,AAPL.US"},
                headers={"Authorization": "Bearer ght_test"},
            )
        call_kwargs = mock_svc.call_args[1]
        assert call_kwargs.get("tickers") == "NVDA.US,AAPL.US"

    def test_tags_param_passed_through(self):
        client = _build_test_client()
        with _patched_service("get_daily_feed", _mock_feed_response()) as mock_svc:
            client.get(
                "/api/v1/agent/daily",
                params={"tags": "AI,Chips"},
                headers={"Authorization": "Bearer ght_test"},
            )
        call_kwargs = mock_svc.call_args[1]
        assert call_kwargs.get("tags") == "AI,Chips"

    def test_author_param_passed_through(self):
        client = _build_test_client()
        with _patched_service("get_daily_feed", _mock_feed_response()) as mock_svc:
            client.get(
                "/api/v1/agent/daily",
                params={"author": "Bloomberg"},
                headers={"Authorization": "Bearer ght_test"},
            )
        call_kwargs = mock_svc.call_args[1]
        assert call_kwargs.get("author") == "Bloomberg"

    def test_dedup_default_true(self):
        client = _build_test_client()
        with _patched_service("get_daily_feed", _mock_feed_response()) as mock_svc:
            client.get(
                "/api/v1/agent/daily",
                headers={"Authorization": "Bearer ght_test"},
            )
        call_kwargs = mock_svc.call_args[1]
        assert call_kwargs.get("dedup") is True

    def test_dedup_can_be_false(self):
        client = _build_test_client()
        with _patched_service("get_daily_feed", _mock_feed_response()) as mock_svc:
            client.get(
                "/api/v1/agent/daily",
                params={"dedup": "false"},
                headers={"Authorization": "Bearer ght_test"},
            )
        call_kwargs = mock_svc.call_args[1]
        assert call_kwargs.get("dedup") is False


# ---------------------------------------------------------------------------
# Tests: _parse_since in service layer
# ---------------------------------------------------------------------------

class TestParseSince:
    """Unit-test _parse_since directly — no HTTP overhead."""

    def _call(self, since):
        from backend.services.agent_service import _parse_since
        return _parse_since(since)

    def test_none_returns_7d_default(self):
        from datetime import timedelta
        df, dt = self._call(None)
        assert dt is None
        delta = datetime.now(timezone.utc) - df
        assert timedelta(days=6, hours=23) < delta < timedelta(days=7, minutes=1)

    def test_24h(self):
        from datetime import timedelta
        df, _ = self._call("24h")
        assert timedelta(hours=23, minutes=59) < (datetime.now(timezone.utc) - df) < timedelta(hours=24, minutes=1)

    def test_14d(self):
        from datetime import timedelta
        df, _ = self._call("14d")
        delta = datetime.now(timezone.utc) - df
        assert timedelta(days=13, hours=23) < delta < timedelta(days=14, minutes=1)

    def test_2w(self):
        from datetime import timedelta
        df, _ = self._call("2w")
        delta = datetime.now(timezone.utc) - df
        assert timedelta(days=13, hours=23) < delta < timedelta(days=14, minutes=1)

    def test_48h(self):
        from datetime import timedelta
        df, _ = self._call("48h")
        delta = datetime.now(timezone.utc) - df
        assert timedelta(hours=47, minutes=59) < delta < timedelta(hours=48, minutes=1)

    def test_iso_datetime(self):
        df, dt = self._call("2026-01-01T00:00:00Z")
        assert df.year == 2026
        assert df.month == 1
        assert df.day == 1
        assert dt is None

    def test_invalid_raises_400(self):
        from fastapi import HTTPException
        with pytest.raises(HTTPException) as exc_info:
            self._call("yesterday")
        assert exc_info.value.status_code == 400

    def test_invalid_raises_400_for_number_only(self):
        from fastapi import HTTPException
        with pytest.raises(HTTPException):
            self._call("14")  # missing unit letter

    def test_invalid_raises_400_for_unsupported_unit(self):
        from fastapi import HTTPException
        with pytest.raises(HTTPException):
            self._call("2m")  # months not supported


# ---------------------------------------------------------------------------
# Tests: GET /api/v1/agent/daily with diagnostics
# ---------------------------------------------------------------------------

class TestDiagnosticsInResponse:
    def test_diagnostics_present_when_zero_results(self):
        from backend.schemas.agent import AgentSearchDiagnostics

        diag = AgentSearchDiagnostics(
            parsed_q_groups=[{"op": "and", "terms": ["重要", "访华"]}],
            terms_in_corpus={"重要": 0, "访华": 47},
            since_used="2026-05-11T00:00:00+00:00",
            sources_used=["twitter", "youtube_feed"],
            hint="Term(s) ['重要'] have 0 matches anywhere.",
        )
        resp_obj = _mock_feed_response(items=[], total=0, diagnostics=diag)

        client = _build_test_client()
        with _patched_service("get_daily_feed", resp_obj):
            resp = client.get(
                "/api/v1/agent/daily",
                params={"q": "重要 访华"},
                headers={"Authorization": "Bearer ght_test"},
            )
        data = resp.json()
        assert data["total"] == 0
        assert data["diagnostics"] is not None
        assert "terms_in_corpus" in data["diagnostics"]
        assert data["diagnostics"]["terms_in_corpus"]["重要"] == 0

    def test_diagnostics_null_when_results_found(self):
        resp_obj = _mock_feed_response(items=["x"], total=1)
        client = _build_test_client()
        with _patched_service("get_daily_feed", resp_obj):
            resp = client.get(
                "/api/v1/agent/daily",
                headers={"Authorization": "Bearer ght_test"},
            )
        data = resp.json()
        assert data["diagnostics"] is None


# ---------------------------------------------------------------------------
# Tests: POST /api/v1/agent/daily/multi
# ---------------------------------------------------------------------------

class TestMultiSearch:
    def test_dedup_removes_duplicates(self):
        """Service-level dedup: same URL across two groups should appear once."""
        from backend.services.agent_service import AgentService
        from backend.schemas.agent import (
            AgentDailyItem,
            AgentDailyMultiResponse,
            AgentMultiGroupStats,
            AgentMultiSearchRequest,
        )

        item_a = AgentDailyItem(**_mock_item("feed_1"))
        item_a.url = "https://example.com/shared"
        item_b = AgentDailyItem(**_mock_item("feed_2"))
        item_b.url = "https://example.com/unique"
        item_c = AgentDailyItem(**_mock_item("feed_1"))  # same id + url as item_a
        item_c.url = "https://example.com/shared"

        # Simulate two groups returning overlapping results
        group_responses = [
            _mock_feed_response(items=["a"], total=1),
            _mock_feed_response(items=["b"], total=1),
        ]
        group_responses[0].items = [item_a]
        group_responses[1].items = [item_b, item_c]

        svc = AgentService.__new__(AgentService)

        call_count = [0]

        def fake_get_daily_feed(user, **kwargs):
            resp = group_responses[call_count[0]]
            call_count[0] += 1
            return resp

        svc.get_daily_feed = fake_get_daily_feed

        req = AgentMultiSearchRequest(
            groups=[
                {"q": "group1"},
                {"q": "group2"},
            ],
            dedup_by="url",
            limit_per_group=50,
        )
        result = svc.multi_search(FAKE_TOKEN_DATA, req)

        # shared URL should appear only once
        urls = [item.url for item in result.items]
        assert urls.count("https://example.com/shared") == 1
        assert "https://example.com/unique" in urls
        assert result.total_merged == 2

    def test_per_group_stats_length_matches_groups(self):
        from backend.services.agent_service import AgentService
        from backend.schemas.agent import AgentMultiSearchRequest

        group_responses = [
            _mock_feed_response(items=["a"] * 3, total=3),
            _mock_feed_response(items=["b"] * 5, total=5),
        ]
        for i, r in enumerate(group_responses):
            r.items = [AgentDailyItem(**_mock_item(f"item_{i}_{j}")) for j in range(len(r.items))]

        svc = AgentService.__new__(AgentService)
        call_count = [0]

        def fake_feed(user, **kwargs):
            r = group_responses[call_count[0]]
            call_count[0] += 1
            return r

        svc.get_daily_feed = fake_feed

        req = AgentMultiSearchRequest(
            groups=[{"q": "a"}, {"q": "b"}],
            dedup_by="id",
        )
        result = svc.multi_search(FAKE_TOKEN_DATA, req)
        assert len(result.per_group) == 2
        assert result.per_group[0].total == 3
        assert result.per_group[1].total == 5


# ---------------------------------------------------------------------------
# Tests: 410 Gone stubs
# ---------------------------------------------------------------------------

class TestGoneStubs:
    def test_search_returns_410(self):
        client = _build_test_client()
        resp = client.get("/api/v1/agent/search", params={"q": "test"})
        assert resp.status_code == 410
        assert "replacement" in resp.json()

    def test_twitter_daily_returns_410(self):
        client = _build_test_client()
        resp = client.get("/api/v1/agent/reports/twitter-daily/latest")
        assert resp.status_code == 410

    def test_youtube_daily_returns_410(self):
        client = _build_test_client()
        resp = client.get("/api/v1/agent/reports/youtube-daily/latest")
        assert resp.status_code == 410


# ---------------------------------------------------------------------------
# Tests: POST /api/v1/agent/daily/batch
# ---------------------------------------------------------------------------

class TestBatchEndpoint:
    def test_rejects_more_than_10_ids(self):
        """Pydantic should reject ids list > 10 before service is called."""
        client = _build_test_client()
        too_many_ids = [f"feed_{i}" for i in range(11)]
        resp = client.post(
            "/api/v1/agent/daily/batch",
            json={"ids": too_many_ids},
            headers={"Authorization": "Bearer ght_test"},
        )
        # Pydantic v2 validates max_length; expect 422
        assert resp.status_code == 422

    def test_accepts_up_to_10_ids(self):
        from backend.schemas.agent import AgentDailyBatchResponse, AgentDailyItemDetail

        mock_items = [
            AgentDailyItemDetail(**_mock_item(f"feed_{i}"))
            for i in range(3)
        ]
        batch_resp = AgentDailyBatchResponse(items=mock_items, errors={})

        client = _build_test_client()
        with _patched_service("get_daily_items_batch", batch_resp):
            resp = client.post(
                "/api/v1/agent/daily/batch",
                json={"ids": ["feed_1", "feed_2", "feed_3"]},
                headers={"Authorization": "Bearer ght_test"},
            )
        assert resp.status_code == 200
        data = resp.json()
        assert "items" in data
        assert "errors" in data
