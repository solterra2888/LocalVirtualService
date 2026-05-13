# -*- coding: utf-8 -*-
"""Unit tests for Agent PAT helpers (no DB)."""

from datetime import datetime, timezone

from backend.services.agent_service import _decode_cursor, _encode_cursor, _parse_since
from backend.services.personal_token_service import PersonalTokenService


def test_cursor_roundtrip():
    assert _decode_cursor(None) == 0
    assert _decode_cursor("") == 0
    c = _encode_cursor(40)
    assert _decode_cursor(c) == 40


def test_parse_since_keywords():
    df, dt = _parse_since("24h")
    assert dt is None
    assert df.tzinfo == timezone.utc
    df7, dt7 = _parse_since("7d")
    assert dt7 is None
    assert df7.tzinfo == timezone.utc
    assert df7 < datetime.now(timezone.utc)


def test_parse_since_iso():
    df, dt = _parse_since("2026-01-01T00:00:00Z")
    assert dt is None
    assert df.year == 2026


def test_normalize_scopes_default():
    assert PersonalTokenService._normalize_scopes(None) == ["daily:read", "reports:read"]
    assert PersonalTokenService._normalize_scopes([]) == ["daily:read", "reports:read"]
    assert PersonalTokenService._normalize_scopes(["daily:read"]) == ["daily:read"]
    assert PersonalTokenService._normalize_scopes(["daily:read", "bogus"]) == ["daily:read"]
    assert PersonalTokenService._normalize_scopes(["bogus"]) == ["daily:read", "reports:read"]
