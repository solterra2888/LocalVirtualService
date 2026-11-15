# -*- coding: utf-8 -*-
"""Substack archive cursor 分页 + URL 合法性校验单元测试。

背景（2026-11）：Substack /api/v1/archive 废弃 offset 分页——请求带 offset 一律
400 Bad Request；新返回结构为 {"posts": [...], "nextCursor": "<base64>"}，
翻页改用 cursor 查询参数（nextCursor 为 base64，含 + / =，需整体 urlencode）。
"""

import pytest
import requests

from backend.services.substack_subscription_service import SubstackClient
from backend.services.substack_credential_service import normalize_publication_url


class _FakeResp:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        pass

    def json(self):
        return self._payload


def _make_posts(start, count):
    return [
        {"id": i, "canonical_url": f"https://x.substack.com/p/{i}"}
        for i in range(start, start + count)
    ]


class TestCursorPagination:
    def test_new_contract_multi_page(self, monkeypatch):
        """新契约：{posts, nextCursor} 多页聚合，cursor 需 urlencode，永不带 offset。"""
        pages = [
            {"posts": _make_posts(0, 15), "nextCursor": "Y3Vyc29yMTIz+/=="},
            {"posts": _make_posts(15, 5)},  # 末页无 nextCursor
        ]
        calls = []

        def fake_get(url, timeout=None):
            calls.append(url)
            return _FakeResp(pages[0] if "cursor=" not in url else pages[1])

        monkeypatch.setattr(requests, "get", fake_get)
        client = SubstackClient(None, "https://x.substack.com")
        items = client._paginate_archive("https://x.substack.com", 30, timeout=5)

        assert len(items) == 20
        assert items[0]["id"] == 0 and items[-1]["id"] == 19
        # 新契约下任何请求都不应再带 offset
        assert all("offset" not in c for c in calls)
        # base64 游标（含 + / =）必须整体 urlencode
        assert calls[1].endswith("cursor=Y3Vyc29yMTIz%2B%2F%3D%3D")

    def test_new_contract_stops_when_no_cursor(self, monkeypatch):
        """满页但响应无 nextCursor → 到达末页，停止翻页。"""
        monkeypatch.setattr(
            requests, "get", lambda url, timeout=None: _FakeResp({"posts": _make_posts(0, 15)})
        )
        client = SubstackClient(None, "https://x.substack.com")
        assert len(client._paginate_archive("https://x.substack.com", 30, timeout=5)) == 15

    def test_legacy_array_contract_still_paginates(self, monkeypatch):
        """旧契约兜底：纯数组响应时继续用 offset 翻页。"""
        pages = [
            _make_posts(0, 15),
            _make_posts(15, 7),  # 不足一页 → 停
        ]
        calls = []

        def fake_get(url, timeout=None):
            calls.append(url)
            return _FakeResp(pages[0] if "offset=" not in url else pages[1])

        monkeypatch.setattr(requests, "get", fake_get)
        client = SubstackClient(None, "https://x.substack.com")
        assert len(client._paginate_archive("https://x.substack.com", 30, timeout=5)) == 22
        assert "offset=15" in calls[1]

    def test_empty_or_malformed_payload(self, monkeypatch):
        """空 posts / 非 dict 非 list 响应 → 安全返回空列表。"""
        monkeypatch.setattr(requests, "get", lambda url, timeout=None: _FakeResp({"posts": []}))
        client = SubstackClient(None, "https://x.substack.com")
        assert client._paginate_archive("https://x.substack.com", 30, timeout=5) == []

        monkeypatch.setattr(requests, "get", lambda url, timeout=None: _FakeResp(None))
        assert client._paginate_archive("https://x.substack.com", 30, timeout=5) == []


class TestNormalizePublicationUrl:
    def test_rejects_host_with_space(self):
        with pytest.raises(ValueError):
            normalize_publication_url("cassandra unchained.substack.com")

    def test_accepts_regular_hosts(self):
        assert normalize_publication_url("capitalflows.substack.com") == "https://capitalflows.substack.com"
        assert normalize_publication_url("https://www.citriniresearch.com") == "https://citriniresearch.com"
        assert normalize_publication_url("shrubstack.com") == "https://shrubstack.com"
