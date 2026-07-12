# -*- coding: utf-8 -*-
"""Substack URL fallback / notes 归属对齐单元测试。"""

from backend.services.substack_credential_service import (
    host_matches_author_handle,
    publication_url_aliases,
    publication_url_from_api,
    resolve_subscribed_publication_url,
)


class TestPublicationUrlAliases:
    def test_custom_domain_prefers_www_then_substack_slug(self):
        aliases = publication_url_aliases("https://shrubstack.com")
        assert aliases[0] == "https://www.shrubstack.com"
        assert "https://shrubstack.substack.com" in aliases
        assert aliases[-1] == "https://shrubstack.com"

    def test_substack_host_has_no_extra_candidates(self):
        aliases = publication_url_aliases("https://capitalflows.substack.com")
        assert aliases == ["https://capitalflows.substack.com"]

    def test_strips_www_for_primary_but_keeps_www_candidate(self):
        aliases = publication_url_aliases("https://www.geopoliticaldispatch.com")
        assert "https://www.geopoliticaldispatch.com" in aliases
        assert "https://geopoliticaldispatch.substack.com" in aliases
        assert "https://geopoliticaldispatch.com" in aliases


class TestPublicationUrlFromApi:
    def test_prefers_subdomain_over_custom_domain(self):
        url = publication_url_from_api({
            "subdomain": "capitalflows",
            "custom_domain": "capitalflowsresearch.com",
        })
        assert url == "https://capitalflows.substack.com"

    def test_falls_back_to_custom_domain(self):
        url = publication_url_from_api({"custom_domain": "citriniresearch.com"})
        assert url == "https://citriniresearch.com"


class TestResolveSubscribedPublicationUrl:
    def test_exact_match(self):
        pub, uids = resolve_subscribed_publication_url(
            "https://citriniresearch.com",
            "citrini",
            {"https://citriniresearch.com": [2]},
        )
        assert pub == "https://citriniresearch.com"
        assert uids == [2]

    def test_handle_maps_custom_domain_notes_to_substack_subscription(self):
        # Capital Flows: notes 来自 custom domain，用户订的是 *.substack.com
        pub, uids = resolve_subscribed_publication_url(
            "https://capitalflowsresearch.com",
            "capitalflows",
            {"https://capitalflows.substack.com": [2, 9]},
        )
        assert pub == "https://capitalflows.substack.com"
        assert uids == [2, 9]

    def test_handle_maps_substack_author_to_custom_subscription(self):
        pub, uids = resolve_subscribed_publication_url(
            "https://citrini.substack.com",
            "citrini",
            {"https://citriniresearch.com": [2]},
        )
        assert pub == "https://citriniresearch.com"
        assert uids == [2]

    def test_orphan_falls_back_to_all_users(self):
        pub, uids = resolve_subscribed_publication_url(
            "https://notesonly.substack.com",
            "notesonly",
            {"https://other.substack.com": [5, 6]},
        )
        assert pub == "https://notesonly.substack.com"
        assert uids == [5, 6]


class TestHostMatchesAuthorHandle:
    def test_substack_slug(self):
        assert host_matches_author_handle("capitalflows.substack.com", "capitalflows")

    def test_custom_domain_prefix(self):
        assert host_matches_author_handle("citriniresearch.com", "citrini")

    def test_mismatch(self):
        assert not host_matches_author_handle("shrubstack.com", "leshrub")
