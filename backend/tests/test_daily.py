# -*- coding: utf-8 -*-
"""Tests for backend/api/daily.py source-subtype filtering & normalisation.

Focus: ``podcast`` is a *dual-source* type — it is both an Upload file_type
(Apple Podcast single-episode upload) AND a Feed channel_type (Podcast RSS
channel subscription). Before the fix, the filter arm only matched the Upload
side (because podcast lives in ``_UPLOAD_SUBTYPE_TO_DB_TYPES``), so RSS podcast
subscriptions never reached Daily / MCP; and the display-normalisation map
``_FEED_CHANNEL_TYPE_MAP`` had no podcast entry, so any podcast feed row that
did surface was relabelled ``news``.

Run:
    pytest backend/tests/test_daily.py -v
"""

from __future__ import annotations


# ---------------------------------------------------------------------------
# _FEED_CHANNEL_TYPE_MAP — feed channel_type → display source_subtype
# ---------------------------------------------------------------------------

class TestFeedChannelTypeMap:
    def test_podcast_mapped(self):
        from backend.api.daily import _FEED_CHANNEL_TYPE_MAP
        assert _FEED_CHANNEL_TYPE_MAP.get("podcast") == "podcast"

    def test_known_feed_types_unchanged(self):
        from backend.api.daily import _FEED_CHANNEL_TYPE_MAP
        assert _FEED_CHANNEL_TYPE_MAP["twitter"] == "twitter"
        assert _FEED_CHANNEL_TYPE_MAP["youtube"] == "youtube_feed"
        assert _FEED_CHANNEL_TYPE_MAP["rss"] == "news"
        assert _FEED_CHANNEL_TYPE_MAP["zhishixingqiu"] == "zhishixingqiu"


# ---------------------------------------------------------------------------
# _build_filter_clauses — source_subtypes → upload/file_type + feed/channel_type
# ---------------------------------------------------------------------------

class TestBuildFilterClausesPodcast:
    """source=podcast must filter BOTH the upload arm (file_type) and the feed
    arm (channel_type)."""

    def _call(self, source_subtypes):
        from backend.api.daily import _build_filter_clauses
        upload_extra, feed_extra, params = _build_filter_clauses(
            date_from=None,
            date_to=None,
            source_subtypes=source_subtypes,
            q=None,
            tickers=None,
        )
        return upload_extra, feed_extra, params

    def test_podcast_queries_both_sides(self):
        _, _, params = self._call(["podcast"])
        # Apple Podcast 单集上传 (upload arm)
        assert "podcast" in params["upload_subtypes"]
        # Podcast RSS 频道订阅 (feed arm) — was missing before the fix
        assert "podcast" in params["feed_subtypes"]

    def test_podcast_does_not_leak_other_types(self):
        _, _, params = self._call(["podcast"])
        assert set(params["upload_subtypes"]) == {"podcast"}
        assert set(params["feed_subtypes"]) == {"podcast"}

    def test_upload_only_type_does_not_touch_feed(self):
        # A pure upload type (e.g. image) must NOT add a feed_subtypes param.
        _, _, params = self._call(["image"])
        assert "image" in params["upload_subtypes"]
        assert "feed_subtypes" not in params  # feed arm gets AND FALSE

    def test_mixed_podcast_and_twitter(self):
        _, _, params = self._call(["podcast", "twitter"])
        # podcast touches both arms; twitter only the feed arm
        assert "podcast" in params["upload_subtypes"]
        assert "podcast" in params["feed_subtypes"]
        assert "twitter" in params["feed_subtypes"]
        assert "twitter" not in params["upload_subtypes"]


class TestFeedChannelTypeMapSubstack:
    def test_substack_mapped(self):
        from backend.api.daily import _FEED_CHANNEL_TYPE_MAP
        assert _FEED_CHANNEL_TYPE_MAP.get("substack") == "substack"


class TestBuildFilterClausesSubstack:
    def _call(self, source_subtypes):
        from backend.api.daily import _build_filter_clauses
        return _build_filter_clauses(
            date_from=None,
            date_to=None,
            source_subtypes=source_subtypes,
            q=None,
            tickers=None,
        )

    def test_substack_queries_feed_arm_only(self):
        _, _, params = self._call(["substack"])
        assert params["feed_subtypes"] == ("substack",)
        assert "upload_subtypes" not in params

    def test_mixed_zhishixingqiu_and_substack(self):
        _, _, params = self._call(["zhishixingqiu", "substack"])
        assert set(params["feed_subtypes"]) == {"zhishixingqiu", "substack"}


class TestExpandPresetForLegacyFeeds:
    def test_adds_substack_when_legacy_feed_preset(self):
        from backend.api.daily import _expand_preset_for_legacy_feeds
        legacy = ["twitter", "news", "youtube_feed", "zhishixingqiu", "weibo"]
        expanded = _expand_preset_for_legacy_feeds(legacy)
        assert "substack" in expanded
        assert expanded.index("substack") == expanded.index("zhishixingqiu") + 1

    def test_noop_when_substack_already_present(self):
        from backend.api.daily import _expand_preset_for_legacy_feeds
        preset = ["zhishixingqiu", "substack"]
        assert _expand_preset_for_legacy_feeds(preset) == preset
