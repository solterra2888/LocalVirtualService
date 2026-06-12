# -*- coding: utf-8 -*-
"""Podcast 订阅抓取时间窗 / ASR 窗口单元测试"""

from datetime import datetime, timedelta, timezone

from backend.services.podcast_subscription_service import PodcastSubscriptionService
from backend.utils.datetime_utils import utcnow


def _entry(published_at: datetime) -> dict:
    ts = published_at.astimezone(timezone.utc)
    return {
        "title": "episode",
        "published_parsed": (
            ts.year,
            ts.month,
            ts.day,
            ts.hour,
            ts.minute,
            ts.second,
            ts.weekday(),
            ts.timetuple().tm_yday,
            ts.dst(),
        ),
        "enclosures": [{"href": "https://example.com/a.mp3"}],
    }


def test_filter_entries_by_recency_keeps_last_7_days():
    now = utcnow()
    entries = [
        _entry(now - timedelta(days=1)),
        _entry(now - timedelta(days=3)),
        _entry(now - timedelta(days=8)),
    ]
    filtered = PodcastSubscriptionService._filter_entries_by_recency(
        entries, recent_days=7, max_items=100
    )
    assert len(filtered) == 2


def test_entry_within_asr_window_48h(monkeypatch):
    monkeypatch.setenv("PODCAST_ASR_RECENT_HOURS", "48")
    now = utcnow()
    assert PodcastSubscriptionService._entry_within_asr_window(_entry(now - timedelta(hours=12)))
    assert not PodcastSubscriptionService._entry_within_asr_window(_entry(now - timedelta(hours=72)))
