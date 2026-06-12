# -*- coding: utf-8 -*-
"""CNBC Closing Bell 视频筛选逻辑测试"""

from datetime import datetime, timezone

from backend.services.cnbc_closing_bell_service import (
    CLOSING_BELL_MATCH_PATTERN,
    CNBCClosingBellService,
    is_cnbc_closing_bell_video,
)


def test_is_cnbc_closing_bell_video_by_title():
    assert is_cnbc_closing_bell_video(
        "Closing Bell: Market Rally Continues 6/11/2026"
    )


def test_is_cnbc_closing_bell_video_by_pipe_title():
    assert is_cnbc_closing_bell_video(
        "Stocks Surge To Session Highs | Closing Bell 6/10/2026"
    )


def test_is_cnbc_closing_bell_video_standalone():
    assert is_cnbc_closing_bell_video("Closing Bell 4/29/2026")


def test_is_cnbc_closing_bell_video_by_content():
    """Podcast episode — title 可能不含节目名，但 content（description）含 'Closing Bell'"""
    assert is_cnbc_closing_bell_video(
        title="CNBC Market Update June 11",
        content="In this episode of Closing Bell, we discuss the market rally...",
    )


def test_is_cnbc_closing_bell_video_rejects_other_shows():
    assert not is_cnbc_closing_bell_video("Squawk Box 6/10/2026")
    assert not is_cnbc_closing_bell_video("CNBC Power Lunch")
    assert not is_cnbc_closing_bell_video(None)
    assert not is_cnbc_closing_bell_video("Random Podcast Episode", "No show name here")


def test_fetch_closing_bell_videos_matches_title_not_description():
    """RSS description 常不含 'Closing Bell'，应靠 title 命中。"""
    svc = CNBCClosingBellService()
    window_end = datetime(2026, 6, 12, 15, 26, 0, tzinfo=timezone.utc)
    window_start = datetime(2026, 6, 11, 15, 26, 0, tzinfo=timezone.utc)
    videos = svc._fetch_closing_bell_videos(window_start, window_end)
    assert videos, "应命中 6/11 Closing Bell 主节目与 Overtime"
    assert all("Closing Bell" in (v.get("title") or "") for v in videos)
    assert CLOSING_BELL_MATCH_PATTERN == "%Closing Bell%"
