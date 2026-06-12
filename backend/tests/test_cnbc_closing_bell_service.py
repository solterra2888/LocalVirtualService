# -*- coding: utf-8 -*-
"""CNBC Closing Bell 视频筛选逻辑测试"""

from datetime import date, datetime

from backend.services.cnbc_closing_bell_service import (
    CNBCClosingBellService,
    is_cnbc_closing_bell_video,
)
from backend.utils.datetime_utils import resolve_report_content_window


def test_is_cnbc_closing_bell_video_by_title():
    assert is_cnbc_closing_bell_video(
        "Closing Bell: Market Rally Continues 6/11/2026"
    )


def test_is_cnbc_closing_bell_video_by_pipe_title():
    assert is_cnbc_closing_bell_video(
        "Stocks Surge To Session Highs | Closing Bell 6/10/2026"
    )


def test_is_cnbc_closing_bell_video_by_content_description():
    assert is_cnbc_closing_bell_video(
        "Rick Rieder: No way markets are in a bubble",
        content="Rick Rieder, BlackRock CIO, joins 'Closing Bell' to discuss markets.",
    )


def test_is_cnbc_closing_bell_video_standalone():
    assert is_cnbc_closing_bell_video("Closing Bell 4/29/2026")


def test_is_cnbc_closing_bell_video_rejects_other_shows():
    assert not is_cnbc_closing_bell_video("Squawk Box 6/10/2026")
    assert not is_cnbc_closing_bell_video("CNBC Power Lunch")
    assert not is_cnbc_closing_bell_video(None)
    assert not is_cnbc_closing_bell_video(
        "Wholesale prices rose 1.1% in May",
        content="Generic market news without the show name.",
    )


def test_fetch_closing_bell_videos_matches_content_not_title():
    """CNBC clip 标题不含 Closing Bell，但 description(content) 含 joins 'Closing Bell'。"""
    svc = CNBCClosingBellService()
    ws, we = resolve_report_content_window(date(2026, 6, 11), None, 24)
    videos = svc._fetch_closing_bell_videos(ws, we)
    assert len(videos) >= 1
    assert any(
        is_cnbc_closing_bell_video(v.get("title"), v.get("content"))
        for v in videos
    )


def test_prepare_merged_transcript_combines_all_segments():
    svc = CNBCClosingBellService()
    videos = [
        {
            "content_id": "vid1",
            "title": "Guest clip A",
            "published_at": datetime(2026, 6, 11, 19, 0, 0),
            "transcript_cleaned": "First segment words here.",
        },
        {
            "content_id": "vid2",
            "title": "Overtime segment",
            "published_at": datetime(2026, 6, 11, 21, 0, 0),
            "transcript": "Second segment with more words.",
        },
        {
            "content_id": "vid3",
            "title": "No transcript yet",
            "published_at": datetime(2026, 6, 11, 22, 0, 0),
            "transcript": "",
        },
    ]
    merged, word_count, used = svc._prepare_merged_transcript(videos)

    assert len(used) == 2
    assert "Segment 1/2" in merged
    assert "Segment 2/2" in merged
    assert "First segment words here." in merged
    assert "Second segment with more words." in merged
    assert word_count == len("First segment words here.".split()) + len(
        "Second segment with more words.".split()
    )


def test_format_video_title_for_llm_multiple_segments():
    videos = [
        {"title": "Rick Rieder on markets"},
        {"title": "Closing Bell Overtime: SpaceX IPO"},
    ]
    title = CNBCClosingBellService._format_video_title_for_llm(videos)
    assert "2 segments" in title
    assert "Rick Rieder on markets" in title
    assert "SpaceX IPO" in title
