# -*- coding: utf-8 -*-
"""Bloomberg Surveillance 视频筛选与转录合并逻辑测试"""

from datetime import datetime, timezone

from backend.services.bloomberg_surveillance_service import (
    MAX_TRANSCRIPT_CHARS,
    SURVEILLANCE_TITLE_PATTERN,
    BloombergSurveillanceService,
    is_bloomberg_surveillance_video,
)


# ── 节目识别 ────────────────────────────────────────────────────────────

def test_is_surveillance_video_by_title_prefix():
    # standalone 前缀格式（youtube_adapter standalone_shows）
    assert is_bloomberg_surveillance_video(
        "Bloomberg Surveillance - 09/16/2026"
    )


def test_is_surveillance_video_by_pipe_title():
    assert is_bloomberg_surveillance_video(
        "Fed Officials Speak | Bloomberg Surveillance 9/15/26"
    )


def test_is_surveillance_video_case_insensitive():
    assert is_bloomberg_surveillance_video(
        "BLOOMBERG SURVEILLANCE: Guests on CPI"
    )


def test_is_surveillance_video_by_show_name():
    assert is_bloomberg_surveillance_video(
        title="Anything the title says",
        show_name="Bloomberg Surveillance",
    )


def test_is_surveillance_video_rejects_other_shows():
    assert not is_bloomberg_surveillance_video(
        "Stocks Pare Tech-Led Drop | The Close"
    )
    assert not is_bloomberg_surveillance_video("Bloomberg Balance of Power")
    assert not is_bloomberg_surveillance_video(None)
    assert not is_bloomberg_surveillance_video("Bloomberg Technology")
    assert SURVEILLANCE_TITLE_PATTERN == "%Bloomberg Surveillance%"


# ── 转录合并 ────────────────────────────────────────────────────────────

# 绕过 __init__（避免真实 DB 连接）
_svc = BloombergSurveillanceService.__new__(BloombergSurveillanceService)


def _video(idx: int, text: str, title: str = None):
    return {
        "content_id": f"vid{idx}",
        "title": title or f"Bloomberg Surveillance Segment {idx}",
        "url": f"https://youtu.be/vid{idx}",
        "published_at": datetime(
            2026, 9, 16, 12 + idx, 0, tzinfo=timezone.utc
        ),
        "transcript": text,
        "transcript_cleaned": None,
        "metadata": {"show_name": "Bloomberg Surveillance"},
    }


def test_merged_transcript_single_segment():
    merged, words, used, truncated = (
        _svc._prepare_merged_transcript(
            [_video(1, "alpha beta gamma")]
        )
    )
    assert used and len(used) == 1
    assert words == 3
    assert truncated is False
    assert merged.startswith("=== Segment 1/1:")


def test_merged_transcript_multiple_segments_ordered():
    videos = [
        _video(1, "one two", title="Bloomberg Surveillance - 09/16/26"),
        _video(2, "three four five"),
    ]
    merged, words, used, truncated = (
        _svc._prepare_merged_transcript(videos)
    )
    assert len(used) == 2
    assert words == 5
    assert truncated is False
    assert "=== Segment 1/2:" in merged and "=== Segment 2/2:" in merged
    assert merged.index("Segment 1/2") < merged.index("Segment 2/2")
    assert "\n\n---\n\n" in merged


def test_merged_transcript_skips_videos_without_text():
    videos = [_video(1, ""), _video(2, "alpha")]
    merged, words, used, truncated = (
        _svc._prepare_merged_transcript(videos)
    )
    assert len(used) == 1
    assert words == 1
    assert "Segment 1/1" in merged


def test_merged_transcript_all_empty():
    merged, words, used, truncated = (
        _svc._prepare_merged_transcript([_video(1, "")])
    )
    assert merged == "" and words == 0 and used == [] and truncated is False


def test_merged_transcript_truncates_over_limit():
    long_text = "word " * 40000  # ~200k chars > MAX_TRANSCRIPT_CHARS
    assert len(long_text) > MAX_TRANSCRIPT_CHARS
    merged, _, _, truncated = (
        _svc._prepare_merged_transcript([_video(1, long_text)])
    )
    assert truncated is True
    assert "转录过长，已截断" in merged
    assert len(merged) < len(long_text)


def test_merged_transcript_prefers_cleaned():
    v = _video(1, "raw fallback")
    v["transcript_cleaned"] = "cleaned text here"
    merged, words, _, _ = _svc._prepare_merged_transcript([v])
    assert "cleaned text here" in merged
    assert words == 3


# ── LLM 标题格式化 ──────────────────────────────────────────────────────

def test_format_video_title_single():
    title = BloombergSurveillanceService._format_video_title_for_llm(
        [_video(1, "x", title="Bloomberg Surveillance - 09/16/26")]
    )
    assert title == "Bloomberg Surveillance - 09/16/26"


def test_format_video_title_multi():
    videos = [_video(1, "x"), _video(2, "y")]
    title = BloombergSurveillanceService._format_video_title_for_llm(videos)
    assert title.startswith("Bloomberg Surveillance — 2 segments:")
    assert "1. Bloomberg Surveillance Segment 1" in title
    assert "2. Bloomberg Surveillance Segment 2" in title


def test_format_video_title_empty():
    assert (
        BloombergSurveillanceService._format_video_title_for_llm([])
        == "Bloomberg Surveillance"
    )
