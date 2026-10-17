"""Surveillance Flash（速览版）服务契约测试。

覆盖纯函数逻辑：精华集挑选规则、report_date 口径（北京 + 1 天与完整版配对）、
官方字幕清洗、幂等跳过（完整版勿降级 / 同集跳过 / 占位可重生成）。
"""
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from backend.services.surveillance_flash_service import (
    HIGHLIGHT_MAX_SECONDS,
    SurveillanceFlashService,
    flash_report_date,
    select_evening_highlight,
)

UTC = timezone.utc
NOW = datetime(2026, 10, 15, 14, 30, tzinfo=UTC)  # 北京 22:30，节目当晚


def _ep(title, published, minutes, clip="clip-1"):
    return {
        "title": title,
        "published_utc": published,
        "duration_seconds": minutes * 60,
        "transcript_url": f"https://api.omny.fm/clips/{clip}/transcript",
        "clip_id": clip,
    }


class TestSelectEveningHighlight:
    def test_picks_latest_highlight_in_window(self):
        early = _ep("Bond Volatility", NOW - timedelta(hours=2), 40, "clip-early")
        late = _ep("CPI and the AI Trade", NOW - timedelta(minutes=30), 50, "clip-late")
        got = select_evening_highlight([early, late], NOW, lookback_hours=4)
        assert got["clip_id"] == "clip-late"

    def test_excludes_daily_edition(self):
        daily = _ep(
            "Bloomberg Surveillance TV: October 15th, 2026",
            NOW - timedelta(minutes=30),
            22,
            "clip-daily",
        )
        assert select_evening_highlight([daily], NOW, lookback_hours=4) is None

    def test_excludes_short_and_overlong(self):
        short = _ep("Too Short", NOW - timedelta(hours=1), 10, "clip-short")
        long_ = _ep("Weekend Special", NOW - timedelta(hours=1), 120, "clip-long")
        assert select_evening_highlight([short], NOW, lookback_hours=4) is None
        assert select_evening_highlight([long_], NOW, lookback_hours=4) is None
        assert HIGHLIGHT_MAX_SECONDS == 75 * 60

    def test_excludes_outside_lookback_and_missing_transcript(self):
        stale = _ep("Old Episode", NOW - timedelta(hours=6), 40, "clip-old")
        no_tr = _ep("No Transcript", NOW - timedelta(hours=1), 40, "clip-notr")
        no_tr["transcript_url"] = None
        assert select_evening_highlight([stale], NOW, lookback_hours=4) is None
        assert select_evening_highlight([no_tr], NOW, lookback_hours=4) is None

    def test_lookback_extension_finds_stale_episode(self):
        stale = _ep("Friday Show", NOW - timedelta(hours=20), 45, "clip-fri")
        assert select_evening_highlight([stale], NOW, lookback_hours=4) is None
        assert (
            select_evening_highlight([stale], NOW, lookback_hours=48)["clip_id"]
            == "clip-fri"
        )


class TestFlashReportDate:
    def test_report_date_is_beijing_next_day(self):
        # 北京 2026-10-15 22:30 (UTC 14:30) → report_date 2026-10-16，
        # 与次日早间完整版（label=北京 10-16）配对
        now = datetime(2026, 10, 15, 14, 30, tzinfo=UTC)
        assert str(flash_report_date(now)) == "2026-10-16"

    def test_just_after_beijing_midnight_still_next_day(self):
        # 北京 10-16 00:10 (UTC 10-15 16:10) → report_date 10-17
        now = datetime(2026, 10, 15, 16, 10, tzinfo=UTC)
        assert str(flash_report_date(now)) == "2026-10-17"


class TestCleanTranscript:
    def test_strips_timestamp_lines_keeps_speakers(self):
        raw = (
            "00:00:02\n"
            "Speaker 1: Bloomberg Audio Studios.\n"
            "\n"
            "00:00:11\n"
            "Speaker 2: This is the Bloomberg Surveillance Podcast.\n"
        )
        cleaned = SurveillanceFlashService._clean_transcript(raw)
        assert "00:00:02" not in cleaned
        assert "Speaker 1: Bloomberg Audio Studios." in cleaned
        assert "Speaker 2: This is the Bloomberg Surveillance Podcast." in cleaned


class TestShouldSkipFlash:
    def test_full_version_never_downgraded(self):
        existing = {
            "status": "completed",
            "video_id": "yt-123",
            "metadata": {"variant": "full", "video_ids": ["yt-123"]},
        }
        assert SurveillanceFlashService._should_skip_flash(existing, "clip-x")

    def test_same_clip_skipped(self):
        existing = {
            "status": "completed",
            "video_id": "clip-a",
            "metadata": {"variant": "flash", "video_ids": ["clip-a"]},
        }
        assert SurveillanceFlashService._should_skip_flash(existing, "clip-a")

    def test_new_clip_regenerates(self):
        existing = {
            "status": "completed",
            "video_id": "clip-a",
            "metadata": {"variant": "flash", "video_ids": ["clip-a"]},
        }
        assert not SurveillanceFlashService._should_skip_flash(existing, "clip-b")

    def test_placeholder_and_missing_regenerate(self):
        placeholder = {
            "status": "completed",
            "metadata": {"reason": "episode_not_yet_published"},
        }
        assert not SurveillanceFlashService._should_skip_flash(placeholder, "clip-a")
        assert not SurveillanceFlashService._should_skip_flash(None, "clip-a")
        failed = {"status": "failed", "metadata": {}}
        assert not SurveillanceFlashService._should_skip_flash(failed, "clip-a")
