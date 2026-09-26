# -*- coding: utf-8 -*-
"""The Close Summary 视频筛选逻辑测试"""

from datetime import date, datetime, timezone

from backend.services.the_close_summary_service import (
    _select_primary_the_close_video,
    is_bloomberg_the_close_video,
    should_defer_no_video_placeholder,
)


def test_is_bloomberg_the_close_video_by_show_name():
    assert is_bloomberg_the_close_video("Any Title", show_name="Bloomberg The Close")


def test_is_bloomberg_the_close_video_by_pipe_title():
    assert is_bloomberg_the_close_video(
        "US Inflation Picks Up | The Close 6/10/2026"
    )


def test_is_bloomberg_the_close_video_by_standalone_title():
    assert is_bloomberg_the_close_video("The Close 4/29/2026")


def test_is_bloomberg_the_close_video_rejects_other_shows():
    assert not is_bloomberg_the_close_video("Bloomberg Surveillance 6/10/2026")
    assert not is_bloomberg_the_close_video(None)
    assert not is_bloomberg_the_close_video("Swedish Election Result Too Close to Call")


def test_is_bloomberg_the_close_video_by_opening_bumper():
    transcript = 'DAY. THIS IS "THE CLOSE." ROMAINE: Stocks rallied into the bell.'
    assert is_bloomberg_the_close_video(
        "Fed Hikes Rates for the First Time Since 2023",
        transcript=transcript,
    )
    assert is_bloomberg_the_close_video(
        "Rebuilding After 9/11, Cantor Fitzgerald's Annual Charity",
        transcript="Welcome to a special edition of The Close. We kick off...",
    )
    cold_open = ("market update. " * 600) + 'DAY. THIS IS "THE CLOSE."'
    assert is_bloomberg_the_close_video(
        "Fed Hikes Rates for the First Time Since 2023",
        transcript=cold_open,
    )


def test_is_bloomberg_the_close_video_by_description():
    assert is_bloomberg_the_close_video(
        "Markets Rise as AI, Oil & Treasury Yields Drive the Week",
        description=(
            "The Close brings you the latest news and analysis leading up "
            "to the final minutes and seconds before and after the closing bell."
        ),
    )
    assert not is_bloomberg_the_close_video(
        "Swedish Election Result Too Close to Call",
        description="The election result was too close to call.",
    )


def test_bumper_rejects_market_jargon_and_other_shows():
    assert not is_bloomberg_the_close_video(
        "Chip tease",
        transcript="Equities got into the close 35 minutes before the bell.",
    )
    assert not is_bloomberg_the_close_video(
        "Oracle Earnings Beat as Cloud Revenue Surges",
        transcript="Reporting after the close today. Tune in later.",
    )
    assert not is_bloomberg_the_close_video(
        "Bloomberg Surveillance: The Fed Decides 9/16/2026",
        transcript='DAY. THIS IS "THE CLOSE."',
    )
    assert not is_bloomberg_the_close_video(
        "Swedish Election Result Too Close to Call",
        transcript="this is the closest comparison we have seen",
    )


def test_select_primary_prefers_later_full_episode():
    tease = {
        "content_id": "tease",
        "title": "AI Bosses Urge Caution, Bonds Climb",
        "published_at": datetime(2026, 9, 14, 21, 6),
        "transcript": "COMING UP NEXT, THE CLOSE. Stay with us.",
        "metadata": {},
    }
    episode = {
        "content_id": "ep",
        "title": "Chip Stocks Tumble on AI Safety Fears & Rising Yields",
        "published_at": datetime(2026, 9, 14, 22, 17),
        "transcript": "DAY. THIS IS THE CLOSE. " + ("market " * 400),
        "metadata": {},
    }
    surveillance = {
        "content_id": "surv",
        "title": "Bloomberg Surveillance: The Fed Decides 9/16/2026",
        "published_at": datetime(2026, 9, 16, 21, 46),
        "transcript": 'DAY. THIS IS "THE CLOSE." ' + ("fed " * 800),
        "metadata": {},
    }
    primary, selected = _select_primary_the_close_video([tease, episode, surveillance])
    assert primary["content_id"] == "ep"
    assert [v["content_id"] for v in selected] == ["ep"]


def test_should_defer_placeholder_before_typical_publish():
    morning = datetime(2026, 9, 17, 5, 1, tzinfo=timezone.utc)  # 13:01 Beijing
    assert should_defer_no_video_placeholder(date(2026, 9, 17), now=morning)
    late = datetime(2026, 9, 17, 15, 0, tzinfo=timezone.utc)  # 23:00 Beijing
    assert not should_defer_no_video_placeholder(date(2026, 9, 17), now=late)
    assert not should_defer_no_video_placeholder(date(2026, 9, 16), now=morning)
