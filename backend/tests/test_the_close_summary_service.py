# -*- coding: utf-8 -*-
"""The Close Summary 视频筛选逻辑测试"""

from backend.services.the_close_summary_service import is_bloomberg_the_close_video


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
