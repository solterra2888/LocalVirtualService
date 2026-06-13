# -*- coding: utf-8 -*-
"""知识星球每日摘要服务测试"""

from datetime import date, datetime, timezone

from backend.services.zhishixingqiu_summary_service import (
    ZhishixingqiuSummaryService,
    resolve_zhishixingqiu_content_window,
)


def test_resolve_zhishixingqiu_content_window_beijing_day():
    """默认按北京自然日计算窗口。"""
    report_date = date(2026, 6, 12)
    start, end = resolve_zhishixingqiu_content_window(report_date)
    # 北京 6-12 00:00 = UTC 6-11 16:00
    assert start == datetime(2026, 6, 11, 16, 0, tzinfo=timezone.utc)
    assert end == datetime(2026, 6, 12, 16, 0, tzinfo=timezone.utc)


def test_format_group_topics():
    svc = ZhishixingqiuSummaryService()
    topics = [
        {
            "title": "宏观观点",
            "content": "看好科技板块",
            "metadata": {"post_author": "作者甲"},
            "published_at": datetime(2026, 6, 12, 8, 0, tzinfo=timezone.utc),
            "url": "https://example.com/t1",
        },
        {
            "title": "",
            "content": "关注新能源",
            "metadata": {"post_author": "作者乙"},
            "published_at": datetime(2026, 6, 12, 10, 0, tzinfo=timezone.utc),
            "url": "",
        },
    ]
    text, truncated = svc._format_group_topics(topics)
    assert "宏观观点" in text
    assert "作者甲" in text
    assert "看好科技板块" in text
    assert "关注新能源" in text
    assert not truncated


def test_assemble_final_summary():
    svc = ZhishixingqiuSummaryService()
    group_summaries = {
        "g1": {
            "group_name": "星球A",
            "summary_content": "- 观点一\n- 观点二",
            "topic_count": 3,
        },
        "g2": {
            "group_name": "星球B",
            "summary_content": "- 新能源看好",
            "topic_count": 2,
        },
    }
    result = svc._assemble_final_summary(group_summaries)
    assert "# 知识星球每日摘要" in result
    assert "## 星球A" in result
    assert "## 星球B" in result
    assert "观点一" in result
    assert "新能源看好" in result
    assert "共 2 个星球" in result


def test_assemble_final_summary_subset_only():
    """仅拼接传入的星球子集（模拟用户只订阅部分星球）。"""
    svc = ZhishixingqiuSummaryService()
    all_groups = {
        "g1": {
            "group_name": "星球A",
            "summary_content": "- 观点一",
            "topic_count": 1,
        },
        "g2": {
            "group_name": "星球B",
            "summary_content": "- 观点二",
            "topic_count": 1,
        },
        "g3": {
            "group_name": "星球C",
            "summary_content": "- 观点三",
            "topic_count": 1,
        },
    }
    user_subscribed = {"g1": all_groups["g1"], "g3": all_groups["g3"]}
    result = svc._assemble_final_summary(user_subscribed)
    assert "## 星球A" in result
    assert "## 星球C" in result
    assert "## 星球B" not in result
    assert "观点一" in result
    assert "观点三" in result
    assert "观点二" not in result
    assert "共 2 个星球" in result
