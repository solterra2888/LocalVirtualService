# -*- coding: utf-8 -*-
"""
Feeds Dashboard 主题聚合 Celery 任务 (P0).

调度链路: task_scheduling(feeds_topic_summary) → scheduler_service
          → aggregate_feed_topics_task (queue: data_fetching)
          → FeedTopicAggregationService.aggregate_days (零 LLM, 幂等)

P1 将在本模块追加 Phase A 主题日摘要 (feed_topic_daily_summaries, LLM).
"""

import logging
from typing import Any, Dict, Optional

from backend.celery_app import app

logger = logging.getLogger(__name__)


@app.task(
    name='aggregate_feed_topics',
    queue='data_fetching',
    time_limit=600,
    soft_time_limit=540,
)
def aggregate_feed_topics_task(
    days: int = 7,
    force: bool = False,
) -> Dict[str, Any]:
    """聚合最近 N 天 (默认 7, 覆盖 Dashboard「近一周」查询) 的主题快照.

    幂等: 每天全量重建 feed_topic_daily_items, 重复执行总是收敛.
    force 参数为 P1 Phase A 预留 (跳过摘要缓存), P0 聚合本身天然幂等忽略之.
    """
    from backend.services.feed_topic_aggregation_service import FeedTopicAggregationService

    days = max(1, min(int(days or 7), 30))
    try:
        results = FeedTopicAggregationService().aggregate_days(days=days)
        ok = all(r.get("success") for r in results)
        return {
            'success': ok,
            'days': days,
            'results': results,
        }
    except Exception as exc:
        logger.error(f"aggregate_feed_topics_task 失败: {exc}")
        return {'success': False, 'error': str(exc), 'days': days}


@app.task(
    name='aggregate_feed_topics_single',
    queue='data_fetching',
    time_limit=600,
    soft_time_limit=540,
)
def aggregate_feed_topics_single_task(
    report_date: Optional[str] = None,
    force: bool = False,
) -> Dict[str, Any]:
    """聚合单个日期 (管理员手动触发/历史回填). report_date 为 YYYY-MM-DD (北京自然日)."""
    from datetime import date

    from backend.services.feed_topic_aggregation_service import FeedTopicAggregationService

    try:
        target = date.fromisoformat(report_date) if report_date else None
        return FeedTopicAggregationService().aggregate(report_date=target)
    except ValueError:
        return {'success': False, 'error': f'非法日期: {report_date}'}
    except Exception as exc:
        logger.error(f"aggregate_feed_topics_single_task 失败 ({report_date}): {exc}")
        return {'success': False, 'error': str(exc), 'report_date': report_date}
