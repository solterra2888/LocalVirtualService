# -*- coding: utf-8 -*-
"""
The Close Summary Celery Tasks
"""

import asyncio
from datetime import date as _date, datetime as _datetime

from backend.celery_app import app
from backend.utils.logger import get_logger

logger = get_logger("the_close_summary_tasks")


def _run_async(coro):
    """在 Celery 同步 worker 中执行 async 协程。"""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_closed():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    return loop.run_until_complete(coro)


@app.task(
    bind=True,
    name="generate_the_close_summary",
    queue="ai_processing",
    max_retries=1,
)
def generate_the_close_summary_task(
    self,
    report_date: str = None,
    window_end: str = None,
    window_hours: int = 24,
    force: bool = False,
):
    """
    生成 Bloomberg The Close 每日 PM Brief 摘要（全局，非用户维度）。

    Args:
        report_date: ISO 日期字符串 (YYYY-MM-DD)，默认为今天。
        window_end: ISO 时间字符串（UTC），内容窗口结束时间，默认当前时刻。
        window_hours: 回溯小时数，默认 24。
        force: 是否强制重新生成，默认 False。
    """
    from backend.services.the_close_summary_service import TheCloseSummaryService

    try:
        svc = TheCloseSummaryService()

        if report_date:
            as_of = _date.fromisoformat(report_date)
        else:
            from backend.utils.datetime_utils import utcnow, window_label_date
            as_of = window_label_date(utcnow(), 'Asia/Shanghai')
        we = _datetime.fromisoformat(window_end) if window_end else None

        result = _run_async(
            svc.generate(
                report_date=as_of,
                window_end=we,
                window_hours=window_hours,
                force=force,
            )
        )

        if result.get("success"):
            logger.info(
                f"The Close Summary generated: date={as_of}, "
                f"summary_id={result.get('summary_id')}, "
                f"skipped={result.get('skipped', False)}"
            )
        else:
            logger.error(f"The Close Summary FAILED: {result.get('message')}")

        return result

    except Exception as exc:
        logger.error(f"The Close Summary unexpected error: {exc}", exc_info=True)
        raise
