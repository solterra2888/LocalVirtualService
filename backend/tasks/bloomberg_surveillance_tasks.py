# -*- coding: utf-8 -*-
"""
Bloomberg Surveillance Summary Celery Tasks
"""

import asyncio
from datetime import date as _date, datetime as _datetime

from backend.celery_app import app
from backend.utils.logger import get_logger

logger = get_logger("bloomberg_surveillance_tasks")


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
    name="generate_bloomberg_surveillance_summary",
    queue="ai_processing",
    max_retries=1,
)
def generate_bloomberg_surveillance_task(
    self,
    report_date: str = None,
    window_end: str = None,
    window_hours: int = 24,
    force: bool = False,
):
    """
    生成 Bloomberg Surveillance 每日 AM Brief 摘要（全局，非用户维度）。

    Args:
        report_date: ISO 日期字符串 (YYYY-MM-DD)，默认为北京自然日。
        window_end: ISO 时间字符串（UTC），内容窗口结束时间，默认当前时刻。
        window_hours: 回溯小时数，默认 24。
        force: 是否强制重新生成，默认 False。
    """
    from backend.services.bloomberg_surveillance_service import (
        BloombergSurveillanceService,
    )

    try:
        svc = BloombergSurveillanceService()

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
                f"Bloomberg Surveillance Summary generated: date={as_of}, "
                f"summary_id={result.get('summary_id')}, "
                f"skipped={result.get('skipped', False)}"
            )

            # 后置钩子：生成 Bloomberg Surveillance Report（独立报告，
            # 处理方式同 The Close / Closing Bell Report）。
            # 摘要已落库，此处渲染失败不影响摘要结果；skip 路径同样执行，
            # 以覆盖「摘要已存在但报告缺失/落后」的场景（如首次上线、上次渲染失败）。
            try:
                from backend.services.surveillance_report_service import (
                    SurveillanceReportService,
                )

                brief = SurveillanceReportService().generate(report_date=as_of)
                if brief.get("success"):
                    logger.info(
                        f"Surveillance Report: date={as_of}, "
                        f"skipped={brief.get('skipped', False)}, "
                        f"reason={brief.get('reason') or 'rendered'}"
                    )
                else:
                    logger.error(
                        f"Surveillance Report FAILED: date={as_of}, {brief.get('error')}"
                    )
            except Exception as hook_exc:
                logger.error(
                    f"Surveillance Report hook error: {hook_exc}", exc_info=True
                )
        else:
            logger.error(
                f"Bloomberg Surveillance Summary FAILED: {result.get('message')}"
            )

        return result

    except Exception as exc:
        logger.error(
            f"Bloomberg Surveillance Summary unexpected error: {exc}", exc_info=True
        )
        raise
