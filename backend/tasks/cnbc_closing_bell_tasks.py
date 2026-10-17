# -*- coding: utf-8 -*-
"""
CNBC Closing Bell Summary Celery Tasks
"""

import asyncio
from datetime import date as _date

from backend.celery_app import app
from backend.utils.logger import get_logger

logger = get_logger("cnbc_closing_bell_tasks")


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
    name="generate_cnbc_closing_bell_summary",
    queue="ai_processing",
    max_retries=1,
)
def generate_cnbc_closing_bell_task(
    self,
    report_date: str = None,
    window_end: str = None,
    window_hours: int = 24,
    force: bool = False,
):
    """
    生成 CNBC Closing Bell 每日 PM Brief 摘要（全局，非用户维度）。

    Args:
        report_date: ISO 日期字符串 (YYYY-MM-DD)，默认为今天。
        window_end: ISO 时间字符串（UTC），内容窗口结束时间，默认当前时刻。
        window_hours: 回溯小时数，默认 24。
        force: 是否强制重新生成，默认 False。
    """
    from backend.services.cnbc_closing_bell_service import CNBCClosingBellService

    try:
        svc = CNBCClosingBellService()

        from backend.utils.datetime_utils import (
            parse_iso_utc,
            utcnow,
            window_label_date,
        )

        if report_date:
            as_of = _date.fromisoformat(report_date)
        else:
            as_of = window_label_date(utcnow(), 'Asia/Shanghai')
        # 缺省用当前时刻的滚动窗口：自然日窗口按 naive 与 UTC 存储 published_at
        # 比较，会系统性漏掉前一日美东正片（与 The Close 的处理一致）。
        we = parse_iso_utc(window_end) or utcnow()

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
                f"CNBC Closing Bell Summary generated: date={as_of}, "
                f"summary_id={result.get('summary_id')}, "
                f"skipped={result.get('skipped', False)}"
            )

            # 后置钩子与 The Close / Surveillance 相同：占位摘要不渲染。
            from backend.services.cnbc_closing_bell_service import PLACEHOLDER_REASONS

            if result.get("reason") in PLACEHOLDER_REASONS:
                logger.info(
                    f"Closing Bell Report skipped: placeholder "
                    f"{result.get('reason')} date={as_of}"
                )
            else:
                try:
                    from backend.services.closing_bell_report_service import (
                        ClosingBellReportService,
                    )

                    brief = ClosingBellReportService().generate(report_date=as_of)
                    if brief.get("success"):
                        logger.info(
                            f"Closing Bell Report: date={as_of}, "
                            f"skipped={brief.get('skipped', False)}, "
                            f"reason={brief.get('reason') or 'rendered'}"
                        )
                    else:
                        logger.error(
                            f"Closing Bell Report FAILED: date={as_of}, {brief.get('error')}"
                        )
                except Exception as hook_exc:
                    logger.error(
                        f"Closing Bell Report hook error: {hook_exc}", exc_info=True
                    )
        else:
            logger.error(f"CNBC Closing Bell Summary FAILED: {result.get('message')}")

        return result

    except Exception as exc:
        logger.error(f"CNBC Closing Bell Summary unexpected error: {exc}", exc_info=True)
        raise
