# -*- coding: utf-8 -*-
"""
Bloomberg Surveillance Flash (速览版) Celery Tasks
"""

import asyncio

from backend.celery_app import app
from backend.utils.logger import get_logger

logger = get_logger("surveillance_flash_tasks")


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
    name="generate_bloomberg_surveillance_flash_summary",
    queue="ai_processing",
    max_retries=1,
)
def generate_bloomberg_surveillance_flash_task(
    self,
    report_date: str = None,
    lookback_hours: float = 4.0,
    force: bool = False,
):
    """生成 Surveillance 速览版摘要（播客精华集官方字幕，当晚 22:30 出报）。

    Args:
        report_date: ISO 日期字符串 (YYYY-MM-DD)，默认北京自然日 + 1
            （与同场节目的完整版同一 report_date，成对升级）。
        lookback_hours: 精华集检索回溯窗口（小时），默认 4。
        force: 是否强制重新生成，默认 False。
    """
    from datetime import date as _date

    from backend.services.surveillance_flash_service import SurveillanceFlashService

    try:
        svc = SurveillanceFlashService()
        as_of = _date.fromisoformat(report_date) if report_date else None

        result = _run_async(
            svc.generate(
                report_date=as_of,
                lookback_hours=lookback_hours,
                force=force,
            )
        )

        if result.get("success"):
            as_of_final = as_of
            if as_of_final is None:
                from backend.services.surveillance_flash_service import (
                    flash_report_date,
                )
                as_of_final = flash_report_date()

            logger.info(
                f"Surveillance Flash generated: date={as_of_final}, "
                f"summary_id={result.get('summary_id')}, "
                f"skipped={result.get('skipped', False)}"
            )

            # 后置钩子与完整版一致：占位摘要不渲染。
            if result.get("reason"):
                logger.info(
                    f"Surveillance Flash Report skipped: "
                    f"{result.get('reason')} date={as_of_final}"
                )
            else:
                try:
                    from backend.services.surveillance_report_service import (
                        SurveillanceReportService,
                    )

                    brief = SurveillanceReportService().generate(
                        report_date=as_of_final
                    )
                    if brief.get("success"):
                        logger.info(
                            f"Surveillance Flash Report: date={as_of_final}, "
                            f"skipped={brief.get('skipped', False)}, "
                            f"reason={brief.get('reason') or 'rendered'}"
                        )
                    else:
                        logger.error(
                            f"Surveillance Flash Report FAILED: "
                            f"date={as_of_final}, {brief.get('error')}"
                        )
                except Exception as hook_exc:
                    logger.error(
                        f"Surveillance Flash Report hook error: {hook_exc}",
                        exc_info=True,
                    )
        else:
            logger.error(
                f"Surveillance Flash FAILED: {result.get('message')}"
            )

        return result

    except Exception as exc:
        logger.error(
            f"Surveillance Flash unexpected error: {exc}", exc_info=True
        )
        raise
