# -*- coding: utf-8 -*-
"""
Daily Market Report Celery Tasks
"""

from backend.celery_app import app
from backend.utils.logger import get_logger

logger = get_logger("daily_market_report_tasks")


@app.task(
    bind=True,
    name="generate_daily_market_report",
    queue="data_fetching",
    max_retries=1,
)
def generate_daily_market_report_task(self, report_date: str = None):
    """
    生成 Daily Market Report（全局，非用户维度）。

    Args:
        report_date: 可选的 ISO 日期字符串，默认为今天。
    """
    from datetime import date as _date
    from backend.services.daily_market_report_service import DailyMarketReportService

    try:
        svc = DailyMarketReportService()
        as_of = _date.fromisoformat(report_date) if report_date else None
        result = svc.generate(report_date=as_of)

        if result.get("success"):
            logger.info(
                f"Daily Market Report generated: {result.get('report_date')}, "
                f"file={result.get('file_path')}"
            )
        else:
            logger.error(f"Daily Market Report FAILED: {result.get('error')}")

        return result

    except Exception as exc:
        logger.error(f"Daily Market Report unexpected error: {exc}", exc_info=True)
        raise
