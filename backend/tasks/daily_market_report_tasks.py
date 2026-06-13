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
def generate_daily_market_report_task(self, user_id: str = None, report_date: str = None):
    """
    生成 Daily Market Report（按用户维度）。

    Args:
        user_id: 用户 ID（字符串形式，如 "123"）。必填。
        report_date: 可选的 ISO 日期字符串，默认为今天。
    """
    from datetime import date as _date
    from backend.services.daily_market_report_service import DailyMarketReportService

    try:
        if not user_id:
            raise ValueError("user_id is required for daily_market_report generation")

        svc = DailyMarketReportService()
        as_of = _date.fromisoformat(report_date) if report_date else None
        result = svc.generate(user_id=user_id, report_date=as_of)

        if result.get("success"):
            logger.info(
                f"Daily Market Report generated: user={user_id}, "
                f"date={result.get('report_date')}, "
                f"file={result.get('file_path')}"
            )
        else:
            logger.error(
                f"Daily Market Report FAILED: user={user_id}, {result.get('error')}"
            )

        return result

    except Exception as exc:
        logger.error(f"Daily Market Report unexpected error: {exc}", exc_info=True)
        raise


@app.task(
    bind=True,
    name="generate_all_daily_market_reports",
    queue="data_fetching",
    max_retries=1,
)
def generate_all_daily_market_reports_task(self, user_id: str = None, report_date: str = None):
    """
    为所有（或指定）用户生成 Daily Market Report（fan-out）。

    定时调度场景下 report_date 为空时，取【北京自然日】作为报告日期，与上游对齐。
    各上游（Bloomberg The Close / CNBC Closing Bell / Twitter / YouTube / 知识星球）
    的 report_date 均为北京自然日（window_label_date + Asia/Shanghai），且都在
    北京 08:00 前生成当天报告，故 Daily Market Report 查询当天即可凑齐四个标签。

    Args:
        user_id:     可选，仅生成指定用户；为 None 则遍历 users 表所有用户。
        report_date: 可选，ISO 日期字符串（'YYYY-MM-DD'）；为 None 则北京自然日。

    Returns:
        dict: {total, succeeded, failed, results: [{user_id, success, ...}]}
    """
    from datetime import date as _date
    from backend.services.daily_market_report_service import DailyMarketReportService
    from backend.utils.datetime_utils import utcnow, window_label_date

    try:
        svc = DailyMarketReportService()

        # 解析 report_date：调度场景默认「北京自然日」（与上游 report_date 体系一致）
        if report_date:
            as_of = _date.fromisoformat(report_date)
        else:
            as_of = window_label_date(utcnow(), 'Asia/Shanghai')

        # 决定目标用户列表
        if user_id:
            target_users = [user_id]
        else:
            target_users = svc.get_eligible_users()

        if not target_users:
            logger.info("generate_all_daily_market_reports: no eligible users, skipping.")
            return {"total": 0, "succeeded": 0, "failed": 0, "results": []}

        logger.info(
            f"generate_all_daily_market_reports: generating for {len(target_users)} "
            f"user(s) on {as_of}"
        )

        results = []
        succeeded = 0
        failed = 0

        for uid in target_users:
            try:
                result = svc.generate(user_id=uid, report_date=as_of)
                results.append({"user_id": uid, **result})
                if result.get("success"):
                    succeeded += 1
                    logger.info(f"generate_all_daily_market_reports: OK for user={uid}")
                else:
                    failed += 1
                    logger.error(
                        f"generate_all_daily_market_reports: FAILED for user={uid}: "
                        f"{result.get('error')}"
                    )
            except Exception as exc:
                failed += 1
                logger.error(
                    f"generate_all_daily_market_reports: exception for user={uid}: {exc}",
                    exc_info=True,
                )
                results.append({"user_id": uid, "success": False, "error": str(exc)})

        summary = {
            "total": len(target_users),
            "succeeded": succeeded,
            "failed": failed,
            "results": results,
        }
        logger.info(
            f"generate_all_daily_market_reports: done — {succeeded}/{len(target_users)} succeeded"
        )
        return summary

    except Exception as exc:
        logger.error(f"generate_all_daily_market_reports unexpected error: {exc}", exc_info=True)
        raise
