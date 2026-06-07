# -*- coding: utf-8 -*-
"""
Market Weekly Report Celery 任务

generate_market_weekly_report: 为指定用户生成静态 HTML Market Weekly 报告，
并上传至阿里云 OSS，将记录写入 market_weekly_reports 表。
"""

from backend.celery_app import app
from backend.utils.logger import info, error


@app.task(bind=True, name='generate_market_weekly_report', queue='data_fetching')
def generate_market_weekly_report_task(
    self,
    user_id: str = None,
    week_end: str = None,
    scheduler_timezone: str = None,
):
    """
    为指定用户生成 Market Weekly Report。

    Args:
        user_id:     用户标识（必须提供）
        week_end:    可选，ISO 日期字符串（'YYYY-MM-DD'）；为 None 则用最新周五。
        scheduler_timezone: 可选，调度器时区（用于显示生成时间）

    Returns:
        dict: {success, oss_object_name, share_token, week_start, week_end, stats, error}
    """
    from datetime import date as _date
    from backend.services.market_weekly_service import MarketWeeklyService

    try:
        if not user_id:
            info("generate_market_weekly_report: no user_id provided, skipping.")
            return {"success": False, "error": "user_id is required"}

        svc = MarketWeeklyService()

        # 解析 week_end
        as_of = None
        if week_end:
            as_of = _date.fromisoformat(week_end)

        info(f"generate_market_weekly_report: generating for user={user_id}, week_end={week_end or 'latest'}")

        result = svc.generate_and_upload(
            user_id=user_id,
            week_end=as_of,
            scheduler_timezone=scheduler_timezone,
        )

        if result.get("success"):
            info(
                f"generate_market_weekly_report: OK for user={user_id}, "
                f"week={result.get('week_start')}-{result.get('week_end')}, "
                f"stats={result.get('stats')}"
            )
        else:
            error(
                f"generate_market_weekly_report: FAILED for user={user_id}: "
                f"{result.get('error')}"
            )

        return result

    except Exception as exc:
        error(f"generate_market_weekly_report: unexpected error: {exc}", exc_info=True)
        return {"success": False, "error": str(exc)}