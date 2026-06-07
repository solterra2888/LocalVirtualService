# -*- coding: utf-8 -*-
"""
Market Weekly Report Celery 任务

generate_all_market_weekly_reports: 为所有打了 'Market Weekly' 标签的用户各生成一份静态 HTML 报告。
generate_market_weekly_report: 为指定用户生成单份报告（手动触发等场景）。
"""

from backend.celery_app import app
from backend.utils.logger import info, error


@app.task(bind=True, name='generate_all_market_weekly_reports', queue='data_fetching')
def generate_all_market_weekly_reports_task(
    self,
    user_id: str = None,
    week_end: str = None,
    scheduler_timezone: str = None,
):
    """
    为所有（或指定）打了 'Market Weekly' 标签的用户生成 Market Weekly Report。

    Args:
        user_id:     可选，仅生成指定用户的报告；为 None 则遍历所有打标用户。
        week_end:    可选，ISO 日期字符串（'YYYY-MM-DD'）；为 None 则用最新周五。
        scheduler_timezone: 可选，调度器时区（用于显示生成时间）

    Returns:
        dict: {total, succeeded, failed, results: [{user_id, success, ...}]}
    """
    from datetime import date as _date
    from backend.services.market_weekly_service import MarketWeeklyService

    try:
        svc = MarketWeeklyService()

        as_of = None
        if week_end:
            as_of = _date.fromisoformat(week_end)

        if user_id:
            target_users = [user_id]
        else:
            target_users = svc.get_users_with_tag()

        if not target_users:
            info("generate_all_market_weekly_reports: no users with 'Market Weekly' tag found, skipping.")
            return {"total": 0, "succeeded": 0, "failed": 0, "results": []}

        info(f"generate_all_market_weekly_reports: generating for {len(target_users)} user(s): {target_users}")

        results = []
        succeeded = 0
        failed = 0

        for uid in target_users:
            try:
                result = svc.generate_and_upload(
                    user_id=uid,
                    week_end=as_of,
                    scheduler_timezone=scheduler_timezone,
                )
                results.append({"user_id": uid, **result})
                if result.get("success"):
                    succeeded += 1
                    info(
                        f"generate_all_market_weekly_reports: OK for user={uid}, "
                        f"week={result.get('week_start')}-{result.get('week_end')}"
                    )
                else:
                    failed += 1
                    error(
                        f"generate_all_market_weekly_reports: FAILED for user={uid}: "
                        f"{result.get('error')}"
                    )
            except Exception as exc:
                failed += 1
                error(f"generate_all_market_weekly_reports: exception for user={uid}: {exc}")
                results.append({"user_id": uid, "success": False, "error": str(exc)})

        summary = {
            "total": len(target_users),
            "succeeded": succeeded,
            "failed": failed,
            "results": results,
        }
        info(f"generate_all_market_weekly_reports: done — {succeeded}/{len(target_users)} succeeded")
        return summary

    except Exception as exc:
        error(f"generate_all_market_weekly_reports: unexpected error: {exc}", exc_info=True)
        raise


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