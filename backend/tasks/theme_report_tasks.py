# -*- coding: utf-8 -*-
"""
Theme Report Celery 任务

generate_all_theme_reports: 为所有打了 'Theme Report' 标签的用户各生成一份静态 HTML 报告，
并上传至阿里云 OSS，将记录写入 theme_reports 表。
"""

from backend.celery_app import app
from backend.utils.logger import info, error


@app.task(bind=True, name='backfill_theme_report_eod_history', queue='data_fetching')
def backfill_theme_report_eod_history_task(self, days_back: int = 400):
    """
    为 'Theme Report' 标签的所有主题关联 tickers 回填 EOD 历史数据。

    直接调用已有的 fetch_eodhd_eod_stock_prices_task（days_back=400），
    该任务会从 theme_company_associations 中读取所有 ticker 并补全历史。

    Args:
        days_back: 回溯天数，默认 400（覆盖 1Y+）。
    """
    from backend.tasks.data_tasks import fetch_eodhd_eod_stock_prices_task

    info(f"backfill_theme_report_eod_history: triggering EOD backfill with days_back={days_back}")
    result = fetch_eodhd_eod_stock_prices_task(days_back=days_back, periods=['d'])
    info(f"backfill_theme_report_eod_history: EOD backfill complete — {result}")
    return result


@app.task(bind=True, name='generate_all_theme_reports', queue='data_fetching')
def generate_all_theme_reports_task(
    self,
    user_id: str = None,
    report_date: str = None,
    scheduler_timezone: str = None,
):
    """
    为所有（或指定）打了 'Theme Report' 标签的用户生成 Theme Report。

    Args:
        user_id:     可选，仅生成指定用户的报告；为 None 则遍历所有打标用户。
        report_date: 可选，ISO 日期字符串（'YYYY-MM-DD'）；为 None 则用今天。

    Returns:
        dict: {total, succeeded, failed, results: [{user_id, success, ...}]}
    """
    from datetime import date as _date
    from backend.services.theme_report_service import ThemeReportService

    try:
        svc = ThemeReportService()

        # 解析 report_date
        as_of = None
        if report_date:
            as_of = _date.fromisoformat(report_date)

        # 决定目标用户列表
        if user_id:
            target_users = [user_id]
        else:
            target_users = svc.get_users_with_tag()

        if not target_users:
            info("generate_all_theme_reports: no users with 'Theme Report' tag found, skipping.")
            return {"total": 0, "succeeded": 0, "failed": 0, "results": []}

        info(f"generate_all_theme_reports: generating for {len(target_users)} user(s): {target_users}")

        results = []
        succeeded = 0
        failed = 0

        for uid in target_users:
            try:
                result = svc.generate_and_upload(
                    user_id=uid,
                    report_date=as_of,
                    scheduler_timezone=scheduler_timezone,
                )
                results.append({"user_id": uid, **result})
                if result.get("success"):
                    succeeded += 1
                    info(
                        f"generate_all_theme_reports: OK for user={uid}, "
                        f"themes={result.get('stats', {}).get('theme_count', '?')}"
                    )
                else:
                    failed += 1
                    error(
                        f"generate_all_theme_reports: FAILED for user={uid}: "
                        f"{result.get('error')}"
                    )
            except Exception as exc:
                failed += 1
                error(f"generate_all_theme_reports: exception for user={uid}: {exc}")
                results.append({"user_id": uid, "success": False, "error": str(exc)})

        summary = {
            "total": len(target_users),
            "succeeded": succeeded,
            "failed": failed,
            "results": results,
        }
        info(f"generate_all_theme_reports: done — {succeeded}/{len(target_users)} succeeded")
        return summary

    except Exception as exc:
        error(f"generate_all_theme_reports: unexpected error: {exc}", exc_info=True)
        raise
