# -*- coding: utf-8 -*-
"""
Substack 每日摘要 Celery Tasks

三层任务结构（对标 zhishixingqiu_summary_tasks）:
  1. generate_substack_summary — Phase 1 全局批量（scheduler 入口）
  2. generate_all_substack_user_reports — 派发所有用户的 Phase 2
  3. generate_substack_user_report — 单用户 Phase 2 机械拼接
"""

import asyncio
from datetime import date as _date, datetime as _datetime
from typing import Optional, Tuple

from backend.celery_app import app
from backend.utils.logger import get_logger

logger = get_logger("substack_summary_tasks")

DEFAULT_TIMEZONE = "Asia/Shanghai"


def _resolve_substack_task_dates(
    report_date: Optional[str] = None,
    window_end: Optional[str] = None,
) -> Tuple[_date, Optional[_datetime]]:
    from backend.utils.datetime_utils import parse_iso_utc, utcnow, window_label_date

    we = parse_iso_utc(window_end)
    if we is None:
        we = utcnow()

    if report_date:
        as_of = _date.fromisoformat(report_date)
    else:
        as_of = window_label_date(we, DEFAULT_TIMEZONE)

    return as_of, we


def _run_async(coro):
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
    name="generate_substack_summary",
    queue="ai_processing",
    max_retries=1,
)
def generate_substack_summary_task(
    self,
    report_date: str = None,
    window_end: str = None,
    window_hours: int = 24,
    force: bool = False,
    chain_user_reports: bool = True,
):
    """Phase 1 入口：批量生成所有 publication 的当日摘要（全局缓存）。"""
    from backend.services.substack_summary_service import SubstackSummaryService

    task_id = self.request.id
    try:
        svc = SubstackSummaryService()
        as_of, we = _resolve_substack_task_dates(report_date, window_end)
        window_end_iso = we.isoformat()

        result = _run_async(
            svc.generate_all_publication_summaries(
                report_date=as_of,
                window_end=we,
                window_hours=window_hours,
                force=force,
            )
        )

        logger.info(
            f"[Celery Task {task_id}] Phase 1 完成: date={as_of}, "
            f"message={result.get('message')}"
        )

        if chain_user_reports:
            report_task = generate_all_substack_user_reports_task.delay(
                report_date=str(as_of),
                force=force,
                window_end=window_end_iso,
                window_hours=window_hours,
            )
            logger.info(
                f"[Celery Task {task_id}] 已链式触发用户报告: task_id={report_task.id}"
            )

        return {**result, "report_date": str(as_of), "chain_user_reports": chain_user_reports}

    except Exception as exc:
        logger.error(f"[Celery Task {task_id}] Phase 1 失败: {exc}", exc_info=True)
        raise


@app.task(
    bind=True,
    name="generate_all_substack_user_reports",
    queue="ai_processing",
    soft_time_limit=600,
    time_limit=900,
)
def generate_all_substack_user_reports_task(
    self,
    report_date: str,
    force: bool = False,
    window_end: str = None,
    window_hours: int = 24,
):
    """为所有有启用 Substack 订阅的用户分发 Phase 2 报告生成任务。"""
    task_id = self.request.id
    logger.info(
        f"[Celery Task {task_id}] 开始分发 Substack 用户报告: report_date={report_date}"
    )

    try:
        from backend.services.substack_summary_service import SubstackSummaryService

        service = SubstackSummaryService()
        enabled_users = service._get_enabled_substack_user_ids()

        if not enabled_users:
            logger.info(f"[Celery Task {task_id}] 没有启用的用户，跳过报告生成")
            return {"success": True, "message": "没有启用的用户", "dispatched": 0}

        logger.info(
            f"[Celery Task {task_id}] 发现 {len(enabled_users)} 个用户: {enabled_users}"
        )

        dispatched = 0
        for uid in enabled_users:
            sub_task = generate_substack_user_report_task.delay(
                user_id=uid,
                report_date=report_date,
                force=force,
                window_end=window_end,
                window_hours=window_hours,
            )
            dispatched += 1
            logger.info(
                f"[Celery Task {task_id}] 已分发子任务: user_id={uid}, "
                f"sub_task_id={sub_task.id}"
            )

        return {
            "success": True,
            "message": f"已分发 {dispatched} 个用户的报告生成任务",
            "dispatched": dispatched,
            "report_date": report_date,
        }

    except Exception as exc:
        logger.error(f"[Celery Task {task_id}] 分发失败: error={str(exc)}")
        raise


@app.task(
    bind=True,
    name="generate_substack_user_report",
    queue="ai_processing",
    soft_time_limit=600,
    time_limit=900,
)
def generate_substack_user_report_task(
    self,
    user_id: int,
    report_date: str,
    force: bool = False,
    window_end: str = None,
    window_hours: int = 24,
):
    """为单个用户生成 Substack 每日报告（Phase 2 机械拼接）。"""
    from backend.services.substack_summary_service import SubstackSummaryService

    task_id = self.request.id
    logger.info(
        f"[Celery Task {task_id}] 开始生成 Substack 用户报告: "
        f"user_id={user_id}, report_date={report_date}, force={force}"
    )

    try:
        svc = SubstackSummaryService()
        as_of, we = _resolve_substack_task_dates(report_date, window_end)

        result = _run_async(
            svc.generate_user_report(
                user_id=user_id,
                report_date=as_of,
                window_end=we,
                window_hours=window_hours,
                force=force,
            )
        )

        if result.get("success"):
            logger.info(
                f"[Celery Task {task_id}] 用户报告完成: user_id={user_id}, "
                f"report_id={result.get('report_id')}, skipped={result.get('skipped', False)}"
            )
        else:
            logger.error(f"[Celery Task {task_id}] 用户报告失败: {result.get('message')}")

        return result

    except Exception as exc:
        logger.error(f"[Celery Task {task_id}] 用户报告意外错误: {exc}", exc_info=True)
        raise
