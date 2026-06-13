# -*- coding: utf-8 -*-
"""
知识星球每日摘要 Celery Tasks

三层任务结构（对标 Twitter Daily Report）:
  1. generate_zhishixingqiu_summary — Phase 1 全局批量（scheduler 入口）
  2. generate_all_zhishixingqiu_user_reports — 派发所有用户的 Phase 2
  3. generate_zhishixingqiu_user_report — 单用户 Phase 2 机械拼接
"""

import asyncio
from datetime import date as _date, datetime as _datetime

from backend.celery_app import app
from backend.utils.logger import get_logger

logger = get_logger("zhishixingqiu_summary_tasks")


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
    name="generate_zhishixingqiu_summary",
    queue="ai_processing",
    max_retries=1,
)
def generate_zhishixingqiu_summary_task(
    self,
    report_date: str = None,
    window_end: str = None,
    window_hours: int = 24,
    force: bool = False,
    chain_user_reports: bool = True,
):
    """
    Phase 1 入口：批量生成所有星球的当日摘要（全局缓存）。
    完成后可选链式触发所有用户的 Phase 2 报告生成。

    Args:
        report_date: ISO 日期字符串 (YYYY-MM-DD)，默认为今天。
        window_end: ISO 时间字符串（UTC），内容窗口结束时间。
        window_hours: 回溯小时数，默认 24。
        force: 是否强制重新生成，默认 False。
        chain_user_reports: 完成后是否自动触发按用户报告生成。
    """
    from backend.services.zhishixingqiu_summary_service import ZhishixingqiuSummaryService

    task_id = self.request.id
    try:
        svc = ZhishixingqiuSummaryService()

        as_of = _date.fromisoformat(report_date) if report_date else _date.today()
        we = _datetime.fromisoformat(window_end) if window_end else None

        result = _run_async(
            svc.generate_all_group_summaries(
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
            report_task = generate_all_zhishixingqiu_user_reports_task.delay(
                report_date=str(as_of),
                force=force,
                window_end=window_end,
                window_hours=window_hours,
            )
            logger.info(
                f"[Celery Task {task_id}] 已链式触发用户报告: task_id={report_task.id}"
            )

        return {
            **result,
            "report_date": str(as_of),
            "chain_user_reports": chain_user_reports,
        }

    except Exception as exc:
        logger.error(
            f"[Celery Task {task_id}] Phase 1 失败: {exc}", exc_info=True
        )
        raise


@app.task(
    bind=True,
    name="generate_all_zhishixingqiu_user_reports",
    queue="ai_processing",
    soft_time_limit=600,
    time_limit=900,
)
def generate_all_zhishixingqiu_user_reports_task(
    self,
    report_date: str,
    force: bool = False,
    window_end: str = None,
    window_hours: int = 24,
):
    """为所有有启用知识星球订阅的用户分发 Phase 2 报告生成任务。"""
    task_id = self.request.id
    logger.info(
        f"[Celery Task {task_id}] 开始分发知识星球用户报告: report_date={report_date}"
    )

    try:
        from backend.services.zhishixingqiu_summary_service import ZhishixingqiuSummaryService

        service = ZhishixingqiuSummaryService()
        enabled_users = service._get_enabled_zhishixingqiu_user_ids()

        if not enabled_users:
            logger.info(f"[Celery Task {task_id}] 没有启用的用户，跳过报告生成")
            return {"success": True, "message": "没有启用的用户", "dispatched": 0}

        logger.info(
            f"[Celery Task {task_id}] 发现 {len(enabled_users)} 个用户: {enabled_users}"
        )

        dispatched = 0
        for uid in enabled_users:
            sub_task = generate_zhishixingqiu_user_report_task.delay(
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
    name="generate_zhishixingqiu_user_report",
    queue="ai_processing",
    soft_time_limit=600,
    time_limit=900,
)
def generate_zhishixingqiu_user_report_task(
    self,
    user_id: int,
    report_date: str,
    force: bool = False,
    window_end: str = None,
    window_hours: int = 24,
):
    """为单个用户生成知识星球每日报告（Phase 2 机械拼接）。"""
    from backend.services.zhishixingqiu_summary_service import ZhishixingqiuSummaryService

    task_id = self.request.id
    logger.info(
        f"[Celery Task {task_id}] 开始生成知识星球用户报告: "
        f"user_id={user_id}, report_date={report_date}, force={force}"
    )

    try:
        svc = ZhishixingqiuSummaryService()
        as_of = _date.fromisoformat(report_date)
        we = _datetime.fromisoformat(window_end) if window_end else None

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
            logger.error(
                f"[Celery Task {task_id}] 用户报告失败: {result.get('message')}"
            )

        return result

    except Exception as exc:
        logger.error(
            f"[Celery Task {task_id}] 用户报告意外错误: {exc}", exc_info=True
        )
        raise
