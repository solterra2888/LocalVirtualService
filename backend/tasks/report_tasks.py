# -*- coding: utf-8 -*-
"""
报告生成 Celery 任务
将耗时的 LLM 报告生成（200-500s/份）放到 Celery worker 异步执行，
避免阻塞 Web 进程和 Scheduler 事件循环。
"""

import asyncio
import time
from celery import current_task
from celery.utils.log import get_task_logger
from backend.celery_app import app

logger = get_task_logger(__name__)


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
    name='generate_twitter_daily_report',
    queue='ai_processing',
    soft_time_limit=600,
    time_limit=900,
)
def generate_twitter_daily_report_task(
    self,
    user_id: int,
    report_date: str,
    force: bool = False,
):
    """
    为单个用户生成 Twitter Daily Report。

    Args:
        user_id: 用户 ID
        report_date: 报告日期 (YYYY-MM-DD)
        force: 是否强制重新生成
    """
    task_id = self.request.id
    start = time.time()
    logger.info(f"[Celery Task {task_id}] 开始生成 Twitter Daily Report: "
                f"user_id={user_id}, report_date={report_date}, force={force}")

    current_task.update_state(state='PROGRESS', meta={
        'status': 'generating',
        'user_id': user_id,
        'report_date': report_date,
    })

    try:
        from datetime import datetime
        from backend.services.twitter_daily_report_service import TwitterDailyReportService

        dt = datetime.strptime(report_date, '%Y-%m-%d').date()
        service = TwitterDailyReportService()
        result = _run_async(service.generate_report(user_id=user_id, report_date=dt, force=force))

        elapsed = round(time.time() - start, 2)
        logger.info(f"[Celery Task {task_id}] Twitter Daily Report 生成完成: "
                    f"user_id={user_id}, success={result.get('success')}, "
                    f"message={result.get('message')}, elapsed={elapsed}s")
        return result

    except Exception as exc:
        elapsed = round(time.time() - start, 2)
        logger.error(f"[Celery Task {task_id}] Twitter Daily Report 生成失败: "
                     f"user_id={user_id}, elapsed={elapsed}s, error={str(exc)}")
        raise self.retry(exc=exc, max_retries=1, countdown=30)


@app.task(
    bind=True,
    name='generate_all_twitter_daily_reports',
    queue='ai_processing',
    soft_time_limit=3600,
    time_limit=4200,
)
def generate_all_twitter_daily_reports_task(self, report_date: str):
    """
    为所有启用的用户分发 Twitter Daily Report 生成任务。
    每个用户作为独立的子任务提交，实现并行生成。

    Args:
        report_date: 报告日期 (YYYY-MM-DD)
    """
    task_id = self.request.id
    logger.info(f"[Celery Task {task_id}] 开始分发 Twitter Daily Report: report_date={report_date}")

    try:
        from backend.services.twitter_daily_report_service import TwitterDailyReportService

        service = TwitterDailyReportService()
        enabled_users = service._get_enabled_twitter_user_ids()

        if not enabled_users:
            logger.info(f"[Celery Task {task_id}] 没有启用的用户，跳过报告生成")
            return {'success': True, 'message': '没有启用的用户', 'dispatched': 0}

        logger.info(f"[Celery Task {task_id}] 发现 {len(enabled_users)} 个启用用户: {enabled_users}")

        dispatched = 0
        for uid in enabled_users:
            sub_task = generate_twitter_daily_report_task.delay(
                user_id=uid,
                report_date=report_date,
                force=False,
            )
            dispatched += 1
            logger.info(f"[Celery Task {task_id}] 已分发子任务: user_id={uid}, sub_task_id={sub_task.id}")

        logger.info(f"[Celery Task {task_id}] 分发完成: {dispatched} 个子任务, report_date={report_date}")
        return {
            'success': True,
            'message': f'已分发 {dispatched} 个用户的报告生成任务',
            'dispatched': dispatched,
            'report_date': report_date,
        }

    except Exception as exc:
        logger.error(f"[Celery Task {task_id}] 分发失败: error={str(exc)}")
        raise
