# -*- coding: utf-8 -*-
"""
报告生成 Celery 任务
将耗时的 LLM 报告生成放到 Celery worker 异步执行，
避免阻塞 Web 进程和 Scheduler 事件循环。

Twitter 任务分两阶段：
  1. 作者摘要（全局共享）: generate_all_author_summaries → generate_author_summary
  2. 用户报告（per-user）: generate_all_twitter_daily_reports → generate_twitter_daily_report

YouTube 任务分两阶段（同构）：
  1. 频道摘要（全局共享）: generate_all_yt_author_summaries → generate_yt_author_summary
  2. 用户报告（per-user）: generate_all_youtube_daily_reports → generate_youtube_daily_report
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


# ── 阶段 A：全局作者摘要任务 ─────────────────────────────────────────────

@app.task(
    bind=True,
    name='generate_author_summary',
    queue='ai_processing',
    soft_time_limit=300,
    time_limit=600,
)
def generate_author_summary_task(
    self,
    author_identifier: str,
    author_name: str,
    summary_date: str,
    force: bool = False,
    window_end: str = None,
    window_hours: int = 24,
):
    """
    为单个 Twitter 作者生成指定日期的摘要（Phase 1）。
    结果存入 twitter_author_daily_summaries，供所有用户共享。
    """
    task_id = self.request.id
    start = time.time()
    logger.info(f"[Celery Task {task_id}] 开始生成作者摘要: "
                f"@{author_identifier}, date={summary_date}, force={force}")

    try:
        from datetime import datetime
        from backend.services.twitter_author_summary_service import TwitterAuthorSummaryService
        from backend.utils.datetime_utils import parse_iso_utc

        dt = datetime.strptime(summary_date, '%Y-%m-%d').date()
        service = TwitterAuthorSummaryService()
        result = _run_async(service.generate_author_summary(
            author_identifier=author_identifier,
            author_name=author_name,
            summary_date=dt,
            force=force,
            window_end=parse_iso_utc(window_end),
            window_hours=window_hours,
        ))

        elapsed = round(time.time() - start, 2)
        logger.info(f"[Celery Task {task_id}] 作者摘要完成: @{author_identifier}, "
                    f"success={result.get('success')}, elapsed={elapsed}s")
        return result

    except Exception as exc:
        elapsed = round(time.time() - start, 2)
        logger.error(f"[Celery Task {task_id}] 作者摘要失败: "
                     f"@{author_identifier}, elapsed={elapsed}s, error={str(exc)}")
        raise self.retry(exc=exc, max_retries=1, countdown=30)


@app.task(
    bind=True,
    name='generate_all_author_summaries',
    queue='ai_processing',
    soft_time_limit=3600,
    time_limit=4200,
)
def generate_all_author_summaries_task(
    self,
    summary_date: str,
    force: bool = False,
    chain_daily_reports: bool = True,
    window_end: str = None,
    window_hours: int = 24,
):
    """
    为所有启用的 Twitter 作者批量生成当日摘要（同步执行，逐作者串行）。
    全部完成后再链式触发 generate_all_twitter_daily_reports，
    确保 Daily Report 生成时所有缓存摘要已就绪。

    Args:
        summary_date: 摘要日期 (YYYY-MM-DD)
        force: 是否强制重新生成
        chain_daily_reports: 完成后是否自动触发 Daily Report 生成
    """
    task_id = self.request.id
    logger.info(f"[Celery Task {task_id}] 开始批量生成作者摘要: date={summary_date}")

    try:
        from datetime import datetime
        from backend.services.twitter_author_summary_service import TwitterAuthorSummaryService
        from backend.utils.datetime_utils import parse_iso_utc

        dt = datetime.strptime(summary_date, '%Y-%m-%d').date()
        parsed_window_end = parse_iso_utc(window_end)
        service = TwitterAuthorSummaryService()
        result = _run_async(service.generate_all_author_summaries(
            dt,
            force=force,
            window_end=parsed_window_end,
            window_hours=window_hours,
        ))

        logger.info(f"[Celery Task {task_id}] 作者摘要批量完成: {result.get('message')}")

        if chain_daily_reports:
            report_task = generate_all_twitter_daily_reports_task.delay(
                report_date=summary_date,
                force=force,
                window_end=window_end,
                window_hours=window_hours,
            )
            logger.info(f"[Celery Task {task_id}] 已链式触发 Daily Report: task_id={report_task.id}, force={force}")

        return {
            'success': True,
            'message': result.get('message', ''),
            'total': result.get('total', 0),
            'succeeded': result.get('succeeded', 0),
            'skipped': result.get('skipped', 0),
            'summary_date': summary_date,
            'chain_daily_reports': chain_daily_reports,
        }

    except Exception as exc:
        logger.error(f"[Celery Task {task_id}] 作者摘要批量失败: error={str(exc)}")
        raise


# ── 阶段 B：用户报告任务 ──────────────────────────────────────────────────

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
    window_end: str = None,
    window_hours: int = 24,
):
    """
    为单个用户生成 Twitter Daily Report。
    会从 twitter_author_daily_summaries 读取预计算摘要，
    缺失时自动 fallback 到即时生成。
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
        from backend.utils.datetime_utils import parse_iso_utc

        dt = datetime.strptime(report_date, '%Y-%m-%d').date()
        service = TwitterDailyReportService()
        result = _run_async(service.generate_report(
            user_id=user_id,
            report_date=dt,
            force=force,
            window_end=parse_iso_utc(window_end),
            window_hours=window_hours,
        ))

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
def generate_all_twitter_daily_reports_task(
    self,
    report_date: str,
    force: bool = False,
    window_end: str = None,
    window_hours: int = 24,
):
    """
    为所有启用的用户分发 Twitter Daily Report 生成任务。
    每个用户作为独立的子任务提交，实现并行生成。
    """
    task_id = self.request.id
    logger.info(f"[Celery Task {task_id}] 开始分发 Twitter Daily Report: report_date={report_date}, force={force}")

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
                force=force,
                window_end=window_end,
                window_hours=window_hours,
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


# ══════════════════════════════════════════════════════════════════════════
# YouTube — 阶段 A：全局频道摘要任务
# ══════════════════════════════════════════════════════════════════════════

@app.task(
    bind=True,
    name='generate_yt_author_summary',
    queue='ai_processing',
    soft_time_limit=300,
    time_limit=600,
)
def generate_yt_author_summary_task(
    self,
    author_identifier: str,
    author_name: str,
    summary_date: str,
    force: bool = False,
    window_end: str = None,
    window_hours: int = 24,
):
    """
    为单个 YouTube 频道生成指定日期的摘要（Phase 1）。
    结果存入 youtube_author_daily_summaries，供所有用户共享。
    """
    task_id = self.request.id
    start = time.time()
    logger.info(f"[Celery Task {task_id}] 开始生成 YouTube 频道摘要: "
                f"{author_identifier}, date={summary_date}, force={force}")

    try:
        from datetime import datetime
        from backend.services.youtube_author_summary_service import YouTubeAuthorSummaryService
        from backend.utils.datetime_utils import parse_iso_utc

        dt = datetime.strptime(summary_date, '%Y-%m-%d').date()
        service = YouTubeAuthorSummaryService()
        result = _run_async(service.generate_author_summary(
            author_identifier=author_identifier,
            author_name=author_name,
            summary_date=dt,
            force=force,
            window_end=parse_iso_utc(window_end),
            window_hours=window_hours,
        ))

        elapsed = round(time.time() - start, 2)
        logger.info(f"[Celery Task {task_id}] YouTube 频道摘要完成: {author_identifier}, "
                    f"success={result.get('success')}, elapsed={elapsed}s")
        return result

    except Exception as exc:
        elapsed = round(time.time() - start, 2)
        logger.error(f"[Celery Task {task_id}] YouTube 频道摘要失败: "
                     f"{author_identifier}, elapsed={elapsed}s, error={str(exc)}")
        raise self.retry(exc=exc, max_retries=1, countdown=30)


@app.task(
    bind=True,
    name='generate_all_yt_author_summaries',
    queue='ai_processing',
    soft_time_limit=3600,
    time_limit=4200,
)
def generate_all_yt_author_summaries_task(
    self,
    summary_date: str,
    force: bool = False,
    chain_daily_reports: bool = True,
    window_end: str = None,
    window_hours: int = 24,
):
    """
    为所有启用的 YouTube 频道批量生成当日摘要（同步执行，逐频道串行）。
    全部完成后再链式触发 generate_all_youtube_daily_reports，
    确保 Daily Report 生成时所有缓存摘要已就绪。
    """
    task_id = self.request.id
    logger.info(f"[Celery Task {task_id}] 开始批量生成 YouTube 频道摘要: date={summary_date}")

    try:
        from datetime import datetime
        from backend.services.youtube_author_summary_service import YouTubeAuthorSummaryService
        from backend.utils.datetime_utils import parse_iso_utc

        dt = datetime.strptime(summary_date, '%Y-%m-%d').date()
        service = YouTubeAuthorSummaryService()
        result = _run_async(service.generate_all_author_summaries(
            dt,
            force=force,
            window_end=parse_iso_utc(window_end),
            window_hours=window_hours,
        ))

        logger.info(f"[Celery Task {task_id}] YouTube 频道摘要批量完成: {result.get('message')}")

        if chain_daily_reports:
            report_task = generate_all_youtube_daily_reports_task.delay(
                report_date=summary_date,
                force=force,
                window_end=window_end,
                window_hours=window_hours,
            )
            logger.info(f"[Celery Task {task_id}] 已链式触发 YouTube Daily Report: task_id={report_task.id}, force={force}")

        return {
            'success': True,
            'message': result.get('message', ''),
            'total': result.get('total', 0),
            'succeeded': result.get('succeeded', 0),
            'skipped': result.get('skipped', 0),
            'summary_date': summary_date,
            'chain_daily_reports': chain_daily_reports,
        }

    except Exception as exc:
        logger.error(f"[Celery Task {task_id}] YouTube 频道摘要批量失败: error={str(exc)}")
        raise


# ── YouTube — 阶段 B：用户报告任务 ─────────────────────────────────────────

@app.task(
    bind=True,
    name='generate_youtube_daily_report',
    queue='ai_processing',
    soft_time_limit=600,
    time_limit=900,
)
def generate_youtube_daily_report_task(
    self,
    user_id: int,
    report_date: str,
    force: bool = False,
    window_end: str = None,
    window_hours: int = 24,
):
    """
    为单个用户生成 YouTube Daily Report。
    会从 youtube_author_daily_summaries 读取预计算摘要，
    缺失时自动 fallback 到即时生成。
    """
    task_id = self.request.id
    start = time.time()
    logger.info(f"[Celery Task {task_id}] 开始生成 YouTube Daily Report: "
                f"user_id={user_id}, report_date={report_date}, force={force}")

    current_task.update_state(state='PROGRESS', meta={
        'status': 'generating',
        'user_id': user_id,
        'report_date': report_date,
    })

    try:
        from datetime import datetime
        from backend.services.youtube_daily_report_service import YouTubeDailyReportService
        from backend.utils.datetime_utils import parse_iso_utc

        dt = datetime.strptime(report_date, '%Y-%m-%d').date()
        service = YouTubeDailyReportService()
        result = _run_async(service.generate_report(
            user_id=user_id,
            report_date=dt,
            force=force,
            window_end=parse_iso_utc(window_end),
            window_hours=window_hours,
        ))

        elapsed = round(time.time() - start, 2)
        logger.info(f"[Celery Task {task_id}] YouTube Daily Report 生成完成: "
                    f"user_id={user_id}, success={result.get('success')}, "
                    f"message={result.get('message')}, elapsed={elapsed}s")
        return result

    except Exception as exc:
        elapsed = round(time.time() - start, 2)
        logger.error(f"[Celery Task {task_id}] YouTube Daily Report 生成失败: "
                     f"user_id={user_id}, elapsed={elapsed}s, error={str(exc)}")
        raise self.retry(exc=exc, max_retries=1, countdown=30)


@app.task(
    bind=True,
    name='generate_all_youtube_daily_reports',
    queue='ai_processing',
    soft_time_limit=3600,
    time_limit=4200,
)
def generate_all_youtube_daily_reports_task(
    self,
    report_date: str,
    force: bool = False,
    window_end: str = None,
    window_hours: int = 24,
):
    """
    为所有启用的用户分发 YouTube Daily Report 生成任务。
    每个用户作为独立的子任务提交，实现并行生成。
    """
    task_id = self.request.id
    logger.info(f"[Celery Task {task_id}] 开始分发 YouTube Daily Report: report_date={report_date}, force={force}")

    try:
        from backend.services.youtube_daily_report_service import YouTubeDailyReportService

        service = YouTubeDailyReportService()
        enabled_users = service._get_enabled_youtube_user_ids()

        if not enabled_users:
            logger.info(f"[Celery Task {task_id}] 没有启用 YouTube 订阅的用户，跳过报告生成")
            return {'success': True, 'message': '没有启用的用户', 'dispatched': 0}

        logger.info(f"[Celery Task {task_id}] 发现 {len(enabled_users)} 个启用用户: {enabled_users}")

        dispatched = 0
        for uid in enabled_users:
            sub_task = generate_youtube_daily_report_task.delay(
                user_id=uid,
                report_date=report_date,
                force=force,
                window_end=window_end,
                window_hours=window_hours,
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
        logger.error(f"[Celery Task {task_id}] YouTube Daily Report 分发失败: error={str(exc)}")
        raise
