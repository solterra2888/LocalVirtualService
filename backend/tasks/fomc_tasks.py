# -*- coding: utf-8 -*-
"""
央行 Dashboard — Fed FOMC Celery Tasks

任务链: fomc_poll_updates（发现）→ fomc_fetch_document（抓取+抽取）
       → fomc_summarize（LLM 概括）
兜底:  fomc_retry_pending（每日重试 failed/pending）
回填:  fomc_backfill_2026（2026 年五次会议一次性回填）
"""

import asyncio

from backend.celery_app import app
from backend.utils.logger import get_logger

logger = get_logger("fomc_tasks")


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


def _get_service():
    from backend.services.fed_dashboard_service import FedDashboardService
    return FedDashboardService()


def _dispatch(task_name: str, **kwargs):
    """链式派发下一个任务（同进程直接调，跨队列走 Celery delay）"""
    try:
        from backend.celery_app import app as celery_app
        celery_app.send_task(task_name, kwargs=kwargs)
        logger.info(f"[FOMC] 已派发 {task_name} kwargs={kwargs}")
    except Exception as e:
        logger.warning(f"[FOMC] 派发 {task_name} 失败: {e}")


@app.task(bind=True, name="fomc_poll_updates", queue="data_fetching", max_retries=1)
def fomc_poll_updates_task(self):
    """轮询 Fed press_all.xml RSS: 写 updates 流 + 发现新 FOMC 文档 + 派发抓取"""
    try:
        svc = _get_service()
        result = svc.poll_fed_updates()

        # 对 pending 文档派发抓取
        rows = svc.db_manager.fetch_all(
            "SELECT id, doc_type FROM fomc_documents "
            "WHERE fetch_status = 'pending' ORDER BY id LIMIT 10"
        )
        for doc_id, doc_type in rows:
            _dispatch("fomc_fetch_document", doc_id=doc_id)

        # 每次轮询顺带兜底重试少量 failed（纪要 404 未发布场景靠这里自愈）
        svc.retry_pending_documents()

        return result
    except Exception as exc:
        logger.error(f"[FOMC] poll_updates 失败: {exc}", exc_info=True)
        raise


@app.task(bind=True, name="fomc_fetch_document", queue="data_fetching", max_retries=1)
def fomc_fetch_document_task(self, doc_id: int):
    """抓取单个文档 + （声明）结构化抽取 + 派发 LLM 概括"""
    try:
        svc = _get_service()
        svc._patch_meeting_date_from_url(doc_id, _doc_type_of(svc, doc_id))
        result = svc.fetch_document(doc_id)

        doc = svc._get_document(doc_id)
        if doc and doc["doc_type"] == "statement":
            svc.extract_decision(doc_id)

        # 抓取成功且内容有变化 → 派发概括（ai_processing 队列）
        if not result.get("unchanged"):
            _dispatch("fomc_summarize", doc_id=doc_id)

        return {"doc_id": doc_id, **result}
    except Exception as exc:
        logger.error(f"[FOMC] fetch_document doc={doc_id} 失败: {exc}", exc_info=True)
        raise


def _doc_type_of(svc, doc_id: int) -> str:
    doc = svc._get_document(doc_id)
    return doc["doc_type"] if doc else "statement"


@app.task(bind=True, name="fomc_summarize", queue="ai_processing", max_retries=1)
def fomc_summarize_task(self, doc_id: int, force: bool = False):
    """对单个文档生成 LLM 中文概括"""
    try:
        svc = _get_service()
        result = _run_async(svc.generate_summary(doc_id, force=force))
        return result
    except Exception as exc:
        logger.error(f"[FOMC] summarize doc={doc_id} 失败: {exc}", exc_info=True)
        raise


@app.task(bind=True, name="fomc_retry_pending", queue="data_fetching", max_retries=1)
def fomc_retry_pending_task(self):
    """兜底重试 pending/failed 文档（每日一次）"""
    try:
        svc = _get_service()
        count = svc.retry_pending_documents()
        # 重试成功的派发概括
        rows = svc.db_manager.fetch_all(
            """
            SELECT d.id FROM fomc_documents d
            WHERE d.fetch_status = 'fetched'
              AND NOT EXISTS (
                  SELECT 1 FROM fomc_summaries s
                  WHERE s.document_id = d.id AND s.status = 'completed'
              )
            ORDER BY d.id LIMIT 10
            """
        )
        for (doc_id,) in rows:
            _dispatch("fomc_summarize", doc_id=doc_id)
        return {"retried": count}
    except Exception as exc:
        logger.error(f"[FOMC] retry_pending 失败: {exc}", exc_info=True)
        raise


@app.task(bind=True, name="fomc_backfill_2026", queue="data_fetching", max_retries=1)
def fomc_backfill_2026_task(self, summarize: bool = True):
    """回填 2026 年五次会议声明+纪要（幂等，可重复执行）"""
    try:
        svc = _get_service()
        stats = svc.backfill_2026()
        if summarize:
            # 回填完成后对全部未概括文档派发 LLM
            rows = svc.db_manager.fetch_all(
                """
                SELECT d.id FROM fomc_documents d
                WHERE d.fetch_status = 'fetched'
                  AND NOT EXISTS (
                      SELECT 1 FROM fomc_summaries s
                      WHERE s.document_id = d.id AND s.status = 'completed'
                  )
                ORDER BY d.meeting_date, d.doc_type
                """
            )
            for (doc_id,) in rows:
                _dispatch("fomc_summarize", doc_id=doc_id)
        return stats
    except Exception as exc:
        logger.error(f"[FOMC] backfill_2026 失败: {exc}", exc_info=True)
        raise
