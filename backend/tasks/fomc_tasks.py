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


_UNSUMMARIZED_ORDERS = {
    "id": "d.id",
    "meeting": "d.meeting_date, d.doc_type",
}


def _needs_summary(svc, doc_id: int) -> bool:
    """已抓取文档是否还没有 completed 概括。"""
    rows = svc.db_manager.fetch_all(
        """
        SELECT 1 FROM fomc_summaries
        WHERE document_id = %s AND status = 'completed'
        LIMIT 1
        """,
        (doc_id,),
    )
    return not rows


def _dispatch_unsummarized(svc, *, order: str = "id", limit: int = 10):
    """给已 fetched 但无 completed 概括的文档补派发 fomc_summarize。

    覆盖：纪要 404 自愈后漏派发、回填后未概括、以及 poll 与内联 retry 竞态遗留。
    generate_summary 对已 completed 幂等跳过，重复派发安全。
    """
    order_sql = _UNSUMMARIZED_ORDERS.get(order, "d.id")
    rows = svc.db_manager.fetch_all(
        f"""
        SELECT d.id FROM fomc_documents d
        WHERE d.fetch_status = 'fetched'
          AND NOT EXISTS (
              SELECT 1 FROM fomc_summaries s
              WHERE s.document_id = d.id AND s.status = 'completed'
          )
        ORDER BY {order_sql}
        LIMIT %s
        """,
        (limit,),
    )
    for (doc_id,) in rows:
        _dispatch("fomc_summarize", doc_id=doc_id)
    return [doc_id for (doc_id,) in rows]


@app.task(bind=True, name="fomc_poll_updates", queue="data_fetching", max_retries=1)
def fomc_poll_updates_task(self):
    """轮询 Fed press_all.xml RSS: 写 updates 流 + 发现新 FOMC 文档 + 派发抓取"""
    try:
        svc = _get_service()
        result = svc.poll_fed_updates()

        # pending + failed 统一走 fomc_fetch_document（抽取 + 概括派发）。
        # 不内联 retry_pending_documents：避免与 Celery 抢抓 pending，
        # 导致 content_hash 已写入、后续任务判定 unchanged 而跳过概括。
        rows = svc.db_manager.fetch_all(
            "SELECT id FROM fomc_documents "
            "WHERE fetch_status IN ('pending', 'failed') "
            "ORDER BY id LIMIT 10"
        )
        for (doc_id,) in rows:
            _dispatch("fomc_fetch_document", doc_id=doc_id)

        # 已 fetched 但无 completed 概括（404 自愈漏派发）→ 补派发
        _dispatch_unsummarized(svc)

        # 概率快照: 每天首个轮询窗口顺带执行（service 内按日节流，
        # 已有当日快照时 no-op；task_scheduling UNIQUE(type) 决定不单设调度行）
        _dispatch("fomc_compute_probabilities")

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

        # 内容有变化，或已抓取但从未完成概括（404 自愈 / 竞态遗留）→ 派发 LLM
        if not result.get("unchanged") or _needs_summary(svc, doc_id):
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
        _dispatch_unsummarized(svc)
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
            _dispatch_unsummarized(svc, order="meeting", limit=20)
        return stats
    except Exception as exc:
        logger.error(f"[FOMC] backfill_2026 失败: {exc}", exc_info=True)
        raise


@app.task(bind=True, name="fomc_compute_probabilities", queue="data_fetching", max_retries=1)
def fomc_compute_probabilities_task(self, force: bool = False):
    """每日概率快照: 同步会议日历 + ZQ 期货 + EFFR → 未来 4 次会议概率入库

    默认按日节流（poll 链派发时已跑过则跳过）；force=True 供手动刷新。
    """
    try:
        svc = _get_service()
        return svc.compute_probabilities(force=force)
    except Exception as exc:
        logger.error(f"[FOMC] compute_probabilities 失败: {exc}", exc_info=True)
        raise


@app.task(bind=True, name="fomc_backfill_probabilities", queue="data_fetching", max_retries=1)
def fomc_backfill_probabilities_task(self, start: str = "2026-01-01"):
    """一次性回填历史概率（ZQ=F 连续合约 + EFFR 日序列，幂等）"""
    try:
        svc = _get_service()
        return svc.backfill_probabilities(start=start)
    except Exception as exc:
        logger.error(f"[FOMC] backfill_probabilities 失败: {exc}", exc_info=True)
        raise
