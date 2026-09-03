# -*- coding: utf-8 -*-
"""
Form 13F — SEC 季度数据集 Celery Tasks

任务链: form13f_poll（发现）→ form13f_download（下载 ZIP）
       → form13f_import（TSV 流式导入+行业分类）→ form13f_aggregate（快照预计算）
兜底:  form13f_retry（重试 failed/pending 数据集）
回填:  form13f_backfill（按顺序派发全部历史数据集）
"""

from backend.celery_app import app
from backend.utils.logger import get_logger

logger = get_logger("form13f_tasks")


def _get_service():
    from backend.services.form13f_service import Form13fService
    return Form13fService()


def _dispatch(task_name: str, **kwargs):
    """链式派发下一个任务（跨队列走 Celery send_task）"""
    try:
        from backend.celery_app import app as celery_app
        celery_app.send_task(task_name, kwargs=kwargs)
        logger.info(f"[13F] 已派发 {task_name} kwargs={kwargs}")
    except Exception as e:
        logger.warning(f"[13F] 派发 {task_name} 失败: {e}")


@app.task(bind=True, name="form13f_poll", queue="data_fetching", max_retries=1)
def form13f_poll_task(self):
    """轮询 SEC 13F 数据页：发现新季度数据集并派发下载

    同时补推状态卡住/失败的数据集（幂等：download 对已完成数据集跳过）。
    """
    try:
        svc = _get_service()
        result = svc.poll_sec_datasets()

        # pending（新发现）+ failed（下载失败重试）→ 派发下载
        # 跳过仅大小写不同、但已有进行中/已完成副本的行
        rows = svc.db_manager.fetch_all(
            """
            SELECT d.id, d.status FROM form13f_datasets d
            WHERE d.status IN ('pending', 'failed')
              AND NOT EXISTS (
                  SELECT 1 FROM form13f_datasets x
                  WHERE LOWER(x.dataset_name) = LOWER(d.dataset_name)
                    AND x.id <> d.id
                    AND x.status IN ('imported', 'importing',
                                     'downloaded', 'downloading'))
            ORDER BY d.id
            """)
        for ds_id, status in rows:
            _dispatch("form13f_download", dataset_id=ds_id)

        # downloaded 但未导入（导入任务派发丢失/worker 崩溃）→ 补派发
        rows = svc.db_manager.fetch_all(
            """
            SELECT d.id FROM form13f_datasets d
            WHERE d.status = 'downloaded'
              AND NOT EXISTS (
                  SELECT 1 FROM form13f_datasets x
                  WHERE LOWER(x.dataset_name) = LOWER(d.dataset_name)
                    AND x.id <> d.id
                    AND x.status IN ('imported', 'importing'))
            ORDER BY d.id
            """)
        for (ds_id,) in rows:
            _dispatch("form13f_import", dataset_id=ds_id)

        # imported 但快照缺失（aggregate 派发丢失）→ 补派发
        rows = svc.db_manager.fetch_all(
            """
            SELECT d.id FROM form13f_datasets d
            WHERE d.status = 'imported'
              AND NOT EXISTS (
                  SELECT 1 FROM form13f_snapshots s WHERE s.quarter = d.quarter)
            ORDER BY d.id
            """)
        for (ds_id,) in rows:
            _dispatch("form13f_aggregate", dataset_id=ds_id)

        return result
    except Exception as exc:
        logger.error(f"[13F] poll 失败: {exc}", exc_info=True)
        raise


@app.task(bind=True, name="form13f_download", queue="data_fetching", max_retries=1)
def form13f_download_task(self, dataset_id: int, force: bool = False):
    """下载单个季度 ZIP（流式 + sha256 校验）→ 派发导入"""
    try:
        svc = _get_service()
        result = svc.download_dataset(dataset_id, force=force)
        _dispatch("form13f_import", dataset_id=dataset_id)
        return result
    except Exception as exc:
        logger.error(f"[13F] download dataset={dataset_id} 失败: {exc}", exc_info=True)
        raise


@app.task(
    bind=True, name="form13f_import", queue="data_fetching",
    max_retries=1, time_limit=7200)
def form13f_import_task(self, dataset_id: int, local_dir: str = None):
    """导入单个季度数据集（COVERPAGE 规划 + INFOTABLE 流式聚合入库）→ 派发快照

    time_limit=7200: 350MB TSV 流式导入约 10-20 分钟，留足余量。
    """
    try:
        svc = _get_service()
        result = svc.import_dataset(dataset_id, local_dir=local_dir)
        _dispatch("form13f_aggregate", dataset_id=dataset_id)
        return result
    except Exception as exc:
        logger.error(f"[13F] import dataset={dataset_id} 失败: {exc}", exc_info=True)
        raise


@app.task(
    bind=True, name="form13f_aggregate", queue="data_fetching",
    max_retries=1, time_limit=3600)
def form13f_aggregate_task(self, dataset_id: int, quarter: str = None):
    """为数据集对应季度全部机构预计算快照（sector 分布 / top20 / top5 买卖）"""
    try:
        svc = _get_service()
        return svc.compute_snapshots(dataset_id, quarter=quarter)
    except Exception as exc:
        logger.error(f"[13F] aggregate dataset={dataset_id} 失败: {exc}", exc_info=True)
        raise


@app.task(bind=True, name="form13f_retry", queue="data_fetching", max_retries=1)
def form13f_retry_task(self):
    """兜底重试 failed/pending 数据集（每日一次）"""
    try:
        svc = _get_service()
        counts = svc.retry_failed_datasets()
        # poll 内含完整的补推逻辑，直接复用
        if counts['pending'] or counts['failed']:
            return svc.poll_sec_datasets()
        return {"retried": 0}
    except Exception as exc:
        logger.error(f"[13F] retry 失败: {exc}", exc_info=True)
        raise


@app.task(bind=True, name="form13f_backfill", queue="data_fetching", max_retries=1)
def form13f_backfill_task(self):
    """回填全部历史数据集（按 id 顺序派发下载，Celery 队列串行执行避免并发下载）"""
    try:
        svc = _get_service()
        result = svc.backfill_history()
        for ds in result['datasets']:
            _dispatch("form13f_download", dataset_id=ds['id'])
        return {"dispatched": len(result['datasets'])}
    except Exception as exc:
        logger.error(f"[13F] backfill 失败: {exc}", exc_info=True)
        raise
