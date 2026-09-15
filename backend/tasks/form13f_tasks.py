# -*- coding: utf-8 -*-
"""
Form 13F — 预设机构 CIK 同步 Celery Tasks

主任务: form13f_poll（20 家 CIK 拉 13F-HR → 入库 → 快照）
重算:   form13f_aggregate（按季度重算快照）
兼容:   download/import/retry/backfill 均走 poll，避免旧调度/API 派发 NotRegistered
"""

from backend.celery_app import app
from backend.utils.logger import get_logger

logger = get_logger("form13f_tasks")


def _get_service():
    from backend.services.form13f_service import Form13fService
    return Form13fService()


def _sync_presets():
    return _get_service().sync_preset_filings()


@app.task(bind=True, name="form13f_poll", queue="data_fetching",
          max_retries=1, time_limit=3600)
def form13f_poll_task(self):
    """同步 20 家预设机构的 13F-HR（含 amendment 折叠与快照）。"""
    try:
        return _sync_presets()
    except Exception as exc:
        logger.error(f"[13F] poll 失败: {exc}", exc_info=True)
        raise


@app.task(bind=True, name="form13f_aggregate", queue="data_fetching",
          max_retries=1, time_limit=1800)
def form13f_aggregate_task(self, dataset_id: int = None, quarter: str = None):
    """重算快照。优先 quarter；未指定则重算全部已入库季度。"""
    try:
        svc = _get_service()
        if quarter:
            return svc.compute_snapshots(quarter)
        return {"quarters": svc.recompute_all_snapshots()}
    except Exception as exc:
        logger.error(f"[13F] aggregate 失败: {exc}", exc_info=True)
        raise


@app.task(bind=True, name="form13f_download", queue="data_fetching",
          max_retries=1, time_limit=3600)
def form13f_download_task(self, dataset_id: int = None, force: bool = False):
    """兼容旧 API：改为同步预设机构。"""
    return _sync_presets()


@app.task(bind=True, name="form13f_import", queue="data_fetching",
          max_retries=1, time_limit=3600)
def form13f_import_task(self, dataset_id: int = None, local_dir: str = None):
    """兼容旧 API：改为同步预设机构。"""
    return _sync_presets()


@app.task(bind=True, name="form13f_retry", queue="data_fetching",
          max_retries=1, time_limit=3600)
def form13f_retry_task(self):
    return _sync_presets()


@app.task(bind=True, name="form13f_backfill", queue="data_fetching",
          max_retries=1, time_limit=3600)
def form13f_backfill_task(self):
    return _sync_presets()
