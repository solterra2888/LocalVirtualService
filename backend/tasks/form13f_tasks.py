# -*- coding: utf-8 -*-
"""
Form 13F — 预设机构 CIK 同步 Celery Tasks

主任务: form13f_poll（20 家 CIK 拉 13F-HR → 入库 → 快照）
重算:   form13f_aggregate（按季度重算快照）
兼容:   download/import/retry/backfill 均走 poll，避免旧调度/API 派发 NotRegistered

2026-09~11 停摆教训：任务连续失败两个月无人知晓（调度侧只记派发成功）。
现在任务失败时旁路发 Lark 告警（发送失败静默，绝不影响任务主流程）；
数据新鲜度兜底检查在 scheduler 侧 _execute_form13f_task。
"""

from datetime import datetime, timezone

from backend.celery_app import app
from backend.utils.logger import get_logger

logger = get_logger("form13f_tasks")


def _alert_lark_failure(task_name: str, exc: Exception) -> None:
    """poll 失败旁路告警（复用 lark_notifier，LARK_WEBHOOK_URL 未配置时静默跳过）。"""
    try:
        from backend.utils.lark_notifier import send_text
        send_text(
            f"【form13f 🔴】{task_name} 执行失败",
            [f"时间: {datetime.now(tz=timezone.utc).isoformat(timespec='seconds')}",
             f"错误: {str(exc)[:300]}",
             "处置: 查看 logs/celery_data_worker.log；依赖见 requirements.txt "
             "(edgartools>=5.62.0,<6 / httpx==0.28.1 / anyio<4)"])
    except Exception as notify_exc:  # noqa: BLE001 — 告警是旁路，绝不向上抛
        logger.warning(f"[13F] 失败告警发送异常(忽略): {notify_exc}")


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
        _alert_lark_failure("form13f_poll", exc)
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
