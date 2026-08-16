# -*- coding: utf-8 -*-
"""
知识星球附件文件下载/上传 Celery 任务

队列：zsxq_file_processing（专用单并发 worker rc-celery-zsxq-files）

任务：
  - process_zsxq_file_task : 三阶段幂等消费者
        pending → [下载] → downloaded → [上传] → uploaded
        1030→blocked / 重试耗尽→failed / 401→cookie 熔断
  - dispatch_zsxq_file_downloads : 定时扫描 pending/downloaded 入队（见下，Phase 3）
  - reap_zsxq_stale_files        : 定时回退僵死任务（见下，Phase 3）

幂等基石：file_id 主键 + DB 状态机；崩在哪从哪续，不重复下/传大文件。
"""

import logging
import os
from typing import Any, Dict

from backend.celery_app import app

logger = logging.getLogger(__name__)

MAX_DOWNLOAD_RETRIES = 5   # 下载阶段失败上限
MAX_UPLOAD_RETRIES = 5     # 上传阶段失败上限


def _local_path(group_id: str, file_id: str, name: str) -> str:
    """本地临时下载路径（上传成功后删除）。"""
    upload_dir = os.getenv("UPLOAD_DIR", "/tmp/uploads")
    from backend.utils.zsxq_file_download import sanitize_filename
    safe = sanitize_filename(name) or f"{file_id}.bin"
    return os.path.join(upload_dir, "zsxq", f"{group_id}_{file_id}_{safe}")


def _alert_cookie_expired(cred_id: int, file_id: str, err: str) -> None:
    """cookie 过期 → Lark 告警（最易踩的坑，需人刷新 cookie）。"""
    try:
        from backend.utils.lark_notifier import send_text
        send_text(
            f"⚠️ 知识星球 cookie 过期\n凭证 id={cred_id}\n"
            f"触发文件 file_id={file_id}\n错误: {err[:200]}\n"
            f"下载队列已暂停，刷新 cookie 后自动恢复。"
        )
    except Exception as e:
        logger.warning("ZSXQ cookie 过期告警发送失败: %s", e)


@app.task(
    name='process_zsxq_file',
    bind=True,
    rate_limit='6/m',          # 单线程 + 限速，降封号风险
    max_retries=3,
    default_retry_delay=300,
    soft_time_limit=1800,      # 30min 软上限（大文件 mp3）
    time_limit=2100,
)
def process_zsxq_file_task(self, file_id: str) -> Dict[str, Any]:
    """
    处理单个 ZSXQ 附件文件：下载 → 上传 OSS。三阶段幂等。

    幂等：
      - begin_processing 仅在非终态时加锁（终态直接跳过）
      - 已 downloaded 则跳过下载阶段，直接上传
      - OSS 固定 key，重传安全
    """
    from backend.services.zsxq_file_service import ZsxqFileService
    from backend.services.zsxq_credential_service import ZsxqCredentialService
    from backend.services.oss_service import OSSService
    from backend.utils.zsxq_file_download import (
        get_download_url, download_to_file, build_oss_object_name, CookieExpired,
    )

    svc = ZsxqFileService()
    row = svc.begin_processing(file_id)
    if not row:
        return {"file_id": str(file_id), "skipped": "terminal_or_missing"}

    group_id = str(row["group_id"])
    name = str(row.get("name") or f"{file_id}.bin")
    size = int(row.get("size") or 0)
    status = str(row.get("status"))

    # 取 cookie（与抓取共用同一账号；无可用凭证则释放锁留待下次）
    cred = ZsxqCredentialService().get_first_valid_cookie()
    if not cred:
        svc.clear_lock(file_id)
        logger.warning("ZSXQ 文件 %s 处理跳过：无可用凭证", file_id)
        return {"file_id": str(file_id), "skipped": "no_credential"}
    cred_id, cookie = cred

    try:
        # ── 阶段① 下载（幂等：已 downloaded 且本地文件存在则跳过） ──
        local_path = str(row.get("local_path") or "")
        need_download = status != "downloaded" or not local_path or not os.path.exists(local_path)
        if need_download:
            local_path = _local_path(group_id, str(file_id), name)
            os.makedirs(os.path.dirname(local_path), exist_ok=True)

            try:
                dl_url, st = get_download_url(str(file_id), cookie, group_id)
            except CookieExpired as e:
                # cookie 过期 → 标记凭证 + 告警 + 释放锁（队列随凭证失效自然暂停）
                ZsxqCredentialService().mark_expired(cred_id, str(e))
                _alert_cookie_expired(cred_id, str(file_id), str(e))
                svc.clear_lock(file_id)
                return {"file_id": str(file_id), "skipped": "cookie_expired"}

            if st == "blocked_1030":
                svc.mark_blocked(str(file_id), "mobile_only_1030")
                logger.info("ZSXQ 文件 %s 命中 1030，标记 blocked", file_id)
                return {"file_id": str(file_id), "blocked": True}
            if st != "ok" or not dl_url:
                n = svc.bump_retry(str(file_id), f"get_download_url: {st}")
                if n >= MAX_DOWNLOAD_RETRIES:
                    svc.mark_failed(str(file_id), f"get_download_url 重试耗尽: {st}")
                    return {"file_id": str(file_id), "failed": True}
                raise self.retry(countdown=300, exc=Exception(f"get_download_url {st}"))

            ok, got = download_to_file(
                dl_url, local_path, cookie, group_id,
                expected_size=size, fallback_name=name, file_id=str(file_id),
            )
            if not ok:
                n = svc.bump_retry(str(file_id), f"download_to_file failed got={got}")
                if n >= MAX_DOWNLOAD_RETRIES:
                    svc.mark_failed(str(file_id), f"下载重试耗尽 got={got}")
                    return {"file_id": str(file_id), "failed": True}
                raise self.retry(countdown=300, exc=Exception(f"download failed got={got}"))

            svc.mark_downloaded(str(file_id), local_path)

        # ── 阶段② 上传 OSS（固定 key，幂等重传安全） ──
        object_name = build_oss_object_name(group_id, str(file_id), name)
        up = OSSService().upload_local_file_fixed_key(local_path, object_name, name)
        if not up.get("success"):
            # 上传失败：保留 downloaded + 本地文件（不重下），退避重试上传
            n = svc.note_retry(str(file_id), up.get("error") or "oss_upload_failed")
            if n >= MAX_UPLOAD_RETRIES:
                svc.mark_failed(str(file_id), f"上传重试耗尽: {up.get('error')}")
                _safe_remove(local_path)
                return {"file_id": str(file_id), "failed": True}
            raise self.retry(countdown=300, exc=Exception(up.get("error") or "oss_upload_failed"))

        # 上传成功：落 OSS 位置，删本地文件
        svc.mark_uploaded(str(file_id), object_name, up.get("bucket") or "")
        _safe_remove(local_path)
        logger.info("ZSXQ 文件 %s 处理完成 → %s", file_id, object_name)
        return {
            "file_id": str(file_id),
            "uploaded": True,
            "object_name": object_name,
            "size": up.get("file_size"),
        }

    except Exception as exc:
        # 兜底：未显式处理的异常 → 释放锁，交由 reaper / 下次 dispatch
        logger.exception("ZSXQ 文件 %s 处理异常: %s", file_id, exc)
        svc.clear_lock(file_id)
        raise


def _safe_remove(path: str) -> None:
    try:
        if path and os.path.exists(path):
            os.remove(path)
    except Exception as e:
        logger.warning("删除本地临时文件失败 %s: %s", path, e)


# ── 调度 / 容灾 ──────────────────────────────────────────────────────────────

IN_FLIGHT_CAP = 20  # 同时在途（downloading/downloaded）上限


@app.task(name='dispatch_zsxq_file_downloads')
def dispatch_zsxq_file_downloads_task() -> Dict[str, Any]:
    """
    扫描 pending/downloaded 文件入队。由 fetch_zhishixingqiu_subscription 抓取完成后
    链式触发(每轮调度 ~30min,新订阅首抓也会触发),不依赖 beat。

    开头先跑 reaper(reset_stale)回退僵死任务,使每轮抓取顺带完成崩溃恢复。
    cookie 熔断：无可用凭证时跳过入队（凭证刷新后自动恢复）。
    in-flight 上限：控制在途任务数，避免堆积。
    路由到 data_fetching 队列（轻量控制任务，不占文件 worker）。
    """
    from backend.services.zsxq_file_service import ZsxqFileService
    from backend.services.zsxq_credential_service import ZsxqCredentialService

    # 僵死恢复(原独立 reaper 任务):卡住 >15min 的行回退后重新参与本次入队
    reset_n = ZsxqFileService().reset_stale(timeout_minutes=15)

    if not ZsxqCredentialService().get_first_valid_cookie():
        logger.info("ZSXQ 文件 dispatcher 跳过：无可用凭证")
        return {"dispatched": 0, "reason": "no_valid_credential"}

    svc = ZsxqFileService()
    in_flight = svc.count_in_flight()
    budget = max(0, IN_FLIGHT_CAP - in_flight)
    if budget == 0:
        return {"dispatched": 0, "reason": "in_flight_cap_reached", "in_flight": in_flight}

    ids = svc.fetch_resumable_ids(limit=budget)
    for fid in ids:
        process_zsxq_file_task.delay(fid)
    if ids:
        logger.info("ZSXQ 文件 dispatcher 入队 %d 个 (in_flight=%d)", len(ids), in_flight)
    return {
        "dispatched": len(ids),
        "in_flight_before": in_flight,
        "stale_reset": reset_n,
    }


@app.task(name='reap_zsxq_stale_files')
def reap_zsxq_stale_files_task() -> Dict[str, Any]:
    """
    回退僵死任务（卡在 downloading/uploaded 且 locked_at 超时）。
      - downloading（下载中断）→ 回退 pending，清 local_path
      - downloaded（上传中断）→ 仅释放锁，保留 downloaded 等重新派发上传

    已折叠进 dispatch 开头(每轮抓取自动执行),本任务保留为手动运维兜底:
    排查时可随时 `.apply()` 触发。
    """
    from backend.services.zsxq_file_service import ZsxqFileService
    n = ZsxqFileService().reset_stale(timeout_minutes=15)
    return {"reset": n}

