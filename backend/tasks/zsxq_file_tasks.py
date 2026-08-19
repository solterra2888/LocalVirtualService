# -*- coding: utf-8 -*-
"""
知识星球附件文件下载/上传 Celery 任务

队列：zsxq_file_processing（专用单并发 worker rc-celery-zsxq-files）

任务：
  - process_zsxq_file_task : 三阶段幂等消费者
        pending → [下载] → downloaded → [上传] → uploaded
        1030→blocked / 重试耗尽→failed / 401→cookie 熔断 / 音频→skipped
  - dispatch_zsxq_file_downloads : 扫描 pending/downloaded 入队
        由抓取完成、单文件处理完成、以及 scheduler(每 5 分钟) 触发
  - reap_zsxq_stale_files        : 手动运维兜底

幂等基石：file_id 主键 + DB 状态机；崩在哪从哪续，不重复下/传大文件。
音频不下载（入库即为 skipped）。
"""

import logging
import os
from typing import Any, Dict

from backend.celery_app import app

logger = logging.getLogger(__name__)

MAX_DOWNLOAD_RETRIES = 5   # 下载阶段失败上限
MAX_UPLOAD_RETRIES = 5     # 上传阶段失败上限
IN_FLIGHT_CAP = 40         # 已领取未完成上限（claim 时加锁，避免重复入队）


def _local_path(group_id: str, file_id: str, name: str) -> str:
    """本地临时下载路径（上传成功后删除）。文件名按 UTF-8 字节截到 ≤255。"""
    upload_dir = os.getenv("UPLOAD_DIR", "/tmp/uploads")
    from backend.utils.zsxq_file_download import local_download_basename
    return os.path.join(upload_dir, "zsxq", local_download_basename(group_id, str(file_id), name))


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


def _kick_dispatch(reason: str = "") -> None:
    """处理完成后立刻补货入队，避免等下一轮抓取。失败不影响主流程。"""
    try:
        dispatch_zsxq_file_downloads_task.delay()
    except Exception as exc:
        logger.warning("ZSXQ 链式 dispatch 失败%s: %s", f"({reason})" if reason else "", exc)


@app.task(
    name='process_zsxq_file',
    bind=True,
    rate_limit='10/m',         # 现只处理 PDF 等文档，略提高但仍限速防封
    max_retries=3,
    default_retry_delay=300,
    soft_time_limit=600,
    time_limit=720,
)
def process_zsxq_file_task(self, file_id: str) -> Dict[str, Any]:
    """
    处理单个 ZSXQ 附件文件：下载 → 上传 OSS。三阶段幂等。

    幂等：
      - begin_processing 仅在非终态时加锁（终态直接跳过）
      - 已 downloaded 则跳过下载阶段，直接上传
      - OSS 固定 key，重传安全
    """
    from backend.services.zsxq_file_service import ZsxqFileService, is_audio_filename
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

    if is_audio_filename(name):
        svc.mark_skipped(str(file_id), "audio_not_downloaded")
        _kick_dispatch("audio_skipped")
        return {"file_id": str(file_id), "skipped": "audio"}

    # 按星球绑定账号取 Cookie；无可用凭证则释放锁留待下次
    cred = ZsxqCredentialService().get_cookie_for_group(group_id)
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
                ZsxqCredentialService().mark_expired(cred_id, str(e))
                _alert_cookie_expired(cred_id, str(file_id), str(e))
                svc.clear_lock(file_id)
                return {"file_id": str(file_id), "skipped": "cookie_expired"}

            if st == "blocked_1030":
                svc.mark_blocked(str(file_id), "mobile_only_1030")
                logger.info("ZSXQ 文件 %s 命中 1030，标记 blocked", file_id)
                _kick_dispatch("blocked")
                return {"file_id": str(file_id), "blocked": True}
            if st != "ok" or not dl_url:
                n = svc.bump_retry(str(file_id), f"get_download_url: {st}")
                if n >= MAX_DOWNLOAD_RETRIES:
                    svc.mark_failed(str(file_id), f"get_download_url 重试耗尽: {st}")
                    _kick_dispatch("failed")
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
                    _kick_dispatch("failed")
                    return {"file_id": str(file_id), "failed": True}
                raise self.retry(countdown=300, exc=Exception(f"download failed got={got}"))

            svc.mark_downloaded(str(file_id), local_path)

        # ── 阶段② 上传 OSS（固定 key，幂等重传安全） ──
        object_name = build_oss_object_name(group_id, str(file_id), name)
        up = OSSService().upload_local_file_fixed_key(local_path, object_name, name)
        if not up.get("success"):
            n = svc.note_retry(str(file_id), up.get("error") or "oss_upload_failed")
            if n >= MAX_UPLOAD_RETRIES:
                svc.mark_failed(str(file_id), f"上传重试耗尽: {up.get('error')}")
                _safe_remove(local_path)
                _kick_dispatch("failed")
                return {"file_id": str(file_id), "failed": True}
            raise self.retry(countdown=300, exc=Exception(up.get("error") or "oss_upload_failed"))

        svc.mark_uploaded(str(file_id), object_name, up.get("bucket") or "")
        _safe_remove(local_path)
        logger.info("ZSXQ 文件 %s 处理完成 → %s", file_id, object_name)
        _kick_dispatch("uploaded")
        return {
            "file_id": str(file_id),
            "uploaded": True,
            "object_name": object_name,
            "size": up.get("file_size"),
        }

    except Exception as exc:
        logger.exception("ZSXQ 文件 %s 处理异常: %s", file_id, exc)
        svc.clear_lock(file_id)
        raise


def _safe_remove(path: str) -> None:
    try:
        if path and os.path.exists(path):
            os.remove(path)
    except Exception as e:
        logger.warning("删除本地临时文件失败 %s: %s", path, e)


@app.task(name='dispatch_zsxq_file_downloads')
def dispatch_zsxq_file_downloads_task() -> Dict[str, Any]:
    """
    扫描 pending/downloaded 非音频文件入队。

    触发：抓取完成链式、单文件处理完成链式、scheduler 每 5 分钟兜底。
    领取时加 locked_at，避免重复入队。
    """
    from backend.services.zsxq_file_service import ZsxqFileService
    from backend.services.zsxq_credential_service import ZsxqCredentialService

    svc = ZsxqFileService()
    skipped_audio = svc.skip_audio_backlog()
    reset_n = svc.reset_stale(timeout_minutes=15)

    if not ZsxqCredentialService().get_first_valid_cookie():
        logger.info("ZSXQ 文件 dispatcher 跳过：无可用凭证")
        return {
            "dispatched": 0,
            "reason": "no_valid_credential",
            "skipped_audio": skipped_audio,
            "stale_reset": reset_n,
        }

    in_flight = svc.count_in_flight()
    budget = max(0, IN_FLIGHT_CAP - in_flight)
    if budget == 0:
        return {
            "dispatched": 0,
            "reason": "in_flight_cap_reached",
            "in_flight": in_flight,
            "skipped_audio": skipped_audio,
            "stale_reset": reset_n,
        }

    ids = svc.claim_for_dispatch(limit=budget)
    for fid in ids:
        process_zsxq_file_task.delay(fid)
    if ids:
        logger.info(
            "ZSXQ 文件 dispatcher 入队 %d 个 (in_flight_before=%d skipped_audio=%d)",
            len(ids), in_flight, skipped_audio,
        )
    return {
        "dispatched": len(ids),
        "in_flight_before": in_flight,
        "stale_reset": reset_n,
        "skipped_audio": skipped_audio,
    }


@app.task(name='reap_zsxq_stale_files')
def reap_zsxq_stale_files_task() -> Dict[str, Any]:
    """
    回退僵死任务。已折叠进 dispatch 开头，本任务保留为手动运维兜底。
    """
    from backend.services.zsxq_file_service import ZsxqFileService
    n = ZsxqFileService().reset_stale(timeout_minutes=15)
    return {"reset": n}
