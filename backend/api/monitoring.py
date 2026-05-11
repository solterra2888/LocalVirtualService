# -*- coding: utf-8 -*-
"""
Redis / Celery 运行时监控 API

为 admin-only 的系统监控面板提供：
  - 所有已知 Celery 队列的积压长度 (Redis LLEN)
  - 在线 worker 列表与心跳状态 (app.control.ping)
  - worker 级 active/reserved/scheduled 任务数 + 处理统计
  - 管理员手动清空队列（带二次确认）

设计要点：
  * 队列深度 (LLEN) 极廉价 (<1ms)，无缓存，前端可 5s 高频轮询
  * inspect() 是广播式 RPC，阻塞 1-2s，用内存 TTL 缓存避免打穿
  * 所有端点都走 get_current_admin_user 鉴权
"""
from __future__ import annotations

import asyncio
import collections
import logging
import os
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

import redis as redis_lib

from backend.auth.jwt_utils import get_current_admin_user
from backend.celery_app import app as celery_app
from backend.celery_config import task_routes as _task_routes
from backend.schemas.user import TokenData

logger = logging.getLogger(__name__)

router = APIRouter()


# ---------------------------------------------------------------------------
# 队列清单（自动 + 额外）
# ---------------------------------------------------------------------------
#
# 从 celery_config.task_routes 里自动提取所有声明过的 queue 名，再追加本项目
# 使用但 task_routes 里没登记的（如 local_virtual_service 里的长视频队列）。
# 可通过 env MONITORING_EXTRA_QUEUES=q1,q2 动态追加。

def _discover_queues() -> List[str]:
    auto: set[str] = set()
    for v in _task_routes.values():
        if isinstance(v, dict):
            q = v.get("queue")
            if q:
                auto.add(q)
    # 已知但不在 task_routes 的队列
    extra_builtin = {
        "celery",                        # Celery 默认队列
        "youtube_transcription_long",    # 本地 worker 长视频队列
    }
    env_extra = os.getenv("MONITORING_EXTRA_QUEUES", "").strip()
    env_extra_set = {
        q.strip() for q in env_extra.split(",") if q.strip()
    } if env_extra else set()
    return sorted(auto | extra_builtin | env_extra_set)


KNOWN_QUEUES: List[str] = _discover_queues()


# ---------------------------------------------------------------------------
# Redis 客户端（复用 celery broker 地址，单例）
# ---------------------------------------------------------------------------
_redis_client: Optional[redis_lib.Redis] = None
_redis_lock = threading.Lock()


def _get_redis() -> redis_lib.Redis:
    global _redis_client
    if _redis_client is None:
        with _redis_lock:
            if _redis_client is None:
                url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
                _redis_client = redis_lib.from_url(
                    url,
                    decode_responses=False,   # LLEN/DEL 不需要 decode
                    socket_connect_timeout=2,
                    socket_timeout=2,
                )
    return _redis_client


# ---------------------------------------------------------------------------
# inspect() 结果的进程级 TTL 缓存
# ---------------------------------------------------------------------------
# inspect() 内部广播+等待 worker 回复，耗时 1-2s。前端 5s 轮询全跑一遍会
# 让 Celery control 通道压力很大，这里做 3s TTL 缓存。
_INSPECT_CACHE: Dict[str, Any] = {"ts": 0.0, "data": None}
_INSPECT_CACHE_TTL = float(os.getenv("MONITORING_INSPECT_CACHE_TTL", "3.0"))
_INSPECT_LOCK = threading.Lock()


def _inspect_snapshot(force_refresh: bool = False) -> Dict[str, Any]:
    """采集一次 worker 快照，结果包含 ping/active/reserved/scheduled/stats。

    任何 RPC 失败都返回空字典，避免 500。"""
    now = time.time()
    if not force_refresh:
        with _INSPECT_LOCK:
            if (_INSPECT_CACHE["data"] is not None
                    and now - _INSPECT_CACHE["ts"] < _INSPECT_CACHE_TTL):
                return _INSPECT_CACHE["data"]

    timeout = float(os.getenv("MONITORING_INSPECT_TIMEOUT", "1.5"))
    snapshot: Dict[str, Any] = {
        "ping": {},
        "active": {},
        "reserved": {},
        "scheduled": {},
        "stats": {},
        "inspected_at": now,
    }
    try:
        inspect = celery_app.control.inspect(timeout=timeout)
        # ping 返回 {worker: {'ok': 'pong'}}；其他返回 {worker: [...]} 或 None
        snapshot["ping"] = inspect.ping() or {}
        snapshot["active"] = inspect.active() or {}
        snapshot["reserved"] = inspect.reserved() or {}
        snapshot["scheduled"] = inspect.scheduled() or {}
        snapshot["stats"] = inspect.stats() or {}
    except Exception as e:
        logger.warning("celery inspect 失败: %s", e)

    with _INSPECT_LOCK:
        _INSPECT_CACHE["ts"] = now
        _INSPECT_CACHE["data"] = snapshot
    return snapshot


# ---------------------------------------------------------------------------
# Pydantic 响应模型
# ---------------------------------------------------------------------------

class QueueInfo(BaseModel):
    name: str
    depth: int                       # LLEN 结果；-1 表示探测失败
    error: Optional[str] = None


class QueuesResponse(BaseModel):
    queues: List[QueueInfo]
    total_depth: int
    fetched_at: float


class WorkerInfo(BaseModel):
    name: str
    online: bool                     # ping 是否响应
    queues: List[str]                # 订阅队列列表（从 stats 推断，可能为空）
    active_count: int                # 正在执行
    reserved_count: int              # prefetch 取下待执行
    scheduled_count: int             # ETA/countdown 待执行
    processed_total: Optional[int] = None   # worker 累计处理任务数
    pool_max_concurrency: Optional[int] = None
    pool_processes: Optional[int] = None


class WorkersResponse(BaseModel):
    workers: List[WorkerInfo]
    online_count: int
    fetched_at: float
    inspect_cache_age_sec: float


class LogFileStats(BaseModel):
    type: str
    filename: str
    size_human: str
    size_kb: float
    mtime: Optional[float] = None
    exists: bool


class SummaryResponse(BaseModel):
    queues: List[QueueInfo]
    total_depth: int
    workers: List[WorkerInfo]
    online_count: int
    active_task_count: int           # 所有 worker 当前活跃任务总数
    fetched_at: float
    inspect_cache_age_sec: float
    log_files: List[LogFileStats] = []
    recent_errors_1h: int = 0        # 最近 1 小时 app.log 中 ERROR 行数


class PurgeResponse(BaseModel):
    queue: str
    removed: int
    message: str


# ---------------------------------------------------------------------------
# 日志文件统计（供 /summary 端点聚合）
# ---------------------------------------------------------------------------
_LOG_FILES_FOR_OVERVIEW: dict[str, str] = {
    "app":          "logs/app.log",
    "celery_ai":    "logs/celery_ai_worker.log",
    "celery_data":  "logs/celery_data_worker.log",
    "celery_tasks": "logs/celery_tasks.log",
}
_PROJECT_ROOT_MON = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)


def _human_size(size_bytes: int) -> str:
    if size_bytes >= 1024 ** 3:
        return f"{size_bytes / 1024 ** 3:.1f} GB"
    if size_bytes >= 1024 ** 2:
        return f"{size_bytes / 1024 ** 2:.1f} MB"
    if size_bytes >= 1024:
        return f"{size_bytes / 1024:.1f} KB"
    return f"{size_bytes} B"


def _get_log_file_stats() -> List[LogFileStats]:
    result: list[LogFileStats] = []
    for log_type, rel in _LOG_FILES_FOR_OVERVIEW.items():
        path = os.path.join(_PROJECT_ROOT_MON, rel)
        if os.path.exists(path):
            stat = os.stat(path)
            result.append(LogFileStats(
                type=log_type,
                filename=rel,
                size_kb=round(stat.st_size / 1024, 2),
                size_human=_human_size(stat.st_size),
                mtime=stat.st_mtime,
                exists=True,
            ))
        else:
            result.append(LogFileStats(
                type=log_type,
                filename=rel,
                size_kb=0,
                size_human="0 B",
                exists=False,
            ))
    return result


def _count_recent_errors(tail_lines: int = 3000) -> int:
    """扫描 app.log 最后 tail_lines 行，统计最近 1 小时内 [ERROR] 行数。"""
    path = os.path.join(_PROJECT_ROOT_MON, "logs/app.log")
    if not os.path.exists(path):
        return 0
    try:
        lines = list(collections.deque(
            open(path, "r", encoding="utf-8", errors="ignore"),
            maxlen=tail_lines,
        ))
        now = datetime.now(timezone.utc)
        count = 0
        for line in lines:
            if "[ERROR]" not in line and "/ERROR]" not in line:
                continue
            # 尝试解析行首的时间戳（格式 YYYY-MM-DD HH:MM:SS）
            try:
                ts_str = line[:19]
                ts = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                if (now - ts).total_seconds() <= 3600:
                    count += 1
            except ValueError:
                # 无法解析时间戳，降级：凡包含 ERROR 就算
                count += 1
        return count
    except Exception as e:
        logger.warning("_count_recent_errors 失败: %s", e)
        return 0


# ---------------------------------------------------------------------------
# 内部工具函数
# ---------------------------------------------------------------------------

def _fetch_queue_depths() -> tuple[List[QueueInfo], int]:
    """用一次 pipeline 获取所有队列的 LLEN，最快且一致。"""
    r = _get_redis()
    queues: List[QueueInfo] = []
    total = 0
    try:
        pipe = r.pipeline(transaction=False)
        for q in KNOWN_QUEUES:
            pipe.llen(q)
        depths = pipe.execute()
    except Exception as e:
        logger.warning("redis LLEN pipeline 失败: %s", e)
        # 降级: 逐个查，失败的标 -1
        depths = []
        for q in KNOWN_QUEUES:
            try:
                depths.append(r.llen(q))
            except Exception as ie:
                depths.append(f"__error__:{ie}")

    for q, d in zip(KNOWN_QUEUES, depths):
        if isinstance(d, int):
            queues.append(QueueInfo(name=q, depth=int(d)))
            total += int(d)
        else:
            queues.append(QueueInfo(name=q, depth=-1,
                                    error=str(d).replace("__error__:", "")))
    return queues, total


def _build_worker_infos(snapshot: Dict[str, Any]) -> tuple[List[WorkerInfo], int]:
    """把 inspect 返回的原始 dict 整理成 WorkerInfo 列表。"""
    worker_names: set[str] = set()
    for key in ("ping", "active", "reserved", "scheduled", "stats"):
        d = snapshot.get(key) or {}
        worker_names.update(d.keys())

    ping = snapshot.get("ping") or {}
    active = snapshot.get("active") or {}
    reserved = snapshot.get("reserved") or {}
    scheduled = snapshot.get("scheduled") or {}
    stats = snapshot.get("stats") or {}

    workers: List[WorkerInfo] = []
    online = 0
    for name in sorted(worker_names):
        is_online = bool(ping.get(name))
        if is_online:
            online += 1
        s = stats.get(name) or {}
        pool = s.get("pool") or {}
        total_tasks = s.get("total")
        if isinstance(total_tasks, dict):
            processed = sum(int(v) for v in total_tasks.values() if isinstance(v, (int, float)))
        elif isinstance(total_tasks, (int, float)):
            processed = int(total_tasks)
        else:
            processed = None
        # 订阅的队列列表 (stats.broker? 不稳定, 从 reserved/active 里的消息推断更准)
        q_names: set[str] = set()
        for bucket in (active.get(name) or []) + (reserved.get(name) or []) + (scheduled.get(name) or []):
            dinfo = bucket.get("delivery_info") if isinstance(bucket, dict) else None
            if isinstance(dinfo, dict):
                rk = dinfo.get("routing_key") or dinfo.get("exchange")
                if rk:
                    q_names.add(rk)
        workers.append(WorkerInfo(
            name=name,
            online=is_online,
            queues=sorted(q_names),
            active_count=len(active.get(name) or []),
            reserved_count=len(reserved.get(name) or []),
            scheduled_count=len(scheduled.get(name) or []),
            processed_total=processed,
            pool_max_concurrency=pool.get("max-concurrency"),
            pool_processes=len(pool.get("processes") or []) if isinstance(pool.get("processes"), list) else None,
        ))
    return workers, online


# ---------------------------------------------------------------------------
# 路由
# ---------------------------------------------------------------------------

@router.get(
    "/api/monitoring/queues",
    response_model=QueuesResponse,
    summary="所有已知队列的 Redis 积压长度",
)
async def get_queue_depths(
    current_user: TokenData = Depends(get_current_admin_user),
):
    """返回所有已知 Celery 队列的 LLEN。高频轮询用（每次 < 1ms）。"""
    queues, total = _fetch_queue_depths()
    return QueuesResponse(
        queues=queues,
        total_depth=total,
        fetched_at=time.time(),
    )


@router.get(
    "/api/monitoring/workers",
    response_model=WorkersResponse,
    summary="Celery worker 列表与心跳",
)
async def get_workers(
    force_refresh: bool = False,
    current_user: TokenData = Depends(get_current_admin_user),
):
    """返回所有响应心跳的 worker。inspect 结果带 3s TTL 缓存，
    如要跳过缓存强制刷新：`?force_refresh=true`。"""
    snap = _inspect_snapshot(force_refresh=force_refresh)
    workers, online = _build_worker_infos(snap)
    return WorkersResponse(
        workers=workers,
        online_count=online,
        fetched_at=time.time(),
        inspect_cache_age_sec=round(time.time() - snap.get("inspected_at", 0), 2),
    )


@router.get(
    "/api/monitoring/summary",
    response_model=SummaryResponse,
    summary="监控总览（队列 + worker + 统计）",
)
async def get_summary(
    force_refresh: bool = False,
    current_user: TokenData = Depends(get_current_admin_user),
):
    """一次性返回队列深度 + worker 心跳 + 日志文件概况，前端首屏渲染用。

    Redis LLEN、Celery inspect、磁盘 stat、app.log 扫描原先顺序执行会把延迟叠加；
    这里用 asyncio.to_thread 并行执行并在独立线程里跑阻塞式 inspect，避免拖死 ASGI 事件循环。
    """
    (
        queues_tuple,
        snap,
        log_files,
        recent_errors,
    ) = await asyncio.gather(
        asyncio.to_thread(_fetch_queue_depths),
        asyncio.to_thread(_inspect_snapshot, force_refresh),
        asyncio.to_thread(_get_log_file_stats),
        asyncio.to_thread(_count_recent_errors),
    )
    queues, total_depth = queues_tuple
    workers, online = _build_worker_infos(snap)
    active_task_count = sum(w.active_count for w in workers)
    return SummaryResponse(
        queues=queues,
        total_depth=total_depth,
        workers=workers,
        online_count=online,
        active_task_count=active_task_count,
        fetched_at=time.time(),
        inspect_cache_age_sec=round(time.time() - snap.get("inspected_at", 0), 2),
        log_files=log_files,
        recent_errors_1h=recent_errors,
    )


@router.post(
    "/api/monitoring/queues/{queue_name}/purge",
    response_model=PurgeResponse,
    summary="清空指定队列（危险操作，管理员，前端二次确认）",
)
async def purge_queue(
    queue_name: str,
    confirm: Optional[str] = None,
    current_user: TokenData = Depends(get_current_admin_user),
):
    """
    清空指定 Redis 队列中所有待消费的消息。

    **双重防护**：
      1. 仅允许清空 `KNOWN_QUEUES` 里明确登记过的队列，防拼写/误清
      2. 需要 query 参数 `confirm=<queue_name>` 与 URL 路径一致，
         前端会在弹窗里要求用户输入队列名确认

    返回被删除的消息数量（redis LLEN 值）。
    """
    if queue_name not in KNOWN_QUEUES:
        raise HTTPException(
            status_code=400,
            detail=(f"未登记的队列名: {queue_name}。"
                    f"已知队列: {', '.join(KNOWN_QUEUES)}"),
        )
    if confirm != queue_name:
        raise HTTPException(
            status_code=400,
            detail="confirm 参数必须与 queue_name 一致（二次确认机制）",
        )

    r = _get_redis()
    try:
        removed = r.llen(queue_name)   # 先取数
        r.delete(queue_name)           # 再删除
    except Exception as e:
        logger.exception("清空队列失败: %s", queue_name)
        raise HTTPException(status_code=500, detail=f"清空队列失败: {e}")

    user = getattr(current_user, "username", None) or getattr(current_user, "user_id", None)
    logger.warning(
        "[MONITORING] admin=%s 清空了队列 %s (removed=%d)",
        user, queue_name, removed,
    )
    return PurgeResponse(
        queue=queue_name,
        removed=int(removed),
        message=f"已清空队列 {queue_name}，共删除 {removed} 条消息",
    )
