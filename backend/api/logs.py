# -*- coding: utf-8 -*-
"""
系统日志 API 路由

改进点：
  - LOG_FILES 与部署中实际 systemd --logfile 路径对齐
  - read_log 改为 collections.deque 流式 tail，避免将大文件整体加载进内存
  - 增加 level / q 服务端过滤，降低传输量
  - 单请求行数上限 MAX_TAIL_LINES 防止进程 OOM
  - 新增 /download 端点（StreamingResponse 整文件流式传输）
  - clear_log 增加 confirm 二次确认，与 purge queue 安全级别一致
  - list_logs 返回 mtime 与人性化 size 字符串
"""

from __future__ import annotations

import collections
import logging
import os
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from backend.auth.jwt_utils import get_current_admin_user
from backend.schemas.user import TokenData

logger = logging.getLogger(__name__)
router = APIRouter()

# ---------------------------------------------------------------------------
# 日志文件注册表（与 deployment/templates 里的 --logfile 路径对齐）
# ---------------------------------------------------------------------------
LOG_FILES: dict[str, str] = {
    "app":          "logs/app.log",
    "celery_ai":    "logs/celery_ai_worker.log",
    "celery_data":  "logs/celery_data_worker.log",
    "celery_tasks": "logs/celery_tasks.log",
    # 兼容旧部署：如文件存在则展示，不存在也不报错
    "celery_worker": "logs/celery_worker.log",
}

_PROJECT_ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)

# 单次请求最多返回行数，防止超大文件把进程内存耗尽
MAX_TAIL_LINES = 50_000


# ---------------------------------------------------------------------------
# Pydantic 响应模型
# ---------------------------------------------------------------------------
class LogFileInfo(BaseModel):
    type: str
    filename: str
    size_kb: float
    size_human: str
    exists: bool
    mtime: Optional[float] = None   # Unix 时间戳，方便前端格式化


class LogListResponse(BaseModel):
    logs: List[LogFileInfo]


class LogResponse(BaseModel):
    filename: str
    content: str
    file_total_lines: int   # 文件真实总行数（tail 模式时仍统计全文）
    returned_lines: int     # 本次实际返回行数（过滤后）
    size_kb: float
    size_human: str
    truncated: bool         # 是否被 MAX_TAIL_LINES 硬截断


# ---------------------------------------------------------------------------
# 内部工具
# ---------------------------------------------------------------------------
def _abs(rel: str) -> str:
    return os.path.join(_PROJECT_ROOT, rel)


def _human_size(size_bytes: int) -> str:
    if size_bytes >= 1024 ** 3:
        return f"{size_bytes / 1024 ** 3:.1f} GB"
    if size_bytes >= 1024 ** 2:
        return f"{size_bytes / 1024 ** 2:.1f} MB"
    if size_bytes >= 1024:
        return f"{size_bytes / 1024:.1f} KB"
    return f"{size_bytes} B"


def _tail_deque(path: str, n: int) -> list[str]:
    """流式读最后 n 行，内存上限 ≈ n 行，不加载整个文件。"""
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        return list(collections.deque(f, maxlen=n))


def _count_lines_fast(path: str) -> int:
    """块读统计文件行数，不把内容加载进内存。"""
    count = 0
    try:
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(1 << 20), b""):
                count += chunk.count(b"\n")
    except Exception:
        pass
    return count


def _get_log_path(log_type: str) -> str:
    if log_type not in LOG_FILES:
        raise HTTPException(
            status_code=400,
            detail=f"不支持的日志类型: {log_type!r}。已知类型: {', '.join(LOG_FILES)}",
        )
    path = _abs(LOG_FILES[log_type])
    if not os.path.exists(path):
        raise HTTPException(
            status_code=404,
            detail=f"日志文件不存在: {LOG_FILES[log_type]}",
        )
    return path


# ---------------------------------------------------------------------------
# 路由
# ---------------------------------------------------------------------------
@router.get("/api/logs/list", response_model=LogListResponse, summary="获取可用日志文件列表")
async def list_logs(current_user: TokenData = Depends(get_current_admin_user)):
    """返回所有已知日志文件的状态（需要管理员权限）。"""
    result: list[LogFileInfo] = []
    seen: set[str] = set()
    for log_type, rel in LOG_FILES.items():
        path = _abs(rel)
        if path in seen:
            continue
        seen.add(path)
        if os.path.exists(path):
            stat = os.stat(path)
            result.append(LogFileInfo(
                type=log_type,
                filename=rel,
                size_kb=round(stat.st_size / 1024, 2),
                size_human=_human_size(stat.st_size),
                exists=True,
                mtime=stat.st_mtime,
            ))
        else:
            result.append(LogFileInfo(
                type=log_type,
                filename=rel,
                size_kb=0,
                size_human="0 B",
                exists=False,
            ))
    return LogListResponse(logs=result)


@router.get("/api/logs/{log_type}/download", summary="下载完整日志文件（流式）")
async def download_log(
    log_type: str,
    current_user: TokenData = Depends(get_current_admin_user),
):
    """以 StreamingResponse 流式传输完整日志文件，不将内容加载进进程内存（需要管理员权限）。"""
    log_path = _get_log_path(log_type)
    filename = os.path.basename(log_path)

    def _iter():
        with open(log_path, "rb") as f:
            while True:
                chunk = f.read(1 << 16)  # 64 KB/次
                if not chunk:
                    break
                yield chunk

    return StreamingResponse(
        _iter(),
        media_type="text/plain; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.get("/api/logs/{log_type}", response_model=LogResponse, summary="读取日志内容")
async def read_log(
    log_type: str,
    lines: Optional[int] = Query(
        None, ge=0, le=MAX_TAIL_LINES,
        description=f"读取最后 N 行（0 或不填=全部，上限 {MAX_TAIL_LINES}）",
    ),
    level: Optional[str] = Query(None, description="按日志级别过滤，如 ERROR、WARNING、INFO"),
    q: Optional[str] = Query(None, description="关键字过滤（大小写不敏感）"),
    current_user: TokenData = Depends(get_current_admin_user),
):
    """
    读取指定日志文件内容（需要管理员权限）。

    - **lines**: 读取最后 N 行，0 或不填则读取全部（受 MAX_TAIL_LINES 硬上限保护）
    - **level**: 过滤指定级别，如 `ERROR`、`WARNING`
    - **q**: 关键字过滤（大小写不敏感）
    """
    log_path = _get_log_path(log_type)
    size_bytes = os.path.getsize(log_path)

    want = min(lines, MAX_TAIL_LINES) if (lines and lines > 0) else None

    if want:
        raw_lines = _tail_deque(log_path, want)
        file_total = _count_lines_fast(log_path)
    else:
        # 全量但施加硬上限
        with open(log_path, "r", encoding="utf-8", errors="ignore") as f:
            raw_lines = f.readlines()
        file_total = len(raw_lines)
        if len(raw_lines) > MAX_TAIL_LINES:
            raw_lines = raw_lines[-MAX_TAIL_LINES:]

    truncated = len(raw_lines) >= MAX_TAIL_LINES

    # 服务端过滤
    if level:
        lvl = level.upper()
        raw_lines = [ln for ln in raw_lines
                     if f"[{lvl}]" in ln or f"/{lvl}]" in ln or f" {lvl} " in ln]
    if q:
        q_lower = q.lower()
        raw_lines = [ln for ln in raw_lines if q_lower in ln.lower()]

    return LogResponse(
        filename=LOG_FILES[log_type],
        content="".join(raw_lines),
        file_total_lines=file_total,
        returned_lines=len(raw_lines),
        size_kb=round(size_bytes / 1024, 2),
        size_human=_human_size(size_bytes),
        truncated=truncated,
    )


@router.delete("/api/logs/{log_type}/clear", summary="清空日志文件（二次确认）")
async def clear_log(
    log_type: str,
    confirm: Optional[str] = Query(
        None,
        description="必须与 log_type 完全一致，作为二次确认防止误操作",
    ),
    current_user: TokenData = Depends(get_current_admin_user),
):
    """
    清空指定日志文件（需要管理员权限）。

    **二次确认**：query 参数 `confirm` 必须与 `log_type` 完全一致，
    与 purge queue 端点的安全机制对齐。
    """
    if confirm != log_type:
        raise HTTPException(
            status_code=400,
            detail=f"confirm 参数必须与 log_type 一致（收到 {confirm!r}，期望 {log_type!r}）",
        )
    log_path = _get_log_path(log_type)
    try:
        with open(log_path, "w", encoding="utf-8") as f:
            f.write("")
        user = (
            getattr(current_user, "username", None)
            or getattr(current_user, "user_id", None)
        )
        logger.warning("[LOGS] admin=%s 清空了日志文件 %s", user, LOG_FILES[log_type])
        return {"message": f"日志文件 {LOG_FILES[log_type]} 已清空", "filename": LOG_FILES[log_type]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"清空日志文件失败: {str(e)}")
