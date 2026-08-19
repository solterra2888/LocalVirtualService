# -*- coding: utf-8 -*-
"""
知识星球文件下载状态机服务

封装 zsxq_files 表的 CRUD，供抓取链路、消费者任务、API 读取层共用：
  - upsert_pending_batch : 抓取链路写入 pending 行（ON CONFLICT DO NOTHING，重抓不重置）
  - begin_processing     : 消费者领任务（locked_at 加锁；pending→downloading）
  - mark_downloaded      : 下载完成（落 local_path）
  - mark_uploaded        : 上传完成（落 oss_object_name，清 local_path）
  - mark_blocked / mark_failed / bump_retry
  - get_by_file_id       : 下载代理端点用
  - get_by_topic_ids     : API 读取合并用（按 topic_id 批量）
  - fetch_resumable_ids  : dispatcher 入队用（pending + downloaded 且未锁定）
  - count_in_flight      : dispatcher in-flight 上限控制
  - reset_stale          : reaper 崩溃恢复

状态机：pending → downloading → downloaded → uploaded（终态成功）
       blocked（1030 硬限制）/ failed（重试耗尽）/ skipped（音频等不下载）
"""

import logging
import os
import re
from typing import Any, Dict, List, Optional

import psycopg2
import psycopg2.extras

from backend.utils.database_config import get_database_config
from backend.utils.env_loader import load_project_env

load_project_env()

logger = logging.getLogger(__name__)

# 终态：不再被消费者/dispatcher 处理
TERMINAL_STATUSES = ("uploaded", "blocked", "failed", "skipped")
# 可被消费者继续推进的状态（dispatcher 入队 + begin_processing 接受）
RESUMABLE_STATUSES = ("pending", "downloaded")

# 知识星球附件里的音视频不入库下载队列（用户侧不需要）
_AUDIO_EXT_RE = re.compile(
    r"\.(mp3|m4a|mp4|wav|aac|flac|ogg|wma|amr|opus|mov|mkv|avi|webm)$",
    re.IGNORECASE,
)
_AUDIO_SQL_RE = r"\.(mp3|m4a|mp4|wav|aac|flac|ogg|wma|amr|opus|mov|mkv|avi|webm)$"


def is_audio_filename(name: str) -> bool:
    return bool(_AUDIO_EXT_RE.search(name or ""))


def _get_conn():
    cfg = get_database_config()
    return psycopg2.connect(
        host=cfg["db_host"],
        port=cfg["db_port"],
        dbname=cfg["db_name"],
        user=cfg["db_user"],
        password=cfg["db_password"],
    )


class ZsxqFileService:
    """知识星球附件文件下载状态机。"""

    # ---------- 写入（抓取链路） ----------

    def upsert_pending_batch(
        self,
        group_id: str,
        topic_id: str,
        files: List[Dict[str, Any]],
    ) -> int:
        """
        把一个 topic 的所有附件文件以 pending 写入（幂等：重抓不重置已下载状态）。

        files 元素形如 {file_id, name, size}。跳过无 file_id 的项。
        返回实际插入的行数（已存在的行因 ON CONFLICT DO NOTHING 不计数）。
        """
        if not files:
            return 0
        topic_id = str(topic_id or "")
        rows = []
        for f in files:
            fid = f.get("file_id") or f.get("id")
            if not fid:
                continue
            rows.append((
                str(fid),
                str(group_id or ""),
                topic_id,
                str(f.get("name") or ""),
                int(f.get("size") or 0),
            ))
        if not rows:
            return 0

        inserted = 0
        with _get_conn() as conn:
            with conn.cursor() as cur:
                for params in rows:
                    status = "skipped" if is_audio_filename(params[3]) else "pending"
                    error = "audio_not_downloaded" if status == "skipped" else ""
                    cur.execute(
                        """
                        INSERT INTO zsxq_files (file_id, group_id, topic_id, name, size, status, error)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (file_id) DO NOTHING
                        """,
                        params + (status, error),
                    )
                    inserted += cur.rowcount
            conn.commit()
        if inserted:
            logger.info(
                "ZSXQ 文件入队 topic=%s 新增 %d/%d 个 pending",
                topic_id, inserted, len(rows),
            )
        return inserted

    # ---------- 消费者领任务 ----------

    def begin_processing(self, file_id: str) -> Optional[Dict[str, Any]]:
        """
        原子地标记开始处理：locked_at=now()；若 status='pending' 则置 'downloading'。
        已是终态（uploaded/blocked/failed）则返回 None（幂等跳过）。
        """
        with _get_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    UPDATE zsxq_files
                    SET locked_at   = now(),
                        status      = CASE WHEN status = 'pending' THEN 'downloading' ELSE status END,
                        updated_at  = now()
                    WHERE file_id = %s
                      AND status <> ALL(%s)
                    RETURNING file_id, group_id, topic_id, name, size,
                              status, local_path, oss_object_name, retry_count
                    """,
                    (str(file_id), list(TERMINAL_STATUSES)),
                )
                row = cur.fetchone()
            conn.commit()
        return dict(row) if row else None

    def mark_downloaded(self, file_id: str, local_path: str) -> None:
        with _get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE zsxq_files
                    SET status = 'downloaded', local_path = %s, error = '', updated_at = now()
                    WHERE file_id = %s
                    """,
                    (local_path, str(file_id)),
                )
            conn.commit()

    def mark_uploaded(self, file_id: str, oss_object_name: str, oss_bucket: str) -> None:
        """上传完成：落 OSS 位置，清空本地临时路径（文件已由调用方删除）。"""
        with _get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE zsxq_files
                    SET status = 'uploaded',
                        oss_object_name = %s,
                        oss_bucket      = %s,
                        local_path      = '',
                        error           = '',
                        locked_at       = NULL,
                        updated_at      = now()
                    WHERE file_id = %s
                    """,
                    (oss_object_name, oss_bucket, str(file_id)),
                )
            conn.commit()

    def mark_blocked(self, file_id: str, error: str) -> None:
        """1030 硬限制：终态跳过，不再重试。"""
        with _get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE zsxq_files
                    SET status = 'blocked', error = %s, locked_at = NULL, updated_at = now()
                    WHERE file_id = %s
                    """,
                    (error[:500], str(file_id)),
                )
            conn.commit()

    def mark_failed(self, file_id: str, error: str) -> None:
        """重试耗尽：终态失败。"""
        with _get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE zsxq_files
                    SET status = 'failed', error = %s, locked_at = NULL, updated_at = now()
                    WHERE file_id = %s
                    """,
                    (error[:500], str(file_id)),
                )
            conn.commit()

    def mark_skipped(self, file_id: str, error: str = "audio_not_downloaded") -> None:
        """主动跳过（音频等不下载）：终态，不再入队。"""
        with _get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE zsxq_files
                    SET status = 'skipped', error = %s, locked_at = NULL, updated_at = now()
                    WHERE file_id = %s
                      AND status <> ALL(%s)
                    """,
                    ((error or "")[:500], str(file_id), list(("uploaded",))),
                )
            conn.commit()

    def skip_audio_backlog(self) -> int:
        """把尚未成功上传的音视频标为 skipped，避免占下载队列。"""
        with _get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE zsxq_files
                    SET status = 'skipped',
                        error = CASE WHEN error = '' THEN 'audio_not_downloaded' ELSE error END,
                        locked_at = NULL,
                        updated_at = now()
                    WHERE status <> ALL(%s)
                      AND name ~* %s
                    """,
                    (list(("uploaded", "skipped")), _AUDIO_SQL_RE),
                )
                affected = cur.rowcount
            conn.commit()
        if affected:
            logger.info("ZSXQ 跳过音频积压 %d 个", affected)
        return affected

    def bump_retry(self, file_id: str, error: str) -> int:
        """下载阶段失败：retry_count+1，回退 pending 并清空 local_path（需重新下载）。返回新 retry_count。"""
        with _get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE zsxq_files
                    SET retry_count = retry_count + 1,
                        error = %s,
                        status = 'pending',
                        local_path = '',
                        locked_at = NULL,
                        updated_at = now()
                    WHERE file_id = %s
                    RETURNING retry_count
                    """,
                    (error[:500], str(file_id)),
                )
                row = cur.fetchone()
            conn.commit()
        return int(row[0]) if row else 0

    def note_retry(self, file_id: str, error: str) -> int:
        """上传阶段失败：retry_count+1，保留 status='downloaded' 与 local_path（不重下大文件）。返回新 retry_count。"""
        with _get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE zsxq_files
                    SET retry_count = retry_count + 1,
                        error = %s,
                        locked_at = NULL,
                        updated_at = now()
                    WHERE file_id = %s
                    RETURNING retry_count
                    """,
                    (error[:500], str(file_id)),
                )
                row = cur.fetchone()
            conn.commit()
        return int(row[0]) if row else 0

    def clear_lock(self, file_id: str) -> None:
        """释放锁（保留当前状态），供熔断时使用。"""
        with _get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE zsxq_files SET locked_at = NULL, updated_at = now() WHERE file_id = %s",
                    (str(file_id),),
                )
            conn.commit()

    # ---------- 读取 ----------

    def get_by_file_id(self, file_id: str) -> Optional[Dict[str, Any]]:
        with _get_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT file_id, group_id, topic_id, name, size, status,
                           local_path, oss_object_name, oss_bucket, retry_count, error
                    FROM zsxq_files
                    WHERE file_id = %s
                    """,
                    (str(file_id),),
                )
                row = cur.fetchone()
        return dict(row) if row else None

    def get_by_topic_ids(self, topic_ids: List[str]) -> Dict[str, List[Dict[str, Any]]]:
        """
        按 topic_id 批量查询，返回 {topic_id: [file_row, ...]}。
        供 API 读取层合并 metadata.files 用。
        """
        out: Dict[str, List[Dict[str, Any]]] = {}
        if not topic_ids:
            return out
        with _get_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT file_id, topic_id, name, size, status, oss_object_name
                    FROM zsxq_files
                    WHERE topic_id = ANY(%s)
                    """,
                    (list(topic_ids),),
                )
                rows = cur.fetchall()
        for r in rows:
            out.setdefault(r["topic_id"], []).append(dict(r))
        return out

    # ---------- dispatcher / reaper ----------

    def fetch_resumable_ids(self, limit: int = 20) -> List[str]:
        """返回待入队的 file_id 列表（非音频、pending/downloaded、未锁定）。"""
        return self.claim_for_dispatch(limit=limit)

    def claim_for_dispatch(self, limit: int = 40) -> List[str]:
        """
        原子领取待处理文件：加上 locked_at，避免重复入队。
        只领非音频的 pending/downloaded。
        """
        ids: List[str] = []
        with _get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE zsxq_files AS f
                    SET locked_at = now(), updated_at = now()
                    WHERE f.file_id IN (
                        SELECT file_id FROM zsxq_files
                        WHERE status = ANY(%s)
                          AND locked_at IS NULL
                          AND name !~* %s
                        ORDER BY updated_at
                        LIMIT %s
                        FOR UPDATE SKIP LOCKED
                    )
                    RETURNING f.file_id
                    """,
                    (list(RESUMABLE_STATUSES), _AUDIO_SQL_RE, int(limit)),
                )
                ids = [r[0] for r in cur.fetchall()]
            conn.commit()
        return ids

    def count_in_flight(self) -> int:
        """已领取/处理中、尚未到终态的数量（含已入队但 worker 未开始的行）。"""
        with _get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COUNT(*) FROM zsxq_files
                    WHERE locked_at IS NOT NULL
                      AND status <> ALL(%s)
                    """,
                    (list(TERMINAL_STATUSES),),
                )
                row = cur.fetchone()
        return int(row[0]) if row else 0

    def reset_stale(self, timeout_minutes: int = 15) -> int:
        """
        reaper：把卡住超过 timeout_minutes 的行回退。
          - downloading（下载中断，本地文件可能不完整）→ 回退 pending，清 local_path
          - downloaded（下载已完成，上传中断）→ 仅释放锁，保留 downloaded，等重新派发上传
          - pending 已领取但 worker 未开始 → 仅释放锁
          - downloading 且 locked_at 为空（历史僵死）→ 回退 pending
        返回受影响行数。
        """
        with _get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE zsxq_files
                    SET status = CASE
                            WHEN status = 'downloading' THEN 'pending'
                            ELSE status
                        END,
                        local_path = CASE
                            WHEN status = 'downloading' THEN ''
                            ELSE local_path
                        END,
                        locked_at = NULL,
                        updated_at = now()
                    WHERE status <> ALL(%s)
                      AND (
                            (locked_at IS NOT NULL
                             AND locked_at < now() - (%s || ' minutes')::interval)
                         OR (status = 'downloading' AND locked_at IS NULL)
                      )
                    """,
                    (list(TERMINAL_STATUSES), str(timeout_minutes)),
                )
                affected = cur.rowcount
            conn.commit()
        if affected:
            logger.info("ZSXQ reaper 回退 %d 个僵死文件任务", affected)
        return affected


# ---------- API 读取层合并 ----------

def _file_ext(name: str) -> str:
    return os.path.splitext(name or "")[1].lower().lstrip(".")


def _ui_status(db_status: str, name: str = "") -> str:
    """前端展示用状态：uploaded / processing / skipped / failed。"""
    st = str(db_status or "")
    if st == "uploaded":
        return "uploaded"
    if st == "skipped" or is_audio_filename(name):
        return "skipped"
    if st in ("blocked", "failed"):
        return "failed"
    return "processing"


def build_files_view(
    records: List[Dict[str, Any]],
    meta_files: Any,
) -> List[Dict[str, Any]]:
    """
    构造前端可消费的 metadata.files 视图：
      [{file_id, name, size, ext, ready, status}]

    - records（来自 zsxq_files）非空时：以 records 为准
    - 否则：从 metadata.files 兜底（兼容历史 string[] / 对象数组）
    """
    out: List[Dict[str, Any]] = []
    if records:
        for r in records:
            name = str(r.get("name") or "")
            ui = _ui_status(str(r.get("status") or ""), name)
            out.append({
                "file_id": str(r.get("file_id") or ""),
                "name": name,
                "size": int(r.get("size") or 0),
                "ext": _file_ext(name),
                "ready": ui == "uploaded",
                "status": ui,
            })
        return out

    if not meta_files:
        return []
    if isinstance(meta_files, str):
        meta_files = [meta_files]
    for f in meta_files:
        if isinstance(f, dict):
            name = str(f.get("name") or "")
            ui = _ui_status("", name)
            out.append({
                "file_id": str(f.get("file_id") or ""),
                "name": name,
                "size": int(f.get("size") or 0),
                "ext": _file_ext(name),
                "ready": False,
                "status": ui,
            })
        elif isinstance(f, str) and f.strip():
            ui = _ui_status("", f)
            out.append({
                "file_id": "",
                "name": f,
                "size": 0,
                "ext": _file_ext(f),
                "ready": False,
                "status": ui,
            })
    return out


def enrich_contents_files(contents: List[Dict[str, Any]]) -> None:
    """
    原地把 zsxq_files 下载状态合并进 zhishixingqiu 内容的 metadata.files。

    供 subscriptions / daily 的内容列表/详情端点在返回前调用（零侵入 subscription_content）。
    """
    zsxq_items = [
        c for c in contents
        if c.get("channel_type") == "zhishixingqiu" and c.get("content_id") is not None
    ]
    if not zsxq_items:
        return
    topic_ids = [str(c["content_id"]) for c in zsxq_items]
    files_by_topic = ZsxqFileService().get_by_topic_ids(topic_ids)
    for c in zsxq_items:
        tid = str(c["content_id"])
        meta = c.get("metadata") or {}
        records = files_by_topic.get(tid) or []
        # 仅当该 topic 有文件（records 或 metadata.files）时才覆盖，避免空数组覆盖
        if not records and not (meta.get("files")):
            continue
        merged = build_files_view(records, meta.get("files"))
        meta["files"] = merged
        c["metadata"] = meta

