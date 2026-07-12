# -*- coding: utf-8 -*-
"""
Substack 订阅抓取服务（双轨）

核心职责：
  1. 从 substack_credentials 读取有效 Cookie（Fernet 解密 → SubstackAuth）
  2. 文章轨（源A）：按 publication_url 聚合用户订阅 → Newsletter.get_posts 抓文章
  3. 动态轨（源B）：遍历 substack_followed_authors → cursor 翻页抓 Notes
  4. 通过 SubstackAdapter → SubscriptionContentService.save_content 写入 subscription_content
  5. 更新 substack_publications / substack_followed_authors 同步锚点
  6. 401/403 抛 CookieExpiredError，标记凭证过期，不中断整轮

架构对齐：zsxq_subscription_service.py（聚合 + 逐源顺序抓取 + 共享内容行）
关键差异：浅分页（get_posts(limit=N)，无 end_time 多页）+ 动态按作者 cursor 翻页
"""

import json
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
import psycopg2.extras

from backend.utils.database_config import get_database_config
from backend.utils.env_loader import load_project_env
from backend.utils.datetime_utils import utcnow
from backend.services.subscription_content_service import SubscriptionContentService
from backend.services.substack_credential_service import (
    SID_KEY,
    SubstackCredentialService,
    fetch_author_notes_via_auth,
    normalize_publication_url,
    parse_cookie_json,
)

load_project_env()

logger = logging.getLogger(__name__)

# ---------- 常量（对齐 scripts/substack/my_substack.py） ----------
BACKFILL_POSTS = int(os.getenv("SUBSTACK_BACKFILL_POSTS", "30") or 30)
INCREMENT_MAX_POSTS = int(os.getenv("SUBSTACK_INCREMENT_MAX_POSTS", "20") or 20)
PUB_INTERVAL = int(os.getenv("SUBSTACK_PUB_INTERVAL", "5") or 5)

# 动态轨
CRAWL_NOTES = (os.getenv("SUBSTACK_CRAWL_NOTES", "true").strip().lower() not in ("0", "false", "no", "off", ""))
BACKFILL_NOTES = int(os.getenv("SUBSTACK_BACKFILL_NOTES", "50") or 50)
NOTES_PAGE_CAP = int(os.getenv("SUBSTACK_NOTES_PAGE_CAP", "10") or 10)
NOTES_INTERVAL = float(os.getenv("SUBSTACK_NOTES_INTERVAL", "2.0") or 2.0)
NOTES_PAGE_SIZE = 25
AUTHOR_INTERVAL = NOTES_INTERVAL


# ---------- 异常 ----------

class CookieExpiredError(Exception):
    """凭证 Cookie 已失效（HTTP 401/403 或 SubstackAuth 认证失败）。"""


@dataclass
class SubstackRunReport:
    """单次抓取运行的错误与统计汇总（供 Scheduler 完成后推送 Lark）。"""

    started_at: datetime = field(default_factory=utcnow)
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    post_summaries: List[Dict[str, Any]] = field(default_factory=list)
    note_summaries: List[Dict[str, Any]] = field(default_factory=list)
    failed_publications: List[str] = field(default_factory=list)
    failed_authors: List[str] = field(default_factory=list)

    def record_error(self, message: str) -> None:
        if message and message not in self.errors:
            self.errors.append(message)

    def record_warning(self, message: str) -> None:
        if message and message not in self.warnings:
            self.warnings.append(message)

    def add_post_summary(self, name: str, mode: str, new_count: int) -> None:
        self.post_summaries.append({"name": name, "mode": mode, "new": new_count})

    def add_note_summary(self, handle: str, new_count: int) -> None:
        self.note_summaries.append({"handle": handle, "new": new_count})

    def to_dict(self) -> Dict[str, Any]:
        finished = utcnow()
        return {
            "errors": self.errors,
            "warnings": self.warnings,
            "post_summaries": self.post_summaries,
            "note_summaries": self.note_summaries,
            "failed_publications": self.failed_publications,
            "failed_authors": self.failed_authors,
            "finished_at": finished.strftime("%Y-%m-%d %H:%M:%S UTC"),
            "duration_sec": (finished - self.started_at).total_seconds(),
        }


# ---------- DB helpers ----------

def _get_conn():
    cfg = get_database_config()
    return psycopg2.connect(
        host=cfg["db_host"],
        port=cfg["db_port"],
        dbname=cfg["db_name"],
        user=cfg["db_user"],
        password=cfg["db_password"],
    )


def _parse_dt(value: Any) -> Optional[datetime]:
    """解析 ISO 时间字符串（含 Z/+00:00）或 epoch；失败返回 None。"""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    s = str(value).strip()
    if not s:
        return None
    # epoch ms?
    if s.isdigit() and len(s) >= 12:
        try:
            return datetime.fromtimestamp(int(s) / 1000.0, tz=timezone.utc)
        except (ValueError, OSError):
            pass
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None


def _extract_note_text(body_json: Any) -> str:
    """遍历 ProseMirror 树拼接所有 text 节点为纯文本。"""
    out: List[str] = []

    def walk(node: Any) -> None:
        if isinstance(node, dict):
            if node.get("type") == "text" and isinstance(node.get("text"), str):
                out.append(node["text"])
            for child in node.get("content") or []:
                walk(child)
        elif isinstance(node, list):
            for child in node:
                walk(child)

    walk(body_json)
    return "".join(out).strip()


# ---------- 半标准化（对应 zsxq _extract_content / 脚本 post_to_row、note_item_to_row） ----------

def _post_to_row(post: Any, pub_url: str, pub_name: str, avatar: str = "") -> Dict[str, Any]:
    meta = post.get_metadata() or {}
    post_id = meta.get("id")
    content = post.get_content() or meta.get("truncated_body_text") or meta.get("description") or ""
    authors = meta.get("publishedBylines") or []
    author = ", ".join(a.get("name", "") for a in authors if a.get("name"))
    return {
        "_content_kind": "post",
        "post_id": int(post_id) if post_id is not None else None,
        "publication_url": pub_url,
        "publication_name": pub_name,
        "title": meta.get("title") or "",
        "subtitle": meta.get("subtitle") or "",
        "canonical_url": meta.get("canonical_url") or getattr(post, "url", "") or "",
        "post_date": meta.get("post_date"),
        "author": author,
        "audience": meta.get("audience") or "",
        "is_paywalled": bool(post.is_paywalled()),
        "content": content or "",
        "author_avatar": avatar,
    }


def _note_to_row(item: Dict[str, Any], profile: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    entity_key = str(item.get("entity_key") or "")
    if not entity_key.startswith("c-"):
        return None
    try:
        note_id = int(entity_key[2:])
    except ValueError:
        return None

    ctx = item.get("context") or {}
    comment = item.get("comment") or {}
    body_text = comment.get("body") or ""
    if not body_text and comment.get("body_json"):
        body_text = _extract_note_text(comment["body_json"])

    handle = profile.get("author_handle") or ""
    published_at = ctx.get("timestamp") or comment.get("date") or None
    note_type = ctx.get("type") or item.get("type") or ""
    note_url = f"https://substack.com/@{handle}/note/c-{note_id}" if handle else ""

    return {
        "_content_kind": "note",
        "note_id": note_id,
        "publication_url": profile.get("publication_url") or "https://substack.com",
        "author_user_id": profile.get("substack_user_id"),
        "author_handle": handle,
        "author_name": profile.get("author_name") or "",
        "note_url": note_url,
        "note_type": note_type,
        "body_text": body_text,
        "published_at": published_at,
        "author_avatar": profile.get("author_avatar") or "",
    }


# ---------- SubstackClient（轻量封装 substack-api） ----------

class SubstackClient:
    """对 Newsletter/Post 的轻量封装；auth 由调用方注入。"""

    def __init__(self, auth: Any, pub_url: str):
        from substack_api import Newsletter
        self.auth = auth
        self.pub_url = pub_url
        self.newsletter = Newsletter(pub_url, auth=auth)

    def get_posts(self, limit: int) -> List[Any]:
        return self.newsletter.get_posts(limit=limit)


# ---------- 入库（对应 zsxq _store_topic_batch） ----------

def _store_batch(
    content_service: SubscriptionContentService,
    batch: List[Dict[str, Any]],
    user_ids: List[int],
    label: str,
    report: Optional[SubstackRunReport] = None,
) -> Tuple[int, int, List[int]]:
    new_count = 0
    updated_count = 0
    new_ids: List[int] = []
    for row in batch:
        try:
            content_db_id, is_new = content_service.save_content(
                channel_type="substack",
                raw_data=row,
                user_ids=user_ids,
            )
            if is_new:
                new_count += 1
                new_ids.append(content_db_id)
            else:
                updated_count += 1
        except Exception as exc:
            logger.exception("Substack 内容入库失败 %s", row.get("post_id") or row.get("note_id"))
            if report:
                report.record_error(f"{label} 入库失败: {exc}")
    return new_count, updated_count, new_ids


def _trigger_tagging(new_ids: List[int], label: str, report: Optional[SubstackRunReport]) -> None:
    if not new_ids:
        return
    try:
        from backend.tasks.file_tasks import trigger_auto_feed_tagging
        trigger_auto_feed_tagging(new_ids)
    except Exception as exc:
        logger.warning("Substack 触发打标失败，将在下次 task 中补全")
        if report:
            report.record_warning(f"{label} 触发打标失败: {exc}")


# ---------- 锚点持久化 ----------

def _save_publication_state(
    conn,
    pub_url: str,
    rows: List[Dict[str, Any]],
    mode: str,
    head_ids: Optional[List[int]] = None,
    backfill_complete: Optional[bool] = None,
) -> None:
    dates = [_parse_dt(r.get("post_date")) for r in rows]
    dates = [d for d in dates if d]
    updates: Dict[str, Any] = {
        "last_posts_sync_at": utcnow().isoformat(),
        "last_posts_sync_mode": mode,
    }
    if dates:
        batch_min, batch_max = min(dates), max(dates)
        updates["new_min"] = batch_min
        updates["new_max"] = batch_max
    if head_ids is not None:
        updates["recent_post_ids"] = head_ids
    if backfill_complete is None:
        backfill_complete = mode == "backfill"
    updates["backfill_complete"] = backfill_complete

    with conn.cursor() as cur:
        if "new_min" in updates:
            cur.execute(
                """
                UPDATE substack_publications SET
                    last_posts_sync_at   = %s,
                    last_posts_sync_mode = %s,
                    min_post_date = LEAST(COALESCE(min_post_date, %s::timestamptz), %s::timestamptz),
                    max_post_date = GREATEST(COALESCE(max_post_date, %s::timestamptz), %s::timestamptz),
                    recent_post_ids = COALESCE(%s, recent_post_ids),
                    backfill_complete = CASE WHEN %s THEN true ELSE backfill_complete END,
                    updated_at = now()
                WHERE publication_url = %s
                """,
                (
                    updates["last_posts_sync_at"], mode,
                    updates["new_min"], updates["new_min"],
                    updates["new_max"], updates["new_max"],
                    updates.get("recent_post_ids"),
                    updates["backfill_complete"],
                    pub_url,
                ),
            )
        else:
            cur.execute(
                """
                UPDATE substack_publications SET
                    last_posts_sync_at   = %s,
                    last_posts_sync_mode = %s,
                    recent_post_ids = COALESCE(%s, recent_post_ids),
                    backfill_complete = CASE WHEN %s THEN true ELSE backfill_complete END,
                    updated_at = now()
                WHERE publication_url = %s
                """,
                (
                    updates["last_posts_sync_at"], mode,
                    updates.get("recent_post_ids"),
                    updates["backfill_complete"],
                    pub_url,
                ),
            )
    conn.commit()


def _save_author_state(
    conn,
    user_id: int,
    head_ids: Optional[List[int]],
    backfill_complete: bool,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE substack_followed_authors SET
                last_notes_sync_at = %s,
                recent_note_ids = COALESCE(%s, recent_note_ids),
                backfill_complete = CASE WHEN %s THEN true ELSE backfill_complete END,
                updated_at = now()
            WHERE substack_user_id = %s
            """,
            (utcnow().isoformat(), head_ids, backfill_complete, user_id),
        )
    conn.commit()


# ---------- 主服务 ----------

class SubstackSubscriptionService:
    """Substack 订阅抓取服务（双轨）。"""

    def __init__(self):
        self.content_service = SubscriptionContentService()
        self._cred_svc = SubstackCredentialService()
        # cred_id → (auth, tmp_path) 缓存，运行结束统一清理
        self._auth_cache: Dict[int, Tuple[Any, str]] = {}

    # ---- 凭证 / auth 物化 ----

    def _get_auth_for_credential(self, cred_id: Optional[int]) -> Tuple[int, Any]:
        """按 publication/author 的 credential_id 物化 SubstackAuth，缓存复用。"""
        if cred_id and cred_id in self._auth_cache:
            return cred_id, self._auth_cache[cred_id][0]

        # 选定 cred_id：优先传入的，否则首个有效凭证
        target_cred = cred_id
        if not target_cred:
            result = self._cred_svc.get_first_valid_cookie()
            if not result:
                raise RuntimeError("无可用的 Substack 凭证，请在管理员后台录入有效 Cookie")
            target_cred = result[0]

        if target_cred in self._auth_cache:
            return target_cred, self._auth_cache[target_cred][0]

        try:
            auth, tmp_path = self._cred_svc._materialize_auth(target_cred)
        except RuntimeError:
            # 物化失败 → 标记过期并抛 CookieExpiredError，由主循环捕获
            self._cred_svc.mark_expired(target_cred, "SubstackAuth 物化失败（cookies 无效）")
            raise CookieExpiredError(f"凭证 id={target_cred} 物化失败")

        self._auth_cache[target_cred] = (auth, tmp_path)
        return target_cred, auth

    def _cleanup_auth_cache(self) -> None:
        for cred_id, (auth, tmp_path) in list(self._auth_cache.items()):
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
        self._auth_cache.clear()

    def _mark_credential_expired(self, cred_id: int, error: str) -> None:
        self._cred_svc.mark_expired(cred_id, error)
        # 失效后从缓存移除（下次不再复用）
        if cred_id in self._auth_cache:
            _auth, tmp_path = self._auth_cache.pop(cred_id)
            try:
                os.unlink(tmp_path)
            except OSError:
                pass

    # ---- 聚合 ----

    def _aggregate_publications(
        self,
        subscription_id: Optional[int] = None,
        user_id: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """按 publication_url 聚合 enabled substack 订阅的 user_ids 并集。"""
        with _get_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                sql = """
                    SELECT
                        sl.channel_identifier AS publication_url,
                        sl.channel_name AS publication_name,
                        ARRAY_AGG(DISTINCT sl.user_id) AS user_ids
                    FROM subscription_list sl
                    WHERE sl.channel_type = 'substack' AND sl.enabled = true
                """
                params: List[Any] = []
                if subscription_id:
                    sql += " AND sl.id = %s"
                    params.append(subscription_id)
                if user_id:
                    sql += " AND sl.user_id = %s"
                    params.append(user_id)
                sql += " GROUP BY sl.channel_identifier, sl.channel_name ORDER BY sl.channel_identifier"
                cur.execute(sql, params)
                rows = cur.fetchall()

        result = []
        for row in rows:
            url = row["publication_url"]
            try:
                url = normalize_publication_url(url)
            except ValueError:
                continue
            result.append({
                "publication_url": url,
                "publication_name": row["publication_name"] or url,
                "user_ids": list(row["user_ids"] or []),
            })
        return result

    def _user_ids_for_pub(self, agg: List[Dict[str, Any]]) -> Dict[str, List[int]]:
        return {item["publication_url"]: item["user_ids"] for item in agg}

    def _get_publication_meta(self, pub_url: str) -> Dict[str, Any]:
        with _get_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT * FROM substack_publications WHERE publication_url = %s",
                    (pub_url,),
                )
                row = cur.fetchone()
        return dict(row) if row else {}

    def _list_enabled_followed_authors(self) -> List[Dict[str, Any]]:
        with _get_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT substack_user_id, author_handle, author_name,
                           publication_url, credential_id, backfill_complete,
                           recent_note_ids, avatar
                    FROM substack_followed_authors
                    WHERE enabled = true
                    ORDER BY author_name, author_handle
                    """
                )
                return [dict(r) for r in cur.fetchall()]

    # ---- 文章轨（源 A） ----

    def _crawl_posts_for_publication(
        self,
        auth: Any,
        pub: Dict[str, Any],
        meta: Dict[str, Any],
        force_fetch: bool,
        user_ids: List[int],
        report: Optional[SubstackRunReport],
        idx: int,
        total: int,
    ) -> Tuple[int, int]:
        pub_url = pub["publication_url"]
        pub_name = pub["publication_name"] or meta.get("publication_name") or pub_url
        pub_avatar = str(meta.get("avatar") or "").strip()
        backfill_done = bool(meta.get("backfill_complete", False))
        known_ids: List[int] = [int(x) for x in (meta.get("recent_post_ids") or [])]

        client = SubstackClient(auth, pub_url)

        if not backfill_done or force_fetch:
            mode = "backfill"
            logger.info("[%d/%d] Substack %s backfill %d 篇", idx, total, pub_name, BACKFILL_POSTS)
            posts = client.get_posts(BACKFILL_POSTS)
            rows = [_post_to_row(p, pub_url, pub_name, pub_avatar) for p in posts]
            head_ids = [r["post_id"] for r in rows if r.get("post_id") is not None][:20]
        else:
            mode = "incremental"
            logger.info("[%d/%d] Substack %s 增量, 锚点 %d 个", idx, total, pub_name, len(known_ids))
            posts = client.get_posts(INCREMENT_MAX_POSTS)
            known = set(known_ids)
            rows = []
            head_ids = []
            for p in posts:
                row = _post_to_row(p, pub_url, pub_name, pub_avatar)
                if row.get("post_id") is not None:
                    head_ids.append(row["post_id"])
                    if row["post_id"] in known:
                        continue
                rows.append(row)

        new_count, updated_count, new_ids = _store_batch(
            self.content_service, rows, user_ids, pub_name, report=report
        )
        _trigger_tagging(new_ids, pub_name, report)

        with _get_conn() as conn:
            _save_publication_state(
                conn, pub_url, rows, mode,
                head_ids=head_ids or None,
                backfill_complete=(mode == "backfill"),
            )

        logger.info("Substack %s 完成 [%s] 新 %d 条", pub_name, mode, new_count)
        if report:
            report.add_post_summary(pub_name, mode, new_count)
        return new_count, updated_count

    # ---- 动态轨（源 B） ----

    def _crawl_notes_for_author(
        self,
        auth: Any,
        author: Dict[str, Any],
        user_ids_by_pub: Dict[str, List[int]],
        report: Optional[SubstackRunReport],
        idx: int,
        total: int,
    ) -> int:
        uid = author["substack_user_id"]
        handle = author.get("author_handle") or str(uid)
        pub_url = author.get("publication_url") or "https://substack.com"
        profile = {
            "substack_user_id": uid,
            "author_handle": handle,
            "author_name": author.get("author_name") or "",
            "publication_url": pub_url,
            "author_avatar": author.get("avatar") or "",
        }
        # map 的 key 是归一化后的 publication_url (见 _aggregate_publications :455),
        # 而 author.publication_url 来自 substack_followed_authors 未必归一化 -> 先归一化再查,
        # 否则已订阅 pub 也可能因 www/尾斜杠/大小写不一致而 miss (第二类孤儿).
        try:
            norm_pub_url = normalize_publication_url(pub_url)
        except ValueError:
            norm_pub_url = pub_url
        user_ids = user_ids_by_pub.get(norm_pub_url)
        if not user_ids:
            # followed author 的 publication 可能未被任何用户直接订阅 (notes-only 作者,
            # 或 publication_url 缺省 'https://substack.com') -> 反查为空, note 以 user_ids=[]
            # 入库后对所有用户不可见 (孤儿). 回退归属给所有启用 substack 订阅的用户 (共享 credential
            # 架构下无 per-user 归属; 结构性根治 —— 给 substack_followed_authors 加 user 关联 —— 延后).
            user_ids = sorted({uid for ids in user_ids_by_pub.values() for uid in ids})
        known = {int(x) for x in (author.get("recent_note_ids") or [])}
        backfill_done = bool(author.get("backfill_complete", False))

        collected: List[Dict[str, Any]] = []
        head_ids: List[int] = []
        cursor: Optional[str] = None
        pages = 0
        stop = False
        backfill_limit = BACKFILL_NOTES if not backfill_done else NOTES_PAGE_CAP * NOTES_PAGE_SIZE

        while pages < NOTES_PAGE_CAP and not stop:
            data = fetch_author_notes_via_auth(auth, uid, cursor=cursor, limit=NOTES_PAGE_SIZE)
            items = data.get("items") or [] if isinstance(data, dict) else []
            hit_known = False
            for it in items:
                row = _note_to_row(it, profile)
                if not row:
                    continue
                collected.append(row)
                if row["note_id"] not in head_ids:
                    head_ids.append(row["note_id"])
                if row["note_id"] in known:
                    hit_known = True
            pages += 1
            if not items or hit_known or len(collected) >= backfill_limit:
                stop = True
            cursor = data.get("nextCursor") if isinstance(data, dict) else None
            if not cursor:
                break
            if not stop:
                time.sleep(NOTES_INTERVAL)

        new_count, _updated, new_ids = _store_batch(
            self.content_service, collected, user_ids, f"@{handle}", report=report
        )
        _trigger_tagging(new_ids, f"@{handle}", report)

        with _get_conn() as conn:
            _save_author_state(
                conn, uid,
                head_ids=head_ids[:50] or None,
                backfill_complete=(not backfill_done),
            )

        logger.info(
            "[notes %d/%d] @%s | 拉取 %d 条, 新 %d (%d 页)",
            idx, total, handle, len(collected), new_count, pages,
        )
        if report:
            report.add_note_summary(handle, new_count)
        return new_count

    # ---- 主入口 ----

    def fetch_and_store(
        self,
        subscription_id: Optional[int] = None,
        user_id: Optional[int] = None,
        force_fetch: bool = False,
        *,
        collect_report: bool = False,
    ) -> Dict[str, Any]:
        """
        主入口：抓取所有（或指定）Substack 订阅源的文章 + 关注作者的动态，写入 subscription_content。

        Returns:
            {success, publications_processed, posts_new, posts_updated,
             notes_new, authors_processed, failed_publications, failed_authors, report?}
        """
        report = SubstackRunReport() if collect_report else None

        def _result(**kwargs: Any) -> Dict[str, Any]:
            base = {
                "success": kwargs.pop("success", True),
                "publications_processed": kwargs.pop("publications_processed", 0),
                "posts_new": kwargs.pop("posts_new", 0),
                "posts_updated": kwargs.pop("posts_updated", 0),
                "notes_new": kwargs.pop("notes_new", 0),
                "authors_processed": kwargs.pop("authors_processed", 0),
                "failed_publications": kwargs.pop("failed_publications", []),
                "failed_authors": kwargs.pop("failed_authors", []),
            }
            base.update(kwargs)
            if report:
                base["report"] = report.to_dict()
                if base["failed_publications"]:
                    report.failed_publications = list(base["failed_publications"])
                if base["failed_authors"]:
                    report.failed_authors = list(base["failed_authors"])
            return base

        # 预检凭证
        try:
            self._get_auth_for_credential(None)
        except RuntimeError as e:
            logger.error("Substack 凭证不可用: %s", e)
            if report:
                report.record_error(str(e))
            return _result(success=False, error=str(e))

        agg = self._aggregate_publications(subscription_id=subscription_id, user_id=user_id)
        user_ids_by_pub = self._user_ids_for_pub(agg)

        total_posts_new = 0
        total_posts_updated = 0
        total_notes_new = 0
        failed_pubs: List[str] = []
        failed_authors: List[str] = []
        pubs_ok = 0
        authors_ok = 0

        # ---- 文章轨 ----
        for idx, pub in enumerate(agg):
            pub_url = pub["publication_url"]
            pub_name = pub["publication_name"]
            user_ids = pub["user_ids"]
            meta = self._get_publication_meta(pub_url)
            if meta.get("publication_name"):
                pub_name = meta["publication_name"]

            try:
                cred_id, auth = self._get_auth_for_credential(meta.get("credential_id"))
            except RuntimeError as e:
                logger.error("Substack %s 凭证不可用，跳过: %s", pub_name, e)
                if report:
                    report.record_error(f"{pub_name} 凭证不可用: {e}")
                failed_pubs.append(pub_name or pub_url)
                continue
            except CookieExpiredError as e:
                logger.error("Substack %s 凭证过期 (cred=%s): %s", pub_name, cred_id, e)
                if report:
                    report.record_error(f"{pub_name} Cookie 已过期 (cred={cred_id}): {e}")
                failed_pubs.append(pub_name or pub_url)
                continue

            try:
                n_new, n_upd = self._crawl_posts_for_publication(
                    auth, pub, meta, force_fetch, user_ids, report, idx + 1, len(agg)
                )
                total_posts_new += n_new
                total_posts_updated += n_upd
                pubs_ok += 1
            except CookieExpiredError as e:
                logger.error("Substack %s 凭证过期 (cred=%s): %s", pub_name, cred_id, e)
                self._mark_credential_expired(cred_id, str(e))
                if report:
                    report.record_error(f"{pub_name} Cookie 已过期 (cred={cred_id}): {e}")
                failed_pubs.append(pub_name or pub_url)
            except Exception as exc:
                logger.exception("Substack %s 文章抓取失败", pub_url)
                if report:
                    report.record_error(f"{pub_name} 文章抓取失败: {exc}")
                failed_pubs.append(pub_name or pub_url)

            if idx < len(agg) - 1:
                time.sleep(PUB_INTERVAL)

        # ---- 动态轨 ----
        if CRAWL_NOTES:
            authors = self._list_enabled_followed_authors()
            for idx, author in enumerate(authors):
                uid = author["substack_user_id"]
                handle = author.get("author_handle") or str(uid)
                try:
                    cred_id, auth = self._get_auth_for_credential(author.get("credential_id"))
                except (RuntimeError, CookieExpiredError) as e:
                    logger.error("Substack @%s 凭证不可用，跳过: %s", handle, e)
                    if report:
                        report.record_error(f"@{handle} 凭证不可用: {e}")
                    failed_authors.append(handle)
                    continue

                try:
                    n_new = self._crawl_notes_for_author(
                        auth, author, user_ids_by_pub, report, idx + 1, len(authors)
                    )
                    total_notes_new += n_new
                    authors_ok += 1
                except CookieExpiredError as e:
                    logger.error("Substack @%s 凭证过期 (cred=%s): %s", handle, cred_id, e)
                    self._mark_credential_expired(cred_id, str(e))
                    if report:
                        report.record_error(f"@{handle} Cookie 已过期: {e}")
                    failed_authors.append(handle)
                except Exception as exc:
                    logger.exception("Substack @%s 动态抓取失败", handle)
                    if report:
                        report.record_error(f"@{handle} 动态抓取失败: {exc}")
                    failed_authors.append(handle)

                if idx < len(authors) - 1:
                    time.sleep(AUTHOR_INTERVAL)
        else:
            logger.info("Substack 动态轨已关闭 (SUBSTACK_CRAWL_NOTES=false)")

        # 统一清理临时 cookies 文件
        self._cleanup_auth_cache()

        # 统一回写 subscription_list 状态 + history (P1-1: 原本 substack 只写 substack_publications).
        try:
            from backend.services.subscription_status_service import record_fetch_outcome_for_channel
            record_fetch_outcome_for_channel(
                'substack', status='success',
                items_new=total_posts_new + total_notes_new,
            )
        except Exception:
            pass

        return _result(
            success=True,
            publications_processed=pubs_ok,
            posts_new=total_posts_new,
            posts_updated=total_posts_updated,
            notes_new=total_notes_new,
            authors_processed=authors_ok,
            failed_publications=failed_pubs,
            failed_authors=failed_authors,
        )
