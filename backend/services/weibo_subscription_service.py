# -*- coding: utf-8 -*-
"""
微博订阅抓取服务

核心职责：
  1. 从 weibo_credentials 读取有效 Cookie（Fernet 解密）
  2. 按 uid 聚合所有 enabled weibo 订阅及 user_ids 并集
  3. 调用 crawl4weibo 拉取博文（首次 backfill + 后续 incremental，page 翻页）
  4. 通过 WeiboAdapter → SubscriptionContentService.save_content 写入 subscription_content
  5. 更新 weibo_accounts 表中的同步锚点
  6. crawl4weibo 抛 AuthenticationError 时包装为 WeiboCookieExpiredError，标记凭证过期，
     不让调用方崩溃

架构对齐：zsxq_subscription_service.py
关键差异（基于 Step 0 验证）：
  - 抓取客户端为 crawl4weibo.WeiboClient（page=1,2,3 翻页，非 end_time 游标）
  - 构造必须 use_browser_cookies=False, auto_fetch_cookies=False, use_proxy=False
  - Cookie 失效信号 = crawl4weibo.exceptions.AuthenticationError（精确，非启发式）
  - Post.id 为 str、created_at 已是 datetime、转发为 retweeted_status: Optional[Post]
  - backfill 每次 page=1 起翻到 start_bound（不支持 end_time 游标续抓；幂等靠 ON CONFLICT）
"""

import logging
import random
import time
from pathlib import Path
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
import psycopg2.extras

from backend.utils.database_config import get_database_config
from backend.utils.env_loader import load_project_env
from backend.utils.datetime_utils import utcnow
from backend.services.subscription_content_service import SubscriptionContentService

load_project_env()

logger = logging.getLogger(__name__)

# ---------- crawl4weibo 依赖（稳健导入） ----------

try:
    from crawl4weibo import WeiboClient as _C4WClient
    try:
        from crawl4weibo.exceptions import (  # type: ignore
            AuthenticationError as _C4WAuthError,
            RateLimitError as _C4WRateLimit,
            NetworkError as _C4WNetwork,
        )
    except ImportError:
        from crawl4weibo.exceptions.base import (  # type: ignore
            AuthenticationError as _C4WAuthError,
            RateLimitError as _C4WRateLimit,
            NetworkError as _C4WNetwork,
        )
    _CRAWL4WEIBO_AVAILABLE = True
except ImportError:
    _CRAWL4WEIBO_AVAILABLE = False

    class _C4WAuthError(Exception):  # type: ignore[no-redef]
        pass

    class _C4WRateLimit(Exception):  # type: ignore[no-redef]
        pass

    class _C4WNetwork(Exception):  # type: ignore[no-redef]
        pass


# ---------- 常量（微博风控较严，比 zsxq 保守） ----------

COUNT = 20                 # crawl4weibo get_user_posts 每页约 20 条
BACKFILL_DAYS = 1          # 首次只补 1 天
BACKFILL_MAX_PAGES = 10    # backfill 最多翻 10 页（~200 条，覆盖活跃博主 1 天）
INCREMENT_MAX_PAGES = 3    # 增量最多翻 3 页
PAGE_INTERVAL = 8.0        # 每页间隔 8s
PAGES_PER_BATCH = 8        # 每 8 页长休眠
LONG_SLEEP_MIN = 200.0
LONG_SLEEP_MAX = 400.0
ACCOUNT_INTERVAL = 20      # 博主间隔 20s

# 访客模式 Cookie 持久化路径（无登录凭证时 crawl4weibo 用 Playwright 自动获取访客 Cookie）
_VISITOR_COOKIE_PATH = str(Path.home() / ".crawl4weibo" / "weibo_visitor_state.json")


# ---------- 异常 ----------

class WeiboCookieExpiredError(Exception):
    """crawl4weibo Cookie 失效（AuthenticationError 包装）。"""


@dataclass
class WeiboRunReport:
    """单次抓取运行的错误与统计汇总（供 Scheduler 完成后推送 Lark）。"""

    started_at: datetime = field(default_factory=utcnow)
    api_retry_count: int = 0
    api_retry_samples: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    account_summaries: List[Dict[str, Any]] = field(default_factory=list)
    failed_accounts: List[str] = field(default_factory=list)

    def record_api_retry(self, account_label: str, message: str) -> None:
        self.api_retry_count += 1
        sample = f"{account_label}: {message[:160]}"
        if sample not in self.api_retry_samples and len(self.api_retry_samples) < 5:
            self.api_retry_samples.append(sample)

    def record_error(self, message: str) -> None:
        if message and message not in self.errors:
            self.errors.append(message)

    def record_warning(self, message: str) -> None:
        if message and message not in self.warnings:
            self.warnings.append(message)

    def add_account_summary(self, name: str, mode: str, new_count: int) -> None:
        self.account_summaries.append({"name": name, "mode": mode, "new": new_count})

    def to_dict(self) -> Dict[str, Any]:
        finished = utcnow()
        return {
            "api_retry_count": self.api_retry_count,
            "api_retry_samples": self.api_retry_samples,
            "errors": self.errors,
            "warnings": self.warnings,
            "account_summaries": self.account_summaries,
            "failed_accounts": self.failed_accounts,
            "finished_at": finished.strftime("%Y-%m-%d %H:%M:%S UTC"),
            "duration_sec": (finished - self.started_at).total_seconds(),
        }


# ---------- 工具 ----------

def _get_conn():
    cfg = get_database_config()
    return psycopg2.connect(
        host=cfg["db_host"],
        port=cfg["db_port"],
        dbname=cfg["db_name"],
        user=cfg["db_user"],
        password=cfg["db_password"],
    )


def _to_utc(dt: Any) -> Optional[datetime]:
    """确保 datetime 带时区；naive 视作 UTC。"""
    if not isinstance(dt, datetime):
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _to_int(value: Any) -> Optional[int]:
    """post_id 安全转 int（weibo_accounts.recent_post_ids 为 BIGINT[]）。"""
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _page_sleep(page: int) -> None:
    time.sleep(PAGE_INTERVAL)
    if page > 0 and page % PAGES_PER_BATCH == 0:
        long = random.uniform(LONG_SLEEP_MIN, LONG_SLEEP_MAX)
        logger.info("微博长休眠 %.0fs", long)
        time.sleep(long)


# ---------- crawl4weibo 客户端包装 ----------

class WeiboClientWrapper:
    """包 crawl4weibo.WeiboClient：把 AuthenticationError 翻译成 WeiboCookieExpiredError。
    关键：禁用浏览器/自动取 Cookie/代理（凭证由 weibo_credentials 提供）。
    """

    def __init__(self, cookie: Optional[str], uid: str, *, report: Optional[WeiboRunReport] = None,
                 account_label: str = ""):
        if not _CRAWL4WEIBO_AVAILABLE:
            raise RuntimeError("crawl4weibo 未安装，请 pip install crawl4weibo")
        self.uid = str(uid)
        self.report = report
        self.account_label = account_label or str(uid)
        try:
            if cookie:
                # 登录态 Cookie（来自 weibo_credentials 或 WEIBO_COOKIE env）
                self._client = _C4WClient(
                    cookies=cookie,
                    use_browser_cookies=False,
                    auto_fetch_cookies=False,
                    log_level="WARNING",
                )
            else:
                # 访客模式：crawl4weibo 用 Playwright 自动获取访客 Cookie 并持久化
                # （微博已加强反爬，纯 requests 不够；需 `playwright install chromium`）
                # 对齐参考代码 scripts/参考代码/weibo_script/verify.py 的 fallback
                self._client = _C4WClient(
                    use_browser_cookies=True,
                    auto_fetch_cookies=True,
                    cookie_storage_path=_VISITOR_COOKIE_PATH,
                    browser_headless=True,
                    log_level="WARNING",
                )
        except _C4WAuthError as e:
            raise WeiboCookieExpiredError(f"初始化即认证失败: {e}") from e

    def get_user_posts(self, uid: Optional[str] = None, page: int = 1) -> List[Any]:
        target_uid = str(uid) if uid else self.uid
        try:
            return self._client.get_user_posts(
                target_uid, page=page, expand=True, use_proxy=False
            ) or []
        except _C4WAuthError as e:
            raise WeiboCookieExpiredError(f"拉取认证失败 (page={page}): {e}") from e
        except _C4WRateLimit as e:
            if self.report:
                self.report.record_api_retry(self.account_label, f"RateLimit: {e}")
            logger.warning("微博 %s 触发限流 (page=%d): %s", self.account_label, page, e)
            raise
        except _C4WNetwork as e:
            logger.warning("微博 %s 网络错误 (page=%d): %s", self.account_label, page, e)
            raise


# ---------- 帖子解析（Post → adapter 输入 dict） ----------

def _extract_post(post: Any, uid: str, screen_name: str, avatar: str = "") -> Dict[str, Any]:
    """crawl4weibo Post 对象 → WeiboAdapter.normalize 的 raw_data dict。"""
    rt = getattr(post, "retweeted_status", None)
    return {
        "post_id": getattr(post, "id", None),
        "bid": getattr(post, "bid", "") or "",
        "uid": str(uid),
        "screen_name": screen_name or "",
        "avatar": avatar or "",
        "text": getattr(post, "text", "") or "",
        "created_at": getattr(post, "created_at", None),
        "source": getattr(post, "source", "") or "",
        "pic_urls": list(getattr(post, "pic_urls", None) or []),
        "video_url": getattr(post, "video_url", "") or "",
        "is_original": bool(getattr(post, "is_original", True)),
        "reposts_count": int(getattr(post, "reposts_count", 0) or 0),
        "comments_count": int(getattr(post, "comments_count", 0) or 0),
        "attitudes_count": int(getattr(post, "attitudes_count", 0) or 0),
        "retweet_uid": getattr(rt, "user_id", None) if rt else None,
        "retweet_bid": getattr(rt, "bid", None) if rt else None,
        "retweet_text": getattr(rt, "text", None) if rt else None,
    }


# ---------- 翻页逻辑 ----------

def _iter_backfill_pages(client: WeiboClientWrapper, uid: str, *, backfill_days: int = BACKFILL_DAYS):
    """历史补全：从 page=1 往旧翻，直到 created_at < start_bound / 空页 / 全重复 / max_pages。
    注意：不依赖「每页条数」判断末页（微博 mobile API 每页约 10 条且波动，COUNT 不可靠），
    改用「该页全部已在本批出现过」判定翻到尽头。
    yield (page_num, batch_posts, head_post_ids_or_None)
    """
    start_bound = utcnow() - timedelta(days=backfill_days)
    page = 0
    running_total = 0
    seen_ids: set = set()

    while page < BACKFILL_MAX_PAGES:
        page += 1
        posts = client.get_user_posts(uid=uid, page=page)
        if not posts:
            break

        head_post_ids: Optional[List[int]] = None
        if page == 1:
            head_post_ids = [
                pid for pid in (_to_int(getattr(p, "id", None)) for p in posts) if pid is not None
            ]

        stopped = False
        batch: List[Any] = []
        for p in posts:
            pid = str(getattr(p, "id", ""))
            ca = _to_utc(getattr(p, "created_at", None))
            if ca is not None and ca < start_bound:
                stopped = True
                continue
            if pid in seen_ids:
                continue
            seen_ids.add(pid)
            batch.append(p)

        running_total += len(batch)
        logger.info(
            "微博 backfill %s 第 %d 页 +%d 条，累计 %d 条",
            uid, page, len(batch), running_total,
        )
        yield page, batch, head_post_ids

        if stopped:
            break
        if not batch:
            # 该页全部已在本批出现过（或被边界过滤）→ 已翻到尽头
            logger.info("微博 backfill %s 第 %d 页无新内容，停止翻页", uid, page)
            break
        _page_sleep(page)


def _crawl_incremental(
    client: WeiboClientWrapper,
    uid: str,
    known_ids: List[int],
) -> Tuple[List[Any], List[int]]:
    """增量：page=1 起翻，遇全部已知则停。返回 (posts, head_ids)。
    去重靠 known_ids（recent_post_ids）+ 入库时 ON CONFLICT 兜底。
    """
    known = {str(k) for k in (known_ids or [])}
    all_posts: List[Any] = []
    head_ids: List[int] = []
    max_pages = 1 if not known else INCREMENT_MAX_PAGES
    page = 0

    while page < max_pages:
        page += 1
        posts = client.get_user_posts(uid=uid, page=page)
        if not posts:
            break

        page_ids = [str(getattr(p, "id", "")) for p in posts]
        if page == 1:
            head_ids = [
                pid for pid in (_to_int(pid_str) for pid_str in page_ids) if pid is not None
            ]

        all_posts.extend(posts)

        new_count = sum(1 for pid in page_ids if pid and pid not in known)
        logger.info("微博增量 %s 第 %d 页 %d 条（新 %d）", uid, page, len(posts), new_count)

        if known and page_ids and all(pid in known for pid in page_ids):
            break
        _page_sleep(page)

    return all_posts, head_ids


# ---------- 同步状态 ----------

def _save_account_state(
    conn,
    uid: str,
    posts: List[Any],
    mode: str,
    head_ids: Optional[List[int]] = None,
    backfill_complete: Optional[bool] = None,
) -> None:
    times = [t for t in (_to_utc(getattr(p, "created_at", None)) for p in posts) if t]
    last_sync_at = utcnow().isoformat()
    if backfill_complete is None:
        backfill_complete = mode == "backfill"

    with conn.cursor() as cur:
        if times:
            batch_min, batch_max = min(times).isoformat(), max(times).isoformat()
            cur.execute(
                """
                UPDATE weibo_accounts SET
                    last_sync_at     = %s,
                    last_sync_mode   = %s,
                    min_create_time  = LEAST(COALESCE(min_create_time, %s::timestamptz), %s::timestamptz),
                    max_create_time  = GREATEST(COALESCE(max_create_time, %s::timestamptz), %s::timestamptz),
                    recent_post_ids  = COALESCE(%s, recent_post_ids),
                    backfill_complete = CASE WHEN %s THEN true ELSE backfill_complete END,
                    updated_at       = now()
                WHERE uid = %s
                """,
                (
                    last_sync_at, mode,
                    batch_min, batch_min, batch_max, batch_max,
                    head_ids, backfill_complete, str(uid),
                ),
            )
        else:
            cur.execute(
                """
                UPDATE weibo_accounts SET
                    last_sync_at     = %s,
                    last_sync_mode   = %s,
                    recent_post_ids  = COALESCE(%s, recent_post_ids),
                    backfill_complete = CASE WHEN %s THEN true ELSE backfill_complete END,
                    updated_at       = now()
                WHERE uid = %s
                """,
                (last_sync_at, mode, head_ids, backfill_complete, str(uid)),
            )
    conn.commit()


def _store_post_batch(
    content_service: SubscriptionContentService,
    batch: List[Any],
    uid: str,
    screen_name: str,
    avatar: str,
    user_ids: List[int],
    report: Optional[WeiboRunReport] = None,
) -> Tuple[int, int, List[int]]:
    """入库一批博文，返回 (new_count, updated_count, new_content_ids)。"""
    new_count = 0
    updated_count = 0
    new_ids: List[int] = []
    for post in batch:
        raw = _extract_post(post, uid, screen_name, avatar)
        post_id = raw.get("post_id")
        try:
            content_db_id, is_new = content_service.save_content(
                channel_type="weibo",
                raw_data=raw,
                user_ids=user_ids,
            )
            if is_new:
                new_count += 1
                new_ids.append(content_db_id)
            else:
                updated_count += 1
        except Exception as exc:
            logger.exception("微博博文 %s 入库失败", post_id)
            if report:
                report.record_error(f"{screen_name} 博文 {post_id} 入库失败: {exc}")
    return new_count, updated_count, new_ids


# ---------- 主服务 ----------

class WeiboSubscriptionService:
    """微博订阅抓取服务。

    调用入口：fetch_and_store_posts(subscription_id?, user_id?, force_fetch?)
    """

    def __init__(self):
        self.content_service = SubscriptionContentService()

    def _get_active_credential(self) -> Optional[Tuple[int, str]]:
        """获取首个有效凭证的 (id, cookie)，无则返回 None（调用方回退到访客模式）。"""
        from backend.services.weibo_credential_service import WeiboCredentialService
        return WeiboCredentialService().get_first_valid_cookie()

    def _get_credential_for_account(self, meta: Dict[str, Any]) -> Optional[Tuple[int, str]]:
        from backend.services.weibo_credential_service import WeiboCredentialService
        svc = WeiboCredentialService()
        cred_id = meta.get("credential_id")
        if cred_id:
            cookie = svc.get_decrypted_cookie(cred_id)
            if cookie:
                return cred_id, cookie
            logger.warning("微博博主 %s credential_id=%s 凭证不可用，回退到首个有效凭证",
                           meta.get("uid"), cred_id)
        return self._get_active_credential()

    def _mark_credential_expired(self, cred_id: int, error: str) -> None:
        from backend.services.weibo_credential_service import WeiboCredentialService
        WeiboCredentialService().mark_expired(cred_id, error)

    def _aggregate_accounts(
        self,
        subscription_id: Optional[int] = None,
        user_id: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """按 uid 聚合 enabled weibo 订阅的 user_ids 并集。"""
        with _get_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                sql = """
                    SELECT
                        sl.channel_identifier AS uid,
                        sl.channel_name AS screen_name,
                        ARRAY_AGG(DISTINCT sl.user_id) AS user_ids
                    FROM subscription_list sl
                    WHERE sl.channel_type = 'weibo'
                      AND sl.enabled = true
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

        return [
            {
                "uid": row["uid"],
                "screen_name": row["screen_name"] or row["uid"],
                "user_ids": list(row["user_ids"] or []),
            }
            for row in rows
        ]

    def _get_account_meta(self, uid: str) -> Dict[str, Any]:
        from backend.services.weibo_credential_service import WeiboCredentialService
        return WeiboCredentialService().get_account_meta(uid)

    def fetch_and_store_posts(
        self,
        subscription_id: Optional[int] = None,
        user_id: Optional[int] = None,
        force_fetch: bool = False,
        *,
        collect_report: bool = False,
    ) -> Dict[str, Any]:
        """主入口：抓取所有（或指定）微博订阅的最新博文，写入 subscription_content。"""
        report = WeiboRunReport() if collect_report else None

        def _result(**kwargs: Any) -> Dict[str, Any]:
            base = {
                "success": kwargs.pop("success", True),
                "accounts_processed": kwargs.pop("accounts_processed", 0),
                "posts_new": kwargs.pop("posts_new", 0),
                "posts_updated": kwargs.pop("posts_updated", 0),
                "failed_accounts": kwargs.pop("failed_accounts", []),
            }
            base.update(kwargs)
            if report:
                base["report"] = report.to_dict()
                if base["failed_accounts"]:
                    report.failed_accounts = list(base["failed_accounts"])
            return base

        # 预检凭证（可选：无登录凭证则所有博主走访客模式 —— 需 playwright + chromium）
        active_cred = self._get_active_credential()
        if active_cred:
            logger.info("微博抓取使用登录态凭证 (credential_id=%s)", active_cred[0])
        else:
            logger.warning("微博无可用登录凭证，回退到访客模式（需 playwright + chromium；风控更严，建议在 /api/admin/weibo 录入 Cookie）")
            if report:
                report.record_warning("无登录态凭证，使用访客模式")
        visitor_client: Optional[WeiboClientWrapper] = None

        accounts = self._aggregate_accounts(subscription_id=subscription_id, user_id=user_id)
        if not accounts:
            logger.info("无 enabled 微博订阅，跳过")
            return _result(success=True)

        total_new = 0
        total_updated = 0
        failed_accounts: List[str] = []

        for idx, acct in enumerate(accounts):
            uid = str(acct["uid"])
            screen_name = acct["screen_name"]
            user_ids = acct["user_ids"]

            meta = self._get_account_meta(uid)
            avatar = meta.get("avatar", "") or ""

            try:
                cred_result = self._get_credential_for_account(meta)
                if cred_result:
                    cred_id, cookie = cred_result
                else:
                    cred_id, cookie = None, None
            except Exception as e:
                logger.error("微博 %s (%s) 凭证获取异常，跳过: %s", screen_name, uid, e)
                if report:
                    report.record_error(f"{screen_name} 凭证异常: {e}")
                failed_accounts.append(screen_name or uid)
                continue

            try:
                if cookie:
                    client = WeiboClientWrapper(cookie, uid, report=report, account_label=screen_name)
                else:
                    # 访客模式：所有博主共享同一个 visitor client（避免每个博主重复启动浏览器取 Cookie）
                    if visitor_client is None:
                        visitor_client = WeiboClientWrapper(None, uid, report=report, account_label="visitor")
                    client = visitor_client
                backfill_done = meta.get("backfill_complete", False)
                known_ids: List[int] = [
                    i for i in (_to_int(x) for x in (meta.get("recent_post_ids") or []))
                    if i is not None
                ]

                if not backfill_done or force_fetch:
                    mode = "backfill"
                    logger.info("[%d/%d] 微博 %s (%s) 开始 backfill", idx + 1, len(accounts), screen_name, uid)
                    head_ids: Optional[List[int]] = None
                    group_new = 0
                    backfill_page_err: Optional[str] = None
                    try:
                        for _page, batch, page_head_ids in _iter_backfill_pages(client, uid):
                            if page_head_ids:
                                head_ids = page_head_ids
                            batch_new, batch_updated, new_ids = _store_post_batch(
                                self.content_service, batch, uid, screen_name, avatar, user_ids, report=report,
                            )
                            group_new += batch_new
                            total_new += batch_new
                            total_updated += batch_updated
                            if new_ids:
                                try:
                                    from backend.tasks.file_tasks import trigger_auto_feed_tagging
                                    trigger_auto_feed_tagging(new_ids)
                                except Exception as tag_exc:
                                    logger.warning("微博触发打标失败，将在下次 task 中补全")
                                    if report:
                                        report.record_warning(f"{screen_name} 触发打标失败: {tag_exc}")
                            if batch:
                                with _get_conn() as conn:
                                    _save_account_state(conn, uid, batch, mode, head_ids=page_head_ids, backfill_complete=False)
                    except WeiboCookieExpiredError:
                        raise
                    except Exception as page_exc:
                        backfill_page_err = str(page_exc)
                        logger.warning("微博 %s backfill 中途失败（已抓 %d 条），下次续抓: %s",
                                       screen_name, group_new, page_exc)
                        if report:
                            report.record_warning(f"{screen_name} backfill 中途失败: {page_exc}")

                    if group_new > 0 or backfill_page_err is None:
                        with _get_conn() as conn:
                            _save_account_state(
                                conn, uid, [], mode,
                                head_ids=head_ids,
                                backfill_complete=(backfill_page_err is None),
                            )
                    logger.info("微博 %s 完成 [%s] 新 %d 条%s", screen_name, mode, group_new,
                                "（部分完成，待续抓）" if backfill_page_err else "")
                    if report:
                        report.add_account_summary(screen_name, mode, group_new)
                    continue
                else:
                    mode = "incremental"
                    logger.info("[%d/%d] 微博 %s (%s) 增量，锚点 %d 个",
                                idx + 1, len(accounts), screen_name, uid, len(known_ids))
                    posts, head_ids = _crawl_incremental(client, uid, known_ids)

                batch_new, batch_updated, new_ids = _store_post_batch(
                    self.content_service, posts, uid, screen_name, avatar, user_ids, report=report,
                )
                total_new += batch_new
                total_updated += batch_updated

                if new_ids:
                    try:
                        from backend.tasks.file_tasks import trigger_auto_feed_tagging
                        trigger_auto_feed_tagging(new_ids)
                    except Exception as tag_exc:
                        logger.warning("微博触发打标失败，将在下次 task 中补全")
                        if report:
                            report.record_warning(f"{screen_name} 触发打标失败: {tag_exc}")

                with _get_conn() as conn:
                    _save_account_state(conn, uid, posts, mode, head_ids=head_ids)

                logger.info("微博 %s 完成 [%s] 新 %d 条", screen_name, mode, batch_new)
                if report:
                    report.add_account_summary(screen_name, mode, batch_new)

            except WeiboCookieExpiredError as e:
                logger.error("微博 %s 凭证过期 (credential_id=%s): %s", screen_name, cred_id, e)
                self._mark_credential_expired(cred_id, str(e))
                if report:
                    report.record_error(f"{screen_name} Cookie 已过期 (credential_id={cred_id}): {e}")
                failed_accounts.append(screen_name or uid)
                # 不 break：跳过同凭证后续博主，继续处理其他凭证的博主

            except Exception as exc:
                logger.exception("微博 %s (%s) 抓取失败", screen_name, uid)
                if report:
                    report.record_error(f"{screen_name or uid} 抓取失败: {exc}")
                failed_accounts.append(screen_name or uid)

            if idx < len(accounts) - 1:
                time.sleep(ACCOUNT_INTERVAL)

        return _result(
            success=True,
            accounts_processed=len(accounts) - len(failed_accounts),
            posts_new=total_new,
            posts_updated=total_updated,
            failed_accounts=failed_accounts,
        )
