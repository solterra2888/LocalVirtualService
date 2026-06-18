# -*- coding: utf-8 -*-
"""
知识星球订阅抓取服务

核心职责：
  1. 从 zsxq_credentials 读取有效 Cookie（Fernet 解密）
  2. 按 group_id 聚合所有 enabled zhishixingqiu 订阅及 user_ids 并集
  3. 调用知识星球 API 拉取话题（首次 backfill + 后续 incremental）
  4. 通过 ZsxqAdapter → SubscriptionContentService.save_content 写入 subscription_content
  5. 更新 zsxq_groups 表中的同步锚点
  6. 401 时抛 CookieExpiredError 并标记凭证过期，不让调用方崩溃

架构对齐：twitter_subscription_service.py（聚合 author/user_ids，逐 group 顺序抓取）
"""

import logging
import random
import re
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote

import psycopg2
import psycopg2.extras
import requests

from backend.utils.database_config import get_database_config
from backend.utils.env_loader import load_project_env
from backend.utils.datetime_utils import utcnow
from backend.services.subscription_content_service import SubscriptionContentService

load_project_env()

logger = logging.getLogger(__name__)

# ---------- 常量（对齐 scripts/zsxq/my_zsxq.py） ----------
COUNT = 15
BACKFILL_DAYS = 1
INCREMENT_MAX_PAGES = 5
PAGE_INTERVAL = 5.0
PAGES_PER_BATCH = 15
LONG_SLEEP_MIN = 180.0
LONG_SLEEP_MAX = 300.0
GROUP_INTERVAL = 15
API_VERSION = "2.89.0"


# ---------- 异常 ----------

class CookieExpiredError(Exception):
    """凭证 Cookie 已失效（HTTP 401）。"""


@dataclass
class ZsxqRunReport:
    """单次抓取运行的错误与统计汇总（供 Scheduler 完成后推送 Lark）。"""

    started_at: datetime = field(default_factory=utcnow)
    api_retry_count: int = 0
    api_retry_samples: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    group_summaries: List[Dict[str, Any]] = field(default_factory=list)
    failed_groups: List[str] = field(default_factory=list)

    def record_api_retry(self, group_label: str, message: str) -> None:
        self.api_retry_count += 1
        sample = f"{group_label}: {message[:160]}"
        if sample not in self.api_retry_samples and len(self.api_retry_samples) < 5:
            self.api_retry_samples.append(sample)

    def record_error(self, message: str) -> None:
        if message and message not in self.errors:
            self.errors.append(message)

    def record_warning(self, message: str) -> None:
        if message and message not in self.warnings:
            self.warnings.append(message)

    def add_group_summary(self, name: str, mode: str, new_count: int) -> None:
        self.group_summaries.append({"name": name, "mode": mode, "new": new_count})

    def to_dict(self) -> Dict[str, Any]:
        finished = utcnow()
        return {
            "api_retry_count": self.api_retry_count,
            "api_retry_samples": self.api_retry_samples,
            "errors": self.errors,
            "warnings": self.warnings,
            "group_summaries": self.group_summaries,
            "failed_groups": self.failed_groups,
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


# ---------- 时间工具 ----------

def _parse_zsxq_time(value: str) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("+0800", "+08:00"))
    except (ValueError, TypeError):
        return None


def _dt_to_zsxq_time(value: Any) -> str:
    """将 DB timestamptz / datetime 转为知识星球 API 的 create_time 字符串。"""
    if isinstance(value, str):
        return value
    dt = value
    if not isinstance(dt, datetime):
        return ""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    try:
        from zoneinfo import ZoneInfo

        dt = dt.astimezone(ZoneInfo("Asia/Shanghai"))
    except Exception:
        pass
    ms = int(dt.microsecond / 1000)
    return dt.strftime("%Y-%m-%dT%H:%M:%S") + f".{ms:03d}+0800"


def _calc_next_end_time(create_time: str) -> str:
    """计算翻页 end_time（与 scripts/zsxq/my_zsxq.py 保持一致，避免 API 报错）。"""
    if not create_time or len(create_time) < 23:
        return create_time
    if create_time[20:23] == "000":
        end_time = create_time[:20] + "999" + create_time[23:]
        dt = datetime.strptime(end_time[:19], "%Y-%m-%dT%H:%M:%S") - timedelta(seconds=1)
        end_time = dt.strftime("%Y-%m-%dT%H:%M:%S") + end_time[19:]
    else:
        ms = int(create_time[20:23]) - 1
        end_time = create_time[:20] + str(ms).zfill(3) + create_time[23:]
    end_time = quote(end_time)
    if len(end_time) == 33:
        end_time = end_time[:24] + "0" + end_time[24:]
    return end_time


def _strip_html_tags(text: str) -> str:
    if not text:
        return ""
    text = re.sub(r"<e[^>]*>", "", text)
    text = re.sub(r"</e>", "", text)
    return text.strip()


# ---------- ZsxqClient ----------

class ZsxqClient:
    """知识星球 API 客户端（从凭证服务获取 Cookie）。"""

    def __init__(
        self,
        cookie: str,
        group_id: str,
        api_version: str = API_VERSION,
        *,
        report: Optional[ZsxqRunReport] = None,
        group_label: str = "",
    ):
        self.cookie = cookie
        self.group_id = group_id
        self.api_version = api_version
        self.report = report
        self.group_label = group_label or group_id
        self.session = requests.Session()
        self.base_url = "https://api.zsxq.com"
        self._topics_api: Optional[str] = None

    def _headers(self) -> Dict[str, str]:
        return {
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "zh-CN,zh;q=0.9",
            "Cache-Control": "no-cache",
            "Cookie": self.cookie,
            "Origin": "https://wx.zsxq.com",
            "Pragma": "no-cache",
            "Referer": f"https://wx.zsxq.com/dweb2/index/group/{self.group_id}",
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/131.0.0.0 Safari/537.36"
            ),
            "X-Request-Id": str(uuid.uuid4()),
            "X-Timestamp": str(int(time.time())),
            "X-Version": self.api_version,
        }

    def fetch_topics_page(
        self,
        count: int = COUNT,
        end_time: Optional[str] = None,
        max_retries: int = 8,
    ) -> Dict[str, Any]:
        """拉取话题列表一页，遇 401 抛 CookieExpiredError。"""
        count = min(max(1, count), 30)

        def make_endpoints(et: Optional[str]):
            v1q = f"count={count}"
            v2q = f"scope=all&count={count}"
            if et:
                v1q += f"&end_time={et}"
                v2q += f"&end_time={et}"
            eps = [
                (f"{self.base_url}/v2/groups/{self.group_id}/topics", v2q, "v2"),
                (f"{self.base_url}/v1.10/groups/{self.group_id}/topics", v1q, "v1.10"),
            ]
            if self._topics_api:
                eps = [e for e in eps if e[2] == self._topics_api]
            return eps

        last_err = ""
        retry_end_time = end_time

        for attempt in range(max_retries):
            need_retry = False
            for path, query, label in make_endpoints(retry_end_time):
                try:
                    resp = self.session.get(f"{path}?{query}", headers=self._headers(), timeout=30)
                    if resp.status_code == 401:
                        raise CookieExpiredError("Cookie 无效或已过期 (HTTP 401)")
                    if resp.status_code != 200:
                        last_err = f"[{label}] HTTP {resp.status_code}"
                        continue
                    data = resp.json()
                    if data.get("succeeded"):
                        self._topics_api = label
                        return data
                    code = data.get("code")
                    last_err = data.get("info") or data.get("resp_err") or str(data)
                    err_str = str(last_err)
                    # code==2「版本太旧」或「非官方工具」提示：不直接报错，走退避重试。
                    # 同时解除接口锁定，下次两个接口都能尝试。
                    if code == 2 or "非官方" in err_str or "版本太旧" in err_str:
                        need_retry = True
                        last_err = f"[{label}] {last_err}"
                        self._topics_api = None
                        break
                    if code == 1059 or "内部" in err_str:
                        need_retry = True
                        last_err = f"[{label}] {last_err}"
                        break
                    last_err = f"[{label}] {last_err}"
                except CookieExpiredError:
                    raise
                except requests.RequestException as e:
                    last_err = f"[{label}] {e}"

            if not need_retry:
                break
            wait = min(2 + attempt * 2, 15)
            logger.warning("ZSXQ API 重试 (%ds): %s", wait, last_err)
            if self.report:
                self.report.record_api_retry(self.group_label, last_err)
            time.sleep(wait)

        if self.report:
            self.report.record_error(f"{self.group_label} 获取话题失败: {last_err}")
        raise RuntimeError(f"获取话题失败: {last_err}")


# ---------- 话题解析 ----------

def _extract_content(topic: Dict[str, Any]) -> Dict[str, Any]:
    """从 API 原始 topic 提取有效字段（对齐 scripts/zsxq/my_zsxq.py extract_content）。"""
    content = (
        topic.get("talk")
        or topic.get("question")
        or topic.get("task")
        or topic.get("solution")
        or {}
    )
    owner = content.get("owner") or {}
    author = "匿名" if content.get("anonymous") else owner.get("name", "")

    images = []
    for img in content.get("images") or []:
        large = img.get("large") or img.get("original") or {}
        url = large.get("url") or img.get("url")
        if url:
            images.append(url)

    talk = topic.get("talk") or {}
    files_raw = talk.get("files") or content.get("files") or []
    files = [f.get("name", "") for f in files_raw if f.get("name")]

    return {
        "topic_id": topic.get("topic_id"),
        "create_time": topic.get("create_time", ""),
        "type": topic.get("type", "talk"),
        "title": _strip_html_tags(topic.get("title") or ""),
        "author": author,
        "text": _strip_html_tags(content.get("text", "")),
        "images": images,
        "files": files,
    }


# ---------- 翻页逻辑 ----------

def _iter_backfill_pages(
    client: ZsxqClient,
    backfill_days: int = BACKFILL_DAYS,
    resume_from_min_time: Optional[str] = None,
):
    """
    历史补全分页迭代器：从今天往前拉 backfill_days 天的话题。

    每页 yield (page_num, batch_topics, first_page_ids_or_none)。
    resume_from_min_time 用于断点续抓（从已入库最早时间继续往前翻页）。
    """
    today = datetime.now().date()
    start_bound = (today - timedelta(days=backfill_days - 1)).isoformat() + "T00:00:00.000+0800"
    end_bound = today.isoformat() + "T23:59:59.999+0800"

    if resume_from_min_time:
        page_end_time: Optional[str] = _calc_next_end_time(resume_from_min_time)
        logger.info("ZSXQ backfill 断点续抓，end_time 锚点 %s", resume_from_min_time[:19])
    else:
        # 首页不传合成的 end_time，与增量拉取保持一致，避免触发 API 非官方工具校验。
        # 客户端日期过滤（start_bound / end_bound）保留，补全范围不变。
        page_end_time = None

    page = 0
    running_total = 0

    while True:
        page += 1
        data = client.fetch_topics_page(count=COUNT, end_time=page_end_time)
        topics = data.get("resp_data", {}).get("topics") or []
        if not topics:
            break

        first_page_ids: Optional[List[int]] = None
        if page == 1 and not resume_from_min_time:
            first_page_ids = [
                raw["topic_id"] for raw in topics if raw.get("topic_id") is not None
            ]

        stopped = False
        batch: List[Dict[str, Any]] = []
        for raw in topics:
            ct = raw.get("create_time", "")
            if ct and ct < start_bound:
                stopped = True
                continue
            if ct <= end_bound:
                batch.append(_extract_content(raw))

        running_total += len(batch)
        logger.info(
            "ZSXQ backfill 第 %d 页 +%d 条，累计 %d 条",
            page,
            len(batch),
            running_total,
        )
        yield page, batch, first_page_ids

        if stopped or len(topics) < COUNT:
            break

        page_end_time = _calc_next_end_time(topics[-1].get("create_time", ""))
        _page_sleep(page)


def _crawl_incremental(
    client: ZsxqClient,
    known_ids: List[int],
) -> Tuple[List[Dict[str, Any]], List[int]]:
    """增量：从最新一页起拉，遇到全部已知则停。返回 (话题列表, 首页 id 列表)。"""
    known = set(known_ids)
    all_topics: List[Dict[str, Any]] = []
    head_ids: List[int] = []
    page_end_time: Optional[str] = None
    max_pages = 1 if not known else INCREMENT_MAX_PAGES
    page = 0

    while page < max_pages:
        page += 1
        data = client.fetch_topics_page(count=COUNT, end_time=page_end_time)
        topics = data.get("resp_data", {}).get("topics") or []
        if not topics:
            break

        page_ids = [raw["topic_id"] for raw in topics if raw.get("topic_id") is not None]
        if page == 1:
            head_ids = page_ids

        for raw in topics:
            all_topics.append(_extract_content(raw))

        new_count = sum(1 for tid in page_ids if tid not in known)
        logger.info("ZSXQ 增量第 %d 页 %d 条（新 %d）", page, len(topics), new_count)

        if known and all(tid in known for tid in page_ids):
            logger.debug("ZSXQ 增量第 %d 页全部已知，停止", page)
            break
        if len(topics) < COUNT:
            break

        page_end_time = _calc_next_end_time(topics[-1].get("create_time", ""))
        _page_sleep(page)

    return all_topics, head_ids


def _page_sleep(page: int) -> None:
    time.sleep(PAGE_INTERVAL)
    if page > 0 and page % PAGES_PER_BATCH == 0:
        long = random.uniform(LONG_SLEEP_MIN, LONG_SLEEP_MAX)
        logger.info("ZSXQ 长休眠 %.0fs", long)
        time.sleep(long)


# ---------- 同步状态 ----------

def _load_group_state(conn, group_id: str) -> Dict[str, Any]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            "SELECT * FROM zsxq_groups WHERE group_id = %s",
            (group_id,),
        )
        row = cur.fetchone()
    return dict(row) if row else {}


def _save_group_state(
    conn,
    group_id: str,
    topics: List[Dict[str, Any]],
    mode: str,
    head_ids: Optional[List[int]] = None,
    backfill_complete: Optional[bool] = None,
) -> None:
    times = [t["create_time"] for t in topics if t.get("create_time")]
    updates: Dict[str, Any] = {
        "last_sync_at": utcnow().isoformat(),
        "last_sync_mode": mode,
    }
    if times:
        batch_min, batch_max = min(times), max(times)
        # 使用 SQL 来 MIN/MAX 合并，避免覆盖旧值
        updates["new_min"] = batch_min
        updates["new_max"] = batch_max

    if head_ids:
        updates["recent_topic_ids"] = head_ids
    if backfill_complete is None:
        backfill_complete = mode == "backfill"
    updates["backfill_complete"] = backfill_complete

    with conn.cursor() as cur:
        if "new_min" in updates:
            cur.execute(
                """
                UPDATE zsxq_groups SET
                    last_sync_at     = %s,
                    last_sync_mode   = %s,
                    min_create_time  = LEAST(COALESCE(min_create_time, %s::timestamptz), %s::timestamptz),
                    max_create_time  = GREATEST(COALESCE(max_create_time, %s::timestamptz), %s::timestamptz),
                    recent_topic_ids = COALESCE(%s, recent_topic_ids),
                    backfill_complete = CASE WHEN %s THEN true ELSE backfill_complete END,
                    updated_at       = now()
                WHERE group_id = %s
                """,
                (
                    updates["last_sync_at"],
                    mode,
                    updates["new_min"],
                    updates["new_min"],
                    updates["new_max"],
                    updates["new_max"],
                    updates.get("recent_topic_ids"),
                    updates["backfill_complete"],
                    group_id,
                ),
            )
        else:
            cur.execute(
                """
                UPDATE zsxq_groups SET
                    last_sync_at     = %s,
                    last_sync_mode   = %s,
                    recent_topic_ids = COALESCE(%s, recent_topic_ids),
                    backfill_complete = CASE WHEN %s THEN true ELSE backfill_complete END,
                    updated_at       = now()
                WHERE group_id = %s
                """,
                (
                    updates["last_sync_at"],
                    mode,
                    updates.get("recent_topic_ids"),
                    updates["backfill_complete"],
                    group_id,
                ),
            )
    conn.commit()


def _store_topic_batch(
    content_service: SubscriptionContentService,
    batch: List[Dict[str, Any]],
    gid: str,
    gname: str,
    user_ids: List[int],
    report: Optional[ZsxqRunReport] = None,
) -> Tuple[int, int, List[int]]:
    """入库一页话题，返回 (new_count, updated_count, new_content_ids)。"""
    new_count = 0
    updated_count = 0
    new_ids: List[int] = []
    for t in batch:
        t["group_id"] = gid
        t["group_name"] = gname
        try:
            content_db_id, is_new = content_service.save_content(
                channel_type="zhishixingqiu",
                raw_data=t,
                user_ids=user_ids,
            )
            if is_new:
                new_count += 1
                new_ids.append(content_db_id)
            else:
                updated_count += 1
        except Exception as exc:
            logger.exception("ZSXQ 话题 %s 入库失败", t.get("topic_id"))
            if report:
                report.record_error(
                    f"{gname} 话题 {t.get('topic_id')} 入库失败: {exc}"
                )
    return new_count, updated_count, new_ids


def _fetch_head_topic_ids(client: ZsxqClient) -> List[int]:
    """拉取最新一页 topic_id，供增量锚点使用。"""
    data = client.fetch_topics_page(count=COUNT)
    topics = data.get("resp_data", {}).get("topics") or []
    return [raw["topic_id"] for raw in topics if raw.get("topic_id") is not None]


# ---------- 主服务 ----------

class ZsxqSubscriptionService:
    """
    知识星球订阅抓取服务。

    调用入口：fetch_and_store_topics(subscription_id?, user_id?, force_fetch?)
    """

    def __init__(self):
        self.content_service = SubscriptionContentService()

    def _get_active_credential(self) -> Tuple[int, str]:
        """获取首个有效凭证的 (id, cookie)，无则抛 RuntimeError。"""
        from backend.services.zsxq_credential_service import ZsxqCredentialService
        result = ZsxqCredentialService().get_first_valid_cookie()
        if not result:
            raise RuntimeError(
                "无可用的知识星球凭证，请在管理员后台录入有效 Cookie"
            )
        return result

    def _get_credential_for_group(self, group_meta: Dict[str, Any]) -> Tuple[int, str]:
        """
        按 zsxq_groups.credential_id 返回该星球对应账号的凭证。
        若凭证已过期或不存在，回退到首个有效凭证。
        """
        from backend.services.zsxq_credential_service import ZsxqCredentialService
        svc = ZsxqCredentialService()
        cred_id = group_meta.get("credential_id")
        if cred_id:
            cookie = svc.get_decrypted_cookie(cred_id)
            if cookie:
                return cred_id, cookie
            logger.warning("知识星球星球 credential_id=%s 凭证不可用，尝试回退到首个有效凭证", cred_id)
        return self._get_active_credential()

    def _mark_credential_expired(self, cred_id: int, error: str) -> None:
        from backend.services.zsxq_credential_service import ZsxqCredentialService
        ZsxqCredentialService().mark_expired(cred_id, error)

    def _aggregate_groups(
        self,
        subscription_id: Optional[int] = None,
        user_id: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        查询 subscription_list 中所有 enabled zhishixingqiu 订阅，
        按 group_id（channel_identifier）聚合 user_ids 并集。

        返回：[{group_id, group_name, user_ids: [...]}, ...]
        """
        with _get_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                sql = """
                    SELECT
                        sl.channel_identifier AS group_id,
                        sl.channel_name AS group_name,
                        ARRAY_AGG(DISTINCT sl.user_id) AS user_ids
                    FROM subscription_list sl
                    WHERE sl.channel_type = 'zhishixingqiu'
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

        result = []
        for row in rows:
            gid = row["group_id"]
            gname = row["group_name"] or gid
            # 从 zsxq_groups 补全星球名（可能比 subscription_list 更准确）
            result.append({
                "group_id": gid,
                "group_name": gname,
                "user_ids": list(row["user_ids"] or []),
            })
        return result

    def _get_group_meta(self, group_id: str) -> Dict[str, Any]:
        """从 zsxq_groups 读取星球名及同步锚点。"""
        with _get_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT * FROM zsxq_groups WHERE group_id = %s",
                    (group_id,),
                )
                row = cur.fetchone()
        return dict(row) if row else {}

    def fetch_and_store_topics(
        self,
        subscription_id: Optional[int] = None,
        user_id: Optional[int] = None,
        force_fetch: bool = False,
        *,
        collect_report: bool = False,
    ) -> Dict[str, Any]:
        """
        主入口：抓取所有（或指定）知识星球订阅的最新话题，写入 subscription_content。

        Returns:
            {
                "success": bool,
                "groups_processed": int,
                "topics_new": int,
                "topics_updated": int,
                "failed_groups": [...],
                "report": {...},  # collect_report=True 时附带
            }
        """
        report = ZsxqRunReport() if collect_report else None

        def _result(**kwargs: Any) -> Dict[str, Any]:
            base = {
                "success": kwargs.pop("success", True),
                "groups_processed": kwargs.pop("groups_processed", 0),
                "topics_new": kwargs.pop("topics_new", 0),
                "topics_updated": kwargs.pop("topics_updated", 0),
                "failed_groups": kwargs.pop("failed_groups", []),
            }
            base.update(kwargs)
            if report:
                base["report"] = report.to_dict()
                if base["failed_groups"]:
                    report.failed_groups = list(base["failed_groups"])
            return base

        # 预检：至少存在一个有效凭证
        try:
            self._get_active_credential()
        except RuntimeError as e:
            logger.error("知识星球凭证不可用: %s", e)
            if report:
                report.record_error(str(e))
            return _result(success=False, error=str(e))

        groups = self._aggregate_groups(subscription_id=subscription_id, user_id=user_id)
        if not groups:
            logger.info("无 enabled 知识星球订阅，跳过")
            return _result(success=True)

        total_new = 0
        total_updated = 0
        failed_groups: List[str] = []

        for idx, grp in enumerate(groups):
            gid = grp["group_id"]
            gname = grp["group_name"]
            user_ids = grp["user_ids"]

            # 从 zsxq_groups 补全 group_name，并获取该星球对应的凭证
            meta = self._get_group_meta(gid)
            if meta.get("group_name"):
                gname = meta["group_name"]

            try:
                cred_id, cookie = self._get_credential_for_group(meta)
            except RuntimeError as e:
                logger.error("知识星球 %s 凭证不可用，跳过: %s", gname, e)
                if report:
                    report.record_error(f"{gname} 凭证不可用: {e}")
                failed_groups.append(gname or gid)
                continue

            try:
                client = ZsxqClient(
                    cookie,
                    gid,
                    report=report,
                    group_label=gname,
                )
                backfill_done = meta.get("backfill_complete", False)
                known_ids: List[int] = list(meta.get("recent_topic_ids") or [])

                if not backfill_done or force_fetch:
                    mode = "backfill"
                    resume_from: Optional[str] = None
                    if not backfill_done and meta.get("min_create_time"):
                        resume_from = _dt_to_zsxq_time(meta["min_create_time"])
                    logger.info(
                        "[%d/%d] 知识星球 %s (%s) 开始 backfill%s",
                        idx + 1,
                        len(groups),
                        gname,
                        gid,
                        "（断点续抓）" if resume_from else "",
                    )
                    head_ids: Optional[List[int]] = None
                    group_new = 0
                    backfill_page_err: Optional[str] = None
                    try:
                        for _page, batch, page_head_ids in _iter_backfill_pages(
                            client, resume_from_min_time=resume_from
                        ):
                            if page_head_ids:
                                head_ids = page_head_ids
                            batch_new, batch_updated, new_ids = _store_topic_batch(
                                self.content_service,
                                batch,
                                gid,
                                gname,
                                user_ids,
                                report=report,
                            )
                            group_new += batch_new
                            total_new += batch_new
                            total_updated += batch_updated
                            if new_ids:
                                try:
                                    from backend.tasks.file_tasks import trigger_auto_feed_tagging
                                    trigger_auto_feed_tagging(new_ids)
                                except Exception as tag_exc:
                                    logger.warning("ZSXQ 触发打标失败，将在下次 task 中补全")
                                    if report:
                                        report.record_warning(
                                            f"{gname} 触发打标失败: {tag_exc}"
                                        )
                            if batch:
                                with _get_conn() as conn:
                                    _save_group_state(
                                        conn,
                                        gid,
                                        batch,
                                        mode,
                                        head_ids=page_head_ids,
                                        backfill_complete=False,
                                    )
                    except CookieExpiredError:
                        raise
                    except Exception as page_exc:
                        # 中途某页失败：保留已入库数据，记录警告，本 group 标记为未完成
                        # （下次任务会断点续抓），不计入 failed_groups。
                        backfill_page_err = str(page_exc)
                        logger.warning(
                            "知识星球 %s backfill 中途失败（已抓 %d 条），将在下次任务续抓: %s",
                            gname, group_new, page_exc,
                        )
                        if report:
                            report.record_warning(f"{gname} backfill 中途失败: {page_exc}")

                    if group_new > 0 or backfill_page_err is None:
                        # 全部页成功：标记 backfill_complete=True
                        if not head_ids:
                            try:
                                head_ids = _fetch_head_topic_ids(client)
                            except Exception:
                                pass
                        with _get_conn() as conn:
                            _save_group_state(
                                conn,
                                gid,
                                [],
                                mode,
                                head_ids=head_ids,
                                backfill_complete=(backfill_page_err is None),
                            )
                    logger.info(
                        "知识星球 %s 完成 [%s] 新 %d 条%s",
                        gname, mode, group_new,
                        "（部分完成，待续抓）" if backfill_page_err else "",
                    )
                    if report:
                        report.add_group_summary(gname, mode, group_new)
                    continue
                else:
                    mode = "incremental"
                    logger.info("[%d/%d] 知识星球 %s (%s) 增量, 锚点 %d 个", idx + 1, len(groups), gname, gid, len(known_ids))
                    topics, head_ids = _crawl_incremental(client, known_ids)

                batch_new, batch_updated, new_ids = _store_topic_batch(
                    self.content_service,
                    topics,
                    gid,
                    gname,
                    user_ids,
                    report=report,
                )
                total_new += batch_new
                total_updated += batch_updated

                # 自动打标
                if new_ids:
                    try:
                        from backend.tasks.file_tasks import trigger_auto_feed_tagging
                        trigger_auto_feed_tagging(new_ids)
                    except Exception as tag_exc:
                        logger.warning("ZSXQ 触发打标失败，将在下次 task 中补全")
                        if report:
                            report.record_warning(f"{gname} 触发打标失败: {tag_exc}")

                # 保存同步锚点
                with _get_conn() as conn:
                    _save_group_state(conn, gid, topics, mode, head_ids)

                logger.info("知识星球 %s 完成 [%s] 新 %d 条", gname, mode, batch_new)
                if report:
                    report.add_group_summary(gname, mode, batch_new)

            except CookieExpiredError as e:
                logger.error("知识星球 %s 凭证过期 (credential_id=%s): %s", gname, cred_id, e)
                self._mark_credential_expired(cred_id, str(e))
                if report:
                    report.record_error(f"{gname} Cookie 已过期 (credential_id={cred_id}): {e}")
                failed_groups.append(gname or gid)
                # 不 break：只跳过同一凭证的后续 group，继续处理其他凭证的 group

            except Exception as exc:
                logger.exception("知识星球 %s 抓取失败", gid)
                if report:
                    report.record_error(f"{gname or gid} 抓取失败: {exc}")
                failed_groups.append(gname or gid)

            if idx < len(groups) - 1:
                time.sleep(GROUP_INTERVAL)

        return _result(
            success=True,
            groups_processed=len(groups) - len(failed_groups),
            topics_new=total_new,
            topics_updated=total_updated,
            failed_groups=failed_groups,
        )
