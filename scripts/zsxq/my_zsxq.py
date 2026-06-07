#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
知识星球话题采集 → PostgreSQL（zsxq_groups_topics）

运行：python my_zsxq.py（无需参数）
  - 设置了 DATE_START / DATE_END → 一次性按指定区间采集后退出
  - 否则若存在未完成 BACKFILL_DAYS 补全的星球 → 补全后进入定时
  - 全部星球已补全 → 定时增量（拉列表头部，topic_id 对比停步）

必填配置见 .env（DATABASE_URL 等）；Cookie 在 my_zsxq.py 顶部。
"""

import json
import logging
import os
import random
import re
import time
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote

import psycopg2
import psycopg2.extras
import requests
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("zsxq")

# ---------- 配置（修改 Cookie；API_VERSION 与浏览器 F12 的 x-version 一致） ----------
COOKIE = "zsxq_access_token=73D19B08-F3A8-4F94-AF94-6AF20A6444D1_A2210141E560B087; sensorsdata2015jssdkcross=%7B%22distinct_id%22%3A%22218522421882411%22%2C%22first_id%22%3A%2219e5f8d9b44232f-0895f2a7db7a8e8-26061151-2073600-19e5f8d9b4525a4%22%2C%22props%22%3A%7B%7D%2C%22identities%22%3A%22eyIkaWRlbnRpdHlfY29va2llX2lkIjoiMTllNWY4ZDliNDQyMzJmLTA4OTVmMmE3ZGI3YThlOC0yNjA2MTE1MS0yMDczNjAwLTE5ZTVmOGQ5YjQ1MjVhNCIsIiRpZGVudGl0eV9sb2dpbl9pZCI6IjIxODUyMjQyMTg4MjQxMSJ9%22%2C%22history_login_id%22%3A%7B%22name%22%3A%22%24identity_login_id%22%2C%22value%22%3A%22218522421882411%22%7D%7D; abtest_env=product"

# ---------- 默认值（一般无需改，也不走 .env） ----------
COUNT = 15
BACKFILL_DAYS_DEFAULT = 15
INCREMENT_MAX_PAGES = 5
SYNC_STATE_PATH = "data/group_sync_state.json"

PAGE_INTERVAL_MIN = 5.0
PAGE_INTERVAL_MAX = 5.0
GROUP_INTERVAL = 15
PAGES_PER_BATCH = 15
LONG_SLEEP_MIN = 180.0
LONG_SLEEP_MAX = 300.0

API_VERSION = "2.89.0"
SAVE_JSON = False
OUTPUT_DIR = "output"

LOG_LEVEL = "INFO"
LOG_DIR = "logs"

SCHEDULE_ACTIVE_INTERVAL = 600
SCHEDULE_NIGHT_INTERVAL = 3600
SCHEDULE_NIGHT_START = 1
SCHEDULE_NIGHT_END = 8
# -------------------------------------------------------

FILE_SEP = " | "
IMAGE_SEP = ","

UPSERT_SQL = """
INSERT INTO zsxq_groups_topics (
    topic_id, group_id, group_name, type, create_time,
    author, title, content, images, files, create_at, updated_at
) VALUES (
    %(topic_id)s, %(group_id)s, %(group_name)s, %(type)s, %(create_time)s,
    %(author)s, %(title)s, %(content)s, %(images)s, %(files)s, now(), now()
)
ON CONFLICT (topic_id) DO UPDATE SET
    group_name  = EXCLUDED.group_name,
    type        = EXCLUDED.type,
    create_time = EXCLUDED.create_time,
    author      = EXCLUDED.author,
    title       = EXCLUDED.title,
    content     = EXCLUDED.content,
    images      = EXCLUDED.images,
    files       = EXCLUDED.files,
    updated_at  = now()
"""

SCHEMA_DDL = """
CREATE TABLE IF NOT EXISTS zsxq_groups_topics (
    topic_id     BIGINT PRIMARY KEY,
    group_id     BIGINT NOT NULL,
    group_name   TEXT NOT NULL DEFAULT '',
    type         VARCHAR(20) NOT NULL DEFAULT 'talk',
    create_time  TIMESTAMPTZ NOT NULL,
    author       TEXT NOT NULL DEFAULT '',
    title        TEXT NOT NULL DEFAULT '',
    content      TEXT NOT NULL DEFAULT '',
    images       TEXT NOT NULL DEFAULT '',
    files        TEXT NOT NULL DEFAULT '',
    create_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_zsxq_groups_topics_group_time
    ON zsxq_groups_topics (group_id, create_time DESC);
"""


def _env(key: str, default: str = "") -> str:
    return os.getenv(key, default).strip()


def _env_int(key: str, default: int) -> int:
    raw = _env(key)
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def load_groups() -> List[Dict[str, Any]]:
    """从 .env 的 GROUPS（单行 JSON）或 groups.json 读取星球列表。"""
    groups_file = Path(_env("GROUPS_FILE", "groups.json"))
    groups_raw = _env("GROUPS")

    if groups_raw:
        try:
            groups = json.loads(groups_raw)
        except json.JSONDecodeError as e:
            hint = ""
            if groups_raw.strip() in ("[", "[]") or "\n" in groups_raw:
                hint = "（.env 里 JSON 不能换行，请写成一行，或删掉 GROUPS 改用 groups.json）"
            raise ValueError(f"GROUPS JSON 解析失败{hint}: {e}") from e
    elif groups_file.is_file():
        with open(groups_file, encoding="utf-8") as f:
            groups = json.load(f)
        logger.info("从 %s 加载星球列表", groups_file)
    else:
        raise ValueError("请在 .env 配置 GROUPS（单行 JSON）或提供 groups.json")

    if not isinstance(groups, list):
        raise ValueError("GROUPS 必须是 JSON 数组")
    return groups


def load_config() -> Dict[str, Any]:
    groups = load_groups()

    return {
        "cookie": COOKIE,
        "database_url": _env("DATABASE_URL"),
        "groups": groups,
        "date_start": _env("DATE_START"),
        "date_end": _env("DATE_END"),
        "backfill_days": _env_int("BACKFILL_DAYS", BACKFILL_DAYS_DEFAULT),
        "count": COUNT,
        "increment_max_pages": INCREMENT_MAX_PAGES,
        "sync_state_path": SYNC_STATE_PATH,
        "page_interval_min": PAGE_INTERVAL_MIN,
        "page_interval_max": PAGE_INTERVAL_MAX,
        "group_interval": GROUP_INTERVAL,
        "pages_per_batch": PAGES_PER_BATCH,
        "long_sleep_min": LONG_SLEEP_MIN,
        "long_sleep_max": LONG_SLEEP_MAX,
        "api_version": API_VERSION,
        "save_json": SAVE_JSON,
        "output_dir": OUTPUT_DIR,
        "log_level": LOG_LEVEL.upper(),
        "log_dir": LOG_DIR,
        "schedule_active_interval": SCHEDULE_ACTIVE_INTERVAL,
        "schedule_night_interval": SCHEDULE_NIGHT_INTERVAL,
        "schedule_night_start": SCHEDULE_NIGHT_START,
        "schedule_night_end": SCHEDULE_NIGHT_END,
    }


def setup_logging(log_dir: str, level_name: str) -> None:
    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)
    log_file = log_path / f"zsxq_{datetime.now():%Y%m%d}.log"

    level = getattr(logging, level_name, logging.INFO)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%H:%M:%S")

    root = logging.getLogger("zsxq")
    root.handlers.clear()
    root.setLevel(level)

    for handler in (logging.FileHandler(log_file, encoding="utf-8"), logging.StreamHandler()):
        handler.setFormatter(fmt)
        handler.setLevel(level)
        root.addHandler(handler)

    logger.debug("日志文件: %s", log_file)


def clean_cookie(cookie: str) -> str:
    cookie = cookie.strip()
    if "\n" in cookie:
        cookie = cookie.split("\n")[0]
    cookie = cookie.rstrip("\\")
    if cookie.startswith(("b'", 'b"')):
        cookie = cookie[2:-1]
    elif (cookie.startswith("'") and cookie.endswith("'")) or (
        cookie.startswith('"') and cookie.endswith('"')
    ):
        cookie = cookie[1:-1]
    cookie = cookie.replace("\\n", "")

    # 常见误填：把整段浏览器 Cookie 又套了一层 zsxq_access_token=
    if cookie.startswith("zsxq_access_token=_c_"):
        cookie = cookie[len("zsxq_access_token=") :]

    # 合并同名 key（后者覆盖前者，与浏览器行为一致）
    parts: Dict[str, str] = {}
    order: List[str] = []
    for segment in cookie.split(";"):
        segment = segment.strip()
        if not segment or "=" not in segment:
            continue
        key, value = segment.split("=", 1)
        key, value = key.strip(), value.strip()
        if key not in parts:
            order.append(key)
        parts[key] = value

    if "zsxq_access_token" not in parts and parts:
        # 仅粘贴了 token 字符串
        only = next(iter(parts.values())) if len(parts) == 1 and len(order) == 1 else None
        if only and "-" in only and "_" in only:
            return f"zsxq_access_token={only}"

    return "; ".join(f"{k}={parts[k]}" for k in order)


def backfill_target_start(backfill_days: int) -> str:
    end = datetime.now().date()
    start = end - timedelta(days=backfill_days - 1)
    return start.isoformat()


class SyncState:
    """本地每星球同步状态（时间范围 + 是否已完成 BACKFILL_DAYS 补全）。"""

    def __init__(self, path: str):
        self.path = Path(path)
        self.data: Dict[str, Dict[str, Any]] = self._load()

    def _load(self) -> Dict[str, Dict[str, Any]]:
        if not self.path.exists():
            return {}
        try:
            with open(self.path, encoding="utf-8") as f:
                raw = json.load(f)
            return raw if isinstance(raw, dict) else {}
        except (json.JSONDecodeError, OSError) as e:
            logger.warning("读取同步状态失败 %s: %s，将重建", self.path, e)
            return {}

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self.data, f, ensure_ascii=False, indent=2)
        tmp.replace(self.path)

    def get(self, group_id: str) -> Optional[Dict[str, Any]]:
        return self.data.get(group_id)

    def is_backfill_complete(self, group_id: str) -> bool:
        entry = self.get(group_id)
        return bool(entry and entry.get("backfill_complete"))

    def incomplete_groups(self, groups: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return [g for g in groups if not self.is_backfill_complete(str(g["group_id"]))]

    def all_backfill_complete(self, groups: List[Dict[str, Any]]) -> bool:
        return len(self.incomplete_groups(groups)) == 0

    @staticmethod
    def _topic_times(topics: List[Dict[str, Any]]) -> List[str]:
        return [t["create_time"] for t in topics if t.get("create_time")]

    @staticmethod
    def _topic_ids(topics: List[Dict[str, Any]]) -> List[int]:
        return [int(t["topic_id"]) for t in topics if t.get("topic_id") is not None]

    def get_recent_topic_ids(self, group_id: str) -> List[int]:
        entry = self.get(group_id) or {}
        raw = entry.get("recent_topic_ids") or []
        return [int(x) for x in raw]

    def update_after_crawl(
        self,
        group_id: str,
        group_name: str,
        topics: List[Dict[str, Any]],
        mode: str,
        head_page_ids: Optional[List[int]] = None,
    ) -> None:
        times = self._topic_times(topics)
        entry = dict(self.get(group_id) or {})
        entry["group_name"] = group_name
        entry["last_sync_at"] = datetime.now().isoformat(timespec="seconds")
        entry["last_mode"] = mode

        if times:
            batch_min, batch_max = min(times), max(times)
            if entry.get("min_create_time"):
                entry["min_create_time"] = min(entry["min_create_time"], batch_min)
            else:
                entry["min_create_time"] = batch_min
            if entry.get("max_create_time"):
                entry["max_create_time"] = max(entry["max_create_time"], batch_max)
            else:
                entry["max_create_time"] = batch_max

        if head_page_ids:
            entry["recent_topic_ids"] = head_page_ids
        elif mode == "backfill" and topics:
            entry["recent_topic_ids"] = self._topic_ids(topics[:COUNT])

        if mode == "backfill":
            entry["backfill_complete"] = True

        self.data[group_id] = entry
        self.save()


def resolve_date_range_for_group(
    cfg: Dict[str, Any],
    group_id: str,
    mode: str,
    state: SyncState,
) -> Tuple[str, str]:
    """manual / backfill 使用日期区间；incremental 不走此函数。"""
    if mode == "manual":
        return cfg["date_start"], cfg["date_end"]

    today = datetime.now().date().isoformat()
    return backfill_target_start(cfg["backfill_days"]), today


def normalize_date_boundary(date_str: str, end_of_day: bool = False) -> Optional[str]:
    if not date_str or not date_str.strip():
        return None
    date_str = date_str.strip()
    if "T" in date_str:
        return date_str
    if end_of_day:
        return f"{date_str}T23:59:59.999+0800"
    return f"{date_str}T00:00:00.000+0800"


def in_date_range(create_time: str, start: Optional[str], end: Optional[str]) -> bool:
    if not create_time:
        return False
    if start and create_time < start:
        return False
    if end and create_time > end:
        return False
    return True


def parse_api_time(value: str) -> datetime:
    if not value:
        return datetime.now()
    return datetime.fromisoformat(value.replace("+0800", "+08:00"))


def calc_next_end_time(create_time: str) -> str:
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


def shift_end_time_back(end_time: str, seconds: int = 1) -> str:
    from urllib.parse import unquote

    raw = unquote(end_time) if "%" in end_time else end_time
    try:
        if len(raw) >= 23:
            dt = datetime.strptime(raw[:19], "%Y-%m-%dT%H:%M:%S") - timedelta(seconds=seconds)
            raw = dt.strftime("%Y-%m-%dT%H:%M:%S") + raw[19:]
        return calc_next_end_time(raw) if len(raw) >= 23 else quote(raw)
    except ValueError:
        return end_time


def strip_html_tags(text: str) -> str:
    if not text:
        return ""
    text = re.sub(r"<e[^>]*>", "", text)
    text = re.sub(r"</e>", "", text)
    return text.strip()


def extract_file_names(topic: Dict[str, Any]) -> List[str]:
    talk = topic.get("talk") or {}
    if talk.get("files"):
        files_raw = talk["files"]
    else:
        content = (
            topic.get("question")
            or topic.get("task")
            or topic.get("solution")
            or {}
        )
        files_raw = content.get("files") or []
    return [f.get("name", "") for f in files_raw if f.get("name")]


def extract_content(topic: Dict[str, Any]) -> Dict[str, Any]:
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

    return {
        "topic_id": topic.get("topic_id"),
        "create_time": topic.get("create_time", ""),
        "type": topic.get("type", ""),
        "title": topic.get("title", ""),
        "author": author,
        "text": strip_html_tags(content.get("text", "")),
        "images": images,
        "files": extract_file_names(topic),
    }


def topic_to_row(
    topic: Dict[str, Any], group_id: str, group_name: str
) -> Dict[str, Any]:
    return {
        "topic_id": topic["topic_id"],
        "group_id": int(group_id),
        "group_name": group_name,
        "type": topic.get("type") or "talk",
        "create_time": parse_api_time(topic.get("create_time", "")),
        "author": topic.get("author") or "",
        "title": topic.get("title") or "",
        "content": topic.get("text") or "",
        "images": IMAGE_SEP.join(topic.get("images") or []),
        "files": FILE_SEP.join(topic.get("files") or []),
    }


def page_sleep(page: int, cfg: Dict[str, Any]) -> None:
    delay = random.uniform(cfg["page_interval_min"], cfg["page_interval_max"])
    logger.debug("页间等待 %.1fs", delay)
    time.sleep(delay)
    if page > 0 and page % cfg["pages_per_batch"] == 0:
        long_delay = random.uniform(cfg["long_sleep_min"], cfg["long_sleep_max"])
        logger.info("长休眠 %.0fs (已完成 %d 页)", long_delay, page)
        time.sleep(long_delay)


class ZsxqClient:
    """请求逻辑对齐 my_zsxq_仅数据验证.py：v2 优先，可重试，成功后固定 v2 翻页。"""

    def __init__(self, cookie: str, group_id: str, api_version: str):
        self.cookie = clean_cookie(cookie)
        self.group_id = group_id
        self.api_version = api_version
        self.session = requests.Session()
        self.base_url = "https://api.zsxq.com"
        self._topics_api: Optional[str] = None

    def _headers(self) -> Dict[str, str]:
        return {
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Cache-Control": "no-cache",
            "Cookie": self.cookie,
            "Origin": "https://wx.zsxq.com",
            "Pragma": "no-cache",
            "Referer": f"https://wx.zsxq.com/dweb2/index/group/{self.group_id}",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-site",
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
        self, count: int, end_time: Optional[str] = None, max_retries: int = 8
    ) -> Dict[str, Any]:
        count = min(max(1, count), 30)
        retry_end_time = end_time
        if retry_end_time and "%" not in retry_end_time:
            retry_end_time = quote(retry_end_time)

        def make_endpoints(et: Optional[str]) -> List[Tuple[str, str, str]]:
            v1 = f"count={count}"
            v2 = f"scope=all&count={count}"
            if et:
                v1 += f"&end_time={et}"
                v2 += f"&end_time={et}"
            eps = [
                (f"{self.base_url}/v2/groups/{self.group_id}/topics", v2, "v2"),
                (f"{self.base_url}/v1.10/groups/{self.group_id}/topics", v1, "v1.10"),
            ]
            if self._topics_api:
                eps = [e for e in eps if e[2] == self._topics_api]
            return eps

        last_err = ""
        endpoints = make_endpoints(retry_end_time)

        for attempt in range(max_retries):
            need_retry = False
            for path, query, label in endpoints:
                try:
                    resp = self.session.get(
                        f"{path}?{query}", headers=self._headers(), timeout=30
                    )
                    if resp.status_code == 401:
                        raise RuntimeError(
                            "Cookie 无效或已过期 (HTTP 401)，请更新 my_zsxq.py 的 COOKIE"
                        )
                    if resp.status_code != 200:
                        last_err = f"[{label}] HTTP {resp.status_code}"
                        continue
                    data = resp.json()
                    if data.get("succeeded"):
                        self._topics_api = label
                        return data
                    code = data.get("code")
                    last_err = (
                        data.get("info")
                        or data.get("resp_err")
                        or data.get("error")
                        or str(data)
                    )
                    if code == 2:
                        logger.debug("[%s] code=2，尝试下一接口", label)
                        continue
                    if (
                        code == 1059
                        or "内部" in str(last_err)
                        or "非官方" in str(last_err)
                    ):
                        need_retry = True
                        last_err = f"[{label}] {last_err}"
                        break
                    last_err = f"[{label}] {last_err}"
                except requests.RequestException as e:
                    last_err = f"[{label}] {e}"

            if not need_retry:
                break
            wait = min(2 + attempt * 2, 15)
            logger.warning("API 重试 (%ds): %s", wait, last_err)
            time.sleep(wait)
            if retry_end_time:
                retry_end_time = shift_end_time_back(retry_end_time, 1)
            endpoints = make_endpoints(retry_end_time)

        raise RuntimeError(
            f"获取话题失败: {last_err}。"
            f"若含版本过旧，请将 API_VERSION 改为浏览器 x-version（当前 {self.api_version}）"
        )


def crawl_topics(
    client: ZsxqClient,
    cfg: Dict[str, Any],
    date_start: str,
    date_end: str,
) -> List[Dict[str, Any]]:
    count = cfg["count"]
    start_bound = normalize_date_boundary(date_start, end_of_day=False)
    end_bound = normalize_date_boundary(date_end, end_of_day=True)
    page_end_time: Optional[str] = quote(end_bound) if end_bound else None
    all_formatted: List[Dict[str, Any]] = []
    page = 0

    while True:
        page += 1
        logger.debug("请求第 %d 页 count=%d", page, count)

        try:
            data = client.fetch_topics_page(count=count, end_time=page_end_time)
        except RuntimeError as e:
            if all_formatted and page > 1:
                logger.warning(
                    "第 %d 页失败，保留已采集 %d 条继续入库: %s",
                    page,
                    len(all_formatted),
                    e,
                )
                break
            raise

        topics = data.get("resp_data", {}).get("topics") or []
        if not topics:
            logger.info("第 %d 页无数据，停止", page)
            break

        stopped_by_date = False
        for raw in topics:
            ct = raw.get("create_time", "")
            if start_bound and ct and ct < start_bound:
                stopped_by_date = True
                continue
            if in_date_range(ct, start_bound, end_bound):
                all_formatted.append(extract_content(raw))

        logger.info("第 %d 页 +%d 条，累计 %d 条", page, len(topics), len(all_formatted))

        if stopped_by_date:
            logger.info("已早于起始日期 %s，停止", date_start)
            break
        if len(topics) < count:
            logger.info("已到最后一页")
            break

        page_end_time = calc_next_end_time(topics[-1].get("create_time", ""))
        page_sleep(page, cfg)

    return all_formatted


def crawl_topics_incremental(
    client: ZsxqClient,
    cfg: Dict[str, Any],
    known_ids: List[int],
) -> Tuple[List[Dict[str, Any]], List[int]]:
    """
    增量：从最新一页起拉，与 known topic_id 对比；整页已知则停。
    返回 (待入库话题, 首页 topic_id 列表供写入 state)。
    """
    count = cfg["count"]
    max_pages = cfg["increment_max_pages"]
    known = set(known_ids)
    all_formatted: List[Dict[str, Any]] = []
    head_page_ids: List[int] = []
    page_end_time: Optional[str] = None
    page = 0

    if not known:
        logger.info("无 recent_topic_ids，仅拉首页建立锚点")
        max_pages = 1

    while page < max_pages:
        page += 1
        logger.debug("增量第 %d 页 count=%d", page, count)

        data = client.fetch_topics_page(count=count, end_time=page_end_time)
        topics = data.get("resp_data", {}).get("topics") or []
        if not topics:
            logger.info("增量第 %d 页无数据，停止", page)
            break

        page_ids = [raw["topic_id"] for raw in topics if raw.get("topic_id") is not None]
        if page == 1:
            head_page_ids = page_ids

        for raw in topics:
            all_formatted.append(extract_content(raw))

        new_count = sum(1 for tid in page_ids if tid not in known)
        logger.info(
            "增量第 %d 页 %d 条（新 %d / 已知 %d），累计 %d 条",
            page,
            len(topics),
            new_count,
            len(page_ids) - new_count,
            len(all_formatted),
        )

        if known and all(tid in known for tid in page_ids):
            logger.info("增量第 %d 页全部已知，停止", page)
            break
        if len(topics) < count:
            logger.info("增量已到最后一页")
            break

        page_end_time = calc_next_end_time(topics[-1].get("create_time", ""))
        page_sleep(page, cfg)

    return all_formatted, head_page_ids


class TopicRepository:
    def __init__(self, database_url: str):
        self.database_url = database_url

    @staticmethod
    def ensure_schema(database_url: str) -> None:
        with psycopg2.connect(database_url) as conn:
            with conn.cursor() as cur:
                cur.execute(SCHEMA_DDL)
            conn.commit()
        logger.info("数据库表 zsxq_groups_topics 已就绪")

    def upsert_many(self, rows: List[Dict[str, Any]]) -> Tuple[int, int]:
        """返回 (新入库条数, 更新条数)。"""
        if not rows:
            return 0, 0
        topic_ids = [r["topic_id"] for r in rows]
        with psycopg2.connect(self.database_url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT topic_id FROM zsxq_groups_topics WHERE topic_id = ANY(%s)",
                    (topic_ids,),
                )
                existing = {row[0] for row in cur.fetchall()}
                psycopg2.extras.execute_batch(cur, UPSERT_SQL, rows, page_size=100)
            conn.commit()
        inserted = sum(1 for tid in topic_ids if tid not in existing)
        return inserted, len(topic_ids) - inserted


def save_json_backup(
    output_dir: str, group_id: str, topics: List[Dict[str, Any]], meta: Dict[str, Any]
) -> str:
    out_dir = os.path.join(output_dir, group_id)
    os.makedirs(out_dir, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(out_dir, f"topics_{ts}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump({"meta": meta, "topics": topics}, f, ensure_ascii=False, indent=2)
    return path


def validate_config(cfg: Dict[str, Any]) -> None:
    if not cfg["cookie"] or "在此填写" in cfg["cookie"] or "your_token" in cfg["cookie"]:
        raise SystemExit("请在 my_zsxq.py 顶部填写 COOKIE")
    url = cfg["database_url"]
    if not url or "your_db" in url:
        raise SystemExit("请在 .env 中配置 DATABASE_URL")
    if url.count("@") > 1:
        raise SystemExit(
            "DATABASE_URL 格式有误：密码中的 @ 会破坏 URL 解析，"
            "请将特殊字符 URL 编码（@→%40，+→%2B，,→%2C，!→%21）"
        )
    if not cfg["groups"]:
        raise SystemExit("请在 groups.json 或 .env 的 GROUPS 中配置星球列表")


def check_database_connection(database_url: str) -> None:
    """启动前探测连接；OperationalError 表示连不上库，不是建表权限问题。"""
    try:
        with psycopg2.connect(database_url, connect_timeout=10) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
    except psycopg2.OperationalError as e:
        raise SystemExit(f"无法连接 PostgreSQL: {e}") from e
    except psycopg2.Error as e:
        raise SystemExit(f"PostgreSQL 连接异常: {e}") from e


def is_night_hours(now: datetime, cfg: Dict[str, Any]) -> bool:
    start, end = cfg["schedule_night_start"], cfg["schedule_night_end"]
    h = now.hour
    if start < end:
        return start <= h < end
    return h >= start or h < end


def get_schedule_interval(cfg: Dict[str, Any], now: Optional[datetime] = None) -> int:
    now = now or datetime.now()
    if is_night_hours(now, cfg):
        return cfg["schedule_night_interval"]
    return cfg["schedule_active_interval"]


def run_crawl_job(
    cfg: Dict[str, Any],
    mode: str,
    state: Optional[SyncState] = None,
    groups: Optional[List[Dict[str, Any]]] = None,
) -> int:
    if state is None and mode != "manual":
        state = SyncState(cfg["sync_state_path"])

    target_groups = groups if groups is not None else cfg["groups"]
    repo = TopicRepository(cfg["database_url"])
    total_saved = 0
    total_updated = 0
    failed_groups: List[str] = []

    mode_label = {"manual": "手动区间", "backfill": "补全", "incremental": "增量"}.get(
        mode, mode
    )
    logger.info("开始采集 [%s] | 星球 %d 个", mode_label, len(target_groups))

    for i, group in enumerate(target_groups):
        group_id = str(group["group_id"])
        group_name = group.get("group_name") or ""

        try:
            client = ZsxqClient(cfg["cookie"], group_id, cfg["api_version"])

            if mode == "incremental":
                known_ids = state.get_recent_topic_ids(group_id) if state else []
                logger.info(
                    "[%d/%d] %s (%s) | 头部增量 | 锚点 %d 个 id",
                    i + 1,
                    len(target_groups),
                    group_name,
                    group_id,
                    len(known_ids),
                )
                topics, head_page_ids = crawl_topics_incremental(
                    client, cfg, known_ids
                )
                date_start, date_end = "", ""
            else:
                date_start, date_end = resolve_date_range_for_group(
                    cfg, group_id, mode, state
                )
                logger.info(
                    "[%d/%d] %s (%s) | %s ~ %s",
                    i + 1,
                    len(target_groups),
                    group_name,
                    group_id,
                    date_start or "不限",
                    date_end or "不限",
                )
                topics = crawl_topics(client, cfg, date_start, date_end)
                head_page_ids = None

            rows = [topic_to_row(t, group_id, group_name) for t in topics]
            inserted, updated = repo.upsert_many(rows)
            total_saved += inserted
            total_updated += updated
            if updated:
                logger.info("新入库 %d 条，更新 %d 条", inserted, updated)
            else:
                logger.info("新入库 %d 条", inserted)

            if state is not None and mode != "manual":
                state.update_after_crawl(
                    group_id, group_name, topics, mode, head_page_ids=head_page_ids
                )
                entry = state.get(group_id) or {}
                logger.info(
                    "状态更新 min=%s max=%s 锚点=%d backfill=%s",
                    (entry.get("min_create_time") or "-")[:19],
                    (entry.get("max_create_time") or "-")[:19],
                    len(entry.get("recent_topic_ids") or []),
                    entry.get("backfill_complete", False),
                )

            if cfg["save_json"]:
                path = save_json_backup(
                    cfg["output_dir"],
                    group_id,
                    topics,
                    {
                        "group_id": group_id,
                        "group_name": group_name,
                        "mode": mode,
                        "date_start": date_start or None,
                        "date_end": date_end or None,
                        "total_topics": len(topics),
                    },
                )
                logger.info("JSON 备份: %s", path)
        except Exception:
            logger.exception("星球 %s 采集失败", group_id)
            failed_groups.append(group_name or group_id)

        if i < len(target_groups) - 1:
            logger.debug("group 间隔 %ds", cfg["group_interval"])
            time.sleep(cfg["group_interval"])

    if total_updated:
        logger.info(
            "本轮完成 [%s]，新入库 %d 条，更新 %d 条",
            mode_label,
            total_saved,
            total_updated,
        )
    else:
        logger.info("本轮完成 [%s]，新入库 %d 条", mode_label, total_saved)
    if failed_groups:
        logger.warning(
            "失败 %d 个星球（未更新 sync 状态，下次会重试）: %s",
            len(failed_groups),
            ", ".join(failed_groups),
        )
    return total_saved


def run_scheduler(cfg: Dict[str, Any], state: SyncState) -> None:
    ns, ne = cfg["schedule_night_start"], cfg["schedule_night_end"]
    logger.info(
        "定时增量启动 | 活跃 %ds | 夜间(%02d:00-%02d:00) %ds",
        cfg["schedule_active_interval"],
        ns,
        ne,
        cfg["schedule_night_interval"],
    )
    while True:
        started = datetime.now()
        try:
            run_crawl_job(cfg, mode="incremental", state=state)
        except Exception:
            logger.exception("本轮采集异常")

        interval = get_schedule_interval(cfg, started + timedelta(seconds=1))
        next_at = datetime.now() + timedelta(seconds=interval)
        mode = "夜间" if is_night_hours(next_at, cfg) else "活跃"
        logger.info(
            "下次执行 %s (%s, %ds)",
            next_at.strftime("%Y-%m-%d %H:%M:%S"),
            mode,
            interval,
        )
        time.sleep(interval)


def run_auto(cfg: Dict[str, Any]) -> None:
    if cfg["date_start"] or cfg["date_end"]:
        logger.info("检测到 DATE_START/DATE_END，一次性手动采集")
        run_crawl_job(cfg, mode="manual")
        return

    state = SyncState(cfg["sync_state_path"])
    incomplete = state.incomplete_groups(cfg["groups"])

    if incomplete:
        names = ", ".join(f'{g.get("group_name", g["group_id"])}' for g in incomplete)
        logger.info(
            "未完成 %d 天补全的星球 %d 个: %s",
            cfg["backfill_days"],
            len(incomplete),
            names,
        )
        run_crawl_job(cfg, mode="backfill", state=state, groups=incomplete)

        still = state.incomplete_groups(cfg["groups"])
        if still:
            logger.warning(
                "仍有 %d 个星球未完成补全（可能采集中失败），请检查日志后重跑",
                len(still),
            )
            return

        logger.info("全部星球已完成 %d 天补全，进入定时增量", cfg["backfill_days"])

    if not state.all_backfill_complete(cfg["groups"]):
        logger.error("补全状态异常，退出")
        return

    run_scheduler(cfg, state)


def main():
    cfg = load_config()
    setup_logging(cfg["log_dir"], cfg["log_level"])
    validate_config(cfg)
    check_database_connection(cfg["database_url"])
    try:
        TopicRepository.ensure_schema(cfg["database_url"])
    except psycopg2.Error as e:
        logger.error("自动建表失败: %s", e)
        raise SystemExit("建表失败（已连上库，可能是账号无 CREATE 权限）") from e
    run_auto(cfg)


if __name__ == "__main__":
    main()
