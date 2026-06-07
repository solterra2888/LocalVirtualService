#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
知识星球话题采集（精简版，参考 ZsxqCrawler）
仅：指定星球 ID → 拉取话题 → 格式化 → 保存 JSON
"""

import json
import os
import re
import time
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from urllib.parse import quote

import requests

# ============ 配置（修改这里） ============
COOKIE = "zsxq_access_token=73D19B08-F3A8-4F94-AF94-6AF20A6444D1_A2210141E560B087; sensorsdata2015jssdkcross=%7B%22distinct_id%22%3A%22218522421882411%22%2C%22first_id%22%3A%2219e5f8d9b44232f-0895f2a7db7a8e8-26061151-2073600-19e5f8d9b4525a4%22%2C%22props%22%3A%7B%7D%2C%22identities%22%3A%22eyIkaWRlbnRpdHlfY29va2llX2lkIjoiMTllNWY4ZDliNDQyMzJmLTA4OTVmMmE3ZGI3YThlOC0yNjA2MTE1MS0yMDczNjAwLTE5ZTVmOGQ5YjQ1MjVhNCIsIiRpZGVudGl0eV9sb2dpbl9pZCI6IjIxODUyMjQyMTg4MjQxMSJ9%22%2C%22history_login_id%22%3A%7B%22name%22%3A%22%24identity_login_id%22%2C%22value%22%3A%22218522421882411%22%7D%7D; abtest_env=product"
GROUP_ID = "15555452148452"

# 每次 API 请求的条数（最大 30）
COUNT = 15

# 日期范围，空字符串表示不限制；格式 YYYY-MM-DD
DATE_START = "2026-06-01"
DATE_END = "2026-06-06"

# 请求间隔（秒），避免触发限流
REQUEST_INTERVAL = 5

# 模拟网页版客户端版本（过旧会返回 code=2，可按浏览器 Network 里 x-version 更新）
API_VERSION = "2.89.0"

# 是否下载话题附件（talk.files）
DOWNLOAD_FILES = False
FILE_DOWNLOAD_INTERVAL = 2

OUTPUT_DIR = "output"
# =========================================


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
    if "zsxq_access_token" not in cookie and "_" in cookie:
        cookie = f"zsxq_access_token={cookie}"
    return "; ".join(part.strip() for part in cookie.split(";") if part.strip())


def normalize_date_boundary(date_str: str, end_of_day: bool = False) -> Optional[str]:
    """将 YYYY-MM-DD 转为与 API create_time 可比较的边界字符串。"""
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


def calc_next_end_time(create_time: str) -> str:
    """
    计算下一页 end_time（与 wbsabc/zsxq-spider 一致，避免 API 返回「内部错误」）。
    返回已 urlencode 的字符串，拼 URL 时不要再编码。
    """
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
    # 编码后长度为 33 时补 0，与官方爬虫一致
    if len(end_time) == 33:
        end_time = end_time[:24] + "0" + end_time[24:]
    return end_time


def shift_end_time_back(end_time: str, seconds: int = 1) -> str:
    """失败重试时把 end_time 再往前推一点（传入未编码或已编码均可）。"""
    raw = end_time
    if "%" in end_time:
        from urllib.parse import unquote

        raw = unquote(end_time)
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


def extract_file_list(topic: Dict[str, Any]) -> List[Dict[str, Any]]:
    """提取附件元数据（参考 ZsxqCrawler topic_files，优先 talk.files）。"""
    files_raw: List[Dict[str, Any]] = []
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

    result = []
    for f in files_raw:
        file_id = f.get("file_id") or f.get("id")
        if not file_id:
            continue
        result.append(
            {
                "file_id": file_id,
                "name": f.get("name", ""),
                "hash": f.get("hash", ""),
                "size": f.get("size", 0),
                "duration": f.get("duration", 0),
                "download_count": f.get("download_count", 0),
                "create_time": f.get("create_time", ""),
                "local_path": None,
                "download_ok": False,
            }
        )
    return result


def safe_filename(name: str, file_id: int) -> str:
    safe = "".join(c for c in name if c.isalnum() or c in "._-（）()[]{} ")
    safe = safe.strip() or f"file_{file_id}"
    return safe


def extract_content(topic: Dict[str, Any]) -> Dict[str, Any]:
    """从原始 topic 提取可读字段。"""
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

    files = extract_file_list(topic)

    comments = []
    for c in topic.get("show_comments") or []:
        c_owner = c.get("owner") or {}
        repliee = c.get("repliee")
        comments.append(
            {
                "author": c_owner.get("name", ""),
                "reply_to": repliee.get("name") if repliee else None,
                "text": strip_html_tags(c.get("text", "")),
                "create_time": c.get("create_time", ""),
            }
        )

    answer = topic.get("answer")
    answer_text = ""
    answer_author = ""
    if answer:
        answer_text = strip_html_tags(answer.get("text", ""))
        answer_author = (answer.get("owner") or {}).get("name", "")

    return {
        "topic_id": topic.get("topic_id"),
        "create_time": topic.get("create_time", ""),
        "type": topic.get("type", ""),
        "title": topic.get("title", ""),
        "digested": topic.get("digested", False),
        "sticky": topic.get("sticky", False),
        "author": author,
        "text": strip_html_tags(content.get("text", "")),
        "images": images,
        "files": files,
        "likes_count": topic.get("likes_count", 0),
        "comments_count": topic.get("comments_count", 0),
        "comments": comments,
        "answer_author": answer_author,
        "answer_text": answer_text,
    }


class ZsxqClient:
    def __init__(self, cookie: str, group_id: str):
        self.cookie = clean_cookie(cookie)
        self.group_id = group_id
        self.session = requests.Session()
        self.base_url = "https://api.zsxq.com"

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
            "X-Version": API_VERSION,
        }

    def _get_json(self, url: str, max_retries: int = 5) -> Dict[str, Any]:
        last_err = ""
        for attempt in range(max_retries):
            try:
                resp = self.session.get(url, headers=self._headers(), timeout=30)
                if resp.status_code != 200:
                    last_err = f"HTTP {resp.status_code}"
                    time.sleep(min(2 + attempt, 8))
                    continue
                data = resp.json()
                if data.get("succeeded"):
                    return data
                code = data.get("code")
                last_err = data.get("info") or data.get("resp_err") or data.get("error") or str(data)
                if code == 2:
                    raise RuntimeError(
                        f"API 版本过旧 (code=2): {last_err}。请将 API_VERSION 改为浏览器 "
                        f"Network 中 api.zsxq.com 请求的 x-version 值（当前 {API_VERSION}）"
                    )
                if code in (1059,) or "内部" in str(last_err):
                    time.sleep(min(2 + attempt * 2, 15))
                    continue
            except requests.RequestException as e:
                last_err = str(e)
                time.sleep(min(2 + attempt, 8))
        raise RuntimeError(f"请求失败: {last_err}")

    def get_file_download_url(self, file_id: int) -> Optional[str]:
        url = f"{self.base_url}/v2/files/{file_id}/download_url"
        try:
            data = self._get_json(url, max_retries=5)
            return data.get("resp_data", {}).get("download_url")
        except RuntimeError as e:
            print(f"  ⚠ 获取下载链接失败 file_id={file_id}: {e}")
            return None

    def download_attachment(
        self, file_meta: Dict[str, Any], save_dir: str, topic_id: int
    ) -> Optional[str]:
        file_id = file_meta["file_id"]
        name = file_meta.get("name") or f"file_{file_id}"
        expected_size = file_meta.get("size") or 0

        os.makedirs(save_dir, exist_ok=True)
        fname = f"{topic_id}_{safe_filename(name, file_id)}"
        file_path = os.path.join(save_dir, fname)

        if os.path.exists(file_path):
            if expected_size <= 0 or os.path.getsize(file_path) == expected_size:
                print(f"  📎 已存在，跳过: {fname}")
                return file_path

        download_url = self.get_file_download_url(file_id)
        if not download_url:
            return None

        try:
            print(f"  📥 下载: {name}")
            resp = self.session.get(download_url, timeout=300, stream=True)
            if resp.status_code != 200:
                print(f"  ❌ 下载 HTTP {resp.status_code}: {name}")
                return None
            with open(file_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            print(f"  ✅ 已保存: {file_path}")
            return file_path
        except requests.RequestException as e:
            print(f"  ❌ 下载异常 {name}: {e}")
            return None

    def fetch_topics_page(
        self,
        count: int,
        end_time: Optional[str] = None,
        max_retries: int = 8,
    ) -> Dict[str, Any]:
        """
        拉取一页话题。end_time 若来自 calc_next_end_time 则已编码；
        来自 DATE_END 边界则为未编码原始串。
        """
        count = min(max(1, count), 30)
        base_params = f"scope=all&count={count}"
        retry_end_time = end_time
        if retry_end_time and "%" not in retry_end_time:
            retry_end_time = quote(retry_end_time)

        v1_params = f"count={count}"
        if retry_end_time:
            v1_params += f"&end_time={retry_end_time}"
        v2_query = base_params + (
            f"&end_time={retry_end_time}" if retry_end_time else ""
        )
        endpoints = [
            (
                f"{self.base_url}/v2/groups/{self.group_id}/topics",
                v2_query,
                "v2",
            ),
            (
                f"{self.base_url}/v1.10/groups/{self.group_id}/topics",
                v1_params,
                "v1.10",
            ),
        ]
        last_err = ""

        for attempt in range(max_retries):
            need_retry = False
            for path, query, label in endpoints:
                full_url = f"{path}?{query}"
                try:
                    resp = self.session.get(
                        full_url, headers=self._headers(), timeout=30
                    )
                    if resp.status_code != 200:
                        last_err = f"[{label}] HTTP {resp.status_code}"
                        continue
                    data = resp.json()
                    if data.get("succeeded"):
                        return data
                    code = data.get("code")
                    last_err = (
                        data.get("info")
                        or data.get("resp_err")
                        or data.get("error")
                        or str(data)
                    )
                    if code == 2:
                        print(
                            f"  ⚠ [{label}] 版本过旧 (code=2)，"
                            f"尝试下一接口（API_VERSION={API_VERSION}）..."
                        )
                        continue
                    if code == 1059 or "内部" in str(last_err):
                        need_retry = True
                        break
                    last_err = f"[{label}] {last_err}"
                except requests.RequestException as e:
                    last_err = f"[{label}] {e}"

            if not need_retry:
                break
            wait = min(2 + attempt * 2, 15)
            print(f"  ⚠ {last_err}, {wait}s 后重试...")
            time.sleep(wait)
            if retry_end_time:
                retry_end_time = shift_end_time_back(retry_end_time, 1)

        raise RuntimeError(
            f"获取话题失败: {last_err}。若含 code=2，请在浏览器 F12 查看 "
            f"api.zsxq.com 请求的 x-version，更新脚本中的 API_VERSION（当前 {API_VERSION}）"
        )


def crawl_topics(
    client: ZsxqClient,
    count: int,
    date_start: str,
    date_end: str,
) -> List[Dict[str, Any]]:
    start_bound = normalize_date_boundary(date_start, end_of_day=False)
    end_bound = normalize_date_boundary(date_end, end_of_day=True)

    # 第一页：有 DATE_END 则用边界时间（未编码）；之后用上一页最后一条推算（已编码）
    page_end_time: Optional[str] = quote(end_bound) if end_bound else None
    all_formatted: List[Dict[str, Any]] = []
    page = 0

    while True:
        page += 1
        print(f"请求第 {page} 页, count={count}", end="")
        if page_end_time:
            display = page_end_time[:30] + ("..." if len(page_end_time) > 30 else "")
            print(f", end_time={display}")
        else:
            print("...")

        data = client.fetch_topics_page(count=count, end_time=page_end_time)
        topics = data.get("resp_data", {}).get("topics") or []
        if not topics:
            print("  无更多数据，停止")
            break

        stopped_by_date = False
        for raw in topics:
            ct = raw.get("create_time", "")
            if start_bound and ct and ct < start_bound:
                stopped_by_date = True
                continue
            if in_date_range(ct, start_bound, end_bound):
                all_formatted.append(extract_content(raw))

        print(f"  本页 {len(topics)} 条, 累计有效 {len(all_formatted)} 条")

        if stopped_by_date:
            print(f"  已早于 DATE_START ({date_start})，停止")
            break
        if len(topics) < count:
            print("  已到最后一页")
            break

        last_ct = topics[-1].get("create_time", "")
        page_end_time = calc_next_end_time(last_ct)
        time.sleep(REQUEST_INTERVAL)

    return all_formatted


def download_topic_files(
    client: ZsxqClient, topics: List[Dict[str, Any]], group_id: str
) -> int:
    """下载所有话题附件到 output/{group_id}/downloads/"""
    save_dir = os.path.join(OUTPUT_DIR, group_id, "downloads")
    total = 0
    ok = 0

    for topic in topics:
        files = topic.get("files") or []
        if not files:
            continue
        topic_id = topic.get("topic_id")
        print(f"\n话题 {topic_id}，附件 {len(files)} 个")
        for f in files:
            total += 1
            path = client.download_attachment(f, save_dir, topic_id)
            f["local_path"] = path
            f["download_ok"] = path is not None
            if path:
                ok += 1
            time.sleep(FILE_DOWNLOAD_INTERVAL)

    print(f"\n附件下载: 成功 {ok}/{total}，目录 {save_dir}")
    return ok


def save_result(group_id: str, topics: List[Dict[str, Any]]) -> str:
    out_dir = os.path.join(OUTPUT_DIR, group_id)
    os.makedirs(out_dir, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(out_dir, f"topics_{ts}.json")
    payload = {
        "meta": {
            "group_id": group_id,
            "fetched_at": datetime.now().isoformat(timespec="seconds"),
            "count_per_request": COUNT,
            "date_start": DATE_START or None,
            "date_end": DATE_END or None,
            "api_version": API_VERSION,
            "download_files": DOWNLOAD_FILES,
            "total_topics": len(topics),
        },
        "topics": topics,
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return path


def main():
    if "在此填写" in COOKIE:
        print("请先在 my_zsxq.py 顶部填写 COOKIE 和 GROUP_ID")
        return

    print(f"星球 ID: {GROUP_ID}")
    print(f"每页条数: {COUNT}")
    print(f"日期范围: {DATE_START or '不限'} ~ {DATE_END or '不限'}")
    print(f"API 版本头: {API_VERSION}")

    client = ZsxqClient(COOKIE, GROUP_ID)
    topics = crawl_topics(client, COUNT, DATE_START, DATE_END)

    if DOWNLOAD_FILES:
        download_topic_files(client, topics, GROUP_ID)

    path = save_result(GROUP_ID, topics)
    print(f"\n完成，共 {len(topics)} 条，已保存: {path}")


if __name__ == "__main__":
    main()
