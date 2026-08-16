# -*- coding: utf-8 -*-
"""
知识星球附件文件下载工具（自包含，不依赖 scripts/zsxq-crawl/download_test/_config.py）

移植自已实测的 download_test/mobile_download.py + _config.py（见
scripts/zsxq-crawl/DOWNLOAD_CHANGES.md §4.1-4.3）。

核心：知识星球「禁网页下载」是客户端类型识别（code 1030），不是签名门槛。
用移动端 H5 请求头（Sec-Ch-Ua-Mobile:?1 + Android UA）即可绕过。

链路两段式：
  ① GET /v2/files/{file_id}/download_url   → 拿到带 TTL 的 CDN 直链（JSON）
  ② 流式 GET CDN 直链                       → 写入本地文件
download_url 有 TTL，必须拿到后立刻下，不要存起来稍后用。

错误处理：
  - HTTP 401            → CookieExpired 异常（熔断整条队列，由调用方处理）
  - code 1030           → 'blocked_1030'（连移动头都被拒，硬限制，跳过）
  - 1059 / 5xx / 429    → 退避重试（2→2→2→5→5→5→10s，最多 10 次）
  - CDN 401/403         → 带 Cookie 重试一次
"""

import logging
import random
import re
import time
import uuid
from typing import Optional, Tuple
from urllib.parse import quote

import requests

logger = logging.getLogger(__name__)

BASE_URL = "https://api.zsxq.com"
REQUEST_TIMEOUT = 30
CDN_TIMEOUT = 300
MAX_RETRIES = 10

ANTI_CRAWL_CODE = 1059      # 反爬 → 退避重试（调用方可做整级冷却）
MOBILE_ONLY_CODE = 1030     # 连移动头都被拒 → 硬限制，跳过该文件
RETRYABLE_HTTP = {429, 500, 502, 503, 504}

# 沿用账号已验证的 API 版本（过低会触发 code=2「版本太旧」）
MOBILE_API_VERSION = "2.89.0"

MOBILE_USER_AGENTS = [
    (
        "Mozilla/5.0 (Linux; Android 14; Pixel 8 Pro) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 "
        "Mobile Safari/537.36 MicroMessenger/8.0.49.2600(0x28003133) "
        "NetType/WIFI Language/zh_CN"
    ),
    (
        "Mozilla/5.0 (Linux; Android 13; M2012K11AC) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 "
        "Mobile Safari/537.36"
    ),
]


class CookieExpired(Exception):
    """Cookie 失效（HTTP 401）→ 熔断整条队列，等人刷新 cookie，不要用重试去烧。"""


# ---------- 请求头 ----------

def _sec_ch_ua(ua: str) -> str:
    if "Chrome/125" in ua:
        return '"Google Chrome";v="125", "Chromium";v="125", "Not.A/Brand";v="24"'
    if "Chrome/124" in ua:
        return '"Google Chrome";v="124", "Chromium";v="124", "Not.A/Brand";v="24"'
    return '"Chromium";v="125", "Not.A/Brand";v="24", "Google Chrome";v="125"'


def _platform(ua: str) -> str:
    return '"iOS"' if ("iPhone" in ua or "iPad" in ua) else '"Android"'


def mobile_headers(cookie: str, group_id: Optional[str] = None) -> dict:
    """移动端 H5 头 —— 绕过「禁网页下载」的关键身份。用于 download_url 接口。"""
    ua = random.choice(MOBILE_USER_AGENTS)
    return {
        "Accept": "application/json, text/plain, */*",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "Cookie": cookie,
        "Host": "api.zsxq.com",
        "Origin": "https://wx.zsxq.com",
        "Pragma": "no-cache",
        "Referer": (
            f"https://wx.zsxq.com/dweb2/index/group/{group_id}"
            if group_id else "https://wx.zsxq.com/"
        ),
        "Sec-Ch-Ua": _sec_ch_ua(ua),
        "Sec-Ch-Ua-Mobile": "?1",            # ← 关键判别位
        "Sec-Ch-Ua-Platform": _platform(ua),  # ← "Android"
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-site",
        "User-Agent": ua,
        "X-Requested-With": "XMLHttpRequest",
        "X-Request-Id": str(uuid.uuid4()),
        "X-Timestamp": str(int(time.time())),
        "X-Version": MOBILE_API_VERSION,
    }


def file_stream_headers(
    cookie: str, group_id: Optional[str] = None, include_cookie: bool = False
) -> dict:
    """CDN 文件字节流头：基于移动头，去掉 Host/Origin（不带到 CDN），默认不带 Cookie。"""
    h = mobile_headers(cookie, group_id)
    h.update({
        "Accept": "*/*",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "cross-site",
    })
    h.pop("Host", None)
    h.pop("Origin", None)
    if not include_cookie:
        h.pop("Cookie", None)
    return h


# ---------- 退避 ----------

def _backoff(retry_index: int) -> int:
    """0-based：前 3 次 2s，次 3 次 5s，之后 10s。"""
    if retry_index < 3:
        return 2
    if retry_index < 6:
        return 5
    return 10


# ---------- ① 取下载链接 ----------

def get_download_url(
    file_id: str,
    cookie: str,
    group_id: Optional[str] = None,
    max_retries: int = MAX_RETRIES,
) -> Tuple[Optional[str], str]:
    """
    用移动端头拿 download_url。

    Returns:
        (url, status)，status ∈ {'ok','blocked_1030','retry_exhausted','failed'}
    Raises:
        CookieExpired: HTTP 401（cookie 失效），由调用方熔断。
    """
    url = f"{BASE_URL}/v2/files/{file_id}/download_url"
    sess = requests.Session()
    last_info = ""

    for attempt in range(max_retries):
        if attempt > 0:
            wait = _backoff(attempt) + random.uniform(0, 2)
            logger.debug("ZSXQ 取下载链接第 %d 次重试，等待 %.1fs file_id=%s", attempt, wait, file_id)
            time.sleep(wait)

        try:
            resp = sess.get(url, headers=mobile_headers(cookie, group_id), timeout=REQUEST_TIMEOUT)
        except requests.RequestException as e:
            last_info = f"request_exception: {e}"
            continue

        if resp.status_code == 401:
            raise CookieExpired(f"HTTP 401 file_id={file_id}")
        if resp.status_code in RETRYABLE_HTTP:
            last_info = f"http_{resp.status_code}"
            continue
        if resp.status_code != 200:
            return None, f"http_{resp.status_code}"

        try:
            data = resp.json()
        except ValueError:
            return None, "non_json"

        if data.get("succeeded"):
            dl = (data.get("resp_data") or {}).get("download_url")
            if dl:
                return dl, "ok"
            return None, "no_url"

        code = data.get("code")
        info = data.get("info") or data.get("error_message") or data.get("error") or ""
        if str(code) == str(MOBILE_ONLY_CODE) or _is_mobile_only(code, info):
            logger.warning("ZSXQ 文件 %s 命中 1030 硬限制: %s", file_id, info)
            return None, "blocked_1030"
        # 1059 / 其它 → 退避重试
        last_info = f"code={code} {info}"

    logger.warning("ZSXQ 取下载链接重试 %d 次仍失败 file_id=%s: %s", max_retries, file_id, last_info)
    return None, "retry_exhausted"


def _is_mobile_only(code, message: str) -> bool:
    msg = (message or "").lower()
    return any(m in msg for m in ("手机", "mobile", "app", "客户端"))


# ---------- ② 流式下载 ----------

def _filename_from_response(resp: requests.Response, file_id: str, fallback_name: str) -> str:
    """优先用 Content-Disposition 的文件名，否则用 ZSXQ 给的 name，再否则 file_id.bin。"""
    cd = resp.headers.get("content-disposition", "")
    if "filename=" in cd:
        m = re.search(r"filename\*?=(?:UTF-8'')?([^;]+)", cd, re.IGNORECASE)
        if m:
            name = m.group(1).strip().strip("\"'")
            if name:
                return name
    return fallback_name or f"{file_id}.bin"


def download_to_file(
    download_url: str,
    dest_path: str,
    cookie: str,
    group_id: Optional[str] = None,
    expected_size: int = 0,
    fallback_name: str = "",
    file_id: str = "",
) -> Tuple[bool, int]:
    """
    流式下载 CDN 字节到 dest_path。CDN 401/403 带 Cookie 重试一次。

    Returns:
        (ok, size)。expected_size > 0 且不符时返回 (False, size) 让上层重试。
    """
    sess = requests.Session()
    headers = file_stream_headers(cookie, group_id, include_cookie=False)
    resp = sess.get(download_url, headers=headers, timeout=CDN_TIMEOUT, stream=True)

    if resp.status_code in (401, 403):
        resp.close()
        logger.debug("ZSXQ CDN 无 Cookie 返回 %s，带 Cookie 重试 file_id=%s", resp.status_code, file_id)
        headers = file_stream_headers(cookie, group_id, include_cookie=True)
        resp = sess.get(download_url, headers=headers, timeout=CDN_TIMEOUT, stream=True)

    if resp.status_code != 200:
        body = ""
        try:
            body = resp.text[:200]
        except Exception:
            pass
        logger.warning("ZSXQ 下载失败 HTTP %s file_id=%s: %r", resp.status_code, file_id, body)
        resp.close()
        return False, 0

    size = 0
    with open(dest_path, "wb") as f:
        for chunk in resp.iter_content(8192):
            if chunk:
                f.write(chunk)
                size += len(chunk)
    resp.close()

    # 文件名以后缀为准（Content-Disposition 可能更准），回写 dest_path 由调用方处理
    if expected_size and size != expected_size:
        logger.warning(
            "ZSXQ 文件 %s 大小不匹配：收 %d / 预期 %d", file_id, size, expected_size
        )
        return False, size
    if size <= 0:
        return False, 0
    return True, size


# ---------- 文件名安全化（OSS key 用） ----------

def sanitize_filename(name: str) -> str:
    """OSS object key 安全化：去路径分隔符与空白，保留可读名。"""
    name = (name or "").strip()
    if not name:
        return "file"
    # 去掉路径分隔符与控制字符
    name = re.sub(r"[/\\]+", "_", name)
    name = re.sub(r"[\x00-\x1f]", "", name)
    # 折叠多余空白
    name = re.sub(r"\s+", "_", name)
    return name[:180] or "file"


def build_oss_object_name(group_id: str, file_id: str, name: str) -> str:
    """固定 OSS key（PUT 幂等，重传安全）：zsxq/{group_id}/{file_id}/{name}。"""
    return f"zsxq/{group_id}/{file_id}/{quote(sanitize_filename(name))}"
