# -*- coding: utf-8 -*-
"""Fetch Knowledge Planet (zsxq) CDN images server-side.

Browser requests from non-zsxq origins get a hotlink placeholder
(「图片来自知识星球，未经允许不可引用」). Server fetch with
wx.zsxq.com Referer (or empty Referer) returns the real image.
"""

from __future__ import annotations

from typing import Tuple
from urllib.parse import urlparse

import requests

_FETCH_HEADERS = {
    "Referer": "https://wx.zsxq.com/",
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
}
_TIMEOUT = 15


def is_zsxq_cdn_url(url: str) -> bool:
    if not url or not isinstance(url, str):
        return False
    try:
        parsed = urlparse(url.strip())
    except Exception:
        return False
    if parsed.scheme != "https":
        return False
    host = (parsed.hostname or "").lower()
    return host == "images.zsxq.com" or host.endswith(".images.zsxq.com")


def fetch_zsxq_cdn_image(url: str) -> Tuple[bytes, str]:
    if not is_zsxq_cdn_url(url):
        raise ValueError("URL is not an allowed ZSXQ CDN image")
    resp = requests.get(url.strip(), headers=_FETCH_HEADERS, timeout=_TIMEOUT)
    resp.raise_for_status()
    content_type = (resp.headers.get("Content-Type") or "image/jpeg").split(";")[0].strip()
    if not content_type.startswith("image/"):
        raise ValueError(f"Unexpected content type: {content_type}")
    return resp.content, content_type
