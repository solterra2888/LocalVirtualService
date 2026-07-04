# -*- coding: utf-8 -*-
"""Fetch Weibo CDN (sinaimg.cn) images server-side.

Browser requests are blocked unless Referer is weibo.com / localhost.
``no-referrer`` still fails with a normal browser User-Agent.
"""

from __future__ import annotations

import re
from typing import Tuple
from urllib.parse import urlparse

import requests

_WEIBO_CDN_HOST_RE = re.compile(r"^(?:tvax?\d+|tva\d+|ww\d+)\.sinaimg\.cn$", re.IGNORECASE)
_FETCH_HEADERS = {
    "Referer": "https://weibo.com/",
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
}
_TIMEOUT = 15


def is_weibo_cdn_url(url: str) -> bool:
    if not url or not isinstance(url, str):
        return False
    try:
        parsed = urlparse(url.strip())
    except Exception:
        return False
    if parsed.scheme != "https":
        return False
    host = (parsed.hostname or "").lower()
    return bool(_WEIBO_CDN_HOST_RE.match(host))


def fetch_weibo_cdn_image(url: str) -> Tuple[bytes, str]:
    if not is_weibo_cdn_url(url):
        raise ValueError("URL is not an allowed Weibo CDN image")
    resp = requests.get(url.strip(), headers=_FETCH_HEADERS, timeout=_TIMEOUT)
    resp.raise_for_status()
    content_type = (resp.headers.get("Content-Type") or "image/jpeg").split(";")[0].strip()
    if not content_type.startswith("image/"):
        raise ValueError(f"Unexpected content type: {content_type}")
    return resp.content, content_type
