# -*- coding: utf-8 -*-
"""
头像 URL 解析：将数据库中存储的 OSS object_name 转为可访问的签名 URL。
私有 Bucket 下不应存储会过期的 signed_url，而应存 object_name，在返回给前端时按需生成签名 URL。
"""
import os
from typing import Optional
from urllib.parse import parse_qs, unquote, urlparse

from backend.services.oss_service import OSSService

# 签名 URL 有效期：7 天（私有桶头像展示）
AVATAR_SIGNED_URL_EXPIRES = 7 * 24 * 3600


def _is_our_oss_host(host: str) -> bool:
    if not host:
        return False
    h = host.lower()
    custom = (os.getenv("OSS_CUSTOM_DOMAIN") or "").strip().lower()
    if custom:
        ch = custom.split(":")[0]
        if h == ch:
            return True
    bucket = (os.getenv("OSS_BUCKET_NAME") or "").strip().lower()
    endpoint = (os.getenv("OSS_ENDPOINT") or "").strip().lower()
    if bucket and endpoint and h == f"{bucket}.{endpoint}":
        return True
    return False


def _query_looks_like_oss_signature(query: str) -> bool:
    if not query:
        return False
    qs = parse_qs(query, keep_blank_values=True)
    keys = {k.lower() for k in qs.keys()}
    if "signature" in keys or "x-oss-signature" in keys:
        return True
    if "expires" in keys and (
        "ossaccesskeyid" in keys or "security-token" in keys or "securitytoken" in keys
    ):
        return True
    return False


def object_key_from_our_oss_signed_url(url: str) -> Optional[str]:
    """
    若 url 为本环境 OSS 生成的签名 URL，返回 object key（与 DB 中应存的 object_name 一致）。
    否则返回 None（调用方继续按 object_name 或外链处理）。
    """
    try:
        parsed = urlparse(url.strip())
    except Exception:
        return None
    if parsed.scheme not in ("http", "https"):
        return None
    if not _is_our_oss_host(parsed.netloc):
        return None
    if not _query_looks_like_oss_signature(parsed.query):
        return None
    path = unquote(parsed.path or "")
    key = path.lstrip("/")
    return key if key else None


def coerce_avatar_for_storage(avatar_url: Optional[str]) -> Optional[str]:
    """
    写入 users.avatar_url 前规范化：禁止把带过期时间的 OSS 签名 URL 存进库。
    前端在保存资料时可能把 resolve 后的 signed_url 原样提交，此处还原为 object key。
    """
    if avatar_url is None:
        return None
    s = avatar_url.strip()
    if not s:
        return s
    extracted = object_key_from_our_oss_signed_url(s)
    return extracted if extracted is not None else s


def resolve_avatar_url(avatar_url: Optional[str]) -> Optional[str]:
    """
    将数据库中的 avatar 字段解析为可访问的 URL。
    - 若为 None 或空，返回 None。
    - 若误存为 OSS 签名 URL（历史 bug），先还原为 object_name 再签名。
    - 若为稳定外链（完整 http(s) URL 且非本桶签名链接），原样返回。
    - 否则视为 OSS object_name，生成带有效期的签名 URL 并返回。
    """
    if not avatar_url or not avatar_url.strip():
        return None
    raw = avatar_url.strip()
    coerced = object_key_from_our_oss_signed_url(raw)
    if coerced is not None:
        raw = coerced
    if raw.startswith("http://") or raw.startswith("https://"):
        return raw
    oss = OSSService()
    if not oss.is_configured():
        return raw
    signed = oss.get_signed_url(raw, expires=AVATAR_SIGNED_URL_EXPIRES)
    return signed if signed else raw
