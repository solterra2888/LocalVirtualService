# -*- coding: utf-8 -*-
"""
头像 URL 解析：将数据库中存储的 OSS object_name 转为可访问的签名 URL。
私有 Bucket 下不应存储会过期的 signed_url，而应存 object_name，在返回给前端时按需生成签名 URL。
"""
from typing import Optional

from backend.services.oss_service import OSSService

# 签名 URL 有效期：7 天（私有桶头像展示）
AVATAR_SIGNED_URL_EXPIRES = 7 * 24 * 3600


def resolve_avatar_url(avatar_url: Optional[str]) -> Optional[str]:
    """
    将数据库中的 avatar 字段解析为可访问的 URL。
    - 若为 None 或空，返回 None。
    - 若已是完整 URL（以 http:// 或 https:// 开头），原样返回（兼容历史数据或外链）。
    - 否则视为 OSS object_name，生成带有效期的签名 URL 并返回。
    """
    if not avatar_url or not avatar_url.strip():
        return None
    raw = avatar_url.strip()
    if raw.startswith("http://") or raw.startswith("https://"):
        return raw
    oss = OSSService()
    if not oss.is_configured():
        return raw
    signed = oss.get_signed_url(raw, expires=AVATAR_SIGNED_URL_EXPIRES)
    return signed if signed else raw
