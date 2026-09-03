# -*- coding: utf-8 -*-
"""知识星球头像签名 URL 过期检测。"""

import time

from backend.services.zsxq_credential_service import (
    normalize_avatar_url,
    resolve_zsxq_avatar_url,
    zsxq_signed_url_expired,
)


def test_normalize_avatar_url_rejects_builtin_placeholder():
    assert normalize_avatar_url("builtin://1") == ""


def test_zsxq_signed_url_expired_past():
    past = int(time.time()) - 3600
    url = f"https://images.zsxq.com/abc?e={past}&token=x:y"
    assert zsxq_signed_url_expired(url) is True


def test_zsxq_signed_url_expired_future():
    future = int(time.time()) + 86400
    url = f"https://images.zsxq.com/abc?e={future}&token=x:y"
    assert zsxq_signed_url_expired(url) is False


def test_resolve_zsxq_avatar_url_drops_expired():
    past = int(time.time()) - 60
    url = f"https://images.zsxq.com/abc?e={past}&token=x:y"
    assert resolve_zsxq_avatar_url(url) == ""
