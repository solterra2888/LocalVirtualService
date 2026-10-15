# -*- coding: utf-8 -*-
"""会话版本按客户端类型独立计数的行为测试（web/mobile/extension 互不挤占）."""

from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException
from jose import jwt as jose_jwt

from backend.auth.jwt_utils import (
    ALGORITHM,
    SECRET_KEY,
    SESSION_REPLACED_CODE,
    create_access_token,
    verify_token,
)
from backend.auth.user_auth import (
    SESSION_VERSION_COLUMNS,
    normalize_client_type,
    session_version_column,
)


class TestNormalizeClientType:
    def test_known_types_passthrough(self):
        assert normalize_client_type("web") == "web"
        assert normalize_client_type("mobile") == "mobile"
        assert normalize_client_type("extension") == "extension"

    def test_unknown_or_missing_falls_back_to_web(self):
        assert normalize_client_type(None) == "web"
        assert normalize_client_type("") == "web"
        assert normalize_client_type("iphone") == "web"
        # 大小写敏感，未识别值一律按 web
        assert normalize_client_type("Mobile") == "web"

    def test_column_mapping(self):
        assert SESSION_VERSION_COLUMNS["web"] == "token_version"
        assert session_version_column("mobile") == "token_version_mobile"
        assert session_version_column("extension") == "token_version_extension"
        assert session_version_column("unknown") == "token_version"


def _make_token(tv: int, ct=None) -> str:
    data = {"sub": "alice", "user_id": 1, "is_admin": False, "tv": tv}
    if ct is not None:
        data["ct"] = ct
    return create_access_token(data)


def _mock_user_auth(db_tv):
    mocked = MagicMock()
    mocked.return_value.get_token_version.return_value = db_tv
    return mocked


class TestVerifyToken:
    def test_mobile_token_checks_mobile_counter(self):
        mocked = _mock_user_auth(3)
        with patch("backend.auth.jwt_utils.get_database_config"), \
             patch("backend.auth.jwt_utils.UserAuth", mocked):
            token_data = verify_token(_make_token(tv=3, ct="mobile"))

        assert token_data.client_type == "mobile"
        mocked.return_value.get_token_version.assert_called_once_with(
            "alice", client_type="mobile"
        )

    def test_get_token_version_called_with_client_type(self):
        mocked = _mock_user_auth(3)
        with patch("backend.auth.jwt_utils.get_database_config"), \
             patch("backend.auth.jwt_utils.UserAuth", mocked):
            verify_token(_make_token(tv=3, ct="mobile"))

        mocked.return_value.get_token_version.assert_called_once_with(
            "alice", client_type="mobile"
        )

    def test_legacy_token_without_ct_treated_as_web(self):
        mocked = _mock_user_auth(2)
        with patch("backend.auth.jwt_utils.get_database_config"), \
             patch("backend.auth.jwt_utils.UserAuth", mocked):
            token_data = verify_token(_make_token(tv=2))

        assert token_data.client_type == "web"
        mocked.return_value.get_token_version.assert_called_once_with(
            "alice", client_type="web"
        )

    def test_same_type_version_mismatch_raises_session_replaced(self):
        # mobile 计数被新登录递增到 4，旧 mobile token（tv=3）应被踢
        mocked = _mock_user_auth(4)
        with patch("backend.auth.jwt_utils.get_database_config"), \
             patch("backend.auth.jwt_utils.UserAuth", mocked):
            with pytest.raises(HTTPException) as exc_info:
                verify_token(_make_token(tv=3, ct="mobile"))

        assert exc_info.value.status_code == 401
        assert exc_info.value.detail["code"] == SESSION_REPLACED_CODE

    def test_other_type_counter_unchanged_keeps_token_valid(self):
        # PC 登录只递增 web 计数，mobile=2 的旧 token 校验 mobile 计数仍为 2 → 有效
        mocked = _mock_user_auth(2)
        with patch("backend.auth.jwt_utils.get_database_config"), \
             patch("backend.auth.jwt_utils.UserAuth", mocked):
            token_data = verify_token(_make_token(tv=2, ct="mobile"))

        assert token_data.username == "alice"
        assert token_data.client_type == "mobile"


_USER_INFO = {
    "id": 1,
    "username": "alice",
    "password_hash": "x",
    "salt": "",
    "is_admin": False,
    "created_at": "2026-01-01T00:00:00",
    "last_login": None,
    "avatar_url": None,
}


def _login(client_type=None):
    # 环境内 starlette TestClient 与 httpx 不兼容，直接调用端点函数
    import asyncio
    from backend.api.auth import login
    from backend.schemas.user import UserLoginRequest

    payload = {"username": "alice", "password": "pw"}
    if client_type is not None:
        payload["client_type"] = client_type
    mocked_user_auth = MagicMock()
    mocked_user_auth.increment_token_version.return_value = 7
    mocked_user_auth.get_token_version.return_value = 6
    with patch("backend.api.auth.authenticate_user", return_value=_USER_INFO), \
         patch("backend.api.auth.get_database_config"), \
         patch("backend.api.auth.resolve_avatar_url", side_effect=lambda v: v), \
         patch("backend.api.auth.UserAuth", return_value=mocked_user_auth):
        resp = asyncio.run(login(UserLoginRequest(**payload)))
    claims = jose_jwt.decode(resp.access_token, SECRET_KEY, algorithms=[ALGORITHM])
    return mocked_user_auth, claims


class TestLoginEndpoint:
    def test_mobile_login_increments_mobile_counter_only(self):
        mocked_user_auth, claims = _login("mobile")

        mocked_user_auth.increment_token_version.assert_called_once_with(
            "alice", client_type="mobile"
        )
        mocked_user_auth.get_token_version.assert_not_called()
        assert claims["ct"] == "mobile"
        assert claims["tv"] == 7

    def test_extension_login_never_increments(self):
        mocked_user_auth, claims = _login("extension")

        mocked_user_auth.increment_token_version.assert_not_called()
        mocked_user_auth.get_token_version.assert_called_once_with(
            "alice", client_type="extension"
        )
        assert claims["ct"] == "extension"
        assert claims["tv"] == 6

    def test_missing_client_type_defaults_to_web(self):
        mocked_user_auth, claims = _login()

        mocked_user_auth.increment_token_version.assert_called_once_with(
            "alice", client_type="web"
        )
        assert claims["ct"] == "web"
