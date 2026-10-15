# -*- coding: utf-8 -*-
"""
JWT认证工具
"""

import os
from jose import jwt
from datetime import datetime, timedelta
from typing import Optional
from fastapi import HTTPException, status, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from passlib.context import CryptContext

from backend.schemas.user import TokenData
from backend.auth.user_auth import UserAuth, normalize_client_type
from backend.utils.database_config import get_database_config
from backend.utils.datetime_utils import utcnow
from config.settings import config

# 密码加密上下文
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# HTTP Bearer认证
security = HTTPBearer()

# JWT配置
SECRET_KEY = os.getenv("JWT_SECRET_KEY", "your-secret-key-here")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 480  # 8小时


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """验证密码"""
    return pwd_context.verify(plain_password, hashed_password)


def get_password_hash(password: str) -> str:
    """获取密码哈希"""
    return pwd_context.hash(password)


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    """创建访问令牌"""
    to_encode = data.copy()
    # JWT exp 标准是 UTC 时间戳, 用 utcnow() (aware UTC) 替代 deprecated datetime.utcnow().
    if expires_delta:
        expire = utcnow() + expires_delta
    else:
        expire = utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


# 前端根据 code === SESSION_REPLACED 显示英文 Modal；message 为用户可见文案
SESSION_REPLACED_CODE = "SESSION_REPLACED"
SESSION_REPLACED_DETAIL = {
    "code": SESSION_REPLACED_CODE,
    "message": (
        "This account was signed in on another device. "
        "Your session here has ended. Please sign in again to continue."
    ),
}


def verify_token(token: str) -> TokenData:
    """验证 JWT 并与数据库中对应客户端类型的会话版本比对（同类型新登录会使旧 token 失效）"""
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        user_id: int = payload.get("user_id")
        is_admin: bool = payload.get("is_admin", False)
        tv_claim = payload.get("tv")
        if tv_claim is None:
            tv_claim = 0
        # 旧 token 没有 ct 声明，按 web 处理（与拆分前的校验行为一致）
        client_type = normalize_client_type(payload.get("ct"))

        if username is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="无效的认证凭据",
                headers={"WWW-Authenticate": "Bearer"},
            )

        db_conn_params = get_database_config()
        user_auth = UserAuth(db_conn_params)
        db_tv = user_auth.get_token_version(username, client_type=client_type)
        if db_tv == -1:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="无效的认证凭据",
                headers={"WWW-Authenticate": "Bearer"},
            )
        if db_tv is not None and int(tv_claim) != int(db_tv):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail=SESSION_REPLACED_DETAIL,
                headers={"WWW-Authenticate": "Bearer"},
            )

        return TokenData(
            username=username,
            user_id=user_id,
            is_admin=is_admin,
            token_version=int(tv_claim),
            client_type=client_type,
        )
    except HTTPException:
        raise
    except jwt.JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="无效的认证凭据",
            headers={"WWW-Authenticate": "Bearer"},
        )


async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)) -> TokenData:
    """获取当前用户"""
    token = credentials.credentials
    return verify_token(token)


async def get_current_admin_user(current_user: TokenData = Depends(get_current_user)) -> TokenData:
    """获取当前管理员用户"""
    if not current_user.is_admin:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="权限不足，需要管理员权限"
        )
    return current_user


def authenticate_user(username: str, password: str, db_conn_params: dict) -> Optional[dict]:
    """认证用户"""
    user_auth = UserAuth(db_conn_params)
    
    # 获取用户信息
    user_info = user_auth.get_user_info(username)
    if not user_info:
        return None
    
    stored_hash = user_info.get('password_hash', '')
    stored_salt = user_info.get('salt', '')
    
    # 判断密码哈希格式并选择相应的验证方法
    password_valid = False
    
    if stored_hash.startswith('$2b$'):
        # bcrypt格式 - 使用passlib验证
        try:
            password_valid = verify_password(password, stored_hash)
        except Exception as e:
            print(f"bcrypt验证失败: {e}")
            password_valid = False
    else:
        # PBKDF2格式 - 使用UserAuth验证
        password_valid = UserAuth.verify_password(stored_hash, stored_salt, password)
    
    if not password_valid:
        return None
    
    # 更新最后登录时间
    user_auth.update_last_login(username)
    
    return user_info 