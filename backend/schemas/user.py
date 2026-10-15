# -*- coding: utf-8 -*-
"""
用户相关数据模型
"""

from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import datetime

from backend.utils.datetime_utils import UtcDateTime


class UserLoginRequest(BaseModel):
    """用户登录请求模型"""
    username: str = Field(..., min_length=1, max_length=50, description="用户名")
    password: str = Field(..., min_length=1, description="密码")
    client_type: Optional[str] = Field(
        None,
        description="客户端类型：web（PC 浏览器）/ mobile（手机浏览器）/ extension（浏览器扩展）。"
        "各类型会话独立计数，同类型新登录会踢掉旧会话，不同类型可同时在线；缺省按 web 处理",
    )


class UserCreateRequest(BaseModel):
    """用户创建请求模型"""
    username: str = Field(..., min_length=1, max_length=50, description="用户名")
    password: str = Field(..., min_length=6, description="密码")
    is_admin: bool = Field(default=False, description="是否为管理员")


class UserUpdateRequest(BaseModel):
    """用户更新请求模型"""
    username: Optional[str] = Field(None, min_length=1, max_length=50, description="用户名")
    password: Optional[str] = Field(None, min_length=6, description="新密码")
    is_admin: Optional[bool] = Field(None, description="是否为管理员")
    avatar_url: Optional[str] = Field(None, description="头像URL")


class UserResponse(BaseModel):
    """用户响应模型"""
    id: int
    username: str
    is_admin: bool
    created_at: UtcDateTime
    last_login: Optional[UtcDateTime] = None
    avatar_url: Optional[str] = None
    
    class Config:
        from_attributes = True


class LoginResponse(BaseModel):
    """登录响应模型"""
    access_token: str
    token_type: str = "bearer"
    user: UserResponse


class TokenData(BaseModel):
    """Token数据模型"""
    username: Optional[str] = None
    user_id: Optional[int] = None
    is_admin: Optional[bool] = None
    token_version: Optional[int] = None
    # 会话所属客户端类型（web / mobile / extension），决定校验哪个会话版本计数
    client_type: str = "web"
    # Agent PAT (Personal Access Token) — only set when auth_kind == "pat"
    auth_kind: str = Field(default="jwt", description="jwt | pat")
    pat_id: Optional[int] = None
    pat_scopes: Optional[List[str]] = None