# -*- coding: utf-8 -*-
"""
用户相关数据模型
"""

from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime


class UserLoginRequest(BaseModel):
    """用户登录请求模型"""
    username: str = Field(..., min_length=1, max_length=50, description="用户名")
    password: str = Field(..., min_length=1, description="密码")
    client_type: Optional[str] = Field(None, description="客户端类型，extension 表示浏览器扩展登录（不会踢掉其他会话）")


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
    created_at: datetime
    last_login: Optional[datetime] = None
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