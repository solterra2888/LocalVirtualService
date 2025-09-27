# -*- coding: utf-8 -*-
"""
认证API路由
"""

from fastapi import APIRouter, HTTPException, status, Depends
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from datetime import datetime, timedelta
from typing import Dict, Any

from backend.schemas.user import UserLoginRequest, LoginResponse, UserResponse
from backend.auth.jwt_utils import (
    authenticate_user,
    create_access_token,
    get_current_user,
    ACCESS_TOKEN_EXPIRE_MINUTES
)
from backend.auth.user_auth import UserAuth
from backend.database.db_manager import DatabaseManager
from backend.utils.database_config import get_database_config

router = APIRouter()



@router.post("/login", response_model=LoginResponse, summary="用户登录")
async def login(login_request: UserLoginRequest):
    """
    用户登录
    
    - **username**: 用户名
    - **password**: 密码
    """
    db_conn_params = get_database_config()
    
    # 认证用户
    user_info = authenticate_user(
        login_request.username, 
        login_request.password, 
        db_conn_params
    )
    
    if not user_info:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="用户名或密码错误",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    # 创建访问令牌
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={
            "sub": user_info['username'],
            "user_id": user_info['id'],
            "is_admin": user_info.get('is_admin', False)
        },
        expires_delta=access_token_expires
    )
    
    # 构造用户响应
    user_response = UserResponse(
        id=user_info['id'],
        username=user_info['username'],
        is_admin=user_info.get('is_admin', False),
        created_at=user_info.get('created_at', datetime.now()),
        last_login=user_info.get('last_login')
    )
    
    return LoginResponse(
        access_token=access_token,
        token_type="bearer",
        user=user_response
    )


@router.get("/me", response_model=UserResponse, summary="获取当前用户信息")
async def get_current_user_info(current_user=Depends(get_current_user)):
    """
    获取当前登录用户的信息
    """
    db_conn_params = get_database_config()
    user_auth = UserAuth(db_conn_params)
    
    user_info = user_auth.get_user_info(current_user.username)
    if not user_info:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="用户不存在"
        )
    
    return UserResponse(
        id=user_info['id'],
        username=user_info['username'],
        is_admin=user_info.get('is_admin', False),
        created_at=user_info.get('created_at', datetime.now()),
        last_login=user_info.get('last_login')
    )


@router.post("/logout", summary="用户登出")
async def logout(current_user=Depends(get_current_user)):
    """
    用户登出
    
    注意：JWT是无状态的，实际的登出需要客户端删除token
    """
    return {"message": "登出成功"}


@router.get("/verify", summary="验证Token")
async def verify_token(current_user=Depends(get_current_user)):
    """
    验证当前Token是否有效
    """
    return {
        "valid": True,
        "username": current_user.username,
        "is_admin": current_user.is_admin
    } 