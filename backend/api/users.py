# -*- coding: utf-8 -*-
"""
用户管理API路由
"""

import os
import tempfile
from fastapi import APIRouter, HTTPException, status, Depends, Query, UploadFile, File
from typing import List, Optional
from pydantic import BaseModel, Field

from backend.schemas.user import (
    UserCreateRequest,
    UserUpdateRequest,
    UserResponse
)
from backend.auth.jwt_utils import get_current_user, get_current_admin_user, get_password_hash
from backend.auth.user_auth import UserAuth
from backend.database.db_manager import DatabaseManager
from backend.utils.database_config import get_database_config
from backend.utils.avatar import coerce_avatar_for_storage, resolve_avatar_url
from backend.services.oss_service import OSSService

router = APIRouter()




@router.get("/", response_model=List[UserResponse], summary="获取用户列表")
async def get_users(
    current_user=Depends(get_current_admin_user),
    page: int = Query(1, ge=1, description="页码"),
    page_size: int = Query(20, ge=1, le=100, description="每页大小")
):
    """
    获取用户列表（仅管理员）
    
    - **page**: 页码
    - **page_size**: 每页大小
    """
    db_conn_params = get_database_config()
    user_auth = UserAuth(db_conn_params)
    
    # 获取用户列表
    offset = (page - 1) * page_size
    users = user_auth.get_users_list(limit=page_size, offset=offset)
    
    return [
        UserResponse(
            id=user['id'],
            username=user['username'],
            is_admin=user.get('is_admin', False),
            created_at=user.get('created_at'),
            last_login=user.get('last_login'),
            avatar_url=resolve_avatar_url(user.get('avatar_url'))
        )
        for user in users
    ]


@router.post("/", response_model=UserResponse, summary="创建用户")
async def create_user(
    request: UserCreateRequest,
    current_user=Depends(get_current_admin_user)
):
    """
    创建新用户（仅管理员）
    
    - **username**: 用户名
    - **password**: 密码
    - **is_admin**: 是否为管理员
    """
    db_conn_params = get_database_config()
    user_auth = UserAuth(db_conn_params)
    
    # 检查用户名是否已存在
    existing_user = user_auth.get_user_info(request.username)
    if existing_user:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"用户名 '{request.username}' 已存在"
        )
    
    try:
        # 创建用户
        password_hash = get_password_hash(request.password)
        user_id = user_auth.create_user(
            username=request.username,
            password_hash=password_hash,
            is_admin=request.is_admin
        )
        
        # 获取新创建的用户信息
        user_info = user_auth.get_user_info(request.username)
        
        return UserResponse(
            id=user_info['id'],
            username=user_info['username'],
            is_admin=user_info.get('is_admin', False),
            created_at=user_info.get('created_at'),
            last_login=user_info.get('last_login'),
            avatar_url=resolve_avatar_url(user_info.get('avatar_url'))
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"创建用户失败: {str(e)}"
        )


class ProfileUpdateRequest(BaseModel):
    username: Optional[str] = Field(None, min_length=1, max_length=50)
    avatar_url: Optional[str] = None


@router.get("/me/profile", response_model=UserResponse, summary="获取当前用户信息")
async def get_my_profile(current_user=Depends(get_current_user)):
    db_conn_params = get_database_config()
    user_auth = UserAuth(db_conn_params)
    user_info = user_auth.get_user_by_id(current_user.user_id)
    if not user_info:
        raise HTTPException(status_code=404, detail="用户不存在")
    return UserResponse(
        id=user_info['id'],
        username=user_info['username'],
        is_admin=user_info.get('is_admin', False),
        created_at=user_info.get('created_at'),
        last_login=user_info.get('last_login'),
        avatar_url=resolve_avatar_url(user_info.get('avatar_url'))
    )


@router.put("/me/profile", response_model=UserResponse, summary="更新当前用户信息")
async def update_my_profile(
    request: ProfileUpdateRequest,
    current_user=Depends(get_current_user)
):
    db_conn_params = get_database_config()
    user_auth = UserAuth(db_conn_params)

    update_data = {}
    if request.username is not None:
        existing = user_auth.get_user_info(request.username)
        if existing and existing['id'] != current_user.user_id:
            raise HTTPException(status_code=400, detail="用户名已被使用")
        update_data['username'] = request.username
    if request.avatar_url is not None:
        # 避免把 7 天过期的签名 URL 写入 DB（前端保存资料时常带上传接口返回的 signed_url）
        update_data['avatar_url'] = coerce_avatar_for_storage(request.avatar_url)

    if update_data:
        success = user_auth.update_user(current_user.user_id, **update_data)
        if not success:
            raise HTTPException(status_code=500, detail="更新失败")

    user_info = user_auth.get_user_by_id(current_user.user_id)
    return UserResponse(
        id=user_info['id'],
        username=user_info['username'],
        is_admin=user_info.get('is_admin', False),
        created_at=user_info.get('created_at'),
        last_login=user_info.get('last_login'),
        avatar_url=resolve_avatar_url(user_info.get('avatar_url'))
    )


@router.post("/me/avatar", summary="上传用户头像")
async def upload_avatar(
    file: UploadFile = File(...),
    current_user=Depends(get_current_user)
):
    allowed_ext = {'.jpg', '.jpeg', '.png', '.gif', '.webp'}
    ext = os.path.splitext(file.filename or '')[1].lower()
    if ext not in allowed_ext:
        raise HTTPException(status_code=400, detail=f"不支持的图片格式: {ext}")

    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=ext) as tmp:
            content = await file.read()
            if len(content) > 5 * 1024 * 1024:
                raise HTTPException(status_code=400, detail="头像文件不能超过5MB")
            tmp.write(content)
            tmp_path = tmp.name

        oss = OSSService()
        if not oss.is_configured():
            raise HTTPException(status_code=500, detail="OSS服务未配置")

        result = oss.upload_file(
            file_path=tmp_path,
            filename=f"avatar_{current_user.user_id}{ext}",
            username=current_user.username,
            file_category='image'
        )

        if not result.get('success'):
            raise HTTPException(status_code=500, detail=result.get('error', '上传失败'))

        # 私有 Bucket：数据库存 object_name，返回给前端时再生成签名 URL，避免链接过期
        object_name = result.get('object_name')
        db_conn_params = get_database_config()
        user_auth = UserAuth(db_conn_params)
        user_auth.update_user(current_user.user_id, avatar_url=object_name)

        # 响应中返回当前可用的签名 URL（7 天内有效）
        avatar_url = resolve_avatar_url(object_name)
        return {"avatar_url": avatar_url or result.get('signed_url') or result.get('public_url')}
    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.unlink(tmp_path)


@router.get("/shareable", summary="获取可分享的用户列表")
async def get_shareable_users(
    current_user=Depends(get_current_user)
):
    """
    返回所有可分享的目标用户（排除自己），任何登录用户均可调用。
    """
    db_conn_params = get_database_config()
    user_auth = UserAuth(db_conn_params)
    
    all_users = user_auth.get_users_list(limit=500, offset=0)
    
    return [
        {"id": u["id"], "username": u["username"], "avatar_url": resolve_avatar_url(u.get("avatar_url"))}
        for u in all_users
        if u["id"] != current_user.user_id
    ]


@router.get("/{user_id}", response_model=UserResponse, summary="获取用户详情")
async def get_user(
    user_id: int,
    current_user=Depends(get_current_admin_user)
):
    """
    获取特定用户的详情（仅管理员）
    
    - **user_id**: 用户ID
    """
    db_conn_params = get_database_config()
    user_auth = UserAuth(db_conn_params)
    
    user_info = user_auth.get_user_by_id(user_id)
    if not user_info:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"用户不存在: {user_id}"
        )
    
    return UserResponse(
        id=user_info['id'],
        username=user_info['username'],
        is_admin=user_info.get('is_admin', False),
        created_at=user_info.get('created_at'),
        last_login=user_info.get('last_login'),
        avatar_url=resolve_avatar_url(user_info.get('avatar_url'))
    )


@router.put("/{user_id}", response_model=UserResponse, summary="更新用户")
async def update_user(
    user_id: int,
    request: UserUpdateRequest,
    current_user=Depends(get_current_admin_user)
):
    """
    更新用户信息（仅管理员）
    
    - **user_id**: 用户ID
    - **password**: 新密码（可选）
    - **is_admin**: 是否为管理员（可选）
    """
    db_conn_params = get_database_config()
    user_auth = UserAuth(db_conn_params)
    
    # 检查用户是否存在
    user_info = user_auth.get_user_by_id(user_id)
    if not user_info:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"用户不存在: {user_id}"
        )
    
    try:
        # 更新用户信息
        update_data = {}
        
        # 检查是否要修改用户名
        if request.username:
            # 禁止修改admin账户的用户名
            if user_info['username'] == 'admin':
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="不能修改admin账户的用户名"
                )
            
            # 检查新用户名是否已被其他用户使用
            existing_user = user_auth.get_user_info(request.username)
            if existing_user and existing_user['id'] != user_id:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"用户名 '{request.username}' 已被其他用户使用"
                )
            
            update_data['username'] = request.username
        
        if request.password:
            update_data['password_hash'] = get_password_hash(request.password)
        if request.is_admin is not None:
            # 禁止取消admin账户的管理员权限
            if user_info['username'] == 'admin' and not request.is_admin:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="不能取消admin账户的管理员权限"
                )
            update_data['is_admin'] = request.is_admin
        
        if update_data:
            success = user_auth.update_user(user_id, **update_data)
            if not success:
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail="更新用户信息失败"
                )
        
        # 获取更新后的用户信息
        updated_user_info = user_auth.get_user_by_id(user_id)
        
        return UserResponse(
            id=updated_user_info['id'],
            username=updated_user_info['username'],
            is_admin=updated_user_info.get('is_admin', False),
            created_at=updated_user_info.get('created_at'),
            last_login=updated_user_info.get('last_login'),
            avatar_url=resolve_avatar_url(updated_user_info.get('avatar_url'))
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"更新用户失败: {str(e)}"
        )


@router.delete("/{user_id}", summary="删除用户")
async def delete_user(
    user_id: int,
    current_user=Depends(get_current_admin_user)
):
    """
    删除用户（仅管理员）
    
    - **user_id**: 用户ID
    """
    db_conn_params = get_database_config()
    user_auth = UserAuth(db_conn_params)
    
    # 检查用户是否存在
    user_info = user_auth.get_user_by_id(user_id)
    if not user_info:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"用户不存在: {user_id}"
        )
    
    # 不能删除自己
    if user_id == current_user.user_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="不能删除自己"
        )
    
    try:
        success = user_auth.delete_user(user_id)
        if not success:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="删除用户失败"
            )
        
        return {"message": "用户删除成功"}
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"删除用户失败: {str(e)}"
        )


@router.put("/{user_id}/password", summary="修改用户密码")
async def change_password(
    user_id: int,
    request: dict,  # 接收包含new_password的JSON请求
    current_user=Depends(get_current_user)
):
    """
    修改用户密码
    
    - **user_id**: 用户ID
    - **new_password**: 新密码
    """
    # 只能修改自己的密码或者管理员可以修改任何人的密码
    if user_id != current_user.user_id and not current_user.is_admin:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="权限不足，只能修改自己的密码"
        )
    
    db_conn_params = get_database_config()
    user_auth = UserAuth(db_conn_params)
    
    # 检查用户是否存在
    user_info = user_auth.get_user_by_id(user_id)
    if not user_info:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"用户不存在: {user_id}"
        )
    
    try:
        # 从请求中获取新密码
        new_password = request.get('new_password')
        if not new_password:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="新密码不能为空"
            )
        
        # 更新密码
        password_hash = get_password_hash(new_password)
        success = user_auth.update_user(user_id, password_hash=password_hash)
        
        if not success:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="修改密码失败"
            )
        
        return {"message": "密码修改成功"}
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"修改密码失败: {str(e)}"
        ) 