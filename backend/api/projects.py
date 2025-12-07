# -*- coding: utf-8 -*-
"""
投资项目管理API路由
"""

from fastapi import APIRouter, HTTPException, status, Depends, Query
from typing import List, Optional
from backend.schemas.project import (
    ProjectCreate, ProjectUpdate, ProjectResponse, ProjectListResponse, ProjectListQuery,
    MemberInvite, MemberUpdate, MemberResponse,
    MilestoneCreate, MilestoneUpdate, MilestoneResponse,
    ProjectFileCreate, ProjectFileResponse, AutoFetchNewsRequest, AutoFetchNewsResponse,
    ProjectDashboard, ProjectStatistics
)
from backend.schemas.user import UserResponse
from backend.services.investment_project_service import InvestmentProjectService
from backend.services.project_permission_service import ProjectPermissionService
from backend.auth.jwt_utils import get_current_user
from backend.utils.database_config import get_database_config
from backend.utils.logger import get_logger

router = APIRouter(prefix="/projects", tags=["Investment Projects"])
logger = get_logger("projects_api")


def get_project_service() -> InvestmentProjectService:
    """获取项目服务实例"""
    db_config = get_database_config()
    return InvestmentProjectService(db_config)


def get_permission_service() -> ProjectPermissionService:
    """获取权限服务实例"""
    db_config = get_database_config()
    return ProjectPermissionService(db_config)


# ==================== 项目CRUD ====================

@router.get("/", response_model=ProjectListResponse, summary="获取项目列表")
async def list_projects(
    status: Optional[str] = Query(None, description="项目状态筛选"),
    project_type: Optional[str] = Query(None, description="项目类型筛选"),
    priority: Optional[int] = Query(None, ge=1, le=3, description="优先级筛选"),
    search: Optional[str] = Query(None, description="搜索关键词"),
    created_by_me: Optional[bool] = Query(None, description="只看我创建的"),
    page: int = Query(1, ge=1, description="页码"),
    page_size: int = Query(20, ge=1, le=100, description="每页大小"),
    sort_by: str = Query("created_at", description="排序字段"),
    sort_order: str = Query("desc", description="排序方向"),
    current_user: UserResponse = Depends(get_current_user)
):
    """
    获取项目列表
    
    - 自动筛选用户有权访问的项目
    - 支持多种筛选和排序条件
    """
    try:
        service = get_project_service()
        
        filters = {
            'status': status,
            'project_type': project_type,
            'priority': priority,
            'search': search,
            'created_by_me': created_by_me,
            'sort_by': sort_by,
            'sort_order': sort_order
        }
        
        projects, total = service.list_projects(
            user_id=current_user.user_id,
            filters=filters,
            page=page,
            page_size=page_size
        )
        
        return ProjectListResponse(
            total=total,
            page=page,
            page_size=page_size,
            items=projects
        )
    
    except Exception as e:
        logger.error("获取项目列表失败", user_id=current_user.user_id, exception=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取项目列表失败: {str(e)}"
        )


@router.post("/", response_model=ProjectResponse, summary="创建项目")
async def create_project(
    project: ProjectCreate,
    current_user: UserResponse = Depends(get_current_user)
):
    """
    创建新项目
    
    - 创建者自动成为项目owner
    - 必须指定ticker、industry或sector之一
    """
    try:
        service = get_project_service()
        
        # 转换为字典
        project_data = project.model_dump()
        
        success, project_id, message = service.create_project(
            user_id=current_user.user_id,
            project_data=project_data
        )
        
        if not success:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=message
            )
        
        # 获取创建的项目
        created_project = service.get_project(project_id, current_user.user_id, include_stats=True)
        
        if not created_project:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="创建项目后获取失败"
            )
        
        return created_project
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error("创建项目失败", user_id=current_user.user_id, exception=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"创建项目失败: {str(e)}"
        )


@router.get("/{project_id}", response_model=ProjectResponse, summary="获取项目详情")
async def get_project(
    project_id: int,
    include_stats: bool = Query(True, description="是否包含统计信息"),
    current_user: UserResponse = Depends(get_current_user)
):
    """获取项目详情"""
    try:
        service = get_project_service()
        
        project = service.get_project(
            project_id=project_id,
            user_id=current_user.user_id,
            include_stats=include_stats
        )
        
        if not project:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="项目不存在或无权访问"
            )
        
        return project
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error("获取项目详情失败", project_id=project_id, exception=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取项目详情失败: {str(e)}"
        )


@router.put("/{project_id}", response_model=ProjectResponse, summary="更新项目")
async def update_project(
    project_id: int,
    project_update: ProjectUpdate,
    current_user: UserResponse = Depends(get_current_user)
):
    """更新项目信息（需要管理权限）"""
    try:
        service = get_project_service()
        
        # 转换为字典，排除None值
        update_data = project_update.model_dump(exclude_none=True)
        
        success, message = service.update_project(
            project_id=project_id,
            user_id=current_user.user_id,
            update_data=update_data
        )
        
        if not success:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=message
            )
        
        # 返回更新后的项目
        updated_project = service.get_project(project_id, current_user.user_id, include_stats=True)
        return updated_project
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error("更新项目失败", project_id=project_id, exception=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"更新项目失败: {str(e)}"
        )


@router.delete("/{project_id}", summary="删除项目")
async def delete_project(
    project_id: int,
    current_user: UserResponse = Depends(get_current_user)
):
    """删除项目（真正删除，需要owner权限）"""
    try:
        service = get_project_service()
        
        success, message = service.delete_project(
            project_id=project_id,
            user_id=current_user.user_id
        )
        
        if not success:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=message
            )
        
        return {"success": True, "message": message}
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error("删除项目失败", project_id=project_id, exception=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"删除项目失败: {str(e)}"
        )


# ==================== 项目成员管理 ====================

@router.get("/{project_id}/members", response_model=List[MemberResponse], summary="获取项目成员")
async def get_project_members(
    project_id: int,
    current_user: UserResponse = Depends(get_current_user)
):
    """获取项目成员列表"""
    try:
        service = get_project_service()
        
        members = service.get_project_members(
            project_id=project_id,
            user_id=current_user.user_id
        )
        
        return members
    
    except Exception as e:
        logger.error("获取项目成员失败", project_id=project_id, exception=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取项目成员失败: {str(e)}"
        )


@router.post("/{project_id}/members/invite", summary="邀请成员")
async def invite_members(
    project_id: int,
    invite: MemberInvite,
    current_user: UserResponse = Depends(get_current_user)
):
    """邀请成员加入项目"""
    try:
        service = get_project_service()
        
        success, count, message = service.invite_members(
            project_id=project_id,
            inviter_id=current_user.user_id,
            user_ids=invite.user_ids,
            role=invite.role,
            can_edit=invite.can_edit,
            can_invite=invite.can_invite
        )
        
        if not success:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=message
            )
        
        return {
            "success": True,
            "message": message,
            "invited_count": count
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error("邀请成员失败", project_id=project_id, exception=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"邀请成员失败: {str(e)}"
        )


@router.put("/{project_id}/members/{user_id}", summary="更新成员信息")
async def update_member(
    project_id: int,
    user_id: int,
    member_update: MemberUpdate,
    current_user: UserResponse = Depends(get_current_user)
):
    """更新成员角色和权限"""
    try:
        service = get_project_service()
        
        update_data = member_update.model_dump(exclude_none=True)
        
        success, message = service.update_member(
            project_id=project_id,
            operator_id=current_user.user_id,
            target_user_id=user_id,
            update_data=update_data
        )
        
        if not success:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=message
            )
        
        return {"success": True, "message": message}
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error("更新成员失败", project_id=project_id, exception=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"更新成员失败: {str(e)}"
        )


@router.delete("/{project_id}/members/{user_id}", summary="移除成员")
async def remove_member(
    project_id: int,
    user_id: int,
    current_user: UserResponse = Depends(get_current_user)
):
    """移除项目成员"""
    try:
        service = get_project_service()
        
        success, message = service.remove_member(
            project_id=project_id,
            operator_id=current_user.user_id,
            target_user_id=user_id
        )
        
        if not success:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=message
            )
        
        return {"success": True, "message": message}
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error("移除成员失败", project_id=project_id, exception=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"移除成员失败: {str(e)}"
        )


# ==================== 项目权限查询 ====================

@router.get("/{project_id}/permissions", summary="获取当前用户权限")
async def get_my_permissions(
    project_id: int,
    current_user: UserResponse = Depends(get_current_user)
):
    """获取当前用户在项目中的权限"""
    try:
        permission_service = get_permission_service()
        
        permissions = permission_service.get_member_permissions(
            user_id=current_user.user_id,
            project_id=project_id
        )
        
        role = permission_service.get_user_role_in_project(
            user_id=current_user.user_id,
            project_id=project_id
        )
        
        return {
            "role": role,
            "permissions": permissions
        }
    
    except Exception as e:
        logger.error("获取权限失败", project_id=project_id, exception=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取权限失败: {str(e)}"
        )


# ==================== 项目统计和仪表盘 ====================

@router.get("/{project_id}/statistics", response_model=ProjectStatistics, summary="获取项目统计")
async def get_project_statistics(
    project_id: int,
    current_user: UserResponse = Depends(get_current_user)
):
    """获取项目统计信息"""
    try:
        service = get_project_service()
        
        # 权限检查
        permission_service = get_permission_service()
        if not permission_service.check_project_permission(
            current_user.user_id, project_id, 'view'
        ):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="无权访问该项目"
            )
        
        stats = service._get_project_statistics(project_id)
        return stats
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error("获取项目统计失败", project_id=project_id, exception=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取项目统计失败: {str(e)}"
        )


# ==================== 健康检查 ====================

@router.get("/health/check", summary="健康检查")
async def health_check():
    """API健康检查"""
    return {
        "status": "healthy",
        "service": "Investment Projects API",
        "version": "1.0.0"
    }


# ==================== 项目里程碑管理 ====================

@router.get("/{project_id}/milestones", response_model=List[MilestoneResponse], summary="获取项目里程碑")
async def get_project_milestones(
    project_id: int,
    current_user: UserResponse = Depends(get_current_user)
):
    """获取项目里程碑列表"""
    try:
        service = get_project_service()
        
        milestones = service.get_project_milestones(
            project_id=project_id,
            user_id=current_user.user_id
        )
        
        return milestones
    
    except Exception as e:
        logger.error("获取项目里程碑失败", project_id=project_id, exception=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取项目里程碑失败: {str(e)}"
        )


@router.post("/{project_id}/milestones", response_model=MilestoneResponse, summary="创建里程碑")
async def create_milestone(
    project_id: int,
    milestone: MilestoneCreate,
    current_user: UserResponse = Depends(get_current_user)
):
    """创建项目里程碑"""
    try:
        service = get_project_service()
        
        milestone_data = milestone.model_dump()
        
        success, milestone_id, message = service.create_milestone(
            project_id=project_id,
            user_id=current_user.user_id,
            milestone_data=milestone_data
        )
        
        if not success:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=message
            )
        
        # 获取创建的里程碑
        created_milestone = service.get_milestone(milestone_id, current_user.user_id)
        return created_milestone
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error("创建里程碑失败", project_id=project_id, exception=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"创建里程碑失败: {str(e)}"
        )


@router.put("/{project_id}/milestones/{milestone_id}", response_model=MilestoneResponse, summary="更新里程碑")
async def update_milestone(
    project_id: int,
    milestone_id: int,
    milestone_update: MilestoneUpdate,
    current_user: UserResponse = Depends(get_current_user)
):
    """更新项目里程碑"""
    try:
        service = get_project_service()
        
        update_data = milestone_update.model_dump(exclude_none=True)
        
        success, message = service.update_milestone(
            milestone_id=milestone_id,
            user_id=current_user.user_id,
            update_data=update_data
        )
        
        if not success:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=message
            )
        
        # 返回更新后的里程碑
        updated_milestone = service.get_milestone(milestone_id, current_user.user_id)
        return updated_milestone
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error("更新里程碑失败", milestone_id=milestone_id, exception=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"更新里程碑失败: {str(e)}"
        )


@router.delete("/{project_id}/milestones/{milestone_id}", summary="删除里程碑")
async def delete_milestone(
    project_id: int,
    milestone_id: int,
    current_user: UserResponse = Depends(get_current_user)
):
    """删除项目里程碑"""
    try:
        service = get_project_service()
        
        success, message = service.delete_milestone(
            milestone_id=milestone_id,
            user_id=current_user.user_id
        )
        
        if not success:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=message
            )
        
        return {"success": True, "message": message}
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error("删除里程碑失败", milestone_id=milestone_id, exception=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"删除里程碑失败: {str(e)}"
        )


# ==================== 项目文件管理 ====================

@router.get("/{project_id}/files", response_model=List[ProjectFileResponse], summary="获取项目文件")
async def get_project_files(
    project_id: int,
    current_user: UserResponse = Depends(get_current_user)
):
    """获取项目文件列表"""
    try:
        service = get_project_service()
        
        files = service.get_project_files(
            project_id=project_id,
            user_id=current_user.user_id
        )
        
        return files
    
    except Exception as e:
        logger.error("获取项目文件失败", project_id=project_id, exception=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取项目文件失败: {str(e)}"
        )


@router.post("/{project_id}/files", response_model=ProjectFileResponse, summary="添加文件记录")
async def add_project_file(
    project_id: int,
    file_data: ProjectFileCreate,
    current_user: UserResponse = Depends(get_current_user)
):
    """添加文件记录到项目（文件上传需另行处理）"""
    try:
        service = get_project_service()
        
        file_dict = file_data.model_dump()
        
        success, file_id, message = service.add_project_file(
            project_id=project_id,
            user_id=current_user.user_id,
            file_data=file_dict
        )
        
        if not success:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=message
            )
        
        # 获取创建的文件记录
        created_file = service.get_project_file(file_id, current_user.user_id)
        return created_file
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error("添加文件记录失败", project_id=project_id, exception=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"添加文件记录失败: {str(e)}"
        )


@router.delete("/{project_id}/files/{file_id}", summary="删除文件")
async def delete_project_file(
    project_id: int,
    file_id: int,
    current_user: UserResponse = Depends(get_current_user)
):
    """删除项目文件"""
    try:
        service = get_project_service()
        
        success, message = service.delete_project_file(
            file_id=file_id,
            user_id=current_user.user_id
        )
        
        if not success:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=message
            )
        
        return {"success": True, "message": message}
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error("删除文件失败", file_id=file_id, exception=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"删除文件失败: {str(e)}"
        )

