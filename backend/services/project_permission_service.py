# -*- coding: utf-8 -*-
"""
投资项目权限管理服务
"""

from typing import Dict, Any, Optional
from backend.database.db_manager import DatabaseManager
from backend.utils.logger import get_logger

logger = get_logger("project_permission_service")


class ProjectPermissionService:
    """项目权限管理服务"""
    
    def __init__(self, db_config: Dict[str, Any]):
        """初始化服务"""
        self.db_manager = DatabaseManager(db_config)
        logger.info("项目权限管理服务初始化完成")
    
    def check_project_permission(
        self,
        user_id: int,
        project_id: int,
        action: str
    ) -> bool:
        """
        检查用户对项目的操作权限
        
        Args:
            user_id: 用户ID
            project_id: 项目ID
            action: 操作类型
                - 'view': 查看项目
                - 'edit': 编辑项目内容
                - 'manage': 管理项目（修改项目设置）
                - 'invite': 邀请成员
                - 'delete': 删除项目
                - 'transfer': 转让所有权
        
        Returns:
            bool: 是否有权限
        """
        try:
            # 查询项目和成员信息
            query = """
            SELECT 
                p.created_by,
                p.owner_id,
                p.status,
                pm.role,
                pm.can_edit,
                pm.can_invite,
                pm.status as member_status
            FROM investment_projects p
            LEFT JOIN project_members pm ON p.id = pm.project_id AND pm.user_id = %s
            WHERE p.id = %s
            """
            
            result = self.db_manager.fetch_one(query, [user_id, project_id])
            
            if not result:
                logger.warning("项目不存在", project_id=project_id)
                return False
            
            created_by, owner_id, project_status, role, can_edit, can_invite, member_status = result
            
            # 项目已归档，只有owner可以查看
            if project_status == 'archived' and action != 'view':
                if user_id != owner_id:
                    logger.warning("项目已归档，只有owner可以操作", 
                                 user_id=user_id, project_id=project_id)
                    return False
            
            # 检查是否是成员
            if role is None:
                logger.warning("用户不是项目成员", 
                             user_id=user_id, project_id=project_id)
                return False
            
            # 成员状态必须是active
            if member_status != 'active':
                logger.warning("成员状态非active", 
                             user_id=user_id, project_id=project_id, status=member_status)
                return False
            
            # 权限检查逻辑
            if action == 'view':
                # 所有成员都可以查看
                return True
            
            elif action == 'edit':
                # owner, admin, 以及设置了can_edit=True的member可以编辑
                if role in ['owner', 'admin']:
                    return True
                if role == 'member' and can_edit:
                    return True
                return False
            
            elif action == 'invite':
                # owner, admin, 以及设置了can_invite=True的member可以邀请
                if role in ['owner', 'admin']:
                    return True
                if role == 'member' and can_invite:
                    return True
                return False
            
            elif action == 'manage':
                # 只有owner和admin可以管理项目设置
                return role in ['owner', 'admin']
            
            elif action == 'delete':
                # 只有owner可以删除项目
                return role == 'owner'
            
            elif action == 'transfer':
                # 只有owner可以转让所有权
                return role == 'owner'
            
            else:
                logger.warning("未知的操作类型", action=action)
                return False
        
        except Exception as e:
            logger.error("检查权限失败", 
                        user_id=user_id, 
                        project_id=project_id, 
                        action=action,
                        exception=str(e))
            return False
    
    def get_user_role_in_project(
        self,
        user_id: int,
        project_id: int
    ) -> Optional[str]:
        """
        获取用户在项目中的角色
        
        Args:
            user_id: 用户ID
            project_id: 项目ID
        
        Returns:
            Optional[str]: 角色名称，如果不是成员则返回None
        """
        try:
            query = """
            SELECT role, status
            FROM project_members
            WHERE user_id = %s AND project_id = %s
            """
            
            result = self.db_manager.fetch_one(query, [user_id, project_id])
            
            if result and result[1] == 'active':
                return result[0]
            return None
        
        except Exception as e:
            logger.error("获取用户角色失败", 
                        user_id=user_id, 
                        project_id=project_id,
                        exception=str(e))
            return None
    
    def is_project_member(
        self,
        user_id: int,
        project_id: int
    ) -> bool:
        """
        检查用户是否是项目成员
        
        Args:
            user_id: 用户ID
            project_id: 项目ID
        
        Returns:
            bool: 是否是成员
        """
        try:
            query = """
            SELECT 1
            FROM project_members
            WHERE user_id = %s AND project_id = %s AND status = 'active'
            """
            
            result = self.db_manager.fetch_one(query, [user_id, project_id])
            return result is not None
        
        except Exception as e:
            logger.error("检查成员身份失败", 
                        user_id=user_id, 
                        project_id=project_id,
                        exception=str(e))
            return False
    
    def can_user_access_project(
        self,
        user_id: int,
        project_id: int
    ) -> bool:
        """
        检查用户是否可以访问项目（查看权限）
        
        Args:
            user_id: 用户ID
            project_id: 项目ID
        
        Returns:
            bool: 是否可以访问
        """
        return self.check_project_permission(user_id, project_id, 'view')
    
    def get_member_permissions(
        self,
        user_id: int,
        project_id: int
    ) -> Dict[str, bool]:
        """
        获取用户在项目中的所有权限
        
        Args:
            user_id: 用户ID
            project_id: 项目ID
        
        Returns:
            Dict[str, bool]: 权限字典
        """
        permissions = {
            'can_view': False,
            'can_edit': False,
            'can_invite': False,
            'can_manage': False,
            'can_delete': False,
            'can_transfer': False
        }
        
        try:
            # 批量检查权限
            actions = ['view', 'edit', 'invite', 'manage', 'delete', 'transfer']
            for action in actions:
                permissions[f'can_{action}'] = self.check_project_permission(
                    user_id, project_id, action
                )
            
            return permissions
        
        except Exception as e:
            logger.error("获取成员权限失败", 
                        user_id=user_id, 
                        project_id=project_id,
                        exception=str(e))
            return permissions
    
    def validate_member_operation(
        self,
        operator_id: int,
        target_user_id: int,
        project_id: int,
        operation: str
    ) -> tuple[bool, str]:
        """
        验证成员操作是否合法
        
        Args:
            operator_id: 操作者ID
            target_user_id: 目标用户ID
            project_id: 项目ID
            operation: 操作类型（add, update, remove）
        
        Returns:
            tuple[bool, str]: (是否合法, 错误消息)
        """
        try:
            # 获取操作者和目标用户的角色
            operator_role = self.get_user_role_in_project(operator_id, project_id)
            target_role = self.get_user_role_in_project(target_user_id, project_id)
            
            if operation == 'add':
                # 添加成员需要邀请权限
                if not self.check_project_permission(operator_id, project_id, 'invite'):
                    return False, "没有邀请成员的权限"
                return True, ""
            
            elif operation == 'update':
                # 更新成员信息
                if operator_id == target_user_id:
                    # 不能修改自己的角色和权限
                    return False, "不能修改自己的角色和权限"
                
                if operator_role == 'owner':
                    # owner可以修改所有人
                    return True, ""
                
                if operator_role == 'admin':
                    # admin不能修改owner
                    if target_role == 'owner':
                        return False, "admin不能修改owner的权限"
                    return True, ""
                
                return False, "没有修改成员权限"
            
            elif operation == 'remove':
                # 移除成员
                if operator_id == target_user_id:
                    # 可以退出项目（但owner不能退出，需要先转让）
                    if operator_role == 'owner':
                        return False, "owner不能退出项目，请先转让所有权"
                    return True, ""
                
                if operator_role == 'owner':
                    # owner可以移除所有人
                    return True, ""
                
                if operator_role == 'admin':
                    # admin不能移除owner
                    if target_role == 'owner':
                        return False, "admin不能移除owner"
                    return True, ""
                
                return False, "没有移除成员的权限"
            
            else:
                return False, f"未知的操作类型: {operation}"
        
        except Exception as e:
            logger.error("验证成员操作失败", 
                        operator_id=operator_id,
                        target_user_id=target_user_id,
                        project_id=project_id,
                        operation=operation,
                        exception=str(e))
            return False, f"验证失败: {str(e)}"

