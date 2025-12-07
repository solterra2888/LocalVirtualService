# -*- coding: utf-8 -*-
"""
投资项目管理核心服务
"""

from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, date
from backend.database.db_manager import DatabaseManager
from backend.services.project_permission_service import ProjectPermissionService
from backend.utils.logger import get_logger

logger = get_logger("investment_project_service")


class InvestmentProjectService:
    """投资项目管理核心服务"""
    
    def __init__(self, db_config: Dict[str, Any]):
        """初始化服务"""
        self.db_manager = DatabaseManager(db_config)
        self.permission_service = ProjectPermissionService(db_config)
        logger.info("投资项目管理服务初始化完成")
    
    def _generate_project_code(self, project_type: str, identifier: str) -> str:
        """
        生成项目编号
        
        Args:
            project_type: 项目类型
            identifier: 标识符（ticker/industry/sector）
        
        Returns:
            str: 项目编号，格式：PROJ-{IDENTIFIER}-{DATE}
        """
        today = datetime.now().strftime("%Y%m%d")
        type_prefix = project_type.upper()[:4]
        id_part = identifier.replace('.', '').replace(' ', '-')[:15] if identifier else 'GENERAL'
        return f"PROJ-{id_part}-{today}"
    
    # ==================== 项目CRUD ====================
    
    def create_project(
        self,
        user_id: int,
        project_data: Dict[str, Any]
    ) -> Tuple[bool, Optional[int], str]:
        """
        创建投资项目
        
        Args:
            user_id: 创建者ID
            project_data: 项目数据
        
        Returns:
            Tuple[bool, Optional[int], str]: (成功标志, 项目ID, 消息)
        """
        try:
            # 验证项目类型和关联信息
            project_type = project_data.get('project_type')
            tickers = project_data.get('tickers', [])
            industry = project_data.get('industry')
            sector = project_data.get('sector')
            
            # 确保tickers是列表
            if tickers is None:
                tickers = []
            elif isinstance(tickers, str):
                tickers = [tickers]
            
            # 清理tickers：去重、去空、转大写
            tickers = list(set([t.strip().upper() for t in tickers if t and t.strip()]))
            project_data['tickers'] = tickers
            
            if not tickers and not industry and not sector:
                return False, None, "必须指定至少一个ticker、industry或sector"
            
            # 生成项目编号 - 优先使用第一个ticker，否则用industry或sector
            identifier = tickers[0] if tickers else (industry or sector)
            project_code = self._generate_project_code(project_type, identifier)
            
            # 检查项目编号是否已存在
            check_query = "SELECT id FROM investment_projects WHERE project_code = %s"
            existing = self.db_manager.fetch_one(check_query, [project_code])
            if existing:
                # 如果编号重复，添加随机后缀
                import random
                project_code = f"{project_code}-{random.randint(1000, 9999)}"
            
            # 插入项目
            insert_query = """
            INSERT INTO investment_projects (
                project_code, project_name, project_type,
                tickers, industry, sector, region,
                status, priority, description,
                current_position,
                start_date,
                created_by, owner_id
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            RETURNING id
            """
            
            values = [
                project_code,
                project_data.get('project_name'),
                project_type,
                tickers,  # PostgreSQL数组
                industry,
                sector,
                project_data.get('region'),
                project_data.get('status', 'active'),
                project_data.get('priority', 2),
                project_data.get('description'),
                project_data.get('current_position'),
                project_data.get('start_date'),
                user_id,  # created_by
                user_id   # owner_id
            ]
            
            result = self.db_manager.fetch_one(insert_query, values)
            
            if not result:
                return False, None, "创建项目失败"
            
            project_id = result[0]
            
            # 自动将创建者添加为owner成员
            member_insert_query = """
            INSERT INTO project_members (
                project_id, user_id, role, can_edit, can_invite, status
            ) VALUES (%s, %s, 'owner', true, true, 'active')
            """
            self.db_manager.execute_query(member_insert_query, [project_id, user_id])
            
            logger.info("创建项目成功", 
                       project_id=project_id, 
                       project_code=project_code,
                       user_id=user_id)
            
            return True, project_id, "创建项目成功"
        
        except Exception as e:
            logger.error("创建项目失败", user_id=user_id, exception=str(e))
            return False, None, f"创建项目失败: {str(e)}"
    
    def get_project(
        self,
        project_id: int,
        user_id: Optional[int] = None,
        include_stats: bool = False
    ) -> Optional[Dict[str, Any]]:
        """
        获取项目详情
        
        Args:
            project_id: 项目ID
            user_id: 用户ID（用于权限检查）
            include_stats: 是否包含统计信息
        
        Returns:
            Optional[Dict[str, Any]]: 项目信息
        """
        try:
            # 权限检查
            if user_id and not self.permission_service.check_project_permission(
                user_id, project_id, 'view'
            ):
                logger.warning("用户无权查看项目", user_id=user_id, project_id=project_id)
                return None
            
            # 基础查询
            query = """
            SELECT 
                id, project_code, project_name, project_type,
                tickers, industry, sector, region,
                status, priority, description,
                current_position,
                created_by, owner_id,
                start_date,
                created_at, updated_at
            FROM investment_projects
            WHERE id = %s
            """
            
            result = self.db_manager.fetch_one(query, [project_id])
            
            if not result:
                return None
            
            project = {
                'id': result[0],
                'project_code': result[1],
                'project_name': result[2],
                'project_type': result[3],
                'tickers': list(result[4]) if result[4] else [],
                'industry': result[5],
                'sector': result[6],
                'region': result[7],
                'status': result[8],
                'priority': result[9],
                'description': result[10],
                'current_position': result[11],
                'created_by': result[12],
                'owner_id': result[13],
                'start_date': result[14].isoformat() if result[14] else None,
                'created_at': result[15].isoformat() if result[15] else None,
                'updated_at': result[16].isoformat() if result[16] else None,
            }
            
            # 如果需要统计信息
            if include_stats:
                stats = self._get_project_statistics(project_id)
                project.update(stats)
            
            return project
        
        except Exception as e:
            logger.error("获取项目失败", project_id=project_id, exception=str(e))
            return None
    
    def list_projects(
        self,
        user_id: int,
        filters: Dict[str, Any],
        page: int = 1,
        page_size: int = 20
    ) -> Tuple[List[Dict[str, Any]], int]:
        """
        获取项目列表
        
        Args:
            user_id: 用户ID
            filters: 筛选条件
            page: 页码
            page_size: 每页大小
        
        Returns:
            Tuple[List[Dict[str, Any]], int]: (项目列表, 总数)
        """
        try:
            # 构建WHERE条件
            where_conditions = []
            params = []
            
            # 基础条件：用户必须是项目成员，或者是项目的创建者/所有者
            where_conditions.append("""
                (
                    EXISTS (
                        SELECT 1 FROM project_members pm 
                        WHERE pm.project_id = p.id 
                        AND pm.user_id = %s 
                        AND pm.status = 'active'
                    )
                    OR p.created_by = %s
                    OR p.owner_id = %s
                )
            """)
            params.extend([user_id, user_id, user_id])
            
            # 筛选条件
            if filters.get('status'):
                where_conditions.append("p.status = %s")
                params.append(filters['status'])
            
            if filters.get('project_type'):
                where_conditions.append("p.project_type = %s")
                params.append(filters['project_type'])
            
            if filters.get('priority'):
                where_conditions.append("p.priority = %s")
                params.append(filters['priority'])
            
            if filters.get('search'):
                where_conditions.append("""
                    (p.project_name ILIKE %s OR p.project_code ILIKE %s 
                     OR EXISTS (
                        SELECT 1 FROM unnest(p.tickers) AS ticker 
                        WHERE ticker ILIKE %s
                     ))
                """)
                search_term = f"%{filters['search']}%"
                params.extend([search_term, search_term, search_term])
            
            if filters.get('created_by_me'):
                where_conditions.append("p.created_by = %s")
                params.append(user_id)
            
            where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"
            
            # 获取总数
            count_query = f"""
            SELECT COUNT(DISTINCT p.id)
            FROM investment_projects p
            WHERE {where_clause}
            """
            count_result = self.db_manager.fetch_one(count_query, params)
            total = count_result[0] if count_result else 0
            
            # 获取项目列表（带统计信息）
            offset = (page - 1) * page_size
            sort_by = filters.get('sort_by', 'created_at')
            sort_order = filters.get('sort_order', 'desc').upper()
            
            query = f"""
            SELECT 
                p.id, p.project_code, p.project_name, p.project_type,
                p.tickers, p.industry, p.sector, p.region,
                p.status, p.priority, p.description,
                p.created_by, p.owner_id, p.created_at, p.updated_at,
                COUNT(DISTINCT pm.id) FILTER (WHERE pm.status = 'active') as member_count,
                0 as news_count,
                COUNT(DISTINCT pf.id) as file_count,
                COUNT(DISTINCT pms.id) as milestone_count
            FROM investment_projects p
            LEFT JOIN project_members pm ON p.id = pm.project_id
            LEFT JOIN project_files pf ON p.id = pf.project_id
            LEFT JOIN project_milestones pms ON p.id = pms.project_id
            WHERE {where_clause}
            GROUP BY p.id
            ORDER BY p.{sort_by} {sort_order}
            LIMIT %s OFFSET %s
            """
            
            params.extend([page_size, offset])
            results = self.db_manager.fetch_all(query, params)
            
            projects = []
            for row in results:
                projects.append({
                    'id': row[0],
                    'project_code': row[1],
                    'project_name': row[2],
                    'project_type': row[3],
                    'tickers': list(row[4]) if row[4] else [],
                    'industry': row[5],
                    'sector': row[6],
                    'region': row[7],
                    'status': row[8],
                    'priority': row[9],
                    'description': row[10],
                    'created_by': row[11],
                    'owner_id': row[12],
                    'created_at': row[13].isoformat() if row[13] else None,
                    'updated_at': row[14].isoformat() if row[14] else None,
                    'member_count': row[15],
                    'news_count': row[16],
                    'file_count': row[17],
                    'milestone_count': row[18],
                })
            
            return projects, total
        
        except Exception as e:
            logger.error("获取项目列表失败", user_id=user_id, exception=str(e))
            return [], 0
    
    def update_project(
        self,
        project_id: int,
        user_id: int,
        update_data: Dict[str, Any]
    ) -> Tuple[bool, str]:
        """
        更新项目信息
        
        Args:
            project_id: 项目ID
            user_id: 操作者ID
            update_data: 更新数据
        
        Returns:
            Tuple[bool, str]: (成功标志, 消息)
        """
        try:
            # 权限检查
            if not self.permission_service.check_project_permission(
                user_id, project_id, 'manage'
            ):
                return False, "没有管理项目的权限"
            
            # 构建UPDATE语句
            update_fields = []
            params = []
            
            allowed_fields = [
                'project_name', 'status', 'priority', 'tickers', 'description',
                'current_position'
            ]
            
            for field in allowed_fields:
                if field in update_data:
                    # 特殊处理tickers字段：清理并去重
                    if field == 'tickers':
                        tickers = update_data[field]
                        if tickers is not None:
                            if isinstance(tickers, str):
                                tickers = [tickers]
                            # 清理tickers：去重、去空、转大写
                            tickers = list(set([t.strip().upper() for t in tickers if t and t.strip()]))
                            update_fields.append(f"{field} = %s")
                            params.append(tickers)
                    else:
                        update_fields.append(f"{field} = %s")
                        params.append(update_data[field])
            
            if not update_fields:
                return False, "没有可更新的字段"
            
            params.append(project_id)
            
            update_query = f"""
            UPDATE investment_projects
            SET {', '.join(update_fields)}
            WHERE id = %s
            """
            
            self.db_manager.execute_query(update_query, params)
            
            logger.info("更新项目成功", project_id=project_id, user_id=user_id)
            return True, "更新项目成功"
        
        except Exception as e:
            logger.error("更新项目失败", 
                        project_id=project_id, 
                        user_id=user_id,
                        exception=str(e))
            return False, f"更新项目失败: {str(e)}"
    
    def delete_project(
        self,
        project_id: int,
        user_id: int
    ) -> Tuple[bool, str]:
        """
        删除项目（真正删除，会级联删除所有关联数据）
        
        Args:
            project_id: 项目ID
            user_id: 操作者ID
        
        Returns:
            Tuple[bool, str]: (成功标志, 消息)
        """
        try:
            # 权限检查
            if not self.permission_service.check_project_permission(
                user_id, project_id, 'delete'
            ):
                return False, "只有项目owner可以删除项目"
            
            # 真正删除项目（数据库会自动级联删除关联数据）
            delete_query = """
            DELETE FROM investment_projects
            WHERE id = %s
            """
            
            self.db_manager.execute_query(delete_query, [project_id])
            
            logger.info("删除项目成功", project_id=project_id, user_id=user_id)
            return True, "项目已删除"
        
        except Exception as e:
            logger.error("删除项目失败", 
                        project_id=project_id, 
                        user_id=user_id,
                        exception=str(e))
            return False, f"删除项目失败: {str(e)}"
    
    # ==================== 项目成员管理 ====================
    
    def get_project_members(
        self,
        project_id: int,
        user_id: int
    ) -> List[Dict[str, Any]]:
        """获取项目成员列表"""
        try:
            # 权限检查
            if not self.permission_service.check_project_permission(
                user_id, project_id, 'view'
            ):
                logger.warning("用户无权查看项目成员", user_id=user_id, project_id=project_id)
                return []
            
            query = """
            SELECT 
                pm.id, pm.project_id, pm.user_id, u.username,
                pm.role, pm.can_edit, pm.can_invite, pm.status,
                pm.invited_by, pm.invited_at, pm.joined_at,
                pm.created_at, pm.updated_at
            FROM project_members pm
            JOIN users u ON pm.user_id = u.id
            WHERE pm.project_id = %s
            ORDER BY 
                CASE pm.role 
                    WHEN 'owner' THEN 1
                    WHEN 'admin' THEN 2
                    WHEN 'member' THEN 3
                    WHEN 'viewer' THEN 4
                END,
                pm.joined_at
            """
            
            results = self.db_manager.fetch_all(query, [project_id])
            
            members = []
            for row in results:
                members.append({
                    'id': row[0],
                    'project_id': row[1],
                    'user_id': row[2],
                    'username': row[3],
                    'role': row[4],
                    'can_edit': row[5],
                    'can_invite': row[6],
                    'status': row[7],
                    'invited_by': row[8],
                    'invited_at': row[9].isoformat() if row[9] else None,
                    'joined_at': row[10].isoformat() if row[10] else None,
                    'created_at': row[11].isoformat() if row[11] else None,
                    'updated_at': row[12].isoformat() if row[12] else None,
                })
            
            return members
        
        except Exception as e:
            logger.error("获取项目成员失败", project_id=project_id, exception=str(e))
            return []
    
    def invite_members(
        self,
        project_id: int,
        inviter_id: int,
        user_ids: List[int],
        role: str = 'member',
        can_edit: bool = True,
        can_invite: bool = False
    ) -> Tuple[bool, int, str]:
        """
        邀请成员加入项目
        
        Returns:
            Tuple[bool, int, str]: (成功标志, 成功邀请数量, 消息)
        """
        try:
            # 权限检查
            if not self.permission_service.check_project_permission(
                inviter_id, project_id, 'invite'
            ):
                return False, 0, "没有邀请成员的权限"
            
            success_count = 0
            failed_users = []
            
            for user_id in user_ids:
                # 检查用户是否已是成员
                check_query = """
                SELECT id, status FROM project_members 
                WHERE project_id = %s AND user_id = %s
                """
                existing = self.db_manager.fetch_one(check_query, [project_id, user_id])
                
                if existing:
                    failed_users.append(f"用户{user_id}已是项目成员")
                    continue
                
                # 添加成员
                insert_query = """
                INSERT INTO project_members (
                    project_id, user_id, role, can_edit, can_invite, 
                    status, invited_by, invited_at
                ) VALUES (%s, %s, %s, %s, %s, 'active', %s, %s)
                """
                
                self.db_manager.execute_query(insert_query, [
                    project_id, user_id, role, can_edit, can_invite,
                    inviter_id, datetime.now()
                ])
                
                success_count += 1
            
            message = f"成功邀请{success_count}位成员"
            if failed_users:
                message += f"，失败: {'; '.join(failed_users[:3])}"
            
            logger.info("邀请成员完成", 
                       project_id=project_id, 
                       inviter_id=inviter_id,
                       success_count=success_count)
            
            return True, success_count, message
        
        except Exception as e:
            logger.error("邀请成员失败", 
                        project_id=project_id,
                        inviter_id=inviter_id,
                        exception=str(e))
            return False, 0, f"邀请成员失败: {str(e)}"
    
    def update_member(
        self,
        project_id: int,
        operator_id: int,
        target_user_id: int,
        update_data: Dict[str, Any]
    ) -> Tuple[bool, str]:
        """更新成员信息"""
        try:
            # 验证操作权限
            is_valid, error_msg = self.permission_service.validate_member_operation(
                operator_id, target_user_id, project_id, 'update'
            )
            
            if not is_valid:
                return False, error_msg
            
            # 构建UPDATE语句
            update_fields = []
            params = []
            
            allowed_fields = ['role', 'can_edit', 'can_invite', 'status']
            
            for field in allowed_fields:
                if field in update_data:
                    update_fields.append(f"{field} = %s")
                    params.append(update_data[field])
            
            if not update_fields:
                return False, "没有可更新的字段"
            
            params.extend([project_id, target_user_id])
            
            update_query = f"""
            UPDATE project_members
            SET {', '.join(update_fields)}
            WHERE project_id = %s AND user_id = %s
            """
            
            self.db_manager.execute_query(update_query, params)
            
            logger.info("更新成员信息成功", 
                       project_id=project_id,
                       operator_id=operator_id,
                       target_user_id=target_user_id)
            
            return True, "更新成员信息成功"
        
        except Exception as e:
            logger.error("更新成员信息失败", exception=str(e))
            return False, f"更新成员信息失败: {str(e)}"
    
    def remove_member(
        self,
        project_id: int,
        operator_id: int,
        target_user_id: int
    ) -> Tuple[bool, str]:
        """移除成员"""
        try:
            # 验证操作权限
            is_valid, error_msg = self.permission_service.validate_member_operation(
                operator_id, target_user_id, project_id, 'remove'
            )
            
            if not is_valid:
                return False, error_msg
            
            # 删除成员记录
            delete_query = """
            DELETE FROM project_members
            WHERE project_id = %s AND user_id = %s
            """
            
            self.db_manager.execute_query(delete_query, [project_id, target_user_id])
            
            logger.info("移除成员成功", 
                       project_id=project_id,
                       operator_id=operator_id,
                       target_user_id=target_user_id)
            
            return True, "移除成员成功"
        
        except Exception as e:
            logger.error("移除成员失败", exception=str(e))
            return False, f"移除成员失败: {str(e)}"
    
    # ==================== 辅助方法 ====================
    
    def _get_project_statistics(self, project_id: int) -> Dict[str, Any]:
        """获取项目统计信息"""
        try:
            query = """
            SELECT 
                COUNT(DISTINCT pm.id) FILTER (WHERE pm.status = 'active') as total_members,
                COUNT(DISTINCT pm.id) FILTER (WHERE pm.status = 'active' AND pm.role IN ('owner', 'admin', 'member')) as active_members,
                0 as total_news,
                0 as important_news,
                COUNT(DISTINCT pf.id) as total_files,
                COUNT(DISTINCT pms.id) as total_milestones,
                MAX(pms.milestone_date) as latest_milestone_date,
                NULL::TIMESTAMP WITH TIME ZONE as latest_news_added_at
            FROM investment_projects p
            LEFT JOIN project_members pm ON p.id = pm.project_id
            LEFT JOIN project_files pf ON p.id = pf.project_id
            LEFT JOIN project_milestones pms ON p.id = pms.project_id
            WHERE p.id = %s
            GROUP BY p.id
            """
            
            result = self.db_manager.fetch_one(query, [project_id])
            
            if result:
                return {
                    'member_count': result[0] or 0,
                    'active_members': result[1] or 0,
                    'news_count': result[2] or 0,
                    'important_news': result[3] or 0,
                    'file_count': result[4] or 0,
                    'milestone_count': result[5] or 0,
                    'latest_milestone_date': result[6].isoformat() if result[6] else None,
                    'latest_news_added_at': result[7].isoformat() if result[7] else None,
                }
            
            return {}
        
        except Exception as e:
            logger.error("获取项目统计信息失败", project_id=project_id, exception=str(e))
            return {}
    
    # ==================== 项目里程碑管理 ====================
    
    def get_project_milestones(self, project_id: int, user_id: int) -> List[Dict[str, Any]]:
        """获取项目里程碑列表"""
        try:
            # 权限检查
            if not self.permission_service.can_user_access_project(user_id, project_id):
                return []
            
            query = """
            SELECT 
                pm.id, pm.project_id, pm.milestone_type, pm.title,
                pm.content, pm.file_ids, pm.decision_type, pm.decision_rationale,
                pm.milestone_date, pm.created_by, pm.created_at, pm.updated_at,
                u.username as created_by_username
            FROM project_milestones pm
            LEFT JOIN users u ON pm.created_by = u.id
            WHERE pm.project_id = %s
            ORDER BY pm.milestone_date DESC, pm.created_at DESC
            """
            
            rows = self.db_manager.fetch_all(query, [project_id])
            
            milestones = []
            for row in rows:
                milestones.append({
                    'id': row[0],
                    'project_id': row[1],
                    'milestone_type': row[2],
                    'title': row[3],
                    'content': row[4],
                    'file_ids': row[5] or [],
                    'decision_type': row[6],
                    'decision_rationale': row[7],
                    'milestone_date': row[8].isoformat() if row[8] else None,
                    'created_by': row[9],
                    'created_at': row[10].isoformat() if row[10] else None,
                    'updated_at': row[11].isoformat() if row[11] else None,
                    'created_by_username': row[12]
                })
            
            return milestones
        
        except Exception as e:
            logger.error("获取项目里程碑失败", project_id=project_id, exception=str(e))
            return []
    
    def create_milestone(
        self,
        project_id: int,
        user_id: int,
        milestone_data: Dict[str, Any]
    ) -> Tuple[bool, Optional[int], str]:
        """创建项目里程碑"""
        try:
            # 权限检查
            if not self.permission_service.check_project_permission(user_id, project_id, 'edit'):
                return False, None, "没有编辑权限"
            
            insert_query = """
            INSERT INTO project_milestones (
                project_id, milestone_type, title, content,
                file_ids, decision_type, decision_rationale,
                milestone_date, created_by
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """
            
            result = self.db_manager.fetch_one(insert_query, [
                project_id,
                milestone_data.get('milestone_type'),
                milestone_data.get('title'),
                milestone_data.get('content'),
                milestone_data.get('file_ids', []),
                milestone_data.get('decision_type'),
                milestone_data.get('decision_rationale'),
                milestone_data.get('milestone_date'),
                user_id
            ])
            
            milestone_id = result[0] if result else None
            
            logger.info("创建里程碑成功", project_id=project_id, milestone_id=milestone_id)
            return True, milestone_id, "创建里程碑成功"
        
        except Exception as e:
            logger.error("创建里程碑失败", exception=str(e))
            return False, None, f"创建里程碑失败: {str(e)}"
    
    def get_milestone(self, milestone_id: int, user_id: int) -> Optional[Dict[str, Any]]:
        """获取单个里程碑详情"""
        try:
            query = """
            SELECT 
                pm.id, pm.project_id, pm.milestone_type, pm.title,
                pm.content, pm.file_ids, pm.decision_type, pm.decision_rationale,
                pm.milestone_date, pm.created_by, pm.created_at, pm.updated_at,
                u.username as created_by_username
            FROM project_milestones pm
            LEFT JOIN users u ON pm.created_by = u.id
            WHERE pm.id = %s
            """
            
            row = self.db_manager.fetch_one(query, [milestone_id])
            
            if row:
                return {
                    'id': row[0],
                    'project_id': row[1],
                    'milestone_type': row[2],
                    'title': row[3],
                    'content': row[4],
                    'file_ids': row[5] or [],
                    'decision_type': row[6],
                    'decision_rationale': row[7],
                    'milestone_date': row[8].isoformat() if row[8] else None,
                    'created_by': row[9],
                    'created_at': row[10].isoformat() if row[10] else None,
                    'updated_at': row[11].isoformat() if row[11] else None,
                    'created_by_username': row[12]
                }
            
            return None
        
        except Exception as e:
            logger.error("获取里程碑详情失败", milestone_id=milestone_id, exception=str(e))
            return None
    
    def update_milestone(
        self,
        milestone_id: int,
        user_id: int,
        update_data: Dict[str, Any]
    ) -> Tuple[bool, str]:
        """更新里程碑"""
        try:
            # 获取里程碑所属项目
            query = "SELECT project_id FROM project_milestones WHERE id = %s"
            result = self.db_manager.fetch_one(query, [milestone_id])
            
            if not result:
                return False, "里程碑不存在"
            
            project_id = result[0]
            
            # 权限检查
            if not self.permission_service.check_project_permission(user_id, project_id, 'edit'):
                return False, "没有编辑权限"
            
            # 构建UPDATE语句
            update_fields = []
            params = []
            
            allowed_fields = [
                'milestone_type', 'title', 'content', 'file_ids',
                'decision_type', 'decision_rationale', 'milestone_date'
            ]
            
            for field in allowed_fields:
                if field in update_data:
                    update_fields.append(f"{field} = %s")
                    params.append(update_data[field])
            
            if not update_fields:
                return False, "没有可更新的字段"
            
            params.append(milestone_id)
            
            update_query = f"""
            UPDATE project_milestones
            SET {', '.join(update_fields)}
            WHERE id = %s
            """
            
            self.db_manager.execute_query(update_query, params)
            
            logger.info("更新里程碑成功", milestone_id=milestone_id)
            return True, "更新里程碑成功"
        
        except Exception as e:
            logger.error("更新里程碑失败", exception=str(e))
            return False, f"更新里程碑失败: {str(e)}"
    
    def delete_milestone(
        self,
        milestone_id: int,
        user_id: int
    ) -> Tuple[bool, str]:
        """删除里程碑"""
        try:
            # 获取里程碑所属项目
            query = "SELECT project_id FROM project_milestones WHERE id = %s"
            result = self.db_manager.fetch_one(query, [milestone_id])
            
            if not result:
                return False, "里程碑不存在"
            
            project_id = result[0]
            
            # 权限检查
            if not self.permission_service.check_project_permission(user_id, project_id, 'edit'):
                return False, "没有编辑权限"
            
            delete_query = "DELETE FROM project_milestones WHERE id = %s"
            self.db_manager.execute_query(delete_query, [milestone_id])
            
            logger.info("删除里程碑成功", milestone_id=milestone_id)
            return True, "删除里程碑成功"
        
        except Exception as e:
            logger.error("删除里程碑失败", exception=str(e))
            return False, f"删除里程碑失败: {str(e)}"
    
    # ==================== 项目文件管理 ====================
    
    def get_project_files(self, project_id: int, user_id: int) -> List[Dict[str, Any]]:
        """获取项目文件列表"""
        try:
            # 权限检查
            if not self.permission_service.can_user_access_project(user_id, project_id):
                return []
            
            query = """
            SELECT 
                pf.id, pf.project_id, pf.file_name, pf.file_path,
                pf.file_type, pf.file_size, pf.category, pf.description,
                pf.milestone_id, pf.uploaded_by, pf.uploaded_at,
                u.username as uploaded_by_username
            FROM project_files pf
            LEFT JOIN users u ON pf.uploaded_by = u.id
            WHERE pf.project_id = %s
            ORDER BY pf.uploaded_at DESC
            """
            
            rows = self.db_manager.fetch_all(query, [project_id])
            
            files = []
            for row in rows:
                files.append({
                    'id': row[0],
                    'project_id': row[1],
                    'file_name': row[2],
                    'file_path': row[3],
                    'file_type': row[4],
                    'file_size': row[5],
                    'category': row[6],
                    'description': row[7],
                    'milestone_id': row[8],
                    'uploaded_by': row[9],
                    'uploaded_at': row[10].isoformat() if row[10] else None,
                    'uploaded_by_username': row[11]
                })
            
            return files
        
        except Exception as e:
            logger.error("获取项目文件失败", project_id=project_id, exception=str(e))
            return []
    
    def add_project_file(
        self,
        project_id: int,
        user_id: int,
        file_data: Dict[str, Any]
    ) -> Tuple[bool, Optional[int], str]:
        """添加文件记录到项目"""
        try:
            # 权限检查
            if not self.permission_service.check_project_permission(user_id, project_id, 'edit'):
                return False, None, "没有编辑权限"
            
            insert_query = """
            INSERT INTO project_files (
                project_id, file_name, file_path, file_type,
                file_size, category, description, milestone_id,
                uploaded_by
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """
            
            result = self.db_manager.fetch_one(insert_query, [
                project_id,
                file_data.get('file_name'),
                file_data.get('file_path'),
                file_data.get('file_type'),
                file_data.get('file_size'),
                file_data.get('category'),
                file_data.get('description'),
                file_data.get('milestone_id'),
                user_id
            ])
            
            file_id = result[0] if result else None
            
            logger.info("添加文件记录成功", project_id=project_id, file_id=file_id)
            return True, file_id, "添加文件记录成功"
        
        except Exception as e:
            logger.error("添加文件记录失败", exception=str(e))
            return False, None, f"添加文件记录失败: {str(e)}"
    
    def get_project_file(self, file_id: int, user_id: int) -> Optional[Dict[str, Any]]:
        """获取单个文件详情"""
        try:
            query = """
            SELECT 
                pf.id, pf.project_id, pf.file_name, pf.file_path,
                pf.file_type, pf.file_size, pf.category, pf.description,
                pf.milestone_id, pf.uploaded_by, pf.uploaded_at,
                u.username as uploaded_by_username
            FROM project_files pf
            LEFT JOIN users u ON pf.uploaded_by = u.id
            WHERE pf.id = %s
            """
            
            row = self.db_manager.fetch_one(query, [file_id])
            
            if row:
                return {
                    'id': row[0],
                    'project_id': row[1],
                    'file_name': row[2],
                    'file_path': row[3],
                    'file_type': row[4],
                    'file_size': row[5],
                    'category': row[6],
                    'description': row[7],
                    'milestone_id': row[8],
                    'uploaded_by': row[9],
                    'uploaded_at': row[10].isoformat() if row[10] else None,
                    'uploaded_by_username': row[11]
                }
            
            return None
        
        except Exception as e:
            logger.error("获取文件详情失败", file_id=file_id, exception=str(e))
            return None
    
    def delete_project_file(
        self,
        file_id: int,
        user_id: int
    ) -> Tuple[bool, str]:
        """删除项目文件"""
        try:
            # 获取文件所属项目
            query = "SELECT project_id FROM project_files WHERE id = %s"
            result = self.db_manager.fetch_one(query, [file_id])
            
            if not result:
                return False, "文件不存在"
            
            project_id = result[0]
            
            # 权限检查
            if not self.permission_service.check_project_permission(user_id, project_id, 'edit'):
                return False, "没有编辑权限"
            
            delete_query = "DELETE FROM project_files WHERE id = %s"
            self.db_manager.execute_query(delete_query, [file_id])
            
            logger.info("删除文件成功", file_id=file_id)
            return True, "删除文件成功"
        
        except Exception as e:
            logger.error("删除文件失败", exception=str(e))
            return False, f"删除文件失败: {str(e)}"

