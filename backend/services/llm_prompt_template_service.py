# -*- coding: utf-8 -*-
"""
LLM Prompt模板管理服务

提供Prompt模板的完整CRUD操作、统计分析等功能
"""

import json
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta

from backend.database.db_manager import DatabaseManager
from backend.utils.database_config import get_database_config
from backend.utils.logger import get_logger

logger = get_logger("llm_prompt_template_service")


class LLMPromptTemplateService:
    """Prompt模板管理服务"""
    
    def __init__(self):
        """初始化服务"""
        self.db_config = get_database_config()
        self.db_manager = DatabaseManager(self.db_config)
    
    # ==================== 分类管理 ====================
    
    def list_categories(self, active_only: bool = True) -> List[Dict[str, Any]]:
        """
        获取所有分类
        
        Args:
            active_only: 是否只返回活跃的分类
        
        Returns:
            分类列表
        """
        conn = self.db_manager.get_connection()
        if not conn:
            logger.error("Database connection failed")
            return []
        
        try:
            cursor = conn.cursor()
            
            query = """
                SELECT id, category_code, category_name, description, icon,
                       sort_order, is_active, created_at, updated_at
                FROM llm_prompt_categories
            """
            
            if active_only:
                query += " WHERE is_active = true"
            
            query += " ORDER BY sort_order, category_name"
            
            cursor.execute(query)
            rows = cursor.fetchall()
            
            categories = []
            for row in rows:
                categories.append({
                    'id': row[0],
                    'category_code': row[1],
                    'category_name': row[2],
                    'description': row[3],
                    'icon': row[4],
                    'sort_order': row[5],
                    'is_active': row[6],
                    'created_at': row[7].isoformat() if row[7] else None,
                    'updated_at': row[8].isoformat() if row[8] else None
                })
            
            cursor.close()
            conn.close()
            
            logger.info(f"Retrieved {len(categories)} categories")
            return categories
            
        except Exception as e:
            logger.error(f"Failed to list categories: {str(e)}")
            conn.close()
            return []
    
    def create_category(self, category_data: Dict[str, Any]) -> Optional[int]:
        """
        创建新分类
        
        Args:
            category_data: 分类数据
        
        Returns:
            新分类的ID，失败返回None
        """
        conn = self.db_manager.get_connection()
        if not conn:
            return None
        
        try:
            cursor = conn.cursor()
            
            query = """
                INSERT INTO llm_prompt_categories 
                (category_code, category_name, description, icon, sort_order, is_active)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING id
            """
            
            cursor.execute(query, (
                category_data['category_code'],
                category_data['category_name'],
                category_data.get('description'),
                category_data.get('icon'),
                category_data.get('sort_order', 0),
                category_data.get('is_active', True)
            ))
            
            category_id = cursor.fetchone()[0]
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f"Created category: {category_id}")
            return category_id
            
        except Exception as e:
            logger.error(f"Failed to create category: {str(e)}")
            conn.rollback()
            conn.close()
            return None
    
    # ==================== 模板CRUD ====================
    
    def list_templates(
        self,
        category_id: Optional[int] = None,
        scope: Optional[str] = None,
        user_id: Optional[int] = None,
        is_admin: bool = False,
        search: Optional[str] = None,
        active_only: bool = True
    ) -> List[Dict[str, Any]]:
        """
        获取模板列表
        
        可见性规则：
        - 普通用户：仅自己创建的模板
        - 管理员：系统模板 + 自己创建的模板
        
        Args:
            category_id: 分类ID过滤
            scope: 范围过滤（system/user/shared）
            user_id: 用户ID
            is_admin: 是否为管理员
            search: 搜索关键词
            active_only: 是否只返回活跃模板
        
        Returns:
            模板列表
        """
        conn = self.db_manager.get_connection()
        if not conn:
            return []
        
        try:
            cursor = conn.cursor()
            
            query = """
                SELECT t.id, t.template_code, t.template_name, t.category_id,
                       t.description, t.scope, t.is_active, t.is_default,
                       t.usage_count, t.success_rate, t.avg_cost,
                       t.recommended_provider, t.recommended_model,
                       t.temperature, t.max_tokens, t.version,
                       t.created_by, t.created_at, t.updated_at,
                       c.category_name, c.icon as category_icon,
                       u.username as created_by_name,
                       t.module_code, c.category_code
                FROM llm_prompt_templates t
                LEFT JOIN llm_prompt_categories c ON t.category_id = c.id
                LEFT JOIN users u ON t.created_by = u.id
                WHERE 1=1
            """
            
            params = []
            
            if active_only:
                query += " AND t.is_active = true"
            
            if category_id:
                query += " AND t.category_id = %s"
                params.append(category_id)
            
            if scope:
                query += " AND t.scope = %s"
                params.append(scope)
                # user-scope 额外限制只看自己的（无论是否 admin，私有模板不互见）
                if scope == 'user' and user_id:
                    query += " AND t.created_by = %s"
                    params.append(user_id)
            elif user_id:
                if is_admin:
                    query += " AND (t.scope = 'system' OR t.created_by = %s)"
                    params.append(user_id)
                else:
                    query += " AND t.created_by = %s"
                    params.append(user_id)
            
            if search:
                query += " AND (t.template_name ILIKE %s OR t.template_code ILIKE %s OR t.description ILIKE %s)"
                search_pattern = f"%{search}%"
                params.extend([search_pattern, search_pattern, search_pattern])
            
            query += " ORDER BY t.category_id, t.is_default DESC, t.usage_count DESC, t.template_name"
            
            cursor.execute(query, params)
            rows = cursor.fetchall()
            
            templates = []
            for row in rows:
                templates.append({
                    'id': row[0],
                    'template_code': row[1],
                    'template_name': row[2],
                    'category_id': row[3],
                    'description': row[4],
                    'scope': row[5],
                    'is_active': row[6],
                    'is_default': row[7],
                    'usage_count': row[8] or 0,
                    'success_rate': float(row[9]) if row[9] else None,
                    'avg_cost': float(row[10]) if row[10] else None,
                    'recommended_provider': row[11],
                    'recommended_model': row[12],
                    'temperature': float(row[13]) if row[13] else 0.7,
                    'max_tokens': row[14] or 2000,
                    'version': row[15],
                    'created_by': row[16],
                    'created_at': row[17].isoformat() if row[17] else None,
                    'updated_at': row[18].isoformat() if row[18] else None,
                    'category_name': row[19],
                    'category_icon': row[20],
                    'created_by_name': row[21],
                    'module_code': row[22],
                    'category_code': row[23],
                })
            
            cursor.close()
            conn.close()
            
            logger.info(f"Retrieved {len(templates)} templates")
            return templates
            
        except Exception as e:
            logger.error(f"Failed to list templates: {str(e)}")
            conn.close()
            return []
    
    def get_template(self, template_id: int) -> Optional[Dict[str, Any]]:
        """
        获取模板详情
        
        Args:
            template_id: 模板ID
        
        Returns:
            模板详情
        """
        conn = self.db_manager.get_connection()
        if not conn:
            return None
        
        try:
            cursor = conn.cursor()
            
            query = """
                SELECT t.id, t.template_code, t.template_name, t.category_id,
                       t.system_prompt, t.user_prompt_template, t.variables,
                       t.output_format, t.description, t.scope, t.is_active,
                       t.is_default, t.is_public, t.usage_count, t.success_rate,
                       t.avg_cost, t.recommended_provider, t.recommended_model,
                       t.temperature, t.max_tokens, t.version, t.parent_template_id,
                       t.tags, t.example_input, t.example_output,
                       t.created_by, t.created_at, t.updated_at,
                       c.category_name, c.category_code,
                       u.username as created_by_name
                FROM llm_prompt_templates t
                LEFT JOIN llm_prompt_categories c ON t.category_id = c.id
                LEFT JOIN users u ON t.created_by = u.id
                WHERE t.id = %s
            """
            
            cursor.execute(query, (template_id,))
            row = cursor.fetchone()
            
            if not row:
                cursor.close()
                conn.close()
                return None
            
            template = {
                'id': row[0],
                'template_code': row[1],
                'template_name': row[2],
                'category_id': row[3],
                'system_prompt': row[4],
                'user_prompt_template': row[5],
                'variables': row[6] if row[6] else {},
                'output_format': row[7] if row[7] else {},
                'description': row[8],
                'scope': row[9],
                'is_active': row[10],
                'is_default': row[11],
                'is_public': row[12],
                'usage_count': row[13] or 0,
                'success_rate': float(row[14]) if row[14] else None,
                'avg_cost': float(row[15]) if row[15] else None,
                'recommended_provider': row[16],
                'recommended_model': row[17],
                'temperature': float(row[18]) if row[18] else 0.7,
                'max_tokens': row[19] or 2000,
                'version': row[20],
                'parent_template_id': row[21],
                'tags': row[22] if row[22] else [],
                'example_input': row[23] if row[23] else {},
                'example_output': row[24],
                'created_by': row[25],
                'created_at': row[26].isoformat() if row[26] else None,
                'updated_at': row[27].isoformat() if row[27] else None,
                'category_name': row[28],
                'category_code': row[29],
                'created_by_name': row[30]
            }
            
            cursor.close()
            conn.close()
            
            return template
            
        except Exception as e:
            logger.error(f"Failed to get template: {str(e)}")
            conn.close()
            return None
    
    def get_template_by_code(self, template_code: str) -> Optional[Dict[str, Any]]:
        """
        根据模板代码获取模板信息
        
        Args:
            template_code: 模板代码
        
        Returns:
            模板信息字典，如果未找到则返回None
        """
        conn = self.db_manager.get_connection()
        if not conn:
            logger.error("Database connection failed")
            return None
        
        try:
            cursor = conn.cursor()
            
            query = """
                SELECT t.id, t.template_code, t.template_name, t.category_id,
                       t.system_prompt, t.user_prompt_template, t.variables,
                       t.output_format, t.description, t.scope, t.is_active,
                       t.is_default, t.is_public, t.usage_count, t.success_rate,
                       t.avg_cost, t.recommended_provider, t.recommended_model,
                       t.temperature, t.max_tokens, t.version, t.parent_template_id,
                       t.tags, t.example_input, t.example_output, t.created_by,
                       t.created_at, t.updated_at, c.category_name, c.category_code,
                       u.username as created_by_name
                FROM llm_prompt_templates t
                LEFT JOIN llm_prompt_categories c ON t.category_id = c.id
                LEFT JOIN users u ON t.created_by = u.id
                WHERE t.template_code = %s AND t.is_active = true
            """
            
            cursor.execute(query, (template_code,))
            row = cursor.fetchone()
            
            if not row:
                cursor.close()
                conn.close()
                return None
            
            template = {
                'id': row[0],
                'template_code': row[1],
                'template_name': row[2],
                'category_id': row[3],
                'system_prompt': row[4],
                'user_prompt_template': row[5],
                'variables': row[6] if row[6] else {},
                'output_format': row[7] if row[7] else {},
                'description': row[8],
                'scope': row[9],
                'is_active': row[10],
                'is_default': row[11],
                'is_public': row[12],
                'usage_count': row[13] or 0,
                'success_rate': float(row[14]) if row[14] else None,
                'avg_cost': float(row[15]) if row[15] else None,
                'recommended_provider': row[16],
                'recommended_model': row[17],
                'temperature': float(row[18]) if row[18] else 0.7,
                'max_tokens': row[19] or 2000,
                'version': row[20],
                'parent_template_id': row[21],
                'tags': row[22] if row[22] else [],
                'example_input': row[23] if row[23] else {},
                'example_output': row[24],
                'created_by': row[25],
                'created_at': row[26].isoformat() if row[26] else None,
                'updated_at': row[27].isoformat() if row[27] else None,
                'category_name': row[28],
                'category_code': row[29],
                'created_by_name': row[30]
            }
            
            cursor.close()
            conn.close()
            
            return template
            
        except Exception as e:
            logger.error(f"Failed to get template by code: {str(e)}")
            conn.close()
            return None
    
    def create_template(self, template_data: Dict[str, Any], user_id: Optional[int] = None) -> Optional[int]:
        """
        创建新模板
        
        Args:
            template_data: 模板数据
            user_id: 创建者用户ID
        
        Returns:
            新模板的ID，失败返回None
        """
        conn = self.db_manager.get_connection()
        if not conn:
            return None
        
        try:
            cursor = conn.cursor()
            
            query = """
                INSERT INTO llm_prompt_templates (
                    template_code, template_name, category_id, module_code, system_prompt,
                    user_prompt_template, variables, output_format,
                    recommended_provider, recommended_model, temperature, max_tokens,
                    scope, created_by, is_public, is_active, is_default,
                    description, tags, example_input, example_output
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                RETURNING id
            """
            
            cursor.execute(query, (
                template_data['template_code'],
                template_data['template_name'],
                template_data['category_id'],
                template_data.get('module_code'),
                template_data.get('system_prompt'),
                template_data['user_prompt_template'],
                json.dumps(template_data.get('variables', {})),
                json.dumps(template_data.get('output_format', {})),
                template_data.get('recommended_provider'),
                template_data.get('recommended_model'),
                template_data.get('temperature', 0.7),
                template_data.get('max_tokens', 2000),
                template_data.get('scope', 'user'),
                user_id,
                template_data.get('is_public', False),
                template_data.get('is_active', True),
                template_data.get('is_default', False),
                template_data.get('description'),
                json.dumps(template_data.get('tags', [])),
                json.dumps(template_data.get('example_input', {})),
                template_data.get('example_output')
            ))
            
            template_id = cursor.fetchone()[0]
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f"Created template: {template_id} ({template_data['template_code']})")
            return template_id
            
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Failed to create template: {error_msg}")
            
            # 检查是否是唯一约束冲突（template_code 重复）
            if 'duplicate key value violates unique constraint' in error_msg.lower():
                if 'template_code' in error_msg.lower():
                    template_code = template_data.get('template_code', 'unknown')
                    conn.rollback()
                    conn.close()
                    raise ValueError(f"Template code '{template_code}' already exists. Please use a different template code.")
            
            conn.rollback()
            conn.close()
            raise  # 重新抛出异常，让上层处理
    
    def update_template(self, template_id: int, updates: Dict[str, Any], user_id: Optional[int] = None) -> bool:
        """
        更新模板
        
        Args:
            template_id: 模板ID
            updates: 更新的字段
            user_id: 执行更新的用户ID（用于权限检查）
        
        Returns:
            成功返回True，失败返回False
        """
        conn = self.db_manager.get_connection()
        if not conn:
            return False
        
        try:
            cursor = conn.cursor()
            
            # 权限检查：用户只能更新自己的模板，除非是管理员
            if user_id:
                # 获取模板信息
                cursor.execute(
                    "SELECT created_by, scope FROM llm_prompt_templates WHERE id = %s",
                    (template_id,)
                )
                result = cursor.fetchone()
                if not result:
                    logger.error(f"Template {template_id} not found")
                    conn.close()
                    return False
                
                created_by, scope = result
                
                # 检查用户是否为管理员
                cursor.execute("SELECT is_admin FROM users WHERE id = %s", (user_id,))
                user_result = cursor.fetchone()
                is_admin = user_result[0] if user_result else False
                
                # 系统模板只能由管理员更新
                if scope == 'system' and not is_admin:
                    logger.error(f"User {user_id} (non-admin) attempted to update system template {template_id}")
                    conn.close()
                    return False
                
                # 非管理员只能更新自己创建的模板
                if not is_admin and created_by != user_id:
                    logger.error(f"User {user_id} not authorized to update template {template_id}")
                    conn.close()
                    return False
            
            # 构建更新语句
            update_fields = []
            params = []
            
            field_mapping = {
                'template_name': 'template_name',
                'module_code': 'module_code',
                'system_prompt': 'system_prompt',
                'user_prompt_template': 'user_prompt_template',
                'description': 'description',
                'recommended_provider': 'recommended_provider',
                'recommended_model': 'recommended_model',
                'temperature': 'temperature',
                'max_tokens': 'max_tokens',
                'is_active': 'is_active',
                'is_public': 'is_public',
                'is_default': 'is_default'
            }
            
            for field, db_field in field_mapping.items():
                if field in updates:
                    update_fields.append(f"{db_field} = %s")
                    params.append(updates[field])
            
            # JSON字段特殊处理
            json_fields = ['variables', 'output_format', 'tags', 'example_input']
            for field in json_fields:
                if field in updates:
                    update_fields.append(f"{field} = %s")
                    params.append(json.dumps(updates[field]))
            
            if 'example_output' in updates:
                update_fields.append("example_output = %s")
                params.append(updates['example_output'])
            
            if not update_fields:
                logger.warning("No fields to update")
                conn.close()
                return False
            
            params.append(template_id)
            query = f"UPDATE llm_prompt_templates SET {', '.join(update_fields)} WHERE id = %s"
            
            cursor.execute(query, params)
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f"Updated template: {template_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to update template: {str(e)}")
            conn.rollback()
            conn.close()
            return False
    
    def delete_template(self, template_id: int, user_id: Optional[int] = None) -> bool:
        """
        删除模板：系统模板(scope=system)软删除(is_active=false)，用户/共享模板硬删除(物理删除)。
        
        Args:
            template_id: 模板ID
            user_id: 执行删除的用户ID
        
        Returns:
            成功返回True，失败返回False
        """
        template = self.get_template(template_id)
        if not template:
            return False
        if template.get('scope') == 'system':
            # 系统模板：软删除，保留数据
            return self.update_template(template_id, {'is_active': False}, user_id)
        # user/shared 模板：硬删除（表中外键已配置 ON DELETE CASCADE / SET NULL）
        conn = self.db_manager.get_connection()
        if not conn:
            return False
        try:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM llm_prompt_templates WHERE id = %s", (template_id,))
            deleted = cursor.rowcount > 0
            conn.commit()
            cursor.close()
            conn.close()
            if deleted:
                logger.info(f"Hard deleted template: {template_id} (scope={template.get('scope')})")
            return deleted
        except Exception as e:
            logger.error(f"Failed to hard delete template: {str(e)}")
            conn.rollback()
            conn.close()
            return False
    
    # ==================== 统计分析 ====================
    
    def get_template_stats(self, template_id: int, days: int = 30) -> Dict[str, Any]:
        """
        获取模板使用统计
        
        Args:
            template_id: 模板ID
            days: 统计天数
        
        Returns:
            统计数据
        """
        conn = self.db_manager.get_connection()
        if not conn:
            return {}
        
        try:
            cursor = conn.cursor()
            
            since_date = datetime.now() - timedelta(days=days)
            
            query = """
                SELECT 
                    COUNT(*) as total_uses,
                    COUNT(DISTINCT user_id) as unique_users,
                    AVG(tokens_used) as avg_tokens,
                    AVG(cost) as avg_cost,
                    AVG(response_time_ms) as avg_response_time,
                    SUM(CASE WHEN success = true THEN 1 ELSE 0 END)::float / COUNT(*) * 100 as success_rate
                FROM llm_prompt_usage_logs
                WHERE template_id = %s AND created_at >= %s
            """
            
            cursor.execute(query, (template_id, since_date))
            row = cursor.fetchone()
            
            stats = {
                'template_id': template_id,
                'period_days': days,
                'total_uses': row[0] or 0,
                'unique_users': row[1] or 0,
                'avg_tokens': float(row[2]) if row[2] else 0,
                'avg_cost': float(row[3]) if row[3] else 0,
                'avg_response_time_ms': float(row[4]) if row[4] else 0,
                'success_rate': float(row[5]) if row[5] else 0
            }
            
            cursor.close()
            conn.close()
            
            return stats
            
        except Exception as e:
            logger.error(f"Failed to get template stats: {str(e)}")
            conn.close()
            return {}
    
    def log_template_usage(
        self,
        template_id: int,
        user_id: Optional[int],
        rendered_prompt: str,
        variables_data: Dict,
        response_content: str,
        provider: str,
        model: str,
        tokens_used: int,
        cost: float,
        response_time_ms: int,
        success: bool,
        error_message: Optional[str] = None
    ) -> bool:
        """
        记录模板使用日志
        
        Returns:
            成功返回True
        """
        conn = self.db_manager.get_connection()
        if not conn:
            return False
        
        try:
            cursor = conn.cursor()
            
            import json as _json
            variables_json = _json.dumps(variables_data, ensure_ascii=False, default=str) if variables_data else None

            query = """
                INSERT INTO llm_prompt_usage_logs (
                    template_id, user_id, rendered_prompt, variables_data,
                    response_content, provider, model, tokens_used, cost,
                    response_time_ms, success, error_message
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(query, (
                template_id, user_id, rendered_prompt, variables_json,
                response_content, provider, model, tokens_used, cost,
                response_time_ms, success, error_message
            ))
            
            # 更新模板的使用计数
            cursor.execute(
                "UPDATE llm_prompt_templates SET usage_count = usage_count + 1 WHERE id = %s",
                (template_id,)
            )
            
            conn.commit()
            cursor.close()
            conn.close()
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to log template usage: {str(e)}")
            conn.rollback()
            conn.close()
            return False
    
    def get_popular_templates(self, category_id: Optional[int] = None, limit: int = 10) -> List[Dict[str, Any]]:
        """
        获取热门模板（按使用次数排序）
        
        Args:
            category_id: 分类ID（可选）
            limit: 返回数量
        
        Returns:
            模板列表
        """
        conn = self.db_manager.get_connection()
        if not conn:
            return []
        
        try:
            cursor = conn.cursor()
            
            query = """
                SELECT id, template_code, template_name, category_id,
                       usage_count, success_rate, avg_cost
                FROM llm_prompt_templates
                WHERE is_active = true
            """
            
            params = []
            if category_id:
                query += " AND category_id = %s"
                params.append(category_id)
            
            query += " ORDER BY usage_count DESC LIMIT %s"
            params.append(limit)
            
            cursor.execute(query, params)
            rows = cursor.fetchall()
            
            templates = []
            for row in rows:
                templates.append({
                    'id': row[0],
                    'template_code': row[1],
                    'template_name': row[2],
                    'category_id': row[3],
                    'usage_count': row[4] or 0,
                    'success_rate': float(row[5]) if row[5] else None,
                    'avg_cost': float(row[6]) if row[6] else None
                })
            
            cursor.close()
            conn.close()
            
            return templates
            
        except Exception as e:
            logger.error(f"Failed to get popular templates: {str(e)}")
            conn.close()
            return []

    def get_variables_library(self) -> List[Dict[str, Any]]:
        """
        获取所有模板中使用的变量信息
        
        Returns:
            变量信息列表
        """
        conn = self.db_manager.get_connection()
        if not conn:
            logger.error("Database connection failed")
            return []
        
        try:
            cursor = conn.cursor()
            
            query = """
                SELECT 
                    t.template_code,
                    t.template_name,
                    c.category_name,
                    t.variables
                FROM llm_prompt_templates t
                LEFT JOIN llm_prompt_categories c ON t.category_id = c.id
                WHERE t.is_active = true
                AND t.variables IS NOT NULL
                AND t.variables != '{}'::jsonb
                ORDER BY c.category_name, t.template_name
            """
            
            cursor.execute(query)
            results = cursor.fetchall()
            
            variables_library = []
            variable_map = {}
            
            for row in results:
                template_code, template_name, category_name, variables_json = row
                
                if variables_json:
                    try:
                        variables = json.loads(variables_json) if isinstance(variables_json, str) else variables_json
                        
                        for var_name, var_def in variables.items():
                            if var_name not in variable_map:
                                variable_map[var_name] = {
                                    'name': var_name,
                                    'type': var_def.get('type', 'string'),
                                    'description': var_def.get('description', ''),
                                    'required': var_def.get('required', False),
                                    'default': var_def.get('default'),
                                    'examples': self._generate_variable_examples(var_name, var_def.get('type', 'string')),
                                    'used_in_templates': [],
                                    'categories': set()
                                }
                            
                            variable_map[var_name]['used_in_templates'].append({
                                'template_code': template_code,
                                'template_name': template_name,
                                'category_name': category_name
                            })
                            variable_map[var_name]['categories'].add(category_name)
                    except Exception as e:
                        logger.warning(f"Failed to parse variables for template {template_code}: {str(e)}")
                        continue
            
            # 转换set为list并排序
            for var_info in variable_map.values():
                var_info['categories'] = sorted(list(var_info['categories']))
                variables_library.append(var_info)
            
            # 按变量名排序
            variables_library.sort(key=lambda x: x['name'])
            
            cursor.close()
            conn.close()
            
            return variables_library
            
        except Exception as e:
            logger.error(f"Failed to get variables library: {str(e)}")
            return []
        finally:
            if conn:
                conn.close()

    def _generate_variable_examples(self, var_name: str, var_type: str) -> List[str]:
        """生成变量示例"""
        examples_map = {
            'title': ['新闻标题', '公司公告标题', '市场报告标题'],
            'content': ['新闻正文内容', '详细描述', '完整文本'],
            'symbols': ['AAPL,MSFT,GOOGL', '股票代码列表', '相关证券代码'],
            'max_length': ['100', '200', '500'],
            'date': ['2024-01-15', '今天', '最近一周'],
            'language': ['中文', 'English', 'zh-CN'],
            'format': ['JSON', 'Markdown', 'Plain Text'],
            'temperature': ['0.7', '0.3', '1.0'],
            'model': ['qwen-plus', 'doubao-seed', 'glm-4'],
            'provider': ['qwen', 'doubao', 'glm'],
            'description': ['详细描述', '简要说明', '功能描述'],
            'keywords': ['关键词1,关键词2', '标签列表', '搜索词'],
            'categories': ['分类1,分类2', '类别列表', '分组信息']
        }
        
        if var_name in examples_map:
            return examples_map[var_name]
        
        # 根据类型生成通用示例
        type_examples = {
            'string': [f'示例{var_name}值1', f'示例{var_name}值2'],
            'integer': ['1', '10', '100'],
            'float': ['0.5', '1.0', '2.5'],
            'boolean': ['true', 'false'],
            'array': ['["item1", "item2"]', '["value1", "value2"]'],
            'object': ['{"key": "value"}', '{"field": "data"}']
        }
        
        return type_examples.get(var_type, [f'示例{var_name}值'])

