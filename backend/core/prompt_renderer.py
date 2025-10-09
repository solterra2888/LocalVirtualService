# -*- coding: utf-8 -*-
"""
Prompt模板渲染引擎

功能：
1. 渲染prompt模板，支持变量替换
2. 验证变量
3. 解析LLM输出
4. 管理渲染上下文
"""

import json
import re
from typing import Dict, Any, Optional, List
from dataclasses import dataclass

from backend.database.db_manager import DatabaseManager
from backend.utils.database_config import get_database_config
from backend.utils.logger import get_logger

logger = get_logger("prompt_renderer")


@dataclass
class RenderedPrompt:
    """渲染后的Prompt对象"""
    system_prompt: Optional[str]
    user_prompt: str
    template_id: int
    template_code: str
    variables_used: Dict[str, Any]
    recommended_config: Dict[str, Any]
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            'system_prompt': self.system_prompt,
            'user_prompt': self.user_prompt,
            'template_id': self.template_id,
            'template_code': self.template_code,
            'variables_used': self.variables_used,
            'recommended_config': self.recommended_config
        }


@dataclass
class ValidationResult:
    """变量验证结果"""
    is_valid: bool
    errors: List[str]
    warnings: List[str]
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            'is_valid': self.is_valid,
            'errors': self.errors,
            'warnings': self.warnings
        }


class PromptRenderer:
    """Prompt模板渲染引擎"""
    
    def __init__(self):
        """初始化渲染器"""
        self.db_config = get_database_config()
        self.db_manager = DatabaseManager(self.db_config)
        self.variable_pattern = re.compile(r'\{\{(\w+)\}\}')
    
    def render(
        self, 
        template_id: Optional[int] = None,
        template_code: Optional[str] = None,
        variables_data: Dict[str, Any] = None,
        user_id: Optional[int] = None
    ) -> RenderedPrompt:
        """
        渲染模板
        
        Args:
            template_id: 模板ID（template_id和template_code至少提供一个）
            template_code: 模板代码
            variables_data: 变量数据
            user_id: 用户ID（用于获取用户偏好）
        
        Returns:
            RenderedPrompt对象
        """
        if variables_data is None:
            variables_data = {}
        
        # 1. 加载模板
        template = self._load_template(template_id, template_code, user_id)
        if not template:
            raise ValueError(f"Template not found: id={template_id}, code={template_code}")
        
        # 2. 验证变量
        validation = self.validate_variables(template, variables_data)
        if not validation.is_valid:
            error_msg = f"Variable validation failed: {', '.join(validation.errors)}"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        if validation.warnings:
            for warning in validation.warnings:
                logger.warning(f"Template rendering warning: {warning}")
        
        # 3. 合并默认值
        final_variables = self._merge_default_values(template, variables_data)
        
        # 4. 渲染系统prompt
        system_prompt = None
        if template.get('system_prompt'):
            system_prompt = self._render_text(template['system_prompt'], final_variables)
        
        # 5. 渲染用户prompt
        user_prompt = self._render_text(template['user_prompt_template'], final_variables)
        
        # 6. 构建推荐配置
        recommended_config = {
            'provider': template.get('recommended_provider'),
            'model': template.get('recommended_model'),
            'temperature': float(template.get('temperature', 0.7)),
            'max_tokens': template.get('max_tokens', 2000)
        }
        
        logger.info(
            f"Rendered template successfully",
            template_id=template['id'],
            template_code=template['template_code'],
            variables_count=len(final_variables)
        )
        
        return RenderedPrompt(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            template_id=template['id'],
            template_code=template['template_code'],
            variables_used=final_variables,
            recommended_config=recommended_config
        )
    
    def _load_template(
        self, 
        template_id: Optional[int],
        template_code: Optional[str],
        user_id: Optional[int]
    ) -> Optional[Dict[str, Any]]:
        """
        加载模板（支持用户偏好）
        
        优先级：
        1. 用户自定义模板（如果user_id提供且有偏好设置）
        2. 指定的template_id或template_code
        """
        conn = self.db_manager.get_connection()
        if not conn:
            logger.error("Database connection failed")
            return None
        
        try:
            cursor = conn.cursor()
            
            # 如果提供了user_id，先检查用户偏好
            if user_id and template_code:
                # 通过template_code找到category_id
                cursor.execute("""
                    SELECT category_id FROM llm_prompt_templates 
                    WHERE template_code = %s
                """, (template_code,))
                result = cursor.fetchone()
                if result:
                    category_id = result[0]
                    
                    # 查找用户偏好
                    cursor.execute("""
                        SELECT preferred_template_id, custom_parameters
                        FROM llm_user_template_preferences
                        WHERE user_id = %s AND category_id = %s AND is_active = true
                    """, (user_id, category_id))
                    pref = cursor.fetchone()
                    
                    if pref and pref[0]:
                        template_id = pref[0]
                        logger.info(f"Using user preferred template: {template_id}")
            
            # 加载模板
            if template_id:
                query = """
                    SELECT id, template_code, template_name, category_id,
                           system_prompt, user_prompt_template, variables,
                           output_format, recommended_provider, recommended_model,
                           temperature, max_tokens, scope, is_active
                    FROM llm_prompt_templates
                    WHERE id = %s AND is_active = true
                """
                cursor.execute(query, (template_id,))
            elif template_code:
                query = """
                    SELECT id, template_code, template_name, category_id,
                           system_prompt, user_prompt_template, variables,
                           output_format, recommended_provider, recommended_model,
                           temperature, max_tokens, scope, is_active
                    FROM llm_prompt_templates
                    WHERE template_code = %s AND is_active = true
                """
                cursor.execute(query, (template_code,))
            else:
                return None
            
            row = cursor.fetchone()
            if not row:
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
                'recommended_provider': row[8],
                'recommended_model': row[9],
                'temperature': row[10],
                'max_tokens': row[11],
                'scope': row[12],
                'is_active': row[13]
            }
            
            cursor.close()
            conn.close()
            
            return template
            
        except Exception as e:
            logger.error(f"Failed to load template: {str(e)}")
            conn.close()
            return None
    
    def _render_text(self, template_text: str, variables: Dict[str, Any]) -> str:
        """
        渲染文本，替换变量
        
        支持格式：{{variable_name}}
        """
        def replace_variable(match):
            var_name = match.group(1)
            value = variables.get(var_name)
            if value is None:
                logger.warning(f"Variable '{var_name}' not found, using empty string")
                return ''
            return str(value)
        
        return self.variable_pattern.sub(replace_variable, template_text)
    
    def _merge_default_values(
        self, 
        template: Dict[str, Any], 
        variables_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        合并默认值
        
        如果用户没有提供某个变量，使用模板定义的默认值
        """
        variables_def = template.get('variables', {})
        final_variables = {}
        
        for var_name, var_config in variables_def.items():
            if var_name in variables_data:
                final_variables[var_name] = variables_data[var_name]
            elif 'default' in var_config:
                final_variables[var_name] = var_config['default']
                logger.debug(f"Using default value for variable '{var_name}': {var_config['default']}")
        
        # 添加用户提供的但模板未定义的变量
        for var_name, value in variables_data.items():
            if var_name not in final_variables:
                final_variables[var_name] = value
                logger.debug(f"Using user-provided variable '{var_name}' (not in template definition)")
        
        return final_variables
    
    def validate_variables(
        self, 
        template: Dict[str, Any], 
        variables_data: Dict[str, Any]
    ) -> ValidationResult:
        """
        验证变量是否符合模板要求
        
        检查：
        1. 必需变量是否提供
        2. 变量类型是否正确
        3. 变量值是否在有效范围内
        """
        errors = []
        warnings = []
        
        variables_def = template.get('variables', {})
        
        # 检查必需变量
        for var_name, var_config in variables_def.items():
            is_required = var_config.get('required', False)
            has_default = 'default' in var_config
            
            if is_required and var_name not in variables_data and not has_default:
                errors.append(f"Required variable '{var_name}' is missing")
        
        # 检查变量类型
        for var_name, value in variables_data.items():
            if var_name in variables_def:
                var_config = variables_def[var_name]
                expected_type = var_config.get('type', 'string')
                
                if not self._check_type(value, expected_type):
                    errors.append(
                        f"Variable '{var_name}' has incorrect type. "
                        f"Expected: {expected_type}, Got: {type(value).__name__}"
                    )
            else:
                warnings.append(f"Variable '{var_name}' is not defined in template")
        
        is_valid = len(errors) == 0
        
        return ValidationResult(
            is_valid=is_valid,
            errors=errors,
            warnings=warnings
        )
    
    def _check_type(self, value: Any, expected_type: str) -> bool:
        """检查值的类型是否符合预期"""
        type_mapping = {
            'string': str,
            'integer': int,
            'float': (int, float),
            'boolean': bool,
            'array': list,
            'object': dict
        }
        
        expected_python_type = type_mapping.get(expected_type)
        if expected_python_type is None:
            logger.warning(f"Unknown type: {expected_type}, skipping type check")
            return True
        
        return isinstance(value, expected_python_type)
    
    def parse_output(
        self, 
        template_id: int, 
        llm_response: str
    ) -> Optional[Dict[str, Any]]:
        """
        根据模板的output_format解析LLM响应
        
        Args:
            template_id: 模板ID
            llm_response: LLM的响应文本
        
        Returns:
            解析后的结构化数据，如果解析失败返回None
        """
        # 加载模板
        template = self._load_template(template_id=template_id, template_code=None, user_id=None)
        if not template:
            logger.error(f"Template {template_id} not found for output parsing")
            return None
        
        output_format = template.get('output_format', {})
        format_type = output_format.get('type', 'text')
        
        if format_type == 'json':
            return self._parse_json_output(llm_response, output_format)
        elif format_type == 'text':
            return {'content': llm_response}
        else:
            logger.warning(f"Unknown output format type: {format_type}")
            return {'content': llm_response}
    
    def _parse_json_output(self, response: str, output_format: Dict) -> Optional[Dict[str, Any]]:
        """解析JSON格式的输出"""
        try:
            # 尝试提取JSON（有时LLM会在JSON前后添加文字说明）
            json_match = re.search(r'\{.*\}', response, re.DOTALL)
            if json_match:
                json_str = json_match.group(0)
                parsed = json.loads(json_str)
                logger.info("Successfully parsed JSON output")
                return parsed
            else:
                # 直接尝试解析
                parsed = json.loads(response)
                return parsed
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON output: {str(e)}")
            logger.debug(f"Response content: {response[:200]}...")
            # 返回原始文本
            return {'content': response, 'parse_error': str(e)}
    
    def get_template_variables(self, template_id: int) -> Dict[str, Any]:
        """
        获取模板的变量定义
        
        Args:
            template_id: 模板ID
        
        Returns:
            变量定义字典
        """
        template = self._load_template(template_id=template_id, template_code=None, user_id=None)
        if template:
            return template.get('variables', {})
        return {}

