# -*- coding: utf-8 -*-
"""
LLM配置解析器
统一解析配置优先级：Provider Config → Template Recommendations → Runtime Parameters
Global Settings 只负责策略（provider选择、fallback等），不负责参数默认值
"""

from typing import Dict, List, Optional, Any
from backend.utils.logger import get_logger

logger = get_logger("config_resolver")


class ConfigResolver:
    """
    LLM配置解析器
    
    配置优先级（从低到高）：
    1. Provider Config (参数默认值)
    2. Template Recommendations (任务特定优化)
    3. Runtime Parameters (显式传入的参数)
    
    Global Settings 不参与参数配置，只负责：
    - 选择默认provider
    - fallback策略
    - 系统行为开关
    """
    
    def __init__(self, db_manager):
        """
        初始化配置解析器
        
        Args:
            db_manager: 数据库管理器实例
        """
        self.db_manager = db_manager
        self._global_cache = None
        self._provider_cache = {}
        self._template_cache = {}
        
        logger.info("配置解析器初始化完成")
    
    def resolve_config(
        self,
        template_code: Optional[str] = None,
        provider_id: Optional[str] = None,
        runtime_params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        解析最终配置
        
        Args:
            template_code: 模板代码
            provider_id: Provider ID
            runtime_params: 运行时参数
        
        Returns:
            解析后的完整配置字典
        
        配置流程：
        1. 如果没有指定provider，从Global Settings获取default_provider
        2. 从Provider Config获取参数基准值（默认值）
        3. 如果有模板，用模板推荐值覆盖
        4. 如果有运行时参数，用运行时参数覆盖
        """
        config = {}
        
        # 步骤1: 确定使用哪个Provider（从Global Settings获取策略）
        if not provider_id:
            global_settings = self._get_global_settings()
            provider_id = self._get_setting_value(global_settings, 'default_provider', 'qwen')
            logger.debug(f"未指定provider，使用全局默认: {provider_id}")
        
        config['provider'] = provider_id
        
        # 步骤2: 从Provider Config获取参数基准值
        provider_config = self._get_provider_config(provider_id)
        if provider_config:
            config['model'] = provider_config.get('default_model', 'qwen-max')
            config['temperature'] = float(provider_config.get('temperature', 0.7))
            config['max_tokens'] = int(provider_config.get('max_tokens', 2000))
            config['top_p'] = float(provider_config.get('top_p', 0.9))
            config['timeout'] = int(provider_config.get('timeout', 90))
            
            logger.debug(
                f"Provider配置加载完成",
                provider=provider_id,
                model=config['model'],
                temperature=config['temperature'],
                max_tokens=config['max_tokens'],
                timeout=config['timeout']
            )
        else:
            logger.warning(f"未找到Provider配置: {provider_id}，使用安全默认值")
            # 提供安全的兜底值
            config['model'] = 'qwen-max'
            config['temperature'] = 0.7
            config['max_tokens'] = 2000
            config['top_p'] = 0.9
            config['timeout'] = 90
        
        # 步骤3: 模板推荐值覆盖（如果提供了模板）
        if template_code:
            template_config = self._get_template_config(template_code)
            if template_config:
                # 模板可以推荐provider
                if template_config.get('recommended_provider'):
                    template_provider = template_config['recommended_provider']
                    if template_provider != config['provider']:
                        logger.debug(f"模板推荐使用不同的provider: {template_provider}")
                        config['provider'] = template_provider
                        # 重新加载新provider的配置
                        new_provider_config = self._get_provider_config(template_provider)
                        if new_provider_config:
                            config['model'] = new_provider_config.get('default_model', config['model'])
                            config['timeout'] = int(new_provider_config.get('timeout', config['timeout']))
                
                # 模板推荐的模型
                if template_config.get('recommended_model'):
                    config['model'] = template_config['recommended_model']
                
                # 模板推荐的参数
                if template_config.get('temperature') is not None:
                    config['temperature'] = float(template_config['temperature'])
                
                if template_config.get('max_tokens'):
                    config['max_tokens'] = int(template_config['max_tokens'])
                
                logger.debug(
                    f"模板推荐配置已应用",
                    template=template_code,
                    model=config['model'],
                    temperature=config['temperature'],
                    max_tokens=config['max_tokens']
                )
        
        # 步骤4: 运行时参数覆盖（最高优先级）
        if runtime_params:
            original_config = config.copy()
            for key, value in runtime_params.items():
                if value is not None and key in ['provider', 'model', 'temperature', 'max_tokens', 'top_p', 'timeout']:
                    config[key] = value
            
            # 记录被覆盖的参数
            overridden = [k for k in runtime_params.keys() if k in config and runtime_params[k] != original_config.get(k)]
            if overridden:
                logger.debug(f"运行时参数覆盖: {overridden}")
        
        # 最终配置日志
        logger.info(
            f"配置解析完成",
            provider=config['provider'],
            model=config['model'],
            temperature=config['temperature'],
            max_tokens=config['max_tokens'],
            timeout=config['timeout'],
            has_template=bool(template_code),
            has_runtime_params=bool(runtime_params)
        )
        
        return config
    
    def get_fallback_config(self) -> Dict[str, Any]:
        """
        获取故障转移配置
        
        Returns:
            包含 fallback_enabled 和 fallback_order 的字典
        """
        global_settings = self._get_global_settings()
        
        return {
            'fallback_enabled': self._get_setting_value(global_settings, 'fallback_enabled', True),
            'fallback_order': self._get_setting_value(global_settings, 'fallback_order', ['qwen', 'doubao', 'glm'])
        }
    
    def explain_config(
        self,
        template_code: Optional[str] = None,
        provider_id: Optional[str] = None,
        runtime_params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        解释配置来源（用于调试和UI展示）
        
        Returns:
            每个参数的来源和覆盖链
        """
        explanation = {}
        
        # 确定provider
        if not provider_id:
            global_settings = self._get_global_settings()
            provider_id = self._get_setting_value(global_settings, 'default_provider', 'qwen')
            explanation['provider'] = {
                'final_value': provider_id,
                'source': 'Global Settings (default_provider)',
                'override_chain': [{
                    'layer': 'Global Settings',
                    'value': provider_id
                }]
            }
        else:
            explanation['provider'] = {
                'final_value': provider_id,
                'source': 'Explicitly specified',
                'override_chain': [{
                    'layer': 'Runtime Parameters',
                    'value': provider_id
                }]
            }
        
        # Provider配置
        provider_config = self._get_provider_config(provider_id)
        
        # 解释每个参数
        params = ['model', 'temperature', 'max_tokens', 'top_p', 'timeout']
        for param in params:
            chain = []
            
            # Provider层
            if provider_config:
                provider_key = 'default_model' if param == 'model' else param
                provider_value = provider_config.get(provider_key)
                if provider_value is not None:
                    chain.append({
                        'layer': 'Provider Config',
                        'value': provider_value
                    })
            
            # Template层
            if template_code:
                template_config = self._get_template_config(template_code)
                if template_config:
                    template_key = f'recommended_{param}' if param == 'model' else param
                    template_value = template_config.get(template_key)
                    if template_value is not None:
                        chain.append({
                            'layer': 'Template Recommendations',
                            'value': template_value
                        })
            
            # Runtime层
            if runtime_params and param in runtime_params and runtime_params[param] is not None:
                chain.append({
                    'layer': 'Runtime Parameters',
                    'value': runtime_params[param]
                })
            
            explanation[param] = {
                'final_value': chain[-1]['value'] if chain else None,
                'override_chain': chain
            }
        
        return explanation
    
    def clear_cache(self):
        """清除缓存（配置更新后调用）"""
        self._global_cache = None
        self._provider_cache = {}
        self._template_cache = {}
        logger.info("配置缓存已清除")
    
    def _get_global_settings(self) -> Dict[str, Any]:
        """获取全局设置（带缓存）"""
        if self._global_cache is None:
            self._global_cache = self.db_manager.get_llm_global_settings()
            logger.debug("全局设置已加载")
        return self._global_cache
    
    def _get_provider_config(self, provider_id: str) -> Optional[Dict[str, Any]]:
        """获取Provider配置（带缓存）"""
        if provider_id not in self._provider_cache:
            row = self.db_manager.get_llm_provider_config(provider_id)
            if row:
                self._provider_cache[provider_id] = {
                    'id': row[1],
                    'name': row[2],
                    'default_model': row[7],
                    'temperature': float(row[10]) if row[10] else 0.7,
                    'max_tokens': row[9] if row[9] else 2000,
                    'top_p': float(row[11]) if row[11] else 0.9,
                    'timeout': row[12] if row[12] else 90,
                    'enabled': row[6]
                }
                logger.debug(f"Provider配置已加载: {provider_id}")
            else:
                self._provider_cache[provider_id] = None
                logger.warning(f"Provider配置未找到: {provider_id}")
        
        return self._provider_cache.get(provider_id)
    
    def _get_template_config(self, template_code: str) -> Optional[Dict[str, Any]]:
        """获取Template配置（每次从DB读取，避免缓存导致模板更新不生效）"""
        try:
            from backend.services.llm_prompt_template_service import LLMPromptTemplateService
            template_service = LLMPromptTemplateService()
            template = template_service.get_template_by_code(template_code)
            
            if template:
                config = {
                    'recommended_provider': template.get('recommended_provider'),
                    'recommended_model': template.get('recommended_model'),
                    'temperature': template.get('temperature'),
                    'max_tokens': template.get('max_tokens')
                }
                logger.debug(f"模板配置已加载: {template_code}, model={config['recommended_model']}, temp={config['temperature']}")
                return config
            else:
                logger.warning(f"模板配置未找到: {template_code}")
                return None
        except Exception as e:
            logger.error(f"加载模板配置失败: {template_code}, 错误: {str(e)}")
            return None
    
    def _get_setting_value(self, settings: Dict[str, Any], key: str, default: Any) -> Any:
        """
        安全获取设置值
        
        Args:
            settings: 设置字典
            key: 设置键
            default: 默认值
        
        Returns:
            设置值或默认值
        """
        setting = settings.get(key, {})
        if not setting:
            return default
        
        value = setting.get('value')
        if value is None:
            return default
        
        # 处理JSON字符串
        if isinstance(value, str):
            import json
            try:
                return json.loads(value)
            except (json.JSONDecodeError, ValueError):
                return value
        
        return value

