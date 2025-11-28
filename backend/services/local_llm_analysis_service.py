# -*- coding: utf-8 -*-
"""
本地LLM文件分析服务
使用Prompt Template进行文件内容分析
"""

import asyncio
import time
import os
from typing import Dict, List, Any, Optional, Callable, Tuple
from backend.core.llm_framework import llm_service, LLMMessage
from backend.core.prompt_renderer import PromptRenderer
from backend.utils.logger import get_logger

logger = get_logger("local_llm_analysis_service")


class LocalLLMAnalysisService:
    """本地LLM文件分析服务"""
    
    def __init__(
        self, 
        template_id: Optional[int] = None,
        template_code: Optional[str] = None,
        log_callback: Optional[Callable] = None
    ):
        """
        初始化本地LLM分析服务
        
        Args:
            template_id: Prompt模板ID
            template_code: Prompt模板代码（template_id和template_code至少提供一个）
            log_callback: 日志回调函数
        """
        if not template_id and not template_code:
            raise ValueError("必须提供template_id或template_code")
        
        self.template_id = template_id
        self.template_code = template_code
        self.log_callback = log_callback
        self.renderer = PromptRenderer()
        
        logger.info(
            f"初始化本地LLM分析服务",
            template_id=template_id,
            template_code=template_code
        )
    
    def log(self, message: str, level: str = "info"):
        """发送日志消息"""
        # 根据level参数调用对应的日志方法
        log_method = getattr(logger, level.lower(), logger.info)
        log_method(message)
        if self.log_callback:
            self.log_callback(log_message=message)
    
    async def analyze_file_content(
        self,
        file_content: str,
        file_name: str = None,
        custom_variables: Dict[str, Any] = None,
        user_id: Optional[int] = None,
        provider: Optional[str] = None,
        model: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        分析单个文件内容
        
        Args:
            file_content: 文件内容（Markdown格式）
            file_name: 文件名（用于日志）
            custom_variables: 自定义模板变量
            user_id: 用户ID
            provider: 指定LLM提供商（可选，覆盖模板推荐）
            model: 指定模型（可选，覆盖模板推荐）
        
        Returns:
            分析结果字典，包含:
            - report_content: 分析报告内容
            - total_tokens: 使用的token数
            - total_price: 费用
            - response_time: 响应时间
            - model: 使用的模型
            - provider: LLM提供商
            - model_usage: 详细使用信息
        """
        start_time = time.time()
        file_prefix = f"[{file_name}] " if file_name else ""
        
        try:
            self.log(f"{file_prefix}开始使用本地LLM分析...")
            
            # 1. 准备模板变量
            variables = custom_variables.copy() if custom_variables else {}
            variables['file_content'] = file_content
            
            self.log(f"{file_prefix}准备模板变量: {list(variables.keys())}")
            
            # 2. 渲染Prompt
            self.log(f"{file_prefix}渲染Prompt模板...")
            rendered = self.renderer.render(
                template_id=self.template_id,
                template_code=self.template_code,
                variables_data=variables,
                user_id=user_id
            )
            
            # 3. 构建消息
            messages = []
            if rendered.system_prompt:
                messages.append(LLMMessage(role="system", content=rendered.system_prompt))
            messages.append(LLMMessage(role="user", content=rendered.user_prompt))
            
            # 4. 使用 ConfigResolver 解析配置（新架构）
            # 准备Runtime参数（如果有显式传入的provider/model）
            runtime_params = {}
            if provider:
                runtime_params['provider'] = provider
            if model:
                runtime_params['model'] = model
            
            # 初始化ConfigResolver并解析配置
            try:
                from backend.core.config_resolver import ConfigResolver
                from backend.utils.database_config import get_database_config
                from backend.database.db_manager import DatabaseManager
                
                db_config = get_database_config()
                db_manager = DatabaseManager(db_config)
                resolver = ConfigResolver(db_manager)
                
                # 解析最终配置（优先级：Runtime > Template > Provider）
                resolved_config = resolver.resolve_config(
                    template_code=self.template_code or rendered.template_code,
                    provider_id=provider,
                    runtime_params=runtime_params
                )
                
                final_provider = resolved_config['provider']
                final_model = resolved_config['model']
                temperature = resolved_config['temperature']
                max_tokens = resolved_config['max_tokens']
                
            except Exception as e:
                # 如果ConfigResolver失败，回退到旧逻辑
                logger.warning(f"ConfigResolver初始化失败，使用旧配置逻辑: {str(e)}")
                final_provider = provider or rendered.recommended_config.get('provider')
                final_model = model or rendered.recommended_config.get('model')
                temperature = rendered.recommended_config.get('temperature')
                max_tokens = rendered.recommended_config.get('max_tokens')
            
            self.log(
                f"{file_prefix}调用LLM: provider={final_provider}, model={final_model}, "
                f"temperature={temperature}, max_tokens={max_tokens}"
            )
            
            # 5. 调用LLM服务
            llm_start_time = time.time()
            response = await llm_service.chat_completion(
                messages,
                provider=final_provider,
                model=final_model,
                temperature=temperature,
                max_tokens=max_tokens
            )
            llm_end_time = time.time()
            
            # 6. 计算处理时间
            total_time = time.time() - start_time
            llm_time = llm_end_time - llm_start_time
            
            self.log(
                f"{file_prefix}✅ LLM分析完成: "
                f"总时间={total_time:.2f}s, LLM时间={llm_time:.2f}s, "
                f"tokens={response.usage.get('total_tokens', 0)}, "
                f"费用={response.cost:.6f} {response.currency}"
            )
            
            # 7. 构建返回结果
            result = {
                'report_content': response.content,
                'total_tokens': response.usage.get('total_tokens', 0),
                'total_price': response.cost,
                'response_time': llm_time,
                'model': response.model,
                'provider': response.provider,
                'currency': response.currency,
                'model_usage': [{
                    'model_name': response.model,
                    'total_tokens': response.usage.get('total_tokens', 0),
                    'total_price': response.cost,
                    'currency': response.currency
                }]
            }
            
            return result
            
        except Exception as e:
            error_msg = f"{file_prefix}本地LLM分析失败: {str(e)}"
            self.log(error_msg, "error")
            logger.error(error_msg, exc_info=True)
            raise
    
    async def process_all_contents(
        self,
        successful_results: List[Dict[str, Any]],
        custom_variables: Dict[str, Any] = None,
        user_id: Optional[int] = None,
        provider: Optional[str] = None,
        model: Optional[str] = None
    ) -> Tuple[Optional[str], List[Dict[str, Any]]]:
        """
        批量处理文件内容
        
        Args:
            successful_results: 成功处理的文件结果列表
            custom_variables: 自定义模板变量
            user_id: 用户ID
            provider: 指定LLM提供商
            model: 指定模型
        
        Returns:
            元组 (final_report, file_processing_times)
            - final_report: 最后一个文件的分析报告
            - file_processing_times: 每个文件的处理时间和费用信息列表
        """
        if not successful_results:
            self.log("没有成功处理的文件内容可分析", "warning")
            return None, []
        
        self.log(f"🚀 开始批量处理 {len(successful_results)} 个文件内容...")
        
        final_report = None
        file_processing_times = []
        
        for index, result in enumerate(successful_results):
            content = result.get('content', '')
            original_file = result.get('original_file', '')
            file_name = os.path.basename(original_file)
            
            self.log(f"📄 处理第 {index+1}/{len(successful_results)} 个文件: {file_name}")
            self.log(f"   文件内容长度: {len(content)} 字符")
            
            try:
                # 分析文件内容
                analysis_result = await self.analyze_file_content(
                    file_content=content,
                    file_name=file_name,
                    custom_variables=custom_variables,
                    user_id=user_id,
                    provider=provider,
                    model=model
                )
                
                # 如果是最后一个文件，保存报告内容
                if index == len(successful_results) - 1:
                    final_report = analysis_result['report_content']
                    self.log(f"📋 保存最终报告（来自文件: {file_name}）")
                
                # 添加此文件的处理信息
                file_info = {
                    'file': file_name,
                    'file_path': original_file,
                    'ai_time': analysis_result['response_time'],
                    'total_tokens': analysis_result['total_tokens'],
                    'total_price': analysis_result['total_price'],
                    'model_usage': analysis_result['model_usage'],
                    'model': analysis_result['model'],
                    'provider': analysis_result['provider']
                }
                file_processing_times.append(file_info)
                
                self.log(
                    f"✅ 文件 '{file_name}' 分析完成: "
                    f"耗时={analysis_result['response_time']:.2f}s, "
                    f"tokens={analysis_result['total_tokens']}, "
                    f"费用={analysis_result['total_price']:.6f} {analysis_result['currency']}"
                )
                
            except Exception as e:
                error_msg = f"文件 '{file_name}' 分析失败: {str(e)}"
                self.log(error_msg, "error")
                
                # 添加失败记录
                file_info = {
                    'file': file_name,
                    'file_path': original_file,
                    'ai_time': 0,
                    'total_tokens': 0,
                    'total_price': 0,
                    'model_usage': [],
                    'error': str(e)
                }
                file_processing_times.append(file_info)
        
        # 汇总统计
        total_tokens = sum(f.get('total_tokens', 0) for f in file_processing_times)
        total_price = sum(f.get('total_price', 0) for f in file_processing_times)
        total_time = sum(f.get('ai_time', 0) for f in file_processing_times)
        
        self.log(
            f"📊 批量处理完成: "
            f"成功={len([f for f in file_processing_times if 'error' not in f])}/{len(successful_results)}, "
            f"总时间={total_time:.2f}s, "
            f"总tokens={total_tokens}, "
            f"总费用={total_price:.6f}"
        )
        
        if not final_report:
            self.log("⚠️ 警告: 未能生成最终报告", "warning")
        
        return final_report, file_processing_times
    
    def get_template_info(self) -> Dict[str, Any]:
        """
        获取模板信息
        
        Returns:
            模板信息字典
        """
        try:
            template = self.renderer._load_template(
                template_id=self.template_id,
                template_code=self.template_code,
                user_id=None
            )
            return template if template else {}
        except Exception as e:
            logger.error(f"获取模板信息失败: {str(e)}")
            return {}

