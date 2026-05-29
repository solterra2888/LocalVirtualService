# -*- coding: utf-8 -*-
"""
统一的大语言模型框架
支持中国主流大模型提供商
"""

import asyncio
import json
import os
import time
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, AsyncIterator, Any, Union
from dataclasses import dataclass
from enum import Enum
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from backend.utils.logger import get_logger
from backend.utils.env_loader import load_project_env
from backend.utils.processing_timeout import get_file_processing_timeout_seconds
from backend.database.db_manager import DatabaseManager
from backend.utils.database_config import get_database_config
from backend.core.llm_pricing import calculate_cost


class LLMProviderType(Enum):
    """LLM提供商类型枚举"""
    QWEN = "qwen"
    DOUBAO = "doubao"
    GLM = "glm"
    CLAUDE = "claude"


class LLMModelStatus(Enum):
    """模型状态枚举"""
    CONNECTED = "connected"
    DISCONNECTED = "disconnected"
    TESTING = "testing"
    ERROR = "error"


@dataclass
class LLMMessage:
    """LLM消息结构"""
    role: str  # system, user, assistant
    content: str
    name: Optional[str] = None


@dataclass
class LLMResponse:
    """LLM响应结构"""
    content: str
    model: str
    provider: str
    usage: Dict[str, Any]
    response_time: float
    cost: float = 0.0
    currency: str = "RMB"


@dataclass
class LLMConfig:
    """LLM配置结构"""
    provider_type: LLMProviderType
    api_key: str
    base_url: str
    default_model: str
    models: List[str]
    max_tokens: int = 2000
    temperature: float = 0.7
    top_p: float = 0.8
    timeout: int = 90  # 读取超时增加到90秒，适应长文本分析和内容审查
    connect_timeout: int = 15  # 连接超时15秒
    retry_count: int = 2  # 减少重试次数（从3改为2），避免总体等待时间过长
    enabled: bool = True


# ===== Fallback 错误分类 =====
# transient: 网络/超时/限流/5xx —— 换家大概率能成，值得整链兜底
# content_filtered: 内容审查拦截 —— 同生态(qwen/doubao/glm)审查策略相近，换家通常仍被拒；默认快速失败，避免浪费时间与费用
# client_error: 其它 4xx（鉴权、模型不存在、参数错误）—— 多为 provider 自身配置问题，换家可能有效，允许兜底
# unknown: 兜底归类，按可兜底处理
ERROR_TRANSIENT = "transient"
ERROR_CONTENT_FILTERED = "content_filtered"
ERROR_CLIENT = "client_error"
ERROR_UNKNOWN = "unknown"

# 内容审查命中后是否继续兜底其它 provider。默认 False（快速失败、缩短重试）。
FALLBACK_ON_CONTENT_FILTER = False


def classify_llm_error(exc: Exception) -> str:
    """将 provider 调用异常归类，用于决定是否值得故障转移。"""
    msg = str(exc).lower()

    # 内容审查（阿里 data_inspection_failed / 通用 content moderation 关键词）
    if "data_inspection_failed" in msg or "inappropriate content" in msg or "content_filter" in msg:
        return ERROR_CONTENT_FILTERED

    # 网络层 / 超时 / 连接重置
    if any(k in msg for k in ("timed out", "timeout", "connection reset", "connection aborted",
                              "max retries exceeded", "connection broken", "read timed out")):
        return ERROR_TRANSIENT

    # HTTP 状态码（异常文本形如 "API request failed: <code> - ..."）
    if "429" in msg or "rate limit" in msg:
        return ERROR_TRANSIENT
    if any(code in msg for code in ("500", "502", "503", "504")):
        return ERROR_TRANSIENT
    if any(code in msg for code in ("400", "401", "403", "404", "model_not_found",
                                    "invalid_request", "invalid_api_key")):
        return ERROR_CLIENT

    return ERROR_UNKNOWN


def should_fallback(category: str) -> bool:
    """根据错误类别判断是否应尝试其它 provider。"""
    if category == ERROR_CONTENT_FILTERED:
        return FALLBACK_ON_CONTENT_FILTER
    # transient / client_error / unknown 均允许兜底
    return True


class LLMProvider(ABC):
    """LLM提供商抽象基类"""
    
    def __init__(self, config: LLMConfig):
        self.config = config
        self.logger = get_logger(f"llm_provider_{config.provider_type.value}")
        self.session = self._create_session()
        self.status = LLMModelStatus.DISCONNECTED
        
    def _create_session(self) -> requests.Session:
        """创建配置了重试的HTTP会话"""
        session = requests.Session()
        
        # 配置重试策略，但不包含read timeout（由请求时设置）
        retry_strategy = Retry(
            total=self.config.retry_count,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "POST", "PUT", "DELETE", "OPTIONS", "TRACE"],
            # 不设置read参数，避免覆盖请求时的timeout设置
            raise_on_status=False
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        return session
    
    @abstractmethod
    async def chat_completion(
        self, 
        messages: List[LLMMessage], 
        model: Optional[str] = None,
        **kwargs
    ) -> LLMResponse:
        """聊天补全抽象方法"""
        pass
    
    @abstractmethod
    async def stream_completion(
        self, 
        messages: List[LLMMessage], 
        model: Optional[str] = None,
        **kwargs
    ) -> AsyncIterator[LLMResponse]:
        """流式响应抽象方法"""
        pass
    
    @abstractmethod
    async def test_connection(self) -> bool:
        """测试连接抽象方法"""
        pass
    
    def get_provider_info(self) -> Dict[str, Any]:
        """获取提供商信息"""
        return {
            "provider": self.config.provider_type.value,
            "status": self.status.value,
            "models": self.config.models,
            "default_model": self.config.default_model,
            "enabled": self.config.enabled
        }

    def _resolve_read_timeout(self, kwargs: Dict[str, Any]) -> int:
        """单次请求的读超时：kwargs['timeout'] 优先于 provider 配置。"""
        raw = kwargs.get("timeout")
        if raw is not None:
            return int(raw)
        return int(self.config.timeout)


class QwenProvider(LLMProvider):
    """Qwen provider implementation"""
    
    def __init__(self, config: LLMConfig):
        super().__init__(config)
        # 使用OpenAI兼容模式端点
        self.api_url = f"{self.config.base_url}/compatible-mode/v1/chat/completions"
        # Embedding API 端点
        self.embedding_url = "https://dashscope.aliyuncs.com/api/v1/services/embeddings/text-embedding/text-embedding"
    
    async def chat_completion(
        self, 
        messages: List[LLMMessage], 
        model: Optional[str] = None,
        **kwargs
    ) -> LLMResponse:
        """Qwen chat completion implementation (using OpenAI compatible format)"""
        start_time = time.time()
        
        model = model or self.config.default_model
        
        headers = {
            "Authorization": f"Bearer {self.config.api_key}",
            "Content-Type": "application/json"
        }
        
        # 转换消息格式为OpenAI兼容格式
        formatted_messages = [
            {"role": msg.role, "content": msg.content} 
            for msg in messages
        ]
        
        data = {
            "model": model,
            "messages": formatted_messages,
            "max_tokens": kwargs.get("max_tokens", self.config.max_tokens),
            "temperature": kwargs.get("temperature", self.config.temperature),
            "top_p": kwargs.get("top_p", self.config.top_p)
        }
        
        try:
            response = await self._make_request(
                headers, data, read_timeout=self._resolve_read_timeout(kwargs)
            )
            response_time = time.time() - start_time
            
            if response.status_code == 200:
                result = response.json()
                # OpenAI兼容格式的响应
                content = result.get("choices", [{}])[0].get("message", {}).get("content", "")
                usage = result.get("usage", {})
                cost, currency = self._calculate_cost(usage, model)
                return LLMResponse(
                    content=content,
                    model=model,
                    provider=self.config.provider_type.value,
                    usage=usage,
                    response_time=response_time,
                    cost=cost,
                    currency=currency,
                )
            else:
                raise Exception(f"API request failed: {response.status_code} - {response.text}")
                
        except Exception as e:
            self.logger.error(f"Chat completion failed: {str(e)}")
            raise
    
    async def stream_completion(
        self, 
        messages: List[LLMMessage], 
        model: Optional[str] = None,
        **kwargs
    ) -> AsyncIterator[LLMResponse]:
        """Qwen streaming response implementation"""
        # TODO: 实现流式响应
        # 目前先返回普通响应
        response = await self.chat_completion(messages, model, **kwargs)
        yield response
    
    async def test_connection(self) -> bool:
        """Test Qwen connection"""
        try:
            self.status = LLMModelStatus.TESTING
            self.logger.info(f"Starting Qwen connection test, API URL: {self.api_url}")
            self.logger.info(f"测试参数 - 默认模型: {self.config.default_model}")
            self.logger.info(f"测试参数 - 温度值: {self.config.temperature}")
            self.logger.info(f"测试参数 - 最大Token: {self.config.max_tokens}")
            self.logger.info(f"测试参数 - Top P: {self.config.top_p}")
            self.logger.info(f"测试参数 - 超时时间: {self.config.timeout}秒")
            self.logger.info(f"测试参数 - 重试次数: {self.config.retry_count}")
            self.logger.info(f"测试参数 - 可用模型: {self.config.models}")
            
            test_messages = [LLMMessage(role="user", content="你好")]
            response = await self.chat_completion(test_messages)
            
            if response.content:
                self.status = LLMModelStatus.CONNECTED
                self.logger.info(f"Qwen connection test successful, response: {response.content[:50]}...")
                self.logger.info(f"实际使用模型: {response.model}")
                self.logger.info(f"响应时间: {response.response_time:.2f}秒")
                return True
            else:
                self.status = LLMModelStatus.ERROR
                self.logger.error(f"Qwen connection test failed: empty response content")
                return False
                
        except Exception as e:
            self.logger.error(f"Qwen connection test failed: {str(e)}")
            import traceback
            self.logger.error(f"错误堆栈: {traceback.format_exc()}")
            self.status = LLMModelStatus.ERROR
            return False
    
    
    async def _make_request(self, headers: Dict, data: Dict, read_timeout: Optional[int] = None) -> requests.Response:
        """发送异步请求。read_timeout 优先于 provider 配置（支持模板/策略动态抬升）。"""
        loop = asyncio.get_event_loop()
        connect_timeout = getattr(self.config, 'connect_timeout', 15)
        effective_read_timeout = int(read_timeout) if read_timeout else self.config.timeout
        return await loop.run_in_executor(
            None, 
            lambda: self.session.post(
                self.api_url, 
                headers=headers, 
                json=data,
                timeout=(connect_timeout, effective_read_timeout)  # 连接超时固定，读取超时可被动态覆盖
            )
        )
    
    def _calculate_cost(self, usage: Dict, model: str) -> tuple:
        """按官方定价表计算本次调用费用。"""
        prompt_tokens     = usage.get("prompt_tokens", 0)
        completion_tokens = usage.get("completion_tokens", 0)
        if not prompt_tokens and not completion_tokens:
            # 部分响应只返回 total_tokens，按 3:1 比例粗估拆分
            total = usage.get("total_tokens", 0)
            prompt_tokens     = int(total * 0.75)
            completion_tokens = total - prompt_tokens
        return calculate_cost("qwen", model, prompt_tokens, completion_tokens)
    
    async def generate_embeddings(
        self, 
        texts: List[str],
        model: str = "text-embedding-v3",
        text_type: str = "document"
    ) -> List[List[float]]:
        """
        生成文本向量（Embedding）
        
        Args:
            texts: 文本列表（最多25条）
            model: embedding模型名称，默认 text-embedding-v3
            text_type: 文本类型，"document" 或 "query"
            
        Returns:
            向量列表，每个向量是1024维的浮点数列表
            
        Raises:
            Exception: API调用失败
            
        Example:
            embeddings = await qwen_provider.generate_embeddings(
                texts=["新闻标题1", "新闻标题2"],
                text_type="document"
            )
        """
        if not texts or len(texts) == 0:
            raise ValueError("texts参数不能为空")
        
        if len(texts) > 25:
            self.logger.warning(f"文本数量超过25条，将只处理前25条")
            texts = texts[:25]
        
        headers = {
            "Authorization": f"Bearer {self.config.api_key}",
            "Content-Type": "application/json"
        }
        
        data = {
            "model": model,
            "input": {
                "texts": texts
            },
            "parameters": {
                "text_type": text_type
            }
        }
        
        try:
            start_time = time.time()
            response = await self._make_embedding_request(headers, data)
            response_time = time.time() - start_time
            
            if response.status_code == 200:
                result = response.json()
                embeddings = []
                
                # 解析响应
                output = result.get("output", {})
                embedding_list = output.get("embeddings", [])
                
                for item in embedding_list:
                    embedding = item.get("embedding")
                    if embedding:
                        embeddings.append(embedding)
                
                # 记录使用情况
                usage = result.get("usage", {})
                total_tokens = usage.get("total_tokens", 0)
                
                self.logger.debug(
                    f"Generated embeddings for {len(texts)} texts, "
                    f"tokens: {total_tokens}, time: {response_time:.2f}s"
                )
                
                return embeddings
            else:
                error_msg = f"Embedding API failed: {response.status_code} - {response.text}"
                self.logger.error(error_msg)
                raise Exception(error_msg)
                
        except Exception as e:
            self.logger.error(f"Generate embeddings failed: {str(e)}")
            raise
    
    async def _make_embedding_request(self, headers: Dict, data: Dict) -> requests.Response:
        """发送Embedding API请求"""
        loop = asyncio.get_event_loop()
        connect_timeout = getattr(self.config, 'connect_timeout', 15)
        return await loop.run_in_executor(
            None, 
            lambda: self.session.post(
                self.embedding_url, 
                headers=headers, 
                json=data,
                timeout=(connect_timeout, self.config.timeout)  # 使用配置的超时时间
            )
        )


class DoubaoProvider(LLMProvider):
    """Doubao provider implementation"""
    
    def __init__(self, config: LLMConfig):
        super().__init__(config)
        self.api_url = f"{self.config.base_url}/api/v3/chat/completions"
    
    async def chat_completion(
        self, 
        messages: List[LLMMessage], 
        model: Optional[str] = None,
        **kwargs
    ) -> LLMResponse:
        """Doubao chat completion implementation"""
        start_time = time.time()
        
        model = model or self.config.default_model
        
        headers = {
            "Authorization": f"Bearer {self.config.api_key}",
            "Content-Type": "application/json"
        }
        
        # 转换消息格式为OpenAI兼容格式
        formatted_messages = [
            {"role": msg.role, "content": msg.content} 
            for msg in messages
        ]
        
        data = {
            "model": model,
            "messages": formatted_messages,
            "max_tokens": kwargs.get("max_tokens", self.config.max_tokens),
            "temperature": kwargs.get("temperature", self.config.temperature),
            "top_p": kwargs.get("top_p", self.config.top_p)
        }
        
        try:
            response = await self._make_request(headers, data, read_timeout=self._resolve_read_timeout(kwargs))
            response_time = time.time() - start_time
            
            if response.status_code == 200:
                result = response.json()
                content = result.get("choices", [{}])[0].get("message", {}).get("content", "")
                usage = result.get("usage", {})
                cost, currency = self._calculate_cost(usage, model)
                return LLMResponse(
                    content=content,
                    model=model,
                    provider=self.config.provider_type.value,
                    usage=usage,
                    response_time=response_time,
                    cost=cost,
                    currency=currency,
                )
            else:
                raise Exception(f"API request failed: {response.status_code} - {response.text}")
                
        except Exception as e:
            self.logger.error(f"Chat completion failed: {str(e)}")
            raise
    
    async def stream_completion(
        self, 
        messages: List[LLMMessage], 
        model: Optional[str] = None,
        **kwargs
    ) -> AsyncIterator[LLMResponse]:
        """Doubao streaming response implementation"""
        # TODO: 实现流式响应
        response = await self.chat_completion(messages, model, **kwargs)
        yield response
    
    async def test_connection(self) -> bool:
        """Test Doubao connection"""
        try:
            self.status = LLMModelStatus.TESTING
            self.logger.info(f"Starting Doubao connection test, API URL: {self.api_url}")
            self.logger.info(f"测试参数 - 默认模型: {self.config.default_model}")
            self.logger.info(f"测试参数 - 温度值: {self.config.temperature}")
            self.logger.info(f"测试参数 - 最大Token: {self.config.max_tokens}")
            self.logger.info(f"测试参数 - Top P: {self.config.top_p}")
            self.logger.info(f"测试参数 - 超时时间: {self.config.timeout}秒")
            self.logger.info(f"测试参数 - 重试次数: {self.config.retry_count}")
            self.logger.info(f"测试参数 - 可用模型: {self.config.models}")
            
            test_messages = [LLMMessage(role="user", content="你好")]
            response = await self.chat_completion(test_messages)
            
            if response.content:
                self.status = LLMModelStatus.CONNECTED
                self.logger.info(f"Doubao connection test successful, response: {response.content[:50]}...")
                self.logger.info(f"实际使用模型: {response.model}")
                self.logger.info(f"响应时间: {response.response_time:.2f}秒")
                return True
            else:
                self.status = LLMModelStatus.ERROR
                self.logger.error(f"Doubao connection test failed: empty response content")
                return False
                
        except Exception as e:
            self.logger.error(f"Doubao connection test failed: {str(e)}")
            import traceback
            self.logger.error(f"错误堆栈: {traceback.format_exc()}")
            self.status = LLMModelStatus.ERROR
            return False
    
    async def _make_request(self, headers: Dict, data: Dict, read_timeout: Optional[int] = None) -> requests.Response:
        """发送异步请求。read_timeout 优先于 provider 配置（支持模板/策略动态抬升）。"""
        loop = asyncio.get_event_loop()
        connect_timeout = getattr(self.config, 'connect_timeout', 15)
        effective_read_timeout = int(read_timeout) if read_timeout else self.config.timeout
        return await loop.run_in_executor(
            None, 
            lambda: self.session.post(
                self.api_url, 
                headers=headers, 
                json=data,
                timeout=(connect_timeout, effective_read_timeout)  # 连接超时固定，读取超时可被动态覆盖
            )
        )
    
    def _calculate_cost(self, usage: Dict, model: str) -> tuple:
        """按官方定价表计算本次调用费用。"""
        prompt_tokens     = usage.get("prompt_tokens", 0)
        completion_tokens = usage.get("completion_tokens", 0)
        if not prompt_tokens and not completion_tokens:
            total = usage.get("total_tokens", 0)
            prompt_tokens     = int(total * 0.75)
            completion_tokens = total - prompt_tokens
        return calculate_cost("doubao", model, prompt_tokens, completion_tokens)




class GLMProvider(LLMProvider):
    """GLM provider implementation"""
    
    def __init__(self, config: LLMConfig):
        super().__init__(config)
        self.api_url = f"{self.config.base_url}/api/paas/v4/chat/completions"
    
    async def chat_completion(
        self, 
        messages: List[LLMMessage], 
        model: Optional[str] = None,
        **kwargs
    ) -> LLMResponse:
        """GLM chat completion implementation"""
        start_time = time.time()
        
        model = model or self.config.default_model
        
        headers = {
            "Authorization": f"Bearer {self.config.api_key}",
            "Content-Type": "application/json"
        }
        
        # 转换消息格式为OpenAI兼容格式
        formatted_messages = [
            {"role": msg.role, "content": msg.content} 
            for msg in messages
        ]
        
        data = {
            "model": model,
            "messages": formatted_messages,
            "max_tokens": kwargs.get("max_tokens", self.config.max_tokens),
            "temperature": kwargs.get("temperature", self.config.temperature),
            "top_p": kwargs.get("top_p", self.config.top_p)
        }
        
        try:
            response = await self._make_request(headers, data, read_timeout=self._resolve_read_timeout(kwargs))
            response_time = time.time() - start_time
            
            if response.status_code == 200:
                result = response.json()
                content = result.get("choices", [{}])[0].get("message", {}).get("content", "")
                usage = result.get("usage", {})
                cost, currency = self._calculate_cost(usage, model)
                return LLMResponse(
                    content=content,
                    model=model,
                    provider=self.config.provider_type.value,
                    usage=usage,
                    response_time=response_time,
                    cost=cost,
                    currency=currency,
                )
            else:
                raise Exception(f"API request failed: {response.status_code} - {response.text}")
                
        except Exception as e:
            self.logger.error(f"Chat completion failed: {str(e)}")
            raise
    
    async def stream_completion(
        self, 
        messages: List[LLMMessage], 
        model: Optional[str] = None,
        **kwargs
    ) -> AsyncIterator[LLMResponse]:
        """GLM streaming response implementation"""
        # TODO: 实现流式响应
        response = await self.chat_completion(messages, model, **kwargs)
        yield response
    
    async def test_connection(self) -> bool:
        """Test GLM connection"""
        try:
            self.status = LLMModelStatus.TESTING
            self.logger.info(f"Starting GLM connection test, API URL: {self.api_url}")
            self.logger.info(f"测试参数 - 默认模型: {self.config.default_model}")
            self.logger.info(f"测试参数 - 温度值: {self.config.temperature}")
            self.logger.info(f"测试参数 - 最大Token: {self.config.max_tokens}")
            self.logger.info(f"测试参数 - Top P: {self.config.top_p}")
            self.logger.info(f"测试参数 - 超时时间: {self.config.timeout}秒")
            self.logger.info(f"测试参数 - 重试次数: {self.config.retry_count}")
            self.logger.info(f"测试参数 - 可用模型: {self.config.models}")
            
            test_messages = [LLMMessage(role="user", content="你好")]
            response = await self.chat_completion(test_messages)
            
            if response.content:
                self.status = LLMModelStatus.CONNECTED
                self.logger.info(f"GLM connection test successful, response: {response.content[:50]}...")
                self.logger.info(f"实际使用模型: {response.model}")
                self.logger.info(f"响应时间: {response.response_time:.2f}秒")
                return True
            else:
                self.status = LLMModelStatus.ERROR
                self.logger.error(f"GLM connection test failed: empty response content")
                return False
                
        except Exception as e:
            self.logger.error(f"GLM connection test failed: {str(e)}")
            import traceback
            self.logger.error(f"错误堆栈: {traceback.format_exc()}")
            self.status = LLMModelStatus.ERROR
            return False
    
    async def _make_request(self, headers: Dict, data: Dict, read_timeout: Optional[int] = None) -> requests.Response:
        """发送异步请求。read_timeout 优先于 provider 配置（支持模板/策略动态抬升）。"""
        loop = asyncio.get_event_loop()
        connect_timeout = getattr(self.config, 'connect_timeout', 15)
        effective_read_timeout = int(read_timeout) if read_timeout else self.config.timeout
        return await loop.run_in_executor(
            None, 
            lambda: self.session.post(
                self.api_url, 
                headers=headers, 
                json=data,
                timeout=(connect_timeout, effective_read_timeout)  # 连接超时固定，读取超时可被动态覆盖
            )
        )
    
    def _calculate_cost(self, usage: Dict, model: str) -> tuple:
        """按官方定价表计算本次调用费用。"""
        prompt_tokens     = usage.get("prompt_tokens", 0)
        completion_tokens = usage.get("completion_tokens", 0)
        if not prompt_tokens and not completion_tokens:
            total = usage.get("total_tokens", 0)
            prompt_tokens     = int(total * 0.75)
            completion_tokens = total - prompt_tokens
        return calculate_cost("glm", model, prompt_tokens, completion_tokens)


class ClaudeProvider(LLMProvider):
    """Anthropic Claude provider implementation（通过 OpenAI 兼容网关，如 CherryIN）。

    CherryIN（https://open.cherryin.net）及同类聚合网关均使用 OpenAI 兼容格式：
      - 端点：/v1/chat/completions
      - 鉴权：Authorization: Bearer <api_key>
      - 消息格式：system 写进 messages 数组（与 Qwen/Doubao/GLM 一致）
      - 模型名：使用网关约定的名称，如 anthropic/claude-sonnet-4.6
    """

    def __init__(self, config: LLMConfig):
        super().__init__(config)
        base = (self.config.base_url or "https://open.cherryin.net").rstrip("/")
        self.api_url = f"{base}/v1/chat/completions"

    async def chat_completion(
        self,
        messages: List[LLMMessage],
        model: Optional[str] = None,
        **kwargs,
    ) -> LLMResponse:
        """Claude chat completion（OpenAI 兼容格式）"""
        start_time = time.time()
        model = model or self.config.default_model

        headers = {
            "Authorization": f"Bearer {self.config.api_key}",
            "Content-Type": "application/json",
        }

        formatted_messages = [
            {"role": msg.role, "content": msg.content}
            for msg in messages
        ]

        # Claude (via AWS Bedrock) 不允许同时传 temperature 和 top_p，只取 temperature
        data = {
            "model": model,
            "messages": formatted_messages,
            "max_tokens": kwargs.get("max_tokens", self.config.max_tokens),
            "temperature": kwargs.get("temperature", self.config.temperature),
        }

        try:
            response = await self._make_request(headers, data, read_timeout=self._resolve_read_timeout(kwargs))
            response_time = time.time() - start_time

            if response.status_code == 200:
                result = response.json()
                content = result.get("choices", [{}])[0].get("message", {}).get("content", "")
                usage = result.get("usage", {})
                cost, currency = self._calculate_cost(usage, model)
                return LLMResponse(
                    content=content,
                    model=result.get("model", model),
                    provider=self.config.provider_type.value,
                    usage=usage,
                    response_time=response_time,
                    cost=cost,
                    currency=currency,
                )
            else:
                raise Exception(f"API request failed: {response.status_code} - {response.text}")

        except Exception as e:
            self.logger.error(f"Chat completion failed: {str(e)}")
            raise

    async def stream_completion(
        self,
        messages: List[LLMMessage],
        model: Optional[str] = None,
        **kwargs,
    ) -> AsyncIterator[LLMResponse]:
        """Claude streaming（暂用普通响应包装，TODO: 实现 SSE 流式）"""
        response = await self.chat_completion(messages, model, **kwargs)
        yield response

    async def test_connection(self) -> bool:
        """Test Claude connection"""
        try:
            self.status = LLMModelStatus.TESTING
            self.logger.info(f"Starting Claude connection test, API URL: {self.api_url}")
            self.logger.info(f"测试参数 - 默认模型: {self.config.default_model}")
            self.logger.info(f"测试参数 - 温度值: {self.config.temperature}")
            self.logger.info(f"测试参数 - 最大Token: {self.config.max_tokens}")
            self.logger.info(f"测试参数 - 超时时间: {self.config.timeout}秒")
            self.logger.info(f"测试参数 - 重试次数: {self.config.retry_count}")
            self.logger.info(f"测试参数 - 可用模型: {self.config.models}")

            test_messages = [LLMMessage(role="user", content="Hello")]
            response = await self.chat_completion(test_messages)

            if response.content:
                self.status = LLMModelStatus.CONNECTED
                self.logger.info(f"Claude connection test successful, response: {response.content[:50]}...")
                self.logger.info(f"实际使用模型: {response.model}")
                self.logger.info(f"响应时间: {response.response_time:.2f}秒")
                return True
            else:
                self.status = LLMModelStatus.ERROR
                self.logger.error("Claude connection test failed: empty response content")
                return False

        except Exception as e:
            self.logger.error(f"Claude connection test failed: {str(e)}")
            import traceback
            self.logger.error(f"错误堆栈: {traceback.format_exc()}")
            self.status = LLMModelStatus.ERROR
            return False

    async def _make_request(self, headers: Dict, data: Dict, read_timeout: Optional[int] = None) -> requests.Response:
        """发送异步请求。read_timeout 优先于 provider 配置（支持模板/策略动态抬升）。"""
        loop = asyncio.get_event_loop()
        connect_timeout = getattr(self.config, "connect_timeout", 15)
        effective_read_timeout = int(read_timeout) if read_timeout else self.config.timeout
        return await loop.run_in_executor(
            None,
            lambda: self.session.post(
                self.api_url,
                headers=headers,
                json=data,
                timeout=(connect_timeout, effective_read_timeout),
            ),
        )

    def _calculate_cost(self, usage: Dict, model: str) -> tuple:
        """按定价表计算本次调用费用（USD）。"""
        prompt_tokens = usage.get("prompt_tokens", 0)
        completion_tokens = usage.get("completion_tokens", 0)
        if not prompt_tokens and not completion_tokens:
            total = usage.get("total_tokens", 0)
            prompt_tokens = int(total * 0.75)
            completion_tokens = total - prompt_tokens
        return calculate_cost("claude", model, prompt_tokens, completion_tokens)


class LLMService:
    """统一的LLM服务入口"""
    
    def __init__(self):
        self.providers: Dict[str, LLMProvider] = {}
        self.default_provider: Optional[str] = None
        self.fallback_enabled: bool = True
        self.fallback_order: List[str] = []
        self.logger = get_logger("llm_service")
        
        # 配置解析器（延迟初始化）
        self.config_resolver = None
        
        # 加载配置
        self._load_configurations()
    
    def _load_configurations(self):
        """加载配置（优先从数据库，否则从环境变量）"""
        load_project_env()
        
        # 尝试从数据库加载
        try:
            if self._load_from_database():
                self.logger.info("成功从数据库加载LLM配置")
                return
        except Exception as e:
            self.logger.warning(f"从数据库加载LLM配置失败: {str(e)}")
        
        # 回退到从环境变量加载
        self.logger.info("从环境变量加载LLM配置")
        self._load_from_env()
    
    def _load_from_database(self) -> bool:
        """从数据库加载配置"""
        try:
            db_config = get_database_config()
            db_manager = DatabaseManager(db_config)
            
            # 获取提供商配置
            db_providers = db_manager.get_llm_provider_configs(enabled_only=True)
            
            if not db_providers:
                return False
            
            # 清空现有配置
            self.providers = {}

            # 先加载全局配置（供构建各 provider 时使用：超时默认、重试次数）
            db_settings = db_manager.get_llm_global_settings()

            def get_setting_value(key, default_value):
                """安全获取设置值，处理JSON字符串"""
                setting = db_settings.get(key, {})
                if not setting:
                    return default_value
                value = setting.get('value')
                if value is None:
                    return default_value
                # 如果是字符串，尝试解析JSON
                if isinstance(value, str):
                    try:
                        return json.loads(value)
                    except (json.JSONDecodeError, ValueError):
                        return value
                return value

            global_timeout = int(get_setting_value('global_timeout', 180))
            global_retry_count = int(get_setting_value('global_retry_count', 2))

            # 加载每个提供商配置
            for row in db_providers:
                provider_id = row[1]
                provider_name = row[2]
                api_key = row[4]
                base_url = row[5]
                enabled = row[6]
                default_model = row[7]
                available_models = row[8] if row[8] else []
                max_tokens = row[9] if row[9] else 2000
                temperature = float(row[10]) if row[10] else 0.7
                top_p = float(row[11]) if row[11] else 0.8
                timeout = row[12] if row[12] else global_timeout  # provider 未配置时回退到 global_timeout
                cap = get_file_processing_timeout_seconds()
                timeout = min(int(timeout), cap)
                
                if not enabled or not api_key:
                    continue
                
                # 确定提供商类型
                provider_type = None
                provider_class = None
                
                if provider_id == 'qwen':
                    provider_type = LLMProviderType.QWEN
                    provider_class = QwenProvider
                elif provider_id == 'doubao':
                    provider_type = LLMProviderType.DOUBAO
                    provider_class = DoubaoProvider
                elif provider_id == 'glm':
                    provider_type = LLMProviderType.GLM
                    provider_class = GLMProvider
                elif provider_id == 'claude':
                    provider_type = LLMProviderType.CLAUDE
                    provider_class = ClaudeProvider
                else:
                    self.logger.warning(f"未知的提供商类型: {provider_id}")
                    continue
                
                # 创建配置对象
                config = LLMConfig(
                    provider_type=provider_type,
                    api_key=api_key,
                    base_url=base_url,
                    default_model=default_model,
                    models=available_models,
                    max_tokens=max_tokens,
                    temperature=temperature,
                    top_p=top_p,
                    timeout=timeout,
                    retry_count=global_retry_count,
                    enabled=enabled
                )
                
                # 创建提供商实例
                self.providers[provider_id] = provider_class(config)
                self.logger.info(f"已加载提供商: {provider_id} ({provider_name})")
            
            # 应用全局策略配置（settings 已在循环前加载）
            self.default_provider = get_setting_value('default_provider', 'qwen')
            self.fallback_enabled = get_setting_value('fallback_enabled', True)
            self.fallback_order = get_setting_value('fallback_order', ['qwen', 'doubao', 'glm', 'claude'])
            
            return True
            
        except Exception as e:
            self.logger.error(f"从数据库加载配置失败: {str(e)}")
            return False
    
    def _load_from_env(self):
        """从环境变量加载配置（回退方案）"""
        # 加载各个提供商配置
        self._load_qwen_config()
        self._load_doubao_config()
        self._load_glm_config()
        self._load_claude_config()

        # 设置默认提供商和故障转移
        self.default_provider = os.getenv('DEFAULT_LLM_PROVIDER', 'qwen')
        self.fallback_enabled = os.getenv('LLM_FALLBACK_ENABLED', 'true').lower() == 'true'
        fallback_order_str = os.getenv('LLM_FALLBACK_ORDER', 'qwen,doubao,glm,claude')
        self.fallback_order = [p.strip() for p in fallback_order_str.split(',')]
    
    def reload_from_database(self):
        """重新从数据库加载配置"""
        self.logger.info("重新加载LLM配置...")
        if self._load_from_database():
            self.logger.info("配置重新加载成功")
            # 清除配置解析器缓存
            if self.config_resolver:
                self.config_resolver.clear_cache()
        else:
            self.logger.warning("配置重新加载失败，保持现有配置")
    
    def _init_config_resolver(self):
        """初始化配置解析器"""
        if self.config_resolver is None:
            try:
                from backend.core.config_resolver import ConfigResolver
                db_config = get_database_config()
                db_manager = DatabaseManager(db_config)
                self.config_resolver = ConfigResolver(db_manager)
                self.logger.info("配置解析器初始化完成")
            except Exception as e:
                self.logger.error(f"配置解析器初始化失败: {str(e)}")
                # 不抛出异常，允许继续使用旧逻辑
    
    def _load_qwen_config(self):
        """Load Qwen configuration"""
        api_key = os.getenv('QWEN_API_KEY')
        if api_key and os.getenv('QWEN_ENABLED', 'false').lower() == 'true':
            _cap = get_file_processing_timeout_seconds()
            config = LLMConfig(
                provider_type=LLMProviderType.QWEN,
                api_key=api_key,
                base_url=os.getenv('QWEN_BASE_URL', 'https://dashscope.aliyuncs.com'),
                default_model=os.getenv('QWEN_DEFAULT_MODEL', 'qwen-turbo'),
                models=['qwen-turbo', 'qwen-plus', 'qwen-max', 'qwen-max-longcontext',
                        'qwen3-max',
                        'qwen3.6-flash', 'qwen3.6-plus', 'qwen3.6-max-preview'],
                max_tokens=int(os.getenv('QWEN_MAX_TOKENS', '2000')),
                temperature=float(os.getenv('QWEN_TEMPERATURE', '0.7')),
                top_p=float(os.getenv('QWEN_TOP_P', '0.8')),
                timeout=min(90, _cap),
            )
            self.providers['qwen'] = QwenProvider(config)
            self.logger.info(f"已加载提供商: qwen (Qwen)")
    
    def _load_doubao_config(self):
        """Load Doubao configuration"""
        api_key = os.getenv('DOUBAO_API_KEY')
        if api_key and os.getenv('DOUBAO_ENABLED', 'false').lower() == 'true':
            _cap = get_file_processing_timeout_seconds()
            config = LLMConfig(
                provider_type=LLMProviderType.DOUBAO,
                api_key=api_key,
                base_url=os.getenv('DOUBAO_BASE_URL', 'https://ark.cn-beijing.volces.com'),
                default_model=os.getenv('DOUBAO_DEFAULT_MODEL', 'doubao-seed-1-6-250615'),
                models=['doubao-seed-1-6-250615'],
                max_tokens=int(os.getenv('DOUBAO_MAX_TOKENS', '4000')),
                temperature=float(os.getenv('DOUBAO_TEMPERATURE', '0.7')),
                top_p=float(os.getenv('DOUBAO_TOP_P', '0.9')),
                timeout=min(90, _cap),
            )
            self.providers['doubao'] = DoubaoProvider(config)
    
    
    def _load_glm_config(self):
        """Load GLM configuration"""
        api_key = os.getenv('GLM_API_KEY')
        if api_key and os.getenv('GLM_ENABLED', 'false').lower() == 'true':
            _cap = get_file_processing_timeout_seconds()
            config = LLMConfig(
                provider_type=LLMProviderType.GLM,
                api_key=api_key,
                base_url=os.getenv('GLM_BASE_URL', 'https://open.bigmodel.cn'),
                default_model=os.getenv('GLM_DEFAULT_MODEL', 'glm-4.6'),
                models=['glm-4.6', 'glm-4', 'glm-4-flash'],
                max_tokens=int(os.getenv('GLM_MAX_TOKENS', '8000')),
                temperature=float(os.getenv('GLM_TEMPERATURE', '0.7')),
                top_p=float(os.getenv('GLM_TOP_P', '0.8')),
                timeout=min(90, _cap),
            )
            self.providers['glm'] = GLMProvider(config)
            self.logger.info(f"已加载提供商: glm (GLM)")

    def _load_claude_config(self):
        """Load Anthropic Claude configuration（via OpenAI-compatible gateway, e.g. CherryIN）"""
        api_key = os.getenv('CLAUDE_API_KEY')
        if api_key and os.getenv('CLAUDE_ENABLED', 'false').lower() == 'true':
            _cap = get_file_processing_timeout_seconds()
            config = LLMConfig(
                provider_type=LLMProviderType.CLAUDE,
                api_key=api_key,
                base_url=os.getenv('CLAUDE_BASE_URL', 'https://open.cherryin.net'),
                default_model=os.getenv('CLAUDE_DEFAULT_MODEL', 'anthropic/claude-sonnet-4.6'),
                models=[
                    'anthropic/claude-sonnet-4.6',
                    'anthropic/claude-opus-4.7',
                    'anthropic/claude-opus-4-20250514',
                    'anthropic/claude-sonnet-4-20250514',
                    'anthropic/claude-3-5-sonnet-20241022',
                    'anthropic/claude-3-5-haiku-20241022',
                ],
                max_tokens=int(os.getenv('CLAUDE_MAX_TOKENS', '4096')),
                temperature=float(os.getenv('CLAUDE_TEMPERATURE', '0.7')),
                top_p=float(os.getenv('CLAUDE_TOP_P', '0.9')),
                timeout=min(120, _cap),
            )
            self.providers['claude'] = ClaudeProvider(config)
            self.logger.info("已加载提供商: claude (Anthropic Claude via CherryIN)")
    
    async def chat_completion(
        self, 
        messages: List[LLMMessage], 
        provider: Optional[str] = None,
        model: Optional[str] = None,
        **kwargs
    ) -> LLMResponse:
        """聊天补全接口"""
        provider_name = provider or self.default_provider
        
        # 尝试使用指定的提供商
        if provider_name in self.providers:
            try:
                return await self.providers[provider_name].chat_completion(messages, model, **kwargs)
            except Exception as e:
                category = classify_llm_error(e)
                self.logger.error(f"Provider {provider_name} failed [{category}]: {str(e)}")

                # 内容审查类错误默认不兜底（换家通常仍被拒），快速失败省时省钱
                if not self.fallback_enabled or not should_fallback(category):
                    if category == ERROR_CONTENT_FILTERED:
                        self.logger.warning(
                            f"内容审查拦截，跳过故障转移（FALLBACK_ON_CONTENT_FILTER={FALLBACK_ON_CONTENT_FILTER}）"
                        )
                    raise
                return await self._try_fallback_providers(messages, provider_name, model, **kwargs)
        else:
            raise ValueError(f"Provider {provider_name} not found or not enabled")
    
    async def _try_fallback_providers(
        self, 
        messages: List[LLMMessage], 
        failed_provider: str,
        model: Optional[str] = None,
        **kwargs
    ) -> LLMResponse:
        """尝试故障转移提供商"""
        last_error: Optional[Exception] = None
        for provider_name in self.fallback_order:
            if provider_name != failed_provider and provider_name in self.providers:
                try:
                    self.logger.info(f"Trying fallback provider: {provider_name}")
                    # Fallback时不传递原始model，让provider使用自己的default_model
                    # 因为不同provider支持的模型不同
                    return await self.providers[provider_name].chat_completion(messages, model=None, **kwargs)
                except Exception as e:
                    last_error = e
                    category = classify_llm_error(e)
                    self.logger.error(f"Fallback provider {provider_name} also failed [{category}]: {str(e)}")
                    # 内容审查类错误对同生态后续 provider 同样无效，提前终止整链兜底
                    if not should_fallback(category):
                        self.logger.warning("故障转移链遇内容审查拦截，提前终止")
                        break
                    continue

        raise Exception(f"All providers failed; last error: {last_error}")
    
    async def test_provider(self, provider_name: str) -> bool:
        """测试指定提供商"""
        if provider_name in self.providers:
            return await self.providers[provider_name].test_connection()
        else:
            return False
    
    def get_provider_status(self, provider_name: str) -> Dict[str, Any]:
        """获取提供商状态"""
        if provider_name in self.providers:
            return self.providers[provider_name].get_provider_info()
        else:
            return {"error": "Provider not found"}
    
    def get_all_providers_status(self) -> Dict[str, Dict[str, Any]]:
        """获取所有提供商状态"""
        return {
            name: provider.get_provider_info() 
            for name, provider in self.providers.items()
        }
    
    async def chat_completion_with_template(
        self, 
        template_id: Optional[int] = None,
        template_code: Optional[str] = None,
        variables: Dict[str, Any] = None,
        user_id: Optional[int] = None,
        provider: Optional[str] = None,
        model: Optional[str] = None,
        **kwargs
    ) -> LLMResponse:
        """
        使用模板进行对话补全
        
        Args:
            template_id: 模板ID
            template_code: 模板代码（template_id和template_code至少提供一个）
            variables: 模板变量值
            user_id: 用户ID（用于记录使用日志）
            provider: LLM提供商（可选，覆盖模板推荐）
            model: 模型（可选，覆盖模板推荐）
            **kwargs: 其他LLM参数
        
        Returns:
            LLM响应对象，包含parsed_data字段
        
        流程：
        1. 从PromptRenderer获取渲染后的prompt
        2. 使用模板推荐的配置（可被参数覆盖）
        3. 调用chat_completion
        4. 记录使用日志
        5. 根据模板的output_format解析响应
        """
        if variables is None:
            variables = {}
        
        # 导入必要的模块
        from backend.core.prompt_renderer import PromptRenderer
        from backend.services.llm_prompt_template_service import LLMPromptTemplateService
        
        # 1. 渲染prompt
        renderer = PromptRenderer()
        try:
            rendered = renderer.render(
                template_id=template_id,
                template_code=template_code,
                variables_data=variables,
            )
        except Exception as e:
            self.logger.error(f"Failed to render template: {str(e)}")
            raise Exception(f"模板渲染失败: {str(e)}")
        
        # 2. 构建消息
        messages = []
        if rendered.system_prompt:
            messages.append(LLMMessage(role="system", content=rendered.system_prompt))
        messages.append(LLMMessage(role="user", content=rendered.user_prompt))
        
        # 3. 使用配置解析器获取最终配置
        self._init_config_resolver()
        
        # 准备运行时参数（显式传入的优先级最高）
        runtime_params = {}
        if provider:
            runtime_params['provider'] = provider
        if model:
            runtime_params['model'] = model
        if 'temperature' in kwargs:
            runtime_params['temperature'] = kwargs.pop('temperature')
        if 'max_tokens' in kwargs:
            runtime_params['max_tokens'] = kwargs.pop('max_tokens')
        if 'timeout' in kwargs:
            runtime_params['timeout'] = kwargs.pop('timeout')
        
        # 使用ConfigResolver解析配置（优先级：Runtime > Template > Provider）
        if self.config_resolver:
            resolved_config = self.config_resolver.resolve_config(
                template_code=template_code or rendered.template_code,
                provider_id=provider,
                runtime_params=runtime_params
            )
            provider = resolved_config['provider']
            model = resolved_config['model']
            temperature = resolved_config['temperature']
            max_tokens = resolved_config['max_tokens']
            timeout = resolved_config.get('timeout')
        else:
            # 回退到旧逻辑（如果ConfigResolver初始化失败）
            provider = provider or rendered.recommended_config.get('provider')
            model = model or rendered.recommended_config.get('model')
            temperature = runtime_params.get('temperature', rendered.recommended_config.get('temperature'))
            max_tokens = runtime_params.get('max_tokens', rendered.recommended_config.get('max_tokens'))
            timeout = runtime_params.get('timeout')
        
        # 4. 调用LLM
        import time
        start_time = time.time()

        # PR-F: per-user × per-provider 限速 (Redis 固定 1 分钟窗口).
        # 仅当带 user_id 时生效; 系统调用 (聚合 / 后台批处理) 免限速.
        # Redis 不可达时 fail-open, 不阻塞业务.
        if user_id:
            from backend.core.llm_rate_limiter import (
                LLMRateLimiter,
                LLMRateLimitExceeded,
            )
            try:
                LLMRateLimiter.instance().check_and_consume(
                    user_id=user_id,
                    provider=provider or "default",
                )
            except LLMRateLimitExceeded:
                # 失败提前记录 usage_log, 然后向上抛供 API 转 429
                response_time_ms = int((time.time() - start_time) * 1000)
                LLMPromptTemplateService().log_template_usage(
                    template_id=rendered.template_id,
                    user_id=user_id,
                    rendered_prompt=f"System: {rendered.system_prompt}\n\nUser: {rendered.user_prompt}",
                    variables_data=rendered.variables_used,
                    response_content="",
                    provider=provider or "unknown",
                    model=model or "unknown",
                    tokens_used=0,
                    cost=0.0,
                    response_time_ms=response_time_ms,
                    success=False,
                    error_message="rate_limit_exceeded",
                )
                raise

        try:
            if timeout is not None:
                kwargs['timeout'] = timeout
            response = await self.chat_completion(
                messages, 
                provider=provider,
                model=model,
                temperature=temperature,
                max_tokens=max_tokens,
                **kwargs
            )
        except Exception as e:
            # 记录失败日志
            response_time_ms = int((time.time() - start_time) * 1000)
            service = LLMPromptTemplateService()
            service.log_template_usage(
                template_id=rendered.template_id,
                user_id=user_id,
                rendered_prompt=f"System: {rendered.system_prompt}\n\nUser: {rendered.user_prompt}",
                variables_data=rendered.variables_used,
                response_content="",
                provider=provider or "unknown",
                model=model or "unknown",
                tokens_used=0,
                cost=0.0,
                response_time_ms=response_time_ms,
                success=False,
                error_message=str(e)
            )
            raise
        
        response_time_ms = int((time.time() - start_time) * 1000)
        
        # 5. 解析响应（如果模板定义了output_format）
        parsed_output = renderer.parse_output(rendered.template_id, response.content)
        response.parsed_data = parsed_output
        
        # 6. 记录使用日志
        service = LLMPromptTemplateService()
        service.log_template_usage(
            template_id=rendered.template_id,
            user_id=user_id,
            rendered_prompt=f"System: {rendered.system_prompt}\n\nUser: {rendered.user_prompt}",
            variables_data=rendered.variables_used,
            response_content=response.content,
            provider=response.provider,
            model=response.model,
            tokens_used=response.usage.get('total_tokens', 0),
            cost=response.cost,
            response_time_ms=response_time_ms,
            success=True,
            prompt_tokens=response.usage.get('prompt_tokens'),
            completion_tokens=response.usage.get('completion_tokens'),
        )
        
        self.logger.info(
            f"Template completion successful",
            template_id=rendered.template_id,
            template_code=rendered.template_code,
            provider=response.provider,
            model=response.model,
            tokens=response.usage.get('total_tokens', 0)
        )
        
        return response


# 全局LLM服务实例
llm_service = LLMService()
