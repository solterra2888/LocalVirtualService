# -*- coding: utf-8 -*-
"""
内容适配器基类 - 定义标准化接口
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, List
from datetime import datetime


class BaseContentAdapter(ABC):
    """
    内容适配器基类
    
    所有渠道适配器都必须实现此接口，将原始数据转换为标准化格式
    """
    
    @property
    @abstractmethod
    def channel_type(self) -> str:
        """返回渠道类型"""
        pass
    
    @abstractmethod
    def normalize(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        将原始数据标准化为统一格式
        
        Args:
            raw_data: 从外部API获取的原始数据
            
        Returns:
            标准化后的数据字典，包含以下字段:
            {
                'channel_type': str,         # 渠道类型
                'content_id': str,           # 外部唯一ID
                'title': str | None,         # 标题（可选）
                'content': str,              # 主要内容
                'author_name': str | None,   # 作者名称
                'author_identifier': str,    # 作者唯一标识
                'url': str | None,           # 内容链接
                'media_urls': List[str],     # 媒体URL列表
                'published_at': datetime,    # 发布时间
                'engagement_count': int,     # 互动数
                'view_count': int,           # 观看数
                'metadata': Dict[str, Any],  # 扩展数据
            }
        """
        pass
    
    def validate(self, normalized_data: Dict[str, Any]) -> bool:
        """
        验证标准化数据的完整性
        
        Args:
            normalized_data: 标准化后的数据
            
        Returns:
            是否通过验证
        """
        required_fields = ['channel_type', 'content_id', 'content', 'author_identifier', 'published_at']
        
        for field in required_fields:
            if field not in normalized_data or normalized_data[field] is None:
                return False
        
        return True
    
    def extract_media_urls(self, data: Dict[str, Any], keys: List[str]) -> List[str]:
        """
        从嵌套数据结构中提取媒体URL
        
        Args:
            data: 数据字典
            keys: 要查找的键路径
            
        Returns:
            媒体URL列表
        """
        urls = []
        
        def traverse(obj, path):
            if isinstance(obj, dict):
                for key, value in obj.items():
                    if key in keys and isinstance(value, str) and value.startswith('http'):
                        urls.append(value)
                    elif isinstance(value, (dict, list)):
                        traverse(value, path + [key])
            elif isinstance(obj, list):
                for item in obj:
                    traverse(item, path)
        
        traverse(data, [])
        return urls
    
    def parse_datetime(self, date_str: str, format: str = None) -> datetime:
        """
        解析日期时间字符串
        
        Args:
            date_str: 日期时间字符串
            format: 日期格式（可选）
            
        Returns:
            datetime 对象
        """
        if format:
            return datetime.strptime(date_str, format)
        
        # 尝试常见格式
        from dateutil import parser
        return parser.parse(date_str)
