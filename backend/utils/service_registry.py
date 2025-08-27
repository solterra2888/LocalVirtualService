#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
服务注册中心

避免重复初始化服务，提高性能和资源利用率
"""

from typing import Dict, Any, Type, Optional
from threading import Lock

class ServiceRegistry:
    """服务注册中心 - 单例模式"""
    
    _instance = None
    _lock = Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(ServiceRegistry, cls).__new__(cls)
                    cls._instance._services = {}
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if not hasattr(self, '_initialized') or not self._initialized:
            self._services: Dict[str, Any] = {}
            self._initialized = True
    
    def register_service(self, service_name: str, service_instance: Any) -> None:
        """注册服务实例"""
        self._services[service_name] = service_instance
    
    def get_service(self, service_name: str) -> Optional[Any]:
        """获取服务实例"""
        return self._services.get(service_name)
    
    def get_or_create_service(
        self, 
        service_name: str, 
        service_class: Type, 
        *args, 
        **kwargs
    ) -> Any:
        """获取或创建服务实例"""
        service = self.get_service(service_name)
        if service is None:
            service = service_class(*args, **kwargs)
            self.register_service(service_name, service)
        return service
    
    def clear_service(self, service_name: str) -> None:
        """清除特定服务"""
        if service_name in self._services:
            del self._services[service_name]
    
    def clear_all_services(self) -> None:
        """清除所有服务"""
        self._services.clear()
    
    def list_services(self) -> list:
        """列出所有注册的服务"""
        return list(self._services.keys())

# 全局服务注册中心实例
service_registry = ServiceRegistry()

def get_service_registry() -> ServiceRegistry:
    """获取全局服务注册中心"""
    return service_registry
