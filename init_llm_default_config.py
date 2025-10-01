#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
初始化LLM配置的默认数据到数据库
运行此脚本将在数据库中创建4个LLM提供商的初始配置记录
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from backend.database.db_manager import DatabaseManager
from backend.utils.database_config import get_database_config
import json


def init_default_llm_config():
    """初始化默认的LLM配置到数据库"""
    
    # 获取数据库连接
    db_config = get_database_config()
    db_manager = DatabaseManager(db_config)
    
    # 默认的提供商配置
    default_providers = [
        {
            'provider_id': 'qwen',
            'provider_name': 'qwen',
            'display_name': '通义千问',
            'api_key': '',  # 用户需要填写
            'base_url': 'https://dashscope.aliyuncs.com',
            'enabled': False,
            'default_model': 'qwen-turbo',
            'available_models': json.dumps(['qwen-turbo', 'qwen-plus', 'qwen-max', 'qwen-max-longcontext']),
            'max_tokens': 2000,
            'temperature': 0.7,
            'top_p': 0.8,
            'timeout': 30,
            'description': '阿里云通义千问大语言模型，支持多种规格',
            'icon': '🔷',
            'is_default_provider': True,
            'fallback_priority': 1
        },
        {
            'provider_id': 'doubao',
            'provider_name': 'doubao',
            'display_name': '豆包',
            'api_key': '',
            'base_url': 'https://ark.cn-beijing.volces.com',
            'enabled': False,
            'default_model': 'doubao-seed-1-6-250615',
            'available_models': json.dumps(['doubao-seed-1-6-250615']),
            'max_tokens': 4000,
            'temperature': 0.7,
            'top_p': 0.9,
            'timeout': 30,
            'description': '字节跳动豆包大模型，强大的中文理解能力',
            'icon': '🫘',
            'is_default_provider': False,
            'fallback_priority': 2
        },
        {
            'provider_id': 'glm',
            'provider_name': 'glm',
            'display_name': '智谱GLM',
            'api_key': '',
            'base_url': 'https://open.bigmodel.cn',
            'enabled': False,
            'default_model': 'glm-4.6',
            'available_models': json.dumps(['glm-4.6', 'glm-4', 'glm-4-flash']),
            'max_tokens': 2000,
            'temperature': 0.7,
            'top_p': 0.8,
            'timeout': 30,
            'description': '清华智谱AI大语言模型',
            'icon': '🧠',
            'is_default_provider': False,
            'fallback_priority': 4
        }
    ]
    
    # 默认的全局配置
    global_settings = [
        ('default_provider', 'qwen', '默认LLM提供商'),
        ('fallback_enabled', True, '是否启用故障转移'),
        ('fallback_order', ['qwen', 'doubao', 'wenxin', 'glm'], '故障转移顺序'),
        ('global_timeout', 30, '全局超时时间(秒)'),
        ('global_retry_count', 3, '全局重试次数'),
        ('enable_logging', True, '是否启用详细日志'),
        ('enable_monitoring', True, '是否启用性能监控')
    ]
    
    print("=" * 60)
    print("开始初始化LLM配置到数据库...")
    print("=" * 60)
    
    # 保存提供商配置
    print("\n📋 初始化提供商配置...")
    for provider in default_providers:
        try:
            success = db_manager.save_llm_provider_config(provider, 'system')
            if success:
                print(f"✅ {provider['display_name']} ({provider['provider_id']}) - 初始化成功")
            else:
                print(f"❌ {provider['display_name']} ({provider['provider_id']}) - 初始化失败")
        except Exception as e:
            print(f"❌ {provider['display_name']} ({provider['provider_id']}) - 错误: {str(e)}")
    
    # 保存全局配置
    print("\n⚙️  初始化全局配置...")
    for key, value, desc in global_settings:
        try:
            # 将值转换为JSON字符串
            json_value = json.dumps(value)
            success = db_manager.update_llm_global_setting(key, json_value, 'system')
            if success:
                print(f"✅ {key}: {value}")
            else:
                print(f"❌ {key} - 初始化失败")
        except Exception as e:
            print(f"❌ {key} - 错误: {str(e)}")
    
    print("\n" + "=" * 60)
    print("✅ LLM配置初始化完成！")
    print("=" * 60)
    print("\n💡 下一步:")
    print("1. 访问 LLM Configuration 页面")
    print("2. 为每个提供商输入API密钥")
    print("3. 点击'测试连接'验证API密钥")
    print("4. 启用需要使用的提供商")
    print("5. 点击'保存配置'按钮保存到数据库")
    print("\n⚠️  注意: 测试连接成功后，必须点击'保存配置'按钮才会保存到数据库！")
    print()


if __name__ == "__main__":
    init_default_llm_config()

