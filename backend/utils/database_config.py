#!/usr/bin/env python3
"""
数据库配置工具模块
提供统一的数据库连接配置获取函数
"""

import os
from config.settings import config


def get_database_config():
    """
    获取标准化的数据库配置
    
    Returns:
        dict: 包含数据库连接参数的字典
    """
    return {
        'db_host': os.getenv('DB_HOST', 'localhost'),
        'db_port': os.getenv('DB_PORT', '5432'),
        'db_name': os.getenv('DB_NAME', 'rc_database'),  # 统一使用 rc_database
        'db_user': os.getenv('DB_USER', 'postgres'),
        'db_password': os.getenv('DB_PASSWORD', '')
    }


def get_psycopg2_config():
    """
    获取适用于 psycopg2 的数据库配置格式
    (用于某些特殊服务如 finviz_screener_service)
    
    Returns:
        dict: 包含 psycopg2 格式的数据库连接参数
    """
    return {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': os.getenv('DB_PORT', '5432'),
        'database': os.getenv('DB_NAME', 'rc_database'),
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASSWORD', '')
    }
