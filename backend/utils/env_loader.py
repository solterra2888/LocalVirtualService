# -*- coding: utf-8 -*-
"""
环境变量加载工具模块
统一的 .env 文件加载逻辑
"""

import os
from dotenv import load_dotenv


def load_project_env():
    """
    加载项目根目录的 .env 文件
    
    Returns:
        bool: 是否成功加载
    """
    # 获取项目根目录
    current_file = os.path.abspath(__file__)
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_file)))
    env_path = os.path.join(project_root, '.env')
    
    # 加载 .env 文件
    return load_dotenv(dotenv_path=env_path)


# 自动加载（导入时即加载）
load_project_env()
