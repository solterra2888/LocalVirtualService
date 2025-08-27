# -*- coding: utf-8 -*-
"""
配置管理模块
"""

import os
import json
import platform
from dotenv import load_dotenv


def get_app_config_path():
    """获取应用程序配置文件路径，确保配置保存在用户目录下而不是临时目录"""
    app_name = "LlamaParse"
    if platform.system() == "Windows":
        app_data_dir = os.path.join(os.environ["APPDATA"], app_name)
    elif platform.system() == "Darwin":  # macOS
        app_data_dir = os.path.join(os.path.expanduser("~/Library/Application Support"), app_name)
    else:  # Linux 和其他系统
        app_data_dir = os.path.join(os.path.expanduser("~/.config"), app_name)
    
    # 确保目录存在
    if not os.path.exists(app_data_dir):
        os.makedirs(app_data_dir)
    
    return os.path.join(app_data_dir, "config.env")


class AppConfig:
    """应用程序配置管理类"""
    
    def __init__(self):
        self.config_path = self._get_app_config_path()
        self._ensure_config_exists()
        
        # 首先尝试加载项目根目录的 .env 文件
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        project_env_path = os.path.join(project_root, '.env')
        
        if os.path.exists(project_env_path):
            print(f"Loading environment variables from: {project_env_path}")
            load_dotenv(dotenv_path=project_env_path, override=True)
        else:
            print(f"Project .env file not found at: {project_env_path}")
        
        # 然后加载用户配置目录的配置文件（作为补充，不覆盖）
        if os.path.exists(self.config_path):
            print(f"Loading additional config from: {self.config_path}")
            load_dotenv(dotenv_path=self.config_path, override=False)
    
    def _get_app_config_path(self):
        """获取应用程序配置文件路径，确保配置保存在用户目录下而不是临时目录"""
        # 使用公共函数避免代码重复
        return get_app_config_path()
    
    def _ensure_config_exists(self):
        """确保配置文件存在"""
        if not os.path.exists(self.config_path):
            # 创建默认配置文件
            with open(self.config_path, 'w', encoding='utf-8') as f:
                f.write("# LlamaParse 配置文件\n")
                f.write("# 自动生成，请勿手动编辑\n\n")
    
    def _read_config(self):
        """读取配置文件内容"""
        env_content = {}
        if os.path.exists(self.config_path):
            with open(self.config_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, value = line.split('=', 1)
                        env_content[key.strip()] = value.strip()
        return env_content
    
    def _write_config(self, env_content):
        """写入配置文件内容"""
        with open(self.config_path, 'w', encoding='utf-8') as f:
            f.write("# LlamaParse 配置文件\n")
            f.write("# 自动生成，请勿手动编辑\n\n")
            for key, value in env_content.items():
                f.write(f"{key}={value}\n")
    
    def get(self, key, default=None):
        """获取配置值"""
        return os.getenv(key, default)
    
    def set(self, key, value):
        """设置配置值"""
        env_content = self._read_config()
        env_content[key] = str(value)
        self._write_config(env_content)
        # 更新环境变量
        os.environ[key] = str(value)
    
    def get_last_login_username(self):
        """获取上次登录的用户名"""
        return self.get('LAST_LOGIN_USERNAME')
    
    def save_last_login_username(self, username):
        """保存上次登录的用户名"""
        self.set('LAST_LOGIN_USERNAME', username)
    
    def get_llama_api_key(self):
        """获取LlamaIndex API密钥"""
        return self.get('LLAMA_CLOUD_API_KEY', '')
    
    def save_llama_api_key(self, api_key):
        """保存LlamaIndex API密钥"""
        if api_key:
            self.set('LLAMA_CLOUD_API_KEY', api_key)
    
    def get_db_config(self):
        """获取数据库配置"""
        return {
            'host': self.get('DB_HOST', ''),
            'port': self.get('DB_PORT', '5432'),
            'name': self.get('DB_NAME', ''),
            'user': self.get('DB_USER', ''),
            'password': self.get('DB_PASSWORD', ''),
        }
    
    def save_db_config(self, host, port, name, user, password):
        """保存数据库配置"""
        self.set('DB_HOST', host)
        self.set('DB_PORT', port)
        self.set('DB_NAME', name)
        self.set('DB_USER', user)
        self.set('DB_PASSWORD', password)
    
    def get_default_admin_config(self):
        """获取默认管理员配置"""
        return {
            'user': self.get('DEFAULT_ADMIN_USER', 'admin'),
            'password': self.get('DEFAULT_ADMIN_PASSWORD', 'admin')
        }
    
    def has_valid_db_config(self):
        """检查是否有有效的数据库配置"""
        db_config = self.get_db_config()
        return all([db_config['host'], db_config['name'], 
                   db_config['user'], db_config['password']])
    
    def get_rc_agent_tokens(self):
        """获取所有RC Agent令牌配置（向后兼容）"""
        env_content = self._read_config()
        tokens = {}
        
        # 获取默认令牌
        default_token = self.get('RC_AGENT_TOKEN_default') or self.get('RC_AGENT_TOKEN')
        if default_token:
            tokens['default'] = {
                'name': '默认任务',
                'token': default_token,
                'input_fields': self._parse_rc_agent_fields('default')
            }
        
        # 获取其他令牌
        for key, value in env_content.items():
            if key.startswith('RC_AGENT_TOKEN_') and key != 'RC_AGENT_TOKEN_default':
                task_id = key[15:]  # 去掉 'RC_AGENT_TOKEN_' 前缀
                name_key = f'RC_AGENT_NAME_{task_id}'
                task_name = env_content.get(name_key, f"任务 {task_id}")
                
                tokens[task_id] = {
                    'name': task_name,
                    'token': value,
                    'input_fields': self._parse_rc_agent_fields(task_id)
                }
        
        return tokens
    
    def _parse_rc_agent_fields(self, task_id):
        """解析RC Agent输入字段"""
        fields_key = f'RC_AGENT_FIELDS_{task_id}'
        fields_str = self.get(fields_key, '')
        if fields_str:
            try:
                return json.loads(fields_str)
            except json.JSONDecodeError:
                pass
        return []
    
    def save_rc_agent_tokens(self, tokens_dict):
        """保存RC Agent令牌配置（向后兼容）"""
        env_content = self._read_config()
        
        # 删除所有旧的RC Agent相关配置
        keys_to_remove = []
        for key in env_content:
            if (key.startswith('RC_AGENT_TOKEN_') or 
                key.startswith('RC_AGENT_NAME_') or 
                key.startswith('RC_AGENT_FIELDS_') or 
                key == 'RC_AGENT_TOKEN'):
                keys_to_remove.append(key)
        
        for key in keys_to_remove:
            del env_content[key]
        
        # 保存新的配置
        for task_id, task_info in tokens_dict.items():
            if task_id == "default":
                if task_info.get('token'):
                    env_content['RC_AGENT_TOKEN_default'] = task_info['token']
                    env_content['RC_AGENT_NAME_default'] = "默认任务"
                    env_content['RC_AGENT_TOKEN'] = task_info['token']  # 向后兼容
                    
                    if task_info.get('input_fields'):
                        env_content['RC_AGENT_FIELDS_default'] = json.dumps(task_info['input_fields'])
            else:
                if task_info.get('token'):
                    env_content[f'RC_AGENT_TOKEN_{task_id}'] = task_info['token']
                    env_content[f'RC_AGENT_NAME_{task_id}'] = task_info['name']
                    
                    if task_info.get('input_fields'):
                        env_content[f'RC_AGENT_FIELDS_{task_id}'] = json.dumps(task_info['input_fields'])
        
        self._write_config(env_content)
        # 重新加载环境变量
        load_dotenv(dotenv_path=self.config_path)
    
    def get_logging_config(self):
        """获取日志配置"""
        return {
            'level': self.get('LOG_LEVEL', 'INFO'),
            'log_to_file': self.get('LOG_TO_FILE', 'true').lower() == 'true',
            'log_to_console': self.get('LOG_TO_CONSOLE', 'true').lower() == 'true',
            'log_file_days': int(self.get('LOG_FILE_DAYS', '30')),
            'log_dir': self.get('LOG_DIR', 'logs')
        }
    
    def save_logging_config(self, level='INFO', log_to_file=True, log_to_console=True, 
                           log_file_days=30, log_dir='logs'):
        """保存日志配置"""
        self.set('LOG_LEVEL', level)
        self.set('LOG_TO_FILE', 'true' if log_to_file else 'false')
        self.set('LOG_TO_CONSOLE', 'true' if log_to_console else 'false')
        self.set('LOG_FILE_DAYS', str(log_file_days))
        self.set('LOG_DIR', log_dir)


# 全局配置实例
config = AppConfig()

def get_db_conn_params():
    """获取数据库连接参数"""
    return {
        'db_host': config.get('DB_HOST', 'localhost'),
        'db_port': config.get('DB_PORT', '5432'),
        'db_name': config.get('DB_NAME', 'targettracking'),
        'db_user': config.get('DB_USER', 'postgres'),
        'db_password': config.get('DB_PASSWORD', '')
    } 