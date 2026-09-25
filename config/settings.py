# -*- coding: utf-8 -*-
"""
配置管理模块
"""

import os
import json
from dotenv import load_dotenv


# 不再需要用户配置目录相关函数，所有配置都从项目 .env 文件获取


class AppConfig:
    """应用程序配置管理类"""
    
    def __init__(self):
        # 只加载项目根目录的 .env 文件
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        project_env_path = os.path.join(project_root, '.env')
        
        if os.path.exists(project_env_path):
            print(f"Loading environment variables from: {project_env_path}")
            load_dotenv(dotenv_path=project_env_path, override=True)
        else:
            print(f"Project .env file not found at: {project_env_path}")
            print("Warning: No .env file found. Please create one with required configurations.")

    # ── 集中管理的项目标识和常量 ──
    
    @property
    def APP_NAME(self):
        return os.getenv("APP_NAME", "Broadview Hub")
    
    @property
    def APP_VERSION(self):
        return os.getenv("APP_VERSION", "2.0.0")

    @property
    def ENABLE_API_DOCS(self):
        """是否挂载 Swagger (/api/docs) 与 ReDoc。生产环境默认关闭以降低公网攻击面。"""
        raw = self.get("ENABLE_API_DOCS")
        if raw is not None and str(raw).strip() != "":
            return str(raw).strip().lower() in ("1", "true", "yes")
        env = (self.get("ENV", "development") or "development").strip().lower()
        return env != "production"
    
    @property
    def USER_AGENT(self):
        return f"{self.APP_NAME}/{self.APP_VERSION}"
    
    @property
    def REDIS_URL(self):
        return os.getenv("REDIS_URL", "redis://localhost:6379/0")
    
    @property
    def RC_AGENT_BASE_URL(self):
        return os.getenv("RC_AGENT_BASE_URL", "http://localhost:81/v1/chat-messages")
    
    @property
    def WEBAPP_PORT(self):
        return int(os.getenv("WEBAPP_PORT", "8000"))
    
    @property
    def SCHEDULER_PORT(self):
        return int(os.getenv("SCHEDULER_PORT", "8001"))
    
    @property
    def FRONTEND_PORT(self):
        return int(os.getenv("FRONTEND_PORT", "3000"))
    
    @property
    def SERVER_HOST(self):
        return os.getenv("SERVER_HOST", "localhost")
    
    
    def get(self, key, default=None):
        """获取配置值"""
        return os.getenv(key, default)
    
    def set(self, key, value):
        """设置配置值（仅更新环境变量，不持久化）"""
        # 只更新当前进程的环境变量，不写入文件
        # 所有持久化配置应该在 .env 文件中手动管理
        os.environ[key] = str(value)
    
    def get_last_login_username(self):
        """获取上次登录的用户名"""
        return self.get('LAST_LOGIN_USERNAME')
    
    def save_last_login_username(self, username):
        """保存上次登录的用户名（仅更新环境变量）"""
        # 警告：此配置不会持久化，重启后丢失
        self.set('LAST_LOGIN_USERNAME', username)
    
    def get_llama_api_key(self):
        """获取LlamaIndex API密钥"""
        return self.get('LLAMA_CLOUD_API_KEY', '')
    
    def save_llama_api_key(self, api_key):
        """保存LlamaIndex API密钥（仅更新环境变量）"""
        # 警告：此配置不会持久化，请在 .env 文件中配置 LLAMA_CLOUD_API_KEY
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
        """获取所有RC Agent令牌配置"""
        tokens = {}
        
        # 获取默认令牌
        default_token = self.get('RC_AGENT_TOKEN_default') or self.get('RC_AGENT_TOKEN')
        if default_token:
            tokens['default'] = {
                'name': '默认任务',
                'token': default_token,
                'input_fields': self._parse_rc_agent_fields('default')
            }
        
        # 获取其他令牌（从环境变量中解析）
        for key, value in os.environ.items():
            if key.startswith('RC_AGENT_TOKEN_') and key != 'RC_AGENT_TOKEN_default':
                task_id = key[15:]  # 去掉 'RC_AGENT_TOKEN_' 前缀
                name_key = f'RC_AGENT_NAME_{task_id}'
                task_name = os.environ.get(name_key, f"任务 {task_id}")
                
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
        """保存RC Agent令牌配置（仅更新环境变量）"""
        # 警告：这个方法只更新当前进程的环境变量
        # 持久化配置需要手动在 .env 文件中配置
        print("Warning: save_rc_agent_tokens only updates environment variables.")
        print("For persistent configuration, please update the .env file manually.")
        
        # 删除所有旧的RC Agent相关环境变量
        keys_to_remove = []
        for key in os.environ:
            if (key.startswith('RC_AGENT_TOKEN_') or 
                key.startswith('RC_AGENT_NAME_') or 
                key.startswith('RC_AGENT_FIELDS_') or 
                key == 'RC_AGENT_TOKEN'):
                keys_to_remove.append(key)
        
        for key in keys_to_remove:
            if key in os.environ:
                del os.environ[key]
        
        # 保存新的配置到环境变量
        for task_id, task_info in tokens_dict.items():
            if task_id == "default":
                if task_info.get('token'):
                    os.environ['RC_AGENT_TOKEN_default'] = task_info['token']
                    os.environ['RC_AGENT_NAME_default'] = "默认任务"
                    os.environ['RC_AGENT_TOKEN'] = task_info['token']  # 向后兼容
                    
                    if task_info.get('input_fields'):
                        os.environ['RC_AGENT_FIELDS_default'] = json.dumps(task_info['input_fields'])
            else:
                if task_info.get('token'):
                    os.environ[f'RC_AGENT_TOKEN_{task_id}'] = task_info['token']
                    os.environ[f'RC_AGENT_NAME_{task_id}'] = task_info['name']
                    
                    if task_info.get('input_fields'):
                        os.environ[f'RC_AGENT_FIELDS_{task_id}'] = json.dumps(task_info['input_fields'])
    
    def get_youtube_download_config(self):
        """获取 YouTube 下载（yt-dlp）相关配置"""
        return {
            'proxy': (os.getenv('YOUTUBE_PROXY') or
                      os.getenv('HTTPS_PROXY') or
                      os.getenv('HTTP_PROXY')),
            'cookies_file': os.getenv('YOUTUBE_COOKIES_FILE'),
            'po_token': os.getenv('YOUTUBE_PO_TOKEN'),
            'visitor_data': os.getenv('YOUTUBE_VISITOR_DATA'),
        }

    def get_logging_config(self):
        """获取日志配置"""
        return {
            'level': self.get('LOG_LEVEL', 'INFO'),
            'log_to_file': self.get('LOG_TO_FILE', 'true').lower() == 'true',
            'log_to_console': self.get('LOG_TO_CONSOLE', 'true').lower() == 'true',
            'log_file_days': int(self.get('LOG_FILE_DAYS', '30')),
            'log_dir': self.get('LOG_DIR', 'logs'),
            'log_async': self.get('LOG_ASYNC', 'true').lower() in ('1', 'true', 'yes'),
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
