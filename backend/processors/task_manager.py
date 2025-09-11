# -*- coding: utf-8 -*-
"""
任务管理器 - 管理RC Agent AI和凭据
"""

import os
import json
import time
import random
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any
from config.settings import config
from backend.database.db_manager import DatabaseManager

# 全局缓存，避免重复加载
_task_manager_cache = {}

class TaskManager:
    """任务管理器类"""
    
    def __init__(self, username=None, is_admin=False, db_conn_params=None):
        """
        初始化任务管理器
        
        Args:
            username: 用户名
            is_admin: 是否为管理员
            db_conn_params: 数据库连接参数
        """
        self.username = username or "guest"
        self.is_admin = is_admin
        self.db_conn_params = db_conn_params or {}
        self.db_manager = DatabaseManager(db_conn_params)
        self.task_types = {}
        
        # 生成缓存键
        self.cache_key = f"{self.username}_{self.is_admin}"
        
        # 尝试从缓存加载
        if self.cache_key in _task_manager_cache:
            cached_data = _task_manager_cache[self.cache_key]
            # 检查缓存是否过期（5分钟）
            cache_age = time.time() - cached_data['timestamp']
            if cache_age < 300:
                self.task_types = cached_data['task_types'].copy()
                print(f"✅ 从缓存加载了 {len(self.task_types)} 个AGENT凭据 (缓存年龄: {cache_age:.1f}秒)")
                return
            else:
                print(f"⚠️ 缓存已过期 (年龄: {cache_age:.1f}秒)，重新加载")
        
        # 如果缓存不存在或已过期，重新加载
        self.load_agent_credentials()
    
    def load_agent_credentials(self):
        """加载AGENT凭据"""
        # 首先尝试从数据库加载
        if not self._load_from_database():
            # 如果数据库加载失败，尝试从配置文件加载
            if not self._load_from_config():
                # 如果都失败，创建默认任务
                self._ensure_default_task()
        
        # 更新缓存
        _task_manager_cache[self.cache_key] = {
            'task_types': self.task_types.copy(),
            'timestamp': time.time()
        }
        print(f"🔄 已更新缓存 (用户: {self.username}, 管理员: {self.is_admin}) - {len(self.task_types)} 个AGENT凭据")
    
    def _ensure_default_task(self):
        """确保存在默认任务"""
        if "default" not in self.task_types:
            self._create_default_task()
    
    def _create_default_task(self):
        """创建默认任务"""
        self.task_types["default"] = {
            "name": "默认任务",
            "token": "",
            "input_fields": [],
            "remarks": "系统默认任务",
            "created_by": "system",
            "created_at": datetime.now()
        }
    
    def _load_from_database(self):
        """从数据库加载AGENT凭据"""
        try:
            print("从数据库加载AGENT凭据...")
            agents = self.db_manager.get_agent_credentials(self.username, self.is_admin)
            
            if not agents:
                print("数据库中没有找到AGENT凭据")
                return False
            
            print(f"从数据库加载了 {len(agents)} 个AGENT凭据")
            
            # 清除现有RC Agent
            self.task_types = {}
            
            # 处理所有的agents
            for agent in agents:
                agent_id = str(agent[0])
                agent_name = agent[1]
                agent_token = agent[2]
                is_default = agent[3]
                
                # 处理input_fields
                input_fields = []
                input_fields_str = agent[4]
                if input_fields_str:
                    try:
                        if isinstance(input_fields_str, str):
                            input_fields = json.loads(input_fields_str)
                        else:  # PostgreSQL可能已经解析了JSON
                            input_fields = input_fields_str
                        print(f"加载了{agent_name}的 {len(input_fields)} 个输入字段")
                    except json.JSONDecodeError:
                        print(f"无法解析{agent_name}的输入字段JSON")
                
                # 获取备注、创建者和创建时间
                remarks = ""
                if len(agent) > 5:  # 如果有remarks列
                    remarks = agent[5] or ""
                
                created_by = ""
                if len(agent) > 6:  # 如果有created_by列
                    created_by = agent[6] or ""
                
                created_at = None
                if len(agent) > 7:  # 如果有created_at列
                    created_at = agent[7]
                
                # 获取agent_type
                agent_type = "chatbot"  # 默认值
                if len(agent) > 8:  # 如果有agent_type列
                    agent_type = agent[8] or "chatbot"
                
                # 保存到RC Agent字典
                task_id = "default" if is_default else agent_id
                self.task_types[task_id] = {
                    "name": agent_name,
                    "token": agent_token,
                    "input_fields": input_fields,
                    "remarks": remarks,
                    "created_by": created_by,
                    "created_at": created_at,
                    "agent_type": agent_type
                }
                
                if is_default:
                    print(f"加载了默认AGENT: {agent_name}")
            
            return True
        except Exception as e:
            print(f"从数据库加载AGENT凭据时出错: {str(e)}")
            return False
    
    def _load_from_config(self):
        """从配置文件加载AGENT凭据（向后兼容）"""
        try:
            print("从配置文件加载AGENT凭据...")
            
            # 获取RC Agent令牌配置
            tokens_dict = config.get_rc_agent_tokens()
            
            if not tokens_dict:
                print("配置文件中没有找到RC Agent令牌配置")
                return False
            
            # 更新RC Agent
            self.task_types = tokens_dict
            
            print(f"从配置文件加载了 {len(self.task_types)} 个RC Agent")
            
            # 尝试迁移到数据库
            self._migrate_to_database()
            
            return True
        except Exception as e:
            print(f"从配置文件加载AGENT凭据时出错: {str(e)}")
            return False
    
    def _migrate_to_database(self):
        """将环境变量中的AGENT信息迁移到数据库"""
        try:
            # 检查数据库中是否已有数据
            agents = self.db_manager.get_agent_credentials(self.username, self.is_admin)
            if agents:
                return  # 数据库中已有数据，跳过迁移
            
            print("开始将环境变量中的AGENT迁移到数据库...")
            
            # 保存到数据库
            if self.db_manager.save_agent_credentials(self.username, self.task_types):
                print(f"成功将 {len(self.task_types)} 个AGENT凭据从环境变量迁移到数据库")
            else:
                print("迁移AGENT凭据到数据库失败")
        except Exception as e:
            print(f"迁移环境变量AGENT信息到数据库时出错: {e}")
    
    def get_task_types(self):
        """获取所有RC Agent"""
        return self.task_types.copy()
    
    def get_task_info(self, task_id):
        """获取指定任务的信息"""
        return self.task_types.get(task_id)
    
    def add_task_type(self, task_name, task_token, input_fields=None, remarks="", agent_type="chatbot"):
        """
        添加新的RC Agent
        
        Args:
            task_name: 任务名称
            task_token: RC Agent令牌
            input_fields: 自定义输入字段列表
            remarks: 备注
            agent_type: Agent类型 ('chatbot' 或 'workflow')
            
        Returns:
            tuple: (success, message, task_id)
        """
        # 验证输入
        if not task_name:
            return False, "任务名称不能为空", None
        
        if task_name.lower() in ["默认任务", "default task"]:
            return False, "不能创建名为\"默认任务\"的新任务，此名称已被系统保留", None
        
        # 检查名称是否重复
        if task_name in [info['name'] for info in self.task_types.values()]:
            return False, "任务名称不能重复", None
        
        # 生成唯一的数字格式任务ID
        task_id = str(int(time.time() * 1000) + random.randint(1, 1000))
        
        # 验证agent_type
        if agent_type not in ["chatbot", "workflow"]:
            return False, "Agent类型必须是 'chatbot' 或 'workflow'", None
        
        # 添加任务
        self.task_types[task_id] = {
            "name": task_name,
            "token": task_token,
            "input_fields": input_fields or [],
            "remarks": remarks,
            "created_by": self.username,
            "created_at": datetime.now(),
            "agent_type": agent_type
        }
        
        # 保存到数据库
        success = self.save_to_database()
        
        if success:
            # 清除缓存，强制重新加载
            self._clear_cache()
            return True, f"已添加新RC Agent: '{task_name}'", task_id
        else:
            # 如果保存失败，从内存中移除
            del self.task_types[task_id]
            return False, "保存到数据库失败", None
    
    def update_task_type(self, task_id, task_name=None, task_token=None, input_fields=None, remarks=None, agent_type=None):
        """
        更新RC Agent
        
        Args:
            task_id: 任务ID
            task_name: 新的任务名称（可选）
            task_token: 新的RC Agent令牌（可选）
            input_fields: 新的自定义输入字段列表（可选）
            remarks: 新的备注（可选）
            agent_type: 新的Agent类型（可选）
            
        Returns:
            tuple: (success, message)
        """
        if task_id not in self.task_types:
            return False, "任务不存在"
        
        task_info = self.task_types[task_id]
        
        # 名称验证（包括默认任务）
        if task_name:
            # 检查更改后的名称是否与其他任务重复
            for other_id, other_info in self.task_types.items():
                if other_id != task_id and other_info['name'] == task_name:
                    return False, "任务名称不能重复"
        
        # 验证agent_type
        if agent_type is not None and agent_type not in ["chatbot", "workflow"]:
            return False, "Agent类型必须是 'chatbot' 或 'workflow'"
        
        # 更新任务信息
        if task_name is not None:
            task_info["name"] = task_name
        if task_token is not None:
            task_info["token"] = task_token
        if input_fields is not None:
            task_info["input_fields"] = input_fields
        if remarks is not None:
            task_info["remarks"] = remarks
        if agent_type is not None:
            task_info["agent_type"] = agent_type
        
        # 保存到数据库
        success = self.save_to_database()
        
        if success:
            # 清除缓存，强制重新加载
            self._clear_cache()
            return True, f"已更新任务 '{task_info['name']}' 的设置"
        else:
            return False, "保存到数据库失败"
    
    def delete_task_type(self, task_id):
        """
        删除RC Agent
        
        Args:
            task_id: 任务ID
            
        Returns:
            tuple: (success, message)
        """
        if task_id == "default":
            return False, "不能删除默认任务"
        
        if task_id not in self.task_types:
            return False, "任务不存在"
        
        task_name = self.task_types[task_id]["name"]
        
        # 从数据库中删除
        success = self.db_manager.delete_agent_credential(task_id, task_name)
        
        if success:
            # 从内存中删除
            del self.task_types[task_id]
            # 清除缓存，强制重新加载
            self._clear_cache()
            return True, f"已删除RC Agent: '{task_name}'"
        else:
            return False, "从数据库删除失败"
    
    def save_to_database(self):
        """保存到数据库"""
        try:
            return self.db_manager.save_agent_credentials(self.username, self.task_types)
        except Exception as e:
            print(f"保存到数据库时出错: {str(e)}")
            return False
    
    def save_to_config(self):
        """保存到配置文件（向后兼容）"""
        try:
            config.save_rc_agent_tokens(self.task_types)
            return True
        except Exception as e:
            print(f"保存到配置文件时出错: {str(e)}")
            return False
    
    def get_task_list_for_ui(self):
        """获取用于UI显示的任务列表"""
        task_list = []
        for task_id, task_info in self.task_types.items():
            task_list.append({
                "id": task_id,
                "name": task_info["name"],
                "token": task_info["token"],
                "input_fields": task_info["input_fields"],
                "remarks": task_info.get("remarks", ""),
                "creator": task_info.get("created_by", ""),
                "created_at": task_info.get("created_at"),
                "is_default": task_id == "default",
                "agent_type": task_info.get("agent_type", "chatbot")
            })
        return task_list
    
    def validate_task_selection(self, task_id, use_ai_agent=True):
        """
        验证任务选择
        
        Args:
            task_id: 任务ID
            use_ai_agent: 是否使用AI Agent
            
        Returns:
            tuple: (success, message, task_info)
        """
        if task_id not in self.task_types:
            return False, f"任务ID '{task_id}' 不存在", None
        
        task_info = self.task_types[task_id]
        
        if use_ai_agent and not task_info["token"]:
            return False, f"任务 '{task_info['name']}' 没有配置RC Agent令牌", None
        
        return True, "验证通过", task_info
    
    def collect_custom_inputs(self, input_widgets_data):
        """
        收集自定义输入
        
        Args:
            input_widgets_data: 输入组件数据
            
        Returns:
            dict: 自定义输入字典
        """
        custom_inputs = {}
        
        for field_name, field_data in input_widgets_data.items():
            if field_data.get("enabled", False):
                custom_inputs[field_name] = field_data.get("value", "")
        
        return custom_inputs
    
    def _clear_cache(self):
        """清除缓存"""
        if self.cache_key in _task_manager_cache:
            del _task_manager_cache[self.cache_key]
    
    @classmethod
    def clear_all_cache(cls):
        """清除所有缓存"""
        global _task_manager_cache
        _task_manager_cache.clear()
        print("已清除所有TaskManager缓存") 