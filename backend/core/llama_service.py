# -*- coding: utf-8 -*-
"""
LlamaIndex 服务模块 - 完全独立实现
"""

import os
import sys
import time
import json
import asyncio
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import platform
from pathlib import Path

# 统一的环境变量加载
from backend.utils.env_loader import load_project_env
from backend.utils.logger import get_logger


class LlamaService:
    """LlamaIndex 服务管理类 - 完全独立实现"""
    
    def __init__(self, api_key=None, output_folder=None, db_path=None, polling_interval=5, log_callback=None):
        """
        初始化LlamaIndex服务
        
        Args:
            api_key (str, optional): LlamaIndex API key
            output_folder (str, optional): 输出文件夹路径
            db_path (str, optional): 数据库路径
            polling_interval (int, optional): 轮询间隔（秒）
            log_callback (callable, optional): 日志回调函数
        """
        # 配置已经在应用启动时从项目 .env 文件加载，这里直接使用环境变量
        
        # 设置API密钥
        self.api_key = api_key or os.environ.get('LLAMA_CLOUD_API_KEY', '')
        self.polling_interval = polling_interval
        
        # 创建配置了重试和超时的会话
        self.session = self._create_session()
        
        # 获取应用基础目录
        if getattr(sys, 'frozen', False):  # 打包应用
            self.base_dir = Path(os.path.dirname(sys.executable))
        else:  # 开发环境
            self.base_dir = Path(os.path.dirname(os.path.abspath(__file__)))
        
        # 设置输出文件夹
        if output_folder:
            self.output_folder = output_folder
        else:
            if platform.system() == 'Windows':
                self.output_folder = os.path.join(os.path.expanduser('~'), 'Documents', 'LlamaParse', 'output')
            elif platform.system() == 'Darwin':  # macOS
                self.output_folder = os.path.join(os.path.expanduser('~'), 'Library', 'Application Support', 'LlamaParse', 'output')
            else:  # Linux/Unix
                self.output_folder = os.path.join(os.path.expanduser('~'), '.llamaparse', 'output')
        
        os.makedirs(self.output_folder, exist_ok=True)
        
        # 设置数据库路径（已废弃，现在使用PostgreSQL）
        self.db_path = db_path  # 保留参数以兼容现有代码
        
        # 设置环境变量
        if self.api_key:
            os.environ['LLAMA_CLOUD_API_KEY'] = self.api_key
        
        # 设置日志回调（兼容现有代码）
        self.log_callback = log_callback
        # 使用统一日志系统
        self.logger = get_logger("llama_service")
    
    def _create_session(self):
        """创建配置了重试和超时的请求会话"""
        session = requests.Session()
        
        # 配置重试策略
        retry_strategy = Retry(
            total=3,  # 最多重试3次
            backoff_factor=1,  # 重试间隔（秒）
            status_forcelist=[429, 500, 502, 503, 504],  # 需要重试的HTTP状态码
            allowed_methods=["HEAD", "GET", "POST", "PUT", "DELETE", "OPTIONS", "TRACE"]
        )
        
        # 创建适配器
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        return session
    
    def log(self, message, level="info"):
        """发送日志消息"""
        # 使用统一日志系统
        self.logger.log_llama_service(message, level)
        
        # 保持向后兼容性
        if self.log_callback:
            self.log_callback(log_message=message, log_level=level)
    
    def get_supported_extensions(self):
        """获取支持的文件扩展名"""
        return ['.pdf', '.xlsx', '.xls', '.doc', '.docx', '.jpg', '.jpeg', '.png', '.gif', '.bmp']
    
    def validate_file(self, file_path):
        """验证文件是否支持处理"""
        file_extension = os.path.splitext(file_path)[1].lower()
        return file_extension in self.get_supported_extensions()
    
    def get_file_info(self, file_path):
        """获取文件信息"""
        file_name = os.path.basename(file_path)
        file_extension = os.path.splitext(file_name)[1].lower()
        file_size = 0
        
        try:
            file_size = os.path.getsize(file_path) / (1024 * 1024)  # 转换为MB
        except:
            pass
        
        # 将文件扩展名映射到标准化的文件类型
        file_type = ""
        if file_extension:
            ext = file_extension[1:]  # 移除点号
            if ext in ['pdf']:
                file_type = 'pdf'
            elif ext in ['xlsx', 'xls']:
                file_type = 'excel'
            elif ext in ['doc', 'docx']:
                file_type = 'doc'
            elif ext in ['jpg', 'jpeg', 'png', 'gif', 'bmp']:
                file_type = 'image'
            else:
                file_type = 'other'
            
        return {
            'file_name': file_name,
            'file_type': file_type,
            'file_size_mb': round(file_size, 2),
            'file_path': file_path
        }
    
    def upload_file(self, file_path, params=None):
        """
        上传文件到LlamaIndex API
        
        Args:
            file_path (str): 要上传的文件路径
            params (dict, optional): 自定义上传参数
            
        Returns:
            tuple: (job_id, upload_params) 或 (None, None) 如果失败
        """
        url = 'https://api.cloud.llamaindex.ai/api/v1/parsing/upload'
        headers = {
            'accept': 'application/json',
            'Authorization': f'Bearer {self.api_key}'
        }
        
        # 检查API密钥
        if not self.api_key:
            print(f"❌ 错误：未设置LLAMA_CLOUD_API_KEY")
            return None, None
        
        # 检查文件是否存在
        if not os.path.exists(file_path):
            print(f"❌ 错误：文件不存在 {file_path}")
            return None, None
            
        # 获取文件大小
        file_size = os.path.getsize(file_path) / (1024 * 1024)  # MB
        print(f"📄 准备上传文件: {os.path.basename(file_path)} ({file_size:.2f} MB)")
        
        # 默认参数
        if params is None:
            params = {
                "language": ["ch_sim", "en"],
                "parsing_instruction": "",
                "fast_mode": "false",
                "disable_ocr": "false",
                "max_pages": "10",
                "target_pages": "",
                "skip_diagonal_text": "false",
                "skip_table_extraction": "false",
                "extract_images": "false",
                "use_vendor_multimodal_model": "false",
                "vendor_multimodal_api_key": ""
            }
        
        retry_count = 0
        max_retries = 3
        
        while retry_count < max_retries:
            try:
                with open(file_path, 'rb') as f:
                    # 根据文件扩展名确定MIME类型
                    file_extension = os.path.splitext(file_path)[1].lower()
                    mime_type = 'application/pdf'
                    
                    if file_extension in ['.xlsx', '.xls']:
                        mime_type = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                    elif file_extension in ['.jpg', '.jpeg', '.png', '.gif', '.bmp']:
                        mime_type = f'image/{file_extension[1:]}' if file_extension != '.jpg' else 'image/jpeg'
                    
                    files = {
                        'file': (os.path.basename(file_path), f, mime_type)
                    }
                    
                    print(f"🔄 正在上传文件到 LlamaIndex... (尝试 {retry_count + 1}/{max_retries})")
                    
                    # 使用session进行请求，设置超时
                    response = self.session.post(
                        url, 
                        headers=headers, 
                        files=files, 
                        data=params,
                        timeout=(30, 300)  # 连接超时30秒，读取超时300秒
                    )
                    
                    # 检查响应状态
                    if response.status_code == 200:
                        result = response.json()
                        job_id = result.get('id')
                        if job_id:
                            print(f"✅ 文件上传成功，任务ID: {job_id}")
                            return job_id, params
                        else:
                            print(f"❌ 上传响应中未找到任务ID")
                            print(f"   响应内容: {response.text}")
                            return None, None
                    else:
                        print(f"❌ 上传文件失败，HTTP状态码: {response.status_code}")
                        print(f"   响应内容: {response.text}")
                        
                        # 尝试解析错误信息
                        try:
                            error_data = response.json()
                            error_message = error_data.get('error', error_data.get('message', 'Unknown error'))
                            print(f"   错误信息: {error_message}")
                        except:
                            pass
                        
                        # 如果是服务器错误，尝试重试
                        if response.status_code >= 500:
                            retry_count += 1
                            if retry_count < max_retries:
                                print(f"   将在 {retry_count * 2} 秒后重试...")
                                time.sleep(retry_count * 2)
                                continue
                        
                        return None, None
                    
            except FileNotFoundError:
                print(f"❌ 错误：文件未找到 {file_path}")
                return None, None
            except requests.exceptions.Timeout:
                retry_count += 1
                if retry_count < max_retries:
                    print(f"❌ 上传超时，将在 {retry_count * 2} 秒后重试... (尝试 {retry_count + 1}/{max_retries})")
                    time.sleep(retry_count * 2)
                    continue
                else:
                    print(f"❌ 错误：上传超时，已重试 {max_retries} 次 {file_path}")
                    return None, None
            except requests.exceptions.ConnectionError:
                retry_count += 1
                if retry_count < max_retries:
                    print(f"❌ 网络连接失败，将在 {retry_count * 2} 秒后重试... (尝试 {retry_count + 1}/{max_retries})")
                    time.sleep(retry_count * 2)
                    continue
                else:
                    print(f"❌ 错误：网络连接失败，已重试 {max_retries} 次 {file_path}")
                    return None, None
            except requests.exceptions.RequestException as e:
                retry_count += 1
                if retry_count < max_retries:
                    print(f"❌ 网络请求错误: {e}，将在 {retry_count * 2} 秒后重试... (尝试 {retry_count + 1}/{max_retries})")
                    time.sleep(retry_count * 2)
                    continue
                else:
                    print(f"❌ 上传文件时出错，已重试 {max_retries} 次 {file_path}: {e}")
                    try:
                        if hasattr(e, 'response') and e.response:
                            print(f"   响应内容: {e.response.text}")
                    except:
                        pass
                    return None, None
            except Exception as e:
                print(f"❌ 处理文件时发生未知错误 {file_path}: {e}")
                return None, None
        
        return None, None
    
    async def poll_job_status(self, job_id):
        """
        轮询LlamaIndex API获取任务状态
        
        Args:
            job_id (str): 要检查的任务ID
            
        Returns:
            tuple: (response_data, poll_count) 包含状态的响应数据和轮询次数，失败时返回 (None, 0)
        """
        url = f'https://api.cloud.llamaindex.ai/api/parsing/job/{job_id}'
        headers = {
            'accept': 'application/json',
            'Authorization': f'Bearer {self.api_key}'
        }

        poll_count = 0
        consecutive_failures = 0
        max_consecutive_failures = 5
        
        while True:
            poll_count += 1
            try:
                response = self.session.get(
                    url, 
                    headers=headers, 
                    timeout=(10, 60)  # 连接超时10秒，读取超时60秒
                )
                response.raise_for_status()
                data = response.json()
                status = data.get('status')
                print(f"任务 {job_id} 当前状态 (第{poll_count}次轮询): {status}")
                
                # 重置连续失败计数
                consecutive_failures = 0

                if status in ['SUCCESS', 'ERROR', 'PARTIAL_SUCCESS', 'CANCELLED']:
                    # 如果是ERROR状态，打印详细错误信息
                    if status == 'ERROR':
                        error_message = data.get('error', 'Unknown error')
                        error_details = data.get('error_details', '')
                        print(f"❌ 任务 {job_id} 处理失败:")
                        print(f"   错误信息: {error_message}")
                        if error_details:
                            print(f"   错误详情: {error_details}")
                        print(f"   完整响应: {json.dumps(data, ensure_ascii=False, indent=2)}")
                    return data, poll_count
                    
            except requests.exceptions.Timeout:
                consecutive_failures += 1
                print(f"⚠️ 轮询任务 {job_id} 超时 (第{poll_count}次轮询，连续失败 {consecutive_failures} 次)")
                if consecutive_failures >= max_consecutive_failures:
                    print(f"❌ 连续失败 {max_consecutive_failures} 次，停止轮询")
                    return None, poll_count
            except requests.exceptions.ConnectionError:
                consecutive_failures += 1
                print(f"⚠️ 轮询任务 {job_id} 网络连接失败 (第{poll_count}次轮询，连续失败 {consecutive_failures} 次)")
                if consecutive_failures >= max_consecutive_failures:
                    print(f"❌ 连续失败 {max_consecutive_failures} 次，停止轮询")
                    return None, poll_count
            except requests.exceptions.RequestException as e:
                consecutive_failures += 1
                print(f"⚠️ 轮询任务 {job_id} 时出错 (第{poll_count}次轮询，连续失败 {consecutive_failures} 次): {e}")
                if consecutive_failures >= max_consecutive_failures:
                    print(f"❌ 连续失败 {max_consecutive_failures} 次，停止轮询")
                    return None, poll_count
                # 尝试获取响应内容
                try:
                    if hasattr(e, 'response') and e.response:
                        print(f"错误响应内容: {e.response.text}")
                except:
                    pass
            except Exception as e:
                consecutive_failures += 1
                print(f"⚠️ 轮询任务 {job_id} 时发生未知错误 (第{poll_count}次轮询，连续失败 {consecutive_failures} 次): {e}")
                if consecutive_failures >= max_consecutive_failures:
                    print(f"❌ 连续失败 {max_consecutive_failures} 次，停止轮询")
                    return None, poll_count

            # 如果有连续失败，增加等待时间
            if consecutive_failures > 0:
                wait_time = self.polling_interval * (1 + consecutive_failures)
                print(f"   等待 {wait_time} 秒后重试...")
                await asyncio.sleep(wait_time)
            else:
                # 等待后再次轮询
                await asyncio.sleep(self.polling_interval)
    
    def get_markdown_result(self, job_id, original_file_path):
        """
        获取已完成任务的markdown结果
        
        Args:
            job_id (str): 要获取结果的任务ID
            original_file_path (str): 原始文件路径用于引用
            
        Returns:
            str: Markdown内容，失败时返回None
        """
        url = f'https://api.cloud.llamaindex.ai/api/parsing/job/{job_id}/result/markdown'
        headers = {
            'accept': 'application/json',
            'Authorization': f'Bearer {self.api_key}'
        }

        retry_count = 0
        max_retries = 3
        
        while retry_count < max_retries:
            try:
                print(f"🔄 正在获取任务 {job_id} 的处理结果... (尝试 {retry_count + 1}/{max_retries})")
                
                response = self.session.get(
                    url, 
                    headers=headers, 
                    timeout=(10, 120)  # 连接超时10秒，读取超时120秒
                )
                
                if response.status_code == 200:
                    result_json = response.json()
                    markdown_content = result_json.get('markdown', '')
                    
                    if markdown_content:
                        content_length = len(markdown_content.split())
                        print(f"✅ 任务 {job_id} ({os.path.basename(original_file_path)}) 的 Markdown 内容已获取 ({content_length} 词)")
                        return markdown_content
                    else:
                        print(f"❌ 任务 {job_id} ({os.path.basename(original_file_path)}) 返回空内容")
                        return None
                else:
                    print(f"❌ 获取任务 {job_id} ({os.path.basename(original_file_path)}) 结果失败")
                    print(f"   HTTP状态码: {response.status_code}")
                    print(f"   响应内容: {response.text}")
                    
                    # 尝试解析错误信息
                    try:
                        error_data = response.json()
                        error_message = error_data.get('error', error_data.get('message', 'Unknown error'))
                        print(f"   错误信息: {error_message}")
                    except:
                        pass
                    
                    # 如果是服务器错误，尝试重试
                    if response.status_code >= 500:
                        retry_count += 1
                        if retry_count < max_retries:
                            print(f"   将在 {retry_count * 2} 秒后重试...")
                            time.sleep(retry_count * 2)
                            continue
                    
                    return None

            except requests.exceptions.Timeout:
                retry_count += 1
                if retry_count < max_retries:
                    print(f"❌ 获取结果超时，将在 {retry_count * 2} 秒后重试... (尝试 {retry_count + 1}/{max_retries})")
                    time.sleep(retry_count * 2)
                    continue
                else:
                    print(f"❌ 获取任务 {job_id} ({os.path.basename(original_file_path)}) 结果超时，已重试 {max_retries} 次")
                    return None
            except requests.exceptions.ConnectionError:
                retry_count += 1
                if retry_count < max_retries:
                    print(f"❌ 获取结果时网络连接失败，将在 {retry_count * 2} 秒后重试... (尝试 {retry_count + 1}/{max_retries})")
                    time.sleep(retry_count * 2)
                    continue
                else:
                    print(f"❌ 获取任务 {job_id} ({os.path.basename(original_file_path)}) 结果时网络连接失败，已重试 {max_retries} 次")
                    return None
            except requests.exceptions.RequestException as e:
                retry_count += 1
                if retry_count < max_retries:
                    print(f"❌ 获取结果时网络请求错误: {e}，将在 {retry_count * 2} 秒后重试... (尝试 {retry_count + 1}/{max_retries})")
                    time.sleep(retry_count * 2)
                    continue
                else:
                    print(f"❌ 获取任务 {job_id} ({os.path.basename(original_file_path)}) 结果时出错，已重试 {max_retries} 次: {e}")
                    try:
                        if hasattr(e, 'response') and e.response:
                            print(f"   响应内容: {e.response.text}")
                    except:
                        pass
                    return None
            except Exception as e:
                print(f"❌ 处理任务 {job_id} ({os.path.basename(original_file_path)}) 结果时发生未知错误: {e}")
                return None
        
        return None
    
    async def process_file(self, file_path, results_collector, failed_files, params=None):
        """
        处理单个文件（上传、监控、获取结果）
        
        Args:
            file_path (str): 要处理的文件路径
            results_collector (list): 收集成功结果的列表
            failed_files (list): 收集失败文件的列表
            params (dict, optional): 自定义上传参数
            
        Returns:
            tuple: (file_info, processing_time) 包含元数据和处理时间
        """
        self.log(f"开始处理文件: {file_path}")
        file_start_time = time.time()

        # 计算文件大小 (MB)
        file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
        
        # 获取文件类型
        file_extension = os.path.splitext(file_path)[1].lower()
        file_type = file_extension[1:] if file_extension else "unknown"

        # 步骤1: 上传文件
        job_id, upload_params = self.upload_file(file_path, params)

        if not job_id:
            self.log(f"文件上传失败，跳过处理: {file_path}", "error")
            failed_files.append(file_path)
            
            # 计算处理时间（即使失败）
            file_end_time = time.time()
            file_total_time = file_end_time - file_start_time
            self.log(f"文件处理失败: {os.path.basename(file_path)}, 耗时: {file_total_time:.2f} 秒", "error")
            
            # 返回失败文件信息
            file_info = {
                'file_path': file_path,
                'file_name': os.path.basename(file_path),
                'file_type': file_type,
                'file_size_mb': file_size_mb,
                'processing_time': file_total_time,
                'content': None,
                'is_success': False
            }
            return file_info, file_total_time


        # 步骤2: 轮询任务状态
        self.log(f"使用轮询方式监控任务状态...")
        status_data, poll_count = await self.poll_job_status(job_id)
        final_status = status_data.get('status') if status_data else "未知 (监听失败)"
        self.log(f"文件 {os.path.basename(file_path)} (任务ID: {job_id}) 处理完成, 总计轮询 {poll_count} 次")

        # 步骤3: 如果成功，获取结果
        if final_status == 'SUCCESS':
            self.log(f"任务 {job_id} 处理成功，正在获取结果...")
            content = self.get_markdown_result(job_id, file_path)
            
            if content is not None:
                # 计算内容长度（简单的token估算）
                content_length = len(content.split())
                
                file_end_time = time.time()
                file_total_time = file_end_time - file_start_time
                
                # 添加到结果收集器，包含处理时间
                results_collector.append({
                    'content': content,
                    'original_file': file_path,
                    'content_length': content_length,
                    'processing_time': file_total_time
                })
                self.log(f"文件处理完成: {os.path.basename(file_path)}, 耗时: {file_total_time:.2f} 秒, 轮询次数: {poll_count}次")
                
                # 返回成功文件信息
                file_info = {
                    'file_path': file_path,
                    'file_name': os.path.basename(file_path),
                    'file_type': file_type,
                    'file_size_mb': file_size_mb,
                    'processing_time': file_total_time,
                    'content': content,
                    'content_length': content_length,
                    'poll_count': poll_count,
                    'is_success': True
                }
                return file_info, file_total_time
            else:
                self.log(f"任务 {job_id} ({os.path.basename(file_path)}) 获取内容失败", "error")
                failed_files.append(file_path)
        else:
            # 获取错误信息
            error_message = "处理失败"
            if status_data:
                error_message = status_data.get('error', error_message)
                error_details = status_data.get('error_details', '')
                if error_details:
                    error_message += f": {error_details}"
            
            self.log(f"任务 {job_id} ({os.path.basename(file_path)}) 未成功完成，最终状态: {final_status}", "error")
            self.log(f"错误信息: {error_message}", "error")
            failed_files.append(file_path)

        file_end_time = time.time()
        file_total_time = file_end_time - file_start_time
        self.log(f"文件处理完成: {os.path.basename(file_path)}, 耗时: {file_total_time:.2f} 秒, 轮询次数: {poll_count}次")
        
        # 返回失败文件信息，包含错误信息
        file_info = {
            'file_path': file_path,
            'file_name': os.path.basename(file_path),
            'file_type': file_type,
            'file_size_mb': file_size_mb,
            'processing_time': file_total_time,
            'content': None,
            'content_length': 0,
            'poll_count': poll_count,
            'is_success': False,
            'error': error_message if 'error_message' in locals() else "Unknown error"
        }
        return file_info, file_total_time
    
    def save_individual_result(self, file_path, content):
        """
        保存单个文件的结果到磁盘
        
        Args:
            file_path (str): 原始文件路径
            content (str): 要保存的Markdown内容
            
        Note: 这个方法已废弃，因为内容现在保存到数据库中
        """
        # 不再创建文件，因为内容存储在数据库中
        print(f"文件结果已保存到数据库: {os.path.basename(file_path)}")
        return None
 