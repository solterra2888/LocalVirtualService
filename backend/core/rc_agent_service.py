# -*- coding: utf-8 -*-
"""
RC Agent 服务模块
"""

import json
import time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from config.settings import config
from backend.utils.logger import get_logger
import os


class RCAgentService:
    """RC Agent 服务管理类"""
    
    def __init__(self, rc_agent_token=None, base_url=None, log_callback=None):
        """
        初始化RC Agent服务
        
        Args:
            rc_agent_token: RC Agent API令牌
            base_url: RC Agent服务器地址
            log_callback: 日志回调函数
        """
        self.rc_agent_token = rc_agent_token
        self.base_url = base_url or 'http://111.230.48.119/v1/chat-messages'
        self.conversation_id = None
        self.log_callback = log_callback
        # 使用统一日志系统
        self.logger = get_logger("rc_agent_service")
        
        # 创建配置了重试和超时的会话
        self.session = self._create_session()
    
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
        # RC Agent步骤日志只发送给前端，不在后端打印
        if "🔄 RC Agent步骤" in message:
            # 只通过callback发送给前端
            if self.log_callback:
                self.log_callback(log_message=message, log_level=level)
        else:
            # 其他日志同时在后端记录和发送给前端
            self.logger.log_rc_agent_service(message, level)
            if self.log_callback:
                self.log_callback(log_message=message, log_level=level)
    
    def send_to_rc_agent(self, content, conversation_id=None, custom_inputs=None, current_file_name=None):
        """
        发送内容到RC Agent服务器
        
        Args:
            content: 要发送的内容
            conversation_id: 现有会话ID (可选)
            custom_inputs: 自定义输入字段字典 (可选)
            current_file_name: 当前处理的文件名 (可选)
            
        Returns:
            tuple: (conversation_id, report_content, model_usage_info)
        """
        # 记录RC Agent请求开始时间
        rc_agent_start_time = time.time()
        
        if not self.rc_agent_token:
            self.log("错误：未提供RC_AGENT_TOKEN", "error")
            return None, None, None
        
        if not content:
            self.log("警告：没有有效的内容可发送", "warning")
            return conversation_id, None, None
        
        # 设置请求参数
        headers = {
            'Authorization': f'Bearer {self.rc_agent_token}',
            'Content-Type': 'application/json'
        }
        
        # 调试输出token信息
        self.log(f"🔑 RC Agent Token: {self.rc_agent_token[:20]}... (长度: {len(self.rc_agent_token)})")
        
        # 确保自定义输入是正确的字典格式
        if not isinstance(custom_inputs, dict):
            self.log(f"警告：自定义输入字段不是字典类型，而是 {type(custom_inputs)}，将被忽略", "warning")
            custom_inputs = {}
        else:
            custom_inputs = custom_inputs.copy() if custom_inputs else {}
        
        # 调试输出自定义输入字段
        if custom_inputs:
            self.log(f"自定义输入字段: {json.dumps(custom_inputs, ensure_ascii=False)}")
        
        # 准备请求数据
        data = {
            "inputs": custom_inputs,
            "query": content,
            "response_mode": "streaming",
            "user": "llamaindex-processor"
        }
        
        # 调试输出完整请求数据结构
        self.log(f"RC Agent请求数据结构: {json.dumps(data, ensure_ascii=False, indent=2)[:50]}...")
        
        # 如果有自定义输入字段，在日志中显示
        if custom_inputs:
            self.log(f"使用自定义输入字段: {', '.join(custom_inputs.keys())}")
        
        # 使用现有会话ID（如果有）
        if conversation_id:
            data["conversation_id"] = conversation_id
            self.log(f"使用现有会话ID: {conversation_id}")
        else:
            self.log("第一份文件，创建新的conversation_id")
        
        # 用于存储提取的报告内容
        report_content = None
        
        # 用于收集模型使用信息
        model_usage_info = []
        
        retry_count = 0
        max_retries = 3
        
        while retry_count < max_retries:
            try:
                # 发送请求
                file_info = f" - 文件: {current_file_name}" if current_file_name else ""
                retry_info = f" (尝试 {retry_count + 1}/{max_retries})" if retry_count > 0 else ""
                self.log(f"正在发送请求到RC Agent服务器{file_info}{retry_info}...")
                
                response = self.session.post(
                    self.base_url, 
                    headers=headers, 
                    json=data, 
                    stream=True,
                    timeout=(30, 300)  # 连接超时30秒，读取超时300秒
                )
                
                # 检查错误状态码并打印完整响应以进行调试
                if response.status_code != 200:
                    error_text = ""
                    try:
                        error_text = response.text
                        error_json = response.json()
                        self.log(f"RC Agent服务器返回错误: {response.status_code}", "error")
                        self.log(f"错误详情: {json.dumps(error_json, ensure_ascii=False, indent=2)}", "error")
                    except:
                        self.log(f"RC Agent服务器返回错误: {response.status_code}", "error")
                        self.log(f"错误响应内容: {error_text}", "error")
                    
                    # 如果是服务器错误，尝试重试
                    if response.status_code >= 500:
                        retry_count += 1
                        if retry_count < max_retries:
                            self.log(f"   将在 {retry_count * 2} 秒后重试...")
                            time.sleep(retry_count * 2)
                            continue
                    
                    return conversation_id, None, None
                
                # 继续正常响应处理
                response.raise_for_status()
                
                # 打印完整响应文本
                header = f" - {current_file_name}" if current_file_name else ""
                self.log(f"======== RC Agent处理流程开始{header} ========")
                
                # 从响应中提取conversation_id
                step_index = 1  # 添加步骤索引计数器
                rc_agent_process_start_time = time.time()  # 全局RC Agent处理开始时间
                last_step_time = rc_agent_process_start_time  # 上一步完成时间，初始为开始时间
                
                for line in response.iter_lines():
                    if line:
                        try:
                            # 解析每行为JSON数据
                            data_str = line.decode('utf-8').strip()
                            
                            if data_str.startswith('data: '):
                                json_data = json.loads(data_str[6:])
                                
                                # 提取conversation_id（如果存在且我们还没有）
                                if 'conversation_id' in json_data and not conversation_id:
                                    conversation_id = json_data['conversation_id']
                                    self.log(f"📋 获取到新会话ID: {conversation_id}")
                                
                                # 打印算法模型步骤
                                if json_data.get('event') == 'node_finished':
                                    title = json_data['data'].get('title')
                                    
                                    if title:
                                        # 计算当前步骤耗时（从上一步结束到现在）
                                        current_time = time.time()
                                        elapsed_time = current_time - last_step_time
                                        last_step_time = current_time  # 更新上一步完成时间
                                            
                                        # 使用特殊格式突出显示步骤，并添加耗时
                                        self.log(f'🔄 RC Agent步骤 {step_index}: {title} 耗时 {int(elapsed_time * 1000)}毫秒')
                                        step_index += 1
                                    
                                    # 提取模型名称和费用信息
                                    node_data = json_data.get('data', {})
                                    node_type = node_data.get('node_type')
                                    if node_type == 'llm':
                                        # 提取模型名称
                                        process_data = node_data.get('process_data', {})
                                        model_name = process_data.get('model_name')
                                        
                                        # 提取使用信息
                                        execution_metadata = node_data.get('execution_metadata', {})
                                        total_tokens = execution_metadata.get('total_tokens', 0)
                                        total_price = execution_metadata.get('total_price', 0)
                                        currency = execution_metadata.get('currency', 'RMB')
                                        
                                        if model_name and total_tokens:
                                            usage_info = {
                                                'model_name': model_name,
                                                'total_tokens': total_tokens,
                                                'total_price': float(total_price),
                                                'currency': currency
                                            }
                                            model_usage_info.append(usage_info)
                                            # 输出模型使用信息
                                            self.log(f'   💡 使用模型: {model_name}, Tokens: {total_tokens}, 费用: {float(total_price):.6f} {currency}')
                                
                                # 提取workflow_finished事件的answer内容
                                if json_data.get('event') == 'workflow_finished':
                                    if 'data' in json_data and 'outputs' in json_data['data'] and 'answer' in json_data['data']['outputs']:
                                        report_content = json_data['data']['outputs']['answer']
                                        file_prefix = f"[{current_file_name}] " if current_file_name else ""
                                        self.log(f"✅ {file_prefix}成功提取最终报告内容")
                                    
                        except ValueError as e:
                            # 捕获并打印JSON解析错误，但继续处理下一行
                            self.log(f"解析行失败: {data_str}", "warning")
                            self.log(f"错误: {e}", "warning")
                        except Exception as e:
                            self.log(f"处理响应时出错: {e}", "error")
                
                footer = f" - {current_file_name}" if current_file_name else ""
                self.log(f"======== RC Agent处理流程结束{footer} ========")
                
                # 计算RC Agent请求时间
                rc_agent_end_time = time.time()
                rc_agent_time = rc_agent_end_time - rc_agent_start_time
                file_prefix = f"[{current_file_name}] " if current_file_name else ""
                
                # 汇总使用信息
                if model_usage_info:
                    total_tokens_sum = sum(info['total_tokens'] for info in model_usage_info)
                    total_price_sum = sum(info['total_price'] for info in model_usage_info)
                    file_prefix = f"[{current_file_name}] " if current_file_name else ""
                    self.log(f"📊 {file_prefix}模型使用汇总 - 总tokens: {total_tokens_sum}, 总费用: {total_price_sum:.6f} {model_usage_info[0]['currency']}")
                
                if report_content:
                    file_prefix = f"[{current_file_name}] " if current_file_name else ""
                    self.log(f"✅ {file_prefix}成功从RC Agent获取响应内容")
                else:
                    file_prefix = f"[{current_file_name}] " if current_file_name else ""
                    self.log(f"⚠️ {file_prefix}警告：未能从RC Agent响应中提取内容", "warning")
                    
                return conversation_id, report_content, model_usage_info
                
            except requests.exceptions.Timeout:
                retry_count += 1
                if retry_count < max_retries:
                    self.log(f"❌ RC Agent请求超时，将在 {retry_count * 2} 秒后重试... (尝试 {retry_count + 1}/{max_retries})")
                    time.sleep(retry_count * 2)
                    continue
                else:
                    self.log(f"❌ RC Agent请求超时，已重试 {max_retries} 次", "error")
                    return conversation_id, None, None
            except requests.exceptions.ConnectionError:
                retry_count += 1
                if retry_count < max_retries:
                    self.log(f"❌ RC Agent连接失败，将在 {retry_count * 2} 秒后重试... (尝试 {retry_count + 1}/{max_retries})")
                    time.sleep(retry_count * 2)
                    continue
                else:
                    self.log(f"❌ RC Agent连接失败，已重试 {max_retries} 次", "error")
                    return conversation_id, None, None
            except requests.exceptions.RequestException as e:
                retry_count += 1
                if retry_count < max_retries:
                    self.log(f"❌ RC Agent网络请求错误: {e}，将在 {retry_count * 2} 秒后重试... (尝试 {retry_count + 1}/{max_retries})")
                    time.sleep(retry_count * 2)
                    continue
                else:
                    self.log(f"发送到RC Agent服务器失败，已重试 {max_retries} 次: {str(e)}", "error")
                    return conversation_id, None, None
            except Exception as e:
                self.log(f"发送到RC Agent服务器时发生未知错误: {str(e)}", "error")
                import traceback
                error_details = traceback.format_exc()
                self.log(f"异常详情: {error_details}", "error")
                return conversation_id, None, None
        
        return conversation_id, None, None
    
    def process_all_contents(self, successful_results, custom_inputs=None):
        """
        处理所有成功文件的内容并发送到RC Agent
        
        Args:
            successful_results (list): 成功处理的文件结果列表
            custom_inputs (dict, optional): 自定义输入字段
            
        Returns:
            tuple: (final_report, file_rc_agent_times)
        """
        if not successful_results:
            print("没有成功处理的文件内容可发送")
            return None, []
        
        if not hasattr(self, 'rc_agent_token') or not self.rc_agent_token:
            print("未提供RC Agent令牌，跳过RC Agent处理")
            return None, []
        
        print(f"\n🚀 开始批量处理 {len(successful_results)} 个文件内容...")
        
        # 初始化变量
        conversation_id = None
        final_report = None
        file_rc_agent_times = []
        
        # 逐个处理文件，显示详细步骤
        for index, result in enumerate(successful_results):
            content = result['content']
            original_file = result['original_file']
            file_name = os.path.basename(original_file)
            
            print(f"\n📄 开始处理第 {index+1}/{len(successful_results)} 个文件: {file_name}")
            print(f"=== 文件内容长度: {len(content.split())} 词 ===")
            
            # 记录开始时间
            start_time = time.time()
            
            # 发送到RC Agent并获取完整处理步骤
            conversation_id, report_content, model_usage_info = self.send_to_rc_agent(
                content, conversation_id, custom_inputs, file_name
            )
            
            # 计算处理时间
            end_time = time.time()
            rc_agent_time = end_time - start_time
            
            # 如果是最后一个文件，保存报告内容
            if index == len(successful_results) - 1 and report_content:
                final_report = report_content
            
            # 添加此文件的处理时间记录
            file_info = {
                'file': file_name,
                'file_path': original_file,
                'ai_time': rc_agent_time,  # AI处理时间
                'total_tokens': sum(info.get('total_tokens', 0) for info in model_usage_info) if model_usage_info else 0,
                'total_price': sum(info.get('total_price', 0) for info in model_usage_info) if model_usage_info else 0,
                'model_usage': model_usage_info or []
            }
            file_rc_agent_times.append(file_info)
            
            # 显示此文件的处理情况总结
            print(f"📄 文件 '{file_name}' 的RC Agent处理耗时: {rc_agent_time:.2f} 秒")
            
            # 如果处理成功但未获得报告内容，显示警告
            if not report_content and index == len(successful_results) - 1:
                print("⚠️ 警告: 未能从最后一个文件的处理中获取报告内容")
        
        return final_report, file_rc_agent_times 