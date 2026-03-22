# -*- coding: utf-8 -*-
"""
Fun-ASR 语音识别服务
使用阿里云 DashScope 的 Fun-ASR 模型进行语音识别
"""

import os
import json
import time
from http import HTTPStatus
from typing import Dict, Any, List, Optional
from urllib import request as urllib_request

try:
    from dashscope.audio.asr import Transcription
    import dashscope
    DASHSCOPE_AVAILABLE = True
except ImportError:
    DASHSCOPE_AVAILABLE = False

from backend.utils.logger import info, error, warning
from backend.utils.processing_timeout import get_file_processing_timeout_seconds


class ASRService:
    """Fun-ASR 语音识别服务类"""
    
    def __init__(self):
        """初始化 ASR 服务"""
        # 从环境变量读取 API Key
        self.api_key = os.getenv('DASHSCOPE_API_KEY')
        
        # 设置 API 基础 URL（北京地域）
        if DASHSCOPE_AVAILABLE:
            dashscope.base_http_api_url = 'https://dashscope.aliyuncs.com/api/v1'
            if self.api_key:
                dashscope.api_key = self.api_key
        
        # 检查配置
        if not self.api_key:
            warning("DASHSCOPE_API_KEY 未配置，ASR 服务将无法使用")
        
        if not DASHSCOPE_AVAILABLE:
            warning("dashscope SDK 未安装，ASR 服务将无法使用。请运行: pip install dashscope")
    
    def is_configured(self) -> bool:
        """检查 ASR 服务是否已正确配置"""
        return DASHSCOPE_AVAILABLE and self.api_key is not None
    
    def submit_transcription_task(
        self,
        audio_url: str,
        model: str = 'fun-asr',
        language_hints: Optional[List[str]] = None,
        diarization_enabled: bool = True
    ) -> Dict[str, Any]:
        """
        提交语音识别任务（异步）
        
        Args:
            audio_url: 音频文件的公网 URL
            model: 使用的模型（默认 fun-asr）
            language_hints: 语言提示（如 ['zh', 'en']）
            diarization_enabled: 是否启用说话人分离（默认 True）
            
        Returns:
            Dict[str, Any]: 任务提交结果
                {
                    "success": bool,
                    "task_id": str,
                    "message": str,
                    "error": str  # 如果失败
                }
        """
        result = {
            "success": False,
            "task_id": "",
            "message": "",
            "error": ""
        }
        
        try:
            # 检查配置
            if not self.is_configured():
                result["error"] = "ASR 服务未配置。请设置 DASHSCOPE_API_KEY 并安装 dashscope SDK"
                return result
            
            # 默认语言提示
            if language_hints is None:
                language_hints = ['zh', 'en']
            
            info(f"提交 ASR 任务: audio_url={audio_url}, model={model}, language_hints={language_hints}, diarization_enabled={diarization_enabled}")
            
            # 调用 DashScope API 提交任务
            task_response = Transcription.async_call(
                model=model,
                file_urls=[audio_url],
                language_hints=language_hints,
                diarization_enabled=diarization_enabled
            )
            
            if task_response and task_response.output:
                task_id = task_response.output.task_id
                result["success"] = True
                result["task_id"] = task_id
                result["message"] = "ASR 任务提交成功"
                info(f"ASR 任务提交成功: task_id={task_id}")
            else:
                result["error"] = "任务提交失败：未返回 task_id"
                error(f"ASR 任务提交失败: {task_response}")
            
        except Exception as e:
            error_msg = f"提交 ASR 任务失败: {str(e)}"
            error(error_msg)
            result["error"] = error_msg
        
        return result
    
    def get_transcription_status(self, task_id: str) -> Dict[str, Any]:
        """
        查询语音识别任务状态
        
        Args:
            task_id: 任务 ID
            
        Returns:
            Dict[str, Any]: 任务状态
                {
                    "success": bool,
                    "status": str,  # "PENDING", "RUNNING", "SUCCEEDED", "FAILED"
                    "progress": int,  # 0-100
                    "message": str,
                    "error": str  # 如果失败
                }
        """
        result = {
            "success": False,
            "status": "UNKNOWN",
            "progress": 0,
            "message": "",
            "error": ""
        }
        
        try:
            # 检查配置
            if not self.is_configured():
                result["error"] = "ASR 服务未配置"
                return result
            
            # 查询任务状态
            status_response = Transcription.fetch(task=task_id)
            
            if status_response and status_response.output:
                task_status = status_response.output.get('task_status', 'UNKNOWN')
                result["success"] = True
                result["status"] = task_status
                result["message"] = f"任务状态: {task_status}"
                
                # 估算进度
                if task_status == 'PENDING':
                    result["progress"] = 10
                elif task_status == 'RUNNING':
                    result["progress"] = 50
                elif task_status == 'SUCCEEDED':
                    result["progress"] = 100
                elif task_status == 'FAILED':
                    result["progress"] = 0
            else:
                result["error"] = "无法获取任务状态"
            
        except Exception as e:
            error_msg = f"查询 ASR 任务状态失败: {str(e)}"
            error(error_msg)
            result["error"] = error_msg
        
        return result
    
    def wait_for_completion(
        self,
        task_id: str,
        timeout: Optional[int] = None,
        poll_interval: int = 5
    ) -> Dict[str, Any]:
        """
        等待语音识别任务完成
        
        Args:
            task_id: 任务 ID
            timeout: 超时时间（秒），默认取自 FILE_PROCESSING_TIMEOUT_MINUTES
            poll_interval: 轮询间隔（秒），默认 5 秒
            
        Returns:
            Dict[str, Any]: 任务结果
                {
                    "success": bool,
                    "status": str,
                    "results": List[Dict],  # 识别结果列表
                    "error": str  # 如果失败
                }
        """
        result = {
            "success": False,
            "status": "UNKNOWN",
            "results": [],
            "error": ""
        }
        
        try:
            # 检查配置
            if not self.is_configured():
                result["error"] = "ASR 服务未配置"
                return result

            if timeout is None:
                timeout = get_file_processing_timeout_seconds()
            
            info(f"等待 ASR 任务完成: task_id={task_id}, timeout={timeout}s")
            
            deadline = time.monotonic() + timeout
            while time.monotonic() < deadline:
                transcription_response = Transcription.fetch(task=task_id)
                sc = getattr(transcription_response, "status_code", HTTPStatus.OK)
                if sc != HTTPStatus.OK:
                    warning(f"ASR fetch 返回非 OK: status_code={sc}, task_id={task_id}")
                    time.sleep(poll_interval)
                    continue
                if not transcription_response or not transcription_response.output:
                    time.sleep(poll_interval)
                    continue
                out = transcription_response.output
                if isinstance(out, dict):
                    task_status = out.get("task_status", "UNKNOWN")
                    results = out.get("results", [])
                else:
                    task_status = getattr(out, "task_status", None) or "UNKNOWN"
                    results = getattr(out, "results", None) or []
                if task_status == "SUCCEEDED":
                    result["success"] = True
                    result["status"] = "SUCCEEDED"
                    result["results"] = results
                    info(f"ASR 任务完成: task_id={task_id}, 结果数={len(results)}")
                    return result
                if task_status == "FAILED":
                    result["status"] = "FAILED"
                    if isinstance(out, dict):
                        err_detail = out.get("message") or out.get("code") or out.get("task_id")
                    else:
                        err_detail = getattr(out, "message", None) or getattr(out, "code", None)
                    result["error"] = f"任务失败: {err_detail or 'Unknown error'}"
                    error(f"ASR 任务失败: {result['error']}")
                    return result
                time.sleep(poll_interval)

            result["status"] = "TIMEOUT"
            result["error"] = f"ASR 任务等待超时（{timeout} 秒）"
            error(f"{result['error']}, task_id={task_id}")
            
        except Exception as e:
            error_msg = f"等待 ASR 任务完成失败: {str(e)}"
            error(error_msg)
            result["error"] = error_msg
            result["status"] = "ERROR"
        
        return result
    
    def get_transcription_results(
        self,
        task_id: str
    ) -> Dict[str, Any]:
        """
        获取语音识别结果（含详细转写内容）
        
        Args:
            task_id: 任务 ID
            
        Returns:
            Dict[str, Any]: 识别结果
                {
                    "success": bool,
                    "task_id": str,
                    "status": str,
                    "transcriptions": List[Dict],  # 详细的转写结果
                    "error": str  # 如果失败
                }
        """
        result = {
            "success": False,
            "task_id": task_id,
            "status": "UNKNOWN",
            "transcriptions": [],
            "error": ""
        }
        
        try:
            # 等待任务完成
            wait_result = self.wait_for_completion(task_id)
            
            if not wait_result["success"]:
                result["error"] = wait_result["error"]
                result["status"] = wait_result["status"]
                return result
            
            # 获取结果
            transcriptions = []
            for subtask in wait_result["results"]:
                if subtask.get('subtask_status') == 'SUCCEEDED':
                    # 获取转写结果 URL
                    transcription_url = subtask.get('transcription_url')
                    if transcription_url:
                        # 下载并解析转写结果
                        response = urllib_request.urlopen(transcription_url)
                        transcription_data = json.loads(response.read().decode('utf8'))
                        transcriptions.append(transcription_data)
                        
                        info(f"获取 ASR 结果成功: file_url={transcription_data.get('file_url')}")
                        
                        # 调试：检查是否包含说话人信息
                        properties = transcription_data.get('properties', {})
                        diarization = properties.get('diarization')
                        if diarization:
                            info(f"说话人分离信息: {diarization}")
                        else:
                            warning(f"ASR 返回结果中未包含说话人分离信息 (diarization)")
                        
                        # 检查句子中是否有 speaker 字段
                        sample_sentences = []
                        for transcript in transcription_data.get('transcripts', []):
                            sentences = transcript.get('sentences', [])[:3]  # 只看前3句
                            for sent in sentences:
                                sample_sent = {
                                    'text': sent.get('text', '')[:50],  # 只看前50字符
                                    'has_speaker': 'speaker' in sent,
                                    'speaker': sent.get('speaker', 'N/A')
                                }
                                sample_sentences.append(sample_sent)
                        
                        if sample_sentences:
                            info(f"句子样本（前3句）: {json.dumps(sample_sentences, ensure_ascii=False)}")
                else:
                    warning(f"ASR 子任务失败: {subtask}")
            
            result["success"] = True
            result["status"] = "COMPLETED"
            result["transcriptions"] = transcriptions
            
            info(f"ASR 任务完成，共 {len(transcriptions)} 个结果")
            
        except Exception as e:
            error_msg = f"获取 ASR 结果失败: {str(e)}"
            error(error_msg)
            result["error"] = error_msg
            result["status"] = "ERROR"
        
        return result
    
    def transcribe_audio(
        self,
        audio_url: str,
        model: str = 'fun-asr',
        language_hints: Optional[List[str]] = None,
        wait_for_result: bool = True,
        diarization_enabled: bool = True
    ) -> Dict[str, Any]:
        """
        一键语音识别（提交任务 + 等待结果）
        
        Args:
            audio_url: 音频文件的公网 URL
            model: 使用的模型（默认 fun-asr）
            language_hints: 语言提示（如 ['zh', 'en']）
            wait_for_result: 是否等待结果（默认 True）
            diarization_enabled: 是否启用说话人分离（默认 True）
            
        Returns:
            Dict[str, Any]: 识别结果
                {
                    "success": bool,
                    "task_id": str,
                    "status": str,
                    "transcriptions": List[Dict],  # 如果 wait_for_result=True
                    "text": str,  # 合并的文本
                    "error": str  # 如果失败
                }
        """
        result = {
            "success": False,
            "task_id": "",
            "status": "PENDING",
            "transcriptions": [],
            "text": "",
            "error": ""
        }
        
        try:
            # 1. 提交任务
            submit_result = self.submit_transcription_task(
                audio_url=audio_url,
                model=model,
                language_hints=language_hints,
                diarization_enabled=diarization_enabled
            )
            
            if not submit_result["success"]:
                result["error"] = submit_result["error"]
                return result
            
            task_id = submit_result["task_id"]
            result["task_id"] = task_id
            
            # 2. 如果需要等待结果
            if wait_for_result:
                transcription_result = self.get_transcription_results(task_id)
                
                if transcription_result["success"]:
                    result["success"] = True
                    result["status"] = transcription_result["status"]
                    result["transcriptions"] = transcription_result["transcriptions"]
                    
                    # 提取文本
                    texts = []
                    for trans in transcription_result["transcriptions"]:
                        for transcript in trans.get('transcripts', []):
                            text = transcript.get('text', '')
                            if text:
                                texts.append(text)
                    
                    result["text"] = "\n".join(texts)
                else:
                    result["error"] = transcription_result["error"]
                    result["status"] = transcription_result["status"]
            else:
                # 不等待结果，直接返回 task_id
                result["success"] = True
                result["status"] = "SUBMITTED"
                result["message"] = "任务已提交，请稍后查询结果"
            
        except Exception as e:
            error_msg = f"语音识别失败: {str(e)}"
            error(error_msg)
            result["error"] = error_msg
        
        return result
    
    def format_transcription_result(
        self,
        transcription_data: Dict[str, Any],
        include_timestamps: bool = True,
        include_words: bool = False,
        include_speaker: bool = True,
        timestamped_only: bool = False
    ) -> str:
        """
        格式化转写结果为可读文本

        Args:
            transcription_data: 转写结果数据
            include_timestamps: 是否包含时间戳
            include_words: 是否包含词级别详情
            include_speaker: 是否包含说话人信息（默认 True）
            timestamped_only: 若为 True，仅输出带时间戳的句子，每段一行，用于保存到 DB

        Returns:
            str: 格式化后的文本
        """
        output = []

        if timestamped_only:
            # 仅输出带时间戳的句子，每段一行，段与段之间换行
            for transcript in transcription_data.get('transcripts', []):
                if not include_timestamps:
                    continue
                for sentence in transcript.get('sentences', []):
                    begin = sentence.get('begin_time', 0) / 1000
                    end = sentence.get('end_time', 0) / 1000
                    sent_text = sentence.get('text', '')
                    speaker_label = ""
                    if include_speaker and 'speaker' in sentence:
                        speaker = sentence.get('speaker', '')
                        speaker_label = f"[{speaker}] "
                    output.append(f"{speaker_label}[{begin:.2f}s - {end:.2f}s] {sent_text}")
            return "\n".join(output)

        # 完整格式（保留原有逻辑，供 API 返回等使用）
        file_url = transcription_data.get('file_url', '')
        output.append(f"音频文件: {file_url}")
        output.append("")

        properties = transcription_data.get('properties', {})
        duration = properties.get('original_duration_in_milliseconds', 0) / 1000
        output.append(f"时长: {duration:.2f} 秒")

        diarization = properties.get('diarization', {})
        if diarization and include_speaker:
            speaker_count = diarization.get('speaker_count', 0)
            if speaker_count > 0:
                output.append(f"说话人数量: {speaker_count}")

        output.append("")
        output.append("=" * 60)
        output.append("转写结果:")
        output.append("=" * 60)

        for transcript in transcription_data.get('transcripts', []):
            text = transcript.get('text', '')
            output.append(text)
            output.append("")

            if include_timestamps:
                output.append("句子详情:")
                for sentence in transcript.get('sentences', []):
                    begin = sentence.get('begin_time', 0) / 1000
                    end = sentence.get('end_time', 0) / 1000
                    sent_text = sentence.get('text', '')
                    speaker_label = ""
                    if include_speaker and 'speaker' in sentence:
                        speaker = sentence.get('speaker', '')
                        speaker_label = f"[{speaker}] "
                    output.append(f"  {speaker_label}[{begin:.2f}s - {end:.2f}s] {sent_text}")
                    if include_words:
                        output.append("    词级别:")
                        for word in sentence.get('words', []):
                            word_begin = word.get('begin_time', 0) / 1000
                            word_end = word.get('end_time', 0) / 1000
                            word_text = word.get('text', '')
                            punct = word.get('punctuation', '')
                            output.append(f"      [{word_begin:.2f}s - {word_end:.2f}s] {word_text}{punct}")
                output.append("")

        return "\n".join(output)
