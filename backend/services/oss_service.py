# -*- coding: utf-8 -*-
"""
阿里云 OSS 服务
用于上传音频文件到 OSS 并生成公网访问 URL
"""

import os
import oss2
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
from backend.utils.logger import info, error, warning
from backend.utils.env_loader import load_project_env

# 确保加载项目环境变量
load_project_env()


class OSSService:
    """阿里云 OSS 服务类"""
    
    # 支持的音频格式（根据 Fun-ASR 要求）
    SUPPORTED_AUDIO_FORMATS = {
        '.aac', '.amr', '.avi', '.flac', '.flv', '.m4a', '.mkv', 
        '.mov', '.mp3', '.mp4', '.mpeg', '.ogg', '.opus', '.wav', 
        '.webm', '.wma', '.wmv'
    }
    
    # 最大文件大小：2GB
    MAX_FILE_SIZE = 2 * 1024 * 1024 * 1024
    
    def __init__(self):
        """初始化 OSS 服务"""
        # 从环境变量读取配置
        self.access_key_id = os.getenv('OSS_ACCESS_KEY_ID')
        self.access_key_secret = os.getenv('OSS_ACCESS_KEY_SECRET')
        self.bucket_name = os.getenv('OSS_BUCKET_NAME', 'rc-audio-files')
        self.endpoint = os.getenv('OSS_ENDPOINT', 'oss-cn-shenzhen.aliyuncs.com')
        self.audio_prefix = os.getenv('OSS_AUDIO_PREFIX', 'audio/')
        
        # 验证配置
        if not self.access_key_id or not self.access_key_secret:
            warning("OSS 配置不完整，请在 .env 中设置 OSS_ACCESS_KEY_ID 和 OSS_ACCESS_KEY_SECRET")
            self.bucket = None
        else:
            try:
                # 创建 Auth 对象
                auth = oss2.Auth(self.access_key_id, self.access_key_secret)
                # 创建 Bucket 对象
                self.bucket = oss2.Bucket(auth, self.endpoint, self.bucket_name)
                info(f"OSS 服务初始化成功: bucket={self.bucket_name}, endpoint={self.endpoint}")
            except Exception as e:
                error(f"OSS 服务初始化失败: {str(e)}")
                self.bucket = None
    
    def is_configured(self) -> bool:
        """检查 OSS 是否已正确配置"""
        return self.bucket is not None
    
    def is_audio_file(self, filename: str) -> bool:
        """
        检查文件是否为支持的音频格式
        
        Args:
            filename: 文件名
            
        Returns:
            bool: 是否为支持的音频格式
        """
        ext = os.path.splitext(filename)[1].lower()
        return ext in self.SUPPORTED_AUDIO_FORMATS
    
    def validate_audio_file(self, file_path: str, file_size: int) -> tuple[bool, str]:
        """
        验证音频文件是否符合要求
        
        Args:
            file_path: 文件路径
            file_size: 文件大小（字节）
            
        Returns:
            tuple[bool, str]: (是否有效, 错误信息)
        """
        # 检查文件格式
        if not self.is_audio_file(file_path):
            ext = os.path.splitext(file_path)[1]
            return False, f"不支持的音频格式: {ext}。支持的格式: {', '.join(sorted(self.SUPPORTED_AUDIO_FORMATS))}"
        
        # 检查文件大小
        if file_size > self.MAX_FILE_SIZE:
            size_gb = file_size / (1024 * 1024 * 1024)
            return False, f"音频文件过大: {size_gb:.2f}GB。最大支持 2GB"
        
        return True, ""
    
    def generate_object_name(self, filename: str, username: str = "anonymous") -> str:
        """
        生成 OSS 对象名称
        
        Args:
            filename: 原始文件名
            username: 用户名
            
        Returns:
            str: OSS 对象名称
        """
        # 获取文件扩展名
        _, ext = os.path.splitext(filename)
        
        # 生成时间戳
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # 生成对象名: audio/{username}/{timestamp}_{filename}
        object_name = f"{self.audio_prefix}{username}/{timestamp}_{filename}"
        
        return object_name
    
    def upload_audio_file(
        self, 
        file_path: str, 
        filename: str,
        username: str = "anonymous"
    ) -> Dict[str, Any]:
        """
        上传音频文件到 OSS
        
        Args:
            file_path: 本地文件路径
            filename: 原始文件名
            username: 用户名
            
        Returns:
            Dict[str, Any]: 上传结果
                {
                    "success": bool,
                    "object_name": str,  # OSS 对象名称
                    "public_url": str,   # 公网访问 URL
                    "file_size": int,    # 文件大小（字节）
                    "error": str         # 错误信息（如果有）
                }
        """
        result = {
            "success": False,
            "object_name": "",
            "public_url": "",
            "file_size": 0,
            "error": ""
        }
        
        try:
            # 检查 OSS 是否配置
            if not self.is_configured():
                result["error"] = "OSS 服务未配置，请在 .env 中设置 OSS 配置"
                return result
            
            # 检查文件是否存在
            if not os.path.exists(file_path):
                result["error"] = f"文件不存在: {file_path}"
                return result
            
            # 获取文件大小
            file_size = os.path.getsize(file_path)
            result["file_size"] = file_size
            
            # 验证音频文件
            is_valid, error_msg = self.validate_audio_file(file_path, file_size)
            if not is_valid:
                result["error"] = error_msg
                return result
            
            # 生成 OSS 对象名称
            object_name = self.generate_object_name(filename, username)
            result["object_name"] = object_name
            
            # 上传文件
            info(f"开始上传音频文件到 OSS: {filename} -> {object_name}")
            self.bucket.put_object_from_file(object_name, file_path)
            
            # 生成签名 URL（临时访问，更安全，不需要 bucket 为公共读）
            # 设置 48 小时有效期（足够长时间用于 ASR 识别）
            expires = 3600 * 48  # 48 小时 = 172800 秒
            signed_url = self.bucket.sign_url('GET', object_name, expires)
            result["public_url"] = signed_url
            
            # 方式2（备用）：使用 bucket 自己的域名（需要 bucket 设置为公共读）
            # public_url = f"https://{self.bucket_name}.{self.endpoint}/{object_name}"
            # result["public_url"] = public_url
            
            result["success"] = True
            info(f"audio file uploaded successfully: {filename}, signed URL valid for 48 hours")
            
        except oss2.exceptions.OssError as e:
            error_msg = f"OSS upload failed: {e.message}"
            error(error_msg)
            result["error"] = error_msg
        except Exception as e:
            error_msg = f"upload audio file failed: {str(e)}"
            error(error_msg)
            result["error"] = error_msg
        
        return result
    
    def generate_signed_url(
        self, 
        object_name: str, 
        expires: int = 3600
    ) -> Optional[str]:
        """
        生成签名 URL（临时访问链接）
        
        Args:
            object_name: OSS 对象名称
            expires: 过期时间（秒），默认1小时
            
        Returns:
            Optional[str]: 签名 URL，失败返回 None
        """
        try:
            if not self.is_configured():
                return None
            
            url = self.bucket.sign_url('GET', object_name, expires)
            return url
        except Exception as e:
            error(f"generate signed URL failed: {str(e)}")
            return None
    
    def delete_audio_file(self, object_name: str) -> bool:
        """
        删除 OSS 中的音频文件
        
        Args:
            object_name: OSS 对象名称
            
        Returns:
            bool: 是否删除成功
        """
        try:
            if not self.is_configured():
                return False
            
            self.bucket.delete_object(object_name)
            info(f"deleted OSS audio file: {object_name}")
            return True
        except Exception as e:
            error(f"delete OSS audio file failed: {str(e)}")
            return False
    
    def get_audio_info(self, object_name: str) -> Optional[Dict[str, Any]]:
        """
        获取音频文件信息
        
        Args:
            object_name: OSS 对象名称
            
        Returns:
            Optional[Dict[str, Any]]: 文件信息，失败返回 None
        """
        try:
            if not self.is_configured():
                return None
            
            # 获取对象元数据
            metadata = self.bucket.get_object_meta(object_name)
            
            return {
                "size": int(metadata.headers.get('Content-Length', 0)),
                "content_type": metadata.headers.get('Content-Type', ''),
                "last_modified": metadata.headers.get('Last-Modified', ''),
                "etag": metadata.headers.get('ETag', '')
            }
        except Exception as e:
            error(f"get audio file info failed: {str(e)}")
            return None
