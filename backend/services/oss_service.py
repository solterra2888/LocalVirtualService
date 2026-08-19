# -*- coding: utf-8 -*-
"""
阿里云 OSS 服务
用于上传文件到 OSS 并生成公网访问 URL
支持音频、文档、图片等多种文件类型
"""

import os
import re
import uuid
import oss2
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
from backend.utils.logger import info, error, warning
from backend.utils.env_loader import load_project_env
from backend.utils.datetime_utils import utcnow

# 确保加载项目环境变量
load_project_env()


class OSSService:
    """阿里云 OSS 服务类 - 支持多种文件类型上传"""

    _instance: Optional["OSSService"] = None
    _startup_logged = False

    def __new__(cls):
        if cls._instance is None:
            inst = super().__new__(cls)
            inst._initialized = False
            cls._instance = inst
        return cls._instance

    # 支持的音频格式（根据 Fun-ASR 要求）
    SUPPORTED_AUDIO_FORMATS = {
        '.aac', '.amr', '.avi', '.flac', '.flv', '.m4a', '.mkv', 
        '.mov', '.mp3', '.mp4', '.mpeg', '.ogg', '.opus', '.wav', 
        '.webm', '.wma', '.wmv'
    }
    
    # 支持的文档格式
    SUPPORTED_DOCUMENT_FORMATS = {
        '.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx',
        '.txt', '.csv', '.md', '.rtf'
    }
    
    # 支持的图片格式
    SUPPORTED_IMAGE_FORMATS = {
        '.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp', '.svg', '.ico'
    }
    
    # 最大文件大小：2GB
    MAX_FILE_SIZE = 2 * 1024 * 1024 * 1024
    
    def __init__(self):
        """初始化 OSS 服务（进程内单例，仅首次构造时连接并打日志）"""
        if getattr(self, "_initialized", False):
            return
        self._initialized = True

        # 从环境变量读取配置
        self.access_key_id = os.getenv('OSS_ACCESS_KEY_ID')
        self.access_key_secret = os.getenv('OSS_ACCESS_KEY_SECRET')
        self.bucket_name = os.getenv('OSS_BUCKET_NAME', 'rc-audio-files')
        self.endpoint = os.getenv('OSS_ENDPOINT', 'oss-cn-shenzhen.aliyuncs.com')
        
        # 各类文件的 OSS 前缀
        self.audio_prefix = os.getenv('OSS_AUDIO_PREFIX', 'audio/')
        self.document_prefix = os.getenv('OSS_DOCUMENT_PREFIX', 'documents/')
        self.image_prefix = os.getenv('OSS_IMAGE_PREFIX', 'images/')
        self.web_clip_prefix = os.getenv('OSS_WEB_CLIP_PREFIX', 'web-clips/')
        self.temp_prefix = os.getenv('OSS_TEMP_PREFIX', 'temp/')
        
        # 自定义域名（可选，用于在浏览器内预览 PDF）
        self.custom_domain = os.getenv('OSS_CUSTOM_DOMAIN', '').strip()

        # 验证配置
        if not self.access_key_id or not self.access_key_secret:
            self.bucket = None
            self.internal_bucket = None
            if not OSSService._startup_logged:
                warning("OSS 配置不完整，请在 .env 中设置 OSS_ACCESS_KEY_ID 和 OSS_ACCESS_KEY_SECRET")
                OSSService._startup_logged = True
        else:
            try:
                auth = oss2.Auth(self.access_key_id, self.access_key_secret)
                self.bucket = oss2.Bucket(auth, self.endpoint, self.bucket_name)
                self.internal_bucket = None
                internal_ep = os.getenv("OSS_INTERNAL_ENDPOINT", "").strip()
                if (
                    not internal_ep
                    and self.endpoint
                    and "internal" not in self.endpoint
                    and self.endpoint.endswith(".aliyuncs.com")
                ):
                    internal_ep = self.endpoint.replace(
                        ".aliyuncs.com", "-internal.aliyuncs.com"
                    )
                if internal_ep and internal_ep != self.endpoint:
                    try:
                        self.internal_bucket = oss2.Bucket(
                            auth, internal_ep, self.bucket_name
                        )
                    except Exception as ie:
                        warning(f"OSS 内网 endpoint 初始化失败，回落公网: {ie}")
                        self.internal_bucket = None
                if not OSSService._startup_logged:
                    if self.custom_domain:
                        info(f"OSS 自定义域名已配置: {self.custom_domain}")
                    info(
                        f"OSS 服务初始化成功: bucket={self.bucket_name}, "
                        f"endpoint={self.endpoint}"
                        + (
                            f", internal={internal_ep}"
                            if self.internal_bucket is not None
                            else ""
                        )
                    )
                    OSSService._startup_logged = True
            except Exception as e:
                self.bucket = None
                self.internal_bucket = None
                if not OSSService._startup_logged:
                    error(f"OSS 服务初始化失败: {str(e)}")
                    OSSService._startup_logged = True
    
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
    
    def is_document_file(self, filename: str) -> bool:
        """
        检查文件是否为支持的文档格式
        
        Args:
            filename: 文件名
            
        Returns:
            bool: 是否为支持的文档格式
        """
        ext = os.path.splitext(filename)[1].lower()
        return ext in self.SUPPORTED_DOCUMENT_FORMATS
    
    def is_image_file(self, filename: str) -> bool:
        """
        检查文件是否为支持的图片格式
        
        Args:
            filename: 文件名
            
        Returns:
            bool: 是否为支持的图片格式
        """
        ext = os.path.splitext(filename)[1].lower()
        return ext in self.SUPPORTED_IMAGE_FORMATS
    
    def get_file_category(self, filename: str) -> str:
        """
        根据文件名判断文件类别
        
        Args:
            filename: 文件名
            
        Returns:
            str: 'audio', 'document', 'image', 或 'other'
        """
        if self.is_audio_file(filename):
            return 'audio'
        elif self.is_document_file(filename):
            return 'document'
        elif self.is_image_file(filename):
            return 'image'
        else:
            return 'other'
    
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
    
    def _sanitize_filename(self, filename: str) -> str:
        """
        清理文件名，移除特殊字符
        
        Args:
            filename: 原始文件名
            
        Returns:
            str: 清理后的文件名
        """
        # 保留中文、英文、数字、下划线、连字符、点
        name, ext = os.path.splitext(filename)
        # 替换空格为下划线
        name = name.replace(' ', '_')
        # 移除其他特殊字符（保留中文）
        name = re.sub(r'[^\w\u4e00-\u9fff\-]', '', name)
        return f"{name}{ext}"
    
    def _get_content_type(self, filename: str) -> str:
        """
        根据文件扩展名获取 MIME 类型
        
        Args:
            filename: 文件名
            
        Returns:
            str: MIME 类型
        """
        ext = os.path.splitext(filename)[1].lower()
        mime_types = {
            # 文档
            '.pdf': 'application/pdf',
            '.doc': 'application/msword',
            '.docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
            '.xls': 'application/vnd.ms-excel',
            '.xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            '.ppt': 'application/vnd.ms-powerpoint',
            '.pptx': 'application/vnd.openxmlformats-officedocument.presentationml.presentation',
            '.txt': 'text/plain',
            '.csv': 'text/csv',
            '.md': 'text/markdown',
            '.rtf': 'application/rtf',
            # 图片
            '.jpg': 'image/jpeg',
            '.jpeg': 'image/jpeg',
            '.png': 'image/png',
            '.gif': 'image/gif',
            '.bmp': 'image/bmp',
            '.webp': 'image/webp',
            '.svg': 'image/svg+xml',
            '.ico': 'image/x-icon',
            # 音频
            '.mp3': 'audio/mpeg',
            '.wav': 'audio/wav',
            '.m4a': 'audio/mp4',
            '.aac': 'audio/aac',
            '.ogg': 'audio/ogg',
            '.flac': 'audio/flac',
        }
        return mime_types.get(ext, 'application/octet-stream')
    
    def generate_object_name(
        self, 
        filename: str, 
        username: str = "anonymous",
        file_category: str = None
    ) -> str:
        """
        生成 OSS 对象名称（兼容旧版 audio 调用）
        
        Args:
            filename: 原始文件名
            username: 用户名
            file_category: 文件类别（可选，自动检测）
            
        Returns:
            str: OSS 对象名称
        """
        # 如果未指定类别，自动检测
        if file_category is None:
            file_category = self.get_file_category(filename)
        
        # 获取对应前缀
        prefix_map = {
            'audio': self.audio_prefix,
            'document': self.document_prefix,
            'image': self.image_prefix,
            'web_clip': self.web_clip_prefix,
            'other': self.temp_prefix
        }
        prefix = prefix_map.get(file_category, self.audio_prefix)
        
        # 清理文件名
        safe_filename = self._sanitize_filename(filename)
        
        # 生成时间戳和唯一 ID
        date_str = datetime.now().strftime("%Y-%m-%d")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        
        # 生成对象名: prefix/username/date/uniqueid_timestamp_filename
        object_name = f"{prefix}{username}/{date_str}/{unique_id}_{timestamp}_{safe_filename}"
        
        return object_name
    
    def upload_local_file_fixed_key(
        self,
        file_path: str,
        object_name: str,
        filename: str,
        content_type: str = None,
        max_retries: int = 3,
    ) -> Dict[str, Any]:
        """
        上传本地文件到【固定】object_name（PUT 幂等，重传安全）。

        用于知识星球附件等需要确定性 OSS key 的场景：相同 file_id 多次上传到同一 key，
        覆盖即可，不产生孤儿对象。

        与 upload_file 的区别：
          - object_name 由调用方指定（确定性），而非 generate_object_name（含 uuid/时间戳）
          - 大文件自动走 oss2.resumable_upload 分片续传（断点可恢复）
          - 对 OssError 做有限退避重试（满足 OSS 重连/自动重试要求）
          - 不返回签名 URL（按需走 get_signed_url / 下载代理端点即时签）

        Returns:
            {success, object_name, bucket, file_size, error}
        """
        import time as _time

        result = {
            "success": False,
            "object_name": object_name,
            "bucket": self.bucket_name,
            "file_size": 0,
            "error": None,
        }

        if not self.is_configured():
            result["error"] = "OSS 服务未配置，请在 .env 中设置 OSS_ACCESS_KEY_ID/SECRET"
            return result
        if not os.path.exists(file_path):
            result["error"] = f"文件不存在: {file_path}"
            return result

        file_size = os.path.getsize(file_path)
        result["file_size"] = file_size
        if file_size > self.MAX_FILE_SIZE:
            result["error"] = f"文件过大: {file_size / (1024 ** 3):.2f}GB，最大 2GB"
            return result

        if not content_type:
            content_type = self._get_content_type(filename)

        # Content-Disposition：PDF/图片 inline（可预览），其余 attachment（下载，中文文件名编码）
        file_ext = os.path.splitext(filename)[1].lower()
        if file_ext in ['.pdf', '.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp', '.svg']:
            content_disposition = 'inline'
        else:
            from urllib.parse import quote
            content_disposition = f"attachment; filename*=UTF-8''{quote(filename)}"

        headers = {
            'Content-Type': content_type,
            'Content-Disposition': content_disposition,
        }

        last_err = None
        buckets = []
        if getattr(self, "internal_bucket", None) is not None:
            buckets.append(("internal", self.internal_bucket))
        buckets.append(("public", self.bucket))

        for attempt in range(max_retries):
            last_oss_err = None
            uploaded = False
            for bucket_label, bucket in buckets:
                try:
                    if file_size >= 10 * 1024 * 1024:
                        oss2.resumable_upload(
                            bucket, object_name, file_path,
                            store=oss2.ResumableStore(),
                            headers=headers,
                            num_threads=4,
                        )
                    else:
                        with open(file_path, 'rb') as f:
                            bucket.put_object(object_name, f, headers=headers)
                    uploaded = True
                    info(
                        f"OSS 固定 key 上传成功({bucket_label}): "
                        f"{filename} -> {object_name} ({file_size} bytes)"
                    )
                    break
                except Exception as e:
                    last_oss_err = e
                    warning(
                        f"OSS 上传失败 endpoint={bucket_label} "
                        f"object={object_name}: {str(e)[:200]}"
                    )
            if uploaded:
                result["success"] = True
                return result

            last_err = f"exception: {str(last_oss_err)[:200]}" if last_oss_err else "OSS 上传失败"
            if isinstance(last_oss_err, oss2.exceptions.OssError):
                last_err = (
                    f"oss_error status={getattr(last_oss_err, 'status', '?')} "
                    f"code={getattr(last_oss_err, 'code', '?')}"
                )
            warning(f"OSS 上传失败（第 {attempt + 1}/{max_retries}）object={object_name}: {last_err}")

            if attempt < max_retries - 1:
                _time.sleep(2 * (attempt + 1))

        result["error"] = last_err or "OSS 上传失败"
        error(f"OSS 固定 key 上传最终失败 object={object_name}: {result['error']}")
        return result

    def upload_file(
        self,
        file_path: str,
        filename: str,
        username: str = "anonymous",
        file_category: str = None,
        content_type: str = None,
        expires: int = 3600,
        asr_public_read: bool = False
    ) -> Dict[str, Any]:
        """
        通用文件上传到 OSS（支持文档、图片、音频等）
        
        Args:
            file_path: 本地文件路径
            filename: 原始文件名
            username: 用户名
            file_category: 文件类别（'document', 'image', 'audio', 'other'），不指定则自动检测
            content_type: MIME 类型（可选，自动检测）
            expires: 签名 URL 过期时间（秒），默认 1 小时
            asr_public_read: 若为 True，上传后将对象 ACL 设为 public-read，便于 Fun-ASR 等通过公网 URL 下载（仅建议用于私有桶+ASR 场景）
            
        Returns:
            Dict[str, Any]: 上传结果
                {
                    "success": True/False,
                    "object_name": "documents/user/2026-02-11/xxx.pdf",
                    "public_url": "https://bucket.oss-cn-shenzhen.aliyuncs.com/...",
                    "signed_url": "https://...?Expires=...",
                    "bucket": "rc-files-bucket",
                    "file_size": 12345,
                    "file_category": "document",
                    "upload_time": "2026-02-11T10:30:00",
                    "error": ""  # 如果失败
                }
        """
        result = {
            "success": False,
            "object_name": None,
            "public_url": None,
            "signed_url": None,
            "bucket": self.bucket_name,
            "file_size": 0,
            "file_category": file_category,
            "upload_time": None,
            "error": None
        }
        
        try:
            # 检查 OSS 是否配置
            if not self.is_configured():
                result["error"] = "OSS 服务未配置，请在 .env 中设置 OSS_ACCESS_KEY_ID 和 OSS_ACCESS_KEY_SECRET"
                return result
            
            # 检查文件是否存在
            if not os.path.exists(file_path):
                result["error"] = f"文件不存在: {file_path}"
                return result
            
            # 获取文件大小
            file_size = os.path.getsize(file_path)
            result["file_size"] = file_size
            
            # 检查文件大小
            if file_size > self.MAX_FILE_SIZE:
                size_gb = file_size / (1024 * 1024 * 1024)
                result["error"] = f"文件过大: {size_gb:.2f}GB。最大支持 2GB"
                return result
            
            # 自动检测文件类别
            if file_category is None:
                file_category = self.get_file_category(filename)
                result["file_category"] = file_category
            
            # 生成 OSS 对象名称
            object_name = self.generate_object_name(filename, username, file_category)
            result["object_name"] = object_name
            
            # 自动检测 Content-Type
            if not content_type:
                content_type = self._get_content_type(filename)
            
            # 设置 Content-Disposition（决定浏览器是预览还是下载）
            # PDF 和图片：inline（浏览器内预览）
            # 其他文件：attachment（下载）
            file_ext = os.path.splitext(filename)[1].lower()
            if file_ext in ['.pdf', '.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp', '.svg']:
                # 可预览文件：inline
                content_disposition = 'inline'
            else:
                # 其他文件：attachment（下载）
                # 对文件名进行 URL 编码以支持中文
                from urllib.parse import quote
                encoded_filename = quote(filename)
                content_disposition = f'attachment; filename*=UTF-8\'\'{encoded_filename}'
            
            # 上传到 OSS
            info(f"开始上传文件到 OSS: {filename} -> {object_name} (category: {file_category}, disposition: {content_disposition.split(';')[0]})")
            
            with open(file_path, 'rb') as f:
                self.bucket.put_object(
                    object_name,
                    f,
                    headers={
                        'Content-Type': content_type,
                        'Content-Disposition': content_disposition
                    }
                )
            
            # 若为 ASR 场景且希望 Fun-ASR 通过公网 URL 拉取（私有桶时签名 URL 可能不被支持）
            if asr_public_read:
                try:
                    self.bucket.put_object_acl(object_name, oss2.OBJECT_ACL_PUBLIC_READ)
                    info(f"已设置对象 ACL 为 public-read（供 Fun-ASR 拉取）: {object_name}")
                except Exception as e:
                    warning(f"设置对象 public-read 失败: {e}，ASR 可能无法下载该文件")
            
            # 生成公网 URL
            if self.custom_domain:
                # 使用自定义域名（不会触发强制下载，可以预览）
                # 注意：如果域名未配置 SSL 证书，使用 HTTP；否则使用 HTTPS
                from urllib.parse import quote
                encoded_object_name = quote(object_name, safe='/')
                public_url = f"http://{self.custom_domain}/{encoded_object_name}"
            else:
                # 使用默认域名（会触发强制下载）
                public_url = f"https://{self.bucket_name}.{self.endpoint}/{object_name}"
            
            result["public_url"] = public_url
            
            # 生成签名 URL（私有桶或需要鉴权访问）
            # 对于公共桶，签名 URL 可选
            signed_url = self.get_signed_url(object_name, expires)
            result["signed_url"] = signed_url
            
            result["success"] = True
            result["upload_time"] = utcnow().isoformat()
            
            info(f"文件已上传到 OSS: {filename} (size: {file_size / 1024 / 1024:.2f}MB)")
            
        except oss2.exceptions.OssError as e:
            # Python 3 使用 str(e)；e.message 已废弃且可能为空
            detail = getattr(e, "message", None) or str(e) or getattr(e, "details", "") or "未知 OSS 错误"
            error_msg = f"OSS 上传失败: {detail}"
            error(error_msg)
            result["error"] = error_msg
        except Exception as e:
            error_msg = f"上传文件失败: {str(e)}"
            error(error_msg)
            result["error"] = error_msg
        
        return result
    
    def upload_audio_file(
        self, 
        file_path: str, 
        filename: str,
        username: str = "anonymous"
    ) -> Dict[str, Any]:
        """
        上传音频文件到 OSS（兼容旧版接口，内部调用 upload_file）
        
        Args:
            file_path: 本地文件路径
            filename: 原始文件名
            username: 用户名
            
        Returns:
            Dict[str, Any]: 上传结果（与 upload_file 格式相同）
        """
        # 验证音频文件
        if not os.path.exists(file_path):
            return {
                "success": False,
                "error": f"文件不存在: {file_path}"
            }
        
        file_size = os.path.getsize(file_path)
        is_valid, error_msg = self.validate_audio_file(file_path, file_size)
        if not is_valid:
            return {
                "success": False,
                "error": error_msg
            }
        
        # 使用通用上传方法，48 小时有效期（用于 ASR）
        # 若 OSS 为私有桶且 Fun-ASR 报 FILE_DOWNLOAD_FAILED，可设置 OSS_ASR_PUBLIC_READ=true 使上传的音频对象临时公网可读
        asr_public_read = os.getenv('OSS_ASR_PUBLIC_READ', '').strip().lower() in ('1', 'true', 'yes')
        return self.upload_file(
            file_path=file_path,
            filename=filename,
            username=username,
            file_category='audio',
            expires=3600 * 48,  # 48 小时
            asr_public_read=asr_public_read
        )
    
    def upload_bytes(
        self,
        object_name: str,
        data: bytes,
        content_type: str = 'application/octet-stream',
        content_disposition: str = 'inline'
    ) -> Dict[str, Any]:
        """
        直接将字节内容上传到 OSS（无需本地文件），适用于 Web Clip 等场景
        
        Args:
            object_name: OSS 对象名称
            data: 字节内容
            content_type: MIME 类型
            content_disposition: inline 或 attachment
            
        Returns:
            Dict[str, Any]: 上传结果
        """
        result = {
            "success": False,
            "object_name": object_name,
            "bucket": self.bucket_name,
            "file_size": len(data),
            "error": None
        }
        
        try:
            if not self.is_configured():
                result["error"] = "OSS 服务未配置"
                return result
            
            self.bucket.put_object(
                object_name,
                data,
                headers={
                    'Content-Type': content_type,
                    'Content-Disposition': content_disposition
                }
            )
            
            result["success"] = True
            result["upload_time"] = utcnow().isoformat()
            info(f"字节内容已上传到 OSS: {object_name} (size: {len(data) / 1024:.1f}KB)")
            
        except Exception as e:
            error_msg = f"OSS 字节上传失败: {str(e)}"
            error(error_msg)
            result["error"] = error_msg
        
        return result

    def get_signed_url(
        self, 
        object_name: str, 
        expires: int = 3600,
        force_inline: bool = None
    ) -> Optional[str]:
        """
        获取签名 URL（临时访问链接）- 推荐用于私有桶
        
        Args:
            object_name: OSS 对象名称
            expires: 过期时间（秒），默认 1 小时
            force_inline: 是否强制浏览器内预览（True: inline, False: attachment, None: 自动检测）
            
        Returns:
            Optional[str]: 签名 URL，失败返回 None
        """
        try:
            if not self.is_configured():
                warning("OSS 未配置，无法生成签名 URL")
                return None
            
            # 自动检测文件类型，决定是否使用 inline
            if force_inline is None:
                file_ext = os.path.splitext(object_name)[1].lower()
                # PDF 和图片自动 inline，其他文件 attachment
                force_inline = file_ext in ['.pdf', '.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp', '.svg', '.html', '.htm']
            
            # 如果需要强制 inline（浏览器预览），设置响应头
            if force_inline:
                # 注意：OSS 不允许覆盖 Content-Type，只能覆盖 Content-Disposition
                # 设置响应参数（对抗 OSS 默认域名的强制下载）
                params = {
                    'response-content-disposition': 'inline'
                }
                url = self.bucket.sign_url('GET', object_name, expires, params=params)
            else:
                url = self.bucket.sign_url('GET', object_name, expires)
            
            return url
        except Exception as e:
            error(f"生成签名 URL 失败: {str(e)}")
            return None
    
    def generate_signed_url(
        self, 
        object_name: str, 
        expires: int = 3600
    ) -> Optional[str]:
        """
        生成签名 URL（兼容旧版方法名）
        
        Args:
            object_name: OSS 对象名称
            expires: 过期时间（秒），默认1小时
            
        Returns:
            Optional[str]: 签名 URL，失败返回 None
        """
        return self.get_signed_url(object_name, expires)
    
    def delete_file(self, object_name: str) -> bool:
        """
        删除 OSS 中的文件（通用方法）
        
        Args:
            object_name: OSS 对象名称
            
        Returns:
            bool: 是否删除成功
        """
        try:
            if not self.is_configured():
                warning("OSS 未配置，无法删除文件")
                return False
            
            self.bucket.delete_object(object_name)
            info(f"已删除 OSS 文件: {object_name}")
            return True
        except Exception as e:
            error(f"删除 OSS 文件失败: {str(e)}")
            return False
    
    def delete_audio_file(self, object_name: str) -> bool:
        """
        删除 OSS 中的音频文件（兼容旧版接口）
        
        Args:
            object_name: OSS 对象名称
            
        Returns:
            bool: 是否删除成功
        """
        return self.delete_file(object_name)
    
    def get_file_info(self, object_name: str) -> Optional[Dict[str, Any]]:
        """
        获取文件信息（通用方法）
        
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
            error(f"获取文件信息失败: {str(e)}")
            return None
    
    def get_audio_info(self, object_name: str) -> Optional[Dict[str, Any]]:
        """
        获取音频文件信息（兼容旧版接口）
        
        Args:
            object_name: OSS 对象名称
            
        Returns:
            Optional[Dict[str, Any]]: 文件信息，失败返回 None
        """
        return self.get_file_info(object_name)
    
    def get_object_bytes(self, object_name: str) -> Optional[bytes]:
        """
        Read an OSS object directly into memory and return its raw bytes.
        Returns None on error (object not found, OSS not configured, etc.).
        """
        try:
            if not self.is_configured():
                warning("OSS 未配置，无法读取对象")
                return None
            result = self.bucket.get_object(object_name)
            return result.read()
        except Exception as e:
            error(f"OSS 读取对象失败 ({object_name}): {str(e)}")
            return None

    def download_to_file(self, object_name: str, local_path: str) -> bool:
        """
        从 OSS 下载文件到本地（用于 LlamaParse 等需要本地文件的场景）
        
        Args:
            object_name: OSS 对象名称
            local_path: 本地保存路径
            
        Returns:
            bool: 是否下载成功
        """
        try:
            if not self.is_configured():
                warning("OSS 未配置，无法下载文件")
                return False
            
            # 确保目标目录存在
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            
            self.bucket.get_object_to_file(object_name, local_path)
            info(f"已从 OSS 下载文件: {object_name} -> {local_path}")
            return True
        except Exception as e:
            error(f"从 OSS 下载文件失败: {str(e)}")
            return False
