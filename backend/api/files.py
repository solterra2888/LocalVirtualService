# -*- coding: utf-8 -*-
"""
文件处理API - 使用Celery异步任务队列
"""

import os
import uuid
import json
import time
from datetime import datetime
from backend.utils.datetime_utils import utcnow
from typing import List, Dict, Any, Optional, Literal
from pydantic import BaseModel, Field
from fastapi import APIRouter, File, UploadFile, HTTPException, status, Depends, Body, Form, Header
from fastapi.responses import FileResponse, StreamingResponse

from backend.schemas.file import (
    FileUploadResponse,
    FileProcessRequest,
    FileProcessResponse,
    FileDetailResponse,
    FileType,
    ProcessingParamsRequest,
    WebClipRequest,
    WebLinkRequest
)
from backend.schemas.file_tag import (
    FileTagCreate,
    FileTagUpdate,
    FileTagResponse,
    FileTagBatchCreate,
    FileTagStats,
    FileWithTag
)
from backend.auth.jwt_utils import get_current_user
from backend.processors.file_processor import FileProcessor
from backend.database.db_manager import DatabaseManager
from backend.utils.logger import info, error, warning, debug
from backend.utils.database_config import get_database_config
from backend.services.llm_prompt_template_service import LLMPromptTemplateService
from backend.services.oss_service import OSSService
from backend.services.asr_service import ASRService
from backend.services.youtube_service import YouTubeService

# Celery相关导入
from celery.result import AsyncResult
from backend.celery_app import app as celery_app
from backend.tasks.file_tasks import (
    process_files_task, 
    analyze_audio_task, 
    analyze_document_task,
    upload_and_transcribe_audio_task,
    upload_and_process_document_task,
    process_web_clip_task,
    trigger_auto_tagging,
    trigger_auto_naming,
    trigger_auto_classify,
    trigger_auto_analyze,
)

# 统一的环境变量加载
from backend.utils.env_loader import load_project_env

router = APIRouter()


def _strip_llamaindex_key_from_client_params(params: Any) -> None:
    """LlamaIndex Cloud key must come from server env (LLAMA_CLOUD_API_KEY), not the client."""
    if isinstance(params, dict):
        params.pop('llamaindex_api_key', None)

# 上传文件目录
UPLOAD_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "uploads")
os.makedirs(UPLOAD_DIR, exist_ok=True)


def _extract_filename_from_audio_url(audio_url: str) -> str:
    """
    从 audio_url 中提取原始文件名
    
    处理签名 URL 的情况：
    - 移除查询参数（?OSSAccessKeyId=...）
    - URL 解码（%E9%A6%99... → 香）
    - 提取文件名部分
    
    Args:
        audio_url: 音频文件 URL（可能包含查询参数）
    
    Returns:
        原始文件名
    
    Examples:
        - 输入: "https://bucket.oss.com/audio%2Fadmin%2F20260207_165504_%E9%A6%99.aac?OSSAccessKeyId=..."
        - 输出: "20260207_165504_香.aac"
    """
    from urllib.parse import unquote, urlparse
    
    try:
        # 移除查询参数
        url_without_params = audio_url.split('?')[0]
        
        # URL 解码
        decoded_url = unquote(url_without_params)
        
        # 提取最后的文件名部分
        filename = decoded_url.split('/')[-1]
        
        return filename
    except Exception as e:
        # 如果提取失败，返回 URL 的最后部分
        warning(f"从 URL 提取文件名失败: {str(e)}", source="api")
        return audio_url.split('/')[-1].split('?')[0]


def _oss_object_key_from_url(url: str) -> Optional[str]:
    """
    若 url 为阿里云 OSS 虚拟主机风格对象地址，返回 URL 路径经解码后的 object key。
    用于兼容旧数据：ASR 任务曾将签名 URL 写入 original_file_path 且未标记 file_storage_type。
    """
    from urllib.parse import urlparse, unquote

    if not url or not (url.startswith("http://") or url.startswith("https://")):
        return None
    try:
        parsed = urlparse(url)
        host = (parsed.netloc or "").lower()
        if "aliyuncs.com" not in host and "aliyuncs.net" not in host:
            return None
        path = unquote(parsed.path or "")
        key = path.lstrip("/")
        return key or None
    except Exception:
        return None


def _oss_virtual_host_bucket_endpoint(url: str) -> Optional[tuple[str, str]]:
    """
    从 OSS 虚拟主机风格 URL 解析 bucket 与 endpoint。
    例: https://guanhetech-hk.oss-cn-hongkong.aliyuncs.com/audio/x.m4a
        -> ("guanhetech-hk", "oss-cn-hongkong.aliyuncs.com")
    兼容模式读取时必须与 URL 一致；仅用环境变量默认 bucket 会在跨桶/迁桶时 NoSuchKey。
    """
    from urllib.parse import urlparse

    if not url or not (url.startswith("http://") or url.startswith("https://")):
        return None
    try:
        parsed = urlparse(url)
        host = (parsed.hostname or "").lower()
        if ".oss-" not in host or "aliyuncs.com" not in host:
            return None
        parts = host.split(".", 1)
        if len(parts) != 2 or not parts[0] or not parts[1].startswith("oss-"):
            return None
        return parts[0], parts[1]
    except Exception:
        return None


def _legacy_oss_buckets_to_try(file_path: str, oss_bucket_db: Optional[str], svc: OSSService):
    """
    旧数据 original_file_path 为完整 OSS URL 时，按优先级返回可尝试的 oss2.Bucket 列表。
    1) URL 主机名对应的 bucket+endpoint（最可信）
    2) 数据库 oss_bucket 字段 + 当前 endpoint
    3) 环境变量默认 bucket
    """
    import oss2

    if not svc.is_configured():
        return []
    ak = os.getenv("OSS_ACCESS_KEY_ID")
    sk = os.getenv("OSS_ACCESS_KEY_SECRET")
    if not ak or not sk:
        return []
    auth = oss2.Auth(ak, sk)
    out = []
    seen = set()

    be = _oss_virtual_host_bucket_endpoint(file_path)
    if be:
        bname, ep = be
        k = (bname, ep)
        if k not in seen:
            seen.add(k)
            out.append(oss2.Bucket(auth, ep, bname))

    dbn = (oss_bucket_db or "").strip()
    if dbn:
        k = (dbn, svc.endpoint)
        if k not in seen:
            seen.add(k)
            out.append(oss2.Bucket(auth, svc.endpoint, dbn))

    k = (svc.bucket_name, svc.endpoint)
    if k not in seen:
        seen.add(k)
        out.append(svc.bucket)

    return out


def _is_audio_like_file_type(file_type: Optional[str]) -> bool:
    """audio 与 podcast（Apple 单集音频）共用播放器、HTTP Range、inline 预览与 MIME 兜底。"""
    return (file_type or "").lower() in ("audio", "podcast")


def _serve_file_bytes_response(content: bytes, filename: str, file_type: Optional[str]):
    """
    将 OSS/本地读取的文件字节返回给浏览器（PDF/图片 inline，其余 attachment）。
    与 get_original_file_info 中「走 serve 代理」的场景一致。
    """
    import mimetypes
    from urllib.parse import quote
    from starlette.responses import Response as RawResponse

    ft = (file_type or "").lower()
    guessed, _ = mimetypes.guess_type(filename or "")
    fallback_types = {
        "pdf": "application/pdf",
        "doc": "application/msword",
        "docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "xls": "application/vnd.ms-excel",
        "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "csv": "text/csv",
        "jpg": "image/jpeg",
        "jpeg": "image/jpeg",
        "png": "image/png",
        "gif": "image/gif",
        "bmp": "image/bmp",
        "image": "image/jpeg",
        # DB 常为 audio；无扩展名时避免 octet-stream，否则部分浏览器 <audio> 解码失败
        "audio": "audio/mpeg",
        "podcast": "audio/mpeg",
        "web": "text/html",
    }
    media_type = guessed or fallback_types.get(ft, "application/octet-stream")
    if _is_audio_like_file_type(file_type) and (not media_type or media_type == "application/octet-stream"):
        media_type = guessed or "audio/mpeg"
    encoded_fn = quote(filename or "file")
    # 音频需 inline + 正确 MIME，否则 <audio> 表现不一致（有的能播、有的触发下载或 MEDIA_ERR）
    is_previewable = ft in ["pdf", "jpg", "jpeg", "png", "gif", "bmp", "image", "audio", "podcast", "web"]
    disposition = "inline" if is_previewable else "attachment"
    return RawResponse(
        content=content,
        media_type=media_type,
        headers={"Content-Disposition": f"{disposition}; filename*=UTF-8''{encoded_fn}"},
    )


def _parse_http_range(range_header: Optional[str], total_size: int) -> Optional[tuple[int, int]]:
    """
    解析 HTTP Range（仅支持 bytes=START-END 单段），返回闭区间 [start, end]。
    无效或不支持时返回 None。
    """
    if not range_header or not range_header.startswith("bytes=") or total_size <= 0:
        return None
    try:
        spec = range_header[len("bytes="):].strip()
        if "," in spec:
            return None  # 不支持多段 range
        start_s, end_s = spec.split("-", 1)
        if start_s == "" and end_s == "":
            return None
        if start_s == "":
            # bytes=-N -> 最后 N 字节
            length = int(end_s)
            if length <= 0:
                return None
            start = max(total_size - length, 0)
            end = total_size - 1
            return (start, end)
        start = int(start_s)
        if start < 0 or start >= total_size:
            return None
        if end_s == "":
            end = total_size - 1
        else:
            end = int(end_s)
            if end < start:
                return None
            end = min(end, total_size - 1)
        return (start, end)
    except Exception:
        return None


def _build_asr_structured_data(transcriptions: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    构建结构化的 ASR 数据，用于前端说话人分离可视化
    
    Args:
        transcriptions: ASR 转写结果列表
    
    Returns:
        结构化的 ASR 数据，包含说话人列表和句子列表
    """
    if not transcriptions or len(transcriptions) == 0:
        return None
    
    # 提取第一个转写结果（通常只有一个）
    transcript = transcriptions[0]
    
    # 获取音频属性
    properties = {}
    if 'properties' in transcript:
        props = transcript['properties']
        properties = {
            'duration_ms': props.get('original_duration_in_milliseconds', 0),
            'audio_format': props.get('audio_format', ''),
            'channels': props.get('channels', []),
            'sampling_rate': props.get('original_sampling_rate', 0)
        }
    
    # 提取句子信息
    sentences = []
    speaker_ids = set()
    
    if 'transcripts' in transcript:
        for trans in transcript['transcripts']:
            for sent in trans.get('sentences', []):
                speaker_id = sent.get('speaker_id')
                if speaker_id is not None:
                    speaker_ids.add(speaker_id)
                
                sentence_data = {
                    'sentence_id': sent.get('sentence_id'),
                    'begin_time': sent.get('begin_time', 0),
                    'end_time': sent.get('end_time', 0),
                    'text': sent.get('text', ''),
                    'speaker_id': speaker_id
                }
                sentences.append(sentence_data)
    
    # 按时间排序
    sentences.sort(key=lambda x: x['begin_time'])
    
    # 构建说话人列表
    speakers = [{'speaker_id': sid, 'label': f'speaker {sid}'} for sid in sorted(speaker_ids)]
    
    # 构建最终数据结构
    structured_data = {
        'properties': properties,
        'speakers': speakers,
        'speaker_count': len(speakers),
        'sentences': sentences,
        'total_sentences': len(sentences)
    }
    
    return structured_data


def _rebuild_audio_content_from_asr_structured(asr_data: Dict[str, Any]) -> str:
    """
    根据结构化 ASR（speakers + sentences）重建 file_processing_details.content，
    与 ASRService.format_transcription_result(timestamped_only=True) 中带说话人标签的格式对齐。
    """
    if not asr_data or "sentences" not in asr_data:
        return ""
    speakers_map = {
        s["speaker_id"]: s.get("label", f"speaker {s['speaker_id']}")
        for s in asr_data.get("speakers") or []
        if s.get("speaker_id") is not None
    }
    ordered = sorted(
        asr_data["sentences"],
        key=lambda x: (x.get("begin_time", 0), x.get("sentence_id") or 0),
    )
    lines = []
    for sent in ordered:
        sid = sent.get("speaker_id")
        label = speakers_map.get(sid, f"speaker {sid}" if sid is not None else "")
        begin = (sent.get("begin_time", 0) or 0) / 1000.0
        end = (sent.get("end_time", 0) or 0) / 1000.0
        text = sent.get("text", "") or ""
        if label:
            lines.append(f"[{label}] [{begin:.2f}s - {end:.2f}s] {text}")
        else:
            lines.append(f"[{begin:.2f}s - {end:.2f}s] {text}")
    return "\n".join(lines)


class UpdateAsrSegmentBody(BaseModel):
    """更新 Original 面板中某条转写文本（音频句子或 YouTube 分段）"""

    text: str = Field(..., description="New transcription text")
    segment_kind: Literal["audio", "youtube"] = Field(..., description="audio=说话人分离句子 / youtube=字幕分段")
    index: int = Field(..., ge=0, description="Index in the interface sorting list starting from 0.")


class DeleteAsrSegmentBody(BaseModel):
    """删除 Original 面板中某条转写（音频句子或 YouTube 分段）"""

    segment_kind: Literal["audio", "youtube"] = Field(..., description="audio=说话人分离句子 / youtube=字幕分段.")
    index: int = Field(..., ge=0, description="在界面排序列表中的从 0 开始的下标.")


class ReplaceAsrBody(BaseModel):
    """批量替换转写文本（在所有句子/分段中把 find 全部替换为 replace.）"""

    find: str = Field(..., min_length=1, description="要查找的文本.")
    replace: str = Field(default="", description="替换成的文本，可为空表示删除查找内容.")


def safe_file_type_conversion(file_type_str: str) -> FileType:
    """
    安全地将字符串转换为FileType枚举.
    """
    try:
        return FileType(file_type_str)
    except ValueError:
        return FileType.OTHER


@router.post("/upload", response_model=List[FileUploadResponse], summary="Upload file (supports OSS)")
async def upload_files(
    files: List[UploadFile] = File(...),
    upload_to_oss: bool = True,  # 新增：是否上传到 OSS（默认开启）.
    current_user=Depends(get_current_user)
):
    """
    上传文件到服务器（支持 OSS 或本地存储）.        
    
    - **files**: 要上传的文件列表（当前版本仅支持单个文件）
    - **upload_to_oss**: 是否上传到 OSS（默认 True，推荐）
        - True: 上传到阿里云 OSS（稳定预览，多实例部署友好）
        - False: 上传到本地 uploads/ 目录（兼容模式）
    
    注意：
    - OSS 上传需要在 .env 中配置 OSS_ACCESS_KEY_ID 等参数
    - 如果 OSS 未配置，会自动回退到本地存储
    """
    if not files:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="please select the files to upload"
        )
    
    # 验证只能上传单个文件
    if len(files) > 1:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="only one file is supported, please select one file at a time"
        )
    
    file_processor = FileProcessor()
    oss_service = OSSService() if upload_to_oss else None
    uploaded_files = []
    failed_files = []
    
    # 检查 OSS 是否配置（如果启用 OSS）
    oss_enabled = False
    if upload_to_oss and oss_service and oss_service.is_configured():
        oss_enabled = True
        info(f"OSS is enabled, files will be uploaded to OSS", source="api", user=current_user.username)
    elif upload_to_oss:
        warning(f"OSS is not configured, files will be uploaded to local storage", source="api", user=current_user.username)
    
    for file in files:
        temp_file_path = None
        storage_type = 'local'
        oss_bucket = None
        oss_object_name = None
        
        try:
            if not file.filename:
                failed_files.append({"filename": "unknown file", "error": "file name is empty"})
                continue
            
            # 检查文件类型
            if not file_processor.is_supported_file(file.filename):
                failed_files.append({
                    "filename": file.filename, 
                    "error": f"Unsupported file type: {os.path.splitext(file.filename)[1]}"
                })
                continue
            
            # 1. 先保存到临时文件.
            file_id = str(uuid.uuid4())[:8]
            name, ext = os.path.splitext(file.filename)
            temp_filename = f"temp_{file_id}_{name}{ext}"
            temp_file_path = os.path.join(UPLOAD_DIR, temp_filename)
            
            content = await file.read()
            file_size = len(content)
            
            with open(temp_file_path, "wb") as buffer:
                buffer.write(content)
            
            # 获取文件信息
            file_info = file_processor.get_file_info(temp_file_path)
            file_type = safe_file_type_conversion(file_info["file_type"])
            
            # 2. 判断是否上传到 OSS
            final_file_path = temp_file_path  # 默认使用临时文件路径
            
            if oss_enabled:
                # 判断文件类别（用于 OSS 目录）
                file_category = oss_service.get_file_category(file.filename)
                
                # 上传到 OSS
                upload_result = oss_service.upload_file(
                    file_path=temp_file_path,
                    filename=file.filename,
                    username=current_user.username,
                    file_category=file_category,
                    expires=3600  # 签名 URL 1 小时有效
                )
                
                if upload_result["success"]:
                    # OSS 上传成功
                    storage_type = 'oss'
                    oss_bucket = upload_result["bucket"]
                    oss_object_name = upload_result["object_name"]
                    final_file_path = oss_object_name  # 存储 object_name
                    
                    # 删除临时文件
                    try:
                        os.remove(temp_file_path)
                        temp_file_path = None
                    except Exception as del_err:
                        warning(f"Failed to delete temporary file: {str(del_err)}")
                    
                    info(f"File uploaded to OSS: {file.filename} -> {oss_object_name}", 
                         source="api", user=current_user.username)
                else:
                    # OSS 上传失败，回退到本地
                    warning(f"OSS upload failed, using local storage: {upload_result.get('error', 'unknown error')}", 
                           source="api", user=current_user.username)
                    storage_type = 'local'
                    final_file_path = temp_file_path
            
            uploaded_files.append(FileUploadResponse(
                filename=file.filename,
                file_path=final_file_path,  # OSS: object_name, Local: /uploads/xxx
                file_size=file_size,
                file_type=file_type,
                upload_time=datetime.now()
            ))
            
            # 额外存储 storage_type 和 oss_bucket（用于数据库写入）
            uploaded_files[-1]._storage_type = storage_type  # type: ignore
            uploaded_files[-1]._oss_bucket = oss_bucket  # type: ignore
            
            info(f"File uploaded successfully: {file.filename} (storage: {storage_type})", 
                 source="api", user=current_user.username)
            
        except Exception as e:
            error(f"File upload failed: {file.filename}, error: {str(e)}", 
                  source="api", user=current_user.username)
            
            # 清理临时文件
            if temp_file_path and os.path.exists(temp_file_path):
                try:
                    os.remove(temp_file_path)
                except Exception:
                    pass
            
            failed_files.append({
                "filename": file.filename,
                "error": str(e)
            })
    
    # 如果有失败的文件，返回错误信息
    if failed_files and not uploaded_files:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"all files upload failed: {[f['filename'] for f in failed_files]}"
        )
    
    # 为成功上传的文件创建数据库记录
    if uploaded_files:
        try:
            db_conn_params = get_database_config()
            db_manager = DatabaseManager(db_conn_params)
            conn = db_manager.get_connection()
            
            if conn:
                cursor = conn.cursor()
                
                # 创建processing_stats记录（状态为uploaded）
                cursor.execute("""
                INSERT INTO processing_stats 
                (timestamp, files_processed, files_failed, total_time, 
                 total_file_processing_time, total_ai_time, task_name, 
                 "user", total_tokens, total_price, upload_params, 
                 final_report, rc_agent_status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """, (
                    datetime.now(),
                    len(uploaded_files),  # files_processed = 上传的文件数
                    len(failed_files),    # files_failed = 失败的文件数
                    0,  # total_time
                    0,  # total_file_processing_time
                    0,  # total_ai_time
                    'File Upload Only',  # task_name
                    current_user.username,
                    0,  # total_tokens
                    0,  # total_price
                    json.dumps({"upload_only": True}),  # upload_params
                    '',  # final_report
                    'uploaded'  # rc_agent_status = 'uploaded' 表示仅上传
                ))
                
                processing_stats_id = cursor.fetchone()[0]
                
                # 为每个上传的文件创建file_processing_details记录
                for uploaded_file in uploaded_files:
                    # 获取文件大小（OSS 文件需要从 uploaded_file.file_size）
                    storage_type = getattr(uploaded_file, '_storage_type', 'local')
                    oss_bucket = getattr(uploaded_file, '_oss_bucket', None)
                    
                    if storage_type == 'local':
                        file_info = file_processor.get_file_info(uploaded_file.file_path)
                        file_size_mb = file_info['file_size_mb']
                    else:
                        # OSS 文件，从 uploaded_file.file_size 计算
                        file_size_mb = uploaded_file.file_size / (1024 * 1024)
                    
                    cursor.execute("""
                    INSERT INTO file_processing_details 
                    (processing_stats_id, filename, file_type, file_size_mb, 
                     processing_time, ai_time, content_length, status, 
                     total_tokens, cost, content, created_at, task_id, original_file_path,
                     file_storage_type, oss_bucket)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        processing_stats_id,
                        uploaded_file.filename,
                        uploaded_file.file_type.value,
                        file_size_mb,
                        0,  # processing_time
                        0,  # ai_time
                        0,  # content_length
                        'uploaded',  # status = 'uploaded' 表示仅上传
                        0,  # total_tokens
                        0,  # cost
                        '',  # content
                        utcnow(),  # created_at (aware UTC; psycopg2 adapter -> naive UTC)
                        None,  # task_id (仅上传没有task_id)
                        uploaded_file.file_path,  # original_file_path (OSS: object_name, Local: /uploads/xxx)
                        storage_type,  # 新增：存储类型
                        oss_bucket  # 新增：OSS bucket 名称
                    ))
                
                conn.commit()
                info(f"created {len(uploaded_files)} database records for files", source="api", user=current_user.username)
                conn.close()
        except Exception as e:
            error(f"failed to create database records: {str(e)}", source="api", user=current_user.username)
            # 数据库记录失败不影响文件上传，只记录错误
    
    return uploaded_files


@router.get("/supported-types", summary="Get supported file types")
async def get_supported_file_types():
    """获取支持的文件类型列表"""
    file_processor = FileProcessor()
    return {
        "supported_extensions": file_processor.get_supported_extensions(),
        "descriptions": file_processor.get_file_extension_description()
    }


@router.post("/validate", summary="Validate files")
async def validate_files(
    file_paths: List[str],
    current_user=Depends(get_current_user)
):
    """
    验证文件列表
    
    - **file_paths**: 文件路径列表
    """
    file_processor = FileProcessor()
    
    # 验证文件
    valid_files, invalid_files = file_processor.validate_files(file_paths)
    
    # 获取文件类型统计
    file_type_summary = file_processor.get_file_type_summary(valid_files)
    
    return {
        "valid_files": valid_files,
        "invalid_files": invalid_files,
        "summary": file_type_summary
    }


@router.post("/process", response_model=FileProcessResponse, summary="Process files")
async def process_files(
    request: FileProcessRequest,
    current_user=Depends(get_current_user)
):
    """
    提交文件处理任务到队列（异步处理）
    
    - **file_paths**: 文件路径列表
    - **task_type**: 可选；local_llm 下可为占位或省略
    - **custom_inputs**: 自定义输入参数
    - **params**: 处理参数
    """
    if not request.file_paths:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Please select the files to process"
        )
    
    # 验证只能处理单个文件
    if len(request.file_paths) > 1:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Currently only supports processing single files, please select only one file at a time"
        )
    
    # 验证文件存在（快速验证，保留在API层）
    file_processor = FileProcessor()
    valid_files, invalid_files = file_processor.validate_files(request.file_paths)
    
    if invalid_files:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"There are invalid files: {[f['path'] for f in invalid_files]}"
        )
    
    # 获取数据库连接参数
    db_conn_params = get_database_config()
    
    # 根据分析模式进行不同的验证（仅支持 local_llm）
    task_info = {}
    analysis_mode = request.analysis_mode or 'local_llm'
    if analysis_mode != 'local_llm':
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Only local_llm analysis mode is supported; remote_agent / Dify integration has been removed.",
        )

    if analysis_mode == 'local_llm':
        # 本地LLM模式：验证模板
        if not request.template_id and not request.template_code:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="When using local LLM mode, you must specify template_id or template_code"
            )
        
        # 获取模板名称
        template_name = None
        try:
            template_service = LLMPromptTemplateService(db_conn_params)
            if request.template_id:
                template = template_service.get_template(request.template_id)
                if template:
                    template_name = template.get('template_name')
            elif request.template_code:
                template = template_service.get_template_by_code(request.template_code)
                if template:
                    template_name = template.get('template_name')
        except Exception as e:
            warning(f"Failed to get template name: {str(e)}", source="api", user=current_user.username)
        
        # 使用模板名称作为task_name，如果没有获取到则使用fallback
        task_name = template_name if template_name else (request.template_code or f'ID:{request.template_id}')
        
        # 构造task_info用于传递给worker
        task_info = {
            'name': task_name,
            'token': None,  # 本地LLM不需要token
            'agent_type': 'local_llm'
        }
        
        info(
            f"使用本地LLM模式: template_id={request.template_id}, "
            f"template_code={request.template_code}, template_name={template_name}",
            source="api",
            user=current_user.username
        )
    try:
        # 准备处理参数，添加分析模式相关参数
        processing_params = request.params.copy()
        _strip_llamaindex_key_from_client_params(processing_params)
        processing_params['analysis_mode'] = analysis_mode
        processing_params['user_id'] = current_user.user_id
        
        if analysis_mode == 'local_llm':
            processing_params['template_id'] = request.template_id
            processing_params['template_code'] = request.template_code
            processing_params['template_variables'] = request.template_variables
            processing_params['llm_provider'] = request.llm_provider
            processing_params['llm_model'] = request.llm_model
        
        # 🚀 提交到Celery任务队列（立即返回）
        task = process_files_task.delay(
            file_paths=valid_files,
            task_info=task_info,
            custom_inputs=request.custom_inputs,
            params=processing_params,
            username=current_user.username
        )
        
        info(f"File processing task submitted to queue: {task.id}", source="api", user=current_user.username)
        
        return FileProcessResponse(
            task_id=task.id,  # 使用Celery任务ID
            message="File processing task submitted to queue, processing asynchronously",
            files_count=len(valid_files)
        )
        
    except Exception as e:
        error(f"Failed to submit task to queue: {str(e)}", source="api", user=current_user.username)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Task submission failed, please try again later"
        )


@router.get("/tasks/{task_id}/status", summary="Get task status")
async def get_task_status(
    task_id: str,
    current_user=Depends(get_current_user)
):
    """
    获取任务执行状态（从Redis读取）
    
    - **task_id**: Celery任务ID
    """
    try:
        # 查询该 Celery task_id 关联的 processing_stats.rc_agent_status, 用于让前端 NavBar /
        # 右侧 AI Analysis 面板感知"上传 Celery 主任务已完成, 但自动并行链路的 analyze_*_task
        # 还在跑"的中间态. processing_stats.rc_agent_status 由 trigger_auto_analyze 在投递前
        # 写为 'analyzing', analyze_*_task 完成后再写 'completed' / 'failed'.
        rc_agent_status: Optional[str] = None
        try:
            from backend.database.db_manager import DatabaseManager
            db_manager = DatabaseManager(get_database_config())
            with db_manager.get_connection_context() as conn:
                cur = conn.cursor()
                cur.execute(
                    """
                    SELECT ps.rc_agent_status
                    FROM file_processing_details fpd
                    JOIN processing_stats ps ON ps.id = fpd.processing_stats_id
                    WHERE fpd.task_id = %s
                    ORDER BY fpd.id DESC
                    LIMIT 1
                    """,
                    (task_id,),
                )
                row = cur.fetchone()
                if row and row[0]:
                    rc_agent_status = row[0]
        except Exception:
            # 查询失败不阻塞 Celery 状态返回
            rc_agent_status = None

        # 从Redis获取任务状态
        task_result = AsyncResult(task_id, app=celery_app)
        
        if task_result.state == 'PENDING':
            return {
                "task_id": task_id,
                "status": "pending",
                "progress": 0,
                "message": "Task is waiting to be processed...",
                "current_file": None,
                "files_processed": 0,
                "total_files": 0,
                "logs": [],
                "rc_agent_status": rc_agent_status,
            }
        elif task_result.state == 'PROGRESS':
            info = task_result.info or {}
            out = {
                "task_id": task_id,
                "status": info.get("status", "processing"),
                "progress": info.get("progress", 0),
                "message": info.get("message", "Processing..."),
                "current_file": info.get("current_file"),
                "files_processed": info.get("files_processed", 0),
                "files_failed": info.get("files_failed", 0),
                "total_files": info.get("total_files", 0),
                "logs": info.get("logs", []),
                "rc_agent_status": rc_agent_status,
            }
            if info.get("asr_fallback_notice"):
                out["asr_fallback_notice"] = True
            if info.get("asr_wait_notice"):
                out["asr_wait_notice"] = True
            return out
        elif task_result.state == 'SUCCESS':
            result = task_result.result or {}
            # 部分任务（如 YouTube）在逻辑失败时仍 return 字典而非抛异常，Celery 会标为 SUCCESS，需按 result.status 区分
            if isinstance(result, dict) and result.get("status") == "failed":
                return {
                    "task_id": task_id,
                    "status": "failed",
                    "progress": 0,
                    "message": str(result.get("error", "Processing failed")),
                    "error_type": result.get("error_type"),
                    "files_processed": result.get("files_processed", 0),
                    "files_failed": result.get("files_failed", 1),
                    "total_files": result.get("total_files", 1),
                    "logs": result.get("logs", []),
                    "rc_agent_status": rc_agent_status,
                }
            return {
                "task_id": task_id,
                "status": "completed",
                "progress": 100,
                "message": "Processing completed",
                "files_processed": result.get("files_processed", 0),
                "files_failed": result.get("files_failed", 0),
                "total_files": result.get("total_files", 0),
                "processing_time": result.get("processing_time", 0),
                "result": result,
                "logs": result.get("logs", []),
                "rc_agent_status": rc_agent_status,
            }
        else:  # FAILURE or other states
            # 尝试获取失败信息，如果获取失败则使用默认信息
            try:
                info = task_result.info or {}
                if isinstance(info, Exception):
                    # 如果info本身是异常对象，转换为字典
                    info = {"error": str(info)}
                elif isinstance(info, str):
                    # 如果info是字符串，转换为字典
                    info = {"error": info}
            except Exception as info_error:
                # 如果无法获取info（比如异常反序列化失败），使用默认值
                info = {"error": f"Task failed, cannot get detailed error information"}
            
            return {
                "task_id": task_id,
                "status": "failed",
                "progress": 0,
                "message": str(info.get("error", "Processing failed")),
                "files_processed": info.get("files_processed", 0),
                "files_failed": info.get("files_failed", 0),
                "total_files": info.get("total_files", 0),
                "logs": info.get("logs", []),
                "rc_agent_status": rc_agent_status,
            }
            
    except Exception as e:
        err_msg = str(e)
        # Celery 后端存储的失败任务若缺少 exc_type，解码会抛出此错误；返回 200 并标记为失败，避免前端一直 500
        if "Exception information must include the exception type" in err_msg or (isinstance(e, ValueError) and "exception type" in err_msg.lower()):
            error(f"Task result cannot be decoded (backend exception information is incomplete): {err_msg}", source="api", task_id=task_id)
            return {
                "task_id": task_id,
                "status": "failed",
                "progress": 0,
                "message": "Task execution failed (可能是上传到 OSS 或 LlamaParse 处理时出错），无法获取详细错误。请重试或联系管理员。",
                "files_processed": 0,
                "files_failed": 0,
                "total_files": 0,
                "logs": []
            }
        error(f"Failed to get task status: {err_msg}", source="api", task_id=task_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to get task status"
        )


@router.post("/tasks/{task_id}/cancel", summary="Cancel task")
async def cancel_task(
    task_id: str,
    current_user=Depends(get_current_user)
):
    """
    取消正在执行的任务
    
    - **task_id**: Celery任务ID
    """
    try:
        celery_app.control.revoke(task_id, terminate=True)
        info(f"Task {task_id} has been cancelled by user {current_user.username}", source="api")
        
        return {"message": f"Task {task_id} has been cancelled"}
        
    except Exception as e:
        error(f"Failed to cancel task: {str(e)}", source="api", task_id=task_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to cancel task"
        )


@router.get("/tasks/{task_id}/files", response_model=List[FileDetailResponse], summary="Get task file details")
async def get_task_files(
    task_id: str,
    current_user=Depends(get_current_user)
):
    """
    获取任务处理的文件详情
    
    - **task_id**: 任务ID (Celery任务ID)
    """
    try:
        # 从数据库获取文件处理详情
        db_conn_params = get_database_config()
        db_manager = DatabaseManager(db_conn_params)
        
        conn = db_manager.get_connection()
        if not conn:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="failed to connect to database"
            )
        
        cursor = conn.cursor()
        
        # 查询文件处理详情
        cursor.execute("""
        SELECT id, filename, file_type, file_size_mb, processing_time,
               ai_time, content_length, status, total_tokens, cost,
               content, created_at, original_file_path, display_name
        FROM file_processing_details 
        WHERE task_id = %s
        ORDER BY created_at DESC
        """, (task_id,))
        
        files = []
        for row in cursor.fetchall():
            files.append(FileDetailResponse(
                id=row[0],
                task_id=task_id,
                filename=row[1],
                file_type=safe_file_type_conversion(row[2]),
                file_size=row[3],
                processing_time=row[4],
                ai_time=row[5],
                content_length=row[6],
                status=row[7],
                total_tokens=row[8],
                cost=row[9],
                error_message=row[10],
                created_at=row[11],
                display_name=row[13] if len(row) > 13 else None
            ))
        
        conn.close()

        return files
        
    except Exception as e:
        error(f"failed to get task file details: {str(e)}", source="api", task_id=task_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="failed to get file details"
        )


@router.get("/list", response_model=List[FileDetailResponse], summary="获取文件列表")
async def get_files(
    skip: int = 0,
    limit: int = 100,
    file_type: Optional[str] = None,
    current_user=Depends(get_current_user)
):
    """
    获取文件处理历史列表（按用户权限过滤）
    
    - admin 可看全部
    - 非 admin 只能看自己上传的 + 被分享的
    """
    try:
        db_conn_params = get_database_config()
        db_manager = DatabaseManager(db_conn_params)
        
        conn = db_manager.get_connection()
        if not conn:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="failed to connect to database"
            )
        
        cursor = conn.cursor()
        
        conditions = []
        params = []
        
        if file_type:
            conditions.append("fpd.file_type = %s")
            params.append(file_type)
        
        conditions.append("""(
            ps."user" = %s
            OR EXISTS (SELECT 1 FROM file_shares fs WHERE fs.file_id = fpd.id AND fs.shared_to_user_id = %s)
        )""")
        params.extend([current_user.username, current_user.user_id])
        access_type_expr = """CASE WHEN ps."user" = %s THEN 'owner' ELSE 'shared' END"""
        shared_by_expr = """(
            SELECT u.username FROM file_shares fs2
            JOIN users u ON u.id = fs2.shared_by_user_id
            WHERE fs2.file_id = fpd.id AND fs2.shared_to_user_id = %s
            ORDER BY fs2.created_at DESC LIMIT 1
        )"""
        
        where_clause = ("WHERE " + " AND ".join(conditions)) if conditions else ""
        select_params = [current_user.username, current_user.user_id]
        
        query = f"""
            SELECT fpd.id, fpd.filename, fpd.file_type, fpd.file_size_mb, fpd.processing_time, 
                   fpd.ai_time, fpd.content_length, fpd.status, fpd.total_tokens, fpd.cost, 
                   fpd.content, fpd.created_at, fpd.original_file_path,
                   {access_type_expr} as access_type,
                   {shared_by_expr} as shared_by,
                   fpd.display_name
            FROM file_processing_details fpd
            JOIN processing_stats ps ON fpd.processing_stats_id = ps.id
            {where_clause}
            ORDER BY fpd.created_at DESC
            LIMIT %s OFFSET %s
        """
        
        cursor.execute(query, select_params + params + [limit, skip])
        
        files = []
        for row in cursor.fetchall():
            files.append(FileDetailResponse(
                id=row[0],
                filename=row[1],
                file_type=safe_file_type_conversion(row[2]),
                file_size_mb=row[3],
                processing_time=row[4],
                ai_time=row[5],
                content_length=row[6],
                status=row[7],
                total_tokens=row[8],
                cost=row[9],
                content=row[10],
                created_at=row[11],
                original_file_path=row[12],
                access_type=row[13],
                shared_by=row[14],
                display_name=row[15]
            ))
        
        conn.close()

        return files
        
    except Exception as e:
        error(f"Failed to get file list: {str(e)}", source="api")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to get file list"
        )


@router.get("/stats", summary="Get processing stats")
async def get_processing_stats(
    current_user=Depends(get_current_user)
):
    """
    获取文件处理统计信息
    """
    try:
        db_conn_params = get_database_config()
        db_manager = DatabaseManager(db_conn_params)
        
        conn = db_manager.get_connection()
        if not conn:
            raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to connect to database"
            )
        
        cursor = conn.cursor()
        
        # 查询统计信息
        cursor.execute("""
        SELECT 
                COUNT(*) as total_files,
                COUNT(CASE WHEN status = 'completed' THEN 1 END) as completed_files,
                COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed_files,
                AVG(processing_time) as avg_processing_time,
                AVG(ai_time) as avg_ai_time,
                SUM(total_tokens) as total_tokens,
                SUM(cost) as total_cost
            FROM file_processing_details
        """)
        
        stats = cursor.fetchone()
        
        conn.close()
        
        return {
            "total_files": stats[0] or 0,
            "completed_files": stats[1] or 0,
            "failed_files": stats[2] or 0,
            "avg_processing_time": float(stats[3]) if stats[3] else 0,
            "avg_ai_time": float(stats[4]) if stats[4] else 0,
            "total_tokens": stats[5] or 0,
            "total_cost": float(stats[6]) if stats[6] else 0
        }
        
    except Exception as e:
        error(f"Failed to get processing stats: {str(e)}", source="api")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to get processing stats"
        )


# 文件标签相关API
@router.post("/tags", response_model=FileTagResponse, summary="Create file tags")
async def create_file_tag(
    tag: FileTagCreate,
    current_user=Depends(get_current_user)
):
    """
    为文件创建标签
    """
    try:
        db_conn_params = get_database_config()
        db_manager = DatabaseManager(db_conn_params)
        
        conn = db_manager.get_connection()
        if not conn:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to connect to database"
            )
        
        from psycopg2.extras import Json
        cursor = conn.cursor()
        
        ticker_jsonb = Json([tag.ticker]) if tag.ticker else None
        cursor.execute("""
            INSERT INTO file_tags (file_id, sector, industry, ticker, tagged_by)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id, file_id, sector, industry, ticker, tagged_by, created_at
        """, (
            tag.file_id,
            tag.sector,
            tag.industry,
            ticker_jsonb,
            current_user.username
        ))
        
        result = cursor.fetchone()
        conn.commit()
        conn.close()
        
        ticker_val = result[4] if isinstance(result[4], list) else (result[4] or [])
        return FileTagResponse(
            id=result[0],
            file_id=result[1],
            sector=result[2],
            industry=result[3],
            ticker=json.dumps(ticker_val),
            tagged_by=result[5],
            created_at=result[6]
        )
        
    except Exception as e:
        error(f"Failed to create file tags: {str(e)}", source="api")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create file tags"
        )


@router.get("/tags/stats", response_model=FileTagStats, summary="Get tag stats")
async def get_tag_stats(
    current_user=Depends(get_current_user)
):
    """
    获取文件标签统计信息
    """
    try:
        db_conn_params = get_database_config()
        db_manager = DatabaseManager(db_conn_params)
        
        conn = db_manager.get_connection()
        if not conn:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to connect to database"
            )
        
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                COUNT(*) as total_tags,
                COUNT(DISTINCT sector) as unique_sectors,
                COUNT(DISTINCT industry) as unique_industries,
                (SELECT COUNT(DISTINCT t) FROM file_tags, jsonb_array_elements_text(ticker) AS t) as unique_tickers
            FROM file_tags
        """)
        
        stats = cursor.fetchone()
        
        conn.close()
        
        return FileTagStats(
            total_tags=stats[0] or 0,
            unique_sectors=stats[1] or 0,
            unique_industries=stats[2] or 0,
            unique_tickers=stats[3] or 0
        )
        
    except Exception as e:
        error(f"Failed to get tag stats: {str(e)}", source="api")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to get tag stats"
        )


@router.get("/tags/files", response_model=List[FileWithTag], summary="Get files with tags")
async def get_files_with_tags(
    sector: Optional[str] = None,
    industry: Optional[str] = None,
    ticker: Optional[str] = None,
    current_user=Depends(get_current_user)
):
    """
    获取带标签的文件列表
    """
    try:
        db_conn_params = get_database_config()
        db_manager = DatabaseManager(db_conn_params)
        
        conn = db_manager.get_connection()
        if not conn:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to connect to database"
            )
        
        cursor = conn.cursor()
        
        # 构建查询条件
        where_clause = "WHERE 1=1"
        params = []
        
        if sector:
            where_clause += " AND ft.sector = %s"
            params.append(sector)
        if industry:
            where_clause += " AND ft.industry = %s"
            params.append(industry)
        if ticker:
            where_clause += " AND ft.ticker @> %s::jsonb"
            params.append(json.dumps([ticker]))
        
        # 查询带标签的文件
        query = f"""
            SELECT 
                fpd.id, fpd.filename, fpd.file_type, fpd.file_size_mb,
                fpd.processing_time, fpd.ai_time, fpd.content_length,
                fpd.status, fpd.total_tokens, fpd.cost, fpd.created_at,
                ft.sector, ft.industry, ft.ticker, ft.tagged_by
            FROM file_processing_details fpd
            JOIN file_tags ft ON fpd.id = ft.file_id
            {where_clause}
            ORDER BY fpd.created_at DESC
        """
        
        cursor.execute(query, params)
        
        files = []
        for row in cursor.fetchall():
            ticker_val = row[13] if isinstance(row[13], list) else (row[13] or [])
            files.append(FileWithTag(
                id=row[0],
                filename=row[1],
                file_type=safe_file_type_conversion(row[2]),
                file_size_mb=row[3],
                processing_time=row[4],
                ai_time=row[5],
                content_length=row[6],
                status=row[7],
                total_tokens=row[8],
                cost=row[9],
                created_at=row[10],
                sector=row[11],
                industry=row[12],
                ticker=json.dumps(ticker_val),
                tagged_by=row[14]
            ))
        
        conn.close()
        
        return files
        
    except Exception as e:
        error(f"Failed to get files with tags: {str(e)}", source="api")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to get files with tags"
        )


@router.get("/processing-params", summary="Get processing parameters options")
async def get_processing_params():
    """获取文件处理的参数选项和默认值"""
    return {
        "language_options": [
            {"value": ["ch_sim", "en"], "label": "Chinese Simplified and English"},
            {"value": ["ch_sim"], "label": "Only Chinese Simplified"},
            {"value": ["en"], "label": "Only English"},
            {"value": ["ch_tra"], "label": "Only Chinese Traditional"}
        ],
        "default_language": ["ch_sim", "en"],
        "fast_mode_options": [
            {"value": True, "label": "Fast Mode"},
            {"value": False, "label": "Standard Mode"}
        ],
        "default_fast_mode": False,
        "max_pages_options": [
            {"value": 5, "label": "5 Pages"},
            {"value": 10, "label": "10 Pages"},
            {"value": 20, "label": "20 Pages"},
            {"value": 50, "label": "50 Pages"},
            {"value": 100, "label": "100 Pages"}
        ],
        "default_max_pages": 10
    }


@router.delete("/uploads/{filename}", summary="Delete uploaded file")
async def delete_uploaded_file(
    filename: str,
    current_user=Depends(get_current_user)
):
    """
    删除上传的文件
    
    - **filename**: 文件名
    """
    try:
        file_path = os.path.join(UPLOAD_DIR, filename)
        
        if os.path.exists(file_path):
            os.remove(file_path)
            info(f"文件删除成功: {filename}", source="api", user=current_user.username)
            return {"message": f"File {filename} has been deleted"}
        else:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="File not found"
            )
                
    except Exception as e:
        error(f"Failed to delete file: {str(e)}", source="api", user=current_user.username)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to delete file"
        )


@router.get("/original/{file_id}", summary="Get original file information (supports OSS)")
async def get_original_file_info(
    file_id: str,
    current_user=Depends(get_current_user)
):
    """
    获取原始文件信息（支持 OSS 和本地存储）
    
    - **file_id**: 文件ID
    
    返回：
    - OSS 文件：返回带签名的临时 URL（1小时有效）
    - 本地文件：返回 /serve/{file_id} 端点
    """
    try:
        db_conn_params = get_database_config()
        db_manager = DatabaseManager(db_conn_params)
        
        conn = db_manager.get_connection()
        if not conn:
            raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to connect to database"
            )
        
        cursor = conn.cursor()
        
        access = _check_file_access(cursor, int(file_id), current_user.user_id, current_user.username, current_user.is_admin)
        if not access:
            conn.close()
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No access to this file")
        
        cursor.execute("""
            SELECT filename, file_type, file_size_mb, original_file_path, created_at, 
                   content, asr_with_diarization, file_storage_type, oss_bucket, display_name
            FROM file_processing_details 
            WHERE id = %s
        """, (file_id,))
        
        result = cursor.fetchone()
        conn.close()
        
        if result:
            raw_filename = result[0]
            file_type = result[1]
            file_size_mb = result[2]
            original_file_path = result[3]
            created_at = result[4]
            content = result[5]
            asr_with_diarization = result[6]
            storage_type = result[7] or 'local'  # 兼容旧数据
            oss_bucket = result[8]
            display_name = result[9] if len(result) > 9 else None
            filename = display_name or raw_filename
            
            # 根据存储类型生成 file_url
            if storage_type == 'oss':
                # OSS 文件：根据配置生成 URL
                oss_service = OSSService()
                if oss_service.is_configured():
                    # 如果配置了自定义域名，使用公共 URL（可以预览）
                    if oss_service.custom_domain:
                        from urllib.parse import quote
                        encoded_object_name = quote(original_file_path, safe='/')
                        file_url = f"http://{oss_service.custom_domain}/{encoded_object_name}"
                        info(f"使用 OSS 自定义域名: {oss_service.custom_domain}", source="api")
                    else:
                        # 无自定义域名时，OSS 默认域名会对 PDF 等强制 Content-Disposition: attachment，
                        # 导致 iframe 内无法预览、只能下载。PDF/图片/音频改为走后端 serve 代理（inline 或可下载流）。
                        # Word/Excel 需公网 HTTPS URL 供 Microsoft Office Online Viewer 拉取，故仍用签名 URL。
                        preview_via_serve = (file_type or '').lower() in (
                            'pdf', 'application/pdf', 'jpg', 'jpeg', 'png', 'gif', 'bmp', 'image',
                            'audio', 'podcast', 'web',
                        )
                        if preview_via_serve:
                            file_url = f"/api/files/serve/{file_id}"
                            info(f"OSS 无自定义域名，PDF/图片/音频走 serve 代理: file_id={file_id}", source="api")
                        else:
                            signed_url = oss_service.get_signed_url(original_file_path, expires=7200)
                            if signed_url:
                                file_url = signed_url.replace('http://', 'https://', 1) if signed_url.startswith('http://') else signed_url
                                info(f"使用 OSS 签名 URL (2h): file_id={file_id}", source="api")
                            else:
                                file_url = f"/api/files/serve/{file_id}"
                else:
                    # OSS 未配置，使用 serve 接口
                    warning(f"OSS 未配置，使用 serve 接口: file_id={file_id}")
                    file_url = f"/api/files/serve/{file_id}"
            else:
                # 本地文件：使用 serve 接口
                if os.getenv('ENV', 'development') == 'development':
                    # 本地开发：直接指向后端端口8000
                    file_url = f"http://localhost:{os.getenv('WEBAPP_PORT', '8000')}/api/files/serve/{file_id}"
                else:
                    # 生产环境：使用相对路径
                    file_url = f"/api/files/serve/{file_id}"
            
            # 本地开发时，若 file_url 为相对路径 /api/files/serve/，iframe 会请求到前端 dev server (3000)，
            # 可能被 SPA fallback 返回 index.html，导致右侧 Original 显示主页而非文件。改为绝对 URL 直连后端。
            if os.getenv('ENV', 'development') == 'development' and file_url.startswith('/api/files/serve/'):
                _port = os.getenv('WEBAPP_PORT', '8000')
                file_url = f"http://localhost:{_port}{file_url}"
                info(f"本地开发：file_url 改为直连后端以支持 iframe 预览: {file_url}", source="api")
            
            # 解析 asr_with_diarization JSON 数据
            asr_data = None
            if asr_with_diarization:
                try:
                    if isinstance(asr_with_diarization, str):
                        asr_data = json.loads(asr_with_diarization)
                    else:
                        asr_data = asr_with_diarization  # 已经是dict（JSONB类型）
                except Exception as e:
                    warning(f"解析 asr_with_diarization 数据失败: {str(e)}", source="api", file_id=file_id)
            
            return {
                "file_id": file_id,
                "filename": filename,
                "file_type": file_type,
                "file_size_mb": file_size_mb,
                "original_file_path": original_file_path,
                "file_url": file_url,
                "storage_type": storage_type,
                "oss_bucket": oss_bucket,
                "created_at": created_at,
                "content": content,
                "asr_with_diarization": asr_data,
                "display_name": display_name
            }
        else:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="File not found"
        )
        
    except Exception as e:
        error(f"Failed to get file information: {str(e)}", source="api", file_id=file_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to get file information"
        )


@router.patch("/details/{file_id}/content", summary="Update content in file processing details (Markdown)")
async def update_file_detail_content(
    file_id: str,
    content: str = Body(..., embed=True),
    trigger_llm: bool = Body(False, embed=True),
    current_user: dict = Depends(get_current_user)
):
    """
    更新 file_processing_details 表中的 content 字段（用于编辑 Content 标签页的 Markdown 内容）。
    仅允许该文件所属任务执行记录的用户（或管理员）更新。
    当 trigger_llm=True 时，保存后重新触发 auto-tagging / auto-naming / auto-classify。
    """
    try:
        db_manager = DatabaseManager(get_database_config())
        conn = db_manager.get_connection()
        if not conn:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to connect to database"
            )
        cursor = conn.cursor()
        # PR-4 R2 修复: 同时拉 file_type, 用于 trigger_auto_analyze 决策 (audio/youtube 走 audio_analysis_v1 fallback)
        cursor.execute("""
            SELECT fpd.id, fpd.processing_stats_id, fpd.file_type
            FROM file_processing_details fpd
            WHERE fpd.id = %s
        """, (file_id,))
        row = cursor.fetchone()
        if not row:
            conn.close()
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="File not found"
            )
        processing_stats_id = row[1]
        file_type = row[2]
        cursor.execute("""
            SELECT "user" FROM processing_stats WHERE id = %s
        """, (processing_stats_id,))
        owner = cursor.fetchone()
        if not owner:
            conn.close()
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Task execution record not found"
            )
        if owner[0] != current_user.username:
            conn.close()
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Permission denied, only own content can be edited"
            )
        cursor.execute("""
            UPDATE file_processing_details
            SET content = %s, content_length = %s
            WHERE id = %s
        """, (content, len(content), file_id))
        conn.commit()
        conn.close()
        info(f"更新文件 content 成功: file_id={file_id}", source="api", user=current_user.username)

        if trigger_llm:
            try:
                username = current_user.username
                trigger_auto_tagging(int(file_id), username, delay=int(os.getenv('AUTO_TAG_DELAY', '2')))
                trigger_auto_naming(int(file_id), username, delay=int(os.getenv('AUTO_NAME_DELAY', '2')))
                trigger_auto_classify(int(file_id), username, delay=int(os.getenv('AUTO_CLASSIFY_DELAY', '2')))
                # PR-4 R2 修复: 与 create-text-note / process_*_task 4 路并行触发对齐.
                # 文档/网页/text: 用户未在 Settings 设 file_analysis 偏好则跳过 (trigger_auto_analyze 内部判断);
                # 音频/YouTube: 兜底走 audio_analysis_v1.
                trigger_auto_analyze(
                    file_detail_id=int(file_id),
                    execution_id=processing_stats_id,
                    file_type=file_type or 'document',
                    template_id=None,
                    template_variables=None,
                    username=username,
                    delay=int(os.getenv('AUTO_ANALYZE_DELAY', '2')),
                )
                # 第 5 路并行: 自动向量化 (OSS Vector Bucket / 语义检索)
                try:
                    from backend.tasks.vectorize_triggers import trigger_auto_vectorize_file
                    trigger_auto_vectorize_file(
                        int(file_id),
                        delay=int(os.getenv('AUTO_VECTORIZE_DELAY', '2')),
                    )
                except Exception as ve:
                    warning(f"trigger_auto_vectorize_file failed (non-blocking): {str(ve)}",
                            source="api", file_id=file_id)
            except Exception as e:
                warning(f"Failed to trigger auto LLM tasks after content update: {str(e)}",
                        source="api", file_id=file_id)

        return {"success": True, "message": "Content updated"}
    except HTTPException:
        raise
    except Exception as e:
        error(f"更新文件 content 失败: {str(e)}", source="api", file_id=file_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="更新内容失败"
        )


@router.post("/create-text-note", summary="Create a text note (no file upload, user-authored Markdown)")
async def create_text_note(
    title: str = Body('', embed=True),
    content: str = Body(..., embed=True),
    auto_analyze_template_id: Optional[int] = Body(None, embed=True),
    auto_analyze_template_variables: Optional[Dict[str, Any]] = Body(None, embed=True),
    auto_analyze_enabled: bool = Body(True, embed=True),
    auto_tag_template_id: Optional[int] = Body(None, embed=True),
    auto_tag_template_variables: Optional[Dict[str, Any]] = Body(None, embed=True),
    auto_tag_enabled: bool = Body(True, embed=True),
    current_user: dict = Depends(get_current_user)
):
    """
    用户直接撰写 Markdown 文本笔记，无需上传文件。
    创建 processing_stats + file_processing_details 记录后自动触发
    4 路并行 LLM 任务: tag / name / classify / analyze (analyze 仅当用户选了模板).
    """
    username = current_user.username
    display_title = title.strip() if title and title.strip() else 'Untitled Note'

    try:
        db_manager = DatabaseManager(get_database_config())
        conn = db_manager.get_connection()
        if not conn:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to connect to database"
            )
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO processing_stats
            (timestamp, files_processed, files_failed, total_time,
             total_file_processing_time, total_ai_time, task_name,
             "user", total_tokens, total_price, upload_params,
             final_report, rc_agent_status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (
            datetime.now(),
            1, 0, 0, 0, 0,
            'Text Note',
            username,
            0, 0,
            json.dumps({"text_note": True, "title": display_title}),
            '',
            'completed'
        ))
        processing_stats_id = cursor.fetchone()[0]

        cursor.execute("""
            INSERT INTO file_processing_details
            (processing_stats_id, filename, file_type, file_size_mb,
             processing_time, ai_time, content_length, status,
             total_tokens, cost, content, created_at, task_id,
             original_file_path, file_storage_type, oss_bucket)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (
            processing_stats_id,
            display_title,
            'text',
            0,
            0, 0,
            len(content),
            'completed',
            0, 0,
            content,
            utcnow(),  # created_at (aware UTC; psycopg2 adapter -> naive UTC)
            None,
            None,
            None,
            None
        ))
        file_detail_id = cursor.fetchone()[0]

        conn.commit()
        conn.close()

        info(f"Text note created: file_detail_id={file_detail_id}, title={display_title}",
             source="api", user=username)

        try:
            trigger_auto_tagging(
                file_detail_id, username,
                delay=int(os.getenv('AUTO_TAG_DELAY', '2')),
                template_id=auto_tag_template_id,
                template_variables=auto_tag_template_variables,
                enabled=auto_tag_enabled,
            )
            trigger_auto_naming(file_detail_id, username, delay=int(os.getenv('AUTO_NAME_DELAY', '2')))
            trigger_auto_classify(file_detail_id, username, delay=int(os.getenv('AUTO_CLASSIFY_DELAY', '2')))
            trigger_auto_analyze(
                file_detail_id=file_detail_id,
                execution_id=processing_stats_id,
                file_type='text',
                template_id=auto_analyze_template_id,
                template_variables=auto_analyze_template_variables,
                username=username,
                delay=int(os.getenv('AUTO_ANALYZE_DELAY', '2')),
                enabled=auto_analyze_enabled,
            )
            # 第 5 路并行: 自动向量化 (OSS Vector Bucket / 语义检索)
            try:
                from backend.tasks.vectorize_triggers import trigger_auto_vectorize_file
                trigger_auto_vectorize_file(
                    file_detail_id,
                    delay=int(os.getenv('AUTO_VECTORIZE_DELAY', '2')),
                )
            except Exception as ve:
                warning(f"trigger_auto_vectorize_file failed for text note (non-blocking): {str(ve)}",
                        source="api", user=username)
        except Exception as e:
            warning(f"Failed to trigger auto LLM tasks for text note: {str(e)}",
                    source="api", user=username)

        return {
            "success": True,
            "file_detail_id": file_detail_id,
            "processing_stats_id": processing_stats_id
        }

    except HTTPException:
        raise
    except Exception as e:
        error(f"Failed to create text note: {str(e)}", source="api", user=username)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create text note"
        )


@router.patch("/details/{file_id}/asr-segment", summary="更新 ASR 转写单条文本并同步 Markdown content")
async def update_asr_segment_text(
    file_id: str,
    body: UpdateAsrSegmentBody,
    current_user: dict = Depends(get_current_user),
):
    """
    修改 file_processing_details.asr_with_diarization 中某条转写，并重建 content 字段，
    使 Original 说话人视图与 Markdown 标签页保持一致。
    """
    try:
        db_manager = DatabaseManager(get_database_config())
        conn = db_manager.get_connection()
        if not conn:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="无法连接到数据库",
            )
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT fpd.id, fpd.processing_stats_id, fpd.asr_with_diarization
            FROM file_processing_details fpd
            WHERE fpd.id = %s
            """,
            (file_id,),
        )
        row = cursor.fetchone()
        if not row:
            conn.close()
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="文件不存在")

        processing_stats_id = row[1]
        asr_raw = row[2]
        cursor.execute(
            'SELECT "user" FROM processing_stats WHERE id = %s',
            (processing_stats_id,),
        )
        owner_row = cursor.fetchone()
        if not owner_row:
            conn.close()
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="任务执行记录不存在")
        if owner_row[0] != current_user.username:
            conn.close()
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Permission denied, only the owner can edit the file",
            )

        if not asr_raw:
            conn.close()
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No ASR structured data can be edited",
            )

        asr_data = json.loads(asr_raw) if isinstance(asr_raw, str) else dict(asr_raw)

        if body.segment_kind == "audio":
            sentences = asr_data.get("sentences") or []
            if not sentences:
                conn.close()
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="The current data is not in the audio speaker separation format",
                )
            ordered = sorted(
                sentences,
                key=lambda x: (x.get("begin_time", 0), x.get("sentence_id") or 0),
            )
            if body.index >= len(ordered):
                conn.close()
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Sentence index out of bounds: {body.index}",
                )
            target = ordered[body.index]
            target["text"] = body.text
            new_content = _rebuild_audio_content_from_asr_structured(asr_data)
        else:
            transcript = asr_data.get("transcript")
            if not isinstance(transcript, list) or len(transcript) == 0:
                conn.close()
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="The current data is not in the YouTube subtitle format",
                )
            if body.index >= len(transcript):
                conn.close()
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Subtitle segment index out of bounds: {body.index}",
                )
            transcript[body.index]["text"] = body.text
            new_content = YouTubeService.get_full_text(asr_data)

        cursor.execute(
            """
            UPDATE file_processing_details
            SET asr_with_diarization = %s,
                content = %s,
                content_length = %s
            WHERE id = %s
            """,
            (json.dumps(asr_data, ensure_ascii=False), new_content, len(new_content or ""), file_id),
        )
        conn.commit()
        conn.close()

        info(
            f"更新 ASR 转写片段成功: file_id={file_id}, kind={body.segment_kind}, index={body.index}",
            source="api",
            user=current_user.username,
        )
        return {"success": True, "message": "Transcript updated", "content_length": len(new_content or "")}
    except HTTPException:
        raise
    except Exception as e:
        error(f"更新 ASR 转写片段失败: {str(e)}", source="api", file_id=file_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Update transcript failed: {str(e)}",
        )


def _verify_asr_file_owner(cursor, file_id: str, current_user) -> tuple:
    """校验文件归属，返回 (asr_raw, asr_data) 或 raise HTTPException。"""
    cursor.execute(
        """
        SELECT fpd.id, fpd.processing_stats_id, fpd.asr_with_diarization
        FROM file_processing_details fpd
        WHERE fpd.id = %s
        """,
        (file_id,),
    )
    row = cursor.fetchone()
    if not row:
        return None, None
    processing_stats_id = row[1]
    asr_raw = row[2]
    cursor.execute('SELECT "user" FROM processing_stats WHERE id = %s', (processing_stats_id,))
    owner_row = cursor.fetchone()
    if not owner_row or owner_row[0] != current_user.username:
        return None, None
    if not asr_raw:
        return asr_raw, None
    asr_data = json.loads(asr_raw) if isinstance(asr_raw, str) else dict(asr_raw)
    return asr_raw, asr_data


@router.delete("/details/{file_id}/asr-segment", summary="删除 ASR 转写单条并同步 Markdown content")
async def delete_asr_segment(
    file_id: str,
    body: DeleteAsrSegmentBody,
    current_user: dict = Depends(get_current_user),
):
    """
    从 file_processing_details.asr_with_diarization 中删除指定的一条转写，
    并重建 content 字段。
    """
    try:
        db_manager = DatabaseManager(get_database_config())
        conn = db_manager.get_connection()
        if not conn:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to connect to the database",
            )
        cursor = conn.cursor()
        asr_raw, asr_data = _verify_asr_file_owner(cursor, file_id, current_user)
        if asr_raw is None:
            conn.close()
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="File not found or no permission")
        if asr_data is None:
            conn.close()
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No ASR structured data can be edited",
            )

        if body.segment_kind == "audio":
            sentences = asr_data.get("sentences") or []
            if not sentences:
                conn.close()
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="The current data is not in the audio speaker separation format",
                )
            ordered = sorted(
                sentences,
                key=lambda x: (x.get("begin_time", 0), x.get("sentence_id") or 0),
            )
            if body.index >= len(ordered):
                conn.close()
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Sentence index out of bounds: {body.index}",
                )
            to_remove = ordered[body.index]
            asr_data["sentences"] = [s for s in sentences if s is not to_remove]
            asr_data["total_sentences"] = len(asr_data["sentences"])
            new_content = _rebuild_audio_content_from_asr_structured(asr_data)
        else:
            transcript = asr_data.get("transcript")
            if not isinstance(transcript, list) or len(transcript) == 0:
                conn.close()
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="The current data is not in the YouTube subtitle format",
                )
            if body.index >= len(transcript):
                conn.close()
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Subtitle segment index out of bounds: {body.index}",
                )
            asr_data["transcript"] = transcript[: body.index] + transcript[body.index + 1 :]
            new_content = YouTubeService.get_full_text(asr_data)

        cursor.execute(
            """
            UPDATE file_processing_details
            SET asr_with_diarization = %s,
                content = %s,
                content_length = %s
            WHERE id = %s
            """,
            (json.dumps(asr_data, ensure_ascii=False), new_content, len(new_content or ""), file_id),
        )
        conn.commit()
        conn.close()

        info(
            f"删除 ASR 转写片段成功: file_id={file_id}, kind={body.segment_kind}, index={body.index}",
            source="api",
            user=current_user.username,
        )
        return {"success": True, "message": "Segment deleted", "content_length": len(new_content or "")}
    except HTTPException:
        raise
    except Exception as e:
        error(f"Failed to delete ASR transcription segment: {str(e)}", source="api", file_id=file_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete ASR transcription segment: {str(e)}",
        )


@router.patch("/details/{file_id}/asr-replace", summary="Replace ASR transcription text in bulk and sync Markdown content")
async def replace_asr_text(
    file_id: str,
    body: ReplaceAsrBody,
    current_user: dict = Depends(get_current_user),
):
    """
    在所有转写句子/分段中，将「查找」文本全部替换为「替换」文本，
    并重建 content 字段。适用于纠正常见 ASR 错误（如 open cloud -> open claw）。
    """
    try:
        db_manager = DatabaseManager(get_database_config())
        conn = db_manager.get_connection()
        if not conn:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to connect to the database",
            )
        cursor = conn.cursor()
        asr_raw, asr_data = _verify_asr_file_owner(cursor, file_id, current_user)
        if asr_raw is None:
            conn.close()
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="File not found or no permission")
        if asr_data is None:
            conn.close()
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No ASR structured data can be edited",
            )

        find_str = body.find
        replace_str = body.replace
        replace_count = 0

        if asr_data.get("sentences"):
            for sent in asr_data["sentences"]:
                text = sent.get("text") or ""
                count = text.count(find_str)
                if count:
                    replace_count += count
                    sent["text"] = text.replace(find_str, replace_str)
            new_content = _rebuild_audio_content_from_asr_structured(asr_data)
        elif asr_data.get("transcript"):
            for seg in asr_data["transcript"]:
                text = seg.get("text") or ""
                count = text.count(find_str)
                if count:
                    replace_count += count
                    seg["text"] = text.replace(find_str, replace_str)
            new_content = YouTubeService.get_full_text(asr_data)
        else:
            conn.close()
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="The current data is not in the audio speaker separation format or the YouTube subtitle format",
            )

        cursor.execute(
            """
            UPDATE file_processing_details
            SET asr_with_diarization = %s,
                content = %s,
                content_length = %s
            WHERE id = %s
            """,
            (json.dumps(asr_data, ensure_ascii=False), new_content, len(new_content or ""), file_id),
        )
        conn.commit()
        conn.close()

        info(
            f"Replace ASR transcription text in bulk and sync Markdown content successfully: file_id={file_id}, replace_count={replace_count}",
            source="api",
            user=current_user.username,
        )
        return {
            "success": True,
            "message": f"Replaced {replace_count} occurrence(s)",
            "replace_count": replace_count,
            "content_length": len(new_content or ""),
        }
    except HTTPException:
        raise
    except Exception as e:
        error(f"Failed to replace ASR transcription text in bulk and sync Markdown content: {str(e)}", source="api", file_id=file_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to replace ASR transcription text in bulk and sync Markdown content: {str(e)}",
        )


@router.patch("/{file_id}/speaker/{speaker_id}", summary="Update speaker name")
async def update_speaker_label(
    file_id: str,
    speaker_id: int,
    new_label: str,
    current_user: dict = Depends(get_current_user)
):
    """
    更新音频文件中指定说话人的标签名称
    
    - **file_id**: 文件ID
    - **speaker_id**: 说话人ID
    - **new_label**: 新的标签名称
    """
    try:
        db_manager = DatabaseManager(get_database_config())
        
        # 1. 获取当前的 asr_with_diarization 数据
        query = """
            SELECT asr_with_diarization 
            FROM file_processing_details 
            WHERE id = %s
        """
        result = db_manager.fetch_one(query, (file_id,))
        
        if not result or not result[0]:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="File not found or no ASR data"
            )
        
        # 2. 解析 JSON 数据
        asr_data = result[0]
        if isinstance(asr_data, str):
            asr_data = json.loads(asr_data)
        
        # 3. 更新 speakers 列表中的 label
        speaker_found = False
        if 'speakers' in asr_data:
            for speaker in asr_data['speakers']:
                if speaker.get('speaker_id') == speaker_id:
                    speaker['label'] = new_label
                    speaker_found = True
                    break
        
        if not speaker_found:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Speaker ID {speaker_id} not found"
            )
        
        # 4. 更新数据库
        update_query = """
            UPDATE file_processing_details 
            SET asr_with_diarization = %s
            WHERE id = %s
        """
        success = db_manager.execute_query(
            update_query,
            (json.dumps(asr_data), file_id)
        )
        
        if not success:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Database update failed"
            )
        
        info(f"Update speaker label successfully: file_id={file_id}, speaker_id={speaker_id}, new_label={new_label}", source="api")
        
        return {
            "success": True,
            "message": "Speaker label updated successfully",
            "speaker_id": speaker_id,
            "new_label": new_label
        }
        
    except HTTPException:
        raise
    except Exception as e:
        error(f"Update speaker label failed: {str(e)}", source="api", file_id=file_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Update speaker label failed: {str(e)}"
        )


@router.get("/serve/{file_id}", summary="Provide file access (supports OSS redirection)")
async def serve_file(
    file_id: str,
    token: str = None,
    range_header: Optional[str] = Header(default=None, alias="Range")
):
    """
    提供文件访问（支持 OSS 和本地文件）
    
    - **file_id**: 文件ID
    - **token**: 可选，认证token（query参数）
    
    行为：
    - OSS 文件：重定向到 OSS 签名 URL
    - 本地文件：直接返回文件流
    """
    # 验证token
    from backend.auth.jwt_utils import verify_token
    
    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication token is required. Please add ?token=YOUR_TOKEN to the URL"
        )
    
    try:
        payload = verify_token(token)
        if not payload:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid token"
            )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Failed to verify token: {str(e)}"
        )
    try:
        db_conn_params = get_database_config()
        db_manager = DatabaseManager(db_conn_params)
        
        conn = db_manager.get_connection()
        if not conn:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to connect to the database"
            )
        
        cursor = conn.cursor()
        
        access = _check_file_access(
            cursor, int(file_id),
            payload.user_id, payload.username, payload.is_admin
        )
        if not access:
            conn.close()
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No access to this file")
        
        cursor.execute("""
            SELECT filename, file_type, original_file_path, file_storage_type, oss_bucket
            FROM file_processing_details 
            WHERE id = %s
        """, (file_id,))
        
        result = cursor.fetchone()
        conn.close()
        
        if not result:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="File not found"
            )
        
        filename, file_type, file_path, storage_type, oss_bucket = result
        storage_type = storage_type or 'local'  # 兼容旧数据
        
        # 如果是 OSS 文件，提供访问
        if storage_type == 'oss':
            oss_service = OSSService()
            if oss_service.is_configured():
                # 如果配置了自定义域名，重定向到公共 URL
                if oss_service.custom_domain:
                    from urllib.parse import quote
                    from fastapi.responses import RedirectResponse
                    encoded_object_name = quote(file_path, safe='/')
                    redirect_url = f"https://{oss_service.custom_domain}/{encoded_object_name}"
                    info(f"Redirect to OSS custom domain: {oss_service.custom_domain}", source="api")
                    return RedirectResponse(url=redirect_url)
                else:
                    # OSS 默认域名会强制 attachment 下载，无法在 iframe 中预览。
                    # 通过后端从 OSS 下载并返回，设置 inline 头实现预览。
                    try:
                        total_size = int(oss_service.bucket.head_object(file_path).content_length or 0)
                        parsed_range = _parse_http_range(range_header, total_size) if _is_audio_like_file_type(file_type) else None
                        if parsed_range:
                            start, end = parsed_range
                            oss_result = oss_service.bucket.get_object(file_path, byte_range=(start, end))
                            content = oss_result.read()
                            from mimetypes import guess_type
                            from urllib.parse import quote
                            media_type = guess_type(filename or "")[0] or "audio/mpeg"
                            encoded_fn = quote(filename or "audio")
                            return StreamingResponse(
                                iter([content]),
                                status_code=status.HTTP_206_PARTIAL_CONTENT,
                                media_type=media_type,
                                headers={
                                    "Accept-Ranges": "bytes",
                                    "Content-Range": f"bytes {start}-{end}/{total_size}",
                                    "Content-Length": str(end - start + 1),
                                    # 与整段响应一致：attachment 会导致部分浏览器在 Range 时无法稳定播放
                                    "Content-Disposition": f"inline; filename*=UTF-8''{encoded_fn}",
                                }
                            )
                        oss_result = oss_service.bucket.get_object(file_path)
                        content = oss_result.read()
                        resp = _serve_file_bytes_response(content, filename, file_type)
                        resp.headers["Accept-Ranges"] = "bytes"
                        return resp
                    except Exception as oss_err:
                        error(f"OSS file read failed: {str(oss_err)}", source="api", file_id=file_id)
                        raise HTTPException(
                            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail="OSS file access failed"
                        )
            else:
                error(f"OSS not configured, but file is in OSS: file_id={file_id}")
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail="OSS service not configured"
                )
        
        # 兼容旧 ASR 记录：original_file_path 存的是完整 OSS URL，且 file_storage_type 为空被当作 local
        if (
            storage_type == "local"
            and file_path
            and file_path.startswith(("http://", "https://"))
        ):
            legacy_key = _oss_object_key_from_url(file_path)
            if legacy_key:
                oss_legacy = OSSService()
                if oss_legacy.is_configured():
                    last_err = None
                    for ob in _legacy_oss_buckets_to_try(file_path, oss_bucket, oss_legacy):
                        try:
                            total_size = int(ob.head_object(legacy_key).content_length or 0)
                            parsed_range = _parse_http_range(range_header, total_size) if _is_audio_like_file_type(file_type) else None
                            if parsed_range:
                                start, end = parsed_range
                                oss_result = ob.get_object(legacy_key, byte_range=(start, end))
                                content = oss_result.read()
                                from mimetypes import guess_type
                                from urllib.parse import quote
                                media_type = guess_type(filename or "")[0] or "audio/mpeg"
                                encoded_fn = quote(filename or "audio")
                                return StreamingResponse(
                                    iter([content]),
                                    status_code=status.HTTP_206_PARTIAL_CONTENT,
                                    media_type=media_type,
                                    headers={
                                        "Accept-Ranges": "bytes",
                                        "Content-Range": f"bytes {start}-{end}/{total_size}",
                                        "Content-Length": str(end - start + 1),
                                        "Content-Disposition": f"inline; filename*=UTF-8''{encoded_fn}",
                                    }
                                )
                            oss_result = ob.get_object(legacy_key)
                            content = oss_result.read()
                            info(
                                f"兼容旧数据：从 URL 解析 OSS object key 并返回文件流 file_id={file_id}",
                                source="api",
                            )
                            resp = _serve_file_bytes_response(content, filename, file_type)
                            resp.headers["Accept-Ranges"] = "bytes"
                            return resp
                        except Exception as e:
                            last_err = e
                            continue
                    if last_err is not None:
                        warning(
                            f"兼容模式 OSS 读取失败（已依次尝试 URL 主机 bucket、DB oss_bucket、默认 bucket）: {last_err}",
                            source="api",
                            file_id=file_id,
                        )

        # 本地文件：检查文件是否存在
        if not file_path or not os.path.exists(file_path):
            if isinstance(file_path, str) and file_path.startswith(("http://", "https://")):
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=(
                        "Original file is not available in storage (OSS object not found or removed, "
                        f"or URL outdated). file_id={file_id}"
                    ),
                )
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="File not found",
            )
        
        # 根据文件类型设置MIME type（支持非音频文件预览：pdf/doc/docx/excel/xlsx/xls/csv 等）
        import mimetypes as _mimetypes

        mime_types = {
            'pdf': 'application/pdf',
            'excel': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            'xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            'xls': 'application/vnd.ms-excel',
            'csv': 'text/csv',  # CSV 使用 text/csv
            'doc': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
            'docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
            'word': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
            'image': 'image/jpeg',
            'jpg': 'image/jpeg',
            'jpeg': 'image/jpeg',
            'png': 'image/png',
            'gif': 'image/gif',
            'bmp': 'image/bmp',
            'audio': 'audio/mpeg',
            'podcast': 'audio/mpeg',
            'web': 'text/html',
        }

        ft_lower = (file_type or "").lower()
        guessed_mt, _ = _mimetypes.guess_type(filename or "")
        media_type = mime_types.get(ft_lower, 'application/octet-stream')
        if _is_audio_like_file_type(file_type):
            media_type = guessed_mt or media_type
            if not media_type or media_type == 'application/octet-stream':
                media_type = 'audio/mpeg'

        # 对文件名进行URL编码以支持中文等非ASCII字符
        # 使用RFC 2231标准的filename*参数
        from urllib.parse import quote
        encoded_filename = quote(filename)

        # 使用FileResponse直接返回文件
        # PDF/图片/音频：inline，便于浏览器内预览与 <audio> 播放；其余默认 attachment
        if ft_lower in ['pdf', 'jpg', 'jpeg', 'png', 'gif', 'bmp', 'image', 'audio', 'podcast', 'web']:
            # inline模式 - 浏览器直接显示
            return FileResponse(
                path=file_path,
                media_type=media_type,
                headers={
                    "Content-Disposition": f"inline; filename*=UTF-8''{encoded_filename}"
                }
            )
        else:
            # attachment模式 - 浏览器下载
            return FileResponse(
                path=file_path,
                media_type=media_type,
                headers={
                    "Content-Disposition": f"attachment; filename*=UTF-8''{encoded_filename}"
                }
            )
        
    except HTTPException:
        raise
    except Exception as e:
        error(f"Provide file failed: {str(e)}", source="api", file_id=file_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Provide file failed"
        )


@router.get("/sectors", summary="Get all industry categories")
async def get_sectors(current_user=Depends(get_current_user)):
    """
    获取所有行业分类（sector）及其包含的子行业（industry）
    """
    try:
        db_conn_params = get_database_config()
        db_manager = DatabaseManager(db_conn_params)
        
        # 查询所有sector和industry的组合
        query = """
        SELECT DISTINCT 
            sector, 
            industry,
            COUNT(*) as company_count
        FROM companies 
        WHERE sector IS NOT NULL 
        AND sector != '' 
        AND industry IS NOT NULL 
        AND industry != ''
        AND ticker_active = true
        GROUP BY sector, industry
        ORDER BY sector, company_count DESC
        """
        
        results = db_manager.fetch_all(query)
        
        # 按sector分组整理数据
        sectors_dict = {}
        for row in results:
            sector = row[0]
            industry = row[1]
            company_count = row[2]
            
            if sector not in sectors_dict:
                sectors_dict[sector] = {
                    "value": sector,
                    "label": sector,
                    "children": []
                }
            
            sectors_dict[sector]["children"].append({
                "value": industry,
                "label": f"{industry} ({company_count})"
            })
        
        # 转换为列表并按sector名称排序
        sectors = list(sectors_dict.values())
        sectors.sort(key=lambda x: x["label"])
        
        return sectors
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Get industry classification failed: {str(e)}")


@router.get("/tickers", summary="Search tickers")
async def search_tickers(
    sector: Optional[str] = None,
    industry: Optional[str] = None,
    search: Optional[str] = None,
    current_user=Depends(get_current_user)
):
    """
    根据行业分类和搜索关键词搜索股票代码
    """
    try:
        db_conn_params = get_database_config()
        db_manager = DatabaseManager(db_conn_params)
        
        # 构建查询条件
        where_conditions = ["ticker_active = true"]
        params = []
        
        if sector:
            where_conditions.append("sector = %s")
            params.append(sector)
        
        if industry:
            where_conditions.append("industry = %s")
            params.append(industry)
        
        if search:
            where_conditions.append("(code ILIKE %s OR name ILIKE %s)")
            params.extend([f"%{search}%", f"%{search}%"])
        
        where_clause = " AND ".join(where_conditions)
        
        # 添加排序逻辑
        order_by_clause = "ORDER BY name"
        if search:
            order_by_clause = f"""
            ORDER BY 
                CASE 
                    WHEN UPPER(code) = UPPER(%s) THEN 1
                    WHEN UPPER(name) = UPPER(%s) THEN 2
                    WHEN UPPER(code) LIKE UPPER(%s) THEN 3
                    WHEN UPPER(name) LIKE UPPER(%s) THEN 4
                    ELSE 5
                END,
                name
            """
            params.extend([search.upper(), search.upper(), f"%{search}%", f"%{search}%"])
        
        query = f"""
        SELECT 
            id,
            code,
            name,
            exchange,
            sector,
            industry
        FROM companies 
        WHERE {where_clause}
        {order_by_clause}
        LIMIT 100
        """
        
        results = db_manager.fetch_all(query, params)
        
        tickers = []
        for row in results:
            ticker = {
                "id": row[0],
                "code": row[1],
                "name": row[2],
                "exchange": row[3],
                "sector": row[4],
                "industry": row[5],
                "label": f"{row[1]} - {row[2]} ({row[3]})",
                "value": row[1]
            }
            tickers.append(ticker)
        
        return tickers
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Search tickers failed: {str(e)}"
        )


# ===========================================
# 音频文件上传 API（用于 ASR 语音识别）
# ===========================================

@router.post("/upload-audio", summary="Upload audio file and process asynchronously")
async def upload_audio(
    file: UploadFile = File(...),
    auto_analyze_template_id: Optional[int] = Form(None),
    auto_analyze_template_variables: str = Form('{}'),  # JSON string
    auto_analyze_enabled: bool = Form(True),
    # PR-5: 上传弹窗 file_tagging picker
    auto_tag_template_id: Optional[int] = Form(None),
    auto_tag_template_variables: str = Form('{}'),  # JSON string
    auto_tag_enabled: bool = Form(True),
    current_user=Depends(get_current_user)
):
    """
    上传音频文件到阿里云 OSS 并异步进行语音识别
    
    ⚠️ 重要：此端点使用异步 Celery 任务处理，不会阻塞主线程
    - 适合处理大文件（200MB+）
    - 立即返回任务 ID，不等待上传和识别完成
    - 前端通过轮询或 WebSocket 获取进度
    
    支持的音频格式：
    - aac, amr, avi, flac, flv, m4a, mkv, mov
    - mp3, mp4, mpeg, ogg, opus, wav, webm, wma, wmv
    
    文件要求：
    - 最大文件大小：2GB
    - 最长时长：12小时
    
    返回：
    - task_id: Celery 任务 ID（用于查询进度）
    - filename: 文件名
    - file_size: 文件大小（字节）
    """
    try:
        # 检查文件名
        if not file.filename:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="File name is empty"
            )
        
        # 初始化 OSS 服务（只做基本检查）
        oss_service = OSSService()
        
        # 检查 OSS 是否配置
        if not oss_service.is_configured():
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="OSS service not configured. Please configure OSS_ACCESS_KEY_ID and OSS_ACCESS_KEY_SECRET in the .env file"
            )
        
        # 检查文件格式
        if not oss_service.is_audio_file(file.filename):
            ext = os.path.splitext(file.filename)[1]
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Unsupported audio format: {ext}. Supported formats: {', '.join(sorted(OSSService.SUPPORTED_AUDIO_FORMATS))}"
            )
        
        # 使用流式写入临时文件，避免一次性加载整个文件到内存
        temp_file_id = str(uuid.uuid4())[:8]
        name, ext = os.path.splitext(file.filename)
        temp_filename = f"{name}_{temp_file_id}{ext}"
        temp_file_path = os.path.join(UPLOAD_DIR, temp_filename)
        
        # 流式写入文件（分块读取，不占用大量内存）
        file_size = 0
        chunk_size = 1024 * 1024  # 1MB chunks
        
        with open(temp_file_path, "wb") as buffer:
            while True:
                chunk = await file.read(chunk_size)
                if not chunk:
                    break
                buffer.write(chunk)
                file_size += len(chunk)
        
        # 验证文件大小
        if file_size > OSSService.MAX_FILE_SIZE:
            # 删除临时文件
            if os.path.exists(temp_file_path):
                os.remove(temp_file_path)
            
            size_gb = file_size / (1024 * 1024 * 1024)
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Audio file too large: {size_gb:.2f}GB. Maximum supported size is 2GB"
            )
        
        info(f"Audio file saved to temporary directory: {file.filename} ({file_size / (1024 * 1024):.2f} MB)", 
             source="api", user=current_user.username)

        # 解析模板变量 JSON
        try:
            audio_template_vars = json.loads(auto_analyze_template_variables) if auto_analyze_template_variables else {}
        except json.JSONDecodeError:
            audio_template_vars = {}

        # PR-5: 解析 file_tagging picker 的模板变量
        try:
            audio_tag_template_vars = json.loads(auto_tag_template_variables) if auto_tag_template_variables else {}
        except json.JSONDecodeError:
            audio_tag_template_vars = {}

        # 提交异步任务到 Celery
        task = upload_and_transcribe_audio_task.apply_async(
            args=[temp_file_path, file.filename, current_user.username],
            kwargs={
                'model': 'fun-asr',
                'language_hints': ['zh', 'en'],
                'diarization_enabled': True,
                'auto_analyze_template_id': auto_analyze_template_id,
                'auto_analyze_template_variables': audio_template_vars,
                'auto_analyze_enabled': auto_analyze_enabled,
                # PR-5: file_tagging picker 透传到 Celery 任务
                'auto_tag_template_id': auto_tag_template_id,
                'auto_tag_template_variables': audio_tag_template_vars,
                'auto_tag_enabled': auto_tag_enabled,
            },
            queue='file_processing'
        )
        
        info(f"Audio upload task submitted: task_id={task.id}, file={file.filename}", 
             source="api", user=current_user.username)
        
        # 立即返回任务 ID，不等待完成
        return {
            "success": True,
            "message": "Audio file received, processing in background",
            "task_id": task.id,
            "filename": file.filename,
            "file_size": file_size,
            "file_size_mb": round(file_size / (1024 * 1024), 2),
            "status": "processing",
            "note": "Please use task_id to query processing progress (GET /api/files/tasks/{task_id}/status)"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        error(f"Upload audio file failed: {str(e)}", source="api", user=current_user.username)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Upload audio file failed: {str(e)}"
        )


@router.post("/upload-and-process-document", summary="Upload document file and process asynchronously (recommended, avoid OSS slow connection blocking)")
async def upload_and_process_document(
    file: UploadFile = File(...),
    template_id: Optional[int] = Form(None),
    template_variables: str = Form('{}'),  # JSON string
    auto_analyze_enabled: bool = Form(True),
    custom_inputs: str = Form('{}'),  # JSON string
    params: str = Form('{}'),  # JSON string (sector, industry, ticker, etc.)
    # PR-5: 上传弹窗用户选择的 file_tagging 模板 (可选)
    auto_tag_template_id: Optional[int] = Form(None),
    auto_tag_template_variables: str = Form('{}'),  # JSON string
    auto_tag_enabled: bool = Form(True),
    current_user=Depends(get_current_user)
):
    """
    上传文档文件到 OSS 并异步处理（LlamaParse + AI 分析）
    
    ⚠️ 推荐使用此端点：OSS 服务器在香港，直接上传会阻塞请求
    - 使用 Celery 异步任务，立即返回 task_id
    - 后台上传到 OSS + 可选 AI 分析
    - 不阻塞主线程
    
    支持的文档格式：
    - PDF, Word (doc/docx), Excel (xls/xlsx), PowerPoint (ppt/pptx)
    - Text (txt, md), Images (jpg, png, gif, bmp, webp)
    
    参数：
    - file: 文件（必需）
    - template_id: 上传完成后自动分析时使用的模板 ID（可选）
    - template_variables: 模板变量（JSON 字符串）
    - custom_inputs: 自定义输入（JSON 字符串）
    - params: 处理参数，包含 sector/industry/ticker 等（JSON 字符串）
    
    返回：
    - task_id: Celery 任务 ID（用于查询进度）
    - filename: 文件名
    - file_size: 文件大小（字节）
    """
    try:
        # 检查文件名
        if not file.filename:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="File name is empty"
            )
        
        # 检查 OSS 配置
        oss_service = OSSService()
        if not oss_service.is_configured():
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="OSS service not configured. Please configure OSS_ACCESS_KEY_ID and OSS_ACCESS_KEY_SECRET in the .env file"
            )
        
        # 检查文件格式
        file_processor = FileProcessor()
        if not file_processor.is_supported_file(file.filename):
            ext = os.path.splitext(file.filename)[1]
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Unsupported file type: {ext}"
            )
        
        # 解析 JSON 参数
        try:
            template_vars = json.loads(template_variables) if template_variables else {}
            custom_inputs_dict = json.loads(custom_inputs) if custom_inputs else {}
            params_dict = json.loads(params) if params else {}
            _strip_llamaindex_key_from_client_params(params_dict)
            params_dict['analysis_mode'] = 'local_llm'
            # PR-5: 上传弹窗 file_tagging picker 的模板变量
            auto_tag_template_vars = json.loads(auto_tag_template_variables) if auto_tag_template_variables else {}
        except json.JSONDecodeError as e:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"JSON parameter parsing failed: {str(e)}"
            )
        
        # 流式写入临时文件
        temp_file_id = str(uuid.uuid4())[:8]
        name, ext = os.path.splitext(file.filename)
        temp_filename = f"{name}_{temp_file_id}{ext}"
        temp_file_path = os.path.join(UPLOAD_DIR, temp_filename)
        
        file_size = 0
        chunk_size = 1024 * 1024  # 1MB chunks
        
        with open(temp_file_path, "wb") as buffer:
            while True:
                chunk = await file.read(chunk_size)
                if not chunk:
                    break
                buffer.write(chunk)
                file_size += len(chunk)
        
        info(f"Document file saved to temporary directory: {file.filename} ({file_size / (1024 * 1024):.2f} MB)", 
             source="api", user=current_user.username)
        
        # 组装任务信息（文档上传固定为 local_llm 管线；解析后主分析由 trigger_auto_analyze 等完成）
        task_info = {
            'analysis_mode': 'local_llm',
            'task_type': 'document_upload',
            'template_id': template_id,
            'template_variables': template_vars,
            # 方案 A: 上传弹窗 AI Analysis 开关
            'auto_analyze_enabled': auto_analyze_enabled,
            # PR-5: file_tagging picker 选择的模板, 透传到 Celery 任务,
            # 由 upload_and_process_document_task 末尾的 trigger_auto_tagging 使用.
            'auto_tag_template_id': auto_tag_template_id,
            'auto_tag_template_variables': auto_tag_template_vars,
            # 方案 A: 上传弹窗 AI Tagging 开关
            'auto_tag_enabled': auto_tag_enabled,
        }

        # 提交异步任务到 Celery
        task = upload_and_process_document_task.apply_async(
            args=[temp_file_path, file.filename, current_user.username],
            kwargs={
                'task_info': task_info,
                'custom_inputs': custom_inputs_dict,
                'params': params_dict
            },
            queue='file_processing'
        )
        
        info(f"Document upload and processing task submitted: task_id={task.id}, file={file.filename}", 
             source="api", user=current_user.username)
        
        # 立即返回任务 ID
        return {
            "success": True,
            "message": "Document file received, processing in background",
            "task_id": task.id,
            "filename": file.filename,
            "file_size": file_size,
            "file_size_mb": round(file_size / (1024 * 1024), 2),
            "status": "processing",
            "note": "Please use task_id to query processing progress (GET /api/files/tasks/{task_id}/status)"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        error(f"Upload document file failed: {str(e)}", source="api", user=current_user.username)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Upload document file failed: {str(e)}"
        )


@router.post("/upload-web-clip", summary="Save web page clip from Chrome extension")
async def upload_web_clip(
    request: WebClipRequest,
    current_user = Depends(get_current_user)
):
    """
    接收 Chrome Extension 发送的网页剪藏内容，上传到 OSS 并异步提取文本。
    """
    import re as _re
    import html as _html

    try:
        oss_service = OSSService()
        if not oss_service.is_configured():
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="OSS service not configured"
            )

        title = request.title.strip()
        source_url = request.source_url.strip()
        html_content = request.html

        # 构建完整 HTML 文档
        escaped_title = _html.escape(title)
        escaped_url = _html.escape(source_url)
        full_html = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{escaped_title}</title>
<meta name="source-url" content="{escaped_url}">
<style>
html {{ color-scheme: light dark; }}
body {{ max-width: 860px; margin: 0 auto; padding: 24px; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; line-height: 1.7; color: #333; background: #fff; }}
img {{ max-width: 100%; height: auto; border-radius: 4px; }}
a {{ color: #1677ff; }}
@media (prefers-color-scheme: dark) {{
  body {{ background: #141414; color: #e8eaed; }}
  a {{ color: #69b1ff; }}
  .web-clip-source {{ color: #a0a3ad !important; }}
}}
</style>
</head>
<body>
<p class="web-clip-source" style="font-size:12px;color:#999;margin-bottom:16px;">
  Source: <a href="{escaped_url}" target="_blank" rel="noopener noreferrer">{escaped_url}</a>
</p>
{html_content}
</body>
</html>"""

        # Slugify title for filename
        slug = _re.sub(r'[^\w\u4e00-\u9fff\-]', '', title.replace(' ', '-'))
        slug = slug[:80] or 'web-clip'
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{slug}_{timestamp}.html"

        # Upload to OSS
        date_str = datetime.now().strftime("%Y-%m-%d")
        oss_object_name = f"{oss_service.web_clip_prefix}{current_user.username}/{date_str}/{filename}"

        html_bytes = full_html.encode('utf-8')
        upload_result = oss_service.upload_bytes(
            object_name=oss_object_name,
            data=html_bytes,
            content_type='text/html; charset=utf-8',
            content_disposition='inline'
        )

        if not upload_result["success"]:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"OSS upload failed: {upload_result.get('error')}"
            )

        file_size_mb = len(html_bytes) / (1024 * 1024)

        # Save to DB
        db_conn_params = get_database_config()
        db_manager = DatabaseManager(db_conn_params)
        conn = db_manager.get_connection()
        if not conn:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Database connection failed"
            )

        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO processing_stats
            (timestamp, files_processed, files_failed, total_time,
             total_file_processing_time, total_ai_time, task_name,
             "user", total_tokens, total_price, upload_params,
             final_report, rc_agent_status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (
            datetime.now(), 1, 0, 0, 0, 0,
            'Web Clip',
            current_user.username,
            0, 0,
            json.dumps({
                "source_url": source_url,
                "title": title,
                # 扩展端元数据 (2026-11-22 起): 供 celery 转投 subscription_content 使用
                "published_time": request.published_time,
                "byline": request.byline,
                "site_name": request.site_name,
            }),
            '', 'processing'
        ))
        processing_stats_id = cursor.fetchone()[0]

        cursor.execute("""
            INSERT INTO file_processing_details
            (processing_stats_id, filename, file_type, file_size_mb,
             processing_time, ai_time, content_length, status,
             total_tokens, cost, content, created_at, task_id,
             original_file_path, file_storage_type, oss_bucket)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (
            processing_stats_id,
            filename,
            'web',
            file_size_mb,
            0, 0, 0,
            'processing',
            0, 0, '',
            utcnow(),  # created_at (aware UTC; psycopg2 adapter -> naive UTC)
            '',
            oss_object_name,
            'oss',
            upload_result.get("bucket")
        ))
        file_detail_id = cursor.fetchone()[0]

        conn.commit()
        conn.close()

        # Enqueue async text extraction task
        task = process_web_clip_task.apply_async(
            args=[file_detail_id, processing_stats_id, full_html, current_user.username],
            queue='file_processing'
        )

        # Update task_id in DB
        conn2 = db_manager.get_connection()
        if conn2:
            cursor2 = conn2.cursor()
            cursor2.execute(
                "UPDATE file_processing_details SET task_id = %s WHERE id = %s",
                (task.id, file_detail_id)
            )
            conn2.commit()
            conn2.close()

        info(f"Web clip saved: {filename}, file_id={file_detail_id}, task_id={task.id}",
             source="api", user=current_user.username)

        return {
            "success": True,
            "file_id": file_detail_id,
            "task_id": task.id,
            "filename": filename,
            "title": title,
            "source_url": source_url,
            "file_size_mb": round(file_size_mb, 2),
            "status": "processing",
        }

    except HTTPException:
        raise
    except Exception as e:
        error(f"Web clip upload failed: {str(e)}", source="api", user=current_user.username)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Web clip upload failed: {str(e)}"
        )


@router.post("/upload-web-link", summary="Scrape web page by URL")
async def upload_web_link(
    request: WebLinkRequest,
    current_user = Depends(get_current_user)
):
    """
    接收 URL，服务端抓取网页正文 + 内嵌图片，存储到 OSS 并提取文本。
    功能等同于 Chrome Extension 的 Web Clipper，但无需安装浏览器扩展，
    适用于移动端和任意浏览器。仅支持公开（无需登录）的网页。
    """
    from backend.services.web_scraper_service import WebScraperService
    from backend.tasks.file_tasks import scrape_and_process_web_link_task

    try:
        url = request.url.strip()
        if not WebScraperService.validate_url(url):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid URL. Please provide a valid HTTP/HTTPS link."
            )

        username = current_user.username if hasattr(current_user, 'username') else 'guest'

        info(f"Received Web Link: url={url}, user={username}", source="api")

        task = scrape_and_process_web_link_task.apply_async(
            args=(url, username),
            kwargs={
                'auto_analyze_template_id': request.auto_analyze_template_id,
                'auto_analyze_template_variables': request.auto_analyze_template_variables or {},
                'auto_analyze_enabled': request.auto_analyze_enabled,
                # PR-5: file_tagging picker
                'auto_tag_template_id': request.auto_tag_template_id,
                'auto_tag_template_variables': request.auto_tag_template_variables or {},
                'auto_tag_enabled': request.auto_tag_enabled,
            },
            queue='file_processing'
        )

        info(f"Web Link task started: task_id={task.id}, url={url}, "
             f"auto_analyze_template_id={request.auto_analyze_template_id}", source="api")

        return {
            "task_id": task.id,
            "url": url,
            "message": "Web page scraping started. You can track the progress in the task status."
        }

    except HTTPException:
        raise
    except Exception as e:
        error(f"Web Link upload failed: {str(e)}", source="api")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Web Link processing failed: {str(e)}"
        )


@router.get("/audio/supported-formats", summary="Get supported audio formats")
async def get_supported_audio_formats():
    """
    获取支持的音频格式列表（用于 Fun-ASR）
    """
    return {
        "supported_formats": sorted(list(OSSService.SUPPORTED_AUDIO_FORMATS)),
        "max_file_size_gb": 2,
        "max_duration_hours": 12,
        "description": "Supported audio formats (Fun-ASR compatible)"
    }


@router.post("/audio/transcribe", summary="Speech recognition - Fun-ASR")
async def transcribe_audio(
    request: Dict[str, Any],
    current_user=Depends(get_current_user)
):
    """
    Call Fun-ASR for speech recognition
    
    请求体:
    {
        "audio_url": "https://...",  # Required: Public URL of the audio file
        "model": "fun-asr",           # Optional: Model name, default fun-asr
        "language_hints": ["zh", "en"],  # Optional: Language hints
        "wait_for_result": true,      # Optional: Whether to wait for result, default true
        "diarization_enabled": true   # Optional: Whether to enable speaker diarization, default true
    }
    
    返回:
    {
        "success": true,
        "task_id": "xxx",
        "status": "COMPLETED",
        "transcriptions": [...],  # Detailed transcription result (includes speaker information)
        "text": "Transcription text",       # Merged text
        "formatted_text": "Formatted text"  # Formatted text including timestamps and speakers
    }
    """
    try:
        # 获取请求参数
        audio_url = request.get('audio_url')
        model = request.get('model', 'fun-asr')
        language_hints = request.get('language_hints', ['zh', 'en'])
        wait_for_result = request.get('wait_for_result', True)
        diarization_enabled = request.get('diarization_enabled', True)
        
        # 验证必填参数
        if not audio_url:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Missing required parameter: audio_url"
            )
        
        info(f"Start speech recognition: audio_url={audio_url}, model={model}", 
             source="api", user=current_user.username)
        
        # 初始化 ASR 服务
        asr_service = ASRService()
        
        # 检查 ASR 服务是否配置
        if not asr_service.is_configured():
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="ASR service not configured. Please set DASHSCOPE_API_KEY environment variable and install dashscope SDK (pip install dashscope)"
            )
        
        # 调用语音识别
        result = asr_service.transcribe_audio(
            audio_url=audio_url,
            model=model,
            language_hints=language_hints,
            wait_for_result=wait_for_result,
            diarization_enabled=diarization_enabled
        )
        
        if not result["success"]:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Speech recognition failed: {result.get('error', 'Unknown error')}"
            )
        
        # 格式化结果：API 返回用完整格式，写入 DB 用仅带时间戳的格式
        formatted_texts = []
        content_for_db_parts = []
        if result.get("transcriptions"):
            for trans_data in result["transcriptions"]:
                formatted = asr_service.format_transcription_result(
                    trans_data,
                    include_timestamps=True,
                    include_words=False,
                    include_speaker=diarization_enabled
                )
                formatted_texts.append(formatted)
                part = asr_service.format_transcription_result(
                    trans_data,
                    include_timestamps=True,
                    include_words=False,
                    include_speaker=diarization_enabled,
                    timestamped_only=True
                )
                if part:
                    content_for_db_parts.append(part)
        
        final_report_text = "\n\n".join(formatted_texts) if formatted_texts else result.get("text", "")
        content_for_db = "\n".join(content_for_db_parts) if content_for_db_parts else result.get("text", "")
        
        # 构建结构化的 ASR 数据（用于前端可视化）
        asr_structured_data = None
        if result.get("transcriptions") and diarization_enabled:
            try:
                asr_structured_data = _build_asr_structured_data(result["transcriptions"])
            except Exception as e:
                warning(f"Build structured ASR data failed: {str(e)}", source="api", user=current_user.username)
        
        # 记录到数据库（可选）
        try:
            db_conn_params = get_database_config()
            db_manager = DatabaseManager(db_conn_params)
            conn = db_manager.get_connection()
            
            if conn:
                cursor = conn.cursor()
                
                # 创建 processing_stats 记录
                cursor.execute("""
                INSERT INTO processing_stats 
                (timestamp, files_processed, files_failed, total_time, 
                 total_file_processing_time, total_ai_time, task_name, 
                 "user", total_tokens, total_price, upload_params, 
                 final_report, rc_agent_status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """, (
                    datetime.now(),
                    1 if result["success"] else 0,
                    0 if result["success"] else 1,
                    0,
                    0,
                    0,
                    'ASR Transcription',
                    current_user.username,
                    0,
                    0,
                    json.dumps({
                        "audio_url": audio_url,
                        "model": model,
                        "language_hints": language_hints,
                        "diarization_enabled": diarization_enabled
                    }),
                    '',  # final_report 保留为空，等待未来的 AI Agent 填充总结内容
                    'completed' if result["success"] else 'failed'
                ))
                
                processing_stats_id = cursor.fetchone()[0]
                
                # 从 audio_url 中提取原始文件名（处理 URL 编码和查询参数）
                original_filename = _extract_filename_from_audio_url(audio_url)
                
                # 创建 file_processing_details 记录
                cursor.execute("""
                INSERT INTO file_processing_details 
                (processing_stats_id, filename, file_type, file_size_mb, 
                 processing_time, ai_time, content_length, status, 
                 total_tokens, cost, content, created_at, task_id, original_file_path, asr_with_diarization)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    processing_stats_id,
                    original_filename,  # 使用提取的原始文件名，而不是 URL
                    'audio',
                    0,
                    0,
                    0,
                    len(content_for_db),
                    'completed' if result["success"] else 'failed',
                    0,
                    0,
                    content_for_db,  # 仅保存带时间戳的转写结果，每段一行
                    utcnow(),  # created_at (aware UTC; psycopg2 adapter -> naive UTC)
                    result.get("task_id"),
                    audio_url,
                    json.dumps(asr_structured_data) if asr_structured_data else None
                ))
                
                conn.commit()
                conn.close()
                
                info(f"ASR result recorded to database", source="api", user=current_user.username)
        except Exception as db_error:
            warning(f"Record ASR result to database failed: {str(db_error)}", source="api", user=current_user.username)
        
        # 返回结果
        response_data = {
            "success": result["success"],
            "task_id": result["task_id"],
            "status": result["status"],
            "text": result.get("text", ""),
            "formatted_text": "\n\n".join(formatted_texts) if formatted_texts else "",
            "transcriptions": result.get("transcriptions", [])
        }
        
        info(f"Speech recognition completed: task_id={result['task_id']}, text_length={len(result.get('text', ''))}", 
             source="api", user=current_user.username)
        
        return response_data
        
    except HTTPException:
        raise
    except Exception as e:
        error(f"Speech recognition failed: {str(e)}", source="api", user=current_user.username)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Speech recognition failed: {str(e)}"
        )


@router.post("/audio/transcribe-async", summary="Submit speech recognition task (asynchronous)")
async def transcribe_audio_async(
    request: Dict[str, Any],
    current_user=Depends(get_current_user)
):
    """
    提交语音识别任务（异步，不等待结果）
    
    请求体:
    {
        "audio_url": "https://...",
        "model": "fun-asr",
        "language_hints": ["zh", "en"],
        "diarization_enabled": true  # 可选：是否启用说话人分离，默认 true
    }
    
    返回:
    {
        "success": true,
        "task_id": "xxx",
        "message": "任务已提交"
    }
    """
    try:
        audio_url = request.get('audio_url')
        model = request.get('model', 'fun-asr')
        language_hints = request.get('language_hints', ['zh', 'en'])
        diarization_enabled = request.get('diarization_enabled', True)
        
        if not audio_url:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Missing required parameter: audio_url"
            )
        
        # 初始化 ASR 服务
        asr_service = ASRService()
        
        if not asr_service.is_configured():
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="ASR service not configured"
            )
        
        # 提交任务（不等待结果）
        result = asr_service.submit_transcription_task(
            audio_url=audio_url,
            model=model,
            language_hints=language_hints,
            diarization_enabled=diarization_enabled
        )
        
        if not result["success"]:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Submit task failed: {result.get('error')}"
            )
        
        return {
            "success": True,
            "task_id": result["task_id"],
            "message": "Speech recognition task submitted, please use task_id to query results"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Submit task failed: {str(e)}"
        )


@router.get("/audio/transcribe/{task_id}", summary="Query speech recognition result")
async def get_transcription_result(
    task_id: str,
    current_user=Depends(get_current_user)
):
    """
    查询语音识别任务结果
    
    返回:
    {
        "success": true,
        "task_id": "xxx",
        "status": "COMPLETED",
        "transcriptions": [...],
        "text": "转写文本"
    }
    """
    try:
        # 初始化 ASR 服务
        asr_service = ASRService()
        
        if not asr_service.is_configured():
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="ASR service not configured"
            )
        
        # 获取结果
        result = asr_service.get_transcription_results(task_id)
        
        if not result["success"]:
            # 如果任务还在进行中，返回状态
            if result["status"] in ["PENDING", "RUNNING"]:
                return {
                    "success": True,
                    "task_id": task_id,
                    "status": result["status"],
                    "message": "Task is processing, please query later"
                }
            else:
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Get result failed: {result.get('error')}"
                )
        
        # 格式化结果（包含说话人信息）
        formatted_texts = []
        if result.get("transcriptions"):
            for trans_data in result["transcriptions"]:
                formatted = asr_service.format_transcription_result(
                    trans_data,
                    include_timestamps=True,
                    include_words=False,
                    include_speaker=True  # 总是包含说话人信息（如果有）
                )
                formatted_texts.append(formatted)
        
        # 提取文本
        texts = []
        for trans in result.get("transcriptions", []):
            for transcript in trans.get('transcripts', []):
                text = transcript.get('text', '')
                if text:
                    texts.append(text)
        
        return {
            "success": True,
            "task_id": task_id,
            "status": result["status"],
            "text": "\n".join(texts),
            "formatted_text": "\n\n".join(formatted_texts),
            "transcriptions": result.get("transcriptions", [])
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Query result failed: {str(e)}"
        )


@router.post("/audio/analyze", summary="Audio AI analysis - analyze ASR transcription content")
async def analyze_audio(
    request: Dict[str, Any],
    current_user=Depends(get_current_user)
):
    """
    对已完成语音识别的音频文件进行AI分析。
    
    从 file_processing_details.asr_with_diarization 读取ASR数据，
    使用指定的 Prompt Template（默认 audio_analysis_v1）进行分析，
    分析结果保存到 processing_stats.final_report。
    
    请求体:
    {
        "execution_id": 123,           # 必填：processing_stats.id
        "template_id": 32,             # 可选：模板ID（默认使用 audio_analysis_v1）
        "template_variables": {},       # 可选：额外模板变量
    }
    
    返回:
    {
        "success": true,
        "task_id": "celery-task-id",
        "message": "音频分析任务已提交"
    }
    """
    try:
        execution_id = request.get('execution_id')
        template_id = request.get('template_id')
        template_variables = request.get('template_variables', {})
        
        if not execution_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Missing required parameter: execution_id"
            )
        
        # 获取数据库连接
        db_conn_params = get_database_config()
        db_manager = DatabaseManager(db_conn_params)
        conn = db_manager.get_connection()
        
        if not conn:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Cannot connect to database"
            )
        
        cursor = conn.cursor()
        
        try:
            # 验证 execution 存在且属于当前用户
            cursor.execute("""
                SELECT id, "user", rc_agent_status, task_name
                FROM processing_stats
                WHERE id = %s
            """, (execution_id,))
            ps_row = cursor.fetchone()
            
            if not ps_row:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Execution record does not exist: {execution_id}"
                )
            
            if ps_row[1] != current_user.username:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Permission denied, only own records can be analyzed"
                )
            
            if ps_row[2] == 'analyzing':
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="This record is being analyzed, please wait for completion"
                )
            
            # 获取对应的 file_processing_details（音频文件）
            cursor.execute("""
                SELECT id, filename, file_type, asr_with_diarization, content
                FROM file_processing_details
                WHERE processing_stats_id = %s
                ORDER BY id ASC
                LIMIT 1
            """, (execution_id,))
            file_row = cursor.fetchone()
            
            if not file_row:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="No corresponding file record found"
                )
            
            file_detail_id = file_row[0]
            file_type = file_row[2]
            asr_data = file_row[3]
            content = file_row[4]
            
            # 验证有 ASR 数据或转写内容
            if not asr_data and not content:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="This file has no ASR data or transcription content, please first perform speech recognition"
                )
            
            # 如果未指定模板，使用默认的 audio_analysis_v1
            if not template_id:
                template_service = LLMPromptTemplateService()
                template = template_service.get_template_by_code('audio_analysis_v1')
                if template:
                    template_id = template['id']
                else:
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail="Default audio analysis template audio_analysis_v1 does not exist, please contact the administrator to create"
                    )
            
            # 更新状态为 analyzing
            cursor.execute("""
                UPDATE processing_stats
                SET rc_agent_status = 'analyzing'
                WHERE id = %s
            """, (execution_id,))
            conn.commit()
            
        finally:
            conn.close()
        
        # 提交 Celery 任务
        task = analyze_audio_task.delay(
            execution_id=execution_id,
            file_detail_id=file_detail_id,
            template_id=template_id,
            template_variables=template_variables,
            username=current_user.username
        )
        
        info(
            f"Audio AI analysis task submitted: task_id={task.id}, execution_id={execution_id}",
            source="api", user=current_user.username
        )
        
        return {
            "success": True,
            "task_id": task.id,
            "execution_id": execution_id,
            "message": "Audio AI analysis task submitted, processing in background"
        }
    
    except HTTPException:
        raise
    except Exception as e:
        error(f"Submit audio analysis task failed: {str(e)}", source="api", user=current_user.username)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Submit audio analysis task failed: {str(e)}"
        )


@router.get("/audio/analyze/{task_id}/status", summary="Query audio AI analysis task status")
async def get_audio_analysis_status(
    task_id: str,
    current_user=Depends(get_current_user)
):
    """
    查询音频AI分析的Celery任务状态
    """
    try:
        task_result = AsyncResult(task_id, app=celery_app)
        
        if task_result.state == 'PENDING':
            return {
                "task_id": task_id,
                "status": "pending",
                "progress": 0,
                "message": "Task is waiting to be processed..."
            }
        elif task_result.state == 'PROGRESS':
            task_info = task_result.info or {}
            return {
                "task_id": task_id,
                "status": task_info.get("status", "processing"),
                "progress": task_info.get("progress", 0),
                "message": task_info.get("message", "Analyzing..."),
                "execution_id": task_info.get("execution_id"),
                "logs": task_info.get("logs", [])
            }
        elif task_result.state == 'SUCCESS':
            result = task_result.result or {}
            return {
                "task_id": task_id,
                "status": "completed",
                "progress": 100,
                "message": "Analysis completed",
                "execution_id": result.get("execution_id"),
                "total_tokens": result.get("total_tokens", 0),
                "total_price": result.get("total_price", 0),
                "llm_model": result.get("llm_model"),
                "llm_provider": result.get("llm_provider"),
                "logs": result.get("logs", [])
            }
        else:
            task_info = task_result.info or {}
            return {
                "task_id": task_id,
                "status": "failed",
                "progress": 0,
                "message": str(task_info.get("error", "Analysis failed")),
                "execution_id": task_info.get("execution_id"),
                "logs": task_info.get("logs", [])
            }
    
    except Exception as e:
        error(f"Get audio analysis status failed: {str(e)}", source="api", task_id=task_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Get analysis status failed"
        )


@router.post("/document/analyze", summary="Document AI analysis - analyze uploaded document")
async def analyze_document(
    request: Dict[str, Any],
    current_user=Depends(get_current_user)
):
    """
    对已上传但未经AI分析的文档进行AI分析。
    
    如果文档未经LlamaParse解析（content为空），会先自动解析，再进行AI分析。
    分析结果保存到 processing_stats.final_report。
    
    请求体:
    {
        "execution_id": 123,           # 必填：processing_stats.id
        "template_id": 32,             # 必填：模板ID
        "template_variables": {},       # 可选：额外模板变量
    }
    """
    try:
        execution_id = request.get('execution_id')
        template_id = request.get('template_id')
        template_variables = request.get('template_variables', {})
        
        if not execution_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Missing required parameter: execution_id"
            )
        
        if not template_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Missing required parameter: template_id (document analysis requires selecting a template)"
            )
        
        db_conn_params = get_database_config()
        db_manager = DatabaseManager(db_conn_params)
        conn = db_manager.get_connection()
        
        if not conn:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Cannot connect to database"
            )
        
        cursor = conn.cursor()
        
        try:
            # 验证 execution 存在且属于当前用户
            cursor.execute("""
                SELECT id, "user", rc_agent_status, task_name
                FROM processing_stats
                WHERE id = %s
            """, (execution_id,))
            ps_row = cursor.fetchone()
            
            if not ps_row:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Execution record does not exist: {execution_id}"
                )
            
            if ps_row[1] != current_user.username:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Permission denied, only own records can be analyzed"
                )
            
            if ps_row[2] == 'analyzing':
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="This record is being analyzed, please wait for completion"
                )
            
            # 获取对应的 file_processing_details（文档文件）
            cursor.execute("""
                SELECT id, filename, file_type, content, original_file_path
                FROM file_processing_details
                WHERE processing_stats_id = %s
                ORDER BY id ASC
                LIMIT 1
            """, (execution_id,))
            file_row = cursor.fetchone()
            
            if not file_row:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="No corresponding file record found"
                )
            
            file_detail_id = file_row[0]
            content = file_row[3]
            original_file_path = file_row[4]
            
            # 验证有内容或有原始文件可以解析
            if not content and not original_file_path:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="This file has no content and no original file path, cannot be analyzed"
                )
            
            # 更新状态为 analyzing
            cursor.execute("""
                UPDATE processing_stats
                SET rc_agent_status = 'analyzing'
                WHERE id = %s
            """, (execution_id,))
            conn.commit()
            
        finally:
            conn.close()
        
        # 提交 Celery 任务
        task = analyze_document_task.delay(
            execution_id=execution_id,
            file_detail_id=file_detail_id,
            template_id=template_id,
            template_variables=template_variables,
            username=current_user.username
        )
        
        info(
            f"Document AI analysis task submitted: task_id={task.id}, execution_id={execution_id}, template_id={template_id}",
            source="api", user=current_user.username
        )
        
        return {
            "success": True,
            "task_id": task.id,
            "execution_id": execution_id,
            "message": "Document AI analysis task submitted, processing in background"
        }
    
    except HTTPException:
        raise
    except Exception as e:
        error(f"Submit document analysis task failed: {str(e)}", source="api", user=current_user.username)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Submit document analysis task failed: {str(e)}"
        )


@router.get("/document/analyze/{task_id}/status", summary="Query document AI analysis task status")
async def get_document_analysis_status(
    task_id: str,
    current_user=Depends(get_current_user)
):
    """
    查询文档AI分析的Celery任务状态
    """
    try:
        task_result = AsyncResult(task_id, app=celery_app)
        
        if task_result.state == 'PENDING':
            return {
                "task_id": task_id,
                "status": "pending",
                "progress": 0,
                "message": "Task is waiting to be processed..."
            }
        elif task_result.state == 'PROGRESS':
            task_info = task_result.info or {}
            return {
                "task_id": task_id,
                "status": task_info.get("status", "processing"),
                "progress": task_info.get("progress", 0),
                "message": task_info.get("message", "正在分析..."),
                "execution_id": task_info.get("execution_id"),
                "logs": task_info.get("logs", [])
            }
        elif task_result.state == 'SUCCESS':
            result = task_result.result or {}
            return {
                "task_id": task_id,
                "status": "completed",
                "progress": 100,
                "message": "文档分析完成",
                "execution_id": result.get("execution_id"),
                "total_tokens": result.get("total_tokens", 0),
                "total_price": result.get("total_price", 0),
                "llm_model": result.get("llm_model"),
                "llm_provider": result.get("llm_provider"),
                "logs": result.get("logs", [])
            }
        else:
            task_info = task_result.info or {}
            return {
                "task_id": task_id,
                "status": "failed",
                "progress": 0,
                "message": str(task_info.get("error", "文档分析失败")),
                "execution_id": task_info.get("execution_id"),
                "logs": task_info.get("logs", [])
            }
    
    except Exception as e:
        error(f"Get document analysis status failed: {str(e)}", source="api", task_id=task_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Get document analysis status failed"
        )


@router.post("/document/reparse", summary="Re-parse document - Re-parse Document")
async def reparse_document(
    request: Dict[str, Any],
    current_user=Depends(get_current_user)
):
    """
    重新解析文档（Re-parse）
    
    用于文档解析失败或内容为空时，重新调用 LlamaParse 解析文档。
    解析成功后自动触发打标签任务。
    
    请求参数:
    - file_id: file_processing_details.id（必填）
    - params: LlamaParse 配置参数（可选，包含 language、fast_mode、max_pages 等）
    
    返回:
    - task_id: Celery 任务 ID
    - file_id: 文件 ID
    - message: 提示消息
    """
    from backend.tasks.file_tasks import reparse_document_task
    
    try:
        file_id = request.get('file_id')
        if not file_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Missing required parameter: file_id"
            )
        
        params = dict(request.get('params') or {})
        _strip_llamaindex_key_from_client_params(params)
        username = current_user.username if current_user else None
        
        # 验证文件是否存在
        db_config = get_database_config()
        db_manager = DatabaseManager(db_config)
        conn = db_manager.get_connection()
        
        if not conn:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Database connection failed"
            )
        
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, filename, status
            FROM file_processing_details
            WHERE id = %s
        """, (file_id,))
        
        row = cursor.fetchone()
        conn.close()
        
        if not row:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"File does not exist: file_id={file_id}"
            )
        
        filename = row[1]
        current_status = row[2]
        
        info(f"Submit reparse task: file_id={file_id}, filename={filename}, user={username}", 
             source="api")
        
        # 提交 Celery 任务
        task = reparse_document_task.apply_async(
            args=(file_id, params, username),
            countdown=1
        )
        
        info(f"Reparse task submitted: task_id={task.id}, file_id={file_id}", source="api")
        
        return {
            "success": True,
            "task_id": task.id,
            "file_id": file_id,
            "filename": filename,
            "message": f"Reparse task submitted: {filename}",
            "note": f"请使用 task_id 查询处理进度（GET /api/files/document/reparse/{task.id}/status）"
        }
    
    except HTTPException:
        raise
    except Exception as e:
        error(f"Submit reparse task failed: {str(e)}", source="api")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Submit reparse task failed: {str(e)}"
        )


@router.get("/document/reparse/{task_id}/status", summary="Query document reparse task status")
async def get_reparse_status(
    task_id: str,
    current_user=Depends(get_current_user)
):
    """
    查询文档重新解析的 Celery 任务状态
    """
    try:
        task_result = AsyncResult(task_id, app=celery_app)
        
        if task_result.state == 'PENDING':
            return {
                "task_id": task_id,
                "status": "pending",
                "progress": 0,
                "message": "Task is waiting to be processed..."
            }
        elif task_result.state == 'PROGRESS':
            task_info = task_result.info or {}
            return {
                "task_id": task_id,
                "status": task_info.get("status", "processing"),
                "progress": task_info.get("progress", 0),
                "message": task_info.get("message", "Reparsing..."),
                "file_id": task_info.get("file_id"),
                "logs": task_info.get("logs", [])
            }
        elif task_result.state == 'SUCCESS':
            result = task_result.result or {}
            return {
                "task_id": task_id,
                "status": "completed",
                "progress": 100,
                "message": "Reparse completed",
                "file_id": result.get("file_id"),
                "content_length": result.get("content_length", 0),
                "processing_time": result.get("processing_time", 0),
                "logs": result.get("logs", [])
            }
        else:
            task_info = task_result.info or {}
            return {
                "task_id": task_id,
                "status": "failed",
                "progress": 0,
                "message": str(task_info.get("error", "Reparse failed")),
                "file_id": task_info.get("file_id"),
                "logs": task_info.get("logs", [])
            }
    
    except Exception as e:
        error(f"Get reparse status failed: {str(e)}", source="api", task_id=task_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Get reparse status failed"
        )


@router.post("/upload-youtube", summary="Upload YouTube link")
async def upload_youtube_link(
    request: Dict[str, Any] = Body(...),
    current_user = Depends(get_current_user)
):
    """
    上传 YouTube 链接并异步处理.

    新路径 (默认, YOUTUBE_UPLOAD_USE_LOCAL=true):
      1. 同步 INSERT processing_stats + file_processing_details (status='processing')
      2. 派发 `fetch_youtube_file_transcript_task` 到本地家用 Worker
      3. 立即返回 file_id / task_id, 前端轮询 file_processing_details.status
      本地 Worker 完成字幕/ASR 后, 调用远程 `finalize_youtube_file_task`
      更新 processing_stats 并触发 auto-tag / auto-name / auto-classify.

    回滚路径 (YOUTUBE_UPLOAD_USE_LOCAL=false):
      走远程 `upload_and_process_youtube_task` 旧实现 (已废弃, 仅保留作为紧急出口).

    请求参数:
    - youtube_url: YouTube 视频链接（必填）
    """
    from backend.services.youtube_service import YouTubeService

    try:
        youtube_url = request.get('youtube_url')
        if not youtube_url:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Missing required parameter: youtube_url"
            )

        # 上传弹窗中可选的 AI 分析模板, finalize_youtube_file_task 会从 upload_params 读出来触发
        auto_analyze_template_id = request.get('auto_analyze_template_id')
        auto_analyze_template_variables = request.get('auto_analyze_template_variables') or {}
        try:
            auto_analyze_template_id = int(auto_analyze_template_id) if auto_analyze_template_id else None
        except (TypeError, ValueError):
            auto_analyze_template_id = None
        if not isinstance(auto_analyze_template_variables, dict):
            auto_analyze_template_variables = {}
        # 方案 A: 上传弹窗 AI Analysis 开关 (兼容旧客户端不传 = True)
        auto_analyze_enabled = bool(request.get('auto_analyze_enabled', True))

        # PR-5: file_tagging picker 选择的模板, 同样持久化到 upload_params
        auto_tag_template_id = request.get('auto_tag_template_id')
        auto_tag_template_variables = request.get('auto_tag_template_variables') or {}
        try:
            auto_tag_template_id = int(auto_tag_template_id) if auto_tag_template_id else None
        except (TypeError, ValueError):
            auto_tag_template_id = None
        if not isinstance(auto_tag_template_variables, dict):
            auto_tag_template_variables = {}
        # 方案 A: 上传弹窗 AI Tagging 开关 (兼容旧客户端不传 = True)
        auto_tag_enabled = bool(request.get('auto_tag_enabled', True))

        username = current_user.username if hasattr(current_user, 'username') else 'guest'

        video_id = YouTubeService.extract_video_id(youtube_url)
        if not video_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid YouTube URL. Please provide a valid YouTube video link."
            )

        info(f"Received YouTube link: url={youtube_url}, video_id={video_id}, user={username}, "
             f"auto_analyze_template_id={auto_analyze_template_id}",
             source="api")

        use_local = os.getenv('YOUTUBE_UPLOAD_USE_LOCAL', 'true') \
            .strip().lower() in ('1', 'true', 'yes', 'on')

        if use_local:
            # ── 新路径: 同步建记录 + 派发本地 Worker ──────────────────
            filename = f"YouTube_{video_id}"
            now_iso = utcnow()  # aware UTC; psycopg2 adapter -> naive UTC

            db_manager = DatabaseManager(get_database_config())
            with db_manager.get_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO processing_stats (
                        timestamp, files_processed, files_failed,
                        "user", task_name, total_time,
                        total_file_processing_time, total_ai_time,
                        total_tokens, total_price, upload_params,
                        final_report, rc_agent_status
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                """, (
                    now_iso, 1, 0, username,
                    'YouTube Processing', 0, 0, 0, 0, 0,
                    json.dumps({
                        "youtube_url": youtube_url,
                        "video_id": video_id,
                        "dispatch_target": "local_virtual_service",
                        # 持久化, finalize_youtube_file_task 会读出来触发 trigger_auto_analyze
                        "auto_analyze_template_id": auto_analyze_template_id,
                        "auto_analyze_template_variables": auto_analyze_template_variables,
                        # 方案 A: 上传弹窗 AI Analysis 开关
                        "auto_analyze_enabled": auto_analyze_enabled,
                        # PR-5: file_tagging picker 透传 (finalize 时读出供 trigger_auto_tagging)
                        "auto_tag_template_id": auto_tag_template_id,
                        "auto_tag_template_variables": auto_tag_template_variables,
                        # 方案 A: 上传弹窗 AI Tagging 开关
                        "auto_tag_enabled": auto_tag_enabled,
                    }),
                    '', 'processing',
                ))
                stats_id = cursor.fetchone()[0]

                cursor.execute("""
                    INSERT INTO file_processing_details (
                        processing_stats_id, filename, file_type,
                        file_size_mb, status, content, asr_with_diarization,
                        created_at, task_id, original_file_path,
                        processing_time, content_length
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                """, (
                    stats_id, filename, 'youtube', 0, 'processing',
                    '', None, utcnow(), None, youtube_url, 0, 0,  # created_at: aware UTC -> adapter -> naive UTC
                ))
                file_id = cursor.fetchone()[0]

            # 派发到本地: 不指定 queue, 交由 celery_config 的默认路由
            # (fetch_youtube_file_transcript_task → youtube_fetching)
            from celery import current_app as _celery_app
            task = _celery_app.send_task(
                'fetch_youtube_file_transcript_task',
                args=(file_id, youtube_url, video_id, username),
                countdown=0,
            )

            # 用 task_id 关联 file_processing_details (便于旧版前端按 task_id 轮询)
            try:
                with db_manager.get_connection_context() as conn:
                    conn.cursor().execute(
                        "UPDATE file_processing_details SET task_id=%s WHERE id=%s",
                        (task.id, file_id),
                    )
            except Exception:
                pass

            info(
                f"YouTube local pipeline dispatched: task_id={task.id} file_id={file_id} "
                f"stats_id={stats_id} video_id={video_id}",
                source="api",
            )

            return {
                "task_id": task.id,
                "file_id": file_id,
                "processing_stats_id": stats_id,
                "video_id": video_id,
                "message": "YouTube processing started on local worker. Poll /api/files/details/{file_id}.",
            }

        # ── 回滚路径: 远程旧流水线 ───────────────────────────────
        from backend.tasks.file_tasks import upload_and_process_youtube_task
        task = upload_and_process_youtube_task.apply_async(
            args=(youtube_url, username),
            queue='file_processing',
        )
        info(
            f"YouTube remote legacy pipeline dispatched: task_id={task.id} video_id={video_id}",
            source="api",
        )
        return {
            "task_id": task.id,
            "video_id": video_id,
            "message": "YouTube processing started. You can track the progress in the task status.",
        }

    except HTTPException:
        raise
    except Exception as e:
        error(f"YouTube upload failed: {str(e)}", source="api")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to process YouTube link: {str(e)}"
        )

@router.post("/upload-apple-podcast", summary="Upload Apple Podcast episode link")
async def upload_apple_podcast_link(
    request: Dict[str, Any] = Body(...),
    current_user = Depends(get_current_user),
):
    """
    上传 Apple Podcasts 单集链接并异步处理。

    远程服务器直接通过 yt-dlp 访问 iTunes 公开 API + 第三方 Podcast CDN，
    无需本地家庭 Worker（与 YouTube 不同，Apple 不存在 IP 封锁问题）。

    流程：
      1. 校验 URL 并提取 episode_id
      2. 派发 upload_and_transcribe_apple_podcast_task 到 file_processing 队列
      3. 立即返回 task_id，前端通过 GET /api/files/tasks/{task_id}/status 轮询

    请求参数：
    - apple_url: Apple Podcasts 单集链接（必填，需含 ?i=episode_id）
    - auto_analyze_template_id: 可选，AI 分析模板 ID
    - auto_analyze_template_variables: 可选，模板变量
    - auto_analyze_enabled: 可选，是否开启自动分析（默认 true）
    - auto_tag_template_id: 可选，AI 打标签模板 ID
    - auto_tag_template_variables: 可选，打标签模板变量
    - auto_tag_enabled: 可选，是否开启自动打标签（默认 true）
    """
    from backend.services.apple_podcast_service import ApplePodcastService
    from backend.tasks.file_tasks import upload_and_transcribe_apple_podcast_task

    try:
        apple_url = request.get('apple_url')
        if not apple_url or not apple_url.strip():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Missing required parameter: apple_url",
            )
        apple_url = apple_url.strip()

        episode_id = ApplePodcastService.extract_episode_id(apple_url)
        if not episode_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=(
                    "Invalid Apple Podcasts URL. "
                    "Please provide a valid episode link containing ?i=<episode_id>, "
                    "e.g. https://podcasts.apple.com/us/podcast/.../id...?i=..."
                ),
            )

        auto_analyze_template_id = request.get('auto_analyze_template_id')
        auto_analyze_template_variables = request.get('auto_analyze_template_variables') or {}
        try:
            auto_analyze_template_id = int(auto_analyze_template_id) if auto_analyze_template_id else None
        except (TypeError, ValueError):
            auto_analyze_template_id = None
        if not isinstance(auto_analyze_template_variables, dict):
            auto_analyze_template_variables = {}
        auto_analyze_enabled = bool(request.get('auto_analyze_enabled', True))

        auto_tag_template_id = request.get('auto_tag_template_id')
        auto_tag_template_variables = request.get('auto_tag_template_variables') or {}
        try:
            auto_tag_template_id = int(auto_tag_template_id) if auto_tag_template_id else None
        except (TypeError, ValueError):
            auto_tag_template_id = None
        if not isinstance(auto_tag_template_variables, dict):
            auto_tag_template_variables = {}
        auto_tag_enabled = bool(request.get('auto_tag_enabled', True))

        username = current_user.username if hasattr(current_user, 'username') else 'guest'

        info(
            f"Received Apple Podcast link: episode_id={episode_id}, user={username}",
            source="api",
        )

        task = upload_and_transcribe_apple_podcast_task.apply_async(
            args=(apple_url, episode_id, username),
            kwargs={
                'auto_analyze_template_id': auto_analyze_template_id,
                'auto_analyze_template_variables': auto_analyze_template_variables,
                'auto_analyze_enabled': auto_analyze_enabled,
                'auto_tag_template_id': auto_tag_template_id,
                'auto_tag_template_variables': auto_tag_template_variables,
                'auto_tag_enabled': auto_tag_enabled,
            },
            queue='file_processing',
        )

        info(
            f"Apple Podcast task dispatched: task_id={task.id} episode_id={episode_id}",
            source="api",
        )

        return {
            "task_id": task.id,
            "episode_id": episode_id,
            "message": "Apple Podcast processing started. Use GET /api/files/tasks/{task_id}/status to poll.",
        }

    except HTTPException:
        raise
    except Exception as e:
        error(f"Apple Podcast upload failed: {str(e)}", source="api")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to process Apple Podcast link: {str(e)}",
        )


@router.get(
    "/upload-youtube/status/{file_id}",
    summary="Poll Upload YouTube Link processing status",
)
async def get_upload_youtube_status(
    file_id: int,
    current_user = Depends(get_current_user),
):
    """
    Upload YouTube Link 新路径的轮询入口.

    前端拿到 `POST /files/upload-youtube` 返回的 `file_id` 后,
    用此接口查询最新状态. 与 `/tasks/{task_id}/status` 的区别:
      - 本接口直接查数据库 (file_processing_details + processing_stats),
        跨越本地→远程 → ASR 回调整个链路都能给出正确终态.
      - 不依赖 Celery task 状态, 也就能正确处理「字幕任务派发 ASR 后
        自身已 SUCCESS, 但内容其实还在生成」的情况.
    """
    try:
        db_manager = DatabaseManager(get_database_config())
        with db_manager.get_connection_context() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT fpd.status,
                       fpd.content_length,
                       fpd.filename,
                       fpd.task_id,
                       fpd.original_file_path,
                       ps.rc_agent_status,
                       ps.total_time,
                       ps.upload_params
                FROM file_processing_details fpd
                LEFT JOIN processing_stats ps ON fpd.processing_stats_id = ps.id
                WHERE fpd.id = %s AND ps."user" = %s
                """,
                (file_id, current_user.username),
            )
            row = cur.fetchone()

        if not row:
            raise HTTPException(status_code=404, detail="File not found")

        (file_status, content_length, filename, task_id, original_path,
         rc_agent_status, total_time, upload_params) = row

        # 统一把 file_status 翻译成前端三态: processing / completed / failed
        # asr_pending 是 Caption 任务刚派发 ASR 时写入的中间状态，前端据此弹等待提示
        asr_triggered = file_status == "asr_pending"
        if file_status == "completed":
            ui_status = "completed"
            progress = 100
        elif file_status == "failed":
            ui_status = "failed"
            progress = 0
        elif file_status == "asr_pending":
            ui_status = "processing"
            progress = 40  # 与 HK worker 的 _progress(progress=40) 保持一致
        else:
            ui_status = "processing"
            # 无法精确给出进度, 给一个非零值让前端进度条动起来
            progress = 50 if (content_length or 0) == 0 else 80

        # 提取本地 worker 写入的元信息 (source / reason / asr_used ...) 供前端展示
        caption_source = None
        finalize_reason = None
        asr_used = False
        if isinstance(upload_params, dict):
            caption_source = upload_params.get("caption_source")
            finalize_reason = upload_params.get("finalize_reason")
            asr_used = bool(upload_params.get("asr_used"))

        return {
            "file_id": file_id,
            "task_id": task_id,
            "filename": filename,
            "youtube_url": original_path,
            "status": ui_status,
            "progress": progress,
            "content_length": content_length or 0,
            "total_time": total_time,
            "rc_agent_status": rc_agent_status,
            "caption_source": caption_source,
            "finalize_reason": finalize_reason,
            "asr_used": asr_used,
            "asr_triggered": asr_triggered,
            "message": (
                f"Failed: {finalize_reason}" if ui_status == "failed" and finalize_reason
                else "Done" if ui_status == "completed"
                else "Running Fun-ASR transcription..." if asr_triggered
                else "Processing on local worker..."
            ),
        }

    except HTTPException:
        raise
    except Exception as e:
        error(f"get_upload_youtube_status failed: {e}",
              source="api", file_id=file_id)
        raise HTTPException(status_code=500, detail=str(e))


class FileASRRequest(BaseModel):
    max_duration_seconds: int = 0  # 0 = 完整视频；>0 = 只转写前 N 秒


@router.post("/{file_id}/transcribe-asr", summary="Trigger Fun-ASR transcription for YouTube file")
async def trigger_file_youtube_asr(
    file_id: int,
    body: FileASRRequest = FileASRRequest(),
    current_user=Depends(get_current_user)
):
    """
    对 Upload YouTube Links 保存的视频（无字幕），触发 Fun-ASR 转写任务。
    - max_duration_seconds: Only transcribe first N seconds (0 = full video)
    - 任务进度可通过 GET /api/files/tasks/{task_id}/status 查询。
    """
    try:
        db_conn_params = get_database_config()
        db_manager = DatabaseManager(db_conn_params)
        conn = db_manager.get_connection()
        cursor = conn.cursor()

        cursor.execute(
            "SELECT id, file_type, original_file_path FROM file_processing_details WHERE id = %s",
            (file_id,)
        )
        row = cursor.fetchone()
        cursor.close()
        conn.close()

        if not row:
            raise HTTPException(status_code=404, detail="File not found")

        _, file_type, youtube_url = row
        if file_type != 'youtube':
            raise HTTPException(
                status_code=400,
                detail="Only YouTube files can be transcribed via ASR"
            )

        from backend.services.youtube_service import YouTubeService
        video_id = YouTubeService.extract_video_id(youtube_url) or f"file{file_id}"

        username = current_user.username if hasattr(current_user, 'username') else 'guest'

        # 直接派发到本地 Worker 的 priority ASR 队列。
        # 本接口是用户手动触发"重新转录"，属于交互链路，要求和 upload-youtube
        # 走同一个 priority worker，不受批量 Feed 任务阻塞。
        # 本地版 transcribe_youtube_file_asr_task 签名:
        #   (file_id, youtube_url, video_id, username, max_duration_seconds,
        #    _skip_duration_check=False)
        # 完成后会回调远程 `finalize_youtube_file_task`.
        from celery import current_app as _celery_app
        task = _celery_app.send_task(
            'transcribe_youtube_file_asr_task',
            args=(file_id, youtube_url, video_id, username,
                  max(0, body.max_duration_seconds)),
            queue='youtube_transcription_priority',
        )

        info(
            f"YouTube File ASR task dispatched to local worker: file_id={file_id}, "
            f"video_id={video_id}, task_id={task.id}",
            source="api",
        )

        return {
            "task_id": task.id,
            "file_id": file_id,
            "video_id": video_id,
            "message": "ASR transcription started. Use task_id to poll progress."
        }

    except HTTPException:
        raise
    except Exception as e:
        error(f"Trigger YouTube File ASR failed: {str(e)}", source="api")
        raise HTTPException(status_code=500, detail=str(e))


# ===========================================
# 文件分享 API
# ===========================================

def _check_file_access(cursor, file_id: int, user_id: int, username: str, is_admin: bool) -> str:
    """
    检查用户对文件的访问权限。
    返回 'owner' | 'shared' | None
    """
    cursor.execute("""
        SELECT ps."user"
        FROM file_processing_details fpd
        JOIN processing_stats ps ON fpd.processing_stats_id = ps.id
        WHERE fpd.id = %s
    """, (file_id,))
    row = cursor.fetchone()
    if not row:
        return None
    if row[0] == username:
        return 'owner'
    cursor.execute(
        "SELECT 1 FROM file_shares WHERE file_id = %s AND shared_to_user_id = %s",
        (file_id, user_id)
    )
    if cursor.fetchone():
        return 'shared'
    return None


@router.post("/{file_id}/share", summary="Share file to specified users")
async def share_file(
    file_id: int,
    body: Dict[str, Any] = Body(...),
    current_user=Depends(get_current_user)
):
    """
    分享文件给指定用户（支持再分享）

    Body: { "user_ids": [2, 5] }
    """
    user_ids = body.get("user_ids", [])
    if not user_ids:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="user_ids is required")

    db_conn_params = get_database_config()
    db_manager = DatabaseManager(db_conn_params)
    conn = db_manager.get_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")

    cursor = conn.cursor()
    try:
        access = _check_file_access(cursor, file_id, current_user.user_id, current_user.username, current_user.is_admin)
        if not access:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No access to this file")

        shared_count = 0
        skipped = []
        for uid in user_ids:
            if uid == current_user.user_id:
                skipped.append({"user_id": uid, "reason": "cannot share to yourself"})
                continue
            cursor.execute("SELECT id, username FROM users WHERE id = %s", (uid,))
            target = cursor.fetchone()
            if not target:
                skipped.append({"user_id": uid, "reason": "user not found"})
                continue
            cursor.execute("""
                INSERT INTO file_shares (file_id, shared_by_user_id, shared_to_user_id)
                VALUES (%s, %s, %s)
                ON CONFLICT (file_id, shared_to_user_id) DO NOTHING
            """, (file_id, current_user.user_id, uid))
            if cursor.rowcount > 0:
                shared_count += 1

        conn.commit()
        info(f"File {file_id} shared to {shared_count} users by {current_user.username}", source="api")
        return {"shared_count": shared_count, "skipped": skipped}

    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        error(f"Share file failed: {str(e)}", source="api")
        raise HTTPException(status_code=500, detail=f"Share failed: {str(e)}")
    finally:
        conn.close()


@router.delete("/{file_id}/share", summary="Unshare file")
async def unshare_file(
    file_id: int,
    body: Dict[str, Any] = Body(...),
    current_user=Depends(get_current_user)
):
    """
    Unshare file (only owner or admin)

    Body: { "user_ids": [5] }
    """
    user_ids = body.get("user_ids", [])
    if not user_ids:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="user_ids is required")

    db_conn_params = get_database_config()
    db_manager = DatabaseManager(db_conn_params)
    conn = db_manager.get_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")

    cursor = conn.cursor()
    try:
        access = _check_file_access(cursor, file_id, current_user.user_id, current_user.username, current_user.is_admin)
        if access != 'owner':
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Only file owner can revoke sharing"
            )

        removed = 0
        for uid in user_ids:
            cursor.execute(
                "DELETE FROM file_shares WHERE file_id = %s AND shared_to_user_id = %s",
                (file_id, uid)
            )
            removed += cursor.rowcount

        conn.commit()
        info(f"File {file_id}: removed {removed} shares by {current_user.username}", source="api")
        return {"removed_count": removed}

    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        error(f"Unshare file failed: {str(e)}", source="api")
        raise HTTPException(status_code=500, detail=f"Unshare failed: {str(e)}")
    finally:
        conn.close()


@router.get("/{file_id}/shares", summary="View file shares list")
async def get_file_shares(
    file_id: int,
    current_user=Depends(get_current_user)
):
    """
    View all shares for a specific file
    """
    db_conn_params = get_database_config()
    db_manager = DatabaseManager(db_conn_params)
    conn = db_manager.get_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")

    cursor = conn.cursor()
    try:
        access = _check_file_access(cursor, file_id, current_user.user_id, current_user.username, current_user.is_admin)
        if not access:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No access to this file")

        cursor.execute("""
            SELECT fs.id, fs.file_id,
                   fs.shared_by_user_id, u_by.username as shared_by_username,
                   fs.shared_to_user_id, u_to.username as shared_to_username,
                   fs.created_at
            FROM file_shares fs
            JOIN users u_by ON u_by.id = fs.shared_by_user_id
            JOIN users u_to ON u_to.id = fs.shared_to_user_id
            WHERE fs.file_id = %s
            ORDER BY fs.created_at DESC
        """, (file_id,))

        shares = []
        for row in cursor.fetchall():
            shares.append({
                "id": row[0],
                "file_id": row[1],
                "shared_by_user_id": row[2],
                "shared_by_username": row[3],
                "shared_to_user_id": row[4],
                "shared_to_username": row[5],
                "created_at": row[6].isoformat() if row[6] else None
            })

        return {"file_id": file_id, "shares": shares}

    except HTTPException:
        raise
    except Exception as e:
        error(f"Get file shares failed: {str(e)}", source="api")
        raise HTTPException(status_code=500, detail=f"Get shares failed: {str(e)}")
    finally:
        conn.close()
