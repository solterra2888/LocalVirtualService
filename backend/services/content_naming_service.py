# -*- coding: utf-8 -*-
"""
内容自动命名服务
使用 auto_name_v1 模板对文件内容进行分析并生成有意义的文件名
"""
import os
import re
import time
from typing import Dict, Any, Optional, Callable
from backend.services.local_llm_analysis_service import LocalLLMAnalysisService
from backend.database.db_manager import DatabaseManager
from backend.utils.logger import get_logger

logger = get_logger("content_naming_service")


class ContentNamingService:
    """内容自动命名服务"""
    
    TEMPLATE_CODE = 'auto_name_v1'
    
    def __init__(
        self, 
        db_manager: DatabaseManager,
        log_callback: Optional[Callable] = None
    ):
        self.db_manager = db_manager
        self.log_callback = log_callback
        self.llm_service = LocalLLMAnalysisService(
            template_code=self.TEMPLATE_CODE,
            log_callback=log_callback
        )
    
    def log(self, message: str, level: str = "info", **kwargs):
        log_method = getattr(logger, level.lower(), logger.info)
        log_method(message, **kwargs)
        if self.log_callback:
            self.log_callback(log_message=message)
    
    def get_file_info(self, file_id: int) -> Optional[Dict[str, Any]]:
        """
        从数据库获取文件信息（content、filename、file_type、display_name）
        """
        conn = self.db_manager.get_connection()
        if not conn:
            raise ValueError("无法连接到数据库")
        
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT id, filename, content, file_type, content_length, display_name
                FROM file_processing_details
                WHERE id = %s
            """, (file_id,))
            
            row = cursor.fetchone()
            if not row:
                return None
            
            return {
                'id': row[0],
                'filename': row[1],
                'content': row[2],
                'file_type': row[3],
                'content_length': row[4],
                'display_name': row[5]
            }
        finally:
            conn.close()
    
    @staticmethod
    def sanitize_name(raw_name: str) -> str:
        """清洗 LLM 返回的文件名：去除引号、换行、多余空白、特殊字符等"""
        name = raw_name.strip()
        name = name.strip('"\'""''「」【】《》')
        name = re.sub(r'[\r\n]+', ' ', name)
        name = re.sub(r'[\\/:*?"<>|]', '', name)
        name = re.sub(r'\s+', ' ', name).strip()
        if len(name) > 80:
            name = name[:80]
        return name
    
    async def name_file_content(
        self,
        file_id: int,
        force: bool = False,
        user_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        为文件内容生成智能文件名
        
        Args:
            file_id: 文件ID
            force: 是否强制重新命名（即使已有 display_name）
        """
        start_time = time.time()
        
        self.log(f"正在获取文件信息: file_id={file_id}")
        file_info = self.get_file_info(file_id)
        
        if not file_info:
            raise ValueError(f"文件不存在: file_id={file_id}")
        
        content = (file_info.get('content') or '').strip()
        filename = file_info.get('filename', 'unknown')
        file_type = file_info.get('file_type', '')
        existing_display_name = file_info.get('display_name')
        
        if not content:
            raise ValueError(f"文件内容为空，无法自动命名: {filename}")
        
        if not force and existing_display_name:
            self.log(f"文件已有 display_name，跳过: {filename}", level="info")
            return {
                'success': True,
                'skipped': True,
                'message': '文件已有自动命名',
                'existing_display_name': existing_display_name
            }
        
        content_length = len(content)
        max_chars = int(os.getenv('AUTO_NAME_CONTENT_MAX_CHARS', '3000'))
        if content_length > max_chars:
            content = content[:max_chars]
            self.log(
                f"文件内容已截取前 {max_chars} 字符用于命名（原长 {content_length} 字符）, 文件名: {filename}"
            )
        else:
            self.log(f"文件内容长度: {content_length} 字符, 文件名: {filename}")
        
        variables = {
            'file_content': content,
            'file_type': file_type or 'unknown',
            'original_filename': filename
        }
        
        self.log(f"正在调用AI自动命名: {filename}")
        # PR-4 修复: 透传 user_id 让 llm_prompt_usage_logs 能正确归属
        analysis_result = await self.llm_service.analyze_file_content(
            file_content=content,
            file_name=filename,
            custom_variables=variables,
            user_id=user_id,
        )
        
        raw_name = analysis_result.get('report_content', '').strip()
        
        if not raw_name:
            raise ValueError(f"AI返回的文件名为空: {filename}")
        
        display_name = self.sanitize_name(raw_name)
        
        if not display_name:
            raise ValueError(f"清洗后的文件名为空: raw='{raw_name}'")
        
        self.log(f"正在保存 display_name 到数据库: {filename} -> {display_name}")
        self.save_display_name(file_id, display_name)
        
        processing_time = time.time() - start_time
        
        self.log(
            f"自动命名完成: {filename} -> {display_name}, "
            f"耗时={processing_time:.2f}秒"
        )
        
        return {
            'success': True,
            'file_id': file_id,
            'filename': filename,
            'display_name': display_name,
            'processing_time': processing_time,
            'ai_metadata': {
                'total_tokens': analysis_result.get('total_tokens', 0),
                'total_price': analysis_result.get('total_price', 0),
                'model': analysis_result.get('model'),
                'provider': analysis_result.get('provider')
            }
        }
    
    def save_display_name(self, file_id: int, display_name: str):
        """将 display_name 写入 file_processing_details"""
        conn = self.db_manager.get_connection()
        if not conn:
            raise ValueError("无法连接到数据库")
        
        try:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE file_processing_details
                SET display_name = %s
                WHERE id = %s
            """, (display_name, file_id))
            conn.commit()
        except Exception as e:
            conn.rollback()
            self.log(f"保存 display_name 失败: {str(e)}", level="error")
            raise
        finally:
            conn.close()
