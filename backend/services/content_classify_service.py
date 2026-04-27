# -*- coding: utf-8 -*-
"""
内容自动归类服务
使用 auto_classify_v1 模板，根据文件内容和用户文件夹列表将文件归入最匹配的文件夹
"""
import os
import re
import time
from typing import Dict, Any, Optional, Callable, List, Tuple
from backend.services.local_llm_analysis_service import LocalLLMAnalysisService
from backend.database.db_manager import DatabaseManager
from backend.utils.logger import get_logger

logger = get_logger("content_classify_service")


class ContentClassifyService:
    """内容自动归类服务"""

    TEMPLATE_CODE = 'auto_classify_v1'

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
        """从数据库获取文件信息"""
        conn = self.db_manager.get_connection()
        if not conn:
            raise ValueError("无法连接到数据库")

        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT id, filename, content, file_type, content_length,
                       processing_stats_id
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
                'processing_stats_id': row[5]
            }
        finally:
            conn.close()

    def get_user_folders(self, username: str) -> List[Tuple[int, str]]:
        """获取用户的文件夹列表 [(id, name), ...]"""
        conn = self.db_manager.get_connection()
        if not conn:
            raise ValueError("无法连接到数据库")

        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT id, name FROM file_folders
                WHERE username = %s
                ORDER BY sort_order, created_at
            """, (username,))
            return cursor.fetchall()
        finally:
            conn.close()

    def get_existing_classification(self, username: str, processing_stats_id: int) -> Optional[Dict[str, Any]]:
        """查询用户是否已对该 execution 进行了归类"""
        conn = self.db_manager.get_connection()
        if not conn:
            return None

        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT uef.id, uef.folder_id, ff.name as folder_name
                FROM user_execution_folders uef
                LEFT JOIN file_folders ff ON ff.id = uef.folder_id
                WHERE uef.username = %s AND uef.processing_stats_id = %s
            """, (username, processing_stats_id))
            row = cursor.fetchone()
            if not row:
                return None
            return {
                'id': row[0],
                'folder_id': row[1],
                'folder_name': row[2]
            }
        finally:
            conn.close()

    async def classify_file_content(
        self,
        file_id: int,
        username: str,
        force: bool = False,
        user_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        根据文件内容自动归类到用户文件夹

        Returns:
            包含 folder_id, folder_name, processing_time 等的结果字典
        """
        start_time = time.time()

        self.log(f"正在获取文件信息: file_id={file_id}")
        file_info = self.get_file_info(file_id)

        if not file_info:
            raise ValueError(f"文件不存在: file_id={file_id}")

        content = (file_info.get('content') or '').strip()
        filename = file_info.get('filename', 'unknown')
        file_type = file_info.get('file_type', '')
        processing_stats_id = file_info.get('processing_stats_id')

        if not content:
            raise ValueError(f"文件内容为空，无法自动归类: {filename}")

        if not processing_stats_id:
            raise ValueError(f"文件缺少 processing_stats_id: {filename}")

        if not force:
            existing = self.get_existing_classification(username, processing_stats_id)
            if existing:
                self.log(f"文件已有归类，跳过: {filename} -> {existing['folder_name']}")
                return {
                    'success': True,
                    'skipped': True,
                    'message': '文件已有归类',
                    'existing_folder': existing['folder_name']
                }

        folders = self.get_user_folders(username)
        if not folders:
            self.log(f"用户 {username} 没有文件夹，跳过归类")
            return {
                'success': True,
                'skipped': True,
                'message': '用户没有文件夹，跳过归类',
                'folder_name': 'Uncategorized'
            }

        folder_map = {name: fid for fid, name in folders}
        folder_list_str = ", ".join(name for _, name in folders)

        content_length = len(content)
        max_chars = int(os.getenv('AUTO_CLASSIFY_CONTENT_MAX_CHARS', '3000'))
        if content_length > max_chars:
            content = content[:max_chars]
            self.log(f"文件内容已截取前 {max_chars} 字符用于归类（原长 {content_length} 字符）")

        variables = {
            'file_content': content,
            'folder_list': folder_list_str,
            'file_type': file_type or 'unknown',
            'original_filename': filename
        }

        self.log(f"正在调用AI自动归类: {filename}, 候选文件夹: [{folder_list_str}]")
        # PR-4 修复: 透传 user_id 让 llm_prompt_usage_logs 能正确归属
        analysis_result = await self.llm_service.analyze_file_content(
            file_content=content,
            file_name=filename,
            custom_variables=variables,
            user_id=user_id,
        )

        raw_result = (analysis_result.get('report_content') or '').strip()
        raw_result = raw_result.strip('"\'""''')
        raw_result = re.sub(r'[\r\n]+', '', raw_result).strip()

        self.log(f"LLM 归类结果原文: '{raw_result}'")

        matched_folder_id = folder_map.get(raw_result)

        if matched_folder_id is None:
            for name, fid in folder_map.items():
                if name.lower() == raw_result.lower():
                    matched_folder_id = fid
                    raw_result = name
                    break

        processing_time = time.time() - start_time

        if matched_folder_id is not None:
            self.log(f"归类成功: {filename} -> {raw_result} (folder_id={matched_folder_id})")
            self.save_classification(username, processing_stats_id, matched_folder_id)
            return {
                'success': True,
                'file_id': file_id,
                'filename': filename,
                'folder_id': matched_folder_id,
                'folder_name': raw_result,
                'processing_time': processing_time,
                'ai_metadata': {
                    'total_tokens': analysis_result.get('total_tokens', 0),
                    'total_price': analysis_result.get('total_price', 0),
                    'model': analysis_result.get('model'),
                    'provider': analysis_result.get('provider')
                }
            }
        else:
            self.log(f"无法匹配文件夹，归为 Uncategorized: {filename}, LLM输出='{raw_result}'")
            return {
                'success': True,
                'file_id': file_id,
                'filename': filename,
                'folder_id': None,
                'folder_name': 'Uncategorized',
                'llm_output': raw_result,
                'processing_time': processing_time,
                'ai_metadata': {
                    'total_tokens': analysis_result.get('total_tokens', 0),
                    'total_price': analysis_result.get('total_price', 0),
                    'model': analysis_result.get('model'),
                    'provider': analysis_result.get('provider')
                }
            }

    def save_classification(self, username: str, processing_stats_id: int, folder_id: int):
        """将归类结果写入 user_execution_folders（UPSERT）"""
        conn = self.db_manager.get_connection()
        if not conn:
            raise ValueError("无法连接到数据库")

        try:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO user_execution_folders (username, processing_stats_id, folder_id)
                VALUES (%s, %s, %s)
                ON CONFLICT (username, processing_stats_id)
                DO UPDATE SET folder_id = EXCLUDED.folder_id
            """, (username, processing_stats_id, folder_id))
            conn.commit()
        except Exception as e:
            conn.rollback()
            self.log(f"保存归类结果失败: {str(e)}", level="error")
            raise
        finally:
            conn.close()
