# -*- coding: utf-8 -*-
"""
内容自动打标签服务
使用 content_tag_v1 模板对文件内容进行分析和打标签
"""
import json
import os
import time
from typing import Dict, Any, Optional, Callable
from psycopg2.extras import Json
from backend.services.local_llm_analysis_service import LocalLLMAnalysisService
from backend.database.db_manager import DatabaseManager
from backend.utils.logger import get_logger

logger = get_logger("content_tagging_service")


class ContentTaggingService:
    """内容自动打标签服务"""
    
    # 使用 content_tag_v1 模板
    TEMPLATE_CODE = 'content_tag_v1'
    
    def __init__(
        self, 
        db_manager: DatabaseManager,
        log_callback: Optional[Callable] = None
    ):
        """
        初始化打标签服务
        
        Args:
            db_manager: 数据库管理器
            log_callback: 日志回调函数
        """
        self.db_manager = db_manager
        self.log_callback = log_callback
        self.llm_service = LocalLLMAnalysisService(
            template_code=self.TEMPLATE_CODE,
            log_callback=log_callback
        )
    
    def log(self, message: str, level: str = "info", **kwargs):
        """发送日志消息"""
        log_method = getattr(logger, level.lower(), logger.info)
        log_method(message, **kwargs)
        if self.log_callback:
            self.log_callback(log_message=message)
    
    def get_file_content(self, file_id: int) -> Optional[Dict[str, Any]]:
        """
        从数据库获取文件内容
        
        Args:
            file_id: file_processing_details.id
            
        Returns:
            包含文件信息的字典，如果文件不存在返回None
        """
        conn = self.db_manager.get_connection()
        if not conn:
            raise ValueError("无法连接到数据库")
        
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT id, filename, content, file_type, content_length, processing_stats_id
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
    
    async def tag_file_content(
        self,
        file_id: int,
        additional_context: str = "",
        force: bool = False
    ) -> Dict[str, Any]:
        """
        为文件内容打标签
        
        Args:
            file_id: 文件ID
            additional_context: 额外的上下文信息
            force: 是否强制重新打标签（即使已有标签）
            
        Returns:
            标签结果字典
        """
        start_time = time.time()
        
        # 1. 获取文件内容
        self.log(f"正在获取文件内容: file_id={file_id}")
        file_info = self.get_file_content(file_id)
        
        if not file_info:
            raise ValueError(f"文件不存在: file_id={file_id}")
        
        content = file_info.get('content', '').strip()
        filename = file_info.get('filename', 'unknown')
        
        if not content:
            raise ValueError(f"文件内容为空，无法打标签: {filename}")
        
        content_length = len(content)
        max_chars = int(os.getenv('AUTO_TAG_CONTENT_MAX_CHARS', '6000'))
        if content_length > max_chars:
            content = content[:max_chars]
            self.log(
                f"文件内容已截取前 {max_chars} 字符用于打标签（原长 {content_length} 字符）, 文件名: {filename}"
            )
        else:
            self.log(f"文件内容长度: {content_length} 字符, 文件名: {filename}")
        
        # 2. 检查是否已有标签
        if not force:
            existing_tags = self.get_existing_tags(file_id)
            if existing_tags:
                self.log(f"文件已有标签，跳过: {filename}", level="info")
                return {
                    'success': True,
                    'skipped': True,
                    'message': '文件已有标签',
                    'existing_tags': existing_tags
                }
        
        # 3. 准备模板变量
        variables = {
            'file_content': content,
            'additional_context': additional_context
        }
        
        # 4. 调用LLM分析
        self.log(f"正在调用AI打标签: {filename}")
        analysis_result = await self.llm_service.analyze_file_content(
            file_content=content,
            file_name=filename,
            custom_variables=variables
        )
        
        # 5. 解析结果
        report_content = analysis_result.get('report_content', '')
        
        # 尝试解析JSON结果
        try:
            # 移除可能的markdown代码块标记
            if '```json' in report_content:
                report_content = report_content.split('```json')[1].split('```')[0].strip()
            elif '```' in report_content:
                report_content = report_content.split('```')[1].split('```')[0].strip()
            
            tag_data = json.loads(report_content)
            
            # 验证必需字段
            required_fields = ['sector', 'industry', 'tickers']
            if not all(k in tag_data for k in required_fields):
                raise ValueError(f"AI返回的标签数据缺少必需字段: {required_fields}")
            
            # 确保新字段有默认值（兼容旧模板可能未返回的情况）
            if 'sector_en' not in tag_data:
                tag_data['sector_en'] = ''
            if 'industry_en' not in tag_data:
                tag_data['industry_en'] = ''
            if 'concepts' not in tag_data:
                tag_data['concepts'] = []
            
        except json.JSONDecodeError as e:
            self.log(f"解析AI返回的JSON失败: {str(e)}", level="error")
            raise ValueError(f"AI返回格式错误，无法解析为JSON: {str(e)}")
        
        # 6. 保存标签到数据库
        self.log(f"正在保存标签到数据库: {filename}")
        save_result = self.save_tags(
            file_id=file_id,
            tag_data=tag_data,
            ai_metadata={
                'total_tokens': analysis_result.get('total_tokens', 0),
                'total_price': analysis_result.get('total_price', 0),
                'response_time': analysis_result.get('response_time', 0),
                'model': analysis_result.get('model'),
                'provider': analysis_result.get('provider')
            }
        )
        
        processing_time = time.time() - start_time
        
        self.log(
            f"标签保存成功: {filename}, "
            f"sector={tag_data.get('sector')}({tag_data.get('sector_en', '')}), "
            f"industry={tag_data.get('industry')}({tag_data.get('industry_en', '')}), "
            f"tickers={len(tag_data.get('tickers', []))}, "
            f"concepts={tag_data.get('concepts', [])}, "
            f"耗时={processing_time:.2f}秒"
        )
        
        return {
            'success': True,
            'file_id': file_id,
            'filename': filename,
            'tags': tag_data,
            'tag_id': save_result['tag_id'],
            'processing_time': processing_time,
            'ai_metadata': {
                'total_tokens': analysis_result.get('total_tokens', 0),
                'total_price': analysis_result.get('total_price', 0),
                'model': analysis_result.get('model'),
                'provider': analysis_result.get('provider')
            }
        }
    
    def get_existing_tags(self, file_id: int) -> Optional[Dict[str, Any]]:
        """
        获取文件的现有标签
        
        Args:
            file_id: 文件ID
            
        Returns:
            标签字典，如果不存在返回None
        """
        conn = self.db_manager.get_connection()
        if not conn:
            return None
        
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT id, sector, sector_en, industry, industry_en, ticker, 
                       concepts, additional_tags, confidence, reasoning, 
                       tagged_by, tagged_at, updated_at
                FROM file_tags
                WHERE file_id = %s
            """, (file_id,))
            
            row = cursor.fetchone()
            if not row:
                return None
            
            tickers = row[5] if isinstance(row[5], list) else (row[5] or [])
            concepts = row[6] if isinstance(row[6], list) else (row[6] or [])
            additional_tags = row[7] if row[7] else {}
            
            return {
                'id': row[0],
                'sector': row[1],
                'sector_en': row[2],
                'industry': row[3],
                'industry_en': row[4],
                'tickers': tickers,
                'concepts': concepts,
                'additional_tags': additional_tags,
                'confidence': row[8],
                'reasoning': row[9],
                'tagged_by': row[10],
                'tagged_at': row[11],
                'updated_at': row[12]
            }
        finally:
            conn.close()
    
    def save_tags(
        self,
        file_id: int,
        tag_data: Dict[str, Any],
        ai_metadata: Optional[Dict[str, Any]] = None,
        tagged_by: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        保存标签到数据库（UPSERT）
        
        Args:
            file_id: 文件ID
            tag_data: 标签数据（AI返回的结果）
            ai_metadata: AI元数据（tokens, price等）
            tagged_by: 打标签的用户ID（None表示系统自动）
            
        Returns:
            保存结果
        """
        conn = self.db_manager.get_connection()
        if not conn:
            raise ValueError("无法连接到数据库")
        
        try:
            cursor = conn.cursor()
            
            # 提取核心字段
            sector = tag_data.get('sector', '未分类')
            sector_en = tag_data.get('sector_en', '')
            industry = tag_data.get('industry', '未分类')
            industry_en = tag_data.get('industry_en', '')
            tickers = tag_data.get('tickers', [])
            concepts = tag_data.get('concepts', [])
            confidence = tag_data.get('confidence')
            reasoning = tag_data.get('reasoning')
            
            ticker_jsonb = Json(tickers) if tickers else None
            concepts_json = json.dumps(concepts, ensure_ascii=False) if concepts else '[]'
            
            # 构建additional_tags（包含完整的tickers和AI元数据）
            additional_tags = {
                'tickers_full': tickers,  # 完整的ticker列表
            }
            
            # 添加AI元数据
            if ai_metadata:
                additional_tags['ai_metadata'] = {
                    'total_tokens': ai_metadata.get('total_tokens', 0),
                    'total_price': ai_metadata.get('total_price', 0),
                    'response_time': ai_metadata.get('response_time', 0),
                    'model': ai_metadata.get('model'),
                    'provider': ai_metadata.get('provider')
                }
            
            # UPSERT操作
            cursor.execute("""
                INSERT INTO file_tags 
                (file_id, sector, sector_en, industry, industry_en, ticker, 
                 concepts, additional_tags, confidence, reasoning, tagged_by, tagged_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (file_id) 
                DO UPDATE SET 
                    sector = EXCLUDED.sector,
                    sector_en = EXCLUDED.sector_en,
                    industry = EXCLUDED.industry,
                    industry_en = EXCLUDED.industry_en,
                    ticker = EXCLUDED.ticker,
                    concepts = EXCLUDED.concepts,
                    additional_tags = EXCLUDED.additional_tags,
                    confidence = EXCLUDED.confidence,
                    reasoning = EXCLUDED.reasoning,
                    tagged_by = EXCLUDED.tagged_by,
                    tagged_at = EXCLUDED.tagged_at,
                    updated_at = CURRENT_TIMESTAMP
                RETURNING id
            """, (
                file_id,
                sector,
                sector_en,
                industry,
                industry_en,
                ticker_jsonb,
                concepts_json,
                json.dumps(additional_tags),
                confidence,
                reasoning,
                tagged_by
            ))
            
            tag_id = cursor.fetchone()[0]
            conn.commit()
            
            return {
                'success': True,
                'tag_id': tag_id,
                'file_id': file_id
            }
            
        except Exception as e:
            conn.rollback()
            self.log(f"保存标签失败: {str(e)}", level="error")
            raise
        finally:
            conn.close()
    
    def update_tags(
        self,
        file_id: int,
        updates: Dict[str, Any],
        tagged_by: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        更新文件标签（手动编辑）
        
        Args:
            file_id: 文件ID
            updates: 更新的字段
            tagged_by: 编辑用户ID
            
        Returns:
            更新结果
        """
        conn = self.db_manager.get_connection()
        if not conn:
            raise ValueError("无法连接到数据库")
        
        try:
            cursor = conn.cursor()
            
            # 构建更新SQL
            update_fields = []
            params = []
            
            if 'sector' in updates:
                update_fields.append("sector = %s")
                params.append(updates['sector'])
            
            if 'sector_en' in updates:
                update_fields.append("sector_en = %s")
                params.append(updates['sector_en'])
            
            if 'industry' in updates:
                update_fields.append("industry = %s")
                params.append(updates['industry'])
            
            if 'industry_en' in updates:
                update_fields.append("industry_en = %s")
                params.append(updates['industry_en'])
            
            if 'tickers' in updates:
                tickers = updates['tickers']
                update_fields.append("ticker = %s")
                params.append(Json(tickers) if tickers else None)
                
                update_fields.append("additional_tags = jsonb_set(additional_tags, '{tickers_full}', %s::jsonb)")
                params.append(json.dumps(tickers))
            
            if 'concepts' in updates:
                concepts = updates['concepts']
                update_fields.append("concepts = %s::jsonb")
                params.append(json.dumps(concepts, ensure_ascii=False) if concepts else '[]')
            
            if 'confidence' in updates:
                update_fields.append("confidence = %s")
                params.append(updates['confidence'])
            
            if 'reasoning' in updates:
                update_fields.append("reasoning = %s")
                params.append(updates['reasoning'])
            
            if 'additional_tags' in updates:
                update_fields.append("additional_tags = additional_tags || %s::jsonb")
                params.append(json.dumps(updates['additional_tags']))
            
            if not update_fields:
                return {'success': True, 'message': '没有需要更新的字段'}
            
            # 添加tagged_by和updated_at
            if tagged_by is not None:
                update_fields.append("tagged_by = %s")
                params.append(tagged_by)
            
            update_fields.append("updated_at = CURRENT_TIMESTAMP")
            
            # 添加WHERE条件参数
            params.append(file_id)
            
            sql = f"""
                UPDATE file_tags 
                SET {', '.join(update_fields)}
                WHERE file_id = %s
                RETURNING id
            """
            
            cursor.execute(sql, params)
            result = cursor.fetchone()
            
            if not result:
                raise ValueError(f"文件标签不存在: file_id={file_id}")
            
            conn.commit()
            
            self.log(f"标签更新成功: file_id={file_id}, tag_id={result[0]}")
            
            return {
                'success': True,
                'tag_id': result[0],
                'file_id': file_id
            }
            
        except Exception as e:
            conn.rollback()
            self.log(f"更新标签失败: {str(e)}", level="error")
            raise
        finally:
            conn.close()
    
    def delete_tags(self, file_id: int) -> Dict[str, Any]:
        """
        删除文件标签
        
        Args:
            file_id: 文件ID
            
        Returns:
            删除结果
        """
        conn = self.db_manager.get_connection()
        if not conn:
            raise ValueError("无法连接到数据库")
        
        try:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM file_tags WHERE file_id = %s RETURNING id", (file_id,))
            result = cursor.fetchone()
            conn.commit()
            
            if result:
                self.log(f"标签删除成功: file_id={file_id}")
                return {'success': True, 'deleted': True}
            else:
                return {'success': True, 'deleted': False, 'message': '标签不存在'}
                
        except Exception as e:
            conn.rollback()
            self.log(f"删除标签失败: {str(e)}", level="error")
            raise
        finally:
            conn.close()
