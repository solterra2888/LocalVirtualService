# -*- coding: utf-8 -*-
"""
文件标签API接口
提供标签的增删改查、重新打标签等功能
"""
from fastapi import APIRouter, Depends, HTTPException, Query
from typing import List, Optional
import json
from backend.schemas.file_tags import (
    FileTagsResponse, FileTagsCreate, FileTagsUpdate,
    RetagRequest, BatchRetagRequest, TaggingTaskStatus,
    FileTagsWithFileInfo, FileTagsStats
)
from backend.database.db_manager import DatabaseManager
from backend.utils.database_config import get_database_config
from backend.auth.jwt_utils import get_current_user
from backend.schemas.user import TokenData
from backend.utils.logger import get_logger

logger = get_logger("file_tags_api")

router = APIRouter(prefix="/api/file-tags", tags=["file_tags"])


def get_db():
    """获取数据库管理器"""
    db_config = get_database_config()
    return DatabaseManager(db_config)


@router.get("/{file_id}", response_model=FileTagsResponse)
def get_file_tags(
    file_id: int,
    db: DatabaseManager = Depends(get_db),
    current_user: TokenData = Depends(get_current_user)
):
    """
    获取文件的标签信息
    
    Args:
        file_id: 文件ID (file_processing_details.id)
    """
    conn = db.get_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="数据库连接失败")
    
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, file_id, sector, sector_en, industry, industry_en, 
                   ticker, concepts, additional_tags,
                   confidence, reasoning, tagged_by, tagged_at, created_at, updated_at
            FROM file_tags
            WHERE file_id = %s
        """, (file_id,))
        
        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="文件标签不存在")
        
        # 解析ticker JSON
        tickers = []
        ticker_raw = row[6]
        if ticker_raw:
            try:
                tickers = json.loads(ticker_raw)
            except:
                tickers = [ticker_raw]
        
        # 解析concepts JSON
        concepts = []
        if row[7]:
            try:
                if isinstance(row[7], list):
                    concepts = row[7]
                elif isinstance(row[7], str):
                    concepts = json.loads(row[7])
                else:
                    concepts = row[7]
            except:
                concepts = []
        
        # 解析additional_tags
        additional_tags = row[8] if row[8] else {}
        
        return {
            'id': row[0],
            'file_id': row[1],
            'sector': row[2],
            'sector_en': row[3] or '',
            'industry': row[4],
            'industry_en': row[5] or '',
            'tickers': tickers,
            'ticker_raw': ticker_raw,
            'concepts': concepts,
            'additional_tags': additional_tags,
            'confidence': row[9],
            'reasoning': row[10],
            'tagged_by': row[11],
            'tagged_at': row[12],
            'created_at': row[13],
            'updated_at': row[14]
        }
    finally:
        conn.close()


@router.get("/", response_model=List[FileTagsWithFileInfo])
def list_file_tags(
    sector: Optional[str] = Query(None, description="按行业筛选"),
    industry: Optional[str] = Query(None, description="按子行业筛选"),
    ticker: Optional[str] = Query(None, description="按股票代码筛选"),
    limit: int = Query(50, ge=1, le=500, description="返回数量"),
    offset: int = Query(0, ge=0, description="偏移量"),
    db: DatabaseManager = Depends(get_db),
    current_user: TokenData = Depends(get_current_user)
):
    """
    列出文件标签（支持筛选）
    """
    conn = db.get_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="数据库连接失败")
    
    try:
        cursor = conn.cursor()
        
        # 构建查询条件
        conditions = []
        params = []
        
        if sector:
            conditions.append("ft.sector = %s")
            params.append(sector)
        
        if industry:
            conditions.append("ft.industry = %s")
            params.append(industry)
        
        if ticker:
            # 使用LIKE查询ticker字段（JSON字符串）
            conditions.append("ft.ticker LIKE %s")
            params.append(f'%"{ticker}"%')
        
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        
        # 添加limit和offset
        params.extend([limit, offset])
        
        sql = f"""
            SELECT 
                ft.id, ft.file_id, ft.sector, ft.sector_en, ft.industry, ft.industry_en,
                ft.ticker, ft.concepts, ft.additional_tags, ft.confidence, ft.reasoning,
                ft.tagged_by, ft.tagged_at, ft.created_at, ft.updated_at,
                fpd.filename, fpd.file_type, fpd.processing_stats_id
            FROM file_tags ft
            JOIN file_processing_details fpd ON ft.file_id = fpd.id
            WHERE {where_clause}
            ORDER BY ft.updated_at DESC
            LIMIT %s OFFSET %s
        """
        
        cursor.execute(sql, params)
        rows = cursor.fetchall()
        
        results = []
        for row in rows:
            # 解析ticker
            tickers = []
            ticker_raw = row[6]
            if ticker_raw:
                try:
                    tickers = json.loads(ticker_raw)
                except:
                    tickers = [ticker_raw]
            
            # 解析concepts
            concepts = []
            if row[7]:
                try:
                    if isinstance(row[7], list):
                        concepts = row[7]
                    elif isinstance(row[7], str):
                        concepts = json.loads(row[7])
                    else:
                        concepts = row[7]
                except:
                    concepts = []
            
            additional_tags = row[8] if row[8] else {}
            
            results.append({
                'id': row[0],
                'file_id': row[1],
                'sector': row[2],
                'sector_en': row[3] or '',
                'industry': row[4],
                'industry_en': row[5] or '',
                'tickers': tickers,
                'ticker_raw': ticker_raw,
                'concepts': concepts,
                'additional_tags': additional_tags,
                'confidence': row[9],
                'reasoning': row[10],
                'tagged_by': row[11],
                'tagged_at': row[12],
                'created_at': row[13],
                'updated_at': row[14],
                'filename': row[15],
                'file_type': row[16],
                'processing_stats_id': row[17]
            })
        
        return results
    finally:
        conn.close()


@router.put("/{file_id}", response_model=FileTagsResponse)
def update_file_tags(
    file_id: int,
    updates: FileTagsUpdate,
    db: DatabaseManager = Depends(get_db),
    current_user: TokenData = Depends(get_current_user)
):
    """
    更新文件标签（手动编辑）
    
    Args:
        file_id: 文件ID
        updates: 更新的字段
    """
    from backend.services.content_tagging_service import ContentTaggingService
    
    conn = db.get_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="数据库连接失败")
    
    try:
        # 检查标签是否存在
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM file_tags WHERE file_id = %s", (file_id,))
        if not cursor.fetchone():
            raise HTTPException(status_code=404, detail="文件标签不存在")
        conn.close()
        
        # 使用服务更新
        tagging_service = ContentTaggingService(db_manager=db)
        
        # 转换updates为dict，只包含非None的字段
        update_dict = {}
        if updates.sector is not None:
            update_dict['sector'] = updates.sector
        if updates.industry is not None:
            update_dict['industry'] = updates.industry
        if updates.tickers is not None:
            update_dict['tickers'] = updates.tickers
        if updates.confidence is not None:
            update_dict['confidence'] = updates.confidence
        if updates.reasoning is not None:
            update_dict['reasoning'] = updates.reasoning
        if updates.additional_tags is not None:
            update_dict['additional_tags'] = updates.additional_tags
        
        user_id = current_user.user_id if current_user else None
        tagging_service.update_tags(file_id, update_dict, tagged_by=user_id)
        
        # 返回更新后的标签
        return get_file_tags(file_id, db, current_user)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"更新标签失败: {str(e)}")
        raise HTTPException(status_code=500, detail=f"更新标签失败: {str(e)}")


@router.delete("/{file_id}")
def delete_file_tags(
    file_id: int,
    db: DatabaseManager = Depends(get_db),
    current_user: TokenData = Depends(get_current_user)
):
    """
    删除文件标签
    
    Args:
        file_id: 文件ID
    """
    from backend.services.content_tagging_service import ContentTaggingService
    
    tagging_service = ContentTaggingService(db_manager=db)
    
    try:
        result = tagging_service.delete_tags(file_id)
        
        if result.get('deleted'):
            return {"success": True, "message": "标签已删除"}
        else:
            raise HTTPException(status_code=404, detail="标签不存在")
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"删除标签失败: {str(e)}")
        raise HTTPException(status_code=500, detail=f"删除标签失败: {str(e)}")


@router.post("/retag", response_model=TaggingTaskStatus)
def retag_file(
    request: RetagRequest,
    db: DatabaseManager = Depends(get_db),
    current_user: TokenData = Depends(get_current_user)
):
    """
    重新为文件打标签
    
    Args:
        request: 重新打标签请求
    """
    from backend.tasks.tagging_tasks import tag_file_content_task
    
    conn = db.get_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="数据库连接失败")
    
    try:
        # 检查文件是否存在
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, filename FROM file_processing_details WHERE id = %s
        """, (request.file_id,))
        
        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="文件不存在")
        
        filename = row[1]
        conn.close()
        
        # 提交打标签任务
        username = current_user.username if current_user else None
        task = tag_file_content_task.apply_async(
            args=(request.file_id, username, "", request.force),
            countdown=1
        )
        
        logger.info(f"已提交重新打标签任务: file_id={request.file_id}, task_id={task.id}")
        
        return {
            'task_id': task.id,
            'status': 'pending',
            'progress': 0,
            'message': f'已提交打标签任务: {filename}',
            'file_id': request.file_id
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"提交打标签任务失败: {str(e)}")
        raise HTTPException(status_code=500, detail=f"提交打标签任务失败: {str(e)}")


@router.post("/retag-batch")
def retag_files_batch(
    request: BatchRetagRequest,
    db: DatabaseManager = Depends(get_db),
    current_user: TokenData = Depends(get_current_user)
):
    """
    批量重新打标签
    
    Args:
        request: 批量重新打标签请求
    """
    from backend.tasks.tagging_tasks import batch_tag_files_task
    
    if not request.file_ids:
        raise HTTPException(status_code=400, detail="文件ID列表为空")
    
    username = current_user.username if current_user else None
    
    # 提交批量打标签任务
    task = batch_tag_files_task.apply_async(
        args=(request.file_ids, username, request.force),
        countdown=1
    )
    
    logger.info(
        f"已提交批量打标签任务: {len(request.file_ids)} 个文件, task_id={task.id}"
    )
    
    return {
        'task_id': task.id,
        'status': 'pending',
        'total_files': len(request.file_ids),
        'message': f'已提交批量打标签任务: {len(request.file_ids)} 个文件'
    }


@router.get("/stats/overview", response_model=FileTagsStats)
def get_tags_stats(
    db: DatabaseManager = Depends(get_db),
    current_user: TokenData = Depends(get_current_user)
):
    """
    获取标签统计信息
    """
    conn = db.get_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="数据库连接失败")
    
    try:
        cursor = conn.cursor()
        
        # 总标签数
        cursor.execute("SELECT COUNT(*) FROM file_tags")
        total_tagged_files = cursor.fetchone()[0]
        
        # 按sector统计
        cursor.execute("""
            SELECT sector, COUNT(*) as count
            FROM file_tags
            GROUP BY sector
            ORDER BY count DESC
        """)
        sectors = {row[0]: row[1] for row in cursor.fetchall()}
        
        # 按industry统计
        cursor.execute("""
            SELECT industry, COUNT(*) as count
            FROM file_tags
            GROUP BY industry
            ORDER BY count DESC
            LIMIT 20
        """)
        industries = {row[0]: row[1] for row in cursor.fetchall()}
        
        # Top tickers（从JSON字段中提取）
        cursor.execute("""
            SELECT ticker FROM file_tags WHERE ticker IS NOT NULL
        """)
        ticker_counts = {}
        for row in cursor.fetchall():
            ticker_json = row[0]
            try:
                tickers = json.loads(ticker_json)
                for ticker in tickers:
                    ticker_counts[ticker] = ticker_counts.get(ticker, 0) + 1
            except:
                pass
        
        top_tickers = [
            {'ticker': ticker, 'count': count}
            for ticker, count in sorted(ticker_counts.items(), key=lambda x: x[1], reverse=True)[:20]
        ]
        
        # 平均置信度
        cursor.execute("SELECT AVG(confidence) FROM file_tags WHERE confidence IS NOT NULL")
        avg_confidence_result = cursor.fetchone()[0]
        avg_confidence = float(avg_confidence_result) if avg_confidence_result else None
        
        return {
            'total_tagged_files': total_tagged_files,
            'sectors': sectors,
            'industries': industries,
            'top_tickers': top_tickers,
            'avg_confidence': avg_confidence
        }
    finally:
        conn.close()
