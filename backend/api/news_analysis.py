# -*- coding: utf-8 -*-
"""
新闻LLM分析API接口
"""

from fastapi import APIRouter, Depends, HTTPException, Query
from typing import Optional, List
from pydantic import BaseModel, Field
from backend.utils.database_config import get_database_config
from backend.auth.jwt_utils import get_current_user
from backend.schemas.user import TokenData
from backend.utils.logger import get_logger

logger = get_logger("news_analysis_api")

router = APIRouter(prefix="/news-analysis", tags=["news-analysis"])


# ============ 请求/响应模型 ============

class AnalyzeNewsRequest(BaseModel):
    """分析新闻请求"""
    news_id: int = Field(..., description="新闻ID")
    template_code: str = Field(default="news_summary_v1", description="Prompt模板代码")


class BatchAnalyzeNewsRequest(BaseModel):
    """批量分析新闻请求"""
    news_ids: Optional[List[int]] = Field(None, description="新闻ID列表（如果为空则分析所有未分析的新闻）")
    template_code: str = Field(default="news_summary_v1", description="Prompt模板代码")
    batch_size: int = Field(default=10, ge=1, le=100, description="批量分析数量")
    symbols: Optional[str] = Field(None, description="筛选股票代码，逗号分隔")
    start_date: Optional[str] = Field(None, description="开始日期 YYYY-MM-DD")
    end_date: Optional[str] = Field(None, description="结束日期 YYYY-MM-DD")
    use_concurrent: bool = Field(default=False, description="是否使用并发（谨慎使用，避免API限流）")
    max_concurrent: int = Field(default=3, ge=1, le=10, description="最大并发数")


class AnalysisStatisticsResponse(BaseModel):
    """分析统计响应"""
    pending: int = Field(..., description="待分析数量")
    processing: int = Field(..., description="分析中数量")
    completed: int = Field(..., description="已完成数量")
    failed: int = Field(..., description="失败数量")
    total: int = Field(..., description="总数量")


# ============ API端点 ============

@router.post("/analyze", summary="分析单条新闻")
async def analyze_news(
    request: AnalyzeNewsRequest,
    current_user: TokenData = Depends(get_current_user)
):
    """
    使用Prompt模板分析单条新闻
    """
    try:
        from backend.services.news_analysis_service import NewsAnalysisService
        
        db_config = get_database_config()
        service = NewsAnalysisService(db_config)
        
        # 执行分析
        result = await service.analyze_news(
            news_id=request.news_id,
            template_code=request.template_code,
            user_id=current_user.user_id
        )
        
        if result.get('success'):
            return {
                "success": True,
                "message": f"新闻分析完成",
                "data": result
            }
        else:
            return {
                "success": False,
                "message": f"新闻分析失败: {result.get('error', 'Unknown error')}",
                "data": result
            }
            
    except Exception as e:
        logger.error(f"分析新闻失败: {str(e)}")
        raise HTTPException(status_code=500, detail=f"分析新闻失败: {str(e)}")


@router.post("/analyze-batch", summary="批量分析新闻")
async def analyze_batch(
    request: BatchAnalyzeNewsRequest,
    current_user: TokenData = Depends(get_current_user)
):
    """
    批量分析新闻
    
    - 可以指定新闻ID列表，或自动分析未分析的新闻
    - 支持筛选条件：股票代码、日期范围
    - 支持并发分析（谨慎使用）
    """
    try:
        from backend.services.news_analysis_service import NewsAnalysisService
        
        db_config = get_database_config()
        service = NewsAnalysisService(db_config)
        
        # 如果提供了新闻ID列表，逐个分析
        if request.news_ids:
            logger.info(f"批量分析指定的{len(request.news_ids)}条新闻")
            
            results = []
            success_count = 0
            failed_count = 0
            
            for news_id in request.news_ids:
                result = await service.analyze_news(
                    news_id=news_id,
                    template_code=request.template_code,
                    user_id=current_user.user_id
                )
                results.append(result)
                
                if result.get('success'):
                    success_count += 1
                else:
                    failed_count += 1
            
            return {
                "success": True,
                "message": f"批量分析完成: 成功{success_count}条，失败{failed_count}条",
                "data": {
                    "total": len(request.news_ids),
                    "success": success_count,
                    "failed": failed_count,
                    "results": results
                }
            }
        else:
            # 分析未分析的新闻
            logger.info(f"批量分析未分析的新闻，最多{request.batch_size}条")
            
            # 处理symbols参数
            symbols = None
            if request.symbols:
                symbols = [s.strip() for s in request.symbols.split(',') if s.strip()]
            
            result = await service.batch_analyze_news(
                limit=request.batch_size,
                template_code=request.template_code,
                symbols=symbols,
                start_date=request.start_date,
                end_date=request.end_date,
                user_id=current_user.user_id,
                use_concurrent=request.use_concurrent,
                max_concurrent=request.max_concurrent
            )
            
            return {
                "success": True,
                "message": f"批量分析完成: 成功{result['success']}条，失败{result['failed']}条",
                "data": result
            }
            
    except Exception as e:
        logger.error(f"批量分析新闻失败: {str(e)}")
        raise HTTPException(status_code=500, detail=f"批量分析失败: {str(e)}")


@router.post("/analyze-batch-celery", summary="批量分析新闻（Celery异步）")
async def analyze_batch_celery(
    request: BatchAnalyzeNewsRequest,
    current_user: TokenData = Depends(get_current_user)
):
    """
    使用Celery异步批量分析新闻
    
    适用场景：
    - 大批量新闻（100条以上）
    - 需要后台运行，不阻塞API请求
    - 需要分布式处理
    """
    try:
        from backend.tasks.news_tasks import analyze_unanalyzed_news_task, analyze_news_batch_task
        
        # 如果提供了新闻ID列表
        if request.news_ids:
            task = analyze_news_batch_task.delay(
                news_ids=request.news_ids,
                template_code=request.template_code,
                user_id=current_user.user_id
            )
            
            return {
                "success": True,
                "message": f"已提交Celery任务，分析{len(request.news_ids)}条新闻",
                "data": {
                    "task_id": task.id,
                    "news_count": len(request.news_ids)
                }
            }
        else:
            # 分析未分析的新闻
            symbols = None
            if request.symbols:
                symbols = [s.strip() for s in request.symbols.split(',') if s.strip()]
            
            task = analyze_unanalyzed_news_task.delay(
                limit=request.batch_size,
                template_code=request.template_code,
                symbols=symbols,
                start_date=request.start_date,
                end_date=request.end_date,
                max_concurrent=request.max_concurrent
            )
            
            return {
                "success": True,
                "message": f"已提交Celery任务，最多分析{request.batch_size}条未分析的新闻",
                "data": {
                    "task_id": task.id,
                    "batch_size": request.batch_size
                }
            }
            
    except Exception as e:
        logger.error(f"提交Celery任务失败: {str(e)}")
        raise HTTPException(status_code=500, detail=f"提交任务失败: {str(e)}")


@router.get("/statistics", response_model=AnalysisStatisticsResponse, summary="获取分析统计")
async def get_statistics(
    current_user: TokenData = Depends(get_current_user)
):
    """
    获取新闻分析统计信息
    """
    try:
        from backend.services.news_analysis_service import NewsAnalysisService
        
        db_config = get_database_config()
        service = NewsAnalysisService(db_config)
        
        stats = service.get_analysis_statistics()
        
        return stats
        
    except Exception as e:
        logger.error(f"获取统计信息失败: {str(e)}")
        raise HTTPException(status_code=500, detail=f"获取统计失败: {str(e)}")


@router.get("/unanalyzed", summary="获取未分析的新闻")
async def get_unanalyzed_news(
    limit: int = Query(default=20, ge=1, le=100, description="返回数量"),
    symbols: Optional[str] = Query(None, description="筛选股票代码，逗号分隔"),
    start_date: Optional[str] = Query(None, description="开始日期 YYYY-MM-DD"),
    end_date: Optional[str] = Query(None, description="结束日期 YYYY-MM-DD"),
    current_user: TokenData = Depends(get_current_user)
):
    """
    获取未分析的新闻列表
    """
    try:
        from backend.services.news_analysis_service import NewsAnalysisService
        
        db_config = get_database_config()
        service = NewsAnalysisService(db_config)
        
        # 处理symbols参数
        symbols_list = None
        if symbols:
            symbols_list = [s.strip() for s in symbols.split(',') if s.strip()]
        
        news_list = service.get_unanalyzed_news(
            limit=limit,
            symbols=symbols_list,
            start_date=start_date,
            end_date=end_date
        )
        
        return {
            "success": True,
            "total": len(news_list),
            "news": news_list
        }
        
    except Exception as e:
        logger.error(f"获取未分析新闻失败: {str(e)}")
        raise HTTPException(status_code=500, detail=f"获取失败: {str(e)}")


@router.get("/{news_id}/analysis", summary="获取新闻分析结果")
async def get_news_analysis(
    news_id: int,
    current_user: TokenData = Depends(get_current_user)
):
    """
    获取指定新闻的LLM分析结果
    """
    try:
        from backend.database.db_manager import DatabaseManager
        
        db_config = get_database_config()
        db_manager = DatabaseManager(db_config)
        
        conn = db_manager.get_connection()
        if not conn:
            raise HTTPException(status_code=500, detail="数据库连接失败")
        
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 
                    id, title, analysis_result, analyzed_at, 
                    analysis_template_code, analysis_status, analysis_error
                FROM news
                WHERE id = %s
            """, (news_id,))
            
            row = cursor.fetchone()
            
            if not row:
                raise HTTPException(status_code=404, detail="新闻未找到")
            
            result = {
                'news_id': row[0],
                'title': row[1],
                'analysis_result': row[2],
                'analyzed_at': row[3].isoformat() if row[3] else None,
                'template_code': row[4],
                'analysis_status': row[5],
                'analysis_error': row[6]
            }
            
            cursor.close()
            conn.close()
            
            return {
                "success": True,
                "data": result
            }
            
        except HTTPException:
            raise
        except Exception as e:
            if conn:
                conn.close()
            raise HTTPException(status_code=500, detail=str(e))
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取分析结果失败: {str(e)}")
        raise HTTPException(status_code=500, detail=f"获取失败: {str(e)}")

