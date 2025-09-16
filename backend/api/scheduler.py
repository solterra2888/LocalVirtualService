# -*- coding: utf-8 -*-
"""
定时任务调度器API - 管理Cron表达式定期数据获取策略
"""

from fastapi import APIRouter, HTTPException, Depends, Query, Body
from typing import Optional, Dict, Any, List
from pydantic import BaseModel, Field
from datetime import datetime
import pytz

from backend.auth.jwt_utils import get_current_user
from backend.services.scheduler_service import get_scheduler_service
from backend.utils.logger import get_logger

logger = get_logger("scheduler_api")

router = APIRouter(prefix="/scheduler", tags=["Scheduler"])


# Pydantic模型
class CreateCronTaskRequest(BaseModel):
    """创建Cron定时任务请求模型"""
    name: str = Field(..., description="任务名称")
    task_type: str = Field(..., description="任务类型", pattern="^(eodhd_news|economic_events|macro_indicators|eodhd_fundamentals|finviz_screener)$")
    cron_expression: str = Field(..., description="Cron表达式 (分 时 日 月 星期)")
    timezone: str = Field(default="America/New_York", description="时区")
    enabled: bool = Field(default=True, description="是否启用")
    params: Optional[Dict[str, Any]] = Field(default=None, description="任务参数")


class CreateIntervalTaskRequest(BaseModel):
    """创建间隔定时任务请求模型"""
    name: str = Field(..., description="任务名称")
    task_type: str = Field(..., description="任务类型", pattern="^(eodhd_news|economic_events|macro_indicators|eodhd_fundamentals|finviz_screener)$")
    interval_minutes: int = Field(..., gt=0, description="间隔分钟数")
    timezone: str = Field(default="America/New_York", description="时区")
    enabled: bool = Field(default=True, description="是否启用")
    params: Optional[Dict[str, Any]] = Field(default=None, description="任务参数")


class TaskConfigResponse(BaseModel):
    """任务配置响应模型"""
    id: str
    name: str
    type: str
    schedule_type: str
    cron_expression: Optional[str] = None
    interval_minutes: Optional[int] = None
    timezone: str
    enabled: bool
    params: Dict[str, Any]
    created_at: str
    last_run: Optional[str] = None
    next_run: Optional[str] = None
    success_count: int
    fail_count: int
    status: Optional[str] = None


class CreateEODHDNewsTaskRequest(BaseModel):
    """创建EODHD新闻定时任务请求模型"""
    name: str = Field(default="EODHD News Auto Fetch", description="任务名称")
    cron_expression: str = Field(default="*/15 * * * *", description="Cron表达式")
    timezone: str = Field(default="America/New_York", description="时区")
    enabled: bool = Field(default=True, description="是否启用")
    max_results: int = Field(default=100, ge=1, le=1000, description="最大结果数")
    symbols: Optional[str] = Field(default=None, description="股票代码，逗号分隔")
    topic: Optional[str] = Field(default=None, description="题材标签")
    topic_filter: Optional[str] = Field(default=None, description="主题过滤器")
    keywords: Optional[str] = Field(default=None, description="关键词，逗号分隔")


class CreateEODHDFundamentalsTaskRequest(BaseModel):
    """创建EODHD基本面数据定时任务请求模型"""
    name: str = Field(default="EODHD Fundamentals", description="任务名称")
    cron_expression: str = Field(default="0 6 * * 0", description="Cron表达式 (每周日6:00AM)")
    timezone: str = Field(default="America/New_York", description="时区")
    enabled: bool = Field(default=True, description="是否启用")
    exchanges: List[str] = Field(default=["NYSE", "NASDAQ"], description="交易所列表，支持美国、中国和香港交易所")
    data_types: List[str] = Field(default=["General", "Financials"], description="数据类型列表 (General: 公司基本信息, Financials: 财务报表数据, Highlights: 关键指标, Valuation: 估值数据, Technicals: 技术指标, Holders: 股东数据, outstandingShares: 流通股数据, Earnings: 盈利数据, SharesStats: 股票统计, AnalystRatings: 分析师评级, SplitsDividends: 拆股分红, InsiderTransactions: 内部人交易)")


class CreateFinvizScreenerTaskRequest(BaseModel):
    """创建Finviz筛选数据定时任务请求模型"""
    name: str = Field(default="Finviz Screener Data", description="任务名称")
    cron_expression: str = Field(default="0 8 * * 1-5", description="Cron表达式 (工作日8:00AM)")
    timezone: str = Field(default="America/New_York", description="时区")
    enabled: bool = Field(default=True, description="是否启用")


# ============ 定时任务管理API ============

@router.post("/tasks/cron", summary="创建Cron定时任务")
async def create_cron_task(
    request: CreateCronTaskRequest,
    current_user=Depends(get_current_user)
):
    """
    创建基于Cron表达式的定时任务
    
    支持的任务类型：
    - eodhd_news: EODHD新闻数据获取
    - economic_events: 经济事件数据获取  
    - macro_indicators: 宏观指标数据获取
    
    Cron表达式格式：分 时 日 月 星期
    例如：
    - "*/15 * * * *": 每15分钟执行
    - "0 9,17 * * 1-5": 工作日早上9点和下午5点执行
    - "0 10 * * 1": 每周一上午10点执行
    """
    try:
        logger.info("创建Cron定时任务请求", 
                   user=current_user.username,
                   task_name=request.name,
                   task_type=request.task_type,
                   cron_expression=request.cron_expression)
        
        scheduler_service = await get_scheduler_service()
        
        result = await scheduler_service.create_cron_task(
            task_name=request.name,
            task_type=request.task_type,
            cron_expression=request.cron_expression,
            timezone=request.timezone,
            enabled=request.enabled,
            task_params=request.params,
            created_by=current_user.username
        )
        
        if result['success']:
            logger.info("Cron定时任务创建成功", 
                       task_id=result['task_id'],
                       user=current_user.username)
            return {
                "success": True,
                "message": result['message'],
                "data": {
                    "task_id": result['task_id'],
                    "task_config": result['task_config']
                }
            }
        else:
            logger.error("Cron定时任务创建失败", 
                        failure_reason=result['message'],
                        user=current_user.username)
            raise HTTPException(status_code=400, detail=result['message'])
        
    except ValueError as e:
        logger.error("Cron定时任务创建参数错误", exception=str(e), user=current_user.username)
        raise HTTPException(status_code=400, detail=f"参数错误: {str(e)}")
    except Exception as e:
        logger.error("Cron定时任务创建失败", exception=str(e), user=current_user.username)
        raise HTTPException(status_code=500, detail=f"创建定时任务失败: {str(e)}")


@router.post("/tasks/interval", summary="创建间隔定时任务")
async def create_interval_task(
    request: CreateIntervalTaskRequest,
    current_user=Depends(get_current_user)
):
    """
    创建基于固定间隔的定时任务
    """
    try:
        logger.info("创建间隔定时任务请求", 
                   user=current_user.username,
                   task_name=request.name,
                   task_type=request.task_type,
                   interval_minutes=request.interval_minutes)
        
        scheduler_service = await get_scheduler_service()
        
        result = await scheduler_service.create_interval_task(
            task_name=request.name,
            task_type=request.task_type,
            interval_minutes=request.interval_minutes,
            timezone=request.timezone,
            enabled=request.enabled,
            task_params=request.params,
            created_by=current_user.username
        )
        
        if result['success']:
            logger.info("间隔定时任务创建成功", 
                       task_id=result['task_id'],
                       user=current_user.username)
            return {
                "success": True,
                "message": result['message'],
                "data": {
                    "task_id": result['task_id'],
                    "task_config": result['task_config']
                }
            }
        else:
            logger.error("间隔定时任务创建失败", 
                        failure_reason=result['message'],
                        user=current_user.username)
            raise HTTPException(status_code=400, detail=result['message'])
        
    except ValueError as e:
        logger.error("间隔定时任务创建参数错误", exception=str(e), user=current_user.username)
        raise HTTPException(status_code=400, detail=f"参数错误: {str(e)}")
    except Exception as e:
        logger.error("间隔定时任务创建失败", exception=str(e), user=current_user.username)
        raise HTTPException(status_code=500, detail=f"创建定时任务失败: {str(e)}")


@router.get("/tasks", summary="获取所有定时任务")
async def get_tasks(
    current_user=Depends(get_current_user)
) -> List[TaskConfigResponse]:
    """
    获取所有定时任务列表
    """
    try:
        logger.info("获取定时任务列表请求", user=current_user.username)
        
        scheduler_service = await get_scheduler_service()
        tasks = await scheduler_service.get_tasks()
        
        logger.info(f"成功获取 {len(tasks)} 个定时任务", user=current_user.username)
        return tasks
        
    except Exception as e:
        logger.error("获取定时任务列表失败", exception=str(e), user=current_user.username)
        raise HTTPException(status_code=500, detail=f"获取定时任务列表失败: {str(e)}")


@router.post("/tasks/{task_id}/enable", summary="启用定时任务")
async def enable_task(
    task_id: str,
    current_user=Depends(get_current_user)
):
    """
    启用指定的定时任务
    """
    try:
        logger.info("启用定时任务请求", task_id=task_id, user=current_user.username)
        
        scheduler_service = await get_scheduler_service()
        result = await scheduler_service.enable_task(task_id)
        
        if result['success']:
            logger.info("定时任务启用成功", task_id=task_id, user=current_user.username)
            return {
                "success": True,
                "message": result['message']
            }
        else:
            logger.error("定时任务启用失败", 
                        task_id=task_id,
                        failure_reason=result['message'],
                        user=current_user.username)
            raise HTTPException(status_code=400, detail=result['message'])
        
    except Exception as e:
        logger.error("启用定时任务失败", task_id=task_id, exception=str(e), user=current_user.username)
        raise HTTPException(status_code=500, detail=f"启用定时任务失败: {str(e)}")


@router.post("/tasks/{task_id}/disable", summary="禁用定时任务")
async def disable_task(
    task_id: str,
    current_user=Depends(get_current_user)
):
    """
    禁用指定的定时任务
    """
    try:
        logger.info("禁用定时任务请求", task_id=task_id, user=current_user.username)
        
        scheduler_service = await get_scheduler_service()
        result = await scheduler_service.disable_task(task_id)
        
        if result['success']:
            logger.info("定时任务禁用成功", task_id=task_id, user=current_user.username)
            return {
                "success": True,
                "message": result['message']
            }
        else:
            logger.error("定时任务禁用失败", 
                        task_id=task_id,
                        failure_reason=result['message'],
                        user=current_user.username)
            raise HTTPException(status_code=400, detail=result['message'])
        
    except Exception as e:
        logger.error("禁用定时任务失败", task_id=task_id, exception=str(e), user=current_user.username)
        raise HTTPException(status_code=500, detail=f"禁用定时任务失败: {str(e)}")


@router.delete("/tasks/{task_id}", summary="删除定时任务")
async def delete_task(
    task_id: str,
    current_user=Depends(get_current_user)
):
    """
    删除指定的定时任务
    """
    try:
        logger.info("删除定时任务请求", task_id=task_id, user=current_user.username)
        
        scheduler_service = await get_scheduler_service()
        result = await scheduler_service.delete_task(task_id)
        
        if result['success']:
            logger.info("定时任务删除成功", task_id=task_id, user=current_user.username)
            return {
                "success": True,
                "message": result['message']
            }
        else:
            logger.error("定时任务删除失败", 
                        task_id=task_id,
                        failure_reason=result['message'],
                        user=current_user.username)
            raise HTTPException(status_code=400, detail=result['message'])
        
    except Exception as e:
        logger.error("删除定时任务失败", task_id=task_id, exception=str(e), user=current_user.username)
        raise HTTPException(status_code=500, detail=f"删除定时任务失败: {str(e)}")


# ============ EODHD新闻定时任务专用API ============

@router.post("/tasks/eodhd-news", summary="创建EODHD新闻定时任务")
async def create_eodhd_news_task(
    request: CreateEODHDNewsTaskRequest,
    current_user=Depends(get_current_user)
):
    """
    创建EODHD新闻数据获取定时任务
    
    默认配置：
    - 纽约时间每15分钟执行 (*/15 * * * *)
    - 获取最新100条新闻
    - 使用用户预设的默认参数
    """
    try:
        logger.info("创建EODHD新闻定时任务请求", 
                   user=current_user.username,
                   task_name=request.name,
                   cron_expression=request.cron_expression)
        
        scheduler_service = await get_scheduler_service()
        
        # 构建任务参数
        task_params = {
            'max_results': request.max_results,
            'symbols': request.symbols,
            'topic': request.topic,
            'topic_filter': request.topic_filter,
            'keywords': request.keywords.split(',') if request.keywords else None
        }
        
        result = await scheduler_service.create_cron_task(
            task_name=request.name,
            task_type="eodhd_news",
            cron_expression=request.cron_expression,
            timezone=request.timezone,
            enabled=request.enabled,
            task_params=task_params,
            created_by=current_user.username
        )
        
        if result['success']:
            logger.info("EODHD新闻定时任务创建成功", 
                       task_id=result['task_id'],
                       user=current_user.username)
            return {
                "success": True,
                "message": result['message'],
                "data": {
                    "task_id": result['task_id'],
                    "task_config": result['task_config']
                }
            }
        else:
            logger.error("EODHD新闻定时任务创建失败", 
                        failure_reason=result['message'],
                        user=current_user.username)
            raise HTTPException(status_code=400, detail=result['message'])
        
    except ValueError as e:
        logger.error("EODHD新闻定时任务创建参数错误", exception=str(e), user=current_user.username)
        raise HTTPException(status_code=400, detail=f"参数错误: {str(e)}")
    except Exception as e:
        logger.error("EODHD新闻定时任务创建失败", exception=str(e), user=current_user.username)
        raise HTTPException(status_code=500, detail=f"创建定时任务失败: {str(e)}")


# ============ EODHD基本面数据定时任务专用API ============

@router.post("/tasks/eodhd-fundamentals", summary="创建EODHD基本面数据定时任务")
async def create_eodhd_fundamentals_task(
    request: CreateEODHDFundamentalsTaskRequest,
    current_user=Depends(get_current_user)
):
    """
    创建EODHD基本面数据获取定时任务
    
    默认配置：
    - 纽约时间每周日6:00AM执行 (0 6 * * 0)
    - 支持美国、中国、香港、台湾、日本、英国交易所 (NYSE, NASDAQ, AMEX, SHG, SHE, HK, TWO, JP, LSE, AIM)
    - 支持12种数据类型：General（基本信息）、Financials（财务报表）、Highlights（关键指标）、Valuation（估值数据）、Technicals（技术指标）、Holders（股东数据）、outstandingShares（流通股数据）、Earnings（盈利数据）、SharesStats（股票统计）、AnalystRatings（分析师评级）、SplitsDividends（拆股分红）、InsiderTransactions（内部人交易）
    """
    try:
        logger.info("创建EODHD基本面数据定时任务请求", 
                   user=current_user.username,
                   task_name=request.name,
                   cron_expression=request.cron_expression,
                   exchanges=request.exchanges,
                   data_types=request.data_types)
        
        # 验证交易所 - 支持美国、中国、香港、台湾、日本、英国
        supported_exchanges = ['NYSE', 'NASDAQ', 'AMEX', 'SHG', 'SHE', 'HK', 'TWO', 'JP', 'LSE', 'AIM']
        valid_exchanges = [ex for ex in request.exchanges if ex in supported_exchanges]
        if not valid_exchanges:
            raise HTTPException(
                status_code=400, 
                detail=f"不支持的交易所，支持的交易所: {', '.join(supported_exchanges)}"
            )
        
        # 验证数据类型 - 支持所有基本面数据类型，包括新增的4种类型
        supported_data_types = [
            'General', 'Financials', 'Highlights', 'Valuation', 'Technicals', 
            'Holders', 'outstandingShares', 'Earnings', 'SharesStats', 
            'AnalystRatings', 'SplitsDividends', 'InsiderTransactions'
        ]
        valid_data_types = [dt for dt in request.data_types if dt in supported_data_types]
        if not valid_data_types:
            raise HTTPException(
                status_code=400,
                detail=f"不支持的数据类型，支持的数据类型: {', '.join(supported_data_types)}"
            )
        
        scheduler_service = await get_scheduler_service()
        
        # 构建任务参数
        task_params = {
            'exchanges': valid_exchanges,
            'data_types': valid_data_types
        }
        
        result = await scheduler_service.create_cron_task(
            task_name=request.name,
            task_type="eodhd_fundamentals",
            cron_expression=request.cron_expression,
            timezone=request.timezone,
            enabled=request.enabled,
            task_params=task_params,
            created_by=current_user.username
        )
        
        if result['success']:
            logger.info("EODHD基本面数据定时任务创建成功", 
                       user=current_user.username,
                       task_name=request.name,
                       task_id=result['task_id'],
                       exchanges=valid_exchanges,
                       data_types=valid_data_types)
            
            return {
                "success": True,
                "message": result['message'],
                "data": {
                    "task_id": result['task_id'],
                    "task_config": result['task_config']
                }
            }
        else:
            logger.error("EODHD基本面数据定时任务创建失败", 
                        failure_reason=result.get('message', '未知错误'),
                        user=current_user.username,
                        task_name=request.name)
            raise HTTPException(status_code=400, detail=result.get('message', '创建任务失败'))
    
    except HTTPException:
        raise
    except ValueError as e:
        logger.error("EODHD基本面数据低频定时任务创建参数错误", exception=str(e), user=current_user.username)
        raise HTTPException(status_code=400, detail=f"参数错误: {str(e)}")
    except Exception as e:
        logger.error("创建EODHD基本面数据低频定时任务失败", 
                    exception=str(e), 
                    user=current_user.username,
                    task_name=request.name)
        raise HTTPException(status_code=500, detail=f"创建定时任务失败: {str(e)}")


# ============ Finviz筛选数据定时任务专用API ============

@router.post("/tasks/finviz-screener", summary="创建Finviz筛选数据定时任务")
async def create_finviz_screener_task(
    request: CreateFinvizScreenerTaskRequest,
    current_user=Depends(get_current_user)
):
    """
    创建Finviz筛选数据获取定时任务
    
    默认配置：
    - 纽约时间工作日8:00AM执行 (0 8 * * 1-5)
    - 不需要参数配置，直接获取所有筛选数据
    """
    try:
        logger.info("创建Finviz筛选数据定时任务请求", 
                   user=current_user.username,
                   task_name=request.name,
                   cron_expression=request.cron_expression)
        
        scheduler_service = await get_scheduler_service()
        
        # Finviz任务不需要参数，传递空字典
        task_params = {}
        
        result = await scheduler_service.create_cron_task(
            task_name=request.name,
            task_type="finviz_screener",
            cron_expression=request.cron_expression,
            timezone=request.timezone,
            enabled=request.enabled,
            task_params=task_params,
            created_by=current_user.username
        )
        
        if result['success']:
            logger.info("Finviz筛选数据定时任务创建成功", 
                       user=current_user.username,
                       task_name=request.name,
                       task_id=result['task_id'])
            
            return {
                "success": True,
                "message": result['message'],
                "data": {
                    "task_id": result['task_id'],
                    "task_config": result['task_config']
                }
            }
        else:
            logger.error("Finviz筛选数据定时任务创建失败", 
                        failure_reason=result.get('message', '未知错误'),
                        user=current_user.username,
                        task_name=request.name)
            raise HTTPException(status_code=400, detail=result.get('message', '创建任务失败'))
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error("创建Finviz筛选数据定时任务失败", 
                    exception=str(e), 
                    user=current_user.username,
                    task_name=request.name)
        raise HTTPException(status_code=500, detail=f"创建定时任务失败: {str(e)}")


@router.post("/tasks/eodhd-news/default", summary="创建默认EODHD新闻定时任务")
async def create_default_eodhd_news_task(
    current_user=Depends(get_current_user)
):
    """
    创建默认的EODHD新闻定时任务
    
    默认配置：
    - 任务名称：EODHD News Auto Fetch
    - 纽约时间每15分钟执行 (*/15 * * * *)
    - 获取最新100条新闻
    - 不指定特定股票代码、主题或关键词
    """
    try:
        logger.info("创建默认EODHD新闻定时任务请求", user=current_user.username)
        
        scheduler_service = await get_scheduler_service()
        result = await scheduler_service.create_default_eodhd_news_task(created_by=current_user.username)
        
        if result['success']:
            logger.info("默认EODHD新闻定时任务创建成功", 
                       task_id=result['task_id'],
                       user=current_user.username)
            return {
                "success": True,
                "message": result['message'],
                "data": {
                    "task_id": result['task_id'],
                    "task_config": result['task_config']
                }
            }
        else:
            logger.error("默认EODHD新闻定时任务创建失败", 
                        failure_reason=result['message'],
                        user=current_user.username)
            raise HTTPException(status_code=400, detail=result['message'])
        
    except Exception as e:
        logger.error("创建默认EODHD新闻定时任务失败", exception=str(e), user=current_user.username)
        raise HTTPException(status_code=500, detail=f"创建默认定时任务失败: {str(e)}")


# ============ 手动执行任务API ============

@router.post("/execute/eodhd-fundamentals", summary="手动执行EODHD基本面数据更新")
async def execute_fundamentals_update(
    request: dict = Body(..., description="执行参数"),
    current_user=Depends(get_current_user)
):
    """
    手动执行EODHD基本面数据更新，不创建定时任务
    
    参数：
    - exchanges: 交易所列表，如 ["NYSE", "NASDAQ"]
    - data_types: 数据类型列表，如 ["General", "Financials", "Highlights"]
    """
    try:
        logger.info("手动执行EODHD基本面数据更新请求", 
                   user=current_user.username,
                   exchanges=request.get('exchanges', []),
                   data_types=request.get('data_types', []))
        
        scheduler_service = await get_scheduler_service()
        
        # 构建任务参数
        task_params = {
            'exchanges': request.get('exchanges', ['NYSE', 'NASDAQ']),
            'data_types': request.get('data_types', ['General', 'Financials', 'Highlights', 'Valuation', 'Technicals'])
        }
        
        # 直接执行基本面数据获取任务
        result = await scheduler_service._execute_fundamentals_task(task_params)
        
        if result['success']:
            logger.info("EODHD基本面数据更新执行成功", 
                       user=current_user.username,
                       result_message=result['message'])
            return {
                "success": True,
                "message": result['message'],
                "data": result.get('data', {})
            }
        else:
            logger.error("EODHD基本面数据更新执行失败", 
                        failure_reason=result['message'],
                        user=current_user.username)
            raise HTTPException(status_code=400, detail=result['message'])
        
    except Exception as e:
        logger.error("手动执行EODHD基本面数据更新失败", exception=str(e), user=current_user.username)
        raise HTTPException(status_code=500, detail=f"手动执行基本面数据更新失败: {str(e)}")


# ============ 任务执行日志API ============

@router.get("/tasks/{task_id}/logs", summary="获取任务执行日志")
async def get_task_logs(
    task_id: str,
    limit: int = Query(default=50, ge=1, le=500, description="返回日志条数"),
    offset: int = Query(default=0, ge=0, description="偏移量")
):
    """
    获取指定任务的执行日志
    """
    try:
        logger.info("获取任务执行日志请求", 
                   task_id=task_id,
                   limit=limit,
                   offset=offset)
        
        scheduler_service = await get_scheduler_service()
        logs = await scheduler_service.get_task_execution_logs(task_id, limit, offset)
        
        return {
            "success": True,
            "message": "获取任务执行日志成功",
            "data": {
                "logs": logs,
                "total": len(logs),
                "limit": limit,
                "offset": offset
            }
        }
        
    except Exception as e:
        logger.error("获取任务执行日志失败", 
                    task_id=task_id, 
                    exception=str(e))
        raise HTTPException(status_code=500, detail=f"获取任务执行日志失败: {str(e)}")


@router.get("/execution-logs", summary="获取所有任务执行日志")
async def get_all_execution_logs(
    limit: int = Query(default=50, ge=1, le=500, description="返回日志条数"),
    offset: int = Query(default=0, ge=0, description="偏移量")
):
    """
    获取所有任务的执行日志
    """
    try:
        logger.info("获取所有任务执行日志请求", 
                   limit=limit,
                   offset=offset)
        
        scheduler_service = await get_scheduler_service()
        logs = await scheduler_service.get_task_execution_logs(None, limit, offset)
        
        return {
            "success": True,
            "message": "获取所有任务执行日志成功",
            "data": {
                "logs": logs,
                "total": len(logs),
                "limit": limit,
                "offset": offset
            }
        }
        
    except Exception as e:
        logger.error("获取所有任务执行日志失败", 
                    exception=str(e))
        raise HTTPException(status_code=500, detail=f"获取所有任务执行日志失败: {str(e)}")


@router.get("/execution-logs/download", summary="下载执行日志CSV")
async def download_execution_logs_csv(
    task_id: Optional[str] = Query(default=None, description="任务ID，不提供则下载所有任务日志"),
    limit: int = Query(default=1000, ge=1, le=10000, description="返回日志条数"),
    offset: int = Query(default=0, ge=0, description="偏移量")
):
    """
    下载任务执行日志为CSV格式
    """
    try:
        logger.info("下载执行日志CSV请求", 
                   task_id=task_id,
                   limit=limit,
                   offset=offset)
        
        scheduler_service = await get_scheduler_service()
        logs = await scheduler_service.get_task_execution_logs(task_id, limit, offset)
        
        logger.info(f"获取到 {len(logs)} 条执行日志记录", 
                   task_id=task_id,
                   limit=limit,
                   offset=offset)
        
        # 生成CSV内容
        import csv
        import io
        
        output = io.StringIO()
        writer = csv.writer(output)
        
        # 写入CSV头部
        headers = [
            "任务ID", "任务名称", "任务类型", "开始时间", "结束时间", 
            "执行耗时(毫秒)", "状态", "结果消息", "错误消息", "执行参数"
        ]
        writer.writerow(headers)
        
        # 写入数据行
        for log in logs:
            row = [
                log.get("id", ""),
                log.get("task_name", ""),
                log.get("task_type", ""),
                log.get("started_at", ""),
                log.get("finished_at", ""),
                log.get("duration_ms", ""),
                log.get("status", ""),
                log.get("result_message", ""),
                log.get("error_message", ""),
                str(log.get("execution_params", ""))
            ]
            writer.writerow(row)
        
        csv_content = output.getvalue()
        output.close()
        
        # 生成文件名
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        if task_id:
            filename = f"task_execution_logs_{task_id}_{timestamp}.csv"
        else:
            filename = f"all_execution_logs_{timestamp}.csv"
        
        logger.info(f"成功生成CSV文件: {filename}", 
                   record_count=len(logs))
        
        from fastapi.responses import StreamingResponse
        
        return StreamingResponse(
            io.StringIO(csv_content),
            media_type="text/csv",
            headers={
                "Content-Disposition": f"attachment; filename={filename}",
                "Content-Type": "text/csv; charset=utf-8"
            }
        )
        
    except Exception as e:
        logger.error("下载执行日志CSV失败", 
                    task_id=task_id,
                    exception=str(e))
        raise HTTPException(status_code=500, detail=f"下载执行日志CSV失败: {str(e)}")


@router.delete("/execution-logs/clear", summary="清空执行日志")
async def clear_execution_logs():
    """
    清空所有任务执行日志
    """
    try:
        logger.info("清空执行日志请求")
        
        scheduler_service = await get_scheduler_service()
        
        # 执行清空操作
        success = await scheduler_service.clear_execution_logs()
        
        if success:
            logger.info("成功清空执行日志")
            return {
                "success": True,
                "message": "执行日志已清空"
            }
        else:
            logger.error("清空执行日志失败")
            raise HTTPException(status_code=500, detail="清空执行日志失败")
        
    except Exception as e:
        logger.error("清空执行日志失败", 
                    exception=str(e))
        raise HTTPException(status_code=500, detail=f"清空执行日志失败: {str(e)}")


# ============ 任务预设配置API ============

@router.get("/presets/cron-expressions", summary="获取常用Cron表达式预设")
async def get_cron_presets():
    """
    获取常用的Cron表达式预设
    """
    presets = [
        {
            "name": "每分钟",
            "expression": "* * * * *",
            "description": "每分钟执行一次"
        },
        {
            "name": "每5分钟",
            "expression": "*/5 * * * *",
            "description": "每5分钟执行一次"
        },
        {
            "name": "每15分钟",
            "expression": "*/15 * * * *",
            "description": "每15分钟执行一次（推荐用于新闻获取）"
        },
        {
            "name": "每30分钟",
            "expression": "*/30 * * * *",
            "description": "每30分钟执行一次"
        },
        {
            "name": "每小时",
            "expression": "0 * * * *",
            "description": "每小时的开始执行"
        },
        {
            "name": "工作日上午9点",
            "expression": "0 9 * * 1-5",
            "description": "周一到周五上午9点执行"
        },
        {
            "name": "工作日下午5点",
            "expression": "0 17 * * 1-5",
            "description": "周一到周五下午5点执行"
        },
        {
            "name": "每周一上午10点",
            "expression": "0 10 * * 1",
            "description": "每周一上午10点执行"
        },
        {
            "name": "每月1号上午9点",
            "expression": "0 9 1 * *",
            "description": "每月1号上午9点执行"
        }
    ]
    
    return {
        "success": True,
        "message": "获取Cron表达式预设成功",
        "data": presets
    }


@router.get("/presets/timezones", summary="Get supported timezone list")
async def get_timezone_presets():
    """
    Get supported timezone list
    """
    timezones = [
        {
            "value": "America/New_York",
            "label": "America/New_York Time Zone (EST/EDT)",
            "description": "New York time"
        },
        {
            "value": "Asia/Shanghai",
            "label": "Asia/Shanghai Time Zone (CST)",
            "description": "Beijing time"
        }
    ]
    
    return {
        "success": True,
        "message": "Successfully retrieved timezone list",
        "data": timezones
    }


# ============ 数据库表查看API ============

@router.get("/debug/database-tables", summary="查看数据库表数据（调试用）")
async def get_database_tables(
    table_name: str = Query(..., description="表名", pattern="^task_scheduling$"),
    limit: int = Query(default=50, ge=1, le=1000, description="返回记录数"),
    current_user=Depends(get_current_user)
):
    """
    查看调度器相关数据库表的数据（仅用于调试和管理）
    
    支持的表：
    - task_scheduling: 定时任务配置表
    """
    try:
        logger.info("查看数据库表数据请求", 
                   table_name=table_name,
                   limit=limit,
                   user=current_user.username)
        
        scheduler_service = await get_scheduler_service()
        
        # 根据表名构建查询
        if table_name == "task_scheduling":
            query = "SELECT * FROM task_scheduling ORDER BY created_at DESC LIMIT %s"
            columns = [
                "id", "name", "type", "schedule_type", "cron_expression", 
                "interval_minutes", "timezone", "enabled", "params", 
                "created_at", "last_run", "next_run", "success_count", 
                "fail_count", "created_by", "updated_at"
            ]
        else:
            raise HTTPException(status_code=400, detail=f"不支持的表名: {table_name}")
        
        # 执行查询
        if limit:
            results = scheduler_service.db_manager.fetch_all(query, [limit])
        else:
            results = scheduler_service.db_manager.fetch_all(query)
        
        # 转换结果为字典格式
        data = []
        for row in results:
            row_dict = {}
            for i, column in enumerate(columns):
                value = row[i] if i < len(row) else None
                # 处理特殊类型
                if isinstance(value, (dict, list)):
                    row_dict[column] = value
                elif hasattr(value, 'isoformat'):  # datetime对象
                    row_dict[column] = value.isoformat()
                else:
                    row_dict[column] = value
            data.append(row_dict)
        
        logger.info(f"成功获取 {len(data)} 条 {table_name} 表记录", user=current_user.username)
        
        return {
            "success": True,
            "message": f"成功获取{table_name}表数据",
            "data": {
                "table_name": table_name,
                "columns": columns,
                "rows": data,
                "total_count": len(data)
            }
        }
        
    except Exception as e:
        logger.error("查看数据库表数据失败", 
                    table_name=table_name,
                    exception=str(e), 
                    user=current_user.username)
        raise HTTPException(status_code=500, detail=f"查看数据库表数据失败: {str(e)}")


@router.get("/debug/table-structure", summary="查看表结构")
async def get_table_structure(
    current_user=Depends(get_current_user)
):
    """
    查看调度器相关表的结构信息
    """
    try:
        logger.info("查看表结构请求", user=current_user.username)
        
        scheduler_service = await get_scheduler_service()
        
        tables_info = {}
        table_names = ["task_scheduling", "task_execution_logs"]
        
        for table_name in table_names:
            query = """
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns 
            WHERE table_name = %s
            ORDER BY ordinal_position
            """
            results = scheduler_service.db_manager.fetch_all(query, [table_name])
            
            columns = []
            for row in results:
                columns.append({
                    "name": row[0],
                    "type": row[1],
                    "nullable": row[2] == "YES",
                    "default": row[3]
                })
            
            tables_info[table_name] = {
                "columns": columns,
                "total_columns": len(columns)
            }
        
        return {
            "success": True,
            "message": "成功获取表结构信息",
            "data": tables_info
        }
        
    except Exception as e:
        logger.error("查看表结构失败", exception=str(e), user=current_user.username)
        raise HTTPException(status_code=500, detail=f"查看表结构失败: {str(e)}")
