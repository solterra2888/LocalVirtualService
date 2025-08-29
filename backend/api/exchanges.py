# -*- coding: utf-8 -*-
"""
EODHD Exchange Data API - 交易所数据管理接口
"""

from fastapi import APIRouter, HTTPException, Depends, Query, Body
from typing import Optional, Dict, Any, List
from pydantic import BaseModel, Field

from backend.auth.jwt_utils import get_current_user
from backend.services.exchange_data_service import get_exchange_service
from backend.utils.logger import get_logger

logger = get_logger("exchanges_api")

router = APIRouter(prefix="/exchanges", tags=["Exchanges"])


# Pydantic模型
class ExchangeResponse(BaseModel):
    """交易所响应模型"""
    code: str
    name: str
    country: str
    timezone: str
    mic_code: Optional[str] = None
    enabled: bool
    last_update: Optional[str] = None
    total_tickers: int
    created_at: Optional[str] = None


class TickerResponse(BaseModel):
    """Ticker响应模型"""
    code: str
    name: str
    exchange: str
    type: Optional[str] = None
    currency: Optional[str] = None
    country: Optional[str] = None
    isin: Optional[str] = None
    data_source: Optional[str] = None
    ticker_active: bool
    last_update: Optional[str] = None
    sector: Optional[str] = None
    industry: Optional[str] = None


class FetchExchangeRequest(BaseModel):
    """获取交易所数据请求模型"""
    exchange_codes: List[str] = Field(..., description="交易所代码列表")


# ============ 交易所管理API ============

@router.get("/", summary="获取支持的交易所列表")
async def get_exchanges(
    current_user=Depends(get_current_user)
) -> List[ExchangeResponse]:
    """
    获取所有支持的交易所列表
    """
    try:
        logger.info("获取交易所列表请求", user=current_user.username)
        
        exchange_service = await get_exchange_service()
        exchanges = await exchange_service.get_supported_exchanges()
        
        logger.info(f"成功获取 {len(exchanges)} 个交易所", user=current_user.username)
        return exchanges
        
    except Exception as e:
        logger.error("获取交易所列表失败", error=str(e), user=current_user.username)
        raise HTTPException(status_code=500, detail=f"获取交易所列表失败: {str(e)}")


@router.post("/fetch", summary="获取交易所数据")
async def fetch_exchange_data(
    request: FetchExchangeRequest,
    current_user=Depends(get_current_user)
):
    """
    获取指定交易所的ticker数据
    """
    try:
        logger.info("获取交易所数据请求", 
                   exchanges=request.exchange_codes,
                   user=current_user.username)
        
        exchange_service = await get_exchange_service()
        results = []
        
        for exchange_code in request.exchange_codes:
            logger.info(f"开始获取交易所 {exchange_code} 数据")
            
            result = await exchange_service.fetch_exchange_tickers(exchange_code)
            results.append(result)
            
            if result.get('success'):
                logger.info(f"成功获取 {exchange_code} 数据: {result.get('success_count', 0)} 个ticker")
            else:
                logger.error(f"获取 {exchange_code} 数据失败: {result.get('message', '')}")
        
        # 统计总体结果
        total_success = sum(1 for r in results if r.get('success'))
        total_tickers = sum(r.get('success_count', 0) for r in results if r.get('success'))
        
        return {
            "success": total_success > 0,
            "message": f"共处理 {len(request.exchange_codes)} 个交易所，成功 {total_success} 个，获取 {total_tickers} 个ticker",
            "data": {
                "total_exchanges": len(request.exchange_codes),
                "successful_exchanges": total_success,
                "total_tickers": total_tickers,
                "results": results
            }
        }
        
    except Exception as e:
        logger.error("获取交易所数据失败", error=str(e), user=current_user.username)
        raise HTTPException(status_code=500, detail=f"获取交易所数据失败: {str(e)}")


@router.get("/tickers", summary="获取ticker列表")
async def get_tickers(
    exchange: Optional[str] = Query(None, description="交易所代码"),
    ticker_type: Optional[str] = Query(None, description="股票类型"),
    search: Optional[str] = Query(None, description="搜索关键词"),
    limit: int = Query(1000, ge=1, le=10000, description="返回数量限制"),
    current_user=Depends(get_current_user)
) -> List[TickerResponse]:
    """
    根据条件获取ticker列表
    """
    try:
        logger.info("获取ticker列表请求", 
                   exchange=exchange,
                   ticker_type=ticker_type,
                   search=search,
                   limit=limit,
                   user=current_user.username)
        
        exchange_service = await get_exchange_service()
        tickers = await exchange_service.get_tickers_by_exchange(
            exchange_code=exchange,
            ticker_type=ticker_type,
            search_text=search,
            limit=limit
        )
        
        logger.info(f"成功获取 {len(tickers)} 个ticker", user=current_user.username)
        return tickers
        
    except Exception as e:
        logger.error("获取ticker列表失败", error=str(e), user=current_user.username)
        raise HTTPException(status_code=500, detail=f"获取ticker列表失败: {str(e)}")


@router.get("/statistics", summary="获取ticker统计信息")
async def get_ticker_statistics(
    exchange: Optional[str] = Query(None, description="交易所代码"),
    current_user=Depends(get_current_user)
):
    """
    获取ticker统计信息
    """
    try:
        logger.info("获取ticker统计请求", exchange=exchange, user=current_user.username)
        
        exchange_service = await get_exchange_service()
        stats = await exchange_service.get_ticker_statistics(exchange_code=exchange)
        
        return {
            "success": True,
            "message": "获取统计信息成功",
            "data": stats
        }
        
    except Exception as e:
        logger.error("获取ticker统计失败", error=str(e), user=current_user.username)
        raise HTTPException(status_code=500, detail=f"获取ticker统计失败: {str(e)}")


@router.delete("/{exchange_code}/clear", summary="清除交易所数据")
async def clear_exchange_data(
    exchange_code: str,
    current_user=Depends(get_current_user)
):
    """
    清除指定交易所的所有ticker数据
    """
    try:
        logger.info("清除交易所数据请求", 
                   exchange_code=exchange_code,
                   user=current_user.username)
        
        exchange_service = await get_exchange_service()
        result = await exchange_service.clear_exchange_data(exchange_code)
        
        if result.get('success'):
            logger.info(f"成功清除 {exchange_code} 数据: {result.get('deleted_count', 0)} 个ticker")
            return {
                "success": True,
                "message": result['message'],
                "data": {
                    "exchange_code": exchange_code,
                    "deleted_count": result.get('deleted_count', 0)
                }
            }
        else:
            raise HTTPException(status_code=400, detail=result.get('message', '清除数据失败'))
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("清除交易所数据失败", 
                    exchange_code=exchange_code,
                    error=str(e), 
                    user=current_user.username)
        raise HTTPException(status_code=500, detail=f"清除交易所数据失败: {str(e)}")


@router.get("/types", summary="获取可用的股票类型")
async def get_available_types(
    exchange: Optional[str] = Query(None, description="交易所代码"),
    current_user=Depends(get_current_user)
):
    """
    获取可用的股票类型列表
    """
    try:
        logger.info("获取股票类型列表请求", exchange=exchange, user=current_user.username)
        
        exchange_service = await get_exchange_service()
        types = await exchange_service.get_available_types(exchange_code=exchange)
        
        return {
            "success": True,
            "message": "获取股票类型列表成功",
            "data": types
        }
        
    except Exception as e:
        logger.error("获取股票类型列表失败", error=str(e), user=current_user.username)
        raise HTTPException(status_code=500, detail=f"获取股票类型列表失败: {str(e)}")


@router.get("/available", summary="获取数据库中可用的交易所")
async def get_available_exchanges(
    current_user=Depends(get_current_user)
):
    """
    获取数据库中已有ticker数据的交易所列表
    """
    try:
        logger.info("获取可用交易所列表请求", user=current_user.username)
        
        exchange_service = await get_exchange_service()
        exchanges = await exchange_service.get_available_exchanges()
        
        return {
            "success": True,
            "message": "获取可用交易所列表成功",
            "data": exchanges
        }
        
    except Exception as e:
        logger.error("获取可用交易所列表失败", error=str(e), user=current_user.username)
        raise HTTPException(status_code=500, detail=f"获取可用交易所列表失败: {str(e)}")


# ============ 调试和管理API ============

@router.get("/debug/config", summary="查看交易所配置（调试用）")
async def get_exchange_configs(
    current_user=Depends(get_current_user)
):
    """
    查看所有交易所配置信息（调试用）
    """
    try:
        logger.info("查看交易所配置请求", user=current_user.username)
        
        exchange_service = await get_exchange_service()
        
        # 获取数据库中的配置
        db_configs = await exchange_service.get_supported_exchanges()
        
        # 获取服务中的支持列表
        supported_configs = []
        for code, config in exchange_service.supported_exchanges.items():
            supported_configs.append({
                'code': code,
                'name': config['name'],
                'country': config['country'],
                'timezone': config['timezone'],
                'api_endpoint': config['api_endpoint'],
                'mic_code': config.get('mic_code')
            })
        
        return {
            "success": True,
            "message": "获取交易所配置成功",
            "data": {
                "database_configs": db_configs,
                "supported_configs": supported_configs,
                "total_supported": len(supported_configs)
            }
        }
        
    except Exception as e:
        logger.error("查看交易所配置失败", error=str(e), user=current_user.username)
        raise HTTPException(status_code=500, detail=f"查看交易所配置失败: {str(e)}")

