# -*- coding: utf-8 -*-
"""
Market Weekly Report API Routes
"""

from fastapi import APIRouter, HTTPException, Depends, Query, Request
from fastapi.responses import Response
from typing import Optional, List, Dict, Any
from pydantic import BaseModel
import base64
from backend.services.market_weekly_service import MarketWeeklyService
from backend.utils.logger import get_logger
from backend.auth.jwt_utils import get_current_user
from backend.schemas.user import TokenData

logger = get_logger("market_weekly_api")

router = APIRouter(tags=["Market Weekly"])


# Pydantic models for request/response
class ChartSymbol(BaseModel):
    symbol: str
    label: Optional[str] = None


class BatchChartRequest(BaseModel):
    symbols: List[ChartSymbol]
    days: int = 90
    data_source: str = 'auto'  # 'market', 'stock', or 'auto'


@router.get("/market-weekly/report")
async def get_market_weekly_report(
    period: str = Query("latest", description="Report period, 'latest' for most recent week"),
    use_cache: bool = Query(True, description="Use cache if available"),
    current_user: TokenData = Depends(get_current_user)
):
    """
    Get Market Weekly Report
    
    Returns comprehensive weekly market report including:
    - Executive summary
    - Week-over-week performance (indices, sectors, forex, commodities, yields)
    - Technical view data (list of assets for candlestick charts)
    - Next week outlook (economic calendar, earnings, IPOs)
    
    Uses caching to improve performance. Cache expires after 1 hour.
    """
    try:
        logger.info(f"[get_market_weekly_report] Fetching report for user {current_user.username}, period: {period}, use_cache: {use_cache}")
        
        # 创建服务实例
        service = MarketWeeklyService()
        
        # 生成报告（使用缓存）
        report_data = service.generate_report(period=period, use_cache=use_cache)
        
        if not report_data:
            raise HTTPException(status_code=404, detail="Report data not available")
        
        logger.info(f"[get_market_weekly_report] Report generated successfully for user {current_user.username}")
        
        return {
            "success": True,
            "data": report_data,
            "cached": use_cache  # 指示是否使用了缓存
        }
    
    except Exception as e:
        logger.error(f"[get_market_weekly_report] Error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to generate report: {str(e)}")


@router.get("/market-weekly/watchlist-stocks")
async def get_watchlist_stocks(
    user_id: Optional[int] = Query(None, description="User ID to get watchlist for (defaults to current user)"),
    current_user: TokenData = Depends(get_current_user)
):
    """
    Get user's watchlist stocks for Market Weekly report
    
    Returns list of stocks in user's watchlist with basic info.
    """
    try:
        # 如果没有指定user_id，使用当前用户
        target_user_id = user_id if user_id is not None else current_user.user_id
        
        # 权限检查：只能查询自己的watchlist（除非是管理员）
        if target_user_id != current_user.user_id and not current_user.is_admin:
            raise HTTPException(status_code=403, detail="Not authorized to view this watchlist")
        
        logger.info(f"[get_watchlist_stocks] Fetching watchlist for user_id: {target_user_id}")
        
        # 创建服务实例
        service = MarketWeeklyService()
        
        # 获取watchlist股票
        stocks = service.get_watchlist_stocks(target_user_id)
        
        logger.info(f"[get_watchlist_stocks] Found {len(stocks)} stocks in watchlist")
        
        return {
            "success": True,
            "data": stocks
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[get_watchlist_stocks] Error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch watchlist stocks: {str(e)}")


@router.post("/market-weekly/refresh")
async def refresh_report(
    period: str = Query("latest", description="Report period to refresh"),
    current_user: TokenData = Depends(get_current_user)
):
    """
    Trigger a refresh of the Market Weekly report
    
    This endpoint will clear the cache and regenerate the report with latest data.
    """
    try:
        logger.info(f"[refresh_report] User {current_user.username} triggered report refresh for period: {period}")
        
        # 创建服务实例
        service = MarketWeeklyService()
        
        # 刷新报告（清除缓存）
        service.refresh_report(period=period)
        
        logger.info(f"[refresh_report] Cache cleared successfully for period: {period}")
        
        return {
            "success": True,
            "message": f"Market Weekly report cache cleared for period: {period}. Next request will generate fresh report."
        }
    
    except Exception as e:
        logger.error(f"[refresh_report] Error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to refresh report: {str(e)}")


@router.post("/market-weekly/ohlc-data/batch")
async def get_ohlc_data_batch(
    request: Request,
    batch_request: BatchChartRequest,
    current_user: TokenData = Depends(get_current_user)
):
    """
    批量获取OHLC原始数据（用于前端渲染图表，支持客户端中断检测）
    
    Args:
        symbols: 股票代码列表，格式为 [{"symbol": "SPY.US", "label": "S&P 500"}, ...]
        days: 天数，默认90天
    
    Returns:
        {
            "status": "completed" | "cancelled",
            "processed": int,
            "total": int,
            "results": [
                {
                    "symbol": "SPY.US",
                    "label": "S&P 500",
                    "data": {
                        "ohlc": [{time, open, high, low, close, volume}],
                        "ema20": [{time, value}],
                        "ema50": [{time, value}],
                        "ema200": [{time, value}]
                    },
                    "success": true,
                    "error": null
                },
                ...
            ]
        }
    
    特性：
    - 返回原始OHLC数据而非图片，数据量减少95%
    - 前端使用lightweight-charts渲染，速度从秒级降到毫秒级
    - 如果客户端断开连接，后端会立即停止处理并释放资源
    - 支持批量处理30-40个symbols，大幅减少HTTP请求开销
    """
    try:
        symbols = [{"symbol": s.symbol, "label": s.label or s.symbol} for s in batch_request.symbols]
        total = len(symbols)
        
        logger.info(f"[Batch OHLC API] User {current_user.username} requesting {total} OHLC datasets, days={batch_request.days}")
        
        # 创建服务实例
        service = MarketWeeklyService()
        
        # 批量获取OHLC数据（传入request对象用于中断检测）
        result = await service.get_ohlc_data_batch(
            symbols=symbols,
            days=batch_request.days,
            request=request,  # 🔥 传入request用于检测客户端断开
            data_source=batch_request.data_source  # 🔥 指定数据源
        )
        
        status = result.get("status")
        processed = result.get("processed", 0)
        
        if status == "cancelled":
            logger.info(f"[Batch OHLC API] Request cancelled by client. Processed {processed}/{total}.")
            return {
                "success": False,
                "status": "cancelled",
                "message": f"Client disconnected. Processed {processed}/{total} datasets.",
                "processed": processed,
                "total": total,
                "results": result.get("results", [])
            }
        
        logger.info(f"[Batch OHLC API] Successfully processed {processed}/{total} datasets")
        
        return {
            "success": True,
            "status": "completed",
            "message": f"Processed {processed}/{total} datasets successfully",
            "processed": processed,
            "total": total,
            "results": result.get("results", [])
        }
    
    except Exception as e:
        logger.error(f"[Batch OHLC API] Error processing batch OHLC data: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to process batch OHLC data: {str(e)}")


@router.post("/market-weekly/charts/batch")
async def get_charts_batch(
    request: Request,
    batch_request: BatchChartRequest,
    current_user: TokenData = Depends(get_current_user)
):
    """
    批量生成candlestick图表图片（支持客户端中断检测）
    
    ⚠️ 已过时：建议使用 /market-weekly/ohlc-data/batch 获取原始数据，在前端渲染图表
    
    Args:
        symbols: 股票代码列表，格式为 [{"symbol": "SPY.US", "label": "S&P 500"}, ...]
        days: 天数，默认90天
    
    Returns:
        {
            "status": "completed" | "cancelled",
            "processed": int,
            "total": int,
            "results": [
                {
                    "symbol": "SPY.US",
                    "label": "S&P 500",
                    "data": "base64_image_string",
                    "success": true,
                    "error": null
                },
                ...
            ]
        }
    
    特性：
    - 如果客户端断开连接，后端会立即停止处理并释放资源
    - 支持批量处理30-40个symbols，大幅减少HTTP请求开销
    - 返回每个symbol的处理结果，包括成功和失败的情况
    """
    try:
        symbols = [{"symbol": s.symbol, "label": s.label or s.symbol} for s in batch_request.symbols]
        total = len(symbols)
        
        logger.info(f"[Batch API] User {current_user.username} requesting {total} charts, days={batch_request.days}")
        
        # 创建服务实例
        service = MarketWeeklyService()
        
        # 批量生成图表（传入request对象用于中断检测）
        result = await service.generate_charts_batch(
            symbols=symbols,
            days=batch_request.days,
            request=request  # 🔥 传入request用于检测客户端断开
        )
        
        status = result.get("status")
        processed = result.get("processed", 0)
        
        if status == "cancelled":
            logger.info(f"[Batch API] Request cancelled by client. Processed {processed}/{total} charts.")
            return {
                "success": False,
                "status": "cancelled",
                "message": f"Client disconnected. Processed {processed}/{total} charts.",
                "processed": processed,
                "total": total,
                "results": result.get("results", [])
            }
        
        logger.info(f"[Batch API] Successfully processed {processed}/{total} charts")
        
        return {
            "success": True,
            "status": "completed",
            "message": f"Processed {processed}/{total} charts successfully",
            "processed": processed,
            "total": total,
            "results": result.get("results", [])
        }
    
    except Exception as e:
        logger.error(f"[Batch API] Error processing batch charts: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to process batch charts: {str(e)}")


@router.get("/market-weekly/chart/{symbol}")
async def get_chart_image(
    symbol: str,
    label: Optional[str] = Query(None, description="Chart label/title"),
    days: int = Query(90, ge=30, le=365, description="Number of days of historical data"),
    current_user: TokenData = Depends(get_current_user)
):
    """
    Generate and return candlestick chart image for a symbol
    
    Returns base64-encoded PNG image matching report_debug.py style:
    - Candlestick chart with EMA 20, 50, 200
    - Professional broker style
    - White up candles, red down candles
    
    Note: For multiple charts, use /market-weekly/charts/batch endpoint instead
    """
    try:
        logger.info(f"[get_chart_image] Generating chart for {symbol}, days={days}, user={current_user.username}")
        
        # 创建服务实例
        service = MarketWeeklyService()
        
        # 生成图表图片（base64编码）
        image_base64 = service.generate_chart_image(
            symbol=symbol,
            label=label or symbol,
            days=days
        )
        
        if not image_base64:
            raise HTTPException(status_code=404, detail=f"No data available for symbol {symbol}")
        
        logger.info(f"[get_chart_image] Chart generated successfully for {symbol}")
        
        return {
            "success": True,
            "data": image_base64
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[get_chart_image] Error generating chart for {symbol}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to generate chart: {str(e)}")

