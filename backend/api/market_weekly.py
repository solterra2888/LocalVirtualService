# -*- coding: utf-8 -*-
"""
Market Weekly Report API Routes
"""

from fastapi import APIRouter, HTTPException, Depends, Query
from typing import Optional
from backend.services.market_weekly_service import MarketWeeklyService
from backend.utils.logger import get_logger
from backend.auth.jwt_utils import get_current_user
from backend.schemas.user import TokenData

logger = get_logger("market_weekly_api")

router = APIRouter(tags=["Market Weekly"])


@router.get("/market-weekly/report")
async def get_market_weekly_report(
    period: str = Query("latest", description="Report period, 'latest' for most recent week"),
    current_user: TokenData = Depends(get_current_user)
):
    """
    Get Market Weekly Report
    
    Returns comprehensive weekly market report including:
    - Executive summary
    - Week-over-week performance (indices, sectors, forex, commodities, yields)
    - Technical view data (list of assets for candlestick charts)
    - Next week outlook (economic calendar, earnings, IPOs)
    """
    try:
        logger.info(f"[get_market_weekly_report] Fetching report for user {current_user.username}, period: {period}")
        
        # 创建服务实例
        service = MarketWeeklyService()
        
        # 生成报告
        report_data = service.generate_report(period=period)
        
        if not report_data:
            raise HTTPException(status_code=404, detail="Report data not available")
        
        logger.info(f"[get_market_weekly_report] Report generated successfully for user {current_user.username}")
        
        return {
            "success": True,
            "data": report_data
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
    current_user: TokenData = Depends(get_current_user)
):
    """
    Trigger a refresh of the Market Weekly report
    
    This endpoint will regenerate the report with latest data.
    """
    try:
        logger.info(f"[refresh_report] User {current_user.username} triggered report refresh")
        
        # 创建服务实例
        service = MarketWeeklyService()
        
        # 刷新报告（重新生成并清除缓存）
        service.refresh_report()
        
        logger.info(f"[refresh_report] Report refreshed successfully")
        
        return {
            "success": True,
            "message": "Market Weekly report refreshed successfully"
        }
    
    except Exception as e:
        logger.error(f"[refresh_report] Error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to refresh report: {str(e)}")

