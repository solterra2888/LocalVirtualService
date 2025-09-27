# -*- coding: utf-8 -*-
"""
EOD Fetch API - 历史股价数据获取API
"""

import os
import io
import tempfile
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from fastapi import APIRouter, HTTPException, Depends, Query, Response
from pydantic import BaseModel, Field, validator
import pandas as pd

from backend.auth.jwt_utils import get_current_user
from backend.schemas.user import UserResponse
from backend.services.eod_fetch_service import EODFetchService
from backend.utils.logger import get_logger

logger = get_logger("eod_fetch_api")

router = APIRouter(prefix="/eod-fetch", tags=["EOD Fetch"])


class EODFetchRequest(BaseModel):
    """EOD数据获取请求"""
    ticker: str = Field(..., description="股票代码，格式如 AAPL.US")
    period: str = Field(default="d", description="数据周期: d(日), w(周), m(月)")
    from_date: Optional[str] = Field(None, description="开始日期 YYYY-MM-DD")
    to_date: Optional[str] = Field(None, description="结束日期 YYYY-MM-DD")
    limit: Optional[int] = Field(default=100, description="返回数据条数限制(1-1000)")
    
    @validator('ticker')
    def validate_ticker(cls, v):
        if not v or '.' not in v:
            raise ValueError('Ticker must contain exchange code (e.g., AAPL.US)')
        return v.upper()
    
    @validator('period')
    def validate_period(cls, v):
        if v not in ['d', 'w', 'm']:
            raise ValueError('Period must be d, w, or m')
        return v
    
    @validator('limit')
    def validate_limit(cls, v):
        if v and (v < 1 or v > 1000):
            raise ValueError('Limit must be between 1 and 1000')
        return v


class EODDataResponse(BaseModel):
    """EOD数据响应"""
    date: str
    open: float
    high: float
    low: float
    close: float
    adjusted_close: float
    volume: int
    split_coefficient: Optional[float] = 1.0
    dividend_amount: Optional[float] = 0.0


class EODFetchResponse(BaseModel):
    """EOD获取响应"""
    success: bool
    data: List[EODDataResponse]
    total_records: int
    fetched_records: int
    stored_records: int
    message: str
    warning: Optional[str] = None
    ticker: str
    period: str
    date_range: str


class EODExportRequest(BaseModel):
    """EOD数据导出请求"""
    ticker: str = Field(..., description="股票代码")
    period: str = Field(default="d", description="数据周期")
    from_date: Optional[str] = Field(None, description="开始日期")
    to_date: Optional[str] = Field(None, description="结束日期")


@router.post("/fetch", response_model=EODFetchResponse, summary="获取EOD数据")
async def fetch_eod_data(
    request: EODFetchRequest,
    current_user: UserResponse = Depends(get_current_user)
):
    """
    获取指定股票的历史价格数据 - 优化版本，处理大批量数据
    
    - 先检查现有数据
    - 如果数据量较大，使用分批异步处理
    - 立即返回已有数据，后台继续处理新数据
    - 避免前端超时
    """
    try:
        logger.info(
            f"用户 {current_user.username} 请求获取EOD数据",
            ticker=request.ticker,
            period=request.period,
            from_date=request.from_date,
            to_date=request.to_date,
            limit=request.limit
        )
        
        # 初始化服务
        eod_service = EODFetchService()
        
        # 先检查是否已有存储的数据
        existing_data = await eod_service.get_stored_eod_data(
            ticker=request.ticker,
            period=request.period,
            from_date=request.from_date,
            to_date=request.to_date,
            limit=request.limit
        )
        
        # 如果已有数据，直接返回并在后台更新
        if existing_data:
            logger.info(f"发现已存储的EOD数据 {len(existing_data)} 条，将在后台更新")
            
            # 后台异步更新数据，不阻塞前端响应
            import asyncio
            asyncio.create_task(
                _background_fetch_and_store(
                    eod_service, request.ticker, request.period, 
                    request.from_date, request.to_date
                )
            )
            
            # 构造日期范围字符串
            date_range = "All available data"
            if request.from_date and request.to_date:
                date_range = f"{request.from_date} to {request.to_date}"
            elif request.from_date:
                date_range = f"From {request.from_date}"
            elif request.to_date:
                date_range = f"Until {request.to_date}"
            
            return EODFetchResponse(
                success=True,
                data=[EODDataResponse(**item) for item in existing_data],
                total_records=len(existing_data),
                fetched_records=0,  # 后台处理
                stored_records=len(existing_data),
                message=f"返回已存储的数据，正在后台更新最新数据",
                warning="数据正在后台更新，请稍后刷新获取最新数据",
                ticker=request.ticker,
                period=request.period,
                date_range=date_range
            )
        
        # 如果没有数据，进行首次获取（使用超时控制）
        try:
            # 设置较短的超时时间进行快速检查
            result = await eod_service.fetch_and_store_eod_data_with_timeout(
                ticker=request.ticker,
                period=request.period,
                from_date=request.from_date,
                to_date=request.to_date,
                timeout_seconds=30  # 30秒超时
            )
            
            if result["success"]:
                # 获取已处理的数据
                display_data = await eod_service.get_stored_eod_data(
                    ticker=request.ticker,
                    period=request.period,
                    from_date=request.from_date,
                    to_date=request.to_date,
                    limit=request.limit
                )
                
                # 构造日期范围字符串
                date_range = "All available data"
                if request.from_date and request.to_date:
                    date_range = f"{request.from_date} to {request.to_date}"
                elif request.from_date:
                    date_range = f"From {request.from_date}"
                elif request.to_date:
                    date_range = f"Until {request.to_date}"
                
                return EODFetchResponse(
                    success=True,
                    data=[EODDataResponse(**item) for item in display_data],
                    total_records=result["total_records"],
                    fetched_records=result["fetched_records"],
                    stored_records=result["stored_records"],
                    message=result["message"],
                    warning=result.get("warning"),
                    ticker=request.ticker,
                    period=request.period,
                    date_range=date_range
                )
            else:
                raise HTTPException(status_code=400, detail=result["message"])
                
        except asyncio.TimeoutError:
            # 如果获取超时，启动后台任务并返回提示
            logger.warning(f"EOD数据获取超时，启动后台处理任务")
            
            asyncio.create_task(
                _background_fetch_and_store(
                    eod_service, request.ticker, request.period, 
                    request.from_date, request.to_date
                )
            )
            
            return EODFetchResponse(
                success=True,
                data=[],
                total_records=0,
                fetched_records=0,
                stored_records=0,
                message="数据量较大，已启动后台处理任务",
                warning="由于数据量较大，正在后台处理。请稍后重新查询以获取数据。",
                ticker=request.ticker,
                period=request.period,
                date_range="Processing in background"
            )
        
    except Exception as e:
        logger.error(f"EOD数据获取失败: {str(e)}", ticker=request.ticker)
        raise HTTPException(status_code=500, detail=f"Failed to fetch EOD data: {str(e)}")


async def _background_fetch_and_store(
    eod_service: EODFetchService, 
    ticker: str, 
    period: str, 
    from_date: Optional[str], 
    to_date: Optional[str]
):
    """后台异步获取和存储EOD数据"""
    try:
        logger.info(f"开始后台处理EOD数据", ticker=ticker)
        
        result = await eod_service.fetch_and_store_eod_data(
            ticker=ticker,
            period=period,
            from_date=from_date,
            to_date=to_date
        )
        
        if result["success"]:
            logger.info(f"后台EOD数据处理完成", 
                       ticker=ticker, 
                       total_records=result["total_records"],
                       stored_records=result["stored_records"])
        else:
            logger.error(f"后台EOD数据处理失败", ticker=ticker, error=result["message"])
            
    except Exception as e:
        logger.error(f"后台EOD数据处理异常: {str(e)}", ticker=ticker)


@router.get("/data/{ticker}", response_model=List[EODDataResponse], summary="获取存储的EOD数据")
async def get_stored_eod_data(
    ticker: str,
    period: str = Query(default="d", description="数据周期"),
    from_date: Optional[str] = Query(None, description="开始日期"),
    to_date: Optional[str] = Query(None, description="结束日期"),
    limit: int = Query(default=100, description="返回数据条数限制"),
    offset: int = Query(default=0, description="数据偏移量"),
    current_user: UserResponse = Depends(get_current_user)
):
    """
    获取已存储的EOD数据（用于分页加载）
    """
    try:
        eod_service = EODFetchService()
        
        data = await eod_service.get_stored_eod_data(
            ticker=ticker.upper(),
            period=period,
            from_date=from_date,
            to_date=to_date,
            limit=limit,
            offset=offset
        )
        
        return [EODDataResponse(**item) for item in data]
        
    except Exception as e:
        logger.error(f"获取存储EOD数据失败: {str(e)}", ticker=ticker)
        raise HTTPException(status_code=500, detail=f"Failed to get stored EOD data: {str(e)}")


@router.post("/export", summary="导出EOD数据为Excel")
async def export_eod_data(
    request: EODExportRequest,
    current_user: UserResponse = Depends(get_current_user)
):
    """
    导出EOD数据为Excel文件 - 根据查询条件导出所有数据
    """
    try:
        logger.info(
            f"用户 {current_user.username} 请求导出EOD数据",
            ticker=request.ticker,
            period=request.period,
            from_date=request.from_date,
            to_date=request.to_date
        )
        
        eod_service = EODFetchService()
        
        # 获取查询条件对应的所有数据（不限制数量）
        data = await eod_service.get_stored_eod_data(
            ticker=request.ticker,
            period=request.period,
            from_date=request.from_date,
            to_date=request.to_date,
            limit=None  # 获取所有数据，不限制
        )
        
        logger.info(f"获取到EOD数据 {len(data)} 条用于导出", ticker=request.ticker)
        
        if not data:
            error_msg = f"未找到符合条件的数据: {request.ticker} ({request.period})"
            if request.from_date or request.to_date:
                date_range = f"日期范围: {request.from_date or '开始'} 到 {request.to_date or '现在'}"
                error_msg += f", {date_range}"
            logger.warning(error_msg)
            raise HTTPException(status_code=404, detail=error_msg)
        
        try:
            # 创建DataFrame
            df = pd.DataFrame(data)
            logger.debug(f"创建DataFrame成功，形状: {df.shape}")
            
            # 检查并重新排序列
            available_columns = df.columns.tolist()
            logger.debug(f"可用列: {available_columns}")
            
            # 使用实际存在的列
            column_order = []
            expected_columns = [
                'date', 'open', 'high', 'low', 'close', 
                'adjusted_close', 'volume', 'split_coefficient', 'dividend_amount'
            ]
            
            for col in expected_columns:
                if col in available_columns:
                    column_order.append(col)
            
            df = df[column_order]
            
            # 格式化列名
            column_mapping = {
                'date': 'Date',
                'open': 'Open', 
                'high': 'High', 
                'low': 'Low', 
                'close': 'Close',
                'adjusted_close': 'Adjusted Close', 
                'volume': 'Volume', 
                'split_coefficient': 'Split Coefficient', 
                'dividend_amount': 'Dividend Amount'
            }
            
            df.columns = [column_mapping.get(col, col) for col in df.columns]
            
            logger.debug(f"DataFrame列名格式化完成: {df.columns.tolist()}")
            
            # 创建Excel文件
            output = io.BytesIO()
            
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name='EOD Data', index=False)
                
                # 获取工作表并格式化
                worksheet = writer.sheets['EOD Data']
                
                # 调整列宽
                for column in worksheet.columns:
                    max_length = 0
                    column_letter = column[0].column_letter
                    for cell in column:
                        try:
                            cell_length = len(str(cell.value or ''))
                            if cell_length > max_length:
                                max_length = cell_length
                        except Exception:
                            pass
                    adjusted_width = min(max_length + 2, 25)
                    worksheet.column_dimensions[column_letter].width = adjusted_width
                
                logger.debug("Excel工作表格式化完成")
            
            output.seek(0)
            content_size = len(output.getvalue())
            logger.debug(f"Excel文件生成成功，大小: {content_size} 字节")
            
            # 生成文件名
            date_suffix = ""
            if request.from_date and request.to_date:
                date_suffix = f"_{request.from_date}_{request.to_date}"
            elif request.from_date:
                date_suffix = f"_from_{request.from_date}"
            elif request.to_date:
                date_suffix = f"_until_{request.to_date}"
            
            filename = f"{request.ticker}_{request.period}_EOD_data{date_suffix}.xlsx"
            
            logger.info(f"EOD数据导出成功", 
                       ticker=request.ticker, 
                       records=len(data),
                       filename=filename,
                       file_size=content_size)
            
            from fastapi.responses import StreamingResponse
            
            return StreamingResponse(
                io.BytesIO(output.getvalue()),
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                headers={
                    "Content-Disposition": f"attachment; filename={filename}",
                    "Content-Type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                }
            )
            
        except Exception as excel_error:
            logger.error(f"Excel文件生成失败: {str(excel_error)}", ticker=request.ticker)
            raise HTTPException(status_code=500, detail=f"Excel文件生成失败: {str(excel_error)}")
        
    except HTTPException:
        # 重新抛出HTTP异常
        raise
    except Exception as e:
        logger.error(f"EOD数据导出异常: {str(e)}", ticker=request.ticker, error_type=type(e).__name__)
        raise HTTPException(status_code=500, detail=f"导出失败: {str(e)}")


@router.get("/stats/{ticker}", summary="获取EOD数据统计")
async def get_eod_stats(
    ticker: str,
    period: str = Query(default="d", description="数据周期"),
    current_user: UserResponse = Depends(get_current_user)
):
    """
    获取指定股票的EOD数据统计信息
    """
    try:
        eod_service = EODFetchService()
        
        stats = await eod_service.get_eod_stats(
            ticker=ticker.upper(),
            period=period
        )
        
        return stats
        
    except Exception as e:
        logger.error(f"获取EOD统计失败: {str(e)}", ticker=ticker)
        raise HTTPException(status_code=500, detail=f"Failed to get EOD stats: {str(e)}")
