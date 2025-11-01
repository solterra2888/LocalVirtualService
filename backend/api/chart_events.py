"""
图表事件API接口
"""

from typing import List, Dict, Any, Optional
from fastapi import APIRouter, HTTPException, Depends, status, Query
from pydantic import BaseModel, Field
from datetime import date

from backend.auth.jwt_utils import get_current_user
from backend.schemas.user import UserResponse
from backend.services.chart_events_service import get_chart_events_service
from backend.utils.logger import info, error

router = APIRouter(tags=["Chart Events"])


# === Pydantic Models ===

class ChartEventCreate(BaseModel):
    """创建图表事件的请求模型"""
    symbol: str = Field(..., description="股票代码", max_length=20)
    event_date: date = Field(..., description="事件日期")
    event_type: str = Field(..., description="事件类型")
    title: str = Field(..., description="事件标题", max_length=255)
    description: Optional[str] = Field(None, description="事件描述")
    position: str = Field("middle", description="显示位置")
    related_url: Optional[str] = Field(None, description="相关链接")
    related_file_id: Optional[str] = Field(None, description="相关文件ID")


class ChartEventUpdate(BaseModel):
    """更新图表事件的请求模型"""
    event_date: Optional[date] = Field(None, description="事件日期")
    event_type: Optional[str] = Field(None, description="事件类型")
    title: Optional[str] = Field(None, description="事件标题", max_length=255)
    description: Optional[str] = Field(None, description="事件描述")
    position: Optional[str] = Field(None, description="显示位置")
    related_url: Optional[str] = Field(None, description="相关链接")
    related_file_id: Optional[str] = Field(None, description="相关文件ID")


class ChartEventResponse(BaseModel):
    """图表事件响应模型"""
    id: str = Field(..., description="事件ID")
    symbol: str = Field(..., description="股票代码")
    date: str = Field(..., description="事件日期")
    type: str = Field(..., description="事件类型")
    title: str = Field(..., description="事件标题")
    description: Optional[str] = Field(None, description="事件描述")
    position: str = Field(..., description="显示位置")
    relatedUrl: Optional[str] = Field(None, description="相关链接")
    relatedFileId: Optional[str] = Field(None, description="相关文件ID")
    createdAt: str = Field(..., description="创建时间")
    updatedAt: str = Field(..., description="更新时间")


class ChartEventsSummary(BaseModel):
    """用户事件统计摘要"""
    total_events: int = Field(..., description="总事件数")
    by_type: Dict[str, int] = Field(..., description="按类型统计")
    by_symbol: Dict[str, int] = Field(..., description="按股票统计")


# === API Endpoints ===

@router.post("", response_model=Dict[str, Any], summary="创建图表事件")
async def create_chart_event(
    event_data: ChartEventCreate,
    current_user: UserResponse = Depends(get_current_user)
):
    """
    为指定股票创建新的图表事件标记
    """
    try:
        info(f"创建图表事件: user={current_user.username}, symbol={event_data.symbol}, type={event_data.event_type}", source="chart_events_api")
        
        chart_events_service = get_chart_events_service()
        
        # 转换数据格式
        event_dict = {
            'symbol': event_data.symbol,
            'event_date': event_data.event_date,
            'event_type': event_data.event_type,
            'title': event_data.title,
            'description': event_data.description,
            'position': event_data.position,
            'related_url': event_data.related_url,
            'related_file_id': event_data.related_file_id
        }
        
        event_id = chart_events_service.create_event(current_user.user_id, event_dict)
        
        if event_id > 0:
            info(f"图表事件创建成功: event_id={event_id}", source="chart_events_api")
            return {
                "success": True,
                "message": "事件创建成功",
                "event_id": str(event_id)
            }
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="创建事件失败"
            )
            
    except HTTPException:
        raise
    except Exception as e:
        error(f"创建图表事件失败: {str(e)}", source="chart_events_api")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"创建事件失败: {str(e)}"
        )


@router.get("/{symbol}", summary="获取指定股票的事件")
async def get_chart_events_by_symbol(
    symbol: str,
    current_user: UserResponse = Depends(get_current_user)
):
    """
    获取指定股票的所有图表事件标记
    """
    try:
        info(f"获取图表事件: user={current_user.username}, symbol={symbol}", source="chart_events_api")
        
        chart_events_service = get_chart_events_service()
        events = chart_events_service.get_events_by_symbol(current_user.user_id, symbol)
        
        info(f"获取到 {len(events)} 个图表事件", source="chart_events_api")
        return events
        
    except Exception as e:
        error(f"获取图表事件失败: {str(e)}", source="chart_events_api")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取事件失败: {str(e)}"
        )


@router.put("/{event_id}", response_model=Dict[str, Any], summary="更新图表事件")
async def update_chart_event(
    event_id: int,
    event_data: ChartEventUpdate,
    current_user: UserResponse = Depends(get_current_user)
):
    """
    更新指定的图表事件标记
    """
    try:
        info(f"更新图表事件: user={current_user.username}, event_id={event_id}", source="chart_events_api")
        
        chart_events_service = get_chart_events_service()
        
        # 只更新提供的字段
        update_dict = {}
        if event_data.event_date is not None:
            update_dict['event_date'] = event_data.event_date
        if event_data.event_type is not None:
            update_dict['event_type'] = event_data.event_type
        if event_data.title is not None:
            update_dict['title'] = event_data.title
        if event_data.description is not None:
            update_dict['description'] = event_data.description
        if event_data.position is not None:
            update_dict['position'] = event_data.position
        if event_data.related_url is not None:
            update_dict['related_url'] = event_data.related_url
        if event_data.related_file_id is not None:
            update_dict['related_file_id'] = event_data.related_file_id
        
        success = chart_events_service.update_event(current_user.user_id, event_id, update_dict)
        
        if success:
            info(f"图表事件更新成功: event_id={event_id}", source="chart_events_api")
            return {
                "success": True,
                "message": "事件更新成功"
            }
        else:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="事件不存在或无权限"
            )
            
    except HTTPException:
        raise
    except Exception as e:
        error(f"更新图表事件失败: {str(e)}", source="chart_events_api")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"更新事件失败: {str(e)}"
        )


@router.delete("/{event_id}", response_model=Dict[str, Any], summary="删除图表事件")
async def delete_chart_event(
    event_id: int,
    current_user: UserResponse = Depends(get_current_user)
):
    """
    删除指定的图表事件标记
    """
    try:
        info(f"删除图表事件: user={current_user.username}, event_id={event_id}", source="chart_events_api")
        
        chart_events_service = get_chart_events_service()
        success = chart_events_service.delete_event(current_user.user_id, event_id)
        
        if success:
            info(f"图表事件删除成功: event_id={event_id}", source="chart_events_api")
            return {
                "success": True,
                "message": "事件删除成功"
            }
        else:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="事件不存在或无权限"
            )
            
    except HTTPException:
        raise
    except Exception as e:
        error(f"删除图表事件失败: {str(e)}", source="chart_events_api")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"删除事件失败: {str(e)}"
        )


@router.get("", response_model=ChartEventsSummary, summary="获取用户事件统计")
async def get_user_events_summary(
    current_user: UserResponse = Depends(get_current_user)
):
    """
    获取当前用户的图表事件统计摘要
    """
    try:
        info(f"获取用户事件统计: user={current_user.username}", source="chart_events_api")
        
        chart_events_service = get_chart_events_service()
        summary = chart_events_service.get_user_events_summary(current_user.user_id)
        
        return summary
        
    except Exception as e:
        error(f"获取用户事件统计失败: {str(e)}", source="chart_events_api")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取统计失败: {str(e)}"
        )


@router.get("/export/all", response_model=List[ChartEventResponse], summary="导出所有事件")
async def export_user_events(
    current_user: UserResponse = Depends(get_current_user)
):
    """
    导出当前用户的所有图表事件数据
    """
    try:
        info(f"导出用户事件: user={current_user.username}", source="chart_events_api")
        
        chart_events_service = get_chart_events_service()
        events = chart_events_service.export_user_events(current_user.user_id)
        
        info(f"导出 {len(events)} 个事件", source="chart_events_api")
        return events
        
    except Exception as e:
        error(f"导出用户事件失败: {str(e)}", source="chart_events_api")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"导出事件失败: {str(e)}"
        )

