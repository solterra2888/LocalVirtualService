# -*- coding: utf-8 -*-
"""
调度器代理API - 主Web应用与独立调度器服务的通信桥梁
"""

import httpx
import requests
import asyncio
from fastapi import APIRouter, HTTPException, Depends, Request, Response
from typing import Any, Dict, Optional
from backend.auth.jwt_utils import get_current_user
from backend.utils.logger import get_logger
from config.settings import config

logger = get_logger("scheduler_proxy")

router = APIRouter(prefix="/scheduler", tags=["Scheduler Proxy"])

# 调度器服务配置
SCHEDULER_SERVICE_HOST = '127.0.0.1'  # 调度器服务在本地运行，使用IP地址避免DNS问题
SCHEDULER_SERVICE_PORT = 8001
SCHEDULER_BASE_URL = f"http://{SCHEDULER_SERVICE_HOST}:{SCHEDULER_SERVICE_PORT}/api"

class SchedulerServiceClient:
    """调度器服务客户端"""
    
    def __init__(self):
        self.base_url = SCHEDULER_BASE_URL
        self.timeout = 120.0  # 增加到2分钟，适应数据处理任务
    
    async def _make_request(self, method: str, endpoint: str, headers: Optional[Dict] = None, **kwargs) -> Dict[str, Any]:
        """发起HTTP请求到调度器服务"""
        url = f"{self.base_url}{endpoint}"
        
        # 合并请求头
        request_headers = headers or {}
        
        # 首先尝试使用httpx
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.request(method, url, headers=request_headers, **kwargs)
                
                # 处理响应
                if response.status_code >= 400:
                    logger.error(f"调度器服务请求失败: {response.status_code} - {response.text}")
                    raise HTTPException(
                        status_code=response.status_code,
                        detail=f"调度器服务错误: {response.text}"
                    )
                
                # 尝试解析JSON响应
                try:
                    result = response.json()
                    return result
                except:
                    # 如果不是JSON响应，返回文本
                    return {"content": response.text, "status_code": response.status_code}
                    
        except (httpx.ConnectError, httpx.TimeoutException) as e:
            logger.warning(f"httpx连接失败，尝试使用requests: {str(e)}")
            
            # 如果httpx失败，使用requests作为备选方案
            try:
                # 在线程池中运行requests
                loop = asyncio.get_event_loop()
                response = await loop.run_in_executor(
                    None,
                    lambda: requests.request(
                        method=method,
                        url=url,
                        headers=request_headers,
                        timeout=self.timeout,
                        **kwargs
                    )
                )
                
                # 处理响应
                if response.status_code >= 400:
                    logger.error(f"调度器服务请求失败: {response.status_code} - {response.text}")
                    raise HTTPException(
                        status_code=response.status_code,
                        detail=f"调度器服务错误: {response.text}"
                    )
                
                # 尝试解析JSON响应
                try:
                    result = response.json()
                    return result
                except:
                    # 如果不是JSON响应，返回文本
                    return {"content": response.text, "status_code": response.status_code}
                    
            except requests.exceptions.RequestException as req_e:
                logger.error(f"requests连接也失败: {str(req_e)}")
                raise HTTPException(
                    status_code=503,
                    detail="调度器服务不可用，请确保调度器服务正在运行"
                )
                
        except Exception as e:
            logger.error(f"调度器服务请求异常: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail=f"调度器服务请求失败: {str(e)}"
            )
    
    async def get(self, endpoint: str, params: Optional[Dict] = None, headers: Optional[Dict] = None) -> Dict[str, Any]:
        """GET请求"""
        return await self._make_request("GET", endpoint, headers=headers, params=params)
    
    async def post(self, endpoint: str, json_data: Optional[Dict] = None, data: Optional[Dict] = None, headers: Optional[Dict] = None) -> Dict[str, Any]:
        """POST请求"""
        return await self._make_request("POST", endpoint, headers=headers, json=json_data, data=data)
    
    async def delete(self, endpoint: str, headers: Optional[Dict] = None) -> Dict[str, Any]:
        """DELETE请求"""
        return await self._make_request("DELETE", endpoint, headers=headers)

# 全局客户端实例
scheduler_client = SchedulerServiceClient()

# 辅助函数
def get_auth_headers(request: Request) -> Dict[str, str]:
    """从请求中提取认证头"""
    headers = {}
    
    # 提取Authorization头
    auth_header = request.headers.get("authorization")
    if auth_header:
        headers["authorization"] = auth_header
    
    return headers

# ============ 代理API路由 ============

@router.get("/health", summary="调度器服务健康检查")
async def scheduler_health_proxy():
    """代理调度器服务健康检查"""
    try:
        result = await scheduler_client.get("/health")
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"健康检查代理失败: {str(e)}")

@router.get("/status", summary="调度器详细状态")
async def scheduler_status_proxy(current_user=Depends(get_current_user)):
    """代理调度器详细状态"""
    try:
        result = await scheduler_client.get("/status")
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"状态查询代理失败: {str(e)}")

@router.get("/tasks", summary="获取所有定时任务")
async def get_tasks_proxy(request: Request, current_user=Depends(get_current_user)): 
    """代理获取所有定时任务"""
    try:
        auth_headers = get_auth_headers(request)
        result = await scheduler_client.get("/scheduler/tasks", headers=auth_headers)
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取任务列表代理失败: {str(e)}")

@router.post("/tasks/cron", summary="创建Cron定时任务")
async def create_cron_task_proxy(request: Request, current_user=Depends(get_current_user)):
    """代理创建Cron定时任务"""
    try:
        auth_headers = get_auth_headers(request)
        # 获取请求体
        json_data = await request.json()
        result = await scheduler_client.post("/scheduler/tasks/cron", json_data=json_data, headers=auth_headers)
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"创建Cron任务代理失败: {str(e)}")

@router.post("/tasks/interval", summary="创建间隔定时任务")
async def create_interval_task_proxy(request: Request, current_user=Depends(get_current_user)):
    """代理创建间隔定时任务"""
    try:
        auth_headers = get_auth_headers(request)
        json_data = await request.json()
        result = await scheduler_client.post("/scheduler/tasks/interval", json_data=json_data, headers=auth_headers)
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"创建间隔任务代理失败: {str(e)}")

@router.post("/tasks/eodhd-news", summary="创建EODHD新闻定时任务")
async def create_eodhd_news_task_proxy(request: Request, current_user=Depends(get_current_user)):
    """代理创建EODHD新闻定时任务"""
    try:
        auth_headers = get_auth_headers(request)
        json_data = await request.json()
        result = await scheduler_client.post("/scheduler/tasks/eodhd-news", json_data=json_data, headers=auth_headers)
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"创建EODHD新闻任务代理失败: {str(e)}")

@router.post("/tasks/eodhd-fundamentals", summary="创建EODHD基本面数据定时任务")
async def create_eodhd_fundamentals_task_proxy(request: Request, current_user=Depends(get_current_user)):
    """代理创建EODHD基本面数据定时任务"""
    try:
        auth_headers = get_auth_headers(request)
        json_data = await request.json()
        result = await scheduler_client.post("/scheduler/tasks/eodhd-fundamentals", json_data=json_data, headers=auth_headers)
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"创建EODHD基本面任务代理失败: {str(e)}")

@router.post("/tasks/finviz-screener", summary="创建Finviz筛选数据定时任务")
async def create_finviz_screener_task_proxy(request: Request, current_user=Depends(get_current_user)):
    """代理创建Finviz筛选数据定时任务"""
    try:
        auth_headers = get_auth_headers(request)
        json_data = await request.json()
        result = await scheduler_client.post("/scheduler/tasks/finviz-screener", json_data=json_data, headers=auth_headers)
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"创建Finviz筛选任务代理失败: {str(e)}")

@router.post("/tasks/eodhd-news/default", summary="创建默认EODHD新闻定时任务")
async def create_default_eodhd_news_task_proxy(request: Request, current_user=Depends(get_current_user)):
    """代理创建默认EODHD新闻定时任务"""
    try:
        auth_headers = get_auth_headers(request)
        result = await scheduler_client.post("/scheduler/tasks/eodhd-news/default", headers=auth_headers)
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"创建默认EODHD新闻任务代理失败: {str(e)}")

@router.post("/tasks/watchlist-news", summary="创建Watchlist新闻定时任务")
async def create_watchlist_news_task_proxy(request: Request, current_user=Depends(get_current_user)):
    """代理创建Watchlist新闻定时任务"""
    try:
        auth_headers = get_auth_headers(request)
        json_data = await request.json()
        result = await scheduler_client.post("/scheduler/tasks/watchlist-news", json_data=json_data, headers=auth_headers)
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"创建Watchlist新闻任务代理失败: {str(e)}")

@router.post("/tasks/watchlist-news/default", summary="创建默认Watchlist新闻定时任务")
async def create_default_watchlist_news_task_proxy(request: Request, current_user=Depends(get_current_user)):
    """代理创建默认Watchlist新闻定时任务"""
    try:
        auth_headers = get_auth_headers(request)
        result = await scheduler_client.post("/scheduler/tasks/watchlist-news/default", headers=auth_headers)
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"创建默认Watchlist新闻任务代理失败: {str(e)}")

@router.post("/tasks/eodhd-stock-prices", summary="创建EODHD股票价格定时任务")
async def create_eodhd_stock_prices_task_proxy(request: Request, current_user=Depends(get_current_user)):
    """代理创建EODHD股票价格定时任务"""
    try:
        auth_headers = get_auth_headers(request)
        json_data = await request.json()
        result = await scheduler_client.post("/scheduler/tasks/eodhd-stock-prices", json_data=json_data, headers=auth_headers)
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"创建EODHD股票价格任务代理失败: {str(e)}")

@router.post("/tasks/eodhd-stock-prices/default", summary="创建默认EODHD股票价格定时任务")
async def create_default_eodhd_stock_prices_task_proxy(request: Request, current_user=Depends(get_current_user)):
    """代理创建默认EODHD股票价格定时任务"""
    try:
        auth_headers = get_auth_headers(request)
        result = await scheduler_client.post("/scheduler/tasks/eodhd-stock-prices/default", headers=auth_headers)
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"创建默认EODHD股票价格任务代理失败: {str(e)}")

@router.post("/tasks/eodhd-eod-stock-prices", summary="创建EODHD历史股价定时任务")
async def create_eodhd_eod_stock_prices_task_proxy(request: Request, current_user=Depends(get_current_user)):
    """代理创建EODHD历史股价定时任务"""
    try:
        auth_headers = get_auth_headers(request)
        json_data = await request.json()
        result = await scheduler_client.post("/scheduler/tasks/eodhd-eod-stock-prices", json_data=json_data, headers=auth_headers)
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"创建EODHD历史股价任务代理失败: {str(e)}")

@router.post("/tasks/eodhd-eod-stock-prices/default", summary="创建默认EODHD历史股价定时任务")
async def create_default_eodhd_eod_stock_prices_task_proxy(request: Request, current_user=Depends(get_current_user)):
    """代理创建默认EODHD历史股价定时任务"""
    try:
        auth_headers = get_auth_headers(request)
        result = await scheduler_client.post("/scheduler/tasks/eodhd-eod-stock-prices/default", headers=auth_headers)
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"创建默认EODHD历史股价任务代理失败: {str(e)}")

@router.post("/tasks/futu-themes", summary="创建Futu投资主题定时任务")
async def create_futu_themes_task_proxy(request: Request, current_user=Depends(get_current_user)):
    """代理创建Futu投资主题定时任务"""
    try:
        auth_headers = get_auth_headers(request)
        json_data = await request.json()
        result = await scheduler_client.post("/scheduler/tasks/futu-themes", json_data=json_data, headers=auth_headers)
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"创建Futu投资主题任务代理失败: {str(e)}")

@router.post("/tasks/futu-themes/default", summary="创建默认Futu投资主题定时任务")
async def create_default_futu_themes_task_proxy(request: Request, current_user=Depends(get_current_user)):
    """代理创建默认Futu投资主题定时任务"""
    try:
        auth_headers = get_auth_headers(request)
        result = await scheduler_client.post("/scheduler/tasks/futu-themes/default", headers=auth_headers)
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"创建默认Futu投资主题任务代理失败: {str(e)}")

@router.post("/tasks/market-data", summary="创建市场数据定时任务")
async def create_market_data_task_proxy(request: Request, current_user=Depends(get_current_user)):
    """代理创建市场数据定时任务"""
    try:
        auth_headers = get_auth_headers(request)
        json_data = await request.json()
        result = await scheduler_client.post("/scheduler/tasks/market-data", json_data=json_data, headers=auth_headers)
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"创建市场数据任务代理失败: {str(e)}")

@router.post("/tasks/market-data/default", summary="创建默认市场数据定时任务")
async def create_default_market_data_task_proxy(request: Request, current_user=Depends(get_current_user)):
    """代理创建默认市场数据定时任务"""
    try:
        auth_headers = get_auth_headers(request)
        result = await scheduler_client.post("/scheduler/tasks/market-data/default", headers=auth_headers)
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"创建默认市场数据任务代理失败: {str(e)}")

@router.post("/tasks/earnings-calendar", summary="创建财报日历定时任务")
async def create_earnings_calendar_task_proxy(request: Request, current_user=Depends(get_current_user)):
    """代理创建财报日历定时任务"""
    try:
        auth_headers = get_auth_headers(request)
        json_data = await request.json()
        result = await scheduler_client.post("/scheduler/tasks/earnings-calendar", json_data=json_data, headers=auth_headers)
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"创建财报日历任务代理失败: {str(e)}")

@router.post("/tasks/earnings-calendar/default", summary="创建默认财报日历定时任务")
async def create_default_earnings_calendar_task_proxy(request: Request, current_user=Depends(get_current_user)):
    """代理创建默认财报日历定时任务"""
    try:
        auth_headers = get_auth_headers(request)
        result = await scheduler_client.post("/scheduler/tasks/earnings-calendar/default", headers=auth_headers)
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"创建默认财报日历任务代理失败: {str(e)}")

@router.post("/tasks/{task_id}/enable", summary="启用定时任务")
async def enable_task_proxy(request: Request, task_id: str, current_user=Depends(get_current_user)):
    """代理启用定时任务"""
    try:
        auth_headers = get_auth_headers(request)
        result = await scheduler_client.post(f"/scheduler/tasks/{task_id}/enable", headers=auth_headers)
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"启用任务代理失败: {str(e)}")

@router.post("/tasks/{task_id}/disable", summary="禁用定时任务")
async def disable_task_proxy(request: Request, task_id: str, current_user=Depends(get_current_user)):
    """代理禁用定时任务"""
    try:
        auth_headers = get_auth_headers(request)
        result = await scheduler_client.post(f"/scheduler/tasks/{task_id}/disable", headers=auth_headers)
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"禁用任务代理失败: {str(e)}")

@router.delete("/tasks/{task_id}", summary="删除定时任务")
async def delete_task_proxy(request: Request, task_id: str, current_user=Depends(get_current_user)):
    """代理删除定时任务"""
    try:
        auth_headers = get_auth_headers(request)
        result = await scheduler_client.delete(f"/scheduler/tasks/{task_id}", headers=auth_headers)
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"删除任务代理失败: {str(e)}")

@router.post("/execute/eodhd-fundamentals", summary="手动执行EODHD基本面数据更新")
async def execute_fundamentals_update_proxy(request: Request, current_user=Depends(get_current_user)):
    """代理手动执行EODHD基本面数据更新"""
    try:
        auth_headers = get_auth_headers(request)
        json_data = await request.json()
        result = await scheduler_client.post("/scheduler/execute/eodhd-fundamentals", json_data=json_data, headers=auth_headers)
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"手动执行基本面数据更新代理失败: {str(e)}")

@router.get("/tasks/{task_id}/logs", summary="获取任务执行日志")
async def get_task_logs_proxy(task_id: str, limit: int = 50, offset: int = 0, current_user=Depends(get_current_user)):
    """代理获取任务执行日志"""
    try:
        params = {"limit": limit, "offset": offset}
        result = await scheduler_client.get(f"/scheduler/tasks/{task_id}/logs", params=params)
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取任务日志代理失败: {str(e)}")

@router.get("/execution-logs", summary="获取所有任务执行日志")
async def get_all_execution_logs_proxy(limit: int = 50, offset: int = 0, current_user=Depends(get_current_user)):
    """代理获取所有任务执行日志"""
    try:
        params = {"limit": limit, "offset": offset}
        result = await scheduler_client.get("/scheduler/execution-logs", params=params)
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取所有执行日志代理失败: {str(e)}")

@router.get("/execution-logs/download", summary="下载执行日志CSV")
async def download_execution_logs_csv_proxy(
    task_id: Optional[str] = None, 
    limit: int = 1000, 
    offset: int = 0, 
    current_user=Depends(get_current_user)
):
    """代理下载执行日志CSV"""
    try:
        params = {"limit": limit, "offset": offset}
        if task_id:
            params["task_id"] = task_id
        
        # 对于文件下载，需要特殊处理
        url = f"{SCHEDULER_BASE_URL}/scheduler/execution-logs/download"
        
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.get(url, params=params)
            
            if response.status_code >= 400:
                raise HTTPException(
                    status_code=response.status_code,
                    detail="下载CSV文件失败"
                )
            
            # 返回文件响应
            return Response(
                content=response.content,
                media_type="text/csv",
                headers={
                    "Content-Disposition": response.headers.get("Content-Disposition", "attachment; filename=execution_logs.csv")
                }
            )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"下载CSV代理失败: {str(e)}")

@router.delete("/execution-logs/clear", summary="清空执行日志")
async def clear_execution_logs_proxy(request: Request, current_user=Depends(get_current_user)):
    """代理清空执行日志"""
    try:
        auth_headers = get_auth_headers(request)
        result = await scheduler_client.delete("/scheduler/execution-logs/clear", headers=auth_headers)
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"清空执行日志代理失败: {str(e)}")

@router.get("/presets/cron-expressions", summary="获取常用Cron表达式预设")
async def get_cron_presets_proxy():
    """代理获取常用Cron表达式预设"""
    try:
        result = await scheduler_client.get("/scheduler/presets/cron-expressions")
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取Cron预设代理失败: {str(e)}")

@router.get("/presets/timezones", summary="获取支持的时区列表")
async def get_timezone_presets_proxy():
    """代理获取支持的时区列表"""
    try:
        result = await scheduler_client.get("/scheduler/presets/timezones")
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取时区预设代理失败: {str(e)}")

@router.get("/debug/database-tables", summary="查看数据库表数据（调试用）")
async def get_database_tables_proxy(request: Request, table_name: str, limit: int = 50, current_user=Depends(get_current_user)):
    """代理查看数据库表数据"""
    try:
        auth_headers = get_auth_headers(request)
        params = {"table_name": table_name, "limit": limit}
        result = await scheduler_client.get("/scheduler/debug/database-tables", params=params, headers=auth_headers)
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"查看数据库表代理失败: {str(e)}")

@router.get("/debug/table-structure", summary="查看表结构")
async def get_table_structure_proxy(current_user=Depends(get_current_user)):
    """代理查看表结构"""
    try:
        result = await scheduler_client.get("/scheduler/debug/table-structure")
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"查看表结构代理失败: {str(e)}")

