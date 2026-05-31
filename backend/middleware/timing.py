# -*- coding: utf-8 -*-
"""
性能监控中间件 - 记录 API 响应时间
"""

import time
from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
from backend.utils.logger import get_logger

logger = get_logger("timing")


class TimingMiddleware(BaseHTTPMiddleware):
    """记录每个请求的响应时间"""
    
    async def dispatch(self, request: Request, call_next):
        start_time = time.time()
        
        response = await call_next(request)
        
        process_time = time.time() - start_time
        process_time_ms = process_time * 1000
        
        # 只记录超过 100ms 的请求
        if process_time_ms > 100:
            logger.warning(
                f"⚠️  慢请求: {request.method} {request.url.path} - {process_time_ms:.2f}ms"
            )
        
        response.headers["X-Process-Time"] = str(process_time_ms)
        
        return response
