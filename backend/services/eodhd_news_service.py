# -*- coding: utf-8 -*-
"""
EODHD News HTTP 客户端 (精简版)

仅保留 Feeds 模块所需的 API 调用能力:
  - get_news()                  从 EODHD API 获取新闻列表
  - _extract_news_from_response() 从响应中提取新闻数组

写入 news 表的旧逻辑已随旧 News 模块一同移除。
"""

import os
import time
from typing import List, Dict, Any, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from backend.utils.env_loader import load_project_env
from backend.utils.logger import get_logger
from config.settings import config

load_project_env()

logger = get_logger("eodhd_service")


class EODHDNewsService:
    """EODHD News API 的轻量 HTTP 客户端，供 Feeds 模块拉取新闻原始数据使用。"""

    def __init__(self):
        self.api_token = os.getenv('EODHD_API_TOKEN')
        if not self.api_token:
            raise ValueError("EODHD_API_TOKEN 未在环境变量中设置")

        self.base_url = "https://eodhd.com/api/news"
        self.session = self._create_robust_session()
        logger.info("EODHDNewsService (HTTP client) 初始化完成")

    # ------------------------------------------------------------------
    # HTTP 会话
    # ------------------------------------------------------------------

    def _create_robust_session(self) -> requests.Session:
        """创建带重试机制的 HTTP 会话。"""
        retry_strategy = Retry(
            total=3,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
            raise_on_status=False,
            respect_retry_after_header=True,
        )
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=10,
            pool_maxsize=20,
            pool_block=False,
        )
        session = requests.Session()
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        session.headers.update({
            'User-Agent': f'{config.USER_AGENT} (EODHD News Service)',
            'Accept': 'application/json',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
        })
        return session

    # ------------------------------------------------------------------
    # 公开 API
    # ------------------------------------------------------------------

    def get_news(
        self,
        symbols: Optional[str] = None,
        topic: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> Any:
        """
        调用 EODHD /api/news 端点，返回原始 JSON 响应（list 或 dict）。

        Args:
            symbols:    逗号分隔的股票代码（可选）
            topic:      题材标签（可选）
            start_date: 开始日期 YYYY-MM-DD
            end_date:   结束日期 YYYY-MM-DD
            limit:      每页条数（最大 1000）
            offset:     分页偏移量

        Returns:
            EODHD API 原始响应（通常是 list[dict]）
        """
        params: Dict[str, Any] = {
            'api_token': self.api_token,
            'fmt': 'json',
            'limit': min(limit, 1000),
            'offset': offset,
        }
        if symbols:
            cleaned = symbols.replace(' ', '').strip()
            if cleaned:
                params['s'] = cleaned
        if topic:
            params['t'] = topic
        if start_date:
            params['from'] = start_date
        if end_date:
            params['to'] = end_date

        max_retries = 2
        last_exc: Optional[Exception] = None

        for attempt in range(max_retries + 1):
            try:
                if attempt > 0:
                    wait = 5 * attempt
                    logger.warning(f"第 {attempt + 1} 次重试 EODHD API（等待 {wait}s）")
                    time.sleep(wait)

                response = self.session.get(self.base_url, params=params, timeout=(15, 90))
                response.raise_for_status()

                result = response.json()
                count = len(result) if isinstance(result, list) else (
                    len(result.get('data', [])) if isinstance(result, dict) else 0
                )
                logger.info(f"EODHD API 响应成功: {count} 条新闻", attempt=attempt + 1)
                return result

            except requests.exceptions.Timeout as exc:
                last_exc = exc
                if attempt < max_retries:
                    continue
                raise Exception(f"EODHD API 请求超时（已重试 {max_retries} 次）: {exc}") from exc

            except requests.exceptions.RequestException as exc:
                raise Exception(f"EODHD API 调用失败: {exc}") from exc

        raise Exception(f"EODHD API 超时，已达最大重试次数: {last_exc}")

    def _extract_news_from_response(self, response: Any) -> List[Dict[str, Any]]:
        """从 API 响应中提取新闻数组。"""
        if isinstance(response, list):
            return response
        if isinstance(response, dict):
            return response.get('news', response.get('data', response.get('results', [])))
        return []
