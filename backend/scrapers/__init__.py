# -*- coding: utf-8 -*-
"""
Scrapers Package

包含各种数据爬虫实现：
- Futu投资主题爬虫
- 其他数据源爬虫（待扩展）
"""

from .futu_scraper import scrape_all_markets

__all__ = ['scrape_all_markets']

