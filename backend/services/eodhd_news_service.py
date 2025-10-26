# -*- coding: utf-8 -*-
"""
EODHD新闻服务 - 适配主系统架构
"""

import os
import requests
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# 统一的环境变量加载
from backend.utils.env_loader import load_project_env

from backend.database.db_manager import DatabaseManager
from backend.utils.logger import get_logger
from backend.config.exchange_config import ExchangeConfig, format_ticker_for_eodhd

# 获取日志实例
logger = get_logger("eodhd_service")


class EODHDNewsService:
    """EODHD新闻API服务类"""
    
    def __init__(self):
        """初始化EODHD新闻服务"""
        logger.info("开始初始化EODHD新闻服务")
        
        try:
            # 环境变量已通过 env_loader 自动加载
            logger.info("初始化EODHD新闻服务")
            logger.info("环境变量加载完成")
            
            # 获取API密钥
            logger.info("获取EODHD API密钥")
            self.api_token = os.getenv('EODHD_API_TOKEN')
            if not self.api_token:
                logger.error("EODHD_API_TOKEN 未在环境变量中设置")
                raise ValueError("EODHD_API_TOKEN 未在环境变量中设置")
            
            logger.info("API密钥获取成功", token_length=len(self.api_token))
            self.base_url = "https://eodhd.com/api/news"
            logger.info("设置API基础URL", base_url=self.base_url)
            
            # 创建优化的HTTP会话
            logger.info("创建优化的HTTP会话")
            self.session = self._create_robust_session()
            logger.info("HTTP会话创建完成")
            
            # 获取标准化数据库连接配置
            from backend.utils.database_config import get_database_config
            logger.info("准备数据库连接配置")
            db_config = get_database_config()
            logger.info("数据库配置准备完成", 
                        host=db_config['db_host'], 
                        port=db_config['db_port'],
                        database=db_config['db_name'],
                        user=db_config['db_user'],
                        password_set=bool(db_config['db_password']))
            
            # 创建数据库管理器
            logger.info("创建数据库管理器")
            self.db_manager = DatabaseManager(db_config)
            logger.info("数据库管理器创建完成")
            
            # 测试数据库连接
            logger.info("测试数据库连接")
            test_conn = self.db_manager.get_connection()
            if test_conn:
                test_conn.close()
                logger.info("数据库连接测试成功")
            else:
                logger.warning("数据库连接测试失败，但服务仍可继续")
            
            logger.info("EODHD新闻服务初始化完成")
            
        except Exception as e:
            logger.error("EODHD新闻服务初始化失败", exception=str(e))
            raise
    
    def _create_robust_session(self):
        """创建一个具有重试机制和优化超时设置的会话"""
        session = requests.Session()
        
        # 配置重试策略
        retry_strategy = Retry(
            total=3,  # 总重试次数
            backoff_factor=2,  # 指数退避因子
            status_forcelist=[429, 500, 502, 503, 504],  # 需要重试的状态码
            allowed_methods=["GET"],  # 只对GET请求重试
            raise_on_status=False  # 不立即抛出异常，让重试机制处理
        )
        
        # 创建适配器
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=10,  # 连接池大小
            pool_maxsize=20,  # 最大连接数
            pool_block=False  # 不阻塞连接池
        )
        
        # 挂载适配器
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # 设置默认请求头
        session.headers.update({
            'User-Agent': 'Universal Glance/1.0 (EODHD News Service)',
            'Accept': 'application/json',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive'
        })
        
        return session
    
    def get_news(self, symbols: Optional[str] = None, topic: Optional[str] = None, 
                 start_date: Optional[str] = None, end_date: Optional[str] = None,
                 limit: int = 100, offset: int = 0) -> Dict[str, Any]:
        """
        从EODHD API获取新闻数据
        
        Args:
            symbols: 逗号分隔的股票代码
            topic: 题材标签
            start_date: 开始日期 YYYY-MM-DD
            end_date: 结束日期 YYYY-MM-DD
            limit: 结果数量 (最大1000)
            offset: 分页偏移量
            
        Returns:
            API响应的JSON数据
        """
        # 记录API调用开始
        logger.info("调用EODHD API获取新闻", 
                   symbols=symbols, 
                   topic=topic, 
                   start_date=start_date, 
                   end_date=end_date,
                   limit=limit, 
                   offset=offset)
        
        params = {
            'api_token': self.api_token,
            'fmt': 'json',
            'limit': min(limit, 1000),
            'offset': offset
        }
        
        # 添加可选参数
        if symbols:
            # 清理股票代码：去除空格，确保格式正确
            cleaned_symbols = symbols.replace(' ', '').strip()
            if cleaned_symbols:
                params['s'] = cleaned_symbols
                logger.info("股票代码已清理", original=symbols, cleaned=cleaned_symbols)
        if topic:
            params['t'] = topic
        if start_date:
            params['from'] = start_date
        if end_date:
            params['to'] = end_date
            
        try:
            # 详细记录请求参数
            logger.info("发送EODHD API请求", 
                       url=self.base_url, 
                       params_count=len(params),
                       params=params)  # 完整打印请求参数
            
            # 使用优化的会话和更长的超时时间
            response = self.session.get(
                self.base_url, 
                params=params, 
                timeout=(10, 60)  # 连接超时10秒，读取超时60秒
            )
            response.raise_for_status()
            
            # 记录响应状态和内容预览
            response_text = response.text[:500] if response.text else "Empty response"
            logger.info("EODHD API请求发送完成，正在解析响应", 
                       status_code=response.status_code,
                       response_preview=response_text)
            
            result = response.json()
            result_count = len(result) if isinstance(result, list) else (len(result.get('data', [])) if isinstance(result, dict) else 0)
            logger.info("EODHD API响应成功", status_code=response.status_code, result_count=result_count)
            
            return result
        except requests.exceptions.Timeout as e:
            logger.error("EODHD API请求超时", exception=str(e), url=self.base_url)
            raise Exception(f"获取新闻超时: {e}")
        except requests.exceptions.RequestException as e:
            logger.error("EODHD API调用失败", exception=str(e), url=self.base_url)
            raise Exception(f"获取新闻失败: {e}")
    
    def fetch_and_store_news(self, symbols: Optional[str] = None, topic: Optional[str] = None,
                           start_date: Optional[str] = None, end_date: Optional[str] = None,
                           max_results: Optional[int] = None, topic_filter: Optional[str] = None,
                           keywords: Optional[List[str]] = None, scheduler_source: Optional[str] = None) -> Dict[str, Any]:
        """
        获取新闻并存储到数据库
        
        Args:
            symbols: 股票代码
            topic: 题材标签
            start_date: 开始日期
            end_date: 结束日期
            max_results: 最大结果数
            topic_filter: 主题过滤器
            keywords: 关键词列表
            scheduler_source: 调度器来源类型 ('eodhd_news', 'watchlist_news', 等)
            
        Returns:
            包含详细统计信息的字典
        """
        logger.info("开始新闻获取和存储流程", 
                   symbols=symbols, 
                   topic=topic, 
                   start_date=start_date, 
                   end_date=end_date,
                   max_results=max_results, 
                   topic_filter=topic_filter,
                   keywords_count=len(keywords) if keywords else 0)
        
        try:
            # 步骤1：获取新闻
            logger.info("步骤1：从EODHD API获取原始新闻数据")
            logger.info("调用_get_all_news方法")
            news_articles = self._get_all_news(
                symbols=symbols,
                topic=topic,
                start_date=start_date,
                end_date=end_date,
                max_results=max_results
            )
            logger.info("_get_all_news方法调用完成")
            
            if not news_articles:
                logger.warning("未获取到任何新闻数据")
                return {
                    'success_count': 0,
                    'existing_count': 0,
                    'failed_count': 0,
                    'total_count': 0,
                    'fetched_count': 0
                }
            
            logger.info("原始新闻数据获取完成", raw_count=len(news_articles))
            
            # 步骤2：应用过滤器
            if keywords or topic_filter:
                logger.info("步骤2：应用新闻过滤器", 
                           topic_filter=topic_filter, 
                           keywords_count=len(keywords) if keywords else 0)
                original_count = len(news_articles)
                news_articles = self._filter_news(news_articles, keywords, topic_filter)
                filtered_count = len(news_articles)
                logger.info("新闻过滤完成", 
                           original_count=original_count, 
                           filtered_count=filtered_count,
                           filtered_out=original_count - filtered_count)
            else:
                logger.info("步骤2：跳过过滤器（未指定过滤条件）")
            
            fetched_count = len(news_articles)
            
            # 🚀 优化：预加载主题关键词缓存，避免重复查询数据库
            logger.info("步骤3：预加载主题关键词缓存")
            topic_keywords_cache = self._get_all_topic_keywords()
            logger.info("主题关键词缓存加载完成", topics_count=len(topic_keywords_cache))
            
            # 步骤4：处理新闻数据
            logger.info("步骤4：处理新闻数据以适配数据库", articles_to_process=fetched_count)
            processed_articles = []
            failed_count = 0
            
            for i, article in enumerate(news_articles):
                try:
                    # 传入缓存，避免每条新闻都查询数据库
                    processed = self._process_article_for_db(article, topic_keywords_cache)
                    processed_articles.append(processed)
                    if (i + 1) % 100 == 0:  # 每100条记录一次进度
                        logger.debug("新闻处理进度", 
                                   processed=i + 1, 
                                   total=fetched_count,
                                   percentage=round((i + 1) / fetched_count * 100, 1))
                except Exception as e:
                    logger.warning("处理单条新闻失败", 
                                 article_index=i + 1, 
                                 exception=str(e),
                                 title=article.get('title', 'Unknown')[:50] if isinstance(article, dict) else 'Invalid article')
                    failed_count += 1
                    continue
            
            logger.info("新闻数据处理完成", 
                       total_processed=len(processed_articles),
                       failed_count=failed_count)
            
            if not processed_articles:
                logger.warning("没有成功处理的新闻数据")
                return {
                    'success_count': 0,
                    'new_count': 0,
                    'existing_count': 0,
                    'failed_count': failed_count,
                    'total_count': fetched_count,
                    'fetched_count': fetched_count
                }
            
            # 步骤5：批量插入数据库
            logger.info("步骤5：批量插入新闻到数据库", articles_to_insert=len(processed_articles))
            db_result = self._batch_insert_news(processed_articles, scheduler_source)
            
            # 汇总结果
            final_result = {
                'success_count': db_result['success_count'],
                'new_count': db_result.get('new_count', 0),  # 🔥 新增：传递真正新增的数量
                'existing_count': db_result['existing_count'],
                'failed_count': db_result['failed_count'] + failed_count,
                'total_count': fetched_count,
                'fetched_count': fetched_count,
                'new_news_ids': db_result.get('new_news_ids', [])  # 🔥 新增：传递新插入的新闻ID列表
            }
            
            logger.info("新闻获取和存储流程完成", 
                       success_count=final_result['success_count'],
                       new_count=final_result.get('new_count', 0),
                       existing_count=final_result['existing_count'],
                       failed_count=final_result['failed_count'],
                       total_count=final_result['total_count'],
                       new_news_count=len(final_result['new_news_ids']))
            
            return final_result
            
        except Exception as e:
            logger.error("新闻获取和存储流程失败", exception=str(e))
            raise Exception(f"获取和存储新闻时发生错误: {str(e)}")
    
    def _get_all_news(self, symbols: Optional[str] = None, topic: Optional[str] = None,
                     start_date: Optional[str] = None, end_date: Optional[str] = None,
                     max_results: Optional[int] = None) -> List[Dict[str, Any]]:
        """获取所有新闻"""
        # 如果max_results为None，使用默认值100
        if max_results is None:
            max_results = 100
        
        logger.info("开始分页获取新闻", max_results=max_results, symbols=symbols, topic=topic)
        
        # 如果指定了多个股票代码，尝试逐个股票请求
        if symbols and ',' in symbols:
            logger.info("检测到多个股票代码，尝试逐个股票请求", symbols=symbols)
            return self._get_news_by_individual_symbols(symbols, topic, start_date, end_date, max_results)
        
        all_news = []
        limit = min(1000, max_results)
        offset = 0
        batch_count = 0
        
        while offset < max_results:
            batch_count += 1
            logger.debug("获取新闻批次", 
                        batch=batch_count, 
                        offset=offset, 
                        limit=limit,
                        total_collected=len(all_news))
            
            response = self.get_news(
                symbols=symbols,
                topic=topic,
                start_date=start_date,
                end_date=end_date,
                limit=limit,
                offset=offset
            )
            
            if not response:
                logger.warning("API响应为空，停止获取", batch=batch_count)
                # 如果是第一批且有股票代码限制，尝试逐个股票请求作为备用方案
                if batch_count == 1 and symbols:
                    logger.info("尝试逐个股票请求作为备用方案", symbols=symbols)
                    individual_news = self._get_news_by_individual_symbols(symbols, topic, start_date, end_date, max_results)
                    if individual_news:
                        logger.info("逐个股票请求成功获取到数据", news_count=len(individual_news))
                        return individual_news
                    else:
                        break
                else:
                    break
            
            # 从API响应中提取新闻数组
            current_batch = self._extract_news_from_response(response)
            if not current_batch:
                logger.warning("当前批次无新闻数据，停止获取", batch=batch_count)
                break
                
            all_news.extend(current_batch)
            logger.debug("批次获取完成", 
                        batch=batch_count,
                        batch_size=len(current_batch),
                        total_collected=len(all_news))
            
            if len(current_batch) < limit:
                logger.info("API返回数据少于请求数量，已获取全部可用新闻", 
                           batch=batch_count,
                           expected=limit,
                           actual=len(current_batch))
                break
                
            offset += limit
            
            # API调用间隔
            if offset < max_results:
                logger.debug("API调用间隔等待1秒")
                time.sleep(1)  # 避免频率限制
            
            if offset >= max_results:
                logger.info("达到最大结果数限制", max_results=max_results)
                break
        
        final_news = all_news[:max_results]
        logger.info("分页获取新闻完成", 
                   total_batches=batch_count,
                   raw_collected=len(all_news),
                   final_count=len(final_news))
        
        return final_news
    
    def _get_news_by_individual_symbols(self, symbols: str, topic: Optional[str] = None,
                                      start_date: Optional[str] = None, end_date: Optional[str] = None,
                                      max_results: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        逐个股票代码请求新闻（用于港股等不支持批量请求的情况）
        
        Args:
            symbols: 逗号分隔的股票代码字符串
            topic: 题材标签
            start_date: 开始日期
            end_date: 结束日期
            max_results: 最大结果数
            
        Returns:
            新闻列表
        """
        if not symbols:
            return []
        
        # 解析股票代码
        symbol_list = [s.strip() for s in symbols.split(',') if s.strip()]
        if not symbol_list:
            return []
        
        logger.info("开始逐个股票请求新闻", 
                   symbols_count=len(symbol_list), 
                   symbols=symbol_list,
                   max_results=max_results)
        
        all_news = []
        successful_symbols = []
        failed_symbols = []
        
        # 计算每个股票的最大结果数
        per_symbol_limit = max(1, max_results // len(symbol_list)) if max_results else 50
        
        for i, symbol in enumerate(symbol_list, 1):
            logger.info(f"请求股票 {i}/{len(symbol_list)}: {symbol}")
            
            try:
                # 确保股票代码格式正确（添加.HK后缀如果缺失）
                formatted_symbol = self._format_stock_symbol(symbol)
                
                # 请求单个股票的新闻
                response = self.get_news(
                    symbols=formatted_symbol,
                    topic=topic,
                    start_date=start_date,
                    end_date=end_date,
                    limit=per_symbol_limit,
                    offset=0
                )
                
                if response:
                    news_articles = self._extract_news_from_response(response)
                    if news_articles:
                        all_news.extend(news_articles)
                        successful_symbols.append(symbol)
                        logger.info(f"股票 {symbol} 请求成功: {len(news_articles)} 条新闻")
                    else:
                        logger.warning(f"股票 {symbol} 无新闻数据")
                        failed_symbols.append(symbol)
                else:
                    logger.warning(f"股票 {symbol} API响应为空")
                    failed_symbols.append(symbol)
                
                # 添加延迟避免API限制
                if i < len(symbol_list):
                    logger.debug("等待2秒避免API限制")
                    time.sleep(2)
                    
            except Exception as e:
                logger.error(f"股票 {symbol} 请求失败: {str(e)}")
                failed_symbols.append(symbol)
        
        # 限制总结果数
        if max_results and len(all_news) > max_results:
            all_news = all_news[:max_results]
        
        logger.info("逐个股票请求完成", 
                   total_news=len(all_news),
                   successful_symbols=successful_symbols,
                   failed_symbols=failed_symbols)
        
        return all_news
    
    def _format_stock_symbol(self, symbol: str) -> str:
        """
        格式化股票代码，确保格式正确
        
        使用统一的ExchangeConfig进行格式化，确保与EODHD官方规范一致
        
        Args:
            symbol: 原始股票代码
            
        Returns:
            格式化后的股票代码
        """
        return format_ticker_for_eodhd(symbol)
    
    def _extract_news_from_response(self, response) -> List[Dict[str, Any]]:
        """从API响应中提取新闻数组"""
        if isinstance(response, list):
            return response
        elif isinstance(response, dict):
            return response.get('news', response.get('data', response.get('results', [])))
        else:
            return []
    
    def _filter_news(self, news_articles: List[Dict[str, Any]], 
                    keywords: Optional[List[str]] = None,
                    topic_filter: Optional[str] = None) -> List[Dict[str, Any]]:
        """根据关键词或主题过滤新闻"""
        if not keywords and not topic_filter:
            logger.debug("无过滤条件，返回原始新闻列表")
            return news_articles
        
        logger.info("开始新闻过滤", 
                   input_count=len(news_articles),
                   topic_filter=topic_filter,
                   custom_keywords=len(keywords) if keywords else 0)
        
        # 获取主题关键词
        if topic_filter:
            logger.debug("获取主题关键词", topic_filter=topic_filter)
            topic_keywords = self._get_topic_keywords(topic_filter)
            logger.debug("主题关键词获取完成", 
                        topic_keywords_count=len(topic_keywords),
                        topic_keywords=topic_keywords[:5] if topic_keywords else [])  # 只显示前5个关键词
            
            if not keywords:
                keywords = topic_keywords
            else:
                keywords = keywords + topic_keywords
        
        if not keywords:
            logger.warning("未找到有效的过滤关键词")
            return news_articles
            
        # 转换为小写进行不区分大小写匹配
        keywords = [k.lower() for k in keywords]
        logger.debug("最终过滤关键词", keywords_count=len(keywords), keywords=keywords[:10])  # 显示前10个关键词
        
        filtered_articles = []
        matched_keywords_stats = {}
        
        for i, article in enumerate(news_articles):
            title = article.get('title', '').lower()
            content = article.get('content', '').lower()
            text = title + ' ' + content
            
            # 检查匹配的关键词
            matched_keywords = []
            for keyword in keywords:
                if keyword in text:
                    matched_keywords.append(keyword)
                    matched_keywords_stats[keyword] = matched_keywords_stats.get(keyword, 0) + 1
            
            # 如果包含任意关键词则添加文章
            if matched_keywords:
                filtered_articles.append(article)
                logger.debug("新闻匹配成功", 
                           article_index=i + 1,
                           title=title[:50],
                           matched_keywords=matched_keywords[:3])  # 只显示前3个匹配的关键词
        
        # 记录过滤统计
        logger.info("新闻过滤完成", 
                   input_count=len(news_articles),
                   output_count=len(filtered_articles),
                   filter_rate=round(len(filtered_articles) / len(news_articles) * 100, 1) if news_articles else 0)
        
        if matched_keywords_stats:
            top_keywords = sorted(matched_keywords_stats.items(), key=lambda x: x[1], reverse=True)[:5]
            logger.debug("热门匹配关键词", top_keywords=top_keywords)
                
        return filtered_articles
    
    def _get_topic_keywords(self, topic_name: str) -> List[str]:
        """从数据库获取主题关键词"""
        try:
            conn = self.db_manager.get_connection()
            if not conn:
                return []
            
            cursor = conn.cursor()
            query = """
                SELECT tk.keyword
                FROM news_topics t
                JOIN news_topic_keywords tk ON t.id = tk.topic_id
                WHERE t.topic_name = %s AND t.is_active = true
            """
            cursor.execute(query, (topic_name,))
            results = cursor.fetchall()
            
            keywords = [row[0] for row in results]
            cursor.close()
            conn.close()
            
            return keywords
            
        except Exception:
            return []
    
    def _process_article_for_db(self, article: Dict[str, Any], topic_keywords_cache: Dict[str, List[str]] = None) -> Dict[str, Any]:
        """处理文章数据以适应数据库格式
        
        Args:
            article: 新闻文章数据
            topic_keywords_cache: 主题关键词缓存（可选，用于批量处理时避免重复查询）
        """
        # 从文章中提取情感数据
        sentiment = article.get('sentiment') or {}
        
        # 获取或生成source信息
        source = article.get('source')
        if not source or source == 'EODHD':
            link = article.get('link', '')
            if link:
                source = self._extract_source_from_url(link)
            else:
                source = source or 'Unknown'
        
        processed = {
            'date': article.get('date'),
            'title': article.get('title'),
            'content': article.get('content'),
            'link': article.get('link'),
            'source': source,
            'symbols': article.get('symbols', []),
            'tags': self._auto_tag_article(article, topic_keywords_cache),  # 用户自定义的topic tags
            'api_tags': article.get('tags', []),      # EODHD API原始tags
            'sentiment_polarity': sentiment.get('polarity') if sentiment else None,
            'sentiment_neg': sentiment.get('neg') if sentiment else None,
            'sentiment_neu': sentiment.get('neu') if sentiment else None,
            'sentiment_pos': sentiment.get('pos') if sentiment else None,
            'formatted_text': self._generate_formatted_text(article)
        }
        
        return processed
    
    def _extract_source_from_url(self, url: str) -> str:
        """从URL中提取来源信息"""
        if not url:
            return 'Unknown'
        
        try:
            from urllib.parse import urlparse
            parsed = urlparse(url)
            domain = parsed.netloc
            
            # 清理域名，移除www前缀
            if domain.startswith('www.'):
                domain = domain[4:]
            
            # 根据常见新闻源进行映射
            source_mapping = {
                'finance.yahoo.com': 'Yahoo Finance',
                'seekingalpha.com': 'Seeking Alpha',
                'bloomberg.com': 'Bloomberg',
                'reuters.com': 'Reuters',
                'cnbc.com': 'CNBC',
                'marketwatch.com': 'MarketWatch',
                'wsj.com': 'Wall Street Journal',
                'ft.com': 'Financial Times',
                'forbes.com': 'Forbes',
                'businessinsider.com': 'Business Insider'
            }
            
            # 查找映射
            for domain_key, source_name in source_mapping.items():
                if domain_key in domain:
                    return source_name
            
            # 如果没有找到映射，返回清理后的域名
            return domain.replace('.com', '').replace('.org', '').replace('.net', '').title()
            
        except Exception:
            return 'Unknown'
    
    def _get_all_topic_keywords(self) -> Dict[str, List[str]]:
        """🚀 优化：一次性获取所有主题关键词，供批量处理时使用缓存"""
        topic_keywords = {}
        
        try:
            conn = self.db_manager.get_connection()
            if not conn:
                return topic_keywords
            
            cursor = conn.cursor()
            query = """
                SELECT t.topic_name, tk.keyword
                FROM news_topics t
                JOIN news_topic_keywords tk ON t.id = tk.topic_id
                WHERE t.is_active = true
            """
            cursor.execute(query)
            results = cursor.fetchall()
            
            # 按主题分组关键词
            for row in results:
                topic_name = row[0].upper()
                keyword = row[1].lower()
                
                if topic_name not in topic_keywords:
                    topic_keywords[topic_name] = []
                topic_keywords[topic_name].append(keyword)
            
            cursor.close()
            conn.close()
            
        except Exception as e:
            logger.warning(f"获取主题关键词失败: {e}")
        
        return topic_keywords
    
    def _auto_tag_article(self, article: Dict[str, Any], topic_keywords_cache: Dict[str, List[str]] = None) -> List[str]:
        """根据文章内容自动添加标签
        
        Args:
            article: 新闻文章
            topic_keywords_cache: 主题关键词缓存（如果提供，则使用缓存而不查询数据库）
        """
        tags = []
        
        title = article.get('title', '').lower()
        content = article.get('content', '').lower()
        text = title + ' ' + content
        
        # 🚀 优化：优先使用缓存，避免重复查询数据库
        if topic_keywords_cache is not None:
            topic_keywords = topic_keywords_cache
        else:
            # 如果没有缓存，才查询数据库（兼容单条处理的场景）
            topic_keywords = self._get_all_topic_keywords()
        
        # 检查匹配
        for tag_name, keywords in topic_keywords.items():
            if any(keyword in text for keyword in keywords):
                tags.append(tag_name)
        
        return tags
    
    def _generate_formatted_text(self, article: Dict[str, Any]) -> str:
        """生成结构化文本，便于LLM分析"""
        formatted_parts = []
        
        # 标题
        title = article.get('title', '未知标题')
        formatted_parts.append(f"# {title}")
        formatted_parts.append("")
        
        # 元数据
        formatted_parts.append("## 新闻元数据")
        formatted_parts.append(f"**发布时间:** {article.get('date', '未知')}")
        formatted_parts.append(f"**新闻来源:** {article.get('source', '未知')}")
        
        # 相关股票
        symbols = article.get('symbols', [])
        if symbols:
            formatted_parts.append(f"**相关股票:** {', '.join(symbols)}")
        
        # 链接
        link = article.get('link')
        if link:
            formatted_parts.append(f"**原文链接:** {link}")
        
        formatted_parts.append("")
        
        # 正文内容
        content = article.get('content', '')
        if content:
            formatted_parts.append("## 新闻内容")
            formatted_parts.append(content)
            formatted_parts.append("")
        
        # 情感分析
        sentiment = article.get('sentiment')
        if sentiment:
            polarity = sentiment.get('polarity')
            if polarity is not None:
                formatted_parts.append("## 情感分析")
                sentiment_label = "正面" if polarity > 0.1 else "负面" if polarity < -0.1 else "中性"
                formatted_parts.append(f"**整体情感:** {sentiment_label} ({polarity:.3f})")
                
                for key, label in [('pos', '正面情感'), ('neg', '负面情感'), ('neu', '中性情感')]:
                    value = sentiment.get(key)
                    if value is not None:
                        formatted_parts.append(f"**{label}:** {value:.3f}")
        
        return "\n".join(formatted_parts)
    
    def _batch_insert_news(self, articles: List[Dict[str, Any]], scheduler_source: Optional[str] = None) -> Dict[str, Any]:
        """批量插入新闻到数据库（高性能优化版）"""
        logger.info("开始批量插入新闻到数据库", articles_count=len(articles))
        
        if not articles:
            return {'success_count': 0, 'new_count': 0, 'existing_count': 0, 'failed_count': 0, 'new_news_ids': []}
        
        try:
            # 🚀 优化：分批处理，使用更大的批次（1000条）
            batch_size = 1000  # 每批处理1000条新闻（参考eodhd_fundamentals）
            total_success = 0
            total_new = 0  # 🔥 新增：追踪真正新增的新闻数量
            total_existing = 0
            total_failed = 0
            all_new_news_ids = []  # 🔥 新增：收集所有新插入的新闻ID
            
            total_batches = (len(articles) + batch_size - 1) // batch_size
            
            for i in range(0, len(articles), batch_size):
                batch = articles[i:i + batch_size]
                batch_num = i // batch_size + 1
                
                logger.info(f"处理第 {batch_num}/{total_batches} 批新闻", batch_size=len(batch))
                
                try:
                    # 使用连接上下文管理器，自动管理连接和事务
                    with self.db_manager.get_connection_context() as conn:
                        with conn.cursor() as cursor:
                            # 批量插入/更新新闻（返回去重后的文章列表）
                            news_ids, success_count, failed_count, new_count, existing_count, unique_batch = self._batch_insert_new_articles(cursor, batch, scheduler_source)
                            
                            # 批量处理symbols和tags（使用去重后的文章列表）
                            if news_ids and unique_batch:
                                self._batch_insert_symbols_and_tags(cursor, unique_batch, news_ids)
                            
                            # 🔥 新增：收集新插入的新闻ID（不包括已存在的）
                            if news_ids and new_count > 0:
                                # new_count表示新插入的数量，从news_ids的前new_count个ID是新插入的
                                batch_new_ids = news_ids[:new_count]
                                all_new_news_ids.extend(batch_new_ids)
                                logger.debug(f"批次 {batch_num} 新插入 {new_count} 条新闻，ID: {batch_new_ids[:5]}{'...' if len(batch_new_ids) > 5 else ''}")
                            
                            # 上下文管理器自动提交
                            logger.info(f"第 {batch_num} 批事务提交成功", 
                                      success=success_count, existing=existing_count, failed=failed_count)
                            
                            total_success += success_count
                            total_new += new_count  # 🔥 累积新增数量
                            total_existing += existing_count
                            total_failed += failed_count
                        
                except Exception as batch_error:
                    # 单批失败
                    logger.error(f"第 {batch_num} 批插入失败", exception=str(batch_error))
                    total_failed += len(batch)
            
            result = {
                'success_count': total_success,
                'new_count': total_new,  # 🔥 新增：返回真正新增的数量
                'existing_count': total_existing,
                'failed_count': total_failed,
                'new_news_ids': all_new_news_ids  # 🔥 新增：返回所有新插入的新闻ID
            }
            
            logger.info("批量插入新闻完成", 
                       success_count=total_success,
                       new_count=total_new,
                       existing_count=total_existing,
                       failed_count=total_failed,
                       new_news_count=len(all_new_news_ids))
            return result
                
        except Exception as e:
            logger.error("批量插入新闻失败", exception=str(e))
            return {
                'success_count': 0,
                'new_count': 0,  # 🔥 新增
                'existing_count': 0,
                'failed_count': len(articles),
                'new_news_ids': []
            }
    
    def _batch_check_existing_news(self, cursor, articles: List[Dict[str, Any]]) -> set:
        """批量检查已存在的新闻链接"""
        links = [article.get('link') for article in articles if article.get('link')]
        if not links:
            return set()
        
        # 使用参数化查询批量检查
        placeholders = ','.join(['%s'] * len(links))
        query = f"SELECT link FROM news WHERE link IN ({placeholders})"
        
        cursor.execute(query, links)
        existing_links = {row[0] for row in cursor.fetchall()}
        
        return existing_links
    
    def _batch_insert_new_articles(self, cursor, new_articles: List[Dict[str, Any]], scheduler_source: Optional[str] = None) -> tuple:
        """批量插入新的新闻文章（使用execute_values高性能批量插入）"""
        if not new_articles:
            return [], 0, 0, 0, 0, []
        
        # 🚀 优化：先对文章进行去重，避免同一批次中有重复的link
        # PostgreSQL的ON CONFLICT不允许在同一批次中处理相同约束值的多条记录
        seen_links = set()
        unique_articles = []
        duplicate_count = 0
        
        for article in new_articles:
            link = article.get('link')
            if link:
                if link not in seen_links:
                    seen_links.add(link)
                    unique_articles.append(article)
                else:
                    duplicate_count += 1
                    logger.debug(f"跳过重复新闻: {link}")
            else:
                # 没有link的文章也保留（虽然这种情况应该很少）
                unique_articles.append(article)
        
        if duplicate_count > 0:
            logger.info(f"批次内去重: 原始{len(new_articles)}条，去重后{len(unique_articles)}条，跳过{duplicate_count}条重复")
        
        # 🔥 关键修复：在插入前检查哪些新闻已存在
        links_to_insert = [article.get('link') for article in unique_articles if article.get('link')]
        existing_links_before = set()
        if links_to_insert:
            placeholders = ','.join(['%s'] * len(links_to_insert))
            check_sql = f"SELECT link FROM news WHERE link IN ({placeholders})"
            cursor.execute(check_sql, links_to_insert)
            existing_links_before = {row[0] for row in cursor.fetchall()}
        
        logger.debug(f"插入前检查: 共{len(links_to_insert)}条，已存在{len(existing_links_before)}条")
        
        # 准备批量插入的数据
        news_data = []
        for article in unique_articles:
            news_data.append((
                article.get('date'),
                article.get('title'),
                article.get('content'),
                article.get('link'),
                article.get('source'),
                article.get('sentiment_polarity'),
                article.get('sentiment_neg'),
                article.get('sentiment_neu'),
                article.get('sentiment_pos'),
                article.get('formatted_text'),
                scheduler_source
            ))
                    
        try:
            # 🚀 使用 execute_values 进行高性能批量UPSERT（参考eodhd_fundamentals）
            from psycopg2.extras import execute_values
            
            # 第一步：批量插入/更新新闻
            upsert_sql = """
                INSERT INTO news (date, title, content, link, source,
                                sentiment_polarity, sentiment_neg, sentiment_neu, sentiment_pos,
                                formatted_text, scheduler_source)
                VALUES %s
                ON CONFLICT (link) DO UPDATE SET
                    date = EXCLUDED.date,
                    title = EXCLUDED.title,
                    content = EXCLUDED.content,
                    source = EXCLUDED.source,
                    sentiment_polarity = EXCLUDED.sentiment_polarity,
                    sentiment_neg = EXCLUDED.sentiment_neg,
                    sentiment_neu = EXCLUDED.sentiment_neu,
                    sentiment_pos = EXCLUDED.sentiment_pos,
                    formatted_text = EXCLUDED.formatted_text,
                    scheduler_source = EXCLUDED.scheduler_source,
                    updated_at = CURRENT_TIMESTAMP
            """
            
            # 执行批量插入（一次性处理所有数据）
            execute_values(
                cursor,
                upsert_sql,
                news_data,
                template=None,
                page_size=1000  # 每页1000条
            )
            
            # 第二步：批量获取插入的新闻ID
            # 使用link来查询所有刚插入/更新的新闻的ID
            links = [article.get('link') for article in unique_articles if article.get('link')]
            
            if links:
                placeholders = ','.join(['%s'] * len(links))
                select_sql = f"""
                    SELECT id, link FROM news 
                    WHERE link IN ({placeholders})
                    ORDER BY id
                """
                cursor.execute(select_sql, links)
                results = cursor.fetchall()
                
                # 创建link到id的映射
                link_to_id = {row[1]: row[0] for row in results}
                
                # 按照原始顺序返回news_ids
                news_ids = [link_to_id.get(article.get('link')) for article in unique_articles]
            else:
                news_ids = []
            
            # 🔥 修复：正确区分新增和更新的新闻
            # 统计新增（不在existing_links_before中）和更新的（在existing_links_before中）
            new_count = sum(1 for article in unique_articles 
                          if article.get('link') not in existing_links_before)
            existing_count = len(existing_links_before)
            success_count = len(news_ids)
            # 失败数 = 去重后的数量 - 成功数 + 批次内重复数
            failed_count = (len(unique_articles) - success_count) + duplicate_count
            
            logger.debug(f"批量处理新闻: 成功{success_count}, 新增{new_count}, 更新{existing_count}, 失败{failed_count}")
            # 返回去重后的文章列表，确保与news_ids对应
            return news_ids, success_count, failed_count, new_count, existing_count, unique_articles
            
        except Exception as e:
            logger.error(f"批量处理新闻失败: {e}")
            return [], 0, len(new_articles), 0, 0, []
    
    def _batch_insert_symbols_and_tags(self, cursor, articles: List[Dict[str, Any]], news_ids: List[int]):
        """批量插入symbols、tags和api_tags关联"""
        if not articles or not news_ids:
            return
        
        # 收集所有需要处理的symbols、tags和api_tags
        all_symbols = set()
        all_tags = set()
        all_api_tags = set()
        
        symbol_relations = []  # (news_id, symbol)
        tag_relations = []     # (news_id, tag)
        api_tag_relations = [] # (news_id, api_tag)
        
        for i, article in enumerate(articles):
            news_id = news_ids[i] if i < len(news_ids) else None
            if not news_id:
                continue
                
            # 收集symbols
            symbols = article.get('symbols', [])
            for symbol in symbols:
                if symbol:
                    all_symbols.add(symbol)
                    symbol_relations.append((news_id, symbol))
                    
            # 收集用户自定义tags
            tags = article.get('tags', [])
            for tag in tags:
                if tag:
                    all_tags.add(tag)
                    tag_relations.append((news_id, tag))
            
            # 收集API原始tags
            api_tags = article.get('api_tags', [])
            for api_tag in api_tags:
                if api_tag:
                    all_api_tags.add(api_tag)
                    api_tag_relations.append((news_id, api_tag))
        
        # 批量插入symbols
        if all_symbols:
            self._batch_upsert_symbols(cursor, list(all_symbols))
            self._batch_create_symbol_relations(cursor, symbol_relations)
        
        # 批量插入用户自定义tags
        if all_tags:
            self._batch_upsert_tags(cursor, list(all_tags))
            self._batch_create_tag_relations(cursor, tag_relations)
        
        # 批量插入API原始tags
        if all_api_tags:
            self._batch_upsert_api_tags(cursor, list(all_api_tags))
            self._batch_create_api_tag_relations(cursor, api_tag_relations)
        
        logger.debug(f"批量处理关联: {len(symbol_relations)}个symbols, {len(tag_relations)}个tags, {len(api_tag_relations)}个api_tags")
    
    def _batch_upsert_symbols(self, cursor, symbols: List[str]):
        """批量插入或更新symbols字典（使用execute_values优化）"""
        if not symbols:
            return
            
        # 🚀 使用 execute_values 批量插入（参考eodhd_fundamentals）
        from psycopg2.extras import execute_values
        
        symbol_data = [(symbol,) for symbol in symbols]
        sql = """
            INSERT INTO news_symbols_dict (symbol_code) 
            VALUES %s
            ON CONFLICT (symbol_code) DO NOTHING
        """
        execute_values(cursor, sql, symbol_data, page_size=1000)
    
    def _batch_upsert_tags(self, cursor, tags: List[str]):
        """批量插入或更新tags字典（使用execute_values优化）"""
        if not tags:
            return
            
        # 🚀 使用 execute_values 批量插入（参考eodhd_fundamentals）
        from psycopg2.extras import execute_values
        
        tag_data = [(tag,) for tag in tags]
        sql = """
            INSERT INTO news_tags_dict (tag_name) 
            VALUES %s
            ON CONFLICT (tag_name) DO NOTHING
        """
        execute_values(cursor, sql, tag_data, page_size=1000)
    
    def _batch_upsert_api_tags(self, cursor, api_tags: List[str]):
        """批量插入或更新API原始tags字典（使用execute_values优化）"""
        if not api_tags:
            return
        
        # 创建news_auto_tags表（如果不存在）
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS news_auto_tags (
                id SERIAL PRIMARY KEY,
                tag_name VARCHAR(255) UNIQUE NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # 🚀 使用 execute_values 批量插入（参考eodhd_fundamentals）
        from psycopg2.extras import execute_values
        
        api_tag_data = [(api_tag,) for api_tag in api_tags]
        sql = """
            INSERT INTO news_auto_tags (tag_name) 
            VALUES %s
            ON CONFLICT (tag_name) DO NOTHING
        """
        execute_values(cursor, sql, api_tag_data, page_size=1000)
    
    def _batch_create_symbol_relations(self, cursor, relations: List[tuple]):
        """批量创建新闻-symbols关联（使用execute_values优化）"""
        if not relations:
            return
        
        # 首先获取所有symbol的ID
        symbols = list(set([rel[1] for rel in relations]))
        placeholders = ','.join(['%s'] * len(symbols))
        query = f"SELECT id, symbol_code FROM news_symbols_dict WHERE symbol_code IN ({placeholders})"
        cursor.execute(query, symbols)
        
        symbol_id_map = {row[1]: row[0] for row in cursor.fetchall()}
        
        # 准备关联数据
        relation_data = []
        for news_id, symbol in relations:
            symbol_id = symbol_id_map.get(symbol)
            if symbol_id:
                relation_data.append((news_id, symbol_id))
        
        if relation_data:
            # 🚀 使用 execute_values 批量插入（参考eodhd_fundamentals）
            from psycopg2.extras import execute_values
            
            sql = """
                INSERT INTO news_symbols (news_id, symbol_id) 
                VALUES %s
                ON CONFLICT (news_id, symbol_id) DO NOTHING
            """
            execute_values(cursor, sql, relation_data, page_size=1000)
    
    def _batch_create_tag_relations(self, cursor, relations: List[tuple]):
        """批量创建新闻-tags关联（使用execute_values优化）"""
        if not relations:
            return
        
        # 首先获取所有tag的ID
        tags = list(set([rel[1] for rel in relations]))
        placeholders = ','.join(['%s'] * len(tags))
        query = f"SELECT id, tag_name FROM news_tags_dict WHERE tag_name IN ({placeholders})"
        cursor.execute(query, tags)
        
        tag_id_map = {row[1]: row[0] for row in cursor.fetchall()}
        
        # 准备关联数据
        relation_data = []
        for news_id, tag in relations:
            tag_id = tag_id_map.get(tag)
            if tag_id:
                relation_data.append((news_id, tag_id))
        
        if relation_data:
            # 🚀 使用 execute_values 批量插入（参考eodhd_fundamentals）
            from psycopg2.extras import execute_values
            
            sql = """
                INSERT INTO news_tags (news_id, tag_id) 
                VALUES %s
                ON CONFLICT (news_id, tag_id) DO NOTHING
            """
            execute_values(cursor, sql, relation_data, page_size=1000)
    
    def _batch_create_api_tag_relations(self, cursor, relations: List[tuple]):
        """批量创建新闻-API原始tags关联（使用execute_values优化）"""
        if not relations:
            return
        
        # 创建news_auto_tags_relations表（如果不存在）
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS news_auto_tags_relations (
                id SERIAL PRIMARY KEY,
                news_id INTEGER NOT NULL,
                auto_tag_id INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (auto_tag_id) REFERENCES news_auto_tags(id) ON DELETE CASCADE,
                UNIQUE(news_id, auto_tag_id)
            )
        """)
        
        # 首先获取所有API tag的ID
        api_tags = list(set([rel[1] for rel in relations]))
        placeholders = ','.join(['%s'] * len(api_tags))
        query = f"SELECT id, tag_name FROM news_auto_tags WHERE tag_name IN ({placeholders})"
        cursor.execute(query, api_tags)
        
        api_tag_id_map = {row[1]: row[0] for row in cursor.fetchall()}
        
        # 准备关联数据
        relation_data = []
        for news_id, api_tag in relations:
            api_tag_id = api_tag_id_map.get(api_tag)
            if api_tag_id:
                relation_data.append((news_id, api_tag_id))
        
        if relation_data:
            # 🚀 使用 execute_values 批量插入（参考eodhd_fundamentals）
            from psycopg2.extras import execute_values
            
            sql = """
                INSERT INTO news_auto_tags_relations (news_id, auto_tag_id) 
                VALUES %s
                ON CONFLICT (news_id, auto_tag_id) DO NOTHING
            """
            execute_values(cursor, sql, relation_data, page_size=1000)
    
    def upsert_news_batch(self, articles: List[Dict[str, Any]], scheduler_source: Optional[str] = None) -> Dict[str, int]:
        """
        高性能新闻批量 UPSERT 操作
        使用临时表和MERGE语法实现最高性能
        
        Args:
            articles: 新闻文章列表
            scheduler_source: 调度器来源类型 ('eodhd_news', 'watchlist_news', 等)
            
        Returns:
            Dict[str, int]: 操作结果统计
        """
        logger.info("开始高性能批量UPSERT新闻", articles_count=len(articles))
        
        if not articles:
            return {'success_count': 0, 'existing_count': 0, 'failed_count': 0}
        
        conn = self.db_manager.get_connection()
        if not conn:
            logger.error("数据库连接失败")
            return {'success_count': 0, 'existing_count': 0, 'failed_count': len(articles)}
        
        try:
            # 开始事务
            conn.autocommit = False
            
            with conn.cursor() as cursor:
                # 创建临时表
                cursor.execute("""
                    CREATE TEMP TABLE temp_news AS 
                    SELECT * FROM news LIMIT 0
                """)
                
                # 批量插入到临时表
                temp_data = []
                for article in articles:
                    temp_data.append((
                        article.get('date'),
                        article.get('title'),
                        article.get('content'),
                        article.get('link'),
                        article.get('source'),
                        article.get('sentiment_polarity'),
                        article.get('sentiment_neg'),
                        article.get('sentiment_neu'),
                        article.get('sentiment_pos'),
                        article.get('formatted_text'),
                        scheduler_source
                    ))
                
                temp_sql = """
                    INSERT INTO temp_news 
                    (date, title, content, link, source, sentiment_polarity, 
                     sentiment_neg, sentiment_neu, sentiment_pos, formatted_text, scheduler_source)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                cursor.executemany(temp_sql, temp_data)
                
                # 从临时表UPSERT到主表
                upsert_sql = """
                    INSERT INTO news 
                    (date, title, content, link, source, sentiment_polarity, 
                     sentiment_neg, sentiment_neu, sentiment_pos, formatted_text, scheduler_source)
                    SELECT DISTINCT t.date, t.title, t.content, t.link, t.source,
                           t.sentiment_polarity, t.sentiment_neg, t.sentiment_neu, 
                           t.sentiment_pos, t.formatted_text, t.scheduler_source
                    FROM temp_news t
                    WHERE NOT EXISTS (
                        SELECT 1 FROM news n WHERE n.link = t.link AND t.link IS NOT NULL
                    )
                """
                cursor.execute(upsert_sql)
                new_count = cursor.rowcount
                
                # 计算已存在的数量
                total_count = len(articles)
                existing_count = total_count - new_count
                
                # 提交事务
                conn.commit()
                
                result = {
                    'success_count': new_count,
                    'existing_count': existing_count,
                    'failed_count': 0
                }
                
                logger.info("高性能批量UPSERT新闻完成", **result)
                return result
                
        except Exception as e:
            # 回滚事务
            conn.rollback()
            logger.error("高性能批量UPSERT新闻失败，事务回滚", exception=str(e))
            return {
                'success_count': 0,
                'existing_count': 0,
                'failed_count': len(articles)
            }
        finally:
            conn.close()
            logger.debug("数据库连接已关闭")
    
    def _insert_symbol(self, cursor, news_id: int, symbol: str):
        """插入symbol并建立关系"""
        # 获取或创建symbol
        cursor.execute("""
            INSERT INTO news_symbols_dict (symbol_code) 
            VALUES (%s) 
            ON CONFLICT (symbol_code) DO NOTHING 
            RETURNING id
        """, (symbol,))
        
        result = cursor.fetchone()
        if result:
            symbol_id = result[0]
        else:
            # 如果没有插入（已存在），获取现有ID
            cursor.execute("SELECT id FROM news_symbols_dict WHERE symbol_code = %s", (symbol,))
            symbol_id = cursor.fetchone()[0]
        
        # 创建关联
        cursor.execute("""
            INSERT INTO news_symbols (news_id, symbol_id) 
            VALUES (%s, %s) 
            ON CONFLICT (news_id, symbol_id) DO NOTHING
        """, (news_id, symbol_id))
    
    def _insert_tag(self, cursor, news_id: int, tag: str):
        """插入tag并建立关系"""
        # 获取或创建tag
        cursor.execute("""
            INSERT INTO news_tags_dict (tag_name) 
            VALUES (%s) 
            ON CONFLICT (tag_name) DO NOTHING 
            RETURNING id
        """, (tag,))
        
        result = cursor.fetchone()
        if result:
            tag_id = result[0]
        else:
            # 如果没有插入（已存在），获取现有ID
            cursor.execute("SELECT id FROM news_tags_dict WHERE tag_name = %s", (tag,))
            tag_id = cursor.fetchone()[0]
        
        # 创建关联
        cursor.execute("""
            INSERT INTO news_tags (news_id, tag_id) 
            VALUES (%s, %s) 
            ON CONFLICT (news_id, tag_id) DO NOTHING
        """, (news_id, tag_id))
    
    def _insert_api_tag(self, cursor, news_id: int, api_tag: str):
        """插入API原始tag并建立关系"""
        # 获取或创建API tag
        cursor.execute("""
            INSERT INTO news_auto_tags (tag_name) 
            VALUES (%s) 
            ON CONFLICT (tag_name) DO NOTHING 
            RETURNING id
        """, (api_tag,))
        
        result = cursor.fetchone()
        if result:
            api_tag_id = result[0]
        else:
            # 如果没有插入（已存在），获取现有ID
            cursor.execute("SELECT id FROM news_auto_tags WHERE tag_name = %s", (api_tag,))
            api_tag_id = cursor.fetchone()[0]
        
        # 创建关联
        cursor.execute("""
            INSERT INTO news_auto_tags_relations (news_id, auto_tag_id) 
            VALUES (%s, %s) 
            ON CONFLICT (news_id, auto_tag_id) DO NOTHING
        """, (news_id, api_tag_id))


class NewsThemeService:
    """新闻主题管理服务"""
    
    def __init__(self):
        """初始化主题管理服务"""
        from backend.utils.database_config import get_database_config
        self.db_manager = DatabaseManager(get_database_config())
    
    def get_all_themes(self) -> List[Dict[str, Any]]:
        """获取所有主题及其关键词"""
        conn = self.db_manager.get_connection()
        if not conn:
            return []
        
        try:
            cursor = conn.cursor()
            query = """
                SELECT t.id, t.topic_name, t.description, t.is_active, t.color, t.created_at,
                       STRING_AGG(tk.keyword, ',') as keywords
                FROM news_topics t
                LEFT JOIN news_topic_keywords tk ON t.id = tk.topic_id
                GROUP BY t.id, t.topic_name, t.description, t.is_active, t.color, t.created_at
                ORDER BY t.topic_name
            """
            
            cursor.execute(query)
            rows = cursor.fetchall()
            
            themes = []
            for row in rows:
                theme = {
                    'id': row[0],
                    'topic_name': row[1],
                    'description': row[2],
                    'is_active': row[3],
                    'color': row[4],
                    'created_at': row[5].isoformat() if row[5] else None,
                    'keywords': row[6].split(',') if row[6] else []
                }
                themes.append(theme)
            
            cursor.close()
            conn.close()
            return themes
            
        except Exception as e:
            print(f"获取主题列表失败: {str(e)}")
            return []
    
    def create_theme(self, topic_name: str, description: str = None, 
                    keywords: List[str] = None, color: str = None) -> Optional[int]:
        """创建新的主题"""
        conn = self.db_manager.get_connection()
        if not conn:
            return None
        
        try:
            cursor = conn.cursor()
            
            # 插入主题
            cursor.execute("""
                INSERT INTO news_topics (topic_name, description, color)
                VALUES (%s, %s, %s)
                RETURNING id
            """, (topic_name, description, color))
            
            topic_id = cursor.fetchone()[0]
            
            # 添加关键词
            if keywords:
                for keyword in keywords:
                    cursor.execute("""
                        INSERT INTO news_topic_keywords (topic_id, keyword, weight)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (topic_id, keyword) DO NOTHING
                    """, (topic_id, keyword, 1.0))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            return topic_id
            
        except Exception as e:
            conn.rollback()
            cursor.close()
            conn.close()
            print(f"创建主题失败: {str(e)}")
            return None
    
    def update_theme(self, theme_id: int, topic_name: str = None, 
                    description: str = None, is_active: bool = None,
                    color: str = None) -> bool:
        """更新主题信息"""
        conn = self.db_manager.get_connection()
        if not conn:
            return False
        
        try:
            cursor = conn.cursor()
            
            update_fields = []
            params = []
            
            if topic_name is not None:
                update_fields.append("topic_name = %s")
                params.append(topic_name)
            
            if description is not None:
                update_fields.append("description = %s")
                params.append(description)
            
            if is_active is not None:
                update_fields.append("is_active = %s")
                params.append(is_active)
            
            if color is not None:
                update_fields.append("color = %s")
                params.append(color)
            
            if not update_fields:
                return True
            
            update_fields.append("updated_at = CURRENT_TIMESTAMP")
            params.append(theme_id)
            
            sql = f"UPDATE news_topics SET {', '.join(update_fields)} WHERE id = %s"
            cursor.execute(sql, params)
            
            affected_rows = cursor.rowcount
            conn.commit()
            cursor.close()
            conn.close()
            
            return affected_rows > 0
            
        except Exception as e:
            conn.rollback()
            cursor.close()
            conn.close()
            print(f"更新主题失败: {str(e)}")
            return False
    
    def delete_theme(self, theme_id: int) -> bool:
        """删除主题"""
        conn = self.db_manager.get_connection()
        if not conn:
            return False
        
        try:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM news_topics WHERE id = %s", (theme_id,))
            
            affected_rows = cursor.rowcount
            conn.commit()
            cursor.close()
            conn.close()
            
            return affected_rows > 0
            
        except Exception as e:
            conn.rollback()
            cursor.close()
            conn.close()
            print(f"删除主题失败: {str(e)}")
            return False
    
    def add_theme_keywords(self, theme_id: int, keywords: List[str], 
                          weights: List[float] = None) -> bool:
        """为主题添加关键词"""
        conn = self.db_manager.get_connection()
        if not conn:
            return False
        
        try:
            cursor = conn.cursor()
            
            if weights is None:
                weights = [1.0] * len(keywords)
            elif len(weights) != len(keywords):
                weights = [1.0] * len(keywords)
            
            for keyword, weight in zip(keywords, weights):
                cursor.execute("""
                    INSERT INTO news_topic_keywords (topic_id, keyword, weight)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (topic_id, keyword) DO NOTHING
                """, (theme_id, keyword, weight))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            return True
            
        except Exception as e:
            conn.rollback()
            cursor.close()
            conn.close()
            print(f"添加主题关键词失败: {str(e)}")
            return False
    
    def remove_theme_keywords(self, theme_id: int, keywords: List[str]) -> bool:
        """从主题中删除关键词"""
        conn = self.db_manager.get_connection()
        if not conn:
            return False
        
        try:
            cursor = conn.cursor()
            
            placeholders = ','.join(['%s' for _ in keywords])
            params = [theme_id] + keywords
            
            cursor.execute(f"""
                DELETE FROM news_topic_keywords 
                WHERE topic_id = %s AND keyword IN ({placeholders})
            """, params)
            
            conn.commit()
            cursor.close()
            conn.close()
            
            return True
            
        except Exception as e:
            conn.rollback()
            cursor.close()
            conn.close()
            print(f"删除主题关键词失败: {str(e)}")
            return False

