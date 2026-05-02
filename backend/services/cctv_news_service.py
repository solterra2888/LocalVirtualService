# -*- coding: utf-8 -*-
"""
CCTV新闻爬取与存储服务
从CCTV新闻联播网站爬取新闻内容并存储到数据库
"""

import requests
import time
import random
import re
from datetime import datetime
from backend.utils.datetime_utils import utcnow
from typing import Optional, List, Dict, Any
from backend.database.db_manager import DatabaseManager
from backend.utils.logger import get_logger

# BeautifulSoup延迟导入
try:
    from bs4 import BeautifulSoup
    BS4_AVAILABLE = True
except ImportError:
    BS4_AVAILABLE = False

logger = get_logger("cctv_news_service")


class CCTVNewsService:
    """CCTV新闻爬取与存储服务类"""
    
    # CCTV新闻联播基础URL
    BASE_URL = 'https://tv.cctv.com/lm/xwlb/index.shtml'
    
    # 用户代理池
    USER_AGENTS = [
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
    ]
    
    def __init__(self):
        """初始化服务"""
        from backend.utils.database_config import get_database_config
        self.db_manager = DatabaseManager(get_database_config())
        self.logger = get_logger("cctv_news_service")
        self.session = requests.Session()
        
        if not BS4_AVAILABLE:
            self.logger.error("BeautifulSoup未安装，无法进行网页解析")
            raise ImportError("BeautifulSoup未安装，请运行: pip install beautifulsoup4")
        
        self.logger.info("CCTV新闻服务初始化完成")
    
    def fetch_and_store_news(
        self,
        date: Optional[str] = None,
        max_news: Optional[int] = None,
        scheduler_source: str = "cctv_news",
        auto_analyze: bool = True,
        analysis_templates: Optional[List[str]] = None,
        max_concurrent: int = 5,
        enable_theme_matching: bool = True
    ) -> Dict[str, Any]:
        """
        爬取CCTV新闻并存储到数据库
        
        Args:
            date: 日期字符串，格式为 YYYYMMDD，None表示最新
            max_news: 最大爬取新闻数量，None表示全部
            scheduler_source: 调度器来源标识
            auto_analyze: 是否自动触发LLM分析（默认True）
            analysis_templates: 分析使用的Prompt模板代码列表（默认["news_cctv_summary_v1", "news_classification"]）
            max_concurrent: 最大并发分析数（默认5）
            enable_theme_matching: 是否启用主题匹配（默认True）
        
        Returns:
            Dict[str, Any]: 执行结果
            {
                'success': bool,
                'message': str,
                'data': {
                    'fetched_count': int,
                    'stored_count': int,
                    'new_count': int,
                    'existing_count': int,
                    'failed_count': int,
                    'analysis_triggered': bool,
                    'celery_tasks': List[Dict] (如果触发了分析)
                }
            }
        """
        try:
            self.logger.info("开始执行CCTV新闻爬取任务", 
                           date=date, 
                           max_news=max_news,
                           scheduler_source=scheduler_source)
            
            # 步骤1: 爬取新闻列表
            self.logger.info("步骤1: 爬取新闻列表")
            news_list = self._scrape_news_list(date=date, max_news=max_news)
            
            if not news_list:
                self.logger.warning("未爬取到任何新闻")
                return {
                    'success': False,
                    'message': '未爬取到任何新闻',
                    'data': {
                        'fetched_count': 0,
                        'stored_count': 0,
                        'existing_count': 0,
                        'failed_count': 0
                    }
                }
            
            fetched_count = len(news_list)
            self.logger.info(f"成功爬取 {fetched_count} 条新闻标题")
            
            # 步骤2: 提取每条新闻的内容
            self.logger.info("步骤2: 提取新闻内容")
            success_content_count = 0
            failed_content_count = 0
            
            for idx, news in enumerate(news_list):
                self.logger.debug(f"提取内容 [{idx+1}/{fetched_count}]: {news['title'][:50]}...")
                
                content = self._extract_content_from_page(news['url'])
                
                if content:
                    news['content'] = content
                    news['content_scraped_at'] = utcnow().isoformat()  # UTC ISO
                    success_content_count += 1
                else:
                    news['content'] = None
                    news['content_scraped_at'] = utcnow().isoformat()  # UTC ISO
                    failed_content_count += 1
                    self.logger.warning(f"内容提取失败: {news['url']}")
                
                # 智能延迟（除了最后一条）
                if idx < fetched_count - 1:
                    self._smart_delay(1.0, 2.0)
            
            self.logger.info(f"内容提取完成: 成功 {success_content_count}, 失败 {failed_content_count}")
            
            # 步骤3: 处理新闻数据
            self.logger.info("步骤3: 处理新闻数据以适配数据库")
            processed_articles = []
            
            for news in news_list:
                try:
                    processed = self._process_article_for_db(news, scheduler_source)
                    processed_articles.append(processed)
                except Exception as e:
                    self.logger.error(f"处理新闻失败: {news.get('title', 'Unknown')}", 
                                    exception=str(e))
                    continue
            
            self.logger.info(f"成功处理 {len(processed_articles)} 条新闻")
            
            # 步骤4: 批量存储到数据库
            self.logger.info("步骤4: 批量存储新闻到数据库")
            store_result = self._batch_insert_news(processed_articles, scheduler_source)
            
            # 步骤5: 触发LLM分析任务（如果启用）
            analysis_triggered = False
            celery_tasks = []
            
            if auto_analyze and store_result.get('new_news_ids'):
                new_news_ids = store_result['new_news_ids']
                
                # 设置默认模板列表
                templates = analysis_templates if analysis_templates else ["news_cctv_summary_v1", "news_classification"]
                
                self.logger.info(f"步骤5: 触发Celery分析任务，分析 {len(new_news_ids)} 条新增CCTV新闻，使用 {len(templates)} 个模板")
                
                try:
                    from backend.tasks.news_tasks import analyze_news_by_ids_task
                    
                    # 🔥 多模板并行：为每个模板提交独立的Celery任务
                    for template_code in templates:
                        celery_task = analyze_news_by_ids_task.delay(
                            news_ids=new_news_ids,
                            template_code=template_code,
                            max_concurrent=max_concurrent,
                            enable_theme_matching=enable_theme_matching
                        )
                        
                        celery_tasks.append({
                            'template': template_code,
                            'task_id': celery_task.id,
                            'news_count': len(new_news_ids)
                        })
                        
                        self.logger.info(f"✅ Celery分析任务已触发: task_id={celery_task.id}, "
                                       f"模板={template_code}, 新闻数={len(new_news_ids)}")
                    
                    analysis_triggered = True
                    
                except Exception as e:
                    self.logger.error(f"触发Celery分析任务失败: {str(e)}", exception=str(e))
                    # 不影响主流程，继续返回结果
            else:
                if not auto_analyze:
                    self.logger.info("步骤5: 跳过分析（auto_analyze=False）")
                else:
                    self.logger.info("步骤5: 无新增新闻，跳过分析")
            
            # 构建返回结果
            message_parts = [
                f"CCTV新闻爬取成功: 爬取 {fetched_count} 条",
                f"新增 {store_result['new_count']} 条",
                f"已存在 {store_result['existing_count']} 条"
            ]
            
            if analysis_triggered:
                templates_count = len(celery_tasks)
                message_parts.append(f"已触发 {templates_count} 个模板的LLM分析任务")
            
            result = {
                'success': True,
                'message': ", ".join(message_parts),
                'data': {
                    'fetched_count': fetched_count,
                    'stored_count': store_result['success_count'],
                    'new_count': store_result['new_count'],
                    'existing_count': store_result['existing_count'],
                    'failed_count': store_result['failed_count'],
                    'analysis_triggered': analysis_triggered,
                    'celery_tasks': celery_tasks
                }
            }
            
            self.logger.info("CCTV新闻爬取任务执行完成", 
                           fetched=fetched_count,
                           stored=store_result['success_count'],
                           new=store_result['new_count'],
                           existing=store_result['existing_count'],
                           failed=store_result['failed_count'],
                           analysis_triggered=analysis_triggered,
                           templates_count=len(celery_tasks))
            
            return result
            
        except Exception as e:
            error_msg = f"CCTV新闻爬取任务执行失败: {str(e)}"
            self.logger.error(error_msg, exception=str(e))
            return {
                'success': False,
                'message': error_msg,
                'data': {
                    'fetched_count': 0,
                    'stored_count': 0,
                    'new_count': 0,
                    'existing_count': 0,
                    'failed_count': 0
                }
            }
    
    def _scrape_news_list(
        self,
        date: Optional[str] = None,
        max_news: Optional[int] = None
    ) -> List[Dict]:
        """
        爬取新闻列表
        
        Args:
            date: 日期字符串，格式为 YYYYMMDD
            max_news: 最大爬取新闻数量
        
        Returns:
            List[Dict]: 新闻列表
        """
        try:
            # 构建URL
            if date:
                url = f"{self.BASE_URL}?date={date}"
            else:
                url = self.BASE_URL
            
            self.logger.info(f"开始爬取新闻列表: {url}")
            
            # 发送请求
            response = self._make_request_with_retry(url)
            
            if not response:
                self.logger.error("无法获取页面")
                return []
            
            # 解析页面
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # 查找新闻链接
            all_links = soup.find_all('a', href=True)
            seen_urls = set()
            news_items = []
            
            for link in all_links:
                href = link.get('href', '')
                text = link.get_text(strip=True)
                
                # 处理相对URL
                if href.startswith('/'):
                    href = 'https://tv.cctv.com' + href
                elif href.startswith('./'):
                    href = 'https://tv.cctv.com/lm/xwlb/' + href[2:]
                
                # 过滤出新闻详情页链接
                if 'VIDE' in href.upper() and 'tv.cctv.com' in href and text:
                    if href not in seen_urls:
                        seen_urls.add(href)
                        news_items.append({
                            'url': href,
                            'title': text
                        })
            
            self.logger.info(f"找到 {len(news_items)} 个新闻链接")
            
            # 限制数量
            if max_news and len(news_items) > max_news:
                news_items = news_items[:max_news]
                self.logger.info(f"限制数量为: {max_news}")
            
            # 提取新闻信息
            news_list = []
            for item in news_items:
                title = item['title']
                url = item['url']
                
                # 清理标题
                title = re.sub(r'\s+', ' ', title).strip()
                title = re.sub(r'^完整版\s*', '', title)
                title = re.sub(r'^_完整版_\s*', '', title)
                title = re.sub(r'^\[视频\]', '', title).strip()
                
                if not title:
                    continue
                
                # 从URL中提取日期
                date_match = re.search(r'/(\d{4})/(\d{2})/(\d{2})/', url)
                news_date = None
                if date_match:
                    year, month, day = date_match.groups()
                    news_date = f"{year}-{month}-{day}"
                
                news_list.append({
                    'title': title,
                    'url': url,
                    'date': news_date,
                    'scraped_at': utcnow().isoformat()  # UTC ISO
                })
            
            self.logger.info(f"成功提取 {len(news_list)} 条新闻")
            return news_list
            
        except Exception as e:
            self.logger.error(f"爬取新闻列表失败: {str(e)}")
            return []
    
    def _extract_content_from_page(self, url: str) -> Optional[str]:
        """
        从新闻详情页提取文本内容
        
        Args:
            url: 新闻详情页URL
        
        Returns:
            提取的文本内容，如果失败返回None
        """
        try:
            response = self._make_request_with_retry(url)
            
            if not response:
                return None
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # 方法1: 查找id为content_area的div（主要方法）
            content_area = soup.find('div', id='content_area')
            if content_area:
                paragraphs = []
                for p in content_area.find_all('p'):
                    text = p.get_text(strip=True)
                    if text:
                        paragraphs.append(text)
                
                if paragraphs:
                    content = '\n'.join(paragraphs)
                    return content
            
            # 方法2: 查找class包含content的div
            content_divs = soup.find_all('div', class_=lambda x: x and 'content' in x.lower())
            for div in content_divs:
                paragraphs = []
                for p in div.find_all('p'):
                    text = p.get_text(strip=True)
                    if text:
                        paragraphs.append(text)
                if paragraphs and len(paragraphs) > 2:
                    content = '\n'.join(paragraphs)
                    return content
            
            # 方法3: 查找主要内容区域
            main_content = soup.find('div', class_=lambda x: x and ('main' in x.lower() or 'article' in x.lower()))
            if main_content:
                text = main_content.get_text(separator='\n', strip=True)
                if len(text) > 100:
                    return text
            
            return None
            
        except Exception as e:
            self.logger.error(f"提取内容失败: {url}", exception=str(e))
            return None
    
    def _process_article_for_db(
        self,
        article: Dict[str, Any],
        scheduler_source: str
    ) -> Dict[str, Any]:
        """
        处理新闻数据以适配数据库格式
        
        Args:
            article: 原始新闻数据
            scheduler_source: 调度器来源
        
        Returns:
            Dict[str, Any]: 处理后的新闻数据
        """
        # 生成格式化文本（用于LLM分析）
        formatted_text = self._generate_formatted_text(article)
        
        # 构建数据库记录
        processed = {
            'date': article.get('date'),
            'title': article.get('title'),
            'content': article.get('content'),
            'link': article.get('url'),
            'source': 'CCTV',
            'sentiment_polarity': None,  # CCTV新闻无情感分析数据
            'sentiment_neg': None,
            'sentiment_neu': None,
            'sentiment_pos': None,
            'formatted_text': formatted_text,
            'scheduler_source': scheduler_source
        }
        
        return processed
    
    def _generate_formatted_text(self, article: Dict[str, Any]) -> str:
        """
        生成格式化文本（用于LLM分析）
        
        Args:
            article: 新闻数据
        
        Returns:
            str: 格式化文本
        """
        formatted_parts = []
        
        # 标题
        title = article.get('title')
        if title:
            formatted_parts.append(f"**标题:** {title}")
        
        # 日期
        date = article.get('date')
        if date:
            formatted_parts.append(f"**日期:** {date}")
        
        # 来源
        formatted_parts.append(f"**来源:** CCTV新闻联播")
        
        # 链接
        link = article.get('url')
        if link:
            formatted_parts.append(f"**链接:** {link}")
        
        # 内容
        content = article.get('content')
        if content:
            formatted_parts.append(f"\n**内容:**\n{content}")
        
        return '\n'.join(formatted_parts)
    
    def _batch_insert_news(
        self,
        articles: List[Dict[str, Any]],
        scheduler_source: str
    ) -> Dict[str, Any]:
        """
        批量插入新闻到数据库
        
        Args:
            articles: 处理后的新闻列表
            scheduler_source: 调度器来源
        
        Returns:
            Dict[str, Any]: 插入结果统计
        """
        if not articles:
            return {
                'success_count': 0,
                'new_count': 0,
                'existing_count': 0,
                'failed_count': 0
            }
        
        try:
            batch_size = 100  # 每批100条
            total_success = 0
            total_new = 0
            total_existing = 0
            total_failed = 0
            all_new_news_ids = []  # 收集所有新插入的新闻ID
            
            total_batches = (len(articles) + batch_size - 1) // batch_size
            
            for i in range(0, len(articles), batch_size):
                batch = articles[i:i + batch_size]
                batch_num = i // batch_size + 1
                
                self.logger.info(f"处理第 {batch_num}/{total_batches} 批新闻", batch_size=len(batch))
                
                try:
                    # 使用连接上下文管理器
                    with self.db_manager.get_connection_context() as conn:
                        with conn.cursor() as cursor:
                            # 批量插入/更新新闻
                            success_count, new_count, existing_count, failed_count, new_ids = self._batch_insert_articles(
                                cursor, batch, scheduler_source
                            )
                            
                            # 收集新插入的新闻ID
                            if new_ids:
                                all_new_news_ids.extend(new_ids)
                            
                            self.logger.info(f"第 {batch_num} 批事务提交成功", 
                                          success=success_count,
                                          new=new_count,
                                          existing=existing_count,
                                          failed=failed_count,
                                          new_ids_count=len(new_ids) if new_ids else 0)
                            
                            total_success += success_count
                            total_new += new_count
                            total_existing += existing_count
                            total_failed += failed_count
                            
                except Exception as e:
                    self.logger.error(f"批次 {batch_num} 处理失败", exception=str(e))
                    total_failed += len(batch)
                    continue
            
            result = {
                'success_count': total_success,
                'new_count': total_new,
                'existing_count': total_existing,
                'failed_count': total_failed,
                'new_news_ids': all_new_news_ids  # 返回所有新插入的新闻ID
            }
            
            self.logger.info("批量插入新闻完成",
                           success_count=total_success,
                           new_count=total_new,
                           existing_count=total_existing,
                           failed_count=total_failed)
            
            return result
            
        except Exception as e:
            self.logger.error("批量插入新闻失败", exception=str(e))
            return {
                'success_count': 0,
                'new_count': 0,
                'existing_count': 0,
                'failed_count': len(articles),
                'new_news_ids': []
            }
    
    def _batch_insert_articles(
        self,
        cursor,
        articles: List[Dict[str, Any]],
        scheduler_source: str
    ) -> tuple:
        """
        批量插入新闻文章（使用execute_values高性能批量插入）
        
        Args:
            cursor: 数据库游标
            articles: 新闻列表
            scheduler_source: 调度器来源
        
        Returns:
            tuple: (success_count, new_count, existing_count, failed_count, new_news_ids)
        """
        if not articles:
            return 0, 0, 0, 0, []
        
        # 去重：同一批次中的重复link
        seen_links = set()
        unique_articles = []
        
        for article in articles:
            link = article.get('link')
            if link:
                if link not in seen_links:
                    seen_links.add(link)
                    unique_articles.append(article)
            else:
                unique_articles.append(article)
        
        # 插入前检查已存在的新闻
        links_to_insert = [article.get('link') for article in unique_articles if article.get('link')]
        existing_links_before = set()
        
        if links_to_insert:
            placeholders = ','.join(['%s'] * len(links_to_insert))
            check_sql = f"SELECT link FROM news WHERE link IN ({placeholders})"
            cursor.execute(check_sql, links_to_insert)
            existing_links_before = {row[0] for row in cursor.fetchall()}
        
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
            # 使用 execute_values 进行高性能批量UPSERT
            from psycopg2.extras import execute_values
            
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
            
            execute_values(
                cursor,
                upsert_sql,
                news_data,
                template=None,
                page_size=1000
            )
            
            # 获取新插入的新闻ID（查询刚插入的新闻）
            new_news_ids = []
            new_links = [link for link in links_to_insert if link not in existing_links_before]
            
            if new_links:
                placeholders = ','.join(['%s'] * len(new_links))
                id_query = f"""
                    SELECT id FROM news 
                    WHERE link IN ({placeholders})
                    ORDER BY id DESC
                """
                cursor.execute(id_query, new_links)
                new_news_ids = [row[0] for row in cursor.fetchall()]
            
            # 计算新增和已存在的数量
            new_count = len(new_links)
            existing_count = len(existing_links_before)
            success_count = len(unique_articles)
            failed_count = 0
            
            return success_count, new_count, existing_count, failed_count, new_news_ids
            
        except Exception as e:
            self.logger.error(f"批量插入失败: {str(e)}")
            return 0, 0, 0, len(articles), []
    
    def _make_request_with_retry(
        self,
        url: str,
        max_retries: int = 3,
        timeout: int = 30
    ) -> Optional[requests.Response]:
        """
        带重试机制的请求函数
        
        Args:
            url: 请求URL
            max_retries: 最大重试次数
            timeout: 超时时间（秒）
        
        Returns:
            requests.Response or None
        """
        for attempt in range(max_retries):
            try:
                headers = self._get_random_headers()
                response = self.session.get(url, headers=headers, timeout=timeout)
                response.encoding = response.apparent_encoding or 'utf-8'
                response.raise_for_status()
                return response
                
            except requests.exceptions.Timeout:
                if attempt < max_retries - 1:
                    self.logger.warning(f"请求超时，重试 {attempt + 1}/{max_retries}...")
                    self._smart_delay(2.0, 4.0)
                else:
                    self.logger.error(f"请求超时（所有重试失败）: {url}")
                    
            except requests.exceptions.RequestException as e:
                if attempt < max_retries - 1:
                    self.logger.warning(f"请求错误: {e}，重试 {attempt + 1}/{max_retries}...")
                    self._smart_delay(2.0, 4.0)
                else:
                    self.logger.error(f"请求失败: {e}")
        
        return None
    
    def _get_random_headers(self) -> Dict[str, str]:
        """生成随机请求头"""
        return {
            'User-Agent': random.choice(self.USER_AGENTS),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Cache-Control': 'max-age=0',
        }
    
    def _smart_delay(self, base_min: float = 1.0, base_max: float = 2.0):
        """智能延迟"""
        delay = random.uniform(base_min, base_max)
        time.sleep(delay)
    
    def __del__(self):
        """清理资源"""
        if hasattr(self, 'session'):
            self.session.close()


# 创建服务实例的工厂函数
def create_cctv_news_service() -> CCTVNewsService:
    """创建CCTV新闻服务实例"""
    return CCTVNewsService()

