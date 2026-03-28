# -*- coding: utf-8 -*-
"""
Twitter订阅服务 - 基于用户订阅列表获取Twitter推文
"""

import os
import uuid
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import time
import psycopg2
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional, Tuple
from collections import defaultdict

from backend.database.db_manager import DatabaseManager
from backend.utils.logger import get_logger
from backend.services.subscription_content_service import SubscriptionContentService

logger = get_logger("twitter_subscription_service")


class TwitterSubscriptionService:
    """Twitter订阅服务类"""
    
    def __init__(self):
        """初始化Twitter订阅服务"""
        logger.info("初始化Twitter订阅服务")
        
        try:
            # 获取标准化数据库连接配置
            from backend.utils.database_config import get_database_config
            db_config = get_database_config()
            self.db_manager = DatabaseManager(db_config)
            
            # Twitter API配置
            self.api_key = os.getenv('TWITTER_API_KEY', 'new1_958a42497f844e1e9dd1b866cb54a9d9')
            self.api_base_url = "https://api.twitterapi.io/twitter/tweet/advanced_search"
            
            # 创建带重试机制的session
            self.session = self._create_session_with_retries()
            
            # 初始化内容服务（使用适配器模式）
            self.content_service = SubscriptionContentService()
            
            # 配置参数
            self.default_query_hours = int(os.getenv('TWITTER_DEFAULT_QUERY_HOURS', '24'))
            self.test_fetch_query_hours = 12  # Test Fetch 固定使用12小时
            self.initial_fetch_hours = int(os.getenv('TWITTER_INITIAL_FETCH_HOURS', '168'))  # 首次获取：7天（168小时）
            self.max_tweets_per_request = int(os.getenv('TWITTER_MAX_TWEETS_PER_REQUEST', '20'))
            
            logger.info("Twitter订阅服务初始化完成")
            
        except Exception as e:
            logger.error("Twitter订阅服务初始化失败", exception=str(e))
            raise
    
    def _create_session_with_retries(self) -> requests.Session:
        """创建带有重试机制的requests session"""
        session = requests.Session()
        
        retry_strategy = Retry(
            total=5,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        return session
    
    def validate_twitter_user(self, username: str) -> Dict[str, Any]:
        """
        验证 Twitter 用户是否存在
        
        Args:
            username: Twitter用户名
            
        Returns:
            dict: {
                'exists': bool,
                'user_info': dict or None,
                'error': str or None
            }
        """
        logger.info(f"验证Twitter用户是否存在 | username={username}")
        
        try:
            url = f"https://api.twitterapi.io/twitter/user/info?userName={username}"
            headers = {"X-API-Key": self.api_key}
            
            response = self.session.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            # 检查API返回的状态
            if data.get('status') == 'error':
                logger.warning(f"Twitter用户不存在 | username={username} | msg={data.get('msg')}")
                return {
                    'exists': False,
                    'user_info': None,
                    'error': data.get('msg', 'User not found')
                }
            
            # 用户存在
            user_info = data.get('data', {})
            logger.info(f"Twitter用户验证成功 | username={username} | name={user_info.get('name')} | id={user_info.get('id')}")
            
            return {
                'exists': True,
                'user_info': user_info,
                'error': None
            }
            
        except requests.exceptions.Timeout:
            error_msg = "Twitter API请求超时"
            logger.error(f"{error_msg} | username={username}")
            return {
                'exists': False,
                'user_info': None,
                'error': error_msg
            }
        except requests.exceptions.RequestException as e:
            error_msg = f"Twitter API请求失败: {str(e)}"
            logger.error(f"{error_msg} | username={username}")
            return {
                'exists': False,
                'user_info': None,
                'error': error_msg
            }
        except Exception as e:
            error_msg = f"验证Twitter用户时发生错误: {str(e)}"
            logger.error(f"{error_msg} | username={username}")
            return {
                'exists': False,
                'user_info': None,
                'error': error_msg
            }
    
    def get_all_twitter_subscriptions(self, enabled_only: bool = True) -> List[Dict[str, Any]]:
        """
        获取所有Twitter订阅
        
        Args:
            enabled_only: 是否只获取启用的订阅
            
        Returns:
            订阅列表
        """
        logger.info("获取所有Twitter订阅", enabled_only=enabled_only)
        
        try:
            conn = self.db_manager.get_connection()
            if not conn:
                logger.error("数据库连接失败")
                return []
            
            cursor = conn.cursor()
            
            query = """
                SELECT 
                    id, user_id, channel_identifier, channel_name,
                    priority, fetch_interval_minutes, max_items_per_fetch,
                    last_fetched_at, filter_config, metadata,
                    total_items_fetched, consecutive_failures
                FROM subscription_list
                WHERE channel_type = 'twitter'
            """
            
            if enabled_only:
                query += " AND enabled = true"
            
            query += " ORDER BY priority DESC, last_fetched_at ASC NULLS FIRST"
            
            cursor.execute(query)
            results = cursor.fetchall()
            
            subscriptions = []
            for row in results:
                subscriptions.append({
                    'id': row[0],
                    'user_id': row[1],
                    'channel_identifier': row[2],
                    'channel_name': row[3],
                    'priority': row[4],
                    'fetch_interval_minutes': row[5],
                    'max_items_per_fetch': row[6],
                    'last_fetched_at': row[7],
                    'filter_config': row[8] or {},
                    'metadata': row[9] or {},
                    'total_items_fetched': row[10] or 0,
                    'consecutive_failures': row[11] or 0
                })
            
            cursor.close()
            conn.close()
            
            logger.info("成功获取Twitter订阅", subscription_count=len(subscriptions))
            return subscriptions
            
        except Exception as e:
            logger.error("获取Twitter订阅失败", exception=str(e))
            return []
    
    def get_user_twitter_subscriptions(self, user_id: int, enabled_only: bool = True) -> List[Dict[str, Any]]:
        """
        获取指定用户的Twitter订阅
        
        Args:
            user_id: 用户ID
            enabled_only: 是否只获取启用的订阅
            
        Returns:
            订阅列表
        """
        logger.info("获取用户Twitter订阅", user_id=user_id, enabled_only=enabled_only)
        
        try:
            conn = self.db_manager.get_connection()
            if not conn:
                logger.error("数据库连接失败")
                return []
            
            cursor = conn.cursor()
            
            query = """
                SELECT 
                    id, user_id, channel_identifier, channel_name,
                    priority, fetch_interval_minutes, max_items_per_fetch,
                    last_fetched_at, filter_config, metadata
                FROM subscription_list
                WHERE channel_type = 'twitter' AND user_id = %s
            """
            
            if enabled_only:
                query += " AND enabled = true"
            
            query += " ORDER BY priority DESC, channel_name"
            
            cursor.execute(query, [user_id])
            results = cursor.fetchall()
            
            subscriptions = []
            for row in results:
                subscriptions.append({
                    'id': row[0],
                    'user_id': row[1],
                    'channel_identifier': row[2],
                    'channel_name': row[3],
                    'priority': row[4],
                    'fetch_interval_minutes': row[5],
                    'max_items_per_fetch': row[6],
                    'last_fetched_at': row[7],
                    'filter_config': row[8] or {},
                    'metadata': row[9] or {}
                })
            
            cursor.close()
            conn.close()
            
            logger.info("成功获取用户Twitter订阅", user_id=user_id, subscription_count=len(subscriptions))
            return subscriptions
            
        except Exception as e:
            logger.error("获取用户Twitter订阅失败", user_id=user_id, exception=str(e))
            return []
    
    def aggregate_subscriptions_by_author(self, subscriptions: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """
        将订阅按作者聚合，避免重复调用API
        
        Args:
            subscriptions: 订阅列表
            
        Returns:
            {author: [subscription_dicts]} 字典
        """
        aggregated = defaultdict(list)
        
        for sub in subscriptions:
            author = sub['channel_identifier'].lstrip('@').lower()
            aggregated[author].append(sub)
        
        logger.info("订阅聚合完成", 
                   total_subscriptions=len(subscriptions),
                   unique_authors=len(aggregated))
        
        return dict(aggregated)
    
    def _calculate_fetch_since_hours(self, author_subs: List[Dict[str, Any]]) -> int:
        """
        根据订阅的 last_fetched_at 计算最优时间窗口，避免每次都拉取 24 小时的全量数据。
        """
        latest_fetch = None
        for sub in author_subs:
            lfa = sub.get('last_fetched_at')
            if lfa and (latest_fetch is None or lfa > latest_fetch):
                latest_fetch = lfa
        
        if latest_fetch is None:
            return self.default_query_hours
        
        if not isinstance(latest_fetch, datetime):
            return self.default_query_hours
        
        # DB 列为 timestamp without time zone，存储的是服务器本地时间，
        # 必须用同类型的 naive datetime 比较，避免时区偏移导致负值。
        if latest_fetch.tzinfo is None:
            now = datetime.now()
        else:
            now = datetime.now(timezone.utc)
        
        hours_since = (now - latest_fetch).total_seconds() / 3600
        
        if hours_since < 0:
            return self.default_query_hours
        
        # 加 30 分钟 buffer 并向上取整，最小 1 小时
        smart_hours = max(1, int(hours_since + 1.5))
        return min(smart_hours, self.default_query_hours)
    
    def _calculate_max_pages(self, since_hours: int) -> int:
        """根据时间窗口动态调整最大分页数。窗口越小，所需页数越少。"""
        if since_hours <= 3:
            return 2
        elif since_hours <= 12:
            return 3
        return 5
    
    def _get_existing_tweet_ids(self, conn, tweet_ids: List[str]) -> set:
        """批量查询哪些 tweet ID 已存在于数据库，返回已存在的 ID 集合。"""
        if not tweet_ids:
            return set()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT content_id FROM subscription_content "
            "WHERE channel_type = 'twitter' AND content_id = ANY(%s)",
            (tweet_ids,)
        )
        existing = {row[0] for row in cursor.fetchall()}
        cursor.close()
        return existing
    
    def _ensure_user_ids_for_author(self, conn, author: str, user_ids: List[int]):
        """批量确保该作者所有已有内容都包含这些 user_ids（处理新订阅用户场景）。"""
        if not user_ids:
            return
        cursor = conn.cursor()
        for uid in user_ids:
            cursor.execute("""
                UPDATE subscription_content
                SET user_ids = ARRAY(SELECT DISTINCT UNNEST(user_ids || %s::integer))
                WHERE channel_type = 'twitter'
                AND author_identifier = %s
                AND NOT (%s = ANY(user_ids))
            """, (uid, author, uid))
        cursor.close()
    
    def fetch_tweets_from_api(self, author: str, since_hours: int = None, max_pages: int = 5) -> List[Dict[str, Any]]:
        """
        从Twitter API获取推文
        
        Args:
            author: Twitter用户名（不带@）
            since_hours: 获取最近多少小时的推文（默认使用配置值）
            max_pages: 最大获取页数（默认5页）
            
        Returns:
            推文列表
        """
        if since_hours is None:
            since_hours = self.default_query_hours
        
        logger.info("开始获取Twitter推文", author=author, since_hours=since_hours, max_pages=max_pages)
        
        try:
            until_time = datetime.now(timezone.utc)
            since_time = until_time - timedelta(hours=since_hours)
            
            since_str = since_time.strftime("%Y-%m-%d_%H:%M:%S_UTC")
            until_str = until_time.strftime("%Y-%m-%d_%H:%M:%S_UTC")
            
            query = f"from:{author} since:{since_str} until:{until_str} include:nativeretweets"
            
            params = {
                "query": query,
                "queryType": "Latest"
            }
            
            headers = {
                "X-API-Key": self.api_key
            }
            
            logger.debug("API请求参数", author=author, query=query)
            
            all_tweets = []
            next_cursor = None
            page_count = 0
            api_calls = 0
            
            while page_count < max_pages:
                if next_cursor:
                    params["cursor"] = next_cursor
                
                response = self.session.get(
                    self.api_base_url,
                    headers=headers,
                    params=params,
                    timeout=30,
                    verify=True
                )
                api_calls += 1
                
                if response.status_code != 200:
                    logger.error("API请求失败", 
                               author=author,
                               status_code=response.status_code,
                               response=response.text[:200])
                    break
                
                data = response.json()
                
                tweets = []
                if isinstance(data, list):
                    tweets = data
                elif isinstance(data, dict):
                    tweets = data.get("tweets") or data.get("data") or []
                
                if not tweets:
                    logger.debug("无更多推文，停止分页", author=author, page=page_count)
                    break
                
                all_tweets.extend(tweets)
                
                has_next = False
                cursor_val = ""
                if isinstance(data, dict):
                    has_next = bool(data.get("has_next_page") or data.get("hasNextPage"))
                    cursor_val = (data.get("next_cursor") or data.get("nextCursor") or "")
                
                if has_next and cursor_val:
                    next_cursor = cursor_val
                    page_count += 1
                    time.sleep(1)
                    continue
                break
            
            logger.info("推文获取完成", author=author, total_tweets=len(all_tweets), api_calls=api_calls)
            return all_tweets
            
        except Exception as e:
            logger.error("获取推文失败", author=author, exception=str(e))
            return []
    
    def extract_media_urls(self, tweet_obj: Dict[str, Any]) -> List[str]:
        """从推文对象中提取所有媒体URL"""
        media_urls = []
        
        if 'extendedEntities' in tweet_obj and 'media' in tweet_obj['extendedEntities']:
            for media in tweet_obj['extendedEntities']['media']:
                if 'media_url_https' in media:
                    media_urls.append(media['media_url_https'])
        
        return media_urls
    
    def save_tweet_to_db(self, conn, cursor, tweet: Dict[str, Any], 
                        user_ids: List[int], subscription_ids: List[int]) -> bool:
        """
        保存推文到数据库（使用适配器模式和统一的内容服务）
        
        Args:
            conn: 数据库连接（保留参数兼容性，实际不使用）
            cursor: 数据库游标（保留参数兼容性，实际不使用）
            tweet: 推文数据
            user_ids: 订阅该推文的用户ID列表
            subscription_ids: 订阅ID列表（保留参数兼容性，实际不使用）
            
        Returns:
            是否成功保存（True=新增，False=已存在）
        """
        try:
            # 使用统一的内容服务保存（会自动使用 TwitterAdapter 标准化数据）
            content_id, is_new = self.content_service.save_content(
                channel_type='twitter',
                raw_data=tweet,
                user_ids=user_ids
            )
            
            logger.debug(
                f"{'新增' if is_new else '更新'}推文",
                content_id=content_id,
                tweet_id=tweet.get('id'),
                author=tweet.get('author', {}).get('username', '')
            )
            
            return is_new
            
        except Exception as e:
            logger.error("保存推文失败", tweet_id=tweet.get('id'), exception=str(e))
            return False
    
    def fetch_and_store_tweets(self, subscription_id: Optional[int] = None,
                              user_id: Optional[int] = None,
                              force_fetch: bool = False) -> Dict[str, Any]:
        """
        获取并存储推文
        
        Args:
            subscription_id: 指定订阅ID（可选）
            user_id: 指定用户ID（可选）
            force_fetch: 是否强制获取（忽略上次获取时间）
            
        Returns:
            执行结果统计
        """
        logger.info("开始获取并存储推文",
                   subscription_id=subscription_id,
                   user_id=user_id,
                   force_fetch=force_fetch)
        
        start_time = time.time()
        
        try:
            # 步骤1: 获取订阅列表
            if subscription_id:
                subscriptions = [sub for sub in self.get_all_twitter_subscriptions() 
                               if sub['id'] == subscription_id]
            elif user_id:
                subscriptions = self.get_user_twitter_subscriptions(user_id)
            else:
                subscriptions = self.get_all_twitter_subscriptions()
            
            if not subscriptions:
                logger.info("未找到符合条件的订阅，任务正常完成")
                return {
                    'success': True,
                    'message': '未找到符合条件的订阅，无需获取推文',
                    'subscriptions_count': 0,
                    'authors_count': 0,
                    'tweets_fetched': 0,
                    'tweets_new': 0,
                    'tweets_existing': 0
                }
            
            # 步骤2: 按作者聚合订阅
            aggregated = self.aggregate_subscriptions_by_author(subscriptions)
            
            # 步骤3: 逐个作者获取推文
            conn = self.db_manager.get_connection()
            cursor = conn.cursor()
            
            total_tweets_fetched = 0
            total_tweets_new = 0
            total_tweets_existing = 0
            author_results = {}
            
            total_api_calls_saved = 0
            
            for author, author_subs in aggregated.items():
                try:
                    logger.info("处理作者", author=author, subscriptions_count=len(author_subs))
                    
                    # 收集user_ids和subscription_ids
                    user_ids = list({sub['user_id'] for sub in author_subs})
                    subscription_ids = [sub['id'] for sub in author_subs]
                    
                    # 检查是否是首次获取（该作者没有任何内容）
                    cursor.execute(
                        "SELECT COUNT(*) FROM subscription_content "
                        "WHERE channel_type = 'twitter' AND author_identifier = %s",
                        (author,)
                    )
                    content_count = cursor.fetchone()[0]
                    is_first_fetch = content_count == 0
                    
                    if force_fetch:
                        if is_first_fetch:
                            logger.info("首次获取订阅内容", author=author, hours=self.initial_fetch_hours)
                            tweets = self.fetch_tweets_from_api(
                                author, 
                                since_hours=self.initial_fetch_hours, 
                                max_pages=3
                            )
                        else:
                            tweets = self.fetch_tweets_from_api(
                                author, 
                                since_hours=self.test_fetch_query_hours, 
                                max_pages=2
                            )
                    else:
                        # 定时任务：根据 last_fetched_at 动态计算时间窗口
                        smart_hours = self._calculate_fetch_since_hours(author_subs)
                        smart_max_pages = self._calculate_max_pages(smart_hours)
                        
                        if smart_hours < self.default_query_hours:
                            logger.info("增量拉取",
                                       author=author,
                                       smart_hours=smart_hours,
                                       max_pages=smart_max_pages)
                        
                        tweets = self.fetch_tweets_from_api(
                            author,
                            since_hours=smart_hours,
                            max_pages=smart_max_pages
                        )
                    
                    total_tweets_fetched += len(tweets)
                    
                    # 批量去重：先查哪些 tweet 已存在，跳过无需写库的
                    new_count = 0
                    existing_count = 0
                    
                    if tweets:
                        tweet_ids = [str(t.get('id', '')) for t in tweets if t.get('id')]
                        existing_ids = self._get_existing_tweet_ids(conn, tweet_ids)
                        skipped = len(existing_ids)
                        
                        if skipped > 0:
                            total_api_calls_saved += skipped
                            logger.debug("批量去重跳过已知推文",
                                        author=author, skipped=skipped, remaining=len(tweets) - skipped)
                        
                        # 确保所有 user_ids 在已有内容上得到合并
                        if existing_ids:
                            self._ensure_user_ids_for_author(conn, author, user_ids)
                        
                        for tweet in tweets:
                            tid = str(tweet.get('id', ''))
                            if tid in existing_ids:
                                existing_count += 1
                                continue
                            
                            if self.save_tweet_to_db(conn, cursor, tweet, user_ids, subscription_ids):
                                new_count += 1
                            else:
                                existing_count += 1
                    
                    total_tweets_new += new_count
                    total_tweets_existing += existing_count
                    
                    # 更新订阅状态
                    for sub in author_subs:
                        self._update_subscription_status(cursor, sub['id'], 
                                                        items_count=new_count,
                                                        status='success')
                    
                    author_results[author] = {
                        'fetched': len(tweets),
                        'new': new_count,
                        'existing': existing_count
                    }
                    
                    # 记录获取历史
                    for sub in author_subs:
                        self._record_fetch_history(cursor, sub['id'],
                                                  items_fetched=len(tweets),
                                                  items_new=new_count,
                                                  items_duplicated=existing_count,
                                                  status='success')
                    
                    conn.commit()
                    
                    time.sleep(2)
                    
                except Exception as e:
                    logger.error("处理作者失败", author=author, exception=str(e))
                    conn.rollback()
                    
                    for sub in author_subs:
                        self._update_subscription_status(cursor, sub['id'],
                                                        items_count=0,
                                                        status='failed',
                                                        error=str(e))
                        self._record_fetch_history(cursor, sub['id'],
                                                  items_fetched=0,
                                                  status='failed',
                                                  error_message=str(e))
                    conn.commit()
                    continue
            
            cursor.close()
            conn.close()
            
            execution_duration = int((time.time() - start_time) * 1000)
            
            result = {
                'success': True,
                'message': '推文获取完成',
                'subscriptions_count': len(subscriptions),
                'authors_count': len(aggregated),
                'tweets_fetched': total_tweets_fetched,
                'tweets_new': total_tweets_new,
                'tweets_existing': total_tweets_existing,
                'db_writes_saved': total_api_calls_saved,
                'author_results': author_results,
                'execution_duration_ms': execution_duration
            }
            
            logger.info("推文获取并存储完成", **{k: v for k, v in result.items() if k != 'author_results'})
            
            return result
            
        except Exception as e:
            logger.error("获取并存储推文失败", exception=str(e))
            return {
                'success': False,
                'message': f'执行失败: {str(e)}',
                'error': str(e)
            }
    
    def _update_subscription_status(self, cursor, subscription_id: int,
                                   items_count: int, status: str,
                                   error: str = None):
        """更新订阅状态"""
        try:
            if status == 'success':
                cursor.execute('''
                UPDATE subscription_list
                SET last_fetched_at = CURRENT_TIMESTAMP,
                    last_fetch_items_count = %s,
                    total_items_fetched = total_items_fetched + %s,
                    consecutive_failures = 0
                WHERE id = %s
                ''', (items_count, items_count, subscription_id))
            else:
                cursor.execute('''
                UPDATE subscription_list
                SET last_fetched_at = CURRENT_TIMESTAMP,
                    last_fetch_items_count = 0,
                    consecutive_failures = consecutive_failures + 1
                WHERE id = %s
                ''', (subscription_id,))
        except Exception as e:
            logger.error("更新订阅状态失败", subscription_id=subscription_id, exception=str(e))
    
    def _record_fetch_history(self, cursor, subscription_id: int,
                             items_fetched: int = 0,
                             items_new: int = 0,
                             items_duplicated: int = 0,
                             status: str = 'success',
                             error_message: str = None,
                             execution_duration_ms: int = None):
        """记录获取历史。订阅已删除时跳过；INSERT 用 SAVEPOINT 隔离，避免单条 FK 失败中止整批事务。"""
        cursor.execute(
            "SELECT 1 FROM subscription_list WHERE id = %s",
            (subscription_id,),
        )
        if not cursor.fetchone():
            logger.warning(
                "跳过抓取历史记录：订阅不存在（可能已删除）",
                subscription_id=subscription_id,
            )
            return
        sp = f"sp_rf_{uuid.uuid4().hex[:16]}"
        try:
            cursor.execute(f"SAVEPOINT {sp}")
            cursor.execute('''
            INSERT INTO subscription_fetch_history
            (subscription_id, items_fetched, items_new, items_duplicated,
             status, error_message, execution_duration_ms)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ''', (subscription_id, items_fetched, items_new, items_duplicated,
                  status, error_message, execution_duration_ms))
            cursor.execute(f"RELEASE SAVEPOINT {sp}")
        except Exception as e:
            try:
                cursor.execute(f"ROLLBACK TO SAVEPOINT {sp}")
            except Exception:
                pass
            logger.error("记录获取历史失败", subscription_id=subscription_id, exception=str(e))
