# -*- coding: utf-8 -*-
"""
Twitter订阅服务 - 基于用户订阅列表获取Twitter推文

抓取策略（详见 docs/instructions/FEEDS_MODULE.md §6.3）：
  - 增量轮询：每个作者每次只发 1 个请求，窗口为 [上次成功抓取 - 缓冲, now]。
    仅当窗口内超过一页（20 条）时，把 until_time 前移到已收到的最旧一条之前继续取，
    不重复抓，也不依赖 cursor 分页。
  - 重试：按 twitterapi.io 官方建议 5s → 15s → 30s → 60s；单个作者失败只影响该作者。
  - 兜底：搜索失败的作者改走 /twitter/user/last_tweets，每轮限量。
  - 深度回扫：每天一次重新扫描过去 N 小时，兜住上游的索引延迟。
"""

import logging
import os
import time
import psycopg2
import requests
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
from collections import defaultdict

from backend.database.db_manager import DatabaseManager
from backend.services.content_adapters.twitter_adapter import _snowflake_to_utc
from backend.utils.datetime_utils import ensure_utc, utcnow
from backend.utils.logger import get_logger
from backend.services.subscription_content_service import SubscriptionContentService

logger = get_logger("twitter_subscription_service")
# 运维关键日志走标准 logger：Celery worker 进程里 universal_glance 的 handler 不会落盘，
# 2026-11 排查上游 503 时整轮抓取的服务层日志全部丢失。标准 logger 经 Celery 的 handler
# 写入 logs/celery_data_worker.log（与 substack/weibo service 的做法一致）。
oplog = logging.getLogger(__name__)

API_BASE = "https://api.twitterapi.io"
USER_INFO_PATH = "/twitter/user/info"
LAST_TWEETS_PATH = "/twitter/user/last_tweets"

# twitterapi.io Errors 文档：这些状态码属于"临时问题"，可退避重试；其余 4xx 重试无意义
_RETRYABLE_STATUS = {429, 500, 502, 503, 504}

# 深度回扫的上次启动时间（unix 秒），Redis 共享，避免每轮都回扫
_DEEP_SWEEP_MARKER_KEY = "twitter:deep_sweep:last_started_at"
_DEEP_SWEEP_MARKER_TTL = 7 * 86400

# 单个作者的抓取计划：(author, author_subs, since_ts, until_ts, max_pages)
_AuthorPlan = Tuple[str, List[Dict[str, Any]], int, int, int]


class TwitterApiError(Exception):
    """twitterapi.io 请求失败。status 为 HTTP 状态码（网络异常时为 None）；
    retryable=True 表示按退避重试后仍失败，False 表示请求本身无效（如 400/401/402/403/404）。"""

    def __init__(self, message: str, status: Optional[int] = None, retryable: bool = False):
        super().__init__(message)
        self.status = status
        self.retryable = retryable


class _AbortRun(Exception):
    """key 失效或额度耗尽（401/402）：继续请求其余作者没有意义，整轮中止。"""


def _env_bool(name: str, default: str = 'true') -> bool:
    return os.getenv(name, default).lower() == 'true'


def _env_int_list(name: str, default: str) -> List[int]:
    try:
        values = [int(x) for x in os.getenv(name, default).split(',') if x.strip()]
    except ValueError:
        values = []
    return values or [5, 15, 30, 60]


def _extract_tweets(data: Any) -> List[Dict[str, Any]]:
    """从 twitterapi.io 的响应里取出推文列表。

    搜索接口是顶层 `tweets`；last_tweets 文档写的是顶层 `tweets`，实际返回嵌套在
    `data.tweets` 下（2026-11 实测），两种都兼容。
    """
    if isinstance(data, list):
        return [t for t in data if isinstance(t, dict)]
    if not isinstance(data, dict):
        return []
    tweets = data.get("tweets")
    if tweets is None and isinstance(data.get("data"), dict):
        tweets = data["data"].get("tweets")
    return [t for t in (tweets or []) if isinstance(t, dict)]


def _tweet_epoch_seconds(tweet: Dict[str, Any]) -> Optional[int]:
    """推文创建时间（unix 秒）。优先用 Snowflake ID：createdAt 偶发被上游误标时区，
    ID 里编码的时间不受影响（与 twitter_adapter 入库口径一致）。"""
    created = _snowflake_to_utc(str(tweet.get("id") or ""))
    if created is None:
        raw = tweet.get("createdAt")
        if not raw:
            return None
        try:
            from dateutil import parser
            created = ensure_utc(parser.parse(raw))
        except Exception:
            return None
    return int(created.timestamp())


def _author_handle(tweet: Dict[str, Any]) -> str:
    """推文作者的 handle（小写、不带 @）。搜索接口的字段是 author.username，
    时间线接口（last_tweets）是 author.userName，两种都要认。"""
    author = tweet.get("author") or {}
    if not isinstance(author, dict):
        return ""
    handle = author.get("username") or author.get("userName") or author.get("screen_name") or ""
    return str(handle).lstrip("@").lower()


def _is_soft_time_limit(exc: BaseException) -> bool:
    try:
        from celery.exceptions import SoftTimeLimitExceeded
        return isinstance(exc, SoftTimeLimitExceeded)
    except Exception:
        return False


_redis_client = None
_redis_unavailable = False


def _get_redis():
    """惰性获取 Redis 客户端；不可用时返回 None（深度回扫随之跳过，见 _deep_sweep_due）。"""
    global _redis_client, _redis_unavailable
    if _redis_client is not None:
        return _redis_client
    if _redis_unavailable:
        return None
    try:
        import redis
        client = redis.from_url(os.getenv('REDIS_URL', 'redis://localhost:6379/0'),
                                socket_connect_timeout=2, socket_timeout=2)
        client.ping()
        _redis_client = client
        return _redis_client
    except Exception as e:
        _redis_unavailable = True
        oplog.warning("[Twitter] Redis 不可用，深度回扫跳过: %s", e)
        return None


def _deep_sweep_due(interval_hours: float, now_ts: Optional[float] = None) -> bool:
    client = _get_redis()
    if client is None:
        return False
    try:
        last = client.get(_DEEP_SWEEP_MARKER_KEY)
    except Exception as e:
        oplog.warning("[Twitter] 读取回扫标记失败，深度回扫跳过: %s", e)
        return False
    if not last:
        return True
    now_ts = time.time() if now_ts is None else now_ts
    return (now_ts - float(last)) >= interval_hours * 3600


def _mark_deep_sweep_started(now_ts: Optional[float] = None) -> None:
    client = _get_redis()
    if client is None:
        return
    try:
        client.set(_DEEP_SWEEP_MARKER_KEY, int(now_ts or time.time()), ex=_DEEP_SWEEP_MARKER_TTL)
    except Exception as e:
        oplog.warning("[Twitter] 写入回扫标记失败: %s", e)


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
            self.api_base_url = f"{API_BASE}/twitter/tweet/advanced_search"
            
            # 不挂 urllib3 Retry：状态码重试统一由 _get_with_retry 按官方退避节奏处理，
            # 否则 adapter 会先按自己的 backoff 重试若干轮，外层拿到的已是最终响应。
            self.session = requests.Session()
            
            # 初始化内容服务（使用适配器模式）
            self.content_service = SubscriptionContentService()
            
            # 配置参数
            # 单次拉取「最多向过去看多少小时」：既是增量轮询的窗口上限，也是没有抓取历史时的首拉窗口
            self.default_query_hours = int(os.getenv('TWITTER_DEFAULT_QUERY_HOURS', '48'))
            self.test_fetch_query_hours = 12  # Test Fetch 固定使用12小时
            self.initial_fetch_hours = int(os.getenv('TWITTER_INITIAL_FETCH_HOURS', '168'))  # 首次获取：7天（168小时）
            self.max_tweets_per_request = int(os.getenv('TWITTER_MAX_TWEETS_PER_REQUEST', '20'))
            
            # 请求节奏与重试
            self.request_timeout = int(os.getenv('TWITTER_REQUEST_TIMEOUT', '30'))
            # twitterapi.io 官方建议的退避序列；每个请求最多尝试 1 + len(backoff) 次
            self.retry_backoff = _env_int_list('TWITTER_RETRY_BACKOFF_SECONDS', '5,15,30,60')
            self.author_pause_seconds = float(os.getenv('TWITTER_AUTHOR_PAUSE_SECONDS', '0.5'))
            
            # 增量轮询窗口：从上次成功抓取往前多留一段缓冲
            self.poll_buffer_hours = float(os.getenv('TWITTER_POLL_BUFFER_HOURS', '1'))
            self.max_pages_per_author = int(os.getenv('TWITTER_MAX_PAGES_PER_AUTHOR', '25'))
            self.initial_fetch_max_pages = int(os.getenv('TWITTER_INITIAL_FETCH_MAX_PAGES', '60'))
            
            # 兜底：搜索失败的作者改走时间线接口（按返回条数计费，所以每轮限量）
            self.fallback_enabled = _env_bool('TWITTER_FALLBACK_ENABLED')
            self.fallback_max_calls = int(os.getenv('TWITTER_FALLBACK_MAX_CALLS_PER_RUN', '10'))
            self.fallback_max_pages = int(os.getenv('TWITTER_FALLBACK_MAX_PAGES', '3'))
            
            # 深度回扫
            self.deep_sweep_enabled = _env_bool('TWITTER_DEEP_SWEEP_ENABLED')
            self.deep_sweep_interval_hours = float(os.getenv('TWITTER_DEEP_SWEEP_INTERVAL_HOURS', '22'))
            self.deep_sweep_window_hours = float(os.getenv('TWITTER_DEEP_SWEEP_WINDOW_HOURS', '36'))
            self.deep_sweep_chunk_hours = float(os.getenv('TWITTER_DEEP_SWEEP_CHUNK_HOURS', '6'))
            # 增量轮询的作者失败率超过该值时跳过本次回扫：上游不稳时回扫只会白耗重试时间
            self.deep_sweep_max_fail_ratio = float(os.getenv('TWITTER_DEEP_SWEEP_MAX_FAIL_RATIO', '0.1'))
            
            # 时间预算。data worker 只有 2 个并发槽，长任务会挤占同一队列的其他数据任务
            self.run_budget_minutes = float(os.getenv('TWITTER_RUN_BUDGET_MINUTES', '20'))
            self.deep_sweep_budget_minutes = float(os.getenv('TWITTER_DEEP_SWEEP_BUDGET_MINUTES', '12'))
            
            self._reset_run_state()
            
            logger.info("Twitter订阅服务初始化完成")
            
        except Exception as e:
            logger.error("Twitter订阅服务初始化失败", exception=str(e))
            raise
    
    def _reset_run_state(self) -> None:
        self.stats: Dict[str, int] = {
            'requests': 0, 'ok': 0, 'retries': 0,
            'http_503': 0, 'http_429': 0, 'http_other': 0, 'network_errors': 0,
        }
        self._db_writes_saved = 0
        self._pending_user_ids: List[Tuple[str, List[int]]] = []
        self.last_result: Optional[Dict[str, Any]] = None
    
    def _get_with_retry(self, url: str, params: Optional[Dict[str, Any]], *, what: str) -> Dict[str, Any]:
        """
        GET 并解析 JSON。429/5xx/网络异常按官方退避重试，其余 4xx 不重试。
        
        Raises:
            TwitterApiError: retryable=True 表示重试后仍失败；False 表示请求本身无效
        """
        headers = {"X-API-Key": self.api_key}
        delays = self.retry_backoff
        
        attempt = 0
        while True:
            attempt += 1
            self.stats['requests'] += 1
            status: Optional[int] = None
            try:
                response = self.session.get(
                    url,
                    headers=headers,
                    params=params,
                    timeout=self.request_timeout,
                    verify=True,
                )
                status = response.status_code
                if status == 200:
                    self.stats['ok'] += 1
                    try:
                        return response.json()
                    except ValueError as e:
                        raise TwitterApiError(f"{what}: HTTP 200 但响应不是 JSON ({e})",
                                              status=200, retryable=False)
                detail = f"HTTP {status}: {response.text[:160]}"
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
                detail = f"{type(e).__name__}: {str(e)[:160]}"
            
            if status is None:
                self.stats['network_errors'] += 1
            elif status == 503:
                self.stats['http_503'] += 1
            elif status == 429:
                self.stats['http_429'] += 1
            else:
                self.stats['http_other'] += 1
            
            if status is not None and status not in _RETRYABLE_STATUS:
                raise TwitterApiError(f"{what}: {detail}", status=status, retryable=False)
            if attempt > len(delays):
                raise TwitterApiError(f"{what}: {detail}（已重试 {attempt - 1} 次）",
                                      status=status, retryable=True)
            
            sleep_seconds = delays[attempt - 1]
            self.stats['retries'] += 1
            oplog.warning("[Twitter] 重试 | what=%s attempt=%d/%d status=%s sleep=%ss",
                          what, attempt, len(delays) + 1, status, sleep_seconds)
            time.sleep(sleep_seconds)
    
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
            data = self._get_with_retry(f"{API_BASE}{USER_INFO_PATH}", {"userName": username},
                                        what=f"user/info {username}")
        except TwitterApiError as e:
            error_msg = f"Twitter API请求失败: {e}"
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
        
        if not isinstance(data, dict) or data.get('status') == 'error':
            msg = data.get('msg', 'User not found') if isinstance(data, dict) else 'Unexpected response'
            logger.warning(f"Twitter用户不存在 | username={username} | msg={msg}")
            return {
                'exists': False,
                'user_info': None,
                'error': msg
            }
        
        user_info = data.get('data', {})
        logger.info(f"Twitter用户验证成功 | username={username} | name={user_info.get('name')} | id={user_info.get('id')}")
        
        return {
            'exists': True,
            'user_info': user_info,
            'error': None
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
                    priority, max_items_per_fetch,
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
                    'max_items_per_fetch': row[5],
                    'last_fetched_at': row[6],
                    'filter_config': row[7] or {},
                    'metadata': row[8] or {},
                    'total_items_fetched': row[9] or 0,
                    'consecutive_failures': row[10] or 0
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
                    priority, max_items_per_fetch,
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
                    'max_items_per_fetch': row[5],
                    'last_fetched_at': row[6],
                    'filter_config': row[7] or {},
                    'metadata': row[8] or {}
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
    
    def _last_success_time(self, cursor, subscription_ids: List[int]) -> Optional[datetime]:
        """该作者最近一次成功抓取的时间（UTC aware）。

        以 fetch history 为准，而不是 subscription_list.last_fetched_at —— 后者在抓取失败时也会被刷新。
        """
        cursor.execute(
            "SELECT max(fetch_time) FROM subscription_fetch_history "
            "WHERE subscription_id = ANY(%s) AND status = 'success'",
            (subscription_ids,)
        )
        row = cursor.fetchone()
        return ensure_utc(row[0]) if row else None
    
    def _poll_window_start(self, last_success: Optional[datetime], now: datetime) -> int:
        """增量轮询的窗口起点（unix 秒）：上次成功抓取往前留一段缓冲，但不超过单次拉取上限。"""
        now_ts = int(now.timestamp())
        max_span = int(self.default_query_hours * 3600)
        if last_success is None:
            return now_ts - max_span
        since_ts = int((last_success - timedelta(hours=self.poll_buffer_hours)).timestamp())
        # 时钟异常保护：起点不得晚于「现在」
        return min(max(since_ts, now_ts - max_span), now_ts)
    
    def _plan_windows(self, cursor, aggregated: Dict[str, List[Dict[str, Any]]],
                      now: datetime, force_fetch: bool) -> List[_AuthorPlan]:
        """为每个作者生成抓取计划。增量模式下窗口起点越早 = 越久没抓成功，排在前面：
        预算不够时优先补最陈旧的作者。"""
        plan: List[_AuthorPlan] = []
        until_ts = int(now.timestamp())
        
        for author, author_subs in aggregated.items():
            if force_fetch:
                # 检查是否是首次获取（该作者没有任何内容）。
                # 注意：aggregate_subscriptions_by_author 把 author 强制小写 (deitaone),
                # 但 DB 里 author_identifier 保留 Twitter 原始大小写 (DeItaone)。
                # 因此用 lower(author_identifier) 做大小写不敏感比较，否则会把已有
                # 内容的作者误判为"首次获取"，触发 168h 全量拉取，浪费 API 配额。
                cursor.execute(
                    "SELECT COUNT(*) FROM subscription_content "
                    "WHERE channel_type = 'twitter' "
                    "AND lower(author_identifier) = lower(%s)",
                    (author,)
                )
                if cursor.fetchone()[0] == 0:
                    logger.info("首次获取订阅内容", author=author, hours=self.initial_fetch_hours)
                    since_ts = until_ts - int(self.initial_fetch_hours * 3600)
                    max_pages = self.initial_fetch_max_pages
                else:
                    since_ts = until_ts - int(self.test_fetch_query_hours * 3600)
                    max_pages = self.max_pages_per_author
            else:
                # 定时任务：从上次成功抓取起算
                since_ts = self._poll_window_start(
                    self._last_success_time(cursor, [sub['id'] for sub in author_subs]), now)
                max_pages = self.max_pages_per_author
            
            plan.append((author, author_subs, since_ts, until_ts, max_pages))
        
        if not force_fetch:
            plan.sort(key=lambda item: item[2])
        
        return plan
    
    def fetch_tweets_in_window(self, author: str, since_ts: int, until_ts: int,
                               max_pages: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        取作者在窗口 (since_ts, until_ts] 内的推文。
        
        一页最多 20 条。如果窗口内还有更早的，就把 until_time 前移到「已收到的最旧一条」
        所在秒的下一秒再取一次（twitterapi.io 官方推荐的做法），直到取完或达到页数上限。
        不走 cursor 分页：Twitter 侧的分页会漏结果。
        
        Raises:
            TwitterApiError: 任一页重试后仍失败，或请求本身无效
        """
        if max_pages is None:
            max_pages = self.max_pages_per_author
        
        collected: Dict[str, Dict[str, Any]] = {}
        page_until = until_ts
        pages = 0
        truncated = False
        
        while True:
            if page_until <= since_ts:
                break
            if pages >= max_pages:
                truncated = True
                break
            
            query = (f"from:{author} since_time:{since_ts} until_time:{page_until} "
                     f"include:nativeretweets")
            data = self._get_with_retry(
                self.api_base_url,
                {"query": query, "queryType": "Latest"},
                what=f"search {author}"
            )
            pages += 1
            
            tweets = _extract_tweets(data) if isinstance(data, dict) else []
            for tweet in tweets:
                tweet_id = str(tweet.get("id") or "")
                if tweet_id:
                    collected[tweet_id] = tweet
            
            # 满页也按「可能还有」处理：上游会过滤广告，页数不满不代表没有更多
            has_more = bool(data.get("has_next_page") or data.get("hasNextPage")) \
                or len(tweets) >= self.max_tweets_per_request
            if not tweets or not has_more:
                break
            
            oldest_ts = min(
                (ts for ts in (_tweet_epoch_seconds(t) for t in tweets) if ts is not None),
                default=None
            )
            # until_time 是开区间上界，+1s 保证与最旧那条同一秒的更早推文不会漏掉
            next_until = oldest_ts + 1 if oldest_ts is not None else page_until
            if next_until >= page_until:
                # 这一页只剩边界那一秒的推文，说明已经取到窗口起点了，是正常结束。
                # 只有上游无视 until_time、又返回了满满一页时才算异常。
                if oldest_ts is None or len(tweets) >= self.max_tweets_per_request:
                    oplog.warning("[Twitter] 续取无进展，停止翻页 | author=%s until=%d", author, page_until)
                break
            page_until = next_until
            time.sleep(0.3)
        
        if truncated:
            oplog.warning(
                "[Twitter] 达到单作者页数上限，窗口内更早的推文未取 | author=%s max_pages=%d until=%d since=%d",
                author, max_pages, page_until, since_ts)
        
        return list(collected.values())
    
    def fetch_tweets_from_api(self, author: str, since_hours: int = None, max_pages: int = 5) -> List[Dict[str, Any]]:
        """
        从Twitter API获取最近 since_hours 小时的推文（兼容旧入口；请求失败时返回空列表）。
        """
        if since_hours is None:
            since_hours = self.default_query_hours
        
        until_ts = int(time.time())
        try:
            return self.fetch_tweets_in_window(author, until_ts - int(since_hours * 3600), until_ts,
                                               max_pages=max_pages)
        except TwitterApiError as e:
            logger.error("获取推文失败", author=author, exception=str(e))
            return []
    
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
        """批量确保该作者所有已有内容都包含这些 user_ids（处理新订阅用户场景）。

        author 已被 aggregate_subscriptions_by_author 强制小写，但 DB 存大小写敏感的
        原始 username。用 lower(author_identifier) 兜底，避免漏更新。
        """
        if not user_ids:
            return
        cursor = conn.cursor()
        for uid in user_ids:
            cursor.execute("""
                UPDATE subscription_content
                SET user_ids = ARRAY(SELECT DISTINCT UNNEST(user_ids || %s::integer))
                WHERE channel_type = 'twitter'
                AND lower(author_identifier) = lower(%s)
                AND NOT (%s = ANY(user_ids))
            """, (uid, author, uid))
        cursor.close()
    
    def _store_author_tweets(self, conn, cursor, author: str, author_subs: List[Dict[str, Any]],
                             tweets: List[Dict[str, Any]]) -> Tuple[int, int, List[int]]:
        """把抓到的推文落库。返回 (新增数, 已存在数, 本轮新插入的 subscription_content.id)。"""
        user_ids = list({sub['user_id'] for sub in author_subs})
        subscription_ids = [sub['id'] for sub in author_subs]
        
        new_count = 0
        existing_count = 0
        author_new_ids: List[int] = []
        
        if tweets:
            # 批量去重：先查哪些 tweet 已存在，跳过无需写库的
            tweet_ids = [str(t.get('id', '')) for t in tweets if t.get('id')]
            existing_ids = self._get_existing_tweet_ids(conn, tweet_ids)
            
            if len(existing_ids) > 0:
                self._db_writes_saved += len(existing_ids)
                logger.debug("批量去重跳过已知推文",
                            author=author, skipped=len(existing_ids), remaining=len(tweet_ids) - len(existing_ids))
            
            # 确保所有 user_ids 在已有内容上得到合并
            if existing_ids:
                self._ensure_user_ids_for_author(conn, author, user_ids)
            
            for tweet in tweets:
                tid = str(tweet.get('id', ''))
                if tid in existing_ids:
                    existing_count += 1
                    continue
                
                new_content_id, is_new = self.save_tweet_to_db(
                    conn, cursor, tweet, user_ids, subscription_ids
                )
                if is_new:
                    new_count += 1
                    if new_content_id is not None:
                        author_new_ids.append(new_content_id)
                else:
                    existing_count += 1
        
        return new_count, existing_count, author_new_ids
    
    def _dispatch_tagging(self, author: str, new_content_ids: List[int]) -> None:
        """commit 后立即派发打标，不等所有作者结束 —— 任务被超时杀掉时循环外的派发会全部丢失。"""
        if not new_content_ids:
            return
        try:
            from backend.tasks.file_tasks import trigger_auto_feed_tagging
            trigger_auto_feed_tagging(new_content_ids, delay=2)
            logger.info(
                "已派发 Feeds 自动打标",
                author=author,
                new_content_count=len(new_content_ids),
            )
        except Exception as tag_err:
            logger.error(
                "派发 Feeds 打标失败（不影响推文抓取）",
                author=author,
                exception=str(tag_err),
            )
    
    def _record_failure(self, conn, cursor, author_subs: List[Dict[str, Any]], error: str) -> None:
        self._pending_user_ids = []
        conn.rollback()
        
        for sub in author_subs:
            self._update_subscription_status(cursor, sub['id'],
                                            items_count=0,
                                            status='failed',
                                            error=error)
            self._record_fetch_history(cursor, sub['id'],
                                      items_fetched=0,
                                      status='failed',
                                      error_message=error[:500])
        conn.commit()
    
    def _cached_user_id(self, author_subs: List[Dict[str, Any]]) -> Optional[str]:
        for sub in author_subs:
            user_id = (sub.get('metadata') or {}).get('twitter_user_id')
            if user_id:
                return str(user_id)
        return None
    
    def _persist_user_id_cache(self, cursor) -> None:
        """把本轮查到的 twitter user id 写进对应订阅的 metadata，下次兜底不用再查。"""
        for user_id, sub_ids in self._pending_user_ids:
            cursor.execute(
                "UPDATE subscription_list "
                "SET metadata = COALESCE(metadata, '{}'::jsonb) || jsonb_build_object('twitter_user_id', %s::text) "
                "WHERE id = ANY(%s)",
                (user_id, sub_ids)
            )
        self._pending_user_ids = []
    
    def _fetch_via_timeline(self, conn, author: str, author_subs: List[Dict[str, Any]],
                            since_ts: int, calls_budget: int) -> Tuple[Optional[List[Dict[str, Any]]], int]:
        """
        兜底通道：改走 /twitter/user/last_tweets 拿该作者最新的推文。
        
        twitterapi.io 在 2026-11 搜索故障期间推荐用它跟踪指定账号。它按返回条数计费
        （一页 20 条约 300 credits），所以只在搜索失败后调用，并从新到旧翻到已入库的
        推文或窗口起点为止。calls_budget 是本轮剩余的兜底请求额度，方法内部不会超出。
        
        Returns:
            (推文列表, 实际请求次数)；额度不足、没有真正发起兜底时推文列表为 None
        """
        calls = 0
        params: Dict[str, Any] = {'includeReplies': 'true'}
        
        user_id = self._cached_user_id(author_subs)
        if user_id:
            params['userId'] = user_id
        else:
            if calls_budget - calls < 2:
                # 额度不够「查一次 id + 取一页」：直接放弃，不要白花一次查询
                oplog.info("[Twitter] 兜底额度不足，跳过时间线兜底 | author=%s", author)
                return None, 0
            # 官方建议用 userId 调用（比 userName 更稳更快）：查一次，成功后随本轮事务写进订阅 metadata
            info = self._get_with_retry(f"{API_BASE}{USER_INFO_PATH}", {'userName': author},
                                        what=f"user/info {author}")
            calls += 1
            user_id = str(((info or {}).get('data') or {}).get('id') or '') if isinstance(info, dict) else ''
            if user_id:
                self._pending_user_ids.append((user_id, [sub['id'] for sub in author_subs]))
                params['userId'] = user_id
            else:
                params['userName'] = author
        
        # 距窗口起点超过 2 小时才多翻几页，否则只取第一页
        max_pages = self.fallback_max_pages if (int(time.time()) - since_ts) > 2 * 3600 else 1
        max_pages = min(max_pages, calls_budget - calls)
        
        collected: Dict[str, Dict[str, Any]] = {}
        page_cursor = ''
        pages = 0
        for _ in range(max_pages):
            if page_cursor:
                params['cursor'] = page_cursor
            data = self._get_with_retry(f"{API_BASE}{LAST_TWEETS_PATH}", params,
                                        what=f"last_tweets {author}")
            calls += 1
            pages += 1
            if not isinstance(data, dict):
                raise TwitterApiError(f"last_tweets {author}: 响应格式异常", status=200, retryable=False)
            if data.get('status') == 'error':
                raise TwitterApiError(
                    f"last_tweets {author}: {data.get('msg') or data.get('message') or 'status=error'}",
                    status=200, retryable=False)
            
            page = _extract_tweets(data)
            # 只保留该作者自己的推文：时间线里会混入平台插入的他人推文，不能记到别的作者名下
            own = [t for t in page if _author_handle(t) == author]
            for tweet in own:
                author_info = tweet.get("author")
                if isinstance(author_info, dict) and not author_info.get("username") and author_info.get("userName"):
                    # last_tweets 用 author.userName，入库适配器只认 author.username（否则会退回从 URL 截取）
                    author_info["username"] = author_info["userName"]
                tweet_id = str(tweet.get("id") or "")
                if tweet_id:
                    collected[tweet_id] = tweet
            
            if not page or not data.get('has_next_page') or not data.get('next_cursor'):
                break
            oldest_ts = min(
                (ts for ts in (_tweet_epoch_seconds(t) for t in page) if ts is not None),
                default=None
            )
            if oldest_ts is None or oldest_ts <= since_ts:
                break
            # 官方建议：翻到已经见过的推文就可以停
            page_ids = [str(t.get('id')) for t in own if t.get('id')]
            if page_ids and self._get_existing_tweet_ids(conn, page_ids):
                break
            
            page_cursor = data['next_cursor']
            time.sleep(0.3)
        
        oplog.info("[Twitter] 时间线兜底 | author=%s user_id=%s pages=%d requests=%d tweets=%d",
                   author, user_id or '-', pages, calls, len(collected))
        return list(collected.values()), calls
    
    def extract_media_urls(self, tweet_obj: Dict[str, Any]) -> List[str]:
        """从推文对象中提取所有媒体URL"""
        media_urls = []
        
        if 'extendedEntities' in tweet_obj and 'media' in tweet_obj['extendedEntities']:
            for media in tweet_obj['extendedEntities']['media']:
                if 'media_url_https' in media:
                    media_urls.append(media['media_url_https'])
        
        return media_urls
    
    def save_tweet_to_db(self, conn, cursor, tweet: Dict[str, Any], 
                        user_ids: List[int], subscription_ids: List[int]) -> Tuple[Optional[int], bool]:
        """
        保存推文到数据库（使用适配器模式和统一的内容服务）
        
        Args:
            conn: 数据库连接（保留参数兼容性，实际不使用）
            cursor: 数据库游标（保留参数兼容性，实际不使用）
            tweet: 推文数据
            user_ids: 订阅该推文的用户ID列表
            subscription_ids: 订阅ID列表（保留参数兼容性，实际不使用）
            
        Returns:
            (content_id, is_new):
                - content_id: subscription_content.id (失败时为 None)
                - is_new: 是否本次新插入 (False=已存在或失败)
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
            
            return content_id, is_new
            
        except Exception as e:
            logger.error("保存推文失败", tweet_id=tweet.get('id'), exception=str(e))
            return None, False
    
    def _close_quietly(self, cursor, conn) -> None:
        for resource in (cursor, conn):
            if resource is None:
                continue
            try:
                resource.close()
            except Exception as e:
                logger.warning(f"关闭数据库资源失败: {e}")
    
    def _process_author(self, conn, cursor, author: str, author_subs: List[Dict[str, Any]],
                        since_ts: int, until_ts: int, max_pages: int,
                        allow_fallback: bool, fallback_calls_budget: int) -> Dict[str, Any]:
        """抓取并入库一个作者。只有 401/402（向外抛 _AbortRun）和软超时会继续向上抛。"""
        tweets: List[Dict[str, Any]] = []
        source = 'search'
        error: Optional[str] = None
        fallback_requests = 0
        
        try:
            tweets = self.fetch_tweets_in_window(author, since_ts, until_ts, max_pages=max_pages)
        except TwitterApiError as e:
            if e.status in (401, 402):
                self._record_failure(conn, cursor, author_subs, str(e))
                raise _AbortRun(str(e)) from e
            error = str(e)
            oplog.warning("[Twitter] 搜索失败 | author=%s error=%s", author, error)
            
            if allow_fallback:
                try:
                    tweets, fallback_requests = self._fetch_via_timeline(
                        conn, author, author_subs, since_ts, calls_budget=fallback_calls_budget)
                    if tweets is None:
                        # 额度不足，兜底没有真正发起：保留搜索失败的错误，不能记成成功
                        error = f"{error}; 兜底额度不足，未发起 last_tweets"
                        tweets = []
                    else:
                        source = 'timeline'
                        error = None
                except TwitterApiError as fallback_exc:
                    if fallback_exc.status in (401, 402):
                        self._record_failure(conn, cursor, author_subs, str(fallback_exc))
                        raise _AbortRun(str(fallback_exc)) from fallback_exc
                    error = f"{error}; 兜底 last_tweets: {fallback_exc}"
                    tweets = []
        
        if error is not None:
            self._record_failure(conn, cursor, author_subs, error)
            return {'status': 'failed', 'source': source, 'error': error,
                    'fetched': 0, 'new': 0, 'existing': 0, 'fallback_requests': fallback_requests}
        
        new_count, existing_count, author_new_ids = self._store_author_tweets(
            conn, cursor, author, author_subs, tweets)
        
        # 更新订阅状态
        for sub in author_subs:
            self._update_subscription_status(cursor, sub['id'], 
                                            items_count=new_count,
                                            status='success')
        
        # 记录获取历史
        for sub in author_subs:
            self._record_fetch_history(cursor, sub['id'],
                                      items_fetched=len(tweets),
                                      items_new=new_count,
                                      items_duplicated=existing_count,
                                      status='success')
        
        self._persist_user_id_cache(cursor)
        conn.commit()
        
        self._dispatch_tagging(author, author_new_ids)
        
        return {'status': 'success', 'source': source, 'error': None,
                'fetched': len(tweets), 'new': new_count, 'existing': existing_count,
                'fallback_requests': fallback_requests}
    
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
        self._reset_run_state()
        
        conn = None
        cursor = None
        subscriptions: List[Dict[str, Any]] = []
        plan: List[_AuthorPlan] = []
        author_results: Dict[str, Dict[str, Any]] = {}
        processed = 0
        processed_ok = 0
        failed_authors: List[Dict[str, str]] = []
        fallback_requests = 0
        sweep_summary: Optional[Dict[str, Any]] = None
        abort_message: Optional[str] = None
        soft_limit_hit = False
        
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
            
            conn = self.db_manager.get_connection()
            cursor = conn.cursor()
            
            # 步骤3: 规划窗口，逐个作者获取推文
            try:
                plan = self._plan_windows(cursor, aggregated, utcnow(), force_fetch)
                conn.commit()  # 规划阶段只读，先结束事务再进入耗时较长的 API 阶段
                
                deadline = start_time + self.run_budget_minutes * 60
                for index, (author, author_subs, since_ts, until_ts, max_pages) in enumerate(plan):
                    if time.time() >= deadline:
                        # 预算用完：剩下的作者留到下一轮，它们仍是最久没成功的，会被优先处理
                        break
                    
                    try:
                        outcome = self._process_author(
                            conn, cursor, author, author_subs, since_ts, until_ts, max_pages,
                            allow_fallback=(self.fallback_enabled
                                            and fallback_requests < self.fallback_max_calls),
                            fallback_calls_budget=self.fallback_max_calls - fallback_requests,
                        )
                    except _AbortRun as e:
                        # 该作者已记为失败；key 失效或额度耗尽时，其余作者不再请求
                        abort_message = str(e)
                        processed += 1
                        author_results[author] = {
                            'fetched': 0, 'new': 0, 'existing': 0,
                            'status': 'failed', 'source': 'search',
                        }
                        failed_authors.append({'author': author, 'error': abort_message})
                        break
                    except Exception as e:
                        if _is_soft_time_limit(e):
                            raise
                        logger.error("处理作者失败", author=author, exception=str(e))
                        error = str(e)
                        self._record_failure(conn, cursor, author_subs, error)
                        outcome = {'status': 'failed', 'source': 'search', 'error': error,
                                   'fetched': 0, 'new': 0, 'existing': 0, 'fallback_requests': 0}
                    
                    processed += 1
                    fallback_requests += outcome['fallback_requests']
                    author_results[author] = {
                        'fetched': outcome['fetched'],
                        'new': outcome['new'],
                        'existing': outcome['existing'],
                        'status': outcome['status'],
                        'source': outcome['source'],
                    }
                    if outcome['status'] == 'failed':
                        failed_authors.append({'author': author, 'error': outcome['error'] or ''})
                    else:
                        processed_ok += 1
                    
                    if index < len(plan) - 1:
                        time.sleep(self.author_pause_seconds)
                
                # 步骤4: 深度回扫 —— 只在本轮作者全部处理完、且搜索基本健康时进行
                is_full_run = subscription_id is None and user_id is None and not force_fetch
                fail_ratio = len(failed_authors) / max(processed, 1)
                if (is_full_run and self.deep_sweep_enabled
                        and processed == len(plan)
                        and fail_ratio <= self.deep_sweep_max_fail_ratio
                        and _deep_sweep_due(self.deep_sweep_interval_hours)):
                    _mark_deep_sweep_started()
                    sweep_summary = self._run_deep_sweep(conn, cursor, aggregated)
            except _AbortRun as e:
                abort_message = str(e)
            except Exception as e:
                if not _is_soft_time_limit(e):
                    raise
                soft_limit_hit = True
            finally:
                conn.rollback()
            
            execution_duration = int((time.time() - start_time) * 1000)
            skipped = len(plan) - processed
            total_fetched = sum(r['fetched'] for r in author_results.values())
            total_new = sum(r['new'] for r in author_results.values())
            total_existing = sum(r['existing'] for r in author_results.values())
            
            if abort_message:
                message = f"twitterapi.io 拒绝了请求，本轮中止: {abort_message}"
            elif soft_limit_hit:
                message = f"推文获取完成（触及软超时，{skipped} 个作者未处理）"
            else:
                message = '推文获取完成'
            
            result = {
                'success': abort_message is None,
                'message': message,
                'subscriptions_count': len(subscriptions),
                'authors_count': len(plan),
                'tweets_fetched': total_fetched,
                'tweets_new': total_new,
                'tweets_existing': total_existing,
                'db_writes_saved': self._db_writes_saved,
                'author_results': author_results,
                'execution_duration_ms': execution_duration,
                # 以下为 2026-11 新增的可观测性字段
                'authors_ok': processed_ok,
                'authors_failed': len(failed_authors),
                'authors_skipped': skipped,
                'failed_authors': failed_authors,
                'api_requests': self.stats['requests'],
                'api_retries': self.stats['retries'],
                'http_503': self.stats['http_503'],
                'timeline_fallback_requests': fallback_requests,
                'deep_sweep': sweep_summary,
            }
            self.last_result = result
        except Exception as e:
            logger.error("获取并存储推文失败", exception=str(e))
            return {
                'success': False,
                'message': f'执行失败: {str(e)}',
                'error': str(e)
            }
        finally:
            self._close_quietly(cursor, conn)
        
        logger.info("推文获取并存储完成", **{k: v for k, v in result.items()
                                        if k not in ('author_results', 'failed_authors', 'deep_sweep')})
        oplog.info(
            "[Twitter] 抓取完成 | %sauthors=%d ok=%d failed=%d skipped=%d tweets_returned=%d "
            "tweets_new=%d requests=%d retries=%d http_503=%d http_429=%d net_errors=%d "
            "timeline_fallback_requests=%d deep_sweep=%s duration_s=%d",
            "ABORTED " if abort_message else ("SOFT_LIMIT " if soft_limit_hit else ""),
            len(plan), processed_ok, len(failed_authors), skipped, total_fetched, total_new,
            self.stats['requests'], self.stats['retries'], self.stats['http_503'],
            self.stats['http_429'], self.stats['network_errors'], fallback_requests,
            (f"authors={sweep_summary['authors']} new={sweep_summary['tweets_new']}"
             if sweep_summary else "not_run"),
            round(time.time() - start_time),
        )
        if abort_message:
            oplog.error("[Twitter] 本轮中止 | reason=%s", abort_message)
        
        return result
    
    def _run_deep_sweep(self, conn, cursor, aggregated: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Any]:
        """
        深度回扫：把所有作者过去 deep_sweep_window_hours 的推文重新扫一遍，补录漏掉的。
        
        增量轮询只回看一小段缓冲，如果上游把推文延迟数小时才编入搜索索引，就会漏掉。
        这里按固定的大块窗口重扫，同样不走 cursor 分页。不写 fetch history —— 它是增量
        轮询的补充，而不是一次独立的抓取。
        """
        started = time.time()
        deadline = started + self.deep_sweep_budget_minutes * 60
        sweep_until = int(started)
        sweep_since = sweep_until - int(self.deep_sweep_window_hours * 3600)
        chunk_seconds = max(int(self.deep_sweep_chunk_hours * 3600), 600)
        
        # 从最新的一块往回扫：预算用完时，越新的数据越重要
        bounds = list(range(sweep_until, sweep_since, -chunk_seconds)) + [sweep_since]
        chunks = list(zip(bounds[1:], bounds[:-1]))
        
        summary: Dict[str, Any] = {
            'authors': 0, 'authors_incomplete': 0, 'windows_failed': 0,
            'tweets_returned': 0, 'tweets_new': 0, 'budget_exhausted': False,
        }
        oplog.info("[Twitter] 深度回扫开始 | authors=%d window_hours=%s chunk_hours=%s budget_min=%s",
                   len(aggregated), self.deep_sweep_window_hours, self.deep_sweep_chunk_hours,
                   self.deep_sweep_budget_minutes)
        
        # 最久没抓成功的作者先扫
        ordered = []
        for author, author_subs in aggregated.items():
            last_ok = self._last_success_time(cursor, [sub['id'] for sub in author_subs])
            ordered.append((last_ok.timestamp() if last_ok else 0.0, author, author_subs))
        ordered.sort(key=lambda item: item[0])
        conn.commit()
        
        for _, author, author_subs in ordered:
            if time.time() >= deadline:
                summary['budget_exhausted'] = True
                break
            
            collected: Dict[str, Dict[str, Any]] = {}
            incomplete = False
            for chunk_since, chunk_until in chunks:
                if time.time() >= deadline:
                    incomplete = True
                    break
                try:
                    for tweet in self.fetch_tweets_in_window(author, chunk_since, chunk_until):
                        tweet_id = str(tweet.get("id") or "")
                        if tweet_id:
                            collected[tweet_id] = tweet
                except TwitterApiError as e:
                    if e.status in (401, 402):
                        raise _AbortRun(str(e)) from e
                    summary['windows_failed'] += 1
                    incomplete = True
            
            tweets = list(collected.values())
            summary['tweets_returned'] += len(tweets)
            new_count, _existing_count, new_ids = self._store_author_tweets(
                conn, cursor, author, author_subs, tweets)
            conn.commit()
            self._dispatch_tagging(author, new_ids)
            
            summary['authors'] += 1
            summary['tweets_new'] += new_count
            if incomplete:
                summary['authors_incomplete'] += 1
        
        summary['duration_s'] = round(time.time() - started)
        oplog.info(
            "[Twitter] 深度回扫完成 | authors=%d incomplete=%d windows_failed=%d tweets_returned=%d "
            "tweets_new=%d budget_exhausted=%s duration_s=%d",
            summary['authors'], summary['authors_incomplete'], summary['windows_failed'],
            summary['tweets_returned'], summary['tweets_new'], summary['budget_exhausted'],
            summary['duration_s'],
        )
        return summary
    
    def _update_subscription_status(self, cursor, subscription_id: int,
                                   items_count: int, status: str,
                                   error: str = None):
        """更新订阅状态.

        注意：`last_fetched_at` 用 Python `utcnow()` 显式传参，**不要用 PG 的
        `CURRENT_TIMESTAMP`**。因为部分 worker 进程的 DB 连接 session 时区
        未必是 UTC（部署滞后或第三方 DSN），CURRENT_TIMESTAMP 落到 naive 列时
        会写成服务器本地（HK +8h）。Python aware UTC 经过 psycopg2 全局适配器
        始终被规整为 UTC 进库（详见 backend/utils/datetime_utils.py
        ::install_psycopg2_utc_adapter）。
        """
        # 委托给共享 helper (subscription_status_service), 与 YouTube/Podcast/... 7 源统一.
        from backend.services.subscription_status_service import _update_subscription_status as _shared
        _shared(cursor, subscription_id, items_count, status)
    
    def _record_fetch_history(self, cursor, subscription_id: int,
                             items_fetched: int = 0,
                             items_new: int = 0,
                             items_duplicated: int = 0,
                             status: str = 'success',
                             error_message: str = None,
                             execution_duration_ms: int = None):
        """记录获取历史。订阅已删除时跳过；INSERT 用 SAVEPOINT 隔离，避免单条 FK 失败中止整批事务。"""
        from backend.services.subscription_status_service import _record_fetch_history as _shared
        _shared(
            cursor, subscription_id,
            items_fetched=items_fetched,
            items_new=items_new,
            items_duplicated=items_duplicated,
            status=status,
            error_message=error_message,
            execution_duration_ms=execution_duration_ms,
        )
