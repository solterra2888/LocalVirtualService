# -*- coding: utf-8 -*-
"""
EODHD News Feed Service - 将 EODHD 新闻作为 Feed 订阅内容写入 subscription_content

与 EODHDNewsService 的区别：
- EODHDNewsService 将新闻写入 news 表（供 News 模块使用）
- 本服务将新闻写入 subscription_content 表（供 My Feeds 模块使用）
- 两套管线完全独立，互不干扰
"""

import os
import time
from datetime import timedelta
from typing import List, Dict, Any, Optional, Set

from backend.database.db_manager import DatabaseManager
from backend.utils.database_config import get_database_config
from backend.utils.datetime_utils import utcnow
from backend.utils.logger import get_logger
from backend.services.content_adapters.news_adapter import NewsAdapter, determine_source
from backend.services.subscription_content_service import SubscriptionContentService

logger = get_logger("eodhd_news_feed_service")


# 前端可订阅且 EODHD 可拉取的新闻源（与 feedChannelConfig.tsx 中 disabled=false 的条目一致）
AVAILABLE_NEWS_SOURCES = [
    'Yahoo Finance',
    'Seeking Alpha',
    'CNBC',
    'Nasdaq',
    'Fxstreet',
    'Globenewswire',
    'U.Today',
]


class EODHDNewsFeedService:
    """
    EODHD News Feed 服务

    职责：
    1. 从 EODHD API 拉取新闻（复用 EODHDNewsService 的 HTTP 客户端）
    2. 按 source 分发到对应订阅用户
    3. 通过 NewsAdapter + SubscriptionContentService 写入 subscription_content
    """

    def __init__(self):
        db_config = get_database_config()
        self.db_manager = DatabaseManager(db_config)
        self.adapter = NewsAdapter()
        self.content_service = SubscriptionContentService()

    def fetch_and_distribute_news(
        self,
        max_results: int = 200,
        recent_days: int = 1,
    ) -> Dict[str, Any]:
        """
        拉取 EODHD 新闻并分发给订阅了对应 source 的用户。

        流程:
        1. 查询 subscription_list 中 channel_type='news' 的所有订阅
        2. 汇总需要的 source 集合
        3. 从 EODHD API 拉取最新新闻
        4. 按 source 匹配，将新闻写入 subscription_content，user_ids 包含所有订阅了该 source 的用户

        Args:
            max_results: 单次从 EODHD 拉取的最大新闻条数
            recent_days: 拉取最近 N 天的新闻

        Returns:
            执行结果统计
        """
        start_time = time.time()
        logger.info("开始 EODHD News Feed 拉取与分发",
                     max_results=max_results, recent_days=recent_days)

        # Step 1: 查询所有 news 类型订阅
        source_to_users = self._get_news_subscriptions()
        if not source_to_users:
            logger.info("没有任何 news 类型订阅，跳过拉取")
            return {
                'success': True,
                'message': '没有活跃的新闻订阅',
                'articles_fetched': 0,
                'articles_saved': 0,
                'articles_new': 0,
                'sources_matched': 0,
            }

        subscribed_sources = set(source_to_users.keys())
        logger.info(f"发现 {len(subscribed_sources)} 个已订阅的新闻源",
                     sources=list(subscribed_sources))

        # Step 2: 从 EODHD API 拉取新闻.
        # 用 utcnow() 而非 datetime.now()：服务器在 HK (UTC+8)，午夜 HK 时
        # datetime.now() 会给出"明天"的 HK 日期，导致 EODHD 返回空结果直到下次重跑。
        now = utcnow()
        end_date = now.strftime('%Y-%m-%d')
        start_date = (now - timedelta(days=recent_days)).strftime('%Y-%m-%d')

        articles = self._fetch_from_eodhd(
            start_date=start_date,
            end_date=end_date,
            max_results=max_results,
        )

        if not articles:
            logger.warning("EODHD API 未返回任何新闻")
            return {
                'success': True,
                'message': 'API 未返回新闻',
                'articles_fetched': 0,
                'articles_saved': 0,
                'articles_new': 0,
                'sources_matched': 0,
            }

        logger.info(f"从 EODHD API 获取到 {len(articles)} 篇新闻")

        # Step 3: 按 source 匹配并写入
        total_saved = 0
        total_new = 0
        sources_matched: Set[str] = set()
        failed = 0
        new_content_ids: List[int] = []

        for article in articles:
            source = determine_source(article)
            source_lower = source.lower()

            # 找到匹配的订阅（大小写不敏感）
            matched_user_ids: List[int] = []
            for subscribed_source, user_ids in source_to_users.items():
                if subscribed_source.lower() == source_lower:
                    matched_user_ids.extend(user_ids)
                    sources_matched.add(subscribed_source)

            if not matched_user_ids:
                continue

            # 去重 user_ids
            matched_user_ids = list(set(matched_user_ids))

            try:
                content_id, is_new = self.content_service.save_content(
                    channel_type='news',
                    raw_data=article,
                    user_ids=matched_user_ids,
                )
                total_saved += 1
                if is_new:
                    total_new += 1
                    new_content_ids.append(content_id)
            except Exception as e:
                failed += 1
                logger.warning(f"保存新闻失败: {str(e)[:100]}",
                               title=article.get('title', '')[:60])

        # Step 4: 触发新内容的自动打标 + 向量化 (派发为持久化 Celery 消息, 避免 scheduler
        # 进程被杀导致永久漏打标; 派发失败由 backfill_untagged_news_task 周期兜底)
        if new_content_ids:
            try:
                from backend.tasks.file_tasks import trigger_auto_news_processing
                trigger_auto_news_processing.apply_async(
                    args=[new_content_ids], kwargs={'delay': 3}, queue='file_processing'
                )
            except Exception as e:
                logger.warning(
                    f"触发 News 自动处理派发失败 (不阻断主链路, 由 backfill sweep 兜底): {e}; "
                    f"content_ids={new_content_ids[:20]}"
                )

        # Step 5: 更新订阅未读计数
        self._update_unread_counts(source_to_users)

        elapsed_ms = int((time.time() - start_time) * 1000)

        result = {
            'success': True,
            'message': f'成功处理 {total_saved} 篇新闻，新增 {total_new} 篇',
            'articles_fetched': len(articles),
            'articles_saved': total_saved,
            'articles_new': total_new,
            'articles_failed': failed,
            'sources_matched': len(sources_matched),
            'sources_detail': list(sources_matched),
            'execution_duration_ms': elapsed_ms,
        }

        logger.info("EODHD News Feed 拉取与分发完成", **result)
        return result

    def _get_news_subscriptions(self) -> Dict[str, List[int]]:
        """
        查询 subscription_list 中 channel_type='news' 且 enabled=true 的记录，
        返回 { source_name: [user_id, ...] } 映射。

        channel_identifier 存储的就是 source 名称（如 "Yahoo Finance"）。
        """
        conn = self.db_manager.get_connection()
        if not conn:
            return {}

        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT channel_identifier, user_id
                FROM subscription_list
                WHERE channel_type = 'news' AND enabled = true
            """)
            rows = cursor.fetchall()
            cursor.close()
            conn.close()

            source_to_users: Dict[str, List[int]] = {}
            for source, user_id in rows:
                source_to_users.setdefault(source, []).append(user_id)

            return source_to_users

        except Exception as e:
            logger.error(f"查询 news 订阅失败: {e}")
            conn.close()
            return {}

    def _fetch_from_eodhd(
        self,
        start_date: str,
        end_date: str,
        max_results: int = 200,
    ) -> List[Dict[str, Any]]:
        """
        从 EODHD API 拉取新闻。
        复用 EODHDNewsService 的实例来调用 API，避免重复实现 HTTP 逻辑。
        """
        try:
            from backend.services.eodhd_news_service import EODHDNewsService
            eodhd_service = EODHDNewsService()

            all_articles = []
            limit = min(1000, max_results)
            offset = 0

            while len(all_articles) < max_results:
                response = eodhd_service.get_news(
                    start_date=start_date,
                    end_date=end_date,
                    limit=limit,
                    offset=offset,
                )

                batch = eodhd_service._extract_news_from_response(response) if response else []
                if not batch:
                    break

                all_articles.extend(batch)

                if len(batch) < limit:
                    break

                offset += limit
                if offset >= max_results:
                    break

                time.sleep(1)

            return all_articles[:max_results]

        except Exception as e:
            logger.error(f"从 EODHD API 拉取新闻失败: {e}")
            return []

    def _update_unread_counts(self, source_to_users: Dict[str, List[int]]) -> None:
        """批量更新受影响订阅的未读计数"""
        conn = self.db_manager.get_connection()
        if not conn:
            return

        try:
            cursor = conn.cursor()

            for source, user_ids in source_to_users.items():
                for user_id in set(user_ids):
                    try:
                        cursor.execute("""
                            SELECT id FROM subscription_list
                            WHERE user_id = %s AND channel_type = 'news'
                              AND channel_identifier = %s
                        """, (user_id, source))
                        row = cursor.fetchone()
                        if row:
                            cursor.execute(
                                "SELECT update_subscription_unread_count(%s, %s)",
                                (row[0], user_id)
                            )
                    except Exception as e:
                        logger.debug(f"更新未读计数失败: user={user_id}, source={source}, error={e}")

            conn.commit()
            cursor.close()
            conn.close()

        except Exception as e:
            logger.error(f"批量更新未读计数失败: {e}")
            try:
                conn.close()
            except Exception:
                pass

    @staticmethod
    def get_available_sources() -> List[Dict[str, str]]:
        """返回前端可选的新闻源列表"""
        return [
            {'identifier': source, 'name': source}
            for source in AVAILABLE_NEWS_SOURCES
        ]
