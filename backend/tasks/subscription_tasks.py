# -*- coding: utf-8 -*-
"""
订阅相关的Celery任务
"""
import logging
from typing import Optional, Dict, Any

from backend.celery_app import app

logger = logging.getLogger(__name__)


@app.task(
    name='fetch_twitter_subscription',
    bind=True,
    max_retries=3,
    default_retry_delay=60,  # 失败后60秒重试
    time_limit=300,  # 5分钟硬限制
    soft_time_limit=240,  # 4分钟软限制
)
def fetch_twitter_subscription_task(
    self,
    subscription_id: Optional[int] = None,
    user_id: Optional[int] = None,
    force_fetch: bool = False
) -> Dict[str, Any]:
    """
    异步获取Twitter订阅内容（Celery任务）
    
    Args:
        subscription_id: 订阅ID
        user_id: 用户ID
        force_fetch: 是否强制获取
    
    Returns:
        执行结果统计
    """
    try:
        logger.info(f"[Celery Task] Starting Twitter fetch for subscription_id={subscription_id}, "
                   f"user_id={user_id}, force_fetch={force_fetch}")
        
        from backend.services.twitter_subscription_service import TwitterSubscriptionService
        
        twitter_service = TwitterSubscriptionService()
        result = twitter_service.fetch_and_store_tweets(
            subscription_id=subscription_id,
            user_id=user_id,
            force_fetch=force_fetch
        )
        
        logger.info(f"[Celery Task] Twitter fetch completed: {result.get('tweets_new', 0)} new tweets, "
                   f"{result.get('tweets_existing', 0)} existing tweets")
        
        return result
        
    except Exception as exc:
        logger.error(f"[Celery Task] Twitter fetch failed: {str(exc)}", exc_info=True)
        
        # 如果不是最后一次重试，则继续重试
        if self.request.retries < self.max_retries:
            logger.warning(f"[Celery Task] Retrying... (attempt {self.request.retries + 1}/{self.max_retries})")
            raise self.retry(exc=exc)
        else:
            logger.error(f"[Celery Task] Max retries reached, task failed permanently")
            return {
                'success': False,
                'message': f'Failed after {self.max_retries} retries: {str(exc)}',
                'tweets_new': 0,
                'tweets_existing': 0,
                'authors_count': 0,
            }


@app.task(
    name='add_user_to_existing_twitter_content',
    bind=True,
    max_retries=2,
    default_retry_delay=5,
    time_limit=60,
    soft_time_limit=45,
)
def add_user_to_existing_content_task(
    self,
    user_id: int,
    channel_type: str,
    channel_identifier: str,
    subscription_id: Optional[int] = None,
) -> Dict[str, Any]:
    """
    将用户添加到现有 subscription_content 的 user_ids（Celery 任务）

    用于复用：Twitter / YouTube / News 等渠道在库里已有该作者内容时，
    新订阅用户需写入 user_ids，否则列表接口（user_id = ANY(user_ids)）查不到数据。

    Args:
        user_id: 新用户 ID
        channel_type: 渠道类型
        channel_identifier: 作者标识符
        subscription_id: 若提供，在合并 user_ids 后刷新该订阅的 unread_count

    Returns:
        更新结果
    """
    try:
        logger.info(f"[Celery Task] Adding user {user_id} to existing content for {channel_identifier}")
        
        from backend.database.db_manager import DatabaseManager
        from backend.utils.database_config import get_database_config
        
        db_config = get_database_config()
        db_manager = DatabaseManager(db_config)
        conn = db_manager.get_connection()
        cursor = conn.cursor()
        
        # 使用 DISTINCT UNNEST 确保不重复
        cursor.execute("""
            UPDATE subscription_content
            SET user_ids = ARRAY(SELECT DISTINCT UNNEST(user_ids || %s::integer))
            WHERE channel_type = %s 
            AND author_identifier = %s
            AND NOT (%s = ANY(user_ids))
        """, (user_id, channel_type, channel_identifier, user_id))
        
        updated_count = cursor.rowcount

        if subscription_id is not None and updated_count >= 0:
            try:
                cursor.execute(
                    "SELECT update_subscription_unread_count(%s, %s)",
                    (subscription_id, user_id),
                )
                cursor.fetchone()
            except Exception as unread_exc:
                logger.warning(
                    f"[Celery Task] update_subscription_unread_count failed: {unread_exc}",
                    exc_info=True,
                )

        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info(
            f"[Celery Task] Added user {user_id} to {updated_count} existing rows "
            f"from {channel_identifier} ({channel_type})"
        )
        
        return {
            'success': True,
            'updated_count': updated_count,
            'user_id': user_id,
            'channel_identifier': channel_identifier,
        }
        
    except Exception as exc:
        logger.error(f"[Celery Task] Failed to add user to existing content: {str(exc)}", exc_info=True)
        
        if self.request.retries < self.max_retries:
            logger.warning(f"[Celery Task] Retrying... (attempt {self.request.retries + 1}/{self.max_retries})")
            raise self.retry(exc=exc)
        else:
            return {
                'success': False,
                'message': str(exc),
                'updated_count': 0,
            }


@app.task(
    name='smart_wait_and_add_user',
    bind=True,
    max_retries=1,
    time_limit=120,  # 2分钟硬限制
    soft_time_limit=90,
)
def smart_wait_and_add_user_task(
    self,
    user_id: int,
    channel_type: str,
    channel_identifier: str,
    subscription_id: int,
    max_wait_seconds: int = 60
) -> Dict[str, Any]:
    """
    智能等待其他用户的获取完成，然后添加当前用户（Celery任务）
    
    用于处理race condition：两个用户几乎同时订阅同一作者
    
    Args:
        user_id: 当前用户ID
        channel_type: 渠道类型
        channel_identifier: 作者标识符
        subscription_id: 当前订阅ID
        max_wait_seconds: 最大等待时间（秒）
    
    Returns:
        操作结果
    """
    import time
    
    try:
        logger.info(f"[Celery Task] Smart waiting for {channel_identifier}, user={user_id}")
        
        from backend.database.db_manager import DatabaseManager
        from backend.utils.database_config import get_database_config
        
        db_config = get_database_config()
        db_manager = DatabaseManager(db_config)
        check_interval = 5  # 每5秒检查一次
        max_checks = max_wait_seconds // check_interval
        
        for i in range(max_checks):
            time.sleep(check_interval)
            
            conn = db_manager.get_connection()
            cursor = conn.cursor()
            
            # 检查是否有内容了
            cursor.execute("""
                SELECT COUNT(*) FROM subscription_content 
                WHERE channel_type = %s 
                AND author_identifier = %s
            """, (channel_type, channel_identifier))
            
            count = cursor.fetchone()[0]
            cursor.close()
            conn.close()
            
            if count > 0:
                logger.info(f"[Celery Task] Content for {channel_identifier} is now available ({count} tweets). "
                           f"Adding user {user_id} to existing content.")
                
                # 添加用户到现有内容
                conn = db_manager.get_connection()
                cursor = conn.cursor()
                cursor.execute("""
                    UPDATE subscription_content
                    SET user_ids = ARRAY(SELECT DISTINCT UNNEST(user_ids || %s::integer))
                    WHERE channel_type = %s 
                    AND author_identifier = %s
                    AND NOT (%s = ANY(user_ids))
                """, (user_id, channel_type, channel_identifier, user_id))
                
                updated_count = cursor.rowcount
                conn.commit()
                cursor.close()
                conn.close()
                
                logger.info(f"[Celery Task] Added user {user_id} to {updated_count} existing tweets from {channel_identifier}")
                
                return {
                    'success': True,
                    'strategy': 'waited_and_reused',
                    'updated_count': updated_count,
                    'wait_seconds': (i + 1) * check_interval,
                }
            else:
                logger.info(f"[Celery Task] Waiting for content... ({i+1}/{max_checks} checks)")
        
        # 等待超时，没有内容，触发API获取
        logger.warning(f"[Celery Task] Waited {max_wait_seconds}s but no content found for {channel_identifier}. "
                      f"Triggering API fetch.")
        
        from backend.services.twitter_subscription_service import TwitterSubscriptionService
        
        twitter_service = TwitterSubscriptionService()
        result = twitter_service.fetch_and_store_tweets(
            subscription_id=subscription_id,
            user_id=None,
            force_fetch=True
        )
        
        logger.info(f"[Celery Task] Fetch completed after wait: {result.get('tweets_new', 0)} new tweets")
        
        return {
            'success': True,
            'strategy': 'waited_then_fetched',
            'tweets_new': result.get('tweets_new', 0),
            'wait_seconds': max_wait_seconds,
        }
        
    except Exception as exc:
        logger.error(f"[Celery Task] Smart wait failed: {str(exc)}", exc_info=True)
        return {
            'success': False,
            'message': str(exc),
        }


@app.task(
    name='fetch_youtube_subscription',
    bind=True,
    max_retries=3,
    default_retry_delay=60,  # 失败后60秒重试
    time_limit=300,  # 5分钟硬限制
    soft_time_limit=240,  # 4分钟软限制
)
def fetch_youtube_subscription_task(
    self,
    subscription_id: Optional[int] = None,
    user_id: Optional[int] = None,
    force_fetch: bool = False
) -> Dict[str, Any]:
    """
    异步获取YouTube订阅内容（Celery任务）
    
    Args:
        subscription_id: 订阅ID
        user_id: 用户ID
        force_fetch: 是否强制获取
    
    Returns:
        执行结果统计
    """
    try:
        logger.info(f"[Celery Task] Starting YouTube fetch for subscription_id={subscription_id}, "
                   f"user_id={user_id}, force_fetch={force_fetch}")
        
        from backend.services.youtube_subscription_service import YouTubeSubscriptionService
        
        youtube_service = YouTubeSubscriptionService()
        result = youtube_service.fetch_and_store_videos(
            subscription_id=subscription_id,
            user_id=user_id,
            force_fetch=force_fetch
        )
        
        logger.info(f"[Celery Task] YouTube fetch completed: {result.get('videos_new', 0)} new videos, "
                   f"{result.get('videos_existing', 0)} existing videos")
        
        return result
        
    except Exception as exc:
        logger.error(f"[Celery Task] YouTube fetch failed: {str(exc)}", exc_info=True)
        
        # 如果不是最后一次重试，则继续重试
        if self.request.retries < self.max_retries:
            logger.warning(f"[Celery Task] Retrying... (attempt {self.request.retries + 1}/{self.max_retries})")
            raise self.retry(exc=exc)
        else:
            logger.error(f"[Celery Task] Max retries reached, task failed permanently")
            return {
                'success': False,
                'message': f'Failed after {self.max_retries} retries: {str(exc)}',
                'videos_new': 0,
                'videos_existing': 0,
            }
