# -*- coding: utf-8 -*-
"""
订阅管理API - 管理Twitter、YouTube、知识星球等多渠道订阅
"""

from fastapi import APIRouter, HTTPException, Depends, Query
from typing import Optional, Dict, Any, List
from pydantic import BaseModel, Field
from datetime import datetime
import pytz
import json

from backend.auth.jwt_utils import get_current_user
from backend.schemas.user import TokenData
from backend.database.db_manager import DatabaseManager
from backend.utils.database_config import get_database_config
from backend.utils.logger import get_logger

logger = get_logger("subscriptions_api")

router = APIRouter()


# Pydantic模型
class CreateSubscriptionRequest(BaseModel):
    """创建订阅请求模型"""
    channel_type: str = Field(..., description="频道类型: twitter, youtube, zhishixingqiu, rss, news")
    channel_identifier: str = Field(..., description="频道唯一标识: Twitter用户名、YouTube频道ID等")
    channel_name: Optional[str] = Field(None, description="频道显示名称（可选，Twitter 等会从 API 自动填充）")
    channel_description: Optional[str] = Field(None, description="频道描述")
    enabled: bool = Field(default=True, description="是否启用")
    filter_config: Optional[Dict[str, Any]] = Field(default=None, description="过滤配置JSON")
    metadata: Optional[Dict[str, Any]] = Field(default=None, description="额外元数据")


class UpdateSubscriptionRequest(BaseModel):
    """更新订阅请求模型"""
    channel_name: Optional[str] = None
    channel_description: Optional[str] = None
    enabled: Optional[bool] = None
    filter_config: Optional[Dict[str, Any]] = None
    metadata: Optional[Dict[str, Any]] = None


class SubscriptionResponse(BaseModel):
    """订阅响应模型"""
    id: int
    user_id: int
    channel_type: str
    channel_identifier: str
    channel_name: Optional[str]
    channel_description: Optional[str]
    enabled: bool
    priority: int
    fetch_interval_minutes: int
    max_items_per_fetch: int
    filter_config: Dict[str, Any]
    metadata: Dict[str, Any]
    created_at: datetime
    updated_at: datetime
    last_fetched_at: Optional[datetime]
    next_fetch_at: Optional[datetime]
    total_items_fetched: int
    last_fetch_items_count: int
    consecutive_failures: int
    unread_count: int = 0  # 🔥 新增：未读内容数量


class SubscriptionStatsResponse(BaseModel):
    """订阅统计响应模型"""
    total_subscriptions: int
    enabled_subscriptions: int
    disabled_subscriptions: int
    by_channel_type: Dict[str, int]
    total_items_fetched: int
    recent_failures: int


class FetchHistoryResponse(BaseModel):
    """获取历史响应模型"""
    id: int
    subscription_id: int
    fetch_time: datetime
    execution_duration_ms: Optional[int]
    items_fetched: int
    items_new: int
    items_duplicated: int
    items_filtered: int
    status: str
    error_message: Optional[str]
    error_code: Optional[str]


def get_db_manager():
    """获取数据库管理器实例"""
    db_params = get_database_config()
    return DatabaseManager(db_params)


@router.get("/news-sources")
async def get_news_sources(
    current_user: TokenData = Depends(get_current_user)
):
    """返回可订阅的新闻源列表"""
    from backend.services.eodhd_news_feed_service import EODHDNewsFeedService
    return EODHDNewsFeedService.get_available_sources()


@router.get("", response_model=List[SubscriptionResponse])
@router.get("/", response_model=List[SubscriptionResponse])
async def get_user_subscriptions(
    channel_type: Optional[str] = Query(None, description="筛选频道类型"),
    enabled: Optional[bool] = Query(None, description="筛选启用状态"),
    current_user: TokenData = Depends(get_current_user)
):
    """
    获取当前用户的所有订阅
    
    - **channel_type**: 可选，筛选特定频道类型（twitter, youtube, zhishixingqiu, rss）
    - **enabled**: 可选，筛选启用状态
    """
    user_id = current_user.user_id
    db_manager = get_db_manager()
    
    try:
        conn = db_manager.get_connection()
        cursor = conn.cursor()
        
        # 构建查询条件
        conditions = ["user_id = %s"]
        params = [user_id]
        
        if channel_type:
            conditions.append("channel_type = %s")
            params.append(channel_type)
        
        if enabled is not None:
            conditions.append("enabled = %s")
            params.append(enabled)
        
        query = f"""
            SELECT 
                id, user_id, channel_type, channel_identifier,
                channel_name, channel_description, enabled, priority,
                fetch_interval_minutes, max_items_per_fetch, filter_config,
                metadata, created_at, updated_at, last_fetched_at,
                next_fetch_at, total_items_fetched, last_fetch_items_count,
                consecutive_failures, unread_count
            FROM subscription_list
            WHERE {' AND '.join(conditions)}
            ORDER BY priority DESC, created_at DESC
        """
        
        cursor.execute(query, params)
        rows = cursor.fetchall()
        
        subscriptions = []
        for row in rows:
            subscriptions.append(SubscriptionResponse(
                id=row[0],
                user_id=row[1],
                channel_type=row[2],
                channel_identifier=row[3],
                channel_name=row[4],
                channel_description=row[5],
                enabled=row[6],
                priority=row[7],
                fetch_interval_minutes=row[8],
                max_items_per_fetch=row[9],
                filter_config=row[10] or {},
                metadata=row[11] or {},
                created_at=row[12],
                updated_at=row[13],
                last_fetched_at=row[14],
                next_fetch_at=row[15],
                total_items_fetched=row[16],
                last_fetch_items_count=row[17],
                consecutive_failures=row[18],
                unread_count=row[19] or 0  # 🔥 新增：未读计数
            ))
        
        cursor.close()
        conn.close()
        
        logger.info(f"Retrieved {len(subscriptions)} subscriptions for user {user_id}")
        return subscriptions
        
    except Exception as e:
        logger.error(f"Failed to get subscriptions for user {user_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to retrieve subscriptions: {str(e)}")


@router.get("/{subscription_id}", response_model=SubscriptionResponse)
async def get_subscription(
    subscription_id: int,
    current_user: TokenData = Depends(get_current_user)
):
    """获取单个订阅详情"""
    user_id = current_user.user_id
    db_manager = get_db_manager()
    
    try:
        conn = db_manager.get_connection()
        cursor = conn.cursor()
        
        query = """
            SELECT 
                id, user_id, channel_type, channel_identifier,
                channel_name, channel_description, enabled, priority,
                fetch_interval_minutes, max_items_per_fetch, filter_config,
                metadata, created_at, updated_at, last_fetched_at,
                next_fetch_at, total_items_fetched, last_fetch_items_count,
                consecutive_failures, unread_count
            FROM subscription_list
            WHERE id = %s AND user_id = %s
        """
        
        cursor.execute(query, (subscription_id, user_id))
        row = cursor.fetchone()
        
        if not row:
            raise HTTPException(status_code=404, detail="Subscription not found")
        
        subscription = SubscriptionResponse(
            id=row[0],
            user_id=row[1],
            channel_type=row[2],
            channel_identifier=row[3],
            channel_name=row[4],
            channel_description=row[5],
            enabled=row[6],
            priority=row[7],
            fetch_interval_minutes=row[8],
            max_items_per_fetch=row[9],
            filter_config=row[10] or {},
            metadata=row[11] or {},
            created_at=row[12],
            updated_at=row[13],
            last_fetched_at=row[14],
            next_fetch_at=row[15],
            total_items_fetched=row[16],
            last_fetch_items_count=row[17],
            consecutive_failures=row[18],
            unread_count=row[19] or 0
        )
        
        cursor.close()
        conn.close()
        
        return subscription
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get subscription {subscription_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to retrieve subscription: {str(e)}")


@router.post("/", response_model=SubscriptionResponse)
async def create_subscription(
    request: CreateSubscriptionRequest,
    current_user: TokenData = Depends(get_current_user)
):
    """创建新订阅并自动触发首次内容获取"""
    user_id = current_user.user_id
    db_manager = get_db_manager()
    
    try:
        # 🔥 如果是 News 订阅，验证 source 是否在可选列表中
        if request.channel_type == 'news':
            from backend.services.eodhd_news_feed_service import AVAILABLE_NEWS_SOURCES
            
            available_lower = {s.lower(): s for s in AVAILABLE_NEWS_SOURCES}
            identifier_lower = request.channel_identifier.lower()
            
            if identifier_lower not in available_lower:
                raise HTTPException(
                    status_code=400,
                    detail=f"News source '{request.channel_identifier}' is not available. "
                           f"Available sources: {', '.join(AVAILABLE_NEWS_SOURCES)}"
                )
            
            # 标准化 channel_identifier（使用规范名称）
            request.channel_identifier = available_lower[identifier_lower]
            if not request.channel_name:
                request.channel_name = request.channel_identifier
            
            logger.info(f"News source 验证成功 | source={request.channel_identifier}")
        
        # 🔥 如果是 Twitter 订阅，先验证用户是否存在
        elif request.channel_type == 'twitter':
            from backend.services.twitter_subscription_service import TwitterSubscriptionService
            
            logger.info(f"验证Twitter用户 | username={request.channel_identifier}")
            twitter_service = TwitterSubscriptionService()
            validation_result = twitter_service.validate_twitter_user(request.channel_identifier)
            
            if not validation_result['exists']:
                error_msg = validation_result.get('error', 'User not found')
                logger.warning(f"Twitter用户验证失败 | username={request.channel_identifier} | error={error_msg}")
                raise HTTPException(
                    status_code=400,
                    detail=f"Twitter user '{request.channel_identifier}' does not exist or cannot be accessed. Please check the username."
                )
            
            # 如果验证成功，可以从 user_info 中获取用户的显示名称等信息
            user_info = validation_result.get('user_info', {})
            if not request.channel_name and user_info.get('name'):
                request.channel_name = user_info['name']
            
            logger.info(f"Twitter用户验证成功 | username={request.channel_identifier} | name={request.channel_name}")
        
        # 🔥 如果是 YouTube 订阅，先验证频道是否存在
        elif request.channel_type == 'youtube':
            from backend.services.youtube_subscription_service import (
                YouTubeSubscriptionService,
                normalize_youtube_channel_id,
            )
            
            # 去除 @ 前缀
            clean_identifier = request.channel_identifier.lstrip('@')
            
            logger.info(f"验证YouTube频道 | identifier={clean_identifier}")
            youtube_service = YouTubeSubscriptionService()
            validation_result = youtube_service.validate_youtube_channel(clean_identifier)
            
            if not validation_result['exists']:
                error_msg = validation_result.get('error', 'Channel not found')
                logger.warning(f"YouTube频道验证失败 | identifier={clean_identifier} | error={error_msg}")
                if validation_result.get('network_unreachable'):
                    raise HTTPException(
                        status_code=503,
                        detail=(
                            "服务器无法访问 YouTube（网络不可达）。"
                            "请在服务器的 .env 文件中配置代理，例如：\n"
                            "YOUTUBE_PROXY=http://127.0.0.1:7890\n"
                            "然后重启后端服务（./restart_backend.sh）。"
                        )
                    )
                raise HTTPException(
                    status_code=400,
                    detail=f"YouTube channel '{clean_identifier}' not found or RSS feed unavailable. Please check the username or channel ID."
                )
            
            # 如果验证成功，自动填充频道名称
            if not request.channel_name and validation_result.get('channel_name'):
                request.channel_name = validation_result['channel_name']
            
            # ⚠️ 重要：使用 channel_id 作为 channel_identifier（保持与 author_identifier 一致）
            channel_id = validation_result.get('channel_id')
            if channel_id:
                request.channel_identifier = (
                    normalize_youtube_channel_id(channel_id) or channel_id
                )  # 使用 channel_id 而不是用户名（补全可能被 RSS 截断的 UC 前缀）
                if not request.metadata:
                    request.metadata = {}
                request.metadata['original_username'] = clean_identifier  # 保存原始用户名
                request.metadata['channel_url'] = validation_result.get('channel_url', '')
            
            logger.info(f"YouTube频道验证成功 | identifier={clean_identifier} | name={request.channel_name} | channel_id={channel_id}")
        
        conn = db_manager.get_connection()
        cursor = conn.cursor()
        
        # 检查是否已存在相同订阅
        cursor.execute(
            "SELECT id FROM subscription_list WHERE user_id = %s AND channel_type = %s AND channel_identifier = %s",
            (user_id, request.channel_type, request.channel_identifier)
        )
        existing = cursor.fetchone()
        if existing:
            raise HTTPException(status_code=400, detail="Subscription already exists")
        
        # 插入新订阅（使用默认值；channel_url 已移除，channel_name 可选并由 API 自动填充）
        query = """
            INSERT INTO subscription_list (
                user_id, channel_type, channel_identifier,
                channel_name, channel_description, enabled, filter_config, metadata
            ) VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb)
            RETURNING id, created_at, updated_at
        """
        
        cursor.execute(query, (
            user_id,
            request.channel_type,
            request.channel_identifier,
            request.channel_name,
            request.channel_description,
            request.enabled,
            json.dumps(request.filter_config or {}),
            json.dumps(request.metadata or {})
        ))
        
        result = cursor.fetchone()
        subscription_id, created_at, updated_at = result
        
        logger.info(f"Created subscription {subscription_id} for user {user_id}: {request.channel_type}/{request.channel_identifier}")
        
        # 🔥 优化：检查是否已有其他用户订阅了相同作者
        cursor.execute("""
            SELECT COUNT(*), MIN(created_at) 
            FROM subscription_list 
            WHERE channel_type = %s 
            AND channel_identifier = %s 
            AND user_id != %s
        """, (request.channel_type, request.channel_identifier, user_id))
        
        row = cursor.fetchone()
        other_subscriptions_count = row[0]
        first_subscription_created_at = row[1]
        
        has_existing_content = False
        existing_content_count = 0
        should_wait_for_other_fetch = False
        
        if other_subscriptions_count > 0:
            # 检查第一个订阅是否是最近创建的（5分钟内）
            # 如果是，可能正在获取中，应该等待
            if first_subscription_created_at:
                from datetime import datetime, timezone, timedelta
                # 处理数据库返回的可能是naive datetime的情况
                if first_subscription_created_at.tzinfo is None:
                    # 假设数据库时间是 UTC（根据系统设置）
                    first_subscription_created_at = first_subscription_created_at.replace(tzinfo=timezone.utc)
                
                now_utc = datetime.now(timezone.utc)
                time_since_first = now_utc - first_subscription_created_at
                
                if time_since_first < timedelta(minutes=5):
                    logger.info(f"First subscription for {request.channel_identifier} was created {time_since_first.total_seconds():.0f}s ago, checking for content...")
                    should_wait_for_other_fetch = True
            
            # 检查数据库中是否已有该作者的内容
            cursor.execute("""
                SELECT COUNT(*) FROM subscription_content 
                WHERE channel_type = %s 
                AND author_identifier = %s
            """, (request.channel_type, request.channel_identifier))
            
            existing_content_count = cursor.fetchone()[0]
            has_existing_content = existing_content_count > 0
        
        conn.commit()
        cursor.close()
        conn.close()
        
        # 🎯 智能决策：是否需要调用API（使用Celery异步任务）
        if request.enabled and request.channel_type == 'twitter':
            from backend.tasks.subscription_tasks import (
                fetch_twitter_subscription_task,
                add_user_to_existing_content_task,
                smart_wait_and_add_user_task
            )
            
            if has_existing_content:
                # ✅ 情况1：已有内容，直接复用，将新用户添加到现有内容
                logger.info(f"Reusing existing content for {request.channel_identifier}: "
                           f"{existing_content_count} tweets found. Adding user {user_id} to user_ids.")
                
                # 🚀 使用Celery任务异步执行（不阻塞FastAPI）
                task = add_user_to_existing_content_task.apply_async(
                    args=[user_id, request.channel_type, request.channel_identifier],
                    countdown=0  # 立即执行
                )
                logger.info(f"Celery task scheduled: add_user_to_existing_content (task_id={task.id})")
                
            elif should_wait_for_other_fetch and not has_existing_content:
                # ✅ 情况2：其他用户刚创建订阅，可能正在获取中，等待后再检查
                logger.info(f"Another user recently subscribed to {request.channel_identifier}. "
                           f"Waiting for their fetch to complete before deciding...")
                
                # 🚀 使用Celery任务异步等待并处理（不阻塞FastAPI）
                task = smart_wait_and_add_user_task.apply_async(
                    args=[user_id, request.channel_type, request.channel_identifier, subscription_id],
                    kwargs={'max_wait_seconds': 60},
                    countdown=0
                )
                logger.info(f"Celery task scheduled: smart_wait_and_add_user (task_id={task.id})")
                
            else:
                # ✅ 情况3：没有内容，需要调用API获取
                logger.info(f"No existing content for {request.channel_identifier}. Triggering API fetch.")
                
                # 🚀 使用Celery任务异步执行Twitter API调用（不阻塞FastAPI）
                task = fetch_twitter_subscription_task.apply_async(
                    args=[subscription_id, None, True],  # force_fetch=True
                    countdown=0
                )
                logger.info(f"Celery task scheduled: fetch_twitter_subscription (task_id={task.id})")
        
        # 🎯 YouTube 订阅：同样的智能决策逻辑
        elif request.enabled and request.channel_type == 'youtube':
            from backend.tasks.subscription_tasks import (
                fetch_youtube_subscription_task,
                add_user_to_existing_content_task,
                smart_wait_and_add_user_task
            )
            
            if has_existing_content:
                # ✅ 情况1：已有内容，直接复用
                logger.info(f"Reusing existing YouTube content for {request.channel_identifier}: "
                           f"{existing_content_count} videos found. Adding user {user_id} to user_ids.")
                
                task = add_user_to_existing_content_task.apply_async(
                    args=[user_id, request.channel_type, request.channel_identifier],
                    countdown=0,
                    queue='data_fetching'
                )
                logger.info(f"Celery task scheduled: add_user_to_existing_content (task_id={task.id})")
                
            elif should_wait_for_other_fetch and not has_existing_content:
                # ✅ 情况2：等待其他用户的获取完成
                logger.info(f"Another user recently subscribed to YouTube channel {request.channel_identifier}. "
                           f"Waiting for their fetch to complete...")
                
                task = smart_wait_and_add_user_task.apply_async(
                    args=[user_id, request.channel_type, request.channel_identifier, subscription_id],
                    kwargs={'max_wait_seconds': 60},
                    countdown=0,
                    queue='data_fetching'
                )
                logger.info(f"Celery task scheduled: smart_wait_and_add_user (task_id={task.id})")
                
            else:
                # ✅ 情况3：没有内容，从 RSS 获取
                logger.info(f"No existing content for YouTube channel {request.channel_identifier}. Triggering RSS fetch.")
                
                task = fetch_youtube_subscription_task.apply_async(
                    args=[subscription_id, None, True],  # force_fetch=True
                    countdown=0,
                    queue='data_fetching'  # 使用 data_fetching 队列
                )
                logger.info(f"Celery task scheduled: fetch_youtube_subscription (task_id={task.id})")
        
        return SubscriptionResponse(
            id=subscription_id,
            user_id=user_id,
            channel_type=request.channel_type,
            channel_identifier=request.channel_identifier,
            channel_name=request.channel_name,
            channel_description=request.channel_description,
            enabled=request.enabled,
            priority=0,  # 使用默认值
            fetch_interval_minutes=30,  # 使用默认值
            max_items_per_fetch=20,  # 使用默认值
            filter_config=request.filter_config or {},
            metadata=request.metadata or {},
            created_at=created_at,
            updated_at=updated_at,
            last_fetched_at=None,
            next_fetch_at=None,
            total_items_fetched=0,
            last_fetch_items_count=0,
            consecutive_failures=0,
            unread_count=0  # 🔥 新创建时未读计数为0
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to create subscription: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to create subscription: {str(e)}")


@router.put("/{subscription_id}", response_model=SubscriptionResponse)
async def update_subscription(
    subscription_id: int,
    request: UpdateSubscriptionRequest,
    current_user: TokenData = Depends(get_current_user)
):
    """更新订阅"""
    user_id = current_user.user_id
    db_manager = get_db_manager()
    
    try:
        conn = db_manager.get_connection()
        cursor = conn.cursor()
        
        # 检查订阅是否存在且属于当前用户
        cursor.execute(
            "SELECT id FROM subscription_list WHERE id = %s AND user_id = %s",
            (subscription_id, user_id)
        )
        if not cursor.fetchone():
            raise HTTPException(status_code=404, detail="Subscription not found")
        
        # 构建更新字段
        update_fields = []
        params = []
        
        if request.channel_name is not None:
            update_fields.append("channel_name = %s")
            params.append(request.channel_name)
        
        if request.channel_description is not None:
            update_fields.append("channel_description = %s")
            params.append(request.channel_description)
        
        if request.enabled is not None:
            update_fields.append("enabled = %s")
            params.append(request.enabled)
        
        if request.filter_config is not None:
            update_fields.append("filter_config = %s::jsonb")
            params.append(json.dumps(request.filter_config))
        
        if request.metadata is not None:
            update_fields.append("metadata = %s::jsonb")
            params.append(json.dumps(request.metadata))
        
        if not update_fields:
            raise HTTPException(status_code=400, detail="No fields to update")
        
        update_fields.append("updated_at = CURRENT_TIMESTAMP")
        params.append(subscription_id)
        params.append(user_id)
        
        query = f"""
            UPDATE subscription_list 
            SET {', '.join(update_fields)}
            WHERE id = %s AND user_id = %s
            RETURNING 
                id, user_id, channel_type, channel_identifier,
                channel_name, channel_description, enabled, priority,
                fetch_interval_minutes, max_items_per_fetch, filter_config,
                metadata, created_at, updated_at, last_fetched_at,
                next_fetch_at, total_items_fetched, last_fetch_items_count,
                consecutive_failures, unread_count
        """
        
        cursor.execute(query, params)
        row = cursor.fetchone()
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info(f"Updated subscription {subscription_id} for user {user_id}")
        
        return SubscriptionResponse(
            id=row[0],
            user_id=row[1],
            channel_type=row[2],
            channel_identifier=row[3],
            channel_name=row[4],
            channel_description=row[5],
            enabled=row[6],
            priority=row[7],
            fetch_interval_minutes=row[8],
            max_items_per_fetch=row[9],
            filter_config=row[10] or {},
            metadata=row[11] or {},
            created_at=row[12],
            updated_at=row[13],
            last_fetched_at=row[14],
            next_fetch_at=row[15],
            total_items_fetched=row[16],
            last_fetch_items_count=row[17],
            consecutive_failures=row[18],
            unread_count=row[19] or 0
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to update subscription {subscription_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to update subscription: {str(e)}")


@router.delete("/{subscription_id}")
async def delete_subscription(
    subscription_id: int,
    current_user: TokenData = Depends(get_current_user)
):
    """删除订阅"""
    user_id = current_user.user_id
    db_manager = get_db_manager()
    
    try:
        conn = db_manager.get_connection()
        cursor = conn.cursor()
        
        # 检查订阅是否存在且属于当前用户
        cursor.execute(
            "SELECT id FROM subscription_list WHERE id = %s AND user_id = %s",
            (subscription_id, user_id)
        )
        if not cursor.fetchone():
            raise HTTPException(status_code=404, detail="Subscription not found")
        
        # 删除订阅
        cursor.execute(
            "DELETE FROM subscription_list WHERE id = %s AND user_id = %s",
            (subscription_id, user_id)
        )
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info(f"Deleted subscription {subscription_id} for user {user_id}")
        return {"success": True, "message": "Subscription deleted successfully"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to delete subscription {subscription_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to delete subscription: {str(e)}")


@router.get("/stats/overview", response_model=SubscriptionStatsResponse)
async def get_subscription_stats(
    current_user: TokenData = Depends(get_current_user)
):
    """获取订阅统计数据"""
    user_id = current_user.user_id
    db_manager = get_db_manager()
    
    try:
        conn = db_manager.get_connection()
        cursor = conn.cursor()
        
        # 总订阅数
        cursor.execute(
            "SELECT COUNT(*) FROM subscription_list WHERE user_id = %s",
            (user_id,)
        )
        total_subscriptions = cursor.fetchone()[0]
        
        # 启用的订阅数
        cursor.execute(
            "SELECT COUNT(*) FROM subscription_list WHERE user_id = %s AND enabled = true",
            (user_id,)
        )
        enabled_subscriptions = cursor.fetchone()[0]
        
        # 按频道类型统计
        cursor.execute(
            """
            SELECT channel_type, COUNT(*) 
            FROM subscription_list 
            WHERE user_id = %s 
            GROUP BY channel_type
            """,
            (user_id,)
        )
        by_channel_type = dict(cursor.fetchall())
        
        # 总获取数量
        cursor.execute(
            "SELECT COALESCE(SUM(total_items_fetched), 0) FROM subscription_list WHERE user_id = %s",
            (user_id,)
        )
        total_items_fetched = cursor.fetchone()[0]
        
        # 最近失败数
        cursor.execute(
            "SELECT COUNT(*) FROM subscription_list WHERE user_id = %s AND consecutive_failures > 0",
            (user_id,)
        )
        recent_failures = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        return SubscriptionStatsResponse(
            total_subscriptions=total_subscriptions,
            enabled_subscriptions=enabled_subscriptions,
            disabled_subscriptions=total_subscriptions - enabled_subscriptions,
            by_channel_type=by_channel_type,
            total_items_fetched=total_items_fetched,
            recent_failures=recent_failures
        )
        
    except Exception as e:
        logger.error(f"Failed to get subscription stats: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to retrieve statistics: {str(e)}")


@router.get("/{subscription_id}/history", response_model=List[FetchHistoryResponse])
async def get_subscription_history(
    subscription_id: int,
    limit: int = Query(default=50, ge=1, le=200, description="返回记录数量"),
    current_user: TokenData = Depends(get_current_user)
):
    """获取订阅的获取历史记录"""
    user_id = current_user.user_id
    db_manager = get_db_manager()
    
    try:
        conn = db_manager.get_connection()
        cursor = conn.cursor()
        
        # 检查订阅是否存在且属于当前用户
        cursor.execute(
            "SELECT id FROM subscription_list WHERE id = %s AND user_id = %s",
            (subscription_id, user_id)
        )
        if not cursor.fetchone():
            raise HTTPException(status_code=404, detail="Subscription not found")
        
        # 获取历史记录
        query = """
            SELECT 
                id, subscription_id, fetch_time, execution_duration_ms,
                items_fetched, items_new, items_duplicated, items_filtered,
                status, error_message, error_code
            FROM subscription_fetch_history
            WHERE subscription_id = %s
            ORDER BY fetch_time DESC
            LIMIT %s
        """
        
        cursor.execute(query, (subscription_id, limit))
        rows = cursor.fetchall()
        
        history = []
        for row in rows:
            history.append(FetchHistoryResponse(
                id=row[0],
                subscription_id=row[1],
                fetch_time=row[2],
                execution_duration_ms=row[3],
                items_fetched=row[4],
                items_new=row[5],
                items_duplicated=row[6],
                items_filtered=row[7],
                status=row[8],
                error_message=row[9],
                error_code=row[10]
            ))
        
        cursor.close()
        conn.close()
        
        return history
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get subscription history: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to retrieve history: {str(e)}")


# Test Fetch 功能已弃用
# 原因：新架构在创建订阅时自动处理内容获取（智能复用或调用API）
# Scheduler 会定期自动更新内容，无需手动触发
# 如需强制刷新，可以使用 Scheduler 的立即执行功能


@router.get("/tasks/{task_id}/status")
async def get_subscription_task_status(
    task_id: str,
    current_user: TokenData = Depends(get_current_user)
):
    """
    查询订阅相关Celery任务的状态
    
    Args:
        task_id: Celery任务ID
    
    Returns:
        任务状态信息
    """
    try:
        from celery.result import AsyncResult
        from backend.celery_app import app as celery_app
        
        task = AsyncResult(task_id, app=celery_app)
        
        response = {
            'task_id': task_id,
            'state': task.state,  # PENDING, STARTED, SUCCESS, FAILURE, RETRY, REVOKED
            'ready': task.ready(),
            'successful': task.successful() if task.ready() else None,
            'failed': task.failed() if task.ready() else None,
        }
        
        if task.ready():
            if task.successful():
                response['result'] = task.result
            elif task.failed():
                response['error'] = str(task.info)
        else:
            # 任务还在运行中
            response['info'] = task.info if task.info else None
        
        return response
        
    except Exception as e:
        logger.error(f"Failed to get task status for {task_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get task status: {str(e)}")


@router.get("/{subscription_id}/contents")
async def get_subscription_contents(
    subscription_id: int,
    limit: int = Query(default=50, le=200, description="每页条数"),
    offset: int = Query(default=0, ge=0, description="偏移量"),
    only_unread: bool = Query(default=False, description="仅未读"),
    current_user: TokenData = Depends(get_current_user)
):
    """
    获取订阅的内容列表
    """
    user_id = current_user.user_id
    db_manager = get_db_manager()
    
    try:
        conn = db_manager.get_connection()
        cursor = conn.cursor()
        
        # 验证订阅所有权
        cursor.execute("""
            SELECT id, user_id, channel_type, channel_identifier
            FROM subscription_list
            WHERE id = %s AND user_id = %s
        """, (subscription_id, user_id))
        
        result = cursor.fetchone()
        if not result:
            raise HTTPException(status_code=404, detail="Subscription not found")
        
        # 获取订阅的作者标识
        channel_type = result[2]
        channel_identifier = result[3]
        
        # 构建查询条件：按 author_identifier 而不是 subscription_id
        # 这样同一作者的所有内容对所有订阅者可见
        where_conditions = [
            "channel_type = %s",
            "author_identifier = %s",
            "%s = ANY(user_ids)"
        ]
        params = [channel_type, channel_identifier, user_id]
        
        if only_unread:
            where_conditions.append(
                f"(read_status->>'{user_id}' IS NULL OR (read_status->>'{user_id}')::jsonb->>'is_read' = 'false')"
            )
        
        where_clause = " AND ".join(where_conditions)
        
        # 查询内容
        query = f"""
            SELECT 
                id,
                channel_type,
                content_id,
                title,
                content,
                author_name,
                author_identifier,
                url,
                media_urls,
                published_at,
                fetched_at,
                engagement_count,
                view_count,
                metadata,
                read_status,
                is_retweet,
                retweeted_author,
                retweeted_content,
                retweeted_url,
                is_quote,
                quoted_author,
                quoted_content,
                quoted_url,
                is_reply,
                in_reply_to_id,
                in_reply_to_username,
                in_reply_to_user_id,
                heat_level,
                ((transcript IS NOT NULL AND transcript != '') OR transcript_data IS NOT NULL) AS has_transcript
            FROM subscription_content
            WHERE {where_clause}
            ORDER BY published_at DESC
            LIMIT %s OFFSET %s
        """
        
        params.extend([limit, offset])
        cursor.execute(query, params)
        rows = cursor.fetchall()
        
        # 获取总数
        count_query = f"""
            SELECT COUNT(*)
            FROM subscription_content
            WHERE {where_clause}
        """
        cursor.execute(count_query, params[:-2])  # 不包括 limit 和 offset
        total = cursor.fetchone()[0]
        
        # 格式化结果
        contents = []
        for row in rows:
            user_read_status = row[14].get(str(user_id), {}) if row[14] else {}
            
            contents.append({
                'id': row[0],
                'channel_type': row[1],
                'content_id': row[2],
                'title': row[3],
                'content': row[4],
                'author_name': row[5],
                'author_identifier': row[6],
                'url': row[7],
                'media_urls': row[8],
                'published_at': row[9].isoformat() if row[9] else None,
                'fetched_at': row[10].isoformat() if row[10] else None,
                'engagement_count': row[11],
                'view_count': row[12],
                'metadata': row[13],
                'is_read': user_read_status.get('is_read', False),
                'read_at': user_read_status.get('read_at'),
                # Twitter 特定字段
                'is_retweet': row[15],
                'retweeted_author': row[16],
                'retweeted_content': row[17],
                'retweeted_url': row[18],
                'is_quote': row[19],
                'quoted_author': row[20],
                'quoted_content': row[21],
                'quoted_url': row[22],
                # Reply 特定字段
                'is_reply': row[23],
                'in_reply_to_id': row[24],
                'in_reply_to_username': row[25],
                'in_reply_to_user_id': row[26],
                # 热度等级
                'heat_level': row[27],
                # YouTube: 仅返回是否有 transcript（不返回正文，减轻加载）
                'has_transcript': bool(row[28]) if row[28] is not None else False,
            })
        
        cursor.close()
        conn.close()
        
        logger.info(f"Retrieved {len(contents)} contents for subscription {subscription_id}")
        
        # 直接返回数据对象，前端会自动包装
        return {
            "contents": contents,
            "total": total,
            "limit": limit,
            "offset": offset
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching contents for subscription {subscription_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/contents/{content_id}/mark-read")
async def mark_content_as_read(
    content_id: int,
    is_read: bool = True,
    current_user: TokenData = Depends(get_current_user)
):
    """
    标记内容为已读/未读，并更新订阅的未读计数
    """
    user_id = current_user.user_id
    
    try:
        from backend.services.subscription_content_service import SubscriptionContentService
        
        service = SubscriptionContentService()
        success = service.mark_as_read(content_id, user_id, is_read)
        
        if success:
            logger.info(f"Marked content {content_id} as {'read' if is_read else 'unread'} for user {user_id}")
            
            # 🔥 更新订阅的未读计数
            try:
                db_manager = get_db_manager()
                conn = db_manager.get_connection()
                cursor = conn.cursor()
                
                # 获取内容的 channel_type 和 author_identifier
                cursor.execute(
                    "SELECT channel_type, author_identifier FROM subscription_content WHERE id = %s",
                    (content_id,)
                )
                row = cursor.fetchone()
                
                if row:
                    channel_type, author_identifier = row
                    
                    # 查找该用户对应的订阅ID
                    cursor.execute(
                        "SELECT id FROM subscription_list WHERE user_id = %s AND channel_type = %s AND channel_identifier = %s",
                        (user_id, channel_type, author_identifier)
                    )
                    sub_row = cursor.fetchone()
                    
                    if sub_row:
                        subscription_id = sub_row[0]
                        # 调用数据库函数更新未读计数
                        cursor.execute(
                            "SELECT update_subscription_unread_count(%s, %s)",
                            (subscription_id, user_id)
                        )
                        new_unread_count = cursor.fetchone()[0]
                        conn.commit()
                        
                        logger.info(f"Updated unread count for subscription {subscription_id}: {new_unread_count}")
                
                cursor.close()
                conn.close()
            except Exception as e:
                logger.error(f"Failed to update unread count: {str(e)}")
                # 不影响主流程，继续返回成功
            
            # 直接返回数据对象
            return {"content_id": content_id, "is_read": is_read}
        else:
            raise HTTPException(status_code=404, detail="Content not found")
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error marking content {content_id} as read: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/contents/tweet/{tweet_id}")
async def get_tweet_by_id(
    tweet_id: str,
    user_id: int = Query(..., description="用户ID"),
    current_user: TokenData = Depends(get_current_user)
):
    """
    根据推文 ID 获取推文详情（用于显示被回复的原推文）
    
    Args:
        tweet_id: 推文的 content_id（Twitter 的 tweet_id）
        user_id: 用户ID
    
    Returns:
        推文详情，如果不存在则返回 null
    """
    try:
        db_config = get_database_config()
        db_manager = DatabaseManager(db_conn_params=db_config)
        conn = db_manager.get_connection()
        cursor = conn.cursor()
        
        # 查询推文详情
        query = """
            SELECT 
                id,
                url,
                fetched_at,
                channel_type,
                content_id,
                title,
                content,
                author_identifier,
                author_name,
                media_urls,
                engagement_count,
                view_count,
                metadata,
                published_at,
                is_retweet,
                retweeted_author,
                retweeted_content,
                retweeted_url,
                is_quote,
                quoted_author,
                quoted_content,
                quoted_url,
                is_reply,
                in_reply_to_id,
                in_reply_to_username,
                in_reply_to_user_id,
                heat_level
            FROM subscription_content
            WHERE channel_type = 'twitter' AND content_id = %s
            LIMIT 1
        """
        
        cursor.execute(query, (tweet_id,))
        row = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        if not row:
            # 推文不在数据库中，返回 null
            logger.info(f"Tweet {tweet_id} not found in database")
            return None
        
        # 构建响应（移除了 subscription_id 字段）
        content = {
            'id': row[0],
            'url': row[1],
            'fetched_at': row[2].isoformat() if row[2] else None,
            'channel_type': row[3],
            'content_id': row[4],
            'title': row[5],
            'content': row[6],
            'author_identifier': row[7],
            'author_name': row[8],
            'media_urls': row[9] or [],
            'engagement_count': row[10] or 0,
            'view_count': row[11] or 0,
            'metadata': row[12] or {},
            'published_at': row[13].isoformat() if row[13] else None,
            'is_retweet': row[14] or False,
            'retweeted_author': row[15],
            'retweeted_content': row[16],
            'retweeted_url': row[17],
            'is_quote': row[18] or False,
            'quoted_author': row[19],
            'quoted_content': row[20],
            'quoted_url': row[21],
            'is_reply': row[22] or False,
            'in_reply_to_id': row[23],
            'in_reply_to_username': row[24],
            'in_reply_to_user_id': row[25],
            'heat_level': row[26] or 2,
        }
        
        logger.info(f"Retrieved tweet {tweet_id} for user {user_id}")
        return content
        
    except Exception as e:
        logger.error(f"Error retrieving tweet {tweet_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/contents/{content_db_id}/transcribe")
async def trigger_youtube_asr_transcription(
    content_db_id: int,
    current_user: TokenData = Depends(get_current_user)
):
    """
    对 Feed 中没有字幕的 YouTube 视频，触发 Fun-ASR 转写任务。
    任务进度可通过 GET /api/files/tasks/{task_id}/status 查询。
    """
    user_id = current_user.user_id
    db_manager = get_db_manager()

    try:
        conn = db_manager.get_connection()
        cursor = conn.cursor()

        # 验证：内容存在、属于该用户、是 YouTube 类型
        cursor.execute("""
            SELECT id, channel_type, content_id, transcript
            FROM subscription_content
            WHERE id = %s AND %s = ANY(user_ids)
        """, (content_db_id, user_id))
        row = cursor.fetchone()
        cursor.close()
        conn.close()

        if not row:
            raise HTTPException(status_code=404, detail="Content not found")

        _, channel_type, video_id, existing_transcript = row

        if channel_type != 'youtube':
            raise HTTPException(
                status_code=400,
                detail="Only YouTube content can be transcribed via ASR"
            )

        video_url = f"https://www.youtube.com/watch?v={video_id}"

        # 派发 Celery 任务
        from backend.tasks.file_tasks import transcribe_youtube_feed_task
        task = transcribe_youtube_feed_task.apply_async(
            args=(content_db_id, video_url, video_id),
            queue='file_processing'
        )

        logger.info(
            f"YouTube ASR task dispatched: content_db_id={content_db_id}, "
            f"video_id={video_id}, task_id={task.id}, user_id={user_id}"
        )

        return {
            "task_id": task.id,
            "video_id": video_id,
            "content_db_id": content_db_id,
            "message": "ASR transcription started. Use task_id to poll progress."
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to trigger YouTube ASR: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
