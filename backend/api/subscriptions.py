# -*- coding: utf-8 -*-
"""
订阅管理API - 管理Twitter、YouTube、知识星球等多渠道订阅
"""

from fastapi import APIRouter, HTTPException, Depends, Query
from fastapi.responses import Response
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
from backend.utils.datetime_utils import ensure_utc
from backend.utils.weibo_image_proxy import fetch_weibo_cdn_image, is_weibo_cdn_url

logger = get_logger("subscriptions_api")

router = APIRouter()


def _parse_weibo_uid(raw: str) -> str:
    """解析微博 UID：支持纯数字 UID 与多种主页链接格式。无法解析返回 ''。"""
    if not raw:
        return ""
    s = raw.strip()
    if s.isdigit():
        return s
    s = s.split("?")[0].split("#")[0].rstrip("/")
    import re
    m = re.search(r"(?:weibo\.com|weibo\.cn)/(?:u|profile)/(\d+)", s)
    if m:
        return m.group(1)
    m = re.search(r"weibo\.com/(\d+)", s)
    if m:
        return m.group(1)
    m = re.search(r"(\d{6,})", s)
    if m:
        return m.group(1)
    return ""


# Pydantic模型
class CreateSubscriptionRequest(BaseModel):
    """创建订阅请求模型"""
    channel_type: str = Field(..., description="频道类型: twitter, youtube, zhishixingqiu, weibo, substack, rss, news, podcast")
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
    channel_avatar: Optional[str] = None  # 频道头像 URL
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
    sort_order: Optional[int] = None  # 显示排序（每用户独立，越小越靠前）


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


@router.get("/proxy-image", summary="代理微博 CDN 图片（防盗链绕过）")
async def proxy_weibo_image(
    url: str = Query(..., description="sinaimg.cn 图片 URL"),
):
    """浏览器直连微博 CDN 会因 Referer / UA 被拒；由服务端带 weibo.com Referer 拉取。"""
    if not is_weibo_cdn_url(url):
        raise HTTPException(status_code=400, detail="Only HTTPS sinaimg.cn image URLs are allowed")
    try:
        data, content_type = fetch_weibo_cdn_image(url)
    except Exception as e:
        logger.warning("proxy-image fetch failed | url=%s | err=%s", url[:160], e)
        raise HTTPException(status_code=502, detail="Failed to fetch image") from e
    return Response(
        content=data,
        media_type=content_type,
        headers={"Cache-Control": "public, max-age=86400"},
    )


@router.get("/news-sources")
async def get_news_sources(
    current_user: TokenData = Depends(get_current_user)
):
    """返回可订阅的新闻源列表"""
    from backend.services.eodhd_news_feed_service import EODHDNewsFeedService
    return EODHDNewsFeedService.get_available_sources()


@router.get("/zsxq-groups")
async def get_zsxq_groups(
    current_user: TokenData = Depends(get_current_user)
):
    """返回可供用户订阅的知识星球列表（管理员已开启 enabled=true 的星球）。"""
    from backend.services.zsxq_credential_service import ZsxqCredentialService
    groups = ZsxqCredentialService().list_groups(enabled_only=True)
    return [
        {
            "group_id": g["group_id"],
            "group_name": g["group_name"],
        }
        for g in groups
    ]


@router.get("/substack-publications")
async def get_substack_publications(
    current_user: TokenData = Depends(get_current_user)
):
    """返回可供用户订阅的 Substack publication 列表（订阅源并集中 enabled=true）。"""
    from backend.services.substack_credential_service import SubstackCredentialService
    pubs = SubstackCredentialService().list_publications(enabled_only=True)
    return [
        {
            "publication_url": p["publication_url"],
            "publication_name": p["publication_name"] or p["publication_url"],
            "is_subscribed": p.get("is_subscribed", False),
            "is_followed": p.get("is_followed", False),
        }
        for p in pubs
    ]


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
                channel_name, channel_description, channel_avatar, enabled, priority,
                fetch_interval_minutes, max_items_per_fetch, filter_config,
                metadata, created_at, updated_at, last_fetched_at,
                next_fetch_at, total_items_fetched, last_fetch_items_count,
                consecutive_failures, unread_count, sort_order
            FROM subscription_list
            WHERE {' AND '.join(conditions)}
            ORDER BY COALESCE(sort_order, 999999) ASC, priority DESC, created_at DESC
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
                channel_avatar=row[6],
                enabled=row[7],
                priority=row[8],
                fetch_interval_minutes=row[9],
                max_items_per_fetch=row[10],
                filter_config=row[11] or {},
                metadata=row[12] or {},
                created_at=row[13],
                updated_at=row[14],
                last_fetched_at=row[15],
                next_fetch_at=row[16],
                total_items_fetched=row[17],
                last_fetch_items_count=row[18],
                consecutive_failures=row[19],
                unread_count=row[20] or 0,  # 🔥 新增：未读计数
                sort_order=row[21]
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
                channel_name, channel_description, channel_avatar, enabled, priority,
                fetch_interval_minutes, max_items_per_fetch, filter_config,
                metadata, created_at, updated_at, last_fetched_at,
                next_fetch_at, total_items_fetched, last_fetch_items_count,
                consecutive_failures, unread_count, sort_order
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
            channel_avatar=row[6],
            enabled=row[7],
            priority=row[8],
            fetch_interval_minutes=row[9],
            max_items_per_fetch=row[10],
            filter_config=row[11] or {},
            metadata=row[12] or {},
            created_at=row[13],
            updated_at=row[14],
            last_fetched_at=row[15],
            next_fetch_at=row[16],
            total_items_fetched=row[17],
            last_fetch_items_count=row[18],
            consecutive_failures=row[19],
            unread_count=row[20] or 0,
            sort_order=row[21]
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
    channel_avatar = None  # 初始化头像变量

    try:
        # 🔥 如果是 News 订阅，验证 source 是否在可选列表中
        if request.channel_type == 'news':
            from backend.services.eodhd_news_feed_service import AVAILABLE_NEWS_SOURCES
            from backend.services.cctv_news_feed_service import CCTV_CHANNEL_IDENTIFIER

            # CCTV 由独立服务处理；EODHD 源由 AVAILABLE_NEWS_SOURCES 决定
            all_available = list(AVAILABLE_NEWS_SOURCES) + [CCTV_CHANNEL_IDENTIFIER]
            available_lower = {s.lower(): s for s in all_available}
            identifier_lower = request.channel_identifier.lower()

            if identifier_lower not in available_lower:
                raise HTTPException(
                    status_code=400,
                    detail=f"News source '{request.channel_identifier}' is not available. "
                           f"Available sources: {', '.join(all_available)}"
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

            # 如果验证成功，可以从 user_info 中获取用户的显示名称和头像等信息
            user_info = validation_result.get('user_info', {})
            if not request.channel_name and user_info.get('name'):
                request.channel_name = user_info['name']

            # 获取用户头像 URL
            channel_avatar = user_info.get('profilePicture')

            logger.info(f"Twitter用户验证成功 | username={request.channel_identifier} | name={request.channel_name} | avatar={channel_avatar}")
        
        # 🔥 如果是 YouTube 订阅，先验证频道是否存在
        elif request.channel_type == 'youtube':
            from backend.services.youtube_subscription_service import (
                YouTubeSubscriptionService,
                normalize_youtube_channel_id,
            )

            # ⚠️ 注意：把原始输入（带 @ 与否）原样传给 service，
            # service 会按 had_at_prefix 决定走 @handle 页面解析还是 ?user= RSS。
            # 见 youtube_subscription_service.validate_youtube_channel 文档。
            raw_identifier = request.channel_identifier.strip()
            clean_identifier = raw_identifier.lstrip('@')
            channel_avatar = None  # YouTube 暂不获取头像

            logger.info(f"验证YouTube频道 | identifier={raw_identifier}")
            youtube_service = YouTubeSubscriptionService()
            validation_result = youtube_service.validate_youtube_channel(raw_identifier)

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

        # 🔥 如果是 Podcast 订阅，验证 RSS Feed URL 是否有效
        elif request.channel_type == 'podcast':
            from backend.services.podcast_subscription_service import PodcastSubscriptionService

            feed_url = request.channel_identifier.strip()

            # 基础 URL 格式校验
            if not feed_url.startswith(('http://', 'https://')):
                raise HTTPException(
                    status_code=400,
                    detail="Podcast channel_identifier 必须是有效的 RSS Feed URL（以 http:// 或 https:// 开头）"
                )

            logger.info(f"验证 Podcast RSS Feed | url={feed_url}")
            validation_result = PodcastSubscriptionService.validate_podcast_feed(feed_url)

            if not validation_result.get('valid'):
                error_msg = validation_result.get('error', 'Invalid RSS Feed')
                logger.warning(f"Podcast Feed 验证失败 | url={feed_url} | error={error_msg}")
                raise HTTPException(
                    status_code=400,
                    detail=f"Podcast RSS Feed 验证失败: {error_msg}"
                )

            # 自动填充频道名称
            if not request.channel_name and validation_result.get('channel_name'):
                request.channel_name = validation_result['channel_name']

            if not request.channel_description and validation_result.get('description'):
                request.channel_description = validation_result['description']

            if not request.metadata:
                request.metadata = {}
            request.metadata['episode_count'] = validation_result.get('episode_count', 0)
            request.metadata['has_audio'] = validation_result.get('has_audio', False)

            logger.info(
                f"Podcast Feed 验证成功 | name={request.channel_name} | "
                f"episodes={validation_result.get('episode_count', 0)}"
            )

        # 🔥 如果是知识星球订阅，校验 group_id 是否在 zsxq_groups 中
        elif request.channel_type == 'zhishixingqiu':
            from backend.services.zsxq_credential_service import ZsxqCredentialService
            zsxq_svc = ZsxqCredentialService()
            groups = zsxq_svc.list_groups(enabled_only=True)
            group_map = {g["group_id"]: g["group_name"] for g in groups}

            group_id = request.channel_identifier.strip()
            if group_id not in group_map:
                raise HTTPException(
                    status_code=400,
                    detail=f"知识星球 group_id '{group_id}' 不在可订阅列表中，请联系管理员添加",
                )
            # 自动填充星球名
            if not request.channel_name:
                request.channel_name = group_map[group_id]

            logger.info(f"知识星球验证成功 | group_id={group_id} | name={request.channel_name}")

        # 🔥 如果是微博订阅，解析 UID、校验博主、回填昵称头像、upsert weibo_accounts
        elif request.channel_type == 'weibo':
            from backend.services.weibo_credential_service import WeiboCredentialService
            uid = _parse_weibo_uid(request.channel_identifier)
            if not uid:
                raise HTTPException(
                    status_code=400,
                    detail="无法解析微博 UID，请输入博主 UID 或主页链接（如 https://weibo.com/u/1234567890）",
                )
            request.channel_identifier = uid  # 标准化为纯 uid

            import os
            weibo_svc = WeiboCredentialService()
            cred = weibo_svc.get_first_valid_cookie()
            if cred:
                cred_id, cookie = cred
            else:
                # 无登录凭证：回退到 WEIBO_COOKIE env，再无则访客模式（crawl4weibo 自动获取，需 playwright）
                cred_id = None
                cookie = os.getenv("WEIBO_COOKIE", "").strip() or None

            # 稳健导入 UserNotFoundError（crawl4weibo 异常路径可能在不同子模块）
            try:
                from crawl4weibo.exceptions import UserNotFoundError as _WBUserNotFound
            except ImportError:
                try:
                    from crawl4weibo.exceptions.base import UserNotFoundError as _WBUserNotFound
                except ImportError:
                    _WBUserNotFound = ()  # type: ignore[assignment]

            # 校验博主：有 cookie 用登录态；无 cookie 走访客模式（crawl4weibo Playwright 自动获取访客 Cookie）
            try:
                from crawl4weibo import WeiboClient
                from backend.services.weibo_subscription_service import _VISITOR_COOKIE_PATH
                if cookie:
                    client = WeiboClient(
                        cookies=cookie,
                        use_browser_cookies=False,
                        auto_fetch_cookies=False,
                        log_level="WARNING",
                    )
                else:
                    client = WeiboClient(
                        use_browser_cookies=True,
                        auto_fetch_cookies=True,
                        cookie_storage_path=_VISITOR_COOKIE_PATH,
                        browser_headless=True,
                        log_level="WARNING",
                    )
                user = client.get_user_by_uid(uid, use_proxy=False)
            except _WBUserNotFound:
                raise HTTPException(status_code=400, detail=f"微博用户 '{uid}' 不存在或为私密账号")
            except Exception as e:
                # 访客模式失败（如 playwright/chromium 未装）时退化为 uid 作昵称，不阻塞订阅
                logger.warning(f"微博用户 {uid} 校验失败（退化为 uid 作昵称）: {e}")
                user = None

            if user is not None:
                screen_name = getattr(user, "screen_name", "") or uid
                avatar = getattr(user, "avatar_url", "") or ""
            else:
                screen_name = uid
                avatar = ""
            # 微博 avatar URL 带 KID/Expires/ssig 签名 query，几天即过期；只保留纯路径长期可访问
            avatar = avatar.split("?")[0]
            if not request.channel_name:
                request.channel_name = screen_name
            channel_avatar = avatar or None

            # 订阅时自动建立博主元数据（不覆盖增量锚点）
            weibo_svc.upsert_account(uid, screen_name, avatar=avatar, credential_id=cred_id)

            _auth_mode = f"凭证={cred_id}" if cred_id else "访客模式"
            logger.info(f"微博验证成功 | uid={uid} | name={screen_name} | {_auth_mode}")

        # 🔥 如果是 Substack 订阅，校验 publication_url 是否在 substack_publications 并集中
        elif request.channel_type == 'substack':
            from backend.services.substack_credential_service import (
                SubstackCredentialService,
                normalize_publication_url,
            )
            try:
                pub_url = normalize_publication_url(request.channel_identifier.strip())
            except ValueError:
                raise HTTPException(
                    status_code=400,
                    detail="Substack channel_identifier 必须是有效的 publication URL（如 https://citrini.substack.com）",
                )
            request.channel_identifier = pub_url  # 标准化为根 URL

            pubs = SubstackCredentialService().list_publications(enabled_only=True)
            pub_map = {p["publication_url"]: p["publication_name"] for p in pubs}
            if pub_url not in pub_map:
                raise HTTPException(
                    status_code=400,
                    detail=f"Substack publication '{pub_url}' 不在可订阅列表中，请联系管理员同步订阅源",
                )
            if not request.channel_name:
                request.channel_name = pub_map[pub_url]
            logger.info(f"Substack 验证成功 | publication_url={pub_url} | name={request.channel_name}")

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
        
        # 计算显示排序：追加到当前用户列表末尾
        cursor.execute(
            "SELECT COALESCE(MAX(sort_order), 0) + 1 FROM subscription_list WHERE user_id = %s",
            (user_id,)
        )
        next_sort_order = cursor.fetchone()[0] or 1

        # 插入新订阅（使用默认值；channel_url 已移除，channel_name 可选并由 API 自动填充）
        query = """
            INSERT INTO subscription_list (
                user_id, channel_type, channel_identifier,
                channel_name, channel_description, channel_avatar, enabled, filter_config, metadata, sort_order
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s)
            RETURNING id, created_at, updated_at
        """

        cursor.execute(query, (
            user_id,
            request.channel_type,
            request.channel_identifier,
            request.channel_name,
            request.channel_description,
            channel_avatar,
            request.enabled,
            json.dumps(request.filter_config or {}),
            json.dumps(request.metadata or {}),
            next_sort_order
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

        # News：库里可能已有该源文章（scheduler 为其他用户写入），与「是否有其他订阅者」无关
        if request.channel_type == 'news':
            cursor.execute(
                """
                SELECT COUNT(*) FROM subscription_content
                WHERE channel_type = 'news'
                AND author_identifier = %s
                """,
                (request.channel_identifier,),
            )
            news_existing_count = cursor.fetchone()[0]
            if news_existing_count > 0:
                has_existing_content = True
                existing_content_count = news_existing_count
        
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

                # 灰度开关: 是否把新订阅首抓下沉到 local_virtual_service.
                # 远程服务器无法直接访问 YouTube, 默认路径应走本地家用 Worker;
                # 保留 False 分支仅作为紧急回滚出口.
                import os as _os
                _use_local = _os.getenv('YOUTUBE_SUBSCRIPTION_USE_LOCAL', 'true') \
                    .strip().lower() in ('1', 'true', 'yes', 'on')

                if _use_local:
                    # 走 celery_config.py 默认路由 → youtube_fetching → 本地 Worker
                    # 本地版 fetch_youtube_subscription 会一次性完成:
                    #   RSS → 字幕 fallback → 必要时派发 ASR → 更新订阅统计
                    from celery import current_app as _celery_app
                    task = _celery_app.send_task(
                        'fetch_youtube_subscription',
                        args=[subscription_id, None, True],  # force_fetch=True
                        countdown=0,
                    )
                    logger.info(
                        f"Local worker task scheduled: fetch_youtube_subscription "
                        f"(task_id={task.id})"
                    )
                else:
                    task = fetch_youtube_subscription_task.apply_async(
                        args=[subscription_id, None, True],  # force_fetch=True
                        countdown=0,
                        queue='data_fetching',
                    )
                    logger.info(
                        f"Remote legacy task scheduled: fetch_youtube_subscription "
                        f"(task_id={task.id})"
                    )

        # 🎙️ Podcast 订阅：已有内容时复用，否则触发首次抓取
        elif request.enabled and request.channel_type == 'podcast':
            from backend.tasks.subscription_tasks import (
                add_user_to_existing_content_task,
            )

            if has_existing_content:
                logger.info(
                    f"Reusing existing Podcast content for {request.channel_identifier}: "
                    f"{existing_content_count} episodes found."
                )
                task = add_user_to_existing_content_task.apply_async(
                    args=[user_id, request.channel_type, request.channel_identifier],
                    countdown=0,
                    queue='data_fetching',
                )
                logger.info(f"Celery task scheduled: add_user_to_existing_content (task_id={task.id})")

            else:
                logger.info(f"Triggering first Podcast fetch for feed={request.channel_identifier}")
                from celery import current_app as _celery_app
                task = _celery_app.send_task(
                    'fetch_podcast_subscription',
                    args=[subscription_id, None, True],
                    countdown=0,
                    queue='data_fetching',
                )
                logger.info(f"Celery task scheduled: fetch_podcast_subscription (task_id={task.id})")

        # 🌟 知识星球：已有内容时把新用户并入 user_ids；否则触发首抓 backfill
        elif request.enabled and request.channel_type == 'zhishixingqiu':
            from backend.tasks.subscription_tasks import (
                fetch_zhishixingqiu_subscription_task,
                add_user_to_existing_content_task,
            )

            if has_existing_content:
                logger.info(
                    f"Reusing existing ZSXQ content for group {request.channel_identifier}: "
                    f"{existing_content_count} topics found. Adding user {user_id} to user_ids."
                )
                task = add_user_to_existing_content_task.apply_async(
                    args=[user_id, request.channel_type, request.channel_identifier, subscription_id],
                    countdown=0,
                    queue='data_fetching',
                )
                logger.info(f"Celery task scheduled: add_user_to_existing_content (zsxq) (task_id={task.id})")
            else:
                logger.info(
                    f"No existing ZSXQ content for group {request.channel_identifier}. "
                    f"Triggering backfill (subscription_id={subscription_id})."
                )
                task = fetch_zhishixingqiu_subscription_task.apply_async(
                    args=[subscription_id, None, True],  # force_fetch=True (backfill)
                    countdown=0,
                    queue='data_fetching',
                )
                logger.info(f"Celery task scheduled: fetch_zhishixingqiu_subscription (task_id={task.id})")

        # 🌟 Substack：已有内容时把新用户并入 user_ids；否则触发首抓 backfill
        elif request.enabled and request.channel_type == 'substack':
            from backend.tasks.subscription_tasks import (
                fetch_substack_subscription_task,
                add_user_to_existing_content_task,
            )

            if has_existing_content:
                logger.info(
                    f"Reusing existing Substack content for {request.channel_identifier}: "
                    f"{existing_content_count} items found. Adding user {user_id} to user_ids."
                )
                task = add_user_to_existing_content_task.apply_async(
                    args=[user_id, request.channel_type, request.channel_identifier, subscription_id],
                    countdown=0,
                    queue='data_fetching',
                )
                logger.info(f"Celery task scheduled: add_user_to_existing_content (substack) (task_id={task.id})")
            else:
                logger.info(
                    f"No existing Substack content for {request.channel_identifier}. "
                    f"Triggering backfill (subscription_id={subscription_id})."
                )
                task = fetch_substack_subscription_task.apply_async(
                    args=[subscription_id, None, True],  # force_fetch=True (backfill)
                    countdown=0,
                    queue='data_fetching',
                )
                logger.info(f"Celery task scheduled: fetch_substack_subscription (task_id={task.id})")

        # 🌟 微博：已有内容时把新用户并入 user_ids；否则触发首抓 backfill
        elif request.enabled and request.channel_type == 'weibo':
            from backend.tasks.subscription_tasks import (
                fetch_weibo_subscription_task,
                add_user_to_existing_content_task,
            )

            if has_existing_content:
                logger.info(
                    f"Reusing existing weibo content for uid {request.channel_identifier}: "
                    f"{existing_content_count} posts found. Adding user {user_id} to user_ids."
                )
                task = add_user_to_existing_content_task.apply_async(
                    args=[user_id, request.channel_type, request.channel_identifier, subscription_id],
                    countdown=0,
                    queue='data_fetching',
                )
                logger.info(f"Celery task scheduled: add_user_to_existing_content (weibo) (task_id={task.id})")
            else:
                logger.info(
                    f"No existing weibo content for uid {request.channel_identifier}. "
                    f"Triggering backfill (subscription_id={subscription_id})."
                )
                task = fetch_weibo_subscription_task.apply_async(
                    args=[subscription_id, None, True],  # force_fetch=True (backfill)
                    countdown=0,
                    queue='data_fetching',
                )
                logger.info(f"Celery task scheduled: fetch_weibo_subscription (task_id={task.id})")

        # News：已有文章时把新用户并入 user_ids（与 Twitter/YouTube 复用逻辑一致）
        elif request.enabled and request.channel_type == 'news':
            from backend.tasks.subscription_tasks import add_user_to_existing_content_task

            if has_existing_content:
                logger.info(
                    f"Reusing existing news content for {request.channel_identifier}: "
                    f"{existing_content_count} items found. Adding user {user_id} to user_ids."
                )
                task = add_user_to_existing_content_task.apply_async(
                    args=[user_id, request.channel_type, request.channel_identifier, subscription_id],
                    countdown=0,
                    queue='data_fetching',
                )
                logger.info(f"Celery task scheduled: add_user_to_existing_content (news) (task_id={task.id})")
        
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
            unread_count=0,  # 🔥 新创建时未读计数为0
            sort_order=next_sort_order
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to create subscription: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to create subscription: {str(e)}")


class ReorderSubscriptionItem(BaseModel):
    """单个订阅的排序更新项"""
    subscription_id: int
    sort_order: int


class ReorderSubscriptionsRequest(BaseModel):
    """批量重排订阅请求"""
    items: List[ReorderSubscriptionItem]


@router.put("/reorder", summary="批量重排当前用户的订阅")
async def reorder_subscriptions(
    request: ReorderSubscriptionsRequest,
    current_user: TokenData = Depends(get_current_user)
):
    """
    批量更新当前用户订阅的显示顺序（拖拽排序持久化）。

    请求体: {"items": [{"subscription_id": int, "sort_order": int}, ...]}
    仅校验并更新属于当前用户的订阅；缺失或不属于该用户的 id 返回 404。
    """
    if not request.items:
        raise HTTPException(status_code=400, detail="Items must be a non-empty list")

    updates = {int(item.subscription_id): int(item.sort_order) for item in request.items}
    if any(so < 1 for so in updates.values()):
        raise HTTPException(status_code=400, detail="sort_order must be >= 1")

    user_id = current_user.user_id
    db_manager = get_db_manager()
    conn = None
    try:
        conn = db_manager.get_connection()
        cursor = conn.cursor()

        # 批量校验所有权：所有 id 必须属于当前用户
        cursor.execute(
            "SELECT id FROM subscription_list WHERE user_id = %s AND id = ANY(%s)",
            (user_id, list(updates.keys()))
        )
        owned = {row[0] for row in cursor.fetchall()}
        missing = sorted(set(updates.keys()) - owned)
        if missing:
            raise HTTPException(
                status_code=404,
                detail=f"Subscriptions not found or permission denied: {missing}"
            )

        # 逐条更新（事务）
        for sid, so in updates.items():
            cursor.execute(
                "UPDATE subscription_list SET sort_order = %s WHERE id = %s AND user_id = %s",
                (so, sid, user_id)
            )

        conn.commit()
        logger.info(f"Reordered {len(updates)} subscriptions for user {user_id}")
        return {"success": True, "updated": len(updates)}

    except HTTPException:
        if conn:
            conn.rollback()
        raise
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Failed to reorder subscriptions for user {user_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to reorder subscriptions: {str(e)}")
    finally:
        if conn:
            conn.close()


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
                channel_name, channel_description, channel_avatar, enabled, priority,
                fetch_interval_minutes, max_items_per_fetch, filter_config,
                metadata, created_at, updated_at, last_fetched_at,
                next_fetch_at, total_items_fetched, last_fetch_items_count,
                consecutive_failures, unread_count, sort_order
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
            channel_avatar=row[6],
            enabled=row[7],
            priority=row[8],
            fetch_interval_minutes=row[9],
            max_items_per_fetch=row[10],
            filter_config=row[11] or {},
            metadata=row[12] or {},
            created_at=row[13],
            updated_at=row[14],
            last_fetched_at=row[15],
            next_fetch_at=row[16],
            total_items_fetched=row[17],
            last_fetch_items_count=row[18],
            consecutive_failures=row[19],
            unread_count=row[20] or 0,
            sort_order=row[21]
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


@router.get("/{subscription_id}/shows")
async def get_subscription_shows(
    subscription_id: int,
    days: int = Query(default=7, ge=1, le=30, description="时间窗天数"),
    current_user: TokenData = Depends(get_current_user),
):
    """
    返回指定订阅在过去 N 天内出现的 distinct show_name 列表（YouTube metadata->>'show_name'）。
    用于 Digest 配置面板的 Program filter 下拉，避免依赖前端已加载的有限列表。
    """
    from datetime import timedelta
    from backend.utils.datetime_utils import utcnow

    user_id = current_user.user_id
    db_manager = get_db_manager()

    try:
        conn = db_manager.get_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT id, channel_type, channel_identifier
            FROM subscription_list
            WHERE id = %s AND user_id = %s
            """,
            (subscription_id, user_id),
        )
        sub_row = cursor.fetchone()
        if not sub_row:
            raise HTTPException(status_code=404, detail="Subscription not found")

        channel_type = sub_row[1]
        channel_identifier = sub_row[2]

        # show_name 只对 YouTube 有意义；其余渠道直接返回空列表
        if channel_type != "youtube":
            cursor.close()
            conn.close()
            return {"shows": []}

        since = utcnow() - timedelta(days=days)

        cursor.execute(
            """
            SELECT DISTINCT metadata->>'show_name'
            FROM subscription_content
            WHERE channel_type = 'youtube'
              AND author_identifier = %s
              AND %s = ANY(user_ids)
              AND COALESCE(published_at, fetched_at) >= %s
              AND metadata->>'show_name' IS NOT NULL
              AND metadata->>'show_name' != ''
            ORDER BY 1
            """,
            (channel_identifier, user_id, since),
        )
        shows = [row[0] for row in cursor.fetchall()]
        cursor.close()
        conn.close()

        return {"shows": shows}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching shows for subscription {subscription_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{subscription_id}/contents")
async def get_subscription_contents(
    subscription_id: int,
    limit: int = Query(default=50, le=200, description="每页条数"),
    offset: int = Query(default=0, ge=0, description="偏移量"),
    only_unread: bool = Query(default=False, description="仅未读"),
    published_after: Optional[datetime] = Query(
        default=None,
        description="仅返回 published_at >= 该时间的条目（UTC ISO8601）",
    ),
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
        # 注意: 引入 LEFT JOIN 后所有列必须加 sc. 前缀以避免歧义
        where_conditions = [
            "sc.channel_type = %s",
            "sc.author_identifier = %s",
            "%s = ANY(sc.user_ids)"
        ]
        params = [channel_type, channel_identifier, user_id]
        
        if only_unread:
            where_conditions.append(
                f"(sc.read_status->>'{user_id}' IS NULL OR (sc.read_status->>'{user_id}')::jsonb->>'is_read' = 'false')"
            )

        if published_after is not None:
            # 部分渠道入库时 published_at 可能为空，回落到 fetched_at 以免日期筛选误排除
            where_conditions.append(
                "COALESCE(sc.published_at, sc.fetched_at) >= %s"
            )
            params.append(ensure_utc(published_after))

        where_clause = " AND ".join(where_conditions)
        
        # 查询内容 (LEFT JOIN subscription_content_tags 带出 AI 标签 + 摘要,
        # LEFT JOIN subscription_content_user_summaries 带出 per-user 个性化摘要,
        # 详见 docs/instructions/FEEDS_AUTO_TAGGING_PLAN.md §7 及
        #      database/migrations/create_subscription_content_user_summaries.sql §读取规则)
        query = f"""
            SELECT
                sc.id,
                sc.channel_type,
                sc.content_id,
                sc.title,
                sc.content,
                sc.author_name,
                sc.author_identifier,
                sc.author_avatar,
                sc.url,
                sc.media_urls,
                sc.published_at,
                sc.fetched_at,
                sc.engagement_count,
                sc.view_count,
                sc.metadata,
                sc.read_status,
                sc.is_retweet,
                sc.retweeted_author,
                sc.retweeted_content,
                sc.retweeted_url,
                sc.is_quote,
                sc.quoted_author,
                sc.quoted_content,
                sc.quoted_url,
                sc.is_reply,
                sc.in_reply_to_id,
                sc.in_reply_to_username,
                sc.in_reply_to_user_id,
                sc.heat_level,
                (sc.transcript IS NOT NULL AND sc.transcript != '') AS has_transcript,
                sct.sector, sct.sector_en, sct.industry, sct.industry_en,
                sct.ticker, sct.concepts, sct.summary, sct.confidence,
                sct.tagged_at, sct.people, sct.countries, sct.tag_schema_version,
                scus.summary    AS personalized_summary,
                scus.reasoning  AS personalized_reasoning,
                CASE WHEN scus.id IS NOT NULL THEN 'personalized' ELSE 'global' END AS summary_source,
                sc.categories,
                COALESCE(
                    (SELECT json_agg(json_build_object(
                        'theme_id', ctm.theme_id,
                        'theme_name', ite.theme_name,
                        'category', ite.category,
                        'match_mode', ctm.match_mode,
                        'confidence', ctm.confidence,
                        'matched_tickers', ctm.matched_tickers
                    ) ORDER BY ctm.confidence DESC)
                    FROM content_theme_matches ctm
                    JOIN investment_theme_embeddings ite ON ite.theme_id = ctm.theme_id
                    WHERE ctm.content_type = 'feed' AND ctm.content_id = sc.id
                    ), '[]'::json
                ) AS matched_themes_raw
            FROM subscription_content sc
            LEFT JOIN subscription_content_tags sct ON sct.subscription_content_id = sc.id
            LEFT JOIN subscription_content_user_summaries scus
                ON scus.subscription_content_id = sc.id
               AND scus.user_id = %s
            WHERE {where_clause}
            ORDER BY sc.published_at DESC
            LIMIT %s OFFSET %s
        """

        # scus JOIN 的 user_id 需要在 where_clause 参数之前传入
        params = [user_id] + params
        params.extend([limit, offset])
        cursor.execute(query, params)
        rows = cursor.fetchall()
        
        # 获取总数 (count 查询无需 JOIN scus, 只需 where_clause 参数)
        # params 结构: [user_id(scus JOIN), channel_type, channel_identifier, user_id(where), limit, offset]
        # count 只需 where_clause 部分: params[1:-2]
        count_query = f"""
            SELECT COUNT(*)
            FROM subscription_content sc
            WHERE {where_clause}
        """
        cursor.execute(count_query, params[1:-2])
        total = cursor.fetchone()[0]
        
        # 格式化结果
        contents = []
        for row in rows:
            user_read_status = row[15].get(str(user_id), {}) if row[15] else {}

            # AI 标签 (LEFT JOIN 结果, 未打标 → None)
            # 列序: [30]sector [31]sector_en [32]industry [33]industry_en
            #       [34]ticker [35]concepts [36]summary [37]confidence [38]tagged_at
            #       [39]people [40]countries [41]tag_schema_version
            #       [42]personalized_summary [43]personalized_reasoning [44]summary_source
            #       [45]categories [46]matched_themes_raw
            tags_payload = None
            if row[30] is not None or row[41] == 'tweet_signal_v1':
                tickers_raw = row[34]
                tickers = tickers_raw if isinstance(tickers_raw, list) else (tickers_raw or [])
                concepts_raw = row[35]
                concepts = concepts_raw if isinstance(concepts_raw, list) else (concepts_raw or [])
                people_raw = row[39]
                people = people_raw if isinstance(people_raw, list) else (people_raw or [])
                countries_raw = row[40]
                countries = (
                    countries_raw if isinstance(countries_raw, list) else (countries_raw or [])
                )
                tags_payload = {
                    'sector': row[30] or '',
                    'sector_en': row[31] or '',
                    'industry': row[32] or '',
                    'industry_en': row[33] or '',
                    'tickers': tickers,
                    'concepts': concepts,
                    'people': people,
                    'countries': countries,
                    'tag_schema_version': row[41] or 'feeds_v1',
                    'summary': row[36],
                    'confidence': row[37],
                    'tagged_at': ensure_utc(row[38]).isoformat() if row[38] else None,
                }

            contents.append({
                'id': row[0],
                'channel_type': row[1],
                'content_id': row[2],
                'title': row[3],
                'content': row[4],
                'author_name': row[5],
                'author_identifier': row[6],
                'author_avatar': row[7],
                'url': row[8],
                'media_urls': row[9],
                'published_at': ensure_utc(row[10]).isoformat() if row[10] else None,
                'fetched_at': ensure_utc(row[11]).isoformat() if row[11] else None,
                'engagement_count': row[12],
                'view_count': row[13],
                'metadata': row[14],
                'is_read': user_read_status.get('is_read', False),
                'read_at': user_read_status.get('read_at'),
                # Twitter 特定字段
                'is_retweet': row[16],
                'retweeted_author': row[17],
                'retweeted_content': row[18],
                'retweeted_url': row[19],
                'is_quote': row[20],
                'quoted_author': row[21],
                'quoted_content': row[22],
                'quoted_url': row[23],
                # Reply 特定字段
                'is_reply': row[24],
                'in_reply_to_id': row[25],
                'in_reply_to_username': row[26],
                'in_reply_to_user_id': row[27],
                # 热度等级
                'heat_level': row[28],
                # YouTube: 仅返回是否有 transcript（不返回正文，减轻加载）
                'has_transcript': bool(row[29]) if row[29] is not None else False,
                # Phase 3 新增: Feeds 自动打标 + 摘要 (LEFT JOIN 结果, 未打标为 null)
                'tags': tags_payload,
                # PR-7: per-user 个性化摘要 (LEFT JOIN subscription_content_user_summaries)
                'personalized_summary': row[42] or None,
                'personalized_reasoning': row[43] or None,
                'summary_source': row[44],
                # News: EODHD 原始分类标签 (TEXT[])，供前端 NewsOriginalView 展示
                'categories': list(row[45]) if row[45] else [],
                # Investment Themes 语义匹配结果
                'matched_themes': row[46] if row[46] else [],
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


@router.get("/{subscription_id}/digest")
async def get_subscription_digest(
    subscription_id: int,
    days: int = Query(default=1, ge=1, le=7, description="时间窗天数（1/3/7）"),
    max_chars_per_item: int = Query(default=12000, ge=1000, le=60000, description="单条最大字符数"),
    max_total_chars: int = Query(default=400000, ge=10000, le=1500000, description="全量最大字符数"),
    show_filter: Optional[str] = Query(default=None, description="按节目名过滤（YouTube show_name），空则不过滤"),
    current_user: TokenData = Depends(get_current_user),
):
    """
    返回订阅在过去 N 天内主要内容的拼接纯文本，供用户/外部 LLM 插件消费。

    - twitter: content（含 retweet/quote/reply 结构）
    - youtube: transcript_cleaned → transcript → content(描述) 三级回退；全空跳过
    - news:    content → url 兜底
    """
    from datetime import timedelta
    from backend.utils.datetime_utils import utcnow
    from backend.services.subscription_digest_service import (
        build_digest,
        QUERY_ROW_LIMIT,
    )

    user_id = current_user.user_id
    db_manager = get_db_manager()

    try:
        conn = db_manager.get_connection()
        cursor = conn.cursor()

        # 1. 验证订阅所有权
        cursor.execute(
            """
            SELECT id, user_id, channel_type, channel_identifier, channel_name
            FROM subscription_list
            WHERE id = %s AND user_id = %s
            """,
            (subscription_id, user_id),
        )
        sub_row = cursor.fetchone()
        if not sub_row:
            raise HTTPException(status_code=404, detail="Subscription not found")

        channel_type = sub_row[2]
        channel_identifier = sub_row[3]
        channel_name = sub_row[4]

        since = utcnow() - timedelta(days=days)

        # 2. 拉取范围内的内容
        #    YouTube 需要 transcript / transcript_cleaned；其余渠道只需 content 等基础列
        #    show_filter 非空时额外过滤 metadata->>'show_name'
        show_clause = ""
        params_extra: list = []
        if show_filter:
            show_clause = "AND metadata->>'show_name' = %s"
            params_extra = [show_filter]

        cursor.execute(
            f"""
            SELECT
                id, channel_type, title, content,
                author_name, author_identifier, url, published_at,
                is_retweet, retweeted_author, retweeted_content, retweeted_url,
                is_quote, quoted_author, quoted_content, quoted_url,
                is_reply, in_reply_to_username,
                transcript, transcript_cleaned,
                metadata
            FROM subscription_content
            WHERE channel_type = %s
              AND author_identifier = %s
              AND %s = ANY(user_ids)
              AND COALESCE(published_at, fetched_at) >= %s
              {show_clause}
            ORDER BY COALESCE(published_at, fetched_at) DESC
            LIMIT %s
            """,
            [channel_type, channel_identifier, user_id, since] + params_extra + [QUERY_ROW_LIMIT],
        )
        col_names = [
            "id", "channel_type", "title", "content",
            "author_name", "author_identifier", "url", "published_at",
            "is_retweet", "retweeted_author", "retweeted_content", "retweeted_url",
            "is_quote", "quoted_author", "quoted_content", "quoted_url",
            "is_reply", "in_reply_to_username",
            "transcript", "transcript_cleaned",
            "metadata",
        ]
        rows = [dict(zip(col_names, r)) for r in cursor.fetchall()]

        cursor.close()
        conn.close()

        # 3. 构建 digest
        subscription = {
            "channel_name": channel_name,
            "channel_identifier": channel_identifier,
            "channel_type": channel_type,
        }
        result = build_digest(
            subscription,
            rows,
            max_chars_per_item=max_chars_per_item,
            max_total_chars=max_total_chars,
            days=days,
            since=since,
        )

        return {
            "subscription_id": subscription_id,
            "channel_name": channel_name or f"@{channel_identifier}",
            "channel_type": channel_type,
            "days": days,
            "since": since.isoformat(),
            "show_filter": show_filter,
            **result,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error building digest for subscription {subscription_id}: {e}")
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


@router.get("/contents/{content_db_id}/transcript")
async def get_subscription_content_transcript(
    content_db_id: int,
    current_user: TokenData = Depends(get_current_user)
):
    """
    按需返回单条订阅内容的字幕/转写正文（不在列表接口中批量返回，减轻负载）。

    优先返回 transcript_cleaned（清洗后的可读版本），
    若 transcript_cleaned 为空则回退到原始 transcript。
    响应键始终为 "transcript"，前端无需改动。
    """
    user_id = current_user.user_id
    db_manager = get_db_manager()

    try:
        conn = db_manager.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT channel_type, transcript, transcript_cleaned
            FROM subscription_content
            WHERE id = %s AND %s = ANY(user_ids)
            """,
            (content_db_id, user_id),
        )
        row = cursor.fetchone()
        cursor.close()
        conn.close()

        if not row:
            raise HTTPException(status_code=404, detail="Content not found")

        channel_type, transcript_raw, transcript_cleaned = row[0], row[1], row[2]

        if channel_type not in ("youtube", "podcast"):
            raise HTTPException(status_code=400, detail="Transcript is only available for YouTube and Podcast content")

        # 优先使用清洗后版本，回退到原始版本
        cleaned_text = (transcript_cleaned or "").strip()
        raw_text = (transcript_raw or "").strip()
        text = cleaned_text or raw_text

        if not text:
            raise HTTPException(status_code=404, detail="Transcript not available for this content")

        return {"transcript": text}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching transcript for content {content_db_id}: {str(e)}")
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
                author_avatar,
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
            'fetched_at': ensure_utc(row[2]).isoformat() if row[2] else None,
            'channel_type': row[3],
            'content_id': row[4],
            'title': row[5],
            'content': row[6],
            'author_identifier': row[7],
            'author_name': row[8],
            'author_avatar': row[9],
            'media_urls': row[10] or [],
            'engagement_count': row[11] or 0,
            'view_count': row[12] or 0,
            'metadata': row[13] or {},
            'published_at': ensure_utc(row[14]).isoformat() if row[14] else None,
            'is_retweet': row[15] or False,
            'retweeted_author': row[16],
            'retweeted_content': row[17],
            'retweeted_url': row[18],
            'is_quote': row[19] or False,
            'quoted_author': row[20],
            'quoted_content': row[21],
            'quoted_url': row[22],
            'is_reply': row[23] or False,
            'in_reply_to_id': row[24],
            'in_reply_to_username': row[25],
            'in_reply_to_user_id': row[26],
            'heat_level': row[27] or 2,
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
    对 Feed 中尚无可用字幕的 YouTube 条目，触发与 Upload YouTube Link / 订阅补字幕
    相同的本地 Worker 流水线（见 UPLOAD_FILES_AND_LLM_ANALYSIS_FLOW.md §2.3）：

    1. `fetch_youtube_transcripts_batch` → `youtube_fetching` 队列（家用 Worker）：
       youtube-transcript-api → yt-dlp 字幕兜底 → 成功则写入 subscription_content；
    2. 仍无可用时由本地 Worker 派发 `transcribe_youtube_feed_task`（Fun-ASR），
       不在云服务器上直连 YouTube，避免机房 IP 被封。

    任务进度：GET /api/files/tasks/{task_id}/status。
    若响应 result.asr_dispatched > 0，表示已降级 ASR，Celery 主任务会很快 SUCCESS，
    需继续轮询 GET .../contents/{id}/transcript 或刷新列表直至 has_transcript 为真。
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

        _, channel_type, content_id_str, _ = row

        if channel_type == 'podcast':
            # Podcast: 直接从 metadata 获取音频 URL，派发 podcast_transcribe_episode
            conn = db_manager.get_connection()
            cursor = conn.cursor()
            cursor.execute("""
                SELECT metadata->>'audio_url', title
                FROM subscription_content
                WHERE id = %s
            """, (content_db_id,))
            meta_row = cursor.fetchone()
            cursor.close()
            conn.close()

            if not meta_row or not meta_row[0]:
                raise HTTPException(
                    status_code=400,
                    detail="Podcast episode has no audio URL, cannot transcribe"
                )

            audio_url = meta_row[0]
            episode_title = meta_row[1] or 'unknown'

            from celery import current_app as _celery_app
            task = _celery_app.send_task(
                "podcast_transcribe_episode",
                args=[{
                    'content_db_id': content_db_id,
                    'audio_url': audio_url,
                    'episode_title': episode_title,
                }],
                countdown=0,
                queue='data_fetching',
            )

            logger.info(
                f"Podcast ASR pipeline dispatched: "
                f"content_db_id={content_db_id}, task_id={task.id}, user_id={user_id}"
            )

            return {
                "task_id": task.id,
                "content_db_id": content_db_id,
                "message": "Podcast ASR pipeline started. Poll GET /api/files/tasks/{task_id}/status.",
            }

        if channel_type != 'youtube':
            raise HTTPException(
                status_code=400,
                detail="Only YouTube and Podcast content can be transcribed via ASR"
            )

        video_url = f"https://www.youtube.com/watch?v={content_id_str}"

        # 与 youtube_subscription_service 新视频补字幕、Upload Sources 一致：派发到本地
        # `fetch_youtube_transcripts_batch`（路由 youtube_fetching），禁止 queue=file_processing
        # 以免在云 worker 上跑 file_tasks.transcribe_youtube_feed_task（yt-dlp 封 IP）。
        from celery import current_app as _celery_app

        video_items = [
            {
                "content_db_id": content_db_id,
                "video_id": content_id_str,
                "video_url": video_url,
            }
        ]
        task = _celery_app.send_task(
            "fetch_youtube_transcripts_batch",
            args=(video_items,),
            countdown=0,
        )

        logger.info(
            f"YouTube feed transcript pipeline dispatched (local batch): "
            f"content_db_id={content_db_id}, video_id={content_id_str}, task_id={task.id}, user_id={user_id}"
        )

        return {
            "task_id": task.id,
            "video_id": content_id_str,
            "content_db_id": content_db_id,
            "message": (
                "YouTube caption/ASR pipeline started on local worker. "
                "Poll GET /api/files/tasks/{task_id}/status; if result.asr_dispatched > 0, "
                "poll transcript until available."
            ),
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to trigger YouTube ASR: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
