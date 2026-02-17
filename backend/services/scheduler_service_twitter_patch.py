# -*- coding: utf-8 -*-
"""
SchedulerService Twitter订阅集成补丁
使用说明：将以下代码添加到 scheduler_service.py 中
"""

# =====================================================
# 1. 在文件顶部添加导入
# =====================================================
"""
在 scheduler_service.py 的导入部分添加：

from backend.services.twitter_subscription_service import TwitterSubscriptionService
"""

# =====================================================
# 2. 在 task_handlers 字典中添加
# =====================================================
"""
在 __init__ 方法的 self.task_handlers 字典中添加：

    'twitter_subscription': self._execute_twitter_subscription_task,  # Twitter订阅推文获取任务
"""

# =====================================================
# 3. 添加任务执行方法
# =====================================================
"""
在 SchedulerService 类中添加以下方法（建议放在 _execute_watchlist_news_task 方法之后）：
"""

async def _execute_twitter_subscription_task(self, params: Dict[str, Any]) -> Dict[str, Any]:
    """
    执行Twitter订阅推文获取任务
    
    Args:
        params: 任务参数
            - subscription_id: 指定订阅ID（可选）
            - user_id: 指定用户ID（可选）
            - force_fetch: 是否强制获取（可选）
    
    Returns:
        执行结果
    """
    try:
        logger.info("执行Twitter订阅推文获取任务", params=params)
        
        subscription_id = params.get('subscription_id')
        user_id = params.get('user_id')
        force_fetch = params.get('force_fetch', False)
        
        # 创建Twitter订阅服务实例
        twitter_service = TwitterSubscriptionService()
        
        # 在线程池中执行耗时的操作，避免阻塞事件循环
        import asyncio
        import concurrent.futures
        
        loop = asyncio.get_event_loop()
        with concurrent.futures.ThreadPoolExecutor() as executor:
            result = await loop.run_in_executor(
                executor,
                twitter_service.fetch_and_store_tweets,
                subscription_id,
                user_id,
                force_fetch
            )
        
        # 记录任务执行结果到数据库
        if result.get('success'):
            await self._log_task_result(
                task_type='twitter_subscription',
                status='success',
                details=result,
                items_processed=result.get('tweets_fetched', 0)
            )
            
            logger.info("Twitter订阅推文获取任务完成",
                       subscriptions=result.get('subscriptions_count'),
                       authors=result.get('authors_count'),
                       tweets_fetched=result.get('tweets_fetched'),
                       tweets_new=result.get('tweets_new'),
                       execution_time_ms=result.get('execution_duration_ms'))
        else:
            await self._log_task_result(
                task_type='twitter_subscription',
                status='failed',
                error_message=result.get('message'),
                details=result
            )
            
            logger.error("Twitter订阅推文获取任务失败",
                        error=result.get('message'))
        
        return result
        
    except Exception as e:
        error_msg = f"Twitter订阅任务执行异常: {str(e)}"
        logger.error(error_msg, exception=str(e))
        
        await self._log_task_result(
            task_type='twitter_subscription',
            status='failed',
            error_message=error_msg
        )
        
        return {
            'success': False,
            'message': error_msg,
            'error': str(e)
        }


# =====================================================
# 4. 完整的修改指南
# =====================================================
"""
完整修改步骤：

1. 打开 backend/services/scheduler_service.py

2. 在文件顶部导入部分（约第20行附近）添加：
   from backend.services.twitter_subscription_service import TwitterSubscriptionService

3. 在 __init__ 方法的 self.task_handlers 字典中（约第55-72行）添加：
   'twitter_subscription': self._execute_twitter_subscription_task,

4. 在类中添加上面的 _execute_twitter_subscription_task 方法
   （建议放在 _execute_watchlist_news_task 方法之后，约第1100行）

5. 保存文件
"""

# =====================================================
# 5. 数据库任务配置SQL
# =====================================================
"""
在数据库中添加Twitter订阅任务配置：

INSERT INTO tasks (
    name,
    task_type,
    schedule_type,
    cron_expression,
    enabled,
    description,
    params
) VALUES (
    'Twitter Subscription Feed',
    'twitter_subscription',
    'cron',
    '*/30 * * * *',  -- 每30分钟执行一次
    true,
    '从用户订阅的Twitter账号获取最新推文',
    '{}'::jsonb
);

-- 如果需要测试，可以添加一个间隔更短的测试任务
INSERT INTO tasks (
    name,
    task_type,
    schedule_type,
    cron_expression,
    enabled,
    description,
    params
) VALUES (
    'Twitter Subscription Feed (Test)',
    'twitter_subscription',
    'cron',
    '*/5 * * * *',  -- 每5分钟执行一次（测试用）
    false,  -- 默认禁用，需要时手动启用
    '测试用Twitter订阅任务',
    '{"force_fetch": true}'::jsonb
);
"""
