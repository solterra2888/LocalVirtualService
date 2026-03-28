# -*- coding: utf-8 -*-
"""
定时任务调度器服务 - 基于APScheduler实现Cron表达式的定期数据获取策略
"""

import os
import json
import pytz
import redis
import asyncio
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any, Callable
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR
from backend.database.db_manager import DatabaseManager
from backend.utils.logger import get_logger
from backend.services.eodhd_news_service import EODHDNewsService
from backend.services.watchlist_news_service import WatchlistNewsService
from backend.config.exchange_config import ExchangeConfig
from config.settings import config

logger = get_logger("scheduler_service")


class SchedulerService:
    """定时任务调度器服务类"""
    
    def __init__(self):
        """初始化调度器服务"""
        logger.info("开始初始化定时任务调度器服务")
        
        # 创建调度器实例
        self.scheduler = AsyncIOScheduler(
            timezone=pytz.timezone('America/New_York'),  # 默认使用纽约时间
            job_defaults={
                'coalesce': True,  # 合并积压的任务，避免重复执行
                'max_instances': 1,
                'misfire_grace_time': 60  # 60秒容错时间，超过则放弃执行
            }
        )
        
        # 获取标准化数据库连接配置
        from backend.utils.database_config import get_database_config
        db_config = get_database_config()
        self.db_manager = DatabaseManager(db_config)
        
        # 初始化 Redis 客户端（用于分布式锁）
        redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379/0')
        self.redis_client = redis.from_url(redis_url, decode_responses=True)
        logger.info(f"Redis 客户端初始化完成: {redis_url}")
        
        # 注册任务类型和执行函数
        self.task_handlers = {
            'eodhd_news': self._execute_eodhd_news_task,
            'watchlist_news': self._execute_watchlist_news_task,
            'twitter_subscription': self._execute_twitter_subscription_task,  # 🔥 Twitter订阅推文获取任务
            'youtube_subscription': self._execute_youtube_subscription_task,  # 🔥 YouTube订阅视频获取任务
            'cctv_news': self._execute_cctv_news_task,  # CCTV新闻爬取任务
            'economic_events': self._execute_economic_events_task,
            'macro_indicators': self._execute_macro_indicators_task,
            'eodhd_fundamentals': self._execute_fundamentals_task,
            'watchlist_fundamentals': self._execute_watchlist_fundamentals_task,  # 🔥 新增：Watchlist 基本面批量更新任务
            'finviz_screener': self._execute_finviz_screener_task,
            'eodhd_stock_prices': self._execute_eodhd_stock_prices_task,
            'eodhd_eod_stock_prices': self._execute_eodhd_eod_stock_prices_task,
            'news_analysis': self._execute_news_analysis_task,  # 新闻LLM分析任务
            # 'futu_themes': 已删除 - 移除 futu 爬虫相关功能
            'theme_sync': self._execute_theme_sync_task,  # 🔥 新增：主题同步任务
            'market_data': self._execute_market_data_task,  # 市场数据获取任务（指数、外汇、大宗、债券、板块）
            'earnings_calendar': self._execute_earnings_calendar_task,  # 财报日历数据获取任务
            'ipo_calendar': self._execute_ipo_calendar_task,  # IPO 日历数据获取任务
            'news_feed': self._execute_news_feed_task,  # EODHD 新闻 Feed 订阅任务
            'twitter_daily_report': self._execute_twitter_daily_report_task,  # Twitter Daily Report 生成
        }
        
        # 添加事件监听器
        self.scheduler.add_listener(self._job_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
        
        logger.info("定时任务调度器服务初始化完成")
    
    async def start(self):
        """启动调度器"""
        try:
            logger.info("启动定时任务调度器")
            
            # 先清理所有现有任务，确保状态同步
            await self._cleanup_existing_jobs()
            
            # 启动调度器
            self.scheduler.start()
            
            # 加载已保存的任务
            await self._load_saved_tasks()
            
            # 添加定期状态同步任务（每5分钟同步一次）
            # 这样可以自动修复数据库与调度器状态不一致的问题
            self.scheduler.add_job(
                self.sync_task_states,
                trigger=IntervalTrigger(minutes=5),
                id='_sync_task_states',
                name='Task State Sync',
                replace_existing=True
            )
            logger.info("✓ 已添加定期状态同步任务（每5分钟）")
            
            logger.info("定时任务调度器启动成功")
        except Exception as e:
            logger.error(f"启动定时任务调度器失败: {str(e)}")
            raise
    
    async def stop(self):
        """停止调度器"""
        try:
            logger.info("停止定时任务调度器")
            self.scheduler.shutdown(wait=False)
            logger.info("定时任务调度器已停止")
        except Exception as e:
            logger.error(f"停止定时任务调度器失败: {str(e)}")
    
    async def create_cron_task(self, task_name: str, task_type: str, cron_expression: str, 
                              timezone: str = "America/New_York", enabled: bool = True,
                              task_params: Optional[Dict[str, Any]] = None, 
                              created_by: str = "system") -> Dict[str, Any]:
        """
        创建基于Cron表达式的定时任务
        
        Args:
            task_name: 任务名称
            task_type: 任务类型 (eodhd_news, economic_events, macro_indicators, eodhd_fundamentals, finviz_screener)
            cron_expression: Cron表达式
            timezone: 时区
            enabled: 是否启用
            task_params: 任务参数
            
        Returns:
            Dict: 创建结果
        """
        try:
            logger.info(f"创建Cron定时任务: {task_name}, 类型: {task_type}, Cron: {cron_expression}")
            
            # 验证任务类型
            if task_type not in self.task_handlers:
                raise ValueError(f"不支持的任务类型: {task_type}")
            
            # 解析Cron表达式
            cron_parts = cron_expression.split()
            if len(cron_parts) != 5:
                raise ValueError("Cron表达式格式错误，应为: 分 时 日 月 星期")
            
            minute, hour, day, month, day_of_week = cron_parts
            
            # 创建任务ID
            task_id = f"{task_type}_{task_name}_{int(datetime.now().timestamp())}"
            
            # 创建Cron触发器
            trigger = CronTrigger(
                minute=minute,
                hour=hour,
                day=day,
                month=month,
                day_of_week=day_of_week,
                timezone=pytz.timezone(timezone)
            )
            
            # 保存任务配置到数据库
            task_config = {
                'id': task_id,
                'name': task_name,
                'type': task_type,
                'schedule_type': 'cron',
                'cron_expression': cron_expression,
                'timezone': timezone,
                'enabled': enabled,
                'params': task_params or {},
                'created_at': datetime.now(tz=pytz.timezone(timezone)).isoformat(),
                'last_run': None,
                'next_run': None,
                'success_count': 0,
                'fail_count': 0,
                'created_by': created_by
            }
            
            # 检查是否已存在相同类型的任务
            existing_task = await self._get_task_by_type(task_type)
            if existing_task:
                # 如果存在，先从调度器中移除旧任务
                old_task_id = existing_task.get('id')
                if old_task_id and self.scheduler.get_job(old_task_id):
                    self.scheduler.remove_job(old_task_id)
                    logger.info(f"已从调度器中移除旧的{task_type}任务: {old_task_id}")
            
            # 保存到数据库
            success = await self._save_task_config(task_config)
            if not success:
                raise Exception("保存任务配置到数据库失败")
            
            # 如果启用，添加到调度器
            if enabled:
                # 使用实际保存到数据库的任务ID
                actual_task_id = task_config['id']
                self.scheduler.add_job(
                    self._execute_task,
                    trigger=trigger,
                    args=[actual_task_id, task_type, task_params or {}],
                    id=actual_task_id,
                    name=task_name,
                    replace_existing=True
                )
                
                # 更新下次运行时间
                job = self.scheduler.get_job(actual_task_id)
                if job and hasattr(job, 'next_run_time') and job.next_run_time:
                    task_config['next_run'] = job.next_run_time.isoformat()
                    await self._update_task_config(actual_task_id, {'next_run': task_config['next_run']})
            
            # 根据是否为更新还是新建，返回不同的消息
            action = "更新" if existing_task else "创建"
            actual_task_id = task_config['id']
            logger.info(f"Cron定时任务{action}成功: {actual_task_id}")
            return {
                'success': True,
                'message': f'{action}定时任务 "{task_name}" 成功。同一策略类型({task_type})只能有一个配置。',
                'task_id': actual_task_id,
                'task_config': task_config,
                'action': action  # 添加动作标识
            }
            
        except Exception as e:
            logger.error(f"创建Cron定时任务失败: {str(e)}")
            return {
                'success': False,
                'message': f'创建定时任务失败: {str(e)}',
                'task_id': None,
                'task_config': None
            }
    
    async def create_interval_task(self, task_name: str, task_type: str, interval_minutes: int,
                                  timezone: str = "America/New_York", enabled: bool = True,
                                  task_params: Optional[Dict[str, Any]] = None, 
                                  created_by: str = "system") -> Dict[str, Any]:
        """
        创建基于间隔的定时任务
        
        Args:
            task_name: 任务名称
            task_type: 任务类型
            interval_minutes: 间隔分钟数
            timezone: 时区
            enabled: 是否启用
            task_params: 任务参数
            
        Returns:
            Dict: 创建结果
        """
        try:
            logger.info(f"创建间隔定时任务: {task_name}, 类型: {task_type}, 间隔: {interval_minutes}分钟")
            
            # 验证任务类型
            if task_type not in self.task_handlers:
                raise ValueError(f"不支持的任务类型: {task_type}")
            
            # 创建任务ID
            task_id = f"{task_type}_{task_name}_{int(datetime.now().timestamp())}"
            
            # 创建间隔触发器
            trigger = IntervalTrigger(
                minutes=interval_minutes,
                timezone=pytz.timezone(timezone)
            )
            
            # 保存任务配置到数据库
            task_config = {
                'id': task_id,
                'name': task_name,
                'type': task_type,
                'schedule_type': 'interval',
                'interval_minutes': interval_minutes,
                'timezone': timezone,
                'enabled': enabled,
                'params': task_params or {},
                'created_at': datetime.now(tz=pytz.timezone(timezone)).isoformat(),
                'last_run': None,
                'next_run': None,
                'success_count': 0,
                'fail_count': 0,
                'created_by': created_by
            }
            
            # 检查是否已存在相同类型的任务
            existing_task = await self._get_task_by_type(task_type)
            if existing_task:
                # 如果存在，先从调度器中移除旧任务
                old_task_id = existing_task.get('id')
                if old_task_id and self.scheduler.get_job(old_task_id):
                    self.scheduler.remove_job(old_task_id)
                    logger.info(f"已从调度器中移除旧的{task_type}任务: {old_task_id}")
            
            # 保存到数据库
            success = await self._save_task_config(task_config)
            if not success:
                raise Exception("保存任务配置到数据库失败")
            
            # 如果启用，添加到调度器
            if enabled:
                # 使用实际保存到数据库的任务ID
                actual_task_id = task_config['id']
                self.scheduler.add_job(
                    self._execute_task,
                    trigger=trigger,
                    args=[actual_task_id, task_type, task_params or {}],
                    id=actual_task_id,
                    name=task_name,
                    replace_existing=True
                )
                
                # 更新下次运行时间
                job = self.scheduler.get_job(actual_task_id)
                if job and hasattr(job, 'next_run_time') and job.next_run_time:
                    task_config['next_run'] = job.next_run_time.isoformat()
                    await self._update_task_config(actual_task_id, {'next_run': task_config['next_run']})
            
            # 根据是否为更新还是新建，返回不同的消息
            action = "更新" if existing_task else "创建"
            actual_task_id = task_config['id']
            logger.info(f"间隔定时任务{action}成功: {actual_task_id}")
            return {
                'success': True,
                'message': f'{action}定时任务 "{task_name}" 成功。同一策略类型({task_type})只能有一个配置。',
                'task_id': actual_task_id,
                'task_config': task_config,
                'action': action  # 添加动作标识
            }
            
        except Exception as e:
            logger.error(f"创建间隔定时任务失败: {str(e)}")
            return {
                'success': False,
                'message': f'创建定时任务失败: {str(e)}',
                'task_id': None,
                'task_config': None
            }
    
    async def get_tasks(self) -> List[Dict[str, Any]]:
        """获取所有任务列表"""
        try:
            tasks = await self._get_all_tasks()
            
            # 更新运行时间信息
            for task in tasks:
                job = self.scheduler.get_job(task['id'])
                if job:
                    task['next_run'] = job.next_run_time.isoformat() if hasattr(job, 'next_run_time') and job.next_run_time else None
                    task['status'] = 'running' if hasattr(job, 'next_run_time') and job.next_run_time else 'paused'
                else:
                    task['status'] = 'paused' if task['enabled'] else 'disabled'
            
            return tasks
        except Exception as e:
            logger.error(f"获取任务列表失败: {str(e)}")
            return []
    
    async def enable_task(self, task_id: str) -> Dict[str, Any]:
        """启用任务"""
        try:
            logger.info(f"开始启用任务: {task_id}")
            
            # 获取任务配置（从数据库获取最新配置）
            task_config = await self._get_task_config(task_id)
            if not task_config:
                logger.error(f"任务不存在: {task_id}")
                return {'success': False, 'message': '任务不存在'}
            
            # 先检查调度器中是否已存在该任务，如果存在先移除
            if self.scheduler.get_job(task_id):
                logger.warning(f"任务 {task_id} 已在调度器中，先移除旧任务")
                self.scheduler.remove_job(task_id)
            
            # 更新数据库启用状态
            await self._update_task_config(task_id, {'enabled': True})
            logger.info(f"已更新数据库，任务 {task_id} 状态: enabled=True")
            
            # 重新获取配置确保是最新的
            task_config = await self._get_task_config(task_id)
            task_config['enabled'] = True  # 确保启用标志已设置
            
            # 添加到调度器
            await self._add_job_to_scheduler(task_config)
            
            # 验证任务已添加到调度器
            if self.scheduler.get_job(task_id):
                logger.info(f"✓ 任务已成功添加到调度器: {task_id}")
            else:
                logger.error(f"✗ 任务未能添加到调度器: {task_id}")
                return {'success': False, 'message': '任务未能添加到调度器'}
            
            logger.info(f"任务已启用: {task_id}")
            return {'success': True, 'message': '任务已启用'}
            
        except Exception as e:
            logger.error(f"启用任务失败: {task_id}, 错误: {str(e)}")
            return {'success': False, 'message': f'启用任务失败: {str(e)}'}
    
    async def disable_task(self, task_id: str) -> Dict[str, Any]:
        """禁用任务 - 确保从调度器内存中移除"""
        try:
            logger.info(f"开始禁用任务: {task_id}")
            
            # 重要：先从调度器移除（内存操作），再更新数据库
            # 这样即使数据库更新失败，至少任务已经停止运行
            job_existed = False
            if self.scheduler.get_job(task_id):
                self.scheduler.remove_job(task_id)
                job_existed = True
                logger.info(f"✓ 已从调度器移除任务: {task_id}")
            else:
                logger.warning(f"任务 {task_id} 不在调度器中（可能已经停止或从未启动）")
            
            # 更新数据库状态
            success = await self._update_task_config(task_id, {'enabled': False})
            if success:
                logger.info(f"✓ 已更新数据库，任务 {task_id} 状态: enabled=False")
            else:
                logger.error(f"✗ 更新数据库失败，但任务已从调度器移除: {task_id}")
            
            # 二次验证：确保任务确实不在调度器中
            if self.scheduler.get_job(task_id):
                logger.error(f"✗ 警告：任务 {task_id} 仍在调度器中！")
                # 再次尝试移除
                self.scheduler.remove_job(task_id)
                logger.info(f"已进行二次移除: {task_id}")
            else:
                logger.info(f"✓ 确认任务 {task_id} 不在调度器中")
            
            status_msg = "任务已禁用" + (" (从调度器移除)" if job_existed else " (仅更新数据库)")
            logger.info(f"任务已禁用: {task_id}")
            return {'success': True, 'message': status_msg}
            
        except Exception as e:
            logger.error(f"禁用任务失败: {task_id}, 错误: {str(e)}")
            return {'success': False, 'message': f'禁用任务失败: {str(e)}'}
    
    async def delete_task(self, task_id: str) -> Dict[str, Any]:
        """删除任务"""
        try:
            # 从调度器移除
            if self.scheduler.get_job(task_id):
                self.scheduler.remove_job(task_id)
            
            # 从数据库删除
            success = await self._delete_task_config(task_id)
            if not success:
                return {'success': False, 'message': '从数据库删除任务失败'}
            
            logger.info(f"任务已删除: {task_id}")
            return {'success': True, 'message': '任务已删除'}
            
        except Exception as e:
            logger.error(f"删除任务失败: {str(e)}")
            return {'success': False, 'message': f'删除任务失败: {str(e)}'}
    
    async def get_running_tasks(self) -> List[Dict[str, Any]]:
        """
        获取调度器中实际运行的任务列表
        用于检查调度器内存状态
        
        Returns:
            List[Dict]: 运行中任务的列表
        """
        try:
            running_tasks = []
            scheduler_jobs = self.scheduler.get_jobs()
            
            for job in scheduler_jobs:
                # 跳过内部同步任务
                if job.id.startswith('_'):
                    continue
                
                running_tasks.append({
                    'id': job.id,
                    'name': job.name,
                    'next_run_time': job.next_run_time.isoformat() if job.next_run_time else None,
                    'trigger': str(job.trigger)
                })
            
            logger.info(f"调度器中运行的任务数量: {len(running_tasks)}")
            return running_tasks
            
        except Exception as e:
            logger.error(f"获取运行任务列表失败: {str(e)}")
            return []
    
    async def sync_task_states(self) -> Dict[str, Any]:
        """
        同步调度器内存状态与数据库状态
        修复状态不一致问题
        
        Returns:
            Dict: 同步结果统计
        """
        try:
            logger.info("开始同步调度器状态与数据库...")
            
            # 获取数据库中的所有任务
            db_tasks = await self._get_all_tasks()
            
            # 获取调度器中的所有任务ID
            scheduler_job_ids = {job.id for job in self.scheduler.get_jobs() if not job.id.startswith('_')}
            
            # 统计信息
            stats = {
                'total_db_tasks': len(db_tasks),
                'total_scheduler_jobs': len(scheduler_job_ids),
                'removed_jobs': [],
                'added_jobs': [],
                'skipped': []
            }
            
            # 遍历数据库任务，同步状态
            for task in db_tasks:
                task_id = task['id']
                enabled = task['enabled']
                
                # 情况1: 数据库显示禁用，但调度器中还在运行
                if not enabled and task_id in scheduler_job_ids:
                    try:
                        self.scheduler.remove_job(task_id)
                        stats['removed_jobs'].append(task_id)
                        logger.warning(f"同步修复: 从调度器移除已禁用的任务 {task_id}")
                    except Exception as e:
                        logger.error(f"移除任务失败 {task_id}: {str(e)}")
                        stats['skipped'].append(f"{task_id} (移除失败)")
                
                # 情况2: 数据库显示启用，但调度器中没有
                elif enabled and task_id not in scheduler_job_ids:
                    try:
                        await self._add_job_to_scheduler(task)
                        stats['added_jobs'].append(task_id)
                        logger.warning(f"同步修复: 添加缺失的启用任务到调度器 {task_id}")
                    except Exception as e:
                        logger.error(f"添加任务失败 {task_id}: {str(e)}")
                        stats['skipped'].append(f"{task_id} (添加失败)")
            
            # 检查是否有调度器中的任务不在数据库中（孤立任务）
            db_task_ids = {task['id'] for task in db_tasks}
            orphan_jobs = scheduler_job_ids - db_task_ids
            
            for orphan_id in orphan_jobs:
                try:
                    self.scheduler.remove_job(orphan_id)
                    stats['removed_jobs'].append(orphan_id)
                    logger.warning(f"同步修复: 移除数据库中不存在的孤立任务 {orphan_id}")
                except Exception as e:
                    logger.error(f"移除孤立任务失败 {orphan_id}: {str(e)}")
            
            logger.info(f"任务状态同步完成: 移除{len(stats['removed_jobs'])}个, 添加{len(stats['added_jobs'])}个, 跳过{len(stats['skipped'])}个")
            
            return {
                'success': True,
                'message': '任务状态同步完成',
                'stats': stats
            }
            
        except Exception as e:
            logger.error(f"任务状态同步失败: {str(e)}")
            return {
                'success': False,
                'message': f'任务状态同步失败: {str(e)}',
                'stats': {}
            }
    
    async def create_default_eodhd_news_task(self, created_by: str = "system") -> Dict[str, Any]:
        """创建默认的EODHD新闻获取任务（纽约时间每15分钟）"""
        return await self.create_cron_task(
            task_name="EODHD News Auto Fetch",
            task_type="eodhd_news",
            cron_expression="*/15 * * * *",  # 每15分钟
            timezone="America/New_York",
            enabled=True,
            task_params={
                'max_results': 100,
                'symbols': None,
                'topic': None,
                'topic_filter': None,
                'keywords': None,
                'auto_analyze': False,
                'use_celery': True,
                'analysis_templates': ["news_summary_v1", "news_classification"],
                'analysis_batch_size': 1000,
                'max_concurrent': 5,
                'enable_theme_matching': True
            },
            created_by=created_by
        )
    
    async def create_default_watchlist_news_task(self, created_by: str = "system") -> Dict[str, Any]:
        """创建默认的Watchlist新闻获取任务（纽约时间每15分钟）"""
        return await self.create_cron_task(
            task_name="My Watchlist News Auto Fetch",
            task_type="watchlist_news",
            cron_expression="*/15 * * * *",  # 每15分钟
            timezone="America/New_York",
            enabled=True,
            task_params={
                'max_results': 100,
                'recent_days': None,
                'user_id': None,  # 获取所有用户的watchlist
                'auto_analyze': False,
                'use_celery': True,
                'analysis_templates': ["news_summary_v1", "news_classification"],
                'analysis_batch_size': 1000,
                'max_concurrent': 5,
                'enable_theme_matching': True
            },
            created_by=created_by
        )
    
    async def _execute_task(self, task_id: str, task_type: str, task_params: Dict[str, Any]):
        """执行任务（带分布式锁保护）"""
        execution_log_id = None
        started_at = datetime.now(tz=timezone.utc)
        lock_key = f"scheduler_lock:{task_type}"
        lock_acquired = False
        
        try:
            # 🔒 尝试获取 Redis 分布式锁
            # 使用 SET NX EX 命令：只在键不存在时设置，并设置过期时间
            lock_acquired = self.redis_client.set(
                lock_key,
                f"{task_id}_{started_at.timestamp()}",  # 锁的值包含任务ID和时间戳
                nx=True,  # 只在不存在时设置
                ex=600    # 10分钟后自动过期，防止死锁
            )
            
            if not lock_acquired:
                # 无法获取锁，说明同类型任务正在运行
                logger.warning(
                    f"⏭️  任务 {task_type} 正在运行中，跳过本次执行",
                    task_id=task_id,
                    task_type=task_type
                )
                
                # 记录跳过的执行
                execution_log_id = await self._log_task_execution_start(
                    task_id, f'Task-{task_id}', task_type, started_at, task_params
                )
                
                if execution_log_id:
                    finished_at = datetime.now(tz=timezone.utc)
                    duration_ms = int((finished_at - started_at).total_seconds() * 1000)
                    await self._log_task_execution_finish(
                        execution_log_id,
                        finished_at,
                        duration_ms,
                        'skipped',
                        error_message='任务正在运行，已跳过本次执行'
                    )
                
                return {
                    'success': False,
                    'message': '任务正在运行，已跳过',
                    'skipped': True
                }
            
            logger.info(f"🔓 获取任务锁成功: {task_id}, 类型: {task_type}")
            
            # 获取任务配置以获取任务名称
            task_config = await self._get_task_config(task_id)
            task_name = task_config.get('name', f'Task-{task_id}') if task_config else f'Task-{task_id}'
            
            # 记录任务开始执行到日志表
            execution_log_id = await self._log_task_execution_start(
                task_id, task_name, task_type, started_at, task_params
            )
            
            # 更新最后运行时间
            await self._update_task_config(task_id, {'last_run': started_at.isoformat()})
            
            # 执行任务
            handler = self.task_handlers.get(task_type)
            if not handler:
                error_msg = f"未找到任务类型 {task_type} 的处理器。支持的任务类型: {', '.join(self.task_handlers.keys())}"
                logger.error(error_msg)
                
                # 记录任务执行失败
                await self._log_task_execution_end(
                    execution_log_id, 
                    started_at, 
                    'failed', 
                    error_msg, 
                    error_message=error_msg
                )
                
                return {
                    'success': False,
                    'message': error_msg,
                    'data': None
                }
            
            result = await handler(task_params)
            
            # 计算执行时间
            finished_at = datetime.now(tz=timezone.utc)
            duration_ms = int((finished_at - started_at).total_seconds() * 1000)
            
            if result.get('success', False):
                # 记录成功执行结果
                await self._log_task_execution_finish(
                    execution_log_id, finished_at, duration_ms, 'success',
                    result.get('message', '任务执行成功'), result.get('data')
                )
                
                # 增加成功计数
                await self._increment_success_count(task_id)
                logger.info(f"任务执行成功: {task_id}, 耗时: {duration_ms}ms")
            else:
                # 记录失败执行结果
                await self._log_task_execution_finish(
                    execution_log_id, finished_at, duration_ms, 'failed',
                    error_message=result.get('message', '任务执行失败')
                )
                
                # 增加失败计数
                await self._increment_fail_count(task_id)
                logger.error(f"任务执行失败: {task_id}, 错误: {result.get('message', '未知错误')}")
            
        except asyncio.CancelledError:
            # 🔄 任务被取消（通常是因为应用重新加载）
            finished_at = datetime.now(tz=timezone.utc)
            duration_ms = int((finished_at - started_at).total_seconds() * 1000)
            
            if execution_log_id:
                await self._log_task_execution_finish(
                    execution_log_id, finished_at, duration_ms, 'cancelled',
                    error_message='任务被取消（应用重新加载或手动停止）'
                )
            
            logger.warning(f"任务被取消: {task_id}（通常是因为应用重新加载或手动停止）")
            raise  # 重新抛出，让调度器知道任务被取消
            
        except Exception as e:
            # 记录异常执行结果
            finished_at = datetime.now(tz=timezone.utc)
            duration_ms = int((finished_at - started_at).total_seconds() * 1000)
            
            if execution_log_id:
                await self._log_task_execution_finish(
                    execution_log_id, finished_at, duration_ms, 'failed',
                    error_message=str(e), error_traceback=str(e)
                )
            
            await self._increment_fail_count(task_id)
            logger.error(f"任务执行异常: {task_id}, 错误: {str(e)}")
        
        finally:
            # 🔓 释放 Redis 锁
            if lock_acquired:
                try:
                    self.redis_client.delete(lock_key)
                    logger.info(f"🔓 释放任务锁: {task_type}")
                except Exception as e:
                    logger.error(f"释放 Redis 锁失败: {str(e)}", lock_key=lock_key)
    
    async def _execute_eodhd_news_task(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """执行EODHD新闻获取任务"""
        try:
            logger.info("执行EODHD新闻获取任务", params=params)
            
            # 处理recent_days参数，计算日期范围
            start_date = params.get('start_date')
            end_date = params.get('end_date')
            
            if params.get('recent_days') is not None:
                from datetime import datetime, timedelta
                
                # 获取当前日期
                current_date = datetime.now()
                recent_days = params.get('recent_days')
                
                # 计算开始日期和结束日期
                # 如果当前时间是2022-04-22，用户选择最近天数为10天，则日期范围是2022-04-12到2022-04-22
                end_date = current_date.strftime('%Y-%m-%d')
                start_date = (current_date - timedelta(days=recent_days - 1)).strftime('%Y-%m-%d')
                
                logger.info("根据recent_days参数计算日期范围", 
                           recent_days=recent_days,
                           start_date=start_date, 
                           end_date=end_date)
            
            # 创建EODHD新闻服务实例
            eodhd_service = EODHDNewsService()
            
            # 在线程池中执行耗时的新闻获取操作，避免阻塞事件循环
            import asyncio
            import concurrent.futures
            
            with concurrent.futures.ThreadPoolExecutor() as executor:
                result = await asyncio.get_event_loop().run_in_executor(
                    executor,
                    eodhd_service.fetch_and_store_news,
                    params.get('symbols'),
                    params.get('topic'),
                    start_date,
                    end_date,
                    params.get('max_results'),
                    params.get('topic_filter'),
                    params.get('keywords'),
                    'eodhd_news'  # scheduler_source参数
                )
            
            # 🔥 使用正确的new_count（从result中获取）
            new_count = result.get('new_count', 0)
            
            # 详细的统计信息
            stats_summary = {
                'task_type': 'eodhd_news',
                'fetched_count': result['fetched_count'],
                'deduplicated_count': result['success_count'],
                'new_count': new_count,
                'existing_count': result['existing_count'],
                'failed_count': result.get('failed_count', 0)
            }
            
            if result['existing_count'] > 0:
                message = f"成功获取 {result['fetched_count']} 条新闻，去重后 {result['success_count']} 条，其中新增 {new_count} 条，已存在 {result['existing_count']} 条"
            else:
                message = f"成功获取 {result['fetched_count']} 条新闻，去重后 {result['success_count']} 条，全部为新增"
            
            logger.info(f"📊 EODHD新闻获取统计: {stats_summary}")
            
            # ========== 新增：获取新闻后自动分析 ==========
            auto_analyze = params.get('auto_analyze', False)
            use_celery = params.get('use_celery', False)  # 新增：是否使用Celery
            analysis_result = None
            
            # 🔥 优化：使用新插入的新闻ID列表进行分析，避免时序问题
            # 🔥 新功能：支持多模板并行分析
            if auto_analyze:
                logger.info(f"启用自动分析，新增 {new_count} 条新闻，use_celery={use_celery}")
                
                # 获取模板列表（向后兼容）
                templates = params.get('analysis_templates')
                if not templates:
                    # 兼容旧的单模板配置
                    single_template = params.get('analysis_template', 'news_summary_v1')
                    templates = [single_template]
                
                logger.info(f"将使用 {len(templates)} 个模板进行分析: {templates}")
                
                try:
                    if use_celery:
                        # ========== 方案B：使用Celery（推荐多用户场景）==========
                        logger.info("使用Celery异步处理分析任务")
                        
                        # 🔥 新方案：优先使用新插入的新闻ID列表
                        new_news_ids = result.get('new_news_ids', [])
                        
                        if new_news_ids:
                            # 🔥 使用新的任务：直接分析指定ID的新闻
                            from backend.tasks.news_tasks import analyze_news_by_ids_task
                            
                            logger.info(f"使用新闻ID列表分析，共 {len(new_news_ids)} 条新闻")
                            
                            # 🔥 多模板并行：为每个模板提交独立的Celery任务
                            celery_tasks = []
                            enable_theme_matching = params.get('enable_theme_matching', True)
                            for template_code in templates:
                                task = analyze_news_by_ids_task.delay(
                                    news_ids=new_news_ids,
                                    template_code=template_code,
                                    max_concurrent=params.get('max_concurrent', 8),
                                    enable_theme_matching=enable_theme_matching
                                )
                                celery_tasks.append({
                                    'template': template_code,
                                    'task_id': task.id,
                                    'news_count': len(new_news_ids)
                                })
                                logger.info(f"✓ 已提交 {template_code} 分析任务: {task.id}")
                            
                            analysis_result = {
                                'status': 'submitted_to_celery',
                                'tasks': celery_tasks,
                                'total_templates': len(templates),
                                'news_count': len(new_news_ids),
                                'method': 'by_ids',
                                'estimated_time': f'预计 {len(new_news_ids) * 3} 秒/模板'
                            }
                            
                            task_summary = ', '.join([f"{t['template']}({t['task_id'][:8]})" for t in celery_tasks])
                            message += f"，已提交 {len(templates)} 个模板分析任务: {task_summary}"
                        else:
                            # 🔥 降级方案：如果没有新闻ID列表，使用传统方式查询未分析新闻
                            from backend.tasks.news_tasks import analyze_unanalyzed_news_task
                            
                            logger.warning("未获取到新闻ID列表，降级使用查询未分析新闻方式")
                            
                            # 准备symbols参数
                            analysis_symbols = None
                            if params.get('symbols'):
                                if isinstance(params['symbols'], str):
                                    analysis_symbols = [s.strip() for s in params['symbols'].split(',') if s.strip()]
                                else:
                                    analysis_symbols = params['symbols']
                            
                            # 修复：使用analysis_batch_size作为limit，默认值应该合理（如1000）
                            analysis_limit = params.get('analysis_batch_size', 1000)
                            
                            # 🔥 多模板并行：为每个模板提交独立的Celery任务
                            celery_tasks = []
                            enable_theme_matching = params.get('enable_theme_matching', True)
                            for template_code in templates:
                                task = analyze_unanalyzed_news_task.delay(
                                    limit=analysis_limit,
                                    template_code=template_code,
                                    symbols=analysis_symbols,
                                    start_date=start_date,
                                    end_date=end_date,
                                    max_concurrent=params.get('max_concurrent', 10),
                                    enable_theme_matching=enable_theme_matching
                                )
                                celery_tasks.append({
                                    'template': template_code,
                                    'task_id': task.id,
                                    'limit': analysis_limit
                                })
                                logger.info(f"✓ 已提交 {template_code} 分析任务（查询方式）: {task.id}")
                            
                            analysis_result = {
                                'status': 'submitted_to_celery',
                                'tasks': celery_tasks,
                                'total_templates': len(templates),
                                'news_count': analysis_limit,
                                'method': 'by_query',
                                'estimated_time': '根据未分析新闻数量而定'
                            }
                            
                            task_summary = ', '.join([f"{t['template']}({t['task_id'][:8]})" for t in celery_tasks])
                            message += f"，已提交 {len(templates)} 个模板分析任务: {task_summary}"
                        
                    else:
                        # ========== 方案A：直接分析（仅适合小团队）==========
                        logger.info("使用同步方式分析（适合小团队，多用户场景请启用use_celery）")
                        
                        # 导入新闻分析服务
                        from backend.services.news_analysis_service import NewsAnalysisService
                        from backend.utils.database_config import get_database_config
                        
                        # 创建分析服务
                        db_config = get_database_config()
                        news_analysis_service = NewsAnalysisService(db_config)
                        
                        # 获取分析参数
                        analysis_template = params.get('analysis_template', 'news_summary_v1')
                        # 修复：使用合理的默认值，而不是限制为new_count
                        analysis_batch_size = params.get('analysis_batch_size', 1000)
                        use_concurrent = params.get('use_concurrent', True)
                        max_concurrent = params.get('max_concurrent', 10)
                        
                        # 准备筛选条件
                        analysis_symbols = None
                        if params.get('symbols'):
                            if isinstance(params['symbols'], str):
                                analysis_symbols = [s.strip() for s in params['symbols'].split(',') if s.strip()]
                            else:
                                analysis_symbols = params['symbols']
                        
                        # 执行批量分析
                        logger.info(f"分析参数: template={analysis_template}, batch_size={analysis_batch_size}, "
                                   f"concurrent={use_concurrent}, max_concurrent={max_concurrent}")
                        
                        analysis_result = await news_analysis_service.batch_analyze_news(
                            limit=analysis_batch_size,  # 修复：不再限制为new_count
                            template_code=analysis_template,
                            symbols=analysis_symbols,
                            start_date=start_date,
                            end_date=end_date,
                            use_concurrent=use_concurrent,
                            max_concurrent=max_concurrent
                        )
                        
                        # 更新消息
                        if analysis_result['total'] > 0:
                            analysis_msg = f"，已分析 {analysis_result['success']} 条新闻"
                            if analysis_result['failed'] > 0:
                                analysis_msg += f"（失败 {analysis_result['failed']} 条）"
                            message += analysis_msg
                            
                            # 只记录统计信息，不包含详细的results
                            analysis_summary = {
                                'total': analysis_result['total'],
                                'success': analysis_result['success'],
                                'failed': analysis_result['failed'],
                                'template_code': analysis_result.get('template_code', 'unknown')
                            }
                            logger.info(f"新闻分析完成: {analysis_summary}")
                    
                except Exception as e:
                    logger.error(f"自动分析新闻失败: {str(e)}")
                    analysis_result = {
                        'error': str(e),
                        'total': 0,
                        'success': 0,
                        'failed': 0,
                        'status': 'failed'
                    }
            
            # 返回结果（包含分析结果）
            return_data = result.copy()
            if analysis_result:
                return_data['analysis'] = analysis_result
            
            return {
                'success': True,
                'message': message,
                'data': return_data
            }
            
        except Exception as e:
            logger.error(f"EODHD新闻获取任务执行失败: {str(e)}")
            return {
                'success': False,
                'message': f"EODHD新闻获取失败: {str(e)}",
                'data': None
            }
    
    async def _execute_watchlist_news_task(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """执行Watchlist新闻获取任务"""
        try:
            logger.info("执行Watchlist新闻获取任务", params=params)
            
            # 处理recent_days参数，计算日期范围
            start_date = None
            end_date = None
            
            if params.get('recent_days') is not None:
                from datetime import datetime, timedelta
                
                # 获取当前日期
                current_date = datetime.now()
                recent_days = params.get('recent_days')
                
                # 计算开始日期和结束日期
                end_date = current_date.strftime('%Y-%m-%d')
                start_date = (current_date - timedelta(days=recent_days - 1)).strftime('%Y-%m-%d')
                
                logger.info("根据recent_days参数计算日期范围", 
                           recent_days=recent_days,
                           start_date=start_date, 
                           end_date=end_date)
            
            # 创建Watchlist新闻服务实例
            watchlist_service = WatchlistNewsService()
            
            # 在线程池中执行耗时的新闻获取操作，避免阻塞事件循环
            import asyncio
            import concurrent.futures
            
            with concurrent.futures.ThreadPoolExecutor() as executor:
                result = await asyncio.get_event_loop().run_in_executor(
                    executor,
                    watchlist_service.fetch_and_store_watchlist_news,
                    params.get('max_results'),
                    params.get('recent_days'),
                    params.get('user_id')  # 可选：指定用户ID，为None时获取所有用户的watchlist
                )
            
            # 获取实际新增的新闻数量（从result中直接获取）
            new_count = result.get('new_count', 0)
            
            # 详细的统计信息
            stats_summary = {
                'task_type': 'watchlist_news',
                'fetched_count': result['fetched_count'],
                'deduplicated_count': result['success_count'],
                'new_count': new_count,
                'existing_count': result['existing_count'],
                'failed_count': result.get('failed_count', 0),
                'tickers_count': len(result.get('tickers', []))
            }
            
            if result['existing_count'] > 0:
                message = f"成功获取 {result['fetched_count']} 条watchlist新闻，去重后 {result['success_count']} 条，其中新增 {new_count} 条，已存在 {result['existing_count']} 条"
            else:
                message = f"成功获取 {result['fetched_count']} 条watchlist新闻，去重后 {result['success_count']} 条，全部为新增"
            
            logger.info(f"📊 Watchlist新闻获取统计: {stats_summary}")
            
            # ========== 新增：获取新闻后自动分析 ==========
            auto_analyze = params.get('auto_analyze', False)
            use_celery = params.get('use_celery', False)  # 新增：是否使用Celery
            analysis_result = None
            
            # 修复：应该分析所有未分析的新闻，而不仅仅是新增的
            # 之前的逻辑导致每次只分析1条新闻，造成大量新闻堆积
            # 🔥 新功能：支持多模板并行分析
            if auto_analyze:
                logger.info(f"启用自动分析，新增 {new_count} 条watchlist新闻，use_celery={use_celery}")
                
                # 获取模板列表（向后兼容）
                templates = params.get('analysis_templates')
                if not templates:
                    # 兼容旧的单模板配置
                    single_template = params.get('analysis_template', 'news_summary_v1')
                    templates = [single_template]
                
                logger.info(f"将使用 {len(templates)} 个模板进行分析: {templates}")
                
                try:
                    if use_celery:
                        # ========== 使用Celery（推荐多用户场景）==========
                        logger.info("使用Celery异步处理分析任务")
                        
                        from backend.tasks.news_tasks import analyze_unanalyzed_news_task
                        
                        # 获取watchlist的股票列表作为筛选条件
                        analysis_symbols = None
                        if result.get('tickers'):
                            analysis_symbols = result['tickers']
                            logger.info(f"使用watchlist股票列表进行筛选，共 {len(analysis_symbols)} 个股票")
                        
                        # 修复：使用analysis_batch_size作为limit，默认值应该合理（如1000）
                        # 而不是限制为new_count，这样可以分析所有未分析的历史新闻
                        analysis_limit = params.get('analysis_batch_size', 1000)
                        
                        # 🔥 多模板并行：为每个模板提交独立的Celery任务
                        celery_tasks = []
                        enable_theme_matching = params.get('enable_theme_matching', True)
                        for template_code in templates:
                            task = analyze_unanalyzed_news_task.delay(
                                limit=analysis_limit,
                                template_code=template_code,
                                symbols=analysis_symbols,  # 使用watchlist的股票列表
                                start_date=start_date,     # 添加开始日期
                                end_date=end_date,         # 添加结束日期
                                max_concurrent=params.get('max_concurrent', 10),
                                enable_theme_matching=enable_theme_matching
                            )
                            celery_tasks.append({
                                'template': template_code,
                                'task_id': task.id,
                                'limit': analysis_limit
                            })
                            logger.info(f"✓ 已提交 {template_code} 分析任务: {task.id}")
                        
                        # 更新统计信息
                        stats_summary['analysis_status'] = 'submitted_to_celery'
                        stats_summary['celery_tasks'] = celery_tasks
                        stats_summary['total_templates'] = len(templates)
                        stats_summary['analysis_limit'] = analysis_limit
                        stats_summary['estimated_analysis_count'] = min(analysis_limit, new_count)
                        
                        analysis_result = {
                            'status': 'submitted_to_celery',
                            'tasks': celery_tasks,
                            'total_templates': len(templates),
                            'news_count': analysis_limit,
                            'estimated_time': '根据未分析新闻数量而定'
                        }
                        
                        task_summary = ', '.join([f"{t['template']}({t['task_id'][:8]})" for t in celery_tasks])
                        message += f"，已提交 {len(templates)} 个模板分析任务: {task_summary}"
                        
                    else:
                        # ========== 直接分析（仅适合小团队）==========
                        logger.info("使用同步方式分析（适合小团队，多用户场景请启用use_celery）")
                        
                        # 导入新闻分析服务
                        from backend.services.news_analysis_service import NewsAnalysisService
                        from backend.utils.database_config import get_database_config
                        
                        # 创建分析服务
                        db_config = get_database_config()
                        news_analysis_service = NewsAnalysisService(db_config)
                        
                        # 获取分析参数
                        analysis_template = params.get('analysis_template', 'news_summary_v1')
                        # 修复：使用合理的默认值，而不是限制为new_count
                        analysis_batch_size = params.get('analysis_batch_size', 1000)
                        use_concurrent = params.get('use_concurrent', True)
                        max_concurrent = params.get('max_concurrent', 10)
                        
                        # 获取watchlist的股票列表作为筛选条件
                        analysis_symbols = None
                        if result.get('tickers'):
                            analysis_symbols = result['tickers']
                            logger.info(f"使用watchlist股票列表进行筛选，共 {len(analysis_symbols)} 个股票")
                        
                        # 执行批量分析（添加日期和股票参数）
                        analysis_result = await news_analysis_service.batch_analyze_news(
                            limit=analysis_batch_size,  # 修复：不再限制为new_count
                            template_code=analysis_template,
                            symbols=analysis_symbols,  # 添加股票列表
                            start_date=start_date,     # 添加开始日期
                            end_date=end_date,         # 添加结束日期
                            use_concurrent=use_concurrent,
                            max_concurrent=max_concurrent
                        )
                        
                        # 更新消息
                        if analysis_result['total'] > 0:
                            analysis_msg = f"，已分析 {analysis_result['success']} 条新闻"
                            if analysis_result['failed'] > 0:
                                analysis_msg += f"（失败 {analysis_result['failed']} 条）"
                            message += analysis_msg
                            
                            # 只记录统计信息，不包含详细的results
                            analysis_summary = {
                                'total': analysis_result['total'],
                                'success': analysis_result['success'],
                                'failed': analysis_result['failed'],
                                'template_code': analysis_result.get('template_code', 'unknown')
                            }
                            logger.info(f"新闻分析完成: {analysis_summary}")
                    
                except Exception as e:
                    logger.error(f"自动分析新闻失败: {str(e)}")
                    analysis_result = {
                        'error': str(e),
                        'total': 0,
                        'success': 0,
                        'failed': 0,
                        'status': 'failed'
                    }
            
            # 返回结果（包含分析结果）
            return_data = result.copy()
            if analysis_result:
                return_data['analysis'] = analysis_result
            
            return {
                'success': True,
                'message': message,
                'data': return_data
            }
            
        except Exception as e:
            logger.error(f"Watchlist新闻获取任务执行失败: {str(e)}")
            return {
                'success': False,
                'message': f"Watchlist新闻获取失败: {str(e)}",
                'data': None
            }
    
    async def _execute_twitter_subscription_task(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        执行Twitter订阅推文获取任务 — 通过 Celery 异步分发给 data-worker。
        Twitter 拉取耗时约 200-300s，必须走 Celery worker 避免阻塞 scheduler。

        Args:
            params: 任务参数
                - subscription_id: 指定订阅ID（可选）
                - user_id: 指定用户ID（可选）
                - force_fetch: 是否强制获取（可选）

        Returns:
            执行结果（立即返回 Celery task_id，实际执行在 data-worker 中）
        """
        try:
            logger.info("分发 Twitter 订阅推文获取 Celery 任务", params=params)

            from backend.tasks.subscription_tasks import fetch_twitter_subscription_task

            subscription_id = params.get('subscription_id')
            user_id = params.get('user_id')
            force_fetch = params.get('force_fetch', False)

            task = fetch_twitter_subscription_task.delay(
                subscription_id=subscription_id,
                user_id=user_id,
                force_fetch=force_fetch,
            )

            message = (
                f"✅ Twitter 订阅推文获取任务已提交 Celery 异步执行，"
                f"task_id={task.id}"
            )
            logger.info("Twitter 订阅推文获取任务已提交", celery_task_id=task.id)

            return {'success': True, 'message': message, 'celery_task_id': task.id}

        except Exception as e:
            error_msg = f"Twitter订阅任务分发异常: {str(e)}"
            logger.error(error_msg, exception=str(e))

            return {
                'success': False,
                'message': f"❌ {error_msg}",
                'error': str(e)
            }
    
    async def _execute_youtube_subscription_task(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        执行YouTube订阅视频获取任务
        
        Args:
            params: 任务参数
                - subscription_id: 指定订阅ID（可选）
                - user_id: 指定用户ID（可选）
                - force_fetch: 是否强制获取（可选）
        
        Returns:
            执行结果
        """
        try:
            logger.info("执行YouTube订阅视频获取任务", params=params)
            
            subscription_id = params.get('subscription_id')
            user_id = params.get('user_id')
            force_fetch = params.get('force_fetch', False)
            
            # 导入YouTube订阅服务
            from backend.services.youtube_subscription_service import YouTubeSubscriptionService
            
            # 在线程池中执行耗时的操作，避免阻塞事件循环
            import asyncio
            import concurrent.futures
            
            loop = asyncio.get_event_loop()
            with concurrent.futures.ThreadPoolExecutor() as executor:
                # 如果指定了 subscription_id 或 user_id，调用单个订阅获取
                if subscription_id or user_id:
                    result = await loop.run_in_executor(
                        executor,
                        YouTubeSubscriptionService.fetch_and_store_videos,
                        subscription_id,
                        user_id,
                        force_fetch
                    )
                else:
                    # 批量获取所有订阅
                    result = await loop.run_in_executor(
                        executor,
                        YouTubeSubscriptionService.fetch_all_subscriptions
                    )
            
            # 返回任务执行结果
            if result.get('success'):
                logger.info("YouTube订阅视频获取任务完成",
                           subscriptions=result.get('subscriptions_count', 0),
                           channels=result.get('channels_count', 0),
                           videos_fetched=result.get('videos_fetched', 0),
                           videos_new=result.get('videos_new', 0),
                           execution_time_ms=result.get('execution_duration_ms'))
                
                # 构建成功消息
                if 'channels_count' in result:
                    message = f"✅ 成功获取 {result.get('channels_count', 0)} 个频道，" + \
                             f"新增 {result.get('videos_new', 0)} 个视频，" + \
                             f"已存在 {result.get('videos_existing', 0)} 个"
                else:
                    message = f"✅ 成功获取 {result.get('videos_fetched', 0)} 个视频，" + \
                             f"新增 {result.get('videos_new', 0)} 个"
                
                return {
                    'success': True,
                    'message': message,
                    'data': result
                }
            else:
                logger.error("YouTube订阅视频获取任务失败",
                            error=result.get('message'))
                
                return {
                    'success': False,
                    'message': f"❌ {result.get('message', 'YouTube订阅任务执行失败')}",
                    'data': result
                }
            
        except Exception as e:
            error_msg = f"YouTube订阅任务执行异常: {str(e)}"
            logger.error(error_msg, exception=str(e))
            
            return {
                'success': False,
                'message': f"❌ {error_msg}",
                'error': str(e)
            }
    
    async def _execute_news_feed_task(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        执行 EODHD 新闻 Feed 订阅任务 —— 拉取新闻并写入 subscription_content

        Args:
            params: 任务参数
                - max_results: 每次拉取最大条数（默认 200）
                - recent_days: 拉取最近 N 天的新闻（默认 1）
        """
        try:
            logger.info("执行 News Feed 订阅任务", params=params)

            max_results = params.get('max_results', 200)
            recent_days = params.get('recent_days', 1)

            from backend.services.eodhd_news_feed_service import EODHDNewsFeedService

            import asyncio
            import concurrent.futures

            loop = asyncio.get_event_loop()
            with concurrent.futures.ThreadPoolExecutor() as executor:
                result = await loop.run_in_executor(
                    executor,
                    lambda: EODHDNewsFeedService().fetch_and_distribute_news(
                        max_results=max_results,
                        recent_days=recent_days,
                    )
                )

            if result.get('success'):
                message = (
                    f"✅ 获取 {result.get('articles_fetched', 0)} 篇新闻，"
                    f"新增 {result.get('articles_new', 0)} 篇，"
                    f"匹配 {result.get('sources_matched', 0)} 个源"
                )
                return {'success': True, 'message': message, 'data': result}
            else:
                return {
                    'success': False,
                    'message': f"❌ {result.get('message', 'News Feed 任务执行失败')}",
                    'data': result,
                }

        except Exception as e:
            error_msg = f"News Feed 任务执行异常: {str(e)}"
            logger.error(error_msg, exception=str(e))
            return {'success': False, 'message': f"❌ {error_msg}", 'error': str(e)}

    async def _execute_twitter_daily_report_task(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        执行 Twitter Daily Report 生成任务 — 通过 Celery 异步分发给所有启用的用户。
        每份报告耗时 200-500s，必须走 Celery worker 避免阻塞 scheduler。

        Args:
            params: 任务参数
                - report_date: 报告日期 (YYYY-MM-DD)，默认昨天
        """
        try:
            logger.info("分发 Twitter Daily Report Celery 任务", params=params)

            from datetime import date, timedelta, datetime
            from backend.tasks.report_tasks import generate_all_twitter_daily_reports_task

            report_date_str = params.get('report_date')
            if report_date_str:
                report_date = datetime.strptime(report_date_str, '%Y-%m-%d').date()
            else:
                report_date = date.today() - timedelta(days=1)

            task = generate_all_twitter_daily_reports_task.delay(report_date=str(report_date))

            message = (
                f"✅ Twitter Daily Report ({report_date}): "
                f"已提交 Celery 异步任务 {task.id}"
            )
            return {'success': True, 'message': message, 'celery_task_id': task.id}

        except Exception as e:
            error_msg = f"Twitter Daily Report 任务分发异常: {str(e)}"
            logger.error(error_msg, exception=str(e))
            return {'success': False, 'message': f"❌ {error_msg}", 'error': str(e)}

    async def _execute_cctv_news_task(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """执行CCTV新闻爬取任务"""
        try:
            logger.info("开始执行CCTV新闻爬取任务", params=params)
            
            # 导入服务
            from backend.services.cctv_news_service import create_cctv_news_service
            cctv_service = create_cctv_news_service()
            
            # 提取参数
            date = params.get('date')  # 可选：指定日期(YYYYMMDD格式)
            max_news = params.get('max_news')  # 可选：限制爬取数量
            auto_analyze = params.get('auto_analyze', True)  # 默认启用自动分析
            
            # 🔥 支持多模板（向后兼容）
            analysis_templates = params.get('analysis_templates')
            if not analysis_templates:
                # 兼容旧的单模板配置
                single_template = params.get('analysis_template', 'news_cctv_summary_v1')
                analysis_templates = [single_template, 'news_classification']
            
            max_concurrent = params.get('max_concurrent', 5)  # 默认并发5
            enable_theme_matching = params.get('enable_theme_matching', True)  # 默认启用主题匹配
            
            logger.info(f"将使用 {len(analysis_templates)} 个模板进行分析: {analysis_templates}")
            
            # 关键：在线程池中执行同步方法（爬虫是同步的，避免阻塞事件循环）
            import asyncio
            import concurrent.futures
            
            with concurrent.futures.ThreadPoolExecutor() as executor:
                result = await asyncio.get_event_loop().run_in_executor(
                    executor,
                    cctv_service.fetch_and_store_news,
                    date,
                    max_news,
                    'cctv_news',
                    auto_analyze,
                    analysis_templates,
                    max_concurrent,
                    enable_theme_matching
                )
            
            if result['success']:
                data = result.get('data', {})
                fetched_count = data.get('fetched_count', 0)
                new_count = data.get('new_count', 0)
                existing_count = data.get('existing_count', 0)
                analysis_triggered = data.get('analysis_triggered', False)
                celery_tasks = data.get('celery_tasks', [])
                
                message = f"✅ {result['message']}"
                
                if celery_tasks:
                    task_summary = ', '.join([f"{t['template']}({t['task_id'][:8]})" for t in celery_tasks])
                    message += f"，已提交 {len(celery_tasks)} 个模板分析任务: {task_summary}"
                
                logger.info("CCTV新闻爬取任务执行成功", 
                          fetched=fetched_count,
                          new=new_count,
                          existing=existing_count,
                          analysis_triggered=analysis_triggered,
                          celery_tasks_count=len(celery_tasks))
                
                return {
                    'success': True,
                    'message': message,
                    'data': data
                }
            else:
                logger.error("CCTV新闻爬取任务执行失败", error=result['message'])
                return {
                    'success': False,
                    'message': f"❌ {result['message']}",
                    'data': result.get('data')
                }
                
        except Exception as e:
            error_msg = f"CCTV新闻爬取任务执行异常: {str(e)}"
            logger.error(error_msg, exception=str(e))
            return {
                'success': False,
                'message': f"❌ {error_msg}",
                'data': None
            }
    
    async def _execute_economic_events_task(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        执行经济事件获取任务
        
        Args:
            params: 任务参数
                - start_date: 开始日期 (YYYY-MM-DD)，可选
                - end_date: 结束日期 (YYYY-MM-DD)，可选
                - days: 从今天开始的天数，可选（默认30天）
                - fetch_next_week: 是否获取下周数据，可选（默认False）
                - country: 国家代码，可选
        
        Returns:
            Dict包含任务执行结果
        """
        try:
            logger.info(f"开始执行经济事件获取任务 | params={params}")
            
            # 关键：在线程池中执行同步方法（避免阻塞事件循环）
            import asyncio
            import concurrent.futures
            from datetime import datetime, timedelta
            
            def _fetch_and_store_economic_events():
                """同步方法：获取并存储经济事件数据"""
                # 导入所需的服务
                from backend.services.economic_events_service import EconomicEventsService
                from backend.services.market_data_service import create_market_data_service
                
                # 初始化服务
                api_client = EconomicEventsService()
                market_service = create_market_data_service()
                
                # 计算日期范围（参考 Earnings Calendar 的方式）
                from_date = None
                to_date = None
                
                if 'start_date' in params and 'end_date' in params:
                    # 使用指定的日期范围
                    from_date = params['start_date']
                    to_date = params['end_date']
                elif 'fetch_next_week' in params and params['fetch_next_week']:
                    # 获取下周数据（周一到周日）
                    today = datetime.now()
                    days_until_monday = (7 - today.weekday()) % 7
                    if days_until_monday == 0:
                        days_until_monday = 7
                    next_monday = today + timedelta(days=days_until_monday)
                    next_sunday = next_monday + timedelta(days=6)
                    from_date = next_monday.strftime('%Y-%m-%d')
                    to_date = next_sunday.strftime('%Y-%m-%d')
                elif 'days' in params:
                    # 使用相对天数
                    days = params['days']
                    today = datetime.now()
                    from_date = today.strftime('%Y-%m-%d')
                    to_date = (today + timedelta(days=days)).strftime('%Y-%m-%d')
                else:
                    # 默认获取未来30天的数据
                    today = datetime.now()
                    from_date = today.strftime('%Y-%m-%d')
                    to_date = (today + timedelta(days=30)).strftime('%Y-%m-%d')
                
                # 处理多个国家的情况
                countries = []
                if 'country' in params:
                    country_param = params['country']
                    if isinstance(country_param, list):
                        # 如果是数组
                        countries = country_param
                    elif isinstance(country_param, str):
                        # 如果是字符串，支持逗号分隔
                        countries = [c.strip() for c in country_param.split(',') if c.strip()]
                    else:
                        countries = [str(country_param)]
                
                logger.info(f"经济事件请求参数: from_date={from_date}, to_date={to_date}, countries={countries if countries else 'All'}")
                
                # 收集所有国家的数据
                all_events_data = []
                
                if countries:
                    # 对每个国家分别请求
                    for country in countries:
                        api_params = {
                            'from_date': from_date,
                            'to_date': to_date,
                            'country': country,
                            'limit': 1000
                        }
                        logger.info(f"获取国家 {country} 的经济事件数据")
                        country_events = api_client.get_economic_events(**api_params)
                        if country_events:
                            all_events_data.extend(country_events)
                            logger.info(f"国家 {country} 获取到 {len(country_events)} 条数据")
                else:
                    # 没有指定国家，获取所有国家
                    api_params = {
                        'from_date': from_date,
                        'to_date': to_date,
                        'limit': 1000
                    }
                    all_events_data = api_client.get_economic_events(**api_params)
                
                if not all_events_data:
                    return {
                        'success': True,
                        'message': '未获取到新数据',
                        'data': {'total_fetched': 0, 'new_records': 0}
                    }
                
                # 使用收集到的数据
                events_data = all_events_data
                
                # 格式化数据并计算impact
                formatted_data = []
                for event in events_data:
                    formatted_event = api_client.format_event_data(event)
                    
                    # 计算impact值
                    event_name = formatted_event.get('event', '')
                    # 简单的impact计算：根据事件名称判断重要性
                    formatted_event['impact'] = 1  # 默认值
                    if event_name:
                        # 重要事件关键词
                        high_impact_keywords = ['Interest Rate', 'Non-Farm Payrolls', 'GDP', 'CPI', 'Inflation', 'Unemployment', 'FOMC', 'Fed', 'Central Bank']
                        medium_impact_keywords = ['PMI', 'Employment', 'Retail Sales', 'Consumer', 'Trade Balance']
                        
                        event_lower = event_name.lower()
                        if any(kw.lower() in event_lower for kw in high_impact_keywords):
                            formatted_event['impact'] = 3
                        elif any(kw.lower() in event_lower for kw in medium_impact_keywords):
                            formatted_event['impact'] = 2
                    
                    formatted_data.append(formatted_event)
                
                # 保存到数据库
                batch_result = market_service.batch_insert_economic_events(formatted_data)
                inserted_count = batch_result.get('inserted', 0)
                updated_count = batch_result.get('updated', 0)
                total_before_dedup = batch_result.get('total_before_dedup', len(formatted_data))
                duplicates_removed = batch_result.get('duplicates_removed', 0)
                
                # 构建消息
                country_info = f" ({', '.join(countries)} 国家)" if countries else " (所有国家)"
                
                # 构建详细的执行信息
                dedup_info = f"，去重移除{duplicates_removed}条" if duplicates_removed > 0 else ""
                
                logger.info(
                    f"经济事件任务执行完成: API返回{len(events_data)}条，格式化{len(formatted_data)}条{dedup_info}，"
                    f"新增{inserted_count}条，更新{updated_count}条 (日期范围: {from_date} 至 {to_date}{country_info})"
                )
                
                # 构建用户友好的消息
                message_parts = [
                    f"成功获取并保存 {inserted_count} 条新增经济事件，更新 {updated_count} 条现有记录"
                ]
                if duplicates_removed > 0:
                    message_parts.append(f"去重移除 {duplicates_removed} 条重复数据")
                message_parts.append(f"(日期: {from_date} 至 {to_date}{country_info})")
                
                return {
                    'success': True,
                    'message': '，'.join(message_parts),
                    'data': {
                        'api_fetched': len(events_data),
                        'formatted': len(formatted_data),
                        'before_dedup': total_before_dedup,
                        'duplicates_removed': duplicates_removed,
                        'after_dedup': total_before_dedup - duplicates_removed,
                        'new_records': inserted_count,
                        'updated_records': updated_count,
                        'date_range': {
                            'from_date': from_date,
                            'to_date': to_date
                        },
                        'countries': countries if countries else None,
                        'params': params
                    }
                }
            
            # 在线程池中执行同步方法
            with concurrent.futures.ThreadPoolExecutor() as executor:
                result = await asyncio.get_event_loop().run_in_executor(
                    executor,
                    _fetch_and_store_economic_events
                )
            
            if result['success']:
                message = f"✅ {result['message']}"
                logger.info("经济事件任务执行成功", result_msg=message, data=result.get('data'))
                return result
            else:
                logger.error("经济事件任务执行失败", error=result['message'])
                return {
                    'success': False,
                    'message': f"❌ {result['message']}"
                }
                
        except Exception as e:
            error_msg = f"经济事件任务执行异常: {str(e)}"
            logger.error(error_msg, exception=str(e))
            return {
                'success': False,
                'message': f"❌ {error_msg}",
                'data': None
            }
    
    async def _execute_macro_indicators_task(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """执行宏观指标获取任务"""
        try:
            logger.info(f"开始执行宏观指标获取任务，参数: {params}")
            
            # 获取参数
            countries = params.get('country', [])
            indicators = params.get('indicator', [])
            
            # 参数验证
            if not countries:
                logger.warning("未指定国家参数，跳过宏观指标获取任务")
                return {
                    'success': False, 
                    'message': '未指定国家参数，请至少选择一个国家'
                }
            
            # 确保countries是列表格式
            if isinstance(countries, str):
                countries = [countries]
            
            # 确保indicators是列表格式
            if isinstance(indicators, str):
                indicators = [indicators]
            
            # 如果没有指定指标，获取所有可用指标
            if not indicators:
                from backend.services.macro_indicators_service import MacroIndicatorsService
                macro_service = MacroIndicatorsService()
                indicators = list(macro_service.AVAILABLE_INDICATORS.keys())
                logger.info(f"未指定指标，将获取所有 {len(indicators)} 个指标")
            
            logger.info(f"准备获取 {len(countries)} 个国家的 {len(indicators)} 个指标数据")
            
            # 初始化宏观指标服务
            from backend.services.macro_indicators_service import MacroIndicatorsService
            macro_service = MacroIndicatorsService()
            
            total_success = 0
            total_failed = 0
            results = []
            
            # 遍历每个国家
            for country in countries:
                logger.info(f"开始获取国家 {country} 的宏观指标数据")
                
                # 如果指定了特定指标，逐个获取
                if indicators and len(indicators) > 0:
                    for indicator in indicators:
                        try:
                            logger.info(f"获取 {country} 的 {indicator} 指标数据")
                            data = macro_service.get_macro_indicator(country, indicator)
                            if data:
                                total_success += 1
                                results.append({
                                    'country': country,
                                    'indicator': indicator,
                                    'data_count': len(data),
                                    'status': 'success'
                                })
                                logger.info(f"成功获取 {country} 的 {indicator} 数据，共 {len(data)} 条记录")
                            else:
                                total_failed += 1
                                results.append({
                                    'country': country,
                                    'indicator': indicator,
                                    'data_count': 0,
                                    'status': 'failed',
                                    'error': '未获取到数据'
                                })
                                logger.warning(f"获取 {country} 的 {indicator} 数据失败")
                            
                            # 避免API请求过于频繁
                            import asyncio
                            await asyncio.sleep(0.1)
                            
                        except Exception as e:
                            total_failed += 1
                            error_msg = f"获取 {country} 的 {indicator} 数据时出错: {str(e)}"
                            logger.error(error_msg)
                            results.append({
                                'country': country,
                                'indicator': indicator,
                                'data_count': 0,
                                'status': 'error',
                                'error': str(e)
                            })
                else:
                    # 获取该国家的所有指标
                    try:
                        logger.info(f"获取 {country} 的所有指标数据")
                        data = macro_service.get_all_indicators_for_country(country)
                        if data:
                            total_success += 1
                            results.append({
                                'country': country,
                                'indicator': 'all',
                                'data_count': len(data),
                                'status': 'success'
                            })
                            logger.info(f"成功获取 {country} 的所有指标数据，共 {len(data)} 条记录")
                        else:
                            total_failed += 1
                            results.append({
                                'country': country,
                                'indicator': 'all',
                                'data_count': 0,
                                'status': 'failed',
                                'error': '未获取到数据'
                            })
                            logger.warning(f"获取 {country} 的所有指标数据失败")
                    except Exception as e:
                        total_failed += 1
                        error_msg = f"获取 {country} 的所有指标数据时出错: {str(e)}"
                        logger.error(error_msg)
                        results.append({
                            'country': country,
                            'indicator': 'all',
                            'data_count': 0,
                            'status': 'error',
                            'error': str(e)
                        })
            
            # 生成执行结果消息
            if total_success > 0:
                message = f"宏观指标获取任务执行完成: 成功 {total_success} 个，失败 {total_failed} 个"
                if total_failed > 0:
                    message += f"。失败详情: {[r for r in results if r['status'] != 'success']}"
            else:
                message = f"宏观指标获取任务执行失败: 所有 {total_failed} 个请求都失败了"
            
            logger.info(f"宏观指标获取任务执行完成: {message}")
            
            return {
                'success': total_success > 0,
                'message': message,
                'data': {
                    'total_requests': total_success + total_failed,
                    'successful': total_success,
                    'failed': total_failed,
                    'results': results,
                    'params': params
                }
            }
            
        except Exception as e:
            error_msg = f"宏观指标获取任务执行失败: {str(e)}"
            logger.error(error_msg)
            return {
                'success': False,
                'message': error_msg,
                'data': None
            }
    
    async def _execute_fundamentals_task(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        执行EODHD基本面数据获取任务（通过Celery异步执行）
        
        注意：此方法会等待Celery任务完成（可能需要5-15分钟），以便记录准确的执行结果
        
        Args:
            params: 任务参数
                - exchanges: 交易所列表
                - data_types: 数据类型列表
                - wait_for_completion: 是否等待任务完成（默认: True）
                - timeout: 任务超时时间（秒，默认: 1800 = 30分钟）
        
        Returns:
            Dict包含任务执行结果
        """
        import asyncio
        from celery.result import AsyncResult
        
        try:
            logger.info("提交EODHD基本面数据获取任务到Celery", params=params)
            
            # 获取参数
            exchanges = params.get('exchanges', [])
            data_types = params.get('data_types', ['General', 'Financials', 'Highlights', 'Valuation', 'Technicals', 'Holders', 'outstandingShares', 'Earnings', 'SharesStats', 'AnalystRatings', 'SplitsDividends', 'InsiderTransactions'])
            wait_for_completion = params.get('wait_for_completion', True)
            timeout = params.get('timeout', 1800)  # 默认30分钟超时
            
            # 参数验证
            if not exchanges:
                logger.warning("未指定交易所参数，跳过基本面数据获取任务")
                return {
                    'success': False, 
                    'message': '未指定交易所参数，请至少选择一个交易所'
                }
            
            # 确保exchanges是列表格式
            if isinstance(exchanges, str):
                exchanges = [exchanges]
            
            # 验证支持的交易所
            supported_exchanges = ExchangeConfig.get_supported_exchanges('fundamentals')
            valid_exchanges = [ex for ex in exchanges if ExchangeConfig.is_exchange_supported(ex, 'fundamentals')]
            
            if not valid_exchanges:
                logger.warning(f"不支持的交易所: {exchanges}，支持的交易所: {supported_exchanges}")
                return {
                    'success': False, 
                    'message': f'不支持的交易所，支持的交易所: {", ".join(supported_exchanges)}'
                }
            
            # 验证数据类型
            supported_data_types = ['General', 'Financials', 'Highlights', 'Valuation', 'Technicals', 'Holders', 'outstandingShares', 'Earnings', 'SharesStats', 'AnalystRatings', 'SplitsDividends', 'InsiderTransactions']
            valid_data_types = [dt for dt in data_types if dt in supported_data_types]
            
            if not valid_data_types:
                logger.warning(f"无效的数据类型: {data_types}，支持的数据类型: {supported_data_types}")
                valid_data_types = ['General', 'Financials', 'Highlights', 'Valuation', 'Technicals', 'Holders', 'outstandingShares', 'Earnings', 'SharesStats', 'AnalystRatings', 'SplitsDividends', 'InsiderTransactions']
            
            logger.info(f"准备获取 {len(valid_exchanges)} 个交易所的基本面数据，数据类型: {valid_data_types}")
            
            # 导入Celery任务
            from backend.tasks.data_tasks import fetch_eodhd_fundamentals_task
            
            # 提交Celery任务
            task = fetch_eodhd_fundamentals_task.apply_async(
                kwargs={
                    'exchanges': valid_exchanges,
                    'data_types': valid_data_types
                }
            )
            
            logger.info(f"EODHD基本面数据获取任务已提交到Celery: task_id={task.id}")
            
            # 如果不等待完成，立即返回
            if not wait_for_completion:
                return {
                    'success': True,
                    'message': f"EODHD基本面数据获取任务已提交到Celery队列（任务ID: {task.id}）",
                    'data': {
                        'task_id': task.id,
                        'status': 'PENDING',
                        'exchanges': valid_exchanges,
                        'data_types': valid_data_types
                    }
                }
            
            # 等待任务完成（异步等待，不阻塞事件循环）
            logger.info(f"等待Celery任务完成: {task.id} (超时: {timeout}秒)")
            
            try:
                # 在线程池中执行阻塞的 task.get()
                loop = asyncio.get_event_loop()
                result = await loop.run_in_executor(
                    None,  # 使用默认线程池
                    lambda: task.get(timeout=timeout, propagate=False)
                )
                
                # 检查任务结果
                if isinstance(result, Exception):
                    # 任务执行失败
                    error_msg = f"EODHD基本面数据获取任务执行失败: {str(result)}"
                    logger.error(error_msg, task_id=task.id)
                    return {
                        'success': False,
                        'message': error_msg,
                        'data': {
                            'task_id': task.id,
                            'error': str(result)
                        }
                    }
                
                # 任务执行成功，返回结果
                if result and result.get('success'):
                    # 直接返回Celery任务的结果
                    logger.info("EODHD基本面数据获取任务执行成功", 
                               task_id=task.id,
                               result_message=result.get('message'))
                    return result
                else:
                    # 任务执行失败
                    error_msg = result.get('message', '未知错误') if result else '任务返回结果为空'
                    logger.error(f"EODHD基本面数据获取失败: {error_msg}", task_id=task.id)
                    return {
                        'success': False,
                        'message': f"EODHD基本面数据获取失败: {error_msg}",
                        'data': {'task_id': task.id}
                    }
                    
            except asyncio.TimeoutError:
                # 任务超时
                error_msg = f"EODHD基本面数据获取任务超时（超过{timeout}秒）"
                logger.error(error_msg, task_id=task.id)
                # 尝试撤销任务
                try:
                    task.revoke(terminate=True)
                except:
                    pass
                return {
                    'success': False,
                    'message': error_msg,
                    'data': {'task_id': task.id}
                }
                
        except Exception as e:
            error_msg = f"执行EODHD基本面数据获取任务失败: {str(e)}"
            logger.error(error_msg, exception=str(e))
            return {
                'success': False,
                'message': error_msg,
                'data': None
            }
    
    async def _execute_watchlist_fundamentals_task(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        执行 Watchlist Fundamentals 批量更新任务（通过Celery异步执行）
        
        此任务会批量更新 target_tracking_config 表中所有活跃股票的基本面数据
        
        Args:
            params: 任务参数
                - data_types: 要更新的数据类型列表（可选，默认全部12种）
                - skip_recent_hours: 跳过最近N小时内已更新的股票（默认24小时）
                - wait_for_completion: 是否等待任务完成（默认: False，后台执行）
                - timeout: 任务超时时间（秒，默认: 3600 = 1小时）
        
        Returns:
            Dict包含任务执行结果
        """
        import asyncio
        from celery.result import AsyncResult
        
        try:
            logger.info("提交 Watchlist Fundamentals 批量更新任务到 Celery", params=params)
            
            # 获取参数
            data_types = params.get('data_types')  # None 表示使用默认（全部12种）
            skip_recent_hours = params.get('skip_recent_hours', 24)
            wait_for_completion = params.get('wait_for_completion', True)  # 默认等待完成以获取详细结果
            timeout = params.get('timeout', 3600)  # 默认1小时超时
            
            logger.info(f"准备批量更新 watchlist fundamentals 数据",
                       data_types=data_types or "all",
                       skip_recent_hours=skip_recent_hours)
            
            # 导入Celery任务
            from backend.tasks.data_tasks import update_watchlist_fundamentals_batch_task
            
            # 提交Celery任务
            task = update_watchlist_fundamentals_batch_task.apply_async(
                kwargs={
                    'data_types': data_types,
                    'skip_recent_hours': skip_recent_hours
                }
            )
            
            logger.info(f"Watchlist fundamentals 批量更新任务已提交到 Celery: task_id={task.id}")
            
            # 如果不等待完成，立即返回
            if not wait_for_completion:
                return {
                    'success': True,
                    'message': f"Watchlist fundamentals 批量更新任务已提交到 Celery 队列（任务ID: {task.id}）",
                    'data': {
                        'task_id': task.id,
                        'status': 'PENDING',
                        'data_types': data_types or 'all',
                        'skip_recent_hours': skip_recent_hours
                    }
                }
            
            # 等待任务完成（异步等待，不阻塞事件循环）
            logger.info(f"等待 Celery 任务完成: {task.id} (超时: {timeout}秒)")
            
            try:
                # 在线程池中执行阻塞的 task.get()
                loop = asyncio.get_event_loop()
                result = await loop.run_in_executor(
                    None,  # 使用默认线程池
                    lambda: task.get(timeout=timeout, propagate=False)
                )
                
                # 检查任务结果
                if isinstance(result, Exception):
                    # 任务执行失败
                    error_msg = f"Watchlist fundamentals 批量更新任务执行失败: {str(result)}"
                    logger.error(error_msg, task_id=task.id)
                    return {
                        'success': False,
                        'message': error_msg,
                        'data': {
                            'task_id': task.id,
                            'error': str(result)
                        }
                    }
                
                # 任务执行成功，返回结果
                if result and result.get('success'):
                    # 直接返回Celery任务的结果
                    logger.info("Watchlist fundamentals 批量更新任务执行成功", 
                               task_id=task.id,
                               result_message=result.get('message'))
                    return result
                else:
                    # 任务执行失败
                    error_msg = result.get('message', '未知错误') if result else '任务返回结果为空'
                    logger.error(f"Watchlist fundamentals 批量更新失败: {error_msg}", task_id=task.id)
                    return {
                        'success': False,
                        'message': f"Watchlist fundamentals 批量更新失败: {error_msg}",
                        'data': {'task_id': task.id}
                    }
                    
            except asyncio.TimeoutError:
                # 任务超时
                error_msg = f"Watchlist fundamentals 批量更新任务超时（超过{timeout}秒）"
                logger.error(error_msg, task_id=task.id)
                # 尝试撤销任务
                try:
                    task.revoke(terminate=True)
                except:
                    pass
                return {
                    'success': False,
                    'message': error_msg,
                    'data': {'task_id': task.id}
                }
                
        except Exception as e:
            error_msg = f"执行 Watchlist fundamentals 批量更新任务失败: {str(e)}"
            logger.error(error_msg, exception=str(e))
            return {
                'success': False,
                'message': error_msg,
                'data': None
            }
    
    async def _execute_finviz_screener_task(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """执行Finviz筛选数据获取任务"""
        try:
            logger.info(f"开始执行Finviz筛选数据获取任务，参数: {params}")
            
            # Finviz Screener 任务不需要参数配置，直接执行
            logger.info("准备获取Finviz筛选数据")
            
            # 初始化Finviz服务
            from backend.services.finviz_screener_service import get_finviz_service
            finviz_service = await get_finviz_service()
            
            # 执行Finviz数据获取
            result = await finviz_service.fetch_and_store_finviz_data()
            
            if result['success']:
                success_msg = f"Finviz筛选数据获取成功: {result['message']}"
                logger.info(success_msg)
                return {
                    'success': True,
                    'message': success_msg,
                    'data': result['data']
                }
            else:
                error_msg = f"Finviz筛选数据获取失败: {result['message']}"
                logger.error(error_msg)
                return {
                    'success': False,
                    'message': error_msg,
                    'data': result['data']
                }
            
        except Exception as e:
            error_msg = f"Finviz筛选数据获取任务执行失败: {str(e)}"
            logger.error(error_msg)
            return {
                'success': False,
                'message': error_msg,
                'data': None
            }
    
    async def _execute_eodhd_stock_prices_task(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """执行EODHD股票价格获取任务"""
        try:
            logger.info("执行EODHD股票价格获取任务", params=params)
            
            # 导入股价服务
            from backend.services.eodhd_stock_prices_service import create_eodhd_stock_prices_service
            
            # 创建服务实例
            stock_prices_service = create_eodhd_stock_prices_service()
            
            # 在线程池中执行耗时的价格获取操作，避免阻塞事件循环
            import asyncio
            import concurrent.futures
            
            with concurrent.futures.ThreadPoolExecutor() as executor:
                result = await asyncio.get_event_loop().run_in_executor(
                    executor,
                    stock_prices_service.fetch_and_store_watchlist_prices
                )
            
            if result.get('error'):
                error_msg = f"EODHD股票价格获取失败: {result['error']}"
                logger.error(error_msg)
                return {
                    'success': False,
                    'message': error_msg,
                    'data': result
                }
            else:
                success_msg = f"EODHD股票价格获取成功: 处理了{result['total_tickers']}个ticker，成功{result['success_count']}个，失败{result['failed_count']}个，耗时{result['execution_time']:.2f}秒"
                logger.info(success_msg)
                return {
                    'success': True,
                    'message': success_msg,
                    'data': result
                }
                
        except Exception as e:
            error_msg = f"EODHD股票价格获取任务执行失败: {str(e)}"
            logger.error(error_msg)
            return {
                'success': False,
                'message': error_msg,
                'data': None
            }
    
    async def _execute_eodhd_eod_stock_prices_task(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        执行EODHD历史股价数据获取任务（通过Celery异步执行，支持多周期）
        
        功能：
        - 获取watchlist中所有股票的历史价格数据（从target_tracking_config表）
        - 获取theme_company_associations表中所有股票的历史价格数据
        - 支持同时获取多个周期的数据（日线、周线、月线）
        - 自动合并并去重，存储到stock_prices_eod表
        
        注意：此方法会等待Celery任务完成（可能需要3-10分钟），以便记录准确的执行结果
        
        Args:
            params: 任务参数
                - days_back: 回溯天数（默认: 60天）
                - periods: 数据周期列表 ['d', 'w', 'm']（默认: ['d', 'w', 'm']）
                - wait_for_completion: 是否等待任务完成（默认: True）
                - timeout: 任务超时时间（秒，默认: 1200 = 20分钟）
        
        Returns:
            Dict包含任务执行结果
        """
        import asyncio
        from celery.result import AsyncResult
        
        try:
            logger.info("提交EODHD历史股价数据获取任务到Celery（多周期）", params=params)
            
            # 获取参数
            days_back = params.get('days_back', 60)
            periods = params.get('periods', ['d', 'w', 'm'])  # 默认获取所有周期
            wait_for_completion = params.get('wait_for_completion', True)
            timeout = params.get('timeout', 1200)  # 默认20分钟超时（多周期需要更长时间）
            
            # 确保periods是列表格式
            if isinstance(periods, str):
                periods = [periods]
            
            logger.info(f"准备获取 {len(periods)} 个周期的数据: {periods}")
            
            # 导入Celery任务
            from backend.tasks.data_tasks import fetch_eodhd_eod_stock_prices_task
            
            # 提交Celery任务（传入periods参数）
            task = fetch_eodhd_eod_stock_prices_task.apply_async(
                kwargs={'days_back': days_back, 'periods': periods}
            )
            
            logger.info(f"EODHD历史股价数据获取任务已提交到Celery: task_id={task.id}, periods={periods}")
            
            # 如果不等待完成，立即返回
            if not wait_for_completion:
                return {
                    'success': True,
                    'message': f"EODHD历史股价数据获取任务已提交到Celery队列（任务ID: {task.id}, 周期: {periods}）",
                    'data': {
                        'task_id': task.id,
                        'status': 'PENDING',
                        'days_back': days_back,
                        'periods': periods
                    }
                }
            
            # 等待任务完成（异步等待，不阻塞事件循环）
            logger.info(f"等待Celery任务完成: {task.id} (超时: {timeout}秒)")
            
            try:
                # 在线程池中执行阻塞的 task.get()
                loop = asyncio.get_event_loop()
                result = await loop.run_in_executor(
                    None,  # 使用默认线程池
                    lambda: task.get(timeout=timeout, propagate=False)
                )
                
                # 检查任务结果
                if isinstance(result, Exception):
                    # 任务执行失败
                    error_msg = f"EODHD历史股价数据获取任务执行失败: {str(result)}"
                    logger.error(error_msg, task_id=task.id)
                    return {
                        'success': False,
                        'message': error_msg,
                        'data': {
                            'task_id': task.id,
                            'error': str(result)
                        }
                    }
                
                # 任务执行成功，返回结果
                if result and result.get('success'):
                    logger.info("EODHD历史股价数据获取任务执行成功", 
                               task_id=task.id,
                               message=result.get('message'))
                    return result
                else:
                    # 任务执行失败
                    error_msg = result.get('message', '未知错误') if result else '任务返回结果为空'
                    logger.error(f"EODHD历史股价数据获取失败: {error_msg}", task_id=task.id)
                    return {
                        'success': False,
                        'message': f"EODHD历史股价数据获取失败: {error_msg}",
                        'data': {'task_id': task.id}
                    }
                    
            except asyncio.TimeoutError:
                # 任务超时
                error_msg = f"EODHD历史股价数据获取任务超时（超过{timeout}秒）"
                logger.error(error_msg, task_id=task.id)
                # 尝试撤销任务
                try:
                    task.revoke(terminate=True)
                except:
                    pass
                return {
                    'success': False,
                    'message': error_msg,
                    'data': {'task_id': task.id}
                }
                
        except Exception as e:
            error_msg = f"执行EODHD历史股价数据获取任务失败: {str(e)}"
            logger.error(error_msg, exception=str(e))
            return {
                'success': False,
                'message': error_msg,
                'data': None
            }
    
    async def _execute_news_analysis_task(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        执行新闻LLM分析任务
        
        Args:
            params: 任务参数
                - template_code: Prompt模板代码（默认: news_summary_v1）
                - batch_size: 每次分析的新闻数量（默认: 10）
                - symbols: 筛选特定股票代码的新闻（可选）
                - start_date: 开始日期（可选）
                - end_date: 结束日期（可选）
                - use_concurrent: 是否使用并发（默认: False）
                - max_concurrent: 最大并发数（默认: 3）
        
        Returns:
            Dict包含任务执行结果
        """
        try:
            logger.info("开始执行新闻LLM分析任务", params=params)
            
            # 导入新闻分析服务
            from backend.services.news_analysis_service import NewsAnalysisService
            from backend.utils.database_config import get_database_config
            
            # 创建服务实例
            db_config = get_database_config()
            news_analysis_service = NewsAnalysisService(db_config)
            
            # 获取参数
            template_code = params.get('template_code', 'news_summary_v1')
            batch_size = params.get('batch_size', 10)
            symbols = params.get('symbols')  # 可以是逗号分隔的字符串或列表
            start_date = params.get('start_date')
            end_date = params.get('end_date')
            use_concurrent = params.get('use_concurrent', False)
            max_concurrent = params.get('max_concurrent', 3)
            
            # 处理symbols参数
            if symbols and isinstance(symbols, str):
                symbols = [s.strip() for s in symbols.split(',') if s.strip()]
            
            # 执行批量分析
            result = await news_analysis_service.batch_analyze_news(
                limit=batch_size,
                template_code=template_code,
                symbols=symbols,
                start_date=start_date,
                end_date=end_date,
                use_concurrent=use_concurrent,
                max_concurrent=max_concurrent
            )
            
            if result['total'] == 0:
                info_msg = "没有需要分析的新闻"
                logger.info(info_msg)
                return {
                    'success': True,
                    'message': info_msg,
                    'data': result
                }
            elif result['failed'] > 0:
                warning_msg = f"新闻分析部分成功: 成功{result['success']}条，失败{result['failed']}条"
                logger.warning(warning_msg)
                return {
                    'success': True,
                    'message': warning_msg,
                    'data': result
                }
            else:
                success_msg = f"新闻分析全部成功: 分析了{result['total']}条新闻"
                logger.info(success_msg)
                return {
                    'success': True,
                    'message': success_msg,
                    'data': result
                }
                
        except Exception as e:
            error_msg = f"新闻分析任务执行失败: {str(e)}"
            logger.error(error_msg)
            return {
                'success': False,
                'message': error_msg,
                'data': None
            }
    
    # _execute_futu_themes_task 已删除
    # 原因：移除 futu 爬虫相关功能，改为纯手动主题管理
    
    async def _execute_theme_sync_task(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        执行主题同步任务
        将EODHD、Vector Retrieval、LLM分类的主题数据同步到统一表
        
        Args:
            params: 任务参数
                - days: 同步最近N天的新闻（默认: 7）
                - force_resync: 是否强制重新同步（默认: False）
                - sync_unsynced: 是否只同步未同步的新闻（默认: True）
        
        Returns:
            Dict包含任务执行结果
        """
        try:
            logger.info("开始执行主题同步任务", params=params)
            
            # 导入必要的模块
            from backend.database.async_db_manager import AsyncDatabaseManager
            from backend.services.theme_unification_service import ThemeUnificationService
            from backend.utils.database_config import get_database_config
            
            # 创建服务实例
            db_config = get_database_config()
            async_db = AsyncDatabaseManager(db_config)
            service = ThemeUnificationService(async_db)
            
            # 获取参数
            days = params.get('days', 7)
            force_resync = params.get('force_resync', False)
            sync_unsynced = params.get('sync_unsynced', True)
            
            # 执行同步
            if sync_unsynced:
                logger.info("同步未同步的新闻主题")
                result = await service.sync_unsynced_news()
            else:
                logger.info(f"同步最近 {days} 天的新闻主题")
                result = await service.sync_recent_news(days=days, force_resync=force_resync)
            
            # 构建返回结果
            if result.success:
                total_themes = sum(result.details.values()) if result.details else 0
                message = f"主题同步成功: {result.synced_count} 条新闻，共 {total_themes} 条主题"
                
                logger.info(message, details=result.details)
                
                return {
                    'success': True,
                    'message': message,
                    'data': {
                        'synced_count': result.synced_count,
                        'failed_count': result.failed_count,
                        'eodhd_tags': result.details.get('eodhd_auto_tag', 0),
                        'vector_matches': result.details.get('vector_retrieval', 0),
                        'llm_classifications': result.details.get('llm_classification', 0),
                        'total_themes': total_themes
                    }
                }
            else:
                error_msg = f"主题同步部分失败: {result.failed_count} 条新闻失败"
                logger.warning(error_msg, errors=result.errors[:5])
                
                return {
                    'success': False,
                    'message': error_msg,
                    'data': {
                        'synced_count': result.synced_count,
                        'failed_count': result.failed_count,
                        'errors': result.errors[:10]  # 最多返回10个错误
                    }
                }
        
        except Exception as e:
            logger.error(f"主题同步任务执行失败: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'message': f"主题同步任务执行失败: {str(e)}"
            }
    
    async def _execute_market_data_task(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        执行市场数据获取任务
        获取指数、外汇、大宗商品、债券、板块等市场数据
        
        Args:
            params: 任务参数
                - asset_types: 资产类型列表 ['index', 'forex', 'commodity', 'bond', 'sector']
                - fetch_realtime: 是否获取实时数据（默认: True）
                - fetch_eod: 是否获取EOD历史数据（默认: True）
                - eod_days: 获取最近几天的EOD数据（默认: 5）
        
        Returns:
            Dict包含任务执行结果
        """
        try:
            logger.info("开始执行市场数据获取任务", params=params)
            
            # 导入服务
            from backend.services.market_data_service import create_market_data_service
            market_data_service = create_market_data_service()
            
            # 提取参数
            asset_types = params.get('asset_types')
            fetch_realtime = params.get('fetch_realtime', True)
            fetch_eod = params.get('fetch_eod', True)
            eod_days = params.get('eod_days', 5)
            
            # 关键：在线程池中执行同步方法（避免阻塞事件循环）
            import asyncio
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as executor:
                result = await asyncio.get_event_loop().run_in_executor(
                    executor,
                    market_data_service.fetch_and_store_market_data,
                    asset_types,
                    fetch_realtime,
                    fetch_eod,
                    eod_days
                )
            
            if result['success']:
                message = f"✅ {result['message']}"
                logger.info("市场数据任务执行成功", result_msg=message, data=result.get('data'))
                return {
                    'success': True,
                    'message': message,
                    'data': result.get('data')
                }
            else:
                logger.error("市场数据任务执行失败", error=result['message'])
                return {
                    'success': False,
                    'message': f"❌ {result['message']}"
                }
                
        except Exception as e:
            error_msg = f"市场数据任务执行异常: {str(e)}"
            logger.error(error_msg, exception=str(e))
            return {
                'success': False,
                'message': f"❌ {error_msg}",
                'data': None
            }
    
    async def _execute_earnings_calendar_task(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        执行财报日历数据获取任务（使用Celery异步处理）
        
        对于13000+条数据的大批量处理，使用Celery任务队列：
        - 避免阻塞APScheduler
        - 支持进度追踪
        - 支持任务重试和错误恢复
        - 独立worker处理，不影响调度器性能
        
        Args:
            params: 任务参数
                - start_date: 开始日期 (YYYY-MM-DD)，可选
                - end_date: 结束日期 (YYYY-MM-DD)，可选
                - days: 从今天开始的天数，可选（默认7天）
                - fetch_next_week: 是否获取下周数据，可选（默认False）
        
        Returns:
            Dict包含任务执行结果
        """
        try:
            logger.info("开始执行财报日历数据获取任务（使用Celery）", params=params)
            
            # 导入Celery任务
            from backend.tasks.data_tasks import fetch_earnings_calendar_task
            
            # 提取参数
            start_date = params.get('start_date')
            end_date = params.get('end_date')
            days = params.get('days')
            fetch_next_week = params.get('fetch_next_week', False)
            
            # 提交Celery任务（异步执行）
            celery_task = fetch_earnings_calendar_task.delay(
                start_date=start_date,
                end_date=end_date,
                days=days,
                fetch_next_week=fetch_next_week
            )
            
            logger.info(f"财报日历Celery任务已提交", 
                       task_id=celery_task.id,
                       celery_task_id=celery_task.id)
            
            # 等待任务完成（可选：也可以异步返回任务ID，让调用方查询状态）
            # 这里选择等待完成，因为调度器需要记录执行结果
            try:
                result = celery_task.get(timeout=1800)  # 30分钟超时
                
                if result.get('success'):
                    message = f"✅ {result['message']}"
                    logger.info("财报日历任务执行成功", 
                               result_msg=message, 
                               data=result.get('data'),
                               celery_task_id=celery_task.id)
                    return {
                        'success': True,
                        'message': message,
                        'data': result.get('data'),
                        'celery_task_id': celery_task.id
                    }
                else:
                    logger.error("财报日历任务执行失败", 
                                error=result.get('message'),
                                celery_task_id=celery_task.id)
                    return {
                        'success': False,
                        'message': f"❌ {result.get('message', '任务执行失败')}",
                        'celery_task_id': celery_task.id
                    }
                    
            except Exception as celery_error:
                error_msg = f"Celery任务执行异常: {str(celery_error)}"
                logger.error(error_msg, 
                            exception=str(celery_error),
                            celery_task_id=celery_task.id)
                return {
                    'success': False,
                    'message': f"❌ {error_msg}",
                    'celery_task_id': celery_task.id,
                    'data': None
                }
                
        except Exception as e:
            error_msg = f"财报日历任务提交异常: {str(e)}"
            logger.error(error_msg, exception=str(e))
            return {
                'success': False,
                'message': f"❌ {error_msg}",
                'data': None
            }
    
    async def _execute_ipo_calendar_task(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        执行 IPO 日历数据获取任务（使用Celery异步处理）
        
        对于 IPO 数据的大批量处理，使用Celery任务队列：
        - 避免阻塞APScheduler
        - 支持进度追踪
        - 支持任务重试和错误恢复
        - 独立worker处理，不影响调度器性能
        
        Args:
            params: 任务参数
                - start_date: 开始日期 (YYYY-MM-DD)，可选
                - end_date: 结束日期 (YYYY-MM-DD)，可选
                - days: 从今天开始的天数，可选（默认30天）
                - fetch_next_week: 是否获取下周数据，可选（默认False）
                
        注意：EODHD API 不支持 exchanges 参数，会返回所有交易所的数据。
        如果需要过滤特定交易所，可以在查询数据库时使用 WHERE 条件。
        
        Returns:
            Dict包含任务执行结果
        """
        try:
            logger.info("开始执行 IPO 日历数据获取任务（使用Celery）", params=params)
            
            # 导入Celery任务
            from backend.tasks.data_tasks import fetch_ipo_calendar_task
            
            # 提取参数
            start_date = params.get('start_date')
            end_date = params.get('end_date')
            days = params.get('days')
            fetch_next_week = params.get('fetch_next_week', False)
            
            # 提交Celery任务（异步执行）
            # 注意：EODHD API 不支持 exchanges 参数，已移除
            celery_task = fetch_ipo_calendar_task.delay(
                start_date=start_date,
                end_date=end_date,
                days=days,
                fetch_next_week=fetch_next_week
            )
            
            logger.info(f"IPO 日历Celery任务已提交", 
                       task_id=celery_task.id,
                       celery_task_id=celery_task.id)
            
            # 等待任务完成（可选：也可以异步返回任务ID，让调用方查询状态）
            # 这里选择等待完成，因为调度器需要记录执行结果
            try:
                result = celery_task.get(timeout=1800)  # 30分钟超时
                
                if result.get('success'):
                    message = f"✅ {result['message']}"
                    logger.info("IPO 日历任务执行成功", 
                               result_msg=message, 
                               data=result.get('data'),
                               celery_task_id=celery_task.id)
                    return {
                        'success': True,
                        'message': message,
                        'data': result.get('data'),
                        'celery_task_id': celery_task.id
                    }
                else:
                    logger.error("IPO 日历任务执行失败", 
                                error=result.get('message'),
                                celery_task_id=celery_task.id)
                    return {
                        'success': False,
                        'message': f"❌ {result.get('message', '任务执行失败')}",
                        'celery_task_id': celery_task.id
                    }
                    
            except Exception as celery_error:
                error_msg = f"Celery任务执行异常: {str(celery_error)}"
                logger.error(error_msg, 
                            exception=str(celery_error),
                            celery_task_id=celery_task.id)
                return {
                    'success': False,
                    'message': f"❌ {error_msg}",
                    'celery_task_id': celery_task.id,
                    'data': None
                }
                
        except Exception as e:
            error_msg = f"IPO 日历任务提交异常: {str(e)}"
            logger.error(error_msg, exception=str(e))
            return {
                'success': False,
                'message': f"❌ {error_msg}",
                'data': None
            }
    
    def _job_listener(self, event):
        """任务执行监听器"""
        if event.exception:
            logger.error(f"任务执行出错: {event.job_id}, 异常: {event.exception}")
        else:
            logger.info(f"任务执行完成: {event.job_id}")
    
    async def _cleanup_existing_jobs(self):
        """清理所有现有任务，确保状态同步"""
        try:
            logger.info("清理现有任务，确保状态同步")
            
            # 获取所有任务
            tasks = await self._get_all_tasks()
            
            # 移除所有任务
            for task in tasks:
                task_id = task['id']
                if self.scheduler.get_job(task_id):
                    self.scheduler.remove_job(task_id)
                    logger.info(f"已移除任务: {task_id}")
            
            logger.info(f"已清理 {len(tasks)} 个任务")
            
        except Exception as e:
            logger.error(f"清理现有任务失败: {str(e)}")
    
    async def _load_saved_tasks(self):
        """加载已保存的任务"""
        try:
            logger.info("加载已保存的定时任务")
            tasks = await self._get_all_tasks()
            
            for task in tasks:
                if task['enabled']:
                    await self._add_job_to_scheduler(task)
            
            logger.info(f"已加载 {len([t for t in tasks if t['enabled']])} 个启用的定时任务")
            
        except Exception as e:
            logger.error(f"加载已保存任务失败: {str(e)}")
    
    async def _add_job_to_scheduler(self, task_config: Dict[str, Any]):
        """添加任务到调度器"""
        try:
            task_id = task_config['id']
            task_type = task_config['type']
            task_name = task_config['name']
            params = task_config.get('params', {})
            timezone_str = task_config.get('timezone', 'America/New_York')
            
            # 创建触发器
            if task_config['schedule_type'] == 'cron':
                cron_expression = task_config['cron_expression']
                cron_parts = cron_expression.split()
                minute, hour, day, month, day_of_week = cron_parts
                
                trigger = CronTrigger(
                    minute=minute,
                    hour=hour,
                    day=day,
                    month=month,
                    day_of_week=day_of_week,
                    timezone=pytz.timezone(timezone_str)
                )
            elif task_config['schedule_type'] == 'interval':
                interval_minutes = task_config['interval_minutes']
                trigger = IntervalTrigger(
                    minutes=interval_minutes,
                    timezone=pytz.timezone(timezone_str)
                )
            else:
                raise ValueError(f"不支持的调度类型: {task_config['schedule_type']}")
            
            # 添加到调度器
            self.scheduler.add_job(
                self._execute_task,
                trigger=trigger,
                args=[task_id, task_type, params],
                id=task_id,
                name=task_name,
                replace_existing=True
            )
            
            logger.info(f"任务已添加到调度器: {task_id}")
            
        except Exception as e:
            logger.error(f"添加任务到调度器失败: {str(e)}")
    
    # 执行日志记录方法
    async def _log_task_execution_start(self, task_id: str, task_name: str, task_type: str, 
                                      started_at: datetime, execution_params: Dict[str, Any]) -> Optional[int]:
        """记录任务开始执行"""
        try:
            conn = self.db_manager.get_connection()
            if not conn:
                logger.error("无法连接到数据库记录执行日志")
                return None
            
            cursor = conn.cursor()
            try:
                # 首先检查任务是否存在于 task_scheduling 表中
                cursor.execute("SELECT id FROM task_scheduling WHERE id = %s", (task_id,))
                if not cursor.fetchone():
                    logger.warning(f"任务 {task_id} 不存在于 task_scheduling 表中，跳过执行日志记录")
                    return None
                
                cursor.execute("""
                    INSERT INTO task_execution_logs 
                    (task_id, task_name, task_type, started_at, status, execution_params)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    RETURNING id
                """, (
                    task_id, task_name, task_type, started_at, 
                    'running', json.dumps(execution_params)
                ))
                
                execution_log_id = cursor.fetchone()[0]
                conn.commit()
                logger.info(f"任务执行开始记录已保存: {execution_log_id}")
                return execution_log_id
                
            finally:
                conn.close()
                
        except Exception as e:
            logger.error(f"记录任务执行开始失败: {str(e)}")
            return None
    
    async def _log_task_execution_finish(self, execution_log_id: Optional[int], finished_at: datetime,
                                       duration_ms: int, status: str, result_message: str = None,
                                       result_data: Any = None, error_message: str = None,
                                       error_traceback: str = None):
        """记录任务执行完成"""
        if not execution_log_id:
            return
            
        try:
            conn = self.db_manager.get_connection()
            if not conn:
                logger.error("无法连接到数据库更新执行日志")
                return
            
            cursor = conn.cursor()
            try:
                cursor.execute("""
                    UPDATE task_execution_logs SET
                        finished_at = %s,
                        duration_ms = %s,
                        status = %s,
                        result_message = %s,
                        result_data = %s,
                        error_message = %s,
                        error_traceback = %s
                    WHERE id = %s
                """, (
                    finished_at, duration_ms, status, result_message,
                    json.dumps(result_data) if result_data else None,
                    error_message, error_traceback, execution_log_id
                ))
                
                conn.commit()
                logger.info(f"任务执行完成记录已更新: {execution_log_id}, 状态: {status}")
                
            finally:
                conn.close()
                
        except Exception as e:
            logger.error(f"记录任务执行完成失败: {str(e)}")

    async def get_task_execution_logs(self, task_id: str = None, limit: int = 50, 
                                    offset: int = 0, task_types: List[str] = None) -> List[Dict[str, Any]]:
        """获取任务执行日志
        
        Args:
            task_id: 任务ID（可选）
            limit: 返回记录数限制
            offset: 偏移量
            task_types: 任务类型列表（可选），用于过滤特定类型的任务
        """
        try:
            conn = self.db_manager.get_connection()
            if not conn:
                logger.error("无法连接到数据库获取执行日志")
                return []
            
            cursor = conn.cursor()
            try:
                if task_id:
                    cursor.execute("""
                        SELECT id, task_id, task_name, task_type, started_at, finished_at,
                               duration_ms, status, result_message, result_data, 
                               error_message, error_traceback, execution_params
                        FROM task_execution_logs 
                        WHERE task_id = %s
                        ORDER BY started_at DESC 
                        LIMIT %s OFFSET %s
                    """, (task_id, limit, offset))
                elif task_types and len(task_types) > 0:
                    # 按 task_type 列表过滤
                    placeholders = ','.join(['%s'] * len(task_types))
                    query = f"""
                        SELECT id, task_id, task_name, task_type, started_at, finished_at,
                               duration_ms, status, result_message, result_data, 
                               error_message, error_traceback, execution_params
                        FROM task_execution_logs 
                        WHERE task_type IN ({placeholders})
                        ORDER BY started_at DESC 
                        LIMIT %s OFFSET %s
                    """
                    cursor.execute(query, (*task_types, limit, offset))
                else:
                    cursor.execute("""
                        SELECT id, task_id, task_name, task_type, started_at, finished_at,
                               duration_ms, status, result_message, result_data, 
                               error_message, error_traceback, execution_params
                        FROM task_execution_logs 
                        ORDER BY started_at DESC 
                        LIMIT %s OFFSET %s
                    """, (limit, offset))
                
                rows = cursor.fetchall()
                
                logs = []
                for row in rows:
                    log = {
                        'id': str(row[0]),
                        'task_id': row[1],
                        'task_name': row[2],
                        'task_type': row[3],
                        'started_at': row[4].isoformat() if row[4] else None,
                        'finished_at': row[5].isoformat() if row[5] else None,
                        'duration_ms': row[6],
                        'status': row[7],
                        'result_message': row[8],
                        'result_data': row[9],
                        'error_message': row[10],
                        'error_traceback': row[11],
                        'execution_params': row[12]
                    }
                    logs.append(log)
                
                return logs
                
            finally:
                conn.close()
                
        except Exception as e:
            logger.error(f"获取任务执行日志失败: {str(e)}")
            return []

    async def get_execution_summary(self, days: int = 7, tz_name: str = "America/New_York") -> Dict[str, Any]:
        """根据时间范围获取任务执行统计数据"""
        normalized_days = max(1, min(days, 90))  # 限制最大查询范围，避免一次性返回过多数据

        try:
            target_tz = pytz.timezone(tz_name)
        except Exception:
            logger.warning(f"提供的时区 {tz_name} 无效，回退为 UTC")
            target_tz = pytz.UTC
            tz_name = "UTC"

        end_dt_local = datetime.now(target_tz)
        start_dt_local = (end_dt_local - timedelta(days=normalized_days - 1)).replace(hour=0, minute=0, second=0, microsecond=0)

        # 转换为UTC时间用于数据库查询
        end_dt_utc = end_dt_local.astimezone(timezone.utc)
        start_dt_utc = start_dt_local.astimezone(timezone.utc)

        logger.info(
            "开始统计任务执行数据",
            days=normalized_days,
            timezone=tz_name,
            start=start_dt_utc.isoformat(),
            end=end_dt_utc.isoformat()
        )

        try:
            conn = self.db_manager.get_connection()
            if not conn:
                logger.error("无法连接到数据库获取执行统计")
                return {}

            cursor = conn.cursor()
            try:
                # 获取任务配置（确保无执行日志的任务也会包含在统计中）
                cursor.execute("""
                    SELECT type, name
                    FROM task_scheduling
                    ORDER BY type
                """)
                task_config_rows = cursor.fetchall()
                task_configs = {}
                for task_type, task_name in task_config_rows:
                    if task_type not in task_configs:
                        task_configs[task_type] = task_name

                date_list = [
                    (start_dt_local + timedelta(days=i)).date()
                    for i in range(normalized_days)
                ]

                query = """
                    SELECT 
                        task_type,
                        task_name,
                        DATE(timezone(%s, started_at)) AS local_date,
                        COUNT(*) AS total_runs,
                        SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) AS success_count,
                        SUM(CASE WHEN status IN ('failed', 'error') THEN 1 ELSE 0 END) AS failure_count,
                        SUM(CASE WHEN status = 'running' THEN 1 ELSE 0 END) AS running_count,
                        SUM(CASE WHEN status = 'timeout' THEN 1 ELSE 0 END) AS timeout_count,
                        SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled_count,
                        AVG(duration_ms) AS avg_duration_ms
                    FROM task_execution_logs
                    WHERE started_at BETWEEN %s AND %s
                    GROUP BY task_type, task_name, local_date
                    ORDER BY local_date ASC, task_type ASC
                """

                cursor.execute(query, (tz_name, start_dt_utc, end_dt_utc))
                rows = cursor.fetchall()

                summary: Dict[str, Any] = {}
                totals: Dict[str, Dict[str, Any]] = {}

                for row in rows:
                    task_type, task_name, local_date, total_runs, success_count, failure_count, running_count, timeout_count, cancelled_count, avg_duration = row
                    date_key = local_date.isoformat()
                    task_key = task_type

                    if task_key not in summary:
                        summary[task_key] = {
                            "task_type": task_type,
                            "task_name": task_name,
                            "daily_stats": [],
                        }
                        totals[task_key] = {
                            "total_runs": 0,
                            "success_count": 0,
                            "failure_count": 0,
                            "running_count": 0,
                            "timeout_count": 0,
                            "cancelled_count": 0,
                        }

                    summary[task_key]["daily_stats"].append({
                        "date": date_key,
                        "total_runs": int(total_runs),
                        "success_count": int(success_count or 0),
                        "failure_count": int(failure_count or 0),
                        "running_count": int(running_count or 0),
                        "timeout_count": int(timeout_count or 0),
                        "cancelled_count": int(cancelled_count or 0),
                        "avg_duration_ms": float(avg_duration) if avg_duration is not None else None,
                        "success_rate": float(success_count) / total_runs if total_runs else None
                    })

                    totals_info = totals[task_key]
                    totals_info["total_runs"] += int(total_runs)
                    totals_info["success_count"] += int(success_count or 0)
                    totals_info["failure_count"] += int(failure_count or 0)
                    totals_info["running_count"] += int(running_count or 0)
                    totals_info["timeout_count"] += int(timeout_count or 0)
                    totals_info["cancelled_count"] += int(cancelled_count or 0)

                # 确保所有任务都包含在结果中，即使查询范围内没有执行记录
                for task_type, task_name in task_configs.items():
                    if task_type not in summary:
                        summary[task_type] = {
                            "task_type": task_type,
                            "task_name": task_name,
                            "daily_stats": [],
                        }
                        totals[task_type] = {
                            "total_runs": 0,
                            "success_count": 0,
                            "failure_count": 0,
                            "running_count": 0,
                            "timeout_count": 0,
                            "cancelled_count": 0,
                        }
                    elif not summary[task_type].get("task_name"):
                        summary[task_type]["task_name"] = task_name

                    existing_stats = {stat["date"]: stat for stat in summary[task_type]["daily_stats"]}
                    for date_value in date_list:
                        date_key = date_value.isoformat()
                        if date_key not in existing_stats:
                            empty_stat = {
                                "date": date_key,
                                "total_runs": 0,
                                "success_count": 0,
                                "failure_count": 0,
                                "running_count": 0,
                                "timeout_count": 0,
                                "cancelled_count": 0,
                                "avg_duration_ms": None,
                                "success_rate": None
                            }
                            summary[task_type]["daily_stats"].append(empty_stat)
                    summary[task_type]["daily_stats"].sort(key=lambda item: item["date"])

                # 将 totals 合并到 summary 中
                for task_key, totals_info in totals.items():
                    total_runs = totals_info["total_runs"]
                    success_count = totals_info["success_count"]
                    summary[task_key]["totals"] = {
                        **totals_info,
                        "success_rate": (success_count / total_runs) if total_runs else None
                    }

                return {
                    "timezone": tz_name,
                    "days": normalized_days,
                    "start": start_dt_local.date().isoformat(),
                    "end": end_dt_local.date().isoformat(),
                    "tasks": list(summary.values())
                }
            finally:
                conn.close()

        except Exception as e:
            logger.error(f"获取执行统计数据失败: {str(e)}")
            return {}

    async def clear_execution_logs(self) -> bool:
        """清空所有任务执行日志"""
        try:
            conn = self.db_manager.get_connection()
            if not conn:
                logger.error("无法连接到数据库清空执行日志")
                return False
            
            cursor = conn.cursor()
            try:
                # 执行清空操作
                cursor.execute("DELETE FROM task_execution_logs")
                conn.commit()
                
                deleted_count = cursor.rowcount
                logger.info(f"成功清空 {deleted_count} 条执行日志记录")
                
                return True
                
            finally:
                conn.close()
                
        except Exception as e:
            logger.error(f"清空任务执行日志失败: {str(e)}")
            return False

    # 数据库操作方法
    async def _save_task_config(self, task_config: Dict[str, Any]) -> bool:
        """保存任务配置到数据库 - 支持基于任务类型的唯一性约束"""
        try:
            # 首先检查是否已存在相同类型的任务
            existing_task = await self._get_task_by_type(task_config['type'])
            
            if existing_task:
                # 如果存在相同类型的任务，更新现有记录
                logger.info(f"发现已存在的{task_config['type']}任务，将更新现有配置")
                
                # 保留原有的统计信息
                task_config['success_count'] = existing_task.get('success_count', 0)
                task_config['fail_count'] = existing_task.get('fail_count', 0)
                task_config['created_at'] = existing_task.get('created_at', task_config['created_at'])
                
                # 保持原有的任务ID不变，只更新其他字段
                original_task_id = existing_task['id']
                task_config['id'] = original_task_id  # 使用原有ID
                
                query = """
                    UPDATE task_scheduling SET
                        name = %s,
                        schedule_type = %s,
                        cron_expression = %s,
                        interval_minutes = %s,
                        timezone = %s,
                        enabled = %s,
                        params = %s,
                        last_run = %s,
                        next_run = %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                """
                
                params = [
                    task_config['name'],
                    task_config['schedule_type'],
                    task_config.get('cron_expression'),
                    task_config.get('interval_minutes'),
                    task_config['timezone'],
                    task_config['enabled'],
                    json.dumps(task_config['params']),
                    task_config.get('last_run'),
                    task_config.get('next_run'),
                    original_task_id  # 使用原有ID作为WHERE条件
                ]
            else:
                # 如果不存在相同类型的任务，插入新记录
                logger.info(f"创建新的{task_config['type']}任务配置")
                
                query = """
                    INSERT INTO task_scheduling (
                        id, name, type, schedule_type, cron_expression, interval_minutes,
                        timezone, enabled, params, created_at, last_run, next_run,
                        success_count, fail_count, created_by
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                """
                
                params = [
                    task_config['id'],
                    task_config['name'],
                    task_config['type'],
                    task_config['schedule_type'],
                    task_config.get('cron_expression'),
                    task_config.get('interval_minutes'),
                    task_config['timezone'],
                    task_config['enabled'],
                    json.dumps(task_config['params']),
                    task_config['created_at'],
                    task_config.get('last_run'),
                    task_config.get('next_run'),
                    task_config['success_count'],
                    task_config['fail_count'],
                    task_config.get('created_by', 'system')  # 添加created_by字段
                ]
            
            return self.db_manager.execute_query(query, params)
            
        except Exception as e:
            logger.error(f"保存任务配置失败: {str(e)}")
            return False
    
    async def _get_task_by_type(self, task_type: str) -> Optional[Dict[str, Any]]:
        """根据任务类型获取现有任务配置"""
        try:
            query = "SELECT * FROM task_scheduling WHERE type = %s"
            result = self.db_manager.fetch_one(query, [task_type])
            
            if result:
                # 将查询结果转换为字典
                columns = [
                    'id', 'name', 'type', 'schedule_type', 'cron_expression', 
                    'interval_minutes', 'timezone', 'enabled', 'params', 
                    'created_at', 'last_run', 'next_run', 'success_count', 
                    'fail_count', 'created_by', 'updated_at'
                ]
                
                task_dict = {}
                for i, column in enumerate(columns):
                    if i < len(result):
                        value = result[i]
                        # 处理特殊类型
                        if column == 'params':
                            if isinstance(value, str) and value:
                                task_dict[column] = json.loads(value)
                            elif isinstance(value, dict):
                                task_dict[column] = value
                            else:
                                task_dict[column] = {}
                        elif hasattr(value, 'isoformat'):  # datetime对象
                            task_dict[column] = value.isoformat()
                        else:
                            task_dict[column] = value
                    else:
                        task_dict[column] = None
                
                return task_dict
            
            return None
            
        except Exception as e:
            logger.error(f"根据类型获取任务配置失败: {str(e)}")
            return None

    async def _get_task_config(self, task_id: str) -> Optional[Dict[str, Any]]:
        """获取任务配置"""
        try:
            query = "SELECT * FROM task_scheduling WHERE id = %s"
            result = self.db_manager.fetch_one(query, [task_id])
            
            if result:
                return {
                    'id': result[0],
                    'name': result[1],
                    'type': result[2],
                    'schedule_type': result[3],
                    'cron_expression': result[4],
                    'interval_minutes': result[5],
                    'timezone': result[6],
                    'enabled': result[7],
                    'params': json.loads(result[8]) if isinstance(result[8], str) and result[8] else (result[8] if isinstance(result[8], dict) else {}),
                    'created_at': result[9],
                    'last_run': result[10],
                    'next_run': result[11],
                    'success_count': result[12] or 0,
                    'fail_count': result[13] or 0
                }
            return None
            
        except Exception as e:
            logger.error(f"获取任务配置失败: {str(e)}")
            return None
    
    async def _get_all_tasks(self) -> List[Dict[str, Any]]:
        """获取所有任务配置"""
        try:
            query = "SELECT * FROM task_scheduling ORDER BY created_at DESC"
            results = self.db_manager.fetch_all(query)
            
            tasks = []
            for result in results:
                # 处理datetime字段的序列化
                created_at = result[9].isoformat() if result[9] and hasattr(result[9], 'isoformat') else None
                last_run = result[10].isoformat() if result[10] and hasattr(result[10], 'isoformat') else None
                next_run = result[11].isoformat() if result[11] and hasattr(result[11], 'isoformat') else None
                
                tasks.append({
                    'id': result[0],
                    'name': result[1],
                    'type': result[2],
                    'schedule_type': result[3],
                    'cron_expression': result[4],
                    'interval_minutes': result[5],
                    'timezone': result[6],
                    'enabled': result[7],
                    'params': json.loads(result[8]) if isinstance(result[8], str) and result[8] else (result[8] if isinstance(result[8], dict) else {}),
                    'created_at': created_at,
                    'last_run': last_run,
                    'next_run': next_run,
                    'success_count': result[12] or 0,
                    'fail_count': result[13] or 0
                })
            
            return tasks
            
        except Exception as e:
            logger.error(f"获取所有任务配置失败: {str(e)}")
            return []
    
    async def _update_task_config(self, task_id: str, updates: Dict[str, Any]) -> bool:
        """更新任务配置"""
        try:
            # 构建更新语句
            set_clauses = []
            params = []
            
            for key, value in updates.items():
                set_clauses.append(f"{key} = %s")
                if key == 'params':
                    params.append(json.dumps(value))
                else:
                    params.append(value)
            
            params.append(task_id)
            
            query = f"UPDATE task_scheduling SET {', '.join(set_clauses)} WHERE id = %s"
            return self.db_manager.execute_query(query, params)
            
        except Exception as e:
            logger.error(f"更新任务配置失败: {str(e)}")
            return False
    
    async def _delete_task_config(self, task_id: str) -> bool:
        """删除任务配置"""
        try:
            query = "DELETE FROM task_scheduling WHERE id = %s"
            return self.db_manager.execute_query(query, [task_id])
            
        except Exception as e:
            logger.error(f"删除任务配置失败: {str(e)}")
            return False
    
    async def _increment_success_count(self, task_id: str):
        """增加成功计数"""
        try:
            query = "UPDATE task_scheduling SET success_count = success_count + 1 WHERE id = %s"
            self.db_manager.execute_query(query, [task_id])
        except Exception as e:
            logger.error(f"更新成功计数失败: {str(e)}")
    
    async def _increment_fail_count(self, task_id: str):
        """增加失败计数"""
        try:
            query = "UPDATE task_scheduling SET fail_count = fail_count + 1 WHERE id = %s"
            self.db_manager.execute_query(query, [task_id])
        except Exception as e:
            logger.error(f"更新失败计数失败: {str(e)}")


# 全局调度器实例
scheduler_service = None

async def get_scheduler_service():
    """获取调度器服务单例"""
    global scheduler_service
    if scheduler_service is None:
        scheduler_service = SchedulerService()
        await scheduler_service.start()
    return scheduler_service

async def shutdown_scheduler_service():
    """关闭调度器服务"""
    global scheduler_service
    if scheduler_service is not None:
        await scheduler_service.stop()
        scheduler_service = None
