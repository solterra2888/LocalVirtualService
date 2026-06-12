# -*- coding: utf-8 -*-
"""
定时任务调度器服务 - 基于APScheduler实现Cron表达式的定期数据获取策略
"""

import os
import json
import pytz
import asyncio
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any, Callable
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR
from backend.database.db_manager import DatabaseManager
from backend.utils.datetime_utils import utcnow
from backend.utils.logger import get_logger
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
        
        # 注册任务类型和执行函数
        self.task_handlers = {
            'twitter_subscription': self._execute_twitter_subscription_task,
            'youtube_subscription': self._execute_youtube_subscription_task,
            'economic_events': self._execute_economic_events_task,
            'macro_indicators': self._execute_macro_indicators_task,
            'eodhd_fundamentals': self._execute_fundamentals_task,
            'watchlist_fundamentals': self._execute_watchlist_fundamentals_task,
            'finviz_screener': self._execute_finviz_screener_task,
            'eodhd_stock_prices': self._execute_eodhd_stock_prices_task,
            'eodhd_eod_stock_prices': self._execute_eodhd_eod_stock_prices_task,
            'market_data': self._execute_market_data_task,
            'earnings_calendar': self._execute_earnings_calendar_task,
            'ipo_calendar': self._execute_ipo_calendar_task,
            'news_feed': self._execute_news_feed_task,  # EODHD + CCTV Feed 订阅任务
            'zhishixingqiu_subscription': self._execute_zhishixingqiu_subscription_task,
            'twitter_author_summary': self._execute_twitter_author_summary_task,
            'youtube_author_summary': self._execute_youtube_author_summary_task,
            'theme_report': self._execute_theme_report_task,
            'market_weekly_report': self._execute_market_weekly_report_task,
            'daily_market_report': self._execute_daily_market_report_task,
            'the_close_summary': self._execute_the_close_summary_task,
        }
        
        # 添加事件监听器
        self.scheduler.add_listener(self._job_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
        
        logger.info("定时任务调度器服务初始化完成")
    
    async def start(self):
        """启动调度器 (幂等)

        防御性: 若 lifespan 因某种原因被调多次 (如 uvicorn --reload 子进程异常重启),
        重复调用 start() 时直接返回, 避免再次 _cleanup + _load_saved_tasks 把同一批任务
        二次注册到调度器 (虽然 add_job 用了 replace_existing=True, 仍以幂等保护更稳).
        """
        try:
            if self.scheduler.running:
                logger.warning("调度器已在运行, 跳过重复 start() 调用")
                return

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
            
            # 获取数据库中的所有任务（降序，同类型取最新）
            db_tasks = await self._get_all_tasks()

            # 与 _load_saved_tasks 保持一致：按 task_type 去重，同类型只保留最新
            seen_sync_types: set = set()
            deduped_db_tasks: list = []
            for t in db_tasks:
                if t['type'] in seen_sync_types:
                    logger.warning(
                        f"sync_task_states: 发现重复任务类型 {t['type']}，跳过旧记录 {t['id']}"
                    )
                    continue
                seen_sync_types.add(t['type'])
                deduped_db_tasks.append(t)
            
            # 获取调度器中的所有任务ID
            scheduler_job_ids = {job.id for job in self.scheduler.get_jobs() if not job.id.startswith('_')}
            
            # 统计信息
            stats = {
                'total_db_tasks': len(deduped_db_tasks),
                'total_scheduler_jobs': len(scheduler_job_ids),
                'removed_jobs': [],
                'added_jobs': [],
                'skipped': []
            }
            
            # 遍历数据库任务，同步状态
            for task in deduped_db_tasks:
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
            db_task_ids = {task['id'] for task in deduped_db_tasks}
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
    
    def _compute_period_lock_key(
        self,
        task_type: str,
        task_id: str,
        started_at: datetime,
    ) -> tuple:
        """根据 task 的 trigger 计算「周期窗口」分布式锁的 (lock_key, ttl_seconds).

        - cron 触发: lock_key = scheduler_lock:{type}:cron:{当前 fire_time YYYYMMDDHHMM}
        - interval 触发: lock_key = scheduler_lock:{type}:int:{floor(epoch / 周期秒)}
        - 拿不到 trigger 时退化为旧的「逐次锁」(scheduler_lock:{type}, TTL=600s),
          保证向后兼容; 但此时仍有可能漏掉重复触发, 应在调度层修复.
        TTL 取「周期长度 * 0.9」, 保证当前窗口结束前锁不释放, 同时不阻塞下一次正常 fire.
        """
        fallback_key = f"scheduler_lock:{task_type}"
        fallback_ttl = 600  # 与历史行为一致

        try:
            job = self.scheduler.get_job(task_id)
            trigger = getattr(job, 'trigger', None) if job else None
            if trigger is None:
                return fallback_key, fallback_ttl

            if isinstance(trigger, IntervalTrigger):
                period = int(trigger.interval.total_seconds())
                if period <= 0:
                    return fallback_key, fallback_ttl
                bucket = int(started_at.timestamp()) // period
                ttl = max(int(period * 0.9), 60)
                return f"scheduler_lock:{task_type}:int:{bucket}", ttl

            if isinstance(trigger, CronTrigger):
                # APScheduler trigger.get_next_fire_time(previous_fire_time, now)
                # 返回「>= now 的下一次触发」. 我们用 (started_at - 60s) 作为 now：
                # - 60s 足够覆盖 APScheduler 调度延迟（任务极少晚于 fire_time 超过数秒）
                # - 对于最频繁的 cron（1 分钟周期）, 60s 回溯恰好不跨越上一个窗口
                #   （probe = fire_time - ~58s，get_next_fire_time 仍返回 fire_time）
                # - 旧的 1s 回溯在任务仅晚于 fire_time 1~2s 启动时就会算出下一个窗口,
                #   导致两个重复 job 的 lock_key 不同, Redis 锁无法拦截第二次执行.
                probe = started_at - timedelta(seconds=60)
                current_fire = trigger.get_next_fire_time(None, probe)
                if current_fire is None:
                    return fallback_key, fallback_ttl
                # 估算周期长度: 下下次 fire - 当前 fire
                next_fire = trigger.get_next_fire_time(
                    current_fire, current_fire + timedelta(seconds=1)
                )
                if next_fire is not None:
                    period = int((next_fire - current_fire).total_seconds())
                else:
                    period = 3600  # 拿不到下一次时保守用 1 小时
                ttl = max(int(period * 0.9), 60)
                window_key = current_fire.strftime("%Y%m%d%H%M")
                return f"scheduler_lock:{task_type}:cron:{window_key}", ttl
        except Exception as e:
            logger.warning(
                f"计算周期锁键失败, 回退为逐次锁: {e}",
                task_id=task_id,
                task_type=task_type,
            )
        return fallback_key, fallback_ttl

    def _compute_period_key(
        self,
        task_type: str,
        task_id: str,
        started_at: datetime,
    ) -> Optional[str]:
        """返回当前执行周期的唯一标识字符串，用于 PostgreSQL 分布式锁。

        与 _compute_period_lock_key 逻辑相同，但只返回 period_key 字符串
        （不含 'scheduler_lock:' 前缀），供 DB INSERT ON CONFLICT 使用。

        - cron 任务: 'cron:YYYYMMDDHHMM'（当前 fire_time）
        - interval 任务: 'int:{epoch // period_seconds}'
        - 无法计算时返回 None（退化为无锁保护，不影响正常执行）
        """
        try:
            job = self.scheduler.get_job(task_id)
            trigger = getattr(job, 'trigger', None) if job else None
            if trigger is None:
                return None

            if isinstance(trigger, IntervalTrigger):
                period = int(trigger.interval.total_seconds())
                if period <= 0:
                    return None
                bucket = int(started_at.timestamp()) // period
                return f"int:{bucket}"

            if isinstance(trigger, CronTrigger):
                probe = started_at - timedelta(seconds=60)
                current_fire = trigger.get_next_fire_time(None, probe)
                if current_fire is None:
                    return None
                return f"cron:{current_fire.strftime('%Y%m%d%H%M')}"

        except Exception as e:
            logger.warning(
                f"计算周期键失败: {e}",
                task_id=task_id,
                task_type=task_type,
            )
        return None

    async def _execute_task(self, task_id: str, task_type: str, task_params: Dict[str, Any]):
        """执行任务（带「按周期窗口」分布式锁保护）

        语义说明 (重要):
        旧实现用各机器本地 Redis SET NX 做周期锁，无法跨机器协调：
          - 本地 Mac Mini 和远程服务器各自在自己的 Redis 上成功拿到「周期窗口锁」
          - 两台机器同一 cron 周期都执行任务，产生两条独立的 Celery 任务

        新实现改用 PostgreSQL INSERT ON CONFLICT DO NOTHING 作为真正的分布式锁：
          - period_key = cron fire_time 的 'cron:YYYYMMDDHHMM' / interval bucket
          - task_execution_logs 表上有唯一索引 (task_type, period_key) WHERE period_key IS NOT NULL
          - 第一个执行 INSERT 的调度实例获得锁（RETURNING id 返回行）
          - 第二个 INSERT 触发唯一冲突，ON CONFLICT DO NOTHING 返回 0 行 → 跳过
          - 由于所有调度实例共享同一个 PostgreSQL 数据库，该锁天然跨机器生效
        """
        execution_log_id = None
        started_at = datetime.now(tz=timezone.utc)

        # 计算周期键（用于 DB 分布式锁）
        period_key = self._compute_period_key(task_type, task_id, started_at)

        try:
            # 获取任务配置以获取任务名称（在 DB 锁之前，以便锁记录包含正确名称）
            task_config = await self._get_task_config(task_id)
            task_name = task_config.get('name', f'Task-{task_id}') if task_config else f'Task-{task_id}'

            # 🔒 通过 INSERT ON CONFLICT DO NOTHING 尝试获取 DB 周期窗口锁
            # period_key 非 None 时才启用锁；None 时跳过冲突检测（退化为无保护）
            execution_log_id = await self._log_task_execution_start(
                task_id, task_name, task_type, started_at, task_params,
                period_key=period_key
            )

            if execution_log_id is None:
                # DB 唯一约束冲突：另一个调度实例已在此周期执行
                logger.warning(
                    f"⏭️  任务 {task_type} 在当前周期窗口已被其他实例执行, 跳过本次触发",
                    task_id=task_id,
                    task_type=task_type,
                    period_key=period_key,
                )

                # 插入 skipped 日志（period_key=None 不触发约束，始终插入）
                skipped_log_id = await self._log_task_execution_start(
                    task_id, task_name, task_type, started_at, task_params,
                    period_key=None
                )
                if skipped_log_id:
                    finished_at = datetime.now(tz=timezone.utc)
                    duration_ms = int((finished_at - started_at).total_seconds() * 1000)
                    await self._log_task_execution_finish(
                        skipped_log_id,
                        finished_at,
                        duration_ms,
                        'skipped',
                        error_message=f'当前周期窗口已被其他实例执行 (period_key={period_key})'
                    )

                return {
                    'success': False,
                    'message': '当前周期窗口已被其他实例执行, 已跳过',
                    'skipped': True
                }

            logger.info(
                f"🔓 获取 DB 周期窗口锁成功: {task_id}, 类型: {task_type}",
                period_key=period_key,
            )

            # 更新最后运行时间, 顺手把 APScheduler 算好的下次触发时间写回 task_scheduling.next_run
            update_payload: Dict[str, Any] = {'last_run': started_at.isoformat()}
            try:
                _job = self.scheduler.get_job(task_id)
                if _job is not None and getattr(_job, 'next_run_time', None):
                    update_payload['next_run'] = _job.next_run_time.isoformat()
            except Exception as _e:
                logger.warning(f"读取 next_run_time 失败, 仅更新 last_run: {_e}", task_id=task_id)
            await self._update_task_config(task_id, update_payload)

            # 执行任务
            handler = self.task_handlers.get(task_type)
            if not handler:
                error_msg = f"未找到任务类型 {task_type} 的处理器。支持的任务类型: {', '.join(self.task_handlers.keys())}"
                logger.error(error_msg)

                finished_at = datetime.now(tz=timezone.utc)
                duration_ms = int((finished_at - started_at).total_seconds() * 1000)
                await self._log_task_execution_finish(
                    execution_log_id,
                    finished_at,
                    duration_ms,
                    'failed',
                    error_message=error_msg
                )

                return {
                    'success': False,
                    'message': error_msg,
                    'data': None
                }

            merged_params = dict(task_params or {})
            if task_config and task_config.get('timezone'):
                merged_params['scheduler_timezone'] = task_config['timezone']
            result = await handler(merged_params)

            # 计算执行时间
            finished_at = datetime.now(tz=timezone.utc)
            duration_ms = int((finished_at - started_at).total_seconds() * 1000)

            if result.get('success', False):
                await self._log_task_execution_finish(
                    execution_log_id, finished_at, duration_ms, 'success',
                    result.get('message', '任务执行成功'), result.get('data')
                )
                await self._increment_success_count(task_id)
                logger.info(f"任务执行成功: {task_id}, 耗时: {duration_ms}ms")
            else:
                await self._log_task_execution_finish(
                    execution_log_id, finished_at, duration_ms, 'failed',
                    error_message=result.get('message', '任务执行失败')
                )
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
            # DB 周期窗口锁通过唯一索引行持久存在，无需显式释放。
            # 下一 cron tick 会使用新的 period_key，自然进入新窗口。
            pass
    
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
    
    async def _execute_zhishixingqiu_subscription_task(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        执行知识星球订阅话题获取任务 — 通过 Celery 异步分发给 data-worker。

        知识星球 backfill 可能需要翻多页，分发到 data_fetching 队列（time_limit=3600s）。

        Args:
            params: 任务参数
                - subscription_id: 指定订阅ID（可选）
                - user_id: 指定用户ID（可选）
                - force_fetch: 是否强制获取（可选）

        Returns:
            执行结果（立即返回 Celery task_id，实际执行在 data-worker 中）
        """
        try:
            logger.info("分发知识星球订阅话题获取 Celery 任务", params=params)

            from backend.tasks.subscription_tasks import fetch_zhishixingqiu_subscription_task

            subscription_id = params.get('subscription_id')
            user_id = params.get('user_id')
            force_fetch = params.get('force_fetch', False)

            task = fetch_zhishixingqiu_subscription_task.delay(
                subscription_id=subscription_id,
                user_id=user_id,
                force_fetch=force_fetch,
            )

            message = (
                f"✅ 知识星球订阅话题获取任务已提交 Celery 异步执行，"
                f"task_id={task.id}"
            )
            logger.info("知识星球订阅话题获取任务已提交", celery_task_id=task.id)

            return {'success': True, 'message': message, 'celery_task_id': task.id}

        except Exception as e:
            error_msg = f"知识星球订阅任务分发异常: {str(e)}"
            logger.error(error_msg, exception=str(e))

            return {
                'success': False,
                'message': f"❌ {error_msg}",
                'error': str(e)
            }

    async def _execute_youtube_subscription_task(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        执行YouTube订阅视频获取任务 —— 分发到 Celery youtube_fetching 队列。

        任务由家用电脑的远程 Worker 执行（住宅 IP），
        避免云服务器 IP 被 YouTube 封锁导致 transcript 获取失败。

        Args:
            params: 任务参数
                - subscription_id: 指定订阅ID（可选）
                - user_id: 指定用户ID（可选）
                - force_fetch: 是否强制获取（可选）

        Returns:
            执行结果（含 Celery task ID）
        """
        try:
            logger.info("分发 YouTube 订阅视频获取 Celery 任务", params=params)

            from backend.tasks.subscription_tasks import (
                fetch_youtube_subscription_task,
                fetch_all_youtube_subscriptions_task,
            )

            subscription_id = params.get('subscription_id')
            user_id = params.get('user_id')
            force_fetch = params.get('force_fetch', False)

            if subscription_id or user_id:
                task = fetch_youtube_subscription_task.delay(
                    subscription_id, user_id, force_fetch
                )
                detail = f"subscription_id={subscription_id}, user_id={user_id}"
            else:
                task = fetch_all_youtube_subscriptions_task.delay()
                detail = "全量频道"

            message = (
                f"✅ YouTube 订阅获取 ({detail}): "
                f"已提交 Celery 异步任务 {task.id}"
            )
            logger.info(message)
            return {'success': True, 'message': message, 'celery_task_id': task.id}

        except Exception as e:
            error_msg = f"YouTube 订阅任务分发异常: {str(e)}"
            logger.error(error_msg, exception=str(e))
            return {'success': False, 'message': f"❌ {error_msg}", 'error': str(e)}
    
    async def _execute_news_feed_task(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        执行 News Feed 订阅任务 —— 包含 EODHD 新闻源和 CCTV 新闻联播。

        Args:
            params: 任务参数
                - max_results:      EODHD 每次拉取最大条数（默认 200）
                - recent_days:      EODHD 拉取最近 N 天的新闻（默认 1）
                - cctv_max:         CCTV 每次最多爬取文章数（默认 20）
                - cctv_recent_days: CCTV 抓取最近几天的数据（默认 2）
                - skip_cctv:        跳过 CCTV 抓取（默认 False）
        """
        import asyncio
        import concurrent.futures

        try:
            logger.info("执行 News Feed 订阅任务（EODHD + CCTV）", params=params)

            max_results = params.get('max_results', 200)
            recent_days = params.get('recent_days', 1)
            cctv_max = params.get('cctv_max', 20)
            cctv_recent_days = params.get('cctv_recent_days', 2)
            skip_cctv = params.get('skip_cctv', False)

            from backend.services.eodhd_news_feed_service import EODHDNewsFeedService

            loop = asyncio.get_event_loop()

            # -- EODHD --
            with concurrent.futures.ThreadPoolExecutor() as executor:
                eodhd_result = await loop.run_in_executor(
                    executor,
                    lambda: EODHDNewsFeedService().fetch_and_distribute_news(
                        max_results=max_results,
                        recent_days=recent_days,
                    )
                )

            # -- CCTV --
            cctv_result: Dict[str, Any] = {"success": True, "articles_fetched": 0, "articles_new": 0, "message": "skipped"}
            if not skip_cctv:
                try:
                    from backend.services.cctv_news_feed_service import CCTVNewsFeedService
                    with concurrent.futures.ThreadPoolExecutor() as executor:
                        cctv_result = await loop.run_in_executor(
                            executor,
                            lambda: CCTVNewsFeedService().fetch_and_distribute(
                                max_articles=cctv_max,
                                recent_days=cctv_recent_days,
                            ),
                        )
                except Exception as cctv_exc:
                    logger.error(f"CCTV Feed 执行异常: {cctv_exc}")
                    cctv_result = {"success": False, "message": str(cctv_exc), "articles_fetched": 0, "articles_new": 0}

            # -- 汇总 --
            eodhd_ok = eodhd_result.get('success', False)
            message = (
                f"✅ EODHD: 获取 {eodhd_result.get('articles_fetched', 0)} 篇，"
                f"新增 {eodhd_result.get('articles_new', 0)} 篇，"
                f"匹配 {eodhd_result.get('sources_matched', 0)} 个源 | "
                f"CCTV: 爬取 {cctv_result.get('articles_fetched', 0)} 篇，"
                f"新增 {cctv_result.get('articles_new', 0)} 篇"
            )
            return {
                'success': eodhd_ok,
                'message': message,
                'data': {'eodhd': eodhd_result, 'cctv': cctv_result},
            }

        except Exception as e:
            error_msg = f"News Feed 任务执行异常: {str(e)}"
            logger.error(error_msg, exception=str(e))
            return {'success': False, 'message': f"❌ {error_msg}", 'error': str(e)}

    async def _execute_twitter_author_summary_task(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        执行 Twitter 每日报告定时任务（Phase A → chain → Phase B）。
        Phase A 为所有作者预生成摘要（全局共享），完成后自动链式触发 Phase B 生成用户报告。
        """
        try:
            logger.info("分发 Twitter 每日报告 Celery 任务", params=params)

            from datetime import datetime
            from backend.tasks.report_tasks import generate_all_author_summaries_task
            from backend.utils.datetime_utils import (
                utcnow,
                parse_iso_utc,
                window_label_date,
            )

            force = params.get('force', False)
            chain_daily_reports = params.get('chain_daily_reports', True)
            window_hours = int(params.get('window_hours', 24))

            report_date_str = params.get('report_date')
            window_end = parse_iso_utc(params.get('window_end'))
            if window_end is None:
                window_end = utcnow()

            if report_date_str:
                label_date = datetime.strptime(report_date_str, '%Y-%m-%d').date()
            else:
                label_date = window_label_date(window_end, params.get('scheduler_timezone'))

            task = generate_all_author_summaries_task.delay(
                summary_date=str(label_date),
                force=force,
                chain_daily_reports=chain_daily_reports,
                window_end=window_end.isoformat(),
                window_hours=window_hours,
            )

            chain_info = "，完成后自动触发 Daily Report" if chain_daily_reports else ""
            force_info = "（强制重新生成）" if force else ""
            logger.info(
                f"分发 Twitter 每日报告 Celery 任务 | params="
                f"{{'chain_daily_reports': {chain_daily_reports}, 'force': {force}, "
                f"'label_date': '{label_date}', 'window_end': '{window_end.isoformat()}'}}"
            )
            message = (
                f"✅ Twitter 每日报告 ({label_date}, 过去{window_hours}h): "
                f"已提交 Celery 异步任务 {task.id}{chain_info}{force_info}"
            )
            return {'success': True, 'message': message, 'celery_task_id': task.id}

        except Exception as e:
            error_msg = f"Twitter 每日报告任务分发异常: {str(e)}"
            logger.error(error_msg, exception=str(e))
            return {'success': False, 'message': f"❌ {error_msg}", 'error': str(e)}

    async def _execute_youtube_author_summary_task(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        执行 YouTube 每日报告定时任务（Phase A → chain → Phase B）。
        Phase A 为所有频道预生成摘要（全局共享），完成后自动链式触发 Phase B 生成用户报告。
        """
        try:
            logger.info("分发 YouTube 每日报告 Celery 任务", params=params)

            from datetime import datetime
            from backend.tasks.report_tasks import generate_all_yt_author_summaries_task
            from backend.utils.datetime_utils import (
                utcnow,
                parse_iso_utc,
                window_label_date,
            )

            force = params.get('force', False)
            chain_daily_reports = params.get('chain_daily_reports', True)
            window_hours = int(params.get('window_hours', 24))

            report_date_str = params.get('report_date')
            window_end = parse_iso_utc(params.get('window_end'))
            if window_end is None:
                window_end = utcnow()

            if report_date_str:
                label_date = datetime.strptime(report_date_str, '%Y-%m-%d').date()
            else:
                label_date = window_label_date(window_end, params.get('scheduler_timezone'))

            task = generate_all_yt_author_summaries_task.delay(
                summary_date=str(label_date),
                force=force,
                chain_daily_reports=chain_daily_reports,
                window_end=window_end.isoformat(),
                window_hours=window_hours,
            )

            chain_info = "，完成后自动触发 Daily Report" if chain_daily_reports else ""
            force_info = "（强制重新生成）" if force else ""
            logger.info(
                f"分发 YouTube 每日报告 Celery 任务 | params="
                f"{{'chain_daily_reports': {chain_daily_reports}, 'force': {force}, "
                f"'label_date': '{label_date}', 'window_end': '{window_end.isoformat()}'}}"
            )
            message = (
                f"✅ YouTube Daily Report ({label_date}, 过去{window_hours}h): "
                f"已提交 Celery 异步任务 {task.id}{chain_info}{force_info}"
            )
            return {'success': True, 'message': message, 'celery_task_id': task.id}

        except Exception as e:
            error_msg = f"YouTube 每日报告任务分发异常: {str(e)}"
            logger.error(error_msg, exception=str(e))
            return {'success': False, 'message': f"❌ {error_msg}", 'error': str(e)}

    async def _execute_theme_report_task(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        为所有打了 'Theme Report' 标签的用户生成 Theme Report 静态 HTML。

        Args:
            params:
                - report_date: 报告日期 (YYYY-MM-DD)，默认今天 (UTC)
                - user_id: 可选，仅生成指定用户
        """
        try:
            logger.info("分发 Theme Report Celery 任务", params=params)

            from datetime import datetime
            from backend.tasks.theme_report_tasks import generate_all_theme_reports_task
            from backend.utils.datetime_utils import utc_today

            report_date_str = params.get('report_date')
            if report_date_str:
                report_date = datetime.strptime(report_date_str, '%Y-%m-%d').date()
            else:
                report_date = utc_today()

            user_id = params.get('user_id')

            task = generate_all_theme_reports_task.delay(
                user_id=user_id,
                report_date=str(report_date),
                scheduler_timezone=params.get('scheduler_timezone'),
            )

            user_info = f", user={user_id}" if user_id else ", all tagged users"
            message = (
                f"✅ Theme Report ({report_date}): "
                f"已提交 Celery 异步任务 {task.id}{user_info}"
            )
            return {'success': True, 'message': message, 'celery_task_id': task.id}

        except Exception as e:
            error_msg = f"Theme Report 任务分发异常: {str(e)}"
            logger.error(error_msg, exception=str(e))
            return {'success': False, 'message': f"❌ {error_msg}", 'error': str(e)}

    async def _execute_market_weekly_report_task(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        为所有打了 'Market Weekly' 标签的用户生成 Market Weekly Report 静态 HTML。

        Args:
            params:
                - user_id: 可选，仅生成指定用户
                - week_end: 周结束日期 (YYYY-MM-DD)，默认最新周五
        """
        try:
            logger.info("分发 Market Weekly Report Celery 任务", params=params)

            from backend.tasks.market_weekly_report_tasks import generate_all_market_weekly_reports_task

            user_id = params.get('user_id')
            week_end = params.get('week_end')
            scheduler_timezone = params.get('scheduler_timezone')

            task = generate_all_market_weekly_reports_task.delay(
                user_id=user_id,
                week_end=week_end,
                scheduler_timezone=scheduler_timezone,
            )

            week_info = f", week_end={week_end}" if week_end else ", latest week"
            user_info = f", user={user_id}" if user_id else ", all tagged users"
            message = (
                f"✅ Market Weekly Report: "
                f"已提交 Celery 异步任务 {task.id}{user_info}{week_info}"
            )
            logger.info(message)
            return {'success': True, 'message': message, 'celery_task_id': task.id}

        except Exception as e:
            error_msg = f"Market Weekly Report 任务分发异常: {str(e)}"
            logger.error(error_msg, exception=str(e))
            return {'success': False, 'message': f"❌ {error_msg}", 'error': str(e)}

    async def _execute_daily_market_report_task(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        生成 Daily Market Report（全局，从 finviz_screener 数据）。
        应在 finviz_screener 刷新完成后调度。

        Args:
            params:
                - report_date: 可选，ISO 日期字符串 (YYYY-MM-DD)
        """
        try:
            logger.info("分发 Daily Market Report Celery 任务", params=params)

            from backend.tasks.daily_market_report_tasks import generate_daily_market_report_task

            report_date = params.get('report_date')

            task = generate_daily_market_report_task.delay(report_date=report_date)

            date_info = f", date={report_date}" if report_date else ", today"
            message = (
                f"✅ Daily Market Report: "
                f"已提交 Celery 异步任务 {task.id}{date_info}"
            )
            logger.info(message)
            return {'success': True, 'message': message, 'celery_task_id': task.id}

        except Exception as e:
            error_msg = f"Daily Market Report 任务分发异常: {str(e)}"
            logger.error(error_msg, exception=str(e))
            return {'success': False, 'message': f"❌ {error_msg}", 'error': str(e)}

    async def _execute_the_close_summary_task(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        生成 Bloomberg The Close 每日 PM Brief 摘要（全局）。
        应在 YouTube 转录完成后调度（美东 ~18:00 = 北京 ~06:00+）。

        Args:
            params:
                - report_date: 可选，ISO 日期字符串 (YYYY-MM-DD)
                - window_end: 可选，ISO 时间字符串（UTC），内容窗口结束时间
                - window_hours: 可选，回溯小时数（默认 24）
                - force: 可选，是否强制重新生成（默认 False）
        """
        try:
            logger.info("分发 The Close Summary Celery 任务", params=params)

            from backend.tasks.the_close_summary_tasks import generate_the_close_summary_task

            report_date = params.get('report_date')
            window_end = params.get('window_end')
            window_hours = int(params.get('window_hours', 24))
            force = params.get('force', False)

            task = generate_the_close_summary_task.delay(
                report_date=report_date,
                window_end=window_end,
                window_hours=window_hours,
                force=force,
            )

            date_info = f", date={report_date}" if report_date else ", today"
            force_info = "（强制重新生成）" if force else ""
            message = (
                f"✅ The Close Summary: "
                f"已提交 Celery 异步任务 {task.id}{date_info}{force_info}"
            )
            logger.info(message)
            return {'success': True, 'message': message, 'celery_task_id': task.id}

        except Exception as e:
            error_msg = f"The Close Summary 任务分发异常: {str(e)}"
            logger.error(error_msg, exception=str(e))
            return {'success': False, 'message': f"❌ {error_msg}", 'error': str(e)}

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
                    # 获取下周数据（周一到周日）。用 utcnow 避免 HK 跨日时段日期偏移.
                    today = utcnow()
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
                    today = utcnow()
                    from_date = today.strftime('%Y-%m-%d')
                    to_date = (today + timedelta(days=days)).strftime('%Y-%m-%d')
                else:
                    # 默认获取未来30天的数据
                    today = utcnow()
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
        """加载已保存的任务（按 task_type 去重，同类型只注册最新的一条）"""
        try:
            logger.info("加载已保存的定时任务")
            tasks = await self._get_all_tasks()  # already ORDER BY created_at DESC

            # 同一 task_type 可能因历史操作在 task_scheduling 中存有多行。
            # 若全部注册到 APScheduler，每行对应独立的 cron/interval job，
            # 同日可能触发多次，且各自的 period-lock-key 不同，Redis 锁无法拦截。
            # 解法：按 task_type 去重，只注册 created_at 最新的那条（列表已降序排列）。
            seen_types: set = set()
            deduped: list = []
            for task in tasks:
                t = task['type']
                if t in seen_types:
                    logger.warning(
                        f"检测到重复的任务类型 {t}，跳过旧记录 {task['id']}（仅保留最新）"
                    )
                    continue
                seen_types.add(t)
                deduped.append(task)

            for task in deduped:
                if task['enabled']:
                    await self._add_job_to_scheduler(task)

            logger.info(f"已加载 {len([t for t in deduped if t['enabled']])} 个启用的定时任务")
            
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
                                      started_at: datetime, execution_params: Dict[str, Any],
                                      period_key: Optional[str] = None) -> Optional[int]:
        """记录任务开始执行，同时充当跨机器分布式锁。

        当 period_key 不为 None 时使用 ON CONFLICT DO NOTHING：
          - INSERT 成功（RETURNING id 返回一行）→ 当前实例获得锁，继续执行
          - INSERT 冲突（RETURNING id 返回空）→ 另一实例已在此周期执行，返回 None

        当 period_key 为 None 时（如记录 skipped 日志），不触发冲突约束，始终插入成功。
        """
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
                    (task_id, task_name, task_type, started_at, status, execution_params, period_key)
                    VALUES (%s, %s, %s, %s, 'running', %s, %s)
                    ON CONFLICT (task_type, period_key) WHERE period_key IS NOT NULL DO NOTHING
                    RETURNING id
                """, (
                    task_id, task_name, task_type, started_at,
                    json.dumps(execution_params), period_key
                ))

                row = cursor.fetchone()
                conn.commit()
                if row is None:
                    # 另一个调度实例已为此周期创建了记录（分布式锁被抢占）
                    logger.warning(
                        f"⏭️  DB 周期锁冲突: task_type={task_type}, period_key={period_key}，"
                        f"说明另一个调度实例已执行此周期，跳过"
                    )
                    return None
                execution_log_id = row[0]
                logger.info(f"任务执行开始记录已保存: {execution_log_id}, period_key={period_key}")
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

                # 按 task_type + 本地日历日聚合（不要带 task_name 分组）：
                # 同一 task_type 可对应多条 task_scheduling（不同 id / 不同 name），
                # 若 GROUP BY task_name，同一天会产出多行；Python 侧又用 task_type 作唯一键
                # 追加 daily_stats / 累加 totals，会把「同一逻辑任务类型」在同日重复加总，造成虚高。
                #
                # 注意：status='skipped' 表示同一周期内重复触发被周期窗口锁拦截的占位记录，
                # 不代表真实执行次数，必须从 total_runs / avg_duration_ms 中排除，
                # 但单独保留 skipped_count 供监控排查使用。
                query = """
                    SELECT 
                        task_type,
                        MAX(task_name) AS task_name,
                        DATE(timezone(%s, started_at)) AS local_date,
                        SUM(CASE WHEN status != 'skipped' THEN 1 ELSE 0 END) AS total_runs,
                        SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) AS success_count,
                        SUM(CASE WHEN status IN ('failed', 'error') THEN 1 ELSE 0 END) AS failure_count,
                        SUM(CASE WHEN status = 'running' THEN 1 ELSE 0 END) AS running_count,
                        SUM(CASE WHEN status = 'timeout' THEN 1 ELSE 0 END) AS timeout_count,
                        SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled_count,
                        SUM(CASE WHEN status = 'skipped' THEN 1 ELSE 0 END) AS skipped_count,
                        AVG(CASE WHEN status != 'skipped' AND duration_ms IS NOT NULL THEN duration_ms END) AS avg_duration_ms
                    FROM task_execution_logs
                    WHERE started_at BETWEEN %s AND %s
                    GROUP BY task_type, local_date
                    ORDER BY local_date ASC, task_type ASC
                """

                cursor.execute(query, (tz_name, start_dt_utc, end_dt_utc))
                rows = cursor.fetchall()

                summary: Dict[str, Any] = {}
                totals: Dict[str, Dict[str, Any]] = {}

                for row in rows:
                    task_type, task_name, local_date, total_runs, success_count, failure_count, running_count, timeout_count, cancelled_count, skipped_count, avg_duration = row
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
                            "skipped_count": 0,
                        }

                    summary[task_key]["daily_stats"].append({
                        "date": date_key,
                        "total_runs": int(total_runs),
                        "success_count": int(success_count or 0),
                        "failure_count": int(failure_count or 0),
                        "running_count": int(running_count or 0),
                        "timeout_count": int(timeout_count or 0),
                        "cancelled_count": int(cancelled_count or 0),
                        "skipped_count": int(skipped_count or 0),
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
                    totals_info["skipped_count"] += int(skipped_count or 0)

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
                            "skipped_count": 0,
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
                                "skipped_count": 0,
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


# 全局调度器实例 + 并发安全锁
# 之前的实现只用 None 判断, asyncio 并发场景下理论上可能重复实例化两次 SchedulerService,
# 进而向同一 Redis/DB 重复 add_job. 加 asyncio.Lock 保证整个进程只构造一次.
scheduler_service = None
_scheduler_init_lock = asyncio.Lock()

async def get_scheduler_service():
    """获取调度器服务单例 (并发安全)"""
    global scheduler_service
    if scheduler_service is not None:
        return scheduler_service
    async with _scheduler_init_lock:
        if scheduler_service is None:
            instance = SchedulerService()
            await instance.start()
            scheduler_service = instance
    return scheduler_service

async def shutdown_scheduler_service():
    """关闭调度器服务"""
    global scheduler_service
    async with _scheduler_init_lock:
        if scheduler_service is not None:
            await scheduler_service.stop()
            scheduler_service = None
