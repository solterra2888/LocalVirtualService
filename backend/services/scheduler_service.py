# -*- coding: utf-8 -*-
"""
定时任务调度器服务 - 基于APScheduler实现Cron表达式的定期数据获取策略
"""

import os
import json
import pytz
import asyncio
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Callable
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR
from backend.database.db_manager import DatabaseManager
from backend.utils.logger import get_logger
from backend.services.eodhd_news_service import EODHDNewsService
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
                'coalesce': False,
                'max_instances': 1,
                'misfire_grace_time': 300  # 5分钟容错时间
            }
        )
        
        # 数据库连接配置
        db_config = {
            'db_host': config.get('DB_HOST', 'localhost'),
            'db_port': config.get('DB_PORT', '5432'),
            'db_name': config.get('DB_NAME', 'targettracking'),
            'db_user': config.get('DB_USER', 'postgres'),
            'db_password': config.get('DB_PASSWORD', '')
        }
        self.db_manager = DatabaseManager(db_config)
        
        # 注册任务类型和执行函数
        self.task_handlers = {
            'eodhd_news': self._execute_eodhd_news_task,
            'economic_events': self._execute_economic_events_task,
            'macro_indicators': self._execute_macro_indicators_task,
            'exchange_data': self._execute_exchange_data_task,
            'eodhd_fundamentals': self._execute_fundamentals_task,
            'finviz_screener': self._execute_finviz_screener_task
        }
        
        # 添加事件监听器
        self.scheduler.add_listener(self._job_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
        
        logger.info("定时任务调度器服务初始化完成")
    
    async def start(self):
        """启动调度器"""
        try:
            logger.info("启动定时任务调度器")
            self.scheduler.start()
            
            # 加载已保存的任务
            await self._load_saved_tasks()
            
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
            task_type: 任务类型 (eodhd_news, economic_events, macro_indicators, exchange_data, eodhd_fundamentals, finviz_screener)
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
            # 获取任务配置
            task_config = await self._get_task_config(task_id)
            if not task_config:
                return {'success': False, 'message': '任务不存在'}
            
            # 更新启用状态
            await self._update_task_config(task_id, {'enabled': True})
            
            # 添加到调度器
            await self._add_job_to_scheduler(task_config)
            
            logger.info(f"任务已启用: {task_id}")
            return {'success': True, 'message': '任务已启用'}
            
        except Exception as e:
            logger.error(f"启用任务失败: {str(e)}")
            return {'success': False, 'message': f'启用任务失败: {str(e)}'}
    
    async def disable_task(self, task_id: str) -> Dict[str, Any]:
        """禁用任务"""
        try:
            # 更新启用状态
            await self._update_task_config(task_id, {'enabled': False})
            
            # 从调度器移除
            if self.scheduler.get_job(task_id):
                self.scheduler.remove_job(task_id)
            
            logger.info(f"任务已禁用: {task_id}")
            return {'success': True, 'message': '任务已禁用'}
            
        except Exception as e:
            logger.error(f"禁用任务失败: {str(e)}")
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
                'keywords': None
            },
            created_by=created_by
        )
    
    async def _execute_task(self, task_id: str, task_type: str, task_params: Dict[str, Any]):
        """执行任务"""
        execution_log_id = None
        started_at = datetime.now(tz=timezone.utc)
        
        try:
            logger.info(f"开始执行任务: {task_id}, 类型: {task_type}")
            
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
                raise ValueError(f"未找到任务类型 {task_type} 的处理器")
            
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
    
    async def _execute_eodhd_news_task(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """执行EODHD新闻获取任务"""
        try:
            logger.info("执行EODHD新闻获取任务", params=params)
            
            # 创建EODHD新闻服务实例
            eodhd_service = EODHDNewsService()
            
            # 执行新闻获取
            result = eodhd_service.fetch_and_store_news(
                symbols=params.get('symbols'),
                topic=params.get('topic'),
                start_date=params.get('start_date'),
                end_date=params.get('end_date'),
                max_results=params.get('max_results'),
                topic_filter=params.get('topic_filter'),
                keywords=params.get('keywords')
            )
            
            return {
                'success': True,
                'message': f"成功获取并存储 {result['success_count']} 条新闻",
                'data': result
            }
            
        except Exception as e:
            logger.error(f"EODHD新闻获取任务执行失败: {str(e)}")
            return {
                'success': False,
                'message': f"EODHD新闻获取失败: {str(e)}",
                'data': None
            }
    
    async def _execute_economic_events_task(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """执行经济事件获取任务"""
        try:
            logger.info(f"执行经济事件获取任务 | params={params}")
            
            # 导入所需的服务
            from backend.services.economic_events_service import EconomicEventsService
            from backend.services.market_data_service import MarketDataService
            
            # 初始化服务
            api_client = EconomicEventsService()
            market_service = MarketDataService()
            
            # 构建API请求参数
            api_params = {}
            if 'from_date' in params:
                api_params['from_date'] = params['from_date']
            if 'to_date' in params:
                api_params['to_date'] = params['to_date']
            if 'country' in params:
                api_params['country'] = params['country']


            if 'limit' in params:
                api_params['limit'] = params['limit']
            
            # 从API获取数据
            events_data = api_client.get_economic_events(**api_params)
            
            if not events_data:
                return {
                    'success': True,
                    'message': '未获取到新数据',
                    'data': {'total_fetched': 0, 'new_records': 0}
                }
            
            # 格式化数据
            formatted_data = []
            for event in events_data:
                formatted_event = api_client.format_event_data(event)
                formatted_data.append(formatted_event)
            
            # 保存到数据库
            inserted_count = market_service.batch_insert_economic_events(formatted_data)
            
            logger.info(f"经济事件任务执行完成: 获取{len(formatted_data)}条，保存{inserted_count}条")
            
            return {
                'success': True,
                'message': f'成功获取并保存 {inserted_count} 条经济事件数据',
                'data': {
                    'total_fetched': len(formatted_data),
                    'new_records': inserted_count,
                    'params': params
                }
            }
            
        except Exception as e:
            logger.error(f"经济事件获取任务执行失败: {str(e)}")
            return {
                'success': False,
                'message': f"经济事件获取失败: {str(e)}",
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
    
    async def _execute_exchange_data_task(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """执行EODHD Exchange数据获取任务"""
        try:
            logger.info(f"开始执行交易所数据获取任务，参数: {params}")
            
            # 获取参数
            exchanges = params.get('exchanges', [])
            
            # 参数验证
            if not exchanges:
                logger.warning("未指定交易所参数，跳过交易所数据获取任务")
                return {
                    'success': False, 
                    'message': '未指定交易所参数，请至少选择一个交易所'
                }
            
            # 确保exchanges是列表格式
            if isinstance(exchanges, str):
                exchanges = [exchanges]
            
            logger.info(f"准备获取 {len(exchanges)} 个交易所的数据")
            
            # 初始化交易所数据服务
            from backend.services.exchange_data_service import get_exchange_service
            exchange_service = await get_exchange_service()
            
            total_success = 0
            total_failed = 0
            results = []
            
            # 遍历每个交易所
            for exchange_code in exchanges:
                logger.info(f"开始获取交易所 {exchange_code} 的数据")
                
                try:
                    # 获取交易所数据
                    result = await exchange_service.fetch_exchange_tickers(
                        exchange_code=exchange_code,
                        progress_callback=None  # 暂时不使用进度回调
                    )
                    
                    if result.get('success'):
                        total_success += 1
                        results.append({
                            'exchange_code': exchange_code,
                            'total_fetched': result.get('total_fetched', 0),
                            'success_count': result.get('success_count', 0),
                            'status': 'success',
                            'message': result.get('message', '')
                        })
                        logger.info(f"成功获取 {exchange_code} 数据，保存 {result.get('success_count', 0)} 个ticker")
                    else:
                        total_failed += 1
                        results.append({
                            'exchange_code': exchange_code,
                            'total_fetched': 0,
                            'success_count': 0,
                            'status': 'failed',
                            'error': result.get('message', '未知错误')
                        })
                        logger.warning(f"获取 {exchange_code} 数据失败: {result.get('message', '')}")
                    
                    # 避免API请求过于频繁
                    await asyncio.sleep(0.5)
                    
                except Exception as e:
                    total_failed += 1
                    error_msg = f"获取 {exchange_code} 数据时出错: {str(e)}"
                    logger.error(error_msg)
                    results.append({
                        'exchange_code': exchange_code,
                        'total_fetched': 0,
                        'success_count': 0,
                        'status': 'error',
                        'error': str(e)
                    })
            
            # 生成执行结果消息
            if total_success > 0:
                total_tickers = sum(r.get('success_count', 0) for r in results if r['status'] == 'success')
                message = f"交易所数据获取任务执行完成: 成功 {total_success} 个交易所，失败 {total_failed} 个，共获取 {total_tickers} 个ticker"
                if total_failed > 0:
                    failed_exchanges = [r['exchange_code'] for r in results if r['status'] != 'success']
                    message += f"。失败的交易所: {', '.join(failed_exchanges)}"
            else:
                message = f"交易所数据获取任务执行失败: 所有 {total_failed} 个交易所都失败了"
            
            logger.info(f"交易所数据获取任务执行完成: {message}")
            
            return {
                'success': total_success > 0,
                'message': message,
                'data': {
                    'total_exchanges': total_success + total_failed,
                    'successful': total_success,
                    'failed': total_failed,
                    'results': results,
                    'params': params
                }
            }
            
        except Exception as e:
            error_msg = f"交易所数据获取任务执行失败: {str(e)}"
            logger.error(error_msg)
            return {
                'success': False,
                'message': error_msg,
                'data': None
            }
    
    async def _execute_fundamentals_task(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """执行EODHD基本面数据获取任务"""
        try:
            logger.info(f"开始执行基本面数据获取任务，参数: {params}")
            
            # 获取参数
            exchanges = params.get('exchanges', [])
            data_types = params.get('data_types', ['General', 'Financials', 'Highlights', 'Valuation', 'Technicals', 'Holders', 'outstandingShares', 'Earnings', 'SharesStats', 'AnalystRatings', 'SplitsDividends', 'InsiderTransactions'])  # 支持所有基本面数据类型
            
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
            supported_exchanges = ['NYSE', 'NASDAQ', 'AMEX', 'SHG', 'SHE', 'HK', 'TWO', 'JP', 'LSE', 'AIM']
            valid_exchanges = [ex for ex in exchanges if ex.upper() in [se.upper() for se in supported_exchanges]]
            
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
                valid_data_types = ['General', 'Financials', 'Highlights', 'Valuation', 'Technicals', 'Holders', 'outstandingShares', 'Earnings', 'SharesStats', 'AnalystRatings', 'SplitsDividends', 'InsiderTransactions']  # 默认使用所有支持的类型
            
            logger.info(f"准备获取 {len(valid_exchanges)} 个交易所的基本面数据，数据类型: {valid_data_types}")
            
            # 初始化基本面数据服务
            try:
                from backend.services.eodhd_fundamentals_service import EODHDFundamentalsService
                fundamentals_service = EODHDFundamentalsService()
                
                # 执行基本面数据获取
                result = await fundamentals_service.fetch_exchange_fundamentals(
                    exchanges=valid_exchanges,
                    data_types=valid_data_types,
                    progress_callback=lambda msg: logger.info(f"进度: {msg}")
                )
                
                if result.get('success', False):
                    total_companies = result.get('total_companies', 0)
                    successful_companies = result.get('successful_companies', 0)
                    failed_companies = result.get('failed_companies', 0)
                    exchanges_data = result.get('exchanges', {})
                    
                    # 详细统计处理结果
                    processed_exchanges = len([ex for ex, data in exchanges_data.items() if data.get('successful', 0) > 0])
                    success_rate = (successful_companies / total_companies * 100) if total_companies > 0 else 0
                    
                    # 构建成功率描述
                    if success_rate >= 90:
                        success_level = "优秀"
                    elif success_rate >= 70:
                        success_level = "良好"
                    elif success_rate >= 50:
                        success_level = "一般"
                    else:
                        success_level = "需要关注"
                    
                    success_msg = f"基本面数据获取完成: 总计 {total_companies} 家公司, 成功 {successful_companies} 家, 失败 {failed_companies} 家 (成功率: {success_rate:.1f}% - {success_level}), 处理 {processed_exchanges} 个交易所"
                    
                    # 添加数据类型详情信息
                    data_type_details = []
                    if 'General' in valid_data_types:
                        data_type_details.append("基本信息")
                    if 'Financials' in valid_data_types:
                        data_type_details.append("财务报表")
                    if 'Highlights' in valid_data_types:
                        data_type_details.append("关键指标")
                    if 'Valuation' in valid_data_types:
                        data_type_details.append("估值数据")
                    if 'Technicals' in valid_data_types:
                        data_type_details.append("技术指标")
                    if 'Holders' in valid_data_types:
                        data_type_details.append("股东数据")
                    if 'outstandingShares' in valid_data_types:
                        data_type_details.append("流通股数据")
                    if 'Earnings' in valid_data_types:
                        data_type_details.append("盈利数据")
                    
                    if data_type_details:
                        success_msg += f" (包含: {', '.join(data_type_details)})"
                    
                    # 添加交易所详细信息
                    exchange_details = []
                    for exchange, data in exchanges_data.items():
                        ex_success = data.get('successful', 0)
                        ex_total = data.get('total', 0)
                        if ex_total > 0:
                            ex_rate = (ex_success / ex_total * 100)
                            exchange_details.append(f"{exchange}: {ex_success}/{ex_total} ({ex_rate:.1f}%)")
                    
                    if exchange_details:
                        success_msg += f"\n交易所详情: {', '.join(exchange_details)}"
                    
                    logger.info(success_msg)
                    return {
                        'success': True,
                        'message': success_msg,
                        'data': result,
                        'statistics': {
                            'total_companies': total_companies,
                            'successful_companies': successful_companies,
                            'failed_companies': failed_companies,
                            'success_rate': success_rate,
                            'processed_exchanges': processed_exchanges,
                            'data_types': valid_data_types,
                            'exchange_details': exchanges_data
                        }
                }
                else:
                    # 详细分析失败原因
                    error_details = result.get('data', {})
                    exchanges_data = error_details.get('exchanges', {})
                    
                    failed_exchanges = []
                    for exchange, data in exchanges_data.items():
                        if data.get('successful', 0) == 0:
                            error_reason = data.get('message', '未知错误')
                            failed_exchanges.append(f"{exchange}: {error_reason}")
                    
                    error_msg = f"基本面数据获取失败: {result.get('message', '未知错误')}"
                    if failed_exchanges:
                        error_msg += f"\n失败的交易所: {'; '.join(failed_exchanges)}"
                    
                    logger.error(error_msg)
                    return {
                        'success': False,
                        'message': error_msg,
                        'data': result,
                        'failed_exchanges': failed_exchanges
                    }
                    
            except ImportError as e:
                error_msg = f"无法导入基本面数据服务: {str(e)}"
                logger.error(error_msg)
                return {
                    'success': False,
                    'message': error_msg,
                    'data': None
                }
            except Exception as service_error:
                error_msg = f"基本面数据服务执行失败: {str(service_error)}"
                logger.error(error_msg)
                return {
                    'success': False,
                    'message': error_msg,
                    'data': None
                }
            
        except Exception as e:
            error_msg = f"基本面数据低频获取任务执行失败: {str(e)}"
            logger.error(error_msg)
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
    
    def _job_listener(self, event):
        """任务执行监听器"""
        if event.exception:
            logger.error(f"任务执行出错: {event.job_id}, 异常: {event.exception}")
        else:
            logger.info(f"任务执行完成: {event.job_id}")
    
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
                                    offset: int = 0) -> List[Dict[str, Any]]:
        """获取任务执行日志"""
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
