# -*- coding: utf-8 -*-
"""
Celery应用实例
"""
from celery import Celery
from celery.signals import worker_init, worker_shutdown, task_prerun, task_postrun, task_failure, task_retry
import os

# ─── 全系统时区契约: Celery worker 启动前注册 psycopg2 UTC 适配器 ───
# 与 backend/main.py 同步; 任何 naive datetime 写入 PG 自动转 UTC.
# 详见 backend/utils/datetime_utils.py.
from backend.utils.datetime_utils import install_psycopg2_utc_adapter
install_psycopg2_utc_adapter()

# 创建Celery应用
app = Celery('universal_glance')

# 加载配置
app.config_from_object('backend.celery_config')

# 自动发现任务模块
app.autodiscover_tasks(['backend.tasks'])

# 显式导入任务模块，确保任务被注册
try:
    from backend.tasks import file_tasks
    print(f"✅ 成功导入任务模块: {file_tasks.__name__}")
except ImportError as e:
    print(f"❌ 导入任务模块失败: {e}")

try:
    from backend.tasks import news_tasks
    print(f"✅ 成功导入新闻分析任务模块: {news_tasks.__name__}")
except ImportError as e:
    print(f"❌ 导入新闻分析任务模块失败: {e}")

try:
    from backend.tasks import theme_tasks
    print(f"✅ 成功导入投资主题任务模块: {theme_tasks.__name__}")
except ImportError as e:
    print(f"❌ 导入投资主题任务模块失败: {e}")

try:
    from backend.tasks import data_tasks
    print(f"✅ 成功导入数据获取任务模块: {data_tasks.__name__}")
except ImportError as e:
    print(f"❌ 导入数据获取任务模块失败: {e}")

try:
    from backend.tasks import tagging_tasks
    print(f"✅ 成功导入打标签任务模块: {tagging_tasks.__name__}")
except ImportError as e:
    print(f"❌ 导入打标签任务模块失败: {e}")

try:
    from backend.tasks import naming_tasks
    print(f"✅ 成功导入自动命名任务模块: {naming_tasks.__name__}")
except ImportError as e:
    print(f"❌ 导入自动命名任务模块失败: {e}")

try:
    from backend.tasks import classify_tasks
    print(f"✅ 成功导入自动归类任务模块: {classify_tasks.__name__}")
except ImportError as e:
    print(f"❌ 导入自动归类任务模块失败: {e}")

try:
    from backend.tasks import subscription_tasks
    print(f"✅ 成功导入订阅任务模块: {subscription_tasks.__name__}")
except ImportError as e:
    print(f"❌ 导入订阅任务模块失败: {e}")

try:
    from backend.tasks import report_tasks
    print(f"✅ 成功导入报告生成任务模块: {report_tasks.__name__}")
except ImportError as e:
    print(f"❌ 导入报告生成任务模块失败: {e}")

try:
    from backend.tasks import cleanup_tasks
    print(f"✅ 成功导入数据清理任务模块: {cleanup_tasks.__name__}")
except ImportError as e:
    print(f"❌ 导入数据清理任务模块失败: {e}")

try:
    from backend.tasks import feed_tagging_tasks
    print(f"✅ 成功导入 Feeds 打标任务模块: {feed_tagging_tasks.__name__}")
except ImportError as e:
    print(f"❌ 导入 Feeds 打标任务模块失败: {e}")

# PR-6: Feeds per-user 个性化 summary 任务
try:
    from backend.tasks import feed_summary_tasks
    print(f"✅ 成功导入 Feeds 个性化 summary 任务模块: {feed_summary_tasks.__name__}")
except ImportError as e:
    print(f"❌ 导入 Feeds 个性化 summary 任务模块失败: {e}")

# 设置Celery日志
@worker_init.connect
def setup_worker_logging(sender=None, **kwargs):
    """Worker初始化时设置日志"""
    try:
        from backend.utils.celery_logger import setup_celery_logging
        setup_celery_logging(
            log_dir='logs',
            worker_log_file='celery_worker.log',
            task_log_file='celery_tasks.log',
            log_level='INFO'
        )
        print("✅ Celery日志系统初始化完成")
    except Exception as e:
        print(f"❌ Celery日志系统初始化失败: {e}")

@worker_shutdown.connect
def worker_shutdown_handler(sender=None, **kwargs):
    """Worker关闭时的处理"""
    print("🔄 Celery Worker正在关闭...")

@task_prerun.connect
def task_prerun_handler(sender=None, task_id=None, task=None, args=None, kwargs=None, **kwds):
    """任务开始前的处理"""
    try:
        from backend.utils.celery_logger import get_celery_logger
        logger = get_celery_logger()
        logger.log_task_started(task_id, task.name)
    except Exception as e:
        print(f"任务开始日志记录失败: {e}")

@task_postrun.connect
def task_postrun_handler(sender=None, task_id=None, task=None, args=None, kwargs=None, retval=None, state=None, **kwds):
    """任务完成后的处理"""
    try:
        from backend.utils.celery_logger import get_celery_logger
        logger = get_celery_logger()
        if state == 'SUCCESS':
            logger.log_task_succeeded(task_id, task.name, retval)
        else:
            logger.log_task_failed(task_id, task.name, f"状态: {state}")
    except Exception as e:
        print(f"任务完成日志记录失败: {e}")

@task_failure.connect
def task_failure_handler(sender=None, task_id=None, exception=None, traceback=None, einfo=None, **kwargs):
    """任务失败时的处理"""
    try:
        from backend.utils.celery_logger import get_celery_logger
        logger = get_celery_logger()
        logger.log_task_failed(task_id, sender.name, str(exception), str(traceback))
    except Exception as e:
        print(f"任务失败日志记录失败: {e}")

@task_retry.connect
def task_retry_handler(sender=None, task_id=None, reason=None, einfo=None, **kwargs):
    """任务重试时的处理"""
    try:
        from backend.utils.celery_logger import get_celery_logger
        logger = get_celery_logger()
        logger.log_task_retry(task_id, sender.name, str(reason))
    except Exception as e:
        print(f"任务重试日志记录失败: {e}")

# 调试信息
if __name__ == '__main__':
    app.start()


