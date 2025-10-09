# -*- coding: utf-8 -*-
"""
Celery应用实例
"""
from celery import Celery

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

# 调试信息
if __name__ == '__main__':
    app.start()


