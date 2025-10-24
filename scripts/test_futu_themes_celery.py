#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试Futu投资主题Celery任务

用途：
1. 验证Celery任务是否正确配置
2. 测试任务提交和状态查询
3. 快速测试爬取功能（小规模）
"""

import sys
import time
import requests
from pathlib import Path

# 添加项目根目录到Python路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# API基础URL
BASE_URL = "http://localhost:8000"


def test_submit_task():
    """测试提交Celery任务"""
    print("=" * 60)
    print("测试1: 提交Futu投资主题爬取任务（小规模测试）")
    print("=" * 60)
    
    url = f"{BASE_URL}/api/investment-themes/fetch-futu-themes"
    
    # 小规模测试：只爬取HK市场的前2页
    payload = {
        "markets": ["HK"],
        "max_pages": 2,
        "scrape_stocks": True
    }
    
    print(f"\n发送请求到: {url}")
    print(f"参数: {payload}")
    
    try:
        response = requests.post(url, json=payload)
        response.raise_for_status()
        
        result = response.json()
        print(f"\n✅ 任务提交成功!")
        print(f"Task ID: {result.get('task_id')}")
        print(f"Message: {result.get('message')}")
        print(f"Status URL: {result.get('status_url')}")
        print(f"预计运行时间: {result.get('estimated_duration')}")
        
        return result.get('task_id')
        
    except requests.exceptions.RequestException as e:
        print(f"\n❌ 任务提交失败: {str(e)}")
        return None


def test_query_status(task_id):
    """测试查询任务状态"""
    print("\n" + "=" * 60)
    print(f"测试2: 查询任务状态 (Task ID: {task_id})")
    print("=" * 60)
    
    url = f"{BASE_URL}/api/investment-themes/task-status/{task_id}"
    
    print(f"\n查询URL: {url}")
    print("\n开始监控任务进度...")
    print("-" * 60)
    
    max_attempts = 60  # 最多查询60次
    attempt = 0
    
    while attempt < max_attempts:
        try:
            response = requests.get(url)
            response.raise_for_status()
            
            result = response.json()
            status = result.get('status')
            progress = result.get('progress', 0)
            message = result.get('message', '')
            
            # 显示进度
            progress_bar = "█" * int(progress / 5) + "░" * (20 - int(progress / 5))
            print(f"\r[{progress_bar}] {progress}% - {status}: {message}", end='', flush=True)
            
            # 检查任务状态
            if status == 'SUCCESS':
                print("\n\n✅ 任务执行成功!")
                task_result = result.get('result', {})
                statistics = task_result.get('statistics', {})
                
                print("\n📊 执行结果:")
                print(f"  - 总主题数: {statistics.get('total_themes', 0)}")
                print(f"  - 成功: {statistics.get('success_count', 0)}")
                print(f"  - 失败: {statistics.get('failed_count', 0)}")
                print(f"  - 总股票数: {statistics.get('total_stocks', 0)}")
                print(f"  - 执行时间: {statistics.get('execution_time', 0):.2f}秒")
                print(f"  - 处理市场: {', '.join(statistics.get('markets', []))}")
                return True
                
            elif status == 'FAILURE':
                print("\n\n❌ 任务执行失败!")
                error = result.get('error', 'Unknown error')
                print(f"错误信息: {error}")
                return False
                
            elif status in ['PENDING', 'PROGRESS']:
                # 任务进行中，继续等待
                time.sleep(5)  # 每5秒查询一次
                attempt += 1
            else:
                print(f"\n\n⚠️ 未知状态: {status}")
                return False
                
        except requests.exceptions.RequestException as e:
            print(f"\n\n❌ 查询状态失败: {str(e)}")
            return False
        except KeyboardInterrupt:
            print("\n\n⚠️ 用户中断监控")
            return False
    
    print("\n\n⏱️ 查询超时（已查询5分钟）")
    print("提示：任务可能仍在后台运行，请稍后手动查询状态")
    return False


def test_celery_connectivity():
    """测试Celery连接性"""
    print("=" * 60)
    print("测试0: 检查Celery连接性")
    print("=" * 60)
    
    try:
        from celery import Celery
        from backend.celery_app import app
        
        # 检查Celery配置
        print(f"\n📋 Celery配置:")
        print(f"  - Broker: {app.conf.broker_url}")
        print(f"  - Backend: {app.conf.result_backend}")
        
        # 尝试ping
        inspect = app.control.inspect()
        stats = inspect.stats()
        
        if stats:
            print(f"\n✅ Celery连接正常")
            print(f"活跃Worker数量: {len(stats)}")
            for worker_name, worker_stats in stats.items():
                print(f"  - {worker_name}")
                print(f"    Pool: {worker_stats.get('pool', {}).get('implementation', 'N/A')}")
                print(f"    Max-concurrency: {worker_stats.get('pool', {}).get('max-concurrency', 'N/A')}")
            return True
        else:
            print(f"\n⚠️ 没有检测到活跃的Celery Worker")
            print(f"请确保已启动Worker: ./development/start_celery_worker_data_fetching.sh")
            return False
            
    except Exception as e:
        print(f"\n❌ Celery连接失败: {str(e)}")
        return False


def main():
    """主测试流程"""
    print("\n" + "🧪" * 30)
    print("Futu投资主题Celery任务测试")
    print("🧪" * 30 + "\n")
    
    # 测试0: 检查Celery连接
    if not test_celery_connectivity():
        print("\n⚠️ 警告：Celery Worker未运行，任务可能无法执行")
        print("建议先启动Worker: ./development/start_celery_worker_data_fetching.sh")
        
        user_input = input("\n是否继续测试任务提交? (y/n): ")
        if user_input.lower() != 'y':
            print("\n测试已取消")
            return
    
    # 测试1: 提交任务
    task_id = test_submit_task()
    
    if not task_id:
        print("\n❌ 测试失败：无法提交任务")
        return
    
    # 询问是否监控任务
    user_input = input("\n是否监控任务执行? (y/n): ")
    if user_input.lower() == 'y':
        # 测试2: 查询任务状态
        test_query_status(task_id)
    else:
        print(f"\n可以稍后通过以下URL查询任务状态:")
        print(f"{BASE_URL}/api/investment-themes/task-status/{task_id}")
    
    print("\n" + "=" * 60)
    print("测试完成")
    print("=" * 60)
    print("\n提示：")
    print("1. 查看Worker日志: tail -f logs/celery_worker_data_fetching.log")
    print("2. 使用Flower监控: celery -A backend.celery_app flower --port=5555")
    print("3. 手动查询状态: curl http://localhost:8000/api/investment-themes/task-status/{task_id}")


if __name__ == "__main__":
    main()

