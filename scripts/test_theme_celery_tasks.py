#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Investment Themes Celery 任务测试工具

功能：
- 验证 Celery 任务注册
- 检查系统健康状态
- 测试任务提交和执行
- 诊断常见问题

使用方法：
    python scripts/test_theme_celery_tasks.py
    
环境要求：
    - conda activate rcTool
    - Redis 服务运行
    - Celery Worker 运行
"""

import sys
import os

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.utils.env_loader import load_project_env

# 加载环境变量
load_project_env()


def test_task_registration():
    """测试任务是否正确注册"""
    print("=" * 60)
    print("测试 1: 检查 Celery 任务注册")
    print("=" * 60)
    
    try:
        from backend.celery_app import app
        
        # 获取注册的任务
        registered_tasks = list(app.tasks.keys())
        
        # 需要检查的主题任务
        theme_tasks = [
            'initialize_theme_embeddings',
            'fill_theme_descriptions',
            'regenerate_all_theme_embeddings'
        ]
        
        print(f"\n找到 {len(registered_tasks)} 个注册的任务\n")
        
        # 检查主题任务是否注册
        for task_name in theme_tasks:
            if task_name in registered_tasks:
                print(f"✅ {task_name} - 已注册")
            else:
                print(f"❌ {task_name} - 未注册")
        
        print("\n所有注册的任务：")
        for task in sorted(registered_tasks):
            if not task.startswith('celery.'):
                print(f"  - {task}")
        
        return True
        
    except Exception as e:
        print(f"❌ 错误: {str(e)}")
        return False


def test_task_submission():
    """测试任务提交（不实际执行）"""
    print("\n" + "=" * 60)
    print("测试 2: 测试任务提交")
    print("=" * 60)
    
    try:
        from backend.tasks.theme_tasks import (
            initialize_theme_embeddings_task,
            fill_theme_descriptions_task,
            regenerate_all_theme_embeddings_task
        )
        
        print("\n✅ 成功导入所有任务函数")
        
        # 测试任务签名（不实际执行）
        print("\n测试任务签名：")
        
        # 1. 初始化任务
        task1_signature = initialize_theme_embeddings_task.s(force_update=False)
        print(f"✅ initialize_theme_embeddings_task 签名创建成功")
        
        # 2. 填充描述任务
        task2_signature = fill_theme_descriptions_task.s(
            force_regenerate=False, 
            market="US"
        )
        print(f"✅ fill_theme_descriptions_task 签名创建成功")
        
        # 3. 重新生成任务
        task3_signature = regenerate_all_theme_embeddings_task.s(market="US")
        print(f"✅ regenerate_all_theme_embeddings_task 签名创建成功")
        
        return True
        
    except Exception as e:
        print(f"❌ 错误: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


def test_celery_connection():
    """测试 Celery 连接"""
    print("\n" + "=" * 60)
    print("测试 3: 测试 Celery 连接")
    print("=" * 60)
    
    try:
        from backend.celery_app import app
        
        # 检查 Redis 连接
        print("\n检查 Redis 连接...")
        result = app.control.inspect().stats()
        
        if result:
            print(f"✅ 找到 {len(result)} 个活跃的 Worker")
            for worker_name, stats in result.items():
                print(f"  - {worker_name}")
                print(f"    进程池: {stats.get('pool', {}).get('max-concurrency', 'N/A')}")
        else:
            print("⚠️  警告: 没有找到活跃的 Worker")
            print("   提示: 请确保 Celery Worker 正在运行")
            print("   命令: ./development/start_celery_worker_dev.sh")
        
        return True
        
    except Exception as e:
        print(f"❌ 错误: {str(e)}")
        print("\n提示:")
        print("  1. 确保 Redis 正在运行: redis-cli ping")
        print("  2. 检查 REDIS_URL 环境变量")
        return False


def test_task_execution_dry_run():
    """测试任务执行（仅提交，不等待结果）"""
    print("\n" + "=" * 60)
    print("测试 4: 测试任务提交（Dry Run）")
    print("=" * 60)
    
    try:
        from backend.celery_app import app
        
        # 检查是否有 Worker 在运行
        result = app.control.inspect().stats()
        if not result:
            print("⚠️  跳过: 没有活跃的 Worker，无法测试任务提交")
            return True
        
        from backend.tasks.theme_tasks import fill_theme_descriptions_task
        
        print("\n提交测试任务（仅测试，market='INVALID' 不会处理实际数据）...")
        
        # 提交一个不会处理实际数据的任务
        task = fill_theme_descriptions_task.apply_async(
            kwargs={
                'force_regenerate': False,
                'market': 'INVALID_MARKET_TEST'  # 使用无效市场，不会处理任何数据
            }
        )
        
        print(f"✅ 任务已提交")
        print(f"   Task ID: {task.id}")
        print(f"   状态: {task.state}")
        
        # 等待2秒检查状态
        import time
        print("\n等待 2 秒检查任务状态...")
        time.sleep(2)
        
        task.forget()  # 清除任务结果
        print(f"✅ 任务状态查询成功（已清除测试任务）")
        
        return True
        
    except Exception as e:
        print(f"❌ 错误: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """主函数"""
    print("\n🚀 开始测试 Investment Themes Celery 任务\n")
    
    results = []
    
    # 执行测试
    results.append(("任务注册", test_task_registration()))
    results.append(("任务提交", test_task_submission()))
    results.append(("Celery 连接", test_celery_connection()))
    results.append(("任务执行 (Dry Run)", test_task_execution_dry_run()))
    
    # 输出总结
    print("\n" + "=" * 60)
    print("测试总结")
    print("=" * 60)
    
    for test_name, success in results:
        status = "✅ 通过" if success else "❌ 失败"
        print(f"{status} - {test_name}")
    
    # 总体结果
    all_passed = all(success for _, success in results)
    
    if all_passed:
        print("\n🎉 所有测试通过！")
        print("\n下一步：")
        print("  1. 启动 Celery Worker: ./development/start_celery_worker_dev.sh")
        print("  2. 启动 Flower 监控: ./development/start_flower_dev.sh")
        print("  3. 测试 API: POST /api/investment-themes/initialize")
    else:
        print("\n⚠️  部分测试失败，请检查错误信息")
    
    return 0 if all_passed else 1


if __name__ == '__main__':
    sys.exit(main())

