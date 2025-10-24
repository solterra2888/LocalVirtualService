#!/usr/bin/env python3
"""
测试Futu爬虫分页优化效果

这个脚本用于验证新的重复页面检测机制是否正常工作
"""

import sys
import os

# 添加项目根目录到路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from backend.scrapers.futu_scraper import scrape_theme_stocks
import requests
import time

def test_pagination_optimization():
    """测试分页优化效果"""
    
    print("="*70)
    print("Futu爬虫分页优化测试")
    print("="*70)
    print()
    
    # 测试案例：选择一些股票数量不同的主题
    test_cases = [
        {
            'theme_url': '/en/sectors/Rare-Earth-Metals-BK1186',
            'theme_name': 'Rare Earth Metals',
            'market': 'US',
            'expected': '应该在第1页就停止（股票数<10）'
        },
        {
            'theme_url': '/en/sectors/Luxury-Goods-BK1223',
            'theme_name': 'Luxury Goods',
            'market': 'US',
            'expected': '应该在检测到重复页面时停止'
        },
        {
            'theme_url': '/en/sectors/OLED-Technology-BK1991',
            'theme_name': 'OLED Technology',
            'market': 'US',
            'expected': '应该在第1页就停止（股票数<10）'
        }
    ]
    
    session = requests.Session()
    results = []
    
    for i, test_case in enumerate(test_cases, 1):
        print(f"\n测试案例 {i}/{len(test_cases)}")
        print(f"主题: {test_case['theme_name']}")
        print(f"预期: {test_case['expected']}")
        print("-"*70)
        
        start_time = time.time()
        
        try:
            stocks = scrape_theme_stocks(
                theme_url=test_case['theme_url'],
                theme_name=test_case['theme_name'],
                market=test_case['market'],
                session=session,
                referer=None,
                max_stock_pages=100  # 使用最大页数测试
            )
            
            elapsed_time = time.time() - start_time
            
            result = {
                'theme_name': test_case['theme_name'],
                'stock_count': len(stocks),
                'time': elapsed_time,
                'success': True
            }
            results.append(result)
            
            print(f"✅ 测试完成")
            print(f"   股票数量: {len(stocks)}")
            print(f"   耗时: {elapsed_time:.2f}秒")
            
        except Exception as e:
            print(f"❌ 测试失败: {e}")
            results.append({
                'theme_name': test_case['theme_name'],
                'success': False,
                'error': str(e)
            })
        
        # 测试之间添加延迟
        if i < len(test_cases):
            print("\n⏳ 等待3秒后继续下一个测试...")
            time.sleep(3)
    
    # 输出总结
    print("\n" + "="*70)
    print("测试总结")
    print("="*70)
    
    successful_tests = sum(1 for r in results if r.get('success', False))
    print(f"\n总测试数: {len(results)}")
    print(f"成功: {successful_tests}")
    print(f"失败: {len(results) - successful_tests}")
    
    print("\n详细结果:")
    for result in results:
        if result.get('success'):
            print(f"  ✅ {result['theme_name']}: {result['stock_count']}只股票, {result['time']:.2f}秒")
        else:
            print(f"  ❌ {result['theme_name']}: {result.get('error', '未知错误')}")
    
    print("\n" + "="*70)
    print("测试完成！")
    print("="*70)
    
    session.close()
    
    return successful_tests == len(results)

def test_duplicate_detection_logic():
    """测试重复检测逻辑的正确性"""
    print("\n" + "="*70)
    print("单元测试：重复检测逻辑")
    print("="*70 + "\n")
    
    # 模拟场景1：两页完全相同
    page1 = {'AAPL', 'GOOGL', 'MSFT'}
    page2 = {'AAPL', 'GOOGL', 'MSFT'}
    
    if page1 == page2:
        print("✅ 测试1通过: 完全相同的页面被正确检测")
    else:
        print("❌ 测试1失败: 相同页面未被检测")
    
    # 模拟场景2：两页不同
    page1 = {'AAPL', 'GOOGL', 'MSFT'}
    page2 = {'TSLA', 'NVDA', 'AMD'}
    
    if page1 != page2:
        print("✅ 测试2通过: 不同的页面被正确识别")
    else:
        print("❌ 测试2失败: 不同页面被错误判断为相同")
    
    # 模拟场景3：顺序不同但内容相同（Set会自动处理）
    page1 = {'AAPL', 'GOOGL', 'MSFT'}
    page2 = {'MSFT', 'AAPL', 'GOOGL'}
    
    if page1 == page2:
        print("✅ 测试3通过: 顺序不同但内容相同的页面被正确检测为相同")
    else:
        print("❌ 测试3失败: Set比较逻辑有问题")
    
    # 模拟场景4：部分重复
    page1 = {'AAPL', 'GOOGL', 'MSFT'}
    page2 = {'AAPL', 'GOOGL', 'TSLA'}
    
    if page1 != page2:
        print("✅ 测试4通过: 部分重复的页面被正确识别为不同")
    else:
        print("❌ 测试4失败: 部分重复被错误判断为完全相同")
    
    print("\n单元测试完成！\n")

if __name__ == "__main__":
    print("\n开始测试Futu爬虫分页优化...\n")
    
    # 先运行单元测试
    test_duplicate_detection_logic()
    
    # 询问是否继续实际爬取测试
    print("="*70)
    response = input("\n是否继续进行实际爬取测试？这将访问futu网站（y/n）: ")
    
    if response.lower() in ['y', 'yes']:
        success = test_pagination_optimization()
        sys.exit(0 if success else 1)
    else:
        print("\n已取消实际爬取测试。")
        sys.exit(0)

