#!/usr/bin/env python3
"""
测试Futu爬虫的分页功能
"""

import sys
import os
from pathlib import Path

# 添加项目根目录到Python路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from backend.scrapers.futu_scraper import scrape_multiple_pages, scrape_all_markets
import time

def test_single_market_few_pages():
    """测试单个市场的前几页"""
    print("\n" + "="*70)
    print("测试1: 爬取HK市场前2页（快速测试）")
    print("="*70)
    
    start_time = time.time()
    
    result = scrape_multiple_pages(
        market='HK',
        max_consecutive_empty=2,
        max_pages=2,  # 只爬前2页
        scrape_stocks=True,
        max_themes_with_stocks=5  # 只爬前5个主题的股票
    )
    
    elapsed = time.time() - start_time
    
    print(f"\n测试1完成:")
    print(f"  - 爬取主题数: {len(result)}")
    print(f"  - 耗时: {elapsed:.1f}秒")
    
    # 统计股票数
    themes_with_stocks = [t for t in result if t.get('stocks')]
    total_stocks = sum(len(t.get('stocks', [])) for t in result)
    
    if themes_with_stocks:
        print(f"  - 有股票的主题数: {len(themes_with_stocks)}")
        print(f"  - 总股票数: {total_stocks}")
        print(f"  - 平均每主题股票数: {total_stocks/len(themes_with_stocks):.1f}")
        
        # 显示前3个主题的股票数
        print(f"\n  前3个主题的股票数:")
        for i, theme in enumerate(themes_with_stocks[:3]):
            stock_count = len(theme.get('stocks', []))
            print(f"    {i+1}. {theme['theme_name']}: {stock_count}只股票")
    
    return result


def test_full_market():
    """测试完整市场爬取（所有页面）"""
    print("\n" + "="*70)
    print("测试2: 爬取HK市场所有页面（完整测试）")
    print("="*70)
    
    confirm = input("这将爬取所有页面，可能需要10-15分钟。是否继续？(y/n): ")
    if confirm.lower() != 'y':
        print("已取消")
        return None
    
    start_time = time.time()
    
    result = scrape_multiple_pages(
        market='HK',
        max_consecutive_empty=3,
        max_pages=None,  # 爬取所有页面
        scrape_stocks=True,
        max_themes_with_stocks=None  # 爬取所有主题的股票
    )
    
    elapsed = time.time() - start_time
    
    print(f"\n测试2完成:")
    print(f"  - 爬取主题数: {len(result)}")
    print(f"  - 耗时: {elapsed/60:.1f}分钟")
    
    # 统计股票数
    themes_with_stocks = [t for t in result if t.get('stocks')]
    total_stocks = sum(len(t.get('stocks', [])) for t in result)
    
    if themes_with_stocks:
        print(f"  - 有股票的主题数: {len(themes_with_stocks)}")
        print(f"  - 总股票数: {total_stocks}")
        print(f"  - 平均每主题股票数: {total_stocks/len(themes_with_stocks):.1f}")
        
        # 找出股票数最多的主题
        max_stocks_theme = max(themes_with_stocks, key=lambda t: len(t.get('stocks', [])))
        print(f"\n  股票数最多的主题:")
        print(f"    {max_stocks_theme['theme_name']}: {len(max_stocks_theme['stocks'])}只股票")
    
    return result


def test_both_markets():
    """测试两个市场的完整爬取"""
    print("\n" + "="*70)
    print("测试3: 爬取HK和US两个市场（完整测试）")
    print("="*70)
    
    confirm = input("这将爬取两个市场的所有页面，可能需要20-30分钟。是否继续？(y/n): ")
    if confirm.lower() != 'y':
        print("已取消")
        return None
    
    start_time = time.time()
    
    result = scrape_all_markets(
        markets_to_scrape=['HK', 'US'],
        max_consecutive_empty=3,
        max_pages=None,
        scrape_stocks=True,
        max_themes_with_stocks=None
    )
    
    elapsed = time.time() - start_time
    
    print(f"\n测试3完成:")
    print(f"  - 总耗时: {elapsed/60:.1f}分钟")
    
    for market, themes in result.items():
        print(f"\n  {market}市场:")
        print(f"    - 主题数: {len(themes)}")
        
        themes_with_stocks = [t for t in themes if t.get('stocks')]
        total_stocks = sum(len(t.get('stocks', [])) for t in themes)
        
        if themes_with_stocks:
            print(f"    - 有股票的主题数: {len(themes_with_stocks)}")
            print(f"    - 总股票数: {total_stocks}")
            print(f"    - 平均每主题股票数: {total_stocks/len(themes_with_stocks):.1f}")
    
    return result


def main():
    """主测试函数"""
    print("\n" + "="*70)
    print("Futu爬虫分页功能测试")
    print("="*70)
    
    print("\n请选择测试模式：")
    print("1. 快速测试（HK市场前2页，约1-2分钟）")
    print("2. 完整测试（HK市场所有页面，约10-15分钟）")
    print("3. 全量测试（HK+US两个市场，约20-30分钟）")
    print("0. 退出")
    
    choice = input("\n请输入选择 (0-3): ")
    
    if choice == '1':
        test_single_market_few_pages()
    elif choice == '2':
        test_full_market()
    elif choice == '3':
        test_both_markets()
    elif choice == '0':
        print("退出测试")
        return
    else:
        print("无效选择")
        return
    
    print("\n" + "="*70)
    print("测试完成！")
    print("="*70)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⏹️  用户中断了测试")
    except Exception as e:
        print(f"\n❌ 测试出错: {e}")
        import traceback
        traceback.print_exc()

