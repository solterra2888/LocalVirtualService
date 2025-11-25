#!/usr/bin/env python3
"""
CCTV新闻内容提取脚本
从新闻列表页爬取新闻，提取文本内容，并导出到JSON和CSV文件

使用方法:
1. 安装依赖:
   pip install requests beautifulsoup4

2. 运行脚本:
   python3 extract_news_content.py
   或
   python3 extract_news_content.py --date 20251124  # 指定日期
   或
   python3 extract_news_content.py --max-news 10  # 限制数量

功能:
- 直接爬取CCTV新闻列表
- 自动提取每条新闻的文本内容
- 导出到JSON和CSV文件
"""

import requests
import json
import csv
import time
import random
from datetime import datetime
from typing import Optional, List, Dict
import argparse
import re
from pathlib import Path

# BeautifulSoup延迟导入
try:
    from bs4 import BeautifulSoup
    BS4_AVAILABLE = True
except ImportError:
    BS4_AVAILABLE = False
    print("⚠️  BeautifulSoup未安装，请运行: pip install beautifulsoup4")

# 用户代理池
USER_AGENTS = [
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
]

# 基础URL
BASE_URL = 'https://tv.cctv.com/lm/xwlb/index.shtml'

def get_random_headers(referer: Optional[str] = None) -> Dict[str, str]:
    """生成随机请求头"""
    headers = {
        'User-Agent': random.choice(USER_AGENTS),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Cache-Control': 'max-age=0',
    }
    
    if referer:
        headers['Referer'] = referer
    
    return headers

def smart_delay(base_min: float = 1.0, base_max: float = 2.0):
    """智能延迟"""
    delay = random.uniform(base_min, base_max)
    time.sleep(delay)

def make_request_with_retry(url: str, session: requests.Session, referer: Optional[str] = None,
                            max_retries: int = 3, timeout: int = 30) -> Optional[requests.Response]:
    """带重试机制的请求函数"""
    for attempt in range(max_retries):
        try:
            headers = get_random_headers(referer)
            response = session.get(url, headers=headers, timeout=timeout)
            response.encoding = response.apparent_encoding or 'utf-8'
            response.raise_for_status()
            return response
        except requests.exceptions.Timeout:
            if attempt < max_retries - 1:
                print(f"   ⚠️  请求超时，重试 {attempt + 1}/{max_retries}...")
                smart_delay(2.0, 4.0)
        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                print(f"   ⚠️  请求错误: {e}，重试 {attempt + 1}/{max_retries}...")
                smart_delay(2.0, 4.0)
            else:
                print(f"   ❌ 请求失败: {e}")
    
    return None

def scrape_news_list(date: Optional[str] = None, max_news: Optional[int] = None,
                    session: requests.Session = None) -> List[Dict]:
    """爬取新闻列表
    
    Args:
        date: 日期字符串，格式为 YYYYMMDD
        max_news: 最大爬取新闻数量
        session: requests会话
    
    Returns:
        List[Dict]: 新闻列表
    """
    if not BS4_AVAILABLE:
        print("❌ BeautifulSoup未安装，无法爬取新闻")
        return []
    
    if session is None:
        session = requests.Session()
        close_session = True
    else:
        close_session = False
    
    try:
        # 构建URL
        if date:
            url = f"{BASE_URL}?date={date}"
        else:
            url = BASE_URL
        
        print(f"📄 正在爬取新闻列表...")
        print(f"   URL: {url}")
        
        response = make_request_with_retry(url, session)
        
        if not response:
            print("   ❌ 无法获取页面")
            return []
        
        print("   ✅ 页面获取成功，正在解析数据...")
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # 查找新闻链接
        all_links = soup.find_all('a', href=True)
        seen_urls = set()
        news_items = []
        
        for link in all_links:
            href = link.get('href', '')
            text = link.get_text(strip=True)
            
            # 处理相对URL
            if href.startswith('/'):
                href = 'https://tv.cctv.com' + href
            elif href.startswith('./'):
                href = 'https://tv.cctv.com/lm/xwlb/' + href[2:]
            
            # 过滤出新闻详情页链接
            if 'VIDE' in href.upper() and 'tv.cctv.com' in href and text:
                if href not in seen_urls:
                    seen_urls.add(href)
                    news_items.append({
                        'link': link,
                        'url': href,
                        'title': text
                    })
        
        print(f"   📊 找到 {len(news_items)} 个新闻链接")
        
        # 限制数量
        if max_news and len(news_items) > max_news:
            news_items = news_items[:max_news]
            print(f"   📊 限制数量为: {max_news}")
        
        # 提取新闻信息
        news_list = []
        for item in news_items:
            title = item['title']
            url = item['url']
            
            # 清理标题
            title = re.sub(r'\s+', ' ', title).strip()
            title = re.sub(r'^完整版\s*', '', title)
            title = re.sub(r'^_完整版_\s*', '', title)
            title = re.sub(r'^\[视频\]', '', title).strip()
            
            if not title:
                continue
            
            # 从URL中提取日期
            date_match = re.search(r'/(\d{4})/(\d{2})/(\d{2})/', url)
            news_date = None
            if date_match:
                year, month, day = date_match.groups()
                news_date = f"{year}-{month}-{day}"
            
            news_list.append({
                'title': title,
                'url': url,
                'date': news_date,
                'scraped_at': datetime.now().isoformat()
            })
        
        print(f"   ✅ 成功提取 {len(news_list)} 条新闻")
        return news_list
        
    except Exception as e:
        print(f"   ❌ 爬取过程中出错: {e}")
        import traceback
        traceback.print_exc()
        return []
    finally:
        if close_session:
            session.close()

def extract_content_from_page(url: str, session: requests.Session) -> Optional[str]:
    """从新闻详情页提取文本内容
    
    Args:
        url: 新闻详情页URL
        session: requests会话
    
    Returns:
        提取的文本内容，如果失败返回None
    """
    if not BS4_AVAILABLE:
        return None
    
    try:
        response = make_request_with_retry(url, session)
        
        if not response:
            return None
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # 方法1: 查找id为content_area的div（主要方法）
        content_area = soup.find('div', id='content_area')
        if content_area:
            paragraphs = []
            for p in content_area.find_all('p'):
                text = p.get_text(strip=True)
                if text:
                    paragraphs.append(text)
            
            if paragraphs:
                content = '\n'.join(paragraphs)
                return content
        
        # 方法2: 查找class包含content的div
        content_divs = soup.find_all('div', class_=lambda x: x and 'content' in x.lower())
        for div in content_divs:
            paragraphs = []
            for p in div.find_all('p'):
                text = p.get_text(strip=True)
                if text:
                    paragraphs.append(text)
            if paragraphs and len(paragraphs) > 2:
                content = '\n'.join(paragraphs)
                return content
        
        # 方法3: 查找主要内容区域
        main_content = soup.find('div', class_=lambda x: x and ('main' in x.lower() or 'article' in x.lower()))
        if main_content:
            text = main_content.get_text(separator='\n', strip=True)
            if len(text) > 100:
                return text
        
        return None
        
    except Exception as e:
        return None

def save_to_json(news_list: List[Dict], json_file: str):
    """保存新闻列表到JSON文件"""
    try:
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(news_list, f, ensure_ascii=False, indent=2)
        print(f"   💾 JSON文件已保存: {json_file}")
    except Exception as e:
        print(f"   ❌ 保存JSON文件失败: {e}")

def save_to_csv(news_list: List[Dict], csv_file: str):
    """保存新闻列表到CSV文件"""
    try:
        if not news_list:
            print(f"   ⚠️  没有数据可保存到CSV")
            return
        
        fieldnames = ['title', 'date', 'url', 'content', 'scraped_at', 'content_scraped_at']
        
        with open(csv_file, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
            writer.writeheader()
            
            for news in news_list:
                row = {
                    'title': news.get('title', ''),
                    'date': news.get('date', ''),
                    'url': news.get('url', ''),
                    'content': news.get('content', '') or '',
                    'scraped_at': news.get('scraped_at', ''),
                    'content_scraped_at': news.get('content_scraped_at', ''),
                }
                writer.writerow(row)
        
        print(f"   💾 CSV文件已保存: {csv_file}")
        
        content_count = sum(1 for n in news_list if n.get('content'))
        print(f"   📊 CSV统计: 共 {len(news_list)} 条，其中 {content_count} 条包含内容")
        
    except Exception as e:
        print(f"   ❌ 保存CSV文件失败: {e}")

def main():
    parser = argparse.ArgumentParser(
        description='CCTV新闻内容提取工具 - 直接爬取并提取内容',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 爬取最新日期的新闻
  python3 extract_news_content.py
  
  # 爬取指定日期的新闻
  python3 extract_news_content.py --date 20251124
  
  # 限制爬取数量
  python3 extract_news_content.py --max-news 10
  
  # 指定输出文件名前缀
  python3 extract_news_content.py --output my_news
        """
    )
    
    parser.add_argument('--date', type=str, help='日期（格式: YYYYMMDD），None表示最新日期')
    parser.add_argument('--max-news', type=int, help='最大爬取新闻数量')
    parser.add_argument('--output', type=str, help='输出文件名前缀（默认: cctv_news_with_content）')
    
    args = parser.parse_args()
    
    if not BS4_AVAILABLE:
        print("❌ 错误: BeautifulSoup未安装")
        print("   请运行: pip install beautifulsoup4")
        return
    
    print("=" * 70)
    print("CCTV新闻内容提取工具")
    print("=" * 70)
    print(f"目标日期: {'最新' if not args.date else args.date}")
    print(f"最大数量: {'全部' if not args.max_news else args.max_news}")
    print()
    
    session = requests.Session()
    start_time = time.time()
    
    try:
        # 1. 爬取新闻列表
        news_list = scrape_news_list(
            date=args.date,
            max_news=args.max_news,
            session=session
        )
        
        if not news_list:
            print("❌ 没有爬取到新闻")
            return
        
        print(f"\n{'='*70}")
        print(f"开始提取 {len(news_list)} 条新闻的内容...")
        print(f"{'='*70}\n")
        
        # 2. 提取每条新闻的内容
        success_count = 0
        failed_count = 0
        
        for idx, news in enumerate(news_list):
            print(f"[{idx+1}/{len(news_list)}] {news['title'][:50]}...")
            
            url = news['url']
            content = extract_content_from_page(url, session)
            
            if content:
                news['content'] = content
                news['content_scraped_at'] = datetime.now().isoformat()
                print(f"   ✅ 成功提取 ({len(content)} 字符)")
                success_count += 1
            else:
                news['content'] = None
                news['content_scraped_at'] = datetime.now().isoformat()
                print(f"   ❌ 提取失败")
                failed_count += 1
            
            # 延迟（除了最后一条）
            if idx < len(news_list) - 1:
                smart_delay(1.0, 2.0)
        
        # 3. 保存文件
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        prefix = args.output or "cctv_news_with_content"
        json_file = f"{prefix}_{timestamp}.json"
        csv_file = f"{prefix}_{timestamp}.csv"
        
        print(f"\n{'='*70}")
        print("💾 正在保存文件...")
        print(f"{'='*70}")
        
        save_to_json(news_list, json_file)
        save_to_csv(news_list, csv_file)
        
        # 统计信息
        elapsed_time = time.time() - start_time
        content_count = sum(1 for n in news_list if n.get('content'))
        
        print(f"\n{'='*70}")
        print("✅ 处理完成！")
        print(f"{'='*70}")
        print(f"   总新闻数: {len(news_list)}")
        print(f"   成功提取内容: {success_count}")
        print(f"   提取失败: {failed_count}")
        print(f"   包含内容的新闻: {content_count}/{len(news_list)}")
        print(f"   总耗时: {elapsed_time:.1f} 秒")
        print(f"\n📁 输出文件:")
        print(f"   JSON: {json_file}")
        print(f"   CSV:  {csv_file}")
        print(f"{'='*70}")
        
    except KeyboardInterrupt:
        print("\n\n⏹️  用户中断了程序")
    except Exception as e:
        print(f"\n❌ 程序运行出错: {e}")
        import traceback
        traceback.print_exc()
    finally:
        session.close()

if __name__ == "__main__":
    main()
