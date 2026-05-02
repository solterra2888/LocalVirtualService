#!/usr/bin/env python3
"""
Futu Investment Themes & Stocks Scraper - Daily Full Update Version
富途投资主题与股票爬虫 - 每日全量更新版

专为每日定时全量爬取优化，保守稳定策略
"""

import requests
import json
import time
import random
from datetime import datetime
from backend.utils.datetime_utils import utcnow
from bs4 import BeautifulSoup
from typing import Optional, List, Dict
import re

# 市场配置
MARKETS = {
    'US': {
        'name': 'US Stock Market',
        'base_url': 'https://www.futunn.com/en/quote/us/concepts',
        'display_name': '美股市场'
    },
    'HK': {
        'name': 'HK Stock Market',
        'base_url': 'https://www.futunn.com/en/quote/hk/concepts',
        'display_name': '港股市场'
    }
}

# 扩展的用户代理池
USER_AGENTS = [
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
]

def get_random_headers(referer: Optional[str] = None) -> Dict[str, str]:
    """生成随机请求头，模拟真实浏览器行为"""
    headers = {
        'User-Agent': random.choice(USER_AGENTS),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'Cache-Control': 'max-age=0',
        'DNT': '1',
    }
    
    if referer:
        headers['Referer'] = referer
        headers['Sec-Fetch-Site'] = 'same-origin'
    
    return headers

def smart_delay(context: str, count: int = 0, error_count: int = 0, 
                base_min: float = 1.0, base_max: float = 2.0):
    """优化的智能延迟策略 - 缩短到原来的1/3时间
    
    基础延迟: 1-2秒（从3-6秒优化而来）
    """
    if context == 'theme_group':
        # 每10个主题后额外延迟
        delay = random.uniform(3, 5)  # 从8-12秒缩短到3-5秒
    elif context == 'theme':
        # 主题之间的延迟
        extra_delay = (count // 10) * 0.3  # 每10个主题增加0.3秒（从1秒缩短）
        delay = random.uniform(base_min + extra_delay, base_max + extra_delay)
    elif context == 'page':
        # 页面之间的延迟
        extra_delay = (count // 5) * 0.3  # 每5页增加0.3秒（从1秒缩短）
        delay = random.uniform(base_min + extra_delay, base_max + extra_delay)
    elif context == 'stock_page':
        # 股票分页之间的延迟（新增）
        delay = random.uniform(0.5, 1.0)  # 股票页之间更短的延迟
    else:
        delay = random.uniform(base_min, base_max)
    
    # 错误重试使用指数退避
    if error_count > 0:
        backoff_delay = min(60, (2 ** error_count))
        delay = random.uniform(backoff_delay, backoff_delay * 1.5)
        print(f"   ⏳ 错误重试，等待 {delay:.1f} 秒...")
    else:
        print(f"   ⏳ 等待 {delay:.1f} 秒...")
    
    time.sleep(delay)

def make_request_with_retry(url: str, session: requests.Session, referer: Optional[str] = None, 
                            max_retries: int = 5, timeout: int = 45) -> Optional[requests.Response]:
    """带重试机制的请求函数 - 保守版
    
    max_retries: 5次（比测试版更多）
    timeout: 45秒（比测试版更长）
    """
    for attempt in range(max_retries):
        try:
            headers = get_random_headers(referer)
            response = session.get(url, headers=headers, timeout=timeout)
            response.raise_for_status()
            return response
        except requests.exceptions.Timeout:
            print(f"   ⚠️  请求超时 (尝试 {attempt + 1}/{max_retries})")
            if attempt < max_retries - 1:
                smart_delay('error', error_count=attempt + 1)
        except requests.exceptions.ConnectionError:
            print(f"   ⚠️  连接错误 (尝试 {attempt + 1}/{max_retries})")
            if attempt < max_retries - 1:
                smart_delay('error', error_count=attempt + 1)
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                print(f"   ⚠️  请求过于频繁 (429), 延长等待时间 (尝试 {attempt + 1}/{max_retries})")
                if attempt < max_retries - 1:
                    time.sleep(random.uniform(30, 45))  # 更长的延迟
            elif e.response.status_code in [403, 503]:
                print(f"   ⚠️  访问被拒绝 ({e.response.status_code}), 延长等待时间 (尝试 {attempt + 1}/{max_retries})")
                if attempt < max_retries - 1:
                    time.sleep(random.uniform(40, 60))  # 更长的延迟
            else:
                print(f"   ⚠️  HTTP错误: {e} (尝试 {attempt + 1}/{max_retries})")
                if attempt < max_retries - 1:
                    smart_delay('error', error_count=attempt + 1)
        except Exception as e:
            print(f"   ⚠️  未知错误: {e} (尝试 {attempt + 1}/{max_retries})")
            if attempt < max_retries - 1:
                smart_delay('error', error_count=attempt + 1)
    
    return None

def scrape_theme_stocks(theme_url: str, theme_name: str, market: str, 
                       session: requests.Session, referer: Optional[str] = None,
                       max_stock_pages: int = 100) -> List[Dict]:
    """爬取某个主题下的所有股票（支持分页）
    
    优化特性：
    - 检测连续重复页面：当发现连续页面返回相同股票时，自动停止爬取
    - 避免无意义的重复请求，提高爬取效率
    
    Args:
        theme_url: 主题URL
        theme_name: 主题名称
        market: 市场代码
        session: requests会话
        referer: 来源页面
        max_stock_pages: 最大爬取页数（默认100页）
    
    Returns:
        List[Dict]: 股票列表
    """
    all_stocks = []
    consecutive_empty_pages = 0
    max_consecutive_empty = 2  # 连续2页无数据就停止
    previous_page_symbols = set()  # 记录前一页的股票代码
    
    try:
        print(f"      📊 正在爬取主题股票: {theme_name}")
        
        for page in range(1, max_stock_pages + 1):
            # 构建URL
            if page == 1:
                full_url = f"https://www.futunn.com{theme_url}"
            else:
                # 添加分页参数
                separator = '&' if '?' in theme_url else '?'
                full_url = f"https://www.futunn.com{theme_url}{separator}page={page}"
            
            if page == 1:
                print(f"         URL: {full_url}")
            else:
                print(f"         第{page}页: {full_url}")
            
            response = make_request_with_retry(full_url, session, referer=referer)
            
            if not response:
                print(f"      ⚠️  无法访问第{page}页")
                consecutive_empty_pages += 1
                if consecutive_empty_pages >= max_consecutive_empty:
                    break
                continue
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # 多级选择器策略
            stock_items = soup.find_all('a', class_='list-item')
            
            if not stock_items:
                stock_items = soup.find_all('a', class_=lambda x: x and 'list-item' in x)
            
            if not stock_items:
                fix_left_divs = soup.find_all('div', class_='fix-left')
                stock_items = [div.parent for div in fix_left_divs if div.parent.name == 'a']
            
            # 如果当前页没有股票，检查是否应该停止
            if not stock_items:
                consecutive_empty_pages += 1
                if consecutive_empty_pages >= max_consecutive_empty:
                    print(f"      ⏹️  连续{max_consecutive_empty}页无数据，停止爬取")
                    break
                continue
            
            # 重置空页计数器
            consecutive_empty_pages = 0
            
            page_stocks = []
            current_page_symbols = set()  # 当前页的股票代码集合
            
            for item in stock_items:
                try:
                    fix_left = item.find('div', class_='fix-left')
                    if not fix_left:
                        fix_left = item
                    
                    # 提取股票代码
                    code_span = fix_left.find('span', class_='code ellipsis')
                    if not code_span:
                        code_span = fix_left.find('span', class_='code')
                    if not code_span:
                        code_span = fix_left.find('span', class_=lambda x: x and 'code' in x)
                    
                    if not code_span:
                        continue
                    
                    symbol = code_span.get('title', '') or code_span.get_text(strip=True)
                    
                    # 提取股票名称
                    name_span = fix_left.find('span', class_='name ellipsis')
                    if not name_span:
                        name_span = fix_left.find('span', class_='name')
                    if not name_span:
                        name_span = fix_left.find('span', class_=lambda x: x and 'name' in x)
                    
                    if not name_span:
                        continue
                    
                    stock_name = name_span.get('title', '') or name_span.get_text(strip=True)
                    stock_url = item.get('href', '')
                    
                    if symbol and stock_name:
                        symbol_clean = symbol.strip()
                        current_page_symbols.add(symbol_clean)
                        page_stocks.append({
                            'symbol': symbol_clean,
                            'stock_name': stock_name.strip(),
                            'stock_url': stock_url.strip(),
                            'market': market
                        })
                except Exception as e:
                    continue
            
            # 检测页面重复：如果当前页的股票代码与前一页完全相同，说明已经到达数据末尾
            if page > 1 and current_page_symbols and previous_page_symbols:
                if current_page_symbols == previous_page_symbols:
                    print(f"      🔄 第{page}页与第{page-1}页数据完全相同，停止爬取（优化：避免重复请求）")
                    break
            
            if page_stocks:
                all_stocks.extend(page_stocks)
                print(f"      ✅ 第{page}页提取 {len(page_stocks)} 只股票 (累计: {len(all_stocks)})")
            
            # 更新前一页的股票代码集合
            previous_page_symbols = current_page_symbols
            
            # 如果这一页的股票数少于预期（比如少于10只），可能已经到最后一页
            if len(page_stocks) < 10:
                print(f"      ℹ️  第{page}页股票数较少，可能已到最后一页")
                break
            
            # 页面之间添加延迟（除了最后一页）
            if page < max_stock_pages:
                smart_delay('stock_page')
        
        print(f"      ✅ 总共成功提取 {len(all_stocks)} 只股票")
        return all_stocks
        
    except Exception as e:
        print(f"      ❌ 爬取主题股票时出错: {e}")
        return all_stocks  # 返回已爬取的股票

def detect_total_pages(base_url, session):
    """动态检测总页数"""
    print("🔍 正在检测总页数...")
    
    try:
        response = make_request_with_retry(base_url, session)
        if not response:
            print("   ⚠️  无法访问首页，使用默认页数")
            return 10
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        pagination_selectors = [
            '.pagination', '.page-nav', '.pager', '.page-list',
            '.pagination-list', '[class*="page"]', '[class*="pagination"]',
            '[class*="pager"]', '.ant-pagination', '.el-pagination',
            '.pagination-wrapper', '.page-container'
        ]
        
        max_page = 1
        
        for selector in pagination_selectors:
            pagination_elements = soup.select(selector)
            for element in pagination_elements:
                page_links = element.find_all('a')
                for link in page_links:
                    text = link.get_text(strip=True)
                    try:
                        page_num = int(text)
                        max_page = max(max_page, page_num)
                    except ValueError:
                        continue
                
                page_buttons = element.find_all(['span', 'div', 'li'], 
                                               class_=lambda x: x and ('page' in x.lower() or 'num' in x.lower()))
                for button in page_buttons:
                    text = button.get_text(strip=True)
                    try:
                        page_num = int(text)
                        max_page = max(max_page, page_num)
                    except ValueError:
                        continue
        
        # 查找JavaScript中的分页信息
        scripts = soup.find_all('script')
        for script in scripts:
            if script.string:
                patterns = [
                    r'totalPages["\']?\s*[:=]\s*(\d+)',
                    r'total_pages["\']?\s*[:=]\s*(\d+)',
                    r'pageCount["\']?\s*[:=]\s*(\d+)',
                    r'maxPage["\']?\s*[:=]\s*(\d+)',
                    r'lastPage["\']?\s*[:=]\s*(\d+)',
                ]
                
                for pattern in patterns:
                    matches = re.findall(pattern, script.string, re.IGNORECASE)
                    for match in matches:
                        try:
                            page_num = int(match)
                            max_page = max(max_page, page_num)
                        except ValueError:
                            continue
        
        if max_page == 1:
            print("   未找到分页元素，采用保守策略...")
            max_page = 15
        
        print(f"   ✅ 检测到总页数: {max_page}")
        return max_page
        
    except Exception as e:
        print(f"   ❌ 检测页数失败: {e}")
        return 10

def scrape_multiple_pages(market='US', max_consecutive_empty=3, max_pages=None, 
                         scrape_stocks=True, max_themes_with_stocks=None):
    """爬取多页投资主题数据 - 每日全量版
    
    默认爬取所有主题的所有股票
    """
    if market not in MARKETS:
        print(f"❌ 不支持的市场: {market}")
        return []
    
    market_config = MARKETS[market]
    print("=" * 70)
    print(f"富途投资主题爬虫 - {market_config['display_name']} ({market})")
    print(f"模式: 每日全量更新 (保守稳定)")
    print("=" * 70)
    
    base_url = market_config['base_url']
    all_themes_data = []
    
    session = requests.Session()
    
    total_pages = detect_total_pages(base_url, session)
    
    if max_pages:
        total_pages = min(total_pages, max_pages)
        print(f"📊 限制最大爬取页数为: {max_pages}")
    
    print(f"📊 连续空页阈值: {max_consecutive_empty}")
    print(f"📊 爬取股票列表: {'是 (全部主题)' if scrape_stocks else '否'}")
    
    consecutive_empty_pages = 0
    previous_url = None
    themes_with_stocks_count = 0
    
    start_time = time.time()
    
    for page in range(1, total_pages + 1):
        print(f"\n📄 正在爬取第 {page}/{total_pages} 页...")
        
        if page == 1:
            url = base_url
        else:
            url = f"{base_url}?page={page}"
        
        try:
            print(f"   访问URL: {url}")
            response = make_request_with_retry(url, session, referer=previous_url)
            
            if not response:
                print(f"   ❌ 无法获取第 {page} 页")
                consecutive_empty_pages += 1
                if consecutive_empty_pages >= max_consecutive_empty:
                    print(f"   ⏹️  连续 {max_consecutive_empty} 页失败，停止爬取")
                    break
                continue
            
            print("   页面获取成功，正在解析数据...")
            soup = BeautifulSoup(response.content, 'html.parser')
            
            list_items = soup.find_all('a', class_='list-item')
            
            if not list_items:
                print(f"   第 {page} 页没有找到数据")
                consecutive_empty_pages += 1
                if consecutive_empty_pages >= max_consecutive_empty:
                    print(f"   ⏹️  连续 {max_consecutive_empty} 页没有数据，停止爬取")
                    break
                continue
            
            print(f"   找到 {len(list_items)} 个投资主题")
            
            page_themes = []
            for idx, item in enumerate(list_items):
                try:
                    theme_data = extract_theme_from_soup_element(item, market)
                    if theme_data:
                        # 每日全量版：爬取所有主题的股票
                        if scrape_stocks:
                            if max_themes_with_stocks is None or themes_with_stocks_count < max_themes_with_stocks:
                                stocks = scrape_theme_stocks(
                                    theme_url=theme_data['theme_url'],
                                    theme_name=theme_data['theme_name'],
                                    market=market,
                                    session=session,
                                    referer=url
                                )
                                theme_data['stocks'] = stocks
                                theme_data['stock_count'] = len(stocks)
                                themes_with_stocks_count += 1
                                
                                # 主题之间的延迟
                                if idx < len(list_items) - 1:
                                    smart_delay('theme', count=themes_with_stocks_count)
                                
                                # 每10个主题额外延迟
                                if themes_with_stocks_count > 0 and themes_with_stocks_count % 10 == 0:
                                    elapsed = time.time() - start_time
                                    print(f"   🔄 已爬取 {themes_with_stocks_count} 个主题，耗时 {elapsed/60:.1f} 分钟，额外延迟...")
                                    smart_delay('theme_group')
                            else:
                                theme_data['stocks'] = []
                                theme_data['stock_count'] = 0
                        
                        page_themes.append(theme_data)
                except Exception as e:
                    print(f"   ⚠️  提取主题数据时出错: {e}")
                    continue
            
            if page_themes:
                print(f"   ✅ 第 {page} 页提取到 {len(page_themes)} 个投资主题")
                all_themes_data.extend(page_themes)
                consecutive_empty_pages = 0
                previous_url = url
            else:
                consecutive_empty_pages += 1
                if consecutive_empty_pages >= max_consecutive_empty:
                    break
            
            # 页面之间的延迟
            if page < total_pages:
                smart_delay('page', count=page)
            
        except Exception as e:
            print(f"   ❌ 处理第 {page} 页时出错: {e}")
            consecutive_empty_pages += 1
            if consecutive_empty_pages >= max_consecutive_empty:
                break
            smart_delay('error', error_count=1)
    
    session.close()
    
    elapsed_time = time.time() - start_time
    print(f"\n⏱️  {market} 市场爬取完成，耗时: {elapsed_time/60:.1f} 分钟")
    
    return all_themes_data

def extract_theme_from_soup_element(element, market='US'):
    """从BeautifulSoup元素中提取投资主题数据"""
    try:
        theme_data = {'market': market}
        
        name_element = element.find('span', class_='plate-name ellipsis')
        if name_element:
            theme_data['theme_name'] = name_element.get_text(strip=True)
        else:
            return None
        
        change_element = element.find('span', class_='change value direct-up ellipsis')
        theme_data['change_percent'] = change_element.get_text(strip=True) if change_element else ""
        
        value_elements = element.find_all('span', class_='value ellipsis')
        same_count_elements = element.find_all('span', class_='same-count value ellipsis')
        
        if len(value_elements) >= 2:
            theme_data['gainers'] = value_elements[0].get_text(strip=True)
            theme_data['losers'] = value_elements[1].get_text(strip=True)
        else:
            theme_data['gainers'] = ""
            theme_data['losers'] = ""
        
        theme_data['unchanged'] = same_count_elements[0].get_text(strip=True) if same_count_elements else ""
        
        stock_element = element.find('object', class_='stock-name')
        if stock_element:
            stock_link = stock_element.find('a')
            if stock_link:
                theme_data['top_stock'] = stock_link.get_text(strip=True)
                theme_data['top_stock_change'] = stock_link.get('title', '')
            else:
                theme_data['top_stock'] = ""
                theme_data['top_stock_change'] = ""
        else:
            theme_data['top_stock'] = ""
            theme_data['top_stock_change'] = ""
        
        theme_data['theme_url'] = element.get('href', '')
        
        # 提取主题ID
        theme_url = theme_data['theme_url']
        match = re.search(r'-(BK\d+)', theme_url)
        theme_data['theme_id'] = match.group(1) if match else ""
        
        theme_data['scraped_at'] = utcnow().isoformat()  # UTC ISO
        
        return theme_data
        
    except Exception as e:
        return None

def save_data(data, filename_prefix="futu_themes"):
    """保存数据到JSON文件"""
    if not data:
        print("❌ 没有数据可保存")
        return None
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_filename = f"{filename_prefix}_{timestamp}.json"
    
    with open(json_filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    
    print(f"📄 JSON数据已保存到: {json_filename}")
    
    # 数据统计
    total_stocks = sum(len(theme.get('stocks', [])) for theme in data)
    themes_with_stocks = sum(1 for theme in data if theme.get('stocks'))
    
    if total_stocks > 0:
        print(f"   📊 统计信息:")
        print(f"      总主题数: {len(data)}")
        print(f"      包含股票的主题数: {themes_with_stocks}")
        print(f"      总股票数: {total_stocks}")
        print(f"      平均每主题股票数: {total_stocks/themes_with_stocks:.1f}")
    
    return json_filename

def display_results(themes_data, show_stocks=False):
    """显示结果摘要"""
    if not themes_data:
        return
    
    print(f"\n📈 总共提取到 {len(themes_data)} 个投资主题")
    print("-" * 70)
    
    for i, theme in enumerate(themes_data[:5]):
        stock_info = f" [{theme.get('stock_count', 0)} stocks]" if 'stock_count' in theme else ""
        print(f"{i+1}. {theme['theme_name']:<35} {theme['change_percent']:<10}{stock_info}")
        
        if show_stocks and theme.get('stocks'):
            for stock in theme['stocks'][:2]:
                print(f"      - {stock['symbol']}: {stock['stock_name']}")
            if len(theme['stocks']) > 2:
                print(f"      ... 还有 {len(theme['stocks']) - 2} 只股票")
    
    if len(themes_data) > 5:
        print(f"... 还有 {len(themes_data) - 5} 个主题")

def scrape_all_markets(markets_to_scrape=None, max_consecutive_empty=3, 
                      max_pages=None, scrape_stocks=True, max_themes_with_stocks=None):
    """爬取所有或指定市场的投资主题数据"""
    if markets_to_scrape is None:
        markets_to_scrape = list(MARKETS.keys())
    
    all_data = {}
    overall_start = time.time()
    
    for i, market in enumerate(markets_to_scrape):
        print(f"\n{'='*70}")
        print(f"正在爬取第 {i+1}/{len(markets_to_scrape)} 个市场: {market}")
        print(f"{'='*70}")
        
        market_data = scrape_multiple_pages(
            market=market,
            max_consecutive_empty=max_consecutive_empty,
            max_pages=max_pages,
            scrape_stocks=scrape_stocks,
            max_themes_with_stocks=max_themes_with_stocks
        )
        
        all_data[market] = market_data
        
        # 市场之间增加延迟
        if i < len(markets_to_scrape) - 1:
            print(f"\n⏳ 市场间延迟，等待 3-5 秒...")
            time.sleep(random.uniform(3, 5))  # 从10-15秒缩短到3-5秒
    
    total_time = time.time() - overall_start
    print(f"\n{'='*70}")
    print(f"🎉 所有市场爬取完成！总耗时: {total_time/60:.1f} 分钟 ({total_time/3600:.2f} 小时)")
    print(f"{'='*70}")
    
    return all_data

def main():
    """主函数 - 每日全量更新配置"""
    try:
        print("\n" + "="*70)
        print("富途投资主题与股票爬虫 - 每日全量更新版")
        print("Futu Investment Themes & Stocks Scraper - Daily Full Update")
        print("="*70)
        
        # ==================== 配置参数 ====================
        # 每日全量更新配置
        MAX_CONSECUTIVE_EMPTY = 3
        MAX_PAGES = None  # None = 爬取所有页面
        MARKETS_TO_SCRAPE = ['HK', 'US']  # 爬取港股和美股
        
        # 股票爬取配置
        SCRAPE_STOCKS = True  # 爬取股票列表
        MAX_THEMES_WITH_STOCKS = None  # None = 爬取所有主题的股票（全量）
        
        # 显示配置信息
        print(f"\n⚙️  配置参数 (每日全量模式 - 优化版):")
        print(f"   目标市场: {', '.join(MARKETS_TO_SCRAPE)}")
        print(f"   页数限制: {'无限制 (全部页面)' if MAX_PAGES is None else MAX_PAGES}")
        print(f"   爬取股票: {'是 (所有主题)' if SCRAPE_STOCKS else '否'}")
        print(f"   股票分页: 最多100页/主题")
        print(f"   User-Agent池: {len(USER_AGENTS)} 个")
        print(f"   延迟策略 (优化):")
        print(f"      - 基础延迟: 1-2秒 (优化: 缩短到原来的1/3)")
        print(f"      - 页面间: 动态增长 (每5页+0.3秒)")
        print(f"      - 主题间: 动态增长 (每10个主题+0.3秒)")
        print(f"      - 股票分页: 0.5-1秒")
        print(f"      - 每10个主题: 额外3-5秒")
        print(f"      - 市场间: 3-5秒")
        print(f"      - 超时设置: 45秒")
        print(f"      - 最大重试: 5次")
        
        # 预估时间
        print(f"\n⏱️  预估时间 (优化后):")
        print(f"   假设每市场约100个主题，每主题平均30只股票")
        print(f"   每个主题约1.5-3秒（含延迟，已优化）")
        print(f"   预计总时间: 20-30分钟 (原来的1/3)")
        
        print(f"\n{'='*70}")
        input("按Enter键开始爬取，或Ctrl+C取消...")
        print(f"{'='*70}\n")
        
        # 开始爬取
        all_markets_data = scrape_all_markets(
            markets_to_scrape=MARKETS_TO_SCRAPE,
            max_consecutive_empty=MAX_CONSECUTIVE_EMPTY,
            max_pages=MAX_PAGES,
            scrape_stocks=SCRAPE_STOCKS,
            max_themes_with_stocks=MAX_THEMES_WITH_STOCKS
        )
        
        # 处理和保存数据
        for market, themes_data in all_markets_data.items():
            if themes_data:
                print(f"\n{'='*70}")
                print(f"{market} 市场结果")
                print(f"{'='*70}")
                
                display_results(themes_data, show_stocks=SCRAPE_STOCKS)
                
                # 保存市场数据
                print(f"\n💾 正在保存 {market} 市场数据...")
                save_data(themes_data, filename_prefix=f"futu_themes_{market.lower()}_daily")
            else:
                print(f"\n❌ {market} 市场爬取失败")
        
        # 保存合并数据
        all_themes = []
        for market_data in all_markets_data.values():
            all_themes.extend(market_data)
        
        if all_themes:
            print(f"\n💾 正在保存合并数据...")
            save_data(all_themes, filename_prefix="futu_themes_all_markets_daily")
            
            # 最终统计
            total_stocks = sum(len(theme.get('stocks', [])) for theme in all_themes)
            print(f"\n{'='*70}")
            print(f"✅ 每日全量爬取完成!")
            print(f"   总主题数: {len(all_themes)}")
            print(f"   总股票数: {total_stocks}")
            print(f"{'='*70}")
        else:
            print("\n❌ 所有市场爬取失败")
            
    except KeyboardInterrupt:
        print("\n\n⏹️  用户中断了程序")
    except Exception as e:
        print(f"\n❌ 程序运行出错: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
