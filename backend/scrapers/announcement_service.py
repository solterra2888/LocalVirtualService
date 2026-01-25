# -*- coding: utf-8 -*-
"""
上市公司公告抓取服务
从巨潮资讯网抓取指定上市公司的公告并下载PDF
"""

import os
import requests
import time
import random
import re
from datetime import datetime
from typing import Optional, List, Dict, Any
import logging

# 尝试导入后端logger，如果失败则使用标准logging
try:
    from backend.utils.logger import get_logger
except ImportError:
    def get_logger(name):
        logger = logging.getLogger(name)
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
        return logger

class AnnouncementService:
    """巨潮资讯网公告抓取服务类"""
    
    # 基础URL
    BASE_URL = 'http://www.cninfo.com.cn'
    SEARCH_URL = 'http://www.cninfo.com.cn/new/information/topSearch/query'
    QUERY_URL = 'http://www.cninfo.com.cn/new/hisAnnouncement/query'
    STATIC_URL = 'http://static.cninfo.com.cn/'
    
    # 用户代理池
    USER_AGENTS = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
    ]
    
    def __init__(self, download_dir: str = "./download"):
        """
        初始化服务
        
        Args:
            download_dir: PDF下载目录
        """
        self.logger = get_logger("announcement_service")
        self.session = requests.Session()
        self.download_dir = download_dir
        
        # 确保下载目录存在
        if not os.path.exists(self.download_dir):
            os.makedirs(self.download_dir)
            self.logger.info(f"创建下载目录: {self.download_dir}")
            
        self.logger.info("公告抓取服务初始化完成")

    def fetch_and_download(
        self,
        stock_code: str,
        start_date: str,
        end_date: str
    ) -> Dict[str, Any]:
        """
        抓取并下载指定股票和日期范围的公告
        
        Args:
            stock_code: 股票代码 (e.g., "000753")
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            
        Returns:
            Dict[str, Any]: 执行结果
        """
        try:
            self.logger.info(f"开始抓取任务: 代码={stock_code}, 时间={start_date}~{end_date}")
            
            # 步骤1: 获取orgId
            org_id = self._get_org_id(stock_code)
            if not org_id:
                return {
                    'success': False,
                    'message': f"未找到股票代码 {stock_code} 对应的orgId",
                    'downloaded_count': 0
                }
            
            # 推断板块 column
            column = self._get_column_by_org_id(org_id)
            self.logger.info(f"获取到 orgId: {org_id}, 板块: {column}")
            
            # 步骤2: 分页获取公告列表并下载
            page_num = 1
            total_downloaded = 0
            has_more = True
            
            while has_more:
                self.logger.info(f"正在抓取第 {page_num} 页...")
                announcements, total_pages = self._get_announcement_list(
                    stock_code, org_id, column, page_num, start_date, end_date
                )
                
                if not announcements:
                    self.logger.info("当前页无数据或请求失败")
                    break
                
                # 下载本页公告
                for ann in announcements:
                    if self._download_announcement(ann):
                        total_downloaded += 1
                    # 随机延迟防止被封
                    self._smart_delay(0.5, 1.5)
                
                if page_num >= total_pages:
                    has_more = False
                else:
                    page_num += 1
                    self._smart_delay(1.0, 2.0)
            
            return {
                'success': True,
                'message': f"任务完成，共下载 {total_downloaded} 个文件",
                'downloaded_count': total_downloaded
            }
            
        except Exception as e:
            error_msg = f"任务执行失败: {str(e)}"
            self.logger.error(error_msg, exception=str(e))
            return {
                'success': False,
                'message': error_msg,
                'downloaded_count': 0
            }

    def _get_org_id(self, stock_code: str) -> Optional[str]:
        """获取股票对应的orgId"""
        try:
            params = {
                "keyWord": stock_code,
                "maxNum": 10
            }
            response = self._make_request(self.SEARCH_URL, method='POST', data=params)
            if not response:
                return None
                
            data = response.json()
            for item in data:
                if item.get('code') == stock_code:
                    return item.get('orgId')
            return None
        except Exception as e:
            self.logger.error(f"获取orgId失败: {str(e)}")
            return None

    def _get_column_by_org_id(self, org_id: str) -> str:
        """根据orgId推断板块"""
        if org_id.startswith('gssz'):
            return 'szse'
        elif org_id.startswith('gssh'):
            return 'sse'
        elif org_id.startswith('gsbj'):
            return 'bse' # 北京证券交易所?
        return 'szse' # 默认

    def _get_announcement_list(
        self,
        stock_code: str,
        org_id: str,
        column: str,
        page_num: int,
        start_date: str,
        end_date: str
    ) -> tuple[List[Dict], int]:
        """获取公告列表"""
        try:
            payload = {
                "pageNum": page_num,
                "pageSize": 30,
                "column": column,
                "tabName": "fulltext",
                "plate": "",
                "stock": f"{stock_code},{org_id}",
                "searchkey": "",
                "secid": "",
                "category": "",
                "trade": "",
                "seDate": f"{start_date}~{end_date}",
                "sortName": "",
                "sortType": "",
                "isHLtitle": "true"
            }
            
            response = self._make_request(self.QUERY_URL, method='POST', data=payload)
            if not response:
                return [], 0
                
            data = response.json()
            announcements = data.get('announcements', [])
            total_pages = data.get('totalpages', 0)
            
            return announcements, total_pages
            
        except Exception as e:
            self.logger.error(f"获取公告列表失败: {str(e)}")
            return [], 0

    def _download_announcement(self, ann: Dict) -> bool:
        """下载单个公告PDF"""
        try:
            title = ann.get('announcementTitle', 'unknown')
            # 清理标题中的非法字符
            title = re.sub(r'[\\/:*?"<>|]', '_', title)
            # 去除HTML标签（如果有）
            title = re.sub(r'<[^>]+>', '', title).strip()
            
            adjunct_url = ann.get('adjunctUrl')
            if not adjunct_url:
                self.logger.warning(f"公告 [{title}] 无下载链接")
                return False
                
            file_url = self.STATIC_URL + adjunct_url
            date_timestamp = ann.get('announcementTime', 0) / 1000
            date_str = datetime.fromtimestamp(date_timestamp).strftime('%Y-%m-%d')
            
            # 构建文件名: 日期_标题.pdf
            file_name = f"{date_str}_{title}.pdf"
            file_path = os.path.join(self.download_dir, file_name)
            
            if os.path.exists(file_path):
                self.logger.info(f"文件已存在，跳过: {file_name}")
                return True
                
            self.logger.info(f"正在下载: {file_name}")
            
            # 下载文件
            response = self.session.get(file_url, stream=True, headers=self._get_random_headers())
            if response.status_code == 200:
                with open(file_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                return True
            else:
                self.logger.error(f"下载失败: {file_url}, 状态码: {response.status_code}")
                return False
                
        except Exception as e:
            self.logger.error(f"下载公告出错: {str(e)}")
            return False

    def _make_request(self, url: str, method: str = 'GET', data: Optional[Dict] = None) -> Optional[requests.Response]:
        """发送HTTP请求（带重试）"""
        retries = 3
        for i in range(retries):
            try:
                headers = self._get_random_headers()
                if method == 'POST':
                    response = self.session.post(url, data=data, headers=headers, timeout=10)
                else:
                    response = self.session.get(url, headers=headers, timeout=10)
                
                if response.status_code == 200:
                    return response
                
                self.logger.warning(f"请求失败 (尝试 {i+1}/{retries}): {url}, 状态码: {response.status_code}")
                time.sleep(1)
                
            except Exception as e:
                self.logger.warning(f"请求异常 (尝试 {i+1}/{retries}): {str(e)}")
                time.sleep(1)
        
        return None

    def _get_random_headers(self) -> Dict[str, str]:
        """生成随机请求头"""
        return {
            'User-Agent': random.choice(self.USER_AGENTS),
            'X-Requested-With': 'XMLHttpRequest'
        }

    def _smart_delay(self, min_s: float, max_s: float):
        """随机延迟"""
        time.sleep(random.uniform(min_s, max_s))

# 主程序入口（用于测试）
if __name__ == "__main__":
    service = AnnouncementService()
    
    # 测试输入: 上市公司代码000753, 时间范围为近2个月
    stock_code = "000753"
    from datetime import datetime, timedelta
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=60)
    
    s_date_str = start_date.strftime('%Y-%m-%d')
    e_date_str = end_date.strftime('%Y-%m-%d')
    
    print(f"测试启动: 抓取 {stock_code} 从 {s_date_str} 到 {e_date_str} 的公告")
    service.fetch_and_download(stock_code, s_date_str, e_date_str)
