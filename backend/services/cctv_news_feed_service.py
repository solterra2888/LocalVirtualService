# -*- coding: utf-8 -*-
"""
CCTV News Feed Service — 将 CCTV 新闻联播写入 Feeds 订阅模块

与旧 CCTVNewsService 的区别：
  - 旧服务写入 news 表（已删除）
  - 本服务写入 subscription_content（channel_type='news', channel_identifier='CCTV'）
  - 不触发旧 LLM 分析管线，只负责抓取 + 分发

用户需先在 "Add Feed" 对话框中订阅 CCTV 频道（channel_type='news',
channel_identifier='CCTV'），抓取结果才会出现在他们的 Feeds 列表中。

调度：由 news_feed 任务（_execute_news_feed_task）在同一 tick 中调用，
与 EODHD 共享同一 scheduler 任务类型，不需要新增 task_type。
"""

import re
import time
import random
import requests
from datetime import datetime
from typing import Optional, List, Dict, Any

from backend.database.db_manager import DatabaseManager
from backend.utils.database_config import get_database_config
from backend.utils.datetime_utils import utcnow, day_window_utc
from backend.utils.logger import get_logger
from backend.services.content_adapters.news_adapter import NewsAdapter
from backend.services.subscription_content_service import SubscriptionContentService

logger = get_logger("cctv_news_feed_service")

# BeautifulSoup 延迟导入
try:
    from bs4 import BeautifulSoup
    BS4_AVAILABLE = True
except ImportError:
    BS4_AVAILABLE = False

CCTV_CHANNEL_IDENTIFIER = "CCTV"
CCTV_BASE_URL = "https://tv.cctv.com/lm/xwlb/index.shtml"

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
]


class CCTVNewsFeedService:
    """
    CCTV 新闻联播 Feed 服务。

    职责：
    1. 从 tv.cctv.com 爬取新闻联播文章列表 + 正文
    2. 查询订阅了 CCTV 频道的用户（subscription_list WHERE channel_identifier='CCTV'）
    3. 经 NewsAdapter 归一化后写入 subscription_content
    4. 更新未读计数
    """

    def __init__(self):
        if not BS4_AVAILABLE:
            raise ImportError("BeautifulSoup4 未安装，请运行: pip install beautifulsoup4")

        db_config = get_database_config()
        self.db_manager = DatabaseManager(db_config)
        self.adapter = NewsAdapter()
        self.content_service = SubscriptionContentService()
        self.session = requests.Session()

    # ------------------------------------------------------------------
    # 公开入口
    # ------------------------------------------------------------------

    def fetch_and_distribute(
        self,
        date: Optional[str] = None,
        max_articles: int = 30,
        recent_days: int = 2,
    ) -> Dict[str, Any]:
        """
        抓取 CCTV 新闻并分发给所有订阅了 CCTV 频道的用户。

        Args:
            date:         日期字符串 YYYYMMDD，None 表示根据 recent_days 抓取多天
            max_articles: 最多爬取文章数（防止单次运行时间过长）
            recent_days:  抓取最近几天的数据（默认2天，包含今天）

        Returns:
            执行结果统计 dict
        """
        start_time = time.time()
        logger.info("开始 CCTV News Feed 抓取", date=date, max_articles=max_articles, recent_days=recent_days)

        # 1. 查询 CCTV 订阅用户
        user_ids = self._get_cctv_subscriber_ids()
        if not user_ids:
            logger.info("没有 CCTV 订阅用户，跳过抓取")
            return {
                "success": True,
                "message": "没有 CCTV 订阅用户",
                "articles_fetched": 0,
                "articles_saved": 0,
                "articles_new": 0,
            }

        # 2. 爬取文章列表
        articles = self._scrape_article_list(date=date, max_articles=max_articles, recent_days=recent_days)
        if not articles:
            logger.warning("未抓取到任何 CCTV 文章")
            return {
                "success": True,
                "message": "未抓取到任何文章",
                "articles_fetched": 0,
                "articles_saved": 0,
                "articles_new": 0,
            }

        # 3. 逐篇抓取正文
        for i, article in enumerate(articles):
            content = self._extract_content(article["url"])
            article["content"] = content or ""
            if i < len(articles) - 1:
                time.sleep(random.uniform(1.0, 2.0))

        # 4. 写入 subscription_content
        total_saved = 0
        total_new = 0
        failed = 0
        new_content_ids: list = []

        for article in articles:
            raw = self._to_raw_data(article)
            try:
                content_id, is_new = self.content_service.save_content(
                    channel_type="news",
                    raw_data=raw,
                    user_ids=user_ids,
                )
                total_saved += 1
                if is_new:
                    total_new += 1
                    new_content_ids.append(content_id)
            except Exception as exc:
                failed += 1
                logger.warning(f"保存 CCTV 文章失败: {str(exc)[:100]}", title=article.get("title", "")[:60])

        # 5. 触发新内容的自动打标 (LLM) + 向量化 (派发为持久化 Celery 消息, 避免 scheduler
        # 进程被杀导致永久漏打标; 派发失败由 backfill_untagged_news_task 周期兜底)
        if new_content_ids:
            try:
                from backend.tasks.file_tasks import trigger_auto_news_processing
                trigger_auto_news_processing.apply_async(
                    args=[new_content_ids], kwargs={'delay': 3}, queue='file_processing'
                )
            except Exception as exc:
                logger.warning(
                    f"触发 CCTV News 自动处理派发失败 (不阻断主链路, 由 backfill sweep 兜底): {exc}; "
                    f"content_ids={new_content_ids[:20]}"
                )

        # 6. 更新未读计数
        self._update_unread_counts(user_ids)

        elapsed_ms = int((time.time() - start_time) * 1000)
        result = {
            "success": True,
            "message": f"CCTV: 爬取 {len(articles)} 篇，保存 {total_saved} 篇，新增 {total_new} 篇",
            "articles_fetched": len(articles),
            "articles_saved": total_saved,
            "articles_new": total_new,
            "articles_failed": failed,
            "execution_duration_ms": elapsed_ms,
        }
        logger.info("CCTV News Feed 抓取完成", **result)
        return result

    # ------------------------------------------------------------------
    # 爬取工具
    # ------------------------------------------------------------------

    def _scrape_article_list(
        self,
        date: Optional[str] = None,
        max_articles: int = 30,
        recent_days: int = 2,
    ) -> List[Dict[str, Any]]:
        """
        抓取 CCTV 文章列表。
        支持单日期或日期范围（默认抓取最近N天，提高覆盖率）。
        """
        from datetime import datetime, timedelta

        # 确定要抓取的日期列表
        dates_to_fetch = []
        if date:
            # 指定的单个日期
            dates_to_fetch = [date]
        else:
            # 默认抓取最近 N 天（提高覆盖率）
            today = datetime.now()
            dates_to_fetch = [
                (today - timedelta(days=i)).strftime("%Y%m%d")
                for i in range(recent_days)
            ]

        all_items = []
        seen_urls = set()

        for fetch_date in dates_to_fetch:
            if fetch_date == datetime.now().strftime("%Y%m%d"):
                url = CCTV_BASE_URL
            else:
                url = f"{CCTV_BASE_URL}?date={fetch_date}"

            logger.debug(f"抓取 CCTV 列表: {url}")
            response = self._request(url)
            if not response:
                continue

            soup = BeautifulSoup(response.content, "html.parser")

            for link in soup.find_all("a", href=True):
                href = link.get("href", "")
                text = link.get_text(strip=True)

                # 处理相对路径和绝对路径
                if href.startswith("/"):
                    href = "https://tv.cctv.com" + href
                elif href.startswith("./"):
                    href = "https://tv.cctv.com/lm/xwlb/" + href[2:]
                elif not href.startswith("http"):
                    # 其他相对路径
                    continue

                # 匹配条件：URL 包含 VIDE 或 VIDEO（支持各种大小写和变体）
                # 例如: VIDEajMiSJz3bVQrCMW4ycrY260607, VIDEO52tKHJTSDyMZlCuHDOt260608
                url_upper = href.upper()
                if not (("VIDE" in url_upper or "VIDEO" in url_upper) and "tv.cctv.com" in href):
                    continue

                if not text or len(text) < 2:
                    continue

                if href in seen_urls:
                    continue
                seen_urls.add(href)

                # 清理标题：移除前缀
                title = re.sub(r"\s+", " ", text).strip()
                title = re.sub(r"^(完整版|_完整版_|\[视频\])\s*", "", title).strip()
                if not title:
                    continue

                # 提取日期
                date_match = re.search(r"/(\d{4})/(\d{2})/(\d{2})/", href)
                pub_date = None
                if date_match:
                    y, m, d = date_match.groups()
                    pub_date = f"{y}-{m}-{d}"

                all_items.append({"title": title, "url": href, "date": pub_date})

                if len(all_items) >= max_articles:
                    break

            if len(all_items) >= max_articles:
                break

        logger.info(f"CCTV 列表抓取完成", count=len(all_items), dates=dates_to_fetch)
        return all_items

    def _extract_content(self, url: str) -> Optional[str]:
        """从 CCTV 页面提取正文内容。返回 None 表示该页面无文字内容（如纯视频）。"""
        response = self._request(url)
        if not response:
            return None

        soup = BeautifulSoup(response.content, "html.parser")

        # 策略1: id="content_area" (最常见)
        content_area = soup.find("div", id="content_area")
        if content_area:
            paragraphs = [p.get_text(strip=True) for p in content_area.find_all("p") if p.get_text(strip=True)]
            if paragraphs:
                text = "\n".join(paragraphs)
                if len(text) > 50:  # 确保有实质内容
                    return text

        # 策略2: class="content"
        content_div = soup.find("div", class_="content")
        if content_div:
            paragraphs = [p.get_text(strip=True) for p in content_div.find_all("p") if p.get_text(strip=True)]
            if paragraphs:
                text = "\n".join(paragraphs)
                if len(text) > 50:
                    return text

        # 策略3: 查找包含"主要内容"的区域
        main_content = soup.find(lambda tag: tag.name in ('div', 'section') and tag.get_text(strip=True).startswith('主要内容'))
        if main_content:
            paragraphs = [p.get_text(strip=True) for p in main_content.find_all("p") if p.get_text(strip=True)]
            if paragraphs:
                text = "\n".join(paragraphs)
                if len(text) > 50:
                    return text

        # 策略4: 查找 video_text 或 transcript 相关区域
        for div in soup.find_all("div", class_=lambda x: x and any(k in str(x).lower() for k in ["text", "desc", "intro"])):
            paragraphs = [p.get_text(strip=True) for p in div.find_all("p") if p.get_text(strip=True)]
            if len(paragraphs) > 2:
                text = "\n".join(paragraphs)
                if len(text) > 50:
                    return text

        # 策略5: 查找 article 或 body 相关区域
        for tag in soup.find_all(["div", "article"], class_=lambda x: x and any(k in str(x).lower() for k in ["article", "body", "main"])):
            paragraphs = [p.get_text(strip=True) for p in tag.find_all("p") if p.get_text(strip=True)]
            if len(paragraphs) > 2:
                text = "\n".join(paragraphs)
                if len(text) > 50:
                    return text

        # 策略6: 查找所有包含"央视网消息"的文本块
        text_elements = soup.find_all(string=lambda t: t and "央视网消息" in t)
        if text_elements:
            parent = text_elements[0].find_parent(["div", "section", "article"])
            if parent:
                paragraphs = [p.get_text(strip=True) for p in parent.find_all("p") if p.get_text(strip=True)]
                if paragraphs:
                    text = "\n".join(paragraphs)
                    if len(text) > 50:
                        return text

        # 检查是否是新闻联播视频页面（无文字内容）
        title_elem = soup.find("h1") or soup.find("h2") or soup.find("title")
        title_text = title_elem.get_text(strip=True) if title_elem else ""
        if "新闻联播" in title_text:
            logger.debug(f"新闻联播视频页面，跳过（无文字内容）: {url}")
            return None

        # 没有提取到内容
        logger.debug(f"CCTV 内容提取失败: {url}")
        return None

    def _request(self, url: str, max_retries: int = 3) -> Optional[requests.Response]:
        for attempt in range(max_retries):
            try:
                headers = {
                    "User-Agent": random.choice(USER_AGENTS),
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
                }
                resp = self.session.get(url, headers=headers, timeout=30)
                resp.encoding = resp.apparent_encoding or "utf-8"
                resp.raise_for_status()
                return resp
            except requests.exceptions.RequestException as exc:
                if attempt < max_retries - 1:
                    time.sleep(random.uniform(2.0, 4.0))
                else:
                    logger.warning(f"CCTV 请求失败: {url} — {exc}")
        return None

    # ------------------------------------------------------------------
    # 数据转换 & 订阅查询
    # ------------------------------------------------------------------

    def _to_raw_data(self, article: Dict[str, Any]) -> Dict[str, Any]:
        """将爬取结果转换为 NewsAdapter 能处理的格式。"""
        pub_date = article.get("date")
        published_at = None
        if pub_date:
            try:
                # CCTV 日期为北京时区广播日; 换算为该日北京午夜对应的 aware UTC,
                # 不能直接 +"+00:00" (会把 +8 北京时间误标 UTC, 偏 8h).
                # 例: 北京 2026-07-12 → 2026-07-11T16:00:00+00:00
                published_at = day_window_utc(
                    datetime.strptime(pub_date, "%Y-%m-%d").date(), "Asia/Shanghai"
                )[0].isoformat()
            except ValueError:
                pass

        return {
            "title": article.get("title", ""),
            "content": article.get("content", ""),
            "link": article["url"],
            "source": CCTV_CHANNEL_IDENTIFIER,
            "date": published_at or utcnow().isoformat(),
            "symbols": [],
            "tags": [],
            "sentiment": None,
        }

    def _get_cctv_subscriber_ids(self) -> List[int]:
        """查询订阅了 CCTV 频道的所有用户 ID。"""
        conn = self.db_manager.get_connection()
        if not conn:
            return []
        try:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT user_id FROM subscription_list
                WHERE channel_type = 'news'
                  AND channel_identifier = %s
                  AND enabled = true
                """,
                (CCTV_CHANNEL_IDENTIFIER,),
            )
            rows = cursor.fetchall()
            cursor.close()
            conn.close()
            return [row[0] for row in rows]
        except Exception as exc:
            logger.error(f"查询 CCTV 订阅用户失败: {exc}")
            try:
                conn.close()
            except Exception:
                pass
            return []

    def _update_unread_counts(self, user_ids: List[int]) -> None:
        """批量更新 CCTV 订阅的未读计数。"""
        conn = self.db_manager.get_connection()
        if not conn:
            return
        try:
            cursor = conn.cursor()
            for user_id in user_ids:
                try:
                    cursor.execute(
                        """
                        SELECT id FROM subscription_list
                        WHERE user_id = %s AND channel_type = 'news'
                          AND channel_identifier = %s
                        """,
                        (user_id, CCTV_CHANNEL_IDENTIFIER),
                    )
                    row = cursor.fetchone()
                    if row:
                        cursor.execute(
                            "SELECT update_subscription_unread_count(%s, %s)",
                            (row[0], user_id),
                        )
                except Exception as exc:
                    logger.debug(f"更新 CCTV 未读计数失败: user={user_id}, error={exc}")
            conn.commit()
            cursor.close()
            conn.close()
        except Exception as exc:
            logger.error(f"批量更新 CCTV 未读计数失败: {exc}")
            try:
                conn.close()
            except Exception:
                pass
