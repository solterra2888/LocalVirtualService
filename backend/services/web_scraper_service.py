# -*- coding: utf-8 -*-
"""
网页抓取与正文提取服务

两阶段抓取策略：
1. httpx 轻量抓取 → readability-lxml 提取正文
2. 若正文过短（JS 渲染页面），自动 fallback 到 Playwright headless 浏览器

功能等同于 Chrome Extension 的 Readability.js + background.js 图片下载逻辑，
但运行在服务端，支持任意设备通过提交 URL 触发。
"""

import base64
import re
import httpx
from html import escape as html_escape
from urllib.parse import urljoin, urlparse

from readability import Document
from bs4 import BeautifulSoup

from backend.utils.logger import info, warning, error

FETCH_TIMEOUT = 30
IMAGE_TIMEOUT = 15
MAX_IMAGE_SIZE = 10 * 1024 * 1024  # 10 MB per image
MAX_IMAGES = 80
MAX_HTML_LENGTH = 20_000_000  # 20 MB

# 正文纯文本低于此阈值视为"内容不足"，触发 Playwright fallback
# 注意：标题+日期+元数据通常就有 200-400 字符，所以阈值必须高于此
MIN_CONTENT_LENGTH = 500

PLAYWRIGHT_TIMEOUT = 30_000  # 30s for initial page load
PLAYWRIGHT_CONTENT_WAIT = 15_000  # max 15s waiting for article content to appear
PLAYWRIGHT_CF_WAIT = 15_000  # max 15s waiting for Cloudflare challenge to resolve

ARTICLE_SELECTORS = [
    "article",
    "[data-test-id='content-container']",
    ".article-body",
    ".post-content",
    "main article",
    "[role='main'] article",
    ".entry-content",
    "#article-body",
    ".caas-body",
    ".story-body",
]

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7",
}


class WebScraperService:
    """服务端网页抓取 + Readability 正文提取 + 图片 base64 内嵌"""


    @staticmethod
    def validate_url(url: str) -> bool:
        parsed = urlparse(url)
        return parsed.scheme in ("http", "https") and bool(parsed.netloc)

    # ── Stage 1: httpx 轻量抓取 ──────────────────────────────────────────────

    @staticmethod
    def fetch_page(url: str) -> str:
        """同步 httpx 抓取（快速，但拿不到 JS 渲染内容）"""
        with httpx.Client(
            headers=DEFAULT_HEADERS,
            timeout=FETCH_TIMEOUT,
            follow_redirects=True,
            max_redirects=5,
        ) as client:
            resp = client.get(url)
            resp.raise_for_status()
            return resp.text

    # ── Stage 2: Playwright headless 浏览器 fallback ─────────────────────────

    @staticmethod
    def fetch_page_with_browser(url: str) -> str:
        """使用 Playwright headless Chromium 抓取完整 JS 渲染后的 HTML。

        策略：
        1. domcontentloaded 快速加载（不卡在 networkidle）
        2. 检测 Cloudflare 挑战页 → 等待挑战自动解决
        3. 等待文章内容 DOM 元素出现（而非固定延时）
        4. 反检测：移除 webdriver 标记、禁用自动化特征
        """
        from playwright.sync_api import sync_playwright

        info(f"Playwright fallback: loading {url}", source="web_scraper")
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                ],
            )
            context = browser.new_context(
                user_agent=DEFAULT_HEADERS["User-Agent"],
                locale="en-US",
                viewport={"width": 1280, "height": 900},
            )
            page = context.new_page()

            # Anti-detection: remove navigator.webdriver flag
            page.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3]});
                window.chrome = {runtime: {}};
            """)

            try:
                page.goto(url, wait_until="domcontentloaded", timeout=PLAYWRIGHT_TIMEOUT)

                # Phase 1: Cloudflare challenge detection & wait
                title = page.title().lower()
                if "just a moment" in title or "checking" in title or "security" in title:
                    info("Cloudflare challenge detected, waiting for resolution...", source="web_scraper")
                    try:
                        page.wait_for_function(
                            """() => {
                                const t = document.title.toLowerCase();
                                return !t.includes('just a moment')
                                    && !t.includes('checking your browser')
                                    && !t.includes('security check');
                            }""",
                            timeout=PLAYWRIGHT_CF_WAIT,
                        )
                        info("Cloudflare challenge resolved", source="web_scraper")
                        page.wait_for_timeout(2000)
                    except Exception:
                        warning("Cloudflare challenge did not resolve in time", source="web_scraper")

                # Phase 2: wait for article content to appear in DOM
                found_selector = None
                for selector in ARTICLE_SELECTORS:
                    try:
                        page.wait_for_selector(selector, timeout=3000)
                        found_selector = selector
                        info(f"Article content appeared (selector: {selector})", source="web_scraper")
                        break
                    except Exception:
                        continue

                if found_selector:
                    page.wait_for_timeout(1500)
                else:
                    # No known selector found; progressive wait until body has meaningful text
                    info("No article selector matched, using progressive content wait", source="web_scraper")
                    for i in range(5):
                        page.wait_for_timeout(2000)
                        body_len = page.evaluate("document.body?.innerText?.length || 0")
                        if body_len > MIN_CONTENT_LENGTH:
                            info(f"Body text reached {body_len} chars after {(i+1)*2}s", source="web_scraper")
                            break

                # Phase 3: scroll to trigger lazy-loaded content
                page.evaluate("window.scrollTo(0, document.body.scrollHeight / 3)")
                page.wait_for_timeout(1500)
                page.evaluate("window.scrollTo(0, document.body.scrollHeight * 2 / 3)")
                page.wait_for_timeout(1000)

                html = page.content()
            finally:
                context.close()
                browser.close()
        info(f"Playwright fetched {len(html)} chars from {url}", source="web_scraper")
        return html

    # ── Smart fetch: httpx first, Playwright if content too short ────────────

    @staticmethod
    def smart_fetch(url: str, on_status=None) -> tuple[str, str, bool]:
        """
        两阶段抓取。返回 (title, article_html, used_playwright)。
        on_status(msg) 可选回调，用于更新 Celery 进度消息。
        若最终内容被检测为反爬/登录墙页面，抛出 ValueError。
        """
        httpx_article: dict | None = None
        httpx_plain_len = 0

        # Stage 1: httpx（快速尝试，失败不致命）
        httpx_raw_html: str = ""
        if on_status:
            on_status("Fetching page (lightweight)...")
        try:
            httpx_raw_html = WebScraperService.fetch_page(url)
            httpx_article = WebScraperService.extract_article(httpx_raw_html, url)
            httpx_plain_len = len(WebScraperService._quick_text_length(httpx_article["html"]))

            if httpx_plain_len >= MIN_CONTENT_LENGTH:
                # 检测是否为反爬页面
                blocked = WebScraperService.detect_blocked_page(
                    httpx_article["title"],
                    WebScraperService._quick_text_length(httpx_article["html"]),
                    url,
                )
                if blocked:
                    warning(f"httpx got blocked page: {blocked}, falling back to Playwright", source="web_scraper")
                else:
                    # 检查原始 HTML 中是否有 JS 加载 / 付费墙指示符
                    raw_issue = WebScraperService._raw_html_needs_browser(httpx_raw_html, httpx_plain_len)
                    if raw_issue:
                        warning(
                            f"httpx content looks incomplete ({raw_issue}), falling back to Playwright: {url}",
                            source="web_scraper"
                        )
                    else:
                        info(f"httpx fetch sufficient: {httpx_plain_len} chars from {url}", source="web_scraper")
                        return httpx_article["title"], httpx_article["html"], False

            if not httpx_plain_len >= MIN_CONTENT_LENGTH:
                # 也检查原始 HTML 的 JS/paywall 指示符以确认需要 Playwright
                raw_issue = WebScraperService._raw_html_needs_browser(httpx_raw_html, httpx_plain_len)
                reason = raw_issue or f"content too short ({httpx_plain_len} chars)"
                warning(f"httpx: {reason}, falling back to Playwright: {url}", source="web_scraper")
        except Exception as exc:
            warning(f"httpx fetch failed ({exc}), falling back to Playwright: {url}", source="web_scraper")

        # Stage 2: Playwright fallback（httpx 失败或内容不足时走此分支）
        if on_status:
            on_status("Using headless browser to load page...")
        try:
            browser_html = WebScraperService.fetch_page_with_browser(url)
            browser_article = WebScraperService.extract_article(browser_html, url)
            browser_plain_len = len(WebScraperService._quick_text_length(browser_article["html"]))

            # 选择较优结果
            if browser_plain_len > httpx_plain_len:
                chosen_title = browser_article["title"]
                chosen_html = browser_article["html"]
                used_pw = True
                info(
                    f"Playwright got better content: {browser_plain_len} chars (vs httpx {httpx_plain_len})",
                    source="web_scraper"
                )
            elif len(browser_html) > 10000 and browser_plain_len >= httpx_plain_len:
                chosen_title = browser_article["title"]
                chosen_html = browser_article["html"]
                used_pw = True
                info(
                    f"Playwright HTML much larger ({len(browser_html)} chars) but extraction similar "
                    f"({browser_plain_len} vs {httpx_plain_len}), using Playwright result",
                    source="web_scraper"
                )
            elif httpx_article:
                chosen_title = httpx_article["title"]
                chosen_html = httpx_article["html"]
                used_pw = False
                info(
                    f"Playwright did not improve ({browser_plain_len} vs {httpx_plain_len}), using httpx result",
                    source="web_scraper"
                )
            else:
                chosen_title = browser_article["title"]
                chosen_html = browser_article["html"]
                used_pw = True

            # ── 最终内容质量校验：检测反爬 / 登录墙 / 付费墙 ──
            chosen_plain = WebScraperService._quick_text_length(chosen_html)
            chosen_plain_len = len(chosen_plain)
            blocked = WebScraperService.detect_blocked_page(chosen_title, chosen_plain, url)

            # 提取文本中检测不到时，再检查原始 HTML（付费墙文本可能在提取块之外）
            if not blocked:
                best_raw = browser_html if used_pw else httpx_raw_html
                raw_issue = WebScraperService._raw_html_needs_browser(best_raw, chosen_plain_len)
                if raw_issue and chosen_plain_len < 1500:
                    blocked = (
                        "The page appears to show only a preview/summary behind a paywall. "
                        "The full article requires login or subscription."
                    )

            if blocked:
                warning(f"Final content is a blocked/paywalled page: {blocked}", source="web_scraper")
                raise ValueError(
                    f"Unable to fetch full article content. {blocked} "
                    f"Please use the Web Clipper Chrome Extension which runs in your "
                    f"logged-in browser and can bypass these restrictions."
                )

            # 清洁提取的 HTML（移除噪音 DOM 元素，对齐 Chrome Extension 的 noiseSelectors）
            chosen_html = WebScraperService.clean_article_html(chosen_html)

            return chosen_title, chosen_html, used_pw

        except ValueError:
            raise
        except Exception as exc:
            if httpx_article:
                # httpx 有结果但也可能是反爬页面，再检查一下
                blocked = WebScraperService.detect_blocked_page(
                    httpx_article["title"],
                    WebScraperService._quick_text_length(httpx_article["html"]),
                    url,
                )
                if blocked:
                    raise ValueError(
                        f"Unable to fetch article content. {blocked} "
                        f"Please use the Web Clipper Chrome Extension which runs in your "
                        f"logged-in browser and can bypass these restrictions."
                    )
                warning(f"Playwright fallback also failed: {exc}, using httpx result", source="web_scraper")
                return httpx_article["title"], httpx_article["html"], False
            raise ValueError(
                f"Unable to fetch this page. The site likely requires login/subscription "
                f"or has anti-scraping protection. "
                f"Please use the Web Clipper Chrome Extension which runs in your "
                f"logged-in browser and can bypass these restrictions."
            )

    # ── 正文提取 ─────────────────────────────────────────────────────────────

    @staticmethod
    def extract_article(html: str, url: str) -> dict:
        """
        使用 readability-lxml 提取正文，readability 不足时自动 fallback 到
        BeautifulSoup 最大内容块搜索。

        Returns:
            {"title": str, "html": str}
        """
        # 预处理：移除噪音 DOM 元素（对齐 Chrome Extension 的 noiseSelectors 预处理）
        pre_soup = BeautifulSoup(html, "lxml")
        for selector in WebScraperService._NOISE_CSS_SELECTORS:
            for el in pre_soup.select(selector):
                el.decompose()
        for tag_name in ["video", "iframe", "svg"]:
            for el in pre_soup.find_all(tag_name):
                el.decompose()
        cleaned_html_for_readability = str(pre_soup)

        doc = Document(cleaned_html_for_readability, url=url)
        title = doc.short_title() or doc.title() or ""
        content_html = doc.summary(html_partial=True)

        readability_text_len = len(re.sub(r'<[^>]+>', '', content_html or '').strip())

        # readability 提取的纯文本太短（但原始 HTML 很大），说明 readability 解析失败
        need_fallback = (
            (not content_html or readability_text_len < MIN_CONTENT_LENGTH)
            and len(html) > 5000
        )

        if need_fallback:
            info(
                f"Readability extracted only {readability_text_len} chars from {len(html)} chars HTML, "
                f"trying BeautifulSoup fallback",
                source="web_scraper"
            )
            fallback_html = WebScraperService._bs_extract_best_block(cleaned_html_for_readability)
            fallback_text_len = len(re.sub(r'<[^>]+>', '', fallback_html).strip())

            if fallback_text_len > readability_text_len:
                info(
                    f"BeautifulSoup fallback improved: {fallback_text_len} chars (vs readability {readability_text_len})",
                    source="web_scraper"
                )
                content_html = fallback_html

        if not content_html or len(content_html.strip()) < 50:
            soup = BeautifulSoup(cleaned_html_for_readability, "lxml")
            for tag in soup.find_all(["script", "style"]):
                tag.decompose()
            main = (
                soup.find("article")
                or soup.find("main")
                or soup.find(attrs={"role": "main"})
                or soup.find("body")
            )
            content_html = str(main) if main else ""

        if not title:
            title_soup = BeautifulSoup(html, "lxml")
            title_tag = title_soup.find("title")
            title = title_tag.get_text(strip=True) if title_tag else urlparse(url).netloc

        title = re.sub(r'\s*[-|–—·]\s*[^-|–—·]{1,30}$', '', title).strip()

        return {"title": title, "html": content_html}

    @staticmethod
    def _bs_extract_best_block(html: str) -> str:
        """从大型 HTML 中找到文本内容最多的内容块（readability 失败时的 fallback）。

        策略：
        1. 按已知内容选择器查找（article, [data-test-id], .article-body 等）
        2. 在所有 div/section 中搜索文本最多且 text/link 比率高的块
        """
        soup = BeautifulSoup(html, "lxml")
        for tag in soup.find_all(["script", "style", "noscript", "iframe"]):
            tag.decompose()

        best_block = None
        best_text_len = 0

        # Phase 1: known content selectors
        content_attrs = [
            {"data-test-id": "content-container"},
            {"data-testid": "article-body"},
            {"class_": "article-body"},
            {"class_": "post-content"},
            {"class_": "entry-content"},
            {"class_": "story-body"},
            {"class_": "caas-body"},
            {"id": "article-body"},
        ]
        for attrs in content_attrs:
            el = soup.find(attrs=attrs)
            if el:
                text_len = len(el.get_text(strip=True))
                if text_len > best_text_len:
                    best_block = el
                    best_text_len = text_len

        # Phase 2: semantic elements
        for tag_name in ["article", "main"]:
            for el in soup.find_all(tag_name):
                text_len = len(el.get_text(strip=True))
                if text_len > best_text_len:
                    best_block = el
                    best_text_len = text_len

        # Phase 3: largest div/section with high text-to-link ratio
        if best_text_len < MIN_CONTENT_LENGTH:
            for el in soup.find_all(["div", "section"]):
                text = el.get_text(strip=True)
                text_len = len(text)
                if text_len <= best_text_len or text_len < MIN_CONTENT_LENGTH:
                    continue
                links = el.find_all("a")
                link_text = sum(len(a.get_text(strip=True)) for a in links)
                if text_len > 0 and link_text / text_len > 0.6:
                    continue
                best_block = el
                best_text_len = text_len

        return str(best_block) if best_block else ""

    # ── 提取后 HTML 清洁（与 Chrome Extension noiseSelectors 对齐）────────

    # 与 Chrome Extension popup.js 中 noiseSelectors 对齐的噪音 CSS 选择器
    _NOISE_CSS_SELECTORS = [
        "nav", "footer", "aside", "header",
        "[role='banner']", "[role='navigation']", "[role='complementary']",
        # Seeking Alpha specific
        "[data-test-id='right-rail']",
        "[data-test-id='recommended-section']",
        "[data-test-id='recommended-for-you-card-body']",
        "[data-test-id='trending-card-container']",
        "[data-test-id='trending-news-cards-container']",
        "[data-test-id='comments-section']",
        "[data-test-id='suggested-symbols-card']",
        "[data-test-id='about-symbol-card']",
        "[data-test-id='post-footer']",
        "[data-test-id='action-block-top']",
        # Generic noise classes/ids
        ".signup_widget_placeholder",
        ".sidebar", ".comments", ".related", ".recommended", ".trending",
        ".ad", ".advertisement", ".share-buttons", ".social-share",
        "#comments", ".comment-section", "#more-links",
    ]

    # 纯文本级别的噪音行模式（逐行检查，完全匹配或短文本匹配时移除）
    _NOISE_LINE_EXACT = {
        "copy link", "save", "share", "print", "comments",
        "see more »", "see more", "see all analysis »", "see all news »",
        "compare", "compare to peers", "related stocks", "trending analysis",
        "trending news", "recommended for you", "more trending news",
        "chart", "end of interactive chart.", "view as data table, chart",
        "eps", "pe", "div rate", "yield", "short interest", "market cap", "volume",
        "full screen", "fullscreen", "画中画", "全屏(f)", "关闭广告",
        "正在加载", "播放视频", "sign up", "quick insights",
        "copy link", "play", "about frph stock",
    }

    _NOISE_LINE_PATTERNS = [
        re.compile(r'^(1D|5D|1M|6M|1Y|5Y|10Y)$'),
        re.compile(r'^Stock Chart Icon', re.IGNORECASE),
        re.compile(r'^Created with Highcharts'),
        re.compile(r'^The chart has \d+ [XY] axis'),
        re.compile(r'^Data ranges from'),
        re.compile(r'^Chart with \d+ data points'),
        re.compile(r'^Follow us on Google', re.IGNORECASE),
        re.compile(r'^Follow Seeking Alpha on Google', re.IGNORECASE),
        re.compile(r'^Sign up for our weekly newsletter', re.IGNORECASE),
        re.compile(r'^Subscribe to \w', re.IGNORECASE),
        re.compile(r'^Newsletters? for Every', re.IGNORECASE),
        re.compile(r'^Smarter \w+ Starts Here', re.IGNORECASE),
        re.compile(r'^Tap into new \w+ opportunities', re.IGNORECASE),
        re.compile(r'^Get daily.*newsletters', re.IGNORECASE),
        re.compile(r'^Sign Up$', re.IGNORECASE),
        re.compile(r'^Disclaimer$', re.IGNORECASE),
        re.compile(r'^广告$'),
        re.compile(r'^ADCountdown'),
        re.compile(r'^尊贵的用户'),
        re.compile(r'^您上次观看'),
        re.compile(r'^播放\(p\)'),
        re.compile(r'^静音\(m\)'),
        re.compile(r'^时长\s+\d+:\d+'),
        re.compile(r'^SymbolLast Price'),
        re.compile(r'^[A-Z]{2,5}\d+\.\d+[-+]?\d+\.?\d*%$'),
        re.compile(r"^Seeking Alpha'?s Disclaimer", re.IGNORECASE),
        re.compile(r"^This article was automatically generated by an AI tool", re.IGNORECASE),
        re.compile(r'^Quick Insights$', re.IGNORECASE),
        re.compile(r'^See All \w+ »$', re.IGNORECASE),
        re.compile(r'^About [A-Z]{1,5} Stock$', re.IGNORECASE),
        re.compile(r'^Post\.\s*\d+\.\d+', re.IGNORECASE),
        re.compile(r'^Play\(\d+min\)$', re.IGNORECASE),
        re.compile(r'^Copy Link$', re.IGNORECASE),
        re.compile(r'^正在加载\s*视频播放器'),
        re.compile(r'Post\.\s*\d+\.\d+[-+]?\d*\.?\d*%$'),
        re.compile(r'^AI-Generated\s', re.IGNORECASE),
        re.compile(r'^Read full article', re.IGNORECASE),
    ]

    @staticmethod
    def clean_article_html(article_html: str) -> str:
        """从提取的文章 HTML 中移除噪音 DOM 元素（导航、广告、侧边栏、股票小部件等）。

        对齐 Chrome Extension 的 noiseSelectors 预处理逻辑。
        """
        soup = BeautifulSoup(article_html, "lxml")

        # 移除匹配噪音选择器的元素
        for selector in WebScraperService._NOISE_CSS_SELECTORS:
            for el in soup.select(selector):
                el.decompose()

        # 移除视频播放器、iframe、脚本、表单
        for tag_name in ["video", "iframe", "script", "style", "form", "svg"]:
            for el in soup.find_all(tag_name):
                el.decompose()

        # 移除隐藏元素
        for el in soup.find_all(attrs={"aria-hidden": "true"}):
            el.decompose()
        for el in soup.find_all(style=re.compile(r'display:\s*none', re.IGNORECASE)):
            el.decompose()

        return str(soup)

    @staticmethod
    def clean_plain_text(text: str) -> str:
        """从纯文本中移除噪音行（UI 按钮文本、图表元素、广告、播放器控件等）。"""
        lines = text.splitlines()
        cleaned = []
        for line in lines:
            stripped = line.strip()
            if not stripped:
                continue
            if stripped.lower() in WebScraperService._NOISE_LINE_EXACT:
                continue
            if any(p.match(stripped) for p in WebScraperService._NOISE_LINE_PATTERNS):
                continue
            # 非常短的行（≤3字符）如果是纯数字或符号则跳过
            if len(stripped) <= 3 and not any(c.isalpha() for c in stripped):
                continue
            cleaned.append(stripped)

        return '\n'.join(cleaned)

    # ── 图片内嵌 ─────────────────────────────────────────────────────────────

    @staticmethod
    def inline_images(article_html: str, base_url: str, on_progress=None) -> str:
        """
        下载文章中的 <img> 图片并替换为 base64 data URL。
        跳过已是 data: 的图片、下载失败的图片。
        """
        soup = BeautifulSoup(article_html, "lxml")
        imgs = soup.find_all("img")
        if not imgs:
            return article_html

        seen_urls: set[str] = set()
        download_list: list[tuple] = []
        for img in imgs:
            src = img.get("src") or img.get("data-src") or ""
            if not src or src.startswith("data:"):
                continue
            abs_url = urljoin(base_url, src)
            if abs_url in seen_urls:
                continue
            seen_urls.add(abs_url)
            download_list.append((img, abs_url))

        if not download_list:
            return str(soup)

        total = min(len(download_list), MAX_IMAGES)
        downloaded = 0
        html_str = str(soup)

        with httpx.Client(
            headers={"User-Agent": DEFAULT_HEADERS["User-Agent"]},
            timeout=IMAGE_TIMEOUT,
            follow_redirects=True,
        ) as client:
            for img_tag, img_url in download_list[:MAX_IMAGES]:
                try:
                    resp = client.get(img_url)
                    resp.raise_for_status()
                    if len(resp.content) > MAX_IMAGE_SIZE:
                        warning(f"Image too large, skipping: {img_url}", source="web_scraper")
                        continue
                    content_type = resp.headers.get("content-type", "image/jpeg").split(";")[0].strip()
                    if not content_type.startswith("image/"):
                        content_type = "image/jpeg"
                    b64 = base64.b64encode(resp.content).decode("ascii")
                    data_url = f"data:{content_type};base64,{b64}"
                    original_src = img_tag.get("src") or img_tag.get("data-src") or ""
                    if original_src:
                        html_str = html_str.replace(original_src, data_url)
                    downloaded += 1
                    if on_progress:
                        on_progress(downloaded, total)
                except Exception as exc:
                    warning(f"Failed to download image {img_url}: {exc}", source="web_scraper")
                    continue

        info(f"Inlined {downloaded}/{total} images from {base_url}", source="web_scraper")
        return html_str

    # ── HTML 构建与文本提取 ──────────────────────────────────────────────────

    @staticmethod
    def build_full_html(title: str, source_url: str, article_html: str) -> str:
        escaped_title = html_escape(title)
        escaped_url = html_escape(source_url)
        return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{escaped_title}</title>
<meta name="source-url" content="{escaped_url}">
<style>
html {{ color-scheme: light dark; }}
body {{ max-width: 860px; margin: 0 auto; padding: 24px; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; line-height: 1.7; color: #333; background: #fff; }}
img {{ max-width: 100%; height: auto; border-radius: 4px; }}
a {{ color: #1677ff; }}
@media (prefers-color-scheme: dark) {{
  body {{ background: #141414; color: #e8eaed; }}
  a {{ color: #69b1ff; }}
  .web-clip-source {{ color: #a0a3ad !important; }}
}}
</style>
</head>
<body>
<h1>{escaped_title}</h1>
<p class="web-clip-source" style="font-size:12px;color:#999;margin-bottom:16px;">
  Source: <a href="{escaped_url}" target="_blank" rel="noopener noreferrer">{escaped_url}</a>
</p>
{article_html}
</body>
</html>"""

    @staticmethod
    def html_to_plain_text(html_content: str) -> str:
        from html import unescape
        text = re.sub(r'<style[^>]*>.*?</style>', '', html_content, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r'<script[^>]*>.*?</script>', '', text, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'</(?:p|div|h[1-6]|li|tr|blockquote|section|article)>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'<[^>]+>', '', text)
        text = unescape(text)
        lines = [line.strip() for line in text.splitlines()]
        plain_text = '\n'.join(line for line in lines if line)
        # 纯文本级噪音清洁
        plain_text = WebScraperService.clean_plain_text(plain_text)
        if len(plain_text) > 500_000:
            plain_text = plain_text[:500_000]
        return plain_text

    @staticmethod
    def _quick_text_length(html: str) -> str:
        """快速从 HTML 提取纯文本（用于判断内容量，不做完整清理）"""
        text = re.sub(r'<[^>]+>', '', html)
        return text.strip()

    # JS 加载指示符：这些在原始 HTML 中出现说明正文需要 JS 渲染
    _JS_LOADING_INDICATORS = [
        "正在加载",
        "loading...",
        "please wait while",
        "content is loading",
        "document.write",
        "window.__INITIAL_STATE__",
        "window.__NEXT_DATA__",
        "react-root",
        "app-root",
    ]

    # 原始 HTML 中的付费墙特征（可能不在 readability 提取结果中）
    _RAW_HTML_PAYWALL_INDICATORS = [
        "subscribe to newsletters",
        "newsletters for every investor",
        "create free account",
        "sign up to unlock",
        "paywall",
        "premium-content",
        "subscriber-only",
        "metered-content",
        "registration-wall",
    ]

    @staticmethod
    def _raw_html_needs_browser(raw_html: str, extracted_text_len: int) -> str | None:
        """检查原始 HTML 是否存在 JS 加载或付费墙特征，需要浏览器渲染。

        Returns:
            None = HTML 看起来正常; str = 需要浏览器渲染的原因
        """
        html_lower = raw_html.lower()

        for indicator in WebScraperService._JS_LOADING_INDICATORS:
            if indicator in html_lower:
                # "react-root" 等 JS 框架标记只在提取内容很短时才触发 fallback
                if indicator in ("react-root", "app-root", "window.__NEXT_DATA__",
                                 "window.__INITIAL_STATE__", "document.write"):
                    if extracted_text_len < 1000:
                        return f"JS framework detected ({indicator}) with short content"
                else:
                    return f"JS loading indicator found: {indicator}"

        if extracted_text_len < 1500:
            for indicator in WebScraperService._RAW_HTML_PAYWALL_INDICATORS:
                if indicator in html_lower:
                    return f"Paywall indicator in raw HTML: {indicator}"

        return None

    # ── 内容质量校验 ─────────────────────────────────────────────────────────

    # Cloudflare / 反爬 / 登录墙的特征短语（title 或 body 纯文本中出现即判定为非真实内容）
    _BLOCKED_PAGE_SIGNATURES = [
        "just a moment",
        "checking your browser",
        "security verification",
        "verifies you are not a bot",
        "enable javascript and cookies to continue",
        "access denied",
        "please verify you are a human",
        "ray id",
        "attention required",
        "one more step",
        "please complete the security check",
        "are you a robot",
    ]

    _LOGIN_WALL_SIGNATURES = [
        "sign in to continue",
        "log in to continue",
        "create a free account",
        "subscribe to read",
        "this content is for subscribers",
        "you need to be logged in",
        "register to continue reading",
        "subscribe to newsletters",
        "sign up to unlock",
        "become a premium member",
        "premium content",
        "start your free trial",
        "unlock this article",
        "continue reading with a subscription",
        # Seeking Alpha — silent "Quick Insights" paywall preview (shown to logged-out users)
        "quick insights",
    ]

    @staticmethod
    def detect_blocked_page(title: str, plain_text: str, url: str = "") -> str | None:
        """检测抓取到的页面是否为反爬/登录墙/付费墙页面，而非真正的文章内容。

        Returns:
            None 表示正常页面；否则返回描述被阻断原因的字符串。
        """
        combined = (title + " " + plain_text).lower()
        text_len = len(plain_text.strip())

        # Phase 1: 反爬/Cloudflare 挑战页 —— 无论内容长度都判定为阻断
        for sig in WebScraperService._BLOCKED_PAGE_SIGNATURES:
            if sig in combined:
                return (
                    "The page returned an anti-bot challenge instead of article content. "
                    "This site has anti-scraping protection that blocks server-side access."
                )

        # Phase 2: 登录/订阅墙 —— 仅在内容较短时判定（长内容中侧边栏可能含这些词）
        if text_len < 2500:
            for sig in WebScraperService._LOGIN_WALL_SIGNATURES:
                if sig in combined:
                    return (
                        "The page requires login or subscription to view the full content."
                    )

        # Phase 3: 付费墙部分展示 —— 内容很短 + 多个付费提示词同时出现
        if text_len < 1500:
            paywall_hints = [
                "sign up", "subscribe", "create account", "free trial",
                "premium", "member", "unlock", "continue reading",
                "newsletters for every", "read full", "start your",
                "become a", "join now",
            ]
            hint_count = sum(1 for h in paywall_hints if h in combined)
            if hint_count >= 2:
                return (
                    "The page appears to show only a preview/summary behind a paywall. "
                    "The full article requires login or subscription."
                )

        return None
