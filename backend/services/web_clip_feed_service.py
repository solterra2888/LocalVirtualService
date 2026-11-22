# -*- coding: utf-8 -*-
"""
Web Clip → subscription_content 转投服务

Chrome 扩展 (GuanHe Clipper) 剪藏的 Bloomberg/Reuters 等高质量新闻源全文，
除照常归档 OSS + 文件模块外，额外转投 subscription_content(channel_type='news')，
让晨报 (build_snapshot JOIN Admin 订阅行) / Daily / Agent / 向量化生态直接消费
E1 级一手全文 —— 补上 wire 层 (Yahoo 二手转载 + 1200 截断) 的结构性短板。

调用方:
  - backend/tasks/file_tasks.py process_web_clip_task (新剪藏实时转投)
  - scripts/backfill_web_clips_to_subscription.py (存量回填)

subscription_content 契约 (与 news_adapter / _save_normalized_content 对齐):
  - content_id = 规范化 URL (VARCHAR(255), 唯一键 (channel_type, content_id))
  - author_name/author_identifier = SOURCE_MAPPING 显示名 ('Bloomberg'/'Reuters')
  - published_at = naive UTC (DB 连接级 TimeZone=UTC 契约)
  - upsert: 同 URL 长正文胜出、user_ids 合并; 仅 is_new 触发打标/向量化

清洗规则 (源自 2026-11-20/21 共 242 条真实剪藏的实测污染分布):
  - 防爬不可见字符 (31% 条目): ZWNJ/ZWSP/ZWJD/WJ/FEFF/SHY 全剥
  - UI 样板 (30-45% 条目): opens new tab / Purchase Licensing Rights /
    Trust Principles / newsletter 促销 / cookie 提示 / Subscribe now
  - 标题截断 (~10% Bloomberg): 正文首行是完整标题，前缀命中即回填
"""

import hashlib
import logging
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse, urlunparse

logger = logging.getLogger(__name__)

# 允许转投的域名白名单 (env 覆盖, 逗号分隔)。其余剪藏保持文件模块原行为。
DEFAULT_FEED_DOMAINS = 'bloomberg.com,reuters.com'

# 与 subscription_content 去重键对齐的 content_id 长度上限
CONTENT_ID_MAX = 255

# 证据级确定性评级: 记者署名 + URL 可核验的一手全文 (INFO_GRADING_STANDARD E 轴)
WEB_CLIP_EVIDENCE_LEVEL = 'E1'
TAG_SCHEMA_WEB_CLIP_V1 = 'web_clip_v1'

# 防爬不可见字符: ZWSP/ZWNJ/ZWJD/Word-Joiner/BOM/Soft-Hyphen
_INVISIBLE_CHARS = dict.fromkeys(map(ord, '\u200b\u200c\u200d\u2060\ufeff\u00ad'), None)

# 整行丢弃的样板 (search 语义: 模式自带锚定; 免责声明按关键短语子串命中)
_LINE_DROP_PATTERNS = [
    re.compile(r'^opens new tab\.?,?$'),
    re.compile(r'^Purchase Licensing Rights\.?$'),
    re.compile(r'^Our Standards: The Thomson Reuters Trust Principles\.?$'),
    re.compile(r"^If you can'?t see the content of video posts, please adjust your cookie settings\.?$"),
    re.compile(r'^Subscribe now\.?$'),
    re.compile(r'^Updated on$'),
    # Reuters 评论栏目免责声明 (多行段落, 逐行子串命中)
    re.compile(r'do not reflect the views of Reuters News'),
    re.compile(r'^Trust Principles, is committed to integrity'),
    re.compile(r'opinions expressed here are those of the author', re.IGNORECASE),
    # 订阅促销整行
    re.compile(r'full access to Reuters\.com'),
    re.compile(r'for just \$1/week'),
]

# 行内替换的样板碎片 (Reuters 链接锚文本残留)
_INLINE_REPLACEMENTS: List[Tuple[re.Pattern, str]] = [
    (re.compile(r',?\s*opens new tab'), ''),
    (re.compile(r'\s*,?\s*Purchase Licensing Rights'), ''),
]

# 正文日期行: "November 20, 2026 · Julien Ponthus · Bloomberg" (无时刻)
_DATELINE_RE = re.compile(
    r'^(January|February|March|April|May|June|July|August|September|October|November|December)'
    r'\s+(\d{1,2}),\s+(\d{4})\b'
)
# Bloomberg 渲染时区行: "November 20, 2026 at 3:57 PM GMT+8"
_DATETIME_TZ_RE = re.compile(
    r'^(January|February|March|April|May|June|July|August|September|October|November|December)'
    r'\s+(\d{1,2}),\s+(\d{4})\s+at\s+(\d{1,2}):(\d{2})\s*(AM|PM)\s+GMT([+-]\d{1,2})',
    re.IGNORECASE,
)

_MONTHS = {m: i + 1 for i, m in enumerate(
    ['January', 'February', 'March', 'April', 'May', 'June', 'July',
     'August', 'September', 'October', 'November', 'December'])}

# 无时刻日期行的缺省锚点: 美东正午 (美东 = UTC-5/-4, 取 -5 保守居中, 不落窗口边界)
_DEFAULT_ET_OFFSET_HOURS = -5

# URL slug → 可读标题的词形映射 (Bloomberg 页面级截断时 slug 是唯一完整描述)
_LOWERCASE_WORDS = {'a', 'an', 'the', 'of', 'in', 'on', 'for', 'to', 'and', 'with',
                    'at', 'by', 'from', 'as', 'is', 'its', 'or', 'be', 'are'}
_ACRONYM_WORDS = {
    'ai': 'AI', 'ipo': 'IPO', 'uk': 'UK', 'us': 'US', 'eu': 'EU', 'fed': 'Fed',
    'sec': 'SEC', 'gdp': 'GDP', 'cpi': 'CPI', 'fomc': 'FOMC', 'ecb': 'ECB',
    'boj': 'BOJ', 'boe': 'BOE', 'imf': 'IMF', 'etf': 'ETF', 'etfs': 'ETFs',
    'reit': 'REIT', 'reits': 'REITs', 'nyse': 'NYSE', 'ftse': 'FTSE', 'opec': 'OPEC',
    'saas': 'SaaS', 'mbs': 'MBS', 'cmbs': 'CMBS', 'doj': 'DOJ', 'ftc': 'FTC',
    'cftc': 'CFTC', 'svb': 'SVB', 'hsbc': 'HSBC', 'ubs': 'UBS', 'bp': 'BP',
    'rbc': 'RBC', 'at1': 'AT1', 'otc': 'OTC', 'fca': 'FCA', 'gop': 'GOP', 'ira': 'IRA',
}


def feed_domains() -> List[str]:
    """转投域名白名单 (env WEB_CLIP_FEED_DOMAINS 覆盖)"""
    raw = os.getenv('WEB_CLIP_FEED_DOMAINS', DEFAULT_FEED_DOMAINS)
    return [d.strip().lower() for d in raw.split(',') if d.strip()]


def match_feed_domain(url: str) -> Optional[str]:
    """URL 命中白名单域名时返回该域名 (子域安全), 否则 None"""
    try:
        host = (urlparse(url).netloc or '').lower()
    except Exception:
        return None
    if not host:
        return None
    if host.startswith('www.'):
        host = host[4:]
    for domain in feed_domains():
        if host == domain or host.endswith('.' + domain):
            return domain
    return None


def strip_invisible(text: str) -> str:
    """剥离防爬不可见字符 (ZWNJ/ZWSP/WJ 等, 会污染 LLM 分词与数字对账)"""
    return text.translate(_INVISIBLE_CHARS)


def _is_promo_line(line: str) -> bool:
    """newsletter/订阅促销独立行: 依赖 newsletter+sign up 共现或固定开头"""
    if len(line) > 250:
        return False
    if line.startswith('Get the latest ') and line.endswith('newsletter'):
        return True
    low = line.lower()
    return 'newsletter' in low and ('sign up' in low or 'subscribe' in low)


def clean_clip_text(text: str) -> str:
    """清洗剪藏纯文本: 不可见字符 → 行内样板 → 整行样板 → 压缩空行"""
    if not text:
        return ''
    text = strip_invisible(text)
    kept: List[str] = []
    for raw_line in text.splitlines():
        line = raw_line
        for pattern, repl in _INLINE_REPLACEMENTS:
            line = pattern.sub(repl, line)
        line = line.strip()
        if not line:
            kept.append('')
            continue
        if any(p.search(line) for p in _LINE_DROP_PATTERNS):
            continue
        if _is_promo_line(line):
            continue
        kept.append(line)
    # 压缩 3+ 连续空行为 1
    cleaned = re.sub(r'\n{3,}', '\n\n', '\n'.join(kept))
    return cleaned.strip('\n')


def first_headline(clean_text: str) -> str:
    """清洗后正文首行 = Readability 完整标题 (剪藏结构: 标题 / Source: / 日期行…)"""
    for line in clean_text.splitlines():
        line = line.strip()
        if line:
            return line
    return ''


def _slug_title(url: str) -> str:
    """URL slug → 可读标题 (Bloomberg 页面级截断时 slug 是唯一完整描述);
    剥 Reuters 尾部 -yyyy-mm-dd 日期 token。"""
    slug = urlparse(url).path.rstrip('/').rsplit('/', 1)[-1]
    if not slug or '-' not in slug:
        return ''
    tokens = [t for t in slug.split('-') if t]
    if len(tokens) >= 3 and tokens[-1].isdigit() and tokens[-2].isdigit() \
            and len(tokens[-3]) == 4 and tokens[-3].isdigit():
        tokens = tokens[:-3]
    if len(tokens) < 4:
        return ''
    parts = []
    for i, tok in enumerate(tokens):
        if tok in _ACRONYM_WORDS:
            parts.append(_ACRONYM_WORDS[tok])
        elif any(c.isdigit() for c in tok):
            parts.append(tok)
        elif i > 0 and tok in _LOWERCASE_WORDS:
            parts.append(tok)
        else:
            parts.append(tok[:1].upper() + tok[1:])
    return ' '.join(parts)


def recover_title(title: str, clean_text: str, url: str = '') -> str:
    """截断标题回填 (2026-11 实测: 页面 <title> 与 Readability H1 同源同截):
    ① 正文首行是完整标题且 title 是其前缀 → 用首行
    ② 仍为残根 (<len 25, 如 "Cash"/"Tiger"/"K") 且 URL slug 可读 → slug 标题
    """
    headline = first_headline(clean_text)
    title = (title or '').strip()
    if not title:
        return headline
    if headline and title != headline:
        t_core = title.rstrip('…-.… ')
        if headline.startswith(t_core) and len(t_core) >= 8:
            return headline
        if len(title) < 25 and len(headline) > len(title) * 1.6:
            return headline
    if len(title) < 25 and url:
        slug_t = _slug_title(url)
        if slug_t:
            return slug_t
    return title


def _to_naive_utc(dt: datetime) -> datetime:
    """aware → naive UTC (DB timestamp without time zone 契约)"""
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def parse_published_at(
    clean_text: str,
    published_time: Optional[str] = None,
    clip_time: Optional[datetime] = None,
) -> datetime:
    """发布时间三级来源 → naive UTC:
    ① 扩展传的 published_time (Readability/meta, ISO 串)
    ② 正文时区行 "… at 3:57 PM GMT+8" → 日期行 "Month D, YYYY ·…" (12:00 ET)
    ③ 剪藏时刻兜底 (同日, 略保守)
    """
    if published_time:
        try:
            from dateutil import parser as du_parser
            return _to_naive_utc(du_parser.parse(published_time))
        except ImportError:
            # 系统 python 无 dateutil 时退 fromisoformat (guanhe 环境两者皆有)
            try:
                return _to_naive_utc(datetime.fromisoformat(published_time.strip().replace('Z', '+00:00')))
            except ValueError:
                pass
        except Exception:
            pass

    for line in clean_text.splitlines()[:8]:
        line = line.strip()
        m = _DATETIME_TZ_RE.match(line)
        if m:
            month, day, year = _MONTHS[m.group(1)], int(m.group(2)), int(m.group(3))
            hour, minute = int(m.group(4)), int(m.group(5))
            if m.group(6).upper() == 'PM' and hour != 12:
                hour += 12
            if m.group(6).upper() == 'AM' and hour == 12:
                hour = 0
            offset = int(m.group(7))
            try:
                return datetime(year, month, day, hour, minute) - timedelta(hours=offset)
            except ValueError:
                continue

    for line in clean_text.splitlines()[:8]:
        m = _DATELINE_RE.match(line.strip())
        if m:
            try:
                base = datetime(int(m.group(3)), _MONTHS[m.group(1)], int(m.group(2)), 12)
                return base - timedelta(hours=_DEFAULT_ET_OFFSET_HOURS)
            except ValueError:
                continue

    if clip_time is not None:
        return _to_naive_utc(clip_time)
    from backend.utils.datetime_utils import utcnow_naive
    return utcnow_naive()


def normalize_url(url: str) -> str:
    """content_id 用规范化 URL: 去 query/fragment; 超长回退 domain+sha1"""
    try:
        parts = urlparse(url.strip())
        path = parts.path or ''
        if not path.endswith('/') and '.' not in path.rsplit('/', 1)[-1]:
            path += '/'  # 目录型路径统一尾斜杠, 与 Reuters 原始形态一致
        normalized = urlunparse((parts.scheme, parts.netloc, path, '', '', ''))
    except Exception:
        normalized = url.strip()
    if len(normalized) <= CONTENT_ID_MAX:
        return normalized
    host = urlparse(normalized).netloc or 'clip'
    return f"{host}:sha1:{hashlib.sha1(url.encode('utf-8')).hexdigest()}"


def extract_source_name(url: str) -> str:
    """域名 → 订阅流显示名 (复用 news_adapter.SOURCE_MAPPING, 如 Bloomberg/Reuters)"""
    from backend.services.content_adapters.news_adapter import extract_source_from_url
    return extract_source_from_url(url)


def _write_evidence_tag(content_pk: int, channel_type: str, tagged_by: Optional[int]) -> None:
    """确定性写 E 级标签 (E1 一手全文); 不动已有行 (ON CONFLICT DO NOTHING),
    info_level 留空由 LLM 管线或后续 NEWS_LLM_TAG_SOURCES 决定。"""
    from backend.database.db_manager import DatabaseManager
    from backend.utils.database_config import get_database_config
    from psycopg2.extras import Json

    db = DatabaseManager(get_database_config())
    conn = db.get_connection()
    if not conn:
        raise ValueError("无法连接到数据库")
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO subscription_content_tags (
                subscription_content_id, sector, sector_en, industry, industry_en,
                ticker, concepts, people, countries, additional_tags,
                confidence, reasoning, summary,
                channel_type, tag_schema_version,
                info_level, evidence_level, tagged_by, tagged_at
            ) VALUES (
                %s, '未分类', '', '未分类', '',
                NULL, '[]'::jsonb, '[]'::jsonb, '[]'::jsonb, '{}'::jsonb,
                NULL, NULL, NULL,
                %s, %s,
                NULL, %s, %s, CURRENT_TIMESTAMP
            )
            ON CONFLICT (subscription_content_id) DO NOTHING
            """,
            (content_pk, channel_type, TAG_SCHEMA_WEB_CLIP_V1,
             WEB_CLIP_EVIDENCE_LEVEL, tagged_by),
        )
        conn.commit()
        cursor.close()
    finally:
        conn.close()


def forward_clip_to_feed(
    source_url: str,
    title: str,
    plain_text: str,
    user_ids: List[int],
    published_time: Optional[str] = None,
    clip_time: Optional[datetime] = None,
    trigger_processing: bool = True,
) -> Optional[Dict[str, Any]]:
    """剪藏 → subscription_content('news') 转投入口。

    Args:
        source_url: 原文 URL (白名单域名才转投)
        title: 扩展上报标题 (可能截断, 会用正文首行回填)
        plain_text: process_web_clip_task 提取的纯文本
        user_ids: 订阅者 (含剪藏者 uid)
        published_time: 扩展上报的 ISO 发布时间 (可缺省, 走正文解析)
        clip_time: 剪藏时刻 (发布时间兜底)
        trigger_processing: is_new 时是否派发向量化/自动处理 (celery)

    Returns:
        转投结果 dict {db_id, is_new, source, title, published_at, content_len};
        域名不在白名单或正文为空 → None (调用方视为"无需转投", 非错误)。
    """
    if not match_feed_domain(source_url):
        return None
    clean = clean_clip_text(plain_text or '')
    if len(clean) < 100:
        logger.warning("web clip 转投跳过: 清洗后正文过短 url=%s len=%d", source_url, len(clean))
        return None

    final_title = recover_title(title or '', clean, source_url)
    published_at = parse_published_at(clean, published_time, clip_time)
    source_name = extract_source_name(source_url)
    content_id = normalize_url(source_url)

    normalized = {
        'channel_type': 'news',
        'content_id': content_id,
        'title': final_title,
        'content': clean,
        'author_name': source_name,
        'author_identifier': source_name,
        'url': source_url,
        'media_urls': [],
        'published_at': published_at,
        'engagement_count': 0,
        'view_count': 0,
        'metadata': {
            'news_source': 'chrome_clip',
            'sentiment': None,
            'symbols': [],
            'api_tags': [],
        },
        'categories': [],
    }

    from backend.services.subscription_content_service import SubscriptionContentService
    svc = SubscriptionContentService()
    db_id, is_new = svc._save_normalized_content(normalized_data=normalized, user_ids=user_ids)

    tagged_by = user_ids[0] if user_ids else None
    try:
        _write_evidence_tag(db_id, 'news', tagged_by)
    except Exception as e:  # 标签失败不阻断转投 (无 tags 行 = 未评级, 下游放行)
        logger.error("web clip E1 标签写入失败 db_id=%s: %s", db_id, e)

    if is_new and trigger_processing:
        try:
            from backend.tasks.file_tasks import trigger_auto_news_processing
            trigger_auto_news_processing.apply_async(args=([db_id],), countdown=3)
        except Exception as e:
            logger.error("web clip 触发自动处理失败 db_id=%s: %s", db_id, e)
        # L 级 LLM 打标: tag_feed_content_task 对"已有标签行"默认 skip (幂等门),
        # 而上面的确定性 E1 行总是先落 → 必须 force=True 才能覆盖出完整 L/E。
        # 仅当源在 NEWS_LLM_TAG_SOURCES 时派发 (与 trigger 路由同一开关)。
        llm_sources = {
            s.strip().lower()
            for s in os.getenv('NEWS_LLM_TAG_SOURCES', 'CCTV').split(',') if s.strip()
        }
        if source_name.lower() in llm_sources:
            try:
                from backend.tasks.feed_tagging_tasks import tag_feed_content_task
                tag_feed_content_task.apply_async(args=(db_id, None, '', True), countdown=5)
            except Exception as e:
                logger.error("web clip LLM 打标派发失败 db_id=%s: %s", db_id, e)

    logger.info(
        "web clip 转投%s: db_id=%s source=%s published_at=%s len=%d url=%s",
        '(new)' if is_new else '(update)', db_id, source_name,
        published_at.isoformat(), len(clean), content_id,
    )
    return {
        'db_id': db_id,
        'is_new': is_new,
        'source': source_name,
        'title': final_title,
        'published_at': published_at,
        'content_len': len(clean),
    }
