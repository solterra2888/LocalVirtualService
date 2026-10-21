# -*- coding: utf-8 -*-
"""
财报电话会 AI Recap 服务（Earnings Calendar 模块）

数据源链（2026-10 实测）:
    1. Motley Fool — 免费网页, 个股 quote 页内嵌该 ticker 的 transcript 历史 URL 列表,
       文章页无付费墙; 人工编辑质量高, 覆盖 ~1000-1500 篇/季（美股为主）
    2. AlphaVantage — EARNINGS_CALL_TRANSCRIPT 端点; 免费 key 实测所有 ticker/季度均返回
       空数组（端点可调、无数据）, 保留该通道以兼容将来升级付费 key

流程: 用户点击获取 → 发现最新 transcript → 拉全文 → llm_service 生成 Markdown Recap
      → 存 earnings_call_recaps 永久缓存（同公司同季只消耗一次抓取）
"""

import asyncio
import calendar
import html as ihtml
import os
import re
import time
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional

import requests

from backend.core.llm_framework import llm_service, LLMMessage
from backend.database.db_manager import DatabaseManager
from backend.utils.logger import get_logger
from backend.utils.database_config import get_database_config

logger = get_logger("earnings_recap_service")

MF_HEADERS = {
    'User-Agent': ('Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 '
                   '(KHTML, like Gecko) Chrome/120.0 Safari/537.36'),
    'Accept-Language': 'en-US,en;q=0.9',
}
MF_QUOTE_URL = 'https://www.fool.com/quote/{exchange}/{ticker}/'
AV_TRANSCRIPT_URL = 'https://www.alphavantage.co/query'

# Yahoo Finance earnings call transcript（免费层=最新几季全文, 历史存档付费——Recap 只取最新场, 免费层够用）
YAHOO_EARNINGS_URL = 'https://finance.yahoo.com/quote/{ticker}/earnings'
YAHOO_LINK_RE = re.compile(r'([A-Z0-9.\-]{1,10})-Q(\d{1,2})-(20\d{2})-earnings_call-(\d+)\.html')
YAHOO_CHROME_PATH = '/root/.cache/ms-playwright/chromium-1208/chrome-linux64/chrome'


def _quarter_rank(period_key: str):
    """'q2-2026' → (2026, 2); 无季度标记的 key 排最低（用于 MF/Yahoo 目标择优）"""
    m = re.match(r'q(\d{1,2})-(20\d{2})$', period_key or '')
    return (int(m.group(2)), int(m.group(1))) if m else (0, 0)

# EODHD 交易所代码 → Motley Fool quote 页路径段
EXCHANGE_MAP = {
    'XNYS': 'nyse', 'XNAS': 'nasdaq', 'XASE': 'amex', 'ARCX': 'nyse',
    'NYSE': 'nyse', 'NASDAQ': 'nasdaq', 'AMEX': 'amex', 'ASE': 'amex',
}
EXCHANGE_FALLBACKS = ['nasdaq', 'nyse', 'amex']

# AlphaVantage 免费档 25 次/天, 留余量
AV_DAILY_LIMIT = 20
AV_NEGATIVE_TTL_HOURS = 24 * 7  # 无数据结果 7 天后才重试

# 个股 quote 页体积大(~700KB), 进程内 15 分钟缓存避免重复拉取
QUOTE_CACHE_TTL_SEC = 900

# transcript 落后最近已公布财报超过该天数 → 标记 stale（漏了一个完整季度以上）
STALE_THRESHOLD_DAYS = 45

TRANSCRIPT_MAX_CHARS = 90000

# Recap 生成模型: qwen3-max 等推理型默认模型会把 max_tokens 耗在思考上, 用稳定别名 + 大额度
RECAP_MODEL = os.getenv('EARNINGS_RECAP_MODEL', 'qwen-max')
RECAP_MAX_TOKENS = 6000

RECAP_SYSTEM_PROMPT = (
    '你是一名严谨的美股卖方分析师助理, 擅长阅读财报电话会(earnings call)实录并提炼要点。'
    '请基于用户提供的 transcript 全文生成一份结构化 Markdown 回顾(Recap), 要求:\n'
    '1. 只使用 transcript 中出现的信息, 不编造数字; 数字保留原文口径\n'
    '2. 使用中文, ticker/财务术语(如 EPS/EBITDA/guidance)保留英文\n'
    '3. 按以下结构输出(用 ### 小标题):\n'
    '   ### 一句话总结\n'
    '   ### 关键数字 (本季营收/EPS 及同比、指引 guidance 变化——transcript 有才写)\n'
    '   ### 管理层要点 (3-5 条)\n'
    '   ### Q&A 焦点 (分析师最关心的 2-3 个问题与管理层回应)\n'
    '   ### 风险与关注点\n'
    '   ### 电话会基调 (Bullish / Neutral / Bearish + 一句理由)\n'
    '4. 总长度控制在 500 字以内, 宁缺毋滥'
)


class EarningsRecapService:
    """财报电话会 AI Recap: 按需抓取 + 永久缓存"""

    def __init__(self):
        self.db_manager = DatabaseManager(get_database_config())
        # ticker -> (fetch_ts, [transcript entries]) 进程内缓存
        self._quote_cache: Dict[str, tuple] = {}

    # ── 对外主入口 ────────────────────────────────────────────────

    async def get_or_generate(self, code: str) -> Dict[str, Any]:
        """
        获取或生成某 ticker 最新一期电话会的 AI Recap。

        Returns:
            {status: ok|no_transcript|unsupported|error|quota_exceeded, recap_md, ...}
        """
        code = (code or '').strip()
        if not code:
            return {'status': 'error', 'message': 'code 不能为空'}

        # 目前两个数据源都只覆盖美股
        if '.' in code:
            return {
                'status': 'unsupported',
                'code': code,
                'message': '目前仅支持美股的财报电话会 Recap',
            }
        bare = code

        # 1) 数据源择优: Motley Fool(快, 纯 HTTP) → 缺失或落后时 Yahoo Finance 补位(覆盖全, 需渲染)
        try:
            target = await asyncio.to_thread(self._find_mf_transcript, code)
            if target:
                target['source'] = 'motley_fool'

            # MF 没有、或 MF 的最新一季已落后最近财报 → 问 Yahoo 要更新的
            if (not target) or target.get('stale'):
                yh_ref = await asyncio.to_thread(self._find_yahoo_transcript, code)
                if yh_ref and ((not target) or
                               _quarter_rank(yh_ref['period_key']) > _quarter_rank(target['period_key'])):
                    target = yh_ref

            if target:
                cached = self._load_cached(code, target['period_key'])
                if cached:
                    return cached
                if target['source'] == 'yahoo_finance':
                    text = await asyncio.to_thread(self._fetch_yahoo_article_text, target['url'])
                else:
                    try:
                        text = await asyncio.to_thread(self._fetch_mf_article_text, target['url'])
                    except Exception as e:
                        logger.error(f"MF 文章抓取失败: {code} {target['url']}: {e}")
                        text = ''
                if text:
                    return await self._generate_and_save(code, target['source'], target, text)

            # 2) AlphaVantage 兜底（免费 key 实测无数据, 付费后自动生效）
            av = await asyncio.to_thread(self._try_alphavantage, code)
            if av:
                cached = self._load_cached(code, av['period_key'])
                if cached:
                    return cached
                if av.get('quota_exceeded'):
                    return {'status': 'quota_exceeded', 'code': code,
                            'message': '今日 AlphaVantage 免费额度已用完, 请明天再试'}
                if av.get('text'):
                    return await self._generate_and_save(code, 'alphavantage', av, av['text'])
        except Exception as e:
            logger.error(f"Recap 生成失败: {code}: {e}")
            return {'status': 'error', 'code': code,
                    'message': f'Recap 生成失败: {str(e)[:200]}'}

        # 3) 两边都没有 → 负缓存 24h
        self._save_negative(code)
        return {
            'status': 'no_transcript',
            'code': code,
            'message': '未找到该公司的财报电话会 transcript（Motley Fool 未覆盖或刚公布尚未上线）',
        }

    # ── Motley Fool: 发现 + 抓取 ──────────────────────────────────

    def _find_mf_transcript(self, code: str) -> Optional[Dict[str, Any]]:
        """从个股 quote 页解析 transcript 列表, 取最新一篇; 数据源漏季时附 stale 标记"""
        entries = self._quote_cache_get(code)
        if entries is None:
            entries = []
            for exchange in self._exchange_candidates(code):
                html = self._http_get(MF_QUOTE_URL.format(exchange=exchange, ticker=code.lower()))
                if not html:
                    continue
                entries = self._parse_quote_transcripts(html)
                if entries:
                    self._quote_cache_set(code, entries)
                    break
            if not entries:
                self._quote_cache_set(code, [])
                return None

        if not entries:
            return None

        from datetime import date as _date
        today = _date.today()
        valid = [e for e in entries if e['date'] <= today]
        target = max(valid or entries, key=lambda e: e['date'])

        report_date = self._latest_report_date(code)
        if report_date and (report_date - target['date']).days > STALE_THRESHOLD_DAYS:
            target = dict(target)
            target['stale'] = True
            target['latest_report_date'] = report_date
        return target

    def _exchange_candidates(self, code: str) -> List[str]:
        """优先用 companies.exchange, 其次通用顺序"""
        candidates: List[str] = []
        try:
            with self.db_manager.get_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT exchange FROM companies WHERE code = %s LIMIT 1", (code,))
                row = cursor.fetchone()
                cursor.close()
            if row and row[0]:
                mapped = EXCHANGE_MAP.get((row[0] or '').strip().upper())
                if mapped:
                    candidates.append(mapped)
        except Exception as e:
            logger.warning(f"查询 exchange 失败: {code}: {e}")
        for ex in EXCHANGE_FALLBACKS:
            if ex not in candidates:
                candidates.append(ex)
        return candidates

    def _parse_quote_transcripts(self, html: str) -> List[Dict[str, Any]]:
        """解析 quote 页内嵌的 transcript URL 列表（escaped JSON 中的路径）"""
        h = html.replace('\\/', '/')
        found = set(re.findall(
            r'/earnings/((?:call-)?transcripts)/(\d{4})/(\d{2})/(\d{2})/([a-z0-9-]+)/', h))
        entries = []
        for prefix, y, m, d, slug in found:
            try:
                pub = date(int(y), int(m), int(d))
            except ValueError:
                continue
            qm = re.search(r'-q(\d{1,2})-(20\d{2})(?:-|$)', slug)
            if qm:
                period_key = f'q{int(qm.group(1))}-{qm.group(2)}'
                fiscal_label = f'Q{int(qm.group(1))} {qm.group(2)}'
            else:
                period_key = f'd{y}{m}{d}'
                fiscal_label = None
            entries.append({
                'url': f'https://www.fool.com/earnings/{prefix}/{y}/{m}/{d}/{slug}/',
                'date': pub,
                'period_key': period_key,
                'fiscal_label': fiscal_label,
            })
        return entries

    def _fetch_mf_article_text(self, url: str) -> str:
        """抓取 transcript 文章页并抽取正文纯文本"""
        html = self._http_get(url)
        if not html:
            return ''
        m = re.search(r'<div[^>]*class="article-body[^"]*transcript-content[^"]*"[^>]*>', html)
        if not m:
            m = re.search(r'<div[^>]*class="article-body[^"]*"[^>]*>', html)
        if not m:
            return ''
        body = html[m.end():]
        # 尾部噪音截断
        for marker in ['class="article-footer', 'self.__next_f', 'Summarize with AI']:
            idx = body.find(marker)
            if idx > 0:
                body = body[:idx]
        body = re.sub(r'<(style|script)[^>]*>.*?</\1>', '', body, flags=re.S)
        text = re.sub(r'<[^>]+>', '\n', body)
        text = ihtml.unescape(text)
        lines = [ln.strip() for ln in text.split('\n') if ln.strip()]
        text = '\n'.join(lines)
        return text[:TRANSCRIPT_MAX_CHARS]

    # ── Yahoo Finance: 发现 + 渲染抓取 ───────────────────────────

    def _find_yahoo_transcript(self, code: str) -> Optional[Dict[str, Any]]:
        """从 Yahoo 个股 earnings 页解析 transcript 链接列表, 取最新一季。
        链接形如 GPC-Q2-2026-earnings_call-652164.html（无发布日期, 按季度排序）;
        staleness: 最近财报日超过该季末 +45 天 → 该季落后（如只有 Q2 而财报已到 Q3 周期）。"""
        html = self._http_get(YAHOO_EARNINGS_URL.format(ticker=code.upper()))
        if not html:
            return None
        seen = {}
        for ticker, q, year, sid in YAHOO_LINK_RE.findall(html):
            if ticker.upper() != code.upper():
                continue
            seen[(int(year), int(q))] = \
                f'https://finance.yahoo.com/quote/{code.upper()}/earnings/' \
                f'{ticker}-Q{q}-{year}-earnings_call-{sid}.html'
        if not seen:
            return None
        year, q = max(seen)
        ref = {
            'url': seen[(year, q)],
            'period_key': f'q{q}-{year}',
            'fiscal_label': f'Q{q} {year}',
            'source': 'yahoo_finance',
            'quarter_end': date(year, q * 3, calendar.monthrange(year, q * 3)[1]),
            # 链接无发布日期, 用"季末+30天"近似通话日(仅用于展示与缓存新鲜度)
            'date': date(year, q * 3, calendar.monthrange(year, q * 3)[1]) + timedelta(days=30),
        }
        report_date = self._latest_report_date(code)
        if report_date and (report_date - ref['quarter_end']).days > 45:
            # 财报日比该季末晚 45 天以上 → 该 transcript 落后一个财报周期
            ref['stale'] = True
            ref['latest_report_date'] = report_date
        return ref

    def _fetch_yahoo_article_text(self, url: str) -> str:
        """无头浏览器渲染 Yahoo transcript 页并抽取正文
        （curl 直取 404, 页面需 JS 渲染; 最新季免费全文, 页脚 Premium 提示为历史存档推销）"""
        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            logger.warning('playwright 未安装, 无法抓取 Yahoo transcript')
            return ''
        chrome_kwargs = {}
        if os.path.exists(YAHOO_CHROME_PATH):
            chrome_kwargs['executable_path'] = YAHOO_CHROME_PATH
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(
                    args=['--no-sandbox', '--disable-dev-shm-usage'], **chrome_kwargs)
                page = browser.new_page(viewport={'width': 1400, 'height': 2000})
                page.goto(url, wait_until='domcontentloaded', timeout=60000)
                page.wait_for_timeout(8000)
                txt = page.inner_text('body')
                browser.close()
        except Exception as e:
            logger.warning(f"Yahoo transcript 渲染失败: {url}: {e}")
            return ''

        # 正文切片: 从 Operator 开场白起, 到页脚推销/版权止
        start = txt.find('Operator')
        if start < 0:
            start = 0
        end = len(txt)
        for marker in ('Premium Upgrade', 'Trending Topics', 'Copyright ©'):
            idx = txt.find(marker, start)
            if 0 < idx < end:
                end = idx
        text = txt[start:end].strip()
        return text[:TRANSCRIPT_MAX_CHARS]

    # ── AlphaVantage 兜底 ─────────────────────────────────────────

    def _try_alphavantage(self, code: str) -> Optional[Dict[str, Any]]:
        """尝试 AV transcript（免费 key 无数据时返回 None; 配额尽返回 quota_exceeded 标记）"""
        api_key = os.getenv('ALPHAVANTAGE_API_KEY')
        if not api_key:
            return None
        if self._av_calls_today() >= AV_DAILY_LIMIT:
            return {'quota_exceeded': True}

        report_date = self._latest_report_date(code) or date.today()
        quarter = (report_date.month - 1) // 3 + 1
        try:
            resp = requests.get(AV_TRANSCRIPT_URL, params={
                'function': 'EARNINGS_CALL_TRANSCRIPT',
                'symbol': code,
                'quarter': str(quarter),
                'year': str(report_date.year),
                'apikey': api_key,
            }, timeout=30)
            data = resp.json() or {}
        except Exception as e:
            logger.warning(f"AV 请求失败: {code}: {e}")
            return None

        segments = data.get('transcript') or []
        text = '\n\n'.join(
            f"{s.get('speaker', '')} ({s.get('title', '')}): {s.get('transcript', '')}"
            for s in segments if s.get('transcript'))
        if not text:
            return None
        return {
            'url': None,
            'date': datetime.strptime(data['call_date'], '%Y-%m-%d').date()
            if data.get('call_date') else report_date,
            'period_key': f'q{quarter}-{report_date.year}',
            'fiscal_label': f'Q{quarter} {report_date.year}',
            'text': text[:TRANSCRIPT_MAX_CHARS],
        }

    def _av_calls_today(self) -> int:
        try:
            with self.db_manager.get_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT count(*) FROM earnings_call_recaps "
                    "WHERE source = 'alphavantage' AND fetched_at::date = CURRENT_DATE")
                n = cursor.fetchone()[0]
                cursor.close()
            return int(n)
        except Exception:
            return 0

    # ── LLM 生成与持久化 ──────────────────────────────────────────

    async def _generate_and_save(
        self, code: str, source: str, ref: Dict[str, Any], transcript_text: str
    ) -> Dict[str, Any]:
        """调用中央 LLM 生成 Recap 并落库"""
        messages = [
            LLMMessage(role='system', content=RECAP_SYSTEM_PROMPT),
            LLMMessage(role='user', content=(
                f'公司 ticker: {code}\n电话会时间: {ref["date"].isoformat()}\n'
                f'财季: {ref.get("fiscal_label") or ref["period_key"]}\n\n'
                f'transcript 全文如下:\n\n{transcript_text}')),
        ]
        start = time.time()
        response = await llm_service.chat_completion(
            messages, model=RECAP_MODEL, timeout=240,
            max_tokens=RECAP_MAX_TOKENS, temperature=0.3)
        elapsed = round(time.time() - start, 1)
        recap_md = (response.content or '').strip()
        if not recap_md:
            raise RuntimeError(
                f'LLM 返回空内容 (model={response.model}, '
                f'completion_tokens={response.usage.get("completion_tokens")})')
        logger.info(f"Recap 生成完成: {code} {ref['period_key']}, "
                    f"model={response.model}, 耗时={elapsed}s, cost={response.cost}")

        call_date = ref['date'] if isinstance(ref.get('date'), date) else None
        self._upsert_recap(
            code=code, period_key=ref['period_key'], source=source,
            source_url=ref.get('url'), fiscal_label=ref.get('fiscal_label'),
            call_date=call_date, transcript_text=transcript_text,
            recap_md=recap_md, model=response.model,
            llm_cost=response.cost, status='ok', error_message=None)

        return {
            'status': 'ok', 'code': code,
            'period': ref.get('fiscal_label') or ref['period_key'],
            'call_date': call_date.isoformat() if call_date else None,
            'source': source, 'source_url': ref.get('url'),
            'recap_md': recap_md, 'model': response.model,
            'cached': False,
            'stale': bool(ref.get('stale')),
            'latest_report_date': ref['latest_report_date'].isoformat()
            if ref.get('latest_report_date') else None,
        }

    def _load_cached(self, code: str, period_key: str) -> Optional[Dict[str, Any]]:
        try:
            with self.db_manager.get_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT recap_md, model, source, source_url, fiscal_label, call_date, "
                    "updated_at FROM earnings_call_recaps "
                    "WHERE code = %s AND period_key = %s AND status = 'ok'", (code, period_key))
                row = cursor.fetchone()
                cursor.close()
        except Exception as e:
            logger.warning(f"读取 recap 缓存失败: {code}: {e}")
            return None
        if not row:
            return None
        # 缓存行读取时重算 staleness（数据源补上最新季度后自动解除标记）
        stale = False
        report_date = None
        if row[5]:
            report_date = self._latest_report_date(code)
            if report_date and (report_date - row[5]).days > STALE_THRESHOLD_DAYS:
                stale = True
        return {
            'status': 'ok', 'code': code,
            'period': row[4] or period_key,
            'call_date': row[5].isoformat() if row[5] else None,
            'source': row[2], 'source_url': row[3],
            'recap_md': row[0], 'model': row[1],
            'cached': True, 'generated_at': row[6].isoformat() if row[6] else None,
            'stale': stale,
            'latest_report_date': report_date.isoformat() if report_date else None,
        }

    def _save_negative(self, code: str) -> None:
        """24h 内不重复尝试无结果的 ticker"""
        try:
            with self.db_manager.get_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT updated_at FROM earnings_call_recaps "
                    "WHERE code = %s AND period_key = 'none'", (code,))
                row = cursor.fetchone()
                if row and row[0] and (datetime.utcnow() - row[0].replace(tzinfo=None)).total_seconds() < 86400:
                    cursor.close()
                    return
                cursor.execute(
                    "INSERT INTO earnings_call_recaps (code, period_key, status, fetched_at, updated_at) "
                    "VALUES (%s, 'none', 'no_transcript', now(), now()) "
                    "ON CONFLICT (code, period_key) DO UPDATE "
                    "SET status = 'no_transcript', fetched_at = now(), updated_at = now()",
                    (code,))
                conn.commit()
                cursor.close()
        except Exception as e:
            logger.warning(f"写入负缓存失败: {code}: {e}")

    def _upsert_recap(self, **fields) -> None:
        try:
            with self.db_manager.get_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    INSERT INTO earnings_call_recaps
                        (code, period_key, source, source_url, fiscal_label, call_date,
                         transcript_text, recap_md, model, llm_cost, status, error_message,
                         fetched_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now())
                    ON CONFLICT (code, period_key) DO UPDATE SET
                        source = EXCLUDED.source, source_url = EXCLUDED.source_url,
                        fiscal_label = EXCLUDED.fiscal_label, call_date = EXCLUDED.call_date,
                        transcript_text = EXCLUDED.transcript_text, recap_md = EXCLUDED.recap_md,
                        model = EXCLUDED.model, llm_cost = EXCLUDED.llm_cost,
                        status = EXCLUDED.status, error_message = EXCLUDED.error_message,
                        fetched_at = now(), updated_at = now()
                    """,
                    (fields['code'], fields['period_key'], fields['source'],
                     fields['source_url'], fields['fiscal_label'], fields['call_date'],
                     fields['transcript_text'], fields['recap_md'], fields['model'],
                     fields['llm_cost'], fields['status'], fields['error_message']))
                conn.commit()
                cursor.close()
        except Exception as e:
            logger.error(f"写入 recap 失败: {fields.get('code')}: {e}")

    # ── 工具 ─────────────────────────────────────────────────────

    def _latest_report_date(self, code: str) -> Optional[date]:
        """earnings_calendar 中最近一次已过去的财报日（用于匹配 transcript 窗口）"""
        try:
            with self.db_manager.get_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT DISTINCT ON (ec.code, ec.report_date) ec.report_date "
                    "FROM earnings_calendar ec "
                    "WHERE ec.code = %s AND ec.report_date <= CURRENT_DATE "
                    "ORDER BY ec.code, ec.report_date DESC, ec.fetch_date DESC LIMIT 1",
                    (code,))
                row = cursor.fetchone()
                cursor.close()
            return row[0] if row else None
        except Exception as e:
            logger.warning(f"查询最近财报日失败: {code}: {e}")
            return None

    def _http_get(self, url: str, timeout: int = 30) -> str:
        try:
            resp = requests.get(url, headers=MF_HEADERS, timeout=timeout)
            if resp.status_code == 200:
                return resp.text
            logger.warning(f"HTTP {resp.status_code}: {url}")
        except Exception as e:
            logger.warning(f"请求失败: {url}: {e}")
        return ''

    def _quote_cache_get(self, ticker: str):
        hit = self._quote_cache.get(ticker)
        if hit and time.time() - hit[0] < QUOTE_CACHE_TTL_SEC:
            return hit[1]
        return None

    def _quote_cache_set(self, ticker: str, entries: List) -> None:
        self._quote_cache[ticker] = (time.time(), entries)
