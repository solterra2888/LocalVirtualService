#!/usr/bin/env python3
"""
EDGAR 财报资料服务 — 为公司详情面板提供 Slides / Report 外链

数据链(全部 SEC 官方免费接口, 零第三方依赖):
1. ticker -> CIK: https://www.sec.gov/files/company_tickers.json (全量一份, 内存缓存 7 天)
2. 申报列表: https://data.sec.gov/submissions/CIK##########.json (每 CIK 缓存 1 小时)
3. 8-K 附件清单: /Archives/edgar/data/{cik}/{acc}/index.json (缓存 1 小时)
   - EX-99.1 类附件 = 业绩新闻稿(Report), EX-99.2 类 = 演示稿(Slides), 按文件名启发式识别
4. 最近 10-Q / 10-K 作为正式季报链接

限速: SEC fair-access 10 请求/秒, 请求间 sleep 0.12s;
身份沿用 form13f_service 的 SEC_USER_AGENT 约定("名字 邮箱")。
探测脚本: backend/scripts/test_edgar_materials.py
"""
import os
import re
import time
import threading
import requests
from typing import Dict, Any, Optional, Tuple, List
from datetime import date, timedelta

from backend.utils.logger import get_logger

logger = get_logger("edgar_materials_service")

SEC = 'https://www.sec.gov'
TICKER_MAP_URL = f'{SEC}/files/company_tickers.json'
SUBMISSIONS_URL = 'https://data.sec.gov/submissions/CIK{cik10}.json'

SEC_USER_AGENT = os.getenv(
    "SEC_USER_AGENT", "GuanHe Capital Research contact@guanhe.example.com")

TICKER_MAP_TTL = 7 * 24 * 3600      # 映射表 7 天
FILING_CACHE_TTL = 3600             # 申报列表/附件清单 1 小时
EIGHTK_LOOKBACK_DAYS = 120          # 8-K 只看最近 120 天(财报类)
REQUEST_GAP = 0.12                  # 请求间隔(秒), 低于 SEC 10 req/s


def _exhibit_kind(filename: str) -> str:
    """附件文件名 -> 'press' | 'slides' | ''(按 EDGAR 命名惯例启发式)"""
    n = filename.lower().replace('_', '').replace('-', '').replace('.', '')
    if 'ex992' in n or 'exhibit992' in n:
        return 'slides'
    if 'slide' in n or 'present' in n:
        return 'slides'
    if 'ex991' in n or 'exhibit991' in n:
        return 'press'
    if 'press' in n or 'release' in n or 'results' in n:
        return 'press'
    if 'earn' in n and 'q' in n:
        return 'press'
    return ''


class EdgarMaterialsService:
    """单例服务: 内存缓存 + 限速, 供公司面板按需取 EDGAR 资料"""

    def __init__(self):
        self._session = requests.Session()
        self._session.headers.update({'User-Agent': SEC_USER_AGENT})
        self._lock = threading.Lock()
        self._last_request_ts = 0.0
        # ticker -> cik
        self._ticker_map: Optional[Dict[str, int]] = None
        self._ticker_map_ts = 0.0
        # cik -> (submissions_recent 列表, ts)
        self._subs_cache: Dict[int, Tuple[Dict[str, list], float]] = {}
        # (cik, accession) -> [文件名列表, ts]
        self._index_cache: Dict[Tuple[int, str], Tuple[List[str], float]] = {}

    # ── 基础请求(限速) ──────────────────────────────────────
    def _get(self, url: str, timeout: int = 25) -> requests.Response:
        with self._lock:
            gap = time.time() - self._last_request_ts
            if gap < REQUEST_GAP:
                time.sleep(REQUEST_GAP - gap)
            self._last_request_ts = time.time()
        return self._session.get(url, timeout=timeout)

    # ── ticker -> CIK ──────────────────────────────────────
    def _ticker_to_cik(self, ticker: str) -> Optional[int]:
        now = time.time()
        if self._ticker_map is None or now - self._ticker_map_ts > TICKER_MAP_TTL:
            try:
                r = self._get(TICKER_MAP_URL)
                r.raise_for_status()
                data = r.json()
                self._ticker_map = {
                    v['ticker'].upper(): int(v['cik_str']) for v in data.values()
                }
                self._ticker_map_ts = now
                logger.info(f"SEC ticker 映射表已加载: {len(self._ticker_map)} 家")
            except Exception as e:
                logger.warning(f"SEC ticker 映射表获取失败: {e}")
                return None
        return self._ticker_map.get(ticker.upper())

    # ── 申报列表(取 recent 的并行数组, 缓存 1h) ─────────────
    def _get_submissions(self, cik: int) -> Optional[Dict[str, list]]:
        now = time.time()
        cached = self._subs_cache.get(cik)
        if cached and now - cached[1] < FILING_CACHE_TTL:
            return cached[0]
        try:
            r = self._get(SUBMISSIONS_URL.format(cik10=str(cik).zfill(10)))
            r.raise_for_status()
            recent = r.json().get('filings', {}).get('recent', {})
            self._subs_cache[cik] = (recent, now)
            return recent
        except Exception as e:
            logger.warning(f"SEC submissions 获取失败 cik={cik}: {e}")
            return None

    # ── 8-K 附件清单(缓存 1h) ──────────────────────────────
    def _get_exhibits(self, cik: int, accession: str) -> List[str]:
        key = (cik, accession)
        now = time.time()
        cached = self._index_cache.get(key)
        if cached and now - cached[1] < FILING_CACHE_TTL:
            return cached[0]
        try:
            url = f"{SEC}/Archives/edgar/data/{cik}/{accession.replace('-', '')}/index.json"
            r = self._get(url)
            r.raise_for_status()
            names = [it['name'] for it in r.json().get('directory', {}).get('item', [])]
            self._index_cache[key] = (names, now)
            return names
        except Exception as e:
            logger.warning(f"SEC 附件清单获取失败 cik={cik} acc={accession}: {e}")
            return []

    # ── 对外主入口 ─────────────────────────────────────────
    def get_materials(self, code: str) -> Dict[str, Any]:
        """美股裸代码 -> EDGAR 财报资料链接(8-K 新闻稿/演示稿 + 最近季报)"""
        bare = (code or '').strip().upper()
        if not bare or '.' in bare:
            return {'found': False, 'code': bare or code, 'reason': 'unsupported_code'}

        cik = self._ticker_to_cik(bare)
        if not cik:
            return {'found': False, 'code': bare, 'reason': 'no_cik'}

        subs = self._get_submissions(cik)
        if not subs:
            return {'found': False, 'code': bare, 'reason': 'no_submissions'}

        forms = subs.get('form', [])
        dates = subs.get('filingDate', [])
        accs = subs.get('accessionNumber', [])
        pdocs = subs.get('primaryDocument', [])

        cutoff = (date.today() - timedelta(days=EIGHTK_LOOKBACK_DAYS)).isoformat()

        # 最近一次 8-K(财报新闻稿载体)
        eightk = None
        tenq = None
        for i, form in enumerate(forms):
            if eightk is None and form == '8-K' and dates[i] >= cutoff:
                eightk = (dates[i], accs[i], pdocs[i])
            if tenq is None and form in ('10-Q', '10-K'):
                tenq = (form, dates[i], accs[i], pdocs[i])
            if eightk and tenq:
                break

        result: Dict[str, Any] = {'found': True, 'code': bare, 'cik': cik}

        if eightk:
            fdate, acc, _ = eightk
            exhibits = self._get_exhibits(cik, acc)
            press = slides = None
            for name in exhibits:
                kind = _exhibit_kind(name)
                if kind == 'press' and not press:
                    press = f"{SEC}/Archives/edgar/data/{cik}/{acc.replace('-', '')}/{name}"
                elif kind == 'slides' and not slides:
                    slides = f"{SEC}/Archives/edgar/data/{cik}/{acc.replace('-', '')}/{name}"
                if press and slides:
                    break
            result['eightk'] = {'filing_date': fdate, 'press_release': press, 'slides': slides}

        if tenq:
            form, fdate, acc, pdoc = tenq
            result['quarterly'] = {
                'form': form,
                'filing_date': fdate,
                'url': f"{SEC}/Archives/edgar/data/{cik}/{acc.replace('-', '')}/{pdoc}",
            }

        return result


# ── 单例 ────────────────────────────────────────────────
_edgar_materials_service: Optional[EdgarMaterialsService] = None


def get_edgar_materials_service() -> EdgarMaterialsService:
    global _edgar_materials_service
    if _edgar_materials_service is None:
        _edgar_materials_service = EdgarMaterialsService()
    return _edgar_materials_service
