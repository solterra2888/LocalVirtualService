# -*- coding: utf-8 -*-
"""
Form 13F 数据服务 — 按预设机构 CIK 从 EDGAR 拉取 13F-HR

数据链: sync_preset_filings（20 家 CIK → 折叠 amendment → holdings）
       → compute_snapshots（行业分布 / top20 / top5 买卖）

身份主键是冻结 CIK，不再用申报名匹配，也不再下载 SEC 全市场 ZIP。
"""

from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from typing import Dict, List, Optional, Tuple

import psycopg2
import psycopg2.extras

from backend.database.db_manager import DatabaseManager
from backend.utils.database_config import get_database_config
from backend.utils.form13f_sector_map import CUSIP_SECTOR, classify_etf, is_fund
from backend.utils.logger import get_logger
import os
import re

logger = get_logger("form13f_service")

# SEC fair-access: identity 必须是「名字 邮箱」。edgartools / EDGAR 共用。
SEC_USER_AGENT = os.getenv(
    "SEC_USER_AGENT", "GuanHe Capital Research contact@guanhe.example.com")

# 默认回填起点（日历季）。更早的 TXT 时代申报不拉，避免历史格式噪音。
DEFAULT_SINCE_QUARTER = os.getenv("FORM13F_SINCE_QUARTER", "2024Q1")

BATCH_SIZE = 5000

HoldingKey = Tuple[str, str]

_MONTHS = {'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
           'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12}


def quarter_of(value) -> Optional[str]:
    """报告期末 → 日历季。接受 datetime / '2026-03-31' / '31-MAR-2026'。"""
    if value is None:
        return None
    if hasattr(value, 'year') and hasattr(value, 'month'):
        return f"{int(value.year)}Q{(int(value.month) - 1) // 3 + 1}"
    text = str(value).strip()
    iso = re.match(r'^(\d{4})-(\d{2})-(\d{2})', text)
    if iso:
        return f"{iso.group(1)}Q{(int(iso.group(2)) - 1) // 3 + 1}"
    m = re.match(r'(\d{2})-([A-Z]{3})-(\d{4})', text.upper())
    if not m or m.group(2) not in _MONTHS:
        return None
    month = _MONTHS[m.group(2)]
    return f"{m.group(3)}Q{(month - 1) // 3 + 1}"


def normalize_cik(cik) -> str:
    digits = re.sub(r'\D', '', str(cik or ''))
    if not digits:
        raise ValueError("empty CIK")
    return digits.zfill(10)


def filer_key_of(cik) -> str:
    """机构唯一键 = 10 位 CIK（不再用申报名哈希）。"""
    return normalize_cik(cik)


def normalize_filer_name(name: str) -> str:
    return re.sub(r'\s+', ' ', (name or '').strip()).upper()


def classify_holding(issuer: str, title: str, cusip: str) -> str:
    if is_fund(issuer, title):
        return classify_etf(issuer, title)
    return CUSIP_SECTOR.get(cusip, 'Other')


def portfolio_pct(value: int, total: int) -> float:
    """持仓市值 / 当季组合总市值，百分数。"""
    if not total:
        return 0.0
    return value / total * 100.0


_OPTION_PUTCALL = frozenset({'put', 'call', 'p', 'c'})
_NON_EQUITY_TITLE = re.compile(
    r'\b(?:NOTE|NOTES|BOND|BONDS|DEBENTURE|DEBENTURES|CONVERTIBLE|'
    r'WARRANT|WARRANTS)\b|\d+\.\d+\s*%',
    re.IGNORECASE)


def is_option_leg(putcall: Optional[str]) -> bool:
    return (putcall or '').strip().lower() in _OPTION_PUTCALL


def is_fixed_income_holding(title: Optional[str]) -> bool:
    """13F 信息表里的债/可转债/权证（MacroMicro 只展示股票与 ETF 多头）。"""
    return bool(_NON_EQUITY_TITLE.search(title or ''))


def is_stock_or_etf_long(putcall: Optional[str], title: Optional[str] = None) -> bool:
    return not is_option_leg(putcall) and not is_fixed_income_holding(title)


def stock_etf_long_totals(rows) -> Dict[int, int]:
    """rows: (filer_id, putcall, title, value_usd) → 股票/ETF 多头市值。"""
    out: Dict[int, int] = defaultdict(int)
    for filer_id, putcall, title, value in rows:
        if is_stock_or_etf_long(putcall, title):
            out[int(filer_id)] += int(value or 0)
    return dict(out)


def share_change_pct(cur_shares: Optional[int], prev_shares: Optional[int]) -> Optional[float]:
    """(本期股数 − 上期股数) / 上期股数。无上期股数返回 None（NEW）。"""
    prev = int(prev_shares or 0)
    cur = int(cur_shares or 0)
    if prev <= 0:
        return None
    if cur <= 0:
        return -100.0
    return (cur - prev) / prev * 100.0


def rank_top5_by_weight_change(
        rows: List[Tuple],
        cur_totals: Dict[int, int],
        prev_totals: Dict[int, int],
        top_n: int = 5,
) -> Tuple[Dict[int, List[dict]], Dict[int, List[dict]]]:
    """Top 5 加码 / 减码，与 MacroMicro 一致。

    只看股票与 ETF 多头（排除 Put/Call、债券/可转债、权证）。
    占比分母是同一口径的多头合计，避免期权大仓稀释股票权重。
    先按股数方向过滤（buys = 新建或 Δ股数>0；sells = 清仓或 Δ股数<0），
    再按 ΔWeight = 本期% − 上期% 排序。股数未变的权重漂移不进榜。

    rows: (filer_id, issuer, cusip, putcall, cur_value, prev_value,
           cur_shares, prev_shares[, title])
    cur_totals / prev_totals: 股票与 ETF 多头市值（不含期权与固定收益）。
    """
    by_filer: Dict[int, List[dict]] = defaultdict(list)
    for row in rows:
        (filer_id, issuer, cusip, putcall,
         cur_value, prev_value, cur_shares, prev_shares) = row[:8]
        title = row[8] if len(row) > 8 else ''
        if not is_stock_or_etf_long(putcall, title):
            continue
        filer_id = int(filer_id)
        cur_value = int(cur_value or 0)
        prev_value = int(prev_value or 0)
        cur_shares = int(cur_shares or 0)
        prev_shares = int(prev_shares or 0)
        cur_total = int(cur_totals.get(filer_id) or 0)
        prev_total = int(prev_totals.get(filer_id) or 0)
        if cur_total <= 0:
            continue
        dsh = cur_shares - prev_shares
        is_new = prev_shares <= 0 < cur_shares
        is_exit = cur_shares <= 0 < prev_shares
        if not is_new and not is_exit and dsh == 0:
            continue
        cur_pct = portfolio_pct(cur_value, cur_total)
        prev_pct = portfolio_pct(prev_value, prev_total)
        dpct = cur_pct - prev_pct
        if is_new:
            kind = 'new'
        elif is_exit:
            kind = 'exited'
        elif dsh > 0:
            kind = 'increase'
        else:
            kind = 'decrease'
        spct = share_change_pct(cur_shares, prev_shares)
        by_filer[filer_id].append({
            'issuer': issuer,
            'cusip': cusip or '',
            'putcall': putcall or '',
            'dv': cur_value - prev_value,
            'value': cur_value,
            'pct': round(cur_pct, 3),
            'dpct': round(dpct, 3),
            'spct': None if spct is None else round(spct, 2),
            'kind': kind,
            '_dpct': dpct,
            '_buy': is_new or dsh > 0,
            '_sell': is_exit or dsh < 0,
        })

    buys_map: Dict[int, List[dict]] = {}
    sells_map: Dict[int, List[dict]] = {}
    extra_keys = ('_dpct', '_buy', '_sell')
    for filer_id, items in by_filer.items():
        buys = sorted(
            (x for x in items if x['_buy']),
            key=lambda x: -x['_dpct'])[:top_n]
        sells = sorted(
            (x for x in items if x['_sell']),
            key=lambda x: x['_dpct'])[:top_n]
        for lst in (buys, sells):
            for item in lst:
                for k in extra_keys:
                    del item[k]
        buys_map[filer_id] = buys
        sells_map[filer_id] = sells
    return buys_map, sells_map


_OTHER_MANAGER_BLOCK = re.compile(
    r'<(?:[\w.]+:)?otherManager\b[^>]*>(.*?)</(?:[\w.]+:)?otherManager>',
    re.IGNORECASE | re.DOTALL)
_CIK_TAG = re.compile(
    r'<(?:[\w.]+:)?cik>\s*(\d+)\s*</(?:[\w.]+:)?cik>', re.IGNORECASE)


def notice_other_manager_ciks(xml: str) -> List[str]:
    """从 13F-NT cover page 提取 otherManager CIK。edgartools 的 other_managers 对 NT 经常是空的。"""
    if not xml:
        return []
    found: List[str] = []
    seen = set()
    for block in _OTHER_MANAGER_BLOCK.finditer(xml):
        m = _CIK_TAG.search(block.group(1))
        if not m:
            continue
        cik = normalize_cik(m.group(1))
        if cik in seen:
            continue
        seen.add(cik)
        found.append(cik)
    return found


def _filing_xml(filing) -> str:
    xml = getattr(filing, 'xml', None)
    if callable(xml):
        try:
            xml = xml()
        except Exception:
            return ''
    return xml if isinstance(xml, str) else ''


def _na_to_str(value, default: str = '') -> str:
    if value is None:
        return default
    try:
        import math
        if isinstance(value, float) and math.isnan(value):
            return default
    except Exception:
        pass
    text = str(value).strip()
    if text.lower() in ('', 'nan', 'none', '-', 'nat'):
        return default
    return text


@dataclass(frozen=True)
class HoldingRow:
    cusip: str
    issuer: str
    title: str
    putcall: str
    value: int
    shares: int


@dataclass
class PeriodFiling:
    accession: str
    filing_date: str
    is_amendment: bool
    amendment_type: Optional[str]
    amendment_number: int
    rows: Dict[HoldingKey, HoldingRow]


def fold_period_filings(
        items: List[PeriodFiling],
) -> Tuple[Dict[HoldingKey, HoldingRow], Optional[str]]:
    """同一 CIK + 同一报告期的多份 13F-HR[/A] 折成一份有效持仓。

    - 原文 / RESTATEMENT / 未标明类型的 amendment：整表替换
    - NEW HOLDINGS：按 (cusip, putcall) 并入（保密仓补披露，不能当整季持仓）
    """
    if not items:
        return {}, None
    ordered = sorted(
        items,
        key=lambda x: (
            x.filing_date or '',
            x.is_amendment,
            x.amendment_number,
            x.accession,
        ),
    )
    holdings: Dict[HoldingKey, HoldingRow] = {}
    chosen: Optional[str] = None
    for it in ordered:
        if not it.rows:
            continue
        at = (it.amendment_type or '').strip().upper()
        if at == 'NEW HOLDINGS':
            holdings.update(it.rows)
        else:
            holdings = dict(it.rows)
        chosen = it.accession
    return holdings, chosen


def holdings_from_thirteenf(report) -> Dict[HoldingKey, HoldingRow]:
    """edgartools ThirteenF.holdings → 按 (CUSIP, PutCall) 的美元持仓。"""
    df = getattr(report, 'holdings', None)
    if df is None or len(df) == 0:
        return {}
    out: Dict[HoldingKey, HoldingRow] = {}
    for rec in df.to_dict('records'):
        cusip = _na_to_str(rec.get('Cusip')).upper()
        if not cusip:
            continue
        pc = _na_to_str(rec.get('PutCall'))
        try:
            value = int(round(float(rec.get('Value') or 0)))
        except (TypeError, ValueError):
            continue
        try:
            shares = int(round(float(rec.get('SharesPrnAmount') or 0)))
        except (TypeError, ValueError):
            shares = 0
        out[(cusip, pc)] = HoldingRow(
            cusip=cusip,
            issuer=_na_to_str(rec.get('Issuer')),
            title=_na_to_str(rec.get('Class')),
            putcall=pc,
            value=value,
            shares=shares,
        )
    return out


class Form13fService:
    """按预设 CIK 同步 13F-HR 并维护快照。"""

    def __init__(self):
        self.db_manager = DatabaseManager(get_database_config())
        self._edgar_ready = False

    def _configure_edgar(self) -> None:
        if self._edgar_ready:
            return
        from edgar import set_identity
        set_identity(SEC_USER_AGENT)
        self._edgar_ready = True

    # ── 预设机构 ──────────────────────────────────────────────────────────

    def ensure_preset_filers(self) -> int:
        """按 form13f_presets.ciK upsert 20 家机构维表（无申报名匹配）。"""
        rows = self.db_manager.fetch_all(
            """
            SELECT label, match_name, cik FROM form13f_presets
            WHERE cik IS NOT NULL AND btrim(cik) <> ''
            ORDER BY sort_order
            """)
        if not rows:
            raise RuntimeError("form13f_presets 没有 CIK，请先执行 add_form13f_preset_cik 迁移")
        upserted = 0
        with self.db_manager.get_connection_context() as conn:
            with conn.cursor() as cur:
                for label, match_name, cik in rows:
                    key = filer_key_of(cik)
                    display = match_name or label
                    cur.execute(
                        """
                        INSERT INTO form13f_filers (
                            filer_key, name_normalized, name_display, cik, is_preset)
                        VALUES (%s, %s, %s, %s, TRUE)
                        ON CONFLICT (filer_key) DO UPDATE SET
                            name_normalized = EXCLUDED.name_normalized,
                            name_display = EXCLUDED.name_display,
                            cik = EXCLUDED.cik,
                            is_preset = TRUE,
                            updated_at = CURRENT_TIMESTAMP
                        """,
                        (key, normalize_filer_name(display), display, key))
                    upserted += 1
        return upserted

    def list_presets(self) -> List[Dict]:
        rows = self.db_manager.fetch_all(
            """
            SELECT p.label, p.match_name, p.cik, p.sort_order, f.id
            FROM form13f_presets p
            LEFT JOIN form13f_filers f ON f.cik = p.cik
            WHERE p.cik IS NOT NULL AND btrim(p.cik) <> ''
            ORDER BY p.sort_order
            """)
        return [{
            'label': r[0], 'match_name': r[1], 'cik': r[2],
            'sort_order': r[3], 'filer_id': r[4],
        } for r in rows]

    # ── 同步 ──────────────────────────────────────────────────────────────

    def sync_preset_filings(self, since_quarter: Optional[str] = None) -> Dict:
        """拉取 20 家预设机构 13F-HR，折叠 amendment 后入库并重算快照。"""
        since = since_quarter or DEFAULT_SINCE_QUARTER
        self._configure_edgar()
        self.ensure_preset_filers()
        presets = self.list_presets()
        filer_errors: List[Dict] = []
        affected: set = set()
        imported = 0
        skipped = 0
        for preset in presets:
            try:
                result = self.sync_filer(preset['cik'], since_quarter=since)
                imported += result['imported']
                skipped += result['skipped']
                affected.update(result['quarters'])
            except Exception as exc:
                logger.error(
                    f"[13F] 同步失败 {preset['label']} cik={preset['cik']}: {exc}",
                    exc_info=True)
                filer_errors.append({
                    'cik': preset['cik'], 'label': preset['label'],
                    'error': str(exc),
                })

        snapshots = []
        for q in self._quarters_to_recompute(affected):
            snapshots.append(self.compute_snapshots(q))

        summary = {
            'since_quarter': since,
            'filers': len(presets),
            'imported': imported,
            'skipped': skipped,
            'quarters': sorted(affected),
            'snapshots': snapshots,
            'errors': filer_errors,
        }
        logger.info(f"[13F] 预设同步完成: {summary}")
        return summary

    def sync_filer(self, cik: str, since_quarter: Optional[str] = None) -> Dict:
        """同步单家机构全部（since 之后）报告期。"""
        since = since_quarter or DEFAULT_SINCE_QUARTER
        cik = normalize_cik(cik)
        filer = self.db_manager.fetch_one(
            "SELECT id FROM form13f_filers WHERE cik = %s", (cik,))
        if not filer:
            self.ensure_preset_filers()
            filer = self.db_manager.fetch_one(
                "SELECT id FROM form13f_filers WHERE cik = %s", (cik,))
        if not filer:
            raise ValueError(f"CIK {cik} 不在 form13f_filers（也不是预设）")
        filer_id = filer[0]

        grouped = self._group_filings_by_quarter(cik, since)
        imported = 0
        skipped = 0
        quarters: List[str] = []
        for quarter, period_filings in grouped.items():
            holdings, accession = fold_period_filings(period_filings)
            if not holdings or not accession:
                continue
            changed = self._replace_filer_quarter(
                filer_id, quarter, accession, holdings)
            if changed:
                imported += 1
                quarters.append(quarter)
            else:
                skipped += 1
        return {
            'cik': cik, 'filer_id': filer_id,
            'imported': imported, 'skipped': skipped,
            'quarters': quarters,
        }

    def _group_filings_by_quarter(
            self, cik: str, since_quarter: str,
    ) -> Dict[str, List[PeriodFiling]]:
        grouped = self._collect_hr_filings(cik, since_quarter)
        for quarter, successor_cik in self._notice_successors(cik, since_quarter).items():
            if any(p.rows for p in grouped.get(quarter, [])):
                continue
            try:
                followed = self._collect_hr_filings(
                    successor_cik, since_quarter, only_quarter=quarter)
            except Exception as exc:
                logger.warning(
                    f"[13F] 跟随 13F-NT 失败 cik={cik} {quarter} → {successor_cik}: {exc}")
                continue
            if followed.get(quarter):
                logger.info(
                    f"[13F] {cik} {quarter} 13F-NT → {successor_cik}")
                grouped[quarter] = followed[quarter]
        return grouped

    def _collect_hr_filings(
            self, cik: str, since_quarter: str,
            only_quarter: Optional[str] = None,
    ) -> Dict[str, List[PeriodFiling]]:
        from edgar import Company

        cik = normalize_cik(cik)
        start_year = int((only_quarter or since_quarter)[:4])
        end_year = int((only_quarter or f"{date.today().year}Q4")[:4])
        years = list(range(start_year, end_year + 1))
        company = Company(int(cik))
        filings = company.get_filings(
            form="13F-HR", year=years, amendments=True)
        grouped: Dict[str, List[PeriodFiling]] = defaultdict(list)
        for filing in filings:
            form = (getattr(filing, 'form', None) or '').upper()
            if not form.startswith('13F-HR'):
                continue
            period = getattr(filing, 'period_of_report', None)
            quarter = quarter_of(period)
            if not quarter or quarter < since_quarter:
                continue
            if only_quarter and quarter != only_quarter:
                continue
            try:
                report = filing.obj()
            except Exception as exc:
                logger.warning(
                    f"[13F] 解析失败 cik={cik} {getattr(filing, 'accession_no', None)}: {exc}")
                continue
            if report is None or not report.has_infotable():
                continue
            accession = (
                getattr(filing, 'accession_no', None)
                or getattr(filing, 'accession_number', None)
                or getattr(report, 'accession_number', None)
            )
            if not accession:
                continue
            grouped[quarter].append(PeriodFiling(
                accession=str(accession),
                filing_date=str(getattr(filing, 'filing_date', '') or ''),
                is_amendment=bool(getattr(report, 'is_amendment', False)
                                  or form.endswith('/A')),
                amendment_type=getattr(report, 'amendment_type', None),
                amendment_number=int(getattr(report, 'amendment_number', None) or 0),
                rows=holdings_from_thirteenf(report),
            ))
        return grouped

    def _notice_successors(
            self, cik: str, since_quarter: str,
    ) -> Dict[str, str]:
        """13F-NT 指向的 other manager：quarter → successor CIK（每季取第一个）。"""
        from edgar import Company

        cik = normalize_cik(cik)
        start_year = int(since_quarter[:4])
        years = list(range(start_year, date.today().year + 1))
        company = Company(int(cik))
        filings = company.get_filings(
            form="13F-NT", year=years, amendments=True)
        out: Dict[str, str] = {}
        for filing in filings:
            form = (getattr(filing, 'form', None) or '').upper()
            if '13F-NT' not in form:
                continue
            period = getattr(filing, 'period_of_report', None)
            quarter = quarter_of(period)
            if not quarter or quarter < since_quarter:
                continue
            others = [
                ocik for ocik in notice_other_manager_ciks(_filing_xml(filing))
                if ocik != cik
            ]
            if others:
                out[quarter] = others[0]
        return out

    def _replace_filer_quarter(
            self, filer_id: int, quarter: str, accession: str,
            holdings: Dict[HoldingKey, HoldingRow],
    ) -> bool:
        """幂等覆盖一个机构×季度。accession 未变则跳过。"""
        existing = self.db_manager.fetch_all(
            """
            SELECT DISTINCT accession_number FROM form13f_holdings
            WHERE filer_id = %s AND quarter = %s
            """,
            (filer_id, quarter))
        accs = {r[0] for r in existing}
        if accs == {accession}:
            return False

        dataset_id = self._ensure_quarter_dataset(quarter)
        rows = []
        for (cusip, putcall), h in holdings.items():
            rows.append((
                dataset_id, quarter, filer_id, accession, cusip,
                h.issuer, h.title, putcall, h.value, h.shares,
                1, classify_holding(h.issuer, h.title, cusip),
            ))
        with self.db_manager.get_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM form13f_holdings "
                    "WHERE filer_id = %s AND quarter = %s",
                    (filer_id, quarter))
                if rows:
                    psycopg2.extras.execute_values(
                        cur,
                        """
                        INSERT INTO form13f_holdings (
                            dataset_id, quarter, filer_id, accession_number, cusip,
                            issuer_name, title_of_class, putcall, value_usd, shares,
                            leg_count, sector)
                        VALUES %s
                        """,
                        rows, page_size=BATCH_SIZE)
        logger.info(
            f"[13F] 写入 {quarter} filer_id={filer_id} "
            f"positions={len(rows)} accession={accession}")
        return True

    def _ensure_quarter_dataset(self, quarter: str) -> int:
        name = f"edgar_{quarter}"
        with self.db_manager.get_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO form13f_datasets (
                        dataset_name, quarter, source_url, status, imported_at)
                    VALUES (%s, %s, %s, 'imported', CURRENT_TIMESTAMP)
                    ON CONFLICT (dataset_name) DO UPDATE SET
                        quarter = EXCLUDED.quarter,
                        status = 'imported',
                        imported_at = CURRENT_TIMESTAMP,
                        updated_at = CURRENT_TIMESTAMP
                    RETURNING id
                    """,
                    (name, quarter, 'https://www.sec.gov/cgi-bin/browse-edgar'))
                return cur.fetchone()[0]

    def _quarters_to_recompute(self, affected: set) -> List[str]:
        if not affected:
            return []
        existing = [r[0] for r in self.db_manager.fetch_all(
            "SELECT DISTINCT quarter FROM form13f_holdings "
            "WHERE quarter IS NOT NULL ORDER BY quarter")]
        to_run = set(affected)
        for i, q in enumerate(existing):
            if i > 0 and existing[i - 1] in affected:
                to_run.add(q)
        return sorted(q for q in to_run if q in existing)

    # ── 快照 ──────────────────────────────────────────────────────────────

    def compute_snapshots(self, quarter: str) -> Dict:
        """为指定季度全部（预设）机构计算快照。"""
        q = quarter
        if not q:
            raise ValueError("compute_snapshots 需要 quarter")

        sec_rows = self.db_manager.fetch_all(
            """
            SELECT filer_id, sector, SUM(value_usd) AS value, COUNT(*) AS positions
            FROM form13f_holdings
            WHERE quarter = %s
            GROUP BY filer_id, sector
            """,
            (q,))
        totals = defaultdict(int)
        counts = defaultdict(int)
        sec_map = defaultdict(dict)
        for filer_id, sector, value, positions in sec_rows:
            totals[filer_id] += value
            counts[filer_id] += positions
            sec_map[filer_id][sector] = value

        top20_rows = self.db_manager.fetch_all(
            """
            SELECT filer_id, issuer_name, title_of_class, cusip, putcall,
                   value_usd, shares
            FROM (
                SELECT h.*, ROW_NUMBER() OVER (
                    PARTITION BY filer_id ORDER BY value_usd DESC) AS rn
                FROM form13f_holdings h WHERE quarter = %s
            ) t WHERE rn <= 20
            """,
            (q,))
        top20_map = defaultdict(list)
        for filer_id, issuer, title, cusip, putcall, value, shares in top20_rows:
            top20_map[filer_id].append(
                (value, issuer, title, cusip, putcall, shares))
        for lst in top20_map.values():
            lst.sort(reverse=True)

        prev_quarter = self.db_manager.fetch_one(
            "SELECT MAX(quarter) FROM form13f_holdings WHERE quarter < %s", (q,))[0]

        buys_map: Dict[int, List[dict]] = {}
        sells_map: Dict[int, List[dict]] = {}
        if prev_quarter:
            prev_hold_rows = self.db_manager.fetch_all(
                """
                SELECT filer_id, putcall, title_of_class, value_usd
                FROM form13f_holdings WHERE quarter = %s
                """,
                (prev_quarter,))
            prev_eq_totals = stock_etf_long_totals(prev_hold_rows)
            cur_hold_rows = self.db_manager.fetch_all(
                """
                SELECT filer_id, putcall, title_of_class, value_usd
                FROM form13f_holdings WHERE quarter = %s
                """,
                (q,))
            cur_eq_totals = stock_etf_long_totals(cur_hold_rows)
            change_rows = self.db_manager.fetch_all(
                """
                SELECT COALESCE(c.filer_id, p.filer_id) AS filer_id,
                       COALESCE(c.issuer_name, p.issuer_name) AS issuer_name,
                       COALESCE(c.cusip, p.cusip) AS cusip,
                       COALESCE(c.putcall, p.putcall) AS putcall,
                       COALESCE(c.value_usd, 0) AS cur_value,
                       COALESCE(p.value_usd, 0) AS prev_value,
                       c.shares AS cur_shares,
                       p.shares AS prev_shares,
                       COALESCE(c.title_of_class, p.title_of_class) AS title
                FROM (SELECT * FROM form13f_holdings WHERE quarter = %s) c
                FULL JOIN (SELECT * FROM form13f_holdings WHERE quarter = %s) p
                  ON p.filer_id = c.filer_id
                 AND p.cusip = c.cusip
                 AND p.putcall = c.putcall
                """,
                (q, prev_quarter))
            buys_map, sells_map = rank_top5_by_weight_change(
                change_rows, cur_eq_totals, prev_eq_totals)

        snap_rows = []
        for filer_id, total in totals.items():
            if total <= 0:
                continue
            total = int(total)
            sector_alloc = {
                sec: round(int(v) / total * 100, 3) for sec, v in sec_map[filer_id].items()}
            top20 = [{
                'issuer': issuer, 'title': title, 'cusip': cusip,
                'putcall': putcall, 'value': int(value),
                'shares': int(shares) if shares is not None else None,
                'pct': round(int(value) / total * 100, 3),
            } for value, issuer, title, cusip, putcall, shares in top20_map.get(filer_id, [])][:20]
            snap_rows.append((
                filer_id, q, total, int(counts[filer_id]),
                psycopg2.extras.Json(sector_alloc),
                psycopg2.extras.Json(top20),
                psycopg2.extras.Json(buys_map.get(filer_id, [])),
                psycopg2.extras.Json(sells_map.get(filer_id, [])),
            ))

        for i in range(0, len(snap_rows), 1000):
            with self.db_manager.get_connection_context() as conn:
                with conn.cursor() as cur:
                    psycopg2.extras.execute_values(
                        cur,
                        """
                        INSERT INTO form13f_snapshots (
                            filer_id, quarter, total_value, position_count,
                            sector_alloc, top20_holdings, top5_buys, top5_sells)
                        VALUES %s
                        ON CONFLICT (filer_id, quarter) DO UPDATE SET
                            total_value = EXCLUDED.total_value,
                            position_count = EXCLUDED.position_count,
                            sector_alloc = EXCLUDED.sector_alloc,
                            top20_holdings = EXCLUDED.top20_holdings,
                            top5_buys = EXCLUDED.top5_buys,
                            top5_sells = EXCLUDED.top5_sells,
                            computed_at = CURRENT_TIMESTAMP
                        """,
                        snap_rows[i:i + 1000], page_size=1000)

        with self.db_manager.get_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE form13f_filers f
                    SET latest_quarter = s.quarter,
                        latest_total_value = s.total_value,
                        updated_at = CURRENT_TIMESTAMP
                    FROM form13f_snapshots s
                    WHERE s.filer_id = f.id AND s.quarter = %s
                      AND (f.latest_quarter IS NULL OR f.latest_quarter <= %s)
                    """,
                    (q, q))
                cur.execute(
                    """
                    UPDATE form13f_filers f
                    SET is_preset = TRUE
                    FROM form13f_presets p
                    WHERE f.cik = p.cik
                    """)

        logger.info(f"[13F] 快照完成 {q}: {len(snap_rows)} 家机构 (对比季度 {prev_quarter})")
        return {"quarter": q, "snapshots": len(snap_rows), "prev_quarter": prev_quarter}

    def recompute_all_snapshots(self) -> List[Dict]:
        quarters = [r[0] for r in self.db_manager.fetch_all(
            "SELECT DISTINCT quarter FROM form13f_holdings "
            "WHERE quarter IS NOT NULL ORDER BY quarter")]
        return [self.compute_snapshots(q) for q in quarters]

    # ── 查询侧 ────────────────────────────────────────────────────────────

    def get_manifest(self, top_n: int = 200) -> Dict:
        """仪表盘清单：季度列表 + 20 家预设机构。"""
        self.ensure_preset_filers()
        quarters = [r[0] for r in self.db_manager.fetch_all(
            "SELECT DISTINCT quarter FROM form13f_snapshots "
            "WHERE quarter IS NOT NULL ORDER BY quarter")]
        presets = self.db_manager.fetch_all(
            """
            SELECT f.id, p.label, f.latest_quarter, f.latest_total_value
            FROM form13f_presets p
            LEFT JOIN form13f_filers f ON f.cik = p.cik
            ORDER BY p.sort_order
            """)
        top_filers = self.db_manager.fetch_all(
            """
            SELECT id, name_display, latest_quarter, latest_total_value
            FROM form13f_filers
            WHERE is_preset = TRUE AND latest_total_value IS NOT NULL
            ORDER BY latest_total_value DESC
            LIMIT %s
            """,
            (top_n,))
        return {
            'quarters': quarters,
            'presets': [{
                'id': p[0], 'label': p[1],
                'latest_quarter': p[2], 'latest_total_value': p[3],
            } for p in presets],
            'top_filers': [{
                'id': f[0], 'name': f[1],
                'latest_quarter': f[2], 'latest_total_value': f[3],
            } for f in top_filers],
        }

    def search_filers(self, q: str, page: int = 1, page_size: int = 20) -> Dict:
        """仅搜索预设机构。"""
        q = (q or '').strip().upper()
        if not q:
            return {'items': [], 'total': 0, 'page': page, 'page_size': page_size}
        pattern = f"%{q}%"
        total = self.db_manager.fetch_one(
            """
            SELECT COUNT(*) FROM form13f_filers
            WHERE is_preset = TRUE AND name_normalized LIKE %s
            """,
            (pattern,))[0]
        rows = self.db_manager.fetch_all(
            """
            SELECT id, name_display, cik, latest_quarter, latest_total_value, is_preset
            FROM form13f_filers
            WHERE is_preset = TRUE AND name_normalized LIKE %s
            ORDER BY latest_total_value DESC NULLS LAST
            LIMIT %s OFFSET %s
            """,
            (pattern, page_size, (page - 1) * page_size))
        return {
            'items': [{
                'id': r[0], 'name': r[1], 'cik': r[2],
                'latest_quarter': r[3], 'latest_total_value': r[4], 'is_preset': r[5],
            } for r in rows],
            'total': total, 'page': page, 'page_size': page_size,
        }

    def get_filer_detail(self, filer_id: int) -> Optional[Dict]:
        filer = self.db_manager.fetch_one(
            "SELECT id, name_display, cik, is_preset FROM form13f_filers WHERE id = %s",
            (filer_id,))
        if not filer:
            return None
        snaps = self.db_manager.fetch_all(
            """
            SELECT quarter, total_value, position_count, sector_alloc,
                   top20_holdings, top5_buys, top5_sells
            FROM form13f_snapshots WHERE filer_id = %s ORDER BY quarter
            """,
            (filer_id,))
        quarters = [{
            'quarter': s[0], 'total_value': s[1], 'position_count': s[2],
            'sector_alloc': s[3] or {}, 'top20_holdings': s[4] or [],
            'top5_buys': s[5] or [], 'top5_sells': s[6] or [],
        } for s in snaps]
        self._attach_prev_shares(filer_id, quarters)
        return {
            'id': filer[0], 'name': filer[1], 'cik': filer[2], 'is_preset': filer[3],
            'quarters': quarters,
        }

    def _attach_prev_shares(self, filer_id: int, quarters: List[Dict]) -> None:
        q_list = [q['quarter'] for q in quarters]
        prev_of = {q_list[i]: q_list[i - 1] for i in range(1, len(q_list))}
        cusips = {
            (h.get('cusip') or '')
            for q in quarters
            for h in (q.get('top20_holdings') or [])
        }
        prev_quarters = list(set(prev_of.values()))
        prev_map: Dict[Tuple[str, str, str], Optional[int]] = {}
        if cusips and prev_quarters:
            rows = self.db_manager.fetch_all(
                """
                SELECT quarter, cusip, COALESCE(putcall, ''), shares
                FROM form13f_holdings
                WHERE filer_id = %s
                  AND quarter = ANY(%s)
                  AND cusip = ANY(%s)
                """,
                (filer_id, prev_quarters, list(cusips)))
            for quarter, cusip, putcall, shares in rows:
                prev_map[(quarter, cusip, putcall or '')] = shares
        for q in quarters:
            pq = prev_of.get(q['quarter'])
            for h in (q.get('top20_holdings') or []):
                if not pq:
                    h['prev_shares'] = None
                    continue
                key = (pq, h.get('cusip') or '', h.get('putcall') or '')
                h['prev_shares'] = prev_map.get(key)
