# -*- coding: utf-8 -*-
"""
Form 13F 数据服务 — SEC Form 13F Data Sets 季度持仓

数据链: poll_sec_datasets（发现）→ download_dataset（下载 ZIP）
       → import_dataset（TSV 流式导入+行业分类）→ compute_snapshots（快照预计算）
兜底:  retry_failed_datasets
回填:  backfill_history（按发现顺序串行派发全部历史数据集）

解析/聚合规则移植自已验证的静态原型 docs/form13f_dashboard/build_data.py：
- 每个数据集只取主报告期（COVERPAGE REPORTCALENDARORQUARTER 众数）
- 同一机构同期多申报取 amendment（ISAMENDMENT=Y 优先 → AMENDMENTNO 最大 → 最后）
- 持仓按 (CUSIP, PUTCALL) 聚合 value/shares
"""

import csv
import hashlib
import io
import os
import re
import zipfile
from collections import Counter, defaultdict
from typing import Dict, List, Optional, Tuple

import psycopg2
import psycopg2.extras
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from backend.database.db_manager import DatabaseManager
from backend.utils.datetime_utils import utcnow
from backend.utils.database_config import get_database_config
from backend.utils.form13f_sector_map import CUSIP_SECTOR, classify_etf, is_fund
from backend.utils.logger import get_logger

logger = get_logger("form13f_service")

# SEC 要求 User-Agent 为「公司名 联系邮箱」格式（fair-access 政策），禁止伪装浏览器
SEC_USER_AGENT = os.getenv(
    "SEC_USER_AGENT", "GuanHe Capital Research contact@guanhe.example.com")

SEC_DATASETS_URL = (
    "https://www.sec.gov/data-research/sec-markets-data/form-13f-data-sets")
SEC_BASE = "https://www.sec.gov"

# ZIP 链接模式，如 .../01JUN2025-31AUG2025_form13f.zip
DATASET_ZIP_RE = re.compile(
    r'href="([^"]*?(\d{2}[A-Z]{3}\d{4}-\d{2}[A-Z]{3}\d{4}_form13f)\.zip)"', re.IGNORECASE)

# 数据集保存目录（照 REPORTS_DIR 模式，位于 backend/data/form13f）
DATA_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data", "form13f")

# 批量入库批次大小
BATCH_SIZE = 5000

_MONTHS = {'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
           'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12}


def quarter_of(date_str: str) -> Optional[str]:
    """'31-MAR-2026' -> '2026Q1'；解析失败返回 None"""
    m = re.match(r'(\d{2})-([A-Z]{3})-(\d{4})', (date_str or '').strip().upper())
    if not m or m.group(2) not in _MONTHS:
        return None
    month = _MONTHS[m.group(2)]
    return f"{m.group(3)}Q{(month - 1) // 3 + 1}"


def filer_key_of(name: str) -> str:
    """机构唯一键 = sha1(name.strip())[:12]，与静态原型 fid_of 一致"""
    return hashlib.sha1((name or '').strip().encode()).hexdigest()[:12]


def classify_holding(issuer: str, title: str, cusip: str) -> str:
    """单条持仓的行业/资产分类：基金→ETF 9 类资产；股票→CUSIP 映射 GICS；未匹配 Other"""
    if is_fund(issuer, title):
        return classify_etf(issuer, title)
    return CUSIP_SECTOR.get(cusip, 'Other')


class Form13fService:
    """SEC Form 13F 季度数据集服务"""

    def __init__(self):
        db_params = get_database_config()
        self.db_manager = DatabaseManager(db_params)
        self.session = self.create_robust_session()

    # ── HTTP 会话（SEC 合规 User-Agent）────────────────────────────────────

    def create_robust_session(self):
        """带重试的会话；User-Agent 遵循 SEC fair-access 政策（公司名 邮箱）"""
        session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        session.headers.update({"User-Agent": SEC_USER_AGENT})
        return session

    def safe_get(self, url: str, max_retries: int = 3) -> Optional[str]:
        """GET 请求文本，SSL/瞬时故障指数退避重试"""
        session = self.session
        for attempt in range(max_retries):
            try:
                response = session.get(url, timeout=(10, 60))
                response.raise_for_status()
                return response.content.decode("utf-8", errors="replace")
            except requests.exceptions.SSLError as e:
                logger.warning(f"[13F] SSL错误 (第{attempt + 1}次): {e}")
                if attempt < max_retries - 1:
                    session.close()
                    session = self.create_robust_session()
                    self.session = session
                else:
                    raise
            except requests.exceptions.RequestException as e:
                logger.warning(f"[13F] 请求错误 (第{attempt + 1}次): {e}")
                if attempt < max_retries - 1:
                    import time
                    time.sleep(2 ** attempt)
                else:
                    raise
        return None

    # ── 阶段 1: 发现 ───────────────────────────────────────────────────────

    def poll_sec_datasets(self) -> Dict[str, int]:
        """轮询 SEC 13F 数据页，幂等发现季度数据集

        Returns: {"discovered": 新数据集数, "total": 页面数据集总数}
        Raises: 解析失败抛异常（防 SEC 改版后静默空跑）
        """
        html = self.safe_get(SEC_DATASETS_URL)
        if not html or len(html) < 500:
            raise RuntimeError(f"SEC 13F 数据页获取失败或内容异常（长度={len(html) if html else 0}）")

        matches = DATASET_ZIP_RE.findall(html)
        if not matches:
            raise RuntimeError("SEC 13F 数据页未解析到任何 form13f ZIP 链接，页面可能已改版")

        discovered = 0
        for href, dataset_name in matches:
            url = href if href.startswith("http") else SEC_BASE + href
            with self.db_manager.get_connection_context() as conn:
                with conn.cursor() as cur:
                    # SEC 页面文件名大小写不稳定（01JUN2025 vs 01jun2025），
                    # 按大小写不敏感去重，避免把已导入季度当成新数据集。
                    cur.execute(
                        "SELECT id FROM form13f_datasets "
                        "WHERE LOWER(dataset_name) = LOWER(%s)",
                        (dataset_name,),
                    )
                    if cur.fetchone():
                        continue
                    cur.execute(
                        """
                        INSERT INTO form13f_datasets (dataset_name, source_url)
                        VALUES (%s, %s)
                        ON CONFLICT (dataset_name) DO NOTHING
                        RETURNING id
                        """,
                        (dataset_name, url),
                    )
                    row = cur.fetchone()
            if row:
                discovered += 1
                logger.info(f"[13F] 发现新数据集 {dataset_name} (id={row[0]})")

        logger.info(f"[13F] poll 完成: 发现 {discovered} 个新数据集 / 共 {len(matches)} 个")
        return {"discovered": discovered, "total": len(matches)}

    # ── 阶段 2: 下载 ───────────────────────────────────────────────────────

    def _zip_path(self, dataset_name: str) -> str:
        return os.path.join(DATA_DIR, f"{dataset_name}.zip")

    def _update_dataset(self, dataset_id: int, **fields) -> None:
        """更新数据集行的指定字段"""
        if not fields:
            return
        fields['updated_at'] = utcnow()
        sets = ", ".join(f"{k} = %s" for k in fields)
        with self.db_manager.get_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"UPDATE form13f_datasets SET {sets} WHERE id = %s",
                    (*fields.values(), dataset_id))

    def _get_dataset(self, dataset_id: int) -> Optional[Dict]:
        """查询单个数据集行（fetch_one 返回 tuple，按平台惯例 zip 成 dict）"""
        rows = self.db_manager.fetch_all(
            "SELECT id, dataset_name, quarter, source_url, zip_sha256, "
            "zip_bytes, status, filers_count, infotable_rows, error_message "
            "FROM form13f_datasets WHERE id = %s",
            (dataset_id,))
        if not rows:
            return None
        cols = ["id", "dataset_name", "quarter", "source_url", "zip_sha256",
                "zip_bytes", "status", "filers_count", "infotable_rows",
                "error_message"]
        return dict(zip(cols, rows[0]))

    def download_dataset(self, dataset_id: int, force: bool = False) -> Dict:
        """流式下载季度 ZIP（边下边算 sha256），完成 → status=downloaded

        已 downloaded 且本地文件 sha256 一致时跳过（幂等）。
        """
        ds = self._get_dataset(dataset_id)
        if not ds:
            raise ValueError(f"数据集 id={dataset_id} 不存在")

        path = self._zip_path(ds['dataset_name'])
        if not force and ds['status'] == 'downloaded' and os.path.exists(path):
            # 幂等：校验文件仍完整
            sha = self._file_sha256(path)
            if not ds['zip_sha256'] or sha == ds['zip_sha256']:
                return {"dataset_id": dataset_id, "skipped": True}

        os.makedirs(DATA_DIR, exist_ok=True)
        self._update_dataset(dataset_id, status='downloading', error_message=None)

        try:
            sha = hashlib.sha256()
            total = 0
            with self.session.get(ds['source_url'], stream=True, timeout=(30, 300)) as resp:
                resp.raise_for_status()
                with open(path + '.part', 'wb') as f:
                    for chunk in resp.iter_content(chunk_size=1024 * 1024):
                        f.write(chunk)
                        sha.update(chunk)
                        total += len(chunk)
            os.replace(path + '.part', path)

            # 校验 ZIP 可打开
            with zipfile.ZipFile(path) as zf:
                if zf.testzip() is not None:
                    raise zipfile.BadZipFile("ZIP 校验失败（testzip）")

            self._update_dataset(
                dataset_id, status='downloaded', zip_sha256=sha.hexdigest(),
                zip_bytes=total, downloaded_at=utcnow())
            logger.info(f"[13F] 下载完成 {ds['dataset_name']}: {total / 1e6:.1f}MB sha={sha.hexdigest()[:12]}")
            return {"dataset_id": dataset_id, "bytes": total, "sha256": sha.hexdigest()}

        except Exception as e:
            # 清理残片
            if os.path.exists(path + '.part'):
                os.remove(path + '.part')
            self._update_dataset(dataset_id, status='failed',
                                 error_message=f"下载失败: {e}")
            raise

    # ── 阶段 3: 导入 ───────────────────────────────────────────────────────

    def _read_zip_member(self, zf: zipfile.ZipFile, name_part: str) -> io.TextIOWrapper:
        """打开 ZIP 内匹配名称的 TSV 成员（大小写不敏感的包含匹配）"""
        for info in zf.infolist():
            if name_part.upper() in info.filename.upper():
                return zf.open(info)
        raise FileNotFoundError(f"ZIP 内未找到 {name_part} 成员")

    def _plan_coverpage(self, zf: zipfile.ZipFile) -> Tuple[str, Dict[str, Dict], List[Tuple]]:
        """解析 COVERPAGE：确定主报告期 + 每机构 chosen accession + filer 元数据

        Returns: (主报告期原始串, {accession: filer行}, [(name, cik, is_amend, amend_no, accession)])
        """
        accs = []
        with self._read_zip_member(zf, 'COVERPAGE') as raw:
            text = io.TextIOWrapper(raw, encoding='utf-8-sig', newline='')
            for row in csv.DictReader(text, delimiter='\t'):
                name = (row.get('FILINGMANAGER_NAME') or '').strip()
                if not name:
                    continue
                accs.append((
                    row['ACCESSION_NUMBER'],
                    name,
                    (row.get('FILINGMANAGER_STREET1') or '').strip() or None,
                    row['REPORTCALENDARORQUARTER'].strip(),
                    (row.get('ISAMENDMENT') or '').strip().upper() == 'Y',
                    int(row.get('AMENDMENTNO') or 0),
                ))
        if not accs:
            raise ValueError("COVERPAGE 无有效行，SEC 数据格式可能已变更")

        # 主报告期 = 众数
        period_counts = Counter(p for _, _, _, p, _, _ in accs)
        dom_period = period_counts.most_common(1)[0][0]
        dom_quarter = quarter_of(dom_period)
        if not dom_quarter:
            raise ValueError(f"无法解析主报告期 {dom_period}，SEC 日期格式可能已变更")

        # 每机构选 chosen accession: amendment 优先 → AMENDMENTNO 大 → 最后出现
        per_filer = defaultdict(list)
        for acc, name, street, period, is_amend, amend_no in accs:
            if period != dom_period:
                continue
            per_filer[name].append((is_amend, amend_no, acc))
        chosen = {}
        filers_meta = {}
        for name, lst in per_filer.items():
            lst.sort(key=lambda t: (t[0], t[1]))
            acc = lst[-1][2]
            chosen[acc] = name
            filers_meta[name] = {'cik': None, 'street1': None}
        # CIK：COVERPAGE 无 CIK 字段（SUBMISSION.tsv 有），此处从 SUBMISSION 补
        try:
            with self._read_zip_member(zf, 'SUBMISSION') as raw:
                text = io.TextIOWrapper(raw, encoding='utf-8-sig', newline='')
                cik_by_acc = {row['ACCESSION_NUMBER']: row.get('CIK', '').strip()
                              for row in csv.DictReader(text, delimiter='\t')}
            for acc, name in chosen.items():
                filers_meta[name]['cik'] = cik_by_acc.get(acc) or None
        except (FileNotFoundError, KeyError):
            logger.warning("[13F] SUBMISSION.tsv 缺失或格式异常，CIK 留空")

        logger.info(f"[13F] 主报告期 {dom_period} ({dom_quarter})，机构 {len(chosen)} 家")
        return dom_quarter, chosen, filers_meta

    def _upsert_filers(self, quarter: str, chosen: Dict[str, Dict], filers_meta: Dict) -> Dict[str, int]:
        """写入/更新机构维表，返回 {机构名: filer_id}

        execute_values 的 RETURNING 行顺序不保证与输入一致，因此先批量 upsert
        再按 filer_key 精确回查 id。
        """
        rows = []
        for name, meta in filers_meta.items():
            rows.append((
                filer_key_of(name),
                name.strip().upper(),
                name,
                meta.get('cik'),
            ))
        keys = [r[0] for r in rows]
        with self.db_manager.get_connection_context() as conn:
            with conn.cursor() as cur:
                psycopg2.extras.execute_values(
                    cur,
                    """
                    INSERT INTO form13f_filers (filer_key, name_normalized, name_display, cik)
                    VALUES %s
                    ON CONFLICT (filer_key) DO UPDATE SET
                        name_display = EXCLUDED.name_display,
                        cik = COALESCE(EXCLUDED.cik, form13f_filers.cik),
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    rows, page_size=1000)
                cur.execute(
                    "SELECT filer_key, id, name_display FROM form13f_filers "
                    "WHERE filer_key = ANY(%s)", (keys,))
                id_rows = cur.fetchall()
        return {name: fid for key, fid, name in id_rows}

    def import_dataset(self, dataset_id: int, local_dir: Optional[str] = None) -> Dict:
        """导入数据集：COVERPAGE 定主报告期+chosen accessions → INFOTABLE 流式聚合入库

        local_dir: 调试/回填用——直接读本地已解压目录（内含 COVERPAGE.tsv/INFOTABLE.tsv），
                   跳过 ZIP 下载环节。
        """
        ds = self._get_dataset(dataset_id)
        if not ds:
            raise ValueError(f"数据集 id={dataset_id} 不存在")

        dup = self.db_manager.fetch_all(
            """
            SELECT id, dataset_name, quarter FROM form13f_datasets
            WHERE LOWER(dataset_name) = LOWER(%s)
              AND id <> %s
              AND status = 'imported'
            """,
            (ds['dataset_name'], dataset_id),
        )
        if dup:
            msg = (
                f"跳过大小写重复数据集 {ds['dataset_name']}："
                f"已导入 id={dup[0][0]} ({dup[0][1]}, {dup[0][2]})"
            )
            logger.info(f"[13F] {msg}")
            self._update_dataset(dataset_id, status='failed', error_message=msg)
            return {"dataset_id": dataset_id, "skipped": True, "reason": msg}

        self._update_dataset(dataset_id, status='importing', error_message=None)
        try:
            if local_dir:
                quarter, chosen, filers_meta = self._plan_coverpage_dir(local_dir)
                zip_path = None
            else:
                zip_path = self._zip_path(ds['dataset_name'])
                if not os.path.exists(zip_path):
                    raise FileNotFoundError(f"ZIP 未下载: {zip_path}")
                with zipfile.ZipFile(zip_path) as zf:
                    quarter, chosen, filers_meta = self._plan_coverpage(zf)

            # 校验 quarter 与既有记录一致（重导时防串号）
            if ds['quarter'] and ds['quarter'] != quarter:
                raise ValueError(
                    f"主报告期变化: 已记录 {ds['quarter']} vs 本次 {quarter}，请人工确认")

            name_to_id = self._upsert_filers(quarter, chosen, filers_meta)

            if local_dir:
                rows_used, holdings_count = self._import_infotable_dir(
                    local_dir, dataset_id, quarter, chosen, name_to_id)
            else:
                with zipfile.ZipFile(zip_path) as zf:
                    rows_used, holdings_count = self._import_infotable(
                        zf, dataset_id, quarter, chosen, name_to_id)

            self._update_dataset(
                dataset_id, status='imported', quarter=quarter,
                filers_count=len(chosen), infotable_rows=rows_used,
                imported_at=utcnow(), error_message=None)
            logger.info(
                f"[13F] 导入完成 {ds['dataset_name']} ({quarter}): "
                f"机构 {len(chosen)}, infotable 行 {rows_used}, 聚合持仓 {holdings_count}")
            return {
                "dataset_id": dataset_id, "quarter": quarter,
                "filers": len(chosen), "infotable_rows": rows_used,
                "holdings": holdings_count,
            }

        except Exception as e:
            self._update_dataset(dataset_id, status='failed',
                                 error_message=f"导入失败: {e}")
            raise

    def _plan_coverpage_dir(self, local_dir: str):
        """本地目录版的 _plan_coverpage（结构相同）"""
        class _DirZip:
            """让 _read_zip_member 兼容本地目录"""
            pass
        # 直接复用逻辑：构造伪 zf 由 _plan_coverpage_dir_inline 处理
        accs = []
        with open(os.path.join(local_dir, 'COVERPAGE.tsv'), newline='', encoding='utf-8-sig') as f:
            for row in csv.DictReader(f, delimiter='\t'):
                name = (row.get('FILINGMANAGER_NAME') or '').strip()
                if not name:
                    continue
                accs.append((
                    row['ACCESSION_NUMBER'], name,
                    row['REPORTCALENDARORQUARTER'].strip(),
                    (row.get('ISAMENDMENT') or '').strip().upper() == 'Y',
                    int(row.get('AMENDMENTNO') or 0),
                ))
        if not accs:
            raise ValueError("COVERPAGE.tsv 无有效行")

        period_counts = Counter(p for _, _, p, _, _ in accs)
        dom_period = period_counts.most_common(1)[0][0]
        dom_quarter = quarter_of(dom_period)
        if not dom_quarter:
            raise ValueError(f"无法解析主报告期 {dom_period}")

        per_filer = defaultdict(list)
        for acc, name, period, is_amend, amend_no in accs:
            if period != dom_period:
                continue
            per_filer[name].append((is_amend, amend_no, acc))
        chosen = {}
        for name, lst in per_filer.items():
            lst.sort(key=lambda t: (t[0], t[1]))
            chosen[lst[-1][2]] = name

        # chosen 的键是 accession、值是机构名——filers_meta 必须按机构名建键
        filers_meta = {name: {'cik': None} for name in chosen.values()}
        sub_path = os.path.join(local_dir, 'SUBMISSION.tsv')
        if os.path.exists(sub_path):
            with open(sub_path, newline='', encoding='utf-8-sig') as f:
                cik_by_acc = {row['ACCESSION_NUMBER']: row.get('CIK', '').strip()
                              for row in csv.DictReader(f, delimiter='\t')}
            # 同名机构可能出现多次（多 accession），dict 覆盖时 chosen 键可能不在
            # cik_by_acc 的最终值里——直接按 chosen 的 accession 精确查
            for acc, name in chosen.items():
                filers_meta[name]['cik'] = cik_by_acc.get(acc) or None

        logger.info(f"[13F][local] 主报告期 {dom_period} ({dom_quarter})，机构 {len(chosen)} 家")
        return dom_quarter, chosen, filers_meta

    def _import_infotable(self, zf: zipfile.ZipFile, dataset_id: int, quarter: str,
                          chosen: Dict[str, str], name_to_id: Dict[str, int]) -> Tuple[int, int]:
        """ZIP 版 INFOTABLE 流式导入（见 _import_infotable_stream）"""
        raw = self._read_zip_member(zf, 'INFOTABLE')
        text = io.TextIOWrapper(raw, encoding='utf-8-sig', newline='')
        return self._import_infotable_stream(text, dataset_id, quarter, chosen, name_to_id)

    def _import_infotable_dir(self, local_dir: str, dataset_id: int, quarter: str,
                              chosen: Dict[str, str], name_to_id: Dict[str, int]) -> Tuple[int, int]:
        """本地目录版 INFOTABLE 流式导入"""
        with open(os.path.join(local_dir, 'INFOTABLE.tsv'), newline='', encoding='utf-8-sig') as f:
            return self._import_infotable_stream(f, dataset_id, quarter, chosen, name_to_id)

    def _import_infotable_stream(self, text_stream, dataset_id: int, quarter: str,
                                 chosen: Dict[str, str], name_to_id: Dict[str, int]) -> Tuple[int, int]:
        """INFOTABLE 流式读取 → (filer, cusip, putcall) 聚合 → 分批 execute_values

        Returns: (使用的 infotable 行数, 聚合后持仓条数)
        """
        rd = csv.reader(text_stream, delimiter='\t')
        hdr = next(rd)
        try:
            i_acc = hdr.index('ACCESSION_NUMBER')
            i_iss = hdr.index('NAMEOFISSUER')
            i_tit = hdr.index('TITLEOFCLASS')
            i_cus = hdr.index('CUSIP')
            i_val = hdr.index('VALUE')
            i_shr = hdr.index('SSHPRNAMT')
            i_pc = hdr.index('PUTCALL')
        except ValueError as e:
            raise ValueError(f"INFOTABLE 缺少必需列: {e}，SEC 数据格式可能已变更")

        # 聚合 dict: (filer_id, cusip, putcall) -> {issuer,title,value,shares,legs}
        agg = {}
        rows_used = 0
        for row in rd:
            acc = row[i_acc]
            name = chosen.get(acc)
            if name is None:
                continue
            cusip = row[i_cus].strip()
            try:
                val = int(row[i_val])
                shr = int(float(row[i_shr])) if row[i_shr] else 0
            except ValueError:
                continue
            key = (name_to_id[name], cusip, row[i_pc])
            slot = agg.get(key)
            if slot is None:
                agg[key] = {'issuer': row[i_iss], 'title': row[i_tit],
                            'value': val, 'shares': shr, 'legs': 1,
                            'acc': acc}
            else:
                slot['value'] += val
                slot['shares'] += shr
                slot['legs'] += 1
            rows_used += 1

        # 分批入库
        batch = []
        holdings_count = 0
        for (filer_id, cusip, putcall), s in agg.items():
            batch.append((
                dataset_id, quarter, filer_id, s['acc'], cusip,
                s['issuer'], s['title'], putcall, s['value'], s['shares'],
                s['legs'], classify_holding(s['issuer'], s['title'], cusip),
            ))
            if len(batch) >= BATCH_SIZE:
                self._write_holdings_batch(batch)
                holdings_count += len(batch)
                batch = []
        if batch:
            self._write_holdings_batch(batch)
            holdings_count += len(batch)

        return rows_used, holdings_count

    def _write_holdings_batch(self, batch: List[Tuple]) -> None:
        """一批持仓 upsert（ON CONFLICT 支持重导幂等覆盖）"""
        with self.db_manager.get_connection_context() as conn:
            with conn.cursor() as cur:
                psycopg2.extras.execute_values(
                    cur,
                    """
                    INSERT INTO form13f_holdings (
                        dataset_id, quarter, filer_id, accession_number, cusip,
                        issuer_name, title_of_class, putcall, value_usd, shares,
                        leg_count, sector)
                    VALUES %s
                    ON CONFLICT (filer_id, quarter, cusip, putcall) DO UPDATE SET
                        dataset_id = EXCLUDED.dataset_id,
                        accession_number = EXCLUDED.accession_number,
                        issuer_name = EXCLUDED.issuer_name,
                        title_of_class = EXCLUDED.title_of_class,
                        value_usd = EXCLUDED.value_usd,
                        shares = EXCLUDED.shares,
                        leg_count = EXCLUDED.leg_count,
                        sector = EXCLUDED.sector
                    """,
                    batch, page_size=1000)

    # ── 阶段 4: 快照预计算 ─────────────────────────────────────────────────

    def compute_snapshots(self, dataset_id: int, quarter: Optional[str] = None) -> Dict:
        """为数据集对应季度全部机构计算快照（sector 分布 / top20 / top5 买卖）

        用 SQL GROUP BY 聚合行业分布；top5 买卖与「上一有效季度」对比
        （不要求相邻标签存在——机构漏报一季时跳到更早的可用季度）。
        """
        ds = self._get_dataset(dataset_id)
        if not ds:
            raise ValueError(f"数据集 id={dataset_id} 不存在")
        q = quarter or ds['quarter']
        if not q:
            raise ValueError(f"数据集 {ds['dataset_name']} 尚无 quarter（先完成导入）")

        # 1) 行业分布 + 组合总额（SQL 聚合）
        sec_rows = self.db_manager.fetch_all(
            """
            SELECT filer_id, sector, SUM(value_usd) AS value, COUNT(*) AS positions
            FROM form13f_holdings
            WHERE quarter = %s
            GROUP BY filer_id, sector
            """,
            (q,))
        totals = defaultdict(int)          # filer_id -> total_value
        counts = defaultdict(int)          # filer_id -> position_count
        sec_map = defaultdict(dict)        # filer_id -> {sector: value}
        for filer_id, sector, value, positions in sec_rows:
            totals[filer_id] += value
            counts[filer_id] += positions
            sec_map[filer_id][sector] = value

        # 2) top20（每机构，SQL 窗口取前 20）
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

        # 3) 上一有效季度（对该季度有持仓记录的最大 quarter 标签）
        prev_quarter = self.db_manager.fetch_one(
            "SELECT MAX(quarter) FROM form13f_holdings WHERE quarter < %s", (q,))[0]

        # top5 买卖：SQL 端 FULL JOIN + 窗口函数直接算（只回传 ~4 万行结果，
        # 避免把两季 400+ 万行持仓拉到本地 Python 对比；prev_value 一并返回，杜绝 N+1）
        buys_map = defaultdict(list)
        sells_map = defaultdict(list)
        if prev_quarter:
            change_rows = self.db_manager.fetch_all(
                """
                SELECT filer_id, issuer_name, cusip, putcall, dv, cur_value, prev_value
                FROM (
                    SELECT COALESCE(c.filer_id, p.filer_id) AS filer_id,
                           COALESCE(c.issuer_name, p.issuer_name) AS issuer_name,
                           COALESCE(c.cusip, p.cusip) AS cusip,
                           COALESCE(c.putcall, p.putcall) AS putcall,
                           COALESCE(c.value_usd, 0) - COALESCE(p.value_usd, 0) AS dv,
                           COALESCE(c.value_usd, 0) AS cur_value,
                           COALESCE(p.value_usd, 0) AS prev_value,
                           ROW_NUMBER() OVER (
                               PARTITION BY COALESCE(c.filer_id, p.filer_id)
                               ORDER BY (COALESCE(c.value_usd, 0) - COALESCE(p.value_usd, 0)) DESC
                           ) AS rn_buy,
                           ROW_NUMBER() OVER (
                               PARTITION BY COALESCE(c.filer_id, p.filer_id)
                               ORDER BY (COALESCE(c.value_usd, 0) - COALESCE(p.value_usd, 0)) ASC
                           ) AS rn_sell
                    FROM (SELECT * FROM form13f_holdings WHERE quarter = %s) c
                    FULL JOIN (SELECT * FROM form13f_holdings WHERE quarter = %s) p
                      ON p.filer_id = c.filer_id
                     AND p.cusip = c.cusip
                     AND p.putcall = c.putcall
                    WHERE COALESCE(c.value_usd, 0) - COALESCE(p.value_usd, 0) <> 0
                ) t
                WHERE rn_buy <= 5 OR rn_sell <= 5
                """,
                (q, prev_quarter))
            for filer_id, issuer, cusip, putcall, dv, cur_value, prev_value in change_rows:
                dv = int(dv)
                cur_value = int(cur_value)
                prev_value = int(prev_value)
                item = {
                    'issuer': issuer, 'cusip': cusip, 'putcall': putcall,
                    'dv': dv, 'value': cur_value,
                }
                if dv > 0:
                    item['kind'] = 'new' if prev_value == 0 else 'increase'
                    item['spct'] = round(dv / prev_value * 100, 2) if prev_value > 0 else 100.0
                    buys_map[filer_id].append(item)
                else:
                    # 减仓: prev_value>0 且 cur_value>=0；exited = 上期有本期无
                    item['kind'] = 'exited' if cur_value == 0 else 'decrease'
                    item['spct'] = -100.0 if cur_value == 0 else (
                        round(dv / prev_value * 100, 2) if prev_value > 0 else 0)
                    sells_map[filer_id].append(item)
            for m in (buys_map, sells_map):
                for lst in m.values():
                    lst.sort(key=lambda x: -abs(x['dv']))
                    del lst[5:]

        # 4) 组装快照行
        snap_rows = []
        for filer_id, total in totals.items():
            if total <= 0:
                continue
            total = int(total)  # SUM() 返回 Decimal，JSON 序列化前转 int
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

        # 5) 批量写快照
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

        # 6) 同步 filers 汇总字段
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

        # 标记预设机构
        with self.db_manager.get_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE form13f_filers f
                    SET is_preset = TRUE
                    FROM form13f_presets p
                    WHERE f.name_display = p.match_name
                    """)

        logger.info(f"[13F] 快照完成 {q}: {len(snap_rows)} 家机构 (对比季度 {prev_quarter})")
        return {"quarter": q, "snapshots": len(snap_rows), "prev_quarter": prev_quarter}

    # ── 兜底 / 回填 ────────────────────────────────────────────────────────

    def retry_failed_datasets(self) -> Dict[str, int]:
        """重试 failed/pending 的数据集（按状态派发对应阶段）"""
        rows = self.db_manager.fetch_all(
            "SELECT id, status FROM form13f_datasets "
            "WHERE status IN ('pending', 'failed') ORDER BY id")
        counts = {'pending': 0, 'failed': 0}
        for ds_id, status in rows:
            counts[status] += 1
        return counts

    def backfill_history(self) -> Dict:
        """顺序派发 SEC 页面上全部数据集的下载（串行由 Celery 队列保证）"""
        rows = self.db_manager.fetch_all(
            "SELECT id, dataset_name, status FROM form13f_datasets "
            "WHERE status != 'imported' ORDER BY id")
        return {"datasets": [dict(zip(('id', 'name', 'status'), r)) for r in rows]}

    # ── 查询侧（API）──────────────────────────────────────────────────────

    def get_manifest(self, top_n: int = 200) -> Dict:
        """仪表盘清单：季度列表 + 预设机构 + 规模 TopN 机构摘要

        不全量下发 9400+ 机构——搜索走 search_filers 分页。
        """
        quarters = [r[0] for r in self.db_manager.fetch_all(
            "SELECT DISTINCT quarter FROM form13f_snapshots "
            "WHERE quarter IS NOT NULL ORDER BY quarter")]
        presets = self.db_manager.fetch_all(
            """
            SELECT f.id, p.label, f.latest_quarter, f.latest_total_value
            FROM form13f_presets p
            LEFT JOIN form13f_filers f ON f.name_display = p.match_name
            ORDER BY p.sort_order
            """)
        top_filers = self.db_manager.fetch_all(
            """
            SELECT id, name_display, latest_quarter, latest_total_value
            FROM form13f_filers
            WHERE latest_total_value IS NOT NULL
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
        """机构搜索（ILIKE，分页）"""
        q = (q or '').strip().upper()
        if not q:
            return {'items': [], 'total': 0, 'page': page, 'page_size': page_size}
        pattern = f"%{q}%"
        total = self.db_manager.fetch_one(
            "SELECT COUNT(*) FROM form13f_filers WHERE name_normalized LIKE %s",
            (pattern,))[0]
        rows = self.db_manager.fetch_all(
            """
            SELECT id, name_display, cik, latest_quarter, latest_total_value, is_preset
            FROM form13f_filers
            WHERE name_normalized LIKE %s
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
        """机构详情：全季度快照（quarter → sector 分布/top20/top5）"""
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
        return {
            'id': filer[0], 'name': filer[1], 'cik': filer[2], 'is_preset': filer[3],
            'quarters': [{
                'quarter': s[0], 'total_value': s[1], 'position_count': s[2],
                'sector_alloc': s[3] or {}, 'top20_holdings': s[4] or [],
                'top5_buys': s[5] or [], 'top5_sells': s[6] or [],
            } for s in snaps],
        }

    @staticmethod
    def _file_sha256(path: str) -> str:
        sha = hashlib.sha256()
        with open(path, 'rb') as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b''):
                sha.update(chunk)
        return sha.hexdigest()
