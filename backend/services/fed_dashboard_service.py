# -*- coding: utf-8 -*-
"""
央行 Dashboard 服务 — 美联储 FOMC 信息聚合

独立管线，从 federalreserve.gov 官网抓取 FOMC 声明/会议纪要原文，
regex 结构化抽取利率决议（区间/票型/异议），并通过 LLM 生成中文概括；
轮询 press_all.xml RSS 维护 Fed updates 流。

数据链路：
  RSS (press_all.xml) 发现 → fomc_documents(pending)
  → fetch_document (bs4 清洗) → fomc_documents(fetched)
  → extract_decision (仅 statement) → fomc_decisions
  → generate_summary (LLM) → fomc_summaries

V1 仅 FED；所有表 central_bank 字段预留央行横向扩展。
原文/抽取/摘要分表：三者生命周期不同（原文不可变、regex 规则可升级重跑、
LLM 模板可升级重生成），分表独立失效重算。
"""

import asyncio
import hashlib
import json
import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import feedparser
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from backend.database.db_manager import DatabaseManager
from backend.utils.database_config import get_database_config
from backend.utils.logger import get_logger

logger = get_logger("fed_dashboard_service")

# ── 常量 ─────────────────────────────────────────────────────────────────

FED_RSS_URL = "https://www.federalreserve.gov/feeds/press_all.xml"
FED_BASE = "https://www.federalreserve.gov"

# LLM 概括模板（max_tokens/provider/model 由 llm_prompt_templates 配置）
FOMC_SUMMARY_TEMPLATE = "fomc_summary_v1"

# RSS 过滤：分类含 Monetary Policy 或标题含 FOMC/利率关键词（并集，防 category 缺失）
MONETARY_CATEGORY_KEYWORDS = ("monetary policy",)
MONETARY_TITLE_KEYWORDS = ("fomc", "federal funds", "interest rate", "open market")

# 纪要 URL 固定模式（会议末日）
MINUTES_URL_PATTERN = re.compile(r"fomcminutes(\d{8})\.htm")

# 声明 URL（前缀 monetary 无规律，必须由 RSS 发现）
STATEMENT_URL_PATTERN = re.compile(r"/monetary(\d{8})a\.htm")

# ── 结构化抽取 regex（regex_v1）───────────────────────────────────────────
# Unicode 规范化: 老声明用 U+2011 non-breaking hyphen (‑), 新声明用普通 hyphen/en-dash,
# smart quotes 等——抽取前统一归一，regex 只需处理 ASCII
_HYPHEN_VARIANTS = str.maketrans({
    "‐": "-", "‑": "-", "‒": "-", "–": "-",
    "—": "-", "−": "-",
    " ": " ", " ": " ", " ": " ",
    "‘": "'", "’": "'", "“": '"', "”": '"',
})


def _normalize_text(s: str) -> str:
    return s.translate(_HYPHEN_VARIANTS)


# 利率区间数字形态（Fed 惯用分数记法，也可能小数）:
#   3-1/2 / 3-3/4 / 3.50 / 3 — 捕获原始串交 _frac_to_float 解析
_RATE_NUM = r"(\d+(?:[.,]\d+)?(?:\s*-\s*\d+/\d|\s*-\s*\d+(?:\.\d+)?)?|\d+/\d)"
# "maintain the target range ... at 3-1/2 to 3-3/4 percent"
# "lower the target range ... by 1/4 percentage point, to 4-1/4 to 4-1/2 percent"
RE_RATE_RANGE = re.compile(
    r"(maintain|raise|lower|increase|reduce)\s+the\s+target\s+range\s+for\s+the\s+"
    r"federal\s+funds\s+rate[^.;]*?\b(?:at|to)\s+"
    + _RATE_NUM + r"\s*(?:percent\s*)?to\s*"
    + _RATE_NUM + r"\s*(?:percent|percentage)",
    re.IGNORECASE,
)
# "approved ... by a 9 - 3 vote" / "9-3"（normalize 后统一为 ASCII hyphen）
RE_VOTE_SPLIT = re.compile(
    r"by\s+an?\s+(\d+)\s*-\s*(\d+)\s+vote", re.IGNORECASE
)
# "Voting against the monetary policy action were X, Y, and Z, who preferred to raise/lower ..."
# 名单内含 "John C." 式中名缩写，结尾不能用裸 \. —— 用 (?=\s*Voting|,\s+who|\.(\s|$)) 终止锚
RE_DISSENT_BLOCK = re.compile(
    r"Voting\s+against\s+(?:this\s+action|the\s+monetary\s+policy\s+action)\s+"
    r"(?:were|was)\s+(.+?)(?=\.\s+(?:For\s+media|Consistent|The\s+Committee|Implementation)|$)",
    re.IGNORECASE | re.DOTALL,
)
# 组内: "姓名列表, who <理由>" — 多组异议用 "; and" 分隔, 每组各带理由从句
RE_DISSENT_GROUP = re.compile(
    r"(?:^|;\s+and\s+)(.+?),\s+who\s+(.+?)(?=;\s+and\s+|$)",
    re.IGNORECASE | re.DOTALL,
)
# "Voting for this action: A, B, and C." / "Voting for ... were A, Chair; B, Vice Chair; ..."
# （统计赞成人数用；同样防中名缩写截断——终止锚必须是句号+大写句首或结尾）
RE_VOTE_FOR = re.compile(
    r"Voting\s+for\s+(?:this\s+action|the\s+monetary\s+policy\s+action)\s*(?:were|:)\s*"
    r"(.+?)(?=\.\s+(?:Voting|Consistent|For\s+media)|$)",
    re.IGNORECASE | re.DOTALL,
)

# HTML 清洗锚点（bs4_v1）
STATEMENT_START_ANCHOR = "For release at"
MINUTES_START_ANCHOR = "Minutes of the Federal Open Market Committee"
END_ANCHORS = ("Last Update:", "Last Update ")


def _frac_to_float(s: str) -> float:
    """'3-1/2' → 3.5, '3-3/4' → 3.75, '3.75' → 3.75, '5' → 5.0"""
    s = s.strip().replace(" ", "").replace(",", ".")
    m = re.match(r"^(\d+)(?:-(?:(\d+)/(\d+)|(\d+(?:\.\d+)?)))?$", s)
    if not m:
        try:
            return float(s)
        except ValueError:
            return 0.0
    whole = float(m.group(1))
    if m.group(2):  # X-Y/Z 分数形式（Fed 惯用）
        return whole + float(m.group(2)) / float(m.group(3))
    if m.group(4):  # X-Y 或 X-Y.ZZ 小数增量形式（如 3-75 表 3.75）
        frac = m.group(4)
        return whole + float("0." + frac) if "." not in frac else whole + float(frac) / (10 ** len(frac.split(".")[1]))
    return whole


class FedDashboardService:
    """美联储 FOMC Dashboard 数据服务"""

    def __init__(self):
        db_params = get_database_config()
        self.db_manager = DatabaseManager(db_params)
        self.session = self.create_robust_session()

    # ── HTTP 会话（照 economic_events_service 模式）─────────────────────────

    def create_robust_session(self):
        """创建一个具有重试机制和超时设置的会话"""
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

        session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko)"
        })
        return session

    def safe_get(self, url: str, max_retries: int = 3) -> Optional[str]:
        """GET 请求文本，处理 SSL 错误与瞬时故障（指数退避）

        强制 UTF-8 解码：Fed 页面 HTTP 头无 charset，requests 会误判
        latin-1 导致 en-dash 变 mojibake、投票票型 regex 失配。
        """
        session = self.session
        for attempt in range(max_retries):
            try:
                response = session.get(url, timeout=(10, 30))
                response.raise_for_status()
                return response.content.decode("utf-8", errors="replace")
            except requests.exceptions.SSLError as e:
                logger.warning(f"[FOMC] SSL错误 (第{attempt + 1}次): {e}")
                if attempt < max_retries - 1:
                    session.close()
                    session = self.create_robust_session()
                    self.session = session
                    time.sleep(2 ** attempt)
                else:
                    raise
            except requests.exceptions.RequestException as e:
                logger.warning(f"[FOMC] 请求错误 (第{attempt + 1}次): {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    raise
        return None

    # ── RSS 轮询与文档发现 ─────────────────────────────────────────────────

    def poll_fed_updates(self) -> Dict[str, int]:
        """轮询 Fed press_all.xml：幂等写 fed_updates + 发现新 FOMC 文档

        Returns: {"updates_written": n, "docs_discovered": n}
        """
        xml_text = self.safe_get(FED_RSS_URL)
        if not xml_text:
            raise Exception(f"RSS 获取失败: {FED_RSS_URL}")

        feed = feedparser.parse(xml_text)
        if feed.bozo and not feed.entries:
            raise Exception(f"RSS 解析失败: {feed.bozo_exception}")

        updates_written = 0
        docs_discovered = 0

        for entry in feed.entries:
            link = (entry.get("link") or "").strip()
            title = (entry.get("title") or "").strip()
            if not link or not title:
                continue

            category = ""
            if entry.get("tags"):
                category = (entry["tags"][0].get("term") or "").strip()
            elif entry.get("category"):
                category = str(entry.get("category")).strip()

            is_mp = (
                any(k in category.lower() for k in MONETARY_CATEGORY_KEYWORDS)
                or any(k in title.lower() for k in MONETARY_TITLE_KEYWORDS)
            )

            pub_date = None
            if entry.get("published"):
                try:
                    from email.utils import parsedate_to_datetime
                    pub_date = parsedate_to_datetime(entry["published"])
                except Exception:
                    pub_date = None

            self._upsert_update(link, title, link, category, is_mp, pub_date)
            updates_written += 1

            # 货币政策类条目尝试发现 FOMC 文档
            if is_mp and self._discover_fomc_document(link, title, pub_date):
                docs_discovered += 1

        logger.info(
            f"[FOMC] RSS 轮询完成: {updates_written} 条 updates, "
            f"{docs_discovered} 个新文档"
        )
        return {"updates_written": updates_written, "docs_discovered": docs_discovered}

    def _upsert_update(self, guid, title, link, category, is_mp, pub_date):
        conn = self.db_manager.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO fed_updates
                        (central_bank, guid, title, link, category,
                         is_monetary_policy, pub_date)
                    VALUES ('FED', %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (central_bank, guid) DO UPDATE SET
                        title = EXCLUDED.title,
                        category = EXCLUDED.category,
                        is_monetary_policy = EXCLUDED.is_monetary_policy,
                        pub_date = EXCLUDED.pub_date,
                        fetched_at = CURRENT_TIMESTAMP
                    """,
                    (guid, title, link, category, is_mp, pub_date),
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _discover_fomc_document(
        self, link: str, title: str, pub_date: Optional[datetime]
    ) -> bool:
        """识别 RSS 条目是否为 FOMC 声明/纪要，是则 upsert fomc_documents(pending)

        - 声明: URL 模式 /monetary{YYYYMMDD}a.htm（前缀无规律，RSS 是唯一可靠发现渠道）
        - 纪要: 标题含 'Minutes' + 'FOMC'，URL 模式 fomcminutes{YYYYMMDD}.htm（日期=会议末日）
        """
        doc_type = None
        meeting_date = None

        m = STATEMENT_URL_PATTERN.search(link)
        if m and ("fomc" in title.lower() or "statement" in title.lower()):
            doc_type = "statement"
            ds = m.group(1)
            # 声明发布于会议末日；会议为两日，会议首日=末日-1（meeting_date 取首日）
            end = datetime.strptime(ds, "%Y%m%d").date()
            meeting_date = end.fromordinal(end.toordinal() - 1)

        if doc_type is None:
            m = MINUTES_URL_PATTERN.search(link)
            if m and "minutes" in title.lower() and "fomc" in title.lower():
                doc_type = "minutes"
                ds = m.group(1)
                end = datetime.strptime(ds, "%Y%m%d").date()
                meeting_date = end.fromordinal(end.toordinal() - 1)

        if doc_type is None:
            return False

        return self._upsert_document(
            doc_type=doc_type,
            meeting_date=meeting_date,
            title=title,
            source_url=link,
            published_at=pub_date,
        )

    def _upsert_document(
        self,
        doc_type: str,
        meeting_date,
        title: str,
        source_url: str,
        published_at: Optional[datetime],
    ) -> bool:
        """幂等写 fomc_documents；已 fetched 且 hash 匹配的行不重置状态。返回是否新插入"""
        conn = self.db_manager.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO fomc_documents
                        (central_bank, doc_type, meeting_date, title,
                         source_url, published_at)
                    VALUES ('FED', %s, %s, %s, %s, %s)
                    ON CONFLICT (central_bank, source_url) DO UPDATE SET
                        title = EXCLUDED.title,
                        published_at = COALESCE(EXCLUDED.published_at,
                                                fomc_documents.published_at),
                        updated_at = CURRENT_TIMESTAMP
                    RETURNING (xmax = 0) AS inserted
                    """,
                    (doc_type, meeting_date, title, source_url, published_at),
                )
                row = cur.fetchone()
                inserted = bool(row[0]) if row else False
            conn.commit()
            return inserted
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    # ── 文档抓取与清洗 ─────────────────────────────────────────────────────

    def fetch_document(self, doc_id: int) -> Dict[str, Any]:
        """抓取文档正文并清洗（bs4_v1）。hash 未变则跳过下游。

        清洗规则: 起点 'For release at'（声明）/ 'Minutes of the FOMC'（纪要），
        终点 'Last Update'；剔除 script/style/Share 按钮文本。
        """
        doc = self._get_document(doc_id)
        if not doc:
            raise Exception(f"文档不存在: {doc_id}")

        try:
            html = self.safe_get(doc["source_url"])
            if not html:
                raise Exception(f"页面获取失败: {doc['source_url']}")

            content, word_count = self._clean_html(html, doc["doc_type"])
            if not content or word_count < 50:
                raise Exception(
                    f"正文清洗结果异常 (words={word_count})，疑似页面结构变更"
                )

            content_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()
            unchanged = (
                doc["content_hash"] is not None
                and doc["content_hash"] == content_hash
            )

            conn = self.db_manager.get_connection()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE fomc_documents
                        SET raw_content = %s, word_count = %s, content_hash = %s,
                            fetch_status = 'fetched', error_message = NULL,
                            fetched_at = CURRENT_TIMESTAMP,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = %s
                        """,
                        (content, word_count, content_hash, doc_id),
                    )
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.close()

            result = {
                "doc_id": doc_id,
                "doc_type": doc["doc_type"],
                "unchanged": unchanged,
                "word_count": word_count,
            }
            logger.info(
                f"[FOMC] 文档抓取完成 id={doc_id} type={doc['doc_type']} "
                f"words={word_count} unchanged={unchanged}"
            )
            return result

        except Exception as e:
            self._mark_document_failed(doc_id, str(e))
            raise

    def _clean_html(self, html: str, doc_type: str) -> tuple:
        """bs4 清洗 Fed 页面 → (正文, 词数)"""
        soup = BeautifulSoup(html, "lxml")

        for tag in soup(["script", "style", "nav", "footer", "header"]):
            tag.decompose()

        # 优先取文章容器，取不到则全页
        container = soup.find("div", id="article") or soup.find("article") or soup
        text = container.get_text(separator=" ", strip=True)
        text = re.sub(r"\s+", " ", text)

        # Share 按钮文本混入正文首句（实测发现），显式剔除
        text = text.replace("Share ", "", 1) if text.startswith("Share ") else text

        start_anchor = (
            STATEMENT_START_ANCHOR if doc_type == "statement"
            else MINUTES_START_ANCHOR
        )
        s = text.find(start_anchor)
        if s < 0:
            s = 0

        e = len(text)
        for anchor in END_ANCHORS:
            j = text.find(anchor, s)
            if j > 0:
                e = min(e, j)

        content = text[s:e].strip()
        return content, len(content.split())

    def _get_document(self, doc_id: int) -> Optional[Dict]:
        rows = self.db_manager.fetch_all(
            "SELECT id, doc_type, meeting_date, title, source_url, "
            "content_hash, fetch_status "
            "FROM fomc_documents WHERE id = %s",
            (doc_id,),
        )
        if not rows:
            return None
        cols = ["id", "doc_type", "meeting_date", "title", "source_url",
                "content_hash", "fetch_status"]
        return dict(zip(cols, rows[0]))

    def _mark_document_failed(self, doc_id: int, message: str):
        conn = self.db_manager.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE fomc_documents
                    SET fetch_status = 'failed', error_message = %s,
                        fetched_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                    """,
                    (message[:2000], doc_id),
                )
            conn.commit()
        except Exception:
            conn.rollback()
        finally:
            conn.close()

    # ── 结构化抽取（仅 statement，regex_v1）─────────────────────────────────

    def extract_decision(self, doc_id: int) -> Dict[str, Any]:
        """从声明原文抽取利率决议结构化数据 → fomc_decisions

        抽取失败的字段落 NULL + error_message，不抛错（卡片降级为原文链接）。
        """
        doc = self._get_document(doc_id)
        if not doc or doc["fetch_status"] != "fetched":
            raise Exception(f"文档未就绪: {doc_id}")

        rows = self.db_manager.fetch_all(
            "SELECT raw_content FROM fomc_documents WHERE id = %s", (doc_id,)
        )
        text = _normalize_text(rows[0][0]) if rows and rows[0][0] else ""

        result: Dict[str, Any] = {
            "meeting_date": doc["meeting_date"],
            "decision_date": None,
            "rate_low_bp": None,
            "rate_high_bp": None,
            "decision_direction": None,
            "change_bp": None,
            "vote_for": None,
            "vote_against": None,
            "dissenters": [],
            "error_fields": [],
        }

        # 利率区间
        m = RE_RATE_RANGE.search(text)
        if m:
            action = m.group(1).lower()
            low = _frac_to_float(m.group(2))
            high = _frac_to_float(m.group(3))
            if high < low:
                low, high = high, low
            result["rate_low_bp"] = int(round(low * 100))
            result["rate_high_bp"] = int(round(high * 100))
            result["decision_direction"] = {
                "maintain": "hold",
                "raise": "hike", "increase": "hike",
                "lower": "cut", "reduce": "cut",
            }.get(action)
        else:
            result["error_fields"].append("rate_range")

        # 票型: 优先 "by a 9-3 vote"
        m = RE_VOTE_SPLIT.search(text)
        if m:
            result["vote_for"] = int(m.group(1))
            result["vote_against"] = int(m.group(2))
        else:
            # 回退: 从 Voting for 名单数人数 + Voting against 名单数人数
            mf = RE_VOTE_FOR.search(text)
            dissenters = self._parse_dissenters(text)
            if mf:
                names = self._split_names(mf.group(1))
                result["vote_for"] = len(names)
            if dissenters:
                result["vote_against"] = len(dissenters)
            if result["vote_for"] is None:
                result["error_fields"].append("vote")

        # 异议者及方向（多组解析: 每组名单 + 各自理由）
        dissenters = self._parse_dissenters(text)
        if dissenters:
            result["dissenters"] = dissenters
            if result["vote_against"] is None:
                result["vote_against"] = len(dissenters)
        # （部分会议纪要/声明无异议: "Voting against ... : None"）

        # decision_date: 声明 URL 中的日期
        dm = STATEMENT_URL_PATTERN.search(doc["source_url"])
        if dm:
            result["decision_date"] = datetime.strptime(
                dm.group(1), "%Y%m%d").date()

        # change_bp: 与上一次决议对比（自 join 上一条 meeting_date）
        prev = self._get_previous_decision(result["meeting_date"])
        if prev and result["rate_high_bp"] is not None:
            result["change_bp"] = result["rate_high_bp"] - prev["rate_high_bp"]
            if result["decision_direction"] is None and prev["rate_high_bp"]:
                result["decision_direction"] = (
                    "hike" if result["change_bp"] > 0
                    else "cut" if result["change_bp"] < 0 else "hold"
                )

        self._upsert_decision(doc_id, doc["source_url"], result)

        ok_fields = sum(
            1 for k in ("rate_high_bp", "vote_for", "vote_against")
            if result.get(k) is not None
        )
        logger.info(
            f"[FOMC] 决议抽取完成 doc={doc_id} 区间={result['rate_low_bp']}-"
            f"{result['rate_high_bp']}bp 票型={result['vote_for']}:"
            f"{result['vote_against']} 异议={len(result['dissenters'])} "
            f"缺字段={result['error_fields'] or '无'}"
        )
        return result

    @classmethod
    def _parse_dissenters(cls, text: str) -> List[Dict[str, str]]:
        """解析异议段 → [{name, direction, reason}]

        支持多组: "A, who preferred to lower ...; and B and C, who supported
        maintaining ... but did not support inclusion of an easing bias ..."
        方向判定: preferred to raise/increase → hawkish; lower/reduce → dovish;
        其余（如反对 easing bias 措辞）按关键词推断, 无法判定标 unknown。
        """
        block = RE_DISSENT_BLOCK.search(text)
        if not block:
            return []
        raw_block = block.group(1)

        # "Voting against ... : None" 场景
        if re.match(r"^\s*(none|no\s+one)\b", raw_block, re.IGNORECASE):
            return []

        dissenters: List[Dict[str, str]] = []
        for gm in RE_DISSENT_GROUP.finditer(raw_block):
            names_raw, reason = gm.group(1).strip(), re.sub(
                r"\s+", " ", gm.group(2)).strip()
            reason_l = reason.lower()

            if re.search(r"preferred\s+to\s+(raise|increase)", reason_l):
                direction = "hawkish"
            elif re.search(r"preferred\s+to\s+(lower|reduce|cut)", reason_l):
                direction = "dovish"
            elif re.search(r"easing\s+bias|too\s+accommodative|"
                           r"did\s+not\s+support\s+inclusion", reason_l):
                direction = "hawkish"
            elif re.search(r"insufficient|more\s+accommodative|"
                           r"preferred\s+a\s+(larger\s+)?cut", reason_l):
                direction = "dovish"
            else:
                direction = "unknown"

            for n in cls._split_names(names_raw):
                dissenters.append({
                    "name": n, "direction": direction, "reason": reason[:200],
                })

        # 组解析失败时整块按 unknown 兜底（保留票数）
        if not dissenters:
            names = cls._split_names(raw_block)
            dissenters = [{"name": n, "direction": "unknown", "reason": ""}
                          for n in names]
        return dissenters

    @staticmethod
    def _split_names(raw: str) -> List[str]:
        """'A, B, and C' → ['A', 'B', 'C']; 'A, Chair; B, Vice Chair' → ['A', 'B']

        老声明用分号分隔人名（逗号留给头衔），新声明用逗号+and。
        头衔剥离: ', Chair' / '; Vice Chair' 等。不做子串切分
        （避免 'Anderson'/'and Lorie K. Logan' 被切坏）。
        """
        raw = re.sub(r"\s+", " ", raw).strip()
        # 兼容 "Voting for this action : Kevin Warsh, ..." 冒号残留
        if ":" in raw.split(",")[0]:
            raw = re.sub(r"^[^:]*:\s*", "", raw)

        # 分号优先（老式: 每人一项, 逗号+头衔在项内）
        if ";" in raw:
            parts = [p.strip() for p in raw.split(";")]
        else:
            parts = [p.strip() for p in raw.split(",")]

        # 末项剥离 "and " 前缀
        if parts and parts[-1].lower().startswith("and "):
            parts[-1] = parts[-1][4:].strip()

        # 项内剥离 ", Title" 头衔（Chair / Vice Chair / President 等）
        title_re = re.compile(
            r",\s*(?:Chair(?:man)?|Vice\s+Chair(?:man)?|President(?:\s+and\s+CEO)?|"
            r"Governor|Alternate)$", re.IGNORECASE)
        cleaned = [title_re.sub("", p).strip() for p in parts]

        return [p for p in cleaned if p and len(p) > 2]

    def _get_previous_decision(self, meeting_date) -> Optional[Dict]:
        if meeting_date is None:
            return None
        rows = self.db_manager.fetch_all(
            """
            SELECT meeting_date, rate_high_bp FROM fomc_decisions
            WHERE central_bank = 'FED' AND meeting_date < %s
            ORDER BY meeting_date DESC LIMIT 1
            """,
            (meeting_date,),
        )
        if not rows or rows[0][1] is None:
            return None
        return {"meeting_date": rows[0][0], "rate_high_bp": rows[0][1]}

    def _upsert_decision(self, doc_id: int, source_url: str, r: Dict):
        conn = self.db_manager.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO fomc_decisions
                        (central_bank, meeting_date, decision_date,
                         rate_low_bp, rate_high_bp, decision_direction,
                         change_bp, vote_for, vote_against, dissenters,
                         statement_doc_id, source_url, extraction_method,
                         error_message)
                    VALUES ('FED', %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, 'regex_v1', %s)
                    ON CONFLICT (central_bank, meeting_date) DO UPDATE SET
                        decision_date = EXCLUDED.decision_date,
                        rate_low_bp = EXCLUDED.rate_low_bp,
                        rate_high_bp = EXCLUDED.rate_high_bp,
                        decision_direction = EXCLUDED.decision_direction,
                        change_bp = EXCLUDED.change_bp,
                        vote_for = EXCLUDED.vote_for,
                        vote_against = EXCLUDED.vote_against,
                        dissenters = EXCLUDED.dissenters,
                        statement_doc_id = EXCLUDED.statement_doc_id,
                        source_url = EXCLUDED.source_url,
                        error_message = EXCLUDED.error_message,
                        extracted_at = CURRENT_TIMESTAMP,
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    (
                        r["meeting_date"], r["decision_date"],
                        r["rate_low_bp"], r["rate_high_bp"],
                        r["decision_direction"], r["change_bp"],
                        r["vote_for"], r["vote_against"],
                        json.dumps(r["dissenters"], ensure_ascii=False),
                        doc_id, source_url,
                        ", ".join(r["error_fields"]) or None,
                    ),
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    # ── LLM 概括 ───────────────────────────────────────────────────────────

    # 纪要策略性截断: 保留开头决议段 + Staff Review 首段 + 最后 1/3
    # (participants 观点与政策展望是概括核心); 声明 ~200 词全量不截断
    MINUTES_MAX_CHARS = 24000

    async def generate_summary(self, doc_id: int, force: bool = False) -> Dict[str, Any]:
        """对文档生成 LLM 中文概括 → fomc_summaries

        幂等: 已有 completed 且未 force 时跳过。
        """
        from backend.core.llm_framework import llm_service
        from backend.services.summary_common import (
            SINGLE_LLM_TIMEOUT_SEC, SINGLE_LLM_RETRY,
        )

        doc = self._get_document(doc_id)
        if not doc or doc["fetch_status"] != "fetched":
            raise Exception(f"文档未就绪: {doc_id}")

        existing = self.db_manager.fetch_all(
            "SELECT id, status FROM fomc_summaries "
            "WHERE document_id = %s AND template_id = %s",
            (doc_id, FOMC_SUMMARY_TEMPLATE),
        )
        if existing and existing[0][1] == "completed" and not force:
            return {"doc_id": doc_id, "skipped": True,
                    "summary_id": existing[0][0]}

        rows = self.db_manager.fetch_all(
            "SELECT raw_content, title, meeting_date, doc_type "
            "FROM fomc_documents WHERE id = %s", (doc_id,)
        )
        raw, title, meeting_date, doc_type = rows[0]

        original_text = self._truncate_for_llm(raw, doc_type)
        prev = self._get_previous_decision(meeting_date) or {}
        prev_decision = (
            f"区间 {prev.get('rate_low_bp', '?')}-{prev.get('rate_high_bp', '?')}bp, "
            f"票型 {prev.get('vote_for', '?')}:{prev.get('vote_against', '?')}"
            if prev else "无"
        )

        variables = {
            "doc_type": doc_type,
            "title": title,
            "meeting_date": str(meeting_date or ""),
            "previous_decision": prev_decision,
            "original_text": original_text,
        }

        summary_row_id = self._upsert_summary_pending(doc_id, doc_type)

        last_exc = None
        for attempt in range(SINGLE_LLM_RETRY + 1):
            try:
                response = await llm_service.chat_completion_with_template(
                    template_code=FOMC_SUMMARY_TEMPLATE,
                    variables=variables,
                    timeout=SINGLE_LLM_TIMEOUT_SEC,
                )
                self._complete_summary(
                    summary_row_id, response.content,
                    model=getattr(response, "model", None),
                    pt=getattr(response, "prompt_tokens", None),
                    ct=getattr(response, "completion_tokens", None),
                    prev_meeting_date=prev.get("meeting_date"),
                )
                logger.info(
                    f"[FOMC] 概括完成 doc={doc_id} type={doc_type} "
                    f"len={len(response.content)}"
                )
                return {"doc_id": doc_id, "skipped": False,
                        "summary_id": summary_row_id,
                        "length": len(response.content)}
            except asyncio.TimeoutError as exc:
                last_exc = exc
                if attempt < SINGLE_LLM_RETRY:
                    logger.warning(f"[FOMC] LLM 超时 (attempt {attempt+1}), retrying")
            except Exception as exc:
                last_exc = exc
                if attempt < SINGLE_LLM_RETRY:
                    logger.warning(f"[FOMC] LLM 错误 (attempt {attempt+1}): {exc}")
        self._fail_summary(summary_row_id, str(last_exc))
        raise last_exc  # type: ignore[misc]

    def _truncate_for_llm(self, raw: str, doc_type: str) -> str:
        """纪要 5,400 词策略性截断 (~24k 字符): 头部决议段 + 尾部 1/3"""
        if doc_type != "minutes" or len(raw) <= self.MINUTES_MAX_CHARS:
            return raw
        head = raw[:8000]  # 决议段 + Staff Review 开头
        tail_start = len(raw) - (self.MINUTES_MAX_CHARS - 8000)
        tail = raw[tail_start:]
        return (
            head + "\n\n[...中间部分省略, 保留委员会讨论与政策展望...]\n\n" + tail
        )

    def _upsert_summary_pending(self, doc_id: int, doc_type: str) -> int:
        conn = self.db_manager.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO fomc_summaries
                        (central_bank, document_id, doc_type, template_id,
                         status, error_message)
                    VALUES ('FED', %s, %s, %s, 'generating', NULL)
                    ON CONFLICT (document_id, template_id) DO UPDATE SET
                        status = 'generating',
                        error_message = NULL,
                        updated_at = CURRENT_TIMESTAMP
                    RETURNING id
                    """,
                    (doc_id, doc_type, FOMC_SUMMARY_TEMPLATE),
                )
                row = cur.fetchone()
            conn.commit()
            return row[0]
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _complete_summary(self, row_id, content, model, pt, ct, prev_meeting_date):
        conn = self.db_manager.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE fomc_summaries
                    SET summary_content = %s, model = %s, status = 'completed',
                        prompt_tokens = %s, completion_tokens = %s,
                        prev_meeting_date = %s, updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                    """,
                    (content, model, pt, ct, prev_meeting_date, row_id),
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _fail_summary(self, row_id, message: str):
        conn = self.db_manager.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE fomc_summaries
                    SET status = 'failed', error_message = %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                    """,
                    (message[:2000], row_id),
                )
            conn.commit()
        except Exception:
            conn.rollback()
        finally:
            conn.close()

    # ── 回填 ───────────────────────────────────────────────────────────────

    # 2026 年五次会议（用户确认的回填范围）
    # 声明 URL 已实测确认；纪要按固定模式构造（会议末日）
    BACKFILL_2026_STATEMENTS = [
        "https://www.federalreserve.gov/newsevents/pressreleases/monetary20260128a.htm",
        "https://www.federalreserve.gov/newsevents/pressreleases/monetary20260318a.htm",
        "https://www.federalreserve.gov/newsevents/pressreleases/monetary20260429a.htm",
        "https://www.federalreserve.gov/newsevents/pressreleases/monetary20260617a.htm",
        "https://www.federalreserve.gov/newsevents/pressreleases/monetary20260729a.htm",
    ]
    BACKFILL_2026_MINUTES_DATES = ["20260128", "20260318", "20260429",
                                   "20260617", "20260729"]

    def backfill_2026(self, summarize: bool = True) -> Dict[str, int]:
        """回填 2026 年五次会议的声明+纪要。串行 + 间隔防限流。

        7 月纪要 2026-08-19 前后发布，404 属预期——记录 failed 留待 RSS 增量发现。
        """
        stats = {"statements": 0, "minutes": 0, "skipped": 0, "failed": 0}

        for url in self.BACKFILL_2026_STATEMENTS:
            inserted = self._upsert_document(
                doc_type="statement",
                meeting_date=None,  # 由 URL 日期推导
                title="FOMC statement",
                source_url=url,
                published_at=None,
            )
            doc_id = self._get_doc_id_by_url(url)
            if not inserted and self._already_fetched(doc_id):
                stats["skipped"] += 1
                continue
            try:
                # meeting_date 由 URL 推导后补
                self._patch_meeting_date_from_url(doc_id, "statement")
                self.fetch_document(doc_id)
                self.extract_decision(doc_id)
                stats["statements"] += 1
            except Exception as e:
                logger.warning(f"[FOMC] 回填声明失败 {url}: {e}")
                stats["failed"] += 1
            time.sleep(2)

        for ds in self.BACKFILL_2026_MINUTES_DATES:
            url = f"{FED_BASE}/monetarypolicy/fomcminutes{ds}.htm"
            end = datetime.strptime(ds, "%Y%m%d").date()
            meeting_date = end.fromordinal(end.toordinal() - 1)
            inserted = self._upsert_document(
                doc_type="minutes",
                meeting_date=meeting_date,
                title=f"Minutes of the FOMC, {end.strftime('%B %d–%d, %Y')}",
                source_url=url,
                published_at=None,
            )
            doc_id = self._get_doc_id_by_url(url)
            if not inserted and self._already_fetched(doc_id):
                stats["skipped"] += 1
                continue
            try:
                self.fetch_document(doc_id)
                stats["minutes"] += 1
            except Exception as e:
                # 7 月纪要尚未发布属预期
                logger.warning(f"[FOMC] 回填纪要失败 {url}: {e}")
                stats["failed"] += 1
            time.sleep(2)

        logger.info(f"[FOMC] 2026 回填完成: {stats}")
        return stats

    def _get_doc_id_by_url(self, url: str) -> Optional[int]:
        rows = self.db_manager.fetch_all(
            "SELECT id FROM fomc_documents WHERE source_url = %s", (url,)
        )
        return rows[0][0] if rows else None

    def _already_fetched(self, doc_id: Optional[int]) -> bool:
        if not doc_id:
            return False
        rows = self.db_manager.fetch_all(
            "SELECT fetch_status FROM fomc_documents WHERE id = %s", (doc_id,)
        )
        return bool(rows and rows[0][0] == "fetched")

    def _patch_meeting_date_from_url(self, doc_id: int, doc_type: str):
        """从 URL 日期推导 meeting_date（会议首日 = URL 末日 - 1）"""
        rows = self.db_manager.fetch_all(
            "SELECT source_url FROM fomc_documents WHERE id = %s", (doc_id,)
        )
        if not rows:
            return
        url = rows[0][0]
        pattern = (STATEMENT_URL_PATTERN if doc_type == "statement"
                   else MINUTES_URL_PATTERN)
        m = pattern.search(url)
        if not m:
            return
        end = datetime.strptime(m.group(1), "%Y%m%d").date()
        meeting_date = end.fromordinal(end.toordinal() - 1)
        conn = self.db_manager.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE fomc_documents SET meeting_date = %s, "
                    "updated_at = CURRENT_TIMESTAMP WHERE id = %s",
                    (meeting_date, doc_id),
                )
            conn.commit()
        except Exception:
            conn.rollback()
        finally:
            conn.close()

    # ── API 数据组装（同步）────────────────────────────────────────────────

    def get_dashboard(self) -> Dict[str, Any]:
        """Dashboard 聚合端点数据组装"""
        decision_row = self._latest_decision()
        decision = None
        if decision_row:
            d = decision_row
            prev = self._get_previous_decision(d["meeting_date"])
            decision = {
                "meeting_date": str(d["meeting_date"]) if d["meeting_date"] else None,
                "decision_date": str(d["decision_date"]) if d["decision_date"] else None,
                "rate_low_bp": d["rate_low_bp"],
                "rate_high_bp": d["rate_high_bp"],
                "decision_direction": d["decision_direction"],
                "change_bp": d["change_bp"],
                "vote_for": d["vote_for"],
                "vote_against": d["vote_against"],
                "dissenters": d["dissenters"] or [],
                "source_url": d["source_url"],
                "previous": ({
                    "meeting_date": str(prev["meeting_date"]),
                    "rate_high_bp": prev["rate_high_bp"],
                } if prev else None),
                "eodhd_cross_check": self._eodhd_cross_check(d["decision_date"]),
            }

        stmt = self._latest_summary("statement")
        mins = self._latest_summary("minutes")

        recent = []
        rows = self.db_manager.fetch_all(
            """
            SELECT d.meeting_date, d.decision_direction, d.change_bp,
                   d.vote_for, d.vote_against, d.rate_high_bp
            FROM fomc_decisions d
            WHERE d.central_bank = 'FED'
            ORDER BY d.meeting_date DESC LIMIT 8
            """
        )
        for r in rows:
            recent.append({
                "meeting_date": str(r[0]),
                "decision_direction": r[1],
                "change_bp": r[2],
                "vote_for": r[3],
                "vote_against": r[4],
                "rate_high_bp": r[5],
            })

        updates = self.get_updates(page=1, page_size=20, monetary_only=False)

        return {
            "decision": decision,
            "statement_summary": stmt,
            "minutes_summary": mins,
            "recent_meetings": recent,
            "updates": updates["items"],
            "updates_total": updates["total"],
            "probabilities": None,  # V1 缓做（CME FedWatch/ZQ 期货源调研中）
        }

    def _latest_decision(self) -> Optional[Dict]:
        rows = self.db_manager.fetch_all(
            """
            SELECT meeting_date, decision_date, rate_low_bp, rate_high_bp,
                   decision_direction, change_bp, vote_for, vote_against,
                   dissenters, source_url
            FROM fomc_decisions
            WHERE central_bank = 'FED'
            ORDER BY meeting_date DESC LIMIT 1
            """
        )
        if not rows:
            return None
        cols = ["meeting_date", "decision_date", "rate_low_bp", "rate_high_bp",
                "decision_direction", "change_bp", "vote_for", "vote_against",
                "dissenters", "source_url"]
        d = dict(zip(cols, rows[0]))
        if isinstance(d["dissenters"], str):
            try:
                d["dissenters"] = json.loads(d["dissenters"])
            except (ValueError, TypeError):
                d["dissenters"] = []
        return d

    def _latest_summary(self, doc_type: str) -> Optional[Dict]:
        rows = self.db_manager.fetch_all(
            """
            SELECT s.document_id, s.doc_type, d.meeting_date, d.title,
                   d.source_url, s.status, s.template_id, s.model,
                   s.summary_content, s.prev_meeting_date, s.updated_at
            FROM fomc_summaries s
            JOIN fomc_documents d ON d.id = s.document_id
            WHERE s.doc_type = %s AND s.central_bank = 'FED'
              AND s.template_id = %s
            ORDER BY d.meeting_date DESC NULLS LAST
            LIMIT 1
            """,
            (doc_type, FOMC_SUMMARY_TEMPLATE),
        )
        if not rows:
            return None
        r = rows[0]
        return {
            "document_id": r[0],
            "doc_type": r[1],
            "meeting_date": str(r[2]) if r[2] else None,
            "title": r[3],
            "source_url": r[4],
            "status": r[5],
            "template_id": r[6],
            "model": r[7],
            "summary_content": r[8],
            "prev_meeting_date": str(r[9]) if r[9] else None,
            "generated_at": r[10].isoformat() if r[10] else None,
        }

    def _eodhd_cross_check(self, decision_date) -> Optional[Dict]:
        """从 economic_events 取同日 EODHD 的 Fed 利率决议值（交叉验证用）"""
        if not decision_date:
            return None
        try:
            rows = self.db_manager.fetch_all(
                """
                SELECT actual, forecast, previous
                FROM economic_events
                WHERE country = 'US'
                  AND event = 'Fed Interest Rate Decision'
                  AND date = %s::text
                ORDER BY id DESC LIMIT 1
                """,
                (str(decision_date),),
            )
            if not rows:
                return None
            return {
                "actual": float(rows[0][0]) if rows[0][0] is not None else None,
                "forecast": float(rows[0][1]) if rows[0][1] is not None else None,
                "previous": float(rows[0][2]) if rows[0][2] is not None else None,
                "source": "eodhd",
            }
        except Exception as e:
            logger.warning(f"[FOMC] EODHD 交叉验证查询失败: {e}")
            return None

    def get_document_detail(self, document_id: int) -> Optional[Dict]:
        """单文档详情: 原文 + 元数据 + 关联概括"""
        rows = self.db_manager.fetch_all(
            """
            SELECT id, doc_type, meeting_date, title, source_url,
                   published_at, raw_content, word_count, fetch_status,
                   fetched_at
            FROM fomc_documents WHERE id = %s
            """,
            (document_id,),
        )
        if not rows:
            return None
        r = rows[0]
        summary = self.db_manager.fetch_all(
            """
            SELECT template_id, model, status, summary_content, updated_at
            FROM fomc_summaries
            WHERE document_id = %s AND template_id = %s
            """,
            (document_id, FOMC_SUMMARY_TEMPLATE),
        )
        s = summary[0] if summary else None
        return {
            "id": r[0],
            "doc_type": r[1],
            "meeting_date": str(r[2]) if r[2] else None,
            "title": r[3],
            "source_url": r[4],
            "published_at": r[5].isoformat() if r[5] else None,
            "raw_content": r[6],
            "word_count": r[7],
            "fetch_status": r[8],
            "fetched_at": r[9].isoformat() if r[9] else None,
            "summary": ({
                "template_id": s[0], "model": s[1], "status": s[2],
                "summary_content": s[3],
                "updated_at": s[4].isoformat() if s[4] else None,
            } if s else None),
        }

    def get_updates(self, page: int = 1, page_size: int = 20,
                    monetary_only: bool = False) -> Dict[str, Any]:
        """Fed updates 流分页"""
        where = "WHERE central_bank = 'FED'"
        params: list = []
        if monetary_only:
            where += " AND is_monetary_policy = TRUE"
        offset = (page - 1) * page_size

        total_rows = self.db_manager.fetch_all(
            f"SELECT COUNT(*) FROM fed_updates {where}", tuple(params) or None
        )
        total = total_rows[0][0] if total_rows else 0

        rows = self.db_manager.fetch_all(
            f"""
            SELECT id, title, link, category, is_monetary_policy, pub_date
            FROM fed_updates {where}
            ORDER BY pub_date DESC NULLS LAST
            LIMIT %s OFFSET %s
            """,
            tuple(params + [page_size, offset]),
        )
        items = [{
            "id": r[0], "title": r[1], "link": r[2], "category": r[3],
            "is_monetary_policy": r[4],
            "pub_date": r[5].isoformat() if r[5] else None,
        } for r in rows]
        return {"items": items, "total": total, "page": page,
                "page_size": page_size}

    # ── 兜底重试 ───────────────────────────────────────────────────────────

    def retry_pending_documents(self) -> int:
        """重试 failed/pending 的文档（每日兜底任务调用）"""
        rows = self.db_manager.fetch_all(
            "SELECT id FROM fomc_documents "
            "WHERE fetch_status IN ('pending', 'failed') "
            "ORDER BY created_at LIMIT 20"
        )
        count = 0
        for (doc_id,) in rows:
            try:
                self.fetch_document(doc_id)
                doc = self._get_document(doc_id)
                if doc and doc["doc_type"] == "statement":
                    self.extract_decision(doc_id)
                count += 1
            except Exception as e:
                logger.warning(f"[FOMC] 兜底重试失败 doc={doc_id}: {e}")
            time.sleep(2)
        return count
