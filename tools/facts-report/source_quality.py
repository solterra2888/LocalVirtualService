# -*- coding: utf-8 -*-
"""新闻素材的信源层级与逻辑选材(纯函数:不连库、不调 LLM)。

tier 是独立于 E 级(证据强度)的「信源层级」轴,决定选材权重与引用优先级:
  1  一手通讯社全文 —— Bloomberg / Reuters 剪藏(news 频道,GuanHe Clipper)
  2  原创媒体 / 具名一手信源(CNBC、The Information、AP、Nick Timiraos、CCTV、
     Bloomberg TV / Reuters 视频、星球券商纪要);或转播号 / 聚合站在正文里
     注明了原始出处(origin:"- RTRS"、"per WSJ"、"(Reuters) -")
  3  转播快讯号(*Walter Bloomberg / LiveSquawk / Wall St Engine …,推特默认档)
  4  聚合站与公司稿(Yahoo Finance / Globenewswire,news 频道默认档)

2026-11 审计(11-05~11-20 共 12 期)的结论决定了这里的规则:wire 50 条全部
来自 Yahoo/SA/CNBC、L 级全空导致退化成"最晚 50 条",其中 SEO 荐股文、公司
公告、GuruFocus 自动稿占大半,且有隔日复盘稿把前一交易日涨跌当成 T 日事实
(11-20 ANT +17.07% 进了规范数值);flash 同一条 headline 被 5-9 个转播号重复。
"""

from __future__ import annotations

import re
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Set, Tuple
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")

# ── 信源层级 ───────────────────────────────────────────────────────────

TIER1_NEWS_AUTHORS = frozenset({"bloomberg", "reuters"})
TIER2_AUTHORS = frozenset({
    "cnbc", "cctv", "the information", "the associated press", "techcrunch",
    "nick timiraos", "anna wong", "bloomberg television", "reuters",
    "geopolitical dispatch", "mohamed a. el-erian", "ray dalio",
    "renmac: renaissance macro research", "andreas steno larsen",
    "variant perception",
})
TIER4_AUTHORS = frozenset({"yahoo finance", "globenewswire"})
_CHANNEL_DEFAULT_TIER = {"news": 4, "twitter": 3, "youtube": 2,
                         "zhishixingqiu": 2, "substack": 2}
TIER_LABELS = {1: "Bloomberg/Reuters 全文", 2: "原创媒体/注明出处",
               3: "转播快讯", 4: "聚合/公司稿"}


def source_tier(author: Optional[str], channel_type: Optional[str]) -> int:
    a = (author or "").strip().lower()
    ct = (channel_type or "").strip().lower()
    if ct == "news" and a in TIER1_NEWS_AUTHORS:
        return 1
    if a in TIER2_AUTHORS:
        return 2
    if a in TIER4_AUTHORS:
        return 4
    if ct == "news" and a == "seeking alpha":
        return 3
    return _CHANNEL_DEFAULT_TIER.get(ct, 3)


# 转播号自己不是信源:正文点名的原始出处才是("TO RTRS" → Reuters)
_ORIGIN_PATTERNS: Tuple[Tuple[str, "re.Pattern[str]"], ...] = (
    ("Reuters", re.compile(r"\bRTRS\b|\bReuters\b", re.I)),
    ("Bloomberg", re.compile(
        r"\bBBG\b|(?:\bper|\bvia|according to|\bto|\btold|[-–—(:])\s*Bloomberg\b"
        r"(?!\s+(?:index|intelligence|dollar|commodity|terminal|economics|"
        r"billionaires|television|tv)\b)|\bBloomberg (?:News|reports?|reported)\b",
        re.I)),
    ("WSJ", re.compile(r"\bWSJ\b|Wall Street Journal", re.I)),
    ("FT", re.compile(r"[-–—:]\s*FT\b|\b(?:per|via|to|told) (?:the )?FT\b|"
                      r"\bFT (?:reports?|says)\b|Financial Times", re.I)),
    ("The Information", re.compile(r"\bThe Information\b")),
    ("NYT", re.compile(r"\bNYT\b|New York Times", re.I)),
    ("CNBC", re.compile(r"\bCNBC\b")),
    ("Axios", re.compile(r"\bAxios\b")),
    ("Semafor", re.compile(r"\bSemafor\b")),
    ("Politico", re.compile(r"\bPolitico\b", re.I)),
    ("AP", re.compile(r"\(AP\)|\bAssociated Press\b")),
    ("Nikkei Asia", re.compile(r"Nikkei ?Asia", re.I)),
    ("Xinhua", re.compile(r"\bXinhua\b|新华社")),
)
ORIGIN_OUTLETS = frozenset(name for name, _ in _ORIGIN_PATTERNS)
# 聚合站转载的通讯社稿,署名在导语里:"NEW YORK, Nov 19 (Reuters) - …"
_SYNDICATED_RE = re.compile(r"\((Reuters|Bloomberg|AP)\)\s*(?:--|[-–—])")


def credited_origin(text: str) -> Optional[str]:
    """快讯正文点名的原始出处(最早出现者);未点名返回 None。"""
    best: Optional[Tuple[int, str]] = None
    for name, rx in _ORIGIN_PATTERNS:
        m = rx.search(text or "")
        if m and (best is None or m.start() < best[0]):
            best = (m.start(), name)
    return best[1] if best else None


def syndicated_origin(text: str) -> Optional[str]:
    m = _SYNDICATED_RE.search((text or "")[:300])
    return m.group(1) if m else None


def effective_tier(tier: int, origin: Optional[str]) -> int:
    """注明一手出处的转播/聚合条目升到第 2 层(仍不是全文,不升第 1 层)。"""
    return 2 if tier >= 3 and origin in ORIGIN_OUTLETS else tier


# ── 剪藏正文清洗 ───────────────────────────────────────────────────────

_CLIP_BOILER_LINES = frozenset({"summary", "companies", "takeaways by bloomberg ai",
                                "takeaways", "listen", "share", "save", "gift this article",
                                "report ad", "advertisement", "read more"})
_BYLINE_RE = re.compile(r"^[A-Z][a-z]+ \d{1,2}, \d{4}\s*(?:·|at \d)")
_SOURCE_LINE_RE = re.compile(r"^Source:\s*https?://", re.I)
# Reuters 图片说明:"Item 1 of 5 <caption> REUTERS/<摄影师>" 与 "[1/5]<同一 caption>"
# 各一行,常插在正文前 20%;专栏稿的折叠图说以 "... Read more" 收尾;
# 署名尾行 "Reporting by …; Editing by …"
_CAPTION_LINE_RE = re.compile(r"^Item \d+ of \d+\b|^\[\d+/\d+\]|\bREUTERS/[A-Z]|"
                              r"(?:\.\.\.|…)\s*Read more$")
_CREDIT_LINE_RE = re.compile(r"^(?:Reporting|Writing|Additional reporting) by\b|"
                             r"^Editing by\b")


def clean_clip_body(text: str, title: Optional[str]) -> str:
    """剪藏正文去模板:头部的重复标题 / Source 链接 / 署名 / 时间戳 /
    Takeaways 等(约 150-350 字),以及全文的图片说明行与记者署名尾行 ——
    3000 字配额里不该花在这些上面。"""
    lines = (text or "").splitlines()
    norm = [re.sub(r"\s+", " ", ln).strip() for ln in lines]
    # 页面标题行与恢复后的 title 可能不一致(被截成 "K" 的 "K-Pop …"):
    # 以前 4 行内的 Source 行为锚,其前各行与之后的同文重复行都算标题
    heads = {re.sub(r"\s+", " ", (title or "")).strip().lower()} - {""}
    i = 0
    for j in range(min(4, len(norm))):
        if _SOURCE_LINE_RE.match(norm[j]):
            heads.update(n.lower() for n in norm[:j] if n)
            i = j + 1
            break
    while i < len(lines) and i < 15:
        ln = norm[i]
        low = ln.lower()
        if (not ln or low in heads or _SOURCE_LINE_RE.match(ln)
                or _BYLINE_RE.match(ln) or low in _CLIP_BOILER_LINES):
            i += 1
            continue
        break
    body = [ln for ln in lines[i:]
            if not _CAPTION_LINE_RE.search(ln.strip())
            and not _CREDIT_LINE_RE.match(ln.strip())
            and re.sub(r"\s+", " ", ln).strip().lower() not in _CLIP_BOILER_LINES]
    return "\n".join(body).strip()


def lede(text: str, n: int = 220) -> str:
    """正文第一句(≤n 字),用于编辑计划摘要。"""
    t = re.sub(r"\s+", " ", text or "").strip()
    m = re.search(r"(?<=[.!?。])\s", t[:n + 40])
    s = t[:m.start()] if m and m.start() >= 40 else t[:n]
    return s if len(s) <= n else s[:n - 1] + "…"


# ── 低价值稿件判定(仅用于第 2-4 层;第 1 层不过这道筛) ─────────────────

_PR_RE = re.compile(
    r"--\s*\(BUSINESS WIRE\)|/PRNewswire/|\(GLOBE NEWSWIRE\)|ACCESS Newswire|"
    r"/ACCESSWIRE/|/CNW/|Newsfile Corp|EQS-News|Company Announcement|"
    r"Reference is made to|\(Marketwired\)", re.I)
_SEO_TITLE_RE = re.compile(
    r"Stocks? (?:to|To) (?:Buy|Watch|Own|Consider|Research|Hold)|Stock Picks|"
    r"\bTop \d+\b|^\d+ [\w\s’'&-]*\bStocks?\b|Reasonably Priced|Fair Value|"
    r"Undervalued|Overvalued|Valuation|Is It (?:Time|Too Late)|Should You|"
    r"So What Is|What Comes Next|Drawing Attention|Here[’']s (?:Why|What|the|How)|"
    r"Bullish Signs|Bearish Signs|Stock Forecast|Price Prediction|Can It\b|"
    r"Worth (?:Buying|a Look)|Buy the Dip|Millionaire|\bRetire|Dividend Stocks?|"
    r"\((?:NYSE|NASDAQ|Nasdaq\w*|NYSEAM\w*|SWX|HLSE|LSE|AIM|TSX|TSXV|ASX|SEHK|"
    r"XTRA|ENXT\w*|BIT|BME|OM|CPSE|OB|SZSE|SHSE|TSE|KOSE|KOSDAQ|NSEI|BSE|SGX|"
    r"JSE|WSE|IST|TASE|NZSE|ATSE|DB|SET|IDX|KLSE):[\w.]+\)", re.I)
_SEO_BODY_RE = re.compile(
    r"We just published a list of|Simply Wall St|Motley Fool|Zacks Rank|"
    r"Zacks Consensus Estimate|Zacks Investment Research", re.I)
_AUTO_RE = re.compile(r"This article first appeared on GuruFocus|Earnings Snapshot|"
                      r"Automated Insights", re.I)
_LOWVALUE_TITLE_RE = re.compile(
    r"to (?:Present|Participate|Host|Speak|Attend|Ring)\b|Conference Call|Webcast|"
    r"Declares? (?:[\w-]+ ){0,4}Dividends?|Dividend Declaration|"
    r"(?:Stockholders|Shareholders)[’']? Meeting|Annual (?:General )?Meeting|"
    r"Inducement Grants?|Restricted Stock Units|Employee Share|Issuance of Shares|"
    r"Reverse (?:Stock )?Split|Regains? Compliance|Class Action|Investors Who Lost|"
    r"Shareholder Alert|Investor Alert|Securities Fraud|Best (?:Banks|Places|"
    r"Companies) to Work|Closes? (?:[\w-]+ ){0,4}Offering|Prices? (?:[\w-]+ ){0,4}"
    r"Offering|Notice of|Form 8\.3|Total Voting Rights|Transaction in Own Shares|"
    r"Market (?:Size|Share|Report|Forecast|Outlook)|\bCAGR\b|"
    r"^BC-", re.I)   # BC- = AP 行情表/数据表 slug(Most Active Stocks 等)


def classify_article(title: str, text: str) -> str:
    """news | pr | auto | seo | lowvalue(第 2-4 层的稿件形态)。"""
    head = (text or "")[:600]
    if _SEO_TITLE_RE.search(title or "") or _SEO_BODY_RE.search(head):
        return "seo"
    if _LOWVALUE_TITLE_RE.search(title or ""):
        return "lowvalue"
    if _AUTO_RE.search(f"{title} {head}"):
        return "auto"
    if _PR_RE.search(head):
        return "pr"
    return "news"


_MONTHS = {m: i for i, m in enumerate(
    ("jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov",
     "dec"), 1)}
_MONTH_ALT = (r"(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|June?|July?|"
              r"Aug(?:ust)?|Sep(?:t(?:ember)?)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)")
_STALE_RE = re.compile(
    r"\b(?:clos(?:e|ed|ing)|finish(?:ed)?|settled|session|trading day)\b[^\n]{0,90}?"
    r"\bon " + _MONTH_ALT + r"\.? (\d{1,2})\b|"
    r"\bat the " + _MONTH_ALT + r"\.? (\d{1,2}) (?:regular[- ]session )?close\b", re.I)


def stale_session_date(text: str, report_date: date) -> Optional[date]:
    """导语把涨跌定在 T 之前的收盘("closed … on November 19")→ 返回该日期。

    聚合站隔日复盘稿的典型特征;审计中 11-18 有 8 条、11-20 有 7 条,其中
    一条把 ANT 上市首日 +17.07% 送进了 11-20 的规范数值。"""
    for m in _STALE_RE.finditer((text or "")[:400]):
        mon, day = (m.group(1), m.group(2)) if m.group(1) else (m.group(3), m.group(4))
        try:
            d = date(report_date.year, _MONTHS[mon[:3].lower()], int(day))
        except (KeyError, ValueError):
            continue
        if d > report_date + timedelta(days=7):
            d = d.replace(year=d.year - 1)
        if d < report_date:
            return d
    return None


# ── 相关度(与当日盘面的关联) ─────────────────────────────────────────

_NAME_SUFFIX_RE = re.compile(
    r"[,.]?\s+(?:Inc|Corp|Corporation|Co|Company|Holdings?|Group|plc|PLC|Ltd|"
    r"Limited|N\.V|S\.A|AG|SE|Incorporated|Class [A-C]|Technologies|"
    r"Technology|International|Enterprises|Trust|Bancorp|Brands)\.?\s*$", re.I)
_GENERIC_FIRST = frozenset({
    "the", "american", "general", "united", "first", "international", "advanced",
    "applied", "national", "global", "southern", "northern", "western", "eastern",
    "public", "texas", "new", "royal", "digital", "real", "energy", "federal",
    "consolidated", "capital", "bank", "health", "healthcare", "service",
    "universal", "regions", "state", "fifth", "principal", "citizens", "news",
    "air", "best", "dollar", "match", "target", "visa", "block", "ball", "jack",
    "west", "east", "north", "south", "standard", "marathon", "constellation",
})
_TICKER_FORMS = re.compile(
    r"\$([A-Z]{1,5})\b|\((?:NYSE|NASDAQ|Nasdaq|NasdaqGS|NasdaqGM):\s?([A-Z]{1,5})\)|"
    r"\(([A-Z]{1,5})\)|\(([A-Z]{1,5})\.(?:O|N|OQ|K|A)\)")
_MACRO_KW_RE = re.compile(
    r"\bFed\b|Federal Reserve|\bFOMC\b|Powell|Warsh|rate (?:cut|hike)s?|"
    r"interest rates?|inflation|\bCPI\b|\bPCE\b|payrolls|jobs report|"
    r"unemployment|\bGDP\b|recession|tariffs?|trade (?:deal|war|truce|talks)|"
    r"Treasur(?:y|ies)|central bank|\bECB\b|\bBOJ\b|Bank of Japan|\bPBOC\b|"
    r"yuan|stimulus|deficit|shutdown|White House|Congress|Senate|midterm|"
    r"China|Beijing|Xi Jinping|美联储|降息|加息|通胀|关税|国债|中美", re.I)
_GEO_KW_RE = re.compile(
    r"Iran|Hormuz|Persian Gulf|Ukraine|Russia|Putin|Zelensky|Gaza|Israel|"
    r"Netanyahu|Hezbollah|Houthi|Yemen|Lebanon|ceasefire|sanction|missile|"
    r"Taiwan|North Korea|OPEC|伊朗|霍尔木兹|俄乌|乌克兰|以色列|加沙|制裁|停火", re.I)
_MARKET_KW_RE = re.compile(
    r"\bstocks?\b|Wall Street|S&P 500|Nasdaq|\bDow\b|equit(?:y|ies)|sell-?off|"
    r"rally|yields?|\bbonds?\b|dollar|\boil\b|crude|\bgold\b|bitcoin|crypto|"
    r"volatility|\bVIX\b|IPO|earnings|guidance|outlook", re.I)


def short_company_name(company: str) -> Optional[str]:
    name = re.sub(r"\s+", " ", company or "").strip()
    for _ in range(3):
        stripped = _NAME_SUFFIX_RE.sub("", name).strip()
        if stripped == name:
            break
        name = stripped
    words = name.split()
    if not words:
        return None
    if words[0].lower().strip(".,&") in _GENERIC_FIRST or len(words[0]) < 4:
        cand = " ".join(words[:2]) if len(words) >= 2 else None
    else:
        cand = words[0]
    return cand if cand and len(cand) >= 4 else None


def text_tickers(text: str) -> Set[str]:
    out: Set[str] = set()
    for m in _TICKER_FORMS.finditer(text or ""):
        out.update(g for g in m.groups() if g)
    return out


class RelevanceContext:
    """当日盘面关心的实体(异动股 / 权重股 / 财报股 / flash L1 涉及标的)。"""

    def __init__(self, tickers: Iterable[str], names: Dict[str, str]):
        self.tickers: Set[str] = {t.upper() for t in tickers if t}
        self.names = {t.upper(): n for t, n in names.items() if n}
        alts = sorted({re.escape(n) for n in self.names.values()}, key=len, reverse=True)
        self.name_re = re.compile(r"\b(?:" + "|".join(alts) + r")\b", re.I) if alts else None

    def entities(self, text: str) -> Set[str]:
        hit = {t for t in text_tickers(text) if t in self.tickers}
        if self.name_re is not None:
            for m in self.name_re.finditer(text or ""):
                hit.add(m.group(0).lower())
        return hit

    def score(self, title: str, text: str) -> int:
        """标题命中权重高于导语;实体命中 > 宏观/地缘 > 一般市场词。"""
        head = (text or "")[:600]
        s = 3 * min(len(self.entities(title or "")), 2)
        s += min(len(self.entities(head)), 2)
        s += 2 * bool(_MACRO_KW_RE.search(title or "")) + bool(_MACRO_KW_RE.search(head))
        s += 2 * bool(_GEO_KW_RE.search(title or "")) + bool(_GEO_KW_RE.search(head))
        s += bool(_MARKET_KW_RE.search(title or ""))
        return s


def build_relevance_context(market: Optional[Dict[str, Any]],
                            earnings_calendar: Optional[Dict[str, Any]],
                            flash_tickers: Iterable[str] = ()) -> RelevanceContext:
    market = market or {}
    cons = [r for r in market.get("sp500_constituents") or [] if isinstance(r, dict)]

    def amp(r: Dict[str, Any]) -> float:
        try:
            return abs(float(r.get("change_percent") or 0))
        except (TypeError, ValueError):
            return 0.0

    def cap(r: Dict[str, Any]) -> float:
        try:
            return float(r.get("market_cap") or 0)
        except (TypeError, ValueError):
            return 0.0

    picked = (sorted(cons, key=amp, reverse=True)[:40]
              + sorted(cons, key=cap, reverse=True)[:30]
              + sorted((r for r in market.get("top_movers") or [] if isinstance(r, dict)),
                       key=amp, reverse=True)[:10])
    names: Dict[str, str] = {}
    tickers: Set[str] = set()
    for r in picked:
        t = str(r.get("ticker") or "").upper()
        if t:
            tickers.add(t)
            n = short_company_name(str(r.get("company") or ""))
            if n:
                names[t] = n
    for bucket in ("today", "prior_after_close"):
        for r in (earnings_calendar or {}).get(bucket) or []:
            t = str(r.get("code") or "").upper()
            if t:
                tickers.add(t)
                n = short_company_name(str(r.get("name") or ""))
                if n:
                    names.setdefault(t, n)
    tickers.update(t.upper() for t in flash_tickers if t)
    return RelevanceContext(tickers, names)


# ── 时段 ───────────────────────────────────────────────────────────────

BUCKETS = ("pre", "session", "post")


def time_bucket(ts: Optional[datetime], report_date: date) -> str:
    """盘前(含 T-1 晚间)/ 盘中 / 盘后(ET)。"""
    if ts is None:
        return "session"
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    e = ts.astimezone(ET)
    if e.date() < report_date or (e.date() == report_date and (e.hour, e.minute) < (9, 30)):
        return "pre"
    if e.date() == report_date and e.hour < 16:
        return "session"
    return "post"


def pick_with_bucket_floor(items: Sequence[Dict[str, Any]], n: int,
                           floor: int, key: Callable[[Dict[str, Any]], Any]
                           ) -> List[Dict[str, Any]]:
    """先给每个时段保底 floor 条(各自最优),余量按全局 key 排序补齐。

    比轮转温和:稀疏时段不会把差稿挤进来,也不会因为全天新闻集中在
    收盘后而丢掉盘前/盘中的报道。"""
    ranked = sorted(items, key=key)
    out: List[Dict[str, Any]] = []
    taken: Set[int] = set()
    for b in BUCKETS:
        for it in [x for x in ranked if x.get("_bucket") == b][:floor]:
            if len(out) < n:
                out.append(it)
                taken.add(id(it))
    for it in ranked:
        if len(out) >= n:
            break
        if id(it) not in taken:
            out.append(it)
            taken.add(id(it))
    return out


# ── 去重聚簇 ───────────────────────────────────────────────────────────

_TOKEN_STOP = frozenset("""
THE AND FOR WITH WILL WON NOT ARE HAS HAVE HAD THAT THIS FROM SAYS SAID SAY SEES
AFTER OVER INTO ITS WAS WERE BEEN BUT OUT ABOUT MORE THAN JUST ALSO WHO WHAT WHEN
THEY THEIR THERE BREAKING NEW NEWS OUR WOULD COULD SHOULD CAN MAY YOU YOUR HIS HER
WHICH BEING DOES DON IT'S US. U.S. PER VIA REPORT REPORTS REPORTED CITE CITES
""".split())


def title_tokens(text: str) -> Set[str]:
    t = re.sub(r"https?://\S+", " ", (text or "").upper())
    t = re.sub(r"[^A-Z0-9%\u4e00-\u9fff ]+", " ", t)
    out: Set[str] = set()
    for w in t.split():
        if len(w) < 3 or w in _TOKEN_STOP:
            continue
        if w.isalpha() and len(w) > 4:
            for suf in ("ING", "ED", "ES", "S"):
                if w.endswith(suf) and len(w) - len(suf) >= 4:
                    w = w[:-len(suf)]
                    break
        out.add(w)
    return out


def near_duplicate(a: Set[str], b: Set[str], min_shared: int = 4,
                   min_ratio: float = 0.7) -> bool:
    if not a or not b:
        return False
    shared = len(a & b)
    return shared >= min_shared and shared / min(len(a), len(b)) >= min_ratio


def cluster_flash(items: List[Dict[str, Any]], window_min: int = 45
                  ) -> List[List[Dict[str, Any]]]:
    """同一 headline 被多个转播号重发 → 一簇。items 需带 _ts(aware datetime)。

    与簇内任一成员近似重复(共享 ≥4 词且占短者 ≥70%)、且距该簇最近一条
    ≤window_min 分钟即入簇。阈值按 11-20 特朗普数据中心 9 连发校准;
    同一官员相反表态("support cut" vs "cut not a done deal")不会被并掉。"""
    clusters: List[Dict[str, Any]] = []
    for it in sorted(items, key=lambda x: x["_ts"]):
        toks = title_tokens(it.get("text") or "")
        it["_tokens"] = toks
        home = None
        for c in reversed(clusters):
            if (it["_ts"] - c["last"]).total_seconds() > window_min * 60:
                continue
            if any(near_duplicate(toks, m["_tokens"]) for m in c["members"]):
                home = c
                break
        if home is None:
            clusters.append({"members": [it], "last": it["_ts"]})
        else:
            home["members"].append(it)
            home["last"] = it["_ts"]
    return [c["members"] for c in clusters]


L_RANK = {"L1": 0, "L2": 1, "L3": 2, None: 2, "": 2}


def flash_representative(members: List[Dict[str, Any]]) -> Dict[str, Any]:
    """簇代表:L 级最高 → 注明出处 → 层级高 → 正文最长(信息最全)。"""
    rep = min(members, key=lambda m: (L_RANK.get(m.get("info_level"), 2),
                                      0 if m.get("origin") else 1,
                                      m.get("tier", 3),
                                      -len(m.get("text") or "")))
    if len(members) > 1:
        rep["dup_n"] = len(members)
        others = [m.get("author") for m in members if m is not rep]
        rep["also"] = list(dict.fromkeys(a for a in others if a))[:5]
        rep["tickers"] = sorted({t for m in members for t in m.get("tickers") or []})[:8]
    return rep


def select_flash(items: List[Dict[str, Any]], cap: int, bucket_floor: int = 25
                 ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """聚簇去重 → L1 全留 → 其余按(层级, 带 ticker, 转发数)选,时段保底。"""
    clusters = cluster_flash(items)
    reps = [flash_representative(c) for c in clusters]
    l1 = [r for r in reps if r.get("info_level") == "L1"]
    rest = [r for r in reps if r.get("info_level") != "L1"]

    def key(r: Dict[str, Any]) -> Tuple:
        return (L_RANK.get(r.get("info_level"), 2), r.get("tier", 3),
                0 if r.get("tickers") else 1, -(r.get("dup_n") or 1), r["_ts"])

    if len(l1) >= cap:
        picked = pick_with_bucket_floor(l1, cap, bucket_floor, key)
    else:
        picked = l1 + pick_with_bucket_floor(rest, cap - len(l1), bucket_floor, key)
    picked.sort(key=lambda r: (L_RANK.get(r.get("info_level"), 2), r["_ts"]))
    stats = {"candidates": len(items), "clusters": len(clusters),
             "merged_away": len(items) - len(clusters),
             "selected": len(picked),
             "by_tier": _count(picked, "tier"),
             "by_bucket": _count(picked, "_bucket")}
    return picked, stats


def select_wire(cands: List[Dict[str, Any]],
                t1_max: int, other_max: int,
                other_max_when_t1: int, t1_floor_for_other_cut: int,
                bucket_floor: int = 2) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """全文层选材。cands 需带 tier/_kind/_rel/_stale/_bucket/_ts/info_level。

    第 1 层:L1 全留,其余按相关度(时段保底),封顶 t1_max;
    第 2-4 层:SEO/行政公告丢弃,过期复盘稿丢弃,公司稿/自动稿须点名当日
    实体,一般稿件须与盘面相关;与第 1 层同题的转载丢弃。"""
    stats: Dict[str, Any] = {"candidates": len(cands), "by_tier_in": _count(cands, "tier"),
                             "dropped": {}}

    def drop(reason: str) -> None:
        stats["dropped"][reason] = stats["dropped"].get(reason, 0) + 1

    t1_all = _dedupe_titles([c for c in cands if c["tier"] == 1])
    stats["t1_available"] = len(t1_all)
    stats["t1_l1_available"] = sum(1 for c in t1_all if c.get("info_level") == "L1")
    l1 = [c for c in t1_all if c.get("info_level") == "L1"]
    l1.sort(key=lambda c: (-c["_rel"], c["_ts"]))
    t1 = l1[:t1_max]
    if len(t1) < t1_max:
        t1 += pick_with_bucket_floor(
            [c for c in t1_all if c.get("info_level") != "L1"], t1_max - len(t1),
            bucket_floor, key=lambda c: (L_RANK.get(c.get("info_level"), 2) if c["_rel"] else 3,
                                         -c["_rel"], -c["_ts"].timestamp()))
    t1_tokens = [title_tokens(c.get("title") or "") for c in t1_all]

    others: List[Dict[str, Any]] = []
    for c in cands:
        if c["tier"] == 1:
            continue
        kind = c["_kind"]
        if kind in ("seo", "lowvalue"):
            drop(kind)
            continue
        if c.get("_stale"):
            drop("stale")
            continue
        if kind in ("pr", "auto") and c["_rel_entity"] == 0:
            drop(kind)
            continue
        if kind == "news" and c["_rel"] == 0:
            drop("off_topic")
            continue
        toks = title_tokens(c.get("title") or "")
        if any(near_duplicate(toks, t, min_shared=4, min_ratio=0.6) for t in t1_tokens):
            drop("dup_of_t1")
            continue
        others.append(c)
    others = _dedupe_titles(others)
    n_other = other_max_when_t1 if len(t1) >= t1_floor_for_other_cut else other_max
    others = pick_with_bucket_floor(
        others, n_other, bucket_floor,
        key=lambda c: (L_RANK.get(c.get("info_level"), 2), c["tier"], -c["_rel"],
                       -c["_ts"].timestamp()))
    picked = t1 + others
    picked.sort(key=lambda c: (c["tier"] != 1, -c["_ts"].timestamp()))
    stats.update({"t1_selected": len(t1),
                  "t1_l1_selected": sum(1 for c in t1 if c.get("info_level") == "L1"),
                  "selected": len(picked), "by_tier": _count(picked, "tier"),
                  "by_bucket": _count(picked, "_bucket")})
    return picked, stats


def _dedupe_titles(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """同题多稿(同一篇剪藏两次 / 同社多次更新)只留层级高、相关度高、最新的一条。"""
    ranked = sorted(items, key=lambda c: (c["tier"], -c["_rel"], -c["_ts"].timestamp()))
    out: List[Dict[str, Any]] = []
    seen: List[Set[str]] = []
    for c in ranked:
        toks = title_tokens(c.get("title") or "")
        if any(near_duplicate(toks, s, min_shared=4, min_ratio=0.8) for s in seen):
            continue
        seen.append(toks)
        out.append(c)
    return out


def _count(items: Iterable[Dict[str, Any]], field: str) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for it in items:
        k = str(it.get(field))
        out[k] = out.get(k, 0) + 1
    return dict(sorted(out.items()))


def strip_private(item: Dict[str, Any]) -> Dict[str, Any]:
    """去掉选材用的内部字段(下划线开头)。"""
    return {k: v for k, v in item.items() if not k.startswith("_")}
