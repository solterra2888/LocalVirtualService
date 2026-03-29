# -*- coding: utf-8 -*-
"""
IB Trading Report Extractor
Parses Interactive Brokers Activity Statement CSV exports into structured data.
Supports both English and Chinese (简体中文) report language variants.

Exports:
    is_ib_trading_report(raw_content) -> bool
    extract_tables_from_content(raw_content) -> (tables, orders)
    to_structured_content(tables, orders) -> str   (JSON matching IBReportData)
"""

import csv
import json
from typing import Dict, List, Tuple, Any, Optional


# ── Language detection & mapping ─────────────────────────────────────────

_EN_TITLE_KEYWORDS = ("Activity Statement",)
_CN_TITLE_KEYWORDS = ("活动账单", "活动报告")
_BROKER_KEYWORDS = ("Interactive Brokers", "BrokerName")

# Section name mapping: Chinese → canonical Chinese key used by frontend
_SECTION_CN_MAP: Dict[str, str] = {
    "账户信息": "账户信息",
    "净资产值": "净资产值",
    "净资产值变更": "净资产值变更",
    "按市值计算的表现总结": "按市值计算的表现总结",
    "已实现和未实现的表现总结": "已实现和未实现的表现总结",
    "现金报告": "现金报告",
    "外汇结余": "外汇结余",
    "净股票持仓总结": "净股票持仓总结",
    "交易": "交易",
    "交易费用": "交易费用",
}

# Section name mapping: English → canonical Chinese key
_SECTION_EN_MAP: Dict[str, str] = {
    "Account Information": "账户信息",
    "Net Asset Value": "净资产值",
    "Change in NAV": "净资产值变更",
    "Mark-to-Market Performance Summary": "按市值计算的表现总结",
    "Realized & Unrealized Performance Summary": "已实现和未实现的表现总结",
    "Cash Report": "现金报告",
    "Forex Balances": "外汇结余",
    "Net Stock Position Summary": "净股票持仓总结",
    "Trades": "交易",
    "Transaction Fees": "交易费用",
}


def _resolve_section(name: str) -> Optional[str]:
    """Map a raw section name to the canonical Chinese key."""
    if name in _SECTION_CN_MAP:
        return _SECTION_CN_MAP[name]
    if name in _SECTION_EN_MAP:
        return _SECTION_EN_MAP[name]
    return None


# ── Trades column mapping ────────────────────────────────────────────────
# The Trades header has "DataDiscriminator" (always English) followed by
# asset/currency/symbol/date/qty/price columns.  Chinese reports use "代码"
# for BOTH Symbol and Code columns, so we map by *position index* relative
# to DataDiscriminator instead of by name.

# Positional index (0-based from first column after the section+disc columns,
# i.e. from column index 2 in the raw CSV row) → frontend JSON key.
_TRADES_POS_MAP: Dict[int, str] = {
    0: "_disc",         # DataDiscriminator
    1: "asset_class",   # Asset Category / 资产分类
    2: "currency",      # Currency / 货币
    3: "symbol",        # Symbol / 代码 (first)
    4: "trade_time",    # Date/Time / 日期/时间
    5: "quantity",      # Quantity / 数量
    6: "trade_price",   # T. Price / 交易价格
    7: "close_price",   # C. Price / 收盘价格
    8: "proceeds",      # Proceeds / 收益
    9: "commission",    # Comm/Fee / 佣金/税
    10: "basis",        # Basis / 基础
    11: "realized_pnl", # Realized P/L / 已实现的损益
    12: "mtm_pnl",      # MTM P/L / 按市值计算的损益
    13: "trade_code",   # Code / 代码 (second)
}

# Account field names that may appear in either language
_ACCOUNT_FIELD_MAP: Dict[str, str] = {
    "Account": "Account",
    "账户": "Account",
    "Period": "Period",
}

# IB Account Information: base currency for statement amounts (MTM summary, etc.)
_BASE_CURRENCY_FIELD_NAMES = frozenset(
    {"Base Currency", "基础货币", "記帳貨幣", "记账货币"}
)


# ── Public API ───────────────────────────────────────────────────────────

def is_ib_trading_report(raw_content: str) -> bool:
    """Return True if *raw_content* looks like an IB Activity Statement CSV."""
    if not raw_content:
        return False
    head = raw_content[:3000]
    has_broker = any(kw in head for kw in _BROKER_KEYWORDS)
    has_title = any(kw in head for kw in _EN_TITLE_KEYWORDS + _CN_TITLE_KEYWORDS)
    return has_broker and has_title


# ── Internal helpers ─────────────────────────────────────────────────────

def _parse_rows(raw_content: str) -> List[List[str]]:
    return list(csv.reader(raw_content.splitlines()))


def _collect_sections(rows: List[List[str]]) -> Dict[str, List[List[str]]]:
    """Group rows by their first column (section name)."""
    sections: Dict[str, List[List[str]]] = {}
    for row in rows:
        if not row:
            continue
        section = row[0].strip()
        if section:
            sections.setdefault(section, []).append(row)
    return sections


def _extract_statement_info(rows: List[List[str]]) -> Dict[str, str]:
    """Extract key-value pairs from the Statement section."""
    info: Dict[str, str] = {}
    for row in rows:
        if len(row) >= 4 and row[1].strip() == "Data":
            info[row[2].strip()] = row[3].strip()
    return info


def _extract_kv_table(rows: List[List[str]]) -> List[Dict[str, str]]:
    """For sections structured as (Section, Header/Data, FieldName, FieldValue)."""
    result: List[Dict[str, str]] = []
    for row in rows:
        if len(row) >= 4 and row[1].strip() == "Data":
            result.append({"域名称": row[2].strip(), "域值": row[3].strip()})
    return result


def _extract_columnar_table(rows: List[List[str]]) -> List[Dict[str, str]]:
    """For sections with a Header row followed by Data rows (variable columns)."""
    headers: List[str] = []
    data_rows: List[Dict[str, str]] = []
    for row in rows:
        discriminator = row[1].strip() if len(row) > 1 else ""
        if discriminator == "Header":
            raw_headers = [c.strip() for c in row[2:]]
            # Deduplicate headers by appending index suffix to duplicates
            seen: Dict[str, int] = {}
            deduped: List[str] = []
            for h in raw_headers:
                if h in seen:
                    seen[h] += 1
                    deduped.append(f"{h}_{seen[h]}")
                else:
                    seen[h] = 0
                    deduped.append(h)
            headers = deduped
        elif discriminator == "Data" and headers:
            vals = [c.strip() if i < len(row) - 2 else "" for i, c in enumerate(row[2:])]
            entry = {}
            for i, h in enumerate(headers):
                if h:
                    entry[h] = vals[i] if i < len(vals) else ""
            if any(v for v in entry.values()):
                data_rows.append(entry)
    return data_rows


# Mark-to-Market: only Stocks / Forex rows; exclude Total / Broker Interest / 合计 等汇总行
_MTM_STOCK_CATS = frozenset({"Stocks", "股票"})
_MTM_FOREX_CATS = frozenset({"Forex", "外汇"})


def _extract_mtm_stocks_forex(
    section_rows: List[List[str]],
) -> Tuple[List[Dict[str, str]], List[Dict[str, str]]]:
    """
    Parse Mark-to-Market Performance Summary into stock vs forex tables.
    Omits Total, Total (All Assets), broker-interest summary rows, etc.
    """
    all_rows = _extract_columnar_table(section_rows)
    if not all_rows:
        return [], []

    sample = all_rows[0]
    cat_key: Optional[str] = None
    for k in ("Asset Category", "资产分类"):
        if k in sample:
            cat_key = k
            break
    if not cat_key:
        return [], []

    stocks: List[Dict[str, str]] = []
    forex: List[Dict[str, str]] = []
    for entry in all_rows:
        cat = (entry.get(cat_key) or "").strip()
        if cat in _MTM_STOCK_CATS:
            stocks.append(entry)
        elif cat in _MTM_FOREX_CATS:
            forex.append(entry)
        # Total, Total (All Assets), Broker Interest… — 不包含
    return stocks, forex


# ── Trades section (special positional handling) ─────────────────────────

# IB Activity Statement: only rows with second column == "Data" and
# DataDiscriminator == "Order" are real executions. SubTotal / Total / 小计 / 合计
# rows must never appear in orders.
_TRADE_SUMMARY_ROW_TYPES = frozenset(
    {"SubTotal", "Total", "小计", "合计", "总计", "Sub Total", "Subtotal"}
)


def _is_stock_trades_header(header_cols: List[str]) -> bool:
    """Check if this is the main stock trades header (has 14+ columns with
    DataDiscriminator), as opposed to the shorter forex trades header."""
    return "DataDiscriminator" in header_cols and len(header_cols) >= 14


def _extract_trades(rows: List[List[str]]) -> List[Dict[str, str]]:
    """Extract individual Order rows from the Trades / 交易 section."""
    orders: List[Dict[str, str]] = []
    active_header: Optional[List[str]] = None
    is_stock_header = False

    for row in rows:
        if len(row) < 3:
            continue
        disc = row[1].strip()

        # Skip SubTotal / Total / 小计 / 合计 rows (not under "Data")
        if disc in _TRADE_SUMMARY_ROW_TYPES:
            continue

        if disc == "Header":
            candidate = [c.strip() for c in row[2:]]
            if "DataDiscriminator" in candidate:
                active_header = candidate
                is_stock_header = _is_stock_trades_header(candidate)
            continue

        # Only real execution lines use row type "Data" (IB English export)
        if disc != "Data" or not active_header:
            continue

        vals = [c.strip() for c in row[2:]]

        # DataDiscriminator column: must be "Order", never SubTotal/Total/合计
        disc_idx = 0
        if not vals:
            continue
        row_kind = vals[disc_idx]
        if row_kind in _TRADE_SUMMARY_ROW_TYPES:
            continue
        if row_kind != "Order":
            continue

        if is_stock_header:
            # Use positional mapping for stock trades
            order: Dict[str, str] = {}
            for pos, key in _TRADES_POS_MAP.items():
                if key == "_disc":
                    continue
                order[key] = vals[pos] if pos < len(vals) else ""
            order["quantity"] = order["quantity"].replace(",", "")
            orders.append(order)
        else:
            # Forex trades header: different layout, map by position too
            order = {
                "asset_class": vals[1] if len(vals) > 1 else "",
                "currency": vals[2] if len(vals) > 2 else "",
                "symbol": vals[3] if len(vals) > 3 else "",
                "trade_time": vals[4] if len(vals) > 4 else "",
                "quantity": (vals[5] if len(vals) > 5 else "").replace(",", ""),
                "trade_price": vals[6] if len(vals) > 6 else "",
                "close_price": "",
                "proceeds": vals[8] if len(vals) > 8 else "",
                "commission": vals[9] if len(vals) > 9 else "",
                "basis": "",
                "realized_pnl": "",
                "mtm_pnl": vals[12] if len(vals) > 12 else "",
                "trade_code": "",
            }
            orders.append(order)

    return orders


def extract_tables_from_content(
    raw_content: str,
) -> Tuple[Dict[str, List[Dict[str, str]]], List[Dict[str, str]]]:
    """
    Parse an IB Activity Statement CSV (English or Chinese).

    Returns
    -------
    tables : dict[str, list[dict]]
        Keyed by Chinese table name matching the frontend UI.
    orders : list[dict]
        Individual trade orders keyed for ``ORDER_COLUMNS`` on the frontend.
    """
    rows = _parse_rows(raw_content)
    sections = _collect_sections(rows)
    tables: Dict[str, List[Dict[str, str]]] = {}

    # Statement info (always keyed as "Statement" in both languages)
    stmt_info = _extract_statement_info(sections.get("Statement", []))

    # Build a lookup: canonical Chinese key → raw section rows
    resolved: Dict[str, List[List[str]]] = {}
    for raw_name, raw_rows in sections.items():
        canonical = _resolve_section(raw_name)
        if canonical:
            resolved.setdefault(canonical, []).extend(raw_rows)

    # 账户信息 (Account Information)
    acct_data = _extract_kv_table(resolved.get("账户信息", []))
    period_val = stmt_info.get("Period", "")
    if period_val:
        acct_data.insert(0, {"域名称": "Period", "域值": period_val})
    if acct_data:
        tables["账户信息"] = acct_data

    # 净资产值 (Net Asset Value)
    nav_data = _extract_columnar_table(resolved.get("净资产值", []))
    if nav_data:
        tables["净资产值"] = nav_data

    # 净资产值变更 (Change in NAV)
    nav_change = _extract_kv_table(resolved.get("净资产值变更", []))
    if nav_change:
        tables["净资产值变更"] = nav_change

    # 按市值计算的表现总结 — 股票 / 外汇分表，不含 Total 汇总行
    mtm_stocks, mtm_forex = _extract_mtm_stocks_forex(
        resolved.get("按市值计算的表现总结", [])
    )
    if mtm_stocks:
        tables["按市值计算的表现总结_股票"] = mtm_stocks
    if mtm_forex:
        tables["按市值计算的表现总结_外汇"] = mtm_forex

    # 交易 → orders  (Trades)
    orders = _extract_trades(resolved.get("交易", []))

    return tables, orders


def to_structured_content(
    tables: Dict[str, List[Dict[str, str]]],
    orders: List[Dict[str, str]],
) -> str:
    """
    Serialize to a JSON string that the frontend ``isIBReportContent()``
    recognises as ``IBReportData``.
    """
    account_id = ""
    period = ""
    base_currency = ""
    for row in tables.get("账户信息", []):
        name = row.get("域名称", "")
        val = row.get("域值", "")
        mapped = _ACCOUNT_FIELD_MAP.get(name)
        if mapped == "Account":
            account_id = val
        if mapped == "Period" or name == "Period":
            period = val
        if name in _BASE_CURRENCY_FIELD_NAMES and val.strip():
            base_currency = val.strip()

    md_parts: List[str] = []
    for tname, trows in tables.items():
        if not trows:
            continue
        md_parts.append(f"## {tname}\n")
        keys = list(trows[0].keys())
        md_parts.append("| " + " | ".join(keys) + " |")
        md_parts.append("| " + " | ".join(["---"] * len(keys)) + " |")
        for r in trows:
            md_parts.append("| " + " | ".join(r.get(k, "") for k in keys) + " |")
        md_parts.append("")

    if orders:
        md_parts.append("## 交易明细\n")
        o_keys = list(orders[0].keys())
        md_parts.append("| " + " | ".join(o_keys) + " |")
        md_parts.append("| " + " | ".join(["---"] * len(o_keys)) + " |")
        for o in orders:
            md_parts.append("| " + " | ".join(o.get(k, "") for k in o_keys) + " |")
        md_parts.append("")

    data: Dict[str, Any] = {
        "_type": "ib_trading_report",
        "account_id": account_id,
        "period": period,
        "base_currency": base_currency,
        "tables": tables,
        "orders": orders,
        "markdown": "\n".join(md_parts),
    }
    return json.dumps(data, ensure_ascii=False)
