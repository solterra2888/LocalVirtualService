# -*- coding: utf-8 -*-
"""
Theme Report Service

生成"Theme Details"静态 HTML 报告：
- 为标注了 'Theme Report' 标签的每个主题，计算各 Category 和 Constituent 的多周期收益率
- 用 Jinja2 渲染为自包含 HTML
- 上传至阿里云 OSS
- 将记录写入 theme_reports 表
"""

import os
import re
import json
import time
from datetime import date, datetime, timedelta, timezone
from typing import Dict, List, Any, Optional, Tuple
from zoneinfo import ZoneInfo
from uuid import uuid4

import pandas as pd

from backend.database.db_manager import DatabaseManager
from backend.utils.database_config import get_database_config
from backend.utils.datetime_utils import utcnow, utc_today
from backend.utils.logger import get_logger
from backend.services.oss_service import OSSService

logger = get_logger("theme_report_service")

# 周期名称 -> 回溯日历天数
# All horizons use a fixed calendar-day lookback from price_as_of:
#   1D  = 1  calendar day  (prior trading day close)
#   1W  = 7  calendar days (≈ 5 trading days; handles holidays via MAX_GAP_DAYS)
#   1M  = 21 calendar days (3 weeks)
#   3M  = 63 calendar days (9 weeks)
#   1Y  = 364 calendar days (52 weeks); partial histories included for new listings
#
# NOTE: 1W previously used WTD (week-to-date) logic but that caused 0% returns
# whenever price_as_of fell on a Monday (start == end).  Fixed to uniform
# 7-calendar-day lookback to match the reference dashboard (dashboard.cntlabs.io).
RETURN_HORIZONS: Dict[str, int] = {
    "1D": 1,
    "1W": 7,
    "1M": 21,
    "3M": 63,
    "1Y": 364,
}

TAG_NAME = "Theme Report"
STOCK_CHART_CALENDAR_DAYS = 380  # lookback to collect ~260 trading bars
STOCK_CHART_MAX_BARS = 260

_TZ_DISPLAY_LABELS: Dict[str, str] = {
    "America/New_York": "US Eastern (New York)",
    "Asia/Shanghai": "China (Beijing)",
}


def _build_generation_time_display(
    generated_at_utc: datetime,
    scheduler_timezone: Optional[str] = None,
) -> Dict[str, Optional[str]]:
    """Format report generation instant in NY, Beijing, and optional scheduler TZ."""
    if generated_at_utc.tzinfo is None:
        generated_at_utc = generated_at_utc.replace(tzinfo=timezone.utc)

    def _fmt_in_zone(dt: datetime, tz_name: str, place_label: str) -> str:
        local = dt.astimezone(ZoneInfo(tz_name))
        return f"{local.strftime('%b %d, %Y %H:%M %Z')} ({place_label})"

    out: Dict[str, Optional[str]] = {
        "generated_at_utc": generated_at_utc.strftime("%Y-%m-%d %H:%M UTC"),
        "generated_at_new_york": _fmt_in_zone(
            generated_at_utc, "America/New_York", "New York"
        ),
        "generated_at_beijing": _fmt_in_zone(
            generated_at_utc, "Asia/Shanghai", "Beijing"
        ),
        "scheduler_timezone": scheduler_timezone,
        "scheduler_timezone_label": None,
        "generated_at_scheduler_tz": None,
    }

    if scheduler_timezone:
        try:
            local = generated_at_utc.astimezone(ZoneInfo(scheduler_timezone))
            out["scheduler_timezone_label"] = _TZ_DISPLAY_LABELS.get(
                scheduler_timezone,
                scheduler_timezone.replace("_", " "),
            )
            out["generated_at_scheduler_tz"] = local.strftime("%b %d, %Y %H:%M %Z")
        except Exception:
            out["scheduler_timezone_label"] = scheduler_timezone

    return out


def _title_words(text: str) -> str:
    """Capitalize the first letter of each word (e.g. 'ai data centers' → 'Ai Data Centers')."""
    if not text:
        return text
    return " ".join(word.capitalize() for word in text.split())


def _chart_ticker_key(ticker: str) -> str:
    """Display / chart lookup key (matches reference dashboard data-ticker)."""
    return ticker.replace(".US", "").replace(".HK", "")


def _slugify(text: str) -> str:
    s = text.lower().strip()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return s.strip("_")[:80] or "theme"


def _theme_key(theme_id: str, theme_name: str) -> str:
    if theme_id and not str(theme_id).isdigit():
        candidate = _slugify(str(theme_id))
        if candidate:
            return candidate
    return _slugify(theme_name)


class ThemeReportService:
    """Theme Report 生成服务"""

    def __init__(self):
        db_config = get_database_config()
        self.db_manager = DatabaseManager(db_config)
        self.oss_service = OSSService()
        logger.info("ThemeReportService initialized")

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def generate_and_upload(
        self,
        user_id: str,
        report_date: Optional[date] = None,
        scheduler_timezone: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        为指定用户生成 Theme Report，上传 OSS，写入 theme_reports 表。

        Returns:
            dict with keys: success, oss_object_name, public_url / signed_url, report_date, stats
        """
        if report_date is None:
            report_date = utc_today()

        generated_at_utc = utcnow()
        generation_times = _build_generation_time_display(
            generated_at_utc, scheduler_timezone
        )

        start_ts = time.time()
        date_str = report_date.strftime("%Y%m%d")
        oss_object_name = (
            f"reports/Themes/{user_id}/report_{date_str}/report_embedded.html"
        )

        conn = self.db_manager.get_connection()
        cursor = conn.cursor()

        # 创建 pending 记录（若已存在则更新为 generating）
        cursor.execute(
            """
            INSERT INTO theme_reports
                (user_id, report_date, oss_object_name, status, created_at, updated_at)
            VALUES (%s, %s, %s, 'generating', NOW(), NOW())
            ON CONFLICT (user_id, report_date)
            DO UPDATE SET status='generating', updated_at=NOW(), error_message=NULL
            RETURNING id
            """,
            (user_id, report_date, oss_object_name),
        )
        report_id = cursor.fetchone()[0]
        conn.commit()

        try:
            # 1. 获取带 'Theme Report' 标签的主题及其关联 tickers
            themes_data = self._get_tagged_themes(user_id, cursor)
            if not themes_data:
                raise ValueError(
                    f"No themes tagged '{TAG_NAME}' found for user {user_id}"
                )

            # 2. 计算各 ticker 的多周期收益率
            all_tickers = list(
                {t for td in themes_data.values() for t in td["tickers"]}
            )
            ticker_returns, price_as_of = self._compute_ticker_returns(
                all_tickers, report_date, cursor
            )

            # 3. 构建每个主题的 Theme Details 数据
            themes_details = self._build_theme_details(themes_data, ticker_returns)

            # 3b. 计算主题级别 Performance（Performance Table / Cumulative / Multi-Period）
            theme_performance = self._compute_theme_performance(
                themes_data, ticker_returns, report_date, cursor
            )

            executive_summary = self._build_executive_summary(theme_performance)

            # 3c. OHLCV series for Individual Stock Charts
            stock_chart_data = self._fetch_stock_chart_data(
                all_tickers, report_date, cursor
            )
            stock_charts = self._build_stock_charts(themes_data)

            # 4. 渲染 HTML
            html = self._render_html(
                themes_details=themes_details,
                theme_performance=theme_performance,
                executive_summary=executive_summary,
                stock_charts=stock_charts,
                stock_chart_data=stock_chart_data,
                report_date=report_date,
                price_as_of=price_as_of,
                user_id=user_id,
                generation_times=generation_times,
            )

            # 5. 上传至 OSS
            upload_result = self.oss_service.upload_bytes(
                object_name=oss_object_name,
                data=html.encode("utf-8"),
                content_type="text/html; charset=utf-8",
                content_disposition="inline",
            )
            if not upload_result.get("success"):
                raise RuntimeError(
                    f"OSS upload failed: {upload_result.get('error')}"
                )

            # 6. 统计
            theme_count = len(themes_details)
            category_count = sum(len(td["categories"]) for td in themes_details)
            constituent_count = sum(len(td["constituents"]) for td in themes_details)
            elapsed = round(time.time() - start_ts, 1)

            metadata = {
                "as_of_date": str(report_date),
                "price_as_of_date": str(price_as_of) if price_as_of else None,
                "generation_time_seconds": elapsed,
                "theme_names": [td["theme_name"] for td in themes_details],
                **generation_times,
            }

            share_token = uuid4().hex

            cursor.execute(
                """
                UPDATE theme_reports
                SET status='completed',
                    theme_count=%s, category_count=%s, constituent_count=%s,
                    metadata=%s, share_token=%s, html_content=%s, updated_at=NOW()
                WHERE id=%s
                """,
                (
                    theme_count,
                    category_count,
                    constituent_count,
                    json.dumps(metadata),
                    share_token,
                    html,
                    report_id,
                ),
            )
            conn.commit()

            logger.info(
                f"Theme report generated: user={user_id}, date={report_date}, "
                f"themes={theme_count}, categories={category_count}, "
                f"constituents={constituent_count}, elapsed={elapsed}s"
            )

            return {
                "success": True,
                "report_id": report_id,
                "oss_object_name": oss_object_name,
                "report_date": str(report_date),
                "stats": {
                    "theme_count": theme_count,
                    "category_count": category_count,
                    "constituent_count": constituent_count,
                    "generation_time_seconds": elapsed,
                },
            }

        except Exception as exc:
            logger.error(
                f"Theme report generation failed for user={user_id}: {exc}",
                exc_info=True,
            )
            try:
                cursor.execute(
                    """
                    UPDATE theme_reports
                    SET status='failed', error_message=%s, updated_at=NOW()
                    WHERE id=%s
                    """,
                    (str(exc), report_id),
                )
                conn.commit()
            except Exception:
                pass
            return {"success": False, "error": str(exc)}
        finally:
            cursor.close()
            conn.close()

    # ------------------------------------------------------------------
    # DB helpers
    # ------------------------------------------------------------------

    def _get_tagged_themes(
        self, user_id: str, cursor
    ) -> Dict[str, Dict[str, Any]]:
        """
        返回 {theme_name: {theme_id, categories: {category: [tickers]}, tickers: [...]}}
        仅包含打了 'Theme Report' 标签 AND is_verified=TRUE 的 tickers。
        """
        cursor.execute(
            """
            SELECT DISTINCT
                ite.theme_id,
                ite.theme_name,
                ite.category,
                tca.ticker,
                tca.company_name
            FROM investment_theme_embeddings ite
            INNER JOIN theme_company_associations tca
                ON ite.theme_id = tca.theme_id AND tca.is_verified = TRUE
            INNER JOIN theme_tags tt
                ON ite.theme_id = tt.theme_id
                AND tt.tag_name = %s
                AND tt.user_id = %s
            ORDER BY ite.theme_name, ite.category, tca.ticker
            """,
            (TAG_NAME, user_id),
        )
        rows = cursor.fetchall()

        themes: Dict[str, Dict[str, Any]] = {}
        for theme_id, theme_name, category, ticker, company_name in rows:
            theme_name = _title_words(theme_name)
            category = _title_words(category)
            if theme_name not in themes:
                themes[theme_name] = {
                    "theme_id": theme_id,
                    "theme_name": theme_name,
                    "categories": {},
                    "tickers": [],
                    "ticker_info": {},
                }
            td = themes[theme_name]
            if category not in td["categories"]:
                td["categories"][category] = []
            if ticker not in td["categories"][category]:
                td["categories"][category].append(ticker)
            if ticker not in td["tickers"]:
                td["tickers"].append(ticker)
            td["ticker_info"][ticker] = {
                "company_name": company_name or ticker,
                "category": category,
            }

        logger.info(
            f"Found {len(themes)} themes with tag '{TAG_NAME}' for user {user_id}"
        )
        return themes

    # ------------------------------------------------------------------
    # Return calculations
    # ------------------------------------------------------------------

    def _compute_ticker_returns(
        self,
        tickers: List[str],
        as_of: date,
        cursor,
    ) -> Tuple[Dict[str, Dict[str, Optional[float]]], Optional[date]]:
        """
        For each ticker, compute simple % returns for all horizons.
        Returns:
            ticker_returns: {ticker: {"1D": float|None, "1W": ..., "1M": ..., "3M": ..., "1Y": ...}}
            price_as_of: the actual latest price date available
        """
        if not tickers:
            return {}, None

        # Look back far enough for 1Y (365 + buffer)
        lookback_start = as_of - timedelta(days=400)

        cursor.execute(
            """
            SELECT code, date::date AS trade_date, close
            FROM stock_prices_eod
            WHERE code = ANY(%s)
              AND date BETWEEN %s AND %s
              AND close IS NOT NULL
              AND period = 'd'
            ORDER BY code, date
            """,
            (tickers, lookback_start, as_of),
        )
        rows = cursor.fetchall()

        if not rows:
            logger.warning(
                f"No EOD price data found for {len(tickers)} tickers up to {as_of}"
            )
            return {t: {h: None for h in RETURN_HORIZONS} for t in tickers}, None

        df = pd.DataFrame(rows, columns=["code", "date", "close"])
        df = df.sort_values("date").drop_duplicates(
            subset=["code", "date"], keep="last"
        )

        # Determine actual latest price date
        price_as_of: Optional[date] = df["date"].max() if len(df) else None

        # Pivot: index=date, columns=code
        pivot = df.pivot(index="date", columns="code", values="close")
        pivot = pivot.ffill().bfill()

        ticker_returns: Dict[str, Dict[str, Optional[float]]] = {
            t: {} for t in tickers
        }

        # ----------------------------------------------------------------
        # IMPORTANT: anchor ALL horizons to price_as_of (the last actual
        # trading day with data), NOT to report_date (today's calendar date).
        #
        # If report_date is a weekend/holiday or today's EOD hasn't been
        # fetched yet, using report_date as the end anchor causes:
        #   - 1D: start = report_date - 1 day  → resolves to same trading
        #         day as end → 0% return
        #   - 1W/1M/3M/1Y: window shifts by the gap between report_date
        #         and the last trading day, giving wrong start prices
        #
        # Using price_as_of (e.g. May 29 when report_date=June 1) means:
        #   - 1D: end=May29, start=last≤May28  → correct daily return
        #   - 1W: end=May29, start=last≤May22  → matches reference
        # ----------------------------------------------------------------
        if price_as_of is None:
            return {t: {h: None for h in RETURN_HORIZONS} for t in tickers}, None

        # Maximum tolerated gap (calendar days) between start_anchor and the
        # nearest actual data point.  If the price data is older than this we
        # treat the return as unavailable rather than silently using a stale
        # price that may be months away (caused by data gaps in the DB).
        # Allow up to 10 calendar days to cover normal holiday stretches.
        # Allow up to 10 calendar days gap for short-horizon returns (covers
        # weekends + holiday stretches).  Not applied to 1Y (see below).
        MAX_GAP_DAYS = 10

        for label, days in RETURN_HORIZONS.items():
            # Anchor to the last actual trading date, not the calendar date
            end_anchor = price_as_of
            start_anchor = price_as_of - timedelta(days=days)

            for ticker in tickers:
                if ticker not in pivot.columns:
                    ticker_returns[ticker][label] = None
                    continue
                series = pivot[ticker].dropna()
                # end price: last available on or before price_as_of
                end_idx = series.index[series.index <= end_anchor]
                if len(end_idx) == 0:
                    ticker_returns[ticker][label] = None
                    continue
                end_price = series[end_idx[-1]]

                # start price: last available on or before start_anchor
                start_idx = series.index[series.index <= start_anchor]
                if len(start_idx) == 0:
                    if label == "1Y":
                        # Newly-listed tickers (< 1 year of history): use their
                        # earliest available price so they are included in the
                        # equal-weight average.  This matches the reference
                        # dashboard and avoids survivorship bias from only
                        # keeping long-established high-returning stocks.
                        within_window = series.index[series.index > start_anchor]
                        if len(within_window) == 0:
                            ticker_returns[ticker][label] = None
                            continue
                        start_price = series[within_window[0]]
                    else:
                        ticker_returns[ticker][label] = None
                        continue
                else:
                    actual_start_date = start_idx[-1]
                    gap = (start_anchor - actual_start_date).days
                    if label == "1Y":
                        # No strict gap guard for 1Y: a ticker listed 300 days
                        # ago legitimately has a ~300-day start gap; excluding it
                        # would create survivorship bias.
                        start_price = series[actual_start_date]
                    elif gap > MAX_GAP_DAYS:
                        # Stale ffill across a data gap — treat as unavailable
                        ticker_returns[ticker][label] = None
                        continue
                    else:
                        start_price = series[actual_start_date]

                if start_price and start_price > 0:
                    ticker_returns[ticker][label] = round(
                        (end_price / start_price - 1) * 100, 2
                    )
                else:
                    ticker_returns[ticker][label] = None

        return ticker_returns, price_as_of

    # ------------------------------------------------------------------
    # Theme Details assembly
    # ------------------------------------------------------------------

    def _build_theme_details(
        self,
        themes_data: Dict[str, Dict[str, Any]],
        ticker_returns: Dict[str, Dict[str, Optional[float]]],
    ) -> List[Dict[str, Any]]:
        """
        Build the list of theme detail dicts for HTML rendering.

        Each entry:
        {
            theme_name,
            insights: [str, str, str, str],
            categories: [
                {rank, category, 1D, 1W, 1M, 3M, 1Y, top_tickers: [str]}
            ],
            constituents: [
                {ticker, company_name, category, 1D, 1W, 1M, 3M, 1Y}
            ],
        }
        """
        themes_details = []

        for theme_name, td in themes_data.items():
            # --- Constituents ---
            constituents = []
            for ticker in td["tickers"]:
                rets = ticker_returns.get(ticker, {})
                info = td["ticker_info"].get(ticker, {})
                constituents.append(
                    {
                        "ticker": ticker.replace(".US", "").replace(".HK", ""),
                        "ticker_full": ticker,
                        "company_name": info.get("company_name", ticker),
                        "category": info.get("category", ""),
                        "1D": rets.get("1D"),
                        "1W": rets.get("1W"),
                        "1M": rets.get("1M"),
                        "3M": rets.get("3M"),
                        "1Y": rets.get("1Y"),
                    }
                )
            # Sort by 3M desc
            constituents.sort(
                key=lambda x: x["3M"] if x["3M"] is not None else -9999,
                reverse=True,
            )

            # --- Categories (equal-weighted avg of constituent returns) ---
            categories = []
            for cat_name, cat_tickers in td["categories"].items():
                cat_row: Dict[str, Any] = {"category": cat_name}
                for label in RETURN_HORIZONS:
                    vals = [
                        ticker_returns.get(t, {}).get(label)
                        for t in cat_tickers
                        if ticker_returns.get(t, {}).get(label) is not None
                    ]
                    cat_row[label] = (
                        round(sum(vals) / len(vals), 2) if vals else None
                    )
                # Top constituents for this category (by 3M)
                cat_members = [
                    c for c in constituents if c["category"] == cat_name
                ]
                cat_members_sorted = sorted(
                    cat_members,
                    key=lambda x: x["3M"] if x["3M"] is not None else -9999,
                    reverse=True,
                )
                cat_row["top_tickers"] = [
                    c["ticker"] for c in cat_members_sorted[:5]
                ]
                categories.append(cat_row)

            # Sort categories by 3M desc
            categories.sort(
                key=lambda x: x["3M"] if x["3M"] is not None else -9999,
                reverse=True,
            )
            # Assign ranks
            for i, cat in enumerate(categories, 1):
                cat["rank"] = i

            # --- Insights ---
            def _fmt(v: Optional[float]) -> str:
                if v is None:
                    return "n/a"
                sign = "+" if v >= 0 else ""
                return f"{sign}{v:.1f}%"

            cats_with_1m = [c for c in categories if c["1M"] is not None]
            cats_with_3m = [c for c in categories if c["3M"] is not None]
            constits_with_1m = [c for c in constituents if c["1M"] is not None]
            constits_with_1m_sorted = sorted(
                constits_with_1m,
                key=lambda x: x["1M"],
                reverse=True,
            )

            insights = []
            if cats_with_1m:
                cats_1m_sorted = sorted(cats_with_1m, key=lambda x: x["1M"], reverse=True)
                best_1m = cats_1m_sorted[0]
                worst_1m = cats_1m_sorted[-1]
                insights.append(
                    f"1M category leadership is {best_1m['category']} "
                    f"({_fmt(best_1m['1M'])}); "
                    f"{worst_1m['category']} is the weakest at {_fmt(worst_1m['1M'])}."
                )
            if cats_with_3m:
                cats_3m_sorted = sorted(cats_with_3m, key=lambda x: x["3M"], reverse=True)
                best_3m = cats_3m_sorted[0]
                worst_3m = cats_3m_sorted[-1]
                insights.append(
                    f"3M category leadership is {best_3m['category']} "
                    f"({_fmt(best_3m['3M'])}); "
                    f"{worst_3m['category']} is the weakest at {_fmt(worst_3m['3M'])}."
                )
            if constits_with_1m_sorted:
                top3 = constits_with_1m_sorted[:3]
                insights.append(
                    "Top 1M constituents: "
                    + ", ".join(
                        f"{c['ticker']} {_fmt(c['1M'])}" for c in top3
                    )
                    + "."
                )
                bot3 = constits_with_1m_sorted[-3:]
                bot3.reverse()
                insights.append(
                    "Bottom 1M constituents: "
                    + ", ".join(
                        f"{c['ticker']} {_fmt(c['1M'])}" for c in bot3
                    )
                    + "."
                )

            themes_details.append(
                {
                    "theme_name": theme_name,
                    "insights": insights,
                    "categories": categories,
                    "constituents": constituents,
                }
            )

        # Sort themes by name
        themes_details.sort(key=lambda x: x["theme_name"])
        return themes_details

    # ------------------------------------------------------------------
    # Theme-level performance (Performance Table / Cumulative / Multi-Period)
    # ------------------------------------------------------------------

    def _compute_theme_performance(
        self,
        themes_data: Dict[str, Dict[str, Any]],
        ticker_returns: Dict[str, Dict[str, Optional[float]]],
        as_of: date,
        cursor,
    ) -> List[Dict[str, Any]]:
        """
        Compute theme-level aggregate performance for the Performance section:
          - Multi-period returns (1D/1W/1M/3M/1Y) — equal-weight of constituents
          - Cumulative daily return series (indexed to 0% at the window start)

        Returns a list sorted by 3M descending (same ranking as Performance Table).
        """
        CHART_DAYS = 180  # cumulative chart window

        # ----------------------------------------------------------
        # 1. Fetch daily price data for all tickers (last CHART_DAYS days)
        # ----------------------------------------------------------
        all_tickers = list({t for td in themes_data.values() for t in td["tickers"]})
        window_start = as_of - timedelta(days=CHART_DAYS + 20)  # small buffer

        cursor.execute(
            """
            SELECT code, date::date AS trade_date, close
            FROM stock_prices_eod
            WHERE code = ANY(%s)
              AND date BETWEEN %s AND %s
              AND close IS NOT NULL
              AND period = 'd'
            ORDER BY code, date
            """,
            (all_tickers, window_start, as_of),
        )
        rows = cursor.fetchall()

        if not rows:
            return []

        df = pd.DataFrame(rows, columns=["code", "date", "close"])
        df = df.sort_values("date").drop_duplicates(subset=["code", "date"], keep="last")
        pivot = df.pivot(index="date", columns="code", values="close").ffill().bfill()

        # ----------------------------------------------------------
        # 2. Build per-theme cumulative series
        # ----------------------------------------------------------
        theme_perf = []

        for theme_name, td in themes_data.items():
            tickers = [t for t in td["tickers"] if t in pivot.columns]
            if not tickers:
                continue

            # Equal-weight portfolio: daily level = mean of normalised prices
            sub = pivot[tickers].copy()
            # Normalise each ticker to 1.0 at the first available date
            sub_norm = sub / sub.iloc[0]
            portfolio = sub_norm.mean(axis=1)  # equal-weight index

            # Cumulative % return series (indexed to 0% at window start)
            cum_series_raw = ((portfolio / portfolio.iloc[0]) - 1) * 100
            # Convert to list of {date, value} for JSON serialisation
            cum_series = [
                {"date": str(d), "value": round(float(v), 2)}
                for d, v in cum_series_raw.items()
            ]

            # Multi-period returns from pre-computed ticker_returns (equal-weight)
            def _theme_ret(label: str) -> Optional[float]:
                vals = [
                    ticker_returns.get(t, {}).get(label)
                    for t in td["tickers"]
                    if ticker_returns.get(t, {}).get(label) is not None
                ]
                return round(sum(vals) / len(vals), 2) if vals else None

            theme_perf.append(
                {
                    "theme_name": theme_name,
                    "1D": _theme_ret("1D"),
                    "1W": _theme_ret("1W"),
                    "1M": _theme_ret("1M"),
                    "3M": _theme_ret("3M"),
                    "1Y": _theme_ret("1Y"),
                    "cum_series": cum_series,
                }
            )

        # Sort by 3M descending (None last)
        theme_perf.sort(
            key=lambda x: x["3M"] if x["3M"] is not None else -9999,
            reverse=True,
        )
        # Add rank
        for i, tp in enumerate(theme_perf):
            tp["rank"] = i + 1

        return theme_perf

    def _build_executive_summary(
        self, theme_performance: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Portfolio-level executive summary (KPI cards + PM Read bullets),
        aligned with the reference Theme Performance Dashboard.
        """
        if not theme_performance:
            return {"kpis": [], "insights": []}

        def _fmt(v: Optional[float]) -> str:
            if v is None:
                return "n/a"
            sign = "+" if v >= 0 else ""
            return f"{sign}{v:.1f}%"

        def _tone(ret: Optional[float]) -> str:
            if ret is None:
                return "neutral"
            return "positive" if ret >= 0 else "negative"

        def _best_for(horizon: str) -> Optional[Dict[str, Any]]:
            candidates = [
                tp for tp in theme_performance if tp.get(horizon) is not None
            ]
            if not candidates:
                return None
            return max(candidates, key=lambda tp: tp[horizon])

        def _worst_for(horizon: str) -> Optional[Dict[str, Any]]:
            candidates = [
                tp for tp in theme_performance if tp.get(horizon) is not None
            ]
            if not candidates:
                return None
            return min(candidates, key=lambda tp: tp[horizon])

        def _median(vals: List[float]) -> Optional[float]:
            if not vals:
                return None
            s = sorted(vals)
            n = len(s)
            mid = n // 2
            if n % 2 == 1:
                return s[mid]
            return (s[mid - 1] + s[mid]) / 2

        def _breadth(horizon: str) -> Tuple[int, int]:
            with_data = [
                tp for tp in theme_performance if tp.get(horizon) is not None
            ]
            positive = sum(1 for tp in with_data if tp[horizon] > 0)
            return positive, len(with_data)

        kpis: List[Dict[str, Any]] = []
        for horizon in ("1D", "1W", "1M", "3M", "1Y"):
            best = _best_for(horizon)
            if not best:
                continue
            ret = best[horizon]
            kpis.append(
                {
                    "label": f"Best {horizon} Theme",
                    "theme_name": best["theme_name"],
                    "sublabel": _fmt(ret),
                    "tone": _tone(ret),
                }
            )

        worst_3m = _worst_for("3M")
        if worst_3m:
            ret = worst_3m["3M"]
            kpis.append(
                {
                    "label": "Worst 3M Theme",
                    "theme_name": worst_3m["theme_name"],
                    "sublabel": _fmt(ret),
                    "tone": _tone(ret),
                }
            )

        pos_1m, total_1m = _breadth("1M")
        if total_1m:
            kpis.append(
                {
                    "label": "Positive 1M Breadth",
                    "theme_name": f"{pos_1m} / {total_1m}",
                    "sublabel": "themes above zero",
                    "tone": "positive",
                }
            )

        pos_3m, total_3m = _breadth("3M")
        if total_3m:
            kpis.append(
                {
                    "label": "Positive 3M Breadth",
                    "theme_name": f"{pos_3m} / {total_3m}",
                    "sublabel": "themes above zero",
                    "tone": "positive",
                }
            )

        insights: List[str] = []
        best_1m = _best_for("1M")
        worst_1m = _worst_for("1M")
        if best_1m and worst_1m:
            insights.append(
                f"1M leadership is led by {best_1m['theme_name']} "
                f"({_fmt(best_1m['1M'])}); "
                f"{worst_1m['theme_name']} trails at {_fmt(worst_1m['1M'])}."
            )

        best_3m = _best_for("3M")
        if best_3m and worst_3m:
            insights.append(
                f"3M leadership is led by {best_3m['theme_name']} "
                f"({_fmt(best_3m['3M'])}); "
                f"{worst_3m['theme_name']} trails at {_fmt(worst_3m['3M'])}."
            )

        if total_3m:
            med_3m = _median(
                [tp["3M"] for tp in theme_performance if tp.get("3M") is not None]
            )
            insights.append(
                f"3M breadth is {pos_3m} of {total_3m} themes positive, "
                f"with median return {_fmt(med_3m)}."
            )

        with_1y = [tp["1Y"] for tp in theme_performance if tp.get("1Y") is not None]
        if with_1y:
            med_1y = _median(with_1y)
            insights.append(
                f"1Y median theme return is {_fmt(med_1y)} across available histories."
            )

        return {"kpis": kpis, "insights": insights}

    # ------------------------------------------------------------------
    # Individual Stock Charts (OHLCV embedded in HTML)
    # ------------------------------------------------------------------

    def _fetch_stock_chart_data(
        self,
        tickers: List[str],
        as_of: date,
        cursor,
    ) -> Dict[str, List[List[Any]]]:
        """
        Fetch OHLCV bars for candlestick charts.

        Returns {chart_ticker_key: [[date, open, high, low, close, volume], ...]}
        matching the reference dashboard STOCK_CHART_DATA format.
        """
        if not tickers:
            return {}

        lookback_start = as_of - timedelta(days=STOCK_CHART_CALENDAR_DAYS)
        cursor.execute(
            """
            SELECT code, date::date AS trade_date,
                   open, high, low, close, volume
            FROM stock_prices_eod
            WHERE code = ANY(%s)
              AND date BETWEEN %s AND %s
              AND period = 'd'
              AND close IS NOT NULL
            ORDER BY code, date
            """,
            (tickers, lookback_start, as_of),
        )
        rows = cursor.fetchall()
        if not rows:
            logger.warning("No OHLCV data for stock charts")
            return {}

        by_code: Dict[str, List[List[Any]]] = {}
        for code, trade_date, o, h, l, c, vol in rows:
            close = float(c)
            open_p = float(o) if o is not None else close
            high_p = float(h) if h is not None else close
            low_p = float(l) if l is not None else close
            volume = int(vol) if vol is not None else 0
            bar = [
                str(trade_date),
                open_p,
                high_p,
                low_p,
                close,
                volume,
            ]
            by_code.setdefault(code, []).append(bar)

        chart_data: Dict[str, List[List[Any]]] = {}
        for code, bars in by_code.items():
            if len(bars) < 2:
                continue
            trimmed = bars[-STOCK_CHART_MAX_BARS:]
            chart_data[_chart_ticker_key(code)] = trimmed

        logger.info(
            f"Stock chart data prepared for {len(chart_data)} tickers "
            f"({sum(len(v) for v in chart_data.values())} bars total)"
        )
        return chart_data

    def _build_stock_charts(
        self, themes_data: Dict[str, Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Layout metadata for per-theme stock chart sections."""
        sections: List[Dict[str, Any]] = []
        for theme_name, td in sorted(themes_data.items(), key=lambda x: x[0]):
            categories = []
            for cat_name, cat_tickers in td["categories"].items():
                tickers = []
                for ticker in sorted(cat_tickers):
                    key = _chart_ticker_key(ticker)
                    tickers.append(
                        {
                            "ticker": key,
                            "slug": _slugify(key),
                        }
                    )
                if not tickers:
                    continue
                categories.append(
                    {
                        "category": cat_name,
                        "slug": _slugify(cat_name),
                        "tickers": tickers,
                        "is_multi": len(tickers) > 1,
                    }
                )
            if not categories:
                continue
            sections.append(
                {
                    "theme_name": theme_name,
                    "theme_key": _theme_key(td.get("theme_id", ""), theme_name),
                    "categories": categories,
                }
            )
        return sections

    # ------------------------------------------------------------------
    # HTML rendering
    # ------------------------------------------------------------------

    def _render_html(
        self,
        themes_details: List[Dict[str, Any]],
        theme_performance: List[Dict[str, Any]],
        executive_summary: Dict[str, Any],
        stock_charts: List[Dict[str, Any]],
        stock_chart_data: Dict[str, List[List[Any]]],
        report_date: date,
        price_as_of: Optional[date],
        user_id: str,
        generation_times: Optional[Dict[str, Optional[str]]] = None,
    ) -> str:
        """Render a self-contained HTML string via Jinja2."""
        from jinja2 import Environment, FileSystemLoader, select_autoescape

        template_dir = os.path.join(
            os.path.dirname(__file__), "..", "templates"
        )
        env = Environment(
            loader=FileSystemLoader(os.path.abspath(template_dir)),
            autoescape=select_autoescape(["html"]),
        )
        # Custom filter for formatted returns
        def fmt_ret(v):
            if v is None:
                return "n/a"
            sign = "+" if v >= 0 else ""
            return f"{sign}{v:.1f}%"

        env.filters["fmt_ret"] = fmt_ret
        template = env.get_template("theme_report.html.j2")

        total_categories = sum(len(td["categories"]) for td in themes_details)
        total_constituents = sum(len(td["constituents"]) for td in themes_details)

        gen = generation_times or _build_generation_time_display(utcnow(), None)

        html = template.render(
            report_date=report_date.strftime("%B %d, %Y"),
            price_as_of=(
                price_as_of.strftime("%B %d, %Y") if price_as_of else str(report_date)
            ),
            generated_at=gen.get("generated_at_utc"),
            generated_at_new_york=gen.get("generated_at_new_york"),
            generated_at_beijing=gen.get("generated_at_beijing"),
            scheduler_timezone=gen.get("scheduler_timezone"),
            scheduler_timezone_label=gen.get("scheduler_timezone_label"),
            generated_at_scheduler_tz=gen.get("generated_at_scheduler_tz"),
            user_id=user_id,
            themes=themes_details,
            theme_performance=theme_performance,
            executive_summary=executive_summary,
            theme_performance_json=json.dumps(theme_performance),
            stock_charts=stock_charts,
            stock_chart_data_json=json.dumps(stock_chart_data),
            total_themes=len(themes_details),
            total_categories=total_categories,
            total_constituents=total_constituents,
            horizons=list(RETURN_HORIZONS.keys()),
        )
        return html

    # ------------------------------------------------------------------
    # Report listing
    # ------------------------------------------------------------------

    def get_user_reports(
        self, user_id, limit: int = 30, offset: int = 0
    ) -> Tuple[List[Dict[str, Any]], int]:
        """分页获取用户主题报告列表（供 Agent API 使用）。

        每条记录不含 html_content 正文 —— 正文通过 share_url 访问。
        user_id 内部归一化为 str（表列为 VARCHAR，实际存 username）。
        """
        user_id = str(user_id)
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                "SELECT COUNT(*) FROM theme_reports WHERE user_id = %s",
                (user_id,),
            )
            total = cursor.fetchone()[0]

            cursor.execute(
                """
                SELECT id, report_date, status, metadata, created_at, share_token, theme_count
                FROM theme_reports
                WHERE user_id = %s
                ORDER BY report_date DESC
                LIMIT %s OFFSET %s
                """,
                (user_id, limit, offset),
            )
            columns = [
                "id", "report_date", "status", "metadata",
                "created_at", "share_token", "theme_count",
            ]
            reports = []
            for row in cursor.fetchall():
                d = dict(zip(columns, row))
                if d.get("report_date"):
                    d["report_date"] = str(d["report_date"])
                if d.get("created_at"):
                    d["created_at"] = d["created_at"].isoformat()
                reports.append(d)
            return reports, total
        finally:
            cursor.close()
            conn.close()

    def get_report_by_date(self, user_id, report_date) -> Optional[Dict[str, Any]]:
        """获取指定日期的已完成主题报告（供 Agent API 使用）。"""
        user_id = str(user_id)
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT id, report_date, status, metadata, created_at, share_token, theme_count
                FROM theme_reports
                WHERE user_id = %s AND report_date = %s AND status = 'completed'
                """,
                (user_id, report_date),
            )
            row = cursor.fetchone()
            if not row:
                return None
            columns = [
                "id", "report_date", "status", "metadata",
                "created_at", "share_token", "theme_count",
            ]
            d = dict(zip(columns, row))
            if d.get("report_date"):
                d["report_date"] = str(d["report_date"])
            if d.get("created_at"):
                d["created_at"] = d["created_at"].isoformat()
            return d
        finally:
            cursor.close()
            conn.close()

    def list_reports(
        self, user_id: str, limit: int = 20
    ) -> List[Dict[str, Any]]:
        """Return recent theme_reports rows for a user (newest first)."""
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT id, report_date, oss_object_name, theme_count,
                       category_count, constituent_count, metadata, status,
                       error_message, created_at, share_token
                FROM theme_reports
                WHERE user_id = %s
                ORDER BY report_date DESC
                LIMIT %s
                """,
                (user_id, limit),
            )
            rows = cursor.fetchall()
            result = []
            for row in rows:
                (
                    rid, rdate, obj_name, tc, cc, constit_c,
                    meta, status, err_msg, created_at, share_token,
                ) = row

                # Mint a fresh signed inline URL if the object exists
                signed_url = None
                if obj_name and self.oss_service.is_configured():
                    signed_url = self.oss_service.get_signed_url(
                        obj_name, expires=3600 * 24, force_inline=True
                    )

                # Lazy-mint share_token for legacy completed rows
                if status == "completed" and not share_token:
                    share_token = uuid4().hex
                    try:
                        cursor.execute(
                            "UPDATE theme_reports SET share_token=%s WHERE id=%s",
                            (share_token, rid),
                        )
                        conn.commit()
                    except Exception:
                        share_token = None

                result.append(
                    {
                        "id": rid,
                        "report_date": str(rdate),
                        "oss_object_name": obj_name,
                        "signed_url": signed_url,
                        "share_token": share_token,
                        "theme_count": tc,
                        "category_count": cc,
                        "constituent_count": constit_c,
                        "metadata": meta or {},
                        "status": status,
                        "error_message": err_msg,
                        "created_at": created_at.isoformat() if created_at else None,
                    }
                )
            return result
        finally:
            cursor.close()
            conn.close()

    def get_latest_eod_date(self, user_id: str) -> Optional[date]:
        """
        Return the latest trade date in stock_prices_eod for all tickers
        associated with the user's 'Theme Report' tagged themes.
        """
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            themes_data = self._get_tagged_themes(user_id, cursor)
            if not themes_data:
                return None

            all_tickers = list(
                {t for td in themes_data.values() for t in td["tickers"]}
            )
            if not all_tickers:
                return None

            cursor.execute(
                """
                SELECT MAX(date::date)
                FROM stock_prices_eod
                WHERE code = ANY(%s) AND period = 'd'
                """,
                (all_tickers,),
            )
            row = cursor.fetchone()
            return row[0] if row and row[0] else None
        finally:
            cursor.close()
            conn.close()

    def get_latest_report(self, user_id: str) -> Optional[Dict[str, Any]]:
        """Return the most recent completed report for a user."""
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT id, report_date, oss_object_name, theme_count,
                       category_count, constituent_count, metadata, status,
                       error_message, created_at, share_token
                FROM theme_reports
                WHERE user_id = %s AND status = 'completed'
                ORDER BY report_date DESC
                LIMIT 1
                """,
                (user_id,),
            )
            row = cursor.fetchone()
            if not row:
                return None
            (
                rid, rdate, obj_name, tc, cc, constit_c,
                meta, status, err_msg, created_at, share_token,
            ) = row

            signed_url = None
            if obj_name and self.oss_service.is_configured():
                signed_url = self.oss_service.get_signed_url(
                    obj_name, expires=3600 * 24, force_inline=True
                )

            # Lazy-mint share_token for legacy completed rows
            if not share_token:
                share_token = uuid4().hex
                try:
                    cursor.execute(
                        "UPDATE theme_reports SET share_token=%s WHERE id=%s",
                        (share_token, rid),
                    )
                    conn.commit()
                except Exception:
                    share_token = None

            metadata = meta or {}
            price_as_of_str = metadata.get("price_as_of_date")
            price_as_of_date: Optional[date] = None
            if price_as_of_str:
                try:
                    price_as_of_date = date.fromisoformat(str(price_as_of_str))
                except (ValueError, TypeError):
                    price_as_of_date = None

            latest_eod_date = self.get_latest_eod_date(user_id)
            is_stale = bool(
                price_as_of_date
                and latest_eod_date
                and latest_eod_date > price_as_of_date
            )

            return {
                "id": rid,
                "report_date": str(rdate),
                "oss_object_name": obj_name,
                "signed_url": signed_url,
                "share_token": share_token,
                "theme_count": tc,
                "category_count": cc,
                "constituent_count": constit_c,
                "metadata": metadata,
                "status": status,
                "error_message": err_msg,
                "created_at": created_at.isoformat() if created_at else None,
                "price_as_of_date": price_as_of_str,
                "latest_eod_date": (
                    str(latest_eod_date) if latest_eod_date else None
                ),
                "is_stale": is_stale,
                "generated_at_new_york": metadata.get("generated_at_new_york"),
                "generated_at_beijing": metadata.get("generated_at_beijing"),
                "scheduler_timezone": metadata.get("scheduler_timezone"),
                "scheduler_timezone_label": metadata.get("scheduler_timezone_label"),
                "generated_at_scheduler_tz": metadata.get("generated_at_scheduler_tz"),
            }
        finally:
            cursor.close()
            conn.close()

    def get_report_html_by_token(self, token: str) -> Optional[bytes]:
        """
        Look up a completed report by its share_token and return the raw HTML bytes.

        Primary source: html_content column in DB (no OSS round-trip needed).
        Fallback: OSS fetch for legacy rows that predate the html_content column.
        Returns None if the token is unknown or all sources fail.
        """
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT oss_object_name, html_content
                FROM theme_reports
                WHERE share_token = %s AND status = 'completed'
                LIMIT 1
                """,
                (token,),
            )
            row = cursor.fetchone()
            if not row:
                logger.warning(f"[get_report_html_by_token] token not found: {token[:8]}…")
                return None
            obj_name, html_content = row

            # Primary: serve directly from DB (fast, no OSS dependency)
            if html_content:
                return html_content.encode("utf-8")

            # Fallback: legacy row without html_content — fetch from OSS
            if obj_name:
                logger.info(
                    f"[get_report_html_by_token] html_content missing, fetching from OSS: {obj_name}"
                )
                html_bytes = self.oss_service.get_object_bytes(obj_name)
                if html_bytes is None:
                    logger.error(
                        f"[get_report_html_by_token] OSS fetch failed for object: {obj_name}"
                    )
                return html_bytes

            logger.error(f"[get_report_html_by_token] no html_content and no oss_object_name for token {token[:8]}…")
            return None
        finally:
            cursor.close()
            conn.close()

    def get_users_with_tag(self) -> List[str]:
        """Return distinct user_ids that have at least one theme tagged 'Theme Report'."""
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                "SELECT DISTINCT user_id FROM theme_tags WHERE tag_name = %s",
                (TAG_NAME,),
            )
            return [row[0] for row in cursor.fetchall()]
        finally:
            cursor.close()
            conn.close()
