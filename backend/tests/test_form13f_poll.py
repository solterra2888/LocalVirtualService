# -*- coding: utf-8 -*-
"""Form 13F：季度标签、CIK 身份、amendment 折叠。"""
from backend.services.form13f_service import (
    quarter_of,
    filer_key_of,
    normalize_cik,
    normalize_filer_name,
    fold_period_filings,
    HoldingRow,
    PeriodFiling,
)


def _row(cusip, shares, value=1, putcall=''):
    return HoldingRow(
        cusip=cusip, issuer=cusip, title='COM',
        putcall=putcall, value=value, shares=shares)


class TestQuarterOf:
    def test_march_31_is_calendar_q1_not_filing_window_q2(self):
        assert quarter_of("31-MAR-2026") == "2026Q1"

    def test_iso_date(self):
        assert quarter_of("2026-03-31") == "2026Q1"
        assert quarter_of("2026-06-30") == "2026Q2"

    def test_datetime_like(self):
        class D:
            year = 2025
            month = 12
        assert quarter_of(D()) == "2025Q4"


class TestFilerIdentity:
    def test_cik_key_is_zero_padded(self):
        assert normalize_cik(1067983) == "0001067983"
        assert filer_key_of("0001067983") == filer_key_of(1067983)
        assert normalize_filer_name("  Foo   Bar ") == "FOO BAR"


class TestFoldAmendments:
    def test_new_holdings_unions_with_original(self):
        original = {
            ('A', ''): _row('A', 100, 10),
            ('B', ''): _row('B', 50, 5),
        }
        added = {('C', ''): _row('C', 7, 3)}
        holdings, acc = fold_period_filings([
            PeriodFiling('acc-orig', '2025-05-15', False, None, 0, original),
            PeriodFiling('acc-new', '2025-08-14', True, 'NEW HOLDINGS', 1, added),
        ])
        assert acc == 'acc-new'
        assert set(holdings) == {('A', ''), ('B', ''), ('C', '')}
        assert holdings[('A', '')].shares == 100
        assert holdings[('C', '')].shares == 7

    def test_restatement_replaces_original(self):
        original = {('A', ''): _row('A', 100)}
        restated = {('B', ''): _row('B', 1)}
        holdings, acc = fold_period_filings([
            PeriodFiling('acc-orig', '2025-05-15', False, None, 0, original),
            PeriodFiling('acc-re', '2025-06-01', True, 'RESTATEMENT', 1, restated),
        ])
        assert acc == 'acc-re'
        assert set(holdings) == {('B', '')}

    def test_untyped_amendment_treated_as_restatement(self):
        original = {('A', ''): _row('A', 100)}
        amend = {('B', ''): _row('B', 1)}
        holdings, _acc = fold_period_filings([
            PeriodFiling('acc-orig', '2025-05-15', False, None, 0, original),
            PeriodFiling('acc-a', '2025-06-01', True, None, 1, amend),
        ])
        assert set(holdings) == {('B', '')}

    def test_empty_notice_ignored(self):
        original = {('A', ''): _row('A', 100)}
        holdings, acc = fold_period_filings([
            PeriodFiling('acc-orig', '2025-05-15', False, None, 0, original),
            PeriodFiling('acc-nt', '2025-05-16', False, None, 0, {}),
        ])
        assert acc == 'acc-orig'
        assert set(holdings) == {('A', '')}
