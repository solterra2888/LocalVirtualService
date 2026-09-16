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
    portfolio_pct,
    share_change_pct,
    rank_top5_by_weight_change,
    notice_other_manager_ciks,
    is_option_leg,
    is_fixed_income_holding,
    is_stock_or_etf_long,
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


class TestNoticeOtherManagerCiks:
    def test_extracts_successor_cik_from_13f_nt(self):
        xml = """
        <coverPage>
          <reportType>13F NOTICE</reportType>
          <otherManagersInfo>
            <otherManager>
              <cik>0002026053</cik>
              <form13FFileNumber>028-25746</form13FFileNumber>
              <name>PERSHING SQUARE INC.</name>
            </otherManager>
          </otherManagersInfo>
        </coverPage>
        """
        assert notice_other_manager_ciks(xml) == ['0002026053']

    def test_ignores_filer_credentials_outside_other_manager(self):
        xml = """
        <filer><cik>0001336528</cik></filer>
        <otherManager><cik>2026053</cik></otherManager>
        """
        assert notice_other_manager_ciks(xml) == ['0002026053']

    def test_empty_without_other_manager(self):
        assert notice_other_manager_ciks('<cik>0001336528</cik>') == []


class TestTop5ByWeightChange:
    def test_share_change_pct_new_and_exit(self):
        assert share_change_pct(100, None) is None
        assert share_change_pct(100, 0) is None
        assert share_change_pct(0, 80) == -100.0
        assert share_change_pct(120, 100) == 20.0

    def test_ranks_by_portfolio_weight_not_dollar_delta(self):
        # AUM 8000 → 10000。市值少得更多的 MRVL 权重掉得更少。
        cur_totals = {1: 10_000}
        prev_totals = {1: 8_000}
        rows = [
            # issuer, cusip, putcall, cur_v, prev_v, cur_sh, prev_sh
            (1, 'MSFT', 'MSFT', '', 700, 800, 70, 100),   # dv=-100, dpct=-3.0
            (1, 'MRVL', 'MRVL', '', 50, 240, 5, 50),      # dv=-190, dpct=-2.5
            (1, 'AMZN', 'AMZN', '', 200, 800, 20, 100),    # dv=-600, dpct=-8.0
            (1, 'SPY', 'SPY', '', 3000, 2000, 30, 25),     # dv=+1000, dpct=+5.0
            (1, 'PCG', 'PCG', '', 50, 0, 10, 0),           # new, dpct=+0.5
            (1, 'VOO', 'VOO', '', 400, 100, 40, 10),       # dv=+300, dpct=+2.75
        ]
        buys, sells = rank_top5_by_weight_change(rows, cur_totals, prev_totals)
        assert [x['cusip'] for x in buys[1]] == ['SPY', 'VOO', 'PCG']
        assert buys[1][0]['dpct'] == 5.0
        assert buys[1][0]['spct'] == 20.0
        assert buys[1][2]['kind'] == 'new'
        assert buys[1][2]['spct'] is None
        # 权重：AMZN -8.0, MSFT -3.0, MRVL -2.5（不是按 |dv| 的 AMZN/MRVL/MSFT）
        assert [x['cusip'] for x in sells[1]] == ['AMZN', 'MSFT', 'MRVL']
        assert sells[1][1]['spct'] == -30.0
        assert abs(portfolio_pct(700, 10_000) - 7.0) < 1e-9

    def test_excludes_zero_share_change_and_opposite_share_direction(self):
        # Berkshire 型：AUM 膨胀后，未交易大票权重下跌；减持但股价涨的票权重上升。
        cur_totals = {1: 10_000}
        prev_totals = {1: 8_000}
        rows = [
            (1, 'GOOGL', 'GOOGL', '', 2000, 800, 150, 100),  # 加仓，dpct=+10
            (1, 'DAL', 'DAL', '', 400, 200, 40, 20),         # 加仓，dpct=+1.5
            (1, 'LEN', 'LEN', '', 50, 20, 12, 10),           # 加仓，dpct=+0.25
            (1, 'DVA', 'DVA', '', 500, 240, 90, 100),        # 减持但权重升，应进 sells
            (1, 'SIRI', 'SIRI', '', 200, 80, 10, 10),        # 股数未变，不进榜
            (1, 'OXY', 'OXY', '', 400, 800, 50, 50),         # 股数未变权重大跌，不进榜
            (1, 'KR', 'KR', '', 100, 400, 8, 10),            # 减持，dpct=-4.0
            (1, 'BAC', 'BAC', '', 900, 800, 90, 100),        # 减持，dpct=-1.0
            (1, 'STZ', 'STZ', '', 0, 80, 0, 5),              # 清仓
        ]
        buys, sells = rank_top5_by_weight_change(rows, cur_totals, prev_totals)
        assert [x['cusip'] for x in buys[1]] == ['GOOGL', 'DAL', 'LEN']
        assert 'SIRI' not in [x['cusip'] for x in buys[1]]
        assert 'DVA' not in [x['cusip'] for x in buys[1]]
        assert [x['cusip'] for x in sells[1]] == ['KR', 'BAC', 'STZ', 'DVA']
        assert sells[1][2]['kind'] == 'exited'
        assert 'OXY' not in [x['cusip'] for x in sells[1]]

    def test_excludes_options_and_notes(self):
        assert is_option_leg('Put') and is_option_leg('Call')
        assert is_fixed_income_holding('NOTE 2.000% 6/1')
        assert not is_fixed_income_holding('COM')
        assert not is_option_leg('')
        cur_totals = {1: 10_000}
        prev_totals = {1: 8_000}
        rows = [
            (1, 'SPY PUT', 'SPY', 'Put', 50, 800, 5, 80, 'TR UNIT'),       # 期权，应排除
            (1, 'JAZZ NOTE', 'JAZZ', '', 0, 400, 0, 10, 'NOTE 2.000% 6/1'),  # 可转债，应排除
            (1, 'SEE', 'SEE', '', 0, 300, 0, 20, 'COM'),                    # 股票清仓
            (1, 'AMZN', 'AMZN', '', 200, 400, 20, 40, 'COM'),                # 股票减持
            (1, 'HON', 'HON', '', 0, 200, 0, 8, 'COM'),                     # 股票清仓
        ]
        _buys, sells = rank_top5_by_weight_change(rows, cur_totals, prev_totals)
        assert [x['cusip'] for x in sells[1]] == ['SEE', 'AMZN', 'HON']
        assert 'SPY' not in [x['cusip'] for x in sells[1]]
        assert 'JAZZ' not in [x['cusip'] for x in sells[1]]

    def test_option_overlay_does_not_outrank_equity_exits(self):
        """Oaktree 型：新增大额 Put 撑高 13F 总市值后，股票减仓的 ΔWeight
        应按股票/ETF 多头分母计算，不能压过真实清仓。"""
        assert is_fixed_income_holding('WARRANT')
        assert is_stock_or_etf_long('', 'COMMON STOCK')
        assert not is_stock_or_etf_long('Put', 'ETF')
        # 股票多头 10000 → 7000；另有 8000 新 Put 不进分母。
        cur_eq = {1: 7_000}
        prev_eq = {1: 10_000}
        rows = [
            (1, 'TORM', 'TRMD', '', 5000, 6000, 80, 100, 'COMMON STOCK'),   # 减仓
            (1, 'CBL', 'CBL', '', 0, 2000, 0, 50, 'COMMON STOCK'),          # 清仓
            (1, 'PBR', 'PBR', '', 0, 1500, 0, 40, 'COMMON STOCK'),          # 清仓
            (1, 'KEEP', 'KEEP', '', 2000, 2500, 50, 50, 'COMMON STOCK'),     # 股数未变
            (1, 'SPY PUT', 'SPY', 'Put', 8000, 0, 100, 0, 'ETF'),            # 期权
            (1, 'OCSL', 'OCSL', '', 2000, 500, 40, 10, 'COMMON STOCK'),      # 加仓
        ]
        buys, sells = rank_top5_by_weight_change(rows, cur_eq, prev_eq)
        assert [x['cusip'] for x in buys[1]] == ['OCSL']
        assert [x['cusip'] for x in sells[1]] == ['CBL', 'PBR', 'TRMD']
        assert 'SPY' not in [x['cusip'] for x in sells[1]]
        assert 'KEEP' not in [x['cusip'] for x in sells[1]]
