"""zq_v1 probability formula: EFFR basis + post-meeting rate chaining."""
from datetime import date

from backend.services.fed_dashboard_service import FedDashboardService as S


def test_sep_same_month_contract_with_official_effr():
    # 2026-09-16, September contract, ZQU=96.26, NY Fed EFFR=3.63
    res = S._prob_from_prices(96.26, 3.63, date(2026, 9, 16), 9, 2026)
    assert res["implied_rate"] == 3.74
    assert abs(res["p_hike"] - 0.9428) < 0.002
    assert abs(res["p_hold"] - 0.0572) < 0.002
    assert res["p_cut"] == 0.0
    # post = (3.74*30 - 3.63*16) / 14 ≈ 3.8657
    assert abs(res["post_rate"] - 3.866) < 0.002


def test_oct_next_month_contract_is_not_a_near_certain_second_hike():
    sep = S._prob_from_prices(96.26, 3.63, date(2026, 9, 16), 9, 2026)
    # October meeting uses November contract (d_post < 10)
    oct_ = S._prob_from_prices(96.015, sep["post_rate"], date(2026, 10, 28), 11, 2026)
    assert 0.40 < oct_["p_hike"] < 0.55
    assert oct_["p_cut"] == 0.0
    # Wrong chaining (monthly average 3.74) would print ~98% hike.
    wrong = S._prob_from_prices(96.015, sep["implied_rate"], date(2026, 10, 28), 11, 2026)
    assert wrong["p_hike"] > 0.95


def test_midpoint_fallback_is_more_hawkish_than_effr():
    mid = S._prob_from_prices(96.26, 3.625, date(2026, 9, 16), 9, 2026)
    effr = S._prob_from_prices(96.26, 3.63, date(2026, 9, 16), 9, 2026)
    assert mid["p_hike"] > effr["p_hike"]
    assert abs(mid["p_hike"] - 0.9857) < 0.002
