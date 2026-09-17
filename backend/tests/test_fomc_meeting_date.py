"""meeting_date = statement/minutes URL YYYYMMDD (meeting end / release day)."""
from datetime import date

from backend.services.fed_dashboard_service import meeting_date_from_source_url


def test_statement_url_is_release_day_not_first_day():
    url = "https://www.federalreserve.gov/newsevents/pressreleases/monetary20260916a.htm"
    assert meeting_date_from_source_url(url) == date(2026, 9, 16)


def test_minutes_url_is_meeting_end_day():
    url = "https://www.federalreserve.gov/monetarypolicy/fomcminutes20260729.htm"
    assert meeting_date_from_source_url(url) == date(2026, 7, 29)


def test_unrelated_url_returns_none():
    url = "https://www.federalreserve.gov/newsevents/pressreleases/monetary20260916b.htm"
    assert meeting_date_from_source_url(url) is None
