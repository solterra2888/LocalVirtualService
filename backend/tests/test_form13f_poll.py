# -*- coding: utf-8 -*-
"""Form 13F SEC 数据页 ZIP 文件名解析（2024 年前后两套命名）。"""
import pytest

from backend.services.form13f_service import (
    classify_dataset_name,
    parse_sec_dataset_zips,
    quarter_of,
)

SAMPLE_HTML = """
<table>
  <tr><td><a href="/files/structureddata/data/form-13f-data-sets/01mar2026-31may2026_form13f.zip">2026 March April May 13F</a></td></tr>
  <tr><td><a href="/files/structureddata/data/form-13f-data-sets/01jun2025-31aug2025_form13f.zip">2025 June July August 13F</a></td></tr>
  <tr><td><a href="/files/structureddata/data/form-13f-data-sets/01jan2024-29feb2024_form13f.zip">2024 January February 13F</a></td></tr>
  <tr><td><a href="/files/structureddata/data/form-13f-data-sets/2023q4_form13f.zip">2023 Q4 13F</a></td></tr>
  <tr><td><a href="/files/structureddata/data/form-13f-data-sets/2023q1_form13f.zip">2023 Q1 13F</a></td></tr>
</table>
"""


class TestQuarterOf:
    def test_march_31_is_calendar_q1_not_filing_window_q2(self):
        assert quarter_of("31-MAR-2026") == "2026Q1"

    def test_june_30_is_q2(self):
        assert quarter_of("30-JUN-2026") == "2026Q2"


class TestClassifyDatasetName:
    def test_2024_plus_window_filename(self):
        assert classify_dataset_name("01mar2026-31may2026_form13f") == "window"

    def test_pre_2024_legacy_filename(self):
        assert classify_dataset_name("2023q4_form13f") == "legacy"


class TestParseSecDatasetZips:
    def test_parses_both_filename_conventions(self):
        parsed = parse_sec_dataset_zips(SAMPLE_HTML)
        names = [name for _, name, _ in parsed]
        assert names == [
            "01mar2026-31may2026_form13f",
            "01jun2025-31aug2025_form13f",
            "01jan2024-29feb2024_form13f",
            "2023q4_form13f",
            "2023q1_form13f",
        ]
        kinds = {name: kind for _, name, kind in parsed}
        assert kinds["01mar2026-31may2026_form13f"] == "window"
        assert kinds["2023q4_form13f"] == "legacy"

    def test_raises_when_no_zip_links(self):
        with pytest.raises(RuntimeError, match="页面可能已改版"):
            parse_sec_dataset_zips("<html>no datasets here</html>")
