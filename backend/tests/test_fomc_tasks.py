"""FOMC poll/fetch 概括派发契约。

覆盖纪要 404 自愈后漏派发 fomc_summarize 的缺陷：
  - poll 对 pending+failed 统一派发 fomc_fetch_document（不内联 retry）
  - poll 对已 fetched 但无 completed 概括补派发
  - fetch 在 hash 未变但仍缺概括时仍派发 summarize
"""
from unittest import mock

from backend.tasks import fomc_tasks as ft


def _fake_svc(fetch_all_side_effect):
    svc = mock.MagicMock()
    svc.poll_fed_updates.return_value = {
        "updates_written": 1, "docs_discovered": 0,
    }
    svc.db_manager.fetch_all.side_effect = fetch_all_side_effect
    return svc


def _sql(sql):
    return " ".join((sql or "").split())


class TestFomcPollDispatch:
    def test_poll_dispatches_fetch_for_pending_and_failed(self):
        def fetch_all(sql, params=None):
            s = _sql(sql)
            if "fetch_status IN ('pending', 'failed')" in s:
                return [(11,), (12,)]
            return []

        svc = _fake_svc(fetch_all)
        with mock.patch.object(ft, "_get_service", return_value=svc), \
             mock.patch.object(ft, "_dispatch") as disp:
            ft.fomc_poll_updates_task.run()

        svc.retry_pending_documents.assert_not_called()
        fetch_calls = [
            c for c in disp.call_args_list if c.args[0] == "fomc_fetch_document"
        ]
        assert [c.kwargs["doc_id"] for c in fetch_calls] == [11, 12]

    def test_poll_dispatches_summarize_for_unsummarized_fetched(self):
        def fetch_all(sql, params=None):
            s = _sql(sql)
            if "fetch_status IN ('pending', 'failed')" in s:
                return []
            if "fetch_status = 'fetched'" in s and "fomc_summaries" in s:
                return [(11,)]
            return []

        svc = _fake_svc(fetch_all)
        with mock.patch.object(ft, "_get_service", return_value=svc), \
             mock.patch.object(ft, "_dispatch") as disp:
            ft.fomc_poll_updates_task.run()

        names = [c.args[0] for c in disp.call_args_list]
        assert names == ["fomc_summarize", "fomc_compute_probabilities"]
        assert disp.call_args_list[0].kwargs == {"doc_id": 11}

    def test_poll_always_dispatches_probabilities(self):
        svc = _fake_svc(lambda sql, params=None: [])
        with mock.patch.object(ft, "_get_service", return_value=svc), \
             mock.patch.object(ft, "_dispatch") as disp:
            ft.fomc_poll_updates_task.run()
        assert disp.call_args_list[-1].args[0] == "fomc_compute_probabilities"


class TestFomcFetchSummarizeGate:
    def _run_fetch(self, *, unchanged, completed_exists, doc_type="minutes"):
        svc = mock.MagicMock()
        svc._get_document.return_value = {"doc_type": doc_type}
        svc.fetch_document.return_value = {
            "unchanged": unchanged,
            "word_count": 100,
            "doc_type": doc_type,
        }
        svc.db_manager.fetch_all.return_value = [(1,)] if completed_exists else []
        with mock.patch.object(ft, "_get_service", return_value=svc), \
             mock.patch.object(ft, "_dispatch") as disp:
            result = ft.fomc_fetch_document_task.run(11)
        return svc, disp, result

    def test_unchanged_missing_summary_still_dispatches(self):
        _, disp, _ = self._run_fetch(unchanged=True, completed_exists=False)
        disp.assert_called_once_with("fomc_summarize", doc_id=11)

    def test_unchanged_with_completed_summary_skips(self):
        _, disp, _ = self._run_fetch(unchanged=True, completed_exists=True)
        disp.assert_not_called()

    def test_changed_dispatches_even_if_summary_exists(self):
        _, disp, _ = self._run_fetch(unchanged=False, completed_exists=True)
        disp.assert_called_once_with("fomc_summarize", doc_id=11)

    def test_statement_extracts_decision(self):
        svc, _, _ = self._run_fetch(
            unchanged=False, completed_exists=False, doc_type="statement"
        )
        svc.extract_decision.assert_called_once_with(11)
