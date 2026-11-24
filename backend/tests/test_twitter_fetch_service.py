# -*- coding: utf-8 -*-
"""
Twitter 抓取服务（2026-11 改版）的单元测试。

覆盖：单窗口续取、官方退避重试、单作者失败隔离、时间线兜底、深度回扫的编排。
HTTP 与数据库全部 mock，不访问网络。
"""
import json
import logging
import re
from datetime import datetime, timedelta, timezone
from unittest import mock

import pytest
import requests
from celery.exceptions import SoftTimeLimitExceeded

from backend.services import twitter_subscription_service as tw

NOW = datetime(2026, 11, 24, 12, 0, tzinfo=timezone.utc)
NOW_TS = int(NOW.timestamp())
SEARCH = "/twitter/tweet/advanced_search"


def sid(ts, seq=0):
    """给定 unix 秒生成一个合法的 Snowflake ID（推文时间就编码在 ID 里）。"""
    return str(((ts * 1000 - 1288834974657) << 22) + seq)


def tweet(ts, handle="alpha", seq=0):
    return {"id": sid(ts, seq), "text": f"t{seq}", "author": {"username": handle}}


def tl_tweet(ts, handle="alpha", seq=0):
    """时间线接口（last_tweets）的推文：作者字段是 author.userName，不是 author.username。"""
    return {"id": sid(ts, seq), "text": f"t{seq}", "author": {"userName": handle}}


def resp(status, body=None):
    m = mock.Mock()
    m.status_code = status
    m.text = json.dumps(body) if body is not None else "err"
    if status == 200:
        m.json.return_value = body
    return m


def search_page(tweets, has_next=False):
    return resp(200, {"tweets": tweets, "has_next_page": has_next, "next_cursor": "c" if has_next else None})


def exhausted_503():
    return [resp(503, {"detail": "This search couldn't be completed right now."})] * 5


def make_service(**overrides):
    """绕过 __init__（它会连数据库），直接装配一个可测的服务实例。"""
    svc = object.__new__(tw.TwitterSubscriptionService)
    svc.api_key = "test-key"
    svc.api_base_url = tw.API_BASE + SEARCH
    svc.session = mock.Mock()
    svc.request_timeout = 30
    svc.retry_backoff = [5, 15, 30, 60]
    svc.author_pause_seconds = 0
    svc.default_query_hours = 24
    svc.test_fetch_query_hours = 12
    svc.initial_fetch_hours = 168
    svc.max_tweets_per_request = 20
    svc.poll_buffer_hours = 1
    svc.max_pages_per_author = 25
    svc.initial_fetch_max_pages = 60
    svc.fallback_enabled = True
    svc.fallback_max_calls = 10
    svc.fallback_max_pages = 3
    svc.deep_sweep_enabled = True
    svc.deep_sweep_interval_hours = 22
    svc.deep_sweep_window_hours = 36
    svc.deep_sweep_chunk_hours = 6
    svc.deep_sweep_max_fail_ratio = 0.1
    svc.run_budget_minutes = 20
    svc.deep_sweep_budget_minutes = 12
    svc.db_manager = mock.Mock()
    svc.content_service = mock.Mock()
    for key, value in overrides.items():
        setattr(svc, key, value)
    svc._reset_run_state()
    return svc


def search_queries(mock_get):
    """按调用顺序取出每次搜索请求里的 (since_time, until_time)。"""
    out = []
    for call in mock_get.call_args_list:
        q = call.kwargs["params"]["query"]
        s, u = re.search(r"since_time:(\d+) until_time:(\d+)", q).groups()
        out.append((int(s), int(u)))
    return out


def requested_paths(mock_get):
    return [call.args[0].replace(tw.API_BASE, "") for call in mock_get.call_args_list]


@pytest.fixture(autouse=True)
def _stop_patchers():
    # 服务里的 UnifiedLogger 会写生产的 logs/app.log，测试期间整体关掉日志
    logging.disable(logging.CRITICAL)
    yield
    logging.disable(logging.NOTSET)
    mock.patch.stopall()


@pytest.fixture
def no_sleep(monkeypatch):
    sleeps = []
    monkeypatch.setattr(tw.time, "sleep", lambda s: sleeps.append(s))
    return sleeps


@pytest.fixture
def fixed_now(monkeypatch):
    monkeypatch.setattr(tw, "utcnow", lambda: NOW)


# ── 单窗口续取 ───────────────────────────────────────────────────────────────

class TestFetchTweetsInWindow:
    def test_one_page_needs_one_request(self):
        svc = make_service()
        svc.session.get.return_value = search_page([tweet(1100), tweet(1090)])
        tweets = svc.fetch_tweets_in_window("alpha", 1000, 1200)
        assert len(tweets) == 2
        assert search_queries(svc.session.get) == [(1000, 1200)]
        params = svc.session.get.call_args.kwargs["params"]
        assert params["query"] == "from:alpha since_time:1000 until_time:1200 include:nativeretweets"
        assert params["queryType"] == "Latest"

    def test_full_page_continues_from_just_before_oldest_tweet(self):
        svc = make_service()
        first = [tweet(1100 + i, seq=i) for i in range(20)]          # 最旧一条在 1100
        second = [tweet(1000 + i, seq=100 + i) for i in range(3)]    # 最旧一条正好压在窗口起点
        svc.session.get.side_effect = [search_page(first, has_next=True), search_page(second)]
        assert len(svc.fetch_tweets_in_window("alpha", 1000, 1200)) == 23
        # 第二次请求的 until = 最旧一条所在秒 + 1（until_time 是开区间上界）
        assert search_queries(svc.session.get) == [(1000, 1200), (1000, 1101)]

    def test_full_page_without_has_next_still_checks_once_more(self):
        svc = make_service()
        page = [tweet(1100 + i, seq=i) for i in range(20)]
        svc.session.get.side_effect = [search_page(page), search_page([])]
        assert len(svc.fetch_tweets_in_window("alpha", 1000, 1200)) == 20
        assert svc.session.get.call_count == 2

    def test_tweet_on_page_edge_is_not_duplicated(self):
        svc = make_service()
        edge = tweet(1100, seq=7)
        svc.session.get.side_effect = [
            search_page([tweet(1150), edge], has_next=True),
            search_page([edge, tweet(1050)]),                       # 同一条在两个窗口都出现
        ]
        assert len(svc.fetch_tweets_in_window("alpha", 1000, 1200)) == 3

    def test_continuation_time_comes_from_tweet_id_not_created_at(self):
        svc = make_service()
        mislabeled = tweet(1100)
        mislabeled["createdAt"] = "Tue Nov 24 20:00:00 +0000 2026"   # 被误标 +8h 的 createdAt
        svc.session.get.side_effect = [
            search_page([tweet(1150), mislabeled], has_next=True), search_page([])]
        svc.fetch_tweets_in_window("alpha", 1000, 1200)
        assert search_queries(svc.session.get)[1][1] == 1101

    def test_stops_when_upstream_ignores_until_time(self):
        svc = make_service()
        same = search_page([tweet(1100 + i, seq=i) for i in range(20)], has_next=True)
        svc.session.get.side_effect = [same, same]
        with mock.patch.object(tw.oplog, "warning") as warn:
            svc.fetch_tweets_in_window("alpha", 1000, 1200, max_pages=10)
        assert svc.session.get.call_count == 2
        assert warn.call_count == 1 and "无进展" in warn.call_args.args[0]

    def test_reaching_the_window_start_is_not_reported_as_a_problem(self):
        svc = make_service()
        edge = tweet(1100, seq=7)
        svc.session.get.side_effect = [search_page([tweet(1150), edge], has_next=True), search_page([edge])]
        with mock.patch.object(tw.oplog, "warning") as warn:
            assert len(svc.fetch_tweets_in_window("alpha", 1000, 1200)) == 2
        assert svc.session.get.call_count == 2 and warn.call_count == 0

    def test_max_pages_limits_requests(self):
        svc = make_service()
        pages = [search_page([tweet(oldest + i, seq=i) for i in range(20)], has_next=True)
                 for oldest in (1180, 1140, 1100, 1060)]
        svc.session.get.side_effect = pages
        assert len(svc.fetch_tweets_in_window("alpha", 1000, 1200, max_pages=3)) == 60
        assert svc.session.get.call_count == 3

    def test_empty_window_makes_no_request(self):
        svc = make_service()
        assert svc.fetch_tweets_in_window("alpha", 1200, 1200) == []
        assert svc.session.get.call_count == 0

    def test_legacy_entry_point_still_returns_empty_list_on_failure(self, no_sleep):
        svc = make_service()
        svc.session.get.side_effect = exhausted_503()
        assert svc.fetch_tweets_from_api("alpha", since_hours=6) == []


def test_author_handle_accepts_both_endpoint_spellings():
    assert tw._author_handle({"author": {"username": "DeItaone"}}) == "deitaone"
    assert tw._author_handle({"author": {"userName": "@DeItaone"}}) == "deitaone"
    assert tw._author_handle({"author": {"screen_name": "DeItaone"}}) == "deitaone"
    assert tw._author_handle({"author": None}) == "" and tw._author_handle({}) == ""


# ── 重试 ────────────────────────────────────────────────────────────────────

class TestGetWithRetry:
    def test_retries_503_with_vendor_backoff_then_succeeds(self, no_sleep):
        svc = make_service()
        svc.session.get.side_effect = [resp(503), resp(503), resp(200, {"tweets": []})]
        assert svc._get_with_retry("u", None, what="x") == {"tweets": []}
        assert no_sleep == [5, 15]
        assert svc.stats["requests"] == 3 and svc.stats["http_503"] == 2 and svc.stats["retries"] == 2

    def test_gives_up_after_backoff_sequence(self, no_sleep):
        svc = make_service()
        svc.session.get.side_effect = exhausted_503()
        with pytest.raises(tw.TwitterApiError) as e:
            svc._get_with_retry("u", None, what="x")
        assert e.value.retryable is True and e.value.status == 503
        assert no_sleep == [5, 15, 30, 60]          # 共 5 次尝试，与 twitterapi.io 文档一致
        assert svc.session.get.call_count == 5

    def test_non_retryable_status_is_not_retried(self, no_sleep):
        svc = make_service()
        svc.session.get.return_value = resp(400, {"detail": "bad"})
        with pytest.raises(tw.TwitterApiError) as e:
            svc._get_with_retry("u", None, what="x")
        assert e.value.retryable is False and e.value.status == 400
        assert svc.session.get.call_count == 1 and no_sleep == []

    def test_network_error_is_retried(self, no_sleep):
        svc = make_service()
        svc.session.get.side_effect = [requests.exceptions.ReadTimeout("slow"), resp(200, {"ok": 1})]
        assert svc._get_with_retry("u", None, what="x") == {"ok": 1}
        assert no_sleep == [5] and svc.stats["network_errors"] == 1

    def test_validate_user_still_reports_missing_users(self):
        svc = make_service()
        svc.session.get.return_value = resp(200, {"status": "error", "msg": "User not found"})
        assert svc.validate_twitter_user("ghost") == {"exists": False, "user_info": None, "error": "User not found"}
        svc.session.get.return_value = resp(200, {"status": "success", "data": {"id": "1", "name": "A"}})
        assert svc.validate_twitter_user("alpha") == {"exists": True, "user_info": {"id": "1", "name": "A"}, "error": None}

    def test_validate_user_reports_request_failure(self, no_sleep):
        svc = make_service()
        svc.session.get.return_value = resp(401)
        result = svc.validate_twitter_user("alpha")
        assert result["exists"] is False and "401" in result["error"] and no_sleep == []


# ── 窗口规划 ─────────────────────────────────────────────────────────────────

class TestWindowPlanning:
    def test_window_starts_at_last_success_minus_buffer(self):
        assert make_service()._poll_window_start(NOW - timedelta(hours=2), NOW) == NOW_TS - 3 * 3600

    def test_window_is_capped(self):
        assert make_service()._poll_window_start(NOW - timedelta(days=5), NOW) == NOW_TS - 24 * 3600

    def test_no_history_uses_full_default_window(self):
        assert make_service()._poll_window_start(None, NOW) == NOW_TS - 24 * 3600

    def test_stalest_authors_are_planned_first(self):
        cursor = mock.Mock()
        cursor.fetchone.side_effect = [(NOW - timedelta(hours=9),), (None,), (NOW - timedelta(hours=1),)]
        plan = make_service()._plan_windows(cursor, {"a": [{"id": 1}], "b": [{"id": 2}], "c": [{"id": 3}]},
                                            NOW, force_fetch=False)
        assert [item[0] for item in plan] == ["b", "a", "c"]

    def test_force_fetch_uses_full_week_only_for_new_authors(self):
        cursor = mock.Mock()
        cursor.fetchone.side_effect = [(0,), (500,)]
        plan = make_service()._plan_windows(cursor, {"new": [{"id": 1}], "old": [{"id": 2}]}, NOW, force_fetch=True)
        by_author = {item[0]: item for item in plan}
        assert (by_author["new"][2], by_author["new"][4]) == (NOW_TS - 168 * 3600, 60)
        assert by_author["old"][2] == NOW_TS - 12 * 3600


# ── 主流程 ───────────────────────────────────────────────────────────────────

def wire_run(svc, authors, search_outcomes, timeline=None, user_info=None, sweep_due=False):
    """装配一次完整抓取所需的 mock：订阅列表、数据库、按作者给出的搜索/兜底响应。"""
    subs = [{"id": i + 1, "user_id": 1, "channel_identifier": f"@{a}", "channel_name": a, "priority": 0,
             "max_items_per_fetch": 20, "last_fetched_at": None, "filter_config": {}, "metadata": {}}
            for i, a in enumerate(authors)]
    conn, cursor = mock.Mock(), mock.Mock()
    conn.cursor.return_value = cursor
    cursor.fetchone.return_value = (None,)          # 没有抓取历史
    cursor.fetchall.return_value = []               # 库里没有已知推文
    svc.db_manager.get_connection.return_value = conn
    svc.get_all_twitter_subscriptions = mock.Mock(return_value=subs)
    svc._update_subscription_status = mock.Mock()
    svc._record_fetch_history = mock.Mock()
    svc._dispatch_tagging = mock.Mock()
    svc.save_tweet_to_db = mock.Mock(side_effect=lambda c, cur, t, u, s: (len(t["text"]), True))
    mock.patch.object(tw, "_deep_sweep_due", return_value=sweep_due).start()
    mock.patch.object(tw, "_mark_deep_sweep_started").start()

    pending_timeline = list(timeline or [])
    info = user_info or {"status": "error", "msg": "User not found"}

    def fake_get(url, headers=None, params=None, timeout=None, verify=None):
        if url.endswith("/user/info"):
            return resp(200, info)
        if url.endswith("/last_tweets"):
            return pending_timeline.pop(0)
        outcome = search_outcomes[re.search(r"from:(\w+)", params["query"]).group(1)]
        if isinstance(outcome, BaseException):
            raise outcome
        if not isinstance(outcome, list):
            return outcome
        item = outcome.pop(0)
        if isinstance(item, list):                       # 一组连续的响应（如 5 次 503）
            outcome[:0] = item
            return outcome.pop(0)
        return item

    svc.session.get = mock.Mock(side_effect=fake_get)
    return conn, cursor


class TestFetchAndStore:
    def test_search_success_stores_tweets_and_records_history(self, fixed_now, no_sleep):
        svc = make_service()
        wire_run(svc, ["alpha"], {"alpha": [search_page([tweet(NOW_TS - 60)])]})
        result = svc.fetch_and_store_tweets()
        assert (result["authors_ok"], result["tweets_fetched"], result["tweets_new"]) == (1, 1, 1)
        assert svc._record_fetch_history.call_args.kwargs["items_fetched"] == 1
        svc._dispatch_tagging.assert_called_once()

    def test_one_failed_author_does_not_stop_the_others(self, fixed_now, no_sleep):
        svc = make_service(fallback_enabled=False)
        conn, _ = wire_run(svc, ["alpha", "beta"],
                           {"alpha": exhausted_503(), "beta": [search_page([tweet(NOW_TS - 60)])]})
        result = svc.fetch_and_store_tweets()
        assert (result["authors_ok"], result["authors_failed"], result["tweets_new"]) == (1, 1, 1)
        assert [f["author"] for f in result["failed_authors"]] == ["alpha"]
        assert "503" in result["failed_authors"][0]["error"]
        # 失败要如实写进 fetch history，不能再像以前那样记成 0 条的 success
        assert [c.kwargs["status"] for c in svc._record_fetch_history.call_args_list] == ["failed", "success"]
        assert result["http_503"] == 5

    def test_timeline_fallback_is_used_when_search_fails(self, fixed_now, no_sleep):
        svc = make_service()
        own = [tl_tweet(NOW_TS - 60), tl_tweet(NOW_TS - 120, seq=1)]
        page = resp(200, {"status": "success",
                          "data": {"tweets": own + [tl_tweet(NOW_TS - 30, "someoneelse")]},
                          "has_next_page": False, "next_cursor": ""})
        conn, cursor = wire_run(svc, ["alpha"], {"alpha": exhausted_503()}, timeline=[page],
                                user_info={"status": "success", "data": {"id": "1508426880860704771"}})
        result = svc.fetch_and_store_tweets()
        assert result["author_results"]["alpha"] == {
            "fetched": 2, "new": 2, "existing": 0, "status": "success", "source": "timeline"}
        assert result["timeline_fallback_requests"] == 2          # 查一次 user id + 一页时间线
        # 入库的推文都补上了 adapter 认的 author.username
        stored = [c.args[2] for c in svc.save_tweet_to_db.call_args_list]      # (conn, cursor, tweet, ...)
        assert [t["author"]["username"] for t in stored] == ["alpha", "alpha"]
        tl = [c for c in svc.session.get.call_args_list if c.args[0].endswith("/last_tweets")][0]
        assert tl.kwargs["params"]["userId"] == "1508426880860704771"
        assert tl.kwargs["params"]["includeReplies"] == "true"
        # user id 随本轮事务写进订阅 metadata，下次兜底不用再查
        assert any("twitter_user_id" in str(c.args[0]) for c in cursor.execute.call_args_list)

    def test_fallback_requests_are_capped_per_run(self, fixed_now, no_sleep):
        svc = make_service(fallback_max_calls=2)
        page = resp(200, {"status": "success", "data": {"tweets": []}})
        wire_run(svc, ["alpha", "beta"], {"alpha": exhausted_503(), "beta": exhausted_503()},
                 timeline=[page], user_info={"status": "success", "data": {"id": "42"}})
        result = svc.fetch_and_store_tweets()
        # 本轮 2 次兜底额度全用在第一个作者身上（查 id + 一页），第二个作者不再兜底
        paths = requested_paths(svc.session.get)
        assert (paths.count("/twitter/user/info"), paths.count("/twitter/user/last_tweets")) == (1, 1)
        assert (result["timeline_fallback_requests"], result["authors_failed"]) == (2, 1)

    def test_budget_of_one_skips_fallback_instead_of_wasting_a_lookup(self, fixed_now, no_sleep):
        svc = make_service(fallback_max_calls=1)
        wire_run(svc, ["alpha"], {"alpha": exhausted_503()})
        result = svc.fetch_and_store_tweets()
        assert requested_paths(svc.session.get) == [SEARCH] * 5
        assert result["authors_failed"] == 1

    def test_out_of_credits_aborts_the_run(self, fixed_now, no_sleep):
        svc = make_service()
        wire_run(svc, ["alpha", "beta"],
                 {"alpha": resp(402, {"message": "Credits is not enough.Please recharge"}),
                  "beta": [search_page([tweet(NOW_TS - 60)])]})
        result = svc.fetch_and_store_tweets()
        assert result["success"] is False and "中止" in result["message"]
        assert (result["authors_failed"], result["authors_skipped"]) == (1, 1)
        assert requested_paths(svc.session.get) == [SEARCH]       # beta 没有再发请求

    def test_soft_time_limit_returns_partial_result(self, fixed_now, no_sleep):
        svc = make_service()
        wire_run(svc, ["alpha", "beta"],
                 {"alpha": [search_page([tweet(NOW_TS - 60)])], "beta": SoftTimeLimitExceeded()})
        result = svc.fetch_and_store_tweets()
        assert result["success"] is True and "软超时" in result["message"]
        assert (result["authors_ok"], result["authors_skipped"]) == (1, 1)

    def test_run_budget_leaves_remaining_authors_for_next_run(self, fixed_now, no_sleep):
        svc = make_service(run_budget_minutes=0)
        wire_run(svc, ["alpha"], {"alpha": [search_page([])]})
        result = svc.fetch_and_store_tweets()
        assert (result["authors_ok"], result["authors_skipped"]) == (0, 1)
        assert svc.session.get.call_count == 0


# ── 深度回扫 ─────────────────────────────────────────────────────────────────

class TestDeepSweep:
    def test_sweeps_in_chunks_newest_first_without_writing_history(self, no_sleep):
        svc = make_service(deep_sweep_window_hours=12, deep_sweep_chunk_hours=6)
        wire_run(svc, ["alpha"],
                 {"alpha": [search_page([tweet(NOW_TS - 30)]),
                            search_page([tweet(NOW_TS - 7 * 3600)]),      # 回扫补到一条增量没看到的
                            search_page([])]},
                 sweep_due=True)
        result = svc.fetch_and_store_tweets()
        assert (result["deep_sweep"]["authors"], result["deep_sweep"]["tweets_new"]) == (1, 1)
        windows = search_queries(svc.session.get)
        assert len(windows) == 3
        newest, older = windows[1], windows[2]                     # 最新的一块先扫
        assert newest[1] - newest[0] == 6 * 3600 and older[1] == newest[0]
        assert older[0] - windows[0][0] > 0                         # 每块都落在增量窗口之内
        # 回扫是增量轮询的补充，不单独记 fetch history
        assert svc._record_fetch_history.call_count == 1

    def test_a_failed_chunk_does_not_block_other_authors(self, no_sleep):
        svc = make_service(deep_sweep_window_hours=6)
        wire_run(svc, ["alpha", "beta"],
                 {"alpha": [search_page([]), exhausted_503()], "beta": [search_page([]), search_page([tweet(NOW_TS - 60)])]},
                 sweep_due=True)
        sweep = svc.fetch_and_store_tweets()["deep_sweep"]
        assert (sweep["authors"], sweep["authors_incomplete"], sweep["windows_failed"]) == (2, 1, 1)
        assert sweep["tweets_new"] == 1

    def test_skipped_when_some_authors_failed(self, no_sleep):
        svc = make_service(fallback_enabled=False)
        wire_run(svc, ["alpha", "beta"], {"alpha": exhausted_503(), "beta": [search_page([])]}, sweep_due=True)
        assert svc.fetch_and_store_tweets()["deep_sweep"] is None
        tw._mark_deep_sweep_started.assert_not_called()

    def test_skipped_when_not_due(self, no_sleep):
        svc = make_service()
        wire_run(svc, ["alpha"], {"alpha": [search_page([])]}, sweep_due=False)
        assert svc.fetch_and_store_tweets()["deep_sweep"] is None
        tw._mark_deep_sweep_started.assert_not_called()

    def test_budget_exhaustion_is_reported(self, no_sleep):
        svc = make_service(deep_sweep_budget_minutes=0)
        wire_run(svc, ["alpha"], {"alpha": [search_page([])]}, sweep_due=True)
        sweep = svc.fetch_and_store_tweets()["deep_sweep"]
        assert (sweep["budget_exhausted"], sweep["authors"]) == (True, 0)


class TestDeepSweepMarker:
    def test_due_only_after_the_interval_has_elapsed(self):
        client = mock.Mock()
        with mock.patch.object(tw, "_get_redis", return_value=client):
            client.get.return_value = None
            assert tw._deep_sweep_due(22, now_ts=1000.0) is True      # 从未回扫过
            client.get.return_value = b"1000"
            assert tw._deep_sweep_due(22, now_ts=1000.0 + 21 * 3600) is False
            assert tw._deep_sweep_due(22, now_ts=1000.0 + 22 * 3600) is True

    def test_never_due_without_redis(self):
        with mock.patch.object(tw, "_get_redis", return_value=None):
            assert tw._deep_sweep_due(22) is False

    def test_start_marker_is_written_with_a_ttl(self):
        client = mock.Mock()
        with mock.patch.object(tw, "_get_redis", return_value=client):
            tw._mark_deep_sweep_started(now_ts=1234.5)
        client.set.assert_called_once_with(tw._DEEP_SWEEP_MARKER_KEY, 1234, ex=tw._DEEP_SWEEP_MARKER_TTL)


class TestTaskWiring:
    def test_time_limits_are_bounded_for_a_two_slot_worker(self):
        from backend.tasks.subscription_tasks import fetch_twitter_subscription_task as task
        assert (task.soft_time_limit, task.time_limit) == (2400, 2520)
        assert task.max_retries == 1

    def _run_task_with(self, exc):
        from backend.tasks import subscription_tasks as tasks_mod
        service = mock.Mock()
        service.fetch_and_store_tweets.side_effect = exc
        service.last_result = {"success": True, "message": "partial", "tweets_new": 3}
        fake_self = mock.Mock()
        fake_self.request.retries = 0
        fake_self.max_retries = 1
        with mock.patch.object(tw, "TwitterSubscriptionService", return_value=service):
            result = tasks_mod.fetch_twitter_subscription_task.run.__func__(fake_self)
        return result, fake_self

    def test_soft_time_limit_ends_the_task_instead_of_retrying(self):
        result, fake_self = self._run_task_with(SoftTimeLimitExceeded())
        assert result == {"success": True, "message": "partial", "tweets_new": 3}
        fake_self.retry.assert_not_called()

    def test_other_errors_are_retried(self):
        from backend.tasks import subscription_tasks as tasks_mod
        service = mock.Mock()
        service.fetch_and_store_tweets.side_effect = RuntimeError("db down")
        fake_self = mock.Mock()
        fake_self.request.retries = 0
        fake_self.max_retries = 1
        fake_self.retry.side_effect = RuntimeError("retry scheduled")
        with mock.patch.object(tw, "TwitterSubscriptionService", return_value=service):
            with pytest.raises(RuntimeError, match="retry scheduled"):
                tasks_mod.fetch_twitter_subscription_task.run.__func__(fake_self)
        fake_self.retry.assert_called_once()
