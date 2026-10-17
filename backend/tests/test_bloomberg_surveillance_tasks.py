"""Bloomberg Surveillance 摘要任务派发契约。

覆盖手动 /generate（不传 window_end）曾退化为自然日窗口、
取不到前一日美东正片的缺陷：缺省必须落到当前时刻的滚动窗口。
"""
from datetime import timedelta
from unittest import mock

from backend.tasks.bloomberg_surveillance_tasks import (
    generate_bloomberg_surveillance_task,
)
from backend.utils.datetime_utils import utcnow


def _stub_service():
    svc = mock.MagicMock()
    # 占位 reason 使后置渲染钩子跳过，测试无需 mock 报告服务。
    svc.generate = mock.AsyncMock(return_value={
        "success": True,
        "summary_id": 1,
        "skipped": True,
        "reason": "no_video_found",
    })
    return svc


class TestWindowEndDefault:
    def test_missing_window_end_uses_rolling_window(self):
        svc = _stub_service()
        with mock.patch(
            "backend.services.bloomberg_surveillance_service"
            ".BloombergSurveillanceService",
            return_value=svc,
        ):
            generate_bloomberg_surveillance_task.run(report_date="2026-10-16")

        we = svc.generate.call_args.kwargs["window_end"]
        assert we is not None and we.tzinfo is not None
        assert abs(we - utcnow()) < timedelta(minutes=5)

    def test_explicit_window_end_passthrough(self):
        svc = _stub_service()
        with mock.patch(
            "backend.services.bloomberg_surveillance_service"
            ".BloombergSurveillanceService",
            return_value=svc,
        ):
            generate_bloomberg_surveillance_task.run(
                report_date="2026-10-16",
                window_end="2026-10-16T04:30:00+00:00",
            )

        we = svc.generate.call_args.kwargs["window_end"]
        assert we.isoformat() == "2026-10-16T04:30:00+00:00"
