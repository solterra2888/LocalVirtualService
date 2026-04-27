# -*- coding: utf-8 -*-
"""
LocalLLMAnalysisService 重构 Smoke Test (PR-4)

不真实调用 LLM (避免计费 + 写 usage_log), 只验证:
  1. import + 实例化无报错
  2. get_template_info() 能正常加载真实模板 (content_tag_v1)
  3. analyze_file_content 的 chat_completion_with_template 委托链路
     用 mock 替代后, 返回值结构与旧版兼容
  4. process_all_contents 的循环 + final_report 行为不变

运行:
    cd /Users/kevinliu/Documents/GuanHeProjects/guanhetech
    PYTHONPATH=. python scripts/verify_local_llm_refactor.py
"""
import asyncio
import os
import sys
from unittest.mock import AsyncMock, MagicMock

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, ROOT)

from backend.services.local_llm_analysis_service import LocalLLMAnalysisService  # noqa: E402


def case(label):
    print(f"\n── [{label}] " + "─" * (max(0, 60 - len(label))))


def assert_eq(actual, expected, msg=""):
    if actual == expected:
        print(f"  ✓ {msg or 'OK'}: {actual!r}")
        return True
    print(f"  ✗ FAIL {msg}: expected={expected!r}, actual={actual!r}")
    return False


def assert_in(key, container, msg=""):
    if key in container:
        print(f"  ✓ {msg or 'has key'}: {key!r}")
        return True
    print(f"  ✗ FAIL {msg}: key {key!r} not in {list(container.keys()) if hasattr(container, 'keys') else container!r}")
    return False


async def main():
    all_ok = True

    # ──────────────────────────────────────────────────────────────────
    # Case 1: import + 实例化 (template_code='content_tag_v1')
    # ──────────────────────────────────────────────────────────────────
    case("Case 1: 实例化 LocalLLMAnalysisService(template_code='content_tag_v1')")
    svc = LocalLLMAnalysisService(template_code="content_tag_v1")
    all_ok &= assert_eq(svc.template_code, "content_tag_v1", "template_code")
    all_ok &= assert_eq(svc.template_id, None, "template_id (未传)")

    # ──────────────────────────────────────────────────────────────────
    # Case 2: get_template_info 能正常从生产 DB 加载模板
    # ──────────────────────────────────────────────────────────────────
    case("Case 2: get_template_info 加载 content_tag_v1 (从生产 DB)")
    info = svc.get_template_info()
    all_ok &= assert_eq(info.get("template_code"), "content_tag_v1", "template_code")
    all_ok &= assert_in("system_prompt", info, "包含 system_prompt")
    all_ok &= assert_in("user_prompt_template", info, "包含 user_prompt_template")
    print(f"    模板名: {info.get('template_name')}")
    print(f"    推荐 model: {info.get('recommended_model')}")

    # ──────────────────────────────────────────────────────────────────
    # Case 3: analyze_file_content 的委托与返回值结构 (mock chat_completion_with_template)
    # ──────────────────────────────────────────────────────────────────
    case("Case 3: analyze_file_content 委托 chat_completion_with_template")

    # 构造一个 mock LLMResponse
    fake_response = MagicMock()
    fake_response.content = '{"sector": "信息技术", "industry": "软件", "tickers": ["NVDA.US"]}'
    fake_response.usage = {"total_tokens": 234}
    fake_response.cost = 0.000123
    fake_response.currency = "USD"
    fake_response.model = "qwen3-max"
    fake_response.provider = "qwen"
    fake_response.parsed_data = {"sector": "信息技术", "industry": "软件", "tickers": ["NVDA.US"]}

    # mock llm_service.chat_completion_with_template
    from backend.core import llm_framework
    original_method = llm_framework.llm_service.chat_completion_with_template
    llm_framework.llm_service.chat_completion_with_template = AsyncMock(return_value=fake_response)

    try:
        result = await svc.analyze_file_content(
            file_content="这是一段关于 NVIDIA 的测试内容",
            file_name="test.md",
            custom_variables={"additional_context": "ctx"},
            user_id=42,
        )

        # 验证返回值结构 (与 v1 兼容)
        all_ok &= assert_in("report_content", result, "返回字段 report_content")
        all_ok &= assert_in("total_tokens", result, "返回字段 total_tokens")
        all_ok &= assert_in("total_price", result, "返回字段 total_price")
        all_ok &= assert_in("response_time", result, "返回字段 response_time")
        all_ok &= assert_in("model", result, "返回字段 model")
        all_ok &= assert_in("provider", result, "返回字段 provider")
        all_ok &= assert_in("currency", result, "返回字段 currency")
        all_ok &= assert_in("model_usage", result, "返回字段 model_usage")
        all_ok &= assert_in("parsed_data", result, "新增字段 parsed_data")

        all_ok &= assert_eq(result["total_tokens"], 234, "total_tokens 值")
        all_ok &= assert_eq(result["model"], "qwen3-max", "model 值")
        all_ok &= assert_eq(result["provider"], "qwen", "provider 值")
        all_ok &= assert_eq(result["parsed_data"], fake_response.parsed_data, "parsed_data 透传")

        # 验证 chat_completion_with_template 被正确调用
        call_args = llm_framework.llm_service.chat_completion_with_template.await_args
        passed_kwargs = call_args.kwargs
        all_ok &= assert_eq(passed_kwargs["template_code"], "content_tag_v1", "调用时 template_code")
        all_ok &= assert_eq(passed_kwargs["user_id"], 42, "调用时 user_id 透传")
        all_ok &= assert_in("file_content", passed_kwargs["variables"], "variables 含 file_content")
        all_ok &= assert_in("additional_context", passed_kwargs["variables"], "variables 含 additional_context")
        all_ok &= assert_eq(passed_kwargs["variables"]["file_content"], "这是一段关于 NVIDIA 的测试内容", "file_content 值")

    finally:
        llm_framework.llm_service.chat_completion_with_template = original_method

    # ──────────────────────────────────────────────────────────────────
    # Case 4: process_all_contents 循环行为
    # ──────────────────────────────────────────────────────────────────
    case("Case 4: process_all_contents 批量 + final_report = 最后一个文件")

    fake_response_2 = MagicMock()
    fake_response_2.content = "report-of-second-file"
    fake_response_2.usage = {"total_tokens": 100}
    fake_response_2.cost = 0.00005
    fake_response_2.currency = "USD"
    fake_response_2.model = "qwen3-max"
    fake_response_2.provider = "qwen"
    fake_response_2.parsed_data = None

    fake_response_1 = MagicMock()
    fake_response_1.content = "report-of-first-file"
    fake_response_1.usage = {"total_tokens": 50}
    fake_response_1.cost = 0.00002
    fake_response_1.currency = "USD"
    fake_response_1.model = "qwen3-max"
    fake_response_1.provider = "qwen"
    fake_response_1.parsed_data = None

    llm_framework.llm_service.chat_completion_with_template = AsyncMock(
        side_effect=[fake_response_1, fake_response_2]
    )

    try:
        successful_results = [
            {"content": "first content", "original_file": "/tmp/a.pdf"},
            {"content": "second content", "original_file": "/tmp/b.pdf"},
        ]
        final_report, file_times = await svc.process_all_contents(
            successful_results=successful_results,
            user_id=99,
        )

        all_ok &= assert_eq(final_report, "report-of-second-file", "final_report = 最后一个文件")
        all_ok &= assert_eq(len(file_times), 2, "file_times 长度")
        all_ok &= assert_eq(file_times[0]["file"], "a.pdf", "file 0 名")
        all_ok &= assert_eq(file_times[1]["file"], "b.pdf", "file 1 名")
        all_ok &= assert_eq(file_times[0]["total_tokens"], 50, "file 0 tokens")
        all_ok &= assert_eq(file_times[1]["total_tokens"], 100, "file 1 tokens")

    finally:
        llm_framework.llm_service.chat_completion_with_template = original_method

    # ──────────────────────────────────────────────────────────────────
    # Case 5: 空 successful_results
    # ──────────────────────────────────────────────────────────────────
    case("Case 5: process_all_contents 收到空列表")
    final_report, file_times = await svc.process_all_contents([])
    all_ok &= assert_eq(final_report, None, "空列表 final_report=None")
    all_ok &= assert_eq(file_times, [], "空列表 file_times=[]")

    # ──────────────────────────────────────────────────────────────────
    # Case 6: 缺失 template_id 与 template_code 应抛错
    # ──────────────────────────────────────────────────────────────────
    case("Case 6: 缺失 template_id/code 应抛 ValueError")
    try:
        LocalLLMAnalysisService()
        print("  ✗ FAIL: expected ValueError")
        all_ok = False
    except ValueError as e:
        print(f"  ✓ OK: 正确抛 ValueError: {e}")

    # ──────────────────────────────────────────────────────────────────
    # 总结
    # ──────────────────────────────────────────────────────────────────
    print("\n" + "=" * 70)
    if all_ok:
        print("✅ ALL CASES PASS")
        return 0
    print("❌ SOME CASES FAILED")
    return 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
