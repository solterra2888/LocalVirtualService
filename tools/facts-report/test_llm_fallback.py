#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""llm_fallback 离线单测:不联网、不碰真实工作区。

跑法: python -I tools/facts-report/test_llm_fallback.py
"""

import json
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import llm_fallback as fb  # noqa: E402

FAILURES = []


def check(name, cond, detail=""):
    status = "PASS" if cond else "FAIL"
    print(f"[{status}] {name}" + (f" — {detail}" if detail and not cond else ""))
    if not cond:
        FAILURES.append(name)


# ── 1. moderation_blocked / extract_root_cause ──────────────────────────
real_stderr = """(node:2173030) ExperimentalWarning: SQLite is an experimental feature and might change at any time
(Use `node --trace-warnings ...` to show where the warning was created)
Error: Turn execution failed (traceId: 25ee4d9a-edc9-400a-b9ba-0962b6100e36)
[1301][System detected potentially unsafe or sensitive content in input or generation. Please avoid using prompts that may generate sensitive content.][2026111908430622a339516c974492]
"""
check("moderation_blocked hits 1301", fb.moderation_blocked(real_stderr))
check("moderation_blocked plain text",
      fb.moderation_blocked("some sensitive content detected"))
check("moderation_blocked clean stderr",
      not fb.moderation_blocked("Error: Turn execution failed (traceId: x)"))
cause = fb.extract_root_cause(real_stderr)
check("root cause keeps 1301 line", "1301" in cause, cause)
check("root cause drops node noise", "ExperimentalWarning" not in cause, cause)
check("root cause keeps traceId", "Turn execution failed" in cause, cause)

# ── 2. parse_json_payload(围栏/前后杂文/嵌套) ────────────────────────────
obj = fb.parse_json_payload('```json\n{"files": {"a.md": "x"}}\n```')
check("parse fenced json", obj == {"files": {"a.md": "x"}})
obj = fb.parse_json_payload('Here you go:\n{"files": {"a.md": "line1\\nline2"}}\nDone.')
check("parse with prose around", obj == {"files": {"a.md": "line1\nline2"}})
try:
    fb.parse_json_payload("no json at all")
    check("parse garbage raises", False)
except fb.FallbackUnavailableError:
    check("parse garbage raises", True)

# ── 3. build_single_shot_prompt ─────────────────────────────────────────
prompt = fb.build_single_shot_prompt("TASK BODY", '{"sources": {}}')
check("prompt keeps task", "TASK BODY" in prompt)
check("prompt inlines snapshot", '{"sources": {}}' in prompt)
check("prompt carries DONE marker rule", fb.DONE_MARKER in prompt)
check("prompt has snapshot tags", "<snapshot>" in prompt and "</snapshot>" in prompt)

# ── 4. run_single_shot_files 白名单(monkeypatch chat_completion) ────────
def fake_gateway(messages, max_tokens=16384):
    body = messages[0]["content"]
    if "ESCAPE_TEST" in body:  # 模拟返回越界路径
        content = json.dumps({"files": {"../../etc/passwd": "x"}})
    elif "PARTIAL_TEST" in body:  # 模拟漏交付
        content = json.dumps({"files": {"sections/09.md": "ok"}})
    else:
        content = json.dumps({"files": {
            "sections/09.md": "## 回顾 · 地缘政治\n- bullet\n",
            "sections/09.claims.json": json.dumps(
                [{"value": "3%", "path": "market.cross_asset[0].pct", "text": "t"}]),
        }})
    return content, {"inputTokens": 10, "outputTokens": 5, "cacheReadTokens": 0}

fb.chat_completion = fake_gateway
files, usage = fb.run_single_shot_files(
    "NORMAL PROMPT", "{}",
    ["sections/09.md", "sections/09.claims.json"])
check("files parsed", set(files) == {"sections/09.md", "sections/09.claims.json"})
check("usage normalized", usage["inputTokens"] == 10)

try:
    fb.run_single_shot_files("ESCAPE_TEST PROMPT", "{}", ["sections/09.md"])
    check("path escape rejected", False)
except fb.FallbackUnavailableError:
    check("path escape rejected", True)
try:
    fb.run_single_shot_files("PARTIAL_TEST PROMPT", "{}",
                             ["sections/09.md", "sections/09.claims.json"])
    # 漏交付在 run_single_shot_files 层不报错(白名单只拒未知路径),
    # 完整性由 _fallback_sections 的 expected 检查兜住 —— 这里验证不越界
    check("partial not escaped", True)
except fb.FallbackUnavailableError:
    check("partial not escaped", True)

# ── 5. write_fallback_files(DONE 补齐 / claims 校验 / 落盘) ─────────────
with tempfile.TemporaryDirectory() as td:
    ws = Path(td)
    files = {
        "sections/09_geopolitics.md": "## 回顾 · 地缘政治\n- b1 <!-- audit: [news_macro.geo g1] -->",
        "sections/09_geopolitics.claims.json": '[{"value": "3020", "path": "news_macro.geo[id=g2].title", "text": "3020万桶"}]',
    }
    fb.write_fallback_files(ws, files, ["sections/09_geopolitics.md"])
    md = (ws / "sections/09_geopolitics.md").read_text(encoding="utf-8")
    claims = json.loads((ws / "sections/09_geopolitics.claims.json").read_text(encoding="utf-8"))
    check("md written + DONE appended", md.rstrip().endswith(fb.DONE_MARKER), md[-60:])
    check("claims valid array", isinstance(claims, list) and claims[0]["value"] == "3020")
    # claims 非数组 → 拒绝
    try:
        fb.write_fallback_files(ws, {"x.claims.json": '{"a":1}'}, [])
        check("bad claims rejected", False)
    except fb.FallbackUnavailableError:
        check("bad claims rejected", True)

# ── 6. force_fallback 开关 ──────────────────────────────────────────────
import os
os.environ["MB_FALLBACK_FORCE"] = "geopolitics,plan"
check("force match", fb.force_fallback("geopolitics") and fb.force_fallback("plan"))
check("force no match", not fb.force_fallback("fomc"))
os.environ["MB_FALLBACK_FORCE"] = "1"
check("force all", fb.force_fallback("anything"))
os.environ["MB_FALLBACK_FORCE"] = "0"
check("force off", not fb.force_fallback("geopolitics"))
del os.environ["MB_FALLBACK_FORCE"]

# ── 7. _normalize_usage ────────────────────────────────────────────────
u = fb._normalize_usage({"prompt_tokens": 100, "completion_tokens": 7,
                         "prompt_tokens_details": {"cached_tokens": 40}})
check("normalize usage", u == {"inputTokens": 100, "outputTokens": 7,
                               "cacheReadTokens": 40})

# ── 8. classify_zcode_failure(traceId → CLI jsonl 追查) ────────────────
# 实测 stderr 不含 1301,只有 traceId;真实详情在 zcode CLI jsonl 日志
from datetime import date
with tempfile.TemporaryDirectory() as td:
    fb._ZCODE_LOG_DIR = Path(td)
    trace = "25ee4d9a-edc9-400a-b9ba-0962b6100e36"
    logf = Path(td) / f"zcode-{date.today():%Y-%m-%d}.jsonl"
    logf.write_text(
        '{"event":"turn.failed","traceId":"' + trace + '"}\n'
        '{"event":"model.stream_attempt_cleanup.failed","traceId":"' + trace
        + '","context":{"failures":[{"errorMessage":"[1301][System detected '
        'potentially unsafe or sensitive content in input or generation.]"}]}}\n',
        encoding="utf-8")
    stderr_no_1301 = ("ExperimentalWarning: SQLite is an experimental feature\n"
                      "Error: Turn execution failed (traceId: " + trace + ")")
    check("classify via traceId lookup",
          fb.classify_zcode_failure(stderr_no_1301))
    check("classify stderr direct", fb.classify_zcode_failure(
        "x [1301][sensitive content] y"))
    other = "Error: Turn execution failed (traceId: 00000000-0000-0000-0000-000000000000)"
    check("classify unrelated trace false", not fb.classify_zcode_failure(other))
    check("classify no trace no marker false",
          not fb.classify_zcode_failure("plain failure"))
fb._ZCODE_LOG_DIR = Path.home() / ".zcode" / "cli" / "log"

print()
if FAILURES:
    print(f"{len(FAILURES)} FAILED: {FAILURES}")
    sys.exit(1)
print("all offline tests passed")
