# -*- coding: utf-8 -*-
"""
llm_pricing 模块 Smoke Test

验证内容：
  1. 各主力模型的定价精度（输入/输出分开计费 vs 旧版统一 total_tokens×0.00001）
  2. Qwen 阶梯计费：同一模型不同 prompt_tokens 量触发不同档位
  3. 未知模型降级估算不报错
  4. 三家 Provider 的 QwenProvider._calculate_cost / DoubaoProvider._calculate_cost /
     GLMProvider._calculate_cost 新签名（返回 tuple）可正常调用
  5. 无 prompt/completion 拆分时的 75/25 fallback 逻辑

运行（无需 DB / LLM，纯本地单元测试）：
    cd /Users/kevinliu/Documents/GuanHeProjects/guanhetech
    PYTHONPATH=. python scripts/verify_llm_pricing.py
"""
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, ROOT)

from backend.core.llm_pricing import calculate_cost, get_model_pricing_info  # noqa: E402

PASS = 0
FAIL = 0


def case(label: str):
    print(f"\n── [{label}] " + "─" * max(0, 60 - len(label)))


def ok(msg: str):
    global PASS
    PASS += 1
    print(f"  ✓ {msg}")


def fail(msg: str):
    global FAIL
    FAIL += 1
    print(f"  ✗ FAIL {msg}")


def check_near(actual: float, expected: float, tol: float = 1e-9, label: str = ""):
    if abs(actual - expected) <= tol:
        ok(f"{label}: {actual:.8f}")
    else:
        fail(f"{label}: 期望 {expected:.8f}，实际 {actual:.8f}")


# ──────────────────────────────────────────────────────────────────────────────
# Case 1: Qwen 常用模型精确计费（无阶梯）
# ──────────────────────────────────────────────────────────────────────────────
case("Case 1: Qwen 无阶梯模型精确计费")

# qwen-turbo: 输入 ¥0.3/M，输出 ¥3/M
cost, cur = calculate_cost("qwen", "qwen-turbo", prompt_tokens=750, completion_tokens=250)
check_near(cost, 750 * 0.3 / 1_000_000 + 250 * 3.0 / 1_000_000, label="qwen-turbo 1000tokens(750+250)")
assert cur == "RMB", f"currency should be RMB, got {cur}"
ok("qwen-turbo currency=RMB")

# qwen-max: 输入 ¥2.4/M，输出 ¥9.6/M
cost, _ = calculate_cost("qwen", "qwen-max", prompt_tokens=750, completion_tokens=250)
check_near(cost, 750 * 2.4 / 1_000_000 + 250 * 9.6 / 1_000_000, label="qwen-max 1000tokens(750+250)")

# qwen-max-latest 等同于 qwen-max
cost2, _ = calculate_cost("qwen", "qwen-max-latest", prompt_tokens=750, completion_tokens=250)
check_near(cost2, cost, label="qwen-max-latest 与 qwen-max 相同")

# qwen-max-2024-09-19 旧版：输入 ¥20/M，输出 ¥60/M（明显更贵）
cost_old, _ = calculate_cost("qwen", "qwen-max-2024-09-19", prompt_tokens=750, completion_tokens=250)
if cost_old > cost * 5:
    ok(f"qwen-max-2024-09-19 旧版价格远高于新版：{cost_old:.6f} vs {cost:.6f}")
else:
    fail(f"qwen-max-2024-09-19 旧版价格应远高于新版，{cost_old:.6f} vs {cost:.6f}")

# ──────────────────────────────────────────────────────────────────────────────
# Case 2: Qwen 阶梯计费 — qwen3-max（3档）
# ──────────────────────────────────────────────────────────────────────────────
case("Case 2: qwen3-max 三档阶梯计费")

# 档 1: prompt_tokens ≤ 32,000 → 输入 ¥2.5/M，输出 ¥10/M
c1, _ = calculate_cost("qwen", "qwen3-max", prompt_tokens=10_000, completion_tokens=2_000)
check_near(c1, 10_000 * 2.5 / 1_000_000 + 2_000 * 10.0 / 1_000_000, label="档1 (≤32K)")

# 档 2: prompt_tokens=50,000 → 输入 ¥4/M，输出 ¥16/M
c2, _ = calculate_cost("qwen", "qwen3-max", prompt_tokens=50_000, completion_tokens=2_000)
check_near(c2, 50_000 * 4.0 / 1_000_000 + 2_000 * 16.0 / 1_000_000, label="档2 (32K~128K)")

# 档 3: prompt_tokens=150,000 → 输入 ¥7/M，输出 ¥28/M
c3, _ = calculate_cost("qwen", "qwen3-max", prompt_tokens=150_000, completion_tokens=2_000)
check_near(c3, 150_000 * 7.0 / 1_000_000 + 2_000 * 28.0 / 1_000_000, label="档3 (128K~256K)")

if c1 < c2 < c3:
    ok("三档单价依次递增（整单价制）")
else:
    fail(f"三档单价顺序异常：{c1:.6f} / {c2:.6f} / {c3:.6f}")

# ──────────────────────────────────────────────────────────────────────────────
# Case 3: qwen-plus 三档阶梯
# ──────────────────────────────────────────────────────────────────────────────
case("Case 3: qwen-plus 三档阶梯计费")

# 档 1: ≤128K → 输入 ¥0.8/M，输出 ¥2/M
cp1, _ = calculate_cost("qwen", "qwen-plus", prompt_tokens=100_000, completion_tokens=2_000)
check_near(cp1, 100_000 * 0.8 / 1_000_000 + 2_000 * 2.0 / 1_000_000, label="qwen-plus 档1 (≤128K)")

# 档 2: 128K~256K → 输入 ¥2.4/M，输出 ¥20/M
cp2, _ = calculate_cost("qwen", "qwen-plus", prompt_tokens=200_000, completion_tokens=2_000)
check_near(cp2, 200_000 * 2.4 / 1_000_000 + 2_000 * 20.0 / 1_000_000, label="qwen-plus 档2 (128K~256K)")

# 档 3: >256K → 输入 ¥4.8/M，输出 ¥48/M
cp3, _ = calculate_cost("qwen", "qwen-plus", prompt_tokens=300_000, completion_tokens=2_000)
check_near(cp3, 300_000 * 4.8 / 1_000_000 + 2_000 * 48.0 / 1_000_000, label="qwen-plus 档3 (>256K)")

# ──────────────────────────────────────────────────────────────────────────────
# Case 4: Doubao 阶梯计费 — doubao-seed-1-6-250615
# ──────────────────────────────────────────────────────────────────────────────
case("Case 4: Doubao doubao-seed-1-6-250615 三档阶梯")

d1, _ = calculate_cost("doubao", "doubao-seed-1-6-250615", prompt_tokens=10_000, completion_tokens=2_000)
check_near(d1, 10_000 * 0.8 / 1_000_000 + 2_000 * 8.0 / 1_000_000, label="档1 (≤32K)")

d2, _ = calculate_cost("doubao", "doubao-seed-1-6-250615", prompt_tokens=100_000, completion_tokens=2_000)
check_near(d2, 100_000 * 1.2 / 1_000_000 + 2_000 * 16.0 / 1_000_000, label="档2 (32K~200K)")

d3, _ = calculate_cost("doubao", "doubao-seed-1-6-250615", prompt_tokens=250_000, completion_tokens=2_000)
check_near(d3, 250_000 * 2.4 / 1_000_000 + 2_000 * 24.0 / 1_000_000, label="档3 (>200K)")

# ──────────────────────────────────────────────────────────────────────────────
# Case 5: GLM 统一单价
# ──────────────────────────────────────────────────────────────────────────────
case("Case 5: GLM 统一单价（输入=输出=¥5/M for glm-4）")

g_cost, _ = calculate_cost("glm", "glm-4", prompt_tokens=750, completion_tokens=250)
check_near(g_cost, (750 + 250) * 5.0 / 1_000_000, label="glm-4 (统一 ¥5/M)")

g_flash, _ = calculate_cost("glm", "glm-4-flash", prompt_tokens=1000, completion_tokens=500)
check_near(g_flash, 0.0, label="glm-4-flash 免费 → cost=0")

# ──────────────────────────────────────────────────────────────────────────────
# Case 6: Embedding 定价
# ──────────────────────────────────────────────────────────────────────────────
case("Case 6: text-embedding-v3 定价（¥0.5/M，统一）")

emb, _ = calculate_cost("qwen", "text-embedding-v3", prompt_tokens=50_000, completion_tokens=0)
check_near(emb, 50_000 * 0.5 / 1_000_000, label="embedding 50K tokens")

# ──────────────────────────────────────────────────────────────────────────────
# Case 7: 未知模型降级估算不报错
# ──────────────────────────────────────────────────────────────────────────────
case("Case 7: 未知模型 → 降级估算不报错")

unk_cost, unk_cur = calculate_cost("qwen", "unknown-model-xyz", prompt_tokens=1000, completion_tokens=500)
if unk_cost >= 0 and unk_cur == "RMB":
    ok(f"未知模型降级成功，cost={unk_cost:.6f}")
else:
    fail(f"未知模型降级异常，cost={unk_cost}, cur={unk_cur}")

unk_provider, _ = calculate_cost("unknown_provider", "some-model", prompt_tokens=1000, completion_tokens=500)
if unk_provider >= 0:
    ok(f"未知 provider 降级成功，cost={unk_provider:.6f}")
else:
    fail("未知 provider 应返回非负数")

# ──────────────────────────────────────────────────────────────────────────────
# Case 8: Provider._calculate_cost 新签名返回 tuple（不真实调用 LLM）
# ──────────────────────────────────────────────────────────────────────────────
case("Case 8: Provider._calculate_cost 新签名（返回 tuple）")

from backend.core.llm_framework import QwenProvider, DoubaoProvider, GLMProvider, LLMConfig, LLMProviderType  # noqa: E402

dummy_cfg_qwen = LLMConfig(
    provider_type=LLMProviderType.QWEN,
    api_key="dummy", base_url="https://example.com",
    default_model="qwen-max", models=["qwen-max"],
)
dummy_cfg_doubao = LLMConfig(
    provider_type=LLMProviderType.DOUBAO,
    api_key="dummy", base_url="https://example.com",
    default_model="doubao-seed-1-6-250615", models=["doubao-seed-1-6-250615"],
)
dummy_cfg_glm = LLMConfig(
    provider_type=LLMProviderType.GLM,
    api_key="dummy", base_url="https://example.com",
    default_model="glm-4.6", models=["glm-4.6"],
)

qwen_p   = QwenProvider(dummy_cfg_qwen)
doubao_p = DoubaoProvider(dummy_cfg_doubao)
glm_p    = GLMProvider(dummy_cfg_glm)

for provider, model, name in [
    (qwen_p,   "qwen-max",               "QwenProvider qwen-max"),
    (doubao_p, "doubao-seed-1-6-250615", "DoubaoProvider doubao-seed-1-6"),
    (glm_p,    "glm-4.6",               "GLMProvider glm-4.6"),
]:
    usage = {"prompt_tokens": 800, "completion_tokens": 200, "total_tokens": 1000}
    result = provider._calculate_cost(usage, model)
    if isinstance(result, tuple) and len(result) == 2:
        cost_val, cur_val = result
        if isinstance(cost_val, float) and cost_val >= 0 and cur_val == "RMB":
            ok(f"{name} → cost={cost_val:.8f}, currency={cur_val}")
        else:
            fail(f"{name} 返回值异常: {result}")
    else:
        fail(f"{name} 应返回 (float, str)，实际 {type(result)}: {result}")

# ──────────────────────────────────────────────────────────────────────────────
# Case 9: 75/25 fallback（仅有 total_tokens 时）
# ──────────────────────────────────────────────────────────────────────────────
case("Case 9: usage 只含 total_tokens 时 75/25 fallback")

usage_only_total = {"total_tokens": 1000}
result_fallback = qwen_p._calculate_cost(usage_only_total, "qwen-max")
cost_fallback, _ = result_fallback
# 750 * 2.4/M + 250 * 9.6/M
expected_fallback = 750 * 2.4 / 1_000_000 + 250 * 9.6 / 1_000_000
check_near(cost_fallback, expected_fallback, label="qwen-max 1000 total_tokens fallback")

# ──────────────────────────────────────────────────────────────────────────────
# Case 10: 对比旧版 vs 新版 — 量化偏差倍数
# ──────────────────────────────────────────────────────────────────────────────
case("Case 10: 旧版 vs 新版 — 偏差对比（信息展示）")

OLD_RATE = 0.00001  # 旧版 Qwen: total_tokens × 0.00001
models_to_compare = [
    ("qwen-turbo",   750, 250),
    ("qwen-plus",    750, 250),
    ("qwen-max",     750, 250),
    ("qwen3-max",    750, 250),
    ("qwen3.6-plus", 750, 250),
]
print(f"  {'模型':<22} {'旧版(元)':>12} {'新版(元)':>12} {'高估倍数':>10}")
print(f"  {'-'*22} {'-'*12} {'-'*12} {'-'*10}")
for model_name, pt, ct in models_to_compare:
    old = (pt + ct) * OLD_RATE
    new, _ = calculate_cost("qwen", model_name, pt, ct)
    ratio = old / new if new > 0 else float("inf")
    print(f"  {model_name:<22} {old:>12.6f} {new:>12.6f} {ratio:>9.1f}x")
ok("偏差信息展示完成")

# ──────────────────────────────────────────────────────────────────────────────
# 总结
# ──────────────────────────────────────────────────────────────────────────────
print(f"\n{'='*70}")
print(f"PASS: {PASS}   FAIL: {FAIL}")
if FAIL == 0:
    print("✅ ALL CASES PASS")
    sys.exit(0)
else:
    print("❌ SOME CASES FAILED")
    sys.exit(1)
