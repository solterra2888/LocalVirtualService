# -*- coding: utf-8 -*-
"""
LLMTemplateResolver 集成验证脚本 (PR-3 ④)

使用真实生产 DB 验证三级 fallback 机制:
    1. SYSTEM_DEFAULTS hit (无 user_id, 无 runtime)
    2. user preference hit
    3. runtime override hit
    4. SYSTEM_ONLY 模块强制走默认
    5. 失效模板降级
    6. resolve_code 便利接口

运行:
    cd /Users/kevinliu/Documents/GuanHeProjects/guanhetech
    PYTHONPATH=. python scripts/verify_llm_template_resolver.py
"""
import sys
import os

# 让脚本能从仓库根目录直接跑
HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, ROOT)

from backend.services.llm_template_resolver import (  # noqa: E402
    LLMTemplateResolver,
    ResolveError,
)


def case(label):
    print(f"\n── [{label}] " + "─" * (60 - len(label)))


def assert_eq(actual, expected, msg=""):
    if actual == expected:
        print(f"  ✓ {msg or 'OK'}: {actual!r}")
    else:
        print(f"  ✗ FAIL {msg}: expected={expected!r}, actual={actual!r}")
        return False
    return True


def main():
    resolver = LLMTemplateResolver()
    all_ok = True

    # ──────────────────────────────────────────────────────────────────
    # Case 1: SYSTEM_DEFAULTS hit (无 user_id, 无 runtime)
    # ──────────────────────────────────────────────────────────────────
    case("Case 1: file_tagging 无偏好 → 走 system default content_tag_v1")
    r = resolver.resolve(module_code="file_tagging")
    all_ok &= assert_eq(r.template_code, "content_tag_v1", "template_code")
    all_ok &= assert_eq(r.source, "default", "source")
    all_ok &= assert_eq(r.module_code, "file_tagging", "module_code")
    print(f"    full: {r}")

    # ──────────────────────────────────────────────────────────────────
    # Case 2: user preference hit
    # 生产数据: user_id=2, module_code='twitter_phase2', template_id=47
    # ──────────────────────────────────────────────────────────────────
    case("Case 2: user_id=2 + twitter_phase2 → 走 user preference (template 47)")
    r = resolver.resolve(module_code="twitter_phase2", user_id=2)
    all_ok &= assert_eq(r.template_id, 47, "template_id")
    all_ok &= assert_eq(r.source, "preference", "source")
    print(f"    full: {r}")

    # ──────────────────────────────────────────────────────────────────
    # Case 3: 用户没设偏好 → 回退 default
    # ──────────────────────────────────────────────────────────────────
    case("Case 3: user_id=999 (无偏好) + twitter_phase2 → 走 default")
    r = resolver.resolve(module_code="twitter_phase2", user_id=999)
    all_ok &= assert_eq(r.template_code, "twitter_daily_phase2_default", "template_code")
    all_ok &= assert_eq(r.source, "default", "source")

    # ──────────────────────────────────────────────────────────────────
    # Case 4: runtime override
    # ──────────────────────────────────────────────────────────────────
    case("Case 4: runtime_template_id=35 (content_tag_v1) → 走 runtime, 覆盖一切")
    r = resolver.resolve(
        module_code="file_tagging",
        user_id=2,  # 即使有 user_id 也被 runtime 覆盖
        runtime_template_id=35,
    )
    all_ok &= assert_eq(r.template_id, 35, "template_id")
    all_ok &= assert_eq(r.source, "runtime", "source")

    # ──────────────────────────────────────────────────────────────────
    # Case 5: SYSTEM_ONLY 模块 (twitter_phase1) → 强制系统默认, 忽略 user_id 偏好
    # ──────────────────────────────────────────────────────────────────
    case("Case 5: twitter_phase1 (SYSTEM_ONLY) + user_id=1 → 强制走系统默认")
    r = resolver.resolve(module_code="twitter_phase1", user_id=1)
    all_ok &= assert_eq(r.template_code, "twitter_daily_phase1_default", "template_code")
    all_ok &= assert_eq(r.source, "default", "source")

    # ──────────────────────────────────────────────────────────────────
    # Case 6: runtime_template_id 失效 (不存在的 ID) → 降级到 default, 不抛错
    # ──────────────────────────────────────────────────────────────────
    case("Case 6: runtime_template_id=99999 (不存在) → 降级到 default")
    r = resolver.resolve(
        module_code="file_tagging",
        runtime_template_id=99999,
    )
    all_ok &= assert_eq(r.template_code, "content_tag_v1", "template_code")
    all_ok &= assert_eq(r.source, "default", "source")

    # ──────────────────────────────────────────────────────────────────
    # Case 7: 没有 default 的模块 (feed_summary) + 无 runtime → 抛 ResolveError
    # ──────────────────────────────────────────────────────────────────
    case("Case 7: feed_summary 无 default + 无偏好 + 无 runtime → ResolveError")
    try:
        r = resolver.resolve(module_code="feed_summary")
        print(f"  ✗ FAIL: expected ResolveError, got {r}")
        all_ok = False
    except ResolveError as e:
        print(f"  ✓ OK: 正确抛出 ResolveError: {e}")

    # ──────────────────────────────────────────────────────────────────
    # Case 8: resolve_code 便利接口
    # ──────────────────────────────────────────────────────────────────
    case("Case 8: resolve_code('file_tagging') 直接返回 str")
    code = resolver.resolve_code(module_code="file_tagging")
    all_ok &= assert_eq(code, "content_tag_v1", "resolve_code 返回值")

    # ──────────────────────────────────────────────────────────────────
    # Case 9: 不存在的 module_code → ResolveError
    # ──────────────────────────────────────────────────────────────────
    case("Case 9: 不存在的 module_code='zzz_nope' → ResolveError")
    try:
        r = resolver.resolve(module_code="zzz_nope")
        print(f"  ✗ FAIL: expected ResolveError, got {r}")
        all_ok = False
    except ResolveError as e:
        print(f"  ✓ OK: 正确抛出 ResolveError: {e}")

    # ──────────────────────────────────────────────────────────────────
    # Case 10: user_id=12, youtube_phase2 → 应命中 user preference template 50
    # ──────────────────────────────────────────────────────────────────
    case("Case 10: user_id=12 + youtube_phase2 → 命中偏好 template 50")
    r = resolver.resolve(module_code="youtube_phase2", user_id=12)
    all_ok &= assert_eq(r.template_id, 50, "template_id")
    all_ok &= assert_eq(r.source, "preference", "source")

    # ──────────────────────────────────────────────────────────────────
    # 总结
    # ──────────────────────────────────────────────────────────────────
    print("\n" + "=" * 70)
    if all_ok:
        print("✅ ALL CASES PASS")
        return 0
    else:
        print("❌ SOME CASES FAILED")
        return 1


if __name__ == "__main__":
    sys.exit(main())
