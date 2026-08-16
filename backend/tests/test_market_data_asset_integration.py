#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
集成测试：验证 EODHD 资产注册进 market_data_service 后的完整链路（参数化，可测任意资产）。

模拟调度器 (scheduler_service._execute_market_data_task) 的真实调用方式:
1. 单元级: MAP 注册正确性
2. API 级: EODHD 实时/EOD 接口对指定代码的返回可被 parse_*_data 正确解析
3. 集成级: fetch_and_store_market_data(asset_types) 全链路跑通（真实 API + 真实库）
4. 数据级: market_data_eod / market_data_realtime 中新资产数据符合预期

用法（在项目根目录）:
  python backend/tests/test_market_data_asset_integration.py --codes GSPC.INDX,NDX.INDX --asset-type index
  python backend/tests/test_market_data_asset_integration.py --codes VIX.INDX --asset-type index
  python backend/tests/test_market_data_asset_integration.py --codes GSPC.INDX,NDX.INDX --asset-type index --skip-integration
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

# 简单加载 .env（与 scripts/ 下脚本一致的手法）
def _load_env() -> None:
    env_file = Path(__file__).resolve().parent.parent.parent / ".env"
    if not env_file.exists():
        return
    for line in env_file.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, val = line.split("=", 1)[0].strip(), line.split("=", 1)[1].strip()
        if not os.getenv(key):
            os.environ[key] = val.strip('"').strip("'")


_load_env()

from backend.services.market_data_service import MarketDataService  # noqa: E402

PASSED, FAILED = [], []


def check(name: str, ok: bool, detail: str = "") -> None:
    """记录并打印单项检查结果。"""
    mark = "✅" if ok else "❌"
    suffix = f" — {detail}" if detail else ""
    print(f"{mark} {name}{suffix}")
    (PASSED if ok else FAILED).append(name)


def find_registration(codes: list):
    """在全部 MAP 中查找每个 code 的注册（显示名, asset_type, category）。"""
    from backend.services import market_data_service as mds

    maps = {
        "index": mds.INDEX_MAP, "forex": mds.FOREX_MAP, "commodity": mds.COMMODITY_MAP,
        "bond": mds.BOND_YIELD_MAP, "sector": mds.SECTOR_MAP, "etf": mds.ETF_MAP,
    }
    found = {}
    for code in codes:
        for map_name, mp in maps.items():
            for display_name, (c, at, cat) in mp.items():
                if c == code:
                    found[code] = (map_name, display_name, at, cat)
                    break
            if code in found:
                break
    return found


def test_registration(codes: list, asset_type: str) -> None:
    """1. MAP 注册正确性。"""
    found = find_registration(codes)
    for code in codes:
        reg = found.get(code)
        check(f"MAP 注册表包含 {code}", reg is not None,
              f"MAP={reg[0]}, 显示名={reg[1]}, category={reg[3]}" if reg else "未注册")
        check(f"{code} 元组结构为 (code, asset_type, category)",
              reg is not None and isinstance(reg, tuple) and len(reg) == 4)
        check(f"{code} asset_type = {asset_type}（与注册一致）",
              reg is not None and reg[2] == asset_type)


def test_api_parse(codes: list, asset_type: str) -> None:
    """2. EODHD 真实返回可被现有解析逻辑处理。"""
    svc = MarketDataService()
    found = find_registration(codes)

    for code in codes:
        display_name = found.get(code, (None, code, None, None))[1]

        rt_raw = svc.fetch_realtime_data(code)
        check(f"[{code}] 实时接口有返回", rt_raw is not None)
        parsed_rt = svc.parse_realtime_data(code, display_name, asset_type, rt_raw or {})
        check(f"[{code}] parse_realtime_data 解析成功", parsed_rt is not None,
              f"close={parsed_rt.get('close')}" if parsed_rt else "解析返回 None")

        eod_raw = svc.fetch_eod_data(code, days=7)
        check(f"[{code}] EOD 接口有返回", eod_raw is not None and len(eod_raw) > 0,
              f"{len(eod_raw or [])} 条")
        parsed_eod = svc.parse_eod_data(code, asset_type, eod_raw or [])
        check(f"[{code}] parse_eod_data 解析成功且逐条带 code/asset_type",
              len(parsed_eod) > 0 and all(r["code"] == code and r["asset_type"] == asset_type
                                          for r in parsed_eod),
              f"{len(parsed_eod)} 条")
        if parsed_eod:
            sample = parsed_eod[-1]
            check(f"[{code}] EOD 解析字段完整 (date/close)",
                  bool(sample.get("date")) and sample.get("close") is not None,
                  f"最新 {sample.get('date')} close={sample.get('close')}")


def test_full_integration(asset_type: str) -> None:
    """3. 模拟调度器全链路: fetch_and_store_market_data([asset_type])。"""
    svc = MarketDataService()
    result = svc.fetch_and_store_market_data(
        asset_types=[asset_type],     # 与调度任务 asset_types=null→全类型 等价的子集验证
        fetch_realtime=True,
        fetch_eod=True,
        eod_days=7,                   # 与 task_scheduling.params 一致
    )
    check("fetch_and_store_market_data 返回 success", bool(result.get("success")),
          str(result.get("message"))[:120])


def test_db_state(codes: list) -> None:
    """4. 数据库中新资产数据状态 + 既有资产无回归。"""
    from backend.database.db_manager import DatabaseManager
    from backend.utils.database_config import get_database_config

    db = DatabaseManager(db_conn_params=get_database_config())
    conn = db.get_connection()
    try:
        cur = conn.cursor()

        for code in codes:
            cur.execute("""
                SELECT COUNT(*), MIN(date), MAX(date)
                FROM market_data_eod WHERE code = %s
            """, (code,))
            cnt, earliest, latest = cur.fetchone()
            check(f"market_data_eod 有 {code} 数据", cnt and cnt > 0,
                  f"{cnt} 条, {earliest} ~ {latest}")
            check(f"{code} EOD 最新日期 >= 今天-4天（调度增量在追）",
                  latest is not None and str(latest) >= __import__("datetime").date(
                      __import__("datetime").datetime.utcnow().year,
                      __import__("datetime").datetime.utcnow().month,
                      __import__("datetime").datetime.utcnow().day - 4,
                  ).isoformat(), f"latest={latest}")

            cur.execute("""
                SELECT close, previous_close, change_percent
                FROM market_data_realtime WHERE code = %s
            """, (code,))
            rt = cur.fetchone()
            check(f"market_data_realtime 有 {code} 实时快照", rt is not None,
                  f"close={rt[0] if rt else None}, chg%={rt[2] if rt else None}")

        # 旧资产未受影响（抽查 SPY + VIX）
        for legacy in ("SPY.US", "VIX.INDX"):
            cur.execute("SELECT COUNT(*) FROM market_data_eod WHERE code = %s", (legacy,))
            legacy_cnt = cur.fetchone()[0]
            check(f"既有资产未受影响 ({legacy} 数据仍在)", legacy_cnt > 0, f"{legacy_cnt} 条")
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--codes", required=True, help="EODHD 代码，逗号分隔")
    parser.add_argument("--asset-type", required=True,
                        help="index/forex/commodity/bond/sector")
    parser.add_argument("--skip-integration", action="store_true",
                        help="跳过真实 API / DB 的集成步骤")
    args = parser.parse_args()

    codes = [c.strip() for c in args.codes.split(",") if c.strip()]

    print("== 1. 注册正确性 ==")
    test_registration(codes, args.asset_type)

    if not args.skip_integration:
        print("\n== 2. API 解析 ==")
        test_api_parse(codes, args.asset_type)
        print("\n== 3. 全链路集成（模拟调度器）==")
        test_full_integration(args.asset_type)
        print("\n== 4. 数据库状态 ==")
        test_db_state(codes)

    print(f"\n结果: {len(PASSED)} 通过, {len(FAILED)} 失败")
    if FAILED:
        print("失败项:", ", ".join(FAILED))
        sys.exit(1)


if __name__ == "__main__":
    main()
