#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
IPO Calendar 数据获取脚本

目的：
1. 从 EODHD API 获取 IPO calendar 数据
2. 保存为 JSON 文件以便分析数据格式
3. 用于设计数据库表结构

使用方法：
    python scripts/fetch_ipo_calendar_sample.py

输出：
    - ipo_calendar_sample.json: 包含原始 API 响应数据
    - ipo_calendar_parsed.json: 包含解析后的数据（用于分析字段）
"""

import os
import json
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# EODHD API 配置
BASE_URL = "https://eodhd.com/api"
EODHD_API_KEY = os.getenv('EODHD_API_TOKEN')

if not EODHD_API_KEY:
    raise ValueError("EODHD_API_TOKEN 环境变量未设置，请检查 .env 文件")


def fetch_ipo_calendar(start_date: str, end_date: str, api_key: str) -> dict:
    """
    从 EODHD API 获取 IPO calendar 数据
    
    Args:
        start_date: 开始日期 (YYYY-MM-DD)
        end_date: 结束日期 (YYYY-MM-DD)
        api_key: EODHD API 密钥
    
    Returns:
        dict: API 响应数据
    """
    url = f"{BASE_URL}/calendar/ipos"
    params = {
        "from": start_date,
        "to": end_date,
        "api_token": api_key,
        "fmt": "json"
    }
    
    print(f"请求 IPO Calendar 数据...")
    print(f"  日期范围: {start_date} 到 {end_date}")
    print(f"  API URL: {url}")
    
    try:
        # 禁用代理以避免连接问题
        proxies = {
            'http': None,
            'https': None
        }
        
        response = requests.get(url, params=params, timeout=30, proxies=proxies)
        response.raise_for_status()
        data = response.json()
        
        print(f"  ✅ 请求成功")
        return data
        
    except requests.exceptions.HTTPError as e:
        print(f"  ❌ HTTP 错误: {e}")
        print(f"  响应内容: {e.response.text if e.response else 'N/A'}")
        raise
    except requests.exceptions.RequestException as e:
        print(f"  ❌ 请求异常: {e}")
        raise
    except Exception as e:
        print(f"  ❌ 未知错误: {e}")
        raise


def analyze_data_structure(data: dict) -> dict:
    """
    分析数据结构，提取字段信息
    
    Args:
        data: API 响应数据
    
    Returns:
        dict: 数据结构分析结果
    """
    analysis = {
        "total_records": 0,
        "fields": {},
        "sample_records": [],
        "exchange_distribution": {},
        "date_range": {}
    }
    
    ipos = data.get('ipos', [])
    
    if not ipos:
        print("  ⚠️ 未找到 IPO 数据")
        return analysis
    
    analysis["total_records"] = len(ipos)
    print(f"\n数据统计:")
    print(f"  总记录数: {len(ipos)}")
    
    # 分析字段
    if ipos:
        first_record = ipos[0]
        analysis["fields"] = {
            field: {
                "type": type(value).__name__,
                "sample_value": str(value)[:100] if value is not None else None,  # 限制长度
                "is_nullable": False
            }
            for field, value in first_record.items()
        }
        
        # 收集样本记录（最多5条）
        analysis["sample_records"] = ipos[:5]
        
        # 分析交易所分布
        if 'exchange' in first_record:
            exchanges = {}
            for ipo in ipos:
                exchange = ipo.get('exchange', 'Unknown')
                exchanges[exchange] = exchanges.get(exchange, 0) + 1
            analysis["exchange_distribution"] = exchanges
            print(f"  交易所分布:")
            for exchange, count in sorted(exchanges.items(), key=lambda x: x[1], reverse=True):
                print(f"    {exchange}: {count}")
        
        # 分析日期范围
        if 'start_date' in first_record:
            dates = [ipo.get('start_date') for ipo in ipos if ipo.get('start_date')]
            if dates:
                analysis["date_range"] = {
                    "earliest": min(dates),
                    "latest": max(dates)
                }
                print(f"  日期范围: {analysis['date_range']['earliest']} 到 {analysis['date_range']['latest']}")
    
    return analysis


def filter_us_hk_exchanges(ipos: list) -> list:
    """
    过滤 US 和 HK 交易所的 IPO
    
    Args:
        ipos: IPO 列表
    
    Returns:
        list: 过滤后的 IPO 列表
    """
    if not ipos:
        return []
    
    us_hk_exchanges = ['NYSE', 'NASDAQ', 'AMEX', 'US', 'HKSE', 'Hong Kong']
    
    filtered = []
    for ipo in ipos:
        exchange = ipo.get('exchange', '')
        if any(ex in str(exchange) for ex in us_hk_exchanges):
            filtered.append(ipo)
    
    return filtered


def main():
    """主函数"""
    print("=" * 60)
    print("IPO Calendar 数据获取脚本")
    print("=" * 60)
    
    # 设置日期范围（获取未来30天的数据）
    today = datetime.now()
    start_date = today.strftime("%Y-%m-%d")
    end_date = (today + timedelta(days=30)).strftime("%Y-%m-%d")
    
    print(f"\n配置:")
    print(f"  开始日期: {start_date}")
    print(f"  结束日期: {end_date}")
    print(f"  API Key: {EODHD_API_KEY[:10]}...")
    
    # 获取数据
    try:
        raw_data = fetch_ipo_calendar(start_date, end_date, EODHD_API_KEY)
        
        # 保存原始数据
        output_file_raw = "ipo_calendar_sample.json"
        with open(output_file_raw, 'w', encoding='utf-8') as f:
            json.dump(raw_data, f, indent=2, ensure_ascii=False)
        print(f"\n✅ 原始数据已保存至: {output_file_raw}")
        
        # 分析数据结构
        analysis = analyze_data_structure(raw_data)
        
        # 过滤 US/HK 交易所
        ipos = raw_data.get('ipos', [])
        us_hk_ipos = filter_us_hk_exchanges(ipos)
        print(f"\n  过滤后 (US/HK): {len(us_hk_ipos)} 条记录")
        
        # 保存解析后的数据（包含分析结果）
        parsed_data = {
            "metadata": {
                "fetch_date": datetime.now().isoformat(),
                "date_range": {
                    "start": start_date,
                    "end": end_date
                },
                "total_records": len(ipos),
                "us_hk_records": len(us_hk_ipos)
            },
            "analysis": analysis,
            "raw_data": raw_data,
            "us_hk_data": {
                "ipos": us_hk_ipos
            }
        }
        
        output_file_parsed = "ipo_calendar_parsed.json"
        with open(output_file_parsed, 'w', encoding='utf-8') as f:
            json.dump(parsed_data, f, indent=2, ensure_ascii=False)
        print(f"✅ 解析数据已保存至: {output_file_parsed}")
        
        # 打印字段信息
        print(f"\n字段分析:")
        if analysis.get("fields"):
            print(f"  字段数量: {len(analysis['fields'])}")
            print(f"  字段列表:")
            for field, info in analysis["fields"].items():
                sample = info["sample_value"]
                if sample and len(sample) > 50:
                    sample = sample[:50] + "..."
                print(f"    - {field}: {info['type']} (示例: {sample})")
        
        # 打印样本记录
        if analysis.get("sample_records"):
            print(f"\n样本记录 (前3条):")
            for i, record in enumerate(analysis["sample_records"][:3], 1):
                print(f"\n  记录 {i}:")
                for key, value in record.items():
                    print(f"    {key}: {value}")
        
        print("\n" + "=" * 60)
        print("✅ 数据获取完成！")
        print("=" * 60)
        print(f"\n提示:")
        print(f"  1. 查看 {output_file_raw} 了解原始 API 响应格式")
        print(f"  2. 查看 {output_file_parsed} 了解数据结构分析结果")
        print(f"  3. 使用这些信息设计数据库表结构")
        
    except Exception as e:
        print(f"\n❌ 错误: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())

