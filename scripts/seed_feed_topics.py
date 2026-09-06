#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Feeds Dashboard 主题词典 seed 脚本.

用法（在项目根目录执行）:
  python scripts/seed_feed_topics.py                # 写入 curated 基线词典 (幂等, ON CONFLICT DO NOTHING)
  python scripts/seed_feed_topics.py --scan         # 扫描近 N 天 concepts 频次, 打印报告 (不写库)
  python scripts/seed_feed_topics.py --scan --write # 扫描 + 把未覆盖高频词自动建 topic (needs_review)
  python scripts/seed_feed_topics.py --days 90      # 扫描窗口 (默认 30 天)

说明:
  - curated 基线: 常见金融/投资主题 + 别名, 作为归一化锚点
  --scan 报告列: 频次 / 词 / 是否已被词典覆盖(→ topic_key)
  --write 只写"未被覆盖"的词 (阈值 --min-count, 默认 20), source='auto' 待人工确认
  依赖: backend/database/migrations/create_feed_topic_tables.sql 已执行
"""

from __future__ import annotations

import os
import sys
import argparse
import json
from collections import Counter
from typing import Dict, List

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
os.chdir(PROJECT_ROOT)

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(PROJECT_ROOT, ".env"), override=False)
except ImportError:
    pass

import psycopg2
import psycopg2.extras
from backend.utils.database_config import get_psycopg2_config

# ──────────────────────────────────────────────────────────────────────────────
# curated 基线词典: (topic_key, name_cn, name_en, category, aliases)
# 别名覆盖 LLM 常见自由写法 (concepts 由 content_tag_feeds_v1 产出, 1-4 个中文主题词)
# ──────────────────────────────────────────────────────────────────────────────

CURATED_TOPICS = [
    # ── 宏观 & 货币政策 ──
    ("fed_policy",            "美联储政策",       "Fed Policy",            "宏观", ["美联储", "FOMC", "降息预期", "加息", "联储", "Federal Reserve", "Fed"]),
    ("rates",                 "利率与债券",       "Rates & Bonds",         "宏观", ["美债", "国债收益率", "债券", "收益率曲线", "利率", "Treasury", "Bonds", "收益率"]),
    ("inflation",             "通胀",             "Inflation",             "宏观", ["CPI", "PCE", "物价", "通胀数据", "Inflation"]),
    ("us_economy",            "美国经济",         "US Economy",            "宏观", ["美国GDP", "就业数据", "非农", "衰退", "经济数据", "US Economy"]),
    ("china_economy",         "中国经济",         "China Economy",         "宏观", ["中国GDP", "中国数据", "地产", "China Economy"]),
    ("global_macro",          "全球宏观",         "Global Macro",          "宏观", ["全球经济", "宏观", "Global Macro"]),
    ("currency",              "外汇与美元",       "FX & Dollar",           "宏观", ["美元", "汇率", "日元", "欧元", "人民币汇率", "Dollar", "FX"]),
    ("commodities",           "大宗商品",         "Commodities",           "宏观", ["黄金", "原油", "铜", "商品", "Oil", "Gold", "Commodities"]),
    ("labor_market",          "劳动力市场",       "Labor Market",          "宏观", ["就业", "失业率", "裁员", "工资", "Jobs"]),

    # ── 地缘 & 政策 ──
    ("geopolitics",           "地缘政治",         "Geopolitics",           "地缘", ["地缘", "中美关系", "俄乌", "中东", "台海", "地缘政治风险", "地缘风险", "地缘冲突", "地缘紧张", "Geopolitics"]),
    ("trade_policy",          "贸易与关税",       "Trade & Tariffs",       "地缘", ["关税", "贸易战", "出口管制", "贸易谈判", "Tariffs"]),
    ("regulation",            "监管政策",         "Regulation",            "地缘", ["反垄断", "监管", "制裁", "SEC", "Regulation"]),
    ("us_election",           "美国政治",         "US Politics",           "地缘", ["大选", "特朗普", "Trump", "国会", "白宫", "US Election"]),

    # ── AI & 科技 ──
    ("ai_industry",           "人工智能产业",     "AI Industry",           "科技", ["人工智能", "AI", "AGI", "大模型", "生成式AI", "AI应用", "Artificial Intelligence"]),
    ("ai_compute",            "AI算力",           "AI Compute",            "科技", ["算力", "GPU", "AI基础设施", "数据中心", "英伟达", "H100", "Blackwell", "AI芯片"]),
    ("ai_models",             "AI模型与研发",     "AI Models",             "科技", ["LLM", "开源模型", "模型发布", "GPT", "Claude", "Gemini", "DeepSeek"]),
    ("tech_platforms",        "科技巨头",         "Big Tech",              "科技", ["大型科技公司", "科技股", "FAANG", "Magnificent 7", "M7", "Big Tech"]),
    ("software_saas",         "软件与SaaS",       "Software & SaaS",       "科技", ["SaaS", "云计算", "软件", "企业软件", "Cloud"]),
    ("consumer_electronics",  "消费电子",         "Consumer Electronics",  "科技", ["iPhone", "智能手机", "可穿戴", "苹果产品", "Consumer Electronics"]),

    # ── 半导体 ──
    ("semiconductors",        "半导体",           "Semiconductors",        "科技", ["芯片", "晶圆代工", "台积电", "半导体设备", "先进制程", "Chips", "Semis"]),
    ("chip_supply_chain",     "芯片供应链",       "Chip Supply Chain",     "科技", ["光刻机", "EDA", "封装", "芯片禁令", "Chip Supply"]),

    # ── 新能源 & 传统能源 ──
    ("new_energy",            "新能源",           "New Energy",            "能源", ["电动车", "储能", "锂电池", "光伏", "充电", "EV", "新能源车"]),
    ("energy_transition",     "能源转型",         "Energy Transition",     "能源", ["碳中和", "清洁能源", "净零", "气候", "Clean Energy"]),
    ("oil_gas",               "石油天然气",       "Oil & Gas",             "能源", ["油气", "页岩油", "OPEC", "炼化", "石油", "天然气", "O&G"]),

    # ── 金融 & 市场 ──
    ("crypto",                "加密货币",         "Crypto",                "金融", ["比特币", "以太坊", "数字货币", "区块链", "稳定币", "BTC", "Bitcoin", "Crypto"]),
    ("us_equities",           "美股市场",         "US Equities",           "市场", ["美股", "标普", "纳斯达克", "道琼斯", "美股大盘", "S&P", "Nasdaq"]),
    ("hk_cn_equities",        "港股与A股",        "HK & A-Shares",         "市场", ["港股", "A股", "恒生", "沪深", "港股通"]),
    ("earnings",              "财报季",           "Earnings",              "市场", ["财报", "业绩", "指引", "电话会", "Earnings", "季度业绩"]),
    ("m_a_ipo",               "并购与IPO",        "M&A and IPO",           "市场", ["并购", "收购", "IPO", "上市", "私有化", "M&A"]),
    ("valuation",             "估值与仓位",       "Valuation & Positioning", "市场", ["估值", "泡沫", "仓位", "资金流", "对冲", "Valuation"]),
    ("market_liquidity",      "流动性与资金",     "Market Liquidity",      "市场", ["流动性", "美联储缩表", "回购", "ETF资金流", "Liquidity"]),
    ("banking",               "银行与金融业",     "Banking",               "金融", ["银行", "投行", "信贷", "不良资产", "Banks"]),

    # ── 行业 ──
    ("pharma_biotech",        "医药生物",         "Pharma & Biotech",      "行业", ["医药", "创新药", "生物科技", "FDA", "临床试验", "Biotech", "Pharma"]),
    ("defense_aerospace",     "国防军工",         "Defense & Aerospace",   "行业", ["军工", "国防", "航空航天", "军费", "Defense"]),
    ("real_estate",           "房地产",           "Real Estate",           "行业", ["楼市", "房贷利率", "商业地产", "REIT", "Housing"]),
    ("consumer_brands",       "消费品牌",         "Consumer Brands",       "行业", ["消费", "零售", "奢侈品", "餐饮", "品牌", "Consumer"]),
    ("industrials",           "工业与制造",       "Industrials",           "行业", ["制造业", "工业", "机械", "供应链", "工厂", "Industrials"]),
    ("autonomous_driving",    "自动驾驶与机器人", "Autonomy & Robotics",   "行业", ["自动驾驶", "Robotaxi", "机器人", "人形机器人", "特斯拉FSD", "Robotics"]),
    ("space_satellite",       "商业航天",         "Space",                 "行业", ["航天", "卫星", "火箭", "SpaceX", "星链", "Space"]),
    ("quantum_computing",     "量子计算",         "Quantum Computing",     "行业", ["量子", "量子芯片", "Quantum"]),
    ("biotech_gene",          "基因技术",         "Gene Tech",             "行业", ["基因", "CRISPR", "基因编辑", "基因治疗"]),

    # ── 公司/人物/事件向 (高频 ticker 语境) ──
    ("nvda_ecosystem",        "英伟达产业链",     "Nvidia Ecosystem",      "公司", ["英伟达产业链", "NVDA", "CPO", "光模块", "Nvidia"]),
    ("apple_ecosystem",       "苹果产业链",       "Apple Ecosystem",       "公司", ["苹果", "AAPL", "Apple", "果链"]),
    ("tesla_ecosystem",       "特斯拉产业链",     "Tesla Ecosystem",       "公司", ["特斯拉", "TSLA", "Tesla", "马斯克", "Elon Musk"]),
    ("microsoft_openai",      "微软与OpenAI",     "Microsoft & OpenAI",    "公司", ["微软", "OpenAI", "MSFT", "Azure", "Sam Altman"]),
]


def _connect():
    return psycopg2.connect(**get_psycopg2_config())


def seed_curated(conn) -> List[str]:
    """写入 curated 基线 (幂等). 返回新建的 topic_key 列表."""
    inserted: List[str] = []
    with conn.cursor() as cur:
        for topic_key, name_cn, name_en, category, aliases in CURATED_TOPICS:
            cur.execute(
                """
                INSERT INTO feed_topics (topic_key, name_cn, name_en, aliases, category, source, sort_order)
                VALUES (%s, %s, %s, %s, %s, 'curated', 0)
                ON CONFLICT (topic_key) DO NOTHING
                RETURNING topic_key
                """,
                (topic_key, name_cn, name_en, json.dumps(aliases, ensure_ascii=False), category),
            )
            row = cur.fetchone()
            if row:
                inserted.append(row[0])
    conn.commit()
    return inserted


def load_existing_lookup(conn) -> Dict[str, str]:
    """现有词典 → {归一化键: topic_key} (与聚合器 _load_topic_lookup 同口径)."""
    lookup: Dict[str, str] = {}
    with conn.cursor() as cur:
        cur.execute("SELECT topic_key, name_cn, name_en, aliases FROM feed_topics WHERE is_enabled = true")
        for topic_key, name_cn, name_en, aliases_raw in cur.fetchall():
            keys = [topic_key, name_cn, name_en or ""]
            if aliases_raw:
                keys.extend(a for a in aliases_raw if isinstance(a, str))
            for key in keys:
                normalized = str(key or "").strip().lower()
                if normalized:
                    lookup.setdefault(normalized, topic_key)
    return lookup


def scan_concepts(conn, days: int, top_n: int) -> Counter:
    """近 N 天 subscription_content_tags.concepts 频次统计."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT concept, COUNT(*) AS cnt
            FROM subscription_content_tags sct
            JOIN subscription_content sc ON sc.id = sct.subscription_content_id
            CROSS JOIN LATERAL jsonb_array_elements_text(sct.concepts) AS concept
            WHERE sc.published_at >= NOW() - (%s * INTERVAL '1 day')
              AND btrim(concept) <> ''
            GROUP BY concept
            ORDER BY cnt DESC
            LIMIT %s
            """,
            (days, top_n),
        )
        return Counter({concept: cnt for concept, cnt in cur.fetchall()})


def auto_create_uncovered(conn, counter: Counter, lookup: Dict[str, str], min_count: int) -> List[str]:
    """把未覆盖且频次达标的概念词建成 auto topic (needs_review)."""
    created: List[str] = []
    with conn.cursor() as cur:
        for concept, cnt in counter.most_common():
            if cnt < min_count or len(concept.strip()) < 2:
                continue
            if str(concept).strip().lower() in lookup:
                continue
            cur.execute(
                """
                INSERT INTO feed_topics (topic_key, name_cn, category, source, needs_review)
                VALUES (%s, %s, '未分类', 'auto', true)
                ON CONFLICT (topic_key) DO NOTHING
                RETURNING topic_key
                """,
                (concept, concept),
            )
            row = cur.fetchone()
            if row:
                created.append(row[0])
    conn.commit()
    return created


def main() -> int:
    parser = argparse.ArgumentParser(description="Feeds Dashboard 主题词典 seed")
    parser.add_argument("--scan", action="store_true", help="扫描近 N 天 concepts 频次并打印报告")
    parser.add_argument("--write", action="store_true", help="配合 --scan: 未覆盖高频词自动建 topic")
    parser.add_argument("--days", type=int, default=30, help="扫描窗口天数 (默认 30)")
    parser.add_argument("--top", type=int, default=200, help="报告条数 (默认 200)")
    parser.add_argument("--min-count", type=int, default=20, help="自动建 topic 的频次阈值 (默认 20)")
    args = parser.parse_args()

    conn = _connect()
    try:
        inserted = seed_curated(conn)
        print(f"✅ curated 基线: 新建 {len(inserted)} / 共 {len(CURATED_TOPICS)} 条"
              + (f" ({', '.join(inserted)})" if inserted else ""))

        if not args.scan:
            print("提示: 运行 --scan 查看历史 concepts 频次报告; --scan --write 自动补全高频词")
            return 0

        counter = scan_concepts(conn, args.days, args.top)
        lookup = load_existing_lookup(conn)
        covered = sum(1 for c in counter if str(c).strip().lower() in lookup)
        print(f"\n📊 近 {args.days} 天 concepts 频次 top {len(counter)} (词典已覆盖 {covered} 个):\n")
        print(f"{'频次':>6}  {'状态':<4}  词 → topic_key")
        print("-" * 72)
        for concept, cnt in counter.most_common():
            key = str(concept).strip().lower()
            mapped = lookup.get(key)
            status = f"✓ {mapped}" if mapped else "✗ 未覆盖"
            print(f"{cnt:>6}  {status}")

        if args.write:
            created = auto_create_uncovered(conn, counter, lookup, args.min_count)
            print(f"\n✅ auto 新建 {len(created)} 个 topic (needs_review, 阈值 count>={args.min_count}):")
            for key in created:
                print(f"   + {key}")
        else:
            print(f"\n(如需把未覆盖且频次 >= {args.min_count} 的词自动建 topic, 加 --write)")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
