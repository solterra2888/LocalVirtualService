# -*- coding: utf-8 -*-
"""
Daily Market Report Service

从 finviz_screener 表生成每日市场报告（per-user，按 user_id + report_date 维度）：
- Home 标签页：Bloomberg The Close Summary（全局）
- Stockview 标签页：CNBC Closing Bell Summary（全局）
- My Subscription 标签页：知识星球 / Twitter / YouTube 三份用户每日摘要拼接（per-user）
- Market Overview 标签页：US Assets / US Sector / Thematic ETFs 三个表现表 + 图表（全局）
- 渲染为自包含 HTML，保存到本地文件（reports/daily/{user_id}/）和数据库
"""

import json
import mistune
import os
import time
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4
from zoneinfo import ZoneInfo

from backend.database.db_manager import DatabaseManager
from backend.utils.database_config import get_database_config
from backend.utils.datetime_utils import utcnow, window_label_date
from backend.utils.logger import get_logger

logger = get_logger("daily_market_report_service")

DEFAULT_REPORT_TIMEZONE = "Asia/Shanghai"

# ──────────────────────────────────────────────
# Ticker 分组定义
# ──────────────────────────────────────────────

US_ASSETS_TICKERS = [
    "SHY", "IEF", "TLT", "UUP", "SPY", "QQQ", "IWM", "DIA", "RSP",
    "USO", "BNO", "UNG", "CPER", "GLD", "SLV", "HYG", "LQD",
    # TODO: ^VIX 不在 finviz_screener 中，后续从其他数据源获取
]

US_SECTOR_TICKERS = [
    "XLC", "XLY", "XLP", "XLE", "XLF", "XLV",
    "XLI", "XLB", "XLRE", "XLK", "XLU",
]

THEMATIC_ETF_TICKERS = [
    "ARKK", "ARKQ", "BOTZ", "BUG", "CIBR", "CLOU", "DRIV", "FINX",
    "HACK", "ICLN", "LIT", "ONLN", "PHO", "ROBO", "SKYY",
    "SMH", "SOXX", "SOXQ", "TAN",
]

ALL_TICKERS = US_ASSETS_TICKERS + US_SECTOR_TICKERS + THEMATIC_ETF_TICKERS

US_ASSETS_SET = set(US_ASSETS_TICKERS)
US_SECTOR_SET = set(US_SECTOR_TICKERS)
THEMATIC_ETF_SET = set(THEMATIC_ETF_TICKERS)

# Ticker → 显示名称映射
DISPLAY_NAMES: Dict[str, str] = {
    "SHY": "2Y Treasury", "IEF": "10Y Treasury", "TLT": "30Y Treasury",
    "UUP": "DXY", "SPY": "S&P 500", "QQQ": "Nasdaq 100", "IWM": "Russell 2000",
    "DIA": "Dow Jones 30", "RSP": "S&P 500 Equal Wt",
    "USO": "WTI Crude", "BNO": "Brent Crude", "UNG": "Natural Gas",
    "CPER": "Copper", "GLD": "Gold", "SLV": "Silver",
    "HYG": "HY Credit", "LQD": "IG Credit",
    "XLC": "Communication", "XLY": "Cons Discretionary",
    "XLP": "Cons Staples", "XLE": "Energy", "XLF": "Financials",
    "XLV": "Healthcare", "XLI": "Industrials", "XLB": "Materials",
    "XLRE": "Real Estate", "XLK": "Technology", "XLU": "Utilities",
    "ARKK": "Innovation", "ARKQ": "Autonomous",
    "BOTZ": "AI & Robotics", "BUG": "Cybersecurity", "CIBR": "Cybersecurity",
    "CLOU": "Cloud Computing", "DRIV": "EV & Autonomous", "FINX": "FinTech",
    "HACK": "Cybersecurity", "ICLN": "Clean Energy",
    "LIT": "Lithium & Battery", "ONLN": "E-Commerce", "PHO": "Water",
    "ROBO": "Robotics", "SKYY": "Cloud Computing",
    "SMH": "Semiconductors", "SOXX": "Semiconductors", "SOXQ": "Semiconductors",
    "TAN": "Solar",
}

# ──────────────────────────────────────────────
# ETF 说明注释（US Assets）
# ──────────────────────────────────────────────

US_ASSETS_NOTES: Dict[str, Dict[str, str]] = {
    "SHY": {
        "name": "短期国债",
        "represents": "1-3 年期美国国债走势",
        "index": "iShares 1-3 Year Treasury Bond ETF",
        "description": "跟踪短期国债，对美联储利率决议极为敏感。加息周期中价格承压但收益率上升；降息时价格上涨。波动极低，被视为'现金等价物'。",
    },
    "IEF": {
        "name": "中期国债",
        "represents": "7-10 年期美国国债走势",
        "index": "iShares 7-10 Year Treasury Bond ETF",
        "description": "中期国债代表利率市场的核心定价区间，对美联储政策预期和通胀数据高度敏感。收益率曲线形态变化（陡峭/倒挂）的重要观测标的。",
    },
    "TLT": {
        "name": "长期国债",
        "represents": "20 年以上美国国债走势",
        "index": "iShares 20+ Year Treasury Bond ETF",
        "description": "长期国债对利率变化最敏感，久期最长。在降息周期中涨幅最大，加息周期中跌幅也最深。是观察'市场对长期经济增长和通胀预期'的核心标的。",
    },
    "UUP": {
        "name": "美元指数",
        "represents": "美元相对于一篮子货币的强弱",
        "index": "Invesco DB US Dollar Index Bullish Fund",
        "description": "跟踪美元对欧元、日元、英镑等主要货币的加权汇率。美元走强通常压制大宗商品价格和新兴市场；美元走弱则利好风险资产和商品。受美联储货币政策、全球避险情绪和利差驱动。",
    },
    "SPY": {
        "name": "标普 500",
        "represents": "美国大盘股整体表现",
        "index": "SPDR S&P 500 ETF Trust",
        "description": "跟踪标普 500 指数，覆盖美国约 500 家最大上市公司，约占美股总市值的 80%。是全球规模最大、流动性最好的 ETF，被视为美国股市的基准。科技股权重已超过 30%。",
    },
    "QQQ": {
        "name": "纳斯达克 100",
        "represents": "美国科技与成长股表现",
        "index": "Invesco QQQ Trust (Nasdaq-100)",
        "description": "跟踪纳斯达克 100 指数，涵盖 100 家最大的非金融纳斯达克上市公司。高度集中于科技巨头（Apple、Microsoft、NVIDIA、Meta 等），成长性突出但波动大于 SPY。是观察'科技/AI 投资情绪'的最直接标的。",
    },
    "IWM": {
        "name": "罗素 2000",
        "represents": "美国小盘股整体表现",
        "index": "iShares Russell 2000 ETF",
        "description": "跟踪罗素 2000 指数，覆盖约 2000 家美国小盘股。小盘股对国内经济周期和利率环境更敏感——经济复苏时弹性大，衰退时回撤深。是观察'美国经济内生动能'和'风险偏好'的重要指标。",
    },
    "DIA": {
        "name": "道琼斯 30",
        "represents": "美国蓝筹股整体表现",
        "index": "SPDR Dow Jones Industrial Average ETF",
        "description": "跟踪道琼斯工业平均指数，涵盖 30 家美国最具代表性的蓝筹公司（如 Apple、Microsoft、Goldman Sachs 等）。价格加权指数，工业、金融占比高于 SPY。是历史最悠久的美国股市指标，常被视为'实体经济风向标'。",
    },
    "RSP": {
        "name": "标普 500 等权",
        "represents": "标普 500 等权重表现（大小盘轮动）",
        "index": "Invesco S&P 500 Equal Weight ETF",
        "description": "跟踪标普 500 等权指数，每只成分股权重相同（约 0.2%），消除大盘股集中效应。RSP vs SPY 的相对强弱反映市场宽度——RSP 跑赢意味着广普上涨，SPY 跑赢意味着少数大盘股主导。是观察'市场参与度'和'大小盘轮动'的核心标的。",
    },
    "USO": {
        "name": "WTI 原油",
        "represents": "美国原油价格走势",
        "index": "United States Oil Fund (WTI Crude)",
        "description": "跟踪 WTI 原油期货价格。受 OPEC+产量政策、全球需求预期、美国页岩油产量和地缘政治风险驱动。原油是'工业血液'，其价格反映全球经济增长和通胀预期。",
    },
    "BNO": {
        "name": "布伦特原油",
        "represents": "国际原油基准价格",
        "index": "United States Brent Oil Fund",
        "description": "跟踪布伦特原油期货价格，是全球三分之二原油交易的定价基准。与 WI 原油的价差反映大西洋两岸供需格局差异。布伦特对中东地缘政治和全球海运贸易更敏感。",
    },
    "UNG": {
        "name": "天然气",
        "represents": "美国天然气价格走势",
        "index": "United States Natural Gas Fund",
        "description": "跟踪天然气期货价格。受季节性需求（冬季供暖、夏季制冷）、库存水平、LNG 出口量和页岩气产量影响。波动极大，常因天气预期和库存报告出现剧烈波动。",
    },
    "CPER": {
        "name": "铜",
        "represents": "铜价走势与全球工业需求",
        "index": "Copper ETF (CPER)",
        "description": "跟踪铜期货价格。铜被称为'铜博士'，其价格是全球制造业和基建活动的领先指标。中国需求（占全球 50%+）是最大驱动因素。铜价上涨通常意味着经济扩张预期。",
    },
    "GLD": {
        "name": "黄金",
        "represents": "黄金价格与避险需求",
        "index": "SPDR Gold Shares ETF",
        "description": "跟踪黄金现货价格。黄金是终极避险资产和抗通胀工具。受实际利率（名义利率 - 通胀预期）、美元强弱、央行购金和地缘政治风险驱动。降息周期和危机中表现突出。",
    },
    "SLV": {
        "name": "白银",
        "represents": "白银价格（贵金属+工业双重属性）",
        "index": "iShares Silver Trust ETF",
        "description": "跟踪白银现货价格。白银兼具贵金属（避险/投资）和工业金属（光伏、电子）双重属性，波动率通常高于黄金。金银比（Gold/Silver Ratio）是观察市场情绪的经典指标。",
    },
    "HYG": {
        "name": "高收益信用债",
        "represents": "美国高收益（垃圾）债券市场",
        "index": "iShares iBoxx $ High Yield Corporate Bond ETF",
        "description": "投资于评级低于 BBB- 的企业债券。收益率高但违约风险大，与股市风险偏好高度正相关。利差（HYG 收益率 vs 国债）是衡量市场'恐慌程度'的关键信用指标——利差走阔=风险厌恶上升。",
    },
    "LQD": {
        "name": "投资级信用债",
        "represents": "美国投资级企业债券市场",
        "index": "iShares iBoxx $ Investment Grade Corporate Bond ETF",
        "description": "投资于 BBB 及以上评级的企业债券。信用风险低于 HYG，但利率敏感度更高。对美联储政策和收益率曲线变化敏感，是观察'企业融资成本'和'信用环境松紧'的窗口。",
    },
}

# ──────────────────────────────────────────────
# ETF 说明注释（US Sector）
# ──────────────────────────────────────────────

SECTOR_NOTES: Dict[str, Dict[str, str]] = {
    "XLC": {
        "name": "通信服务",
        "represents": "通信、媒体与互联网娱乐板块的整体表现",
        "index": "S&P 500 Communication Services Select Sector Index",
        "description": "涵盖电信运营商（AT&T、Verizon）、媒体巨头（Disney、Comcast）及互联网平台（Meta、Google、Netflix）。兼具'传统通信防御性'和'互联网成长性'双重属性，对广告支出周期和流媒体订阅增长敏感。",
    },
    "XLY": {
        "name": "可选消费",
        "represents": "消费者信心与可选消费支出景气度",
        "index": "S&P 500 Consumer Discretionary Select Sector Index",
        "description": "涵盖汽车、零售、餐饮、酒店、奢侈品等行业，包含 Amazon、Tesla、Home Depot 等权重股。是衡量消费者信心和经济周期的敏感指标——经济扩张时表现强劲，衰退时率先承压。",
    },
    "XLP": {
        "name": "日常消费",
        "represents": "必需消费品与防御性配置需求",
        "index": "S&P 500 Consumer Staples Select Sector Index",
        "description": "涵盖食品、饮料、家居日用品等必需消费品，典型成分包括 Procter & Gamble、Coca-Cola、PepsiCo。需求刚性、波动低，被视为'防御性板块'，在市场动荡时成为资金避风港。",
    },
    "XLE": {
        "name": "能源",
        "represents": "传统能源板块与油价周期",
        "index": "S&P 500 Energy Select Sector Index",
        "description": "涵盖石油天然气勘探、炼化、设备服务等，主要成分包括 ExxonMobil、Chevron。与原油价格高度正相关，对 OPEC 政策、地缘政治风险和全球需求变化极为敏感，是典型的周期性板块。",
    },
    "XLF": {
        "name": "金融",
        "represents": "金融体系健康度与利率周期",
        "index": "S&P 500 Financial Select Sector Index",
        "description": "涵盖银行、保险、券商、资管等金融机构，包含 Berkshire Hathaway、JPMorgan、Bank of America 等。净息差是核心盈利驱动，对美联储利率政策和收益率曲线形态高度敏感——曲线陡峭利好银行，倒挂时承压。",
    },
    "XLV": {
        "name": "医疗保健",
        "represents": "医疗需求刚性与创新药周期",
        "index": "S&P 500 Health Care Select Sector Index",
        "description": "涵盖制药、生物技术、医疗器械、健康保险等，包含 UnitedHealth、Eli Lilly、Johnson & Johnson。兼具防御性与成长性（GLP-1 药物、基因疗法等创新驱动），受政策监管（药价谈判、FDA 审批）影响显著。",
    },
    "XLI": {
        "name": "工业",
        "represents": "制造业景气与基建投资周期",
        "index": "S&P 500 Industrial Select Sector Index",
        "description": "涵盖航空航天、机械制造、物流运输、建筑工程等，包含 Caterpillar、GE Aerospace、Union Pacific。与制造业 PMI、基建支出和全球供应链紧密相关，对经济扩张/收缩信号高度敏感。",
    },
    "XLB": {
        "name": "材料",
        "represents": "上游原材料与大宗商品周期",
        "index": "S&P 500 Materials Select Sector Index",
        "description": "涵盖化工、金属采矿、特种材料等上游行业，包含 Linde、Sherwin-Williams、Air Products。与大宗商品价格（铜、铝、化工品）高度联动，对全球制造业需求和通胀预期敏感。",
    },
    "XLRE": {
        "name": "房地产",
        "represents": "房地产投资信托（REITs）与利率敏感型资产",
        "index": "S&P 500 Real Estate Select Sector Index",
        "description": "涵盖零售、办公、工业、住宅等各类 REITs。对利率极度敏感——加息推高融资成本、压低估值；降息周期中表现突出。是观察'利率传导至实体经济'的重要窗口。",
    },
    "XLK": {
        "name": "科技",
        "represents": "科技巨头与数字化转型趋势",
        "index": "S&P 500 Technology Select Sector Index",
        "description": "涵盖软件、半导体、硬件、IT 服务等，包含 Apple、Microsoft、NVIDIA 等超高权重股，市值集中度极高。AI 投资、云计算增长和半导体周期是核心驱动。注意：该板块在 S&P 500 中权重已超过 30%，走势几乎决定大盘方向。",
    },
    "XLU": {
        "name": "公用事业",
        "represents": "高股息防御与利率敏感板块",
        "index": "S&P 500 Utilities Select Sector Index",
        "description": "涵盖电力、水务、燃气等公用事业公司。高股息率、低波动、强防御性，常被视为'债券替代品'。对利率变化极为敏感——降息利好（股息吸引力上升），加息承压。近年来因 AI 数据中心电力需求增加，部分公用事业股获得'成长属性'。",
    },
}

# ──────────────────────────────────────────────
# ETF 说明注释（Thematic ETFs）
# ──────────────────────────────────────────────

THEMATIC_NOTES: Dict[str, Dict[str, str]] = {
    "ARKK": {
        "name": "颠覆式创新",
        "represents": "颠覆式创新与高风险高回报主题",
        "index": "ARK 自主研究的颠覆式创新主题指数",
        "description": "主动管理型 ETF，由 Cathie Wood 管理，集中投资于基因编辑、自动驾驶、区块链、AI 等颠覆性技术。高集中度、高波动，牛市中弹性极大，熊市回撤也深。是'科技泡沫'的风向标之一。",
    },
    "ARKQ": {
        "name": "自动驾驶与机器人",
        "represents": "自动驾驶与机器人技术主题",
        "index": "ARK 自主研究的自动化与机器人主题指数",
        "description": "聚焦自动驾驶、工业机器人、3D 打印、储能等前沿领域。是 ARK 系列中相对稳健的一只，波动性低于 ARKK。",
    },
    "BOTZ": {
        "name": "AI 与机器人",
        "represents": "全球 AI 与机器人产业链",
        "index": "Indxx Global Robotics & Artificial Intelligence Thematic Index",
        "description": "覆盖全球范围的机器人与 AI 公司，包括 NVIDIA、Intuitive Surgical、Keyence 等。持仓偏向成熟的工业自动化和医疗机器人，波动相对可控。",
    },
    "BUG": {
        "name": "网络安全",
        "represents": "网络安全行业与数字化防护需求",
        "index": "Indxx Cybersecurity Index",
        "description": "投资于网络安全软件、硬件和服务公司，持仓包括 CrowdStrike、Palo Alto Networks、Zscaler。网络安全需求受数据泄露事件和合规要求驱动，具有'刚需'属性。",
    },
    "CIBR": {
        "name": "网络安全",
        "represents": "网络安全板块（NASDAQ 编制方法）",
        "index": "NASDAQ CTA Cybersecurity Index",
        "description": "与 BUG 同属网络安全主题，使用不同指数编制方法。适合对比 BUG 来观察不同选股规则下的表现差异。",
    },
    "CLOU": {
        "name": "云计算",
        "represents": "云计算基础设施与 SaaS 行业",
        "index": "Indxx Cloud Computing Index",
        "description": "投资于云计算基础设施、平台和 SaaS 服务提供商，包含 Salesforce、ServiceNow、Datadog。与企业 IT 支出向云端迁移的大趋势挂钩。",
    },
    "DRIV": {
        "name": "电动车与自动驾驶",
        "represents": "电动车与自动驾驶出行革命",
        "index": "Indxx Autonomous & Electric Vehicles Index",
        "description": "覆盖电动车整车、电池技术、自动驾驶芯片和系统全产业链，包含 Tesla、BYD、Mobileye。是'出行方式变革'的纯主题标的。",
    },
    "FINX": {
        "name": "金融科技",
        "represents": "金融科技创新与数字化支付",
        "index": "Indxx Global FinTech Thematic Index",
        "description": "投资于数字支付、在线借贷、区块链金融、保险科技等 FinTech 公司，包含 Block、PayPal、Shopify。反映传统金融服务被技术重塑的趋势。",
    },
    "HACK": {
        "name": "网络安全",
        "represents": "网络安全行业（第三方编制）",
        "index": "ITE Indexes Prime Cyber Security Index",
        "description": "与 BUG、CIBR 同属网络安全主题，使用不同的指数供应商和选股方法。可交叉观察以判断板块整体趋势。",
    },
    "ICLN": {
        "name": "清洁能源",
        "represents": "全球清洁能源转型主题",
        "index": "S&P Global Clean Energy Index",
        "description": "投资于全球可再生能源发电（太阳能、风能）、清洁能源设备和相关技术。与碳减排政策、绿色补贴力度正相关。利率上升时（高估值成长股承压）表现较弱。",
    },
    "LIT": {
        "name": "锂电池与储能",
        "represents": "锂电池全产业链与储能主题",
        "index": "Solactive Global Lithium Index",
        "description": "覆盖锂矿开采、电池材料、电池制造和储能系统全链条，包含 Albemarle、SQM 等。电动车销量和储能装机量是核心驱动，锂价波动直接影响 ETF 表现。",
    },
    "ONLN": {
        "name": "电商零售",
        "represents": "电商零售与消费数字化",
        "index": "ProShares Online Retail Index",
        "description": "聚焦纯线上零售商，包括 Amazon、Etsy、Wayfair。是观察'线上渗透率提升'和'消费者购物行为变化'的直接窗口。",
    },
    "PHO": {
        "name": "水资源",
        "represents": "水资源基础设施与水处理主题",
        "index": "NASDAQ OMX US Water Index",
        "description": "投资于水资源收集、净化、输送和废水处理相关公司。水资源是全球稀缺资源主题，具有长期结构性增长逻辑，防御性较强，与经济周期相关性低。",
    },
    "ROBO": {
        "name": "机器人与自动化",
        "represents": "全球工业自动化与机器人产业链",
        "index": "ROBO Global Robotics and Automation Index",
        "description": "覆盖工业自动化、传感技术、系统集成等，持仓偏全球化和多元化。与 BOTZ 的区别在于 ROBO 更偏工业自动化，BOTZ 更偏 AI 与服务机器人。",
    },
    "SKYY": {
        "name": "云计算",
        "represents": "云计算行业（ISE 编制方法）",
        "index": "ISE Cloud Computing Index",
        "description": "与 CLOU 同属云计算主题，使用不同指数编制方法。持仓偏向 SaaS 和云基础设施公司，可用于交叉验证云计算板块走势。",
    },
    "SMH": {
        "name": "半导体",
        "represents": "全球半导体产业链核心",
        "index": "MVIS US Listed Semiconductor 25 Index",
        "description": "高度集中于半导体龙头，NVIDIA、TSMC、AMD 等权重较大。半导体是 AI、云计算、电动车等所有科技主题的'底层基建'，SMH 因此被视为'科技周期最前线'指标。",
    },
    "SOXX": {
        "name": "半导体",
        "represents": "美国半导体龙头",
        "index": "ICE Semiconductor Index",
        "description": "聚焦美国上市半导体公司，包含 NVIDIA、Broadcom、Qualcomm、Texas Instruments。与 SMH 高度相关但成分略有差异，可用于交叉观察半导体板块。",
    },
    "SOXQ": {
        "name": "半导体",
        "represents": "费城半导体指数跟踪",
        "index": "PHLX Semiconductor Sector Index (SOX)",
        "description": "直接跟踪费城半导体指数（SOX）——全球最早、最具影响力的半导体行业基准。与 SMH、SOXX 并列三大半导体 ETF，三者的走势差异可揭示不同选股方法的细微差别。",
    },
    "TAN": {
        "name": "太阳能",
        "represents": "太阳能光伏产业",
        "index": "MAC Global Solar Energy Index",
        "description": "纯太阳能主题，涵盖光伏组件、逆变器、太阳能电站运营等，包含 First Solar、Enphase Energy。对政府补贴政策（IRA 等）、光伏组件价格和全球装机量高度敏感，波动极大，政策驱动特征明显。",
    },
}

# ──────────────────────────────────────────────
# Market Temperature 分组定义（首个 Tab「市场温度感知」）
# ──────────────────────────────────────────────
# 每张卡片: (card_id, 显示名, EODHD code, 副标签)
# 数据全部来自 market_data_eod（asset_type 覆盖 index/forex/commodity/bond）。
# IH/IF/IC/IM 为中金所股指期货，按「期货→标的指数」映射展示
# （SSE50/CSI300/CSI500/CSI1000，见 EODHD_MARKET_DATA_DEVELOPMENT_GUIDE 案例 4）。
# 商品用 ETF 代理（案例 5 选型）；FTSE 100 暂缺（EODHD 不覆盖，备选 ISF.LSE /
# BUK100P.INDX 见指南案例 3）。

# (card_id, 显示名, EODHD code, 副标签, TradingView symbol)
MT_CARD = Tuple[str, str, str, str, str]

MARKET_TEMPERATURE_GROUPS: List[Dict[str, Any]] = [
    {
        "key": "risk",
        "label": "Risk",
        "cards": [
            ("vix", "VIX", "VIX.INDX", "CBOE Volatility Index", "TVC:VIX"),
        ],
    },
    {
        "key": "equity",
        "label": "Equity Indexes",
        "countries": [
            {
                "label": "United States",
                "cards": [
                    ("gspc", "S&P 500", "GSPC.INDX", "", "TVC:SPX"),
                    ("comp", "NASDAQ Composite", "IXIC.INDX", "", "NASDAQ:IXIC"),
                    ("ndx", "NASDAQ 100", "NDX.INDX", "", "NASDAQ:NDX"),
                    ("sox", "PHLX Semiconductor", "SOX.INDX", "", "NASDAQ:SOX"),
                    ("rut", "Russell 2000", "RUT.INDX", "", "TVC:RUT"),
                ],
            },
            {
                "label": "China",
                "cards": [
                    ("ih", "IH · SSE 50", "SSE50.INDX", "", "SSE:000016"),
                    ("if", "IF · CSI 300", "CSI300.INDX", "", "SSE:000300"),
                    ("ic", "IC · CSI 500", "CSI500.INDX", "", "SSE:000905"),
                    ("im", "IM · CSI 1000", "CSI1000.INDX", "", "SSE:000852"),
                ],
            },
            {
                "label": "Others",
                "cards": [
                    ("nikkei", "Nikkei 225", "N225.INDX", "Japan", "TVC:NI225"),
                    ("kospi", "KOSPI", "KS11.INDX", "Korea", "KRX:KOSPI"),
                    ("ftse", "FTSE 100", "FTSE.INDX", "UK", "TVC:UKX"),
                    ("dax", "DAX", "GDAXI.INDX", "Germany", "XETR:DAX"),
                ],
            },
        ],
    },
    {
        "key": "fx",
        "label": "Foreign Exchange",
        "cards": [
            ("jpyusd", "JPY/USD", "JPYUSD.FOREX", "Japanese Yen", "FX_IDC:JPYUSD"),
            ("cnhusd", "CNH/USD", "CNHUSD.FOREX", "Chinese Yuan", "FX_IDC:CNHUSD"),
            ("gbpusd", "GBP/USD", "GBPUSD.FOREX", "British Pound", "OANDA:GBPUSD"),
            ("eurusd", "EUR/USD", "EURUSD.FOREX", "Euro", "OANDA:EURUSD"),
        ],
    },
    {
        "key": "commodities",
        "label": "Commodities",
        "cards": [
            ("gold", "Gold", "GC1.COMMODITY", "COMEX", "COMEX:GC1!"),
            ("wti", "WTI Crude", "CL1.COMMODITY", "NYMEX", "NYMEX:CL1!"),
            ("brent", "Brent Crude", "BZ1.COMMODITY", "NYMEX", "NYMEX:BZ1!"),
            ("silver", "Silver", "SI1.COMMODITY", "COMEX", "COMEX:SI1!"),
            ("copper", "Copper", "HG1.COMMODITY", "COMEX", "COMEX:HG1!"),
            ("natgas", "Natural Gas", "NG1.COMMODITY", "NYMEX", "NYMEX:NG1!"),
        ],
    },
    {
        "key": "treasuries",
        "label": "US Treasuries",
        "cards": [
            ("us2y", "2Y Yield", "US2Y.INDX", "", "TVC:US02Y"),
            ("us5y", "5Y Yield", "US5Y.INDX", "", "TVC:US05Y"),
            ("us10y", "10Y Yield", "US10Y.INDX", "", "TVC:US10Y"),
            ("us30y", "30Y Yield", "US30Y.INDX", "", "TVC:US30Y"),
        ],
    },
]


def _flatten_mt_cards(group: Dict[str, Any]) -> List[MT_CARD]:
    """展平一组的卡片：有 countries 子分组取子分组，否则取顶层 cards。"""
    if "countries" in group:
        return [c for sub in group["countries"] for c in sub["cards"]]
    return list(group.get("cards", []))


MARKET_TEMPERATURE_CARDS: List[MT_CARD] = [
    c for g in MARKET_TEMPERATURE_GROUPS for c in _flatten_mt_cards(g)
]
MARKET_TEMPERATURE_CODES: List[str] = [c[2] for c in MARKET_TEMPERATURE_CARDS]

# 每张卡片内嵌的最大 bar 数（约 1 年交易日）；模板默认展示 3M，可切换 6M/1Y。
MT_MAX_BARS = 260

# ──────────────────────────────────────────────
# Ticker → EODHD code 映射（chart 数据用）
# ──────────────────────────────────────────────

TICKER_EODHD_MAP: Dict[str, str] = {t: f"{t}.US" for t in ALL_TICKERS}

# finviz_screener 查询列
_QUERY_COLUMNS = """
    ticker, price, change_percent, change_from_open, gap,
    performance_week, performance_month, performance_year,
    sma_20_day, sma_50_day, sma_200_day, rsi_14
"""

# Index Performers 配置：(模板 key, 显示名, finviz index_name 中的匹配 token)
# 注意：finviz 的 Index 列是「逗号分隔的多指数归属」字符串，例如 "NDX, S&P 500"，
# 即一只股票可同时属于多个指数。token 用子串匹配：S&P 500 / NDX(=Nasdaq 100) / DJIA(=道指)。
INDEX_PERFORMERS_SPEC = [
    ("sp500", "S&P 500", "S&P 500"),
    ("ndx", "Nasdaq 100", "NDX"),
    ("djia", "Dow Jones", "DJIA"),
]
_INDEX_PERIOD_KEYS = ("day", "week", "year")
_INDEX_MODES = ("return", "weight")

# 报告输出目录
REPORTS_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "reports", "daily"
)


def _parse_pct(val) -> Optional[float]:
    """将 finviz_screener 的百分比 VARCHAR/NUMERIC 解析为 float。"""
    if val is None:
        return None
    if isinstance(val, Decimal):
        return float(val)
    s = str(val).strip().replace("%", "")
    if not s or s == "-":
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _parse_decimal(val) -> Optional[float]:
    """将 NUMERIC 列解析为 float。"""
    if val is None:
        return None
    if isinstance(val, Decimal):
        return float(val)
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _build_row(raw: tuple) -> Dict[str, Any]:
    """将一条 finviz_screener 查询结果转换为模板行数据。"""
    ticker = raw[0]
    return {
        "ticker": ticker,
        "name": DISPLAY_NAMES.get(ticker, ticker),
        "price": _parse_decimal(raw[1]),
        "day": _parse_pct(raw[2]),
        "open": _parse_pct(raw[3]),
        "gap": _parse_pct(raw[4]),
        "week": _parse_pct(raw[5]),
        "month": _parse_pct(raw[6]),
        "y": _parse_pct(raw[7]),
        "sma20": _parse_decimal(raw[8]),
        "sma50": _parse_decimal(raw[9]),
        "sma200": _parse_decimal(raw[10]),
        "rsi": _parse_decimal(raw[11]),
    }


def _compute_index_performers(cursor) -> Dict[str, Any]:
    """
    计算 Index Performers 区段数据：S&P 500 / Nasdaq 100 / Dow Jones 三个指数
    的 Top/Bottom 成分股，按 return（涨跌幅）或 weight contribution
    （权重 × 收益）排名，覆盖 Day/Week/Year 三个周期。数据全部来自 finviz_screener。

    返回结构:
      {
        "indices": [{"key","label"}, ...],
        "panels": [
          {"period","mode","sort_key","cols":[
              {"index_key","index_label","top":[...],"bottom":[...]}, ... 3 个指数
          ]}, ... 6 个 panel (period x mode)
        ]
      }
    每个 performer 行: {ticker, weight, day, week, year,
                        day_contrib, week_contrib, year_contrib, finviz_url}
    无数据时 panels 为空列表，模板会渲染兜底提示。
    """
    finviz_tokens = [s[2] for s in INDEX_PERFORMERS_SPEC]
    # finviz 的 index_name 是逗号分隔的多指数归属串（如 "NDX, S&P 500"），
    # 故用正则子串匹配命中任一目标指数；后续在 Python 里按 token 归类到各指数桶。
    cursor.execute(
        """
        SELECT ticker, index_name, market_cap,
               change_percent, performance_week, performance_year
        FROM finviz_screener
        WHERE index_name ~ %s
          AND market_cap IS NOT NULL
        """,
        ("(" + "|".join(finviz_tokens) + ")",),
    )
    raw = cursor.fetchall()

    # 按 token 子串归类（一只股票可归入多个指数桶），并解析数值
    grouped: Dict[str, List[Dict[str, Any]]] = {token: [] for token in finviz_tokens}
    for r in raw:
        ticker, idx_name, mc, chg, wk, yr = r
        market_cap = _parse_decimal(mc)
        if not market_cap or market_cap <= 0:
            continue
        member = {
            "ticker": ticker,
            "market_cap": market_cap,
            "day": _parse_pct(chg),
            "week": _parse_pct(wk),
            "year": _parse_pct(yr),
        }
        for token in finviz_tokens:
            if idx_name and token in idx_name:
                grouped[token].append(member)

    # 为每个指数计算权重与各周期贡献
    enriched_by_index: Dict[str, List[Dict[str, Any]]] = {}
    for _key, _label, token in INDEX_PERFORMERS_SPEC:
        members = grouped.get(token, [])
        total_mc = sum(m["market_cap"] for m in members) or 1.0
        enriched = []
        for m in members:
            weight = m["market_cap"] / total_mc
            day, week, year = m["day"], m["week"], m["year"]
            enriched.append({
                "ticker": m["ticker"],
                "weight": weight,
                "day": day,
                "week": week,
                "year": year,
                "day_contrib": (weight * day) if day is not None else None,
                "week_contrib": (weight * week) if week is not None else None,
                "year_contrib": (weight * year) if year is not None else None,
                "finviz_url": f"https://elite.finviz.com/quote.ashx?t={m['ticker']}",
            })
        enriched_by_index[token] = enriched

    # 构建 6 个 panel（period x mode）
    panels = []
    for period in _INDEX_PERIOD_KEYS:
        sort_key_return = period                 # return 模式：按该周期收益排名
        sort_key_contrib = f"{period}_contrib"   # weight 模式：按该周期贡献排名
        for mode in _INDEX_MODES:
            sort_key = sort_key_return if mode == "return" else sort_key_contrib
            cols = []
            for key, label, token in INDEX_PERFORMERS_SPEC:
                members = [
                    e for e in enriched_by_index.get(token, [])
                    if e.get(sort_key) is not None
                ]
                # Top: 降序取前 10；Bottom: 升序取前 10（最大跌幅/负贡献）
                top = sorted(members, key=lambda e: e[sort_key], reverse=True)[:10]
                bottom = sorted(members, key=lambda e: e[sort_key])[:10]
                cols.append({
                    "index_key": key,
                    "index_label": label,
                    "top": top,
                    "bottom": bottom,
                })
            panels.append({
                "period": period,
                "mode": mode,
                "sort_key": sort_key,
                "cols": cols,
            })

    return {
        "indices": [{"key": k, "label": l} for k, l, _ in INDEX_PERFORMERS_SPEC],
        "panels": panels,
    }


def _fetch_chart_data_by_code(
    cursor, codes: List[str], min_bars: int = 10
) -> Dict[str, List[Dict[str, Any]]]:
    """
    按 EODHD code 批量查询 OHLCV 日线，返回 {code: [{time,open,high,low,close,volume}]}。
    优先 market_data_eod，bars 不足 min_bars 时 fallback 到 stock_prices_eod（period='d'）。
    """
    result: Dict[str, List[Dict[str, Any]]] = {code: [] for code in codes}

    # 1. 查询 market_data_eod（数据量大，优先）
    cursor.execute(
        """
        SELECT code, date, open, high, low, close, volume
        FROM market_data_eod
        WHERE code = ANY(%s)
        ORDER BY code, date
        """,
        (codes,),
    )
    mde_data: Dict[str, List] = {}
    for row in cursor.fetchall():
        code = row[0]
        mde_data.setdefault(code, []).append({
            "time": row[1].strftime("%Y-%m-%d"),
            "open": float(row[2]) if row[2] else None,
            "high": float(row[3]) if row[3] else None,
            "low": float(row[4]) if row[4] else None,
            "close": float(row[5]) if row[5] else None,
            "volume": int(row[6]) if row[6] else 0,
        })

    # 2. 收集需要 fallback 的 codes
    fallback_codes = [
        code for code in codes if len(mde_data.get(code, [])) < min_bars
    ]
    for code in codes:
        if code not in fallback_codes:
            result[code] = mde_data.get(code, [])

    # 3. Fallback: 查询 stock_prices_eod
    if fallback_codes:
        cursor.execute(
            """
            SELECT code, date, open, high, low, close, volume
            FROM stock_prices_eod
            WHERE code = ANY(%s) AND period = 'd'
            ORDER BY code, date
            """,
            (fallback_codes,),
        )
        spe_data: Dict[str, List] = {}
        for row in cursor.fetchall():
            code = row[0]
            spe_data.setdefault(code, []).append({
                "time": row[1].strftime("%Y-%m-%d"),
                "open": float(row[2]) if row[2] else None,
                "high": float(row[3]) if row[3] else None,
                "low": float(row[4]) if row[4] else None,
                "close": float(row[5]) if row[5] else None,
                "volume": int(row[6]) if row[6] else 0,
            })
        for code in fallback_codes:
            if spe_data.get(code):
                result[code] = spe_data[code]

    # 4. 过滤掉 None 值的 bars，保证 Lightweight Charts 数据完整
    for code in codes:
        result[code] = [
            b for b in result[code]
            if all(b.get(k) is not None for k in ("time", "open", "high", "low", "close"))
        ]

    return result


def _fetch_chart_data(cursor, tickers: List[str]) -> Dict[str, List[Dict[str, Any]]]:
    """
    从 market_data_eod 和 stock_prices_eod 批量查询 OHLCV 日线数据（按 ticker 键返回）。
    优先使用 market_data_eod（~232 bars），不够时 fallback 到 stock_prices_eod。
    """
    by_code = _fetch_chart_data_by_code(
        cursor, [TICKER_EODHD_MAP[t] for t in tickers]
    )
    return {t: by_code[TICKER_EODHD_MAP[t]] for t in tickers}


def _build_temperature_cards(
    by_code: Dict[str, List[Dict[str, Any]]]
) -> Tuple[List[Dict[str, Any]], List[str]]:
    """
    构建 Market Temperature 卡片列表。

    Args:
        by_code: {EODHD code: bars}（来自 _fetch_chart_data_by_code）

    Returns:
        (cards, missing_codes)
        cards: [{id, code, label, sublabel, tv_symbol, last, chg_pct, bars}]，
               bars 截断为最近 MT_MAX_BARS 条；无数据的 code 仍产出卡片
               （bars 为空，模板渲染 "No data" 占位），并计入 missing_codes。
    """
    cards: List[Dict[str, Any]] = []
    missing: List[str] = []
    for card_id, label, code, sublabel, tv_symbol in MARKET_TEMPERATURE_CARDS:
        bars = by_code.get(code, [])[-MT_MAX_BARS:]
        if not bars:
            missing.append(code)
        last = bars[-1]["close"] if bars else None
        prev = bars[-2]["close"] if len(bars) >= 2 else None
        if last is not None and prev:
            chg_pct = (last - prev) / prev * 100.0
        else:
            chg_pct = None
        cards.append({
            "id": card_id,
            "code": code,
            "label": label,
            "sublabel": sublabel,
            "tv_symbol": tv_symbol,
            "last": last,
            "chg_pct": round(chg_pct, 2) if chg_pct is not None else None,
            "bars": bars,
        })
    return cards, missing


class DailyMarketReportService:
    """Daily Market Report 生成服务。"""

    def __init__(self):
        self.db_manager = DatabaseManager(get_database_config())
        self.template_dir = os.path.join(
            os.path.dirname(__file__), "..", "templates"
        )

    # ──────────────────────────────────────────
    # 主入口
    # ──────────────────────────────────────────

    def generate(self, user_id: str, report_date: Optional[date] = None) -> Dict[str, Any]:
        """
        生成 Daily Market Report（per-user），保存到本地和数据库。

        Args:
            user_id: 用户 ID（字符串形式，如 "123"）。
            report_date: 报告日期，默认北京自然日。

        Returns:
            dict with keys: success, report_date, file_path, share_token, metadata
        """
        generated_at_utc = utcnow()
        if report_date is None:
            report_date = window_label_date(generated_at_utc, DEFAULT_REPORT_TIMEZONE)

        conn = self.db_manager.get_connection()
        cursor = conn.cursor()

        # 1. 创建/更新报告记录
        cursor.execute(
            """
            INSERT INTO daily_market_reports
                (user_id, report_date, status, created_at, updated_at)
            VALUES (%s, %s, 'generating', NOW(), NOW())
            ON CONFLICT (user_id, report_date)
            DO UPDATE SET status='generating', updated_at=NOW(), error_message=NULL
            RETURNING id
            """,
            (user_id, report_date),
        )
        report_id = cursor.fetchone()[0]
        conn.commit()

        try:
            start_ts = time.time()

            # 2. 查询 finviz_screener 数据
            cursor.execute(
                f"""
                SELECT {_QUERY_COLUMNS}
                FROM finviz_screener
                WHERE ticker = ANY(%s)
                ORDER BY ticker
                """,
                (ALL_TICKERS,),
            )
            raw_rows = cursor.fetchall()

            if not raw_rows:
                raise ValueError("finviz_screener 表中没有数据，请先刷新 Finviz 数据")

            # 3. 分类和转换
            us_assets, us_sector, thematic = [], [], []
            seen_tickers = set()
            for raw in raw_rows:
                ticker = raw[0]
                if ticker in seen_tickers:
                    continue
                seen_tickers.add(ticker)
                row = _build_row(raw)
                if ticker in US_ASSETS_SET:
                    us_assets.append(row)
                elif ticker in US_SECTOR_SET:
                    us_sector.append(row)
                elif ticker in THEMATIC_ETF_SET:
                    thematic.append(row)

            # 按 US_ASSETS_TICKERS 等定义的顺序排列
            def _sort_by_definition(rows, order):
                order_map = {t: i for i, t in enumerate(order)}
                return sorted(rows, key=lambda r: order_map.get(r["ticker"], 999))

            us_assets = _sort_by_definition(us_assets, US_ASSETS_TICKERS)
            us_sector = _sort_by_definition(us_sector, US_SECTOR_TICKERS)
            thematic = _sort_by_definition(thematic, THEMATIC_ETF_TICKERS)

            # 4. 查询 chart OHLCV 数据
            chart_data = _fetch_chart_data(cursor, ALL_TICKERS)
            chart_tickers_found = sum(1 for v in chart_data.values() if v)
            logger.info(f"Chart data: {chart_tickers_found}/{len(ALL_TICKERS)} tickers have OHLCV data")

            # 4a-0. Market Temperature：按 EODHD code 查询全球资产日线并构建卡片
            mt_data = _fetch_chart_data_by_code(cursor, MARKET_TEMPERATURE_CODES)
            temperature_cards, temperature_missing = _build_temperature_cards(mt_data)
            logger.info(
                f"Market Temperature: "
                f"{len(temperature_cards) - len(temperature_missing)}/"
                f"{len(temperature_cards)} cards have data"
            )

            # 4a. 计算 Index Performers（S&P 500 / Nasdaq 100 / Dow Jones Top/Bottom）
            index_performers = _compute_index_performers(cursor)
            _ip_found = len(index_performers["panels"][0]["cols"][0]["top"]) if index_performers["panels"] else 0
            logger.info(f"Index Performers: S&P 500 day/return top={_ip_found}")

            # 4b. 查询 Bloomberg The Close Summary
            cursor.execute(
                """
                SELECT summary_content
                FROM the_close_summaries
                WHERE report_date = %s AND status = 'completed'
                """,
                (report_date,),
            )
            tc_row = cursor.fetchone()
            the_close_html = self._render_markdown_to_html(tc_row[0]) if tc_row else ""

            # 4c. 查询 CNBC Closing Bell Summary
            cursor.execute(
                """
                SELECT summary_content
                FROM cnbc_closing_bell_summaries
                WHERE report_date = %s AND status = 'completed'
                """,
                (report_date,),
            )
            cb_row = cursor.fetchone()
            closing_bell_html = self._render_markdown_to_html(cb_row[0]) if cb_row else ""

            # 4d. 查询 My Subscription 摘要（知识星球 / Twitter / YouTube）
            #     这三个表 user_id 是 INTEGER，将 VARCHAR user_id 转 int；
            #     非数字 user_id（如 'system'）跳过订阅查询，显示空状态。
            try:
                user_id_int = int(user_id)
            except (ValueError, TypeError):
                user_id_int = None

            subscription_markdowns = []
            if user_id_int is not None:
                # 知识星球
                cursor.execute(
                    """
                    SELECT report_content
                    FROM zhishixingqiu_user_reports
                    WHERE user_id = %s AND report_date = %s AND status = 'completed'
                    """,
                    (user_id_int, report_date),
                )
                zsxq_row = cursor.fetchone()
                if zsxq_row and zsxq_row[0]:
                    subscription_markdowns.append(zsxq_row[0])

                # Twitter
                cursor.execute(
                    """
                    SELECT report_content
                    FROM twitter_daily_reports
                    WHERE user_id = %s AND report_date = %s AND status = 'completed'
                    """,
                    (user_id_int, report_date),
                )
                twitter_row = cursor.fetchone()
                if twitter_row and twitter_row[0]:
                    subscription_markdowns.append(twitter_row[0])

                # YouTube
                cursor.execute(
                    """
                    SELECT report_content
                    FROM youtube_daily_reports
                    WHERE user_id = %s AND report_date = %s AND status = 'completed'
                    """,
                    (user_id_int, report_date),
                )
                youtube_row = cursor.fetchone()
                if youtube_row and youtube_row[0]:
                    subscription_markdowns.append(youtube_row[0])

                # Substack
                cursor.execute(
                    """
                    SELECT report_content
                    FROM substack_user_reports
                    WHERE user_id = %s AND report_date = %s AND status = 'completed'
                    """,
                    (user_id_int, report_date),
                )
                substack_row = cursor.fetchone()
                if substack_row and substack_row[0]:
                    subscription_markdowns.append(substack_row[0])
            else:
                zsxq_row = twitter_row = youtube_row = substack_row = None

            # 拼接三份 Markdown，用分隔线隔开，渲染一次
            if subscription_markdowns:
                combined_markdown = "\n\n---\n\n".join(subscription_markdowns)
                my_subscriptions_html = self._render_markdown_to_html(combined_markdown)
            else:
                my_subscriptions_html = ""

            logger.info(
                f"Summaries: the_close={'✓' if the_close_html else '✗'}, "
                f"closing_bell={'✓' if closing_bell_html else '✗'}, "
                f"subscriptions(zsxq/tw/yt/ss)="
                f"{'✓' if zsxq_row and zsxq_row[0] else '✗'}/"
                f"{'✓' if twitter_row and twitter_row[0] else '✗'}/"
                f"{'✓' if youtube_row and youtube_row[0] else '✗'}/"
                f"{'✓' if substack_row and substack_row[0] else '✗'}"
            )

            # 5. 渲染 HTML
            gen_at_ny = generated_at_utc.astimezone(ZoneInfo("America/New_York"))
            gen_at_bj = generated_at_utc.astimezone(ZoneInfo("Asia/Shanghai"))

            html = self._render_html(
                report_date=report_date,
                generated_at_utc=generated_at_utc.strftime("%Y-%m-%d %H:%M UTC"),
                generated_at_new_york=gen_at_ny.strftime("%b %d, %Y %H:%M ET"),
                generated_at_beijing=gen_at_bj.strftime("%b %d, %Y %H:%M Beijing"),
                us_assets=us_assets,
                us_sector=us_sector,
                thematic=thematic,
                chart_data=chart_data,
                index_performers=index_performers,
                the_close_html=the_close_html,
                closing_bell_html=closing_bell_html,
                my_subscriptions_html=my_subscriptions_html,
                temperature_cards=temperature_cards,
            )

            # 6. 保存到本地文件
            file_path = self._save_to_local_file(html, report_date, user_id)

            # 7. 统计元数据
            elapsed = round(time.time() - start_ts, 1)
            metadata = {
                "generation_time_seconds": elapsed,
                "us_assets_count": len(us_assets),
                "us_sector_count": len(us_sector),
                "thematic_etfs_count": len(thematic),
                "total_tickers_found": len(seen_tickers),
                "missing_tickers": sorted(set(ALL_TICKERS) - seen_tickers),
                "chart_tickers_with_data": chart_tickers_found,
                "generated_at_utc": generated_at_utc.isoformat(),
                "has_the_close_summary": bool(the_close_html),
                "has_closing_bell_summary": bool(closing_bell_html),
                "has_my_subscriptions": bool(my_subscriptions_html),
                "temperature_cards_found": len(temperature_cards) - len(temperature_missing),
                "temperature_missing_codes": temperature_missing,
            }

            share_token = uuid4().hex

            # 8. 更新数据库记录
            cursor.execute(
                """
                UPDATE daily_market_reports
                SET status='completed',
                    file_path=%s,
                    html_content=%s,
                    metadata=%s,
                    share_token=%s,
                    updated_at=NOW()
                WHERE id=%s
                """,
                (file_path, html, json.dumps(metadata), share_token, report_id),
            )
            conn.commit()

            logger.info(
                f"Daily Market Report generated for {report_date}: "
                f"{len(us_assets)} assets, {len(us_sector)} sectors, "
                f"{len(thematic)} thematic, elapsed={elapsed}s"
            )

            return {
                "success": True,
                "report_date": str(report_date),
                "file_path": file_path,
                "share_token": share_token,
                "metadata": metadata,
            }

        except Exception as exc:
            conn.rollback()
            cursor.execute(
                """
                UPDATE daily_market_reports
                SET status='failed', error_message=%s, updated_at=NOW()
                WHERE id=%s
                """,
                (str(exc), report_id),
            )
            conn.commit()
            logger.error(f"Daily Market Report generation failed: {exc}", exc_info=True)
            return {
                "success": False,
                "report_date": str(report_date),
                "error": str(exc),
            }
        finally:
            conn.close()

    # ──────────────────────────────────────────
    # HTML 渲染
    # ──────────────────────────────────────────

    def _render_html(
        self,
        report_date: date,
        generated_at_utc: str,
        generated_at_new_york: str,
        generated_at_beijing: str,
        us_assets: List[Dict],
        us_sector: List[Dict],
        thematic: List[Dict],
        chart_data: Dict[str, List[Dict[str, Any]]],
        index_performers: Dict[str, Any],
        the_close_html: str = "",
        closing_bell_html: str = "",
        my_subscriptions_html: str = "",
        temperature_cards: Optional[List[Dict[str, Any]]] = None,
    ) -> str:
        """通过 Jinja2 渲染自包含 HTML。"""
        from jinja2 import Environment, FileSystemLoader, select_autoescape
        from markupsafe import Markup

        env = Environment(
            loader=FileSystemLoader(os.path.abspath(self.template_dir)),
            autoescape=select_autoescape(["html"]),
        )

        # Custom Jinja2 filters — return Markup to prevent double-escaping
        def _fmt_pct(v):
            if v is None:
                return Markup('<span class="val-na">N/A</span>')
            sign = "+" if v >= 0 else ""
            cls = "val-pos" if v > 0 else ("val-neg" if v < 0 else "val-neutral")
            return Markup(f'<span class="{cls}">{sign}{v:.2f}%</span>')

        def _fmt_pct3(v):
            # 3 位小数版本，用于 Index Performers 的 contribution 列（量级常 < 0.1%）
            if v is None:
                return Markup('<span class="val-na">N/A</span>')
            sign = "+" if v >= 0 else ""
            cls = "val-pos" if v > 0 else ("val-neg" if v < 0 else "val-neutral")
            return Markup(f'<span class="{cls}">{sign}{v:.3f}%</span>')

        def _fmt_price(v):
            if v is None:
                return Markup('<span class="val-na">N/A</span>')
            return f"{v:.2f}"

        def _fmt_rsi(v):
            if v is None:
                return Markup('<span class="val-na">N/A</span>')
            return f"{v:.1f}"

        env.filters["fmt_pct"] = _fmt_pct
        env.filters["fmt_pct3"] = _fmt_pct3
        env.filters["fmt_price"] = _fmt_price
        env.filters["fmt_rsi"] = _fmt_rsi

        template = env.get_template("daily_market_report.html.j2")

        html = template.render(
            report_date=report_date.strftime("%B %d, %Y"),
            generated_at_utc=generated_at_utc,
            generated_at_new_york=generated_at_new_york,
            generated_at_beijing=generated_at_beijing,
            us_assets=us_assets,
            us_sector=us_sector,
            thematic=thematic,
            sector_notes=SECTOR_NOTES,
            thematic_notes=THEMATIC_NOTES,
            us_assets_notes=US_ASSETS_NOTES,
            chart_data_json=json.dumps(chart_data),
            display_names_json=json.dumps(DISPLAY_NAMES),
            index_performers=index_performers,
            the_close_html=the_close_html,
            closing_bell_html=closing_bell_html,
            my_subscriptions_html=my_subscriptions_html,
            temperature_groups=MARKET_TEMPERATURE_GROUPS,
            temperature_cards=temperature_cards or [],
            temperature_json=json.dumps(temperature_cards or []),
        )
        return html

    @staticmethod
    def _render_markdown_to_html(md_text: str) -> str:
        """将 Markdown 转换为 HTML，用于嵌入报告模板。"""
        if not md_text:
            return ""
        import mistune.plugins.table as table_plugin

        renderer = mistune.HTMLRenderer()
        markdown_parser = mistune.Markdown(renderer, plugins=[table_plugin.table])
        return markdown_parser(md_text)

    # ──────────────────────────────────────────
    # 本地文件保存
    # ──────────────────────────────────────────

    def _save_to_local_file(self, html: str, report_date: date, user_id: str) -> str:
        """保存 HTML 到 reports/daily/{user_id}/YYYYMMDD.html。"""
        user_dir = os.path.join(REPORTS_DIR, user_id)
        os.makedirs(user_dir, exist_ok=True)
        filename = f"{report_date.strftime('%Y%m%d')}.html"
        file_path = os.path.join(user_dir, filename)
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(html)
        logger.info(f"Report saved to {file_path}")
        return file_path

    # ──────────────────────────────────────────
    # 用户枚举（fan-out 用）
    # ──────────────────────────────────────────

    def get_eligible_users(self) -> List[str]:
        """返回需要生成 Daily Market Report 的全部用户 ID（字符串形式）。

        Home/Stockview/Market Overview 是通用市场内容，所有用户都可查看；
        My Subscription 按用户填充（无订阅则显示空状态）。因此 fan-out 覆盖
        users 表所有用户。
        """
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT id FROM users ORDER BY id")
            return [str(row[0]) for row in cursor.fetchall()]
        finally:
            conn.close()

    # ──────────────────────────────────────────
    # 查询接口
    # ──────────────────────────────────────────

    def get_user_reports(
        self, user_id, limit: int = 30, offset: int = 0
    ) -> Tuple[List[Dict[str, Any]], int]:
        """分页获取用户市场日报列表（供 Agent API 使用）。

        每条记录不含 html_content 正文 —— 正文通过 share_url 访问。
        user_id 内部归一化为 str（表列为 VARCHAR）。
        """
        user_id = str(user_id)
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                "SELECT COUNT(*) FROM daily_market_reports WHERE user_id = %s",
                (user_id,),
            )
            total = cursor.fetchone()[0]

            cursor.execute(
                """
                SELECT id, report_date, status, metadata, created_at, share_token
                FROM daily_market_reports
                WHERE user_id = %s
                ORDER BY report_date DESC
                LIMIT %s OFFSET %s
                """,
                (user_id, limit, offset),
            )
            columns = [
                "id", "report_date", "status", "metadata", "created_at", "share_token"
            ]
            reports = []
            for row in cursor.fetchall():
                d = dict(zip(columns, row))
                if d.get("report_date"):
                    d["report_date"] = str(d["report_date"])
                if d.get("created_at"):
                    d["created_at"] = d["created_at"].isoformat()
                reports.append(d)
            return reports, total
        finally:
            conn.close()

    def get_report_by_date(self, user_id, report_date) -> Optional[Dict[str, Any]]:
        """获取指定日期的已完成市场日报（供 Agent API 使用）。"""
        user_id = str(user_id)
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT id, report_date, status, metadata, created_at, share_token
                FROM daily_market_reports
                WHERE user_id = %s AND report_date = %s AND status = 'completed'
                """,
                (user_id, report_date),
            )
            row = cursor.fetchone()
            if not row:
                return None
            columns = [
                "id", "report_date", "status", "metadata", "created_at", "share_token"
            ]
            d = dict(zip(columns, row))
            if d.get("report_date"):
                d["report_date"] = str(d["report_date"])
            if d.get("created_at"):
                d["created_at"] = d["created_at"].isoformat()
            return d
        finally:
            conn.close()

    def get_latest_report(self, user_id: str) -> Optional[Dict[str, Any]]:
        """返回指定用户最新已完成的报告。"""
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT id, report_date, file_path, metadata, status,
                       error_message, created_at, share_token
                FROM daily_market_reports
                WHERE user_id = %s AND status = 'completed'
                ORDER BY report_date DESC
                LIMIT 1
                """,
                (user_id,),
            )
            row = cursor.fetchone()
            if not row:
                return None
            return self._row_to_dict(row)
        finally:
            conn.close()

    def list_reports(self, user_id: str, limit: int = 20) -> List[Dict[str, Any]]:
        """返回指定用户最近的报告列表。"""
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT id, report_date, file_path, metadata, status,
                       error_message, created_at, share_token
                FROM daily_market_reports
                WHERE user_id = %s
                ORDER BY report_date DESC
                LIMIT %s
                """,
                (user_id, limit),
            )
            return [self._row_to_dict(row) for row in cursor.fetchall()]
        finally:
            conn.close()

    def get_report_html_by_token(self, token: str) -> Optional[bytes]:
        """通过 share_token 获取报告 HTML。"""
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT html_content FROM daily_market_reports
                WHERE share_token = %s AND status = 'completed'
                """,
                (token,),
            )
            row = cursor.fetchone()
            if not row or not row[0]:
                return None
            return row[0].encode("utf-8")
        finally:
            conn.close()

    @staticmethod
    def _row_to_dict(row: tuple) -> Dict[str, Any]:
        rid, report_date, file_path, metadata, status, error_message, created_at, share_token = row
        return {
            "id": rid,
            "report_date": str(report_date),
            "file_path": file_path,
            "metadata": metadata or {},
            "status": status,
            "error_message": error_message,
            "created_at": created_at.isoformat() if created_at else None,
            "share_token": share_token,
        }
