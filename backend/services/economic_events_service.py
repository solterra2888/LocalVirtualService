#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
经济事件数据服务

基于EODHD API的经济事件数据获取和管理服务
支持从数据库读取已存储的经济事件数据
"""

import requests
import json
import os
import re
import time
from datetime import datetime, timedelta
from backend.utils.datetime_utils import utcnow
from backend.utils.eodhd_quota import EODHDQuotaExhausted, raise_if_quota_exhausted, trip_quota_breaker
from typing import List, Dict, Any, Optional, Union
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from backend.utils.logger import info, error, warning, get_logger
# 统一的环境变量加载
from backend.utils.env_loader import load_project_env
from backend.database.db_manager import DatabaseManager
from config.settings import config
import psycopg2.extras

# 事件类别关键词规则（对齐 Trading Economics Calendar 的类别划分，用于 Category 筛选）。
# 顺序即优先级：先匹配先命中。事件名统一小写做子串匹配；带 \b 的用正则保证词边界。
# 覆盖不了的事件归入 'other'。
EVENT_CATEGORY_RULES: list = [
    ('holiday', [
        'christmas', 'new year', 'good friday', 'easter', 'holiday',
        'thanksgiving', 'boxing day', 'independence day', 'national day',
        'memorial day', 'victory day', 'labour day', 'labor day',
        'may day', 'spring festival', 'mid-autumn', 'dragon boat',
        'qingming', 'national holiday', 'presidents day', "president's day",
        'martin luther', 'juneteenth', 'columbus day', 'veterans day',
        'washington\u2019s birthday', 'epiphany', 'assumption', 'all saints',
        'day of', 'foundation day', 'liberation day', 'republic day',
        'constitution day', 'harvest festival', 'chinese new year',
    ]),
    ('interest-rate', [
        'interest rate', 'rate decision', 'deposit facility', 'lending facility',
        'marginal lending', 'reverse repo', 'repo rate', 'prime rate',
        'loan prime rate', 'overdraft rate', 'cash rate', 'policy rate',
        'discount rate', 'base rate', 'refinancing rate', 'standing lending',
        'standing deposit', 'monetary policy', 'key rate', 'ceil rate',
        'corridor rate', 'bank rate',
    ]),
    ('central-bank', [
        'speech', 'testimony', 'minutes', 'press conference', 'beige book',
        'fomc', 'summary of opinions', 'chart pack', 'bulletin',
        'financial stability', 'economic bulletin', 'economic projections',
        'deliberations', 'monetary policy report', 'meeting', 'decision',
        'statement', 'projections', 'hearings', 'q&a', 'lecture',
        'balance sheet', 'focus market readout', 'focus survey',
    ]),
    ('inflation', [
        'cpi', 'inflation', 'ppi', 'pce', 'wpi', 'hicp', 'producer price',
        'consumer price', 'import price', 'export price', 'core prices',
        'deflator', 'wholesale price', 'selling price', 'price index',
        'prices m/m', 'pipc', 'price pressure',
    ]),
    ('labour', [
        'employment', 'unemploy', 'jobless', 'payroll', 'jolts', 'labour',
        'labor', 'wage', 'hourly earnings', 'participation rate', 'claims',
        'vacanc', 'hiring', 'redundanc', 'labour force', 'workforce',
    ]),
    ('gdp', ['gdp', 'gross domestic']),
    ('trade', [
        'trade balance', 'balance of trade', 'current account',
        'foreign trade', 'goods trade', 'export', 'import',
        'tourism arrival', 'tourist arrivals', 'trade:',
    ]),
    ('government', [
        'budget', 'fiscal', 'tax revenue', 'government', 'treasury international capital',
        'net lending', 'public debt', 'government debt', 'consolidated debt',
        'election',
    ]),
    ('housing', [
        'housing', 'home sales', 'home price', 'house price', 'building permit',
        'construction permit', 'mortgage', 'property price', 'new home',
        'existing home', 'construction output', 'land price', 'appraisals',
        'dwelling', 'residential', 'mba',
    ]),
    ('consumer', [
        'consumer confidence', 'consumer sentiment', 'consumer credit',
        'retail sales', 'personal spending', 'personal income',
        'personal consumption', 'household spending', 'household consumption',
        'consumer spending', 'redbook', 'consumer', 'retail', 'motor vehicle sales',
        'car sales', 'new car', 'vehicle sales', 'ipsos', 'pcsi',
    ]),
    ('business', [
        'pmi', 'ism', 'business confidence', 'business climate',
        'industrial production', 'manufacturing', 'industrial',
        'capacity utilization', 'factory orders', 'durable goods',
        'machinery orders', 'business survey', 'economic sentiment',
        'economic confidence', 'zew', 'ifo', 'tankan', 'construction pmi',
        'industrial confidence', 'production', 'business activity',
        'new orders', 'business conditions', 'composite output',
        'confidence index', 'confidence barometer', 'industry confidence',
        'leading index', 'coincident index', 'business inventories',
        'economic activity', 'wholesale',
    ]),
    ('bonds', ['auction']),
    ('energy', [
        'eia', 'api crude', 'baker hughes', 'rig count', 'natural gas',
        'crude oil', 'gasoline', 'distillate', 'heating oil', 'refinery',
        'oil market report', 'opec', 'wasde', 'nopa', 'grain stocks',
        'corn', 'soybean', 'wheat', 'cushing', 'fuel stocks', 'drilling',
    ]),
    ('money', [
        'money supply', 'm1 ', 'm2 ', 'm3 ', 'reserves', 'foreign exchange',
        'foreign bond investment', 'foreign securities', 'net capital flows',
        'capital flows', 'tic flows', ' liquidity', 'loan growth',
        'bank lending', 'total loans', 'credit to', 'private sector credit',
        'stock investment', 'currency swaps',
    ]),
]

# 分类结果缓存（事件名数量 ~1000 量级，进程内缓存足够）
_EVENT_CATEGORY_CACHE: dict = {}


def classify_event_category(event_name: str) -> str:
    """
    根据事件名称归类到日历类别（对齐 Trading Economics 的 Category 筛选）

    Args:
        event_name: 事件名称

    Returns:
        类别 slug（interest-rate / central-bank / inflation / labour / gdp /
        trade / government / housing / consumer / business / bonds / energy /
        money / other）
    """
    if not event_name:
        return 'other'

    cached = _EVENT_CATEGORY_CACHE.get(event_name)
    if cached is not None:
        return cached

    name = event_name.lower()
    for category, keywords in EVENT_CATEGORY_RULES:
        for kw in keywords:
            if kw in name:
                _EVENT_CATEGORY_CACHE[event_name] = category
                return category

    _EVENT_CATEGORY_CACHE[event_name] = 'other'
    return 'other'


# 前端展示用的类别定义（slug → 显示名），供 API 元数据返回
EVENT_CATEGORIES: list = [
    {'slug': 'interest-rate', 'name': 'Interest Rate'},
    {'slug': 'central-bank', 'name': 'Central Bank'},
    {'slug': 'inflation', 'name': 'Inflation'},
    {'slug': 'labour', 'name': 'Labour Market'},
    {'slug': 'gdp', 'name': 'GDP Growth'},
    {'slug': 'trade', 'name': 'Foreign Trade'},
    {'slug': 'government', 'name': 'Government'},
    {'slug': 'housing', 'name': 'Housing Market'},
    {'slug': 'consumer', 'name': 'Consumer'},
    {'slug': 'business', 'name': 'Business & Industry'},
    {'slug': 'bonds', 'name': 'Bond Auctions'},
    {'slug': 'energy', 'name': 'Energy & Commodities'},
    {'slug': 'money', 'name': 'Money & Credit'},
    {'slug': 'holiday', 'name': 'Holidays'},
    {'slug': 'other', 'name': 'Other'},
]


def _scrub_token(err: Any) -> str:
    """异常信息里 requests 会带完整 URL(含 api_token=...),日志/抛出前脱敏。"""
    return re.sub(r'(api_token=)[^&\s\)\'"]+', r'\1***', str(err))


def calculate_event_impact(event_name: str) -> int:
    """
    根据事件名称计算重要性等级
    
    Args:
        event_name: 事件名称
    
    Returns:
        重要性等级 (1-3星)
    """
    if not event_name:
        return 1
    
    # 3星 - 最高重要性事件
    high_impact_events = [
        # 央行利率决策
        'Fed Interest Rate Decision', 'BoE Interest Rate Decision', 'ECB Interest Rate Decision',
        'BoJ Interest Rate Decision', 'RBA Interest Rate Decision', 'BoC Interest Rate Decision',
        'SNB Interest Rate Decision', 'RBNZ Interest Rate Decision', 'CNB Interest Rate Decision',
        'Norges Bank Interest Rate Decision', 'Riksbank Rate Decision', 'TCMB Interest Rate Decision',
        'RBI Interest Rate Decision', 'SARB Interest Rate Decision', 'BCB Interest Rate Decision',
        
        # 关键经济指标
        'Non-Farm Payrolls', 'Non Farm Payrolls', 'GDP Growth Rate', 'Consumer Price Index',
        'Inflation Rate', 'Unemployment Rate', 'Interest Rate',
        
        # 重要央行会议和报告
        'FOMC Minutes', 'FOMC Economic Projections', 'BoE Monetary Policy Report',
        'ECB Monetary Policy Statement and Press Conference', 'BoJ Monetary Policy Meeting Minutes',
        'RBA Monetary Policy Statement', 'Norges Bank Monetary Policy Report',
        
        # 重要经济数据
        'GDP Growth Annualized', 'GDP Growth Mainland', 'GDP 3-Month Avg',
        'Core CPI', 'Core PCE Price Index', 'Core PCE Prices',
        
        # 重要就业数据
        'Employment Change', 'Unemployment Change', 'Initial Jobless Claims',
        'JOLTs Job Openings', 'Average Hourly Earnings', 'Employment Cost Index',
        
        # 重要通胀数据
        'Harmonised Inflation Rate', 'HICP', 'Core Inflation Rate',
        'Inflation Rate Ex-Food and Energy', 'Mid-month Inflation Rate',
        
        # 重要制造业数据
        'ISM Manufacturing PMI', 'ISM Services PMI', 'ISM Non-Manufacturing PMI',
        'S&P Global Manufacturing PMI', 'S&P Global Services PMI', 'S&P Global Composite PMI',
        'HCOB Manufacturing PMI', 'HCOB Services PMI', 'HCOB Composite PMI',
        
        # 重要消费者数据
        'Michigan Consumer Sentiment', 'CB Consumer Confidence', 'Consumer Confidence',
        'Retail Sales', 'Personal Spending', 'Personal Income',
        
        # 重要贸易数据
        'Trade Balance', 'Balance of Trade', 'Current Account',
        'Exports', 'Imports', 'Goods Trade Balance',
        
        # 重要房价数据
        'Existing Home Sales', 'New Home Sales', 'Housing Starts',
        'Building Permits', 'House Price Index', 'S&P/Case-Shiller Home Price',
        
        # 重要能源数据
        'EIA Crude Oil Stocks Change', 'EIA Crude Oil Imports Change',
        'Baker Hughes Oil Rig Count', 'OPEC Meeting', 'OPEC Monthly Report',
        
        # 重要农产品数据
        'WASDE Report', 'NOPA Crush Report', 'Quarterly Grain Stocks - Corn',
        'Quarterly Grain Stocks - Soy', 'Quarterly Grain Stocks - Wheat',
        
        # 重要货币数据
        'M2 Money Supply', 'M1 Money Supply', 'Money Supply',
        'Foreign Exchange Reserves', 'Central Bank Reserves $',
        
        # 重要债券拍卖
        '10-Year Bond Auction', '30-Year Bond Auction', '2-Year Bond Auction',
        '10-Year Treasury Gilt Auction', '30-Year Treasury Gilt Auction',
        '10-Year Bund Auction', '30-Year Bund Auction', '10-Year BTP Auction',
        '30-Year BTP Auction', '10-Year Note Auction', '30-Year TIPS Auction',
        
        # 重要央行官员讲话
        'Fed Chair Powell Speech', 'Fed Chair Powell Testimony', 'BoE Gov Bailey Speech',
        'ECB President Lagarde Speech', 'BoJ Gov Ueda Speech', 'RBA Gov Bullock Speaks',
        'SNB Press Conference', 'BoC Press Conference', 'RBA Press Conference',
        
        # 重要经济报告
        'Beige Book', 'Fed Beige Book', 'Financial Stability Report',
        'BoE Financial Stability Report', 'ECB Financial Stability Review',
        'RBNZ Financial Stability Report', 'SARB Monetary Policy Review',
        
        # 重要会议
        'BRICS Summit', 'IMF Meeting', 'Euro Summit', 'Eurozone Leaders Meeting',
        'EcoFin Meeting', 'Eurogroup Meeting'
    ]
    
    # 2星 - 中等重要性事件
    medium_impact_events = [
        # 次要央行决策
        'Deposit Facility Rate', 'Lending Facility Rate', 'Marginal Lending Rate',
        'Reverse Repo Rate', 'Prime Rate', 'Prime Overdraft Rate',
        
        # 次要经济指标
        'Core PPI', 'PPI Output', 'PPI Input', 'Producer Price Index',
        'Wholesale Prices', 'Import Prices', 'Export Prices',
        
        # 次要就业数据
        'Continuing Jobless Claims', 'Jobless Claims 4-Week Average',
        'Full Time Employment Chg', 'Part Time Employment Chg',
        'Employment Level', 'Employment Growth', 'Employment Wages',
        'Employment Benefits', 'Employment Cost - Wages', 'Employment Cost - Benefits',
        
        # 次要制造业数据
        'Manufacturing PMI', 'Manufacturing Confidence', 'Manufacturing Output',
        'Manufacturing Production', 'Manufacturing Sales', 'Manufacturing Payrolls',
        'Industrial Production', 'Industrial Sales', 'Industrial Inventories',
        'Industrial Confidence', 'Industrial Sentiment',
        
        # 次要服务业数据
        'Services PMI', 'Services Sentiment', 'Services NZ PSI',
        'ISM Services Business Activity', 'ISM Services Employment', 'ISM Services New Orders',
        'ISM Services Prices', 'ISM Non-Manufacturing Business Activity',
        'ISM Non-Manufacturing Employment', 'ISM Non-Manufacturing New Orders',
        'ISM Non-Manufacturing Prices',
        
        # 次要消费者数据
        'Consumer Credit Change', 'Consumer Spending', 'Household Spending',
        'Household Consumption', 'Retail Sales Ex Autos', 'Retail Sales Ex Fuel',
        'Retail Sales Ex Gas/Autos', 'Retail Inventories Ex Autos',
        'Personal Income', 'Real Consumer Spending', 'Real Earnings',
        
        # 次要通胀数据
        'Food Inflation', 'WPI Food', 'WPI Food Index', 'WPI Fuel',
        'WPI Inflation', 'WPI Manufacturing', 'Raw Materials Prices',
        'Selling Price Expectations', 'Inflation Expectations',
        'Michigan Inflation Expectations', 'Michigan 5 Year Inflation Expectations',
        
        # 次要贸易数据
        'Current Account s.a', 'Foreign Direct Investment', 'FDI',
        'Foreign Bond Investment', 'Foreign Securities Purchases',
        'Foreign Securities Purchases by Canadians', 'Overall Net Capital Flows',
        'Net Long-Term TIC Flows', 'Stock Investment by Foreigners',
        
        # 次要房价数据
        'Residential Property Prices', 'Resi Prop Prices', 'New Housing Price Index',
        'RICS House Price Balance', 'Nationwide Housing Prices',
        'CoreLogic Dwelling Prices', 'Used Car Prices',
        
        # 次要能源数据
        'EIA Gasoline Stocks Change', 'EIA Distillate Stocks Change',
        'EIA Heating Oil Stocks Change', 'EIA Natural Gas Stocks Change',
        'EIA Crude Oil Imports Change', 'EIA Cushing Crude Oil Stocks Change',
        'EIA Distillate Fuel Production Change', 'EIA Gasoline Production Change',
        'EIA Refinery Crude Runs Change', 'EIA Weekly Refinery Utilization Rates WoW',
        'API Crude Oil Stock Change', 'IEA Oil Market Report',
        
        # 次要债券拍卖
        '5-Year Bond Auction', '7-Year Bond Auction', '15-Year Bond Auction',
        '20-Year Bond Auction', '5-Year Treasury Gilt Auction', '7-Year Treasury Gilt Auction',
        '15-Year Treasury Gilt Auction', '20-Year Treasury Gilt Auction',
        '5-Year TIPS Auction', '20-Year TIPS Auction', '5-Year Bobl Auction',
        '7-Year Bund Auction', '15-Year Bund Auction', '20-Year Bund Auction',
        '5-Year BTP Auction', '7-Year BTP Auction', '15-Year BTP Auction',
        '20-Year BTP Auction', '5-Year Note Auction', '7-Year Note Auction',
        '5-Year OAT Auction', '17-Year OAT Auction',
        
        # 次要央行官员讲话
        'Fed Barkin Speech', 'Fed Bostic Speech', 'Fed Bowman Speech',
        'Fed Collins Speech', 'Fed Cook Speech', 'Fed Daly Speech',
        'Fed Goolsbee Speech', 'Fed Harker Speech', 'Fed Jefferson Speech',
        'Fed Kashkari Speech', 'Fed Kugler Speech', 'Fed Logan Speech',
        'Fed Musalem Speech', 'Fed Schmid Speech', 'Fed Waller Speech',
        'Fed Williams Speech', 'BoE Benford Speech', 'BoE Benjamin Speech',
        'BoE Breeden Speech', 'BoE Dhingra Speech', 'BoE Greene Speech',
        'BoE Hauser Speech', 'BoE L Mann Speech', 'BoE Lombardelli Speech',
        'BoE Mann Speech', 'BoE Member Pill Speech', 'BoE Pill Speech',
        'BoE Ramsden Speech', 'ECB Buch Speech', 'ECB Cipollone Speech',
        'ECB De Guindos Speech', 'ECB Elderson Speech', 'ECB Knot Speech',
        'ECB Lane Speech', 'ECB Montagner Speech', 'ECB Nagel Speech',
        'ECB Schnabel Speech', 'ECB Tuominen Speech', 'BoJ Himino Speech',
        'BoJ Nakamura Speech', 'BOJ Uchida Speech', 'RBA Assist Gov Jones Speech',
        'RBA Bullock Speech', 'RBA Connolly Speech', 'RBA Hauser Speech',
        'RBA Hunter Speech', 'RBA Jacobs Speech', 'RBA Jones Speech',
        'RBA Kent Speech', 'BoC Gov Macklem Speech', 'BoC Gravelle Speech',
        'BoC Macklem Speech', 'SNB Moser Speech', 'SNB Schlegel Speech',
        'SNB Tschudin Speech', 'Bundesbank Balz Speech', 'Bundesbank Buch Speech',
        'Bundesbank Mauderer Speech', 'Bundesbank Nagel Speech',
        
        # 次要经济报告
        'BoE Quarterly Bulletin', 'BoJ Summary of Opinions', 'BoJ Monetary Policy Meeting Minutes',
        'ECB Economic Bulletin', 'ECB Monetary Policy Meeting Accounts',
        'ECB Survey of Monetary Analysts', 'ECB Survey of Professional Forecasters',
        'BoC Business Outlook Survey', 'BoC Financial System Review',
        'BoC Market Participants Survey', 'BoC Senior Loan Officer Survey',
        'BoC Summary of Deliberations', 'RBA Chart Pack', 'SNB Quarterly Bulletin',
        'Bundesbank Annual Report', 'Bundesbank Monthly Report',
        'SARB Quarterly Bulletin', 'BCB Inflation Report',
        
        # 次要调查数据
        'Business Confidence', 'Business Climate', 'Business Climate Indicator',
        'Economic Confidence Index', 'Economic Sentiment', 'Economic Sentiment Index',
        'Economic Tendency Indicator', 'Consumer Confidence', 'Westpac Consumer Confidence',
        'Westpac Consumer Confidence Change', 'Westpac Consumer Confidence Index',
        'ANZ Roy Morgan Consumer Confidence', 'FGV Consumer Confidence',
        'Thomson Reuters IPSOS PCSI', 'RCM/TIPP Economic Optimism Index',
        'IBD/TIPP Economic Optimism', 'Sentix Investors Sentiment',
        'ZEW Economic Sentiment Index', 'ZEW Current Conditions', 'ZEW Expectations',
        'Ifo Business Climate', 'Ifo Current Conditions', 'Ifo Expectations',
        'KOF Leading Indicators', 'Leading Economic Index', 'Leading Index',
        'Leading Indicator', 'Leading Business Cycle Indicator', 'Coincident Index',
        'Coincident Indicator', 'Composite Economic Index',
        
        # 次要地区数据
        'Dallas Fed Manufacturing Index', 'Dallas Fed Services Index',
        'Dallas Fed Services Revenues Index', 'Philadelphia Fed Manufacturing Index',
        'Philly Fed Business Conditions', 'Philly Fed CAPEX Index',
        'Philly Fed Employment', 'Philly Fed New Orders', 'Philly Fed Prices Paid',
        'Richmond Fed Manufacturing Index', 'Richmond Fed Manufacturing Shipments Index',
        'Richmond Fed Services Index', 'Richmond Fed Services Revenues Index',
        'Kansas Fed Composite Index', 'Kansas Fed Manufacturing Index',
        'Chicago PMI', 'Chicago Fed National Activity Index',
        'NY Empire State Manufacturing Index', 'Atlanta Fed GDPNow',
        
        # 次要商品数据
        'Gold Production', 'Copper Production', 'Mining Production',
        'Cement Production', 'Car Production', 'Auto Production',
        'Machine Tool Orders', 'Construction Orders', 'Infrastructure Output',
        
        # 次要汽车数据
        'Car Sales', 'Auto Sales', 'New Car Sales', 'New Car Registrations',
        'New Passenger Car Registrations', 'All Car Sales', 'All Truck Sales',
        'Total Vehicle Sales', 'Total New Vehicle Sales', 'Passenger Vehicles Sales',
        'Motorbike Sales', 'New Motor Vehicle Sales',
        
        # 次要信贷数据
        'Bank Lending', 'Bank Loan Growth', 'Loan Growth', 'New Loans',
        'Outstanding Loan Growth', 'Private Sector Credit', 'Private sector loans',
        'Total Credit', 'Total Credit Growth', 'Total Household Debt',
        'Net Lending to Individuals', 'Household Lending Growth',
        'Loans to Companies', 'Loans to Households', 'New Yuan Loans',
        'Total Social Financing', 'Credit Expansion', 'Credit Indicator',
        
        # 次要货币数据
        'M3 Money Supply', 'M4 Money Supply', 'Monetary Base',
        'Currency Reserves', 'Currency Swaps', 'Cash Reserve Ratio',
        'Deposit Growth', 'Deposit Interest Rate',
        
        # 次要政府数据
        'Government Budget', 'Government Budget to GDP', 'Government Budget Value',
        'Government Debt to GDP', 'Gross Debt to GDP', 'Net Debt-to-GDP ratio',
        'Government Payrolls', 'Net Payrolls', 'Public Sector Net Borrowing',
        'Public Sector Net Borrowing Ex Banks', 'Monthly Budget Statement',
        'Federal Tax Revenues', 'Tax Revenue', 'Primary Budget Surplus',
        'Nominal Budget Balance', 'Budget Balance', 'Fiscal Balance',
        'Central Government Debt', 'External Debt',
        
        # 次要劳动力数据
        'Labour Cost Index', 'Labour Productivity', 'Unit Labour Costs',
        'Wage Growth', 'Real Wage Growth', 'Real Wages', 'Average Hourly Wages',
        'Average Weekly Earnings', 'Average Weekly Hours', 'Average Cash Earnings',
        'Average Earnings excl. Bonus', 'Average Earnings incl. Bonus',
        'Corporate Sector Wages', 'Negotiated Wage Growth', 'Overtime Pay',
        'Participation Rate', 'Jobs/applications ratio', 'Jobseekers Total',
        'Unemployed Persons', 'U-6 Unemployment Rate', 'Unemployment Benefit Claims',
        'Registered Jobless Rate',
        
        # 次要建筑数据
        'Construction Output', 'Construction Spending', 'Construction PMI',
        'Building Permits', 'Housing Starts',
        
        # 次要订单数据
        'New Orders', 'Factory Orders', 'Factory Orders ex Transportation',
        'Durable Goods Orders', 'Durable Goods Orders ex Defense',
        'Durable Goods Orders Ex Defense', 'Durable Goods Orders Ex Transp',
        'Non Defense Goods Orders Ex Air', 'Machinery Orders',
        'ISM Manufacturing New Orders', 'ISM Manufacturing Employment',
        'ISM Manufacturing Prices', 'Philly Fed New Orders', 'Philly Fed Employment',
        'Philly Fed Prices Paid',
        
        # 次要库存数据
        'Business Inventories', 'Industrial Inventories', 'Retail Inventories Ex Autos',
        'Wholesale Inventories', 'Manufacturing Inventories',
        
        # 次要投资数据
        'Business Investment', 'Capital Expenditure', 'Gross Fixed Investment',
        'Private Investment', 'Private Spending', 'FDI', 'Foreign Direct Investment',
        
        # 次要生产率数据
        'Nonfarm Productivity', 'Labour Productivity',
        
        # 次要旅游数据
        'Tourism Revenues', 'Tourist Arrivals', 'Visitor Arrivals',
        
        # 次要农业数据
        'Milk Auctions', 'Global Dairy Trade Price Index', 'GlobalDairyTrade Price Index',
        
        # 次要天气和假期
        'Chinese New Year', 'Duarte\'s Day', 'Republic Day', 'Inauguration Day',
        'January 25 Revolution Day', 'Federal Election', 'National People\'s Congress',
        
        # 次要其他数据
        'Cash Remittances', 'Electronic Retail Card Spending', 'Redbook',
        'BRC Retail Sales Monitor', 'BRC Shop Price Index', 'CBI Business Optimism Index',
        'CBI Distributive Trades', 'CBI Industrial Trends Orders', 'CB Leading Index',
        'CB Employment Trends Index', 'CFIB Business Barometer', 'NIESR Monthly GDP Tracker',
        'SECO Consumer Climate', 'SECO Economic Forecasts', 'European Commission Forecasts',
        'European Commission Spring Forecasts', 'European Commission Winter Forecasts',
        'ECB Non-Monetary Policy Meeting', 'BoE MPC Treasury Committee Hearings',
        'BoE MPC Vote Cut', 'BoE MPC Vote Hike', 'BoE MPC Vote Unchanged',
        'BoE Credit Conditions Survey', 'BoE Decision Maker Panel', 'BoE Consumer Credit',
        'BoE FPC Meeting Minutes', 'Fed Balance Sheet', 'Fed Bank Stress Test Results',
        'BCB Copom Meeting Minutes', 'BCB Focus Market Readout', 'BCB National Monetary Council Meeting',
        'CBR Press Conference', 'Summary of the Key Rate Discussion',
        'Irish Exchequer Returns', 'Annual Report'
    ]
    
    # 检查重要性等级
    if event_name in high_impact_events:
        return 3
    elif event_name in medium_impact_events:
        return 2
    else:
        return 1  # 默认1星


class EconomicEventsService:
    """经济事件数据服务类"""
    
    def __init__(self, api_token: str = None):
        """
        初始化API客户端
        
        Args:
            api_token: EODHD API密钥，如果不提供则从环境变量读取
        """
        self.logger = get_logger(__name__)
        self.base_url = "https://eodhd.com/api"
        
        # 获取API密钥
        self.api_token = api_token or os.getenv('EODHD_API_TOKEN')
        if not self.api_token:
            warning("EODHD_API_TOKEN未设置，数据可能受限", source="economic_events")
            self.api_token = "demo"
        
        # 设置默认请求参数
        self.session = self.create_robust_session()
        
        # 初始化数据库管理器
        db_config = config.get_db_config()
        self.db_manager = DatabaseManager({
            'db_host': db_config['host'],
            'db_port': db_config['port'],
            'db_name': db_config['name'],
            'db_user': db_config['user'],
            'db_password': db_config['password']
        })
        
        info("经济事件API服务初始化完成", source="economic_events")
    
    def create_robust_session(self):
        """创建一个具有重试机制和超时设置的会话"""
        session = requests.Session()
        
        # 配置重试策略
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # 设置请求头
        session.headers.update({
            'User-Agent': config.USER_AGENT
        })
        
        return session
    
    def safe_api_request(self, url: str, params: dict, max_retries: int = 3) -> Optional[Dict]:
        """安全的API请求函数，处理SSL错误和各种异常"""
        raise_if_quota_exhausted()
        session = self.session
        
        # 打印完整的EODHD API请求信息
        info(f"EODHD API请求详情:", source="economic_events")
        info(f"  URL: {url}", source="economic_events")
        # 日志中屏蔽 api_token,避免密钥落入日志/终端
        safe_params = {k: ('***' if k == 'api_token' else v) for k, v in params.items()}
        info(f"  参数: {json.dumps(safe_params, indent=2, ensure_ascii=False)}", source="economic_events")
        
        for attempt in range(max_retries):
            try:
                info(f"API请求 (第{attempt + 1}次): {url}", source="economic_events")
                
                response = session.get(url, params=params, timeout=(10, 30))
                response.raise_for_status()
                return response.json()
                
            except requests.exceptions.SSLError as e:
                warning(f"SSL错误 (第{attempt + 1}次): {_scrub_token(e)}", source="economic_events")
                if attempt < max_retries - 1:
                    # 重新创建session以解决SSL连接问题
                    session.close()
                    session = self.create_robust_session()
                    self.session = session
                    time.sleep(2 ** attempt)  # 指数退避
                else:
                    error("达到最大重试次数，SSL错误持续存在", source="economic_events")
                    raise
                    
            except requests.exceptions.RequestException as e:
                # 402 = 配额耗尽：写共享熔断键并立即失败，不做无谓重试
                if hasattr(e, 'response') and e.response is not None and e.response.status_code == 402:
                    error(f"🚨 EODHD 配额耗尽 (402): {url[:120]}", source="economic_events")
                    trip_quota_breaker()
                    raise EODHDQuotaExhausted()
                warning(f"请求错误 (第{attempt + 1}次): {_scrub_token(e)}", source="economic_events")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  # 指数退避
                else:
                    error("达到最大重试次数，API请求失败", source="economic_events")
                    raise
            except json.JSONDecodeError as e:
                error(f"JSON解析失败: {e}", source="economic_events")
                raise Exception(f"响应数据解析失败: {str(e)}")
        
        return None

    def get_economic_events(
        self, 
        from_date: Union[str, datetime] = None,
        to_date: Union[str, datetime] = None,
        country: str = None,
        offset: int = 0,
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """
        获取经济事件数据
        
        Args:
            from_date: 开始日期 (YYYY-MM-DD 或 datetime对象)
            to_date: 结束日期 (YYYY-MM-DD 或 datetime对象)
            country: 国家代码 (ISO 3166格式，2个字符)
            offset: 偏移量 (0-1000)
            limit: 限制数量 (0-1000)
        
        Returns:
            经济事件数据列表
        """
        info("开始获取经济事件数据", source="economic_events")
        
        # 构建请求参数
        params = {
            'api_token': self.api_token,
            'fmt': 'json'
        }
        
        # 处理日期参数
        if from_date:
            if isinstance(from_date, datetime):
                params['from'] = from_date.strftime('%Y-%m-%d')
            else:
                params['from'] = from_date
        
        if to_date:
            if isinstance(to_date, datetime):
                params['to'] = to_date.strftime('%Y-%m-%d')
            else:
                params['to'] = to_date
        
        # 添加其他参数
        if country:
            params['country'] = country
        if offset > 0:
            params['offset'] = offset
        if limit != 50:
            params['limit'] = limit
        
        try:
            # 使用安全的API请求方法
            url = f"{self.base_url}/economic-events"
            data = self.safe_api_request(url, params)
            
            if data is None:
                error("API请求返回空数据", source="economic_events")
                return []
            
            if isinstance(data, list):
                info(f"成功获取 {len(data)} 条经济事件数据", source="economic_events")
                return data
            else:
                warning(f"API返回非预期格式: {type(data)}", source="economic_events")
                return []
                
        except Exception as e:
            error(f"获取经济事件数据失败: {_scrub_token(e)}", source="economic_events")
            raise Exception(f"获取经济事件数据失败: {_scrub_token(e)}")

    def get_economic_events_chunked(
        self,
        from_date: Union[str, datetime],
        to_date: Union[str, datetime],
        country: str = None,
        chunk_days: int = 7,
        limit: int = 1000
    ) -> List[Dict[str, Any]]:
        """
        按 ≤chunk_days 天分片抓取 [from_date, to_date] 的全部经济事件

        EODHD /economic-events 的结果按日期倒序返回：事件总数超过 limit 时只返回
        靠 to 端的最新部分（from 形同虚设），且 offset 上限 1000，即宽窗口实际
        只能拿到最新 ~2000 条。此处把日期范围切成小分片，保证每片一页取全。
        全库约 70 条/天，默认 7 天分片 ≈ 500 条/片。

        单个分片抓取失败时记日志并跳过，不影响其余分片。

        Args:
            from_date: 开始日期 (YYYY-MM-DD 或 datetime对象)
            to_date: 结束日期 (YYYY-MM-DD 或 datetime对象)
            country: 国家代码，可选
            chunk_days: 每个分片的天数，默认 7
            limit: 每片请求的 limit，默认 1000

        Returns:
            经济事件数据列表（未去重，调用方负责）
        """
        def _to_date_obj(value):
            if isinstance(value, datetime):
                return value.date()
            return datetime.strptime(str(value), '%Y-%m-%d').date()

        collected: List[Dict[str, Any]] = []
        chunk_start = _to_date_obj(from_date)
        range_end = _to_date_obj(to_date)

        while chunk_start <= range_end:
            chunk_end = min(chunk_start + timedelta(days=chunk_days - 1), range_end)
            cs = chunk_start.strftime('%Y-%m-%d')
            ce = chunk_end.strftime('%Y-%m-%d')
            try:
                batch = self.get_economic_events(
                    from_date=cs,
                    to_date=ce,
                    country=country,
                    limit=limit
                ) or []
            except Exception as chunk_err:
                error(f"经济事件分片 {cs}~{ce} (country={country or 'ALL'}) 抓取失败: {chunk_err}，跳过该分片", source="economic_events")
                chunk_start = chunk_end + timedelta(days=1)
                continue
            collected.extend(batch)
            info(f"经济事件分片 {cs}~{ce} (country={country or 'ALL'}) 获取 {len(batch)} 条", source="economic_events")
            if len(batch) >= limit:
                warning(f"经济事件分片 {cs}~{ce} (country={country or 'ALL'}) 结果达 {limit} 条上限，可能被截断", source="economic_events")
            chunk_start = chunk_end + timedelta(days=1)

        return collected

    def backfill_event_categories(self) -> Dict[str, int]:
        """
        用 classify_event_category 回填存量事件的 category 列（幂等，可重复执行）

        Returns:
            {'distinct_events': N, 'updated_rows': N}
        """
        conn = None
        try:
            conn = self.db_manager.get_connection()
            if not conn:
                error("无法连接到数据库", source="economic_events")
                return {'distinct_events': 0, 'updated_rows': 0}
            cursor = conn.cursor()
            cursor.execute("SELECT DISTINCT event FROM economic_events")
            names = [row[0] for row in cursor.fetchall()]

            updated = 0
            for name in names:
                cursor.execute(
                    "UPDATE economic_events SET category = %s WHERE event = %s AND (category IS NULL OR category = '' OR category = 'other')",
                    (classify_event_category(name), name)
                )
                updated += cursor.rowcount
            conn.commit()
            cursor.close()
            info(f"category 回填完成: {len(names)} 个事件名, {updated} 行更新", source="economic_events")
            return {'distinct_events': len(names), 'updated_rows': updated}
        except Exception as e:
            if conn:
                conn.rollback()
            error(f"category 回填失败: {str(e)}", source="economic_events")
            return {'distinct_events': 0, 'updated_rows': 0}
        finally:
            if conn:
                conn.close()

    def get_recent_events(self, days: int = 7) -> List[Dict[str, Any]]:
        """
        获取最近几天的经济事件
        
        Args:
            days: 天数
        
        Returns:
            经济事件数据列表
        """
        end_date = utcnow()
        start_date = end_date - timedelta(days=days)
        
        return self.get_economic_events(
            from_date=start_date,
            to_date=end_date,
            limit=100
        )
    
    def get_upcoming_events(self, days: int = 7) -> List[Dict[str, Any]]:
        """
        获取未来几天的经济事件
        
        Args:
            days: 天数
        
        Returns:
            经济事件数据列表
        """
        start_date = utcnow()
        end_date = start_date + timedelta(days=days)
        
        return self.get_economic_events(
            from_date=start_date,
            to_date=end_date,
            limit=100
        )
    
    def get_events_by_country(self, country_code: str, days: int = 30) -> List[Dict[str, Any]]:
        """
        获取特定国家的经济事件
        
        Args:
            country_code: 国家代码 (ISO 3166格式)
            days: 天数范围
        
        Returns:
            经济事件数据列表
        """
        end_date = utcnow() + timedelta(days=days//2)
        start_date = end_date - timedelta(days=days)
        
        return self.get_economic_events(
            from_date=start_date,
            to_date=end_date,
            country=country_code,
            limit=200
        )
    

    
    def get_supported_countries(self) -> List[str]:
        """
        获取支持的国家代码列表
        
        Returns:
            国家代码列表
        """
        # 常见的国家代码
        return [
            'US', 'CN', 'JP', 'DE', 'GB', 'FR', 'IT', 'CA', 'AU', 'KR',
            'IN', 'BR', 'RU', 'MX', 'ES', 'NL', 'CH', 'SE', 'BE', 'NO',
            'AT', 'FI', 'DK', 'PL', 'IE', 'PT', 'CZ', 'NZ', 'IL', 'SG'
        ]
    

    
    def format_event_data(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """
        格式化事件数据
        
        Args:
            event: 原始事件数据
        
        Returns:
            格式化后的事件数据
        """
        # 解析日期时间字符串
        date_str = event.get('date', '')
        time_str = ''
        date_only = ''
        
        if date_str:
            try:
                # 解析完整的日期时间字符串 "2025-08-04 23:50:00"
                if ' ' in date_str:
                    date_only, time_str = date_str.split(' ', 1)
                else:
                    date_only = date_str
            except:
                date_only = date_str
        
        formatted = {
            'date': date_only,
            'time': time_str,
            'country': event.get('country', ''),
            'event': event.get('type', ''),  # API返回的是'type'字段
            'currency': '',  # EODHD API 不返回currency字段，可能需要从其他地方获取
            'actual': event.get('actual', ''),
            'previous': event.get('previous', ''),
            'forecast': event.get('estimate', ''),  # API返回的是'estimate'字段
            'impact': '',  # EODHD API 不返回impact字段
            'unit': '',   # EODHD API 不返回unit字段
            'period': event.get('period', ''),
            'comparison': event.get('comparison'),  # null=指数级 / 'yoy' / 'mom'
            'change': event.get('change', ''),
            'change_percentage': event.get('change_percentage', '')
        }
        
        # 计算变化值
        if formatted['actual'] and formatted['previous']:
            try:
                actual_val = float(formatted['actual'])
                previous_val = float(formatted['previous'])
                change = actual_val - previous_val
                change_pct = (change / previous_val) * 100 if previous_val != 0 else 0
                formatted['calculated_change'] = round(change, 4)
                formatted['calculated_change_pct'] = round(change_pct, 2)
            except (ValueError, TypeError):
                formatted['calculated_change'] = None
                formatted['calculated_change_pct'] = None
        
        return formatted
    
    def get_economic_events_from_db(
        self,
        from_date: Union[str, datetime] = None,
        to_date: Union[str, datetime] = None,
        country: str = None,
        comparison: str = None,
        impacts: List[int] = None,
        categories: List[str] = None,
        event: str = None,
        order: str = 'desc',
        offset: int = 0,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        从数据库获取经济事件数据

        Args:
            from_date: 开始日期 (YYYY-MM-DD 或 datetime对象)
            to_date: 结束日期 (YYYY-MM-DD 或 datetime对象)
            country: 国家代码 (ISO 3166格式，2个字符)，可以是逗号分隔的多个国家
            comparison: 口径过滤。'index' → 只取指数级(comparison IS NULL)；
                        'yoy'/'mom' → 精确匹配；None → 不过滤(全部口径)
            impacts: 重要性等级列表 (如 [1,2,3] 或 [3])
            categories: 类别 slug 列表 (如 ['interest-rate','inflation'])
            event: 事件名精确匹配（日历行点击后的历史序列查询）
            order: 排序方向 'desc'(默认, 兼容旧行为) / 'asc'(日历视图)
            offset: 偏移量
            limit: 限制数量

        Returns:
            经济事件数据列表
        """
        info(f"从数据库获取经济事件数据: from_date={from_date}, to_date={to_date}, country={country}, comparison={comparison}, impacts={impacts}, categories={categories}, event={event}", source="economic_events")

        # 构建SQL查询
        query = "SELECT * FROM economic_events WHERE 1=1"
        params = []

        # 处理日期参数
        if from_date:
            if isinstance(from_date, datetime):
                from_date_str = from_date.strftime('%Y-%m-%d')
            else:
                from_date_str = from_date
            query += " AND date >= %s"
            params.append(from_date_str)

        if to_date:
            if isinstance(to_date, datetime):
                to_date_str = to_date.strftime('%Y-%m-%d')
            else:
                to_date_str = to_date
            query += " AND date <= %s"
            params.append(to_date_str)

        # 处理国家参数（支持多个国家）
        if country:
            # 分割国家代码（支持逗号分隔）
            countries = [c.strip() for c in country.split(',') if c.strip()]
            if countries:
                placeholders = ','.join(['%s'] * len(countries))
                query += f" AND country IN ({placeholders})"
                params.extend(countries)

        # 处理口径参数
        if comparison == 'index':
            query += " AND (comparison IS NULL OR comparison = '')"
        elif comparison in ('yoy', 'mom'):
            query += " AND comparison = %s"
            params.append(comparison)

        # 重要性等级过滤（多选）
        if impacts:
            valid_impacts = [int(i) for i in impacts if str(i).strip().isdigit() and 1 <= int(i) <= 3]
            if valid_impacts:
                placeholders = ','.join(['%s'] * len(valid_impacts))
                query += f" AND impact IN ({placeholders})"
                params.extend(valid_impacts)

        # 类别过滤（多选）
        if categories:
            valid_categories = [str(c).strip() for c in categories if str(c).strip()]
            if valid_categories:
                placeholders = ','.join(['%s'] * len(valid_categories))
                query += f" AND category IN ({placeholders})"
                params.extend(valid_categories)

        # 事件名精确匹配
        if event:
            query += " AND event = %s"
            params.append(event)

        # 添加排序、分页（日历视图按时间正序：最旧日期在最上方）
        direction = 'ASC' if order == 'asc' else 'DESC'
        query += f" ORDER BY date {direction}, time {direction} NULLS LAST"
        query += " LIMIT %s OFFSET %s"
        params.extend([limit, offset])
        
        try:
            conn = self.db_manager.get_connection()
            if not conn:
                error("无法连接到数据库", source="economic_events")
                return []
            
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cursor.execute(query, params)
            results = cursor.fetchall()
            conn.close()
            
            # 转换为字典列表
            events = []
            for row in results:
                event = dict(row)
                # 转换为前端期望的格式
                formatted_event = {
                    'date': event.get('date', ''),
                    'time': event.get('time', ''),
                    'country': event.get('country', ''),
                    'event': event.get('event', ''),
                    'period': event.get('period', ''),
                    'actual': event.get('actual', ''),
                    'previous': event.get('previous', ''),
                    'forecast': event.get('forecast', ''),
                    'change_val': event.get('change_val'),
                    'change_percentage': event.get('change_percentage'),
                    'impact': event.get('impact', 1),
                    'comparison': event.get('comparison', ''),
                    'category': event.get('category') or 'other'
                }
                events.append(formatted_event)
            
            info(f"从数据库获取到 {len(events)} 条经济事件数据", source="economic_events")
            return events
            
        except Exception as e:
            error(f"从数据库获取经济事件数据失败: {str(e)}", source="economic_events")
            return []
    
    def get_available_countries_from_db(self) -> List[Dict[str, str]]:
        """
        从数据库获取可用的国家列表
        
        Returns:
            国家列表，格式为 [{'code': 'US', 'name': 'United States'}, ...]
        """
        info("从数据库获取可用国家列表", source="economic_events")
        
        query = """
            SELECT DISTINCT country 
            FROM economic_events 
            WHERE country IS NOT NULL AND country != ''
            ORDER BY country
        """
        
        try:
            conn = self.db_manager.get_connection()
            if not conn:
                error("无法连接到数据库", source="economic_events")
                return []
            
            cursor = conn.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            conn.close()
            
            # 国家代码到国家名称的映射
            country_names = {
                'US': 'United States', 'CN': 'China', 'JP': 'Japan', 
                'DE': 'Germany', 'GB': 'United Kingdom', 'FR': 'France',
                'IT': 'Italy', 'CA': 'Canada', 'AU': 'Australia', 'KR': 'South Korea',
                'IN': 'India', 'BR': 'Brazil', 'RU': 'Russia', 'MX': 'Mexico',
                'ES': 'Spain', 'NL': 'Netherlands', 'CH': 'Switzerland', 'SE': 'Sweden',
                'BE': 'Belgium', 'NO': 'Norway', 'AT': 'Austria', 'FI': 'Finland',
                'DK': 'Denmark', 'PL': 'Poland', 'IE': 'Ireland', 'PT': 'Portugal',
                'CZ': 'Czech Republic', 'NZ': 'New Zealand', 'IL': 'Israel', 'SG': 'Singapore',
                'HK': 'Hong Kong', 'TW': 'Taiwan', 'TH': 'Thailand', 'MY': 'Malaysia',
                'ID': 'Indonesia', 'PH': 'Philippines', 'VN': 'Vietnam', 'SA': 'Saudi Arabia',
                'AE': 'United Arab Emirates', 'ZA': 'South Africa', 'EG': 'Egypt', 'NG': 'Nigeria',
                'AR': 'Argentina', 'CL': 'Chile', 'CO': 'Colombia', 'PE': 'Peru',
                'TR': 'Turkey', 'GR': 'Greece', 'HU': 'Hungary', 'RO': 'Romania',
                # EODHD 专用代码（非标准 ISO Alpha-2）
                'UK': 'United Kingdom', 'EU': 'Euro Area'
            }
            
            countries = []
            for row in results:
                country_code = row[0]
                country_name = country_names.get(country_code, country_code)
                countries.append({
                    'code': country_code,
                    'name': country_name
                })
            
            info(f"从数据库获取到 {len(countries)} 个国家", source="economic_events")
            return countries
            
        except Exception as e:
            error(f"从数据库获取国家列表失败: {str(e)}", source="economic_events")
            return []


