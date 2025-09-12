import apiClient from './api';

// 数据类型定义
export interface CompanyInfo {
  id: number;
  code: string;
  name: string;
  exchange: string;
  type?: string;
  currencycode?: string;
  currencyname?: string;
  currencysymbol?: string;
  countryname?: string;
  countryiso?: string;
  sector?: string;
  industry?: string;
  description?: string;
  address?: string;
  phone?: string;
  weburl?: string;
  logourl?: string;
  fulltimeemployees?: number;
  ticker_active?: boolean;
  primaryticker?: string;
}

export interface CompanyHighlights {
  id: number;
  company_id: number;
  marketcapitalization?: number;
  marketcapitalizationmln?: number;
  ebitda?: number;
  peratio?: number;
  pegratio?: number;
  bookvalue?: number;
  dividendshare?: number;
  dividendyield?: number;
  earningsshare?: number;
  epsestimatecurrentyear?: number;
  epsestimatenextyear?: number;
  profitmargin?: number;
  operatingmarginttm?: number;
  returnonassetsttm?: number;
  returnonequityttm?: number;
  revenuettm?: number;
  quarterlyrevenuegrowthyoy?: number;
  quarterlyearningsgrowthyoy?: number;
  grossprofitttm?: number;
  data_date: string;
}

export interface BalanceSheetItem {
  id: number;
  company_id: number;
  period_type: string;
  date: string;
  filing_date?: string;
  currency_symbol?: string;
  totalassets?: number;
  totalcurrentassets?: number;
  cash?: number;
  cashandequivalents?: number;
  netreceivables?: number;
  inventory?: number;
  totalliab?: number;
  totalcurrentliabilities?: number;
  accountspayable?: number;
  shorttermdebt?: number;
  longtermdebt?: number;
  totalstockholderequity?: number;
  commonstock?: number;
  retainedearnings?: number;
  commonstocksharesoutstanding?: number;
}

export interface IncomeStatementItem {
  id: number;
  company_id: number;
  period_type: string;
  date: string;
  filing_date?: string;
  currency_symbol?: string;
  totalrevenue?: number;
  costofrevenue?: number;
  grossprofit?: number;
  totaloperatingexpenses?: number;
  researchdevelopment?: number;
  sellinggeneraladministrative?: number;
  operatingincome?: number;
  ebit?: number;
  ebitda?: number;
  interestexpense?: number;
  incomebeforetax?: number;
  incometaxexpense?: number;
  netincome?: number;
  netincomeapplicabletocommonshares?: number;
}

export interface CashFlowItem {
  id: number;
  company_id: number;
  period_type: string;
  date: string;
  filing_date?: string;
  currency_symbol?: string;
  totalcashfromoperatingactivities?: number;
  netincome?: number;
  depreciationandamortization?: number;
  changesinaccountsreceivables?: number;
  changesinliabilities?: number;
  changesinaccountspayable?: number;
  totalcashfromfinancingactivities?: number;
  dividendspaid?: number;
  issuanceofstock?: number;
  repurchaseofstock?: number;
  totalcashfrominvestingactivities?: number;
  capitalexpenditures?: number;
  acquisitions?: number;
  purchasesofinvestments?: number;
  salesormaturitiesofinvestments?: number;
  netchangeincash?: number;
  cashfreeflowfromoperations?: number;
}

export interface CompanyValuations {
  id: number;
  company_id: number;
  trailingpe?: number;
  forwardpe?: number;
  pricesalesttm?: number;
  pricebookmrq?: number;
  enterprisevalue?: number;
  enterprisevaluerevenue?: number;
  enterprisevalueebitda?: number;
  data_date: string;
}

export interface AnalystRating {
  id: number;
  company_id: number;
  rating?: string;
  rating_value?: number;
  target_price?: number;
  analyst_name?: string;
  brokerage?: string;
  rating_date?: string;
  created_at: string;
  updated_at?: string;
  total_analysts?: number;
  rating_breakdown?: {
    strong_buy: { count: number; percentage: number };
    buy: { count: number; percentage: number };
    hold: { count: number; percentage: number };
    sell: { count: number; percentage: number };
    strong_sell: { count: number; percentage: number };
  };
}

export interface DividendInfo {
  id: number;
  company_id: number;
  ex_dividend_date?: string;
  payment_date?: string;
  record_date?: string;
  declared_date?: string;
  amount?: number;
  adjusted_amount?: number;
  flag?: string;
  currency?: string;
}

export interface InsiderTransaction {
  id: number;
  company_id: number;
  date: string;
  ownercik?: string;
  ownername: string;
  transactiondate?: string;
  transactioncode?: string;
  transactionamount?: number;
  transactionprice?: number;
  transactionacquireddisposed?: string;
  posttransactionamount?: number;
  seclink?: string;
  created_at: string;
  updated_at?: string;
}

export interface CompanyTechnicals {
  id: number;
  company_id: number;
  beta?: number;
  '52WeekHigh'?: number;
  '52WeekLow'?: number;
  '50DayMA'?: number;
  '200DayMA'?: number;
  sharesshort?: number;
  sharesshortpriormonth?: number;
  shortratio?: number;
  shortpercent?: number;
  data_date: string;
}

// 盈利数据相关接口
export interface EarningsAnnual {
  id: number;
  company_id: number;
  date: string;
  epsactual?: number;
  created_at: string;
  updated_at?: string;
}

export interface EarningsHistory {
  id: number;
  company_id: number;
  reportdate?: string;
  date: string;
  beforeaftermarket?: string;
  currency?: string;
  epsactual?: number;
  epsestimate?: number;
  epsdifference?: number;
  surprisepercent?: number;
  created_at: string;
  updated_at?: string;
}

export interface EarningsTrend {
  id: number;
  company_id: number;
  date: string;
  period?: string;
  growth?: number;
  earningsestimateavg?: number;
  earningsestimatelow?: number;
  earningsestimatehigh?: number;
  earningsestimateyearagoeps?: number;
  earningsestimatenumberofanalysts?: number;
  earningsestimategrowth?: number;
  revenueestimateavg?: number;
  revenueestimatelow?: number;
  revenueestimatehigh?: number;
  revenueestimateyearagoeps?: number;
  revenueestimatenumberofanalysts?: number;
  revenueestimategrowth?: number;
  epstrendcurrent?: number;
  epstrend7daysago?: number;
  epstrend30daysago?: number;
  epstrend60daysago?: number;
  epstrend90daysago?: number;
  epsrevisionsuplast7days?: number;
  epsrevisionsuplast30days?: number;
  epsrevisionsdownlast7days?: number;
  epsrevisionsdownlast30days?: number;
  created_at: string;
  updated_at?: string;
}

// 股东信息相关接口
export interface InstitutionalHolding {
  id: number;
  company_id: number;
  holder_type: string;
  name: string;
  date: string;
  totalshares?: number;
  totalassets?: number;
  currentshares?: number;
  change?: number;
  change_p?: number;
  created_at: string;
  updated_at?: string;
}

// 高管信息相关接口
export interface CompanyOfficer {
  id: number;
  company_id: number;
  name: string;
  title: string;
  yearborn?: string;
  sort_order?: number;
  is_active?: boolean;
  created_at: string;
  updated_at?: string;
}

// 流通股数据相关接口
export interface OutstandingShares {
  id: number;
  company_id: number;
  date: string;
  dateformatted: string;
  sharesmln?: string;
  shares?: number;
  type: 'annual' | 'quarterly';
  created_at: string;
  updated_at?: string;
}

// 分红数据相关接口
export interface DividendByYear {
  id: number;
  company_id: number;
  year: number;
  count?: number;
  created_at: string;
}

export interface DividendInfo {
  id: number;
  company_id: number;
  forwardannualdividendrate?: number;
  forwardannualdividendyield?: number;
  payoutratio?: number;
  dividenddate?: string;
  exdividenddate?: string;
  lastsplitfactor?: string;
  lastsplitdate?: string;
  data_date: string;
  created_at: string;
  updated_at?: string;
}

export interface CompanyFundamentalsResponse {
  company: CompanyInfo;
  highlights?: CompanyHighlights;
  balance_sheet: BalanceSheetItem[];
  income_statement: IncomeStatementItem[];
  cash_flow: CashFlowItem[];
  valuations?: CompanyValuations;
  analyst_ratings: AnalystRating[];
  dividend_info: DividendInfo[];
  dividend_by_year: DividendByYear[];
  insider_transactions: InsiderTransaction[];
  technicals?: CompanyTechnicals;
  earnings_annual: EarningsAnnual[];
  earnings_history: EarningsHistory[];
  earnings_trend: EarningsTrend[];
  institutional_holdings: InstitutionalHolding[];
  officers: CompanyOfficer[];
  outstanding_shares: OutstandingShares[];
}

export interface CompanySearchRequest {
  query: string;
}

// API函数
export const fundamentalsAPI = {
  // 搜索公司基本面信息
  searchCompany: async (query: string): Promise<CompanyFundamentalsResponse> => {
    const response = await apiClient.post<CompanyFundamentalsResponse>(
      '/markets/company/search',
      { query }
    );
    return response.data;
  }
};

// 工具函数
export const fundamentalsUtils = {
  // 格式化数字显示
  formatNumber: (value: number | null | undefined, decimals: number = 2): string => {
    if (value === null || value === undefined || isNaN(value)) {
      return 'N/A';
    }
    
    if (Math.abs(value) >= 1e12) {
      return `${(value / 1e12).toFixed(decimals)}T`;
    } else if (Math.abs(value) >= 1e9) {
      return `${(value / 1e9).toFixed(decimals)}B`;
    } else if (Math.abs(value) >= 1e6) {
      return `${(value / 1e6).toFixed(decimals)}M`;
    } else if (Math.abs(value) >= 1e3) {
      return `${(value / 1e3).toFixed(decimals)}K`;
    } else {
      return value.toFixed(decimals);
    }
  },

  // 格式化百分比
  formatPercentage: (value: number | null | undefined, decimals: number = 2): string => {
    if (value === null || value === undefined || isNaN(value)) {
      return 'N/A';
    }
    return `${(value * 100).toFixed(decimals)}%`;
  },

  // 格式化货币
  formatCurrency: (value: number | null | undefined, currency: string = 'USD', decimals: number = 2): string => {
    if (value === null || value === undefined || isNaN(value)) {
      return 'N/A';
    }
    
    const currencySymbols: { [key: string]: string } = {
      'USD': '$',
      'HKD': 'HK$',
      'CNY': '¥',
      'EUR': '€',
      'GBP': '£',
      'JPY': '¥'
    };
    
    const symbol = currencySymbols[currency] || currency;
    return `${symbol}${fundamentalsUtils.formatNumber(value, decimals)}`;
  },

  // 格式化日期
  formatDate: (dateString: string | null | undefined): string => {
    if (!dateString) return 'N/A';
    
    try {
      const date = new Date(dateString);
      return date.toLocaleDateString('en-US', {
        year: 'numeric',
        month: 'short',
        day: 'numeric'
      });
    } catch {
      return dateString;
    }
  },

  // 获取比率的颜色
  getRatioColor: (value: number | null | undefined, isHighGood: boolean = true): string => {
    if (value === null || value === undefined || isNaN(value)) {
      return '#999999';
    }
    
    if (isHighGood) {
      return value > 0 ? '#52c41a' : '#ff4d4f';
    } else {
      return value < 0 ? '#52c41a' : '#ff4d4f';
    }
  },

  // 计算年化增长率
  calculateGrowthRate: (currentValue: number, previousValue: number): number => {
    if (!currentValue || !previousValue || previousValue === 0) {
      return 0;
    }
    return (currentValue - previousValue) / previousValue;
  },

  // 获取评级颜色
  getRatingColor: (rating: string | number | null | undefined): string => {
    if (!rating) return '#999999';
    
    // Convert to string and handle non-string values
    const ratingStr = String(rating).toLowerCase();
    if (ratingStr.includes('strong buy') || ratingStr.includes('buy')) {
      return '#52c41a';
    } else if (ratingStr.includes('hold') || ratingStr.includes('neutral')) {
      return '#faad14';
    } else if (ratingStr.includes('sell')) {
      return '#ff4d4f';
    } else {
      return '#999999';
    }
  },

  // 计算市值
  calculateMarketCap: (shares: number | null | undefined, price: number | null | undefined): number | null => {
    if (!shares || !price) return null;
    return shares * price;
  },

  // 获取财务健康度指标
  getFinancialHealthScore: (
    currentRatio: number | null,
    debtToEquity: number | null,
    returnOnEquity: number | null,
    profitMargin: number | null
  ): { score: number; level: string; color: string } => {
    let score = 0;
    
    // 流动比率 (理想值 1.5-3)
    if (currentRatio && currentRatio >= 1.5) score += 25;
    else if (currentRatio && currentRatio >= 1) score += 15;
    
    // 负债权益比 (越低越好，理想值 < 0.5)
    if (debtToEquity !== null && debtToEquity < 0.3) score += 25;
    else if (debtToEquity !== null && debtToEquity < 0.6) score += 15;
    
    // 股本回报率 (越高越好，理想值 > 15%)
    if (returnOnEquity && returnOnEquity > 0.15) score += 25;
    else if (returnOnEquity && returnOnEquity > 0.1) score += 15;
    
    // 利润率 (越高越好，理想值 > 10%)
    if (profitMargin && profitMargin > 0.1) score += 25;
    else if (profitMargin && profitMargin > 0.05) score += 15;
    
    let level = 'Poor';
    let color = '#ff4d4f';
    
    if (score >= 80) {
      level = 'Excellent';
      color = '#52c41a';
    } else if (score >= 60) {
      level = 'Good';
      color = '#73d13d';
    } else if (score >= 40) {
      level = 'Fair';
      color = '#faad14';
    } else if (score >= 20) {
      level = 'Weak';
      color = '#ff7a45';
    }
    
    return { score, level, color };
  }
};
