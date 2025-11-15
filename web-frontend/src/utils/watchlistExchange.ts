import type { WatchlistItem, StockPriceData } from '../services/dashboard';

export type ExchangeFilterValue = 'all' | 'us' | 'hk' | 'cn';

export const EXCHANGE_FILTER_TABS: Array<{ label: string; value: ExchangeFilterValue }> = [
  { label: 'All', value: 'all' },
  { label: 'US', value: 'us' },
  { label: 'HK', value: 'hk' },
  { label: 'CN', value: 'cn' }
];

export const EXCHANGE_CODE_ALIASES: Record<string, string> = {
  NASDAQGS: 'NASDAQ',
  NASDAQGM: 'NASDAQ',
  NASDAQCM: 'NASDAQ',
  NASDAQ: 'NASDAQ',
  NAS: 'NASDAQ',
  NMS: 'NASDAQ',
  NYSEARCA: 'NYSE',
  NYSEAMERICAN: 'AMEX',
  NYSEMKT: 'AMEX',
  ARCA: 'NYSE',
  BATS: 'US',
  OTCQX: 'OTC',
  OTCQB: 'OTC',
  OTCMKTS: 'OTC',
  OTCM: 'OTC',
  PINK: 'OTC',
  HKEX: 'HK',
  HKG: 'HK',
  XHKG: 'HK',
  HK: 'HK',
  SHSE: 'SHG',
  SSE: 'SHG',
  XSHG: 'SHG',
  SZSE: 'SHE',
  XSHE: 'SHE',
  CNEQ: 'SHE',
  CNSZ: 'SHE',
  CNSH: 'SHG'
};

export const EXCHANGE_GROUPS: Record<Exclude<ExchangeFilterValue, 'all'>, Set<string>> = {
  us: new Set([
    'US',
    'NYSE',
    'NASDAQ',
    'AMEX',
    'XNYS',
    'XNAS',
    'XASE',
    'NYSEARCA',
    'NYSEAMERICAN',
    'NYSEMKT',
    'ARCA',
    'OTC',
    'BATS'
  ]),
  hk: new Set(['HK', 'HKEX', 'HKG', 'XHKG']),
  cn: new Set(['SHG', 'SHE', 'SHSE', 'SSE', 'SZSE', 'XSHE', 'XSHG', 'CN'])
};

export const normalizeExchangeCode = (code?: string | null): string | undefined => {
  if (!code) return undefined;
  const upper = code.trim().toUpperCase();
  if (!upper) return undefined;
  return EXCHANGE_CODE_ALIASES[upper] || upper;
};

export const resolveExchangeCode = (
  item: WatchlistItem,
  priceData?: StockPriceData
): string | undefined => {
  const candidates = [
    item.exchange,
    item.symbol?.includes('.') ? item.symbol.split('.')[1] : undefined,
    priceData?.code?.includes('.') ? priceData.code.split('.')[1] : undefined,
    priceData?.code
  ];

  for (const candidate of candidates) {
    const normalized = normalizeExchangeCode(candidate);
    if (normalized) {
      return normalized;
    }
  }

  return undefined;
};

export const resolveBaseSymbol = (symbol?: string): string => {
  if (!symbol) return '';
  return symbol.includes('.') ? symbol.split('.')[0] : symbol;
};

export const matchesExchangeFilter = (
  filter: ExchangeFilterValue,
  exchangeCode?: string
): boolean => {
  if (filter === 'all') {
    return true;
  }
  if (!exchangeCode) {
    return false;
  }
  const exchangeGroup = EXCHANGE_GROUPS[filter];
  return exchangeGroup.has(exchangeCode);
};


