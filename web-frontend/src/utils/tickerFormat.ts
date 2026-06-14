/**
 * Ticker 格式展开工具.
 *
 * 把 Watchlist 里的股票 symbol 转换成传给 Daily `/api/daily/feed?tickers=`
 * 的格式变体数组。DB 里 ticker (subscription_content_tags.ticker /
 * subscription_content.metadata->'symbols' / file_tags.ticker) 统一存为
 * `SYMBOL.EODHD_CODE` 格式 (如 `NVDA.US`、`1810.HK`、`603826.SH`)。
 *
 * ⚠️ 关键: 后端 companies.exchange 存的是完整交易所名 (NASDAQ / NYSE / HK /
 * SHG)，而 DB ticker 后缀是 EODHD 短码 (US / HK / SH)。两者不同，必须先映射，
 * 否则 `AMAT.NASDAQ` 永远匹配不到 DB 里的 `AMAT.US`，导致 Watchlist News 全空。
 *
 * 映射表对齐后端 ExchangeConfig.EXCHANGE_MAPPING 的 code 字段
 * (backend/config/exchange_config.py) 与 EODHD metadata.symbols 实际后缀。
 */

/**
 * companies.exchange (完整交易所名) → DB ticker 用的 EODHD 短码后缀。
 * A 股同时给出 EODHD 实际后缀 (SH/SZ) 与后端 code (SHG/SHE)，全部兜底。
 */
const EXCHANGE_TO_SUFFIXES: Record<string, string[]> = {
  // 美股 (EXCHANGE_MAPPING 中 code='US')
  US: ['US'], NYSE: ['US'], NASDAQ: ['US'], AMEX: ['US'],
  NYSEARCA: ['US'], ARCA: ['US'], OTC: ['US'], PINK: ['US'], BATS: ['US'], IEX: ['US'],
  // 港股
  HK: ['HK'], HKEX: ['HK'], HKSE: ['HK'],
  // A 股：EODHD 用 .SH/.SZ，后端 code 用 SHG/SHE，全覆盖
  SHG: ['SH', 'SHG'], SSE: ['SH'], SHA: ['SH'],
  SHE: ['SZ', 'SHE'], SZSE: ['SZ'], SZA: ['SZ'],
  // 其他（DB 里也存在这些后缀，直通）
  TWO: ['TWO'], TW: ['TWO'], JP: ['JP'], LSE: ['LSE'],
};

/**
 * 把单个 ticker 展开为传给 Daily feed 的格式变体数组。
 *
 * @param symbol   Watchlist 股票代码，如 `NVDA` / `NVDA.US` / `1810`
 * @param exchange companies.exchange 完整交易所名，如 `NASDAQ` / `HK` / `SHG`
 * @returns 去重后的变体数组；空 symbol 返回 `[]`
 */
export function expandTickerForDaily(symbol: string, exchange?: string | null): string[] {
  const sym = (symbol ?? '').trim().toUpperCase();
  if (!sym) return [];

  // 已含交易所后缀：精确匹配，避免 1810.HK 误匹配 1810.US
  if (sym.includes('.')) {
    return [sym];
  }

  const suffixes = EXCHANGE_TO_SUFFIXES[(exchange ?? '').trim().toUpperCase()] ?? [];
  if (suffixes.length > 0) {
    // 裸值 + 各短码后缀 (DB 里 99% 是带后缀格式；裸值兜底防偶发无后缀存储)
    return dedupe([sym, ...suffixes.map((s) => `${sym}.${s}`)]);
  }

  // 无 / 未知 exchange：按 ticker 特征兜底 (对齐后端 get_smart_ticker_formats)
  if (/^\d+$/.test(sym)) {
    // eslint-disable-next-line no-console
    console.warn(
      `[expandTickerForDaily] ticker "${sym}" exchange 未知，数字 ticker 用 SH/SZ/HK/US 多兜底，可能跨市场误匹配。建议在 watchlist 补充 exchange 字段。`,
    );
    return dedupe([`${sym}.SH`, `${sym}.SZ`, `${sym}.HK`, `${sym}.US`]);
  }
  // 字母 ticker (多为美股) 默认 US/HK 兜底
  return dedupe([`${sym}.US`, `${sym}.HK`]);
}

function dedupe(arr: string[]): string[] {
  return Array.from(new Set(arr));
}
