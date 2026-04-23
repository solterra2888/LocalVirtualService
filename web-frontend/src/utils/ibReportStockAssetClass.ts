/**
 * IB 活动报告复盘：是否将「资产分类」视为股票（桌面与手机共用）。
 * EODHD 无法覆盖期权/期货/外汇等，非股票在复盘侧排除。
 */
export function isStockAssetClass(raw: string | undefined): boolean {
  const s = (raw || '').trim();
  if (!s) return false;

  if (
    /期权|期货|外汇|基金|债券|结构性|权证|涡轮|商品|金属|能源|指数|利差|货币|现货金|差价/i.test(s) ||
    /\b(OPT|FOP|FUT|FX|BOND|FUND|CFD|CMDTY|FOR|BAG|COIN|CRYPTO|CMD|IOPT)\b/i.test(s)
  ) {
    return false;
  }
  if (/\bCASH\b/i.test(s) && !/股票/i.test(s)) return false;

  if (/股票|正股|普通股/i.test(s)) return true;
  // IB 英文导出为「Stocks」；\bSTOCK\b 无法匹配「Stocks」（k 与 s 同为词字符，无词边界）
  if (/\b(STK|STOCKS?|EQUITY|EQUITIES)\b/i.test(s)) return true;

  return false;
}
