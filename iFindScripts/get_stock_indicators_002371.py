# -*- coding: utf-8 -*-
"""
根据 More ActionsTicker002371.md 中的指标定义，获取股票数据并导出为JSON
"""

from iFinDPy import *
from datetime import datetime, timedelta
import json
import re
import pandas as pd
from dateutil.relativedelta import relativedelta

# 登录信息（从ifind_demo.py中获取）


def _fix_encoding(s):
    """iFinD 在 Windows 上中文可能被误解码，尝试两种纠错：UTF-8 被当 GBK → gbk→utf8；GBK 被当 UTF-8 → utf8→gbk。"""
    if s is None or not isinstance(s, str) or not s.strip():
        return s
    for enc, dec in [('gbk', 'utf-8'), ('utf-8', 'gbk')]:
        try:
            return s.encode(enc).decode(dec)
        except (UnicodeDecodeError, UnicodeEncodeError):
            continue
    return s


def login():
    """登录同花顺数据接口"""
    thsLogin = THS_iFinDLogin("shhfst003", "F4KcRFg4")
    if thsLogin in {0, -201}:
        print('登录成功')
        return True
    else:
        print(f'登录失败: {thsLogin}')
        return False

def get_latest_quarter_end(dt_str):
    """
    返回指定日期及之前最近的财报季末日期（03-31、06-30、09-30、12-31）。
    iFinD 财务指标要求报表日期为有效报告期结束日，否则易返回空。
    """
    dt = datetime.strptime(dt_str, "%Y-%m-%d")
    y, m = dt.year, dt.month
    if m >= 10:
        return f"{y}-09-30"
    elif m >= 7:
        return f"{y}-06-30"
    elif m >= 4:
        return f"{y}-03-31"
    else:
        return f"{y - 1}-12-31"


def parse_formula(formula):
    """
    解析Excel公式，提取函数名和参数
    例如: ='/iFinD/ThsFunc.xla'!thsiFinD("ths_stock_short_name_stock",B1)
    返回: ('ths_stock_short_name_stock', ['B1'])
    """
    # 提取thsiFinD函数调用
    pattern = r'thsiFinD\("([^"]+)",([^)]+)\)'
    match = re.search(pattern, formula)
    if match:
        func_name = match.group(1)
        params_str = match.group(2)
        # 解析参数（简单处理，假设参数用逗号分隔）
        params = [p.strip() for p in params_str.split(',')]
        return func_name, params
    return None, None

def get_stock_data(ticker, date_str):
    """
    根据指标定义获取股票数据
    """
    result = {
        "Ticker": ticker,
        "数据更新时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    
    # 报表日期：取最近财报季末（03-31/06-30/09-30/12-31），iFinD 仅对有效报告期返回值
    _dt = datetime.strptime(date_str, "%Y-%m-%d")
    report_date = get_latest_quarter_end((_dt - timedelta(days=45)).strftime("%Y-%m-%d"))
    report_date_ly = f"{int(report_date[:4]) - 1}-{report_date[5:]}"  # 上年同季末，用于经营现金流 TTM 等
    date_minus_365 = (_dt - timedelta(days=365)).strftime("%Y-%m-%d")
    
    print(f"获取股票数据: {ticker}, 日期: {date_str}, 报表日期: {report_date}")
    
    try:
        # 1. 公司简称
        print("获取公司简称...")
        data = THS_BD(ticker, 'ths_stock_short_name_stock', date_str)
        if data.errorcode == 0 and len(data.data) > 0:
            raw = data.data.iloc[0, 1] if len(data.data.columns) > 1 else data.data.iloc[0, 0]
            result["公司简称"] = _fix_encoding(str(raw).strip()) if raw is not None and pd.notna(raw) else None
        else:
            result["公司简称"] = None
            print(f"  错误: {data.errmsg if hasattr(data, 'errmsg') else '未知错误'}")
        
        # 2. 申万一级（参数格式 '1,YYYY-MM-DD'）
        print("获取申万一级...")
        data = THS_BD(ticker, 'ths_the_sw_industry_stock', f'1,{date_str}')
        if data.errorcode == 0 and len(data.data) > 0:
            raw = data.data.iloc[0, 1] if len(data.data.columns) > 1 else data.data.iloc[0, 0]
            result["申万一级"] = _fix_encoding(str(raw).strip()) if raw is not None and pd.notna(raw) else None
        else:
            result["申万一级"] = None
        
        # 3. 申万二级（参数格式 '2,YYYY-MM-DD'）
        print("获取申万二级...")
        data = THS_BD(ticker, 'ths_the_sw_industry_stock', f'2,{date_str}')
        if data.errorcode == 0 and len(data.data) > 0:
            raw = data.data.iloc[0, 1] if len(data.data.columns) > 1 else data.data.iloc[0, 0]
            result["申万二级"] = _fix_encoding(str(raw).strip()) if raw is not None and pd.notna(raw) else None
        else:
            result["申万二级"] = None
        
        # 4. 主题标签（可多选）
        print("获取主题标签...")
        data = THS_BD(ticker, 'ths_the_concept_stock', date_str)
        if data.errorcode == 0 and len(data.data) > 0:
            concepts = data.data.iloc[:, 1].tolist() if len(data.data.columns) > 1 else data.data.iloc[:, 0].tolist()
            parts = [str(c) for c in concepts if pd.notna(c) and c is not None]
            result["主题标签（可多选）"] = _fix_encoding(','.join(parts)) if parts else None
        else:
            result["主题标签（可多选）"] = None
        
        # 5. 市值(亿元)
        print("获取市值...")
        data = THS_BD(ticker, 'ths_market_value_stock', date_str)
        if data.errorcode == 0 and len(data.data) > 0:
            value = data.data.iloc[0, 1] if len(data.data.columns) > 1 else data.data.iloc[0, 0]
            result["市值(亿元)"] = float(value) / 100000000 if pd.notna(value) else None
        else:
            result["市值(亿元)"] = None
        
        # 6. 自由流通市值(亿元)
        print("获取自由流通市值...")
        data = THS_BD(ticker, 'ths_free_float_mv_stock', date_str)
        if data.errorcode == 0 and len(data.data) > 0:
            value = data.data.iloc[0, 1] if len(data.data.columns) > 1 else data.data.iloc[0, 0]
            result["自由流通市值(亿元)"] = float(value) / 100000000 if pd.notna(value) else None
        else:
            result["自由流通市值(亿元)"] = None
        
        # 7. 营收TTM(亿元)
        print("获取营收TTM...")
        data = THS_BD(ticker, 'ths_or_ttm_stock', f'{date_str},100')
        if data.errorcode == 0 and len(data.data) > 0:
            value = data.data.iloc[0, 1] if len(data.data.columns) > 1 else data.data.iloc[0, 0]
            result["营收TTM(亿元)"] = float(value) / 100000000 if pd.notna(value) else None
        else:
            result["营收TTM(亿元)"] = None
        
        # 8. 营收YoY
        print("获取营收YoY...")
        data_current = THS_BD(ticker, 'ths_or_ttm_stock', f'{date_str},100')
        data_prev = THS_BD(ticker, 'ths_or_ttm_stock', f'{date_minus_365},100')
        if (data_current.errorcode == 0 and len(data_current.data) > 0 and
            data_prev.errorcode == 0 and len(data_prev.data) > 0):
            value_current = data_current.data.iloc[0, 1] if len(data_current.data.columns) > 1 else data_current.data.iloc[0, 0]
            value_prev = data_prev.data.iloc[0, 1] if len(data_prev.data.columns) > 1 else data_prev.data.iloc[0, 0]
            if pd.notna(value_current) and pd.notna(value_prev) and float(value_prev) != 0:
                result["营收YoY"] = (float(value_current) / float(value_prev) - 1)
            else:
                result["营收YoY"] = None
        else:
            result["营收YoY"] = None
        
        # 9. 净利TTM(亿元)
        print("获取净利TTM...")
        data = THS_BD(ticker, 'ths_np_ttm_stock', f'{date_str},100')
        if data.errorcode == 0 and len(data.data) > 0:
            value = data.data.iloc[0, 1] if len(data.data.columns) > 1 else data.data.iloc[0, 0]
            result["净利TTM(亿元)"] = float(value) / 100000000 if pd.notna(value) else None
        else:
            result["净利TTM(亿元)"] = None
        
        # 10. 归母净利YoY
        print("获取归母净利YoY...")
        data_current = THS_BD(ticker, 'ths_np_ttm_stock', f'{date_str},100')
        data_prev = THS_BD(ticker, 'ths_np_ttm_stock', f'{date_minus_365},100')
        if (data_current.errorcode == 0 and len(data_current.data) > 0 and
            data_prev.errorcode == 0 and len(data_prev.data) > 0):
            value_current = data_current.data.iloc[0, 1] if len(data_current.data.columns) > 1 else data_current.data.iloc[0, 0]
            value_prev = data_prev.data.iloc[0, 1] if len(data_prev.data.columns) > 1 else data_prev.data.iloc[0, 0]
            if pd.notna(value_current) and pd.notna(value_prev) and float(value_prev) != 0:
                result["归母净利YoY"] = (float(value_current) / float(value_prev) - 1)  # 小数，如 0.0871 表示 8.71%
            else:
                result["归母净利YoY"] = None
        else:
            result["归母净利YoY"] = None
        
        # 11. ROE(TTM)
        print("获取ROE(TTM)...")
        data = THS_BD(ticker, 'ths_roe_ttm_stock', f'{date_str},100')
        if data.errorcode == 0 and len(data.data) > 0:
            value = data.data.iloc[0, 1] if len(data.data.columns) > 1 else data.data.iloc[0, 0]
            result["ROE(TTM)"] = float(value) / 100 if pd.notna(value) else None  # 小数，如 0.1743 表示 17.43%
        else:
            result["ROE(TTM)"] = None
        
        # 12. 毛利率(TTM)
        print("获取毛利率(TTM)...")
        data = THS_BD(ticker, 'ths_sales_gir_ttm_stock', f'{date_str},100')
        if data.errorcode == 0 and len(data.data) > 0:
            value = data.data.iloc[0, 1] if len(data.data.columns) > 1 else data.data.iloc[0, 0]
            result["毛利率(TTM)"] = float(value) / 100 if pd.notna(value) else None  # 小数，如 0.4093 表示 40.93%
        else:
            result["毛利率(TTM)"] = None
        
        # 13. 营业利润率(TTM)，按文档：ths_op_stock(ticker, report_date, 100)/1e8/营收TTM
        print("获取营业利润率(TTM)...")
        data_op = THS_BD(ticker, 'ths_op_stock', f'{report_date},100')
        if (data_op.errorcode == 0 and len(data_op.data) > 0 and
            result.get("营收TTM(亿元)") is not None and result["营收TTM(亿元)"] != 0):
            value_op = data_op.data.iloc[0, 1] if len(data_op.data.columns) > 1 else data_op.data.iloc[0, 0]
            if pd.notna(value_op):
                result["营业利润率(TTM)"] = float(value_op) / 100000000 / result["营收TTM(亿元)"]
                print(f"  营业利润率(TTM): {result['营业利润率(TTM)']:.2%}")
            else:
                result["营业利润率(TTM)"] = None
        else:
            result["营业利润率(TTM)"] = None
            if data_op.errorcode != 0 or len(data_op.data) == 0:
                print(f"  ths_op_stock失败或无数据")
            elif not result.get("营收TTM(亿元)"):
                print(f"  营收TTM为空，无法计算")
        
        # 14. 经营现金流TTM(亿元)（公式：上年同季经营现金流 + 本报告期经营现金流）
        print("获取经营现金流TTM...")
        result["经营现金流TTM(亿元)"] = None
        for rd_ly, rd_cur in [(report_date_ly, report_date),
                              ((datetime.strptime(report_date, "%Y-%m-%d") - timedelta(days=365)).strftime("%Y-%m-%d"), report_date)]:
            data_sq = THS_BD(ticker, 'ths_sq_ncf_from_oa_stock', f'{rd_ly},100')
            data_ncf = THS_BD(ticker, 'ths_ncf_from_oa_stock', f'{rd_cur},100')
            if (data_sq.errorcode == 0 and len(data_sq.data) > 0 and
                data_ncf.errorcode == 0 and len(data_ncf.data) > 0):
                value_sq = data_sq.data.iloc[0, 1] if len(data_sq.data.columns) > 1 else data_sq.data.iloc[0, 0]
                value_ncf = data_ncf.data.iloc[0, 1] if len(data_ncf.data.columns) > 1 else data_ncf.data.iloc[0, 0]
                if pd.notna(value_sq) and pd.notna(value_ncf):
                    result["经营现金流TTM(亿元)"] = (float(value_sq) + float(value_ncf)) / 100000000
                    break
        
        # 15. 自由现金流TTM(亿元)
        print("获取自由现金流TTM...")
        data = THS_BD(ticker, 'ths_fcff_ttm_stock', f'{date_str},100')
        if data.errorcode == 0 and len(data.data) > 0:
            value = data.data.iloc[0, 1] if len(data.data.columns) > 1 else data.data.iloc[0, 0]
            result["自由现金流TTM(亿元)"] = float(value) / 100000000 if pd.notna(value) else None
        else:
            result["自由现金流TTM(亿元)"] = None
        
        # 报表日期备用：若主报表日无数据，可尝试上一季末
        report_dates_to_try = [report_date]
        _yr, _md = int(report_date[:4]), report_date[5:]
        if _md == "03-31":
            report_dates_to_try.append(f"{_yr - 1}-12-31")
        elif _md == "06-30":
            report_dates_to_try.append(f"{_yr}-03-31")
        elif _md == "09-30":
            report_dates_to_try.append(f"{_yr}-06-30")
        else:
            report_dates_to_try.append(f"{_yr}-09-30")

        def _ratio_or_none(data, col_idx=1):
            if data is None or data.errorcode != 0 or len(data.data) == 0:
                return None
            v = data.data.iloc[0, col_idx] if len(data.data.columns) > col_idx else data.data.iloc[0, 0]
            return float(v) if pd.notna(v) else None

        # 16. 资产负债率
        print("获取资产负债率...")
        result["资产负债率"] = None
        for rd in report_dates_to_try:
            data = THS_BD(ticker, 'ths_asset_liab_ratio_stock', rd)
            v = _ratio_or_none(data)
            if v is not None:
                result["资产负债率"] = v / 100
                break

        # 17. 有息负债
        print("获取有息负债...")
        result["有息负债"] = None
        for rd in report_dates_to_try:
            data_ratio = THS_BD(ticker, 'ths_ibdebt_ratio_asset_base_stock', rd)
            data_assets = THS_BD(ticker, 'ths_total_assets_stock', f'{rd},100')
            vr = _ratio_or_none(data_ratio)
            va = _ratio_or_none(data_assets)
            if vr is not None and va is not None:
                result["有息负债"] = (vr / 100 * va) / 100000000
                break

        # 18. EBITDA
        print("获取EBITDA...")
        result["EBITDA"] = None
        for rd in report_dates_to_try:
            data_ratio = THS_BD(ticker, 'ths_ebitda_to_total_liab_stock', rd)
            data_liab = THS_BD(ticker, 'ths_total_liab_stock', f'{rd},100')
            vr = _ratio_or_none(data_ratio)
            vl = _ratio_or_none(data_liab)
            if vr is not None and vl is not None:
                result["EBITDA"] = (vr * vl) / 100000000
                break

        # 19. 有息负债/EBITDA
        if result.get("有息负债") is not None and result.get("EBITDA") is not None and result["EBITDA"] != 0:
            result["有息负债/EBITDA"] = result["有息负债"] / result["EBITDA"]
        else:
            result["有息负债/EBITDA"] = None

        # 20. 应收/营收
        print("获取应收/营收...")
        result["应收/营收"] = None
        for rd in report_dates_to_try:
            data = THS_BD(ticker, 'ths_ar_to_revenue_stock', rd)
            v = _ratio_or_none(data)
            if v is not None:
                result["应收/营收"] = v / 100
                break

        # 21. 存货/营收
        print("获取存货/营收...")
        result["存货/营收"] = None
        if result.get("营收TTM(亿元)") is not None and result["营收TTM(亿元)"] != 0:
            for rd in report_dates_to_try:
                data_inv = THS_BD(ticker, 'ths_inventory_stock', f'{rd},100')
                vi = _ratio_or_none(data_inv)
                if vi is not None:
                    result["存货/营收"] = (vi / 100000000 / result["营收TTM(亿元)"])  # 小数，如 0.15 表示 15%
                    break
        
        # 22. 估值PE(TTM)
        print("获取估值PE(TTM)...")
        data = THS_BD(ticker, 'ths_pe_ttm_stock', f'{date_str},100')
        if data.errorcode == 0 and len(data.data) > 0:
            value = data.data.iloc[0, 1] if len(data.data.columns) > 1 else data.data.iloc[0, 0]
            result["估值PE(TTM)"] = float(value) if pd.notna(value) else None
        else:
            result["估值PE(TTM)"] = None
        
        # 23. 估值PB(LF)
        print("获取估值PB(LF)...")
        data = THS_BD(ticker, 'ths_pb_latest_stock', f'{date_str},100')
        if data.errorcode == 0 and len(data.data) > 0:
            value = data.data.iloc[0, 1] if len(data.data.columns) > 1 else data.data.iloc[0, 0]
            result["估值PB(LF)"] = float(value) if pd.notna(value) else None
        else:
            result["估值PB(LF)"] = None
        
        # 24. 估值EV/EBITDA(TTM)
        print("获取估值EV/EBITDA(TTM)...")
        data = THS_BD(ticker, 'ths_ev1_to_ebitda_stock', date_str)
        if data.errorcode == 0 and len(data.data) > 0:
            value = data.data.iloc[0, 1] if len(data.data.columns) > 1 else data.data.iloc[0, 0]
            result["估值EV/EBITDA(TTM)"] = float(value) if pd.notna(value) else None
        else:
            result["估值EV/EBITDA(TTM)"] = None
        
        # 25. 最近12M股价涨跌
        print("获取最近12M股价涨跌...")
        data = THS_BD(ticker, 'ths_chg_ratio_y_stock', f'{date_str},101')
        if data.errorcode == 0 and len(data.data) > 0:
            value = data.data.iloc[0, 1] if len(data.data.columns) > 1 else data.data.iloc[0, 0]
            result["最近12M股价涨跌"] = float(value) / 100 if pd.notna(value) else None
        else:
            result["最近12M股价涨跌"] = None
        
        # 26. 近1M成交额(亿元)
        print("获取近1M成交额...")
        data = THS_BD(ticker, 'ths_amt_m_stock', date_str)
        if data.errorcode == 0 and len(data.data) > 0:
            value = data.data.iloc[0, 1] if len(data.data.columns) > 1 else data.data.iloc[0, 0]
            result["近1M成交额(亿元)"] = float(value) / 100000000 if pd.notna(value) else None
        else:
            result["近1M成交额(亿元)"] = None
        
    except Exception as e:
        print(f"获取数据时发生错误: {e}")
        import traceback
        traceback.print_exc()
    
    return result

def main():
    """主函数"""
    # 登录
    if not login():
        return
    
    # 从文档中获取Ticker
    ticker = "688120.SH"
    date_str = datetime.now().strftime("%Y-%m-%d")
    
    print(f"\n开始获取股票数据: {ticker}")
    print(f"日期: {date_str}\n")
    
    # 获取数据
    result = get_stock_data(ticker, date_str)
    
    # 导出为JSON
    output_file = f"stock_indicators_{ticker.replace('.', '_')}_{datetime.now().strftime('%Y%m%d')}.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    
    print(f"\n数据已导出至: {output_file}")
    print("\n获取的数据:")
    print(json.dumps(result, ensure_ascii=False, indent=2))

if __name__ == '__main__':
    main()
