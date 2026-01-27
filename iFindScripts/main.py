from datetime import datetime
from dateutil.relativedelta import relativedelta

from iFinDPy import *
THS_iFinDLogin('FHT166','andy0424')
date = '2025-05-25'

# 计算报表起始时间
def calculate_modified_date(input_date):
    #计算给定日期的前11天 + 前3个自然月的日期
    if isinstance(input_date, str):
        input_date = datetime.strptime(input_date, "%Y-%m-%d")
    date_minus_11_days = input_date - relativedelta(days=12)
    final_date = date_minus_11_days - relativedelta(months=3)
    return final_date

startDate = calculate_modified_date(date).strftime('%Y-%m-%d')
endDate = (datetime.strptime(date, "%Y-%m-%d") - relativedelta(days=1)).strftime('%Y-%m-%d')
print(startDate, endDate)

# US/HK Glance
# Index
SP_500 = 'SPX.GI'
NASDAQ = '@CCO.NQI'
Russell200 = 'IWL.P'
HSCI = 'HSCI.HK'
HSCIIT = 'HSCIIT.HK'
# Column
Index_Points = 'ths_close_price_index'
Index_Change = 'ths_chg_ratio_index'
Index_Weekly = 'ths_chg_ratio_w_index'
Index_LQ = 'ths_chg_ratio_nd_index'
Index_YTD = 'ths_chg_ratio_y_index'
Index_LTM = 'ths_chg_ratio_nd_index'

# data1 = THS_DS(f'{SP_500},{NASDAQ},{Russell200},{HSCI},{HSCIIT}',f'{Index_Points};{Index_Change};{Index_Weekly};{Index_LQ};{Index_YTD}',';;;-90;','block:history',date,date)
data1 = THS_DS(f'{SP_500},{NASDAQ},{HSCI},{HSCIIT}',f'{Index_Points};{Index_Change};{Index_Weekly};{Index_LQ};{Index_YTD}',';;;-90;','block:history',date,date)
# data2 = THS_DS(f'{SP_500},{NASDAQ},{Russell200},{HSCI},{HSCIIT}',f'{Index_LTM}','-365','block:history',date,date)
print(data1.data)
# print(data2.data)


# 基于Excel表


#经济数据库(EDB)-全球经济-美国:国债收益率:1个月;美国:国债收益率:3个月;美国:国债收益率:6个月;美国:国债收益率:1年;美国:国债收益率:2年;美国:国债收益率:5年等-iFinD数据接口
usBond = THS_EDB('G002600766;G002600767;G002600768;G002600769;G002600770;G002600772;G002600774;G002600775;G002600776','','2025-07-01','2025-07-02')
print(usBond.data)