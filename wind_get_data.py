# -*- coding: utf-8 -*-
"""
Created on Mon Dec 11 11:44:29 2017

@author: Rebecca Cui
----postil by kangyuqiang----
从万德下载数据
"""
import os
import datetime  #datetime
import pandas as pd
from windpy.WindPy import *

#--------------------------------根据需要修改下面的参数--------------------------
"""
market_cols_dict = 
{
'close': '收盘价','open': '开盘价','high','low','mkt_cap_ard','volume','pct_chg',
               'turn','west_netprofit_CAGR','roe_ttm2',
               'beta_100w','yoyeps_basic','free_float_shares','ATR',
               'industry_gicscode','pe_ttm','tot_liab','cap_stk','tot_assets','longdebttodebt'
}
"""

market_cols = ['close','open','high','low','mkt_cap_ard','volume','pct_chg',
               'turn','west_netprofit_CAGR','roe_ttm2',
               'beta_100w','yoyeps_basic','free_float_shares','ATR',
               'industry_gicscode','pe_ttm','tot_liab','cap_stk','tot_assets','longdebttodebt']

index_codes = ['000016.SH','000905.SH','000906.SH','000852.SH','000300.SH']
beginDate = '2017-07-05'
endDate = '2017-08-01'
# path = r'C:\\DELL\\internship\\CICC\\Barra\\raw data' # 结果储存路径
path = os.getcwd() + '/data_path' # 结果储存路径
#------------------------------------------------------------------------------
today = datetime.date.today().strftime('%Y-%m-%d')
#endDate = (datetime.date.today()-datetime.timedelta(days=1)).strftime('%Y-%m-%d')
w.start()

def GetMarketInfo(code,cols):
    """获得单只股票的数据
    
    Args:
        code (str): 股票代码
        cols (list-like): 需要返回的字段
    
    Returns:
        DataFrame: 返回取得的股票信息
    """   
    global beginDate
    # w.wss: 命令用来获取选定证券品种的历史截面数据，比如取沪深300只股票的2012年3季度的净利润财务指标数据。命令原型为：data= w.wss(品种代码,指标,可选参数)；
    # 股票上市时间
    ipoDate = w.wss(code,'ipo_date').Data[0][0].strftime('%Y-%m-%d')
    if ipoDate > beginDate:
        beginDate = ipoDate
    else:
        beginDate = beginDate
    # w.wsd 该命令用来获取选定证券品种的历史序列数据，包括日间的行情数据、基本面数据以及技术数据指标
    dailyQuota = w.wsd(code,','.join(cols),beginDate,endDate,"Fill=Previous")
    df = pd.DataFrame(dict(zip(cols,dailyQuota.Data)),index=dailyQuota.Times)
    return df

def Align(code,df):
    """调整数据
    
    Args:
        code (str): 股票代码
        df (DataFrame): 股票代码对应的数据.
    
    Returns:
        DataFrame: 调整好的 DataFrame
    """

    df.reset_index(inplace=True)
    df.rename(columns={'index':'datetime'},inplace=True)
    df['code'] = code
    # 复合index: level_0 => datetime, level_1 => code
    df.set_index(['datetime','code'],inplace=True)
    try:
        df['pct_chg'] /= 100
    except:
        print('无pct_chg字段')
    return df

def Concat(codes,cols):
    """获得多股票数据组合DataFrame
    
    Args:
        codes (list-like[code...]): 要取得数据的股票代码序列
        cols (list-like[col...]): 需要的字段名称
    
    Returns:
        DataFrame: 多股票数据组合DataFrame
    """

    dfs = pd.DataFrame()
    for code in codes:
        print('开始下载%s数据'%code)
        df_temp = GetMarketInfo(code,cols)
        df_temp = Align(code,df_temp)
        dfs = dfs.append(df_temp)
        dfs.to_pickle(path+r'\test')
    return dfs
# 通过wset取当前市场全部A股
stockSector = w.wset("sectorconstituent","date="+today+";sector=全部A股")
dates,codes,names = stockSector.Data
#股票数据
dfs = Concat(codes,market_cols)
pl = dfs.to_panel()
pl = pl.transpose(1,2,0)
pl.to_pickle(path+r'\data')
#pl.to_pickle(path+r'\stock')
#指数数据
#df_index = Concat(index_codes,['pct_chg'])
#df_index.to_csv(path+r'\stock_index.csv')
w.stop()



    
