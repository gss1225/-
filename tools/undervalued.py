from datetime import datetime

import pandas as pd
import numpy as np
from sqlite3 import Connection

from core import database
from core.schemas import Company
from tools.utils import to_df

# TODO fix to reuse df
def find_undervalued_assets(conn: Connection, companies: list[Company], start_date: datetime, end_date: datetime):
    start = start_date.strftime('%Y%m%d')
    end = end_date.strftime('%Y%m%d')
    company_df = to_df(companies, 'stock_code')
    dps_info = get_recent_dps_and_growth(conn, end_date.year)
    capm_info = get_capm_required_return(conn, companies, start, end)
    
    df = company_df.join(dps_info, how='left').join(capm_info, how='left')
    df['fair_value'] = df.apply(lambda row: get_ggm_fair_value(
        row['dps'], row['growth'], row['required_return']), axis=1)
    
    df['current_price'] = to_df(database.fetch_stock_day_by_date(conn, end), 'stock_code', ['close_price'])  # technically close price
    alt_df = dcf_alternative(conn, companies, end_date)
    df = df.join(alt_df[['V', 'market_cap']], how='left')

    df['undervalued'] = (df['current_price'] < df['fair_value']) | (df['V'] > df['market_cap'])

    return df

def get_recent_dps_and_growth(conn: Connection, year: int) -> pd.DataFrame:
    '''
    제작년 및 작년 배당금 이용
    '''
    dps_df = pd.DataFrame()

    dps_df['dps'] = to_df(database.fetch_stock_year(conn, year-1), 'stock_code', ['dps'])
    dps_df['dps_prev'] = to_df(database.fetch_stock_year(conn, year-2), 'stock_code', ['dps'])

    dps_df['growth'] = dps_df.apply(lambda row: (row['dps'] / row['dps_prev']) - 1 if row['dps_prev'] > 0 else 0.0, axis=1)

    return dps_df

def get_capm_required_return(conn: Connection, companies: list[Company], start: str, end: str, rf: float = 0.03):
    '''
    Returns a DataFrame indexed by 'stock_code' with required return for each stock_code using CAPM.
    DataFrame columns:
        - market_return: annualized market return
        - required_return: required return
    '''
    results = []
    for company in companies:
        stock_code = company.stock_code
        stock_price = to_df(database.fetch_stock_day_by_stock(conn, stock_code, start, end), 'date', ['close_price']).sort_index()
        stock_ret = stock_price['close_price'].pct_change()

        kospi_price = to_df(database.fetch_kospi(conn, start, end), 'date', ['close_price'])
        if kospi_price.empty:
            raise ValueError(f"KOSPI data is empty for {start} to {end}")
        kospi_ret = kospi_price['close_price'].pct_change()

        # Align both Series on their common dates
        common_dates = stock_ret.index.intersection(kospi_ret.index)
        stock_ret_aligned = stock_ret.loc[common_dates]
        kospi_ret_aligned = kospi_ret.loc[common_dates]

        aligned = pd.concat([stock_ret_aligned, kospi_ret_aligned], axis=1, keys=["stock", "market"]).dropna()
        if aligned.empty:
            print(f"{stock_code} data empty")
            continue

        mkt_mean_daily = aligned["market"].mean()
        mkt_annual = mkt_mean_daily * 252

        cov = np.cov(aligned["stock"], aligned["market"])[0, 1]
        var_mkt = aligned["market"].var()
        beta = cov / var_mkt

        required_return = rf + beta * (mkt_annual - rf)
        results.append({
            "stock_code": stock_code,
            "market_return": mkt_annual,  # 필요없음?
            "required_return": required_return
        })
    return pd.DataFrame(results).set_index("stock_code")

def get_ggm_fair_value(recent_dps: float, g: float, r: float):
    if r <= g:  # 배당금 없거나 신생기업의 경우 g == 0.0으로 설정됨
        return np.nan

    fair_value = recent_dps * (1 + g) / (r - g)
    return fair_value

def dcf_alternative(conn: Connection, companies: list[Company], date: datetime, r: float = 0.03):
    data = to_df(companies, 'stock_code')
    market_cap = to_df(database.fetch_stock_day_by_date(conn, date.strftime('%Y%m%d')), 'stock_code', ['market_cap'])
    if market_cap.empty:
        raise ValueError(f"시가총액 정보가 없음 - {date}")
    stock_data = to_df(database.fetch_stock_year(conn, date.year-1), 'stock_code', ['capital', 'net_profit'])
    net_profit_pprev = to_df(database.fetch_stock_year(conn, date.year-3), 'stock_code', ['net_profit'])
    df = data.join(market_cap).join(stock_data).join(net_profit_pprev, rsuffix='_pprev')
    
    g = np.sqrt(df['net_profit'] / df['net_profit_pprev']) - 1
    ni = df['net_profit'] * g
    df['V'] = (df['capital'] + (ni - r * df['capital']) / (r - g))
    
    return df

'''
net_profit is only for last year data
g = sqrt(net_profit / net_profit_pprev) - 1
NI(예상 올해 당기순이익) = net_profit * g
V = B0 + (NI - r * B0) / (r - g)
return V / marketcap (시가총액)
'''