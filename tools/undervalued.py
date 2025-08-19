from datetime import datetime

import pandas as pd
import numpy as np
from sqlite3 import Connection

from tools.utils import get_quater
from core import database

def find_undervalued_assets(conn: Connection, start_date: datetime, end_date: datetime):
    companies = database.fetch_all_companies(conn)
    dps_info, dps_dates = get_recent_dps_and_growth(conn, end_date.year)
    capm_info = get_capm_required_return(conn, companies.index.tolist(), start_date, end_date)
    
    df = companies.join(dps_info, how='left').join(capm_info, how='left')
    df['fair_value'] = df.apply(lambda row: get_ggm_fair_value(
        row['current_dps'], row['growth'], row['required_return']), axis=1)
    
    df['current_price'] = database.fetch_close_price_from_date_pykrx(conn, end_date)

    # logic TODO
    df['undervalued'] = df['current_price'] < df['fair_value']  # ggm
    
    return df

def get_recent_dps_and_growth(conn: Connection, year: int) -> tuple[pd.DataFrame, list[datetime]]:
    '''
    Returns
     - dataframe for recent DPS and growth rate for the last two years.
        index: stock_code
        columns: previous_dps, current_dps, growth
     - queried dates for dps data. year of the dates is the same as the column names.
    '''
    dps_df = pd.DataFrame()

    current = database.fetch_closest_date(conn, get_quater(year))
    previous = database.fetch_closest_date(conn, get_quater(year - 1))
    if current is None or previous is None:
        raise ValueError(f'No data for {year} or {year - 1}')

    dates = [current, previous]
    dps_df['current_dps'] = database.fetch_dps_from_date_pykrx(conn, current)
    dps_df['previous_dps'] = database.fetch_dps_from_date_pykrx(conn, previous)

    dps_df = dps_df.fillna(0.0)
    dps_df['growth'] = dps_df.apply(lambda row: (row['current_dps'] / row['previous_dps']) - 1 if row['previous_dps'] > 0 else 0.0, axis=1)

    return dps_df, dates

def get_capm_required_return(conn: Connection, stock_codes: list[str], start_date: datetime, end_date: datetime, rf: float = 0.03):
    '''
    Returns a DataFrame with required return for each stock_code using CAPM.
    DataFrame columns:
        - stock_code: stock stock_code
        - market_return: annualized market return
        - required_return: required return
    '''
    results = []
    for stock_code in stock_codes:
        stock_price = database.fetch_close_price_from_stock_pykrx(conn, stock_code, start_date, end_date)
        stock_ret = stock_price.pct_change()

        kospi_price = database.fetch_kospi_data(conn, start_date, end_date)['close_price']
        if kospi_price.empty:
            raise ValueError(f"KOSPI data is empty for {start_date} to {end_date}")
        kospi_ret = kospi_price.pct_change()

        aligned = pd.concat([stock_ret, kospi_ret], axis=1, keys=["stock", "market"]).dropna()
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

def get_unnamed(conn: Connection, date: datetime, r: float = 0.1):
    data = database.fetch_all_companies(conn)
    market_cap = database.fetch_cap_pykrx(conn, date)
    dart_data = database.fetch_dart_from_year(conn, date.year-1)
    
    data = data.join(market_cap).join(dart_data, on='corp_code')
    
    g = np.sqrt(data['net_profit'] / data['net_profit_pprev']) - 1
    ni = data['net_profit'] * g
    v = data['net_profit'] + (ni - r * data['net_profit']) / (r - g)
    return v / data['cap']  # 시가총액과 비교하는것

'''
net_profit is only for last year data
g = sqrt(net_profit / net_profit_pprev) - 1
NI(예상 올해 당기순이익) = net_profit * g
V = B0 + (NI - r * B0) / (r - g)
return V / marketcap (시가총액)
'''