from sqlite3 import Connection

from pykrx import stock
from pandas import DataFrame, Series
from datetime import datetime

from core.database import insert_kospi, insert_pykrx_by_stock, insert_pykrx_by_date,fetch_all_companies

from core.logger import get_logger
logger = get_logger(__name__)

def update_pykrx(conn: Connection, start: datetime, end: datetime, stock_codes: list[str]|str):
    if isinstance(stock_codes, str):
        stock_codes = [stock_codes]
    for stock_code in stock_codes:
        try:
            start_str = start.strftime('%Y%m%d')
            end_str = end.strftime('%Y%m%d')
            df = stock.get_market_ohlcv_by_date(start_str, end_str, stock_code)
            
            fundamental_df = stock.get_market_fundamental_by_date(start_str, end_str, stock_code)
            dps = fundamental_df['DPS']
            
            cap_df = stock.get_market_cap_by_date(start_str, end_str, stock_code)
            cap = cap_df['시가총액']
            
            df = df.join(dps, how='left').join(cap, how='left')
            insert_pykrx_by_stock(conn, stock_code, df)
            logger.info(f"Updated {stock_code} from {start_str} to {end_str}")
        except Exception as e:
            logger.error(f"Error fetching data for {stock_code}: {e}")
            continue

def update_daily(conn: Connection, date: datetime):
    assets = fetch_all_companies(conn).index.tolist()
    date_str = date.strftime('%Y%m%d')
    data = stock.get_market_ohlcv_by_ticker(date_str)
    if data.empty:
        logger.info(f"Skipped update for {date_str}")
        return  # Non business day
    fundamental_data = stock.get_market_fundamental_by_ticker(date_str)['DPS']

    df = data[data.index.isin(assets)]
    df = df.join(fundamental_data, how='left')
    insert_pykrx_by_date(conn, date, df)
    logger.info(f"Updated stock data in {date_str} for {len(df)} companies")
    
    update_kospi(conn, date, date)

def update_kospi(conn: Connection, start: datetime, end: datetime):
    try:
        start_str = start.strftime('%Y%m%d')
        end_str = end.strftime('%Y%m%d')
        df = stock.get_index_ohlcv_by_date(start_str, end_str, "1001")
        if df.empty:
            return
        insert_kospi(conn, df)
        if start == end:
            logger.info(f"Updated KOSPI data for {start_str}")
        else:
            logger.info(f"Updated KOSPI data from {start_str} to {end_str}")
        
    except Exception as e:
        logger.error(f"Error fetching KOSPI data: {e}")
