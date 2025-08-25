
import pandas as pd
from sqlite3 import Connection

from pykrx import stock
from datetime import datetime

from core.schemas import Company, Kospi, StockDay

from core.logger import get_logger
logger = get_logger(__name__)

def get_init_stock_day_pykrx(start: str, end: str, stock_code: str) -> list[StockDay]:
    try:
        df = stock.get_market_ohlcv_by_date(start, end, stock_code)
        cap_df = stock.get_market_cap_by_date(start, end, stock_code)
    except Exception as e:
        logger.error(f"pykrx 주식 정보 조회 주 오류. {stock_code}: {e}")
        return []
    cap = cap_df['시가총액']
    df = df.join(cap, how='left')
    stock_days = []
    for date, row in df.iterrows():
        stock_days.append(StockDay(
            stock_code=stock_code,
            date=date.strftime('%Y%m%d'),
            close_price=row['종가'],
            trade_qty=row['거래량'],
            market_cap=row['시가총액'],
            stock_count=None
        ))
    return stock_days

def get_stock_day_pykrx(date: str, companies: list[Company]) -> list[StockDay]:
    data = stock.get_market_ohlcv_by_ticker(date)
    if data.empty:
        logger.info(f"주식 데이터 없음: {date}")
        return []  # Non business day
    
    assets = [company.stock_code for company in companies]
    df = data[data.index.isin(assets)]
    stock_days = []
    for stock_code, row in df.iterrows():
        stock_days.append(StockDay(
            stock_code=stock_code,
            date=date,
            close_price=row['종가'],
            trade_qty=row['거래량'],
            market_cap=row['시가총액'],
            stock_count=None
        ))
    
    return stock_days

def get_kospi(start: str, end: str) -> list[Kospi]:
    df = stock.get_index_ohlcv_by_date(start, end, "1001")

    return [Kospi(
        date=date.strftime('%Y%m%d'),
        close_price=row['종가'],
        trade_qty=row['거래량']
    ) for date, row in df.iterrows()]