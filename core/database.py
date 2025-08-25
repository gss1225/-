import sqlite3
import pandas as pd

from datetime import datetime
from core.schemas import Company, Kospi, StockDay, StockYear

def init_db():
    conn = sqlite3.connect('data/database.db')
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS companies (
            stock_code TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            corp_code TEXT NOT NULL
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS kospi (
            date DATETIME PRIMARY KEY,
            close_price INTEGER NOT NULL,
            trade_qty INTEGER NOT NULL
        )
    ''')

    #  종목코드, 날짜(YYYYMMDD), 종가, 거래량, 시가총액, 주식수
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS stock_daily (
            stock_code TEXT NOT NULL,
            date DATETIME NOT NULL,
            close_price INTEGER NOT NULL,
            trade_qty INTEGER NOT NULL,
            market_cap INTEGER,
            stock_count INTEGER,
            PRIMARY KEY (stock_code, date)
        )
    '''
    )

    # 종목코드, 연도, 당기순이익, 자본, 배당금
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS stock_year (
            stock_code TEXT NOT NULL,
            year INTEGER NOT NULL,
            net_profit INTEGER NOT NULL,
            capital INTEGER NOT NULL,
            dps REAL NOT NULL,
            PRIMARY KEY (stock_code, year)
        )
    ''')
    
    conn.commit()
    conn.close()

# INSERT (UPDATE)
def insert_companies(conn: sqlite3.Connection, data: list[Company]):
    cursor = conn.cursor()
    cursor.executemany('''
        INSERT INTO companies (stock_code, name, corp_code)
        VALUES (?, ?, ?)
        ON CONFLICT(stock_code) DO UPDATE SET
            name = excluded.name,
            corp_code = excluded.corp_code
    ''', [(d.stock_code, d.name, d.corp_code) for d in data])
    conn.commit()

def insert_kospi(conn: sqlite3.Connection, data: list[Kospi]):
    cursor = conn.cursor()
    cursor.executemany('''
        INSERT INTO kospi (date, close_price, trade_qty)
        VALUES (?, ?, ?)
        ON CONFLICT(date) DO UPDATE SET
            close_price = excluded.close_price,
            trade_qty = excluded.trade_qty
    ''', [(d.date, d.close_price, d.trade_qty) for d in data])
    conn.commit()
    
def insert_stock_day(conn: sqlite3.Connection, data: list[StockDay]):
    cursor = conn.cursor()
    cursor.executemany('''
        INSERT INTO stock_daily (stock_code, date, close_price, trade_qty, market_cap, stock_count)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(stock_code, date) DO UPDATE SET
            close_price = excluded.close_price,
            trade_qty = excluded.trade_qty,
            market_cap = excluded.market_cap,
            stock_count = excluded.stock_count
    ''', [(d.stock_code, d.date, d.close_price, d.trade_qty, d.market_cap, d.stock_count) for d in data])
    conn.commit()

def insert_stock_year(conn: sqlite3.Connection, data: list[StockYear]):
    cursor = conn.cursor()
    cursor.executemany(
        '''
        INSERT INTO stock_year (stock_code, year, net_profit, capital, dps)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(stock_code, year) DO UPDATE SET
            net_profit = excluded.net_profit,
            capital = excluded.capital,
            dps = excluded.dps
    ''', [(d.stock_code, d.year, d.net_profit, d.capital, d.dps) for d in data])
    conn.commit()

def fetch_closest_date(conn: sqlite3.Connection, date: str, stock_code: str|None = None) -> datetime|None:
    cursor = conn.cursor()
    if stock_code:
        cursor.execute('SELECT date FROM stock_day WHERE stock_code = ? AND date >= ? ORDER BY date ASC LIMIT 1', (stock_code, date))
    else:
        cursor.execute('SELECT date FROM stock_day WHERE date >= ? ORDER BY date ASC LIMIT 1', (date,))
    row = cursor.fetchone()
    if row:
        return row[0]
    else:
        return None
 
def fetch_all_companies(conn: sqlite3.Connection) -> list[Company]:
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM companies')
    rows = cursor.fetchall()
    return [Company(*row) for row in rows]

def fetch_companies(conn: sqlite3.Connection, assets: list[str]) -> list[Company]:
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM companies WHERE stock_code IN ({})'.format(','.join('?' for _ in assets)), assets)
    rows = cursor.fetchall()
    return [Company(*row) for row in rows]

def fetch_kospi(conn: sqlite3.Connection, start: str, end: str) -> list[Kospi]:
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM kospi WHERE date BETWEEN ? AND ?', (start, end))
    rows = cursor.fetchall()
    return [Kospi(*row) for row in rows]

def fetch_stock_day_by_stock(conn: sqlite3.Connection, stock_code: str, start: str, end: str) -> list[StockDay]:
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM stock_daily WHERE stock_code = ? AND date BETWEEN ? AND ?', (stock_code, start, end))
    rows = cursor.fetchall()
    return [StockDay(*row) for row in rows]

def fetch_stock_day_by_date(conn: sqlite3.Connection, date: str) -> list[StockDay]:
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM stock_daily WHERE date = ?', (date,))
    rows = cursor.fetchall()
    return [StockDay(*row) for row in rows]

def fetch_stock_year(conn: sqlite3.Connection, year: int) -> list[StockYear]:
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM stock_year WHERE year = ?', (year, ))
    rows = cursor.fetchall()
    return [StockYear(*row) for row in rows]

