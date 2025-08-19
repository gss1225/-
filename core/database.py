import sqlite3
import pandas as pd
from datetime import datetime

def init_db():
    conn = sqlite3.connect('database.db')
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
            open_price REAL NOT NULL,
            high_price REAL NOT NULL,
            low_price REAL NOT NULL,
            close_price REAL NOT NULL,
            volume INTEGER NOT NULL
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS pykrx (
            stock_code NOT NULL,
            date DATETIME NOT NULL,
            open_price REAL NOT NULL,
            high_price REAL NOT NULL,
            low_price REAL NOT NULL,
            close_price REAL NOT NULL,
            volume INTEGER NOT NULL,
            cap INTEGER NOT NULL,
            dps REAL NOT NULL,
            PRIMARY KEY (stock_code, date)
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS dart (
            corp_code NOT NULL,
            year INTEGER NOT NULL,
            net_profit INTEGER NOT NULL,
            net_profit_prev INTEGER NOT NULL,
            net_profit_pprev INTEGER NOT NULL,
            dps REAL NOT NULL,
            dps_prev REAL NOT NULL,
            dps_pprev REAL NOT NULL,
            PRIMARY KEY (corp_code, year)
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS kiwoom (
            stock_code TEXT NOT NULL,
            date DATETIME NOT NULL,
            fav TEXT,
            cap TEXT,
            flo_stk TEXT,
            crd_rt TEXT,
            mac TEXT,
            mac_wght TEXT,
            for_ext_rt TEXT,
            repl_pric TEXT,
            per TEXT,
            eps TEXT,
            roe TEXT,
            pbr TEXT,
            ev TEXT,
            bps TEXT,
            sale_amt TEXT,
            bus_pro TEXT,
            cup_nga TEXT,
            high_pric TEXT,
            open_pric TEXT,
            low_pric TEXT,
            upl_pric TEXT,
            lst_pric TEXT,
            base_pric TEXT,
            exp_cntr_pric TEXT,
            exp_ectr_qty TEXT,
            cur_pric TEXT,
            pre_sig TEXT,
            pred_pre TEXT,
            flu_rt TEXT,
            trde_qty TEXT,
            trde_pre TEXT,
            fav_unit TEXT,
            dstr_stk TEXT,
            dstr_rt TEXT,
            PRIMARY KEY (stock_code, date)
        )
    '''
    )
    
    
    conn.commit()
    conn.close()

def insert_companies(conn: sqlite3.Connection, assets: list[str], names: list[str], corp_codes: list[str]):
    cursor = conn.cursor()
    cursor.executemany('''
        INSERT INTO companies (stock_code, name, corp_code)
        VALUES (?, ?, ?)
        ON CONFLICT(stock_code) DO UPDATE SET
            name = excluded.name,
            corp_code = excluded.corp_code
    ''', zip(assets, names, corp_codes))
    conn.commit()
    
def insert_pykrx_by_stock(conn: sqlite3.Connection, stock_code: str, df: pd.DataFrame):
    cursor = conn.cursor()
    for date, row in df.iterrows():
        date_str = date.strftime('%Y-%m-%d')
        cursor.execute('''
            INSERT INTO pykrx (stock_code, date, open_price, high_price, low_price, close_price, volume, cap, dps)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(stock_code, date) DO UPDATE SET
                open_price = excluded.open_price,
                high_price = excluded.high_price,
                low_price = excluded.low_price,
                close_price = excluded.close_price,
                volume = excluded.volume,
                cap = excluded.cap,
                dps = excluded.dps
        ''', (stock_code, date_str, row['시가'], row['고가'], row['저가'], row['종가'], row['거래량'], row['시가총액'], row['DPS']))
    conn.commit()
    
def insert_pykrx_by_date(conn: sqlite3.Connection, date: datetime, df: pd.DataFrame):
    cursor = conn.cursor()
    date_str = date.strftime('%Y-%m-%d')
    for stock_code, row in df.iterrows():
        cursor.execute('''
            INSERT INTO pykrx (stock_code, date, open_price, high_price, low_price, close_price, volume, cap, dps)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(stock_code, date) DO UPDATE SET
                open_price = excluded.open_price,
                high_price = excluded.high_price,
                low_price = excluded.low_price,
                close_price = excluded.close_price,
                volume = excluded.volume,
                cap = excluded.cap,
                dps = excluded.dps
        ''', (stock_code, date_str, row['시가'], row['고가'], row['저가'], row['종가'], row['거래량'], row['시가총액'], row['DPS']))
    conn.commit()

def insert_kospi(conn: sqlite3.Connection, df: pd.DataFrame):
    cursor = conn.cursor()
    for date, row in df.iterrows():
        date_str = date.strftime('%Y-%m-%d')
        try:
            cursor.execute('''
                INSERT INTO kospi (date, open_price, high_price, low_price, close_price, volume)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(date) DO UPDATE SET
                    open_price = excluded.open_price,
                    high_price = excluded.high_price,
                    low_price = excluded.low_price,
                    close_price = excluded.close_price,
                    volume = excluded.volume
            ''', (date_str, row['시가'], row['고가'], row['저가'], row['종가'], row['거래량']))
        except sqlite3.IntegrityError:
            continue
    conn.commit()

def insert_dart(conn: sqlite3.Connection, corp_code: str, year: int, data: dict):
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO dart (corp_code, year, net_profit, net_profit_prev, net_profit_pprev, dps, dps_prev, dps_pprev)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(corp_code, year) DO UPDATE SET
            net_profit = excluded.net_profit,
            net_profit_prev = excluded.net_profit_prev,
            net_profit_pprev = excluded.net_profit_pprev,
            dps = excluded.dps,
            dps_prev = excluded.dps_prev,
            dps_pprev = excluded.dps_pprev
    ''', (corp_code, year, data['net_profit'], data['net_profit_prev'], data['net_profit_pprev'], data['dps'], data['dps_prev'], data['dps_pprev']))
    conn.commit()

def fetch_closest_date(conn: sqlite3.Connection, date: datetime, stock_code: str|None = None) -> datetime|None:
    cursor = conn.cursor()
    date_str = date.strftime('%Y-%m-%d')
    if stock_code:
        cursor.execute('SELECT date FROM pykrx WHERE stock_code = ? AND date >= ? ORDER BY date ASC LIMIT 1', (stock_code, date_str))
    else:
        cursor.execute('SELECT date FROM pykrx WHERE date >= ? ORDER BY date ASC LIMIT 1', (date_str,))
    row = cursor.fetchone()
    if row:
        return datetime.strptime(row[0], '%Y-%m-%d')
    else:
        return None
 
def fetch_all_companies(conn: sqlite3.Connection) -> pd.DataFrame:
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM companies')
    rows = cursor.fetchall()
    df = pd.DataFrame(rows, columns=['stock_code', 'name', 'corp_code'])
    df.set_index('stock_code', inplace=True)
    return df

def fetch_companies(conn: sqlite3.Connection, assets: list[str]) -> pd.DataFrame:
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM companies WHERE stock_code IN ({})'.format(','.join('?' for _ in assets)), assets)
    rows = cursor.fetchall()
    df = pd.DataFrame(rows, columns=['stock_code', 'name', 'corp_code'])
    df.set_index('stock_code', inplace=True)
    return df

def fetch_kospi_data(conn: sqlite3.Connection, start: datetime, end: datetime) -> pd.DataFrame:
    cursor = conn.cursor()
    start_str = start.strftime('%Y-%m-%d')
    end_str = end.strftime('%Y-%m-%d')
    cursor.execute('SELECT * FROM kospi WHERE date BETWEEN ? AND ?', (start_str, end_str))
    rows = cursor.fetchall()
    df = pd.DataFrame(rows, columns=['date', 'open_price', 'high_price', 'low_price', 'close_price', 'volume'])
    df['date'] = pd.to_datetime(df['date'])
    df.set_index('date', inplace=True)
    return df

def fetch_dart_from_year(conn: sqlite3.Connection, year: int) -> pd.DataFrame:
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM dart WHERE year = ?', (year, ))
    rows = cursor.fetchall()
    df = pd.DataFrame(rows, columns=['corp_code', 'year', 'net_profit', 'net_profit_prev', 'net_profit_pprev', 'dps', 'dps_prev', 'dps_pprev'])
    df.set_index(['corp_code'], inplace=True)
    return df

def fetch_dps_from_date_pykrx(conn: sqlite3.Connection, date: datetime) -> pd.Series:
    cursor = conn.cursor()
    date_str = date.strftime('%Y-%m-%d')
    cursor.execute('SELECT stock_code, dps FROM pykrx WHERE date = ?', (date_str,))
    rows = cursor.fetchall()
    df = pd.DataFrame(rows, columns=['stock_code', 'dps'])
    df.set_index('stock_code', inplace=True)
    return df['dps']

def fetch_close_price_from_stock_pykrx(conn: sqlite3.Connection, stock_code: str, start: datetime, end: datetime) -> pd.Series:
    cursor = conn.cursor()
    start_str = start.strftime('%Y-%m-%d')
    end_str = end.strftime('%Y-%m-%d')
    cursor.execute('SELECT date, close_price FROM pykrx WHERE stock_code = ? AND date BETWEEN ? AND ?', (stock_code, start_str, end_str))
    rows = cursor.fetchall()
    df = pd.DataFrame(rows, columns=['date', 'close_price'])
    df['date'] = pd.to_datetime(df['date'])
    df.set_index('date', inplace=True)
    return df['close_price']

def fetch_close_price_from_date_pykrx(conn: sqlite3.Connection, date: datetime) -> pd.Series:
    cursor = conn.cursor()
    date_str = date.strftime('%Y-%m-%d')
    cursor.execute('SELECT stock_code, close_price FROM pykrx WHERE date = ?', (date_str,))
    rows = cursor.fetchall()
    df = pd.DataFrame(rows, columns=['stock_code', 'close_price'])
    df.set_index('stock_code', inplace=True)
    return df['close_price']

def fetch_cap_pykrx(conn: sqlite3.Connection, date: datetime) -> pd.Series:
    cursor = conn.cursor()
    date_str = date.strftime('%Y-%m-%d')
    cursor.execute('SELECT stock_code, cap FROM pykrx WHERE date = ?', (date_str,))
    rows = cursor.fetchall()
    df = pd.DataFrame(rows, columns=['stock_code', 'cap'])
    df.set_index('stock_code', inplace=True)
    return df['cap']
