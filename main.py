
import sqlite3
from datetime import datetime

from dotenv import load_dotenv

import pandas as pd
from api import pykrx, dart_api
from core.database import *
from core.assets import get_assets
from tools.undervalued import find_undervalued_assets, get_recent_dps_and_growth, get_capm_required_return, dcf_alternative
from tools import update
from tools.utils import to_df

from pykrx import stock
from pprint import pprint
from api.dart_api import DartAPI

# def alter_db(conn: sqlite3.Connection):
#     cursor = conn.cursor()
#     cursor.execute('''
#         ALTER TABLE dart ADD COLUMN capital INTEGER DEFAULT 0
#     ''')
#     conn.commit()
    

if __name__ == "__main__":
    load_dotenv()
    init_db()
    
    conn = sqlite3.connect('data/database.db')
    # alter_db(conn)
    dart_api = DartAPI()
    
    today = datetime.today()
    start_date = (today - pd.DateOffset(years=3))
    start = start_date.strftime('%Y%m%d')
    end = today.strftime('%Y%m%d')
    assets = get_assets()
    
    # update.update_companies(conn, assets)
    companies = fetch_companies(conn, assets)
    # pprint(companies)
    # update.update_dart(dart_api, conn, today.year-1, companies)
    # update.init_pykrx(conn, start, end, companies)
    # update.update_pykrx(conn, today)

    # dart_api.get_corp_code()
    

    # pprint(fetch_stock_year(conn, today.year - 1))
    # pprint(fetch_stock_year(conn, today.year - 2))
    # pprint(fetch_stock_year(conn, today.year - 3))

    
    
    # pprint(get_capm_required_return(conn, companies, start, end))
    # pprint(dcf_alternative(conn, companies, today, 0.03))

    undervalued_assets = find_undervalued_assets(conn, companies, start_date, today)
    # undervalued_true = undervalued_assets[undervalued_assets['undervalued'] == True]
    undervalued_true = undervalued_assets[undervalued_assets['undervalued'] == True]
    print(undervalued_true)
    undervalued_assets = undervalued_true.index.tolist()

    # from tools import portfolio
    # result = portfolio.optimize_portfolio(conn, undervalued_assets, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10], start_date, today, rf=0.03)
    # # portfolio.graph_lambda(conn, result['lambda_results'], undervalued_assets)
    # portfolio.graph_sharpe(conn, result['sharpe'], undervalued_assets)

    
    
    # from tools.undervalued import dcf_alternative
    # dcf_alternative(conn, today, 0.03)

    conn.close()