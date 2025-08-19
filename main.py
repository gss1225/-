
import sqlite3
from datetime import datetime

from dotenv import load_dotenv

import pandas as pd
from api import pykrx, dart_api
from core.database import *
from core.assets import get_assets
from tools.undervalued import find_undervalued_assets, get_recent_dps_and_growth, get_capm_required_return

from pykrx import stock
from pprint import pprint
from api.dart_api import DartAPI, update_companies

if __name__ == "__main__":
    load_dotenv()
    init_db()
    
    conn = sqlite3.connect('database.db')
    dart_api = DartAPI()
    
    today = datetime.today()
    start_date = (today - pd.DateOffset(years=3))
    assets = get_assets()
    
    # update_companies(conn, assets)
    company_df = fetch_companies(conn, assets)
    # print(company_df)
    corp_codes = company_df['corp_code'].tolist()

    # dart_api.get_corp_code()
    # dart_api.update_dart(conn, today.year-1, corp_codes)

    # print(len(fetch_dart_by_year(conn, today.year - 1)))

    # pykrx.update_pykrx(conn, start_date, today, assets)
    # pykrx.update_kospi(conn, start_date, today)
    
    # print(fetch_kospi_data(conn, start_date, today))
    # print(fetch_close_price_by_date_pykrx(conn, today))
    # print(get_recent_dps_and_growth(conn, today.year))
    # print(get_capm_required_return(conn, assets, start_date, today))

    undervalued_assets = find_undervalued_assets(conn, start_date, today)
    undervalued_true = undervalued_assets[undervalued_assets['undervalued'] == True]
    print(undervalued_true)
    undervalued_assets = undervalued_true.index.tolist()

    from tools import portfolio
    result = portfolio.optimize_portfolio(conn, undervalued_assets, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10], start_date, today, rf=0.03)
    portfolio.graph_lambda(conn, result['lambda_results'], undervalued_assets)
    portfolio.graph_sharpe(conn, result['sharpe'], undervalued_assets)

    
    # from tools.undervalued import get_unnamed
    # print(get_unnamed(conn, today, 0.03))
    
    # pykrx.update_daily(conn, today)

    conn.close()