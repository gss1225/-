import json
import os
from datetime import datetime
from sqlite3 import Connection
from typing import Literal

from api.dart_api import DartAPI
from api.kiwoom_api import KiwoomAPI
from api.pykrx import get_stock_day_pykrx, get_init_stock_day_pykrx, get_kospi
from core import database
from core.assets import get_assets
from core.database import insert_kospi, insert_stock_day
from core.schemas import Company, Kospi, StockDay, StockYear
from tools.utils import to_int, to_float

from core.logger import get_logger
logger = get_logger(__name__)

def init_stock(conn: Connection, source: Literal['pykrx', 'kiwoom'], dart_api=None, kiwoom_api=None):
    if source == 'kiwoom' and kiwoom_api is None:
        raise ValueError("kiwoom_api must be provided when source is 'kiwoom'")
    assets = get_assets()
    today = datetime.today()
    start_date = today.replace(year=today.year - 3)
    start = start_date.strftime('%Y%m%d')
    end = today.strftime('%Y%m%d')

    
    corpcode_path = os.path.join('data', 'corpcode.json')
    if not os.path.exists(corpcode_path):
        if dart_api is not None:
            dart_api.get_corp_code()
        else:
            logger.warning('dart_api is None, cannot fetch corp code.')

    update_companies(conn, assets)
    companies = database.fetch_companies(conn, assets)
    update_dart(dart_api, conn, today.year-1, companies)
    if source == 'pykrx':
        init_pykrx(conn, start, end, companies)
    elif source == 'kiwoom':
        pass  # init_kiwoom(kiwoom_api, conn, start, end, companies)
    else:
        raise ValueError(f"Unknown source: {source}")
    
    kospi_data = get_kospi(start, end)
    insert_kospi(conn, kospi_data)
    logger.info(f'KOSPI 정보 저장: {start}~{end}, {len(kospi_data)}건')

def update_day(conn: Connection, source: Literal['pykrx', 'kiwoom'], kiwoom_api=None):
    date = datetime.today().strftime('%Y%m%d')
    logger.info(f'주식 정보 갱신 시작: {date}, source={source}')
    if source == 'kiwoom' and kiwoom_api is None:
        raise ValueError("kiwoom_api must be provided when source is 'kiwoom'")
    if source == 'pykrx':
        update_pykrx(conn, date, database.fetch_all_companies(conn))
    elif source == 'kiwoom':
        update_kiwoom(kiwoom_api, conn, date, database.fetch_all_companies(conn))
    else:
        raise ValueError(f"Unknown source: {source}")

def init_pykrx(conn: Connection, start: str, end: str, companies: list[Company]):
    for company in companies:
        stock_data = get_init_stock_day_pykrx(start, end, company.stock_code)
        insert_stock_day(conn, stock_data)
        logger.info(f'초기 주식 정보 저장: {company.name}, {start}~{end}, {len(stock_data)}건')

def init_kiwoom(kiwoom_api: KiwoomAPI, conn: Connection, start: str, end: str, companies: list[Company]):
    pass

def update_pykrx(conn: Connection, date: str, companies: list[Company]):
    stock_data = get_stock_day_pykrx(date, companies)
    if stock_data:
        insert_stock_day(conn, stock_data)
        kospi_data = get_kospi(date, date)
        insert_kospi(conn, kospi_data)
        logger.info(f'주식 정보 갱신: {date}')
    else:
        logger.info(f'갱신할 주식 정보 없음: {date}')
        
def update_kiwoom(kiwoom_api: KiwoomAPI, conn: Connection, date: str, companies: list[Company]):
    pass

def update_companies(conn: Connection, assets: list[str]):
    with open('data/corpcode.json', 'r', encoding='utf-8') as f:
        company_data = json.load(f)

    companies = []
    for company in company_data['list']:
        if company['stock_code'] in assets:
            companies.append(Company(
                stock_code=company['stock_code'],
                name=company['corp_name'],
                corp_code=company['corp_code']
            ))

    database.insert_companies(conn, companies)
    logger.info(f"Inserted {len(companies)} companies into the database.")
        
def update_dart(dart_api: DartAPI, conn: Connection, year: int, companies: list[Company], update_prev = False):
    for company in companies:
        div_info = dart_api.get_div_info(company.corp_code, year)['list']
        fin_info = dart_api.get_fin_info(company.corp_code, year)['list']

        try:
            fin_data = next(filter(lambda x: x['ord'] == "21", fin_info))
        except StopIteration:
            logger.warning(f"CFS 자본총계를 찾을 수 없음: {company.name} - {year}")
            fin_data = next(filter(lambda x: x['ord'] == "22", fin_info), None)
        if fin_data is None:
            logger.warning(f"자본총계를 찾을 수 없음, 건너뜀: {company.name} - {year}")
            continue

        data = {
            'capital': to_int(fin_data['thstrm_amount']),  # B0
            'capital_prev': to_int(fin_data['frmtrm_amount']),
            'capital_pprev': to_int(fin_data['bfefrmtrm_amount']),
        }
        
        profit_data = next(filter(lambda x: x['se'] == '(연결)당기순이익(백만원)', div_info), None)
        if profit_data is None:
            logger.warning(f"당기순이익 정보를 찾을 수 없음: {company.name} - {year}")
            data['net_profit'] = 0
            data['net_profit_prev'] = 0
            data['net_profit_pprev'] = 0
        else:
            data['net_profit'] = to_int(profit_data['thstrm'])*1000000
            data['net_profit_prev'] = to_int(profit_data['frmtrm'])*1000000
            data['net_profit_pprev'] = to_int(profit_data['lwfr'])*1000000

        div_data = next(filter(lambda x: x['se'] == '주당 현금배당금(원)', div_info), None)
        if div_data is None:
            logger.warning(f"배당금 정보를 찾을 수 없음: {company.name} - {year}")
            data['dps'] = 0.0
            data['dps_prev'] = 0.0
            data['dps_pprev'] = 0.0
        else:
            data['dps'] = to_float(div_data['thstrm'])
            data['dps_prev'] = to_float(div_data['frmtrm'])
            data['dps_pprev'] = to_float(div_data['lwfr'])
            if stk_knd := div_data.get('stock_knd'):  # 보통주, 우선주 구분이 없는 경우
                if stk_knd != '보통주' and stk_knd != '보통주식':
                    logger.warning(f'보통주 배당금 정보를 찾을 수 없음, {stk_knd} 이용함: {company.name} - {year}')

        insert_data = []
        insert_data.append(StockYear(company.stock_code, year, data['net_profit'], data['capital'], data['dps']))
        if update_prev:
            insert_data.append(StockYear(company.stock_code, year-1, data['net_profit_prev'], data['capital_prev'], data['dps_prev']))
            insert_data.append(StockYear(company.stock_code, year-2, data['net_profit_pprev'], data['capital_pprev'], data['dps_pprev']))

        database.insert_stock_year(conn, insert_data)
        logger.info(f"DART 정보 갱신: {company.name}, 기준년도: {year}")