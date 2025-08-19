import httpx
import os
import json
import zipfile
import xml.etree.ElementTree as ET

from io import BytesIO
from sqlite3 import Connection

from core.database import insert_dart, insert_companies, fetch_dart_from_year
from tools.utils import to_int, to_float

from core.logger import get_logger
logger = get_logger(__name__)

def _get_key():
    return os.getenv('DART_API_KEY')

class DartAPI:
    def __init__(self):
        pass

    def get_corp_code(self):
        url = 'https://opendart.fss.or.kr/api/corpCode.xml'
        params = {
            'crtfc_key': _get_key()
        }
        response = httpx.get(url, params=params)
        response.raise_for_status()

        # The response is a zip file, extract CORPCODE.xml
        with zipfile.ZipFile(BytesIO(response.content)) as zf:
            with zf.open('CORPCODE.xml') as xml_file:
                xml_content = xml_file.read()

        root = ET.fromstring(xml_content)


        def xml_to_dict(element):
            # If element has children, process them
            children = list(element)
            if children:
                result = {}
                for child in children:
                    child_result = xml_to_dict(child)
                    tag = child.tag
                    # If tag already exists, convert to list
                    if tag in result:
                        if not isinstance(result[tag], list):
                            result[tag] = [result[tag]]
                        result[tag].append(child_result)
                    else:
                        result[tag] = child_result
                return result
            else:
                # If no children, return text
                return element.text.strip() if element.text else None

        data_dict = xml_to_dict(root)

        os.makedirs('data', exist_ok=True)

        with open('data/corpcode.json', 'w', encoding='utf-8') as f:
            json.dump(data_dict, f, ensure_ascii=False, indent=4)
        logger.info("Fetched and saved corp codes from DART API.")

    def get_div_info(self, corp_code: str, year: int):
        url = 'https://opendart.fss.or.kr/api/alotMatter.json'
        params = {
            'crtfc_key': _get_key(),
            'corp_code': corp_code,
            'bsns_year': str(year),
            'reprt_code': '11011'
        }
        response = httpx.get(url, params=params)
        response.raise_for_status()

        return response.json()

    def get_fin_info(self, corp_code: str, year: int):
        url = 'https://opendart.fss.or.kr/api/fnlttSinglAcnt.json'
        params = {
            'crtfc_key': _get_key(),
            'corp_code': corp_code,
            'bsns_year': str(year),
            'reprt_code': '11011'
        }
        response = httpx.get(url, params=params)
        response.raise_for_status()

        return response.json()

    def update_dart(self, conn: Connection, year: int, corp_codes: list[str]):
        existing_data = fetch_dart_from_year(conn, year)
        for corp_code in corp_codes:
            if corp_code in existing_data.index:
                logger.info(f"Skipping {corp_code} for year {year}, already exists in database.")
                continue
            div_info = self.get_div_info(corp_code, year)['list']
            fin_info = self.get_fin_info(corp_code, year)['list']

            try:
                fin_data = next(filter(lambda x: x['ord'] == "21", fin_info))
            except StopIteration:
                logger.warning(f"CFS 자본총계를 찾을 수 없음: {corp_code} - {year}")
                fin_data = next(filter(lambda x: x['ord'] == "22", fin_info), None)
            if fin_data is None:
                logger.warning(f"자본총계를 찾을 수 없음: {corp_code} - {year}")
                continue

            data = {
                'net_profit': to_int(fin_data['thstrm_amount']),  # B0
                'net_profit_prev': to_int(fin_data['frmtrm_amount']),
                'net_profit_pprev': to_int(fin_data['bfefrmtrm_amount'])
            }
            
            
            for div in div_info:
                if div['se'] == '주당 현금배당금(원)':
                    if (stock_knd := div.get('stock_knd')) is not None:
                        if stock_knd == '보통주' or stock_knd == '보통주식':
                            data['dps'] = to_float(div['thstrm'])
                            data['dps_prev'] = to_float(div['frmtrm'])
                            data['dps_pprev'] = to_float(div['lwfr'])
                            break
                        else:
                            continue  # 우선주가 먼저 발견될 경우 (아마도 없음)
                    else:  # 주식 구분 안됨, 배당금 없을 가능성 높음
                        data['dps'] = to_float(div['thstrm'])
                        data['dps_prev'] = to_float(div['frmtrm'])
                        data['dps_pprev'] = to_float(div['lwfr'])
                        break
            else:
                logger.warning(f"보통주 정보를 불러올 수 없었음: {corp_code} - {year}")
                div = next(filter(lambda x: x['se'] == '주당 현금배당금(원)', div_info))
                data['dps'] = to_float(div['thstrm'])
                data['dps_prev'] = to_float(div['frmtrm'])
                data['dps_pprev'] = to_float(div['lwfr'])

            # Process and insert the data into the database
            insert_dart(conn, corp_code, year, data)
            logger.info(f"Inserted DART data for {corp_code} in {year}")

def update_companies(conn: Connection, assets: list[str]):
    with open('data/corpcode.json', 'r', encoding='utf-8') as f:
        company_data = json.load(f)
        
    stock_codes = []
    names = []
    corp_codes = []
    for company in company_data['list']:
        if company['stock_code'] in assets:
            stock_codes.append(company['stock_code'])
            names.append(company['corp_name'])
            corp_codes.append(company['corp_code'])
            
    insert_companies(conn, stock_codes, names, corp_codes)
    logger.info(f"Inserted {len(stock_codes)} companies into the database.")

'''
배당에 관한 사항 개발가이드
https://opendart.fss.or.kr/api/alotMatter.json

[   
    {
        "rcept_no":"20190401004781",
        "corp_cls":"Y",
        "corp_code":"00126380",
        "corp_name":"삼성전자",
        "se":"(연결)당기순이익(백만원)",
        "thstrm":"43,890,877",
        "frmtrm":"41,344,569",
        "lwfr":"22,415,655",
        "stlm_dt":"2018-12-31"
    }
    ...
    {
        "rcept_no":"20190401004781",
        "corp_cls":"Y",
        "corp_code":"00126380",
        "corp_name":"삼성전자",
        "se":"주당 현금배당금(원)",
        "stock_knd":"보통주",
        "thstrm":"1,416",
        "frmtrm":"42,500",
        "lwfr":"28,500",
        "stlm_dt":"2018-12-31"
    },
    {
        "rcept_no":"20190401004781",
        "corp_cls":"Y",
        "corp_code":"00126380",
        "corp_name":"삼성전자",
        "se":"주당 현금배당금(원)",
        "stock_knd":"우선주",
        "thstrm":"1,417",
        "frmtrm":"42,550",
        "lwfr":"28,550",
        "stlm_dt":"2018-12-31"
    },
    ...
]



정기보고서 재무정보
https://opendart.fss.or.kr/api/fnlttSinglAcnt.json

[
    ...
    {
        "rcept_no":"20190401004781",
        "reprt_code":"11011",
        "bsns_year":"2018",
        "corp_code":"00126380",
        "stock_code":"005930",
        "fs_div":"CFS",
        "fs_nm":"연결재무제표",
        "sj_div":"BS",
        "sj_nm":"재무상태표",
        "account_nm":"자본총계",
        "thstrm_nm":"제 50 기",
        "thstrm_dt":"2018.12.31 현재",
        "thstrm_amount":"247,753,177,000,000",  # B0
        "frmtrm_nm":"제 49 기",
        "frmtrm_dt":"2017.12.31 현재",
        "frmtrm_amount":"214,491,428,000,000",
        "bfefrmtrm_nm":"제 48 기",
        "bfefrmtrm_dt":"2016.12.31 현재",
        "bfefrmtrm_amount":"192,963,033,000,000",
        "ord":"21",
        "currency":"KRW"
    },
    ...
]
     
    
'''