import httpx
import os
import json
import zipfile
import xml.etree.ElementTree as ET
from io import BytesIO

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