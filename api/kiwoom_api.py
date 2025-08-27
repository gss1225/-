import httpx
import os
import json

import time

from core.logger import get_logger
logger = get_logger(__name__)

def _get_app_key():
    return os.getenv('KIWOOM_APP_KEY')

def _get_key():
    return os.getenv('KIWOOM_API_KEY')

class KiwoomAPI:
    def __init__(self, api_url: str='https://api.kiwoom.com'):
        self.api_url = api_url
        self.headers = {
            'Content-Type': 'application/json;charset=UTF-8'
        }
        self.token_expiry = None
        self.access_token = None

        self._last_api_call = None

    def _post(self, url, **kwargs):
        # Rate limit of 5 per sec, 0.25 for safety
        min_interval = 0.25
        now = time.time()
        if self._last_api_call is not None:
            elapsed = now - self._last_api_call
            if elapsed < min_interval:
                time.sleep(min_interval - elapsed)
        response = httpx.post(url, **kwargs)
        self._last_api_call = time.time()
        return response

    def get_access_token(self):
        url = f"{self.api_url}/oauth2/token"
        response = self._post(
            url,
            headers=self.headers,
            json={
                'grant_type': 'client_credentials',
                'appkey': _get_app_key(),
                'secretkey': _get_key()
            }
        )
        response.raise_for_status()

        access_token_data = response.json()
        self.access_token = access_token_data.get('token')
        self.token_expiry = access_token_data.get('expires_dt')
        
        if not self.access_token or not self.token_expiry:
            logger.error(f"Failed to get access token: {response.status_code} {response.text}")
            raise httpx.HTTPError(f"Failed to get access token: {response.status_code} {response.text}")
        logger.info(f"접근토큰 발급: 만료시각 - {self.token_expiry}")

        return response.json()

    def revoke_access_token(self):
        if not self.access_token:
            logger.log('No token')
            return

        url = f"{self.api_url}/oauth2/revoke"
        response = self._post(
            url,
            headers=self.headers,
            json={
                'appkey': _get_app_key(),
                'secretkey': _get_key(),
                'token': self.access_token
            }
        )
        response.raise_for_status()
        self.access_token = None
        self.token_expiry = None
        
        logger.info("접근토큰 폐기")

        return response.json()  # test

    def get_stock_info(self, stock_code: str):
        if not self.access_token or self.token_expiry is None:
            self.get_access_token()

        url = f"{self.api_url}/api/dostk/stkinfo"
        headers = {
            **self.headers,
            'authorization': f"Bearer {self.access_token}",
            'api-id': 'ka10001'
        }
        response = self._post(
            url,
            headers=headers,
            json={
                'stk_cd': stock_code
            }
        )
        response.raise_for_status()
        
        return response.json()

    def get_account_info(self):
        if not self.access_token or self.token_expiry is None:
            self.get_access_token()

        url = f"{self.api_url}/api/dostk/acnt"
        headers = {
            **self.headers,
            'authorization': f"Bearer {self.access_token}",
            'api-id': 'kt10018'
        }
        response = self._post(
            url,
            headers=headers,
            json={
                'qry_tp': 1,
                'dmst_stex_tp': 'KRX'
            }
        )
        response.raise_for_status()
        from pprint import pprint
        pprint(response.json())
        return response.json()

    def order(self, stock_code, amount):
        if not self.access_token or self.token_expiry is None:
            self.get_access_token()

        url = f'{self.api_url}/api/dostk/ordr'
        headers = {
            **self.headers,
            'authorization': f"Bearer {self.access_token}",
            'api-id': 'kt10000'
        }
        logger.info(f'주식 매수: {stock_code}, 수량: {amount}')
        response = self._post(
            url,
            headers=headers,
            json={
                'dmst_stex_tp': 'KRX', # 국내거래소구분 KRX,NXT,SOR
                'stk_cd': stock_code, # 종목코드
                'ord_qty': amount, # 주문수량
                'ord_uv': '', # 주문단가
                'trde_tp': '3', # 매매구분 0:보통 , 3:시장가 , 5:조건부지정가 , 81:장마감후시간외 , 61:장시작전시간외, 62:시간외단일가 , 6:최유리지정가 , 7:최우선지정가 , 10:보통(IOC) , 13:시장가(IOC) , 16:최유리(IOC) , 20:보통(FOK) , 23:시장가(FOK) , 26:최유리(FOK) , 28:스톱지정가,29:중간가,30:중간가(IOC),31:중간가(FOK)
                'cond_uv': '', # 조건단가
            }
        )

        logger.info(f'Order response: {response.status_code} {response.text}')
        response.raise_for_status()
        return response.json()

    def sell(self, stock_code, amount):
        if not self.access_token or self.token_expiry is None:
            self.get_access_token()

        url = f'{self.api_url}/api/dostk/ordr'
        headers = {
            **self.headers,
            'authorization': f"Bearer {self.access_token}",
            'api-id': 'kt10001'
        }
        logger.info(f'주식 매도: {stock_code}, 수량: {amount}')
        response = self._post(
            url,
            headers=headers,
            json={
                'dmst_stex_tp': 'KRX', # 국내거래소구분 KRX,NXT,SOR
                'stk_cd': stock_code, # 종목코드
                'ord_qty': amount, # 주문수량
                'ord_uv': '', # 주문단가
                'trde_tp': '3', # 매매구분 0:보통 , 3:시장가 , 5:조건부지정가 , 81:장마감후시간외 , 61:장시작전시간외, 62:시간외단일가 , 6:최유리지정가 , 7:최우선지정가 , 10:보통(IOC) , 13:시장가(IOC) , 16:최유리(IOC) , 20:보통(FOK) , 23:시장가(FOK) , 26:최유리(FOK) , 28:스톱지정가,29:중간가,30:중간가(IOC),31:중간가(FOK)
                'cond_uv': '', # 조건단가
            }
        )

        logger.info(f'Order response: {response.status_code} {response.text}')
        response.raise_for_status()
        return response.json()

    def ongoing_orders(self):
        if not self.access_token or self.token_expiry is None:
            self.get_access_token()

        url = f'{self.api_url}/api/dostk/acnt'
        headers = {
            **self.headers,
            'authorization': f"Bearer {self.access_token}",
            'api-id': 'ka10075'
        }
        response = self._post(
            url,
            headers=headers,
            json={
                'all_stk_tp': '1', # 전체종목구분 0:전체, 1:종목
                'trde_tp': '0', # 매매구분 0:전체, 1:매도, 2:매수
                'stk_cd': '005930', # 종목코드 
                'stex_tp': '0', # 거래소구분 0 : 통합, 1 : KRX, 2 : NXT
            }
        )

        response.raise_for_status()
        return response.json()

def parse_account_info(data):
    """
    Parse Kiwoom account info response into a dict with Korean keys.
    """
    # Top-level field mapping
    top_map = {
        'tot_pur_amt': '총매입금액',
        'tot_evlt_amt': '총평가금액',
        'tot_evlt_pl': '총평가손익금액',
        'tot_prft_rt': '총수익률(%)',
        'prsm_dpst_aset_amt': '추정예탁자산',
        'tot_loan_amt': '총대출금',
        'tot_crd_loan_amt': '총융자금액',
        'tot_crd_ls_amt': '총대주금액',
    }
    # Holdings list field mapping
    item_map = {
        'stk_cd': '종목번호',
        'stk_nm': '종목명',
        'evltv_prft': '평가손익',
        'prft_rt': '수익률(%)',
        'pur_pric': '매입가',
        'pred_close_pric': '전일종가',
        'rmnd_qty': '보유수량',
        'trde_able_qty': '매매가능수량',
        'cur_prc': '현재가',
        # 'pred_buyq': '전일매수수량',
        # 'pred_sellq': '전일매도수량',
        # 'tdy_buyq': '금일매수수량',
        # 'tdy_sellq': '금일매도수량',
        # 'pur_amt': '매입금액',
        # 'pur_cmsn': '매입수수료',
        # 'evlt_amt': '평가금액',
        # 'sell_cmsn': '평가수수료',
        # 'tax': '세금',
        # 'sum_cmsn': '수수료합',
        'poss_rt': '보유비중(%)',
        # 'crd_tp': '신용구분',
        # 'crd_tp_nm': '신용구분명',
        # 'crd_loan_dt': '대출일',
    }
    result = {}
    # Map top-level fields
    for k, v in top_map.items():
        if k in data:
            result[v] = data[k]
    # Map holdings list
    holdings = []
    items = data.get('acnt_evlt_remn_indv_tot', [])
    for item in items:
        mapped = {item_map.get(ik, ik): item[ik] for ik in item_map if ik in item}
        holdings.append(mapped)
    result['계좌평가잔고개별합산'] = holdings
    return result

def parse_ongoing_order(data):
    item_map = {

    }

'''
ka10001 주식기본정보요청 -> 월 1회? 시행. eps, roe, bps, 당기순이익, 영업이익, 매출액 등 정보. 
ka10005 주식일주월시분요청 -> 처음 한번 종가, 거래량 저장
ka10095 관심종목정보요청 -> 매일 시행. 리스트 -> 종목코드, 종가, 시가총액, 일자, 거래량

내 자산총액 -> 비중으로 나눈다 = 투자금액
일단 구매는 시장가로
'''