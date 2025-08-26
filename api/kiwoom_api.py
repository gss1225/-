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

        return response.json()  # test

    def revoke_access_token(self):
        if not self.access_token:
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


'''
ka10001 주식기본정보요청 -> 월 1회? 시행. eps, roe, bps, 당기순이익, 영업이익, 매출액 등 정보. 
ka10005 주식일주월시분요청 -> 처음 한번 종가, 거래량 저장
ka10095 관심종목정보요청 -> 매일 시행. 리스트 -> 종목코드, 종가, 시가총액, 일자, 거래량

내 자산총액 -> 비중으로 나눈다 = 투자금액
일단 구매는 시장가로
'''