import httpx
import os
import json

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

    def get_access_token(self):
        url = f"{self.api_url}/oauth2/token"
        response = httpx.post(
            url,
            headers=self.headers,
            data={
                'grant_type': 'client_credentials',
                'appkey': _get_app_key(),
                'secretkey': _get_key()
            }
        )
        print('Code:', response.status_code)
        print('Header:', json.dumps({key: response.headers.get(key) for key in ['next-key', 'cont-yn', 'api-id']}, indent=4, ensure_ascii=False))
        print('Body:', json.dumps(response.json(), indent=4, ensure_ascii=False))
        response.raise_for_status()

        access_token_data = response.json()
        self.access_token = access_token_data.get('token')
        self.token_expiry = access_token_data.get('expires_dt')

        return response.json()  # test

    def revoke_access_token(self):
        if not self.access_token:
            return

        url = f"{self.api_url}/oauth2/revoke"
        response = httpx.post(
            url,
            headers=self.headers,
            data={
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
        response = httpx.post(
            url,
            headers=headers,
            data={
                'stk_cd': stock_code
            }
        )
        response.raise_for_status()
        
        return response.json()


'''
ka10001 주식기본정보요청 -> 월 1회? 시행. eps, roe, bps, 당기순이익, 영업이익, 매출액 등 정보. 
ka10005 주식일주월시분요청 -> 처음 한번 종가 저장
ka10095 관심종목정보요청 -> 매일 시행. 리스트 -> 종목코드, 종가, 시가총액, 일자
'''