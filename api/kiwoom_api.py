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
            'api-id': 'kt00001'
        }
        response = self._post(
            url,
            headers=headers,
            json={
                'qry_tp': '3', # 조회구분 3:추정조회, 2:일반조회
	        }
        )
        response.raise_for_status()
        return response.json()

    def get_account_stock_info(self):
        if not self.access_token or self.token_expiry is None:
            self.get_access_token()

        url = f"{self.api_url}/api/dostk/acnt"
        headers = {
            **self.headers,
            'authorization': f"Bearer {self.access_token}",
            'api-id': 'kt00018'
        }
        response = self._post(
            url,
            headers=headers,
            json={
                'qry_tp': '1',
                'dmst_stex_tp': 'KRX'
            }
        )
        response.raise_for_status()
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
                'ord_qty': str(amount), # 주문수량
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
                'ord_qty': str(amount), # 주문수량
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

def parse_account_stock_info(data):
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

def parse_account_info(data):
    # Top-level field mapping
    top_map = {
        "entr": "예수금",
        "profa_ch": "주식증거금현금",
        "bncr_profa_ch": "수익증권증거금현금",
        "nxdy_bncr_sell_exct": "익일수익증권매도정산대금",
        "fc_stk_krw_repl_set_amt": "해외주식원화대용설정금",
        "crd_grnta_ch": "신용보증금현금",
        "crd_grnt_ch": "신용담보금현금",
        "add_grnt_ch": "추가담보금현금",
        "etc_profa": "기타증거금",
        "uncl_stk_amt": "미수확보금",
        "shrts_prica": "공매도대금",
        "crd_set_grnta": "신용설정평가금",
        "chck_ina_amt": "수표입금액",
        "etc_chck_ina_amt": "기타수표입금액",
        "crd_grnt_ruse": "신용담보재사용",
        "knx_asset_evltv": "코넥스기본예탁금",
        "elwdpst_evlta": "ELW예탁평가금",
        "crd_ls_rght_frcs_amt": "신용대주권리예정금액",
        "lvlh_join_amt": "생계형가입금액",
        "lvlh_trns_alowa": "생계형입금가능금액",
        "repl_amt": "대용금평가금액(합계)",
        "remn_repl_evlta": "잔고대용평가금액",
        "trst_remn_repl_evlta": "위탁대용잔고평가금액",
        "bncr_remn_repl_evlta": "수익증권대용평가금액",
        "profa_repl": "위탁증거금대용",
        "crd_grnta_repl": "신용보증금대용",
        "crd_grnt_repl": "신용담보금대용",
        "add_grnt_repl": "추가담보금대용",
        "rght_repl_amt": "권리대용금",
        "pymn_alow_amt": "출금가능금액",
        "wrap_pymn_alow_amt": "랩출금가능금액",
        "ord_alow_amt": "주문가능금액",
        "bncr_buy_alowa": "수익증권매수가능금액",
        "20stk_ord_alow_amt": "20%종목주문가능금액",
        "30stk_ord_alow_amt": "30%종목주문가능금액",
        "40stk_ord_alow_amt": "40%종목주문가능금액",
        "100stk_ord_alow_amt": "100%종목주문가능금액",
        "ch_uncla": "현금미수금",
        "ch_uncla_dlfe": "현금미수연체료",
        "ch_uncla_tot": "현금미수금합계",
        "crd_int_npay": "신용이자미납",
        "int_npay_amt_dlfe": "신용이자미납연체료",
        "int_npay_amt_tot": "신용이자미납합계",
        "etc_loana": "기타대여금",
        "etc_loana_dlfe": "기타대여금연체료",
        "etc_loan_tot": "기타대여금합계",
        "nrpy_loan": "미상환융자금",
        "loan_sum": "융자금합계",
        "ls_sum": "대주금합계",
        "crd_grnt_rt": "신용담보비율",
        "mdstrm_usfe": "중도이용료",
        "min_ord_alow_yn": "최소주문가능금액",
        "loan_remn_evlt_amt": "대출총평가금액",
        "dpst_grntl_remn": "예탁담보대출잔고",
        "sell_grntl_remn": "매도담보대출잔고",
        "d1_entra": "d+1추정예수금",
        "d1_slby_exct_amt": "d+1매도매수정산금",
        "d1_buy_exct_amt": "d+1매수정산금",
        "d1_out_rep_mor": "d+1미수변제소요금",
        "d1_sel_exct_amt": "d+1매도정산금",
        "d1_pymn_alow_amt": "d+1출금가능금액",
        "d2_entra": "d+2추정예수금",
        "d2_slby_exct_amt": "d+2매도매수정산금",
        "d2_buy_exct_amt": "d+2매수정산금",
        "d2_out_rep_mor": "d+2미수변제소요금",
        "d2_sel_exct_amt": "d+2매도정산금",
        "d2_pymn_alow_amt": "d+2출금가능금액",
        "50stk_ord_alow_amt": "50%종목주문가능금액",
        "60stk_ord_alow_amt": "60%종목주문가능금액",
    }
    # Nested stk_entr mapping
    stk_entr_map = {
        "crnc_cd": "통화코드",
        "fx_entr": "외화예수금",
        "fc_krw_repl_evlta": "원화대용평가금",
        "fc_trst_profa": "해외주식증거금",
        "pymn_alow_amt": "출금가능금액",
        "pymn_alow_amt_entr": "출금가능금액(예수금)",
        "ord_alow_amt_entr": "주문가능금액(예수금)",
        "fc_uncla": "외화미수(합계)",
        "fc_ch_uncla": "외화현금미수금",
        "dly_amt": "연체료",
        "d1_fx_entr": "d+1외화예수금",
        "d2_fx_entr": "d+2외화예수금",
        "d3_fx_entr": "d+3외화예수금",
        "d4_fx_entr": "d+4외화예수금",
    }
    result = {}
    for k, v in top_map.items():
        if k in data:
            result[v] = data[k]
    # Handle nested stk_entr list
    if "stk_entr" in data and isinstance(data["stk_entr"], list):
        result["종목별예수금"] = [
            {stk_entr_map.get(ik, ik): item[ik] for ik in item if ik in stk_entr_map}
            for item in data["stk_entr"]
        ]
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