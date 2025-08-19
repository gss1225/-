from api.kiwoom_api import KiwoomAPI
from pprint import pprint

kiwoom = KiwoomAPI()
pprint(kiwoom.get_access_token())
pprint(kiwoom.get_stock_info('005930'))  # Example stock_code for Samsung Electronics
pprint(kiwoom.revoke_access_token())