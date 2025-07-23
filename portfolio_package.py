# algo_portfolio/utils.py

from pandas.tseries.offsets import BDay
import pandas as pd


def adjust_to_business_day(date_str):
    """
    지정한 날짜가 영업일이 아니면 가장 가까운 이전 영업일로 보정
    """
    date = pd.to_datetime(date_str)
    while date.weekday() >= 5:  # 토요일(5), 일요일(6)
        date -= BDay(2)
    return date.strftime("%Y%m%d")

# 💡 저장하면 됩니다. 이 파일을 `algo_portfolio/utils.py`로 저장해 주세요.
# 다른 파일들도 차례대로 작성할 것이니, 준비되셨으면 "계속"이라고 입력해 주세요!
