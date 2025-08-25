import dataclasses
import pandas as pd
from datetime import datetime


def get_quater(year: int):
    today = datetime.today()
    if today.month >= 10:
        return datetime(year, 10, 1)
    elif today.month >= 7:
        return datetime(year, 7, 1)
    elif today.month >= 4:
        return datetime(year, 4, 1)
    else:
        return datetime(year, 1, 1)

def to_int(n: str):
    num = n.replace(',', '')
    try:
        return int(num)
    except ValueError:
        return 0

def to_float(n: str):
    num = n.replace(',', '')
    try:
        return float(num)
    except ValueError:
        return 0.0

def to_df(data: list, index: str = None, columns: list[str] = None):
    '''
    Converts list of DataClasses to DataFrames
    '''
    df = pd.DataFrame([dataclasses.asdict(d) for d in data])
    if df.empty:
        raise ValueError("데이터가 없음")
    if index:
        df.set_index(index, inplace=True)
    if columns:
        df = df[columns]
    return df
