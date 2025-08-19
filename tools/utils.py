from datetime import datetime
import pandas as pd

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
    if num.isdigit():
        return int(num)
    else:
        return 0

def to_float(n: str):
    num = n.replace(',', '')
    try:
        return float(num)
    except ValueError:
        return 0.0