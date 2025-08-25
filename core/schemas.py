from dataclasses import dataclass

@dataclass
class Company:
    stock_code: str
    name: str
    corp_code: str

@dataclass
class Kospi:
    date: str
    close_price: int
    trade_qty: int

@dataclass
class StockDay:
    stock_code: str
    date: str
    close_price: int
    trade_qty: int
    market_cap: int|None
    stock_count: int|None

@dataclass
class StockYear:
    stock_code: str
    year: int
    net_profit: int
    capital: int
    dps: float

