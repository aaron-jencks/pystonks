import datetime as dt

from pystonks.apis.sql import SQL_DT_FMT


class SymbolData:
    def __init__(self, symbol: str):
        self.symbol = symbol


class HistoricalData(SymbolData):
    def __init__(self, symbol: str, timestamp: dt.datetime):
        super().__init__(symbol)
        self.timestamp = timestamp


class Bar(HistoricalData):
    def __init__(self, symbol: str, timestamp: dt.datetime, op: float, cl: float, h: float, lw: float, volume: int):
        super().__init__(symbol, timestamp)
        self.open = op
        self.close = cl
        self.high = h
        self.low = lw
        self.volume = volume

    def zero(self) -> bool:
        return self.open == 0 and self.close == 0 and self.low == 0 and self.volume == 0

    def tradeless(self) -> bool:
        return self.volume == 0

    def __repr__(self) -> str:
        return (f'BAR: {{ o: {self.open}, c: {self.close}, h: {self.high}, l: {self.low}, v: {self.volume}, '
                f'ts: {self.timestamp.isoformat()} }}')


class Trade(HistoricalData):
    def __init__(self, symbol: str, timestamp: dt.datetime, exchange: str, count: int, price: float):
        super().__init__(symbol, timestamp)
        self.exchange = exchange
        self.count = count
        self.price = price


class Quote(SymbolData):
    def __init__(self, symbol: str,
                 ask_exchange: str, ask_size: int, ask_price: float,
                 bid_exchange: str, bid_size: int, bid_price: float):
        super().__init__(symbol)
        self.ask_exchange = ask_exchange
        self.ask_size = ask_size
        self.ask_price = ask_price
        self.bid_exchange = bid_exchange
        self.bid_size = bid_size
        self.bid_price = bid_price


class HistoricalQuote(HistoricalData):
    def __init__(self, symbol: str, timestamp: dt.datetime,
                 ask_exchange: str, ask_size: int, ask_price: float,
                 bid_exchange: str, bid_size: int, bid_price: float):
        super().__init__(symbol, timestamp)
        self.ask_exchange = ask_exchange
        self.ask_size = ask_size
        self.ask_price = ask_price
        self.bid_exchange = bid_exchange
        self.bid_size = bid_size
        self.bid_price = bid_price


class News(HistoricalData):
    def __init__(self, symbol: str, timestamp: dt.datetime,
                 news_id: int, author: str, headline: str, url: str, updated: dt.datetime):
        super().__init__(symbol, timestamp)
        self.news_id = news_id
        self.author = author
        self.headline = headline
        self.url = url
        self.updated_at = updated

    def __repr__(self):
        return (f'{self.headline[:10]} By: {self.author} '
                f'@ {self.timestamp.isoformat()} ({self.updated_at.isoformat()})')


class TickerMeta:
    def __init__(self, symbol: str, date: dt.datetime, fl: int, cp: float, op: float, cso: float):
        self.symbol = symbol
        self.date = date
        self.float = fl
        self.current_price = cp
        self.open = op
        self.change_since_open = cso
