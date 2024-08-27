from abc import ABC, abstractmethod
import datetime as dt
from typing import List, Dict

from alpaca.data import TimeFrame

from pystonks.models import Trade, HistoricalQuote, Quote, Bar, News, TickerMeta


class TradingAPI(ABC):
    @abstractmethod
    def buy(self, symbol: str, quantity: int, price: float):
        pass

    @abstractmethod
    def sell(self, symbol: str, count: int, price: float):
        pass

    @abstractmethod
    def balance(self) -> float:
        pass

    @abstractmethod
    def shares(self, symbol: str) -> int:
        pass


class HistoricalMarketDataAPI(ABC):
    @abstractmethod
    def was_market_open(self, date: dt.datetime) -> bool:
        pass

    @abstractmethod
    def historical_bars(self, symbol: str, start: dt.datetime, dur: dt.timedelta,
                        buckets: TimeFrame = TimeFrame.Minute) -> List[Bar]:
        pass

    @abstractmethod
    def historical_trades(self, symbol: str, start: dt.datetime, dur: dt.timedelta) -> List[Trade]:
        pass

    @abstractmethod
    def historical_quotes(self, symbol: str, start: dt.datetime, dur: dt.timedelta) -> List[HistoricalQuote]:
        pass

    @abstractmethod
    def historical_news(self, symbol: str, start: dt.datetime, dur: dt.timedelta) -> List[News]:
        pass


class MarketDataAPI(HistoricalMarketDataAPI, ABC):
    @abstractmethod
    def is_market_open(self) -> bool:
        pass

    @abstractmethod
    def bars(self, symbol: str, buckets: TimeFrame = TimeFrame.Minute) -> List[Bar]:
        pass

    @abstractmethod
    def trades(self, symbol: str) -> List[Trade]:
        pass

    @abstractmethod
    def quotes(self, symbol: str) -> Quote:
        pass

    @abstractmethod
    def news(self, symbol: str) -> List[News]:
        pass


class SymbolDataAPI(ABC):
    @abstractmethod
    def get_ticker_symbols(self, timestamp: dt.datetime) -> List[str]:
        pass

    @abstractmethod
    def get_float(self, symbol: str) -> int:
        pass

    @abstractmethod
    def get_floats(self, symbols: List[str]) -> Dict[str, int]:
        pass

    @abstractmethod
    def get_ticker(self, symbol: str) -> TickerMeta:
        pass

    @abstractmethod
    def historical_ticker(self, symbol: str, start: dt.datetime, dur: dt.timedelta) -> List[TickerMeta]:
        pass


class UnifiedAPI(TradingAPI, MarketDataAPI, SymbolDataAPI):
    def __init__(self, trader: TradingAPI, market: MarketDataAPI, symbols: SymbolDataAPI):
        self.trader = trader
        self.market = market
        self.symbols = symbols

    def buy(self, symbol: str, quantity: int, price: float):
        return self.trader.buy(symbol, quantity, price)

    def sell(self, symbol: str, count: int, price: float):
        return self.trader.sell(symbol, count, price)

    def balance(self) -> float:
        return self.trader.balance()

    def shares(self, symbol: str) -> int:
        return self.trader.shares(symbol)

    def is_market_open(self) -> bool:
        return self.market.is_market_open()

    def was_market_open(self, date: dt.datetime) -> bool:
        return self.market.was_market_open(date)

    def historical_bars(self, symbol: str, start: dt.datetime, dur: dt.timedelta,
                        buckets: TimeFrame = TimeFrame.Minute) -> List[Bar]:
        return self.market.historical_bars(symbol, start, dur, buckets)

    def bars(self, symbol: str, buckets: TimeFrame = TimeFrame.Minute) -> List[Bar]:
        return self.market.bars(symbol, buckets)

    def historical_trades(self, symbol: str, start: dt.datetime, dur: dt.timedelta) -> List[Trade]:
        return self.market.historical_trades(symbol, start, dur)

    def trades(self, symbol: str) -> List[Trade]:
        return self.market.trades(symbol)

    def historical_quotes(self, symbol: str, start: dt.datetime, dur: dt.timedelta) -> List[HistoricalQuote]:
        return self.market.historical_quotes(symbol, start, dur)

    def quotes(self, symbol: str) -> Quote:
        return self.market.quotes(symbol)

    def historical_news(self, symbol: str, start: dt.datetime, dur: dt.timedelta) -> List[News]:
        return self.market.historical_news(symbol, start, dur)

    def news(self, symbol: str) -> List[News]:
        return self.market.news(symbol)

    def get_ticker_symbols(self, timestamp: dt.datetime) -> List[str]:
        return self.symbols.get_ticker_symbols(timestamp)

    def get_float(self, symbol: str) -> int:
        return self.symbols.get_float(symbol)

    def get_floats(self, symbols: List[str]) -> Dict[str, int]:
        return self.symbols.get_floats(symbols)

    def get_ticker(self, symbol: str) -> TickerMeta:
        return self.symbols.get_ticker(symbol)

    def historical_ticker(self, symbol: str, start: dt.datetime, dur: dt.timedelta) -> List[TickerMeta]:
        return self.symbols.historical_ticker(symbol, start, dur)
