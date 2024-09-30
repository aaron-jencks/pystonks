from abc import ABC, abstractmethod
from asyncio import run as arun
import datetime as dt
from typing import Optional

from pystonks.facades import SymbolDataAPI, MarketDataAPI, NewsDataAPI
from pystonks.utils.processing import change_since_news, fill_in_sparse_bars, truncate_datetime


class TickerFilter(ABC):
    @abstractmethod
    def passes(self, symbol: str, day: Optional[dt.datetime] = None) -> bool:
        pass


class StaticTickerFilter(ABC):
    @abstractmethod
    def passes(self, api: SymbolDataAPI, symbol: str) -> bool:
        pass


class StaticIntervalFilter(StaticTickerFilter, ABC):
    def __init__(self, lower, upper):
        self.lower = lower
        self.upper = upper

    def in_interval(self, value) -> bool:
        return (self.lower < 0 or value >= self.lower) and (self.upper < 0 or value <= self.upper)


class StaticFloatFilter(StaticIntervalFilter):
    def __init__(self, lower_limit: int = -1, upper_limit: int = -1):
        super().__init__(lower_limit, upper_limit)

    def passes(self, api: SymbolDataAPI, symbol: str) -> bool:
        fl = api.get_float(symbol)
        return self.in_interval(fl)


class IntervalFilter(TickerFilter, ABC):
    def __init__(self, lower, upper):
        self.lower = lower
        self.upper = upper

    def in_interval(self, value) -> bool:
        return (self.lower < 0 or value >= self.lower) and (self.upper < 0 or value <= self.upper)


class FloatFilter(IntervalFilter):
    def __init__(self, symbol_api: SymbolDataAPI, lower_limit: int = -1, upper_limit: int = -1):
        super().__init__(lower_limit, upper_limit)
        self.symbol_client = symbol_api

    def passes(self, symbol: str, day: Optional[dt.datetime] = None) -> bool:
        fl = self.symbol_client.get_float(symbol)
        return self.in_interval(fl)


class CurrentPriceFilter(IntervalFilter):
    def __init__(self, symbol_api: SymbolDataAPI, lower_limit: float = 0., upper_limit: float = -1):
        super().__init__(lower_limit, upper_limit)
        self.symbol_client = symbol_api

    def passes(self, symbol: str, day: Optional[dt.datetime] = None) -> bool:
        t = self.symbol_client.get_ticker(symbol) \
            if day is None else self.symbol_client.historical_ticker(symbol, day, dt.timedelta(days=1))[0]
        return self.in_interval(t.current_price)


class FloatPriceFilter(TickerFilter):
    def __init__(self, symbol_api: SymbolDataAPI,
                 float_lower_limit: float = 0., float_upper_limit: float = -1,
                 price_lower_limit: float = 0., price_upper_limit: float = -1):
        self.symbol_client = symbol_api
        self.float_lower = float_lower_limit
        self.float_upper = float_upper_limit
        self.price_lower = price_lower_limit
        self.price_upper = price_upper_limit

    def in_interval(self, value: float, lower: float, upper: float) -> bool:
        return (lower < 0 or value >= lower) and (upper < 0 or value <= upper)

    def passes(self, symbol: str, day: Optional[dt.datetime] = None) -> bool:
        t = self.symbol_client.get_ticker(symbol) \
            if day is None else self.symbol_client.historical_ticker(symbol, day, dt.timedelta(days=1))[0]
        return (self.in_interval(t.current_price, self.price_lower, self.price_upper) and
                self.in_interval(t.float, self.float_lower, self.float_upper))


class ChangeSinceOpenFilter(IntervalFilter):
    def __init__(self, symbol_api: SymbolDataAPI, lower_limit: float = 0., upper_limit: float = -1):
        super().__init__(lower_limit, upper_limit)
        self.symbol_client = symbol_api

    def passes(self, symbol: str, day: Optional[dt.datetime] = None) -> bool:
        t = self.symbol_client.get_ticker(symbol) \
            if day is None else self.symbol_client.historical_ticker(symbol, day, dt.timedelta(days=1))[0]
        return self.in_interval(t.current_price)


class ChangeSinceNewsFilter(TickerFilter):
    def __init__(self, market_api: MarketDataAPI, news_api: NewsDataAPI, min_limit: float = 0.):
        super().__init__()
        self.min = min_limit
        self.market_client = market_api
        self.news_client = news_api

    def passes(self, symbol: str, day: Optional[dt.datetime] = None) -> bool:
        if day is not None:
            news = self.news_client.historical_news(symbol, day, dt.timedelta(days=1))
            if len(news) == 0:
                return False
            if self.min <= 0:
                return True
            bars = self.market_client.historical_bars(symbol, day, dt.timedelta(days=1))
        else:
            news = self.news_client.news(symbol)
            if len(news) == 0:
                return False
            if self.min <= 0:
                return True
            bars = self.market_client.bars(symbol)
            day = dt.datetime.now(dt.UTC)
        bars = fill_in_sparse_bars(
            truncate_datetime(day),
            truncate_datetime(day + dt.timedelta(days=1)),
            dt.timedelta(minutes=1),
            bars
        )
        csn = change_since_news(bars, news, self.min)[0]
        return csn > self.min
