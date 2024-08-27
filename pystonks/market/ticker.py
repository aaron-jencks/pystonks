import pathlib
from abc import ABC, abstractmethod
import datetime as dt
from typing import List, Set, Optional

from pystonks.market.filter import TickerFilter


class Ticker:
    def __init__(self, sym: str, fl: int, cp: float, cso: float):
        self.symbol = sym
        self.float = fl
        self.current_price = cp
        self.change_since_open = cso

    def __repr__(self) -> str:
        return f'{self.symbol}({self.float:,d}): ${self.current_price:0.2f} {self.change_since_open:0.2%}'


class TickerFetcher(ABC):
    def __init__(self, filters: List[TickerFilter]):
        self.filters = filters

    @abstractmethod
    def tickers(self, blacklist: Optional[Set[str]] = None) -> List[Ticker]:
        pass

    @abstractmethod
    def htickers(self, day: dt.datetime, blacklist: Optional[Set[str]] = None) -> List[Ticker]:
        pass

    def check_filters(self, s: str, fl: int, day: Optional[dt.datetime] = None) -> bool:
        return all(map(lambda f: f.passes(s, fl, day), self.filters))


class CachedTickerFetcher(TickerFetcher):
    def __init__(self, cache_loc: pathlib.Path, filters: List[TickerFilter], cache: bool = True):
        super().__init__(filters)
        self.db_loc = cache_loc
        self.cache = cache
        self.initialize_tables()

    @abstractmethod
    def initialize_tables(self):
        pass

    @abstractmethod
    def check_cached_tickers(self, day: dt.datetime) -> Optional[List[Ticker]]:
        pass

    @abstractmethod
    def write_cached_tickers(self, day: dt.datetime, tickers: List[Ticker]):
        pass

    @abstractmethod
    def purge_cache(self):
        pass

    def htickers(self, day: dt.datetime, blacklist: Optional[Set[str]] = None) -> List[Ticker]:
        return self.check_cached_tickers(day) if self.cache else None
