import datetime as dt
import random as rng
from typing import List, Any

from alpaca.data import TimeFrame

from pystonks.apis.sql import SQL_DATE_FMT
from pystonks.facades import MarketDataAPI, TradingAPI, HistoricalMarketDataAPI
from pystonks.models import News, Bar, HistoricalQuote, Trade
from pystonks.utils.processing import process_interval, truncate_datetime
from pystonks.utils.structures.caching import CacheAPI, ReadOnlyCacheAPI, CachedClass, ReadOnlyCachedClass


class SimulationResults:
    def __init__(self):
        self.total_profit = 0.
        self.buys = 0
        self.sells = 0
        self.mistakes = 0
        self.bottoms = 0
        self.entries = 0
        self.exits = 0
        self.cancels = 0
        self.holds = 0


def default_empty_saver(date: dt.datetime, rows: List[Any]):
    pass


def default_empty_fetcher(date: dt.datetime) -> List[Any]:
    raise Exception('tried to fetch data in read only trader')


class SimulatedTrader(ReadOnlyCachedClass, HistoricalMarketDataAPI, TradingAPI):
    def __init__(self, cash: float, api_key: str, api_secret: str, paper: bool, cache: ReadOnlyCacheAPI):
        super().__init__(cache)
        self.api_key = api_key
        self.api_secret = api_secret
        self.paper = paper
        self.init_cash = cash
        self.cash = cash
        self.owned = 0
        self.buys = 0
        self.sells = 0
        self.mistakes = 0
        self.bottoms = 0
        self.entries = 0
        self.exits = 0
        self.cancels = 0

    def check_exists(self, tbl_pre: str, symbol: str, date: dt.datetime) -> bool:
        return self.cache_check(
            f'{tbl_pre}_date_processed',
            condition='symbol = ? and date = ?',
            params=(symbol, date.strftime(SQL_DATE_FMT))
        )

    def get_cached_symbols(self) -> List[str]:
        rows = self.cache_lookup('bars_date_processed', columns='distinct symbol')
        return [s[0] for s in rows]

    def get_symbol_dates(self, symbol: str) -> List[dt.datetime]:
        rows = self.cache_lookup('bars_date_processed', columns='distinct date',
                                 condition='symbol = ?', params=(symbol,))
        return [dt.datetime.strptime(r[0], SQL_DATE_FMT) for r in rows]

    def reset(self):
        self.cash = self.init_cash
        self.owned = 0
        self.buys = 0
        self.sells = 0
        self.mistakes = 0
        self.bottoms = 0
        self.entries = 0
        self.exits = 0
        self.cancels = 0

    def get_results(self) -> SimulationResults:
        res = SimulationResults()
        res.buys = self.buys
        res.sells = self.sells
        res.mistakes = self.mistakes
        res.bottoms = self.bottoms
        res.entries = self.entries
        res.exits = self.exits
        res.cancels = self.cancels
        return res

    def balance(self) -> float:
        return self.cash

    def shares(self, symbol: str) -> int:
        return self.owned

    def buy(self, symbol: str, count: int, price: float):
        rc = rng.randint(count // 2, count)
        total = rc * price

        if self.cash < total:
            self.mistakes += 1
            return

        if self.cash == total:
            self.bottoms += 1
        if self.owned == 0:
            self.entries += 1

        self.cash -= total
        self.owned += rc
        self.buys += 1

    def sell(self, symbol: str, count: int, price: float):
        if count > self.owned:
            self.mistakes += 1
            return

        rc = rng.randint(count // 2, count)
        total = rc * price

        if rc == self.shares:
            self.exits += 1

        self.cash += total
        self.owned -= rc
        self.sells += 1

    def cancel_all(self):
        self.cancels += 1

    def historical_news(self, symbol: str, start: dt.datetime, dur: dt.timedelta) -> List[News]:
        def checker(date: dt.datetime) -> bool:
            return self.check_exists('news', symbol, date)

        def loader(date: dt.datetime) -> List[News]:
            rows = self.cache_lookup('news', condition='symbol = ? and date(updated_at) = ?',
                                     params=(symbol, date.strftime(SQL_DATE_FMT)))
            return [
                News(symbol, dt.datetime.fromisoformat(ts), nid, a, h, u, dt.datetime.fromisoformat(ua))
                for nid, _, ts, ua, a, h, u in rows
            ]

        collated = process_interval(truncate_datetime(start), dur,
                                    default_empty_fetcher, loader, checker, default_empty_saver)

        result = []
        for c in collated:
            result += c
        return result

    def historical_bars(self, symbol: str,
                        start: dt.datetime, dur: dt.timedelta,
                        buckets: TimeFrame = TimeFrame.Minute) -> List[Bar]:
        def checker(date: dt.datetime) -> bool:
            return self.check_exists('bars', symbol, date)

        def loader(date: dt.datetime) -> List[Bar]:
            rows = self.cache_lookup('bars', condition='symbol = ? and date(timestamp) = ?',
                                     params=(symbol, date.strftime(SQL_DATE_FMT)))
            return [
                Bar(symbol, dt.datetime.fromisoformat(ts), op, cl, h, l, vol)
                for _, ts, op, cl, h, l, vol in rows
            ]

        collated = process_interval(truncate_datetime(start), dur,
                                    default_empty_fetcher, loader, checker, default_empty_saver)

        result = []
        for c in collated:
            result += c
        return result

    def historical_quotes(self, symbol: str, start: dt.datetime, dur: dt.timedelta) -> List[HistoricalQuote]:
        def checker(date: dt.datetime) -> bool:
            return self.check_exists('quotes', symbol, date)

        def loader(date: dt.datetime) -> List[HistoricalQuote]:
            rows = self.cache_lookup('quotes', condition='symbol = ? and date(timestamp) = ?',
                                     params=(symbol, date.strftime(SQL_DATE_FMT)))
            return [
                HistoricalQuote(symbol, dt.datetime.fromisoformat(ts), aex, asz, ap, bex, bs, bp)
                for _, ts, aex, asz, ap, bex, bs, bp in rows
            ]

        collated = process_interval(truncate_datetime(start), dur,
                                    default_empty_fetcher, loader, checker, default_empty_saver)
        result = []
        for c in collated:
            result += c
        return result

    def historical_trades(self, symbol: str, start: dt.datetime, dur: dt.timedelta) -> List[Trade]:
        def checker(date: dt.datetime) -> bool:
            return self.check_exists('trades', symbol, date)

        def loader(date: dt.datetime) -> List[Trade]:
            rows = self.cache_lookup('trades', condition='symbol = ? and date(timestamp) = ?',
                                     params=(symbol, date.strftime(SQL_DATE_FMT)))
            return [
                Trade(symbol, dt.datetime.fromisoformat(ts), ex, sz, p)
                for _, ts, ex, sz, p in rows
            ]

        collated = process_interval(truncate_datetime(start), dur,
                                    default_empty_fetcher, loader, checker, default_empty_saver)
        result = []
        for c in collated:
            result += c
        return result

    def was_market_open(self, date: dt.datetime) -> bool:
        return self.cache_check('bars_date_processed', condition='date = ?', params=(date.strftime(SQL_DATE_FMT),))
