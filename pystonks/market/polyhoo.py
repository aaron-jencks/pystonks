import datetime as dt
import time
from typing import List, Dict, Callable

from polygon import ReferenceClient
from polygon.enums import TickerType, TickerMarketType
from tqdm import tqdm
import yfinance as yf

from pystonks.apis.sql import SQL_DATE_FMT
from pystonks.facades import SymbolDataAPI, TradingAPI, MarketDataAPI
from pystonks.market.filter import TickerFilter, StaticTickerFilter
from pystonks.models import TickerMeta
from pystonks.utils.loggging import suppress_print
from pystonks.utils.processing import process_interval, truncate_datetime
from pystonks.utils.structures.caching import CacheAPI, CachedClass

YAHOO_DATE_FMT = '%Y-%m-%d'


class PolyHooSymbolData(CachedClass, SymbolDataAPI):
    def __init__(self, polygon_key: str, cache: CacheAPI):
        super().__init__(cache)
        self.polygon_key = polygon_key

    def setup_tables(self):
        self.db.create_table('tickers', 'name text primary key')
        self.db.create_table('tickers_delisted', 'name text primary key, date text')
        self.db.create_table('floats', 'name text primary key, float integer')
        self.db.create_table('yahoo_meta', 'name text, date text, open real, high real, primary key (name, date)')

    def is_delisted(self, symbol: str, d: dt.datetime) -> bool:
        if self.cache_check('tickers_delisted', condition='name = ?', params=(symbol,)):
            row = (self.cache_lookup('tickers_delisted', 'date', 'name = ?', params=(symbol,)))[0]
            return dt.datetime.fromisoformat(row[0]) < d
        return False

    def get_ticker_symbols(self, timestamp: dt.datetime) -> List[str]:
        if self.cache_check('tickers'):
            rows = self.cache_lookup('tickers')
            result = [r[0] for r in rows if not (self.is_delisted(r[0], timestamp))]
        else:
            client = ReferenceClient(self.polygon_key)

            while True:
                tickers = client.get_tickers(symbol_type=TickerType.COMMON_STOCKS, market=TickerMarketType.STOCKS,
                                             all_pages=True)
                if len(tickers) == 1:
                    if 'status' in tickers[0] and tickers[0]['status'] == 'ERROR':
                        print('hit rate limit on polygon controller, sleeping for a bit')
                        time.sleep(12)
                        continue
                break

            tmap = {t['ticker']: t for t in tickers}

            tickers = list(set([s for s in tmap]))
            self.cache_save_many('tickers', [(t,) for t in tickers])
            result = []
            delist = []
            for t in tickers:
                ts = tmap[t]
                if 'delisted_utc' in ts:
                    d = ts['delisted_utc'].isoformat()
                    delist.append((t, d))
                    if timestamp <= d:
                        result.append(t)
                else:
                    result.append(t)
            if len(delist) > 0:
                self.cache_save_many('tickers_delisted', delist)

        return result

    def get_floats(self, symbols: List[str]) -> Dict[str, int]:
        result = {}
        new_entries = []
        for s in tqdm(symbols, desc='fetching floats'):
            if self.cache_check('floats', condition='name = ?', params=(s,)):
                row = self.cache_lookup('floats', columns='float', condition='name = ?', params=(s,))
                result[s] = row[0][0]
            else:
                with suppress_print():
                    info = yf.Ticker(s).info
                fl = -1 if 'floatShares' not in info else int(info['floatShares'])
                result[s] = fl
                new_entries.append((s, fl))
        if len(new_entries) > 0:
            self.cache_save_many('floats', params=new_entries)
        return result

    def get_float(self, symbol: str) -> int:
        if self.cache_check('floats', condition='name = ?', params=(symbol,)):
            row = self.cache_lookup('floats', columns='float', condition='name = ?', params=(symbol,))
            fl = row[0][0]
        else:
            with suppress_print():
                info = yf.Ticker(symbol).info
            fl = -1 if 'floatShares' not in info else int(info['floatShares'])
            self.cache_save('floats', params=(symbol, fl))
        return fl

    def get_ticker(self, symbol: str) -> TickerMeta:
        with suppress_print():
            info = yf.Ticker(symbol).info
        fl = -1 if 'floatShares' not in info else info['floatShares']
        op = -1 if 'open' not in info else info['open']
        cp = -1 if 'currentPrice' not in info else info['currentPrice']
        cso = -1 if op < 0 or cp < 0 else ((cp - op) / op)
        if not self.cache_check('floats', condition='name = ?', params=(symbol,)):
            self.cache_save('floats', params=(symbol, fl))
        return TickerMeta(symbol, dt.datetime.now(), fl, cp, op, cso)

    def historical_ticker(self, symbol: str, start: dt.datetime, dur: dt.timedelta) -> List[TickerMeta]:
        fl = self.get_float(symbol)

        def checker(date: dt.datetime) -> bool:
            return self.cache_check('yahoo_meta', condition='name = ? and date = ?',
                                    params=(symbol, date.strftime(SQL_DATE_FMT)))

        def loader(date: dt.datetime) -> TickerMeta:
            row = self.cache_lookup('yahoo_meta', condition='name = ? and date = ?',
                                    params=(symbol, date.strftime(SQL_DATE_FMT)))
            _, _, op, hgh = row[0]
            cso = -1 if op < 0 or hgh < 0 else (hgh - op) / op
            return TickerMeta(symbol, date, fl, hgh, op, cso)

        def fetcher(date: dt.datetime) -> TickerMeta:
            with suppress_print():
                info = yf.Ticker(symbol).history(
                    start=date.strftime(YAHOO_DATE_FMT),
                    end=(date + dt.timedelta(days=1)).strftime(YAHOO_DATE_FMT)
                )
            hgh = -1 if 'High' not in info or len(info['High'].values) == 0 else info['High'].values[0]
            op = -1 if 'Open' not in info or len(info['Open'].values) == 0 else info['Open'].values[0]
            cso = -1 if op < 0 or hgh < 0 else (hgh - op) / op
            return TickerMeta(symbol, date, fl, hgh, op, cso)

        cache_params = []
        float_params = []

        def saver(date: dt.datetime, row: TickerMeta):
            params = (
                row.symbol, date.strftime(SQL_DATE_FMT),
                row.open, row.current_price
            )
            cache_params.append(params)
            float_params.append((symbol, row.float))

        result = process_interval(truncate_datetime(start), dur, fetcher, loader, checker, saver)

        if len(cache_params) > 0:
            self.cache_save_many('yahoo_meta', cache_params)
            self.cache_save_many('floats', float_params)

        return result


class StaticFilteredPolyHooSymbolData(PolyHooSymbolData):
    def __init__(self, polygon_key: str, filters: List[StaticTickerFilter], cache: CacheAPI):
        super().__init__(polygon_key, cache)
        self.filters = filters

    def setup_tables(self):
        super().setup_tables()
        self.db.create_table('filtered_tickers', 'name text primary key')

    def get_ticker_symbols(self, timestamp: dt.datetime) -> List[str]:
        if self.cache_check('filtered_tickers'):
            rows = self.cache_lookup('filtered_tickers')
            result = [r[0] for r in rows if not (self.is_delisted(r[0], timestamp))]
        else:
            tickers = super().get_ticker_symbols(timestamp)
            result = [
                t for t in tqdm(tickers, desc='applying static filters')
                if all([f.passes(self, t) for f in self.filters])
            ]
            self.cache_save_many('filtered_tickers', [(t,) for t in result])
        return result
