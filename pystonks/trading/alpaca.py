import time
import datetime as dt
from typing import Optional, List

from alpaca.data import TimeFrame, StockHistoricalDataClient, StockBarsRequest, \
    StockTradesRequest, StockQuotesRequest, StockLatestQuoteRequest, NewsRequest
from alpaca.data.historical.news import NewsClient
from alpaca.trading import Position, TradingClient, LimitOrderRequest, OrderSide, TimeInForce, GetCalendarRequest
from alpaca.common import Sort, APIError

from pystonks.models import Bar, HistoricalQuote, Quote, Trade, News

from pystonks.facades import TradingAPI, MarketDataAPI
from pystonks.utils.processing import process_interval, find_bars, timeframe_to_delta, truncate_datetime
from pystonks.apis.sql import SQL_DATE_FMT
from pystonks.utils.structures.caching import CacheAPI, CachedClass

RATE_LIMIT = 60. / 200.


class AlpacaTrader(CachedClass, MarketDataAPI, TradingAPI):
    def __init__(self, api_key: str, api_secret: str, paper: bool, cache: CacheAPI):
        super().__init__(cache)
        self.api_key = api_key
        self.api_secret = api_secret
        self.paper = paper
        self.mdclient: Optional[StockHistoricalDataClient] = None
        self.tclient: Optional[TradingClient] = None
        self.nclient: Optional[NewsClient] = None
        self.last_req = None
        self.connect()

    def setup_tables(self):
        self.db.create_table(
            'news',
            'news_id integer, '
            'symbol text, timestamp text, updated_at text,'
            'author text, headline text, url text, '
            'primary key (news_id, symbol)'
        )
        self.db.create_table(
            'bars',
            'symbol text, timestamp text, '
            'open real, close real, high real, low real, volume integer, '
            'primary key (symbol, timestamp)'
        )
        self.db.create_table(
            'quotes',
            'symbol text, timestamp text, '
            'ask_exchange text, ask_size integer, ask_price real, bid_exchange text, bid_size integer, bid_price real, '
            'primary key (symbol, timestamp)'
        )
        self.db.create_table(
            'trades',
            'symbol text, timestamp text, '
            'exchange text, count integer, price real, '
            'primary key (symbol, timestamp)'
        )
        self.db.create_table(
            'market_status',
            'date text primary key, is_open integer'
        )
        for dp in [
            'news', 'bars', 'quotes', 'trades'
        ]:
            self.db.create_table(
                f'{dp}_date_processed',
                'symbol text, date text, primary key (symbol, date)'
            )

    def connect(self):
        if self.mdclient is None or self.tclient is None or self.nclient is None:
            self.mdclient = StockHistoricalDataClient(self.api_key, self.api_secret)
            self.tclient = TradingClient(self.api_key, self.api_secret, paper=self.paper)
            self.nclient = NewsClient(self.api_key, self.api_secret)

    def handle_request(self):
        self.connect()
        if self.last_req is not None and (dt.datetime.now() - self.last_req).total_seconds() < RATE_LIMIT:
            diff = RATE_LIMIT - (dt.datetime.now() - self.last_req).total_seconds()
            time.sleep(diff)
        self.last_req = dt.datetime.now()

    def check_exists(self, tbl_pre: str, symbol: str, date: dt.datetime) -> bool:
        return self.cache_check(
            f'{tbl_pre}_date_processed',
            condition='symbol = ? and date = ?',
            params=(symbol, date.strftime(SQL_DATE_FMT))
        )

    def save_exists(self, tbl_pre: str, symbol: str, date: dt.datetime):
        self.cache_save(
            f'{tbl_pre}_date_processed',
            params=(symbol, date.strftime(SQL_DATE_FMT))
        )

    def historical_news(self, symbol: str, start: dt.datetime, dur: dt.timedelta) -> List[News]:
        def checker(date: dt.datetime) -> bool:
            return self.check_exists('news', symbol, date)

        def fetcher(date: dt.datetime) -> List[News]:
            self.handle_request()
            ns = self.nclient.get_news(NewsRequest(
                start=date,
                end=date + dt.timedelta(days=1),
                symbols=symbol,
                include_content=False,
                include_contentless=False
            )).data['news']
            return [News(symbol, n.created_at, int(n.id), n.author, n.headline, n.url, n.updated_at) for n in ns]

        def loader(date: dt.datetime) -> List[News]:
            rows = self.cache_lookup('news', condition='symbol = ? and date(updated_at) = ?',
                                     params=(symbol, date.strftime(SQL_DATE_FMT)))
            return [
                News(symbol, dt.datetime.fromisoformat(ts), nid, a, h, u, dt.datetime.fromisoformat(ua))
                for nid, _, ts, ua, a, h, u in rows
            ]

        news_params = []

        def saver(date: dt.datetime, rows: List[News]):
            self.save_exists('news', symbol, date)
            for param in [
                (
                    n.news_id, symbol,
                    n.timestamp.isoformat(),
                    n.updated_at.isoformat(),
                    n.author, n.headline, n.url
                )
                for n in rows
            ]:
                news_params.append(param)

        collated = process_interval(truncate_datetime(start), dur, fetcher, loader, checker, saver)

        if len(news_params) > 0:
            self.cache_save_many('news', news_params)

        result = []
        for c in collated:
            result += c
        return result

    def news(self, symbol: str) -> List[News]:
        self.handle_request()
        ns = self.nclient.get_news(NewsRequest(
            symbols=symbol,
            include_content=False,
            include_contentless=False
        )).news
        return [News(symbol, n.created_at, int(n.id), n.author, n.headline, n.url, n.updated_at) for n in ns]

    def historical_bars(self, symbol: str,
                        start: dt.datetime, dur: dt.timedelta,
                        buckets: TimeFrame = TimeFrame.Minute) -> List[Bar]:
        def checker(date: dt.datetime) -> bool:
            return self.check_exists('bars', symbol, date)

        def fetcher(date: dt.datetime) -> List[Bar]:
            self.handle_request()
            bs = self.mdclient.get_stock_bars(StockBarsRequest(
                symbol_or_symbols=symbol,
                timeframe=buckets,
                start=date,
                end=date + dt.timedelta(days=1)
            )).data[symbol]
            return [Bar(symbol, b.timestamp, b.open, b.close, b.high, b.low, int(b.volume)) for b in bs]

        def loader(date: dt.datetime) -> List[Bar]:
            rows = self.cache_lookup('bars', condition='symbol = ? and date(timestamp) = ?',
                                     params=(symbol, date.strftime(SQL_DATE_FMT)))
            return [
                Bar(symbol, dt.datetime.fromisoformat(ts), op, cl, h, l, vol)
                for _, ts, op, cl, h, l, vol in rows
            ]

        bar_params = []

        def saver(date: dt.datetime, rows: List[Bar]):
            self.save_exists('bars', symbol, date)
            for param in [
                (
                    symbol,
                    b.timestamp.isoformat(),
                    b.open, b.close, b.high, b.low, b.volume
                )
                for b in rows
            ]:
                bar_params.append(param)

        collated = process_interval(truncate_datetime(start), dur, fetcher, loader, checker, saver)

        if len(bar_params) > 0:
            self.cache_save_many('bars', bar_params)

        result = []
        for c in collated:
            result += c
        return result

    def bars(self, symbol: str, buckets: TimeFrame = TimeFrame.Minute) -> List[Bar]:
        self.handle_request()
        bs = self.mdclient.get_stock_bars(StockBarsRequest(
            symbol_or_symbols=symbol,
            timeframe=buckets
        )).data[symbol]
        return [Bar(symbol, b.timestamp, b.open, b.close, b.high, b.low, int(b.volume)) for b in bs]

    def buy(self, symbol: str, count: int, price: float):
        self.handle_request()
        self.tclient.submit_order(LimitOrderRequest(
            symbol=symbol,
            qty=count,
            side=OrderSide.BUY,
            time_in_force=TimeInForce.IOC,
            limit_price=price,
        ))

    def sell(self, symbol: str, count: int, price: float):
        self.handle_request()
        self.tclient.submit_order(LimitOrderRequest(
            symbol=symbol,
            qty=count,
            side=OrderSide.SELL,
            time_in_force=TimeInForce.IOC,
            limit_price=price,
        ))

    def cancel_all(self):
        self.handle_request()
        self.tclient.cancel_orders()

    def historical_quotes(self, symbol: str, start: dt.datetime, dur: dt.timedelta) -> List[HistoricalQuote]:
        def checker(date: dt.datetime) -> bool:
            return self.check_exists('quotes', symbol, date)

        def fetcher(date: dt.datetime) -> List[HistoricalQuote]:
            self.handle_request()
            qs = self.mdclient.get_stock_quotes(StockQuotesRequest(
                symbol_or_symbols=symbol,
                start=date,
                end=date + dt.timedelta(days=1)
            )).data[symbol]
            return [
                HistoricalQuote(
                    symbol, q.timestamp,
                    q.ask_exchange, int(q.ask_size), q.ask_price,
                    q.bid_exchange, int(q.bid_size), q.bid_price
                ) for q in qs
            ]

        def loader(date: dt.datetime) -> List[HistoricalQuote]:
            rows = self.cache_lookup('quotes', condition='symbol = ? and date(timestamp) = ?',
                                           params=(symbol, date.strftime(SQL_DATE_FMT)))
            return [
                HistoricalQuote(symbol, dt.datetime.fromisoformat(ts), aex, asz, ap, bex, bs, bp)
                for _, ts, aex, asz, ap, bex, bs, bp in rows
            ]

        def saver(date: dt.datetime, rows: List[HistoricalQuote]):
            self.save_exists('quotes', symbol, date)
            params = [
                (
                    symbol,
                    q.timestamp.isoformat(),
                    q.ask_exchange, q.ask_size, q.ask_price,
                    q.bid_exchange, q.bid_size, q.bid_price
                )
                for q in rows
            ]
            if len(params) > 0:
                self.cache_save_many('quotes', params)

        collated = process_interval(truncate_datetime(start), dur, fetcher, loader, checker, saver)
        result = []
        for c in collated:
            result += c
        return result

    def quotes(self, symbol: str) -> Quote:
        self.handle_request()
        q = self.mdclient.get_stock_latest_quote(StockLatestQuoteRequest(
            symbol_or_symbols=symbol,
        ))[symbol]
        return Quote(
            symbol, q.ask_exchange, int(q.ask_size), q.ask_price, q.bid_exchange, int(q.bid_size), q.bid_price
        )

    def get_current_price(self, symbol: str):
        qt = self.quotes(symbol)
        return qt.ask_price

    def balance(self) -> float:
        self.handle_request()
        acct = self.tclient.get_account()
        return float(acct.cash) if acct.cash is not None else 0.

    def shares(self, symbol: str) -> int:
        try:
            position = self.position(symbol)
            return int(position.qty_available)
        except APIError:
            return 0

    def position(self, symbol: str) -> Position:
        self.handle_request()
        return self.tclient.get_open_position(symbol)

    def historical_trades(self, symbol: str, start: dt.datetime, dur: dt.timedelta) -> List[Trade]:
        def checker(date: dt.datetime) -> bool:
            return self.check_exists('trades', symbol, date)

        def fetcher(date: dt.datetime) -> List[Trade]:
            self.handle_request()
            ts = self.mdclient.get_stock_trades(StockTradesRequest(
                symbol_or_symbols=symbol,
                start=date,
                end=date + dt.timedelta(days=1),
                sort=Sort.DESC
            )).data[symbol]
            return [
                Trade(
                    symbol, t.timestamp,
                    t.exchange,
                    int(t.size), t.price
                ) for t in ts
            ]

        def loader(date: dt.datetime) -> List[Trade]:
            rows = self.cache_lookup('trades', condition='symbol = ? and date(timestamp) = ?',
                                     params=(symbol, date.strftime(SQL_DATE_FMT)))
            return [
                Trade(symbol, dt.datetime.fromisoformat(ts), ex, sz, p)
                for _, ts, ex, sz, p in rows
            ]

        def saver(date: dt.datetime, rows: List[Trade]):
            self.save_exists('trades', symbol, date)
            params = [
                (
                    symbol,
                    t.timestamp.isoformat(),
                    t.exchange, t.count, t.price
                )
                for t in rows
            ]
            if len(params) > 0:
                self.cache_save_many('trades', params)

        collated = process_interval(truncate_datetime(start), dur, fetcher, loader, checker, saver)
        result = []
        for c in collated:
            result += c
        return result

    def trades(self, symbol: str) -> List[Trade]:
        self.handle_request()
        ts = self.mdclient.get_stock_trades(StockTradesRequest(
            symbol_or_symbols=symbol,
            sort=Sort.DESC
        )).data[symbol]
        return [
            Trade(
                symbol, t.timestamp,
                t.exchange,
                int(t.size), t.price
            ) for t in ts
        ]

    def is_market_open(self) -> bool:
        return self.tclient.get_clock().is_open

    def was_market_open(self, date: dt.datetime) -> bool:
        cal = self.tclient.get_calendar(GetCalendarRequest(
            start=(date - dt.timedelta(days=1)).date(),
            end=(date + dt.timedelta(days=1)).date(),
        ))
        return any([c.date == date.date() for c in cal])


class AlpacaTraderManualBars(AlpacaTrader):
    def historical_bars(self, symbol: str,
                        start: dt.datetime, dur: dt.timedelta,
                        buckets: TimeFrame = TimeFrame.Minute) -> List[Bar]:
        def checker(date: dt.datetime) -> bool:
            return self.check_exists('bars', symbol, date)

        def fetcher(date: dt.datetime) -> List[Bar]:
            trades = self.historical_trades(symbol, date, dt.timedelta(days=1))
            return find_bars(timeframe_to_delta(buckets), trades)

        def loader(date: dt.datetime) -> List[Bar]:
            rows = self.cache_lookup('bars', condition='symbol = ? and date(timestamp) = ?',
                                     params=(symbol, date.strftime(SQL_DATE_FMT)))
            return [
                Bar(symbol, dt.datetime.fromisoformat(ts), op, cl, h, l, vol)
                for _, ts, op, cl, h, l, vol in rows
            ]

        bar_params = []

        def saver(date: dt.datetime, rows: List[Bar]):
            self.save_exists('bars', symbol, date)
            for param in [
                (
                        symbol,
                        b.timestamp.isoformat(),
                        b.open, b.close, b.high, b.low, b.volume
                )
                for b in rows
            ]:
                bar_params.append(param)

        collated = process_interval(truncate_datetime(start), dur, fetcher, loader, checker, saver)

        if len(bar_params) > 0:
            self.cache_save_many('bars', bar_params)

        result = []
        for c in collated:
            result += c
        return result

    def bars(self, symbol: str, buckets: TimeFrame = TimeFrame.Minute) -> List[Bar]:
        trades = self.trades(symbol)
        return find_bars(timeframe_to_delta(buckets), trades)
