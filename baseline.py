import argparse
import datetime as dt
import math
import multiprocessing as mp
import pathlib
import queue
import time
from typing import List, Callable

from pystonks.apis.alpolyhoo import AlPolyHooStaticFilterAPI, AlFinnPolyHooStaticFilterAPI
from pystonks.apis.sql import SqliteAPI
from pystonks.daemons.screener import hscreener, seq_screener
from pystonks.facades import UnifiedAPI
from pystonks.market.filter import TickerFilter, StaticFloatFilter, ChangeSinceNewsFilter
from pystonks.supervised.annotations.utils.metrics import EMAStockMetric
from pystonks.supervised.annotations.utils.models import GeneralStockPlotInfo
from pystonks.supervised.annotations.utils.tk_modules import EMAInfoModule
from pystonks.utils.config import read_config
from pystonks.utils.processing import truncate_datetime

BASELINE_VERSION = '1.1.2'


AlgorithmicProcessor = Callable[[str, UnifiedAPI, mp.Queue], None]


def collect_current_data(ticker: str, api: UnifiedAPI) -> GeneralStockPlotInfo:
    bars = api.bars(ticker)
    news = api.news(ticker)
    return GeneralStockPlotInfo(0, bars, news, [])


def baseline_processor(ticker: str, api: UnifiedAPI, callback: mp.Queue):
    info = collect_current_data(ticker, api)
    ema = EMAStockMetric(EMAInfoModule(26, 2), 0, 0)
    ema.process_all(info)

    if ema.first_derivative[-1] <= 0:
        callback.put(ticker)
        return

    price = api.quotes(ticker).ask_price * 1.05
    shares = int(math.floor(100 / price))
    api.buy(ticker, shares, price)  # buy roughly $100 worth of shares
    print('bought {} shares of {} at ${.2f}'.format(shares, ticker, price))

    start_index = len(ema.first_derivative)
    sell_mark = 1.1
    sell_all = False

    while shares > 0:
        time.sleep(60)  # check once a minute

        # if market is closed, we'll just sleep until it's open for now
        if not api.is_market_open():
            sell_all = True
            continue

        sell_price = api.quotes(ticker).bid_price * 0.95
        profit = sell_price / price

        if sell_all:
            api.sell(ticker, shares, sell_price)
            print('sold {} shares of {} at ${.2f}'.format(shares, ticker, sell_price))
            time.sleep(1)  # give api time to update shares
            shares = api.shares(ticker)
            continue

        info = collect_current_data(ticker, api)
        ema = EMAStockMetric(EMAInfoModule(26, 2), 0, 0)
        ema.process_all(info)
        derivative = ema.first_derivative[start_index:]
        if len(derivative) == 0:
            shares = api.shares(ticker)
            continue

        if any([d <= 0.004 for d in derivative]):  # sell at the peak, or if slope is negative
            api.sell(ticker, shares, sell_price)
            print('sold {} shares of {} at ${.2f}'.format(shares, ticker, sell_price))
        elif profit >= sell_mark and shares > 10:  # sell half every 10%
            api.sell(ticker, shares >> 1, sell_price)
            print('hit profit mark {.2f%}, sold {} shares of {} at ${.2f}'.format(
                sell_mark - 1., shares >> 1, ticker, sell_price
            ))
            shares >>= 1
            sell_mark += 0.1

        time.sleep(1)  # give api time to update shares
        shares = api.shares(ticker)

    callback.put(ticker)


class BaselineModelExecutor:
    def __init__(
            self, controllers: UnifiedAPI, cache: SqliteAPI,
            filters: List[TickerFilter], processor: AlgorithmicProcessor
    ):
        self.controllers = controllers
        self.cache = cache
        self.filters = filters
        self.processor = processor
        self.dispatched_symbols = set()
        self.callback_queue = mp.Queue(30)

    def get_tickers(self) -> List[str]:
        date = truncate_datetime(dt.datetime.now(dt.UTC))
        self.cache.disable_commiting()
        print('fetching ticker symbols...', end='')
        tickers = self.controllers.get_ticker_symbols(date)
        self.cache.commit()
        print('DONE')
        screened = seq_screener(tickers, self.filters)
        print(f'Screened stocks result: \n' + "\n".join(screened))
        self.cache.commit()
        self.cache.reset_connection()
        self.cache.enable_commiting()
        return screened

    def check_callback_queue(self):
        while True:
            try:
                ticker = self.callback_queue.get_nowait()
                print(f'{ticker} has become available for processing again')
                self.dispatched_symbols.remove(ticker)
            except queue.Empty:
                return

    def dispatch_processors(self, tickers: List[str]):
        for t in tickers:
            if t in self.dispatched_symbols:
                continue

            print('dispatching processor for {}'.format(t))
            self.dispatched_symbols.add(t)
            proc = mp.Process(target=self.processor, args=(t, self.controllers, self.callback_queue))
            proc.start()

    def loop(self):
        while True:
            if not self.controllers.is_market_open():
                print('market is closed...')
                time.sleep(1800)
                continue

            self.check_callback_queue()
            screened = self.get_tickers()
            self.dispatch_processors(screened)


if __name__ == '__main__':
    ap = argparse.ArgumentParser(
        description='Runs a simple baseline model that buys when a good stock is found, '
                    'and sells as soon as the first hill peak is found'
    )
    ap.add_argument(
        '-c', '--config',
        type=pathlib.Path, default=pathlib.Path('./config.json'),
        help='the location of the settings config file'
    )
    ap.add_argument(
        '--no_paper',
        action='store_true',
        help='indicates not to use paper markets'
    )
    ap.add_argument(
        '--news_alternative', action='store_true',
        help='indicates to use an alternative news source, finnhub'
    )
    ap.add_argument('-v', '--version', action='store_true', help='show the version and exit')
    args = ap.parse_args()

    if args.version:
        print('baseline model v{}'.format(BASELINE_VERSION))
        exit(0)

    config = read_config(args.config)

    cache = SqliteAPI(config.db_location)

    if args.news_alternative:
        controllers = AlFinnPolyHooStaticFilterAPI(
            config.alpaca_key, config.alpaca_secret, config.polygon_key, config.finnhub_key,
            not args.no_paper,
            [
                StaticFloatFilter(upper_limit=10000000)
            ],
            cache
        )
    else:
        controllers = AlPolyHooStaticFilterAPI(
            config.alpaca_key, config.alpaca_secret, config.polygon_key,
            not args.no_paper,
            [
                StaticFloatFilter(upper_limit=10000000)
            ],
            cache
        )

    filters = [
        ChangeSinceNewsFilter(controllers.market, controllers.news_api, min_limit=0.1)
    ]

    daemon = BaselineModelExecutor(controllers, cache, filters, baseline_processor)
    daemon.loop()
