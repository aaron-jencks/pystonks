import argparse
import datetime as dt
import multiprocessing as mp
import pathlib
import queue
from typing import List, Callable

from pystonks.apis.alpolyhoo import AlPolyHooStaticFilterAPI, AlFinnPolyHooStaticFilterAPI
from pystonks.apis.sql import SqliteAPI
from pystonks.daemons.screener import hscreener
from pystonks.facades import UnifiedAPI
from pystonks.market.filter import TickerFilter, StaticFloatFilter, ChangeSinceNewsFilter
from pystonks.utils.config import read_config
from pystonks.utils.processing import truncate_datetime

BASELINE_VERSION = '1.0.0'


AlgorithmicProcessor = Callable[[str, mp.Queue], None]


def baseline_processor(ticker: str, api: UnifiedAPI, callback: mp.Queue):
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
        hscreened = hscreener(tickers, self.filters, date)
        print(f'Screened stocks result: \n' + "\n".join(hscreened))
        self.cache.commit()
        self.cache.reset_connection()
        self.cache.enable_commiting()
        return hscreened

    def check_callback_queue(self):
        while True:
            try:
                ticker = self.callback_queue.get_nowait()
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

    daemon = BaselineModelExecutor(controllers, filters, baseline_processor)
    daemon.loop()
