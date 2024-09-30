import datetime as dt
from typing import List

from pystonks.daemons.screener import hscreener
from pystonks.market.filter import TickerFilter
from pystonks.supervised.annotations.cluster import AnnotatorCluster
from pystonks.utils.processing import truncate_datetime

BASELINE_VERSION = '1.0.0'


class BaselineModelExecutor:
    def __init__(self, controllers: AnnotatorCluster, filters: List[TickerFilter]):
        self.controllers = controllers
        self.filters = filters
        self.dispatched_symbols = set()

    def get_tickers(self) -> List[str]:
        date = truncate_datetime(dt.datetime.now(dt.UTC))
        self.controllers.cache.disable_commiting()
        print('fetching ticker symbols...', end='')
        tickers = self.controllers.get_ticker_symbols(date)
        self.controllers.cache.commit()
        print('DONE')
        hscreened = hscreener(tickers, self.filters, date)
        print(f'Screened stocks result: \n' + "\n".join(hscreened))
        self.controllers.cache.commit()
        self.controllers.cache.reset_connection()
        self.controllers.cache.enable_commiting()
        return hscreened

    def dispatch_processors(self, tickers: List[str]):
        pass

    def loop(self):
        while True:
            screened = self.get_tickers()
            self.dispatch_processors(screened)


if __name__ == '__main__':
    import argparse
    import pathlib

    from pystonks.market.filter import StaticFloatFilter, ChangeSinceNewsFilter
    from pystonks.supervised.annotations.cluster import FinnAnnotatorCluster
    from pystonks.utils.config import read_config

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

    if args.news_alternative:
        controllers = FinnAnnotatorCluster(
            config.db_location,
            config.alpaca_key, config.alpaca_secret, config.polygon_key, config.finnhub_key,
            [
                StaticFloatFilter(upper_limit=10000000)
            ]
        )
    else:
        controllers = AnnotatorCluster(
            config.db_location,
            config.alpaca_key, config.alpaca_secret, config.polygon_key,
            [
                StaticFloatFilter(upper_limit=10000000)
            ]
        )

    filters = [
        ChangeSinceNewsFilter(controllers.market, controllers.news_api, min_limit=0.1)
    ]

    daemon = BaselineModelExecutor(controllers, filters)
    daemon.loop()
