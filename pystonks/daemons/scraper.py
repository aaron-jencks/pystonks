import pathlib
from argparse import ArgumentParser
import datetime as dt
from typing import List

from tqdm import tqdm

from pystonks.apis.alpolyhoo import AlPolyHooStaticFilterAPI, AlFinnPolyHooStaticFilterAPI
from pystonks.apis.sql import SqliteAPI, SQL_DATE_FMT
from pystonks.daemons.screener import hscreener
from pystonks.facades import UnifiedAPI
from pystonks.market.filter import StaticFloatFilter, ChangeSinceNewsFilter, TickerFilter
from pystonks.utils.config import read_config
from pystonks.utils.processing import truncate_datetime


SCRAPER_VERSION = '1.0.0'


def get_tickers(date: dt.datetime, cache: SqliteAPI, controllers: UnifiedAPI, filters: List[TickerFilter]) -> List[str]:
    cache.disable_commiting()
    print('fetching ticker symbols...', end='')
    tickers = controllers.get_ticker_symbols(date)
    cache.commit()
    print('DONE')
    hscreened = hscreener(tickers, filters, date)
    print(f'Screened stocks result: \n' + "\n".join(hscreened))
    cache.commit()
    current_tickers = []
    for ticker in tqdm(hscreened, desc='Processing screened bar data'):
        current_tickers.append(ticker)
    cache.commit()
    cache.reset_connection()
    cache.enable_commiting()
    return current_tickers


def next_day(current: dt.datetime, controllers: UnifiedAPI) -> dt.datetime:
    current -= dt.timedelta(days=1)
    while current.weekday() > 4 or not controllers.market.was_market_open(current):
        current -= dt.timedelta(days=1)  # skip the weekends and holidays
    print(f'starting processing on date {current.strftime(SQL_DATE_FMT)}')
    return current


def fetch_market_data(date: dt.datetime, cache: SqliteAPI, controllers: UnifiedAPI, filters: List[TickerFilter]):
    print(f'fetching data for date {date.strftime(SQL_DATE_FMT)}')
    filtered_tickers = get_tickers(date, cache, controllers, filters)
    for t in tqdm(filtered_tickers, desc='processing filtered tickers'):
        controllers.historical_news(t, date, dt.timedelta(days=1))
        controllers.historical_bars(t, date, dt.timedelta(days=1))
        controllers.historical_trades(t, date, dt.timedelta(days=1))


def data_scraper(cache: SqliteAPI, controllers: UnifiedAPI, filters: List[TickerFilter]):
    cache.create_table('scraper_processed', 'date text primary key')
    current_date = truncate_datetime(dt.datetime.now(dt.UTC))
    today_parsed = False
    while True:
        # repeatedly check for new days to finish
        if dt.datetime.now(dt.UTC).hour >= 21 and not today_parsed:
            today_parsed = False
        if dt.datetime.now(dt.UTC).hour < 14 and not today_parsed:
            fetch_market_data(truncate_datetime(dt.datetime.now(dt.UTC) - dt.timedelta(days=1)), cache, controllers, filters)
            today_parsed = True

        current_date = next_day(current_date, controllers)
        if cache.exists('scraper_processed', condition='date = ?',
                        params=(current_date.strftime(SQL_DATE_FMT),)):
            continue
        fetch_market_data(current_date, cache, controllers, filters)
        cache.insert_row('scraper_processed', (current_date.strftime(SQL_DATE_FMT),))


if __name__ == '__main__':
    ap = ArgumentParser(description='a daemon that runs and continuously collects data, storing it into the database')
    ap.add_argument('-c', '--config', type=pathlib.Path, default=pathlib.Path('../../config.json'),
                    help='the location of the settings file')
    ap.add_argument('--news_alternative', action='store_true',
                    help='indicates to use an alternative news source, finnhub')
    ap.add_argument('--version', '-v', action='store_true', help='print version and exit')
    args = ap.parse_args()

    if args.version:
        print(f'scraper v{SCRAPER_VERSION}')
        exit(0)

    config = read_config(args.config)

    cache = SqliteAPI(config.db_location)

    if args.news_alternative:
        controllers = AlFinnPolyHooStaticFilterAPI(
            config.alpaca_key, config.alpaca_secret, config.polygon_key, config.finnhub_key,
            True,
            [
                StaticFloatFilter(upper_limit=10000000)
            ], cache
        )
    else:
        controllers = AlPolyHooStaticFilterAPI(
            config.alpaca_key, config.alpaca_secret, config.polygon_key,
            True,
            [
                StaticFloatFilter(upper_limit=10000000)
            ], cache
        )

    filters = [
        ChangeSinceNewsFilter(controllers.market, controllers.news_api, min_limit=0.1)
    ]

    data_scraper(cache, controllers, filters)
