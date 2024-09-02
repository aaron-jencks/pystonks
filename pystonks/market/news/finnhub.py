import datetime as dt
import time
from typing import List

import finnhub

from pystonks.apis.sql import SQL_DATE_FMT
from pystonks.facades import NewsDataAPI
from pystonks.models import News
from pystonks.utils.processing import process_interval, truncate_datetime
from pystonks.utils.structures.caching import CachedClass, CacheAPI


RATE_LIMIT = 1.


class FinnhubNewsAPI(CachedClass, NewsDataAPI):
    def __init__(self, api_key: str, cache: CacheAPI):
        super().__init__(cache)
        self.api_key = api_key
        self.client = finnhub.Client(api_key=self.api_key)
        self.last_req = None

    def setup_tables(self):
        self.db.create_table(
            'news',
            'news_id integer, '
            'symbol text, timestamp text, updated_at text,'
            'author text, headline text, url text, '
            'primary key (news_id, symbol)'
        )
        self.db.create_table(
            'news_date_processed',
            'symbol text, date text, primary key (symbol, date)'
        )

    def handle_request(self):
        if self.last_req is not None and (dt.datetime.now() - self.last_req).total_seconds() < RATE_LIMIT:
            diff = RATE_LIMIT - (dt.datetime.now() - self.last_req).total_seconds()
            time.sleep(diff)
        self.last_req = dt.datetime.now()

    def check_exists(self, symbol: str, date: dt.datetime) -> bool:
        return self.cache_check(
            'news_date_processed',
            condition='symbol = ? and date = ?',
            params=(symbol, date.strftime(SQL_DATE_FMT))
        )

    def save_exists(self, symbol: str, date: dt.datetime):
        self.cache_save(
            'news_date_processed',
            params=(symbol, date.strftime(SQL_DATE_FMT))
        )

    def news(self, symbol: str) -> List[News]:
        self.handle_request()
        date = dt.date.today()
        data = self.client.company_news(symbol, date.strftime(SQL_DATE_FMT), date.strftime(SQL_DATE_FMT))
        return [
            News(
                symbol,
                dt.datetime.fromtimestamp(n['datetime'], tz=dt.UTC),
                n['id'], n['source'], n['headline'], n['url'],
                dt.datetime.fromtimestamp(n['datetime'], tz=dt.UTC)
            )
            for n in data
        ]


    def historical_news(self, symbol: str, start: dt.datetime, dur: dt.timedelta) -> List[News]:
        def checker(date: dt.datetime) -> bool:
            return self.check_exists(symbol, date)

        def fetcher(date: dt.datetime) -> List[News]:
            self.handle_request()
            date = dt.date.today()
            data = self.client.company_news(symbol, date.strftime(SQL_DATE_FMT), date.strftime(SQL_DATE_FMT))
            return [
                News(
                    symbol,
                    dt.datetime.fromtimestamp(n['datetime'], tz=dt.UTC),
                    n['id'], n['source'], n['headline'], n['url'],
                    dt.datetime.fromtimestamp(n['datetime'], tz=dt.UTC)
                )
                for n in data
            ]

        def loader(date: dt.datetime) -> List[News]:
            rows = self.cache_lookup('news', condition='symbol = ? and date(updated_at) = ?',
                                     params=(symbol, date.strftime(SQL_DATE_FMT)))
            return [
                News(symbol, dt.datetime.fromisoformat(ts), nid, a, h, u, dt.datetime.fromisoformat(ua))
                for nid, _, ts, ua, a, h, u in rows
            ]

        news_params = []

        def saver(date: dt.datetime, rows: List[News]):
            self.save_exists(symbol, date)
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
