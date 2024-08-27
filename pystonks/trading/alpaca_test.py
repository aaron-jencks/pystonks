import datetime as dt
import os
import pathlib
import tempfile
import unittest

from pystonks.apis.sql import SqliteAPI, SqliteController
from pystonks.trading.alpaca import AlpacaTrader
from pystonks.utils.config import read_config


class TraderTestCase(unittest.TestCase):
    def setUp(self):
        config = read_config()
        fd, self.fname = tempfile.mkstemp('.db')
        os.close(fd)
        self.cache = SqliteAPI(pathlib.Path(self.fname))
        self.trader = AlpacaTrader(config.alpaca_key, config.alpaca_secret, True, self.cache)

    def tearDown(self):
        self.cache.reset_connection()
        os.remove(self.fname)
        SqliteController.reset_instances()

    def test_trade_fetching(self):
        ticker = 'QLGN'
        trades = self.trader.historical_trades(ticker, dt.datetime(2024, 6, 27), dt.timedelta(days=1))
        self.assertGreater(len(trades), 0, 'trades should return trades')

    def test_bar_caching(self):
        ticker = 'QLGN'
        start = dt.datetime(2024, 6, 27)
        dur = dt.timedelta(days=2)
        bars = self.trader.historical_bars(ticker, start, dur)
        self.assertGreater(len(bars), 0, 'bars should return bars')
        cbars = self.trader.historical_bars(ticker, start, dur)
        self.assertEqual(len(bars), len(cbars), 'cached data should match uncached data')

    def test_was_market_open(self):
        resp = self.trader.was_market_open(dt.datetime(2024, 6, 27))
        self.assertTrue(resp, 'market was open on this day')

    def test_news_caching(self):
        ticker = 'AAPL'
        interval = dt.timedelta(days=2)
        start = dt.datetime(2024, 6, 1)
        news = self.trader.historical_news(ticker, start, interval)
        self.assertEqual(len(news), 5, 'incorrect number of news articles returned')
        cnews = self.trader.historical_news(ticker, start, interval)  # should be cached
        self.assertEqual(len(news), len(cnews), 'two consecutive calls should be equal')

    def test_bar_caching_2(self):
        ticker = 'QLGN'
        start = dt.datetime(2024, 7, 26)
        dur = dt.timedelta(days=1)
        bars = self.trader.historical_bars(ticker, start, dur)
        self.assertGreater(len(bars), 0, 'bars should return bars')
        cbars = self.trader.historical_bars(ticker, start, dur)
        self.assertEqual(len(bars), len(cbars), 'cached data should match uncached data')

    def test_bar_overlap(self):
        ticker = 'IMNN'
        start = dt.datetime(2024, 7, 29)
        dur = dt.timedelta(days=1)
        bars = self.trader.historical_bars(ticker, start, dur)
        self.assertEqual(len(bars), 260, 'bars should return a single day\'s worth of bars')


if __name__ == '__main__':
    unittest.main()
