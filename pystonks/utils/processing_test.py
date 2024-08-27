import datetime as dt
import os
import pathlib
import tempfile
import unittest

from alpaca.data import TimeFrame

from pystonks.apis.sql import SqliteAPI, SqliteController
from pystonks.trading.alpaca import AlpacaTrader
from pystonks.utils.config import read_config
from pystonks.utils.processing import find_bars, timeframe_to_delta, fill_in_sparse_bars, truncate_datetime, \
    datetime_to_second_offset, calculate_normalized_derivatives, change_since_news


class ProcessingTestCase(unittest.TestCase):
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

    def test_manual_bar_processing(self):
        ticker = 'QLGN'
        start = dt.datetime(2024, 7, 26, tzinfo=dt.timezone.utc)
        dur = dt.timedelta(days=1)
        stop = dt.datetime(2024, 7, 27, tzinfo=dt.timezone.utc)
        bucket_size = dt.timedelta(minutes=1)
        bars = fill_in_sparse_bars(start, stop, bucket_size, self.trader.historical_bars(ticker, start, dur))
        mbars = fill_in_sparse_bars(
            start, stop, bucket_size,
            find_bars(timeframe_to_delta(TimeFrame.Minute), self.trader.historical_trades(ticker, start, dur))
        )
        self.assertEqual(len(bars), len(mbars), 'alpaca bars should match manual bars')

    def test_unique_timestamps(self):
        ticker = 'IMNN'
        timestamp = dt.datetime(2024, 7, 29,
                                13, 9, 59, 129272,
                                tzinfo=dt.timezone.utc)
        bars = fill_in_sparse_bars(
            truncate_datetime(timestamp),
            truncate_datetime(timestamp + dt.timedelta(days=1)),
            dt.timedelta(minutes=1),
            self.trader.historical_bars(ticker, timestamp, dt.timedelta(days=1))
        )
        closes = [b.close for b in bars]
        times = [datetime_to_second_offset(b.timestamp) for b in bars]
        self.assertEqual(len(times), len(set(times)), 'generated timestamps should be unique')
        try:
            calculate_normalized_derivatives(times, closes)
        except:
            self.fail('error occurred while processing derivatives')

    def test_change_detection(self):
        ticker = 'QXO'
        timestamp = dt.datetime(2024, 7, 30, tzinfo=dt.timezone.utc)
        bars = fill_in_sparse_bars(
            truncate_datetime(timestamp),
            truncate_datetime(timestamp + dt.timedelta(days=1)),
            dt.timedelta(minutes=1),
            self.trader.historical_bars(ticker, timestamp, dt.timedelta(days=1))
        )
        news = self.trader.historical_news(ticker, timestamp, dt.timedelta(days=1))
        amt, idx = change_since_news(bars, news, 0.1)
        self.assertLessEqual(idx, 800, 'change index should match spike in price')


if __name__ == '__main__':
    unittest.main()
