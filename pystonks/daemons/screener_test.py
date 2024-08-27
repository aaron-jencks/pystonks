import datetime as dt
import os
import pathlib
import tempfile
import unittest

from pystonks.apis.sql import SqliteAPI, SqliteController
from pystonks.daemons.screener import hscreener
from pystonks.market.filter import ChangeSinceNewsFilter
from pystonks.trading.alpaca import AlpacaTrader
from pystonks.utils.config import read_config


class ScreenerTestCase(unittest.TestCase):
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

    def test_pcsa(self):
        """
        the high of this sequence is 2.24 and the news close is 2.05, it should not pass
        """
        ticker = 'PCSA'
        date = dt.datetime(2024, 8, 1, tzinfo=dt.timezone.utc)
        result = hscreener([ticker], [ChangeSinceNewsFilter(self.trader, min_limit=0.1)], date)
        self.assertEqual(len(result), 0, 'ticker pcsa should not pass the filter')


if __name__ == '__main__':
    unittest.main()
