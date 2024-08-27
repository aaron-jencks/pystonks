from asyncio import run as arun
import datetime as dt
import os
import pathlib
import tempfile
import unittest

from pystonks.apis.sql import SqliteAPI, SqliteController
from pystonks.market.polyhoo import PolyHooSymbolData
from pystonks.utils.config import read_config


class PolyHooTestCase(unittest.TestCase):
    def setUp(self):
        config = read_config()
        fd, self.fname = tempfile.mkstemp('.db')
        os.close(fd)
        self.cache = SqliteAPI(pathlib.Path(self.fname))
        self.pw = PolyHooSymbolData(config.polygon_key, self.cache)

    def tearDown(self):
        self.cache.reset_connection()
        os.remove(self.fname)
        SqliteController.reset_instances()

    def test_ticker_fetching(self):
        tickers = self.pw.get_ticker_symbols(dt.datetime.now())
        self.assertLessEqual(abs(5000 - len(tickers)), 100, 'polygon always returns ~5000 tickers')
        ctickers = self.pw.get_ticker_symbols(dt.datetime.now())
        self.assertEqual(len(tickers), len(ctickers), 'duplicate calls should return the same data')


if __name__ == '__main__':
    unittest.main()
