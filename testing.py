import pathlib
from argparse import ArgumentParser
import unittest

from pystonks.utils.config import read_config
from pystonks.apis.sql_test import SqlTestCase
from pystonks.trading.alpaca_test import TraderTestCase
from pystonks.market.polyhoo_test import PolyHooTestCase
from pystonks.utils.processing_test import ProcessingTestCase
from pystonks.daemons.screener_test import ScreenerTestCase

if __name__ == '__main__':
    ap = ArgumentParser(description='runs the unit tests for the rest of the project')
    ap.add_argument('--config', type=pathlib.Path, default=pathlib.Path('./config.json'),
                    help='the location of the configuration file')
    args = ap.parse_args()

    read_config(args.config)

    unittest.main()
