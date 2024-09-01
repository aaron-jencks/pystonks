from typing import List

from pystonks.facades import UnifiedAPI
from pystonks.market.filter import StaticTickerFilter
from pystonks.market.polyhoo import PolyHooSymbolData, StaticFilteredPolyHooSymbolData
from pystonks.trading.alpaca import AlpacaTrader
from pystonks.utils.structures.caching import CacheAPI


class AlPolyHooAPI(UnifiedAPI):
    def __init__(self, alpaca_key: str, alpaca_secret: str, polygon_key: str, paper: bool, cache: CacheAPI):
        alpaca = AlpacaTrader(alpaca_key, alpaca_secret, paper, cache)
        polyhoo = PolyHooSymbolData(polygon_key, cache)
        super().__init__(alpaca, alpaca, alpaca, polyhoo)


class AlPolyHooStaticFilterAPI(UnifiedAPI):
    def __init__(self, alpaca_key: str, alpaca_secret: str, polygon_key: str, paper: bool,
                 static_filters: List[StaticTickerFilter], cache: CacheAPI):
        alpaca = AlpacaTrader(alpaca_key, alpaca_secret, paper, cache)
        polyhoo = StaticFilteredPolyHooSymbolData(polygon_key, static_filters, cache)
        super().__init__(alpaca, alpaca, alpaca, polyhoo)
