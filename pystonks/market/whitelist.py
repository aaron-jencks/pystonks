import pathlib
from abc import ABC
from typing import List, Tuple

from pystonks.market.filter import TickerFilter
from pystonks.market.ticker import CachedTickerFetcher


class WhitelistTickerFetcher(CachedTickerFetcher, ABC):
    def __init__(self, db_loc: pathlib.Path, filters: List[TickerFilter], whitelist: List[str],
                 cache: bool = True):
        super().__init__(db_loc, filters, cache)
        self.whitelist = set(whitelist)
