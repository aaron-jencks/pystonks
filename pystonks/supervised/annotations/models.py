import datetime as dt
from enum import Enum


class TradeActions(Enum):
    HOLD = 0
    BUY_HALF = 1
    BUY_ALL = 2
    SELL_HALF = 3
    SELL_ALL = 4
    ACTION_COUNT = 5


class Annotation:
    def __init__(self, symbol: str, timestamp: dt.datetime, action: TradeActions):
        self.symbol = symbol
        self.timestamp = timestamp
        self.action = action
