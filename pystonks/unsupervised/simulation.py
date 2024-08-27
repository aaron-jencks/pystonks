import datetime as dt
from typing import Dict, Callable, List, Tuple

from pystonks.facades import MarketDataAPI, HistoricalMarketDataAPI
from pystonks.models import Trade
from pystonks.supervised.annotations.models import TradeActions
from pystonks.trading.simulated import SimulatedTrader, SimulationResults

Evaluator = Callable[[List[float], float, float], TradeActions]


class StockIterator:
    def __init__(self, data: List[Trade]):
        self.data = sorted(data, key=lambda d: d.timestamp)
        self.initial = self.data[0].timestamp
        self.window = dt.timedelta(days=1)
        self.current: dt.datetime = self.initial + self.window
        self.ci = 0

    def reset(self):
        self.current = self.initial + self.window
        self.ci = 0

    def next(self) -> List[Trade]:
        result = []

        while len(result) == 0:  # periods with no trades, kinda useless for training
            while self.ci < len(self.data) and self.data[self.ci].timestamp < self.current:
                result.append(self.data[self.ci])
                self.ci += 1

            # skip weekends
            self.current += self.window
            while self.current.weekday() > 4:
                self.current += self.window

        return result

    def done(self) -> bool:
        return self.ci >= len(self.data)


class StockSimulator:

    tfmt = '%j/%Y'
    cache: Dict[str, List[Trade]] = {}

    def __init__(self, t: HistoricalMarketDataAPI):
        self.trader = t

    def fetch_data(self, symbol: str, start: dt.date, days: int) -> List[Trade]:
        td = dt.timedelta(days=days)
        dsh = f'{start.strftime(self.tfmt)}>{(start + td).strftime(self.tfmt)}'
        if dsh in StockSimulator.cache:
            return StockSimulator.cache[dsh]
        trades = self.trader.historical_trades(
            symbol,
            dt.datetime(start.year, start.month, start.day),
            td)
        StockSimulator.cache[dsh] = trades
        return trades

    def get_iterator(self, symbol: str, start: dt.date, days: int) -> StockIterator:
        return StockIterator(self.fetch_data(symbol, start, days))


def run_simulation(trader: SimulatedTrader, bars: List[Tuple[str, List[List[float]]]], ev: Evaluator) -> SimulationResults:
    holds = 0
    trader.reset()
    ibalance = trader.balance()
    for symbol, day_data in bars:
        for dslice in day_data:
            cash = trader.balance()
            shares = trader.shares(symbol)
            action = ev(dslice, cash, shares)
            sprice = dslice[-4]
            if action == TradeActions.BUY_HALF:
                trader.buy((trader.balance() / 2) / sprice, sprice + 0.1)
            elif action == TradeActions.SELL_HALF:
                trader.sell(shares // 2, sprice - 0.1)
            elif action == TradeActions.SELL_ALL:
                trader.sell(shares, sprice - 0.1)
            elif action == TradeActions.HOLD:
                holds += 1
            else:
                raise Exception(f'unrecognized action {action.value}')
    res = trader.get_results()
    res.total_profit = trader.balance() - ibalance
    return res
