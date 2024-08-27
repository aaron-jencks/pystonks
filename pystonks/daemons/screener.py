import datetime as dt
import multiprocessing as mp
import sys
import time
from typing import List

from tqdm import tqdm

from pystonks.market.filter import TickerFilter


def screener(tickers: List[str], filters: List[TickerFilter], interval: dt.timedelta, output: mp.Queue):
    while True:
        loop_start = dt.datetime.now()

        print('starting screener loop', file=sys.stderr)
        for t in tickers:
            if all([f.passes(t, dt.datetime.today()) for f in filters]):
                print('found screened stock: {}'.format(t), file=sys.stderr)
                output.put(t)

        while (dt.datetime.now() - loop_start) < interval:
            time.sleep(1)


def ticker_passes(ticker: str, date: dt.datetime, filters: List[TickerFilter]) -> bool:
    return all([f.passes(ticker, date) for f in filters])


def hscreener(tickers: List[str], filters: List[TickerFilter], date: dt.datetime) -> List[str]:
    # print('screening stocks...', end='')
    # with mp.Pool() as p:
    #     passes = p.starmap(ticker_passes, [(t, date, filters) for t in tickers])
    # print('DONE')
    # return [t for t, p in zip(tickers, passes) if p]
    return [t for t in tqdm(tickers, desc='screening stocks') if all([f.passes(t, date) for f in filters])]
