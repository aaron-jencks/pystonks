from typing import Callable, Any, List, Tuple, Awaitable
import datetime as dt

from alpaca.data import TimeFrame, TimeFrameUnit

from pystonks.models import Bar, News, Trade


DATED_CALLER = Callable[[dt.datetime], Any]
DATED_CHECKER = Callable[[dt.datetime], bool]
DATED_SAVER = Callable[[dt.datetime, Any], None]


def truncate_datetime(current: dt.datetime) -> dt.datetime:
    return dt.datetime(current.year, current.month, current.day, tzinfo=current.tzinfo)


def process_interval(start: dt.datetime, duration: dt.timedelta,
                     fetch: DATED_CALLER, load: DATED_CALLER, check: DATED_CHECKER, save: DATED_SAVER,
                     exclusive_end: bool = True) -> list:
    current = start
    stop = start + duration

    if stop.date() > dt.date.today() or (stop.date() > dt.date.today() and not exclusive_end):
        raise Exception('invalid interval, end date is >= today')

    result = []
    while (current < stop and exclusive_end) or (current <= stop and not exclusive_end):
        if check(current):
            data = load(current)
        else:
            data = fetch(current)
            save(current, data)
        result.append(data)
        current += dt.timedelta(days=1)
    return result


def change_since_news(bars: List[Bar], news: List[News], minimum: float) -> Tuple[float, int]:
    nindex = 0
    ibar = None
    bindex = None

    news = sorted(news, key=lambda n: n.timestamp)

    for bi, b in enumerate(bars):
        if b.timestamp > news[nindex].updated_at:
            ibar = b
            bindex = bi
            break

    if bindex is None:
        return 0., len(bars)-1

    for bi, b in enumerate(bars[bindex + 1:]):
        if nindex < len(news)-1 and b.timestamp > news[nindex+1].updated_at:
            nindex += 1
            if b.close < ibar.close:
                ibar = b
            continue

        cp = ((b.close - ibar.close) / ibar.close) if ibar.close > 0. else 0.
        if cp > minimum:
            return cp, bi + bindex + 1

    return ((((bars[-1].close - ibar.close) / ibar.close) if ibar.close > 0. else 0.)
            if bars[-1] != ibar else 0., len(bars)-1)


def datetime_to_second_offset(d: dt.datetime) -> int:
    return d.hour * 3600 + d.minute * 60 + d.second


def create_smas_win(times: List[float], bars: List[float],
                    windows: List[int], raw: bool = False) -> List[Tuple[List[float], List[float]]]:
    swin = sorted(windows)
    if len(swin) == 0 or len(bars) < swin[0]:
        return []

    result_x = []
    result_y = []

    if raw:
        result_x.append(times)
        result_y.append(bars)

    for _ in swin:
        result_x.append([])
        result_y.append([])

    for i in range(swin[0], len(bars)+1):
        for wi, win in enumerate(swin):
            if i < win:
                break

            s = bars[i-win:i]
            result_x[wi].append(times[i-1])
            result_y[wi].append(sum(s) / len(s))

    result = []
    for x, y in zip(result_x, result_y):
        result.append((x, y))

    return result


def create_sma(times: List[float], bars: List[float], window: int) -> Tuple[List[float], List[float]]:
    if len(bars) < window:
        return [], []

    result_x = []
    result_y = []

    for i in range(window, len(bars)+1):
        s = bars[i-window:i]
        result_x.append(times[i-1])
        result_y.append(sum(s) / len(s))

    return result_x, result_y


def create_continuous_sma(
        previous_bars: List[float],
        times: List[float], bars: List[float],
        window: int
) -> Tuple[List[float], List[float]]:
    if (len(previous_bars) + len(bars)) < window:
        return [], []

    result_x = []
    result_y = []

    for i in range(1, len(bars)+1):
        if i < window:
            diff = window - (i - 1)
            s = previous_bars[-diff:] + bars[:i]
        else:
            s = bars[i-window:i]
        result_x.append(times[i-1])
        result_y.append(sum(s) / len(s))

    return result_x, result_y


def create_continuous_ema(
        previous_bars: List[float],
        times: List[float], bars: List[float],
        window: int, smoothing: float
) -> Tuple[List[float], List[float]]:
    if (len(previous_bars) + len(bars)) < window:
        return [], []

    # first ema value is the sma
    offset = 0
    if len(previous_bars) < window:
        diff = window - len(previous_bars)
        offset = diff
        previous_ema = sum(previous_bars + bars[:diff]) / window
    else:
        previous_ema = sum(previous_bars[-window:]) / window

    result_x = []
    result_y = []

    if offset >= len(bars):
        return [], []

    ema_multiplier = smoothing / (1 + window)
    def ema(previous: float, current: float) -> float:
        return current * ema_multiplier + previous * (1 - ema_multiplier)

    for i in range(offset, len(bars)):
        current_ema = ema(previous_ema, bars[i])
        result_x.append(times[i])
        result_y.append(current_ema)
        previous_ema = current_ema

    return result_x, result_y


def create_ema(
        times: List[float], bars: List[float],
        window: int, smoothing: float
) -> Tuple[List[float], List[float]]:
    if len(bars) < window:
        return [], []

    # first ema value is the sma
    offset = 0
    previous_ema = sum(bars[:window]) / window

    result_x = []
    result_y = []

    if offset >= len(bars):
        return [], []

    ema_multiplier = smoothing / (1 + window)
    def ema(previous: float, current: float) -> float:
        return current * ema_multiplier + previous * (1 - ema_multiplier)

    for i in range(window, len(bars)):
        current_ema = ema(previous_ema, bars[i])
        result_x.append(times[i])
        result_y.append(current_ema)
        previous_ema = current_ema

    return result_x, result_y


def calculate_derivatives(times: List[float], data: List[float]) -> Tuple[List[float], List[float]]:
    d1 = [
        (data[i] - data[i - 1]) / (times[i] - times[i - 1])
        for i in range(1, len(data))
    ]

    d2 = [
        (data[i+1] - 2 * data[i] + data[i - 1]) /
        ((times[i+1] - times[i]) * (times[i+1] - times[i]))
        for i in range(1, len(data)-1)
    ]

    return d1, d2


def calculate_normalized_derivatives(times: List[float], data: List[float]) -> Tuple[List[float], List[float]]:
    d1, d2 = calculate_derivatives(times, data)
    return normalize_bipolar(d1), normalize_bipolar(d2)


def normalize(data: List[float]) -> List[float]:
    mn = min(data)
    mx = max(data)
    return [(d - mn) / (mx - mn) for d in data]


def normalize_bipolar(data: List[float]) -> List[float]:
    if len(data) == 0:
        return []

    mn = min(data)
    mx = max(data)
    return [(d / mx) if d > 0 else -(d / mn) for d in data] if mx != mn else ([0]*len(data))


def timeframe_to_delta(tf: TimeFrame) -> dt.timedelta:
    if tf.unit != TimeFrameUnit.Month:
        return dt.timedelta(
            **{
                tf.unit.name.lower() + 's': tf.amount
            }
        )
    return dt.timedelta(days=30)  # TODO this is a limitation


def fill_in_sparse_bars(start: dt.datetime, stop: dt.datetime, delta: dt.timedelta, bars: List[Bar]) -> List[Bar]:
    if len(bars) == 0:
        return []

    result = []
    bars = sorted(bars, key=lambda b: b.timestamp)

    symbol = bars[0].symbol
    date = truncate_datetime(start)
    init = start
    sinit = datetime_to_second_offset(init)
    nearest_bucket = sinit - (sinit % int(delta.total_seconds()))

    bstart = nearest_bucket
    nts = bstart + int(delta.total_seconds())
    previous = Bar(symbol, init, 0, 0, 0, 0, 0)
    bts = date + dt.timedelta(seconds=nts)

    for b in bars:
        sts = datetime_to_second_offset(b.timestamp)
        while sts > nts:
            result.append(Bar(symbol, bts, previous.close, previous.close, previous.close, previous.close, 0))
            bts += delta
            nts += int(delta.total_seconds())
        result.append(b)
        previous = b
        bts += delta
        nts += int(delta.total_seconds())

    while bts < stop:
        result.append(Bar(symbol, bts, previous.close, previous.close, previous.close, previous.close, 0))
        bts += delta
        nts += int(delta.total_seconds())

    return result


def trim_zero_bars(bars: List[Bar]) -> List[Bar]:
    index = -1
    for bi, b in enumerate(bars):
        if not b.zero():
            index = bi
            break
    if index > 0:
        return bars[index:]
    return bars


def find_bars(delta: dt.timedelta, trades: List[Trade]) -> List[Bar]:
    if len(trades) == 0:
        return []
    result = []
    trades = sorted(trades, key=lambda t: t.timestamp)

    symbol = trades[0].symbol
    date = truncate_datetime(trades[0].timestamp)
    init = trades[0].timestamp
    sinit = datetime_to_second_offset(init)
    nearest_bucket = sinit - (sinit % int(delta.total_seconds()))

    start = nearest_bucket
    nts = start + int(delta.total_seconds())
    o, c, l, h, v = 0, 0, 0, 0, 0
    ct = 0
    first = True
    bts = date + dt.timedelta(seconds=start)
    for t in trades:
        p = t.price
        sts = datetime_to_second_offset(t.timestamp)
        if sts <= nts:
            if first:
                o = p
                l = p
                h = p
                first = False
            if p < l:
                l = p
            if p > h:
                h = p
            c = p
            v += int(t.count)
            ct += 1
        else:
            result.append(Bar(
                symbol, bts, o, c, h, l, v
            ))
            start = nts
            nts += int(delta.total_seconds())
            bts = date + dt.timedelta(seconds=start)
            while sts > nts:
                result.append(Bar(
                    symbol, bts, c, c, c, c, 0
                ))
                start = nts
                nts += int(delta.total_seconds())
                bts = date + dt.timedelta(seconds=start)
            o = p
            l = p
            h = p
            c = p
            v = int(t.count)
            ct = 1
    if ct > 0:
        result.append(Bar(
            symbol, bts, o, c, h, l, v
        ))
    return result


def generate_percentages_since_previous_from_bars(bars: List[Bar]) -> List[Bar]:
    if len(bars) < 2:
        return []

    def percent_change(new: float, old: float) -> float:
        return (new - old) / old

    result = []
    previous = bars[0]
    for b in bars[1:]:
        pb = Bar(
            b.symbol,
            b.timestamp,
            percent_change(b.open, previous.open),
            percent_change(b.close, previous.close),
            percent_change(b.high, previous.high),
            percent_change(b.low, previous.low),
            b.volume
        )
        result.append(pb)
        previous = b

    return result


def generate_percentages_since_bar_from_bars(reference: Bar, bars: List[Bar]) -> List[Bar]:
    if len(bars) == 0:
        return []

    def percent_change(new: float, old: float) -> float:
        return (new - old) / old

    result = []
    first = reference.close
    for b in bars:
        pb = Bar(
            b.symbol,
            b.timestamp,
            percent_change(b.open, first),
            percent_change(b.close, first),
            percent_change(b.high, first),
            percent_change(b.low, first),
            b.volume
        )
        result.append(pb)

    return result
