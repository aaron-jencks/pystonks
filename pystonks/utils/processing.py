from typing import Callable, Any, List, Tuple, Awaitable
import datetime as dt

from alpaca.data import TimeFrame, TimeFrameUnit

from pystonks.models import Bar, News, Trade


DATED_CALLER = Callable[[dt.datetime], Any]
DATED_CHECKER = Callable[[dt.datetime], bool]
DATED_SAVER = Callable[[dt.datetime, Any], None]


def truncate_datetime(current: dt.datetime) -> dt.datetime:
    """
    Takes a datetime object and resets it's time offset to midnight, or zero
    :param current: The datetime to truncate
    :return: Returns a datetime object with the same date, but the time set to zero
    """
    return dt.datetime(current.year, current.month, current.day, tzinfo=current.tzinfo)


def process_interval(start: dt.datetime, duration: dt.timedelta,
                     fetch: DATED_CALLER, load: DATED_CALLER, check: DATED_CHECKER, save: DATED_SAVER,
                     exclusive_end: bool = True) -> list:
    """
    Loops through an interval, one day at a time and uses the given handlers to generate a collated list of results.
    Essentially steps through each day and checks if the `check` method returns True.
    If it does, then it calls `load` for the given date, otherwise it calls `fetch` for the given date.
    If `fetch` was called, then it's results are placed into a list and after the interval is parsed, `save` is called
    with the list.
    Then it increments the date by one day and repeats until the end of the interval is reached.
    This is used for fetching possibly cached data, like stocks, or news, or trades, or bars, etc...
    :param start: The start date of the interval
    :param duration: The duration of the interval (keep in mind that a single day is the smallest increment recognized)
    :param fetch: A handler used to fetch data that didn't exist in the cache
    :param load: A handler used to load data from the cache
    :param check: A handler to determine if data is in the cache, or should be fetched
    :param save: A handler for saving data to the cache after it was fetched
    :param exclusive_end: If this is False and the end of the interval is the current date, then an exception is thrown.
    If it is True, and the end of the interval is past the current date (in the future) then an exception is thrown.
    :return: Returns a collated list of data collected by parsing the interval. This takes the form of:
    [
        [day 1],
        [day 2],
        ...,
        [day n]
    ]
    """
    current = start
    stop = start + duration

    if stop.date() >= dt.date.today() and not exclusive_end:
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
    """
    Determines, given a list of news articles, and a list of candlestick data if:
    At some point in the candlestick data, the close price has increased by the given `minimum` percent (0-1).
    That is to say that since a news article was published the price of the stock has increased by at leaste
    the `minimum` percent.
    :param bars: The candlestick data to check
    :param news: The news articles to use for the check
    :param minimum: The minimum change in percent that the profit must increase by to count
    :return: If found, returns the first index in the candlestick data that exceeds the threshold,
    and the percent change. If not found, returns the current percent change, and the last index of the data.
    """
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
    """
    Converts a given datetime to the number of seconds elapsed since midnight
    :param d: The datetime to convert
    :return: The number of seconds that have elapsed since midnight
    """
    return d.hour * 3600 + d.minute * 60 + d.second


def create_smas_win(times: List[float], bars: List[float],
                    windows: List[int], raw: bool = False) -> List[Tuple[List[float], List[float]]]:
    """
    Creates a series of Simple Moving Averages (SMAs) and returns them in ascending order by window size.
    :param times: The times to use for calculating the averages
    :param bars: The data to use for calculating the averages
    :param windows: A list of window sizes, the number of values to average, for each SMA
    :param raw: If True, the `times` and `bars` are also included in the result
    :return: Returns the list of SMAs in ascending order by their window size. Each in the form of (time, values)
    """
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
    """
    Creates a Simple Moving Average (SMA)
    :param times: The times to use for calculating the average
    :param bars: The data to use for calculating the average
    :param window: A window size, the number of values to average, for the SMA
    :return: Returns the generated SMA in the form of (time, values)
    """
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
    """
    Creates a Simple Moving Average (SMA), similar to `create_sma`,
    but the output length of the SMA is the same as the `bars` list.
    :param previous_bars: The data that occurred before the firs time in `times`,
    used to calculate the first value of the SMA
    :param times: The times to use for calculating the average
    :param bars: The data to use for calculating the average
    :param window: A window size, the number of values to average, for the SMA
    :return: Returns the generated SMA in the form of (time, values)
    """
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
    """
    Creates an Exponential Moving Average (EMA), similar to `create_ema`,
    but the output length of the EMA is the same as the `bars` list.
    :param previous_bars: The data that occurred before the firs time in `times`,
    used to calculate the first value of the EMA
    :param times: The times to use for calculating the average
    :param bars: The data to use for calculating the average
    :param window: A window size, the number of values to average, for the EMA
    :param smoothing: The value to smooth the averages by
    :return: Returns the generated EMA in the form of (time, values)
    """
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
    """
    Creates an Exponential Moving Average (EMA)
    :param times: The times to use for calculating the average
    :param bars: The data to use for calculating the average
    :param window: A window size, the number of values to average, for the EMA
    :param smoothing: The value to smooth the averages by
    :return: Returns the generated EMA in the form of (time, values)
    """
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
    """
    Calculates the discrete first and second derivatives of the times and data given
    :param times: The times used to calculate dt for the derivatives
    :param data: The data used to calculate dx for the derivatives
    :return: Returns the x values of the first and second derivatives (first, second)
    """
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


def calculate_normalized_derivatives(
        times: List[float], data: List[float]
) -> Tuple[List[float], List[float]]:
    """
    Calculates the first and second derivatives, but their values are normalized to be in the range (-1, 1)
    :param times: The times used to calculate dt for the derivatives
    :param data: The data used to calculate dx for the derivatives
    :return: Returns the x values of the first and second derivatives (first, second)
    """
    d1, d2 = calculate_derivatives(times, data)
    return normalize_bipolar(d1), normalize_bipolar(d2)


def calculate_normalized_price_derivatives(
        times: List[float], data: List[float], prices: List[float]
) -> Tuple[List[float], List[float]]:
    """
    Similar to the other derivative functions, this calculates the first and second derivative of the given data.
    It is normalized by the prices given in prices, this is a better normalization method than min-max normalization.
    :param times: The times used to calculate dt for the derivatives
    :param data: The data used to calculate dx for the derivatives
    :param prices: The prices to normalize the data by
    :return: Returns the x values of the first and second derivatives (first, second)
    """
    nd1 = [
        (((data[i] - data[i - 1]) / ((times[i] - times[i - 1]) * prices[i-1])) if prices[i-1] != 0 else 0) * 100
        for i in range(1, len(data))
    ]

    nd2 = [
        ((
            (data[i + 1] - 2 * data[i] + data[i - 1]) /
            ((times[i + 1] - times[i]) * (times[i + 1] - times[i]) * (prices[i] - prices[i-1]))
        ) if (prices[i] - prices[i-1]) != 0 else 0) * 100
        for i in range(1, len(data) - 1)
    ]
    return nd1, nd2


def normalize(data: List[float]) -> List[float]:
    """
    Performs min-max normalization on the given data
    :param data: The data to normalize
    :return: Returns a list of normalized data (0, 1)
    """
    mn = min(data)
    mx = max(data)
    return [(d - mn) / (mx - mn) for d in data]


def normalize_bipolar(data: List[float]) -> List[float]:
    """
    Similar to `normalize` performs min-max normalization on the data,
    but also down shifts the data to make the range (-1, 1) instead of (0, 1).
    :param data: The data to normalize
    :return: Returns a list of normalized data (-1, 1)
    """
    if len(data) == 0:
        return []

    mn = min(data)
    mx = max(data)
    return [(d / mx) if d > 0 else -(d / mn) for d in data] if mx != mn else ([0]*len(data))


def timeframe_to_delta(tf: TimeFrame) -> dt.timedelta:
    """
    Takes an alpaca TimeFrame and converts it into a datetime.timedelta.
    **NOTE**: because datetime does not have a months argument, the month TimeFrame is truncated to 30 days.
    :param tf: The TimeFrame to convert
    :return: Returns a corresponding datetime.timedelta value.
    """
    if tf.unit != TimeFrameUnit.Month:
        return dt.timedelta(
            **{
                tf.unit.name.lower() + 's': tf.amount
            }
        )
    return dt.timedelta(days=30)  # MONTH TRUNCATION


def fill_in_sparse_bars(start: dt.datetime, stop: dt.datetime, delta: dt.timedelta, bars: List[Bar]) -> List[Bar]:
    """
    Takes a list of possibly sparse candlestick data and fills in gaps
    so that the resulting list has a predeterminable number of values.
    :param start: The start time of the interval of the result
    :param stop: The stop time of the interval of the result
    :param delta: The maximum amount of time allowed per candlestick
    :param bars: The possibly sparse candlestick data
    :return: Returns a list of candlestick data that will contain the number of `delta`s that will occur between
    `start` and `stop`
    """
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
    """
    Takes a list of candlestick data and trims off all of the bars at the beginning that fit the condition
    that all of their price values are zero.
    :param bars: The data to trim
    :return: Returns the candlestick data with the empty prefixes trimmed off.
    """
    index = -1
    for bi, b in enumerate(bars):
        if not b.zero():
            index = bi
            break
    if index > 0:
        return bars[index:]
    return bars


def find_bars(delta: dt.timedelta, trades: List[Trade]) -> List[Bar]:
    """
    Takes a list of trades, and computes the candlestick data. This candlestick data is not sparse,
    but the interval for the result will start and end with the timestamps in the trades supplied.
    :param delta: The maximum time allowed per candlestick
    :param trades: The trades to parse into candlesticks
    :return: Returns a list of generated candlesticks
    """
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
    """
    Takes candlestick data and transforms so that each value contains the percent change since the previous value.
    This directly maps the properties, so the percent change of the open,
    is the percent change since the previous value's open.
    And the close is the percent change since the previous value's close.
    :param bars: The candlestick data to convert to percentages
    :return: Returns the transformed candlestick data.
    """
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
    """
    Similar to `generate_percentages_since_previous_from_bars`, but each bar is compared to a single external bar.
    And each property is compared to the external bar's close value.
    :param reference: The external bar to compare to
    :param bars: The candlestick data to transform
    :return: Returns a list of candlesticks transformed to compare to the close of the reference.
    Has the same length as the input `bars`.
    """
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
