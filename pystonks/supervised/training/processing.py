import math
from typing import List, Tuple

import torch

from pystonks.models import Bar
from pystonks.supervised.annotations.models import Annotation, TradeActions
from pystonks.utils.processing import datetime_to_second_offset, generate_percentages_since_previous_from_bars


def flatten_bars(bars: List[Bar]) -> List[float]:
    res = []
    for b in bars:
        res += [
            float(datetime_to_second_offset(b.timestamp)),
            b.open, b.close, b.high, b.low, float(b.volume)
        ]
    return res


def generate_input_data(balance: float, shares: int, use_percents: bool, input_size: int,
                        bars: List[Bar]) -> torch.Tensor:
    result = [balance, float(shares)]

    if use_percents:
        bars = generate_percentages_since_previous_from_bars(bars)

    fb = flatten_bars(bars)
    if len(fb) < input_size - 2:
        diff = (input_size - 2) - len(fb)
        fb += [0.]*diff
    if len(fb) > input_size - 2:
        fb = fb[:input_size - 2]

    result += fb

    return torch.tensor(result)


def handle_simulated_model_response(current_balance: float, current_shares: int,
                                    current_price: float, action: TradeActions) -> Tuple[float, int]:
    if action == TradeActions.BUY_ALL:
        shares = int(math.floor(current_balance / current_price)) if current_price != 0 else 0
        cost = shares * current_price
        current_shares += shares
        current_balance -= cost
    elif action == TradeActions.BUY_HALF:
        h = current_balance / 2
        hs = int(math.floor(h / current_price)) if current_price != 0 else 0
        tc = hs * current_price
        current_shares += hs
        current_balance -= tc
    elif action == TradeActions.SELL_HALF:
        h = current_shares // 2
        hc = h * current_price
        current_shares -= h
        current_balance += hc
    elif action == TradeActions.SELL_ALL:
        c = current_shares * current_price
        current_shares = 0
        current_balance += c
    elif action == TradeActions.HOLD:
        # do nothing
        pass
    else:
        raise Exception(f'unrecognized action {action.name}({action.value})')

    return current_balance, current_shares


def find_current_balance(initial_balance: float, bars: List[Bar], actions: List[Annotation]) -> Tuple[float, int]:
    aindex = 0
    cb = initial_balance
    cs = 0

    if len(actions) == 0:
        return cb, cs

    for b in bars:
        if b.timestamp == actions[aindex].timestamp:
            bc = b.close
            act = actions[aindex].action

            cb, cs = handle_simulated_model_response(cb, cs, bc, act)

            aindex += 1
            if aindex >= len(actions):
                break

    return cb, cs
