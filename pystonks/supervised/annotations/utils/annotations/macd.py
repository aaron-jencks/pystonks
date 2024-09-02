import enum
from typing import Dict, List, Tuple

from tqdm import tqdm

from pystonks.supervised.annotations.models import TradeActions
from pystonks.supervised.annotations.utils.annotations.annotator import Annotator
from pystonks.supervised.annotations.utils.metrics import StockMetric, EMAStockMetric, MACDStockMetric, SignalLineMetric
from pystonks.supervised.annotations.utils.models import GeneralStockPlotInfo
from pystonks.utils.processing import datetime_to_second_offset


def data_arrays_to_dict(times: List[int], values: List[float]) -> Dict[int, float]:
    return {t:v for t, v in zip(times, values)}


class CrossoverTypes(enum.Enum):
    NONE = 0
    NEGATIVE = 1
    POSITIVE = 2


def detect_macd_signal_crossover(
        macd_idx: int, macd: List[float],
        signal_idx: int, signal: List[float]
) -> CrossoverTypes:
    if macd_idx == 0 or signal_idx == 0:
        return CrossoverTypes.NONE

    pm = macd[macd_idx-1]
    ps = signal[signal_idx-1]

    if pm == ps:
        return CrossoverTypes.NONE

    cm = macd[macd_idx]
    cs = signal[signal_idx]

    if cm == cs:
        return CrossoverTypes.NEGATIVE if pm > ps else CrossoverTypes.POSITIVE
    elif pm > ps:
        return CrossoverTypes.NEGATIVE if cm < cs else CrossoverTypes.NONE

    return CrossoverTypes.POSITIVE if cm > cs else CrossoverTypes.NONE


class MACDAnnotator(Annotator):
    def annotate(
            self, start: int,
            data: GeneralStockPlotInfo, metrics: Dict[str, StockMetric]
    ) -> List[Tuple[int, TradeActions]]:
        if 'macd' not in metrics or 'signal' not in metrics:
            raise Exception('macd annotator requires macd and signal metrics to be present')

        ema_26: EMAStockMetric = metrics['ema_26_2']
        macd: MACDStockMetric = metrics['macd']
        signal_line: SignalLineMetric = metrics['signal']

        for m in [
            ema_26, macd, signal_line
        ]:
            if m.first_derivative is None:
                m.process_all(data)

        holding = False
        last_buy = -1
        last_sell = -1
        result = []

        _, macd_raw = macd.get_data(data)
        _, signal_raw = signal_line.get_data(data)
        ema_d1 = ema_26.first_derivative
        ema_d2 = ema_26.second_derivative

        macd_offset = 0
        if len(macd_raw) < len(data.bars):
            macd_offset = len(data.bars) - len(macd_raw)

        for bi, b in tqdm(enumerate(data.bars[start:]), desc='Creating Automated MACD/Signal Annotations'):
            idx = start + bi - macd_offset

            if idx < signal_line.module.window:
                continue

            crossover = detect_macd_signal_crossover(
                idx,
                macd_raw,
                idx - signal_line.module.window,
                signal_raw
            )
            if crossover == CrossoverTypes.NONE:
                continue
            elif crossover == CrossoverTypes.POSITIVE and (last_buy < 0 or idx - last_buy > 10):
                if 0 < idx < len(data.bars) - 1 and (ema_d2[idx-1] > 0 or ema_d1[idx-1] > 0):
                    result.append((idx, TradeActions.BUY_HALF))
                    holding = True
            elif crossover == CrossoverTypes.NEGATIVE and holding:
                if 0 < idx < len(data.bars) - 1 and (ema_d2[idx-1] < 0 or ema_d1[idx-1] < 0):
                    result.append((idx, TradeActions.SELL_HALF))

        return result
