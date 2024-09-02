from typing import Dict, List, Tuple

from tqdm import tqdm

from pystonks.supervised.annotations.models import TradeActions
from pystonks.supervised.annotations.utils.annotations.annotator import Annotator
from pystonks.supervised.annotations.utils.metrics import StockMetric, EMAStockMetric, MACDStockMetric, SignalLineMetric
from pystonks.supervised.annotations.utils.models import GeneralStockPlotInfo
from pystonks.utils.processing import datetime_to_second_offset


def data_arrays_to_dict(times: List[int], values: List[float]) -> Dict[int, float]:
    return {t:v for t, v in zip(times, values)}


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

        for bi, b in tqdm(enumerate(data.bars[start:]), desc='Creating Automated MACD/Signal Annotations'):
            idx = start + bi

            if idx < signal_line.module.window:
                continue

            diff = abs(macd_raw[idx] - signal_raw[idx - signal_line.module.window])
            if diff < 0.001:
                if 0 < idx < len(data.bars) - 1:
                    d2v = ema_d2[idx-1]
                    if d2v > 0:
                        result.append((idx, TradeActions.BUY_HALF))
                        holding = True
                    elif d2v < 0 and holding:
                        result.append((idx, TradeActions.SELL_HALF))
                    elif idx > 0:
                        d1v = ema_d1[idx-1]
                        if d1v > 0:
                            result.append((idx, TradeActions.BUY_HALF))
                            holding = True
                        elif d1v < 0:
                            result.append((idx, TradeActions.SELL_HALF))

        return result
