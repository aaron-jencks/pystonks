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

        macd_raw = data_arrays_to_dict(*macd.get_data(data))
        signal_raw = data_arrays_to_dict(*signal_line.get_data(data))
        ema_d1 = data_arrays_to_dict(*ema_26.first_derivative)
        ema_d2 = data_arrays_to_dict(*ema_26.first_derivative)

        for b in tqdm(data.bars[start:], desc='Creating Automated MACD/Signal Annotations'):
            ts = datetime_to_second_offset(b.timestamp)
            if ts in macd_raw and ts in signal_raw:
                diff = abs(macd_raw[ts] - signal_raw[ts])
                if diff < 0.1:
                    if ts in ema_d2:
                        d2v = ema_d2[ts]
                        if d2v > 0:
                            result.append((ts, TradeActions.BUY_HALF))
                            holding = True
                        elif d2v < 0 and holding:
                            result.append((ts, TradeActions.SELL_HALF))
                        elif ts in ema_d1:
                            d1v = ema_d1[ts]
                            if d1v > 0:
                                result.append((ts, TradeActions.BUY_HALF))
                                holding = True
                            elif d1v < 0:
                                result.append((ts, TradeActions.SELL_HALF))

        return result
