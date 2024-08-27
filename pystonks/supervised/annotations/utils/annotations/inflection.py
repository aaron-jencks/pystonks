from typing import List, Tuple, Dict

from tqdm import tqdm

from pystonks.models import Bar
from pystonks.supervised.annotations.models import TradeActions
from pystonks.supervised.annotations.utils.annotations.annotator import Annotator
from pystonks.supervised.annotations.utils.metrics import StockMetric, SMAStockMetric
from pystonks.supervised.annotations.utils.models import GeneralStockPlotInfo
from pystonks.utils.processing import calculate_normalized_derivatives


class PeakAnnotator(Annotator):
    def __init__(self, trough_limit: float = 0.1, peak_limit: float = 0.8):
        self.trough_limit = trough_limit
        self.peak_limit = peak_limit

    def annotate(self, start: int, data: GeneralStockPlotInfo,
                 metrics: Dict[str, StockMetric]) -> List[Tuple[int, TradeActions]]:
        smas: List[SMAStockMetric] = [metrics[k] for k in metrics if isinstance(metrics[k], SMAStockMetric)]
        swin = sorted(smas, key=lambda s: s.module.window)

        if len(swin) < 3 or len(data.bars) < swin[-1].module.window + 2:
            return []

        for s in smas:
            if s.first_derivative is None:
                s.process_all(data)

        holding = False
        last_buy = -1
        last_sell = -1
        result = []

        for i in tqdm(range(start, len(data.bars)), desc='Creating Automated Annotations'):
            ad1l = abs(swin[0].first_derivative[i - swin[0].module.window])

            if not holding and ad1l < self.trough_limit < swin[-1].first_derivative[i - swin[-1].module.window]:
                result.append((i, TradeActions.BUY_HALF))
                holding = True
                last_buy = i
                continue

            if not holding or (holding and (i - last_buy) < 3):
                continue

            change_since_buy = (data.bars[i].close - data.bars[last_buy].close) / data.bars[last_buy].close
            change_since_sell = ((data.bars[i].close - data.bars[last_sell].close) / data.bars[last_sell].close) \
                if last_sell >= 0 else 100

            if ((last_sell < 0 or change_since_sell >= 0.01)
                    and change_since_buy >= 0.01 and ad1l < self.trough_limit):
                result.append((i, TradeActions.SELL_HALF))
                last_sell = i
                continue

            if i == len(data.bars) - 1 and holding:
                result.append((i, TradeActions.SELL_ALL))
                holding = False
                last_sell = i
                continue

            d2m = swin[1].second_derivative[i - swin[1].module.window]
            d2h = swin[-1].second_derivative[i - swin[-1].module.window]

            if ((last_sell < 0 or change_since_sell >= 0.01) and change_since_buy >= 0.01 and
                    d2m < 0 < d2h < self.trough_limit and ad1l < self.trough_limit):
                result.append((i, TradeActions.SELL_ALL))
                holding = False
                last_sell = i

        return result
