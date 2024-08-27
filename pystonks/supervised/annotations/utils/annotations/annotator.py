from abc import ABC, abstractmethod
from typing import List, Tuple, Dict

from pystonks.supervised.annotations.models import TradeActions
from pystonks.supervised.annotations.utils.metrics import StockMetric
from pystonks.supervised.annotations.utils.models import GeneralStockPlotInfo


class Annotator(ABC):
    @abstractmethod
    def annotate(self, start: int, data: GeneralStockPlotInfo,
                 metrics: Dict[str, StockMetric]) -> List[Tuple[int, TradeActions]]:
        pass
