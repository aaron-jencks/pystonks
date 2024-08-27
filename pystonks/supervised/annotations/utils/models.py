from abc import ABC, abstractmethod
from typing import List, Optional, Tuple

import matplotlib.pyplot as plt

from pystonks.models import Bar, News
from pystonks.supervised.annotations.models import Annotation
from pystonks.utils.processing import datetime_to_second_offset


class StockAxesInfo:
    def __init__(self, default: plt.Axes, volume: plt.Axes, d1: plt.Axes, d2: plt.Axes):
        self.default = default
        self.volume = volume
        self.d1 = d1
        self.d2 = d2


class GeneralStockPlotInfo:
    def __init__(self, entry_index: int, bars: List[Bar], news: List[News], annotations: List[Annotation]):
        self.entry_index = entry_index
        self.first_bar: Optional[Bar] = None
        self.bars = bars
        self.previous_bars: Optional[List[Bar]] = None
        self.previous_opens: Optional[List[float]] = None
        self.previous_closes: Optional[List[float]] = None
        self.previous_highs: Optional[List[float]] = None
        self.previous_lows: Optional[List[float]] = None
        self.previous_times: Optional[List[int]] = None
        self.previous_volumes: Optional[List[int]] = None
        self.news = news
        self.annotations = annotations
        self.opens = [b.open for b in self.bars]
        self.closes = [b.close for b in self.bars]
        self.highs = [b.high for b in self.bars]
        self.lows = [b.low for b in self.bars]
        self.times = [datetime_to_second_offset(b.timestamp) for b in self.bars]
        self.news_times = [datetime_to_second_offset(n.timestamp) for n in self.news]
        self.volumes = [b.volume for b in self.bars]

    def update_bars(self, bars: List[Bar]):
        self.bars = bars
        self.opens = [b.open for b in self.bars]
        self.closes = [b.close for b in self.bars]
        self.highs = [b.high for b in self.bars]
        self.lows = [b.low for b in self.bars]
        self.times = [datetime_to_second_offset(b.timestamp) for b in self.bars]
        self.volumes = [b.volume for b in self.bars]

    def update_previous_bars(self, first_bar: Bar, bars: List[Bar]):
        self.first_bar = first_bar
        self.previous_bars = bars
        self.previous_opens = [b.open for b in bars]
        self.previous_closes = [b.close for b in bars]
        self.previous_highs = [b.high for b in bars]
        self.previous_lows = [b.low for b in bars]
        self.previous_times = [datetime_to_second_offset(b.timestamp) for b in bars]
        self.previous_volumes = [b.volume for b in bars]


class PlotStateInfo:
    def __init__(self):
        self.selected: Optional[Tuple[int, float, int]] = None
        self.is_zoomed = False
        self.zoom_lim: Optional[Tuple[int, int]] = None
        self.is_clicked = False
        self.is_dragging = False
        self.current_pos: Optional[Tuple[int, int]] = None
        self.drag_start: Optional[Tuple[int, int]] = None

    def reset(self):
        self.selected = None
        self.is_zoomed = False
        self.is_clicked = False
        self.is_dragging = False


class StockPlotter(ABC):
    @abstractmethod
    def plot(self, axes: StockAxesInfo, state: PlotStateInfo, data: GeneralStockPlotInfo):
        pass
