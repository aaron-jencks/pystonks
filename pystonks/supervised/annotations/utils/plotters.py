import datetime as dt
from abc import ABC
from typing import List, Dict, Tuple, Optional

from matplotlib import patches

from pystonks.supervised.annotations.models import TradeActions
from pystonks.supervised.annotations.utils.annotations.annotator import Annotator
from pystonks.supervised.annotations.utils.metrics import StockMetric
from pystonks.supervised.annotations.utils.models import StockPlotter, GeneralStockPlotInfo, StockAxesInfo, \
    PlotStateInfo
from pystonks.utils.gui.definitions import DARK_MODE_COLOR
from pystonks.utils.processing import datetime_to_second_offset


class StatedDefaultPlotter(StockPlotter, ABC):
    def __init__(self, linewidth: float, dark: bool = False):
        self.dark = dark
        self.linewidth = linewidth


class DefaultBarNewsPlotter(StatedDefaultPlotter):
    def plot(self, axes: StockAxesInfo, state: PlotStateInfo, data: GeneralStockPlotInfo):
        note_y = min(data.lows)
        note_y *= 0.95 if note_y > 0 else 1.05

        axes.default.plot(data.times, data.closes,
                          linewidth=self.linewidth, c='blue' if not self.dark else 'cyan', label='close', zorder=0)
        axes.default.scatter(data.times, data.highs,
                             s=4, c='green' if not self.dark else 'lime', marker='^', label='high', zorder=4, alpha=0.2)
        axes.default.scatter(data.times, data.lows,
                             s=4, c='red', marker='v', label='low', zorder=5, alpha=0.2)

        ntimes = [nt if nt > data.times[0] else data.times[0] for nt in data.news_times]
        axes.default.scatter(ntimes, [note_y] * len(data.news_times),
                             s=20, c='deepskyblue', marker='*', label='news', zorder=0)

        axes.default.scatter(data.times[data.entry_index], data.closes[data.entry_index],
                             s=100, c='orange', marker='P', label='inital entry', zorder=6)


class DefaultAnnotationPlotter(StatedDefaultPlotter):
    def __get_index_from_time(self, times: List[int], time: dt.datetime):
        index = 0
        dto = datetime_to_second_offset(time)
        while index < len(times) - 1 and times[index] < dto:
            index += 1
        return index

    def plot(self, axes: StockAxesInfo, state: PlotStateInfo, data: GeneralStockPlotInfo):
        buy_x = []
        buy_y = []
        sell_x = []
        sell_y = []
        hold_x = []
        hold_y = []
        for anno in data.annotations:
            index = self.__get_index_from_time(data.times, anno.timestamp)
            if anno.action.name.startswith('BUY'):
                buy_x.append(data.times[index])
                buy_y.append(data.closes[index])
            elif anno.action.name.startswith('SELL'):
                sell_x.append(data.times[index])
                sell_y.append(data.closes[index])
            else:
                hold_x.append(data.times[index])
                hold_y.append(data.closes[index])

        axes.default.scatter(buy_x, buy_y, c='green' if not self.dark else 'lime', marker='^', s=100, zorder=6)
        axes.default.scatter(sell_x, sell_y, c='red', marker='v', s=100, zorder=6)
        axes.default.scatter(hold_x, hold_y, c='orange', marker='s', s=100, zorder=6)


class DefaultStatePlotter(StatedDefaultPlotter):
    def plot(self, axes: StockAxesInfo, state: PlotStateInfo, data: GeneralStockPlotInfo):
        if state.selected is not None:
            x, y, index = state.selected
            ymn, ymx = axes.default.get_ylim()
            axes.default.plot([x, x], [ymn, ymx], c='magenta', zorder=7, linewidth=0.5, alpha=0.5)
            axes.default.scatter(x, y, c='magenta', zorder=7)

        if state.is_dragging:
            cx, cy = state.current_pos
            dx, dy = state.drag_start
            xdiff = cx - dx
            ydiff = cy - dy
            rect = patches.Rectangle((dx, dy), xdiff, ydiff,
                                     linewidth=1, edgecolor='r', facecolor='none')
            axes.default.add_patch(rect)

        if state.is_zoomed:
            cmn, cmx = state.zoom_lim
            axes.default.set_xlim(cmn, cmx)
            chunk_data = [c for t, c in zip(data.times, data.closes) if cmn <= t <= cmx]
            mncd = min(chunk_data)
            mxcd = max(chunk_data)
            axes.default.set_ylim(mncd * (0.85 if mncd > 0 else 1.15), mxcd * (1.15 if mxcd > 0 else 0.85))
            
            
class DefaultVolumePlotter(StatedDefaultPlotter):
    def plot(self, axes: StockAxesInfo, state: PlotStateInfo, data: GeneralStockPlotInfo):
        axes.volume.cla()
        axes.volume.plot(data.times, data.volumes,
                         linewidth=self.linewidth, c='blue' if not self.dark else 'aliceblue')

        axes.volume.set_ylabel('Volume')

        if state.selected is not None:
            x, y, index = state.selected
            axes.volume.scatter(x, data.volumes[index], c='cyan', zorder=2)

        if state.is_zoomed:
            axes.volume.set_xlim(*state.zoom_lim)

        if self.dark:
            axes.volume.set_facecolor(DARK_MODE_COLOR)


class DefaultDerivativeStatePlotter(StatedDefaultPlotter):
    def plot(self, axes: StockAxesInfo, state: PlotStateInfo, data: GeneralStockPlotInfo):
        xmn, xmx = axes.d1.get_xlim()

        axes.d1.plot([xmn, xmx], [0, 0], linewidth=self.linewidth, c='black', zorder=0)
        axes.d2.plot([xmn, xmx], [0, 0], linewidth=self.linewidth, c='black', zorder=0)

        axes.d1.set_ylabel('First Deriv.')
        axes.d2.set_ylabel('Second Deriv.')

        if state.selected is not None:
            x, y, index = state.selected
            axes.d1.scatter(x, 0, c='cyan', zorder=2)
            axes.d2.scatter(x, 0, c='cyan', zorder=2)

        if state.is_zoomed:
            cmn, cmx = state.zoom_lim
            axes.d1.set_xlim(cmn, cmx)
            axes.d2.set_xlim(cmn, cmx)

        if self.dark:
            axes.d1.set_facecolor(DARK_MODE_COLOR)
            axes.d2.set_facecolor(DARK_MODE_COLOR)


class AutoAnnotationPlotter(StatedDefaultPlotter):
    def __init__(self, annotator: Annotator, metric_dict: Dict[str, StockMetric], linewidth: float, dark: bool = False):
        super().__init__(linewidth, dark)
        self.annotator = annotator
        self.metrics = metric_dict
        self.auto_annotations: Optional[List[Tuple[int, TradeActions]]] = None

    def process_annotations(self, start: int, data: GeneralStockPlotInfo):
        self.auto_annotations = self.annotator.annotate(start, data, self.metrics)

    def plot(self, axes: StockAxesInfo, state: PlotStateInfo, data: GeneralStockPlotInfo):
        if self.auto_annotations is None:
            return

        ba = []
        buys = []
        sh = []
        sa = []
        other = []

        for idx, act in self.auto_annotations:
            if act == TradeActions.BUY_ALL:
                ba.append(idx)
            elif act == TradeActions.BUY_HALF:
                buys.append(idx)
            elif act == TradeActions.SELL_HALF:
                sh.append(idx)
            elif act == TradeActions.SELL_ALL:
                sa.append(idx)
            else:
                other.append(idx)

        for idxs, c, m, lbl in [
            (ba, 'lime', 'P', 'Buy All'),
            (buys, 'lime', 'v', 'Buy Half'),
            (sh, 'pink', '^', 'Sell Half'),
            (sa, 'pink', 'P', 'Sell All'),
            (other, 'yellow', 'p', 'Unknown Action')
        ]:
            if len(idxs) == 0:  # avoid having legend entries if we aren't using it
                continue

            x = [data.times[idx] for idx in idxs]
            y = [data.lows[idx] * 0.95 for idx in idxs]
            axes.default.scatter(x, y, s=100, c=c, marker=m, label=f'Auto {lbl}', alpha=0.25)
