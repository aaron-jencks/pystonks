from abc import ABC, abstractmethod
from typing import Callable, List, Tuple, Optional

from pystonks.models import Bar
from pystonks.supervised.annotations.utils.models import StockPlotter, StockAxesInfo, GeneralStockPlotInfo, \
    PlotStateInfo
from pystonks.supervised.annotations.utils.processing import place_on_avg
from pystonks.supervised.annotations.utils.tk_modules import SMAInfoModule, EMAInfoModule
from pystonks.utils.gui.tk_modules import TkLabelModule
from pystonks.utils.processing import calculate_normalized_derivatives, create_continuous_sma, create_continuous_ema, \
    create_ema

METRIC_HANDLER = Callable[[List[Bar]], Tuple[List[int], List[float]]]


class StockMetric(ABC):
    def __init__(self):
        self.result: Optional[Tuple[List[int], List[float]]] = None
        self.enabled = True

    @abstractmethod
    def process_data(self, data: GeneralStockPlotInfo) -> Tuple[List[int], List[float]]:
        pass

    def get_data(self, data: Optional[GeneralStockPlotInfo] = None) -> Tuple[List[int], List[float]]:
        if not self.result:
            if not data:
                raise Exception('metric fetched without bar data to process')
            self.result = self.process_data(data)
        return self.result


class StockMetricModule(StockMetric, ABC):
    def __init__(self, label: str,
                 raw_label: TkLabelModule, d1_label: TkLabelModule, d2_label: TkLabelModule,
                 label_format: str = '{}: ${:0.2f}'):
        super().__init__()
        self.labels = (raw_label, d1_label, d2_label)
        self.label_text = label
        self.label_format = label_format
        self.first_derivative: Optional[List[float]] = None
        self.second_derivative: Optional[List[float]] = None

    def process_derivatives(self, data: GeneralStockPlotInfo):
        times, data = self.get_data(data)
        self.first_derivative, self.second_derivative = calculate_normalized_derivatives(times, data)

    def process_all(self, data: GeneralStockPlotInfo):
        self.get_data(data)
        if self.first_derivative is None:
            self.process_derivatives(data)

    def __find_timestamp_index(self, ts: float) -> int:
        X, _ = self.get_data()

        if len(X) < 1 or ts < X[0]:
            return -1

        for xi, x in enumerate(X):
            if x > ts:
                return (xi - 1) if xi > 0 else 0
            if x == ts:
                return xi

        return len(X)-1

    def update_labels(self, timestamp: float, data: GeneralStockPlotInfo):
        _, raw_y = self.get_data(data)
        if self.first_derivative is None:
            self.process_derivatives(data)
        index = self.__find_timestamp_index(timestamp)

        raw, d1, d2 = self.labels

        if index < 0:
            raw.set(self.label_format.format(self.label_text, -1))
            d1.set(self.label_format.format(self.label_text + "'", -1))
            d2.set(self.label_format.format(self.label_text + "''", -1))
            return

        ry = raw_y[index]
        raw.set(self.label_format.format(self.label_text, ry))

        if index > 0:
            d1y = self.first_derivative[index-1]
            d1.set(self.label_format.format(self.label_text + "'", d1y))

            if index-1 < len(self.second_derivative):
                d2y = self.second_derivative[index-1]
                d2.set(self.label_format.format(self.label_text + "''", d2y))
            else:
                d2.set(self.label_format.format(self.label_text + "''", -1))
        else:
            d1.set(self.label_format.format(self.label_text + "'", -1))
            d2.set(self.label_format.format(self.label_text + "''", -1))


class StockMetricPlotterModule(StockMetricModule, StockPlotter, ABC):
    def __init__(self, label: str,
                 raw_label: TkLabelModule, d1_label: TkLabelModule, d2_label: TkLabelModule,
                 linewidth: float, zorder: int, colors: Optional[Tuple[str, str, str]] = None,
                 label_format: str = '{}: ${:0.2f}'):
        super().__init__(label, raw_label, d1_label, d2_label, label_format)
        self.colors = colors
        self.linewidth = linewidth
        self.zorder = zorder

    def prepare_plotting_data(self, state: PlotStateInfo, data: GeneralStockPlotInfo) -> Tuple[List[int], List[float]]:
        return self.get_data(data)

    def plot(self, axes: StockAxesInfo, state: PlotStateInfo, data: GeneralStockPlotInfo):
        if not self.enabled:
            return

        times, raw = self.prepare_plotting_data(state, data)
        if self.first_derivative is None:
            self.process_derivatives(data)

        if self.colors:
            cr, d1r, d2r = self.colors
            axes.default.plot(times, raw, label=self.label_text, linewidth=self.linewidth, c=cr, zorder=self.zorder)
            axes.d1.plot(times[1:], self.first_derivative, label=self.label_text, linewidth=self.linewidth, c=d1r, zorder=self.zorder)
            axes.d2.plot(times[1:-1], self.second_derivative, label=self.label_text, linewidth=self.linewidth, c=d2r, zorder=self.zorder)
        else:
            axes.default.plot(times, raw, label=self.label_text, linewidth=self.linewidth, zorder=self.zorder)
            axes.d1.plot(times[1:], self.first_derivative, label=self.label_text, linewidth=self.linewidth, zorder=self.zorder)
            axes.d2.plot(times[1:-1], self.second_derivative, label=self.label_text, linewidth=self.linewidth, zorder=self.zorder)


class SMAStockMetric(StockMetricPlotterModule):
    def __init__(self, module: SMAInfoModule,
                 linewidth: float, zorder: int, colors: Optional[Tuple[str, str, str]] = None,
                 label_format: str = '{}: ${:0.2f}'):
        self.module = module
        super().__init__(f'SMA {self.module.window}', self.module.raw, self.module.d1, self.module.d2,
                         linewidth, zorder, colors, label_format)

    def process_data(self, data: GeneralStockPlotInfo) -> Tuple[List[int], List[float]]:
        previous_closes  = data.previous_closes[:-self.module.window]
        return create_continuous_sma(previous_closes, data.times, data.closes, self.module.window)


class EMAStockMetric(StockMetricPlotterModule):
    def __init__(self, module: EMAInfoModule,
                 linewidth: float, zorder: int, colors: Optional[Tuple[str, str, str]] = None,
                 label_format: str = '{}: ${:0.2f}'):
        self.module = module
        super().__init__(f'EMA {self.module.window}({self.module.smoothing})',
                         self.module.raw, self.module.d1, self.module.d2,
                         linewidth, zorder, colors, label_format)

    def process_data(self, data: GeneralStockPlotInfo) -> Tuple[List[int], List[float]]:
        previous_closes = data.previous_closes[:-self.module.window]
        return create_continuous_ema(
            previous_closes, data.times, data.closes,
            self.module.window, self.module.smoothing
        )


class MACDStockMetric(StockMetricPlotterModule):
    def __init__(self, ema_a: EMAStockMetric, ema_b: EMAStockMetric,
                 raw_label: TkLabelModule, d1_label: TkLabelModule, d2_label: TkLabelModule,
                 linewidth: float, zorder: int, colors: Optional[Tuple[str, str, str]] = None,
                 label_format: str = '{}: ${:0.2f}'):
        self.ema_a = ema_a
        self.ema_b = ema_b
        super().__init__(f'MACD', raw_label, d1_label, d2_label, linewidth, zorder, colors, label_format)

    def prepare_plotting_data(self,
                              state: PlotStateInfo, data: GeneralStockPlotInfo) -> Tuple[List[int], List[float]]:
        times, values = self.get_data(data)
        return times, place_on_avg(state, data, times, values)

    def process_data(self, data: GeneralStockPlotInfo) -> Tuple[List[int], List[float]]:
        dat, day = self.ema_a.get_data(data)
        dbt, dby = self.ema_b.get_data(data)

        if dat[0] != dbt[0]:
            raise Exception('macd cannot handle emas with different starting times, add more historical data')

        macd = [day[i] - dby[i] for i in range(len(dat))]

        return dat, macd


class SignalLineMetric(StockMetricPlotterModule):
    def __init__(self, macd: MACDStockMetric, module: EMAInfoModule,
                 linewidth: float, zorder: int, colors: Optional[Tuple[str, str, str]] = None,
                 label_format: str = '{}: ${:0.2f}'):
        self.macd = macd
        self.module = module
        super().__init__(
            f'Signal {self.module.window}',
            self.module.raw, self.module.d1, self.module.d2,
            linewidth, zorder, colors, label_format
        )

    def prepare_plotting_data(self, state: PlotStateInfo, data: GeneralStockPlotInfo) -> Tuple[List[int], List[float]]:
        times, values = self.get_data(data)
        return times, place_on_avg(state, data, times, values)

    def process_data(self, data: GeneralStockPlotInfo) -> Tuple[List[int], List[float]]:
        times, bars = self.macd.get_data(data)
        return create_ema(times, bars, self.module.window, self.module.smoothing)
