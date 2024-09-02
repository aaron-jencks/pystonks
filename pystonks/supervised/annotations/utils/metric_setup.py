import re
import tkinter as tk
from typing import Dict, List, Optional, Protocol

from pystonks.supervised.annotations.utils.metrics import StockMetric, StockMetricModule, StockMetricPlotterModule, \
    SMAStockMetric, EMAStockMetric, MACDStockMetric, SignalLineMetric
from pystonks.supervised.annotations.utils.tk_modules import SMAInfoModule, EMAInfoModule
from pystonks.utils.gui.tk_modules import TkLabelModule


class MetricSetupFunc(Protocol):
    def __call__(
            self,
            name: str,
            metric_dict: Dict[str, StockMetric], labeled_metrics: List[StockMetricModule],
            pre_plotters: List[StockMetricPlotterModule], post_plotters: List[StockMetricPlotterModule],
            linewidth: float,
            dark: bool, master: Optional[tk.Misc] = None,
            **pack_kwargs
    ):
        pass


SMA_SETUP_REGEX = r'sma_(?P<window>\d+)'


def setup_sma(
        name: str,
        metric_dict: Dict[str, StockMetric], labeled_metrics: List[StockMetricModule],
        pre_plotters: List[StockMetricPlotterModule], post_plotters: List[StockMetricPlotterModule],
        linewidth: float,
        dark: bool, master: Optional[tk.Misc] = None,
        **pack_kwargs
):
    m = re.match(SMA_SETUP_REGEX, name)
    if not m:
        return
    window = int(m['window'])
    module = SMAInfoModule(window, dark, master, **pack_kwargs)
    metric = SMAStockMetric(module, linewidth, 1)
    labeled_metrics.append(metric)
    pre_plotters.append(metric)
    metric_dict[f'sma_{module.window}'] = metric


EMA_SETUP_REGEX = r'ema_(?P<window>\d+)_(?P<smoothing>\d*\.?\d+)'


def setup_ema(
        name: str,
        metric_dict: Dict[str, StockMetric], labeled_metrics: List[StockMetricModule],
        pre_plotters: List[StockMetricPlotterModule], post_plotters: List[StockMetricPlotterModule],
        linewidth: float,
        dark: bool, master: Optional[tk.Misc] = None,
        **pack_kwargs
):
    m = re.match(EMA_SETUP_REGEX, name)
    if not m:
        return
    window = int(m['window'])
    smoothing = float(m['smoothing'])
    module = EMAInfoModule(window, smoothing, dark, master, **pack_kwargs)
    metric = EMAStockMetric(module, linewidth, 1)
    labeled_metrics.append(metric)
    pre_plotters.append(metric)
    metric_dict[f'ema_{module.window}_{module.smoothing:.1f}'] = metric


def setup_macd(
        name: str,
        metric_dict: Dict[str, StockMetric], labeled_metrics: List[StockMetricModule],
        pre_plotters: List[StockMetricPlotterModule], post_plotters: List[StockMetricPlotterModule],
        linewidth: float,
        dark: bool, master: Optional[tk.Misc] = None,
        **pack_kwargs
):
    m = re.match('macd', name)
    if not m:
        return

    if 'ema_12_2.0' not in metric_dict or 'ema_26_2.0' not in metric_dict:
        raise Exception('cannot create macd without ema_12_2 and ema_26_2 created first')

    macd = MACDStockMetric(
        metric_dict['ema_12_2.0'], metric_dict['ema_26_2.0'],
        TkLabelModule(dark=dark, master=master, **pack_kwargs),
        TkLabelModule(dark=dark, master=master, **pack_kwargs),
        TkLabelModule(dark=dark, master=master, **pack_kwargs),
        linewidth, 1
    )
    labeled_metrics.append(macd)
    pre_plotters.append(macd)
    metric_dict['macd'] = macd


def setup_signal(
        name: str,
        metric_dict: Dict[str, StockMetric], labeled_metrics: List[StockMetricModule],
        pre_plotters: List[StockMetricPlotterModule], post_plotters: List[StockMetricPlotterModule],
        linewidth: float,
        dark: bool, master: Optional[tk.Misc] = None,
        **pack_kwargs
):
    m = re.match('signal', name)
    if not m:
        return

    if 'macd' not in metric_dict:
        raise Exception('cannot create signal line without macd created first')

    signal_line = SignalLineMetric(
        metric_dict['macd'],
        EMAInfoModule(9, 2, dark, master, **pack_kwargs),
        linewidth, metric_dict['macd'].zorder + 1
    )
    labeled_metrics.append(signal_line)
    pre_plotters.append(signal_line)
    metric_dict['signal'] = signal_line
