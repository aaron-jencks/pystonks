from typing import Optional, List

from matplotlib.gridspec import GridSpec
import matplotlib.pyplot as plt
import tkinter as tk

from pystonks.supervised.annotations.utils.models import GeneralStockPlotInfo, StockPlotter, StockAxesInfo, \
    PlotStateInfo
from pystonks.utils.gui.definitions import DARK_MODE_COLOR
from pystonks.utils.gui.tk_modules import TkCanvasModule


class GeneralStockPlot(TkCanvasModule):
    def __init__(self, pre_legend_metrics: List[StockPlotter], post_legend_metrics: List[StockPlotter],
                 dark: bool = False, master: Optional[tk.Misc] = None, **pack_kwargs):
        self.plt_fig = plt.figure()
        self.gs = GridSpec(6, 1, self.plt_fig)
        self.plt_ax = self.plt_fig.add_subplot(self.gs[:3, 0])

        # subplots
        self.vax = self.plt_fig.add_subplot(self.gs[3, 0])
        self.vax.set_yscale('log')
        self.d1ax = self.plt_fig.add_subplot(self.gs[4, 0])
        self.d2ax = self.plt_fig.add_subplot(self.gs[5, 0])

        self.pre_legend_metrics = pre_legend_metrics
        self.post_legend_metrics = post_legend_metrics

        super().__init__(self.plt_fig, dark, master, **pack_kwargs)

    def update_display(self, state: PlotStateInfo, info: GeneralStockPlotInfo):
        self.plt_ax.cla()
        self.plt_ax.set_title("Candlestick Data")

        self.vax.cla()
        self.d1ax.cla()
        self.d2ax.cla()

        axes_info = StockAxesInfo(
            self.plt_ax,
            self.vax,
            self.d1ax,
            self.d2ax
        )

        for metric in self.pre_legend_metrics:
            metric.plot(axes_info, state, info)

        self.plt_ax.legend()
        # self.vax.legend()
        self.d1ax.legend()
        self.d2ax.legend()

        for metric in self.post_legend_metrics:
            metric.plot(axes_info, state, info)

        if self.dark:
            self.plt_ax.set_facecolor(DARK_MODE_COLOR)
            self.plt_fig.patch.set_facecolor(DARK_MODE_COLOR)

        self.plt_fig.canvas.draw()

