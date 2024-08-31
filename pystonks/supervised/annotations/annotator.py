import datetime as dt
import os
import re
import sys
import tkinter as tk
from argparse import ArgumentParser
from pathlib import Path
from typing import List, Optional, Dict, Tuple

import matplotlib.pyplot as plt
import torch
from tqdm import tqdm

root_project_path = os.path.abspath(os.path.join('../../..'))
if root_project_path not in sys.path:
    sys.path.append(root_project_path)

from pystonks.apis.sql import SQL_DATE_FMT
from pystonks.daemons.screener import hscreener
from pystonks.market.filter import ChangeSinceNewsFilter, TickerFilter, \
    StaticFloatFilter
from pystonks.models import Bar
from pystonks.supervised.annotations.cluster import AnnotatorCluster
from pystonks.supervised.annotations.models import TradeActions, Annotation
from pystonks.supervised.annotations.utils.annotations.annotator import Annotator
from pystonks.supervised.annotations.utils.annotations.macd import MACDAnnotator
from pystonks.supervised.annotations.utils.annotations.nn import NeuralNetworkAnnotator
from pystonks.supervised.annotations.utils.gui import GeneralStockPlot
from pystonks.supervised.annotations.utils.metric_setup import MetricSetupFunc, SMA_SETUP_REGEX, setup_sma, \
    EMA_SETUP_REGEX, setup_ema, setup_signal, setup_macd
from pystonks.supervised.annotations.utils.metrics import StockMetric, \
    StockMetricModule
from pystonks.supervised.annotations.utils.models import PlotStateInfo, GeneralStockPlotInfo
from pystonks.supervised.annotations.utils.plotters import DefaultBarNewsPlotter, DefaultAnnotationPlotter, \
    DefaultStatePlotter, DefaultVolumePlotter, DefaultDerivativeStatePlotter, AutoAnnotationPlotter
from pystonks.supervised.training.definitions import INPUT_COUNT
from pystonks.utils.config import read_config
from pystonks.utils.gui.tk_modules import TkLabelModule, TkFrameModule, TkButtonModule, TkRadioSelection, \
    TkListboxModule
from pystonks.utils.processing import change_since_news, datetime_to_second_offset, fill_in_sparse_bars, \
    truncate_datetime, \
    generate_percentages_since_bar_from_bars, trim_zero_bars


ANNOTATOR_VERSION = '2.1.0'


class Window:
    def __init__(self, controllers: AnnotatorCluster, filters: List[TickerFilter], annotator: Annotator,
                 metrics: List[str], metric_setups: Dict[str, MetricSetupFunc],
                 bar_min: int, show_finished: bool = False,
                 dark: bool = False):
        self.controllers = controllers
        self.filters = filters

        self.annotator = annotator
        self.auto_annotations = []

        self.dark = dark
        self.bar_min = bar_min
        self.show_finished = show_finished

        self.dark_color = '#565956'
        self.current_tickers = []
        self.previous_tickers = []
        self.ticker = None
        self.date = truncate_datetime(dt.datetime.now(tz=dt.timezone.utc)) - dt.timedelta(days=1)  # start yesterday
        self.bars = []

        self.linewidth = 0.5

        self.metrics = metrics
        self.metric_setups = metric_setups

        self.root_tk = tk.Tk()
        self.root_tk.title(f'Annotating stock market data for None on {self.date.strftime(SQL_DATE_FMT)}')
        if self.dark:
            self.root_tk.configure(background=self.dark_color)

        # setup metrics here
        self.metric_dict: Dict[str, StockMetric] = {

        }

        self.labeled_metrics: List[StockMetricModule] = [

        ]

        self.auto_annotator = AutoAnnotationPlotter(self.annotator, self.metric_dict, self.linewidth, self.dark)

        self.pre_plotters = [
            DefaultBarNewsPlotter(self.linewidth, self.dark),
            DefaultVolumePlotter(self.linewidth, self.dark),
            self.auto_annotator,
        ]

        self.post_plotters = [
            DefaultAnnotationPlotter(self.linewidth, self.dark),
            DefaultStatePlotter(self.linewidth, self.dark),
            DefaultDerivativeStatePlotter(self.linewidth, self.dark)
        ]

        self.plot_data: Optional[GeneralStockPlotInfo] = None
        self.plot_state = PlotStateInfo()

        self.tk_plt_canvas = GeneralStockPlot(self.pre_plotters, self.post_plotters,
                                              self.dark, self.root_tk, side=tk.LEFT, fill=tk.BOTH, expand=True)

        self.tk_plt_canvas.plt_fig.canvas.mpl_connect('button_press_event', self.__on_press)
        self.tk_plt_canvas.plt_fig.canvas.mpl_connect('motion_notify_event', self.__on_motion)
        self.tk_plt_canvas.plt_fig.canvas.mpl_connect('button_release_event', self.__on_release)

        frame = TkFrameModule(self.dark, self.root_tk, side=tk.RIGHT, fill=tk.BOTH, expand=True)

        self.entry_index_label = TkLabelModule(self.dark, frame.widget)

        index_frame = TkFrameModule(self.dark, frame.widget, fill=tk.X)

        self.selected_index = TkLabelModule(self.dark, index_frame.widget)

        TkButtonModule('<', self.__dec_index, self.dark, index_frame.widget,
                       side=tk.LEFT, fill=tk.X, expand=True)
        TkButtonModule('>', self.__inc_index, self.dark, index_frame.widget,
                       side=tk.LEFT, fill=tk.X, expand=True)

        self.predicted_profit = TkLabelModule(self.dark, frame.widget)
        self.predicted_errors = TkLabelModule(self.dark, frame.widget)
        self.predicted_errors_description = TkLabelModule(self.dark, frame.widget)

        self.selected_open = TkLabelModule(self.dark, frame.widget)
        self.selected_close = TkLabelModule(self.dark, frame.widget)
        self.selected_high = TkLabelModule(self.dark, frame.widget)
        self.selected_low = TkLabelModule(self.dark, frame.widget)
        self.selected_volume = TkLabelModule(self.dark, frame.widget)
        self.selected_time = TkLabelModule(self.dark, frame.widget)

        for m in self.metrics:
            found = False
            for ms in self.metric_setups:
                if re.match(ms, m) is not None:
                    self.metric_setups[ms](
                        m,
                        self.metric_dict, self.labeled_metrics,
                        self.pre_plotters, self.post_plotters,
                        self.linewidth, self.dark, frame.widget
                    )
                    found = True
                    break
            if not found:
                raise Exception(f'unrecognized metric pattern: {m}')

        radio_values = []
        for en in TradeActions:
            if en.name == 'ACTION_COUNT':
                continue
            radio_values.append((en.name, en.value))

        self.radio_selection = TkRadioSelection(radio_values, self.dark, frame.widget, anchor=tk.W)

        button_frame = TkFrameModule(self.dark, frame.widget, fill=tk.X)

        TkButtonModule('Add Annotation', self.__add_annotation, self.dark, button_frame.widget,
                       side=tk.LEFT, fill=tk.X, expand=True)
        TkButtonModule('Edit Annotation', self.__add_annotation, self.dark, button_frame.widget,
                       side=tk.LEFT, fill=tk.X, expand=True)
        TkButtonModule('Delete Annotation', self.__delete_annotation, self.dark, button_frame.widget,
                       side=tk.LEFT, fill=tk.X, expand=True)
        TkButtonModule('Delete All Annotations', self.__delete_all_annotations, self.dark, button_frame.widget,
                       side=tk.LEFT, fill=tk.X, expand=True)
        TkButtonModule('Adopt Auto Annotations', self.__adopt_autos, self.dark, button_frame.widget,
                       side=tk.LEFT, fill=tk.X, expand=True)

        self.annotations = []
        self.anno_listbox = TkListboxModule(self.__on_list_select, self.dark, frame.widget, fill=tk.BOTH, expand=True)

        next_frame = TkFrameModule(self.dark, frame.widget, fill=tk.X)
        TkButtonModule('Previous', self.previous_ticker, self.dark, next_frame.widget,
                       side=tk.LEFT, fill=tk.X, expand=True)
        TkButtonModule('Next', self.next_ticker, self.dark, next_frame.widget,
                       side=tk.LEFT, fill=tk.X, expand=True)
        TkButtonModule('Mark Finished', self.finish_ticker, self.dark, next_frame.widget,
                       side=tk.LEFT, fill=tk.X, expand=True)
        TkButtonModule('Skip Remaining', self.skip_day, self.dark, next_frame.widget,
                       side=tk.LEFT, fill=tk.X, expand=True)

        self.entry_count = TkLabelModule(self.dark, frame.widget)

        self.update_annotation_entry_count()
        self.next_day()
        self.next_ticker()

    def __del__(self):
        plt.close(self.tk_plt_canvas.plt_fig)

    def loop(self):
        self.root_tk.mainloop()

    def __find_entry_index(self):
        self.entry_index = change_since_news(self.bars, self.news, 0.1)[1] if len(self.news) > 0 else (
                len(self.bars) - 1)
        if self.entry_index > 0:
            self.entry_index -= 1
        self.entry_index_label.set(f'Entry Index: {self.entry_index}')

    def get_date_bars(self, ticker: str, date: dt.datetime, duration: dt.timedelta) -> List[Bar]:
        return trim_zero_bars(fill_in_sparse_bars(
            truncate_datetime(date),
            truncate_datetime(date + duration),
            dt.timedelta(minutes=1),
            self.controllers.historical_bars(ticker, date, duration)
        ))

    def __get_historical_data(self):
        # TODO check the timestamp of the last bar in this response and the first timestamp of the general ones
        # if they match then we need to move the window back by a minute or two
        windows = sorted(
            [self.metric_dict[k].module.window for k in self.metric_dict if k.startswith('sma') or k.startswith('ema')]
        )
        desired_points = (windows[-1] if len(windows) > 0 else 1) + 1

        previous_date = self.date
        previous_day_bars = []
        while len(previous_day_bars) < desired_points:
            previous_date = self.next_date(previous_date)
            previous_day_bars += self.get_date_bars(self.ticker, previous_date, dt.timedelta(days=1))

        self.plot_data.update_previous_bars(
            previous_day_bars[0], generate_percentages_since_bar_from_bars(
                previous_day_bars[0],
                previous_day_bars[1:]
            )
        )

    def __update_ticker_data(self):
        self.bars = self.get_date_bars(self.ticker, self.date, dt.timedelta(days=1))
        self.news = self.controllers.historical_news(self.ticker, self.date, dt.timedelta(days=1))
        self.__find_entry_index()
        self.plot_data = GeneralStockPlotInfo(self.entry_index, self.bars, self.news, self.annotations)
        self.__get_historical_data()
        pbars = generate_percentages_since_bar_from_bars(self.plot_data.first_bar, self.bars)
        self.plot_data.update_bars(pbars)
        self.auto_annotator.process_annotations(self.entry_index, self.plot_data)
        self.root_tk.title(f'Annotating stock market data for {self.ticker} on {self.date.strftime(SQL_DATE_FMT)}')

    def get_tickers(self):
        self.controllers.cache.disable_commiting()
        print('fetching ticker symbols...', end='')
        tickers = self.controllers.get_ticker_symbols(self.date)
        self.controllers.cache.commit()
        print('DONE')
        tickers = [
            t for t in tickers
            if (self.show_finished or not self.controllers.are_annotations_finished(t, self.date))
        ]
        hscreened = hscreener(tickers, self.filters, self.date)
        print(f'Screened stocks result: \n' + "\n".join(hscreened))
        self.controllers.cache.commit()
        self.current_tickers = hscreened
        self.controllers.cache.commit()
        self.controllers.cache.reset_connection()
        self.controllers.cache.enable_commiting()

    def __is_market_open(self, date: dt.datetime) -> bool:
        return self.controllers.was_market_open(date)

    def next_date(self, start: dt.datetime) -> dt.datetime:
        start -= dt.timedelta(days=1)
        while start.weekday() > 4 or not self.__is_market_open(start):
            start -= dt.timedelta(days=1)  # skip the weekends and holidays
        return start

    def next_day(self):
        self.date = self.next_date(self.date)
        print(f'starting processing on date {self.date.strftime(SQL_DATE_FMT)}')
        self.get_tickers()

    def next_ticker(self):
        if self.ticker is not None:
            self.previous_tickers.append(self.ticker)

        while len(self.current_tickers) == 0:
            self.next_day()

        self.ticker = self.current_tickers.pop(0)
        while not self.show_finished and self.controllers.are_annotations_finished(self.ticker, self.date):
            self.ticker = self.current_tickers.pop(0)

        self.plot_state.reset()
        self.__update_ticker_data()
        self.__update_selected_from_timestamp(0)

        self.update_annotations()
        self.update_info_frame()

    def previous_ticker(self):
        while self.ticker is None or len(self.previous_tickers) == 0:
            return

        self.current_tickers.insert(0, self.ticker)

        self.ticker = self.previous_tickers.pop()
        while not self.show_finished and self.controllers.are_annotations_finished(self.ticker, self.date):
            self.ticker = self.previous_tickers.pop()

        self.plot_state.reset()
        self.__update_ticker_data()
        self.__update_selected_from_timestamp(0)

        self.update_annotations()
        self.update_info_frame()

    def skip_day(self):
        self.next_day()
        self.next_ticker()

    def finish_ticker(self):
        self.controllers.annotations.finish(self.ticker, self.date)
        self.update_annotation_entry_count()
        self.next_ticker()

    def __dec_index(self):
        if self.plot_state.selected is None:
            return

        _, _, index = self.plot_state.selected
        if index > 0:
            index -= 1
        self.__update_selected_from_index(index)

    def __inc_index(self):
        if self.plot_state.selected is None:
            return

        _, _, index = self.plot_state.selected
        if index < len(self.plot_data.bars) - 1:
            index += 1
        self.__update_selected_from_index(index)

    def __add_annotation(self):
        if self.plot_state.selected is None:
            return

        _, _, index = self.plot_state.selected
        self.controllers.create_annotation(Annotation(self.ticker,
                                                      self.bars[index].timestamp,
                                                      TradeActions(self.radio_selection.get())))
        self.update_annotation_entry_count()
        self.update_annotations()

    def __delete_annotation(self):
        if self.plot_state.selected is None:
            return

        _, _, index = self.plot_state.selected
        self.controllers.delete_annotation(self.ticker, self.bars[index+1].timestamp)
        self.update_annotation_entry_count()
        self.update_annotations()

    def __delete_all_annotations(self):
        self.controllers.delete_all_annotations(self.ticker, self.date)
        self.update_annotation_entry_count()
        self.update_annotations()

    def __adopt_autos(self):
        if len(self.auto_annotator.auto_annotations) == 0:
            return

        for idx, act in self.auto_annotator.auto_annotations:
            self.controllers.create_annotation(Annotation(self.ticker,
                                                          self.bars[idx].timestamp,
                                                          act))

        self.update_annotation_entry_count()
        self.update_annotations()

        self.auto_annotator.auto_annotations = None
        self.update_display()

    def __get_index_from_time(self, time: int):
        index = 0
        while index < len(self.plot_data.times) - 1 and self.plot_data.times[index] < time:
            index += 1
        return index

    def __update_selected_from_timestamp(self, time: int):
        index = self.__get_index_from_time(time)
        self.__update_selected_from_index(index)

    def __update_selected_from_index(self, index: int):
        self.plot_state.selected = (self.plot_data.times[index], self.plot_data.closes[index], index)
        self.update_info_frame()
        self.update_display()

    def __on_list_select(self, event):
        selected_idx = self.anno_listbox.box.curselection()[0]
        anno = self.annotations[selected_idx]
        self.__update_selected_from_timestamp(datetime_to_second_offset(anno.timestamp))

    def __on_press(self, event):
        self.plot_state.is_clicked = True
        self.plot_state.is_dragging = False
        self.plot_state.drag_start = (event.xdata, event.ydata)

    def __on_motion(self, event):
        if event.inaxes != self.tk_plt_canvas.plt_ax or not self.plot_state.is_clicked:
            return

        self.plot_state.is_dragging = True
        self.plot_state.current_pos = (event.xdata, event.ydata)
        self.update_display()

    def __on_release(self, event):
        self.plot_state.is_clicked = False

        if event.inaxes != self.tk_plt_canvas.plt_ax or event.button not in [1, 3]:
            return

        if event.button == 3:
            # reset zoom
            self.plot_state.is_zoomed = False
            self.update_display()
            return

        if self.plot_state.is_dragging:
            # handle zoom in
            self.plot_state.is_dragging = False
            drag_start = self.plot_state.drag_start[0]
            chunk_data = [t for t in self.plot_data.times if drag_start <= t <= event.xdata]
            if len(chunk_data) > 0:
                self.plot_state.is_zoomed = True
                self.plot_state.zoom_lim = (drag_start, event.xdata)
                self.update_display()
                return

        self.__update_selected_from_timestamp(event.xdata)

    def update_annotation_entry_count(self):
        self.entry_count.set(f'Symbol Days entered: {self.controllers.finished_annotations_count()}')

    def update_display(self):
        self.tk_plt_canvas.update_display(
            self.plot_state, self.plot_data
        )

    def update_info_frame(self):
        if self.plot_state.selected is None:
            self.selected_index.set(f'Index: None/{len(self.plot_data.times) - 1}')
            self.selected_open.set('Open: None')
            self.selected_close.set('Close: None')
            self.selected_high.set('High: None')
            self.selected_low.set('Low: None')
            self.selected_volume.set('Volume: None')
            self.selected_time.set('Time: None')
        else:
            time, close, index = self.plot_state.selected
            bar = self.bars[index+1]

            self.selected_index.set(f'Index: {index}/{len(self.plot_data.times) - 1}')
            self.selected_open.set(f'Open: ${bar.open:0.2f}')
            self.selected_close.set(f'Close: ${close:0.2f}')
            self.selected_high.set(f'High: ${bar.high:0.2f}')
            self.selected_low.set(f'Low: ${bar.low:0.2f}')
            self.selected_volume.set(f'Volume: {int(bar.volume)}')
            self.selected_time.set(f'Time: {bar.timestamp.isoformat()}')

            for metric in self.labeled_metrics:
                metric.update_labels(time, self.plot_data)

    def find_simulated_profit(self) -> Tuple[float, int, int, str]:
        first_error_index = -1
        error_type = ''
        errors = 0
        current_value = 1.
        # we need to know
        # value at buy, current amount held
        holds: List[Tuple[float, float]] = []
        for anno in tqdm(self.plot_data.annotations, desc='Simulating annotations'):
            idx = self.__get_index_from_time(datetime_to_second_offset(anno.timestamp))
            bar = self.plot_data.bars[idx]
            if anno.action == TradeActions.BUY_HALF:
                if current_value == 0:
                    errors += 1
                    if first_error_index < 0:
                        first_error_index = idx
                        error_type = 'over buy'
                    continue

                current_value /= 2
                holds.append((bar.close, current_value))
            elif anno.action == TradeActions.BUY_ALL:
                if current_value == 0:
                    errors += 1
                    if first_error_index < 0:
                        first_error_index = idx
                        error_type = 'over buy'
                    continue

                holds.append((bar.close, current_value))
                current_value = 0.
            elif anno.action == TradeActions.SELL_HALF:
                if len(holds) == 0:
                    errors += 1
                    if first_error_index < 0:
                        first_error_index = idx
                        error_type = 'over sell'
                    continue

                new_holds = []
                for ov, amt in holds:
                    pchange = bar.close / ov
                    namt = amt / 2
                    if namt > 0:
                        current_value += namt * pchange
                        new_holds.append((ov, namt))
            elif anno.action == TradeActions.SELL_ALL:
                if len(holds) == 0:
                    errors += 1
                    if first_error_index < 0:
                        first_error_index = idx
                        error_type = 'over sell'
                    continue

                for ov, amt in holds:
                    pchange = bar.close / ov
                    current_value += amt * pchange

                holds = []

        return current_value, errors, first_error_index, error_type

    def update_simulated_profit(self):
        profit, errors, error_idx, error_type = self.find_simulated_profit()
        self.predicted_profit.set(f'Predicted Profit: {profit:.2f}')
        self.predicted_errors.set(f'Simulated Errors: {errors}')
        if error_idx < 0:
            self.predicted_errors_description.set('First Error Description: No errors found')
        else:
            self.predicted_errors_description.set(f'First Error Description: {error_type} @ {error_idx}')

    def update_annotations(self):
        self.annotations = self.controllers.retrieve_all_annotations(self.ticker, self.date)
        self.plot_data.annotations = self.annotations
        self.update_listbox()
        self.update_simulated_profit()
        self.update_display()

    def update_listbox(self):
        self.anno_listbox.set_values(
            [f'{anno.timestamp.isoformat()} -> {anno.action.name}' for anno in self.annotations]
        )


if __name__ == '__main__':
    ap = ArgumentParser(description='allows you to create csv files with annotations for data')
    ap.add_argument('-v', '--version', action='store_true', help='print the version and exit')
    ap.add_argument('-c', '--config', type=Path, default=Path('./config.json'),
                    help='the location of the config.json to load api keys from')
    ap.add_argument('--show_finished', action='store_true', help='indicates to show finished days')
    ap.add_argument('--bar_min', type=int, default=200,
                    help='the minimum number of bars needed to be returned for annotation')
    ap.add_argument('--dark', action='store_true', help='indicates to use dark theme friendly colors')
    ap.add_argument('-m', '--model', type=Path, default=Path('./model.bin'),
                    help='the location of the stored model to use, if not present the peak annotator is used instead')
    ap.add_argument('--use_model', action='store_true', help='indicates to use the model if it exists')
    ap.add_argument(
        '--metrics',
        type=str, nargs='*',
        help='indicates the metrics to use, use --metric-list to see possible metrics, if not passed, '
             'ema_12_2, ema_26_2, macd, and signal are used'
    )
    ap.add_argument(
        '--metric_list',
        action='store_true',
        help='indicates to print the possible metrics and exit'
    )
    args = ap.parse_args()

    if args.version:
        print(f'Annotator version: v{ANNOTATOR_VERSION}')
        exit(0)

    metric_dict: Dict[str, MetricSetupFunc] = {
        SMA_SETUP_REGEX: setup_sma,
        EMA_SETUP_REGEX: setup_ema,
        'signal': setup_signal,
        'macd': setup_macd
    }

    if args.metric_list:
        print('Possible Metrics:')
        for metric_name in metric_dict:
            print(f'\t{metric_name}')
        exit(0)

    config = read_config(args.config)

    # from twitch viewer @hasenpartyyyy:
    # aaaahhhhhhhhhhhhhhhhhhhhhhhhhh
    # weeeeeeeeeeeeee
    # awim
    # awe
    # awim
    # awe
    # awim
    # awe
    # Programmer
    # at
    # 3
    # am: another
    # error
    # Progress
    # aaaa

    controllers = AnnotatorCluster(config.db_location, config.alpaca_key, config.alpaca_secret, config.polygon_key, [
        StaticFloatFilter(upper_limit=10000000)
    ])

    filters = [
        ChangeSinceNewsFilter(controllers.market, min_limit=0.1)
    ]

    annotator = MACDAnnotator()
    if args.use_model and args.model.exists():
        print('loading saved model...')
        model = torch.load(args.model)
        annotator = NeuralNetworkAnnotator(1000, INPUT_COUNT, model)

    metrics = args.metrics
    if not metrics or len(metrics) == 0:
        metrics = [
            'ema_12_2', 'ema_26_2',
            'macd', 'signal'
        ]

    win = Window(
        controllers, filters, annotator,
        metrics, metric_dict,
        args.bar_min, args.show_finished, args.dark
    )
    win.loop()
