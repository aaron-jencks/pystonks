import tkinter as tk
from typing import Optional

from pystonks.utils.gui.tk_modules import TkLabelModule


class SMAInfoModule:
    def __init__(self, window: int, dark: bool = False, master: Optional[tk.Misc] = None, **pack_kwargs):
        self.window = window
        self.raw = TkLabelModule(dark=dark, master=master, **pack_kwargs)
        self.d1 = TkLabelModule(dark=dark, master=master, **pack_kwargs)
        self.d2 = TkLabelModule(dark=dark, master=master, **pack_kwargs)


class EMAInfoModule(SMAInfoModule):
    def __init__(self, window: int, smoothing: float,
                 dark: bool = False, master: Optional[tk.Misc] = None, **pack_kwargs):
        super().__init__(window, dark, master, **pack_kwargs)
        self.smoothing = smoothing
