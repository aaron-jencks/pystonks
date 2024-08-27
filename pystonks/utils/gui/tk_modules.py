from abc import ABC, abstractmethod
from typing import Optional, List, Tuple

import matplotlib.pyplot as plt
import tkinter as tk

from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg

from pystonks.utils.gui.definitions import DARK_MODE_COLOR, BUTTON_HANDLER, EVENT_HANDLER


class TkBaseModule(ABC):
    def __init__(self, widget: tk.Widget, dark: bool = False, master: Optional[tk.Misc] = None, **pack_kwargs):
        self.master = master
        self.dark = dark
        self.widget = widget
        self.pack(**pack_kwargs)
        if self.dark:
            self.setup_darkmode()

    @abstractmethod
    def setup_darkmode(self):
        pass

    def pack(self, **kwargs):
        self.widget.pack(**kwargs)


class TkFrameModule(TkBaseModule):
    def __init__(self, dark: bool = False, master: Optional[tk.Misc] = None, **pack_kwargs):
        self.frame = tk.Frame(master)
        super().__init__(self.frame, dark, master, **pack_kwargs)

    def setup_darkmode(self):
        self.frame.configure(background=DARK_MODE_COLOR)


class TkLabelModule(TkBaseModule):
    def __init__(self, dark: bool = False, master: Optional[tk.Misc] = None, **pack_kwargs):
        self.var = tk.StringVar()
        self.label = tk.Label(master, textvariable=self.var)
        super().__init__(self.label, dark, master, **pack_kwargs)

    def setup_darkmode(self):
        self.label.configure(background=DARK_MODE_COLOR, foreground='white')

    def set(self, text: str):
        self.var.set(text)


class TkButtonModule(TkBaseModule):
    def __init__(self, text: str, handler: BUTTON_HANDLER,
                 dark: bool = False, master: Optional[tk.Misc] = None, **pack_kwargs):
        self.button = tk.Button(master, text=text, command=handler)
        super().__init__(self.button, dark, master, **pack_kwargs)

    def setup_darkmode(self):
        self.button.configure(background=DARK_MODE_COLOR, foreground='white')


class TkRadioModule(TkBaseModule):
    def __init__(self, label: str, value: int, variable: tk.IntVar,
                 dark: bool = False, master: Optional[tk.Misc] = None, **pack_kwargs):
        self.radio = tk.Radiobutton(master, text=label, variable=variable, value=value)
        super().__init__(self.radio, dark, master, **pack_kwargs)

    def setup_darkmode(self):
        self.radio.configure(background=DARK_MODE_COLOR)


class TkRadioSelection:
    def __init__(self, values: List[Tuple[str, int]],
                 dark: bool = False, master: Optional[tk.Misc] = None, **pack_kwargs):
        self.selection = tk.IntVar()
        self.radios = [TkRadioModule(vt, vv, self.selection, dark, master, **pack_kwargs) for vt, vv in values]

    def get(self) -> int:
        return self.selection.get()


class TkListboxModule(TkBaseModule):
    def __init__(self, select_handler: Optional[EVENT_HANDLER] = None,
                 dark: bool = False, master: Optional[tk.Misc] = None, **pack_kwargs):
        self.select_handler = select_handler
        self.box = tk.Listbox(master)
        self.set_handler(self.select_handler)
        self.values = []
        super().__init__(self.box, dark, master, **pack_kwargs)

    def setup_darkmode(self):
        self.box.configure(background=DARK_MODE_COLOR, foreground='white')

    def set_handler(self, handler: EVENT_HANDLER):
        self.select_handler = handler
        if self.select_handler:
            self.box.bind('<<ListboxSelect>>', self.select_handler)

    def clear_values(self):
        self.box.delete(0, tk.END)

    def set_values(self, values: List[str], clear: bool = True):
        if clear:
            self.clear_values()
        self.values = values
        for v in values:
            self.box.insert(tk.END, v)


class TkCanvasModule(TkBaseModule):
    def __init__(self, figure: plt.Figure, dark: bool = False, master: Optional[tk.Misc] = None, **pack_kwargs):
        self.canvas = FigureCanvasTkAgg(figure, master=master)
        super().__init__(self.canvas.get_tk_widget(), dark, master, **pack_kwargs)
        self.canvas.draw()

    def setup_darkmode(self):
        pass
