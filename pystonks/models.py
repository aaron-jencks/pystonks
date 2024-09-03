import datetime as dt

from pystonks.apis.sql import SQL_DT_FMT


class SymbolData:
    def __init__(self, symbol: str):
        self.symbol = symbol


class HistoricalData(SymbolData):
    def __init__(self, symbol: str, timestamp: dt.datetime):
        super().__init__(symbol)
        self.timestamp = timestamp


class Bar(HistoricalData):
    """
    Represents a single candlestick of a stock
    """
    def __init__(self, symbol: str, timestamp: dt.datetime, op: float, cl: float, h: float, lw: float, volume: int):
        """
        Creates a new bar with the given data
        :param symbol: The ticker symbol of the stock belonging to the bar
        :param timestamp: The timestamp of the bar
        :param op: The open value
        :param cl: The close value
        :param h: The high value
        :param lw: The low value
        :param volume: The volume of trades that took place
        """
        super().__init__(symbol, timestamp)
        self.open = op
        self.close = cl
        self.high = h
        self.low = lw
        self.volume = volume

    def zero(self) -> bool:
        """
        Determines if the bar is simply a filler, or contains actual data
        :return: True if the open, close, low, and high are all zero.
        """
        return self.open == 0 and self.close == 0 and self.low == 0 and self.volume == 0

    def tradeless(self) -> bool:
        """
        Determines if the bar contained no trades. This can happen if the bar is a filler after a previously active bar.
        :return: True if the volume is zero.
        """
        return self.volume == 0

    def __repr__(self) -> str:
        return (f'BAR: {{ o: {self.open}, c: {self.close}, h: {self.high}, l: {self.low}, v: {self.volume}, '
                f'ts: {self.timestamp.isoformat()} }}')


class Trade(HistoricalData):
    """
    Represents a single trade of a stock at a given time
    """
    def __init__(self, symbol: str, timestamp: dt.datetime, exchange: str, count: int, price: float):
        """
        Creates a new trade object with the given data
        :param symbol: The stock ticker symbol being traded
        :param timestamp: The timestamp that the trade was reported
        :param exchange: The stock exchange where the trade occurred
        :param count: The number of shares traded
        :param price: The price of each share
        """
        super().__init__(symbol, timestamp)
        self.exchange = exchange
        self.count = count
        self.price = price


class Quote(SymbolData):
    """
    Represents a quote, or a combination of the best ask and best bid of a certain stock exchange
    """
    def __init__(self, symbol: str,
                 ask_exchange: str, ask_size: int, ask_price: float,
                 bid_exchange: str, bid_size: int, bid_price: float):
        """
        Creates a new quote object
        :param symbol: The ticker symbol being quoted
        :param ask_exchange: The stock exchange of the buyer
        :param ask_size: The number of shares willing to be bought
        :param ask_price: The highest price the buyer is willing to pay
        :param bid_exchange: The stock exchange of the seller
        :param bid_size: The number of shares willing to be sold
        :param bid_price: The lowest price seller is willing to accept
        """
        super().__init__(symbol)
        self.ask_exchange = ask_exchange
        self.ask_size = ask_size
        self.ask_price = ask_price
        self.bid_exchange = bid_exchange
        self.bid_size = bid_size
        self.bid_price = bid_price


class HistoricalQuote(HistoricalData):
    """
    Represents a quote that took place at a given time
    """
    def __init__(self, symbol: str, timestamp: dt.datetime,
                 ask_exchange: str, ask_size: int, ask_price: float,
                 bid_exchange: str, bid_size: int, bid_price: float):
        """
        Creates a new historical quote object
        :param symbol: The ticker symbol being quoted
        :param timestamp: The timestamp at which the quote was good for
        :param ask_exchange: The stock exchange of the buyer
        :param ask_size: The number of shares willing to be bought
        :param ask_price: The highest price the buyer is willing to pay
        :param bid_exchange: The stock exchange of the seller
        :param bid_size: The number of shares willing to be sold
        :param bid_price: The lowest price seller is willing to accept
        """
        super().__init__(symbol, timestamp)
        self.ask_exchange = ask_exchange
        self.ask_size = ask_size
        self.ask_price = ask_price
        self.bid_exchange = bid_exchange
        self.bid_size = bid_size
        self.bid_price = bid_price


class News(HistoricalData):
    """
    Represents a news article
    """
    def __init__(self, symbol: str, timestamp: dt.datetime,
                 news_id: int, author: str, headline: str, url: str, updated: dt.datetime):
        """
        Creates a new news article object
        :param symbol: The symbol associated with this news article
        :param timestamp: The timestamp when the article was published
        :param news_id: The unique identifier of the article
        :param author: The author name of the article
        :param headline: The headline of the article
        :param url: The url to view the article
        :param updated: The timestamp of the last time the article was updated
        """
        super().__init__(symbol, timestamp)
        self.news_id = news_id
        self.author = author
        self.headline = headline
        self.url = url
        self.updated_at = updated

    def __repr__(self):
        return (f'{self.headline[:10]} By: {self.author} '
                f'@ {self.timestamp.isoformat()} ({self.updated_at.isoformat()})')


class TickerMeta:
    """
    Represents some ticker metadata
    """
    def __init__(self, symbol: str, date: dt.datetime, fl: int, cp: float, op: float, cso: float):
        """
        Creates a new object containing ticker metadata
        :param symbol: The stock ticker symbol
        :param date: The date associated with this data
        :param fl: The float shares of the stock company
        :param cp: The current price of the stock
        :param op: The open price of the stock
        :param cso: The percent change since open of the stock
        """
        self.symbol = symbol
        self.date = date
        self.float = fl
        self.current_price = cp
        self.open = op
        self.change_since_open = cso
