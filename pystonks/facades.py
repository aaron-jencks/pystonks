from abc import ABC, abstractmethod
import datetime as dt
from typing import List, Dict

from alpaca.data import TimeFrame

from pystonks.models import Trade, HistoricalQuote, Quote, Bar, News, TickerMeta


class TradingAPI(ABC):
    """
    Represents a facade wrapping an APIs functions to buy and sell stocks
    """
    @abstractmethod
    def buy(self, symbol: str, quantity: int, price: float):
        """
        Buys a given stock symbol
        :param symbol: The ticker symbol to buy
        :param quantity: The number of shares desired
        :param price: The maximum price willing to pay
        :return:
        """
        pass

    @abstractmethod
    def sell(self, symbol: str, count: int, price: float):
        """
        Sells a given stock symbol
        :param symbol: The ticker symbol to sell
        :param count: The number of shares to list
        :param price: The lowest price willing to accept
        :return:
        """
        pass

    @abstractmethod
    def balance(self) -> float:
        """
        Fetches the current account's balance
        :return: Returns the current account balance
        """
        pass

    @abstractmethod
    def shares(self, symbol: str) -> int:
        """
        Determines how many shares the account owns of a given stock symbol
        :param symbol: The ticker symbol to check
        :return: Returns the number of shares owned by the account
        """
        pass


class HistoricalNewsDataAPI(ABC):
    """
    Represents a facade that wraps an APIs ability to fetch historical news
    """
    @abstractmethod
    def historical_news(self, symbol: str, start: dt.datetime, dur: dt.timedelta) -> List[News]:
        """
        Fetches news articles for the given stock symbol over the given interval
        :param symbol: The ticker symbol to get articles for
        :param start: The start date of the interval
        :param dur: The duration of the interval
        :return: Returns the list of news articles released within the interval
        """
        pass


class HistoricalMarketDataAPI(ABC):
    """
    Represents a facade that wraps an APIs ability to lookup historical market data,
    except for news
    """
    @abstractmethod
    def was_market_open(self, date: dt.datetime) -> bool:
        """
        Determines if the stock market was open on the given date
        :param date: The date to check
        :return: Returns whether the market was open on that day
        """
        pass

    @abstractmethod
    def historical_bars(self, symbol: str, start: dt.datetime, dur: dt.timedelta,
                        buckets: TimeFrame = TimeFrame.Minute) -> List[Bar]:
        """
        Finds candlestick data for the given symbol over the given interval
        :param symbol: The ticker symbol to look up
        :param start: The start date of the interval
        :param dur: The duration of the interval
        :param buckets: The size of the candlesticks
        :return: Returns a list of candlesticks for the given symbol over the given period
        """
        pass

    @abstractmethod
    def historical_trades(self, symbol: str, start: dt.datetime, dur: dt.timedelta) -> List[Trade]:
        """
        Finds trade data for the given symbol over the given interval
        :param symbol: The ticker symbol to look up
        :param start: The start date of the interval
        :param dur: The duration of the interval
        :return: Returns a list of trades that occurred during the given interval
        """
        pass

    @abstractmethod
    def historical_quotes(self, symbol: str, start: dt.datetime, dur: dt.timedelta) -> List[HistoricalQuote]:
        """
        Looks up best market quotes for the given symbol over the given interval.
        The frequency of the quotes is up to implementation and is often restricted by the API in use.
        :param symbol: The ticker symbol to look for quotes for
        :param start: The start date of the interval
        :param dur: The duration of the interval
        :return: Returns a list of best market quotes for the given symbol over the interval
        """
        pass


class NewsDataAPI(HistoricalNewsDataAPI, ABC):
    """
    Represents a facade that wraps an API's ability to get news data
    """
    @abstractmethod
    def news(self, symbol: str) -> List[News]:
        """
        Finds all news articles released today for the given stock symbol
        :param symbol: The ticker symbol to look for
        :return: Returns a list of all news articles published today for the given article
        """
        pass


class MarketDataAPI(HistoricalMarketDataAPI, ABC):
    """
    Represents a facade that wraps an API's ability to interact with market data
    """
    @abstractmethod
    def is_market_open(self) -> bool:
        """
        Determines if the stock market is open today
        :return: Returns whether the stock market is open today
        """
        pass

    @abstractmethod
    def bars(self, symbol: str, buckets: TimeFrame = TimeFrame.Minute) -> List[Bar]:
        """
        Fetches candlestick data for the given symbol
        :param symbol: The symbol to get candlestick data for
        :param buckets: The size of the candlesticks
        :return: Returns a list of candlestick data for today
        """
        pass

    @abstractmethod
    def trades(self, symbol: str) -> List[Trade]:
        """
        Fetches trades that have occurred today for the given symbol
        :param symbol: The symbol to get trades for
        :return: Returns a list of all the trades that have occurred today
        """
        pass

    @abstractmethod
    def quotes(self, symbol: str) -> Quote:
        """
        Looks up best market quote for the given symbol at the current time.
        :param symbol: The ticker symbol to find quotes for
        :return: Returns the most recent market quote for the given stock
        """
        pass


class SymbolDataAPI(ABC):
    """
    Represents a facade that wraps an API's ability to get ticker metadata
    """
    @abstractmethod
    def get_ticker_symbols(self, timestamp: dt.datetime) -> List[str]:
        """
        Looks up the list of all ticker symbols that exist for the given date
        :param timestamp: The date to look for
        :return: Returns a list of all ticker symbols available
        """
        pass

    @abstractmethod
    def get_float(self, symbol: str) -> int:
        """
        Finds the float shares of the given symbol, this value does not change over time
        :param symbol: The ticker symbol to check
        :return: Returns the float shares of the given company/symbol
        """
        pass

    @abstractmethod
    def get_floats(self, symbols: List[str]) -> Dict[str, int]:
        """
        Acts the same as get_float, but can process multiple ticker symbols at the same time.
        This can reduce API pounding on the backend if implemented correctly.
        :param symbols: The list of ticker symbols to lookup floats for
        :return: Returns a map of symbols to their respective floats
        """
        pass

    @abstractmethod
    def get_ticker(self, symbol: str) -> TickerMeta:
        """
        Looks up all of the metadata for a given symbol
        :param symbol: The symbol to lookup
        :return: Returns the metadata for the given symbol
        """
        pass

    @abstractmethod
    def historical_ticker(self, symbol: str, start: dt.datetime, dur: dt.timedelta) -> List[TickerMeta]:
        """
        Finds historical metadata for a given symbol over a given interval
        :param symbol: The symbol to lookup
        :param start: The start of the interval
        :param dur: The duration of the interval
        :return: Returns a list of ticker metadata for the symbol over the given interval
        """
        pass


class UnifiedAPI(TradingAPI, MarketDataAPI, SymbolDataAPI, NewsDataAPI):
    def __init__(self, trader: TradingAPI, market: MarketDataAPI, news: NewsDataAPI, symbols: SymbolDataAPI):
        self.trader = trader
        self.market = market
        self.symbols = symbols
        self.news_api = news

    def buy(self, symbol: str, quantity: int, price: float):
        return self.trader.buy(symbol, quantity, price)

    def sell(self, symbol: str, count: int, price: float):
        return self.trader.sell(symbol, count, price)

    def balance(self) -> float:
        return self.trader.balance()

    def shares(self, symbol: str) -> int:
        return self.trader.shares(symbol)

    def is_market_open(self) -> bool:
        return self.market.is_market_open()

    def was_market_open(self, date: dt.datetime) -> bool:
        return self.market.was_market_open(date)

    def historical_bars(self, symbol: str, start: dt.datetime, dur: dt.timedelta,
                        buckets: TimeFrame = TimeFrame.Minute) -> List[Bar]:
        return self.market.historical_bars(symbol, start, dur, buckets)

    def bars(self, symbol: str, buckets: TimeFrame = TimeFrame.Minute) -> List[Bar]:
        return self.market.bars(symbol, buckets)

    def historical_trades(self, symbol: str, start: dt.datetime, dur: dt.timedelta) -> List[Trade]:
        return self.market.historical_trades(symbol, start, dur)

    def trades(self, symbol: str) -> List[Trade]:
        return self.market.trades(symbol)

    def historical_quotes(self, symbol: str, start: dt.datetime, dur: dt.timedelta) -> List[HistoricalQuote]:
        return self.market.historical_quotes(symbol, start, dur)

    def quotes(self, symbol: str) -> Quote:
        return self.market.quotes(symbol)

    def historical_news(self, symbol: str, start: dt.datetime, dur: dt.timedelta) -> List[News]:
        return self.news_api.historical_news(symbol, start, dur)

    def news(self, symbol: str) -> List[News]:
        return self.news_api.news(symbol)

    def get_ticker_symbols(self, timestamp: dt.datetime) -> List[str]:
        return self.symbols.get_ticker_symbols(timestamp)

    def get_float(self, symbol: str) -> int:
        return self.symbols.get_float(symbol)

    def get_floats(self, symbols: List[str]) -> Dict[str, int]:
        return self.symbols.get_floats(symbols)

    def get_ticker(self, symbol: str) -> TickerMeta:
        return self.symbols.get_ticker(symbol)

    def historical_ticker(self, symbol: str, start: dt.datetime, dur: dt.timedelta) -> List[TickerMeta]:
        return self.symbols.historical_ticker(symbol, start, dur)
