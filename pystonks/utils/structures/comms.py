from abc import ABC, abstractmethod
import datetime as dt
import multiprocessing as mp
from typing import Optional

import jsonpickle


class Message:
    def __init__(self, name: str, ack: bool = True):
        self.name = name
        self.index = -1
        self.ack = ack


class OkMessage(Message):
    def __init__(self):
        super().__init__('command processed', False)


class KillMessage(Message):
    def __init__(self):
        super().__init__('kill', False)


class StampedMessage(Message):
    def __init__(self, ts: dt.datetime, name: str, ack: bool = True):
        super().__init__(name, ack)
        self.timestamp = ts


class IntervalMessage(StampedMessage):
    def __init__(self, ts: dt.datetime, dur: dt.timedelta, name: str, ack: bool = True):
        super().__init__(ts, name, ack)
        self.duration = dur


class BooleanResponseMessage(Message):
    def __init__(self, boolean: bool, name: str):
        super().__init__(name, False)
        self.boolean = boolean


class FloatResponseMessage(Message):
    def __init__(self, f: float, name: str):
        super().__init__(name, False)
        self.float = f


class IntegerResponseMessage(Message):
    def __init__(self, i: int, name: str):
        super().__init__(name, False)
        self.integer = i


class ListResponseMessage(Message):
    def __init__(self, l: list, name: str):
        super().__init__(name, False)
        self.list = l


class SymboledIntervalMessage(IntervalMessage):
    def __init__(self, symbol: str, timestamp: dt.datetime, dur: dt.timedelta, name: str, ack: bool = True):
        super().__init__(timestamp, dur, name, ack)
        self.symbol = symbol


class SymboledMessage(Message):
    def __init__(self, symbol: str, name: str, ack: bool = True):
        super().__init__(name, ack)
        self.symbol = symbol


class SymboledStampedMessage(StampedMessage):
    def __init__(self, symbol: str, timestamp: dt.datetime, name: str, ack: bool = True):
        super().__init__(timestamp, name, ack)
        self.symbol = symbol


class CommChannel(ABC):
    def __init__(self):
        super().__init__()
        self.closed: bool = False

    @abstractmethod
    def put(self, msg: Message, block: bool = True, timeout: Optional[float] = None):
        pass

    @abstractmethod
    def get(self, block: bool = True, timeout: Optional[float] = None) -> Message:
        pass

    def ackd_put(self, msg: Message) -> Message:
        self.put(msg)
        resp = self.get()
        return resp

    def close(self):
        self.closed = True


class QueueChannel(CommChannel):
    def __init__(self, index: int, q: mp.Queue, cq: mp.Queue):
        super().__init__()
        self.queue = q
        self.callback_queue = cq
        self.index = index

    def put(self, msg: Message, block: bool = True, timeout: Optional[float] = None):
        msg.index = self.index
        self.queue.put(jsonpickle.encode(msg), block, timeout)

    def get(self, block: bool = True, timeout: Optional[float] = None) -> Message:
        return jsonpickle.decode(self.callback_queue.get(block, timeout))


class CommsManager(ABC):
    def __init__(self):
        self.lock = mp.Lock()

    @abstractmethod
    def get_comm(self) -> CommChannel:
        pass

    def new_connection(self) -> CommChannel:
        cc = self.get_comm()
        return cc

    @abstractmethod
    def get_msg(self, block: bool = True, timeout: Optional[float] = None) -> Message:
        pass

    @abstractmethod
    def send_msg(self, msg: Message, block: bool = True, timeout: Optional[float] = None):
        pass

    @abstractmethod
    def close(self):
        pass


class QueueManager(CommsManager):
    def __init__(self, capacity: int = 0):
        super().__init__()
        self.q = mp.Queue(capacity)
        self.cq = mp.Queue(capacity)

    def __del__(self):
        self.q.close()
        self.cq.close()

    def get_comm(self) -> CommChannel:
        return QueueChannel(self.q, self.cq)

    def get_msg(self, block: bool = True, timeout: Optional[float] = None) -> Message:
        return jsonpickle.decode(self.q.get(block, timeout))

    def send_msg(self, msg: Message, block: bool = True, timeout: Optional[float] = None):
        self.cq.put(jsonpickle.encode(msg), block, timeout)

    def close(self):
        self.q.put(jsonpickle.encode(KillMessage()))

