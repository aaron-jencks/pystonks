from abc import ABC, abstractmethod
import multiprocessing as mp

from pystonks.utils.structures.comms import CommChannel, CommsManager, Message, KillMessage


# multiprocessing safe singleton pattern
# there's a core singleton implementation that's communicated with through pipes or queues
# then the processes are passed a wrapper class that interacts with the pipes or queues


class CoreSingleton:
    def __init__(self, cm: CommsManager):
        self.cm = cm
        self.is_stopping = False

    def get_comm(self) -> CommChannel:
        return self.cm.new_connection()

    def handle_msg(self, msg: Message):
        op = getattr(self, msg.name, None)
        if callable(op):
            op(msg)

    def kill(self, msg: KillMessage):
        self.is_stopping = True

    def start(self) -> mp.Process:
        p = mp.Process(target=core_singleton_loop, args=(self,))
        p.start()
        return p


def core_singleton_loop(singleton: CoreSingleton):
    while not singleton.is_stopping:
        msg = singleton.cm.get_msg()  # blocks to prevent spinning
        singleton.handle_msg(msg)


class SingletonWrapper:
    def __init__(self, core: CoreSingleton):
        self.comm = core.get_comm()

    def close(self):
        self.comm.close()
