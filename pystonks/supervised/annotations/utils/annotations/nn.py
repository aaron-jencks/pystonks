from typing import List, Tuple, Dict

import torch
from torch import nn as tnn

from pystonks.models import Bar
from pystonks.supervised.annotations.models import TradeActions
from pystonks.supervised.annotations.utils.annotations.annotator import Annotator
from pystonks.supervised.annotations.utils.metrics import StockMetric
from pystonks.supervised.annotations.utils.models import GeneralStockPlotInfo
from pystonks.supervised.training.definitions import USE_PERCENTS, DEVICE
from pystonks.supervised.training.processing import generate_input_data, handle_simulated_model_response


class NeuralNetworkAnnotator(Annotator):
    def __init__(self, initial_balance: float, inputs: int, network: tnn.Module):
        self.inputs = inputs
        self.model = network
        self.initial_balance = initial_balance
        self.balance = self.initial_balance
        self.shares = 0
        self.model.to(DEVICE)
        self.model.eval()

    def reset(self):
        self.balance = self.initial_balance
        self.shares = 0

    def annotate(self, start: int, data: GeneralStockPlotInfo,
                 metrics: Dict[str, StockMetric]) -> List[Tuple[int, TradeActions]]:
        self.reset()
        actions = []
        with torch.no_grad():
            for di in range(len(data.bars[start:])):
                dslice = data.bars[start:start + di + 1]
                input_data = generate_input_data(self.balance, self.shares, USE_PERCENTS, self.inputs, dslice)
                input_data = input_data.to(DEVICE)
                pred = self.model(input_data)
                action = TradeActions(pred.argmax(0).item())
                self.balance, self.shares = handle_simulated_model_response(
                    self.balance, self.shares,
                    data.bars[start + di].close, action
                )
                if action != TradeActions.HOLD:
                    actions.append((start + di, action))
        return actions
