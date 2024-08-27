from typing import List

from torch import nn as tnn


class TraderNeuralNetwork(tnn.Module):
    def __init__(self, inputs: int, layers: List[int], outputs: int):
        super().__init__()
        self.input_count = inputs
        self.layers_defs = layers
        self.output_count = outputs

        layer_temp = [
            tnn.Linear(self.input_count, self.layers_defs[0] if len(self.layers_defs) > 0 else self.output_count),
            tnn.ReLU()
        ]
        for li in range(len(self.layers_defs)-1):
            layer_temp.append(tnn.Linear(self.layers_defs[li], self.layers_defs[li+1]))
            layer_temp.append(tnn.ReLU())
        layer_temp.append(tnn.Linear(self.layers_defs[-1], self.output_count))

        self.layers = tnn.Sequential(*layer_temp)

    def forward(self, x):
        return self.layers(x)
