import math
from argparse import ArgumentParser
import datetime as dt
from pathlib import Path
import random as rng
from typing import List, Tuple

import torch
from torch.optim import SGD
from torch import nn
from torch.utils.data import Dataset, DataLoader

from pystonks.apis.sql import SQL_TIME_FMT, SQL_DATE_FMT
from pystonks.market.filter import StaticFloatFilter
from pystonks.models import Bar
from pystonks.supervised.annotations.cluster import AnnotatorCluster
from pystonks.supervised.annotations.models import TradeActions, Annotation
from pystonks.supervised.training.definitions import DEVICE, INPUT_COUNT, HIDDEN_LAYER_DEF, OUTPUT_COUNT, USE_PERCENTS
from pystonks.supervised.training.nn import TraderNeuralNetwork
from pystonks.supervised.training.processing import flatten_bars, generate_input_data, find_current_balance
from pystonks.utils.config import read_config
from pystonks.utils.processing import datetime_to_second_offset, generate_percentages_since_previous_from_bars

TRAINER_VERSION = '1.1.4'


class TradingDataset(Dataset):
    def __init__(self, cluster: AnnotatorCluster, inputs: int, initial_balance: float = 100.):
        self.annotations = cluster
        self.inputs = inputs
        self.initial_balance = initial_balance
        self.raw_anno_count = -1
        self.holds: List[Annotation] = []
        self.create_hold_rows()

    def __len__(self):
        return self.get_raw_annotation_count() * 2

    def __getitem__(self, item: int):
        if item >= self.get_raw_annotation_count():
            item -= self.get_raw_annotation_count()
            target = self.holds[item]
        else:
            target = self.get_target_annotations(item)

        bars = self.find_bar_window(target.symbol, target.timestamp)
        annotations = self.find_action_window(target.symbol, target.timestamp)
        cb, cs = find_current_balance(self.initial_balance, bars, annotations)
        weaved = generate_input_data(cb, cs, USE_PERCENTS, self.inputs, bars)
        return weaved, int(target.action.value)

    def get_raw_annotation_count(self) -> int:
        if self.raw_anno_count < 0:
            self.raw_anno_count = self.annotations.count()
        return self.raw_anno_count

    def create_hold_rows(self):
        self.holds = []
        rows = self.annotations.cache.custom_query(
            'select symbol, timestamp from bars where '
            '(symbol, timestamp) not in '
            '(select symbol, timestamp from annotations) order by date(timestamp) asc, symbol asc, timestamp asc'
        )
        rrows = rng.sample(rows, self.get_raw_annotation_count())
        self.holds = [Annotation(s, dt.datetime.fromisoformat(t), TradeActions.HOLD) for s, t in rrows]

    def find_action_window(self, symbol: str, timestamp: dt.datetime) -> List[Annotation]:
        return self.annotations.retrieve_all_annotations(symbol, timestamp)

    def find_bar_window(self, symbol: str, date: dt.datetime) -> List[Bar]:
        rows = self.annotations.cache.custom_query(
            'select timestamp, open, close, high, low, volume from bars where symbol = ? and date(timestamp) = ? '
            'order by timestamp',
            params=(symbol, date.strftime(SQL_DATE_FMT))
        )
        return [Bar(symbol, dt.datetime.fromisoformat(t), o, c, h, l, v) for t, o, c, h, l, v in rows]

    def get_target_annotations(self, item: int) -> Annotation:
        row = self.annotations.cache.custom_query(
            'select * from annotations order by date(timestamp) asc, symbol asc, timestamp asc limit 1 offset ?',
            (item,)
        )[0]
        return Annotation(row[0], dt.datetime.fromisoformat(row[1]), TradeActions[row[2]])


def training_loop(model: TraderNeuralNetwork, dataset: Dataset, batch_size: int, rate: float, epochs: int):
    size = len(dataset)

    dl = DataLoader(dataset, batch_size=batch_size, shuffle=True)
    loss_fn = nn.CrossEntropyLoss()
    optim = SGD(model.parameters(), lr=rate)

    for epoch in range(epochs):
        start = dt.datetime.now()
        for batch, (X, y) in enumerate(dl):
            X = X.to(DEVICE)
            y = y.to(DEVICE)

            pred = model(X)
            loss = loss_fn(pred, y)

            loss.backward()
            optim.step()
            optim.zero_grad()

            if batch % 10 == 0:
                loss, current = loss.item(), batch * batch_size + len(X)
                print(f"loss: {loss:>7f}  [{current:>5d}/{size:>5d}]")
        print(f"Epoch {epoch}: loss: {loss:>7f}  [{current:>5d}/{size:>5d}] "
              f"@ {(dt.datetime.now() - start).total_seconds()} sec/epoch")


def testing_loop(model: TraderNeuralNetwork, dataset: Dataset, batch_size: int):
    dl = DataLoader(dataset, batch_size=batch_size, shuffle=True)
    loss_fn = nn.CrossEntropyLoss()

    # Set the model to evaluation mode - important for batch normalization and dropout layers
    # Unnecessary in this situation but added for best practices
    model.eval()
    size = len(dl.dataset)
    num_batches = len(dl)
    test_loss, correct = 0, 0

    # Evaluating the model with torch.no_grad() ensures that no gradients are computed during test mode
    # also serves to reduce unnecessary gradient computations and memory usage for tensors with requires_grad=True
    with torch.no_grad():
        for X, y in dl:
            y = y.to(DEVICE)
            pred = model(X.to(DEVICE))
            test_loss += loss_fn(pred, y).item()
            correct += (pred.argmax(1) == y).type(torch.float).sum().item()

    test_loss /= num_batches
    correct /= size
    print(f"Test Error: \n Accuracy: {(100 * correct):>0.1f}%, Avg loss: {test_loss:>8f} \n")


if __name__ == '__main__':
    ap = ArgumentParser(description='allows you to train a neural network using the annotations created with '
                                    'annotating.py')
    ap.add_argument('-v', '--version', action='store_true', help='print the version string and exit')
    ap.add_argument('-c', '--config', type=Path, default=Path('./config.json'),
                    help='the location of the config settings file')
    ap.add_argument('--reuse', action='store_true', help='specifies to load the stored model if it exists')
    ap.add_argument('--out', type=Path, default=Path('./model.bin'), help='the path to save the model to')
    ap.add_argument('--batch', type=int, default=32, help='the batch size to use during training')
    ap.add_argument('--rate', type=float, default=0.000001, help='the learning rate to use during training')
    ap.add_argument('-e', '--epochs', type=int, default=50,
                    help='the number of training dataset iterations')
    args = ap.parse_args()

    if args.version:
        print(f'Trainer Version: v{TRAINER_VERSION}')
        exit(0)

    print(f'Pytorch using device: {DEVICE}')
    print(f'Using model with {INPUT_COUNT} inputs')

    config = read_config(args.config)

    if args.reuse and args.out.exists():
        print('loading saved model...')
        model = torch.load(args.out)
    else:
        model = TraderNeuralNetwork(INPUT_COUNT, HIDDEN_LAYER_DEF, OUTPUT_COUNT)

    model = model.to(DEVICE)
    print(model)

    ds = TradingDataset(
        AnnotatorCluster(
            config.db_location,
            config.alpaca_key, config.alpaca_secret,
            config.polygon_key, [
                StaticFloatFilter(upper_limit=10000000)
            ]
        ),
        INPUT_COUNT
    )

    training_loop(model, ds, args.batch, args.rate, args.epochs)
    testing_loop(model, ds, args.batch)
    torch.save(model, args.out)
