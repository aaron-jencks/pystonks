import argparse
import datetime as dt
import os
import pathlib
from typing import Optional, Callable, List, Tuple
import random as rng

import pygad
import pygad.nn
import pygad.gann
import numpy as np

from pystonks.apis.sql import ReadOnlySqliteAPI
from pystonks.supervised.annotations.models import TradeActions
from pystonks.supervised.training.definitions import INPUT_COUNT
from pystonks.supervised.training.trainer import flatten_bars
from pystonks.trading.simulated import SimulatedTrader
from pystonks.unsupervised.simulation import StockSimulator, run_simulation, SimulationResults
from pystonks.utils.config import read_config
from pystonks.utils.processing import fill_in_sparse_bars


SIMULATOR_VERSION = '1.0.0'


FITNESS_SCORER = Callable[[SimulationResults], float]


class ParallelGA(pygad.GA):
    def __init__(self, sim: StockSimulator, scorer: FITNESS_SCORER, trader: SimulatedTrader,
                 gann_instance: pygad.gann.GANN, days: int, **kwargs):
        super().__init__(**kwargs)
        self.sim = sim
        self.days = days
        self.scorer = scorer
        self.trader = trader
        self.gann = gann_instance
        self.window_bars: List[Tuple[str, List[float]]] = []
        self.pick_random_days()

    def pick_random_days(self):
        print('selecting random bar data')
        symbols = self.trader.get_cached_symbols()
        sample = rng.choices(symbols, k=self.days)
        self.window_bars = []
        for symbol in sample:
            dates = self.trader.get_symbol_dates(symbol)
            d = rng.choice(dates)
            full_day = flatten_bars(fill_in_sparse_bars(
                d, d + dt.timedelta(days=1), dt.timedelta(minutes=1),
                self.trader.historical_bars(symbol, d, dt.timedelta(days=1))
            ))
            slices = []
            for i in range(len(full_day)):
                slice = full_day[:i+1]
                diff = len(full_day) - len(slice)
                slices.append([0.] * diff + slice)
            self.window_bars.append((symbol, slices))


def default_scorer(res: SimulationResults) -> float:
    if res.sells == 0 or res.buys == 0:
        return -10000
    result = 20 * res.total_profit
    comp = res.buys - 10
    result += comp * comp / -10 + 10
    comp = res.sells / (res.buys + 1) - 5
    result += comp * comp / -2.5 + 10
    result -= (res.mistakes + res.bottoms) * 30
    return result


def run_solution(ga_instance: ParallelGA, sol_idx) -> SimulationResults:
    def eval_trades(fb: List[float], cash: float, shares: float) -> TradeActions:
        predictions = pygad.nn.predict(
            last_layer=ga_instance.gann.population_networks[sol_idx],
            data_inputs=np.array([[cash, shares] + fb])
        )
        return TradeActions(predictions[0])

    return run_simulation(ga_instance.trader, ga_instance.window_bars, eval_trades)


def fitness_func(ga_instance: ParallelGA, solution, sol_idx) -> float:
    # If adaptive mutation is used, sometimes sol_idx is None.
    if sol_idx is None:
        sol_idx = 1

    result = run_solution(ga_instance, sol_idx)

    return ga_instance.scorer(result)


def callback_generation(ga_instance: ParallelGA):
    global last_fitness, SIMULATION_START

    population_matrices = pygad.gann.population_as_matrices(population_networks=GANN_instance.population_networks,
                                                            population_vectors=ga_instance.population)

    ga_instance.gann.update_population_trained_weights(population_trained_weights=population_matrices)
    ga_instance.pick_random_days()

    ns = dt.datetime.now()
    td = ns - SIMULATION_START
    bsw, bsf, _ = ga_instance.best_solution()
    fit_diff = bsf - last_fitness
    print(f"Generation      = {ga_instance.generations_completed}")
    print(f"Fitness         = {bsf}")
    print(f"Change          = {fit_diff}")
    print(f'Generation Time = {td.total_seconds():0.3f}s')

    SIMULATION_START = ns

    last_fitness = bsf.copy()
    if fit_diff != 0:
        print('saving checkpoint')
        np.save(os.path.join(
            CHECKPOINT_DIR,
            f'{ns.strftime("%Y-%m-%dT%H-%M")}_{ga_instance.generations_completed}_{int(last_fitness)}.npy'
        ), bsw)
        print('checkpoint saved')


SIMULATOR: Optional[StockSimulator] = None
START_DATE: Optional[dt.datetime] = None
SCORER = default_scorer
TRADER: Optional[SimulatedTrader] = None

GANN_instance: Optional[pygad.gann.GANN] = None
last_fitness = 0

SIMULATION_START = dt.datetime.now()
CHECKPOINT_DIR: Optional[pathlib.Path] = None


if __name__ == '__main__':
    ap = argparse.ArgumentParser(description='uses the genetic algorithm to train traders')
    ap.add_argument('-v', '--version', action='store_true', help='print the version')
    ap.add_argument('-c', '--config', type=pathlib.Path, default=pathlib.Path('../../config.json'),
                    help='the location of the settings file')
    ap.add_argument('--pop_size', type=int, default=15, help='the size of the population per iteration')
    ap.add_argument('-f', '--funds', type=float, default=1000., help='the starting cash for each element')
    ap.add_argument('-d', '--days', type=int, default=10, help='number of days to be trained on')
    ap.add_argument('--prev', type=pathlib.Path, default=pathlib.Path('../../best_solution.npy'),
                    help='the location of the best solution from the previous run')
    ap.add_argument('--prev_count', type=int, default=2,
                    help='the number of initial population to replace with the previous solution')
    ap.add_argument('--no_prev', action='store_true', help='flags to not use the previous solution')
    ap.add_argument('--checkpoint_dir', type=pathlib.Path, default=pathlib.Path('../../pygad_checkpoints'),
                    help='the location to store best solution checkpoints while training')
    args = ap.parse_args()

    if args.version:
        print(f'Simulator Version: v{SIMULATOR_VERSION}')
        exit(0)

    config = read_config(args.config)

    print(f'creating networks with {INPUT_COUNT} inputs')
    print('setting up trader and simulator')

    cache = ReadOnlySqliteAPI(config.db_location)
    TRADER = SimulatedTrader(args.funds, config.alpaca_key, config.alpaca_secret, True, cache)
    SIMULATOR = StockSimulator(TRADER)

    CHECKPOINT_DIR = args.checkpoint_dir
    os.makedirs(CHECKPOINT_DIR, exist_ok=True)

    print('creating initial population')
    GANN_instance = pygad.gann.GANN(num_solutions=args.pop_size,
                                    num_neurons_input=INPUT_COUNT,
                                    num_neurons_hidden_layers=[512, 256, 128, 64, 32, 16],
                                    num_neurons_output=TradeActions.ACTION_COUNT.value)

    if not args.no_prev and args.prev is not None and os.path.exists(args.prev):
        print('found previous best solution, loading')
        weights = np.load(args.prev)
        for i in range(args.prev_count):
            mweights = pygad.nn.layers_weights_as_matrix(GANN_instance.population_networks[i], weights)
            pygad.nn.update_layers_trained_weights(GANN_instance.population_networks[i], mweights)
        print('previous solution loaded')

    population_vectors = pygad.gann.population_as_vectors(population_networks=GANN_instance.population_networks)
    initial_population = population_vectors.copy()

    print('setting up ga runner')
    ga_instance = ParallelGA(SIMULATOR, SCORER, TRADER, GANN_instance, args.days,
                             num_generations=1000,
                             num_parents_mating=6,
                             initial_population=initial_population,
                             fitness_func=fitness_func,
                             init_range_low=-2,
                             init_range_high=5,
                             parent_selection_type='sss',
                             crossover_type='uniform',
                             mutation_type='adaptive',
                             mutation_percent_genes=[35, 10],
                             keep_elitism=2,
                             suppress_warnings=True,
                             on_generation=callback_generation)  # ,
                             # parallel_processing=('process', 32))

    print('running')
    SIMULATION_START = dt.datetime.now()
    ga_instance.run()

    ga_instance.plot_fitness()

    # Returning the details of the best solution.
    solution, solution_fitness, solution_idx = ga_instance.best_solution()
    print(f"Parameters of the best solution : {solution}")
    print(f"Fitness value of the best solution = {solution_fitness}")
    print(f"Index of the best solution : {solution_idx}")

    if ga_instance.best_solution_generation != -1:
        print(f"Best fitness value reached after {ga_instance.best_solution_generation} generations.")

    result = run_solution(ga_instance, solution_idx)
    print(f'Best values:\nProfit: ${result.total_profit:0.2f}\nBuys: {result.buys}\nSells: {result.sells}')

    print('Saving best solution')
    np.save(args.prev, solution)
    print('Saved solution')
