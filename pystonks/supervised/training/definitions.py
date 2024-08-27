import math

import torch

from pystonks.supervised.annotations.models import TradeActions

DEVICE = (
    "cuda"
    if torch.cuda.is_available()
    else "mps"
    if torch.backends.mps.is_available()
    else "cpu"
)

USE_PERCENTS = True


# ten second bucket count per day
# pre-market is 5.5 hours and post-market is 4 hours
# normal market hours are 6.5 hours
# total market time is 16 hours
BUCKET_SIZE = 60
HOURS = 6.5

# tsbc = 3180  # 16 hours at 60 min/hour with 6 buckets/min
# tsbc = 2340  # 6.5 hours
# tsbc = 1170  # 3.25 hours
# tsbc = 360  # 1 hour
tsbc = int(math.ceil(60 / BUCKET_SIZE * 60 * HOURS))

# if using percents then the very first value will be skipped
INPUT_COUNT = tsbc * 6 + 2 - (1 if USE_PERCENTS else 0)  # time open close high low volume, your cash and shares
HIDDEN_LAYER_DEF = [INPUT_COUNT >> 1, 512, 256, 128, 64, 32, 16]
OUTPUT_COUNT = int(TradeActions.ACTION_COUNT.value)
