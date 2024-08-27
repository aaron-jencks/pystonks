import json
import pathlib
import os
from typing import Dict, Any, Optional


class Config:
    def __init__(self, sjson: Dict[str, Any]):
        self.alpaca_key = sjson['alpaca_key']
        self.alpaca_secret = sjson['alpaca_secret']
        self.polygon_key = sjson['polygon_key']
        self.paper = sjson['paper']
        self.db_location = pathlib.Path(sjson['db_location'])


CONFIG_INSTANCE = None


def read_config(loc: Optional[pathlib.Path] = None) -> Config:
    global CONFIG_INSTANCE

    if CONFIG_INSTANCE is None and loc is None:
        raise Exception('config location not yet set')

    if CONFIG_INSTANCE is None:
        if not os.path.exists(loc):
            raise Exception('the config file doesn\'t exist, please see the README for how to setup the config file')

        with open(loc, 'r') as fp:
            d = json.load(fp)
            CONFIG_INSTANCE = Config(d)

    return CONFIG_INSTANCE
