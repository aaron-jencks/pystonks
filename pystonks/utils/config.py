import json
import pathlib
import os
from typing import Dict, Any, Optional


class Config:
    """
    Represents a struct containing all data that can exist in the config.json file
    """
    def __init__(self, sjson: Dict[str, Any]):
        """
        Creates a new Config struct from the json dict supplied.
        This will throw an exception if the required keys do not exist
        :param sjson: The json dict
        """
        self.alpaca_key = sjson['alpaca_key']
        self.alpaca_secret = sjson['alpaca_secret']
        self.polygon_key = sjson['polygon_key']
        self.finnhub_key = sjson['finnhub_key']
        self.paper = sjson['paper']
        self.db_location = pathlib.Path(sjson['db_location'])


CONFIG_INSTANCE = None


def read_config(loc: Optional[pathlib.Path] = None) -> Config:
    """
    Gets the config struct, either from the global variable, or file.
    :param loc: The optional location of the config file, if this is None and the global variable is not initialized,
    then an exception is thrown. If the global variable, `CONFIG_INSTANCE`, is not None,
    then supplying this value does nothing.
    :return: Returns a Config struct containing the config parameters
    """
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
