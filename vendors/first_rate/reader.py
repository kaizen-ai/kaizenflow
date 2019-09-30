import logging

import pandas as pd

from helpers import dbg

_LOG = logging.getLogger(__name__)
dbg.init_logger(verb=logging.DEBUG)


def read_pq(file_path):
    _LOG.info("Reading %s", file_path)
    df = pd.read_pq(file_path)
    return df
