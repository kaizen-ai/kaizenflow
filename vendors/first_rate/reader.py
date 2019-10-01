import logging

import pandas as pd

from helpers import dbg

_LOG = logging.getLogger(__name__)
dbg.init_logger(verb=logging.DEBUG)


def read_pq(file_path):
    """
    Read a parquet file into pandas.

    :param file_path: path to the file
    :return: pd.DataFrame
    """
    _LOG.info("Reading %s", file_path)
    df = pd.read_parquet(file_path)
    return df
