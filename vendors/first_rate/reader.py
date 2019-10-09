import logging

import pandas as pd

_LOG = logging.getLogger(__name__)


def read_pq(file_path):
    """
    Read a parquet file into pandas.

    :param file_path: path to the file
    :return: pd.DataFrame
    """
    _LOG.info("Reading %s", file_path)
    df = pd.read_parquet(file_path)
    return df
